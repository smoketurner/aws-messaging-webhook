//! In-memory `MailStore` test double.
//!
//! Executes the same `PlannedOp`s the real store executes, with the same
//! condition semantics and cancellation decoding, so
//! higher-level tests exercise the real planner (`mail::plan::plan_insert`)
//! and decoder (`mail::txn::decode_cancellation`) rather than a shortcut.
//! Wired into `FakeServices` by delegation.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Mutex;

use aws_messaging_webhook::mail::flows;
use aws_messaging_webhook::mail::keys::PageKey;
use aws_messaging_webhook::mail::plan::{Cond, PlannedOp, WriteOp};
use aws_messaging_webhook::mail::send::{SendKey, SendState, SendStatus};
use aws_messaging_webhook::mail::store::{
    EnqueueOutcome, ListQuery, MailStore, MailStoreError, MarkOutcome, Page, ThreadView,
};
use aws_messaging_webhook::mail::thread::ThreadState;
use aws_messaging_webhook::mail::txn::{CancellationReason, TxnDecision, decode_cancellation};
use aws_messaging_webhook::mail::{Inbox, InboxId, InsertOutcome, MailMessage, RfcHit};
use serde_dynamo::{AttributeValue, Item};

/// A scripted failure [`MailMemoryStore::inject`] queues, consumed once on
/// the next transaction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Injected {
    /// A pre-cancellation SDK failure (network, dispatch): the caller's
    /// action-path mapping treats this as transient/5xx.
    Transient,
    /// A `TransactionConflict` cancellation reason on every planned op,
    /// decoded to [`TxnDecision::Retry`].
    Conflict,
    /// A `ThrottlingError` cancellation reason on every planned op, also
    /// decoded to [`TxnDecision::Retry`].
    Throttle,
}

#[derive(Default)]
struct Inner {
    items: HashMap<(String, String), Item>,
    injected: VecDeque<Injected>,
}

#[derive(Default)]
pub struct MailMemoryStore {
    inner: Mutex<Inner>,
    /// How many transactions have been attempted, so a test can tell a flow
    /// that gave up immediately from one that burned its retries.
    attempts: std::sync::atomic::AtomicUsize,
}

/// A minimal inbound message, for tests that need one in the store without
/// driving a whole SES receipt through ingest. Callers adjust the fields the
/// test is actually about.
#[must_use]
pub fn sample_message(inbox: &str, message_id: &str, thread_id: &str) -> MailMessage {
    MailMessage {
        inbox_id: InboxId(inbox.to_owned()),
        thread_id: thread_id.to_owned(),
        message_id: message_id.to_owned(),
        ses_message_id: None,
        direction: aws_messaging_webhook::mail::Direction::Inbound,
        rfc_message_id: format!("<{message_id}@example.com>"),
        in_reply_to: None,
        labels: vec!["received".to_owned(), "unread".to_owned()],
        timestamp: "00000001-0000".to_owned(),
        from: "sender@example.com".to_owned(),
        to: vec![format!("{inbox}@example.com")],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_owned(),
        preview: "Hello there".to_owned(),
        size: 1_000,
        attachments: Vec::new(),
        attachments_truncated: false,
        raw_s3_key: Some("inbound/x".to_owned()),
        thread_snapshot: None,
        delivery: std::collections::BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 0,
        created_at: "2026-01-01T00:00:00.000Z".to_owned(),
        updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
        expires_at: 0,
    }
}

impl MailMemoryStore {
    /// Queues a scripted failure for the next transaction attempt only.
    pub fn inject(&self, failure: Injected) {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        self.inner.lock().unwrap().injected.push_back(failure);
    }

    fn key(pk: &str, sk: &str) -> (String, String) {
        (pk.to_owned(), sk.to_owned())
    }

    /// How many transactions have been attempted against this store.
    #[must_use]
    pub fn txn_attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The raw item at `pk`/`sk`, for tests asserting on an item the
    /// `MailStore` trait has no reader for.
    #[must_use]
    pub fn raw_item(&self, pk: &str, sk: &str) -> Option<Item> {
        self.get_item(pk, sk)
    }

    fn string_attr(item: &Item, name: &str) -> Option<String> {
        match item.inner().get(name) {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn version_of(item: &Item) -> Option<u64> {
        match item.inner().get("version") {
            Some(AttributeValue::N(n)) => n.parse().ok(),
            _ => None,
        }
    }

    /// Evaluates `cond` against whatever is currently stored at `pk`/`sk`.
    fn eval_cond(items: &HashMap<(String, String), Item>, pk: &str, sk: &str, cond: &Cond) -> bool {
        let existing = items.get(&Self::key(pk, sk));
        match cond {
            Cond::None => true,
            Cond::NotExists => existing.is_none(),
            Cond::VersionEquals(expected) => existing.and_then(Self::version_of) == Some(*expected),
            Cond::NotExistsOrExpired { now_epoch } => match existing {
                None => true,
                Some(item) => match item.inner().get("expires_at") {
                    Some(AttributeValue::N(n)) => {
                        n.parse::<u64>().is_ok_and(|exp| exp < *now_epoch)
                    }
                    _ => false,
                },
            },
            Cond::All(checks) => checks
                .iter()
                .all(|check| Self::check_holds(existing, check)),
        }
    }

    fn check_holds(
        existing: Option<&Item>,
        check: &aws_messaging_webhook::mail::plan::Check,
    ) -> bool {
        use aws_messaging_webhook::mail::plan::Check;
        let Check::Eq(name, expected) = check;
        existing
            .and_then(|item| item.inner().get(*name))
            .is_some_and(|actual| actual == expected)
    }

    fn op_key(op: &WriteOp) -> Option<(String, String)> {
        match op {
            WriteOp::Put { item, .. } => Some((
                Self::string_attr(item, "pk")?,
                Self::string_attr(item, "sk")?,
            )),
            WriteOp::Update { pk, sk, .. } | WriteOp::AliasFirstWriter { pk, sk, .. } => {
                Some((pk.clone(), sk.clone()))
            }
        }
    }

    fn apply_op(items: &mut HashMap<(String, String), Item>, op: &WriteOp) {
        match op {
            WriteOp::Put { item, .. } => {
                if let Some(key) = Self::op_key(op) {
                    items.insert(key, item.clone());
                }
            }
            WriteOp::Update {
                pk,
                sk,
                set,
                remove,
                ..
            } => {
                let key = Self::key(pk, sk);
                let entry = items.entry(key).or_default();
                for (name, value) in set {
                    entry.inner_mut().insert(name.clone(), value.clone());
                }
                for name in remove {
                    entry.inner_mut().remove(name);
                }
            }
            WriteOp::AliasFirstWriter {
                pk,
                sk,
                message_id,
                thread_id,
                expires_at,
            } => {
                let mut item = Item::default();
                item.inner_mut().insert(
                    "expires_at".to_owned(),
                    AttributeValue::N(expires_at.to_string()),
                );
                item.inner_mut().insert(
                    "message_id".to_owned(),
                    AttributeValue::S(message_id.clone()),
                );
                item.inner_mut()
                    .insert("thread_id".to_owned(), AttributeValue::S(thread_id.clone()));
                items.insert(Self::key(pk, sk), item);
            }
        }
    }

    /// One transaction attempt: evaluates every op's condition against the
    /// currently committed state, and either applies every write (all
    /// conditions held) or applies none (mirroring `TransactWriteItems`'
    /// all-or-nothing semantics).
    fn attempt(&self, ops: &[PlannedOp]) -> Result<flows::TxnOutcome, MailStoreError> {
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        let mut guard = self.inner.lock().unwrap();

        if let Some(injected) = guard.injected.pop_front() {
            return match injected {
                Injected::Transient => Err(MailStoreError::Transient(anyhow::anyhow!(
                    "injected transient failure"
                ))),
                Injected::Conflict | Injected::Throttle => {
                    Ok(flows::TxnOutcome::Cancelled(TxnDecision::Retry))
                }
            };
        }

        let cond_of = |op: &WriteOp| -> Cond {
            match op {
                WriteOp::Put { cond, .. } | WriteOp::Update { cond, .. } => cond.clone(),
                WriteOp::AliasFirstWriter { .. } => Cond::NotExists,
            }
        };

        let mut reasons = Vec::with_capacity(ops.len());
        let mut all_ok = true;
        for planned in ops {
            let Some((pk, sk)) = Self::op_key(&planned.op) else {
                reasons.push(CancellationReason::None);
                continue;
            };
            let cond = cond_of(&planned.op);
            if Self::eval_cond(&guard.items, &pk, &sk, &cond) {
                reasons.push(CancellationReason::None);
            } else {
                all_ok = false;
                reasons.push(CancellationReason::ConditionalCheckFailed);
            }
        }

        if !all_ok {
            return Ok(flows::TxnOutcome::Cancelled(decode_cancellation(
                ops, &reasons,
            )));
        }

        for planned in ops {
            Self::apply_op(&mut guard.items, &planned.op);
        }
        Ok(flows::TxnOutcome::Committed)
    }

    /// One page of an index partition, mirroring the real store's query: the
    /// items carrying `pk_attr == partition`, ordered by `sk_attr`, bounded
    /// exclusively by `before`/`after` and resumed after `start`.
    fn query_page<T: serde::de::DeserializeOwned>(
        &self,
        partition: &str,
        pk_attr: &str,
        sk_attr: &str,
        query: &ListQuery,
    ) -> Result<Page<T>, MailStoreError> {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        let guard = self.inner.lock().unwrap();

        let mut rows: Vec<(String, Item)> = Vec::new();
        for item in guard.items.values() {
            let matches_partition =
                Self::string_attr(item, pk_attr).is_some_and(|value| value == partition);
            if !matches_partition {
                continue;
            }
            let Some(sort) = Self::string_attr(item, sk_attr) else {
                continue;
            };
            if query.after.as_ref().is_some_and(|after| sort <= *after) {
                continue;
            }
            if query.before.as_ref().is_some_and(|before| sort >= *before) {
                continue;
            }
            if let Some(start) = &query.start {
                let seen = if query.ascending {
                    sort <= start.sort
                } else {
                    sort >= start.sort
                };
                if seen {
                    continue;
                }
            }
            rows.push((sort, item.clone()));
        }

        rows.sort_by(|(a, _), (b, _)| a.cmp(b));
        if !query.ascending {
            rows.reverse();
        }
        rows.truncate(query.limit);
        // Mirror DynamoDB's LastEvaluatedKey, including the case callers trip
        // over: a `Query` that stops because it hit its limit returns a key
        // even when nothing is left, so a full page always carries a token
        // and following it can land on an empty page.
        let filled = rows.len() == query.limit;

        // The index key plus the table key for the same item, so a token from
        // the fake exercises the same continuation path as a real one.
        let next = filled.then(|| {
            rows.last().and_then(|(sort, item)| {
                Some(PageKey {
                    partition: partition.to_owned(),
                    sort: sort.clone(),
                    table_pk: Self::string_attr(item, "pk")?,
                    table_sk: Self::string_attr(item, "sk")?,
                })
            })
        });
        let mut items = Vec::with_capacity(rows.len());
        for (_, item) in rows {
            items.push(
                serde_dynamo::from_item(item)
                    .map_err(|e| MailStoreError::Permanent(anyhow::anyhow!("query page: {e}")))?,
            );
        }
        Ok(Page {
            items,
            next: next.flatten(),
        })
    }

    fn get_item(&self, pk: &str, sk: &str) -> Option<Item> {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        self.inner
            .lock()
            .unwrap()
            .items
            .get(&Self::key(pk, sk))
            .cloned()
    }
}

impl flows::TxnStore for MailMemoryStore {
    fn read_item<T: serde::de::DeserializeOwned + Send>(
        &self,
        pk: &str,
        sk: &str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send {
        let result = self
            .get_item(pk, sk)
            .map(|item| {
                serde_dynamo::from_item(item).map_err(|e| {
                    MailStoreError::Permanent(anyhow::anyhow!("deserializing item: {e}"))
                })
            })
            .transpose();
        std::future::ready(result)
    }

    fn run_txn(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<flows::TxnOutcome, MailStoreError>> + Send {
        std::future::ready(self.attempt(ops))
    }

    /// Nothing to wait for: this store's "conflicts" are scripted, so a real
    /// backoff would only make the tests slower.
    fn pause(&self, _attempt: u32) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

impl MailStore for MailMemoryStore {
    fn get_inbox(
        &self,
        inbox: &InboxId,
    ) -> impl Future<Output = Result<Option<Inbox>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let item = self.get_item(&keys::inbox_pk(inbox.as_str()), keys::inbox_sk());
        std::future::ready(match item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item).map(Some).map_err(|e| {
                MailStoreError::Permanent(anyhow::anyhow!("deserializing inbox: {e}"))
            }),
        })
    }

    fn ensure_inbox(
        &self,
        inbox: &InboxId,
        now: &str,
    ) -> impl Future<Output = Result<Inbox, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        let mut guard = self.inner.lock().unwrap();
        let key = Self::key(&keys::inbox_pk(inbox.as_str()), keys::inbox_sk());
        if let Some(existing) = guard.items.get(&key) {
            let result = serde_dynamo::from_item(existing.clone()).map_err(|e| {
                MailStoreError::Permanent(anyhow::anyhow!("deserializing inbox: {e}"))
            });
            return std::future::ready(result);
        }
        let (inbox_record, item) = match aws_messaging_webhook::mail::plan::inbox_item(inbox, now) {
            Ok(built) => built,
            Err(error) => return std::future::ready(Err(error)),
        };
        guard.items.insert(key, item);
        std::future::ready(Ok(inbox_record))
    }

    fn message_exists(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> impl Future<Output = Result<bool, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let exists = self
            .get_item(
                &keys::inbox_pk(inbox.as_str()),
                &keys::message_sk(message_id),
            )
            .is_some();
        std::future::ready(Ok(exists))
    }

    fn resolve_rfc_ids(
        &self,
        inbox: &InboxId,
        candidates: &[String],
    ) -> impl Future<Output = Result<Option<RfcHit>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let hit = candidates.iter().find_map(|candidate| {
            let item = self.get_item(
                &keys::rfc_alias_pk(inbox.as_str(), candidate),
                keys::rfc_alias_sk(),
            )?;
            let message_id = Self::string_attr(&item, "message_id")?;
            let thread_id = Self::string_attr(&item, "thread_id")?;
            Some(RfcHit {
                message_id,
                thread_id,
            })
        });
        std::future::ready(Ok(hit))
    }

    fn insert_message(
        &self,
        msg: &MailMessage,
    ) -> impl Future<Output = Result<InsertOutcome, MailStoreError>> + Send {
        flows::insert_message(self, msg)
    }

    fn get_send_key(
        &self,
        key_hash: &str,
    ) -> impl Future<Output = Result<Option<SendKey>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let result = self
            .get_item(&keys::send_key_pk(key_hash), keys::send_key_sk())
            .map(|item| {
                serde_dynamo::from_item(item).map_err(|e| {
                    MailStoreError::Permanent(anyhow::anyhow!("deserializing send key: {e}"))
                })
            })
            .transpose();
        std::future::ready(result)
    }

    fn enqueue_send(
        &self,
        msg: &MailMessage,
        state: &SendState,
        key: Option<&SendKey>,
        now_epoch: u64,
    ) -> impl Future<Output = Result<EnqueueOutcome, MailStoreError>> + Send {
        flows::enqueue_send(self, msg, state, key, now_epoch)
    }

    fn resolve_ses_message(
        &self,
        ses_message_id: &str,
    ) -> impl Future<Output = Result<Option<(InboxId, String)>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let resolved = self
            .get_item(&keys::ses_ref_pk(ses_message_id), keys::ses_ref_sk())
            .and_then(|item| {
                let inbox = Self::string_attr(&item, "inbox_id")?;
                let message_id = Self::string_attr(&item, "message_id")?;
                Some((InboxId(inbox), message_id))
            });
        std::future::ready(Ok(resolved))
    }

    fn list_by_status(
        &self,
        status: SendStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<SendState>, MailStoreError>> + Send {
        // Goes through the same `query_page` the other listings use, keyed by
        // the status index's own attributes, so this double cannot quietly
        // disagree with the real store about which index serves a query.
        let query = ListQuery {
            inbox: InboxId(String::new()),
            limit,
            before: None,
            after: None,
            ascending: true,
            start: None,
        };
        let page = self.query_page(
            &format!("SENDSTATUS#{}", status.as_str()),
            "gsi3pk",
            "gsi3sk",
            &query,
        );
        std::future::ready(page.map(|page| page.items))
    }

    fn get_send_state(
        &self,
        message_id: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let result = self
            .get_item(&keys::outbox_pk(message_id), keys::outbox_sk())
            .map(|item| {
                serde_dynamo::from_item(item).map_err(|e| {
                    MailStoreError::Permanent(anyhow::anyhow!("deserializing send state: {e}"))
                })
            })
            .transpose();
        std::future::ready(result)
    }

    fn claim_send(
        &self,
        message_id: &str,
        now: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send {
        flows::claim_send(self, message_id, now)
    }

    fn note_ses_call(
        &self,
        claimed: &SendState,
        now: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send {
        flows::note_ses_call(self, claimed, now)
    }

    fn mark_send(
        &self,
        state: &SendState,
        outcome: MarkOutcome<'_>,
        now: &str,
    ) -> impl Future<Output = Result<(), MailStoreError>> + Send {
        flows::mark_send(self, state, outcome, now)
    }

    fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &str,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> impl Future<Output = Result<Option<Vec<String>>, MailStoreError>> + Send {
        flows::update_labels(self, inbox, message_id, add, remove, now)
    }

    fn get_message(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> impl Future<Output = Result<Option<MailMessage>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let item = self.get_item(
            &keys::inbox_pk(inbox.as_str()),
            &keys::message_sk(message_id),
        );
        std::future::ready(match item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item).map(Some).map_err(|e| {
                MailStoreError::Permanent(anyhow::anyhow!("deserializing message: {e}"))
            }),
        })
    }

    fn list_inboxes(
        &self,
        limit: usize,
        start: Option<PageKey>,
    ) -> impl Future<Output = Result<Page<Inbox>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let query = ListQuery {
            inbox: InboxId(String::new()),
            limit,
            before: None,
            after: None,
            ascending: true,
            start,
        };
        std::future::ready(self.query_page(keys::inboxes_partition(), "gsi1pk", "gsi1sk", &query))
    }

    fn list_messages(
        &self,
        query: &ListQuery,
    ) -> impl Future<Output = Result<Page<MailMessage>, MailStoreError>> + Send {
        let partition = aws_messaging_webhook::mail::keys::messages_partition(query.inbox.as_str());
        std::future::ready(self.query_page(&partition, "gsi1pk", "gsi1sk", query))
    }

    fn list_threads(
        &self,
        query: &ListQuery,
    ) -> impl Future<Output = Result<Page<ThreadState>, MailStoreError>> + Send {
        let partition = aws_messaging_webhook::mail::keys::threads_partition(query.inbox.as_str());
        std::future::ready(self.query_page(&partition, "gsi1pk", "gsi1sk", query))
    }

    fn get_thread(
        &self,
        inbox: &InboxId,
        thread_id: &str,
        limit: usize,
        start: Option<PageKey>,
    ) -> impl Future<Output = Result<Option<ThreadView>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        let thread = self.get_item(&keys::inbox_pk(inbox.as_str()), &keys::thread_sk(thread_id));
        let Some(thread) = thread else {
            return std::future::ready(Ok(None));
        };
        let thread: ThreadState = match serde_dynamo::from_item(thread) {
            Ok(thread) => thread,
            Err(e) => {
                return std::future::ready(Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "deserializing thread: {e}"
                ))));
            }
        };
        let messages = self.query_page(
            &keys::thread_messages_partition(inbox.as_str(), thread_id),
            "gsi2pk",
            "gsi2sk",
            &ListQuery {
                inbox: inbox.clone(),
                limit,
                before: None,
                after: None,
                ascending: true,
                start,
            },
        );
        std::future::ready(messages.map(|messages| Some(ThreadView { thread, messages })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(message_id: &str, thread_id: &str) -> MailMessage {
        sample_message("support@example.com", message_id, thread_id)
    }

    #[tokio::test]
    async fn ensure_inbox_creates_then_returns_the_same_inbox() {
        let store = MailMemoryStore::default();
        let inbox = InboxId("support@example.com".to_owned());
        let created = store
            .ensure_inbox(&inbox, "2026-01-01T00:00:00.000Z")
            .await
            .unwrap();
        assert_eq!(created.inbox_id, inbox);
        assert_eq!(created.email, "support@example.com");

        let fetched = store.get_inbox(&inbox).await.unwrap().unwrap();
        assert_eq!(fetched.created_at, created.created_at);

        let ensured_again = store
            .ensure_inbox(&inbox, "2026-01-02T00:00:00.000Z")
            .await
            .unwrap();
        assert_eq!(ensured_again.created_at, created.created_at);
    }

    #[tokio::test]
    async fn insert_message_is_fresh_then_a_redelivery_is_a_duplicate() {
        let store = MailMemoryStore::default();
        let msg = message("mid-1", "mid-1");

        let outcome = store.insert_message(&msg).await.unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);
        assert!(store.message_exists(&msg.inbox_id, "mid-1").await.unwrap());

        let redelivered = store.insert_message(&msg).await.unwrap();
        assert_eq!(redelivered, InsertOutcome::Duplicate);
    }

    #[tokio::test]
    async fn insert_message_into_an_existing_thread_updates_it_and_resolves_by_rfc_id() {
        let store = MailMemoryStore::default();
        let first = message("mid-1", "mid-1");
        store.insert_message(&first).await.unwrap();

        let mut second = message("mid-2", "mid-1");
        second.in_reply_to = Some("mid-1@example.com".to_owned());

        let candidates =
            aws_messaging_webhook::mail::thread::candidate_ids(second.in_reply_to.as_deref(), &[]);
        let hit = store
            .resolve_rfc_ids(&second.inbox_id, &candidates)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hit.thread_id, "mid-1");

        let outcome = store.insert_message(&second).await.unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);
    }

    #[tokio::test]
    async fn insert_message_retries_on_an_injected_conflict_then_succeeds() {
        let store = MailMemoryStore::default();
        store.inject(Injected::Conflict);
        let msg = message("mid-1", "mid-1");
        let outcome = store.insert_message(&msg).await.unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);
    }

    #[tokio::test]
    async fn insert_message_surfaces_an_injected_transient_failure() {
        let store = MailMemoryStore::default();
        store.inject(Injected::Transient);
        let msg = message("mid-1", "mid-1");
        let err = store.insert_message(&msg).await.unwrap_err();
        assert!(matches!(err, MailStoreError::Transient(_)));
    }

    /// A queued outbound message and its send state, committed the way the
    /// send API commits them.
    async fn queued_send(store: &MailMemoryStore) -> SendState {
        use aws_messaging_webhook::mail::send::Envelope;

        let mut msg = message("mid-1", "mid-1");
        msg.labels = vec!["queued".to_owned()];
        let state = SendState::queued(
            msg.inbox_id.clone(),
            msg.message_id.clone(),
            msg.thread_id.clone(),
            Envelope {
                to: vec!["recipient@example.com".to_owned()],
                ..Envelope::default()
            },
            None,
            "2026-01-01T00:00:00.000Z",
        );
        store
            .enqueue_send(&msg, &state, None, 1_800_000_000)
            .await
            .unwrap();
        state
    }

    /// Marking a send with a state another writer has already moved past is
    /// a lost claim, not something to retry: the caller's state can never
    /// satisfy the version condition again, and retrying it would race the
    /// writer that took over.
    #[tokio::test]
    async fn marking_a_send_someone_else_moved_on_reports_a_conflict() {
        let store = MailMemoryStore::default();
        let stale = queued_send(&store).await;

        // Another sender claims it, moving the version on.
        store
            .claim_send("mid-1", "2026-01-01T00:00:01.000Z")
            .await
            .unwrap()
            .unwrap();

        let attempts_before = store.txn_attempts();
        let error = store
            .mark_send(
                &stale,
                MarkOutcome::Sent(aws_messaging_webhook::mail::store::SesSent {
                    message_id: "ses-1",
                    region: "us-east-1",
                }),
                "2026-01-01T00:00:02.000Z",
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MailStoreError::Conflict), "{error:?}");
        // One attempt, then the re-read settles it: retrying a claim someone
        // else holds can never succeed, so the budget must not be spent on it.
        assert_eq!(store.txn_attempts() - attempts_before, 1);
    }

    /// A patch that adds a label the message already has, or removes one it
    /// doesn't, writes nothing: it returns the current labels without
    /// spending a transaction or bumping the message's version.
    #[tokio::test]
    async fn a_patch_that_changes_nothing_leaves_the_version_alone() {
        let store = MailMemoryStore::default();
        let mut msg = message("mid-1", "mid-1");
        msg.labels = vec!["received".to_owned(), "unread".to_owned()];
        let inbox = msg.inbox_id.clone();
        store.insert_message(&msg).await.unwrap();
        let before = store.get_message(&inbox, "mid-1").await.unwrap().unwrap();

        let labels = store
            .update_labels(
                &inbox,
                "mid-1",
                &["unread".to_owned()],
                &["spam".to_owned()],
                "2026-01-01T00:00:05.000Z",
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(labels, before.labels);
        let after = store.get_message(&inbox, "mid-1").await.unwrap().unwrap();
        assert_eq!(after.version, before.version);
        assert_eq!(after.updated_at, before.updated_at);
    }

    /// DynamoDB returns a `LastEvaluatedKey` whenever a query stops at its
    /// limit, even with nothing left, so a full page always carries a token
    /// and following it can land on an empty page. A caller that treats a
    /// token as "there is more" would loop or mislead, so this double has to
    /// reproduce it.
    #[tokio::test]
    async fn a_full_page_carries_a_token_that_leads_to_an_empty_page() {
        let store = MailMemoryStore::default();
        let inbox = InboxId("support@example.com".to_owned());
        for id in ["mid-1", "mid-2"] {
            store.insert_message(&message(id, id)).await.unwrap();
        }

        let query = |start: Option<PageKey>| ListQuery {
            inbox: inbox.clone(),
            limit: 2,
            before: None,
            after: None,
            ascending: true,
            start,
        };

        let page = store.list_messages(&query(None)).await.unwrap();
        assert_eq!(page.items.len(), 2);
        let next = page.next.expect("a full page carries a continuation key");

        let page = store.list_messages(&query(Some(next))).await.unwrap();
        assert!(page.items.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn insert_message_gives_up_after_max_retries_on_persistent_throttling() {
        let store = MailMemoryStore::default();
        for _ in 0..=flows::MAX_TXN_RETRIES {
            store.inject(Injected::Throttle);
        }
        let msg = message("mid-1", "mid-1");
        let err = store.insert_message(&msg).await.unwrap_err();
        assert!(matches!(err, MailStoreError::Conflict));
    }

    #[tokio::test]
    async fn resolve_rfc_ids_returns_none_for_unknown_candidates() {
        let store = MailMemoryStore::default();
        let inbox = InboxId("support@example.com".to_owned());
        let hit = store
            .resolve_rfc_ids(&inbox, &["nope@example.com".to_owned()])
            .await
            .unwrap();
        assert!(hit.is_none());
    }
}
