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

use aws_messaging_webhook::mail::keys::PageKey;
use aws_messaging_webhook::mail::plan::{Cond, PlannedOp, TxnKind, WriteOp};
use aws_messaging_webhook::mail::send::{SendKey, SendState};
use aws_messaging_webhook::mail::store::{
    EnqueueOutcome, ListQuery, MailStore, MailStoreError, Page, ThreadView,
};
use aws_messaging_webhook::mail::thread::{ThreadState, apply_message, new_thread};
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

/// A transaction attempt's outcome once condition evaluation has run.
enum Commit {
    /// Every condition held; the writes were applied.
    Applied,
    /// At least one condition failed.
    Cancelled(TxnDecision),
}

#[derive(Default)]
struct Inner {
    items: HashMap<(String, String), Item>,
    injected: VecDeque<Injected>,
}

/// `Retry`/`VersionConflict` loops at most this many times before giving
/// up with `MailStoreError::Conflict`.
const MAX_RETRIES: u32 = 3;

#[derive(Default)]
pub struct MailMemoryStore {
    inner: Mutex<Inner>,
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
        references: Vec::new(),
        labels: vec!["received".to_owned(), "unread".to_owned()],
        timestamp: "00000001-0000".to_owned(),
        from: "sender@example.com".to_owned(),
        reply_to: Vec::new(),
        to: vec![format!("{inbox}@example.com")],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_owned(),
        preview: "Hello there".to_owned(),
        size: 1_000,
        text: Some("hello there".to_owned()),
        html: None,
        body_truncated: false,
        headers: std::collections::BTreeMap::new(),
        attachments: Vec::new(),
        attachments_truncated: false,
        raw_s3_key: Some("inbound/x".to_owned()),
        verdicts: None,
        thread_snapshot: None,
        delivery: std::collections::BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 0,
        created_at: "2026-01-01T00:00:00.000Z".to_owned(),
        updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
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
        match check {
            Check::Exists(name) => existing.is_some_and(|item| item.inner().contains_key(*name)),
            Check::NotExists(name) => {
                !existing.is_some_and(|item| item.inner().contains_key(*name))
            }
            Check::Eq(name, expected) => existing
                .and_then(|item| item.inner().get(*name))
                .is_some_and(|actual| actual == expected),
            Check::In(name, expected) => existing
                .and_then(|item| item.inner().get(*name))
                .is_some_and(|actual| expected.contains(actual)),
        }
    }

    fn op_key(op: &WriteOp) -> Option<(String, String)> {
        match op {
            WriteOp::Put { item, .. } => Some((
                Self::string_attr(item, "pk")?,
                Self::string_attr(item, "sk")?,
            )),
            WriteOp::Update { pk, sk, .. }
            | WriteOp::Delete { pk, sk }
            | WriteOp::AliasFirstWriter { pk, sk, .. } => Some((pk.clone(), sk.clone())),
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
            WriteOp::Delete { pk, sk } => {
                items.remove(&Self::key(pk, sk));
            }
            WriteOp::AliasFirstWriter {
                pk,
                sk,
                message_id,
                thread_id,
            } => {
                let mut item = Item::default();
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
    fn attempt(&self, ops: &[PlannedOp]) -> Result<Commit, MailStoreError> {
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
                    Ok(Commit::Cancelled(TxnDecision::Retry))
                }
            };
        }

        let cond_of = |op: &WriteOp| -> Cond {
            match op {
                WriteOp::Put { cond, .. } | WriteOp::Update { cond, .. } => cond.clone(),
                WriteOp::Delete { .. } => Cond::None,
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
            let decision = decode_cancellation(TxnKind::Insert, ops, &reasons);
            return Ok(Commit::Cancelled(decision));
        }

        for planned in ops {
            Self::apply_op(&mut guard.items, &planned.op);
        }
        Ok(Commit::Applied)
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
        let more = rows.len() > query.limit;
        rows.truncate(query.limit);

        // Mirror DynamoDB's LastEvaluatedKey: the index key plus the table
        // key for the same item, so a token from the fake exercises the same
        // continuation path as a real one.
        let next = more.then(|| {
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
        let inbox_record = Inbox {
            inbox_id: inbox.clone(),
            email: inbox.as_str().to_owned(),
            display_name: None,
            metadata: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        };
        let mut item: Item = match serde_dynamo::to_item(&inbox_record) {
            Ok(item) => item,
            Err(e) => {
                return std::future::ready(Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "serializing inbox: {e}"
                ))));
            }
        };
        item.inner_mut().insert(
            "pk".to_owned(),
            AttributeValue::S(keys::inbox_pk(inbox.as_str())),
        );
        item.inner_mut().insert(
            "sk".to_owned(),
            AttributeValue::S(keys::inbox_sk().to_owned()),
        );
        item.inner_mut().insert(
            "gsi1pk".to_owned(),
            AttributeValue::S(keys::inboxes_partition().to_owned()),
        );
        item.inner_mut().insert(
            "gsi1sk".to_owned(),
            AttributeValue::S(inbox.as_str().to_owned()),
        );
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
        use aws_messaging_webhook::mail::keys;
        use aws_messaging_webhook::mail::plan::plan_insert;

        let result = (|| {
            for _attempt in 0..=MAX_RETRIES {
                let thread_before: Option<ThreadState> = self
                    .get_item(
                        &keys::inbox_pk(msg.inbox_id.as_str()),
                        &keys::thread_sk(&msg.thread_id),
                    )
                    .map(|item| {
                        serde_dynamo::from_item(item).map_err(|e| {
                            MailStoreError::Permanent(anyhow::anyhow!("deserializing thread: {e}"))
                        })
                    })
                    .transpose()?;
                let thread_after = match &thread_before {
                    Some(before) => apply_message(before, msg),
                    None => new_thread(msg),
                };

                let ops = plan_insert(msg, thread_before.as_ref(), &thread_after)?;
                match self.attempt(&ops)? {
                    Commit::Applied => return Ok(InsertOutcome::Fresh),
                    Commit::Cancelled(TxnDecision::Duplicate) => {
                        return Ok(InsertOutcome::Duplicate);
                    }
                    Commit::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {}
                    Commit::Cancelled(TxnDecision::Permanent | TxnDecision::KeyExists) => {
                        return Err(MailStoreError::Permanent(anyhow::anyhow!(
                            "ingest transaction for message {} was cancelled",
                            msg.message_id
                        )));
                    }
                }
            }
            Err(MailStoreError::Conflict)
        })();
        std::future::ready(result)
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
        use aws_messaging_webhook::mail::keys;
        use aws_messaging_webhook::mail::plan::plan_enqueue;

        let result = (|| {
            for _attempt in 0..=MAX_RETRIES {
                let thread_before: Option<ThreadState> = self
                    .get_item(
                        &keys::inbox_pk(msg.inbox_id.as_str()),
                        &keys::thread_sk(&msg.thread_id),
                    )
                    .map(|item| {
                        serde_dynamo::from_item(item).map_err(|e| {
                            MailStoreError::Permanent(anyhow::anyhow!("deserializing thread: {e}"))
                        })
                    })
                    .transpose()?;
                let thread_after = match &thread_before {
                    Some(before) => apply_message(before, msg),
                    None => new_thread(msg),
                };

                let ops = plan_enqueue(
                    msg,
                    state,
                    key,
                    thread_before.as_ref(),
                    &thread_after,
                    now_epoch,
                )?;
                match self.attempt(&ops)? {
                    Commit::Applied => return Ok(EnqueueOutcome::Committed),
                    Commit::Cancelled(TxnDecision::KeyExists) => {
                        return Ok(EnqueueOutcome::KeyExists);
                    }
                    Commit::Cancelled(TxnDecision::Duplicate) => {
                        return Ok(EnqueueOutcome::AlreadyQueued);
                    }
                    Commit::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {}
                    Commit::Cancelled(TxnDecision::Permanent) => {
                        return Err(MailStoreError::Permanent(anyhow::anyhow!(
                            "enqueue transaction for message {} was cancelled",
                            msg.message_id
                        )));
                    }
                }
            }
            Err(MailStoreError::Conflict)
        })();
        std::future::ready(result)
    }

    fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &str,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> impl Future<Output = Result<Option<Vec<String>>, MailStoreError>> + Send {
        use aws_messaging_webhook::mail::keys;
        use aws_messaging_webhook::mail::plan::plan_patch;
        use aws_messaging_webhook::mail::thread::apply_label_patch;

        let result = (|| {
            for _attempt in 0..=MAX_RETRIES {
                let Some(item) = self.get_item(
                    &keys::inbox_pk(inbox.as_str()),
                    &keys::message_sk(message_id),
                ) else {
                    return Ok(None);
                };
                let msg: MailMessage = serde_dynamo::from_item(item).map_err(|e| {
                    MailStoreError::Permanent(anyhow::anyhow!("deserializing message: {e}"))
                })?;
                let Some(thread_item) = self.get_item(
                    &keys::inbox_pk(inbox.as_str()),
                    &keys::thread_sk(&msg.thread_id),
                ) else {
                    return Err(MailStoreError::Permanent(anyhow::anyhow!(
                        "message {message_id} refers to a thread that does not exist"
                    )));
                };
                let thread_before: ThreadState =
                    serde_dynamo::from_item(thread_item).map_err(|e| {
                        MailStoreError::Permanent(anyhow::anyhow!("deserializing thread: {e}"))
                    })?;

                let mut new_labels = msg.labels.clone();
                let mut added = Vec::new();
                for label in add {
                    if !new_labels.iter().any(|existing| existing == label) {
                        new_labels.push(label.clone());
                        added.push(label.clone());
                    }
                }
                let mut removed = Vec::new();
                for label in remove {
                    if new_labels.iter().any(|existing| existing == label) {
                        new_labels.retain(|existing| existing != label);
                        removed.push(label.clone());
                    }
                }
                new_labels.sort();

                if added.is_empty() && removed.is_empty() {
                    return Ok(Some(new_labels));
                }

                let thread_after = apply_label_patch(&thread_before, &added, &removed, now);
                let ops = plan_patch(&msg, &new_labels, &thread_before, &thread_after, now)?;
                match self.attempt(&ops)? {
                    Commit::Applied => return Ok(Some(new_labels)),
                    Commit::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {}
                    Commit::Cancelled(
                        TxnDecision::Permanent | TxnDecision::KeyExists | TxnDecision::Duplicate,
                    ) => {
                        return Err(MailStoreError::Permanent(anyhow::anyhow!(
                            "label patch for message {message_id} was cancelled"
                        )));
                    }
                }
            }
            Err(MailStoreError::Conflict)
        })();
        std::future::ready(result)
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
        let partition = format!("INBOX#{}#MSG", query.inbox.as_str());
        std::future::ready(self.query_page(&partition, "gsi1pk", "gsi1sk", query))
    }

    fn list_threads(
        &self,
        query: &ListQuery,
    ) -> impl Future<Output = Result<Page<ThreadState>, MailStoreError>> + Send {
        let partition = format!("INBOX#{}#THR", query.inbox.as_str());
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
            &format!("THREAD#{}#{}", inbox.as_str(), thread_id),
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
        sample_message("support", message_id, thread_id)
    }

    #[tokio::test]
    async fn ensure_inbox_creates_then_returns_the_same_inbox() {
        let store = MailMemoryStore::default();
        let inbox = InboxId("support".to_owned());
        let created = store
            .ensure_inbox(&inbox, "2026-01-01T00:00:00.000Z")
            .await
            .unwrap();
        assert_eq!(created.inbox_id, inbox);

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

    #[tokio::test]
    async fn insert_message_gives_up_after_max_retries_on_persistent_throttling() {
        let store = MailMemoryStore::default();
        for _ in 0..=MAX_RETRIES {
            store.inject(Injected::Throttle);
        }
        let msg = message("mid-1", "mid-1");
        let err = store.insert_message(&msg).await.unwrap_err();
        assert!(matches!(err, MailStoreError::Conflict));
    }

    #[tokio::test]
    async fn resolve_rfc_ids_returns_none_for_unknown_candidates() {
        let store = MailMemoryStore::default();
        let inbox = InboxId("support".to_owned());
        let hit = store
            .resolve_rfc_ids(&inbox, &["nope@example.com".to_owned()])
            .await
            .unwrap();
        assert!(hit.is_none());
    }
}
