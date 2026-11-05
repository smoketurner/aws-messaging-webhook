//! In-memory mail [`Table`] test double.
//!
//! Executes the same `PlannedOp`s the real store executes, with the same
//! condition semantics and cancellation decoding, so
//! higher-level tests exercise the real planner (`mail::plan::plan_insert`)
//! and decoder (`mail::txn::decode_cancellation`) rather than a shortcut.
//! The test backend's `Table`.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};

use aws_messaging_webhook::mail::keys::{self, PageKey, TableKey};
use aws_messaging_webhook::mail::mailbox::Mailbox;
use aws_messaging_webhook::mail::plan::{Cond, PlannedOp, WriteOp};
use aws_messaging_webhook::mail::send::SendKey;
use aws_messaging_webhook::mail::store::{MailStoreError, Page};
use aws_messaging_webhook::mail::table::{PageQuery, Read, Table, TxnOutcome};
use aws_messaging_webhook::mail::txn::{CancellationReason, TxnDecision, decode_cancellation};
use aws_messaging_webhook::mail::{InboxId, MailMessage, MessageId, PutOutcome, ThreadId};
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
    /// The idempotency-key condition check failed: another request won the
    /// race and committed the same key. Decoded to
    /// [`TxnDecision::KeyExists`], the outcome the send API's
    /// `EnqueueOutcome::KeyExists` path (which runs `discard_uploads`) is
    /// reached with. Lets a sequential test exercise a cleanup path that is
    /// otherwise only reachable when two sends race for the same key.
    KeyExists,
}

/// A scripted failure [`MailMemoryStore::fail_next_resolve`] queues for the
/// next `resolve_ses_message` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFailure {
    /// Maps to [`MailStoreError::Transient`]: worth a retry.
    Transient,
    /// Maps to [`MailStoreError::Permanent`]: a retry would fail the same way.
    Permanent,
}

#[derive(Default)]
struct Inner {
    items: HashMap<(String, String), Item>,
    injected: VecDeque<Injected>,
    resolve_failure: Option<ReadFailure>,
}

/// Clones share one table, so the harness and the fakes that script its
/// failures see the same rows.
#[derive(Clone, Default)]
pub struct MailMemoryStore {
    inner: Arc<Mutex<Inner>>,
    /// How many transactions have been attempted, so a test can tell a flow
    /// that gave up immediately from one that burned its retries.
    attempts: Arc<std::sync::atomic::AtomicUsize>,
}

/// A minimal inbound message, for tests that need one in the store without
/// driving a whole SES receipt through ingest. Callers adjust the fields the
/// test is actually about.
#[must_use]
pub fn sample_message(inbox: &str, message_id: &str, thread_id: &str) -> MailMessage {
    MailMessage {
        inbox_id: InboxId::new(inbox),
        thread_id: ThreadId::new(thread_id),
        message_id: MessageId::new(message_id),
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
    /// The mailbox over this store, for tests that seed or inspect it.
    pub fn mailbox(&self) -> Mailbox<&Self> {
        Mailbox::new(self)
    }

    /// Queues a scripted failure for the next transaction attempt only.
    pub fn inject(&self, failure: Injected) {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        self.inner.lock().unwrap().injected.push_back(failure);
    }

    /// Fails the next `resolve_ses_message` call only.
    pub fn fail_next_resolve(&self, failure: ReadFailure) {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        let mut inner = self.inner.lock().unwrap();
        inner.resolve_failure = Some(failure);
    }

    fn key(pk: &str, sk: &str) -> (String, String) {
        (pk.to_owned(), sk.to_owned())
    }

    /// How many transactions have been attempted against this store.
    #[must_use]
    pub fn txn_attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The raw item at `key`, for tests asserting on an item the
    /// `Mailbox` has no reader for.
    #[must_use]
    pub fn raw_item(&self, key: &TableKey) -> Option<Item> {
        self.get_item(key)
    }

    /// Seeds a `SendKey` item directly, bypassing the transaction path, so a
    /// test can place an `Idempotency-Key` row with an arbitrary `expires_at`
    /// (e.g. in the past) and exercise the read-side expiry logic in
    /// `replay_key` — DynamoDB reaps expired rows lazily, so an expired row
    /// that is still present is exactly the case the read path must handle.
    pub fn seed_send_key(&self, key: &SendKey) {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: SendKey is always serializable; a poisoned lock is a test bug"
        )]
        let mut item: Item = serde_dynamo::to_item(key).unwrap();
        let TableKey { pk, sk } = keys::send_key(&key.key_hash);
        item.inner_mut()
            .insert("pk".to_owned(), AttributeValue::S(pk.clone()));
        item.inner_mut()
            .insert("sk".to_owned(), AttributeValue::S(sk.clone()));
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        self.inner.lock().unwrap().items.insert((pk, sk), item);
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
                if let Some(message_id) = message_id {
                    item.inner_mut().insert(
                        "message_id".to_owned(),
                        AttributeValue::S(message_id.clone()),
                    );
                }
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
    fn attempt(&self, ops: &[PlannedOp]) -> Result<TxnOutcome, MailStoreError> {
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
                    Ok(TxnOutcome::Cancelled(TxnDecision::Retry))
                }
                Injected::KeyExists => Ok(TxnOutcome::Cancelled(TxnDecision::KeyExists)),
            };
        }

        let holds = |items: &HashMap<(String, String), Item>, op: &WriteOp, pk: &str, sk: &str| {
            match op {
                WriteOp::Put { cond, .. } | WriteOp::Update { cond, .. } => {
                    Self::eval_cond(items, pk, sk, cond)
                }
                // `attribute_not_exists(pk)` for a placeholder; a real alias
                // also lands on a row without a `message_id`.
                WriteOp::AliasFirstWriter { message_id, .. } => {
                    match items.get(&Self::key(pk, sk)) {
                        None => true,
                        Some(existing) => {
                            message_id.is_some() && !existing.inner().contains_key("message_id")
                        }
                    }
                }
            }
        };

        let mut reasons = Vec::with_capacity(ops.len());
        let mut all_ok = true;
        for planned in ops {
            let Some((pk, sk)) = Self::op_key(&planned.op) else {
                reasons.push(CancellationReason::None);
                continue;
            };
            if holds(&guard.items, &planned.op, &pk, &sk) {
                reasons.push(CancellationReason::None);
            } else {
                all_ok = false;
                reasons.push(CancellationReason::ConditionalCheckFailed);
            }
        }

        if !all_ok {
            return Ok(TxnOutcome::Cancelled(decode_cancellation(ops, &reasons)));
        }

        for planned in ops {
            Self::apply_op(&mut guard.items, &planned.op);
        }
        Ok(TxnOutcome::Committed)
    }

    /// One page of an index partition, mirroring the real store's query: the
    /// items whose index partition attribute is `query.partition`, ordered by
    /// the index sort attribute, bounded exclusively by `before`/`after` and
    /// resumed after `start`.
    fn query_page<T: serde::de::DeserializeOwned>(
        &self,
        query: PageQuery<'_>,
    ) -> Result<Page<T>, MailStoreError> {
        let partition = query.partition;
        let (pk_attr, sk_attr) = query.index.key_attributes();
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
            if query.after.is_some_and(|after| sort.as_str() <= after) {
                continue;
            }
            if query.before.is_some_and(|before| sort.as_str() >= before) {
                continue;
            }
            if let Some(start) = query.start {
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
            items.push(serde_dynamo::from_item(item).map_err(|e| {
                MailStoreError::Permanent(anyhow::anyhow!("{}: {e}", query.context))
            })?);
        }
        Ok(Page {
            items,
            next: next.flatten(),
        })
    }

    fn get_item(&self, key: &TableKey) -> Option<Item> {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        self.inner
            .lock()
            .unwrap()
            .items
            .get(&Self::key(&key.pk, &key.sk))
            .cloned()
    }

    /// Reads one item by key, deserialized into `T`; `None` when absent.
    fn read<T: serde::de::DeserializeOwned>(
        &self,
        key: &TableKey,
        context: &'static str,
    ) -> Result<Option<T>, MailStoreError> {
        self.get_item(key)
            .map(|item| {
                serde_dynamo::from_item(item)
                    .map_err(|e| MailStoreError::Permanent(anyhow::anyhow!("{context}: {e}")))
            })
            .transpose()
    }
}

impl Table for MailMemoryStore {
    fn get<T: serde::de::DeserializeOwned + Send>(
        &self,
        key: TableKey,
        _read: Read,
        context: &'static str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send {
        // `fail_next_resolve` scripts the SES-reference read alone.
        if key.sk == keys::SES_REF_SK {
            #[expect(
                clippy::unwrap_used,
                reason = "test double: a poisoned lock is a test bug"
            )]
            let failure = self.inner.lock().unwrap().resolve_failure.take();
            match failure {
                Some(ReadFailure::Transient) => {
                    return std::future::ready(Err(MailStoreError::Transient(anyhow::anyhow!(
                        "injected transient read failure"
                    ))));
                }
                Some(ReadFailure::Permanent) => {
                    return std::future::ready(Err(MailStoreError::Permanent(anyhow::anyhow!(
                        "injected permanent read failure"
                    ))));
                }
                None => {}
            }
        }
        std::future::ready(self.read(&key, context))
    }

    fn batch_get<T: serde::de::DeserializeOwned + Send>(
        &self,
        keys: Vec<TableKey>,
        _read: Read,
        _context: &'static str,
    ) -> impl Future<Output = Result<HashMap<TableKey, T>, MailStoreError>> + Send {
        let mut found = HashMap::new();
        for key in keys {
            let Some(item) = self.get_item(&key) else {
                continue;
            };
            let Ok(value) = serde_dynamo::from_item(item) else {
                continue;
            };
            found.insert(key, value);
        }
        std::future::ready(Ok(found))
    }

    fn query<T: serde::de::DeserializeOwned + Send>(
        &self,
        query: PageQuery<'_>,
    ) -> impl Future<Output = Result<Page<T>, MailStoreError>> + Send {
        std::future::ready(self.query_page(query))
    }

    fn put_if_absent(
        &self,
        item: Item,
        _context: &'static str,
    ) -> impl Future<Output = Result<PutOutcome, MailStoreError>> + Send {
        #[expect(
            clippy::unwrap_used,
            reason = "test double: a poisoned lock is a test bug"
        )]
        let mut guard = self.inner.lock().unwrap();
        let outcome = match (
            Self::string_attr(&item, "pk"),
            Self::string_attr(&item, "sk"),
        ) {
            (Some(pk), Some(sk)) => match guard.items.entry(Self::key(&pk, &sk)) {
                std::collections::hash_map::Entry::Occupied(_) => Ok(PutOutcome::AlreadyExists),
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(item);
                    Ok(PutOutcome::Created)
                }
            },
            _ => Err(MailStoreError::Permanent(anyhow::anyhow!(
                "put_if_absent: item has no pk/sk"
            ))),
        };
        std::future::ready(outcome)
    }

    fn transact(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<TxnOutcome, MailStoreError>> + Send {
        std::future::ready(self.attempt(ops))
    }

    /// Nothing to wait for: this store's "conflicts" are scripted, so a real
    /// backoff would only make the tests slower.
    fn pause(&self, _attempt: u32) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_messaging_webhook::mail::send::SendState;
    use aws_messaging_webhook::mail::store::{ListQuery, MarkOutcome};
    use aws_messaging_webhook::mail::{InsertOutcome, RfcHit};

    fn message(message_id: &str, thread_id: &str) -> MailMessage {
        sample_message("support@example.com", message_id, thread_id)
    }

    #[tokio::test]
    async fn ensure_inbox_creates_then_returns_the_same_inbox() {
        let store = MailMemoryStore::default();
        let inbox = InboxId::new("support@example.com");
        let created = Mailbox::new(&store)
            .ensure_inbox(&inbox, "2026-01-01T00:00:00.000Z")
            .await
            .unwrap();
        assert_eq!(created.inbox_id, inbox);
        assert_eq!(created.email, "support@example.com");

        let fetched = Mailbox::new(&store)
            .get_inbox(&inbox)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.created_at, created.created_at);

        let ensured_again = Mailbox::new(&store)
            .ensure_inbox(&inbox, "2026-01-02T00:00:00.000Z")
            .await
            .unwrap();
        assert_eq!(ensured_again.created_at, created.created_at);
    }

    #[tokio::test]
    async fn insert_message_is_fresh_then_a_redelivery_is_a_duplicate() {
        let store = MailMemoryStore::default();
        let msg = message("mid-1", "mid-1");

        let outcome = Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);
        assert!(
            Mailbox::new(&store)
                .message_exists(&msg.inbox_id, &MessageId::new("mid-1"))
                .await
                .unwrap()
        );

        let redelivered = Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap();
        assert_eq!(redelivered, InsertOutcome::Duplicate);
    }

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }

    fn alias_row(store: &MailMemoryStore, id: &str) -> Option<RfcHit> {
        let item = store.get_item(&keys::rfc_alias(&InboxId::new("support@example.com"), id))?;
        Some(RfcHit {
            message_id: MailMemoryStore::string_attr(&item, "message_id").map(MessageId::new),
            thread_id: ThreadId::new(MailMemoryStore::string_attr(&item, "thread_id")?),
        })
    }

    #[tokio::test]
    async fn insert_message_into_an_existing_thread_updates_it_and_resolves_by_rfc_id() {
        let store = MailMemoryStore::default();
        let first = message("mid-1", "mid-1");
        Mailbox::new(&store)
            .insert_message(&first, &ids(&["mid-1@example.com"]))
            .await
            .unwrap();

        let mut second = message("mid-2", "mid-2");
        second.in_reply_to = Some("mid-1@example.com".to_owned());
        let outcome = Mailbox::new(&store)
            .insert_message(&second, &ids(&["mid-2@example.com", "mid-1@example.com"]))
            .await
            .unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);

        let stored = Mailbox::new(&store)
            .get_message(&second.inbox_id, &MessageId::new("mid-2"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.thread_id, "mid-1");
    }

    #[tokio::test]
    async fn an_unresolved_reference_leaves_a_placeholder_in_the_new_thread() {
        let store = MailMemoryStore::default();
        let reply = message("mid-2", "mid-2");
        Mailbox::new(&store)
            .insert_message(&reply, &ids(&["mid-2@example.com", "parent@example.com"]))
            .await
            .unwrap();

        assert_eq!(
            alias_row(&store, "parent@example.com"),
            Some(RfcHit {
                message_id: None,
                thread_id: ThreadId::new("mid-2"),
            })
        );
        assert_eq!(
            alias_row(&store, "mid-2@example.com"),
            Some(RfcHit {
                message_id: Some(MessageId::new("mid-2")),
                thread_id: ThreadId::new("mid-2"),
            })
        );
    }

    /// The parent arriving after its reply joins the reply's thread through
    /// the placeholder, and takes the placeholder's row over.
    #[tokio::test]
    async fn a_late_parent_joins_its_replys_thread_and_claims_the_placeholder() {
        let store = MailMemoryStore::default();
        Mailbox::new(&store)
            .insert_message(
                &message("mid-2", "mid-2"),
                &ids(&["mid-2@example.com", "parent@example.com"]),
            )
            .await
            .unwrap();

        let mut parent = message("mid-1", "mid-1");
        parent.rfc_message_id = "<parent@example.com>".to_owned();
        Mailbox::new(&store)
            .insert_message(&parent, &ids(&["parent@example.com"]))
            .await
            .unwrap();

        let stored = Mailbox::new(&store)
            .get_message(&parent.inbox_id, &MessageId::new("mid-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.thread_id, "mid-2");
        assert_eq!(
            alias_row(&store, "parent@example.com"),
            Some(RfcHit {
                message_id: Some(MessageId::new("mid-1")),
                thread_id: ThreadId::new("mid-2"),
            })
        );
    }

    /// A second message carrying an id another message's alias already holds
    /// joins that thread but leaves the row alone.
    #[tokio::test]
    async fn a_real_alias_is_never_replaced() {
        let store = MailMemoryStore::default();
        Mailbox::new(&store)
            .insert_message(&message("mid-1", "mid-1"), &ids(&["mid-1@example.com"]))
            .await
            .unwrap();

        let mut copy = message("mid-2", "mid-2");
        copy.rfc_message_id = "<mid-1@example.com>".to_owned();
        Mailbox::new(&store)
            .insert_message(&copy, &ids(&["mid-1@example.com"]))
            .await
            .unwrap();

        assert_eq!(
            alias_row(&store, "mid-1@example.com"),
            Some(RfcHit {
                message_id: Some(MessageId::new("mid-1")),
                thread_id: ThreadId::new("mid-1"),
            })
        );
    }

    fn alias_op(id: &str, message_id: Option<&str>, thread_id: &str) -> PlannedOp {
        use aws_messaging_webhook::mail::plan::OpRole;
        PlannedOp {
            role: OpRole::RfcAlias,
            op: WriteOp::AliasFirstWriter {
                pk: keys::rfc_alias(&InboxId::new("support@example.com"), id).pk,
                sk: keys::RFC_ALIAS_SK.to_owned(),
                message_id: message_id.map(str::to_owned),
                thread_id: thread_id.to_owned(),
                expires_at: 0,
            },
        }
    }

    /// The alias conditions themselves: a placeholder lands only where no
    /// row exists, and a real alias also where only a placeholder does.
    #[test]
    fn alias_conditions_match_the_real_store() {
        let store = MailMemoryStore::default();
        let committed =
            |op: PlannedOp| matches!(store.attempt(&[op]).unwrap(), TxnOutcome::Committed);

        assert!(committed(alias_op("a@example.com", None, "t1")));
        assert!(
            !committed(alias_op("a@example.com", None, "t2")),
            "a placeholder never replaces a placeholder"
        );
        assert!(
            committed(alias_op("a@example.com", Some("m1"), "t1")),
            "a real alias takes over a placeholder"
        );
        assert!(
            !committed(alias_op("a@example.com", Some("m2"), "t1")),
            "a real alias never replaces a real alias"
        );
        assert!(
            !committed(alias_op("a@example.com", None, "t1")),
            "a placeholder never replaces a real alias"
        );
    }

    #[tokio::test]
    async fn insert_message_retries_on_an_injected_conflict_then_succeeds() {
        let store = MailMemoryStore::default();
        store.inject(Injected::Conflict);
        let msg = message("mid-1", "mid-1");
        let outcome = Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap();
        assert_eq!(outcome, InsertOutcome::Fresh);
    }

    #[tokio::test]
    async fn insert_message_surfaces_an_injected_transient_failure() {
        let store = MailMemoryStore::default();
        store.inject(Injected::Transient);
        let msg = message("mid-1", "mid-1");
        let err = Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap_err();
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
        Mailbox::new(&store)
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
        Mailbox::new(&store)
            .claim_send(&MessageId::new("mid-1"), "2026-01-01T00:00:01.000Z")
            .await
            .unwrap()
            .unwrap();

        let attempts_before = store.txn_attempts();
        let error = Mailbox::new(&store)
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
        Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap();
        let before = Mailbox::new(&store)
            .get_message(&inbox, &MessageId::new("mid-1"))
            .await
            .unwrap()
            .unwrap();

        let labels = Mailbox::new(&store)
            .update_labels(
                &inbox,
                &MessageId::new("mid-1"),
                &["unread".to_owned()],
                &["spam".to_owned()],
                "2026-01-01T00:00:05.000Z",
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(labels, before.labels);
        let after = Mailbox::new(&store)
            .get_message(&inbox, &MessageId::new("mid-1"))
            .await
            .unwrap()
            .unwrap();
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
        let inbox = InboxId::new("support@example.com");
        for id in ["mid-1", "mid-2"] {
            Mailbox::new(&store)
                .insert_message(&message(id, id), &[])
                .await
                .unwrap();
        }

        let query = |start: Option<PageKey>| ListQuery {
            inbox: inbox.clone(),
            limit: 2,
            before: None,
            after: None,
            ascending: true,
            start,
        };

        let page = Mailbox::new(&store)
            .list_messages(&query(None))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        let next = page.next.expect("a full page carries a continuation key");

        let page = Mailbox::new(&store)
            .list_messages(&query(Some(next)))
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn insert_message_gives_up_after_max_retries_on_persistent_throttling() {
        let store = MailMemoryStore::default();
        for _ in 0..=aws_messaging_webhook::mail::mailbox::MAX_TXN_RETRIES {
            store.inject(Injected::Throttle);
        }
        let msg = message("mid-1", "mid-1");
        let err = Mailbox::new(&store)
            .insert_message(&msg, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, MailStoreError::Conflict));
    }

    #[tokio::test]
    async fn a_batch_read_omits_unknown_keys() {
        let store = MailMemoryStore::default();
        let inbox = InboxId::new("support@example.com");
        let found: HashMap<TableKey, RfcHit> = store
            .batch_get(
                vec![keys::rfc_alias(&inbox, "nope@example.com")],
                Read::Consistent,
                "test",
            )
            .await
            .unwrap();
        assert!(found.is_empty());
    }

    /// `seed_send_key` writes a `SendKey` row exactly where `get_send_key`
    /// reads it, so a test can place a key with an arbitrary `expires_at` and
    /// exercise the read-side expiry logic. Like DynamoDB, the in-memory
    /// store returns an expired row verbatim — the expiry check is the
    /// caller's (`replay_key`), not the store's.
    #[tokio::test]
    async fn seed_send_key_round_trips_through_get_send_key_without_filtering_expiry() {
        let store = MailMemoryStore::default();
        let key = SendKey {
            key_hash: "deadbeef".to_owned(),
            inbox_id: InboxId::new("support@example.com"),
            message_id: MessageId::new("mid-1"),
            thread_id: ThreadId::new("tid-1"),
            request_hash: "abc".to_owned(),
            route: "send".to_owned(),
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            expires_at: 1, // expired in the past
        };
        store.seed_send_key(&key);

        let fetched = Mailbox::new(&store)
            .get_send_key("deadbeef")
            .await
            .unwrap()
            .expect("seed_send_key writes the row get_send_key reads");
        assert_eq!(
            fetched, key,
            "the store returns an expired row verbatim, like DynamoDB"
        );
    }
}
