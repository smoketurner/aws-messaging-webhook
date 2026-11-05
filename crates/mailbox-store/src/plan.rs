//! The transactional write model: every mail-table transaction is planned as
//! a `Vec<PlannedOp>` up front, then executed and its cancellation decoded
//! uniformly by `txn::decode_cancellation`.
//!
//! `plan_insert` plans the ingest transaction: the message, its versioned
//! thread, its `Message-ID` alias and placeholder aliases for the ids it
//! references that no row maps yet. It is pure — every input (the message,
//! the thread state before and after, the alias rows) is computed by the
//! caller from consistent reads; this module only turns that data into
//! `WriteOp`s.

use serde_dynamo::AttributeValue;

use crate::keys::{self, TableKey};
use crate::store::MailStoreError;
use mailbox::send::{SendKey, SendState, SendStatus, SesSent};
use mailbox::thread::{AliasLookup, ThreadState};
use mailbox::{
    Direction, Inbox, InboxId, MESSAGE_USER_LABEL_CAP, MailMessage, MessageId,
    THREAD_USER_LABEL_CAP, ThreadSnapshot, ids, labels, strip_angle_brackets,
};

/// Which role a planned op plays in its transaction — used by
/// [`crate::txn::decode_cancellation`] to interpret a cancellation
/// reason without re-deriving it from the op's shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpRole {
    IdempotencyKey,
    Message,
    SendState,
    Thread,
    RfcAlias,
    SesRef,
}

/// One equality check on an existing item's attribute, rendered into a
/// `ConditionExpression` clause by the op's executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    Eq(&'static str, AttributeValue),
}

/// A write's condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cond {
    None,
    NotExists,
    VersionEquals(u64),
    NotExistsOrExpired {
        now_epoch: u64,
    },
    /// Conjunction of [`Check`]s, rendered to one `ConditionExpression`.
    All(Vec<Check>),
}

/// One write in a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put {
        item: serde_dynamo::Item,
        cond: Cond,
    },
    Update {
        pk: String,
        sk: String,
        set: Vec<(String, AttributeValue)>,
        remove: Vec<String>,
        cond: Cond,
    },
    /// The RFC-alias first-writer-wins put: unconditioned by version.
    ///
    /// With a `message_id` it is a real alias, which may land where no row
    /// exists or where only a placeholder does: the message a placeholder
    /// stood in for takes its row over. Without one it is a placeholder for
    /// a referenced id this inbox holds no message for, which lands only
    /// where no row exists.
    AliasFirstWriter {
        pk: String,
        sk: String,
        message_id: Option<String>,
        thread_id: String,
        /// The aliased message's TTL: an alias outliving its message would
        /// thread a later reply into a thread that no longer exists.
        expires_at: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedOp {
    pub role: OpRole,
    pub op: WriteOp,
}

/// An item type the mail table stores whole: where it lives and the index
/// keys it carries. The struct itself holds no key attributes, so they are
/// added beside its serialized fields.
trait Keyed: serde::Serialize {
    fn key(&self) -> TableKey;

    fn index_keys(&self) -> Vec<(&'static str, AttributeValue)> {
        Vec::new()
    }
}

/// A stored item a transaction writes, and the role its write plays.
trait StoredItem: Keyed {
    const ROLE: OpRole;
}

/// The item for `value`: its serialized fields, then its table and index
/// keys.
fn to_item<I: Keyed>(value: &I) -> Result<serde_dynamo::Item, MailStoreError> {
    let mut item: serde_dynamo::Item = serde_dynamo::to_item(value)
        .map_err(|e| MailStoreError::Permanent(anyhow::anyhow!("serializing item: {e}")))?;
    let TableKey { pk, sk } = value.key();
    item.inner_mut()
        .insert("pk".to_owned(), AttributeValue::S(pk));
    item.inner_mut()
        .insert("sk".to_owned(), AttributeValue::S(sk));
    for (name, value) in value.index_keys() {
        item.inner_mut().insert(name.to_owned(), value);
    }
    Ok(item)
}

/// The single `ByTime` partition every inbox shares, so `GET /v0/inboxes` is
/// one query (there are few enough inboxes for that to stay cheap).
impl Keyed for Inbox {
    fn key(&self) -> TableKey {
        keys::inbox(&self.inbox_id)
    }

    fn index_keys(&self) -> Vec<(&'static str, AttributeValue)> {
        vec![
            (
                "gsi1pk",
                AttributeValue::S(keys::INBOXES_PARTITION.to_owned()),
            ),
            (
                "gsi1sk",
                AttributeValue::S(self.inbox_id.as_str().to_owned()),
            ),
        ]
    }
}

impl StoredItem for MailMessage {
    const ROLE: OpRole = OpRole::Message;
}

impl Keyed for MailMessage {
    fn key(&self) -> TableKey {
        keys::message(&self.inbox_id, &self.message_id)
    }

    fn index_keys(&self) -> Vec<(&'static str, AttributeValue)> {
        vec![
            (
                "gsi1pk",
                AttributeValue::S(keys::messages_partition(&self.inbox_id)),
            ),
            ("gsi1sk", AttributeValue::S(self.message_id.to_string())),
            (
                "gsi2pk",
                AttributeValue::S(keys::thread_messages_partition(
                    &self.inbox_id,
                    &self.thread_id,
                )),
            ),
            ("gsi2sk", AttributeValue::S(self.message_id.to_string())),
        ]
    }
}

impl StoredItem for ThreadState {
    const ROLE: OpRole = OpRole::Thread;
}

impl Keyed for ThreadState {
    fn key(&self) -> TableKey {
        keys::thread(&self.inbox_id, &self.thread_id)
    }

    fn index_keys(&self) -> Vec<(&'static str, AttributeValue)> {
        vec![
            (
                "gsi1pk",
                AttributeValue::S(keys::threads_partition(&self.inbox_id)),
            ),
            (
                "gsi1sk",
                AttributeValue::S(keys::thread_time_sort(&self.timestamp, &self.thread_id)),
            ),
        ]
    }
}

/// A settled send carries no `ByStatus` keys. The index exists for the
/// sweep, which only ever queries `sending` and `unknown`; writing the keys
/// for `sent` and `failed` too would pay an index write on the one
/// transaction every send is guaranteed to make and then keep the whole
/// item (the index projects ALL) in the index until its TTL. Since these
/// are `Put`s, dropping the attributes on the settling write is what
/// removes the entry.
impl StoredItem for SendState {
    const ROLE: OpRole = OpRole::SendState;
}

impl Keyed for SendState {
    fn key(&self) -> TableKey {
        keys::send_state(&self.message_id)
    }

    fn index_keys(&self) -> Vec<(&'static str, AttributeValue)> {
        if !self.send_status.is_swept() {
            return Vec::new();
        }
        vec![
            (
                "gsi3pk",
                AttributeValue::S(keys::send_status_partition(self.send_status)),
            ),
            ("gsi3sk", AttributeValue::S(self.message_id.to_string())),
        ]
    }
}

impl StoredItem for SendKey {
    const ROLE: OpRole = OpRole::IdempotencyKey;
}

impl Keyed for SendKey {
    fn key(&self) -> TableKey {
        keys::send_key(&self.key_hash)
    }
}

/// The item mapping an SES message id back to the message sent under it.
#[derive(serde::Serialize)]
struct SesRefItem<'a> {
    #[serde(skip)]
    ses_message_id: &'a str,
    inbox_id: &'a InboxId,
    message_id: &'a MessageId,
    expires_at: u64,
}

impl StoredItem for SesRefItem<'_> {
    const ROLE: OpRole = OpRole::SesRef;
}

impl Keyed for SesRefItem<'_> {
    fn key(&self) -> TableKey {
        keys::ses_ref(self.ses_message_id)
    }
}

/// Aliases skip ids over this many bytes, keeping the alias pk under
/// DynamoDB's key-length limit with headroom for the `RFC#<inbox>#` prefix.
pub const ALIAS_ID_MAX_BYTES: usize = 900;

/// One transaction's writes, in the order they are added.
#[derive(Default)]
struct Txn {
    ops: Vec<PlannedOp>,
}

impl Txn {
    /// Writes `item` under `cond`.
    fn put<I: StoredItem>(&mut self, item: &I, cond: Cond) -> Result<(), MailStoreError> {
        self.ops.push(PlannedOp {
            role: I::ROLE,
            op: WriteOp::Put {
                item: to_item(item)?,
                cond,
            },
        });
        Ok(())
    }

    /// Writes `item` only where nothing exists yet.
    fn put_new<I: StoredItem>(&mut self, item: &I) -> Result<(), MailStoreError> {
        self.put(item, Cond::NotExists)
    }

    /// Writes `item` over the version that was read, or only where nothing
    /// exists when nothing was read.
    fn put_versioned<I: StoredItem>(
        &mut self,
        item: &I,
        before: Option<u64>,
    ) -> Result<(), MailStoreError> {
        self.put(item, before.map_or(Cond::NotExists, Cond::VersionEquals))
    }

    /// Maps `rfc_id` to `msg`'s thread, skipping ids over
    /// [`ALIAS_ID_MAX_BYTES`]. `message_id` is `msg`'s own id for an alias,
    /// or `None` for a placeholder: an id `msg` references but this inbox
    /// holds no message for, which expires with `msg`, the message whose
    /// reference it records.
    fn put_alias(&mut self, msg: &MailMessage, rfc_id: &str, message_id: Option<&MessageId>) {
        let stripped = strip_angle_brackets(rfc_id);
        if stripped.len() > ALIAS_ID_MAX_BYTES {
            return;
        }
        let TableKey { pk, sk } = keys::rfc_alias(&msg.inbox_id, stripped);
        self.ops.push(PlannedOp {
            role: OpRole::RfcAlias,
            op: WriteOp::AliasFirstWriter {
                pk,
                sk,
                message_id: message_id.map(ToString::to_string),
                thread_id: msg.thread_id.to_string(),
                expires_at: msg.expires_at,
            },
        });
    }

    /// Updates attributes of an existing item in place.
    fn update(
        &mut self,
        role: OpRole,
        TableKey { pk, sk }: TableKey,
        set: Vec<(String, AttributeValue)>,
        remove: Vec<String>,
        cond: Cond,
    ) {
        self.ops.push(PlannedOp {
            role,
            op: WriteOp::Update {
                pk,
                sk,
                set,
                remove,
                cond,
            },
        });
    }

    /// A message's new labels under the version that was read: a new
    /// version, `updated_at`, and the labels — removed rather than written
    /// empty, since DynamoDB has no empty string set — plus `extra` sets.
    fn update_message_labels(
        &mut self,
        msg: &MailMessage,
        new_labels: &[String],
        now: &str,
        extra: Vec<(String, AttributeValue)>,
    ) {
        let mut set = vec![
            (
                "version".to_owned(),
                AttributeValue::N((msg.version + 1).to_string()),
            ),
            ("updated_at".to_owned(), AttributeValue::S(now.to_owned())),
        ];
        let mut remove = Vec::new();
        if new_labels.is_empty() {
            remove.push("labels".to_owned());
        } else {
            set.push(("labels".to_owned(), AttributeValue::Ss(new_labels.to_vec())));
        }
        set.extend(extra);
        self.update(
            OpRole::Message,
            msg.key(),
            set,
            remove,
            Cond::VersionEquals(msg.version),
        );
    }

    fn finish(self) -> Vec<PlannedOp> {
        self.ops
    }
}

/// The inbox record and the item both stores write for it.
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn inbox_item(
    inbox: &InboxId,
    now: &str,
) -> Result<(Inbox, serde_dynamo::Item), MailStoreError> {
    let record = Inbox {
        inbox_id: inbox.clone(),
        email: inbox.as_str().to_owned(),
        display_name: None,
        metadata: None,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let item = to_item(&record)?;
    Ok((record, item))
}

/// The message as stored: for an inbound message the `thread_snapshot` is
/// populated from `thread_after` — the caller-computed thread state that
/// already includes this message — on a clone of `msg`.
fn stored_message(msg: &MailMessage, thread_after: &ThreadState) -> MailMessage {
    let mut stored = msg.clone();
    if matches!(msg.direction, Direction::Inbound) {
        stored.thread_snapshot = Some(ThreadSnapshot::from(thread_after));
    }
    stored
}

/// Plans the ingest transaction: the message, its versioned thread, its
/// `Message-ID` alias, and a placeholder alias for every candidate id in
/// `aliases` that no row maps yet.
///
/// `thread_before` is the thread's state from a consistent read, or `None`
/// for a brand-new thread; `thread_after` is the caller-computed result of
/// applying `msg` to it ([`mailbox::thread::new_thread`] /
/// [`mailbox::thread::apply_message`]).
///
/// The message's own alias is written only where `aliases` found no row or
/// a placeholder, which it takes over; a row another message already holds
/// stays that message's. A placeholder points at `msg`'s thread, so a later
/// message that references the same id, or turns out to be it, lands there.
///
/// Not capped by labels: an inbound message carries only system labels, a
/// fixed set, so it can never push a thread over the user-label cap — and
/// refusing it would lose the mail.
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn plan_insert(
    msg: &MailMessage,
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
    aliases: &AliasLookup,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    debug_assert!(
        is_sorted_and_deduped(&msg.labels),
        "message labels must be sorted and deduplicated before planning"
    );
    debug_assert!(
        is_sorted_and_deduped(&thread_after.labels),
        "thread labels must be sorted and deduplicated before planning"
    );

    let mut txn = Txn::default();
    txn.put_new(&stored_message(msg, thread_after))?;
    txn.put_versioned(thread_after, thread_before.map(|before| before.version))?;

    let own_id = strip_angle_brackets(&msg.rfc_message_id);
    if !own_id.is_empty() && aliases.is_claimable(own_id) {
        txn.put_alias(msg, &msg.rfc_message_id, Some(&msg.message_id));
    }
    for candidate in &aliases.candidates {
        if candidate != own_id && !aliases.found.contains_key(candidate) {
            txn.put_alias(msg, candidate, None);
        }
    }
    Ok(txn.finish())
}

/// Plans the enqueue transaction: the idempotency key (when one was given),
/// the queued message, its send state, its thread, and the `Message-ID`
/// alias.
///
/// Everything lands in one transaction so a queued message can never exist
/// without the state item the sender claims, and an idempotency key can never
/// be recorded for a send that was not committed.
///
/// # Errors
///
/// [`MailStoreError::LabelLimit`] when the message or its thread would carry
/// more user labels than their caps.
pub fn plan_enqueue(
    msg: &MailMessage,
    state: &SendState,
    key: Option<&SendKey>,
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
    now_epoch: u64,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    check_user_labels(&msg.labels, thread_after)?;

    let mut txn = Txn::default();
    // First, so a cancellation names the key conflict before anything else.
    if let Some(key) = key {
        // An expired key is reusable; a live one is a replay, which the
        // caller resolves by reading it back.
        txn.put(key, Cond::NotExistsOrExpired { now_epoch })?;
    }
    txn.put_new(&stored_message(msg, thread_after))?;
    txn.put_new(state)?;
    txn.put_versioned(thread_after, thread_before.map(|before| before.version))?;
    txn.put_alias(msg, &msg.rfc_message_id, Some(&msg.message_id));
    Ok(txn.finish())
}

/// Plans a send-state transition conditioned on the item still being in
/// `expected` status at the version `before` holds.
///
/// The condition is on the status, not just the version. A claim
/// (`expected` = `queued`) must not take a record that is already `sending`,
/// however old its version looks, because that is exactly how the same
/// message gets sent twice; recording the SES call (`expected` = `sending`)
/// must never mark a claim that was lost.
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn plan_state_cas(
    before: &SendState,
    after: &SendState,
    expected: SendStatus,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    let mut txn = Txn::default();
    txn.put(
        after,
        Cond::All(vec![
            Check::Eq(
                "send_status",
                AttributeValue::S(expected.as_str().to_owned()),
            ),
            Check::Eq("version", AttributeValue::N(before.version.to_string())),
        ]),
    )?;
    Ok(txn.finish())
}

/// Plans the end of a send: the new send state, the message's mirrored
/// status and labels, and — when SES gave us one — the alias that maps its
/// message id back to ours.
///
/// Both items move together so a reader can never see a message still
/// labelled `queued` whose send is finished, and the message is written once
/// per outcome rather than once per attempt, which is what keeps the relay
/// from publishing an event for every retry.
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn plan_mark(
    state_before: &SendState,
    state_after: &SendState,
    msg: &MailMessage,
    new_labels: &[String],
    thread: Option<(&ThreadState, &ThreadState)>,
    ses: Option<SesSent<'_>>,
    now: &str,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    debug_assert!(
        is_sorted_and_deduped(new_labels),
        "message labels must be sorted and deduplicated before planning"
    );

    let mut txn = Txn::default();
    txn.put_versioned(state_after, Some(state_before.version))?;

    let mut extra = vec![(
        "send_status".to_owned(),
        AttributeValue::S(state_after.send_status.as_str().to_owned()),
    )];
    if let Some(ses) = ses {
        extra.push((
            "ses_message_id".to_owned(),
            AttributeValue::S(ses.message_id.to_owned()),
        ));
        extra.push(("sent_at".to_owned(), AttributeValue::S(now.to_owned())));
    }
    txn.update_message_labels(msg, new_labels, now, extra);

    // The thread's labels roll up its messages', so a status label that moves
    // on the message moves on the thread in the same transaction.
    if let Some((thread_before, thread_after)) = thread {
        txn.put_versioned(thread_after, Some(thread_before.version))?;
    }

    if let Some(ses) = ses {
        // SES writes its own `Message-ID` over ours, so replies to this
        // message, and its copy if it was sent to this inbox, name the SES
        // forms. A redrive finds these already taken, which the caller
        // resolves by dropping them.
        for rfc_id in ids::ses_rfc_ids(ses.message_id, ses.region) {
            txn.put_alias(msg, &rfc_id, Some(&msg.message_id));
        }

        // Unconditioned: a redrive that marks the same send again should
        // rewrite this rather than cancel the whole transaction.
        txn.put(
            &SesRefItem {
                ses_message_id: ses.message_id,
                inbox_id: &msg.inbox_id,
                message_id: &msg.message_id,
                expires_at: msg.expires_at,
            },
            Cond::None,
        )?;
    }

    Ok(txn.finish())
}

/// Plans a label change: the message's new label set and its thread's
/// recomputed union, both version-conditioned on what the caller read.
///
/// The message is an `Update` rather than a `Put` so a concurrent write to
/// any other attribute survives; only `labels`, `version` and `updated_at`
/// are touched. An empty label set removes the attribute, because DynamoDB
/// has no empty string set.
///
/// # Errors
///
/// [`MailStoreError::LabelLimit`] when the message or its thread would carry
/// more user labels than their caps.
pub fn plan_patch(
    msg: &MailMessage,
    new_labels: &[String],
    thread_before: &ThreadState,
    thread_after: &ThreadState,
    now: &str,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    check_user_labels(new_labels, thread_after)?;
    debug_assert!(
        is_sorted_and_deduped(new_labels),
        "message labels must be sorted and deduplicated before planning"
    );

    let mut txn = Txn::default();
    txn.update_message_labels(msg, new_labels, now, Vec::new());
    txn.put_versioned(thread_after, Some(thread_before.version))?;
    Ok(txn.finish())
}

/// Rejects a write that would leave the message or its thread with more
/// user labels than [`MESSAGE_USER_LABEL_CAP`] or [`THREAD_USER_LABEL_CAP`].
///
/// Only a caller's own labels are counted. System labels are a fixed set the
/// pipeline applies, so counting them would let a thread full of user labels
/// refuse the system labels an inbound message or a send outcome needs.
fn check_user_labels(
    message_labels: &[String],
    thread_after: &ThreadState,
) -> Result<(), MailStoreError> {
    let on_message = labels::user_label_count(message_labels);
    if on_message > MESSAGE_USER_LABEL_CAP {
        return Err(MailStoreError::LabelLimit(format!(
            "a message can carry at most {MESSAGE_USER_LABEL_CAP} labels of your own; \
             this would leave it with {on_message}"
        )));
    }
    let on_thread = labels::user_label_count(&thread_after.labels);
    if on_thread > THREAD_USER_LABEL_CAP {
        return Err(MailStoreError::LabelLimit(format!(
            "a thread can carry at most {THREAD_USER_LABEL_CAP} labels of your own across its \
             messages; this would leave it with {on_thread}"
        )));
    }
    Ok(())
}

fn is_sorted_and_deduped(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use mailbox::ThreadId;
    use mailbox::thread::{apply_message, new_thread};
    use mailbox::{Direction, InboxId, test_fixtures};

    fn message(inbox: &str, thread_id: &str, message_id: &str, labels: &[&str]) -> MailMessage {
        MailMessage {
            inbox_id: InboxId::new(inbox),
            thread_id: ThreadId::new(thread_id),
            message_id: MessageId::new(message_id),
            ses_message_id: None,
            rfc_message_id: format!("<{message_id}@example.com>"),
            labels: labels.iter().map(|l| (*l).to_owned()).collect(),
            timestamp: "00000001-0000".to_owned(),
            size: 1_000,
            version: 0,
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
            ..test_fixtures::message()
        }
    }

    #[test]
    fn plan_insert_for_a_new_thread_writes_the_message_thread_and_alias() {
        let msg = message(
            "support@example.com",
            "tid-1",
            "mid-1",
            &["received", "unread"],
        );
        let thread = new_thread(&msg);
        let ops = plan_insert(&msg, None, &thread, &AliasLookup::default()).unwrap();

        let roles: Vec<OpRole> = ops.iter().map(|o| o.role).collect();
        assert_eq!(
            roles,
            vec![OpRole::Message, OpRole::Thread, OpRole::RfcAlias]
        );

        // A thread nobody has seen before is created, not updated.
        let WriteOp::Put { cond, .. } = &ops.iter().find(|o| o.role == OpRole::Thread).unwrap().op
        else {
            panic!("expected a Put");
        };
        assert_eq!(*cond, Cond::NotExists);
    }

    /// An alias or SES reference outliving its message would point a later
    /// reply or delivery event at a message that no longer exists.
    #[test]
    fn lookup_items_expire_with_their_message() {
        let mut msg = message(
            "support@example.com",
            "tid-1",
            "mid-1",
            &["received", "unread"],
        );
        msg.expires_at = 1_900_000_000;
        let thread = new_thread(&msg);
        assert_eq!(thread.expires_at, msg.expires_at);

        let ops = plan_insert(&msg, None, &thread, &AliasLookup::default()).unwrap();
        let WriteOp::AliasFirstWriter { expires_at, .. } =
            &ops.iter().find(|o| o.role == OpRole::RfcAlias).unwrap().op
        else {
            panic!("expected an alias write");
        };
        assert_eq!(*expires_at, msg.expires_at);

        let mut outbound = msg.clone();
        outbound.direction = Direction::Outbound;
        outbound.labels = vec!["queued".to_owned()];
        let claimed = queued_state(&outbound, None).claimed("2026-01-01T00:00:01.000Z");
        let sent = claimed.sent("2026-01-01T00:00:02.000Z");
        let ops = plan_mark(
            &claimed,
            &sent,
            &outbound,
            &["sent".to_owned()],
            None,
            Some(SesSent {
                message_id: "ses-1",
                region: "eu-west-1",
            }),
            "2026-01-01T00:00:02.000Z",
        )
        .unwrap();

        // Both forms SES may write over our `Message-ID` resolve to the sent
        // message, and expire with it.
        let mut aliases: Vec<(&str, u64)> = ops
            .iter()
            .filter_map(|o| match &o.op {
                WriteOp::AliasFirstWriter {
                    pk,
                    message_id,
                    expires_at,
                    ..
                } if message_id.as_deref() == Some(outbound.message_id.as_str()) => {
                    Some((pk.as_str(), *expires_at))
                }
                _ => None,
            })
            .collect();
        aliases.sort_unstable();
        assert_eq!(
            aliases,
            vec![
                (
                    "RFC#support@example.com#ses-1@email.amazonses.com",
                    msg.expires_at
                ),
                (
                    "RFC#support@example.com#ses-1@eu-west-1.amazonses.com",
                    msg.expires_at
                ),
            ]
        );
        let WriteOp::Put { item, .. } = &ops.iter().find(|o| o.role == OpRole::SesRef).unwrap().op
        else {
            panic!("expected an SES reference put");
        };
        assert_eq!(
            item.inner().get("expires_at"),
            Some(&AttributeValue::N(msg.expires_at.to_string()))
        );
        assert_eq!(
            key_attributes(item),
            pairs(&[("pk", "SESMSG#ses-1"), ("sk", "REF")])
        );
    }

    #[test]
    fn plan_insert_for_an_existing_thread_conditions_on_the_version_it_read() {
        let first = message(
            "support@example.com",
            "tid-1",
            "mid-1",
            &["received", "unread"],
        );
        let before = new_thread(&first);
        let mut second = message(
            "support@example.com",
            "tid-1",
            "mid-2",
            &["received", "unread"],
        );
        second.timestamp = "00000002-0000".to_owned();
        let after = apply_message(&before, &second);

        let ops = plan_insert(&second, Some(&before), &after, &AliasLookup::default()).unwrap();
        let WriteOp::Put { cond, .. } = &ops.iter().find(|o| o.role == OpRole::Thread).unwrap().op
        else {
            panic!("expected a Put");
        };
        assert_eq!(*cond, Cond::VersionEquals(0));
    }

    fn queued_state(msg: &MailMessage, key_pk: Option<&str>) -> SendState {
        SendState::queued(
            msg.inbox_id.clone(),
            msg.message_id.clone(),
            msg.thread_id.clone(),
            mailbox::send::Envelope {
                to: vec!["recipient@example.com".to_owned()],
                ..mailbox::send::Envelope::default()
            },
            key_pk.map(ToOwned::to_owned),
            "2026-01-01T00:00:00.000Z",
        )
    }

    /// The `ByStatus` index serves the sweep alone. A settled send is
    /// nothing for the sweep to find, so its write drops the index keys
    /// rather than keeping the whole item indexed until its TTL.
    #[test]
    fn only_a_sweepable_send_state_carries_the_status_index_keys() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let queued = queued_state(&msg, None);
        let now = "2026-01-01T00:00:10.000Z";

        for (state, indexed) in [
            (queued.clone(), true),
            (queued.claimed(now), true),
            (queued.unknown(now), true),
            (queued.sent(now), false),
            (
                queued.failed(mailbox::send::SendFailure::Rejected, now),
                false,
            ),
        ] {
            let item = to_item(&state).unwrap();
            assert_eq!(
                item.inner().contains_key("gsi3pk"),
                indexed,
                "{}",
                state.send_status.as_str()
            );
            assert_eq!(
                item.inner().contains_key("gsi3sk"),
                indexed,
                "{}",
                state.send_status.as_str()
            );
        }
    }

    #[test]
    fn plan_enqueue_puts_the_idempotency_key_first_and_lets_an_expired_one_go() {
        // The key op leads so a cancellation names the replay before any
        // other conflict, and an expired key must be reusable.
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);
        let state = queued_state(&msg, Some("SENDKEY#abc"));
        let key = mailbox::send::SendKey {
            key_hash: "keyhash".to_owned(),
            inbox_id: msg.inbox_id.clone(),
            message_id: msg.message_id.clone(),
            thread_id: msg.thread_id.clone(),
            request_hash: "abc".to_owned(),
            route: "send".to_owned(),
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            expires_at: 1_800_000_000,
        };

        let ops = plan_enqueue(&msg, &state, Some(&key), None, &thread, 1_700_000_000).unwrap();

        assert_eq!(ops[0].role, OpRole::IdempotencyKey);
        let WriteOp::Put { item, cond } = &ops[0].op else {
            panic!("expected a Put");
        };
        // Keyed by the hash of the header, not of the request body: two
        // different keys must never land on the same item.
        assert_eq!(
            item.inner().get("pk"),
            Some(&AttributeValue::S("SENDKEY#keyhash".to_owned()))
        );
        assert_eq!(
            *cond,
            Cond::NotExistsOrExpired {
                now_epoch: 1_700_000_000
            }
        );
    }

    #[test]
    fn plan_enqueue_indexes_the_send_state_by_its_status() {
        // The sweep finds work through the status index, so a queued send has
        // to be in the queued partition.
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);
        let state = queued_state(&msg, None);

        let ops = plan_enqueue(&msg, &state, None, None, &thread, 1_800_000_000).unwrap();
        let WriteOp::Put { item, .. } =
            &ops.iter().find(|o| o.role == OpRole::SendState).unwrap().op
        else {
            panic!("expected a Put");
        };

        assert_eq!(
            item.inner().get("gsi3pk"),
            Some(&AttributeValue::S("SENDSTATUS#queued".to_owned()))
        );
        assert_eq!(
            item.inner().get("pk"),
            Some(&AttributeValue::S("OUTBOX#mid-1".to_owned()))
        );
        // The envelope rides on the state item, not the message.
        let stored: SendState = serde_dynamo::from_item(item.clone()).unwrap();
        assert_eq!(stored.envelope.to, vec!["recipient@example.com"]);
    }

    /// The key and index attributes of a planned item, as stored.
    fn key_attributes(item: &serde_dynamo::Item) -> Vec<(String, String)> {
        let mut found = Vec::new();
        for (name, value) in item.inner() {
            if (name == "pk" || name == "sk" || name.starts_with("gsi"))
                && let AttributeValue::S(value) = value
            {
                found.push((name.clone(), value.clone()));
            }
        }
        found.sort();
        found
    }

    fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = expected
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        pairs.sort();
        pairs
    }

    /// Every item's key and index attributes are a storage format: these
    /// are the exact names and values the table already holds.
    #[test]
    fn planned_items_carry_the_stored_key_attributes() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);
        let state = queued_state(&msg, None);
        let key = mailbox::send::SendKey {
            key_hash: "keyhash".to_owned(),
            inbox_id: msg.inbox_id.clone(),
            message_id: msg.message_id.clone(),
            thread_id: msg.thread_id.clone(),
            request_hash: "abc".to_owned(),
            route: "send".to_owned(),
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            expires_at: 1_800_000_000,
        };
        let ops = plan_enqueue(&msg, &state, Some(&key), None, &thread, 1_700_000_000).unwrap();

        let thread_sort = format!("{}#tid-1", thread.timestamp);
        let expected = [
            (
                OpRole::IdempotencyKey,
                pairs(&[("pk", "SENDKEY#keyhash"), ("sk", "KEY")]),
            ),
            (
                OpRole::Message,
                pairs(&[
                    ("pk", "INBOX#support@example.com"),
                    ("sk", "MSG#mid-1"),
                    ("gsi1pk", "INBOX#support@example.com#MSG"),
                    ("gsi1sk", "mid-1"),
                    ("gsi2pk", "THREAD#support@example.com#tid-1"),
                    ("gsi2sk", "mid-1"),
                ]),
            ),
            (
                OpRole::SendState,
                pairs(&[
                    ("pk", "OUTBOX#mid-1"),
                    ("sk", "STATE"),
                    ("gsi3pk", "SENDSTATUS#queued"),
                    ("gsi3sk", "mid-1"),
                ]),
            ),
            (
                OpRole::Thread,
                pairs(&[
                    ("pk", "INBOX#support@example.com"),
                    ("sk", "THR#tid-1"),
                    ("gsi1pk", "INBOX#support@example.com#THR"),
                    ("gsi1sk", &thread_sort),
                ]),
            ),
        ];
        for (role, attributes) in expected {
            let WriteOp::Put { item, .. } = &ops.iter().find(|o| o.role == role).unwrap().op else {
                panic!("expected a Put for {role:?}");
            };
            assert_eq!(key_attributes(item), attributes, "{role:?}");
        }
        let WriteOp::AliasFirstWriter { pk, sk, .. } =
            &ops.iter().find(|o| o.role == OpRole::RfcAlias).unwrap().op
        else {
            panic!("expected an alias");
        };
        assert_eq!(pk, "RFC#support@example.com#mid-1@example.com");
        assert_eq!(sk, "RFC");

        let (_, inbox) = inbox_item(
            &InboxId::new("support@example.com"),
            "2026-01-01T00:00:00.000Z",
        )
        .unwrap();
        assert_eq!(
            key_attributes(&inbox),
            pairs(&[
                ("pk", "INBOX#support@example.com"),
                ("sk", "META"),
                ("gsi1pk", "INBOXES"),
                ("gsi1sk", "support@example.com"),
            ])
        );
    }

    /// Truncating `rfc_message_id` to `REFERENCES_MAX_BYTES` only preserves
    /// the alias-join and thread-resolution identities because every id the cap
    /// touches (stripped length > `REFERENCES_MAX_BYTES - 2`) is already larger
    /// than `ALIAS_ID_MAX_BYTES`, so it carries no alias row both before and
    /// after the fix. This ordering — `REFERENCES_MAX_BYTES - 2 > ALIAS_ID_MAX_BYTES`
    /// — is load-bearing: a maintainer who pushes `REFERENCES_MAX_BYTES` down to
    /// `ALIAS_ID_MAX_BYTES + 2` or below would break the alias-join identity.
    /// Gate it with a test rather than a comment.
    #[test]
    fn references_cap_stays_above_the_alias_cap() {
        assert!(
            mailbox::REFERENCES_MAX_BYTES.saturating_sub(2) > ALIAS_ID_MAX_BYTES,
            "REFERENCES_MAX_BYTES - 2 must exceed ALIAS_ID_MAX_BYTES so truncating a \
             Message-ID never changes which ids carry an alias row"
        );
    }

    proptest! {
        /// How many labels a message or its thread carries does not change
        /// how many items the ingest transaction writes — the labels live in
        /// the message and thread items themselves, not in per-label rows.
        #[test]
        fn plan_insert_writes_a_fixed_number_of_items_whatever_the_labels(
            message_label_count in 0usize..=4,
            thread_label_count in 0usize..=THREAD_USER_LABEL_CAP,
        ) {
            let message_labels: Vec<String> = (0..message_label_count)
                .map(|i| format!("m{i:02}"))
                .collect();
            let mut msg = message("support@example.com", "tid-1", "mid-1", &[]);
            msg.labels = message_labels;

            let mut before = new_thread(&msg);
            before.labels = (0..thread_label_count).map(|i| format!("t{i:03}")).collect();
            before.version = 5;

            let mut after = before.clone();
            after.timestamp = "00000002-0000".to_owned();
            after.version += 1;

            let ops = plan_insert(&msg, Some(&before), &after, &AliasLookup::default()).unwrap();
            prop_assert_eq!(ops.len(), 3);
        }
    }
}
