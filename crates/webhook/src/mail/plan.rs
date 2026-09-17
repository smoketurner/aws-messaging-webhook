//! The transactional write model: every mail-table transaction is planned as
//! a `Vec<PlannedOp>` up front, then executed and its cancellation decoded
//! uniformly by `mail::txn::decode_cancellation`.
//!
//! `plan_insert` plans the ingest transaction: the message, its versioned
//! thread, and the `Message-ID` alias. It is pure — every input (the message
//! and the thread state before and after) is computed by the caller from
//! consistent reads; this module only turns that data into `WriteOp`s.

use serde_dynamo::AttributeValue;

use crate::mail::send::{SendKey, SendState, SendStatus};
use crate::mail::store::{MailStoreError, SesSent};
use crate::mail::thread::{THREAD_LABEL_TOTAL_CAP, ThreadState};
use crate::mail::{Direction, MailMessage, ThreadSnapshot, ids, keys};

/// Which role a planned op plays in its transaction — used by
/// [`crate::mail::txn::decode_cancellation`] to interpret a cancellation
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

/// Which flow this transaction belongs to; `decode_cancellation`
/// branches on it (e.g. only `Enqueue` produces `TxnDecision::KeyExists`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnKind {
    Insert,
    Enqueue,
    Patch,
    Delivery,
    MarkSent,
    MarkFailed,
    MarkUnknown,
    ResendUnknown,
    RepointPromotion,
    ExpireOutbox,
}

/// One equality/existence check, rendered into a `ConditionExpression`
/// clause by the op's executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    Eq(&'static str, AttributeValue),
    In(&'static str, Vec<AttributeValue>),
    Exists(&'static str),
    NotExists(&'static str),
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
    Delete {
        pk: String,
        sk: String,
    },
    /// The RFC-alias first-writer-wins put: unconditioned by version,
    /// just `NotExists` on the alias key.
    AliasFirstWriter {
        pk: String,
        sk: String,
        message_id: String,
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

/// Builds a `serde_dynamo::Item` from `value`, inserting `pk`/`sk` (and any
/// extra key attributes) alongside the serialized fields: the struct itself
/// carries no key attributes, so every planner adds them here.
fn item_with_keys<T: serde::Serialize>(
    value: &T,
    keys: impl IntoIterator<Item = (&'static str, AttributeValue)>,
) -> Result<serde_dynamo::Item, MailStoreError> {
    let mut item: serde_dynamo::Item = serde_dynamo::to_item(value)
        .map_err(|e| MailStoreError::Permanent(anyhow::anyhow!("serializing item: {e}")))?;
    for (name, value) in keys {
        item.inner_mut().insert(name.to_owned(), value);
    }
    Ok(item)
}

/// Strips a `Message-ID` header value's surrounding `<`/`>`, if present.
fn strip_angle_brackets(rfc_id: &str) -> &str {
    rfc_id
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(rfc_id)
}

/// Aliases skip ids over this many bytes, keeping the alias pk under
/// DynamoDB's key-length limit with headroom for the `RFC#<inbox>#` prefix.
const ALIAS_ID_MAX_BYTES: usize = 900;

/// Plans the ingest transaction: the message, its versioned thread, and the
/// `Message-ID` alias.
///
/// `thread_before` is the thread's state from a consistent read, or `None`
/// for a brand-new thread; `thread_after` is the caller-computed result of
/// applying `msg` to it ([`crate::mail::thread::new_thread`] /
/// [`crate::mail::thread::apply_message`]).
///
/// # Errors
///
/// Returns [`MailStoreError::LabelLimit`] when `thread_after`'s label union
/// exceeds [`THREAD_LABEL_TOTAL_CAP`], which keeps the thread item's label
/// set clear of DynamoDB's item-size limit. It is permanent: a retry would
/// plan the same oversized item.
pub fn plan_insert(
    msg: &MailMessage,
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    if thread_after.labels.len() > THREAD_LABEL_TOTAL_CAP {
        return Err(MailStoreError::LabelLimit(format!(
            "thread {} would carry {} labels, over the cap of {THREAD_LABEL_TOTAL_CAP}",
            thread_after.thread_id,
            thread_after.labels.len(),
        )));
    }
    debug_assert!(
        is_sorted_and_deduped(&msg.labels),
        "message labels must be sorted and deduplicated before planning"
    );
    debug_assert!(
        is_sorted_and_deduped(&thread_after.labels),
        "thread labels must be sorted and deduplicated before planning"
    );

    // Message, thread, and at most one `Message-ID` alias.
    let mut ops = Vec::with_capacity(3);
    ops.push(message_put(msg, thread_after)?);
    ops.push(thread_put(thread_before, thread_after)?);
    push_rfc_alias_op(&mut ops, msg);

    Ok(ops)
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
/// [`MailStoreError::LabelLimit`] when the thread's union would exceed
/// [`THREAD_LABEL_TOTAL_CAP`].
pub fn plan_enqueue(
    msg: &MailMessage,
    state: &SendState,
    key: Option<&SendKey>,
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
    now_epoch: u64,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    if thread_after.labels.len() > THREAD_LABEL_TOTAL_CAP {
        return Err(MailStoreError::LabelLimit(format!(
            "thread {} would carry {} labels, over the cap of {THREAD_LABEL_TOTAL_CAP}",
            thread_after.thread_id,
            thread_after.labels.len(),
        )));
    }

    let mut ops = Vec::with_capacity(5);

    // First, so a cancellation names the key conflict before anything else.
    if let Some(key) = key {
        let key_keys = [
            ("pk", AttributeValue::S(keys::send_key_pk(&key.key_hash))),
            ("sk", AttributeValue::S(keys::send_key_sk().to_owned())),
        ];
        ops.push(PlannedOp {
            role: OpRole::IdempotencyKey,
            op: WriteOp::Put {
                item: item_with_keys(key, key_keys)?,
                // An expired key is reusable; a live one is a replay, which
                // the caller resolves by reading it back.
                cond: Cond::NotExistsOrExpired { now_epoch },
            },
        });
    }

    ops.push(message_put(msg, thread_after)?);

    ops.push(send_state_put(None, state)?);
    ops.push(thread_put(thread_before, thread_after)?);
    push_rfc_alias_op(&mut ops, msg);

    Ok(ops)
}

/// The `Put` for a send-state item, version-conditioned on what was read.
///
/// The index keys move with the status, which is what keeps the sweep's view
/// of "queued" and "sending" accurate without a second write.
fn send_state_put(
    before: Option<&SendState>,
    after: &SendState,
) -> Result<PlannedOp, MailStoreError> {
    send_state_op(
        after,
        before.map_or(Cond::NotExists, |before| {
            Cond::VersionEquals(before.version)
        }),
    )
}

/// The send-state item with its keys, under an arbitrary condition.
fn send_state_op(state: &SendState, cond: Cond) -> Result<PlannedOp, MailStoreError> {
    let state_keys = [
        ("pk", AttributeValue::S(keys::outbox_pk(&state.message_id))),
        ("sk", AttributeValue::S(keys::outbox_sk().to_owned())),
        (
            "gsi3pk",
            AttributeValue::S(format!("SENDSTATUS#{}", state.send_status.as_str())),
        ),
        ("gsi3sk", AttributeValue::S(state.message_id.clone())),
    ];
    Ok(PlannedOp {
        role: OpRole::SendState,
        op: WriteOp::Put {
            item: item_with_keys(state, state_keys)?,
            cond,
        },
    })
}

/// Plans the claim that hands one queued send to one sender.
///
/// The condition is on the status, not just the version: a record that is
/// already `sending` must not be claimed again, however old its version
/// looks, because that is exactly how the same message gets sent twice.
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn plan_claim(before: &SendState, after: &SendState) -> Result<Vec<PlannedOp>, MailStoreError> {
    Ok(vec![send_state_op(
        after,
        Cond::All(vec![
            Check::Eq(
                "send_status",
                AttributeValue::S(SendStatus::Queued.as_str().to_owned()),
            ),
            Check::Eq("version", AttributeValue::N(before.version.to_string())),
        ]),
    )?])
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

    let mut ops = Vec::with_capacity(4);
    ops.push(send_state_put(Some(state_before), state_after)?);

    let mut set = vec![
        (
            "version".to_owned(),
            AttributeValue::N((msg.version + 1).to_string()),
        ),
        ("updated_at".to_owned(), AttributeValue::S(now.to_owned())),
        (
            "send_status".to_owned(),
            AttributeValue::S(state_after.send_status.as_str().to_owned()),
        ),
    ];
    let mut remove = Vec::new();
    if new_labels.is_empty() {
        remove.push("labels".to_owned());
    } else {
        set.push(("labels".to_owned(), AttributeValue::Ss(new_labels.to_vec())));
    }
    if let Some(ses) = ses {
        set.push((
            "ses_message_id".to_owned(),
            AttributeValue::S(ses.message_id.to_owned()),
        ));
        set.push(("sent_at".to_owned(), AttributeValue::S(now.to_owned())));
    }

    ops.push(PlannedOp {
        role: OpRole::Message,
        op: WriteOp::Update {
            pk: keys::inbox_pk(msg.inbox_id.as_str()),
            sk: keys::message_sk(&msg.message_id),
            set,
            remove,
            cond: Cond::VersionEquals(msg.version),
        },
    });

    // The thread's labels roll up its messages', so a status label that moves
    // on the message moves on the thread in the same transaction.
    if let Some((thread_before, thread_after)) = thread {
        ops.push(thread_put(Some(thread_before), thread_after)?);
    }

    if let Some(ses) = ses {
        // SES writes its own `Message-ID` over ours, so replies to this
        // message, and its copy if it was sent to this inbox, name the SES
        // forms. A redrive finds these already taken, which the caller
        // resolves by dropping them.
        for rfc_id in ids::ses_rfc_ids(ses.message_id, ses.region) {
            push_alias(&mut ops, msg, &rfc_id);
        }

        // Unconditioned: a redrive that marks the same send again should
        // rewrite this rather than cancel the whole transaction.
        let mut item = serde_dynamo::Item::default();
        item.inner_mut().insert(
            "pk".to_owned(),
            AttributeValue::S(keys::ses_ref_pk(ses.message_id)),
        );
        item.inner_mut().insert(
            "sk".to_owned(),
            AttributeValue::S(keys::ses_ref_sk().to_owned()),
        );
        item.inner_mut().insert(
            "inbox_id".to_owned(),
            AttributeValue::S(msg.inbox_id.as_str().to_owned()),
        );
        item.inner_mut().insert(
            "message_id".to_owned(),
            AttributeValue::S(msg.message_id.clone()),
        );
        item.inner_mut().insert(
            "expires_at".to_owned(),
            AttributeValue::N(msg.expires_at.to_string()),
        );
        ops.push(PlannedOp {
            role: OpRole::SesRef,
            op: WriteOp::Put {
                item,
                cond: Cond::None,
            },
        });
    }

    Ok(ops)
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
/// [`MailStoreError::LabelLimit`] when the thread's union would exceed
/// [`THREAD_LABEL_TOTAL_CAP`].
pub fn plan_patch(
    msg: &MailMessage,
    new_labels: &[String],
    thread_before: &ThreadState,
    thread_after: &ThreadState,
    now: &str,
) -> Result<Vec<PlannedOp>, MailStoreError> {
    if thread_after.labels.len() > THREAD_LABEL_TOTAL_CAP {
        return Err(MailStoreError::LabelLimit(format!(
            "thread {} would carry {} labels, over the cap of {THREAD_LABEL_TOTAL_CAP}",
            thread_after.thread_id,
            thread_after.labels.len(),
        )));
    }
    debug_assert!(
        is_sorted_and_deduped(new_labels),
        "message labels must be sorted and deduplicated before planning"
    );

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

    Ok(vec![
        PlannedOp {
            role: OpRole::Message,
            op: WriteOp::Update {
                pk: keys::inbox_pk(msg.inbox_id.as_str()),
                sk: keys::message_sk(&msg.message_id),
                set,
                remove,
                cond: Cond::VersionEquals(msg.version),
            },
        },
        thread_put(Some(thread_before), thread_after)?,
    ])
}

/// Builds the message item's `Put`. For an inbound message the
/// `thread_snapshot` is populated here from `thread_after` — the
/// caller-computed thread state that already includes this message — on a
/// clone of `msg`. `msg` itself, and every other planned op, is unaffected.
fn message_put(msg: &MailMessage, thread_after: &ThreadState) -> Result<PlannedOp, MailStoreError> {
    let mut item_source = msg.clone();
    if matches!(msg.direction, Direction::Inbound) {
        item_source.thread_snapshot = Some(ThreadSnapshot::from(thread_after));
    }

    let message_keys = [
        (
            "pk",
            AttributeValue::S(keys::inbox_pk(item_source.inbox_id.as_str())),
        ),
        (
            "sk",
            AttributeValue::S(keys::message_sk(&item_source.message_id)),
        ),
        (
            "gsi1pk",
            AttributeValue::S(format!("INBOX#{}#MSG", item_source.inbox_id.as_str())),
        ),
        ("gsi1sk", AttributeValue::S(item_source.message_id.clone())),
        (
            "gsi2pk",
            AttributeValue::S(format!(
                "THREAD#{}#{}",
                item_source.inbox_id.as_str(),
                item_source.thread_id
            )),
        ),
        ("gsi2sk", AttributeValue::S(item_source.message_id.clone())),
    ];
    Ok(PlannedOp {
        role: OpRole::Message,
        op: WriteOp::Put {
            item: item_with_keys(&item_source, message_keys)?,
            cond: Cond::NotExists,
        },
    })
}

fn thread_put(
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
) -> Result<PlannedOp, MailStoreError> {
    let thread_keys = [
        (
            "pk",
            AttributeValue::S(keys::inbox_pk(thread_after.inbox_id.as_str())),
        ),
        (
            "sk",
            AttributeValue::S(keys::thread_sk(&thread_after.thread_id)),
        ),
        (
            "gsi1pk",
            AttributeValue::S(format!("INBOX#{}#THR", thread_after.inbox_id.as_str())),
        ),
        (
            "gsi1sk",
            AttributeValue::S(format!(
                "{}#{}",
                thread_after.timestamp, thread_after.thread_id
            )),
        ),
    ];
    let cond = match thread_before {
        Some(before) => Cond::VersionEquals(before.version),
        None => Cond::NotExists,
    };
    Ok(PlannedOp {
        role: OpRole::Thread,
        op: WriteOp::Put {
            item: item_with_keys(thread_after, thread_keys)?,
            cond,
        },
    })
}

/// Pushes the alias op for `msg`'s own `Message-ID`.
fn push_rfc_alias_op(ops: &mut Vec<PlannedOp>, msg: &MailMessage) {
    push_alias(ops, msg, &msg.rfc_message_id);
}

/// Pushes an alias op mapping `rfc_id` to `msg` and its thread, skipping ids
/// over [`ALIAS_ID_MAX_BYTES`].
fn push_alias(ops: &mut Vec<PlannedOp>, msg: &MailMessage, rfc_id: &str) {
    let stripped = strip_angle_brackets(rfc_id);
    if stripped.len() <= ALIAS_ID_MAX_BYTES {
        ops.push(PlannedOp {
            role: OpRole::RfcAlias,
            op: WriteOp::AliasFirstWriter {
                pk: keys::rfc_alias_pk(msg.inbox_id.as_str(), stripped),
                sk: keys::rfc_alias_sk().to_owned(),
                message_id: msg.message_id.clone(),
                thread_id: msg.thread_id.clone(),
                expires_at: msg.expires_at,
            },
        });
    }
}

fn is_sorted_and_deduped(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::mail::thread::{apply_message, new_thread};
    use crate::mail::{Direction, InboxId};

    fn message(inbox: &str, thread_id: &str, message_id: &str, labels: &[&str]) -> MailMessage {
        MailMessage {
            inbox_id: InboxId(inbox.to_owned()),
            thread_id: thread_id.to_owned(),
            message_id: message_id.to_owned(),
            ses_message_id: None,
            direction: Direction::Inbound,
            rfc_message_id: format!("<{message_id}@example.com>"),
            in_reply_to: None,
            labels: labels.iter().map(|l| (*l).to_owned()).collect(),
            timestamp: "00000001-0000".to_owned(),
            from: "sender@example.com".to_owned(),
            to: vec!["support@example.com".to_owned()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            size: 1_000,
            attachments: Vec::new(),
            attachments_truncated: false,
            raw_s3_key: Some("inbound/x".to_owned()),
            thread_snapshot: None,
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 0,
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
            expires_at: 0,
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
        let ops = plan_insert(&msg, None, &thread).unwrap();

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

        let ops = plan_insert(&msg, None, &thread).unwrap();
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
                } if message_id == &outbound.message_id => Some((pk.as_str(), *expires_at)),
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

        let ops = plan_insert(&second, Some(&before), &after).unwrap();
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
            crate::mail::send::Envelope {
                to: vec!["recipient@example.com".to_owned()],
                ..crate::mail::send::Envelope::default()
            },
            key_pk.map(ToOwned::to_owned),
            "2026-01-01T00:00:00.000Z",
        )
    }

    #[test]
    fn plan_enqueue_commits_the_message_and_its_send_state_together() {
        // A queued message without its state item would never be claimed, so
        // both must be in the same transaction, and both must be new.
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);
        let state = queued_state(&msg, None);

        let ops = plan_enqueue(&msg, &state, None, None, &thread, 1_800_000_000).unwrap();

        let roles: Vec<OpRole> = ops.iter().map(|o| o.role).collect();
        assert_eq!(
            roles,
            vec![
                OpRole::Message,
                OpRole::SendState,
                OpRole::Thread,
                OpRole::RfcAlias
            ]
        );
        for role in [OpRole::Message, OpRole::SendState] {
            let WriteOp::Put { cond, .. } = &ops.iter().find(|o| o.role == role).unwrap().op else {
                panic!("expected a Put for {role:?}");
            };
            assert_eq!(*cond, Cond::NotExists, "{role:?} must be new");
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
        let key = crate::mail::send::SendKey {
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

    #[test]
    fn plan_insert_rejects_a_thread_over_the_label_union_cap() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let mut thread = new_thread(&msg);
        for i in 0..THREAD_LABEL_TOTAL_CAP {
            thread.labels.push(format!("label-{i:03}"));
        }
        thread.labels.sort();
        msg.labels = vec!["received".to_owned()];
        let err = plan_insert(&msg, None, &thread).unwrap_err();
        assert!(matches!(err, MailStoreError::LabelLimit(_)));
    }

    #[test]
    fn plan_insert_populates_thread_snapshot_from_thread_after_for_inbound_messages() {
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
        second.from = "other@example.com".to_owned();
        let after = apply_message(&before, &second);

        let ops = plan_insert(&second, Some(&before), &after).unwrap();
        let WriteOp::Put { item, .. } = &ops.iter().find(|o| o.role == OpRole::Message).unwrap().op
        else {
            panic!("expected a Put");
        };
        let stored: MailMessage = serde_dynamo::from_item(item.clone()).unwrap();
        let snapshot = stored
            .thread_snapshot
            .expect("an inbound insert always carries a snapshot");
        assert_eq!(snapshot.thread_id, "tid-1");
        assert_eq!(snapshot.message_count, 2);
        assert_eq!(snapshot.senders, after.senders);
        assert_eq!(snapshot.recipients, after.recipients);
    }

    #[test]
    fn plan_insert_leaves_an_outbound_messages_thread_snapshot_untouched() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);

        let ops = plan_insert(&msg, None, &thread).unwrap();
        let WriteOp::Put { item, .. } = &ops.iter().find(|o| o.role == OpRole::Message).unwrap().op
        else {
            panic!("expected a Put");
        };
        let stored: MailMessage = serde_dynamo::from_item(item.clone()).unwrap();
        assert!(stored.thread_snapshot.is_none());
    }

    #[test]
    fn plan_insert_skips_an_oversized_rfc_alias() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        msg.rfc_message_id = format!("<{}@example.com>", "x".repeat(ALIAS_ID_MAX_BYTES + 10));
        let thread = new_thread(&msg);
        let ops = plan_insert(&msg, None, &thread).unwrap();
        assert!(ops.iter().all(|o| o.role != OpRole::RfcAlias));
    }

    proptest! {
        /// How many labels a message or its thread carries does not change
        /// how many items the ingest transaction writes — the labels live in
        /// the message and thread items themselves, not in per-label rows.
        #[test]
        fn plan_insert_writes_a_fixed_number_of_items_whatever_the_labels(
            message_label_count in 0usize..=4,
            thread_label_count in 0usize..=THREAD_LABEL_TOTAL_CAP,
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

            let ops = plan_insert(&msg, Some(&before), &after).unwrap();
            prop_assert_eq!(ops.len(), 3);
        }
    }
}
