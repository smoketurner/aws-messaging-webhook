//! The transactional write model (§4 "Write model", D27): every mail-table
//! transaction is planned as a `Vec<PlannedOp>` up front, then executed and
//! its cancellation decoded uniformly by `mail::txn::decode_cancellation`.
//!
//! `plan_insert` (ingest, P1) plans the D26 71-op ingest transaction: the
//! message, its versioned thread (D3/D4), message/thread label pointers, and
//! the `Message-ID` alias (D2). It is pure — every input (the message and
//! the thread state before/after) is computed by the caller from consistent
//! reads; this module only turns that data into `WriteOp`s.
//!
//! `plan_label_patch` (PATCH, P2) is added to this file when phase 2 lands —
//! see the `mail::store` deviation note on why this file, like `mail::store`,
//! is revisited outside the plan's literal shared-files list.

use serde_dynamo::AttributeValue;

use crate::mail::store::MailStoreError;
use crate::mail::thread::{THREAD_LABEL_TOTAL_CAP, ThreadState};
use crate::mail::{Direction, MailMessage, ThreadSnapshot, keys, size};

/// The ingest transaction's op-count ceiling (D26): message + thread + 4
/// message pointers + 32 thread-pointer deletes + 32 puts + 1 alias.
const INGEST_TXN_OP_CAP: usize = 71;

/// Which role a planned op plays in its transaction — used by
/// [`crate::mail::txn::decode_cancellation`] to interpret a cancellation
/// reason without re-deriving it from the op's shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpRole {
    IdempotencyKey,
    Message,
    SendState,
    Thread,
    MessagePointer,
    ThreadPointer,
    RfcAlias,
    SesRef,
}

/// Which flow this transaction belongs to (D27); `decode_cancellation`
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
    /// The RFC-alias first-writer-wins put (D2): unconditioned by version,
    /// just `NotExists` on the alias key.
    AliasFirstWriter {
        pk: String,
        sk: String,
        message_id: String,
        thread_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedOp {
    pub role: OpRole,
    pub op: WriteOp,
}

/// Builds a `serde_dynamo::Item` from `value`, inserting `pk`/`sk` (and any
/// extra key attributes) alongside the serialized fields (N18): the struct
/// itself carries no key attributes, so every planner adds them here.
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

/// Builds the label pointer item for a thread (`INBOX#<inbox>#LABEL#<label>`
/// / `THRAT#<timestamp>#<thread_id>`, §4).
fn thread_pointer_item(thread: &ThreadState, label: &str) -> serde_dynamo::Item {
    let mut item = serde_dynamo::Item::default();
    item.inner_mut().insert(
        "pk".to_owned(),
        AttributeValue::S(keys::label_pk(thread.inbox_id.as_str(), label)),
    );
    item.inner_mut().insert(
        "sk".to_owned(),
        AttributeValue::S(keys::thread_pointer_sk(
            &thread.timestamp,
            &thread.thread_id,
        )),
    );
    item.inner_mut().insert(
        "thread_id".to_owned(),
        AttributeValue::S(thread.thread_id.clone()),
    );
    item
}

/// Builds the message pointer item for a label
/// (`INBOX#<inbox>#LABEL#<label>` / `MSGAT#<message_id>`, §4).
fn message_pointer_item(msg: &MailMessage, label: &str) -> serde_dynamo::Item {
    let mut item = serde_dynamo::Item::default();
    item.inner_mut().insert(
        "pk".to_owned(),
        AttributeValue::S(keys::label_pk(msg.inbox_id.as_str(), label)),
    );
    item.inner_mut().insert(
        "sk".to_owned(),
        AttributeValue::S(keys::message_pointer_sk(&msg.message_id)),
    );
    item.inner_mut().insert(
        "message_id".to_owned(),
        AttributeValue::S(msg.message_id.clone()),
    );
    item
}

/// Strips a `Message-ID` header value's surrounding `<`/`>`, if present.
fn strip_angle_brackets(rfc_id: &str) -> &str {
    rfc_id
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(rfc_id)
}

/// Aliases skip ids over this many bytes (D2), keeping the alias pk under
/// DynamoDB's key-length limit with headroom for the `RFC#<inbox>#` prefix.
const ALIAS_ID_MAX_BYTES: usize = 900;

/// Plans the ingest transaction (§6.1 step 7, D2, D3, D4, D26, D30): the
/// message, its versioned thread, message/thread label pointers, and the
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
/// exceeds [`THREAD_LABEL_TOTAL_CAP`] (D26), and
/// [`MailStoreError::Permanent`] if the planned transaction would exceed the
/// [`INGEST_TXN_OP_CAP`] (D26) — both planner-detected cap violations, per
/// the action-path error mapping in §5.
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

    let mut ops = Vec::with_capacity(INGEST_TXN_OP_CAP);
    ops.push(message_put(msg, thread_after)?);
    ops.push(thread_put(thread_before, thread_after)?);
    push_thread_pointer_ops(&mut ops, thread_before, thread_after);
    push_message_pointer_ops(&mut ops, msg);
    push_rfc_alias_op(&mut ops, msg);

    if ops.len() > INGEST_TXN_OP_CAP {
        return Err(MailStoreError::Permanent(anyhow::anyhow!(
            "ingest transaction for message {} would need {} ops, over the cap of {INGEST_TXN_OP_CAP}",
            msg.message_id,
            ops.len(),
        )));
    }

    Ok(ops)
}

/// Builds the message item's `Put`. N19: for an inbound message, the
/// `thread_snapshot` is populated here from `thread_after` — the
/// caller-computed thread state that already includes this message — on a
/// clone of `msg`, then `fit_item` is re-run since the injected snapshot can
/// push the item back over the D5 budget after ingest already fit it without
/// one. `msg` itself (and every other planned op) is unaffected.
fn message_put(msg: &MailMessage, thread_after: &ThreadState) -> Result<PlannedOp, MailStoreError> {
    let mut item_source = msg.clone();
    if matches!(msg.direction, Direction::Inbound) {
        item_source.thread_snapshot = Some(ThreadSnapshot::from(thread_after));
        size::fit_item(&mut item_source);
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

/// Deletes every label pointer at the thread's old timestamp, then puts one
/// at its new timestamp for every label in the union (D3: the pointer's sort
/// key embeds the thread's last-activity timestamp, so it moves on every
/// message even when the label set itself doesn't change).
fn push_thread_pointer_ops(
    ops: &mut Vec<PlannedOp>,
    thread_before: Option<&ThreadState>,
    thread_after: &ThreadState,
) {
    if let Some(before) = thread_before {
        for label in &before.labels {
            ops.push(PlannedOp {
                role: OpRole::ThreadPointer,
                op: WriteOp::Delete {
                    pk: keys::label_pk(before.inbox_id.as_str(), label),
                    sk: keys::thread_pointer_sk(&before.timestamp, &before.thread_id),
                },
            });
        }
    }
    for label in &thread_after.labels {
        ops.push(PlannedOp {
            role: OpRole::ThreadPointer,
            op: WriteOp::Put {
                item: thread_pointer_item(thread_after, label),
                cond: Cond::None,
            },
        });
    }
}

fn push_message_pointer_ops(ops: &mut Vec<PlannedOp>, msg: &MailMessage) {
    for label in &msg.labels {
        ops.push(PlannedOp {
            role: OpRole::MessagePointer,
            op: WriteOp::Put {
                item: message_pointer_item(msg, label),
                cond: Cond::None,
            },
        });
    }
}

/// Pushes the `Message-ID` alias op (D2), skipping ids over
/// [`ALIAS_ID_MAX_BYTES`].
fn push_rfc_alias_op(ops: &mut Vec<PlannedOp>, msg: &MailMessage) {
    let stripped = strip_angle_brackets(&msg.rfc_message_id);
    if stripped.len() <= ALIAS_ID_MAX_BYTES {
        ops.push(PlannedOp {
            role: OpRole::RfcAlias,
            op: WriteOp::AliasFirstWriter {
                pk: keys::rfc_alias_pk(msg.inbox_id.as_str(), stripped),
                sk: keys::rfc_alias_sk().to_owned(),
                message_id: msg.message_id.clone(),
                thread_id: msg.thread_id.clone(),
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
            references: Vec::new(),
            labels: labels.iter().map(|l| (*l).to_owned()).collect(),
            timestamp: "00000001-0000".to_owned(),
            from: "sender@example.com".to_owned(),
            reply_to: Vec::new(),
            to: vec!["support@example.com".to_owned()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            size: 1_000,
            text: Some("hello there".to_owned()),
            html: None,
            body_truncated: false,
            headers: BTreeMap::new(),
            attachments: Vec::new(),
            attachments_truncated: false,
            raw_s3_key: Some("inbound/x".to_owned()),
            verdicts: None,
            thread_snapshot: None,
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 0,
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
        }
    }

    #[test]
    fn plan_insert_for_a_new_thread_has_no_pointer_deletes() {
        let msg = message("support", "tid-1", "mid-1", &["received", "unread"]);
        let thread = new_thread(&msg);
        let ops = plan_insert(&msg, None, &thread).unwrap();

        assert!(ops.iter().any(|o| o.role == OpRole::Message));
        assert!(ops.iter().any(|o| o.role == OpRole::Thread));
        assert_eq!(
            ops.iter()
                .filter(
                    |o| o.role == OpRole::ThreadPointer && matches!(o.op, WriteOp::Delete { .. })
                )
                .count(),
            0
        );
        assert_eq!(
            ops.iter()
                .filter(|o| o.role == OpRole::ThreadPointer && matches!(o.op, WriteOp::Put { .. }))
                .count(),
            2
        );
        assert_eq!(
            ops.iter()
                .filter(|o| o.role == OpRole::MessagePointer)
                .count(),
            2
        );
        assert_eq!(ops.iter().filter(|o| o.role == OpRole::RfcAlias).count(), 1);
    }

    #[test]
    fn plan_insert_for_an_existing_thread_deletes_old_pointers_and_puts_new_ones() {
        let first = message("support", "tid-1", "mid-1", &["received", "unread"]);
        let before = new_thread(&first);
        let mut second = message("support", "tid-1", "mid-2", &["received", "unread"]);
        second.timestamp = "00000002-0000".to_owned();
        let after = apply_message(&before, &second);

        let ops = plan_insert(&second, Some(&before), &after).unwrap();
        let deletes = ops
            .iter()
            .filter(|o| o.role == OpRole::ThreadPointer && matches!(o.op, WriteOp::Delete { .. }))
            .count();
        let puts = ops
            .iter()
            .filter(|o| o.role == OpRole::ThreadPointer && matches!(o.op, WriteOp::Put { .. }))
            .count();
        assert_eq!(deletes, 2);
        assert_eq!(puts, 2);

        let WriteOp::Put { cond, .. } = &ops.iter().find(|o| o.role == OpRole::Thread).unwrap().op
        else {
            panic!("expected a Put");
        };
        assert_eq!(*cond, Cond::VersionEquals(0));
    }

    #[test]
    fn plan_insert_rejects_a_thread_over_the_label_union_cap() {
        let mut msg = message("support", "tid-1", "mid-1", &["received"]);
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
        let first = message("support", "tid-1", "mid-1", &["received", "unread"]);
        let before = new_thread(&first);
        let mut second = message("support", "tid-1", "mid-2", &["received", "unread"]);
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
        let mut msg = message("support", "tid-1", "mid-1", &["queued"]);
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
        let mut msg = message("support", "tid-1", "mid-1", &["received"]);
        msg.rfc_message_id = format!("<{}@example.com>", "x".repeat(ALIAS_ID_MAX_BYTES + 10));
        let thread = new_thread(&msg);
        let ops = plan_insert(&msg, None, &thread).unwrap();
        assert!(ops.iter().all(|o| o.role != OpRole::RfcAlias));
    }

    proptest! {
        /// At maxima (4 message labels, a 32-label thread before and after),
        /// the ingest plan never exceeds the D26 op cap of 71.
        #[test]
        fn plan_insert_never_exceeds_the_op_cap_at_maxima(
            message_label_count in 0usize..=4,
            thread_label_count in 0usize..=THREAD_LABEL_TOTAL_CAP,
        ) {
            let message_labels: Vec<String> = (0..message_label_count)
                .map(|i| format!("m{i:02}"))
                .collect();
            let mut msg = message("support", "tid-1", "mid-1", &[]);
            msg.labels = message_labels;

            let mut before = new_thread(&msg);
            before.labels = (0..thread_label_count).map(|i| format!("t{i:03}")).collect();
            before.version = 5;

            let mut after = before.clone();
            after.timestamp = "00000002-0000".to_owned();
            after.version += 1;

            let result = plan_insert(&msg, Some(&before), &after);
            prop_assert!(result.is_ok());
            prop_assert!(result.unwrap().len() <= INGEST_TXN_OP_CAP);
        }
    }
}
