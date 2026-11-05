//! The transactional write model: every mail-table transaction is planned as
//! a `Vec<PlannedOp>` up front, then executed and its cancellation decoded
//! uniformly by `mail::txn::decode_cancellation`.
//!
//! `plan_insert` plans the ingest transaction: the message, its versioned
//! thread, its `Message-ID` alias and placeholder aliases for the ids it
//! references that no row maps yet. It is pure — every input (the message,
//! the thread state before and after, the alias rows) is computed by the
//! caller from consistent reads; this module only turns that data into
//! `WriteOp`s.

use serde_dynamo::AttributeValue;

use crate::mail::send::{SendKey, SendState, SendStatus};
use crate::mail::store::{MailStoreError, SesSent};
use crate::mail::thread::{AliasLookup, ThreadState};
use crate::mail::{
    Direction, MESSAGE_USER_LABEL_CAP, MailMessage, THREAD_USER_LABEL_CAP, ThreadSnapshot, ids,
    keys, labels, strip_angle_brackets,
};

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

/// The inbox record and the item both stores write for it: its keys, and
/// the single `ByTime` partition every inbox shares so `GET /v0/inboxes` is
/// one query (there are few enough inboxes for that to stay cheap).
///
/// # Errors
///
/// Propagates a serialization failure as [`MailStoreError::Permanent`].
pub fn inbox_item(
    inbox: &crate::mail::InboxId,
    now: &str,
) -> Result<(crate::mail::Inbox, serde_dynamo::Item), MailStoreError> {
    let record = crate::mail::Inbox {
        inbox_id: inbox.clone(),
        email: inbox.as_str().to_owned(),
        display_name: None,
        metadata: None,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let item = item_with_keys(
        &record,
        [
            ("pk", AttributeValue::S(keys::inbox_pk(inbox.as_str()))),
            ("sk", AttributeValue::S(keys::inbox_sk().to_owned())),
            (
                "gsi1pk",
                AttributeValue::S(keys::inboxes_partition().to_owned()),
            ),
            ("gsi1sk", AttributeValue::S(inbox.as_str().to_owned())),
        ],
    )?;
    Ok((record, item))
}

/// The version bump and label rewrite every message update shares: a new
/// version, `updated_at`, and the labels — removed rather than written empty,
/// since DynamoDB has no empty string set.
fn message_label_update(
    msg: &MailMessage,
    new_labels: &[String],
    now: &str,
) -> (Vec<(String, AttributeValue)>, Vec<String>) {
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
    (set, remove)
}

/// One planned op writing a message's new labels under its version.
fn message_label_op(
    msg: &MailMessage,
    set: Vec<(String, AttributeValue)>,
    remove: Vec<String>,
) -> PlannedOp {
    PlannedOp {
        role: OpRole::Message,
        op: WriteOp::Update {
            pk: keys::inbox_pk(msg.inbox_id.as_str()),
            sk: keys::message_sk(&msg.message_id),
            set,
            remove,
            cond: Cond::VersionEquals(msg.version),
        },
    }
}

/// Aliases skip ids over this many bytes, keeping the alias pk under
/// DynamoDB's key-length limit with headroom for the `RFC#<inbox>#` prefix.
const ALIAS_ID_MAX_BYTES: usize = 900;

/// Plans the ingest transaction: the message, its versioned thread, its
/// `Message-ID` alias, and a placeholder alias for every candidate id in
/// `aliases` that no row maps yet.
///
/// `thread_before` is the thread's state from a consistent read, or `None`
/// for a brand-new thread; `thread_after` is the caller-computed result of
/// applying `msg` to it ([`crate::mail::thread::new_thread`] /
/// [`crate::mail::thread::apply_message`]).
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

    let mut ops = Vec::with_capacity(3 + aliases.candidates.len());
    ops.push(message_put(msg, thread_after)?);
    ops.push(thread_put(thread_before, thread_after)?);

    let own_id = strip_angle_brackets(&msg.rfc_message_id);
    if !own_id.is_empty() && aliases.is_claimable(own_id) {
        push_alias_row(
            &mut ops,
            msg,
            &msg.rfc_message_id,
            Some(msg.message_id.clone()),
        );
    }
    for candidate in &aliases.candidates {
        if candidate != own_id && !aliases.found.contains_key(candidate) {
            push_alias_row(&mut ops, msg, candidate, None);
        }
    }

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
    push_alias_row(
        &mut ops,
        msg,
        &msg.rfc_message_id,
        Some(msg.message_id.clone()),
    );

    Ok(ops)
}

/// The `Put` for a send-state item, version-conditioned on what was read.
///
/// The index keys move with the status, which is what keeps the sweep's view
/// of "queued" and "sending" accurate without a second write, and leave the
/// item on the write that settles it.
fn send_state_put(
    before: Option<&SendState>,
    after: &SendState,
) -> Result<PlannedOp, MailStoreError> {
    send_state_op(
        after,
        version_or_absent(before.map(|before| before.version)),
    )
}

/// The optimistic-concurrency condition for a write over what was read: the
/// version it read, or that nothing exists when it read nothing.
fn version_or_absent(version: Option<u64>) -> Cond {
    version.map_or(Cond::NotExists, Cond::VersionEquals)
}

/// The send-state item with its keys, under an arbitrary condition.
///
/// A settled send carries no `ByStatus` keys. The index exists for the
/// sweep, which only ever queries `sending` and `unknown`; writing the keys
/// for `sent` and `failed` too would pay an index write on the one
/// transaction every send is guaranteed to make and then keep the whole
/// item (the index projects ALL) in the index until its TTL. Since these
/// are `Put`s, dropping the attributes on the settling write is what
/// removes the entry.
fn send_state_op(state: &SendState, cond: Cond) -> Result<PlannedOp, MailStoreError> {
    let mut state_keys = vec![
        ("pk", AttributeValue::S(keys::outbox_pk(&state.message_id))),
        ("sk", AttributeValue::S(keys::outbox_sk().to_owned())),
    ];
    if state.send_status.is_swept() {
        state_keys.push((
            "gsi3pk",
            AttributeValue::S(format!("SENDSTATUS#{}", state.send_status.as_str())),
        ));
        state_keys.push(("gsi3sk", AttributeValue::S(state.message_id.clone())));
    }
    Ok(PlannedOp {
        role: OpRole::SendState,
        op: WriteOp::Put {
            item: item_with_keys(state, state_keys)?,
            cond,
        },
    })
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
    Ok(vec![send_state_op(
        after,
        Cond::All(vec![
            Check::Eq(
                "send_status",
                AttributeValue::S(expected.as_str().to_owned()),
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

    let (mut set, remove) = message_label_update(msg, new_labels, now);
    set.push((
        "send_status".to_owned(),
        AttributeValue::S(state_after.send_status.as_str().to_owned()),
    ));
    if let Some(ses) = ses {
        set.push((
            "ses_message_id".to_owned(),
            AttributeValue::S(ses.message_id.to_owned()),
        ));
        set.push(("sent_at".to_owned(), AttributeValue::S(now.to_owned())));
    }

    ops.push(message_label_op(msg, set, remove));

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
            push_alias_row(&mut ops, msg, &rfc_id, Some(msg.message_id.clone()));
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

    let (set, remove) = message_label_update(msg, new_labels, now);

    Ok(vec![
        message_label_op(msg, set, remove),
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
            AttributeValue::S(keys::messages_partition(item_source.inbox_id.as_str())),
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
            AttributeValue::S(keys::threads_partition(thread_after.inbox_id.as_str())),
        ),
        (
            "gsi1sk",
            AttributeValue::S(keys::thread_time_sort(
                &thread_after.timestamp,
                &thread_after.thread_id,
            )),
        ),
    ];
    let cond = version_or_absent(thread_before.map(|before| before.version));
    Ok(PlannedOp {
        role: OpRole::Thread,
        op: WriteOp::Put {
            item: item_with_keys(thread_after, thread_keys)?,
            cond,
        },
    })
}

/// Pushes an alias op mapping `rfc_id` to `msg`'s thread, skipping ids over
/// [`ALIAS_ID_MAX_BYTES`]. `message_id` is `msg`'s own id for an alias, or
/// `None` for a placeholder: an id `msg` references but this inbox holds no
/// message for, which expires with `msg`, the message whose reference it
/// records.
fn push_alias_row(
    ops: &mut Vec<PlannedOp>,
    msg: &MailMessage,
    rfc_id: &str,
    message_id: Option<String>,
) {
    let stripped = strip_angle_brackets(rfc_id);
    if stripped.len() <= ALIAS_ID_MAX_BYTES {
        ops.push(PlannedOp {
            role: OpRole::RfcAlias,
            op: WriteOp::AliasFirstWriter {
                pk: keys::rfc_alias_pk(msg.inbox_id.as_str(), stripped),
                sk: keys::rfc_alias_sk().to_owned(),
                message_id,
                thread_id: msg.thread_id.clone(),
                expires_at: msg.expires_at,
            },
        });
    }
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
    use crate::mail::thread::{apply_message, new_thread};
    use crate::mail::{Direction, InboxId, test_fixtures};

    fn message(inbox: &str, thread_id: &str, message_id: &str, labels: &[&str]) -> MailMessage {
        MailMessage {
            inbox_id: InboxId::new(inbox),
            thread_id: thread_id.to_owned(),
            message_id: message_id.to_owned(),
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

    fn alias_writes(ops: &[PlannedOp]) -> Vec<(String, Option<String>)> {
        ops.iter()
            .filter_map(|o| match &o.op {
                WriteOp::AliasFirstWriter { pk, message_id, .. } => {
                    Some((pk.clone(), message_id.clone()))
                }
                WriteOp::Put { .. } | WriteOp::Update { .. } => None,
            })
            .collect()
    }

    fn lookup(candidates: &[&str], found: &[(&str, Option<&str>)]) -> AliasLookup {
        AliasLookup {
            candidates: candidates.iter().map(|c| (*c).to_owned()).collect(),
            found: found
                .iter()
                .map(|(id, message_id)| {
                    (
                        (*id).to_owned(),
                        crate::mail::RfcHit {
                            message_id: message_id.map(str::to_owned),
                            thread_id: "tid-0".to_owned(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Placeholders cover exactly the referenced ids no row maps: never the
    /// message's own id, which gets a real alias instead.
    #[test]
    fn plan_insert_writes_placeholders_only_for_unmapped_references() {
        let msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let thread = new_thread(&msg);
        let aliases = lookup(
            &[
                "mid-1@example.com",
                "parent@example.com",
                "root@example.com",
            ],
            &[("root@example.com", Some("mid-0"))],
        );
        let ops = plan_insert(&msg, None, &thread, &aliases).unwrap();

        assert_eq!(
            alias_writes(&ops),
            vec![
                (
                    "RFC#support@example.com#mid-1@example.com".to_owned(),
                    Some("mid-1".to_owned())
                ),
                (
                    "RFC#support@example.com#parent@example.com".to_owned(),
                    None
                ),
            ]
        );
    }

    #[test]
    fn plan_insert_claims_its_own_placeholder_but_not_another_messages_alias() {
        let msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let thread = new_thread(&msg);

        let placeholder = lookup(&["mid-1@example.com"], &[("mid-1@example.com", None)]);
        let ops = plan_insert(&msg, None, &thread, &placeholder).unwrap();
        assert_eq!(
            alias_writes(&ops),
            vec![(
                "RFC#support@example.com#mid-1@example.com".to_owned(),
                Some("mid-1".to_owned())
            )]
        );

        let taken = lookup(
            &["mid-1@example.com"],
            &[("mid-1@example.com", Some("mid-0"))],
        );
        let ops = plan_insert(&msg, None, &thread, &taken).unwrap();
        assert_eq!(alias_writes(&ops), Vec::new());
    }

    #[test]
    fn plan_insert_skips_an_oversized_placeholder() {
        let msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let thread = new_thread(&msg);
        let oversized = format!("{}@example.com", "x".repeat(ALIAS_ID_MAX_BYTES));
        let aliases = lookup(&[oversized.as_str()], &[]);
        let ops = plan_insert(&msg, None, &thread, &aliases).unwrap();
        assert_eq!(
            alias_writes(&ops),
            vec![(
                "RFC#support@example.com#mid-1@example.com".to_owned(),
                Some("mid-1".to_owned())
            )]
        );
    }

    #[test]
    fn a_placeholder_points_at_the_messages_thread_and_expires_with_it() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        msg.expires_at = 1_900_000_000;
        let thread = new_thread(&msg);
        let aliases = lookup(&["parent@example.com"], &[]);
        let ops = plan_insert(&msg, None, &thread, &aliases).unwrap();
        let placeholder = ops
            .iter()
            .find_map(|o| match &o.op {
                WriteOp::AliasFirstWriter {
                    message_id: None,
                    thread_id,
                    expires_at,
                    ..
                } => Some((thread_id.as_str(), *expires_at)),
                WriteOp::AliasFirstWriter { .. } | WriteOp::Put { .. } | WriteOp::Update { .. } => {
                    None
                }
            })
            .unwrap();
        assert_eq!(placeholder, ("tid-1", msg.expires_at));
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
            crate::mail::send::Envelope {
                to: vec!["recipient@example.com".to_owned()],
                ..crate::mail::send::Envelope::default()
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
                queued.failed(crate::mail::send::SendFailure::Rejected, now),
                false,
            ),
        ] {
            let op = send_state_op(&state, Cond::None).unwrap();
            let WriteOp::Put { item, .. } = op.op else {
                panic!("send state is written as a Put");
            };
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

    /// A thread full of user labels must still take the system labels an
    /// inbound reply needs, or the mail would be lost.
    #[test]
    fn plan_insert_accepts_a_thread_full_of_user_labels() {
        let msg = message(
            "support@example.com",
            "tid-1",
            "mid-1",
            &["received", "spam", "unread"],
        );
        let mut thread = new_thread(&msg);
        for i in 0..THREAD_USER_LABEL_CAP {
            thread.labels.push(format!("label-{i:03}"));
        }
        thread.labels.sort();
        assert!(plan_insert(&msg, None, &thread, &AliasLookup::default()).is_ok());
    }

    #[test]
    fn plan_patch_rejects_user_labels_over_the_thread_cap() {
        let msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let before = new_thread(&msg);
        let mut after = before.clone();
        for i in 0..=THREAD_USER_LABEL_CAP {
            after.labels.push(format!("label-{i:03}"));
        }
        after.labels.sort();
        let err = plan_patch(&msg, &msg.labels, &before, &after, "now").unwrap_err();
        assert!(matches!(err, MailStoreError::LabelLimit(_)));

        // System labels do not count toward the cap.
        after.labels.truncate(THREAD_USER_LABEL_CAP);
        after
            .labels
            .extend(["received".to_owned(), "unread".to_owned()]);
        after.labels.sort();
        assert!(plan_patch(&msg, &msg.labels, &before, &after, "now").is_ok());
    }

    #[test]
    fn plan_patch_rejects_user_labels_over_the_message_cap() {
        let msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        let thread = new_thread(&msg);
        let mut labels: Vec<String> = (0..=MESSAGE_USER_LABEL_CAP)
            .map(|i| format!("label-{i:03}"))
            .collect();
        labels.push("received".to_owned());
        labels.sort();
        let err = plan_patch(&msg, &labels, &thread, &thread, "now").unwrap_err();
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
        second.preview = "Second message body".to_owned();
        let after = apply_message(&before, &second);

        let ops = plan_insert(&second, Some(&before), &after, &AliasLookup::default()).unwrap();
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
        // The snapshot is built from `thread_after` (the apply_message result),
        // so it carries the last-processed message's preview, not the first's.
        assert_eq!(snapshot.preview, after.preview);
        assert_eq!(snapshot.preview, "Second message body");
        assert_eq!(snapshot.last_message_id, "mid-2");
    }

    #[test]
    fn plan_insert_leaves_an_outbound_messages_thread_snapshot_untouched() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["queued"]);
        msg.direction = Direction::Outbound;
        let thread = new_thread(&msg);

        let ops = plan_insert(&msg, None, &thread, &AliasLookup::default()).unwrap();
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
        let ops = plan_insert(&msg, None, &thread, &AliasLookup::default()).unwrap();
        assert!(ops.iter().all(|o| o.role != OpRole::RfcAlias));
    }

    /// A `Message-ID` truncated to `REFERENCES_MAX_BYTES` by `mime::parse_inbound`
    /// is still too long to carry an alias row: `strip_angle_brackets` yields
    /// `REFERENCES_MAX_BYTES - 2` bytes, which exceeds `ALIAS_ID_MAX_BYTES`, so
    /// `push_alias_row` skips it — exactly as before the cap, when the id was even
    /// longer. The fix shrinks the DynamoDB attribute without changing
    /// alias-join behavior.
    #[test]
    fn plan_insert_still_skips_an_alias_for_a_message_id_truncated_to_the_cap() {
        let mut msg = message("support@example.com", "tid-1", "mid-1", &["received"]);
        // Simulate what `mime::parse_inbound` now produces for an oversized id:
        // a `rfc_message_id` of exactly `REFERENCES_MAX_BYTES` bytes (the cap).
        let id_body = "a".repeat(crate::mail::REFERENCES_MAX_BYTES.saturating_sub(2));
        msg.rfc_message_id = format!("<{id_body}>");
        assert_eq!(msg.rfc_message_id.len(), crate::mail::REFERENCES_MAX_BYTES);
        let thread = new_thread(&msg);
        let ops = plan_insert(&msg, None, &thread, &AliasLookup::default()).unwrap();
        assert!(
            ops.iter().all(|o| o.role != OpRole::RfcAlias),
            "an id at the REFERENCES_MAX_BYTES cap is still over ALIAS_ID_MAX_BYTES \
             once stripped, so the alias must still be skipped"
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
            crate::mail::REFERENCES_MAX_BYTES.saturating_sub(2) > ALIAS_ID_MAX_BYTES,
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
