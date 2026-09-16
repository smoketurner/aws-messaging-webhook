//! Thread resolution and the versioned read-modify-write state.
//!
//! Reply derivation (`Re:` subjects, `reply_all` exclusion, `ses_rfc_ids`
//! composition) is P3 and added when track K lands.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::mail::{
    AttachmentMeta, Direction, InboxId, MailMessage, THREAD_ATTACHMENT_SUMMARIES, ThreadSnapshot,
};

/// The maximum candidate ids [`candidate_ids`] returns.
const CANDIDATE_ID_CAP: usize = 20;

/// The thread's `senders`/`recipients` string-set cap.
const THREAD_ADDRESS_SET_CAP: usize = 50;

/// The message-embedded [`ThreadSnapshot`]'s `senders`/`recipients` cap
/// lower than [`THREAD_ADDRESS_SET_CAP`], since the snapshot is
/// duplicated onto every message item rather than held once per thread.
const THREAD_SNAPSHOT_ADDRESS_CAP: usize = 20;

/// The thread's total label-union cap, keeping the thread item's label set
/// clear of DynamoDB's 400 KB item limit.
pub const THREAD_LABEL_TOTAL_CAP: usize = 32;

/// The in-memory thread state a read-modify-write cycle computes:
/// read the thread consistently, apply this message's effect in Rust, then
/// `Put` it version-conditioned in the same transaction as the message.
///
/// Mirrors the Thread item's attributes one-to-one, plus `inbox_id` and
/// `thread_id` (redundant with the item's key, same pattern `MailMessage`
/// uses for `inbox_id`/`message_id`), so it round-trips through
/// `serde_dynamo::to_item`/`from_item`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadState {
    pub inbox_id: InboxId,
    pub thread_id: String,
    /// Fixed-width (`mail::time::format`) last-activity timestamp: the sort
    /// key component of the thread index, which is what orders a thread list
    /// by most recent activity.
    pub timestamp: String,
    pub subject: String,
    pub preview: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub senders: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub recipients: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub label_counts: BTreeMap<String, u64>,
    pub last_message_id: String,
    pub message_count: u64,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentMeta>,
    pub version: u64,
    pub created_at: String,
    pub updated_at: String,
}

/// Resolves an `In-Reply-To`/`References` header set to the ordered
/// candidate RFC ids a thread lookup should try, nearest first, capped at 20
/// `In-Reply-To` first, then `References` nearest-to-farthest (i.e.
/// `references` reversed, since RFC 5322 orders `References` oldest-first).
/// Duplicates (the same id appearing in both headers) are kept at their
/// first, nearest occurrence.
#[must_use]
pub fn candidate_ids(in_reply_to: Option<&str>, references: &[String]) -> Vec<String> {
    let mut ids: Vec<String> = Vec::with_capacity(CANDIDATE_ID_CAP);
    if let Some(id) = in_reply_to {
        ids.push(id.to_owned());
    }
    for reference in references.iter().rev() {
        if ids.len() >= CANDIDATE_ID_CAP {
            break;
        }
        if !ids.iter().any(|existing| existing == reference) {
            ids.push(reference.clone());
        }
    }
    ids.truncate(CANDIDATE_ID_CAP);
    ids
}

/// Builds the sorted, deduplicated union of `existing` and `label`,
/// respecting no cap (callers check [`THREAD_LABEL_TOTAL_CAP`] separately).
fn add_label(labels: &mut Vec<String>, label: &str) {
    if let Err(index) = labels.binary_search_by(|l| l.as_str().cmp(label)) {
        labels.insert(index, label.to_owned());
    }
}

/// Appends `value` to `set` (sorted, deduplicated, capped at `cap`): once at
/// cap, a new value is dropped rather than evicting an existing one — the
/// existing senders/recipients stay stable across a long-running thread.
fn add_to_capped_set(set: &mut Vec<String>, value: &str, cap: usize) {
    if set.iter().any(|existing| existing == value) {
        return;
    }
    if set.len() >= cap {
        return;
    }
    if let Err(index) = set.binary_search_by(|v| v.as_str().cmp(value)) {
        set.insert(index, value.to_owned());
    }
}

/// Builds the initial thread state for a thread's first message.
#[must_use]
pub fn new_thread(msg: &MailMessage) -> ThreadState {
    let mut senders = Vec::new();
    add_to_capped_set(&mut senders, &msg.from, THREAD_ADDRESS_SET_CAP);

    let mut recipients = Vec::new();
    for address in msg.to.iter().chain(&msg.cc).chain(&msg.bcc) {
        add_to_capped_set(&mut recipients, address, THREAD_ADDRESS_SET_CAP);
    }

    let mut labels = Vec::new();
    let mut label_counts = BTreeMap::new();
    for label in &msg.labels {
        add_label(&mut labels, label);
        *label_counts.entry(label.clone()).or_insert(0) += 1;
    }

    let attachments: Vec<AttachmentMeta> = msg
        .attachments
        .iter()
        .take(THREAD_ATTACHMENT_SUMMARIES)
        .cloned()
        .collect();

    ThreadState {
        inbox_id: msg.inbox_id.clone(),
        thread_id: msg.thread_id.clone(),
        timestamp: msg.timestamp.clone(),
        subject: msg.subject.clone(),
        preview: msg.preview.clone(),
        senders,
        recipients,
        labels,
        label_counts,
        last_message_id: msg.message_id.clone(),
        message_count: 1,
        size: msg.size,
        received_timestamp: matches!(msg.direction, Direction::Inbound)
            .then(|| msg.timestamp.clone()),
        sent_timestamp: matches!(msg.direction, Direction::Outbound).then(|| msg.timestamp.clone()),
        attachments,
        version: 0,
        created_at: msg.created_at.clone(),
        updated_at: msg.updated_at.clone(),
    }
}

/// Applies `msg`'s effect to `existing`, returning the new thread state
/// (computed in Rust from a consistent read, then `Put` version-checked
/// in the same transaction as the message itself).
///
/// Received/sent timestamps: `received_timestamp` is set once, at the first
/// inbound message, and never moves; `sent_timestamp` tracks the most recent
/// outbound message.
#[must_use]
pub fn apply_message(existing: &ThreadState, msg: &MailMessage) -> ThreadState {
    let mut next = existing.clone();

    add_to_capped_set(&mut next.senders, &msg.from, THREAD_ADDRESS_SET_CAP);
    for address in msg.to.iter().chain(&msg.cc).chain(&msg.bcc) {
        add_to_capped_set(&mut next.recipients, address, THREAD_ADDRESS_SET_CAP);
    }

    for label in &msg.labels {
        add_label(&mut next.labels, label);
        *next.label_counts.entry(label.clone()).or_insert(0) += 1;
    }

    next.last_message_id.clone_from(&msg.message_id);
    next.message_count += 1;
    next.size += msg.size;
    if matches!(msg.direction, Direction::Inbound) && next.received_timestamp.is_none() {
        next.received_timestamp = Some(msg.timestamp.clone());
    }
    if matches!(msg.direction, Direction::Outbound) {
        next.sent_timestamp = Some(msg.timestamp.clone());
    }

    for attachment in &msg.attachments {
        next.attachments.push(attachment.clone());
    }
    while next.attachments.len() > THREAD_ATTACHMENT_SUMMARIES {
        next.attachments.remove(0);
    }

    next.timestamp.clone_from(&msg.timestamp);
    next.version += 1;
    next.updated_at.clone_from(&msg.updated_at);
    next
}

/// Applies one message's label change to its thread.
///
/// `label_counts` is how many of the thread's messages carry each label, so a
/// label leaves the thread's union only when its last carrier gives it up.
///
/// The thread's `timestamp` deliberately does not move: relabelling a message
/// is not new activity, and the timestamp is the thread list's sort key, so
/// touching it would jump the thread to the top of every list.
#[must_use]
pub fn apply_label_patch(
    existing: &ThreadState,
    added: &[String],
    removed: &[String],
    now: &str,
) -> ThreadState {
    let mut next = existing.clone();

    for label in added {
        add_label(&mut next.labels, label);
        *next.label_counts.entry(label.clone()).or_insert(0) += 1;
    }

    for label in removed {
        let Some(count) = next.label_counts.get_mut(label) else {
            continue;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            next.label_counts.remove(label);
            next.labels.retain(|existing| existing != label);
        }
    }

    next.version += 1;
    now.clone_into(&mut next.updated_at);
    next
}

/// Builds the message-embedded snapshot from the thread's full state,
/// capping `senders`/`recipients` at [`THREAD_SNAPSHOT_ADDRESS_CAP`] (lower
/// than the thread's own [`THREAD_ADDRESS_SET_CAP`]).
impl From<&ThreadState> for ThreadSnapshot {
    fn from(thread: &ThreadState) -> Self {
        Self {
            thread_id: thread.thread_id.clone(),
            subject: thread.subject.clone(),
            preview: thread.preview.clone(),
            message_count: thread.message_count,
            labels: thread.labels.clone(),
            senders: thread
                .senders
                .iter()
                .take(THREAD_SNAPSHOT_ADDRESS_CAP)
                .cloned()
                .collect(),
            recipients: thread
                .recipients
                .iter()
                .take(THREAD_SNAPSHOT_ADDRESS_CAP)
                .cloned()
                .collect(),
            timestamp: thread.timestamp.clone(),
            created_at: thread.created_at.clone(),
            updated_at: thread.updated_at.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::mail::{AttachmentMeta, Direction, InboxId};

    fn message(direction: Direction, labels: &[&str]) -> MailMessage {
        MailMessage {
            inbox_id: InboxId("support".to_owned()),
            thread_id: "tid-1".to_owned(),
            message_id: "mid-1".to_owned(),
            ses_message_id: None,
            direction,
            rfc_message_id: "<mid-1@example.com>".to_owned(),
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
    fn candidate_ids_puts_in_reply_to_first_then_references_nearest_first() {
        let refs = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let ids = candidate_ids(Some("reply-to"), &refs);
        assert_eq!(ids, vec!["reply-to", "c", "b", "a"]);
    }

    #[test]
    fn candidate_ids_dedupes_keeping_the_nearest_occurrence() {
        let refs = vec!["a".to_owned(), "b".to_owned()];
        let ids = candidate_ids(Some("b"), &refs);
        assert_eq!(ids, vec!["b", "a"]);
    }

    #[test]
    fn candidate_ids_with_no_headers_is_empty() {
        assert!(candidate_ids(None, &[]).is_empty());
    }

    #[test]
    fn candidate_ids_caps_at_twenty() {
        let refs: Vec<String> = (0..30).map(|i| format!("r{i}")).collect();
        let ids = candidate_ids(None, &refs);
        assert_eq!(ids.len(), 20);
        // Nearest-first: the last reference in the header is the first candidate.
        assert_eq!(ids[0], "r29");
    }

    #[test]
    fn new_thread_seeds_counts_from_the_first_message() {
        let msg = message(Direction::Inbound, &["received", "unread"]);
        let thread = new_thread(&msg);
        assert_eq!(thread.message_count, 1);
        assert_eq!(thread.labels, vec!["received", "unread"]);
        assert_eq!(thread.label_counts["received"], 1);
        assert_eq!(thread.received_timestamp.as_deref(), Some("00000001-0000"));
        assert_eq!(thread.sent_timestamp, None);
        assert_eq!(thread.version, 0);
    }

    #[test]
    fn apply_message_increments_version_and_counts() {
        let first = message(Direction::Inbound, &["received", "unread"]);
        let before = new_thread(&first);
        let mut second = message(Direction::Inbound, &["received", "unread"]);
        second.message_id = "mid-2".to_owned();
        second.from = "other@example.com".to_owned();

        let after = apply_message(&before, &second);
        assert_eq!(after.version, before.version + 1);
        assert_eq!(after.message_count, 2);
        assert_eq!(after.label_counts["received"], 2);
        assert_eq!(after.last_message_id, "mid-2");
        assert_eq!(
            after.senders,
            vec!["other@example.com", "sender@example.com"]
        );
    }

    #[test]
    fn apply_message_never_evicts_an_existing_sender_at_cap() {
        let first = message(Direction::Inbound, &["received"]);
        let mut before = new_thread(&first);
        for i in 0..THREAD_ADDRESS_SET_CAP {
            add_to_capped_set(
                &mut before.senders,
                &format!("s{i}@example.com"),
                THREAD_ADDRESS_SET_CAP,
            );
        }
        assert_eq!(before.senders.len(), THREAD_ADDRESS_SET_CAP);

        let mut next = message(Direction::Inbound, &["received"]);
        next.from = "new-overflow@example.com".to_owned();
        let after = apply_message(&before, &next);
        assert_eq!(after.senders.len(), THREAD_ADDRESS_SET_CAP);
        assert!(
            !after
                .senders
                .contains(&"new-overflow@example.com".to_owned())
        );
    }

    #[test]
    fn apply_message_keeps_only_the_newest_attachments() {
        let mut first = message(Direction::Inbound, &["received"]);
        first.attachments = (0..THREAD_ATTACHMENT_SUMMARIES)
            .map(|i| AttachmentMeta {
                attachment_id: format!("att-{i}"),
                object_key: None,
                size: 10,
                filename: None,
                content_type: "text/plain".to_owned(),
                content_disposition: "attachment".to_owned(),
                content_id: None,
            })
            .collect();
        let before = new_thread(&first);
        assert_eq!(before.attachments.len(), THREAD_ATTACHMENT_SUMMARIES);

        let mut second = message(Direction::Inbound, &["received"]);
        second.message_id = "mid-2".to_owned();
        second.attachments = vec![AttachmentMeta {
            attachment_id: "att-new".to_owned(),
            object_key: None,
            size: 10,
            filename: None,
            content_type: "text/plain".to_owned(),
            content_disposition: "attachment".to_owned(),
            content_id: None,
        }];
        let after = apply_message(&before, &second);
        assert_eq!(after.attachments.len(), THREAD_ATTACHMENT_SUMMARIES);
        assert_eq!(after.attachments.last().unwrap().attachment_id, "att-new");
        assert_eq!(after.attachments.first().unwrap().attachment_id, "att-1");
    }

    #[test]
    fn thread_state_round_trips_through_serde_dynamo() {
        let msg = message(Direction::Inbound, &["received", "unread"]);
        let thread = new_thread(&msg);
        let item: serde_dynamo::Item = serde_dynamo::to_item(&thread).unwrap();
        let back: ThreadState = serde_dynamo::from_item(item).unwrap();
        assert_eq!(back.thread_id, thread.thread_id);
        assert_eq!(back.labels, thread.labels);
        assert_eq!(back.version, thread.version);
        assert_eq!(back.message_count, thread.message_count);
    }

    #[test]
    fn thread_snapshot_from_thread_state_carries_full_fidelity() {
        let first = message(Direction::Inbound, &["received", "unread"]);
        let before = new_thread(&first);
        let mut second = message(Direction::Inbound, &["received", "unread"]);
        second.message_id = "mid-2".to_owned();
        second.from = "other@example.com".to_owned();
        let after = apply_message(&before, &second);

        let snapshot = ThreadSnapshot::from(&after);
        assert_eq!(snapshot.thread_id, after.thread_id);
        assert_eq!(snapshot.subject, after.subject);
        assert_eq!(snapshot.message_count, 2);
        assert_eq!(snapshot.labels, after.labels);
        assert_eq!(snapshot.senders, after.senders);
        assert_eq!(snapshot.recipients, after.recipients);
        assert_eq!(snapshot.timestamp, after.timestamp);
    }

    #[test]
    fn thread_snapshot_caps_senders_and_recipients_at_twenty() {
        let mut thread = new_thread(&message(Direction::Inbound, &["received"]));
        thread.senders = (0..THREAD_ADDRESS_SET_CAP)
            .map(|i| format!("s{i}@example.com"))
            .collect();
        thread.recipients = (0..THREAD_ADDRESS_SET_CAP)
            .map(|i| format!("r{i}@example.com"))
            .collect();

        let snapshot = ThreadSnapshot::from(&thread);
        assert_eq!(snapshot.senders.len(), THREAD_SNAPSHOT_ADDRESS_CAP);
        assert_eq!(snapshot.recipients.len(), THREAD_SNAPSHOT_ADDRESS_CAP);
    }

    #[test]
    fn thread_labels_serialize_as_a_string_set() {
        let msg = message(Direction::Inbound, &["received", "unread"]);
        let thread = new_thread(&msg);
        let item: serde_dynamo::Item = serde_dynamo::to_item(&thread).unwrap();
        assert!(matches!(
            item.inner().get("labels"),
            Some(serde_dynamo::AttributeValue::Ss(_))
        ));
    }

    proptest! {
        #[test]
        fn candidate_ids_chains_that_do_not_intersect_never_merge(
            in_reply_to_a in "[a-z]{1,10}",
            in_reply_to_b in "[a-z]{1,10}",
        ) {
            prop_assume!(in_reply_to_a != in_reply_to_b);
            let a = candidate_ids(Some(&in_reply_to_a), &[]);
            let b = candidate_ids(Some(&in_reply_to_b), &[]);
            prop_assert!(a.iter().all(|id| !b.contains(id)));
        }

        #[test]
        fn candidate_ids_never_exceeds_the_cap(
            in_reply_to in proptest::option::of("[a-z]{1,10}"),
            references in proptest::collection::vec("[a-z0-9]{1,10}", 0..40),
        ) {
            let ids = candidate_ids(in_reply_to.as_deref(), &references);
            prop_assert!(ids.len() <= 20);
        }
    }
}
