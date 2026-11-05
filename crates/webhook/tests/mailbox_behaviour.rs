//! What the mailbox's writes leave in the table, driven through `Mailbox`
//! over the in-memory store: alias claiming and skipping, placeholder
//! expiry, label caps, the inbound thread snapshot, and the enqueue
//! transaction.

use aws_messaging_webhook::mail::keys;
use aws_messaging_webhook::mail::plan::ALIAS_ID_MAX_BYTES;
use aws_messaging_webhook::mail::send::{Envelope, SendState, SendStatus};
use aws_messaging_webhook::mail::store::{EnqueueOutcome, MailStoreError};
use aws_messaging_webhook::mail::{
    Direction, InboxId, InsertOutcome, MESSAGE_USER_LABEL_CAP, MailMessage, REFERENCES_MAX_BYTES,
    THREAD_USER_LABEL_CAP,
};
use serde_dynamo::{AttributeValue, Item};
use webhook_test_support::mail_memory::{MailMemoryStore, sample_message};

const INBOX: &str = "support@example.com";
const NOW: &str = "2026-01-01T00:00:10.000Z";

fn inbox() -> InboxId {
    InboxId::new(INBOX)
}

fn message(message_id: &str, thread_id: &str) -> MailMessage {
    let mut msg = sample_message(INBOX, message_id, thread_id);
    msg.expires_at = 1_900_000_000;
    msg
}

fn alias_row(store: &MailMemoryStore, id: &str) -> Option<Item> {
    store.raw_item(&keys::rfc_alias(&inbox(), id))
}

fn string(item: &Item, name: &str) -> Option<String> {
    match item.inner().get(name) {
        Some(AttributeValue::S(value)) => Some(value.clone()),
        _ => None,
    }
}

fn user_labels(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("label-{i:03}")).collect()
}

/// Placeholders cover exactly the referenced ids no row maps: never the
/// message's own id, which gets a real alias, and never an id another row
/// already maps.
#[tokio::test]
async fn placeholders_are_written_only_for_unmapped_references() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let root = message("mid-0", "tid-0");
    mailbox
        .insert_message(&root, &["mid-0@example.com".to_owned()])
        .await
        .unwrap();

    let reply = message("mid-1", "tid-1");
    mailbox
        .insert_message(
            &reply,
            &[
                "mid-1@example.com".to_owned(),
                "parent@example.com".to_owned(),
                "mid-0@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();

    let own = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&own, "message_id").as_deref(), Some("mid-1"));
    let placeholder = alias_row(&store, "parent@example.com").unwrap();
    assert_eq!(string(&placeholder, "message_id"), None);
    let mapped = alias_row(&store, "mid-0@example.com").unwrap();
    assert_eq!(string(&mapped, "message_id").as_deref(), Some("mid-0"));
}

/// A message takes over the placeholder an earlier reference left for it,
/// but never another message's alias for the same id.
#[tokio::test]
async fn a_message_claims_its_own_placeholder_but_not_another_messages_alias() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let referrer = message("mid-0", "tid-0");
    mailbox
        .insert_message(
            &referrer,
            &[
                "mid-0@example.com".to_owned(),
                "mid-1@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();
    let placeholder = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&placeholder, "message_id"), None);

    let late = message("mid-1", "tid-1");
    mailbox
        .insert_message(&late, &["mid-1@example.com".to_owned()])
        .await
        .unwrap();
    let claimed = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&claimed, "message_id").as_deref(), Some("mid-1"));

    let mut duplicate_id = message("mid-2", "tid-2");
    duplicate_id.rfc_message_id = "<mid-1@example.com>".to_owned();
    mailbox
        .insert_message(&duplicate_id, &["mid-1@example.com".to_owned()])
        .await
        .unwrap();
    let kept = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&kept, "message_id").as_deref(), Some("mid-1"));
}

#[tokio::test]
async fn an_oversized_placeholder_is_skipped() {
    let store = MailMemoryStore::default();
    let oversized = format!("{}@example.com", "x".repeat(ALIAS_ID_MAX_BYTES));
    let msg = message("mid-1", "tid-1");
    store
        .mailbox()
        .insert_message(&msg, &["mid-1@example.com".to_owned(), oversized.clone()])
        .await
        .unwrap();
    assert!(alias_row(&store, &oversized).is_none());
    assert!(alias_row(&store, "mid-1@example.com").is_some());
}

/// A placeholder outliving the message whose reference it records would
/// thread a later reply into a thread that no longer exists.
#[tokio::test]
async fn a_placeholder_points_at_the_messages_thread_and_expires_with_it() {
    let store = MailMemoryStore::default();
    let msg = message("mid-1", "tid-1");
    store
        .mailbox()
        .insert_message(
            &msg,
            &[
                "mid-1@example.com".to_owned(),
                "parent@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();
    let placeholder = alias_row(&store, "parent@example.com").unwrap();
    assert_eq!(string(&placeholder, "thread_id").as_deref(), Some("tid-1"));
    assert_eq!(
        placeholder.inner().get("expires_at"),
        Some(&AttributeValue::N(msg.expires_at.to_string()))
    );
}

/// A thread full of user labels must still take the system labels an
/// inbound reply needs, or the mail would be lost.
#[tokio::test]
async fn an_inbound_reply_lands_in_a_thread_full_of_user_labels() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let first = message("mid-1", "tid-1");
    mailbox.insert_message(&first, &[]).await.unwrap();
    mailbox
        .update_labels(
            &inbox(),
            "mid-1",
            &user_labels(THREAD_USER_LABEL_CAP),
            &[],
            NOW,
        )
        .await
        .unwrap();

    let mut reply = message("mid-2", "tid-1");
    reply.labels = vec![
        "received".to_owned(),
        "spam".to_owned(),
        "unread".to_owned(),
    ];
    reply.timestamp = "00000002-0000".to_owned();
    assert_eq!(
        mailbox.insert_message(&reply, &[]).await.unwrap(),
        InsertOutcome::Fresh
    );
}

#[tokio::test]
async fn a_label_patch_over_the_thread_cap_is_refused_but_system_labels_are_not() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    mailbox
        .insert_message(&message("mid-1", "tid-1"), &[])
        .await
        .unwrap();
    let mut second = message("mid-2", "tid-1");
    second.timestamp = "00000002-0000".to_owned();
    mailbox.insert_message(&second, &[]).await.unwrap();
    mailbox
        .update_labels(
            &inbox(),
            "mid-1",
            &user_labels(THREAD_USER_LABEL_CAP),
            &[],
            NOW,
        )
        .await
        .unwrap();

    let err = mailbox
        .update_labels(&inbox(), "mid-2", &["one-more".to_owned()], &[], NOW)
        .await
        .unwrap_err();
    assert!(matches!(err, MailStoreError::LabelLimit(_)));

    // System labels do not count toward the cap.
    assert!(
        mailbox
            .update_labels(&inbox(), "mid-2", &["spam".to_owned()], &[], NOW)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_label_patch_over_the_message_cap_is_refused() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    mailbox
        .insert_message(&message("mid-1", "tid-1"), &[])
        .await
        .unwrap();
    let err = mailbox
        .update_labels(
            &inbox(),
            "mid-1",
            &user_labels(MESSAGE_USER_LABEL_CAP + 1),
            &[],
            NOW,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, MailStoreError::LabelLimit(_)));
}

/// The snapshot is the thread as of this message's arrival, built from the
/// thread state that already includes it.
#[tokio::test]
async fn an_inbound_message_stores_the_thread_snapshot_as_of_its_arrival() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    mailbox
        .insert_message(&message("mid-1", "tid-1"), &[])
        .await
        .unwrap();
    let mut second = message("mid-2", "tid-1");
    second.timestamp = "00000002-0000".to_owned();
    second.from = "other@example.com".to_owned();
    second.preview = "Second message body".to_owned();
    mailbox.insert_message(&second, &[]).await.unwrap();

    let stored = mailbox
        .get_message(&inbox(), "mid-2")
        .await
        .unwrap()
        .unwrap();
    let thread = mailbox
        .get_thread(&inbox(), "tid-1", 10, None)
        .await
        .unwrap()
        .unwrap()
        .thread;
    let snapshot = stored
        .thread_snapshot
        .expect("an inbound insert always carries a snapshot");
    assert_eq!(snapshot.thread_id, "tid-1");
    assert_eq!(snapshot.message_count, 2);
    assert_eq!(snapshot.senders, thread.senders);
    assert_eq!(snapshot.recipients, thread.recipients);
    assert_eq!(snapshot.preview, "Second message body");
    assert_eq!(snapshot.last_message_id, "mid-2");
}

fn queued(msg: &MailMessage) -> SendState {
    SendState::queued(
        msg.inbox_id.clone(),
        msg.message_id.clone(),
        msg.thread_id.clone(),
        Envelope {
            to: vec!["recipient@example.com".to_owned()],
            ..Envelope::default()
        },
        None,
        "2026-01-01T00:00:00.000Z",
    )
}

fn outbound(message_id: &str) -> MailMessage {
    let mut msg = message(message_id, "tid-1");
    msg.direction = Direction::Outbound;
    msg.labels = vec!["queued".to_owned()];
    msg
}

#[tokio::test]
async fn an_outbound_message_carries_no_thread_snapshot() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let msg = outbound("mid-1");
    mailbox
        .enqueue_send(&msg, &queued(&msg), None, 1_800_000_000)
        .await
        .unwrap();
    let stored = mailbox
        .get_message(&inbox(), "mid-1")
        .await
        .unwrap()
        .unwrap();
    assert!(stored.thread_snapshot.is_none());
}

/// A queued message without its state item would never be claimed, so both
/// land together, and both only where nothing exists yet.
#[tokio::test]
async fn an_enqueue_commits_the_message_and_its_send_state_together() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let msg = outbound("mid-1");
    let state = queued(&msg);
    assert_eq!(
        mailbox
            .enqueue_send(&msg, &state, None, 1_800_000_000)
            .await
            .unwrap(),
        EnqueueOutcome::Committed
    );
    assert!(
        mailbox
            .get_message(&inbox(), "mid-1")
            .await
            .unwrap()
            .is_some()
    );
    let stored = mailbox.get_send_state("mid-1").await.unwrap().unwrap();
    assert_eq!(stored.send_status, SendStatus::Queued);

    assert_eq!(
        mailbox
            .enqueue_send(&msg, &state, None, 1_800_000_000)
            .await
            .unwrap(),
        EnqueueOutcome::AlreadyQueued
    );
}

#[tokio::test]
async fn an_oversized_message_id_gets_no_alias() {
    let store = MailMemoryStore::default();
    let mut msg = message("mid-1", "tid-1");
    let id = format!("{}@example.com", "x".repeat(ALIAS_ID_MAX_BYTES + 10));
    msg.rfc_message_id = format!("<{id}>");
    store.mailbox().insert_message(&msg, &[]).await.unwrap();
    assert!(alias_row(&store, &id).is_none());
}

/// A `Message-ID` truncated to `REFERENCES_MAX_BYTES` by `mime::parse_inbound`
/// is still too long to carry an alias row once its angle brackets are
/// stripped, so truncating never changes which ids carry one.
#[tokio::test]
async fn a_message_id_truncated_to_the_cap_still_gets_no_alias() {
    let store = MailMemoryStore::default();
    let mut msg = message("mid-1", "tid-1");
    let id = "a".repeat(REFERENCES_MAX_BYTES.saturating_sub(2));
    msg.rfc_message_id = format!("<{id}>");
    assert_eq!(msg.rfc_message_id.len(), REFERENCES_MAX_BYTES);
    store.mailbox().insert_message(&msg, &[]).await.unwrap();
    assert!(alias_row(&store, &id).is_none());
}
