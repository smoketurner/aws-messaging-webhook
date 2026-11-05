//! What the mailbox's writes leave in the table, driven through `Mailbox`
//! over the in-memory store's public API: label caps, the inbound thread
//! snapshot, the enqueue transaction and its idempotency key, the send
//! lifecycle, label patches, list bounds, and the scripted SES-reference read
//! failure. What insertion leaves in the alias rows is tested inline, in
//! `src/alias_tests.rs`, because reading those rows needs crate internals.

use mailbox::send::{Envelope, MarkOutcome, SendKey, SendState, SendStatus, SesSent};
use mailbox::{
    Direction, InboxId, InsertOutcome, MESSAGE_USER_LABEL_CAP, MailMessage, SesMessageId,
    THREAD_USER_LABEL_CAP,
};
use mailbox::{MessageId, ThreadId};
use mailbox_store::{
    EnqueueOutcome, ListQuery, MailMemoryStore, MailStoreError, ReadFailure, sample_message,
};

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

fn user_labels(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("label-{i:03}")).collect()
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
            &MessageId::new("mid-1"),
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
            &MessageId::new("mid-1"),
            &user_labels(THREAD_USER_LABEL_CAP),
            &[],
            NOW,
        )
        .await
        .unwrap();

    let err = mailbox
        .update_labels(
            &inbox(),
            &MessageId::new("mid-2"),
            &["one-more".to_owned()],
            &[],
            NOW,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, MailStoreError::LabelLimit(_)));

    // System labels do not count toward the cap.
    assert!(
        mailbox
            .update_labels(
                &inbox(),
                &MessageId::new("mid-2"),
                &["spam".to_owned()],
                &[],
                NOW
            )
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
            &MessageId::new("mid-1"),
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
        .get_message(&inbox(), &MessageId::new("mid-2"))
        .await
        .unwrap()
        .unwrap();
    let thread = mailbox
        .get_thread(&inbox(), &ThreadId::new("tid-1"), 10, None)
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
        .get_message(&inbox(), &MessageId::new("mid-1"))
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
            .get_message(&inbox(), &MessageId::new("mid-1"))
            .await
            .unwrap()
            .is_some()
    );
    let stored = mailbox
        .get_send_state(&MessageId::new("mid-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.send_status, SendStatus::Queued);

    assert_eq!(
        mailbox
            .enqueue_send(&msg, &state, None, 1_800_000_000)
            .await
            .unwrap(),
        EnqueueOutcome::AlreadyQueued
    );
}

/// Enqueues `msg` and claims its send, returning the claimed state.
async fn claimed(store: &MailMemoryStore, msg: &MailMessage) -> Result<SendState, MailStoreError> {
    let mailbox = store.mailbox();
    mailbox
        .enqueue_send(msg, &queued(msg), None, 1_800_000_000)
        .await?;
    mailbox
        .claim_send(&msg.message_id, NOW)
        .await?
        .ok_or(MailStoreError::NotFound)
}

/// The status index lists a send under its current status only.
#[tokio::test]
async fn a_send_is_listed_under_its_current_status() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let msg = outbound("mid-1");
    mailbox
        .enqueue_send(&msg, &queued(&msg), None, 1_800_000_000)
        .await
        .unwrap();
    let listed = mailbox
        .list_by_status(SendStatus::Queued, 10)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].message_id, msg.message_id);

    mailbox.claim_send(&msg.message_id, NOW).await.unwrap();
    let still_queued = mailbox
        .list_by_status(SendStatus::Queued, 10)
        .await
        .unwrap();
    assert_eq!(still_queued, []);
    assert_eq!(
        mailbox
            .list_by_status(SendStatus::Sending, 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// Recording the SES call writes over the claim it was given, once: the
/// same claim presented again has been moved on and is reported gone.
#[tokio::test]
async fn the_ses_call_is_recorded_only_over_the_claim_it_was_given() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let claim = claimed(&store, &outbound("mid-1")).await.unwrap();

    let noted = mailbox.note_ses_call(&claim, NOW).await.unwrap().unwrap();
    assert_eq!(noted.ses_call_at.as_deref(), Some(NOW));
    assert_eq!(noted.version, claim.version + 1);
    let stored = mailbox
        .get_send_state(&claim.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, noted);

    assert!(mailbox.note_ses_call(&claim, NOW).await.unwrap().is_none());
}

/// A sent mark moves the message's labels and status, rolls the new label
/// up to the thread even when no label is removed, and maps the SES id back
/// to the message.
#[tokio::test]
async fn a_sent_mark_relabels_the_message_and_thread_and_maps_the_ses_id() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let mut msg = outbound("mid-1");
    msg.labels = Vec::new();
    let claim = claimed(&store, &msg).await.unwrap();

    let sent = SesSent {
        message_id: "ses-1",
        region: "us-east-1",
    };
    mailbox
        .mark_send(&claim, MarkOutcome::Sent(sent), NOW)
        .await
        .unwrap();

    let stored = mailbox
        .get_message(&inbox(), &msg.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.labels, ["sent"]);
    assert_eq!(stored.send_status, Some(SendStatus::Sent));
    assert_eq!(stored.ses_message_id, Some(SesMessageId::new("ses-1")));
    assert_eq!(stored.version, msg.version + 1);

    let view = mailbox
        .get_thread(&inbox(), &msg.thread_id, 10, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        view.thread.labels.iter().any(|label| label == "sent"),
        "{:?}",
        view.thread.labels
    );

    assert_eq!(
        mailbox
            .resolve_ses_message(&SesMessageId::new("ses-1"))
            .await
            .unwrap(),
        Some((inbox(), msg.message_id.clone()))
    );
}

/// A label patch is written to the stored message under a new version.
#[tokio::test]
async fn a_label_patch_is_stored_on_the_message() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let msg = message("mid-1", "tid-1");
    mailbox.insert_message(&msg, &[]).await.unwrap();

    let labels = mailbox
        .update_labels(&inbox(), &msg.message_id, &["work".to_owned()], &[], NOW)
        .await
        .unwrap()
        .unwrap();
    let stored = mailbox
        .get_message(&inbox(), &msg.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.labels, labels);
    assert!(stored.labels.iter().any(|label| label == "work"));
    assert_eq!(stored.version, msg.version + 1);
    assert_eq!(stored.updated_at, NOW);
}

fn send_key(msg: &MailMessage, expires_at: u64) -> SendKey {
    SendKey {
        key_hash: "keyhash".to_owned(),
        inbox_id: msg.inbox_id.clone(),
        message_id: msg.message_id.clone(),
        thread_id: msg.thread_id.clone(),
        request_hash: "request".to_owned(),
        route: "send".to_owned(),
        created_at: NOW.to_owned(),
        expires_at,
    }
}

/// An idempotency key is reusable only once it has expired: one expiring
/// at `now` or later still claims its request.
#[tokio::test]
async fn an_idempotency_key_is_reusable_only_after_it_expires() {
    const NOW_EPOCH: u64 = 1_800_000_000;
    for (expires_at, expected) in [
        (NOW_EPOCH - 1, EnqueueOutcome::Committed),
        (NOW_EPOCH, EnqueueOutcome::KeyExists),
        (NOW_EPOCH + 1, EnqueueOutcome::KeyExists),
    ] {
        let store = MailMemoryStore::default();
        let msg = outbound("mid-1");
        store.seed_send_key(&send_key(&msg, expires_at));
        let outcome = store
            .mailbox()
            .enqueue_send(
                &msg,
                &queued(&msg),
                Some(&send_key(&msg, NOW_EPOCH + 100)),
                NOW_EPOCH,
            )
            .await
            .unwrap();
        assert_eq!(outcome, expected, "key expiring at {expires_at}");
    }
}

/// The scripted failure hits the next SES-reference read only: other reads
/// are untouched, and the one after it succeeds.
#[tokio::test]
async fn a_scripted_resolve_failure_hits_only_the_next_ses_reference_read() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    store.fail_next_resolve(ReadFailure::Transient);
    assert!(mailbox.get_inbox(&inbox()).await.unwrap().is_none());
    let ses = SesMessageId::new("ses-1");
    assert!(matches!(
        mailbox.resolve_ses_message(&ses).await,
        Err(MailStoreError::Transient(_))
    ));
    assert!(mailbox.resolve_ses_message(&ses).await.unwrap().is_none());

    store.fail_next_resolve(ReadFailure::Permanent);
    assert!(matches!(
        mailbox.resolve_ses_message(&ses).await,
        Err(MailStoreError::Permanent(_))
    ));
}

fn list(limit: usize, ascending: bool) -> ListQuery {
    ListQuery {
        inbox: inbox(),
        limit,
        before: None,
        after: None,
        ascending,
        start: None,
    }
}

fn ids(page: &[MailMessage]) -> Vec<&str> {
    page.iter().map(|msg| msg.message_id.as_ref()).collect()
}

/// List bounds are exclusive on both sides, and a newest-first page
/// resumes strictly after the last item it returned.
#[tokio::test]
async fn list_bounds_are_exclusive_and_pages_resume_after_their_last_item() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    for id in ["mid-1", "mid-2", "mid-3"] {
        mailbox
            .insert_message(&message(id, &format!("tid-{id}")), &[])
            .await
            .unwrap();
    }

    let after = ListQuery {
        after: Some("mid-1".to_owned()),
        ..list(10, true)
    };
    let page = mailbox.list_messages(&after).await.unwrap();
    assert_eq!(ids(&page.items), ["mid-2", "mid-3"]);

    let before = ListQuery {
        before: Some("mid-3".to_owned()),
        ..list(10, true)
    };
    let page = mailbox.list_messages(&before).await.unwrap();
    assert_eq!(ids(&page.items), ["mid-1", "mid-2"]);

    let mut seen = Vec::new();
    let mut query = list(1, false);
    for _ in 0..3 {
        let page = mailbox.list_messages(&query).await.unwrap();
        seen.extend(page.items.iter().map(|msg| msg.message_id.to_string()));
        query.start = page.next;
    }
    assert_eq!(seen, ["mid-3", "mid-2", "mid-1"]);
}
