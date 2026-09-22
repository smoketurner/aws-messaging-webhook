#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! The sender: claiming a queued send, building it, calling SES once, and
//! recording what happened.
//!
//! The property these are really about is that one queued message produces at
//! most one SES call, whatever the sender is handed.

use aws_messaging_webhook::actions::SendOutcome;
use aws_messaging_webhook::mail::send::{SendFailure, SendState, SendStatus};
use aws_messaging_webhook::mail::sender::{
    Handled, Resolution, handle_send, resolve_unknown, sweep,
};
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, send, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::objects::ObjectFailure;
use webhook_test_support::{Harness, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";

/// A deadline far enough off that no send runs out of time.
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(120)
}

/// Queues one send through the real API and returns its message id.
async fn queued(h: &Harness, body: &Value) -> String {
    let request = Request::post(format!("/v0/inboxes/{INBOX}/messages/send"))
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    value["message_id"].as_str().unwrap().to_owned()
}

/// The labels on a thread, which roll up its messages' labels.
async fn thread_labels(h: &Harness, thread_id: &str) -> Vec<String> {
    h.state
        .services
        .get_thread(&InboxId(INBOX.to_owned()), thread_id, 1, None)
        .await
        .unwrap()
        .unwrap()
        .thread
        .labels
}

async fn seeded() -> Harness {
    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), "2026-01-01T00:00:00.000Z")
        .await
        .unwrap();
    h
}

fn body() -> Value {
    json!({
        "to": "recipient@example.net",
        "subject": "Hello",
        "text": "the body",
    })
}

fn state_of(h: &Harness, message_id: &str) -> SendState {
    let item = h
        .state
        .services
        .mail
        .raw_item(&format!("OUTBOX#{message_id}"), "STATE")
        .unwrap();
    serde_dynamo::from_item(item).unwrap()
}

#[tokio::test]
async fn a_queued_send_reaches_ses_once_and_is_recorded_sent() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Sent);
    {
        let sent = h.state.services.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].to, vec!["recipient@example.net"]);
        assert!(sent[0].raw_text().contains("Subject: Hello"));
    }

    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sent);

    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.labels, vec!["sent"]);
    assert_eq!(thread_labels(&h, &message.thread_id).await, vec!["sent"]);
    // A settled send's state ages out with its message.
    assert!(message.expires_at > 0);
    assert_eq!(
        state_of(&h, &message_id).expires_at,
        Some(message.expires_at)
    );
    assert_eq!(message.send_status.as_deref(), Some("sent"));
    assert_eq!(
        message.ses_message_id.as_deref(),
        Some(format!("ses-{message_id}").as_str())
    );
    assert!(message.sent_at.is_some());
}

#[tokio::test]
async fn a_second_delivery_of_the_same_record_does_not_send_again() {
    // At-most-once lives or dies here: the stream delivers at least once, so
    // the claim has to stop the second pass.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;

    let first = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    let second = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(first, Handled::Sent);
    assert_eq!(second, Handled::Skipped);
    assert_eq!(
        h.state.services.sent.lock().unwrap().len(),
        1,
        "the message must reach SES exactly once"
    );
}

#[tokio::test]
async fn an_ambiguous_outcome_is_never_resent() {
    // SES may already hold the message, so a retry could deliver it twice.
    // The send stops as `unknown` and waits for an operator.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Unknown {
        reason: "connection reset".to_owned(),
    });

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(handled, Handled::Unknown);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Unknown);

    // A further delivery finds it no longer queued and leaves it alone.
    let again = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(again, Handled::Skipped);
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 1);

    // The message keeps its queued label: it is neither sent nor known to
    // have failed, and claiming either would be a statement we cannot make.
    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.labels, vec!["queued"]);
    assert_eq!(message.send_status.as_deref(), Some("unknown"));
    // Nothing may expire a send an operator still has to resolve.
    assert_eq!(state_of(&h, &message_id).expires_at, None);
}

/// SES writes its own `Message-ID` over ours, so a message sent to this inbox
/// arrives back carrying the SES form. The copy joins the sent message's
/// thread rather than starting one, and a reply from anyone else naming that
/// id would do the same.
#[tokio::test]
async fn a_send_to_this_inbox_arrives_back_in_its_thread() {
    let h = seeded().await;
    let message_id = queued(
        &h,
        &json!({
            "to": INBOX.to_owned(),
            "subject": "Note to self",
            "text": "the body",
        }),
    )
    .await;
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    let raw = format!(
        "From: {INBOX}@example.com\r\nTo: {INBOX}@example.com\r\nSubject: Note to self\r\nMessage-ID: <ses-{message_id}@email.amazonses.com>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nthe body\r\n"
    );
    h.fake().objects.seed(
        "inbound/raw/copy",
        axum::body::Bytes::from(raw),
        "message/rfc822",
    );
    let received_at = "2026-01-01T00:05:00.000Z";
    let inner = json!({
        "notificationType": "Received",
        "mail": { "messageId": "inbound-copy", "timestamp": received_at },
        "receipt": {
            "recipients": [INBOX.to_owned()],
            "timestamp": received_at,
            "spamVerdict": { "status": "PASS" },
            "virusVerdict": { "status": "PASS" },
            "spfVerdict": { "status": "PASS" },
            "dkimVerdict": { "status": "PASS" },
            "dmarcVerdict": { "status": "PASS" },
            "action": {
                "type": "S3",
                "bucketName": webhook_test_support::MAIL_BUCKET,
                "objectKey": "inbound/raw/copy",
            },
        },
    });
    let status = webhook_test_support::post(
        h.state.clone(),
        "/webhooks/ses/inbound",
        &webhook_test_support::wrapped(&h, &inner),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let copy_id = ids::inbound_message_id("inbound-copy", time::parse(received_at).unwrap());
    let copy = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &copy_id.to_string())
        .await
        .unwrap()
        .expect("the copy was stored");
    assert_eq!(copy.thread_id, message_id);
    let thread = h
        .state
        .services
        .get_thread(&InboxId(INBOX.to_owned()), &message_id, 10, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread.thread.message_count, 2);
}

#[tokio::test]
async fn a_refused_send_is_recorded_rejected_and_not_retried() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Failed {
        reason: "MessageRejected".to_owned(),
    });

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Failed);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Failed);
    assert!(state.failure.is_some());

    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.labels, vec!["rejected"]);
    assert_eq!(
        thread_labels(&h, &message.thread_id).await,
        vec!["rejected"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_retryable_outcome_hands_the_send_back() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Retryable {
        reason: "Throttled".to_owned(),
    });

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Skipped);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.transient_failures, 1);
    // `requeued_at` appearing is what re-triggers the sender.
    assert!(state.requeued_at.is_some());

    // The next attempt sends it.
    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(handled, Handled::Sent);
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_send_that_keeps_failing_is_eventually_abandoned() {
    // Otherwise a permanently unavailable SES would keep one message
    // circulating forever.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;

    let mut outcomes = 0;
    for _ in 0..8 {
        *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Retryable {
            reason: "Throttled".to_owned(),
        });
        let handled = handle_send(&h.state, &message_id, deadline())
            .await
            .unwrap();
        outcomes += 1;
        if handled == Handled::Failed {
            break;
        }
    }

    assert!(outcomes < 8, "it should give up rather than loop forever");
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Failed);
}

#[tokio::test]
async fn a_send_whose_spec_is_gone_fails_rather_than_looping() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::NotFound);

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Failed);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Failed);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_object_store_hands_the_send_back() {
    // A transient failure must not strand the send in `sending` with nobody
    // holding it. The release re-triggers the sender by itself, so the
    // invocation succeeds rather than also asking for a redelivery, which
    // would double the attempts.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::Transient);

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(handled, Handled::Skipped);

    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.transient_failures, 1);
    assert!(state.requeued_at.is_some());

    // With the store back, the retry sends it.
    h.state.services.objects.clear(&send::spec_key(&message_id));
    assert_eq!(
        handle_send(&h.state, &message_id, deadline())
            .await
            .unwrap(),
        Handled::Sent
    );
}

#[tokio::test(start_paused = true)]
async fn an_object_store_that_stays_unavailable_eventually_fails_the_send() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::Transient);

    let mut attempts = 0;
    loop {
        attempts += 1;
        let handled = handle_send(&h.state, &message_id, deadline())
            .await
            .unwrap();
        if handled == Handled::Failed || attempts == 10 {
            break;
        }
    }

    assert!(attempts < 10, "it should give up rather than loop forever");
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Failed);
    assert_eq!(state.failure, Some(SendFailure::OutboxUnavailable));
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_object_store_refusal_fails_the_send_without_retrying() {
    // Access denied answers the same way every time.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::Permanent);

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Failed);
    let state = state_of(&h, &message_id);
    assert_eq!(state.failure, Some(SendFailure::OutboxUnavailable));
    assert_eq!(state.transient_failures, 0);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_load_that_outlasts_the_deadline_hands_the_send_back_without_sending() {
    // A slow load must not run the invocation out while it holds the claim.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::Hang);

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Skipped);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.transient_failures, 1);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_accepted_send_that_cannot_be_recorded_keeps_its_ses_call_mark() {
    // SES has the message. Leaving the claim marked is what stops the sweep
    // from sending it a second time.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.store_failures_after_send.lock().unwrap() = 100;

    let result = handle_send(&h.state, &message_id, deadline()).await;

    assert!(result.is_err(), "{result:?}");
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 1);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Sending);
    assert!(state.ses_call_at.is_some());
}

#[tokio::test]
async fn the_sweep_marks_a_stale_claim_unknown_once_ses_may_have_been_called() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    let claimed = h
        .state
        .services
        .claim_send(&message_id, "2020-01-01T00:00:00.000Z")
        .await
        .unwrap()
        .unwrap();
    h.state
        .services
        .note_ses_call(&claimed, "2020-01-01T00:00:01.000Z")
        .await
        .unwrap()
        .unwrap();

    let report = sweep(&h.state).await.unwrap();

    assert_eq!(report.marked_unknown, 1);
    assert_eq!(report.released, 0);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Unknown);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_sweep_carries_on_past_a_send_it_cannot_update() {
    let h = seeded().await;
    let first = queued(&h, &body()).await;
    let second = queued(&h, &body()).await;
    for message_id in [&first, &second] {
        h.state
            .services
            .claim_send(message_id, "2020-01-01T00:00:00.000Z")
            .await
            .unwrap();
    }
    h.state
        .services
        .mail
        .inject(webhook_test_support::mail_memory::Injected::Transient);

    let report = sweep(&h.state).await.unwrap();

    assert_eq!(report.errors, 1);
    assert_eq!(report.released, 1);
}

#[tokio::test]
async fn attachments_are_read_from_the_outbox_and_reach_ses() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);
    let message_id = queued(&h, &request).await;

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Sent);
    let sent = h.state.services.sent.lock().unwrap();
    let raw = sent[0].raw_text();
    assert!(raw.contains("multipart/mixed"), "{raw}");
    assert!(raw.contains("notes.txt"), "{raw}");
}

#[tokio::test]
async fn bcc_is_given_to_ses_but_stays_out_of_the_message() {
    let h = seeded().await;
    let mut request = body();
    request["bcc"] = json!(["hidden@example.net"]);
    let message_id = queued(&h, &request).await;

    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    let sent = h.state.services.sent.lock().unwrap();
    assert_eq!(sent[0].bcc, vec!["hidden@example.net"]);
    assert!(
        !sent[0].raw_text().contains("hidden@example.net"),
        "the hidden recipient must not appear in the document"
    );
}

#[tokio::test]
async fn a_url_attachment_is_fetched_and_reaches_ses() {
    let h = seeded().await;
    h.state
        .services
        .serve_url("https://example.com/report.pdf", b"report bytes");
    let mut request = body();
    request["attachments"] = json!([{
        "url": "https://example.com/report.pdf",
        "filename": "report.pdf",
        "content_type": "application/pdf",
    }]);
    let message_id = queued(&h, &request).await;

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Sent);
    let sent = h.state.services.sent.lock().unwrap();
    assert!(
        sent[0].raw_text().contains("report.pdf"),
        "{}",
        sent[0].raw_text()
    );
    assert_eq!(
        h.state.services.fetched.lock().unwrap().as_slice(),
        ["https://example.com/report.pdf"]
    );
}

#[tokio::test]
async fn a_fetched_attachment_is_stored_so_a_retry_does_not_fetch_again() {
    // The URL's content could change between attempts; the message that was
    // built once must stay the message that is sent.
    let h = seeded().await;
    h.state
        .services
        .serve_url("https://example.com/report.pdf", b"report bytes");
    let mut request = body();
    request["attachments"] = json!([{ "url": "https://example.com/report.pdf" }]);
    let message_id = queued(&h, &request).await;

    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Retryable {
        reason: "Throttled".to_owned(),
    });
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(
        h.state.services.fetched.lock().unwrap().len(),
        1,
        "the second attempt should reuse the stored bytes"
    );
}

#[tokio::test]
async fn an_unfetchable_url_fails_the_send_rather_than_sending_without_it() {
    // Going out without the attachment would be worse than not going out.
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{ "url": "https://example.com/missing.pdf" }]);
    let message_id = queued(&h, &request).await;

    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Failed);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_message_is_skipped() {
    let h = seeded().await;

    let handled = handle_send(&h.state, "no-such-message", deadline())
        .await
        .unwrap();

    assert_eq!(handled, Handled::Skipped);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

/// Feeds one SES event for `ses_message_id` through the real webhook path.
async fn ses_event(h: &Harness, kind: &str, ses_message_id: &str) -> StatusCode {
    let inner = json!({
        "notificationType": kind,
        "mail": { "messageId": ses_message_id },
    });
    let body = webhook_test_support::wrapped(h, &inner);
    webhook_test_support::post(h.state.clone(), "/webhooks/ses/events", &body).await
}

#[tokio::test]
async fn an_ses_delivery_event_labels_the_message_it_belongs_to() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    let status = ses_event(&h, "Delivery", &format!("ses-{message_id}")).await;

    assert_eq!(status, StatusCode::OK);
    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        message.labels.contains(&"delivered".to_owned()),
        "{:?}",
        message.labels
    );
    assert!(message.labels.contains(&"sent".to_owned()));
}

#[tokio::test]
async fn events_for_mail_this_service_did_not_send_are_ignored() {
    // Most events on a shared configuration set are for other senders.
    let h = seeded().await;

    let status = ses_event(&h, "Delivery", "ses-someone-else").await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn later_events_add_labels_rather_than_replacing_them() {
    // These arrive out of order under at-least-once delivery, so a message
    // that both bounced and was opened should say both.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    let ses_id = format!("ses-{message_id}");

    ses_event(&h, "Delivery", &ses_id).await;
    ses_event(&h, "Open", &ses_id).await;

    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert!(message.labels.contains(&"delivered".to_owned()));
    assert!(message.labels.contains(&"opened".to_owned()));
}

#[tokio::test]
async fn the_sweep_releases_a_claim_whose_sender_died() {
    // A sender killed mid-send leaves the state `sending` with no stream
    // record to re-trigger it; nothing but the sweep would notice.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .claim_send(&message_id, "2020-01-01T00:00:00.000Z")
        .await
        .unwrap();
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sending);

    let report = sweep(&h.state).await.unwrap();

    assert_eq!(report.released, 1);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert!(state.requeued_at.is_some());
}

#[tokio::test]
async fn the_sweep_leaves_a_recent_claim_alone() {
    // Taking a send away from a sender still working on it is how the same
    // message gets sent twice.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    let now =
        aws_messaging_webhook::mail::time::format(aws_messaging_webhook::mail::time::now_ms());
    h.state
        .services
        .claim_send(&message_id, &now)
        .await
        .unwrap();

    let report = sweep(&h.state).await.unwrap();

    assert_eq!(report.released, 0);
    assert_eq!(report.still_working, 1);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sending);
}

#[tokio::test]
async fn the_sweep_reports_unknown_sends_without_touching_them() {
    // SES may hold these; releasing one could deliver it twice.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Unknown {
        reason: "connection reset".to_owned(),
    });
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    let report = sweep(&h.state).await.unwrap();

    assert_eq!(report.unknown, 1);
    assert_eq!(report.released, 0);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Unknown);
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 1);
}

/// Drives a send to `unknown`, which is the only state an operator resolves.
async fn stuck_unknown(h: &Harness) -> String {
    let message_id = queued(h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Unknown {
        reason: "connection reset".to_owned(),
    });
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(state_of(h, &message_id).send_status, SendStatus::Unknown);
    message_id
}

#[tokio::test]
async fn an_operator_can_send_an_unknown_message_again() {
    let h = seeded().await;
    let message_id = stuck_unknown(&h).await;

    resolve_unknown(&h.state, &message_id, Resolution::Resend)
        .await
        .unwrap();

    // Back in the queue, with the count of failed attempts reset: the
    // operator has looked at it and decided.
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.transient_failures, 0);
    assert!(state.requeued_at.is_some());
    assert!(state.operator_resend_at.is_some());

    // And the sender will now pick it up.
    assert_eq!(
        handle_send(&h.state, &message_id, deadline())
            .await
            .unwrap(),
        Handled::Sent
    );
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn an_operator_can_close_an_unknown_message_as_sent() {
    let h = seeded().await;
    let message_id = stuck_unknown(&h).await;

    resolve_unknown(&h.state, &message_id, Resolution::CloseSent)
        .await
        .unwrap();

    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sent);
    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.labels, vec!["sent"]);
    // Nothing was sent to close it.
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn an_operator_can_close_an_unknown_message_as_failed() {
    let h = seeded().await;
    let message_id = stuck_unknown(&h).await;

    resolve_unknown(&h.state, &message_id, Resolution::CloseFailed)
        .await
        .unwrap();

    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Failed);
    assert_eq!(
        state
            .failure
            .map(aws_messaging_webhook::mail::send::SendFailure::as_str),
        Some("closed_by_operator")
    );
}

#[tokio::test]
async fn only_an_unknown_send_can_be_resolved() {
    // Resolving a send that is still in flight would race the sender holding
    // it, and resolving a finished one would rewrite a settled outcome.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sent);

    let error = resolve_unknown(&h.state, &message_id, Resolution::Resend)
        .await
        .unwrap_err();

    assert!(format!("{error:?}").contains("not unknown"), "{error:?}");
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Sent);
}

#[tokio::test]
async fn a_sent_message_leaves_no_outbox_objects_behind() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{ "content": STANDARD.encode("file bytes") }]);
    let message_id = queued(&h, &request).await;
    assert!(
        h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id))
    );

    handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();

    assert!(
        !h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id)),
        "the spec should be gone once the message has gone out"
    );
    let deleted = h.state.services.objects.delete_object_calls();
    assert!(
        deleted.iter().any(|k| k.contains("/parts/")),
        "the attachment should be removed too: {deleted:?}"
    );
}

#[tokio::test]
async fn a_failed_send_leaves_no_outbox_objects_behind() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{ "content": STANDARD.encode("file bytes") }]);
    let message_id = queued(&h, &request).await;
    assert!(
        h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id))
    );

    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Failed {
        reason: "MessageRejected".to_owned(),
    });
    let handled = handle_send(&h.state, &message_id, deadline())
        .await
        .unwrap();
    assert_eq!(handled, Handled::Failed);

    assert!(
        !h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id)),
        "the spec should be gone once the message has failed permanently"
    );
    let deleted = h.state.services.objects.delete_object_calls();
    assert!(
        deleted.iter().any(|k| k.contains("/parts/")),
        "the attachment should be removed too: {deleted:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_send_abandoned_after_repeated_transient_failures_clears_its_outbox() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{ "content": STANDARD.encode("file bytes") }]);
    let message_id = queued(&h, &request).await;
    assert!(
        h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id))
    );

    let mut attempts = 0;
    let mut handled = Handled::Skipped;
    while handled != Handled::Failed && attempts < 8 {
        attempts += 1;
        *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Retryable {
            reason: "Throttled".to_owned(),
        });
        handled = handle_send(&h.state, &message_id, deadline())
            .await
            .unwrap();
    }
    assert_eq!(
        handled,
        Handled::Failed,
        "it should give up rather than loop forever"
    );

    assert!(
        !h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id)),
        "the spec should be gone once the message has failed permanently"
    );
    let deleted = h.state.services.objects.delete_object_calls();
    assert!(
        deleted.iter().any(|k| k.contains("/parts/")),
        "the attachment should be removed too: {deleted:?}"
    );
}

#[tokio::test]
async fn an_unknown_send_keeps_its_outbox_so_it_can_be_resent() {
    // Clearing these would make an operator resend impossible.
    let h = seeded().await;
    let message_id = stuck_unknown(&h).await;

    assert!(
        h.state
            .services
            .objects
            .contains(&send::spec_key(&message_id)),
        "an unresolved send must keep what it would be rebuilt from"
    );
}
