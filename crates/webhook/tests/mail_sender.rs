#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! The sender: claiming a queued send, building it, calling SES once, and
//! recording what happened.
//!
//! The property these are really about is that one queued message produces at
//! most one SES call, whatever the sender is handed.

use aws_messaging_webhook::actions::SendOutcome;
use aws_messaging_webhook::mail::send::{SendState, SendStatus};
use aws_messaging_webhook::mail::sender::{Handled, handle_send};
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, send};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::objects::ObjectFailure;
use webhook_test_support::{Harness, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support";

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

    let handled = handle_send(&h.state, &message_id).await.unwrap();

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

    let first = handle_send(&h.state, &message_id).await.unwrap();
    let second = handle_send(&h.state, &message_id).await.unwrap();

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

    let handled = handle_send(&h.state, &message_id).await.unwrap();
    assert_eq!(handled, Handled::Unknown);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Unknown);

    // A further delivery finds it no longer queued and leaves it alone.
    let again = handle_send(&h.state, &message_id).await.unwrap();
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
}

#[tokio::test]
async fn a_refused_send_is_recorded_rejected_and_not_retried() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Failed {
        reason: "MessageRejected".to_owned(),
    });

    let handled = handle_send(&h.state, &message_id).await.unwrap();

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
}

#[tokio::test]
async fn a_retryable_outcome_hands_the_send_back() {
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    *h.state.services.send_outcome.lock().unwrap() = Some(SendOutcome::Retryable {
        reason: "Throttled".to_owned(),
    });

    let handled = handle_send(&h.state, &message_id).await.unwrap();

    assert_eq!(handled, Handled::Skipped);
    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.transient_failures, 1);
    // `requeued_at` appearing is what re-triggers the sender.
    assert!(state.requeued_at.is_some());

    // The next attempt sends it.
    let handled = handle_send(&h.state, &message_id).await.unwrap();
    assert_eq!(handled, Handled::Sent);
    assert_eq!(h.state.services.sent.lock().unwrap().len(), 2);
}

#[tokio::test]
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
        let handled = handle_send(&h.state, &message_id).await.unwrap();
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

    let handled = handle_send(&h.state, &message_id).await.unwrap();

    assert_eq!(handled, Handled::Failed);
    assert_eq!(state_of(&h, &message_id).send_status, SendStatus::Failed);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_unreachable_object_store_releases_the_claim_for_a_retry() {
    // A transient failure must not strand the send in `sending` with nobody
    // holding it.
    let h = seeded().await;
    let message_id = queued(&h, &body()).await;
    h.state
        .services
        .objects
        .inject(send::spec_key(&message_id), ObjectFailure::Transient);

    let error = handle_send(&h.state, &message_id).await.unwrap_err();
    assert!(format!("{error}").contains("object store"));

    let state = state_of(&h, &message_id);
    assert_eq!(state.send_status, SendStatus::Queued);
    assert!(state.requeued_at.is_some());

    // With the store back, the retry sends it.
    h.state.services.objects.clear(&send::spec_key(&message_id));
    assert_eq!(
        handle_send(&h.state, &message_id).await.unwrap(),
        Handled::Sent
    );
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

    let handled = handle_send(&h.state, &message_id).await.unwrap();

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

    handle_send(&h.state, &message_id).await.unwrap();

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

    let handled = handle_send(&h.state, &message_id).await.unwrap();

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
    handle_send(&h.state, &message_id).await.unwrap();
    handle_send(&h.state, &message_id).await.unwrap();

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

    let handled = handle_send(&h.state, &message_id).await.unwrap();

    assert_eq!(handled, Handled::Failed);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_message_is_skipped() {
    let h = seeded().await;

    let handled = handle_send(&h.state, "no-such-message").await.unwrap();

    assert_eq!(handled, Handled::Skipped);
    assert!(h.state.services.sent.lock().unwrap().is_empty());
}
