#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! `POST …/messages/send`: what a queued send leaves behind, and what an
//! `Idempotency-Key` does on a replay.
//!
//! The response means the send is durably queued, not that SES has accepted
//! it — the sender does that from the stream.

use aws_messaging_webhook::mail::send::{SendSpec, SendState, SendStatus};
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, content, send};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::{Harness, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support";

async fn post(
    h: &Harness,
    path: &str,
    body: &Value,
    idempotency_key: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::post(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json");
    if let Some(key) = idempotency_key {
        request = request.header("idempotency-key", key);
    }
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(
            request
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn send_it(h: &Harness, body: &Value, key: Option<&str>) -> (StatusCode, Value) {
    post(h, &format!("/v0/inboxes/{INBOX}/messages/send"), body, key).await
}

async fn seeded() -> Harness {
    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(
            &InboxId(INBOX.to_owned()),
            &format!("{INBOX}@example.com"),
            "2026-01-01T00:00:00.000Z",
        )
        .await
        .unwrap();
    h
}

fn body() -> Value {
    json!({
        "to": "recipient@example.com",
        "subject": "Hello",
        "text": "the body",
    })
}

/// The spec the sender will build from.
fn spec(h: &Harness, message_id: &str) -> SendSpec {
    let bytes = h
        .state
        .services
        .objects
        .get(&send::spec_key(message_id))
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn a_send_is_queued_with_its_state_spec_and_message() {
    let h = seeded().await;

    let (status, body) = send_it(&h, &body(), None).await;

    assert_eq!(status, StatusCode::OK);
    let message_id = body["message_id"].as_str().unwrap().to_owned();
    let thread_id = body["thread_id"].as_str().unwrap().to_owned();
    assert!(!message_id.is_empty());

    // The message is readable straight away, labelled queued.
    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.labels, vec!["queued"]);
    assert_eq!(message.send_status.as_deref(), Some("queued"));
    assert_eq!(message.to, vec!["recipient@example.com"]);
    assert_eq!(message.from, format!("{INBOX}@example.com"));
    assert_eq!(message.thread_id, thread_id);

    // The spec carries the real envelope for the sender.
    let spec = spec(&h, &message_id);
    assert_eq!(spec.envelope.to, vec!["recipient@example.com"]);
    assert_eq!(spec.subject, "Hello");
    assert_eq!(spec.text.as_deref(), Some("the body"));

    // The body is stored in the message's content document, and the item
    // carries a TTL.
    let message_content = content::load(&h.state.services, &message).await.unwrap();
    assert_eq!(message_content.text.as_deref(), Some("the body"));
    assert!(message.expires_at > 0);
}

#[tokio::test]
async fn the_send_state_starts_queued_and_holds_the_envelope() {
    let h = seeded().await;
    let (_, body) = send_it(&h, &body(), None).await;
    let message_id = body["message_id"].as_str().unwrap();

    let item = h
        .state
        .services
        .mail
        .raw_item(&format!("OUTBOX#{message_id}"), "STATE")
        .unwrap();
    let state: SendState = serde_dynamo::from_item(item).unwrap();

    assert_eq!(state.send_status, SendStatus::Queued);
    assert_eq!(state.version, 0);
    assert_eq!(state.envelope.to, vec!["recipient@example.com"]);
    assert!(state.sending_at.is_none());
}

#[tokio::test]
async fn bcc_reaches_the_envelope_but_never_a_header() {
    // Bcc has to be delivered to and never rendered, so it must be on the
    // state item the sender reads.
    let h = seeded().await;
    let mut request = body();
    request["bcc"] = json!(["hidden@example.com"]);

    let (_, response) = send_it(&h, &request, None).await;
    let message_id = response["message_id"].as_str().unwrap();

    let spec = spec(&h, message_id);
    assert_eq!(spec.envelope.bcc, vec!["hidden@example.com"]);
    assert!(!spec.headers.contains_key("Bcc"));
}

#[tokio::test]
async fn an_inline_attachment_is_uploaded_before_the_spec_names_it() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);

    let (status, response) = send_it(&h, &request, None).await;
    assert_eq!(status, StatusCode::OK);
    let message_id = response["message_id"].as_str().unwrap();

    let spec = spec(&h, message_id);
    assert_eq!(spec.attachments.len(), 1);
    let attachment = &spec.attachments[0];
    assert!(attachment.attachment_id.starts_with("att_"));
    assert_eq!(attachment.size, 10);
    assert_eq!(attachment.url, None);

    // The bytes are already in the outbox under the key the spec names.
    let key = attachment.object_key.as_deref().unwrap();
    assert_eq!(
        h.state.services.objects.get(key).as_deref(),
        Some(&b"file bytes"[..])
    );
}

#[tokio::test]
async fn a_url_attachment_is_left_for_the_sender_to_fetch() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "url": "https://example.com/report.pdf",
        "filename": "report.pdf",
        "content_type": "application/pdf",
    }]);

    let (status, response) = send_it(&h, &request, None).await;
    assert_eq!(status, StatusCode::OK);

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    let attachment = &spec.attachments[0];
    assert_eq!(
        attachment.url.as_deref(),
        Some("https://example.com/report.pdf")
    );
    // Nothing is uploaded for it, and its size is unknown until fetched.
    assert_eq!(attachment.object_key, None);
    assert_eq!(attachment.size, 0);
}

#[tokio::test]
async fn the_same_key_and_request_replays_the_original_ids() {
    let h = seeded().await;

    let (first_status, first) = send_it(&h, &body(), Some("key-1")).await;
    let (second_status, second) = send_it(&h, &body(), Some("key-1")).await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(first["message_id"], second["message_id"]);
    assert_eq!(first["thread_id"], second["thread_id"]);
}

#[tokio::test]
async fn the_same_key_with_a_different_request_is_a_conflict() {
    // Otherwise a client that changed its mind mid-retry would send
    // something it never asked for under an id it thinks it knows.
    let h = seeded().await;
    send_it(&h, &body(), Some("key-1")).await;

    let mut changed = body();
    changed["subject"] = json!("Something else");
    let (status, response) = send_it(&h, &changed, Some("key-1")).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["name"], "ConflictError");
}

#[tokio::test]
async fn changing_only_an_attachment_still_conflicts() {
    // The fingerprint covers attachment bytes, not just the visible fields.
    let h = seeded().await;
    let mut first = body();
    first["attachments"] = json!([{ "content": STANDARD.encode("one") }]);
    send_it(&h, &first, Some("key-1")).await;

    let mut second = body();
    second["attachments"] = json!([{ "content": STANDARD.encode("two") }]);
    let (status, _) = send_it(&h, &second, Some("key-1")).await;

    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn two_sends_without_a_key_are_two_messages() {
    let h = seeded().await;

    let (_, first) = send_it(&h, &body(), None).await;
    let (_, second) = send_it(&h, &body(), None).await;

    assert_ne!(first["message_id"], second["message_id"]);
}

#[tokio::test]
async fn an_invalid_request_queues_nothing() {
    let h = seeded().await;
    let request = json!({ "subject": "no recipient and no body" });

    let (status, response) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["name"], "ValidationError");
    // Nothing was uploaded on the way to the rejection.
    assert!(h.state.services.objects.put_object_calls().is_empty());
}

#[tokio::test]
async fn a_private_attachment_url_is_refused() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{ "url": "https://169.254.169.254/latest/meta-data/" }]);

    let (status, response) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["errors"][0]["path"], "attachments[0]");
}

#[tokio::test]
async fn sending_from_an_unknown_inbox_is_not_found() {
    let h = seeded().await;

    let (status, _) = post(&h, "/v0/inboxes/nobody/messages/send", &body(), None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sending_requires_a_key() {
    let h = seeded().await;
    let request = Request::post(format!("/v0/inboxes/{INBOX}/messages/send"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body()).unwrap()))
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_queued_send_appears_in_the_inbox_listing() {
    let h = seeded().await;
    send_it(&h, &body(), None).await;

    let request = Request::get(format!("/v0/inboxes/{INBOX}/messages"))
        .header("authorization", format!("Bearer {KEY}"))
        .body(Body::empty())
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(list["count"], 1);
    assert_eq!(list["messages"][0]["labels"], json!(["queued"]));
}
