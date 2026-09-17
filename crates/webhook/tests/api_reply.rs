#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! `POST …/messages/{id}/reply`: what a reply inherits from the message it
//! answers.

use aws_messaging_webhook::mail::content::{self, MessageContent};
use aws_messaging_webhook::mail::send::SendSpec;
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, MailMessage, ids, send, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support";
const AT: &str = "2026-01-01T09:00:00.000Z";

async fn post(h: &Harness, path: &str, body: &Value) -> (StatusCode, Value) {
    let request = Request::post(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
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

/// Seeds one inbound message to reply to, letting the caller shape it.
/// Stores the original message's content document, as ingest would have.
async fn store_content(h: &Harness, message_id: &str, message_content: MessageContent) {
    content::store(
        &h.state.services,
        &InboxId(INBOX.to_owned()),
        message_id,
        &message_content,
    )
    .await
    .unwrap();
}

async fn seeded(adjust: impl FnOnce(&mut MailMessage)) -> (Harness, String) {
    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(
            &InboxId(INBOX.to_owned()),
            &format!("{INBOX}@example.com"),
            AT,
        )
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "thread-1");
    AT.clone_into(&mut msg.timestamp);
    "customer@example.net".clone_into(&mut msg.from);
    msg.to = vec![format!("{INBOX}@example.com")];
    "Order 42".clone_into(&mut msg.subject);
    "<original@example.net>".clone_into(&mut msg.rfc_message_id);
    adjust(&mut msg);
    h.state.services.insert_message(&msg).await.unwrap();
    (h, id)
}

fn spec(h: &Harness, message_id: &str) -> SendSpec {
    let bytes = h
        .state
        .services
        .objects
        .get(&send::spec_key(message_id))
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn body() -> Value {
    json!({ "text": "thanks for getting in touch" })
}

#[tokio::test]
async fn a_reply_joins_the_original_thread_and_addresses_its_sender() {
    let (h, original_id) = seeded(|_| {}).await;

    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["thread_id"], "thread-1");

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec!["customer@example.net"]);
    assert_eq!(spec.subject, "Re: Order 42");
    assert_eq!(spec.in_reply_to.as_deref(), Some("<original@example.net>"));
    assert_eq!(spec.references, vec!["<original@example.net>"]);
}

#[tokio::test]
async fn an_existing_re_prefix_is_not_repeated() {
    let (h, original_id) = seeded(|m| m.subject = "Re: Order 42".to_owned()).await;

    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.subject, "Re: Order 42");
}

#[tokio::test]
async fn reply_to_on_the_original_wins_over_its_sender() {
    // A sender that asked for replies elsewhere gets them there.
    let (h, original_id) = seeded(|_| {}).await;
    store_content(
        &h,
        &original_id,
        MessageContent {
            reply_to: vec!["desk@example.net".to_owned()],
            ..MessageContent::default()
        },
    )
    .await;

    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec!["desk@example.net"]);
}

#[tokio::test]
async fn reply_all_copies_the_others_but_never_this_inbox() {
    // Replying to all must not send the message back to the inbox that sent
    // it, which would loop.
    let (h, original_id) = seeded(|m| {
        m.to = vec![
            format!("{INBOX}@example.com"),
            "colleague@example.net".to_owned(),
        ];
        m.cc = vec!["watcher@example.net".to_owned()];
    })
    .await;

    let mut request = body();
    request["reply_all"] = json!(true);
    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &request,
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec!["customer@example.net"]);
    assert_eq!(
        spec.envelope.cc,
        vec!["colleague@example.net", "watcher@example.net"]
    );
    assert!(
        !spec.envelope.cc.iter().any(|a| a.starts_with(INBOX)),
        "this inbox must not be copied on its own reply"
    );
}

#[tokio::test]
async fn without_reply_all_only_the_sender_is_addressed() {
    let (h, original_id) = seeded(|m| {
        m.cc = vec!["watcher@example.net".to_owned()];
    })
    .await;

    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec!["customer@example.net"]);
    assert!(spec.envelope.cc.is_empty());
}

#[tokio::test]
async fn an_explicit_recipient_overrides_the_derived_one() {
    let (h, original_id) = seeded(|_| {}).await;

    let mut request = body();
    request["to"] = json!("someone-else@example.net");
    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &request,
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec!["someone-else@example.net"]);
    // The threading still comes from the original.
    assert_eq!(spec.in_reply_to.as_deref(), Some("<original@example.net>"));
}

#[tokio::test]
async fn the_references_chain_grows_from_the_original() {
    let (h, original_id) = seeded(|_| {}).await;
    store_content(
        &h,
        &original_id,
        MessageContent {
            references: vec!["<first@example.net>".to_owned()],
            ..MessageContent::default()
        },
    )
    .await;

    let (_, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(
        spec.references,
        vec!["<first@example.net>", "<original@example.net>"]
    );
}

#[tokio::test]
async fn replying_to_an_unknown_message_is_not_found() {
    let (h, _) = seeded(|_| {}).await;

    let (status, _) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/nope/reply"),
        &body(),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_reply_without_a_body_is_rejected() {
    let (h, original_id) = seeded(|_| {}).await;

    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &json!({}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["name"], "ValidationError");
}

#[tokio::test]
async fn a_reply_shows_up_in_the_original_thread() {
    let (h, original_id) = seeded(|_| {}).await;
    post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{original_id}/reply"),
        &body(),
    )
    .await;

    let request = Request::get(format!("/v0/inboxes/{INBOX}/threads/thread-1"))
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
    let thread: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(thread["count"], 2);
    assert_eq!(thread["message_count"], 2);
}
