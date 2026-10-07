#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! Replying to a message whose inbound `From` (or `To`/`Cc`/`Reply-To`)
//! display name is large enough that the `Name <address>` rendering would
//! exceed `ADDRESS_MAX_BYTES` (320).
//!
//! `format_addr` used to truncate the **combined** rendering from the left,
//! which dropped the `<address>` (or its closing `>` or `@`) once the display
//! name was large enough, leaving a stored `from` that `mail-parser` could
//! not read back as a mailbox. The reply default recipients are derived from
//! those stored display strings (`reply.rs::reply_recipients`), which then
//! failed `envelope_address` / the `@` check in `validate.rs`, and
//! `POST …/messages/{id}/reply` returned HTTP 400 `ValidationError` without
//! queuing anything.
//!
//! The fix truncates the **display name** with the address held whole, so
//! the stored value always round-trips. These tests pin that behavior at
//! both the helper layer (`parse_inbound` → `validate`) and the HTTP layer
//! (`POST …/reply`).

use aws_messaging_webhook::api::send::validate::{Addresses, SendRequest, validate};
use aws_messaging_webhook::mail::content::MessageContent;
use aws_messaging_webhook::mail::mime::parse_inbound;
use aws_messaging_webhook::mail::send::SendSpec;
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, send, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";
const AT: &str = "2026-01-01T09:00:00.000Z";
const ADDRESS: &str = "alice@example.com";

/// Builds the raw inbound message whose `From` display name overflows the
/// 320-byte `ADDRESS_MAX_BYTES` cap, then parses it the way ingest does.
fn oversized_from_message() -> (String, String) {
    let name = "X".repeat(400);
    let raw = format!(
        "From: {name} <{ADDRESS}>\r\nTo: {INBOX}\r\nSubject: oversized sender\r\n\
         Message-ID: <big-from-1@example.com>\r\nMIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n"
    );
    let parsed = parse_inbound(raw.as_bytes()).unwrap();
    (raw, parsed.message.from.clone())
}

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

fn spec(h: &Harness, message_id: &str) -> SendSpec {
    let bytes = h
        .state
        .services
        .objects
        .get(&send::spec_key(message_id))
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// A request body the reply accepts: a body but no explicit recipients, so
/// the reply defaults them from the original message.
fn body() -> Value {
    json!({ "text": "thanks for getting in touch" })
}

/// A minimal send request carrying one `to`, used to drive the public
/// `validate` against a stored display string.
fn minimal_send(to: String) -> SendRequest {
    SendRequest {
        to: Some(Addresses::One(to)),
        cc: None,
        bcc: None,
        reply_to: None,
        subject: Some("Hello".to_owned()),
        text: Some("body".to_owned()),
        html: None,
        labels: None,
        headers: None,
        attachments: None,
    }
}

/// The stored `from` for a `From` whose display name overflows the cap keeps
/// the full `<address>` and stays within `ADDRESS_MAX_BYTES`, and the public
/// `validate` (which re-parses with `mail-parser` and checks the `@`)
/// reduces it back to the bare mailbox — the round-trip the reply path
/// relies on. Before the fix the stored `from` was 320 `X`s with no `<`, `>`
/// or `@`, and this round-trip returned `Err(ApiError::Validation)`.
#[test]
fn parse_inbound_oversized_display_name_preserves_the_round_trippable_mailbox() {
    let (_, stored_from) = oversized_from_message();

    assert!(
        stored_from.len() <= aws_messaging_webhook::mail::ADDRESS_MAX_BYTES,
        "stored `from` must respect the storage cap: {} bytes",
        stored_from.len()
    );
    // The mailbox survives truncation — the address, the wrapping `<>`, and
    // the `@` are all present, so `mail-parser` can read it back.
    assert!(
        stored_from.ends_with(&format!(" <{ADDRESS}>")),
        "stored `from` should end with `<{ADDRESS}>`, got {stored_from:?}"
    );
    assert!(stored_from.contains('@'));
    assert!(!stored_from.contains('\n'));

    // The exact logic the reply endpoint applies: feed the stored display
    // string through `validate`, which calls `envelope_address` and the `@`
    // check. The fix means it now reduces to the bare mailbox.
    let validated = validate(minimal_send(stored_from)).expect(
        "the stored `from` must round-trip through `validate` so the reply \
         is accepted",
    );
    assert_eq!(validated.to, vec![ADDRESS]);
}

/// End-to-end: a reply to a message whose inbound `From` display name
/// overflowed the cap is queued (HTTP 200) and dresses the envelope with
/// the original sender's mailbox. Before the fix the reply returned HTTP
/// 400 `ValidationError` (rejected on the `to` field) and never queued.
#[tokio::test]
async fn reply_to_message_with_an_oversized_sender_is_queued() {
    let (_, stored_from) = oversized_from_message();
    // Sanity: the seeded `from` is exactly what the inbound pipeline would
    // have stored, the value the fix has to keep round-trippable.
    assert!(stored_from.ends_with(&format!(" <{ADDRESS}>")));

    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), AT)
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-big-from-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "thread-big-from");
    AT.clone_into(&mut msg.timestamp);
    msg.from = stored_from;
    msg.to = vec![INBOX.to_owned()];
    "Oversized sender".clone_into(&mut msg.subject);
    "<original-big-from@example.com>".clone_into(&mut msg.rfc_message_id);
    h.state.services.insert_message(&msg).await.unwrap();

    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/reply"),
        &body(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "response body: {response}");
    assert_eq!(response["thread_id"], "thread-big-from");
    let message_id = response["message_id"].as_str().unwrap();
    assert_ne!(message_id, "");

    let spec = spec(&h, message_id);
    // The envelope — what SES actually delivers to — carries the recovered
    // mailbox, not the truncated display string.
    assert_eq!(spec.envelope.to, vec![ADDRESS]);
    assert_eq!(spec.subject, "Re: Oversized sender");
    assert_eq!(
        spec.in_reply_to.as_deref(),
        Some("<original-big-from@example.com>")
    );
}

/// A non-oversized `From` with a display name still round-trips: the fix
/// did not regress the happy path exercised by `tests/api_reply.rs` and the
/// existing `plain_text_message` fixture.
#[tokio::test]
async fn reply_to_message_with_a_normal_sender_still_round_trips() {
    let raw = format!(
        "From: Alice Sender <{ADDRESS}>\r\nTo: {INBOX}\r\nSubject: hi\r\n\
         Message-ID: <normal-from-1@example.com>\r\nMIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n"
    );
    let parsed = parse_inbound(raw.as_bytes()).unwrap();
    let stored_from = parsed.message.from.clone();
    assert_eq!(stored_from, format!("Alice Sender <{ADDRESS}>"));
    let validated = validate(minimal_send(stored_from.clone())).unwrap();
    assert_eq!(validated.to, vec![ADDRESS]);

    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), AT)
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-normal-from-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "thread-normal-from");
    AT.clone_into(&mut msg.timestamp);
    msg.from = stored_from;
    msg.to = vec![INBOX.to_owned()];
    "<original-normal-from@example.com>".clone_into(&mut msg.rfc_message_id);
    h.state.services.insert_message(&msg).await.unwrap();

    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/reply"),
        &body(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "response body: {response}");
    let spec = spec(&h, response["message_id"].as_str().unwrap());
    assert_eq!(spec.envelope.to, vec![ADDRESS]);
}

/// Reply-to, when it overflows the same way, also round-trips: the reply
/// defaults to `Reply-To` over `From` (`reply.rs::reply_recipients`), and the
/// fix applies to every address list `parse_inbound` stores, not just `From`.
#[tokio::test]
async fn reply_to_an_oversized_reply_to_header_goes_to_its_mailbox() {
    let name = "X".repeat(400);
    let raw = format!(
        "From: sender@example.net\r\nTo: {INBOX}\r\n\
         Reply-To: {name} <{ADDRESS}>\r\nSubject: oversized reply-to\r\n\
         Message-ID: <big-reply-to-1@example.com>\r\nMIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n"
    );
    let parsed = parse_inbound(raw.as_bytes()).unwrap();
    let stored_reply_to = parsed.content.reply_to.clone();
    assert_eq!(stored_reply_to.len(), 1);
    assert!(
        stored_reply_to[0].ends_with(&format!(" <{ADDRESS}>")),
        "got {:?}",
        stored_reply_to[0]
    );

    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), AT)
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-big-reply-to-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "thread-big-reply-to");
    AT.clone_into(&mut msg.timestamp);
    "sender@example.net".clone_into(&mut msg.from);
    msg.to = vec![INBOX.to_owned()];
    msg.subject = "Oversized reply-to".to_owned();
    "<original-big-reply-to@example.com>".clone_into(&mut msg.rfc_message_id);

    // Store the content document so the reply can read the original's
    // `Reply-To`, the way ingest would have.
    aws_messaging_webhook::mail::content::store(
        &h.state.services,
        &InboxId(INBOX.to_owned()),
        &id,
        &MessageContent {
            reply_to: stored_reply_to,
            ..MessageContent::default()
        },
    )
    .await
    .unwrap();
    h.state.services.insert_message(&msg).await.unwrap();

    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/reply"),
        &body(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "response body: {response}");
    let spec = spec(&h, response["message_id"].as_str().unwrap());
    // `Reply-To` wins over `From`: the reply goes to alice@example.com.
    assert_eq!(spec.envelope.to, vec![ADDRESS]);
}

/// An oversized `To` (or `Cc`) participant is included when `reply_all` is
/// set — the fix keeps that participant's mailbox round-trippable too.
#[tokio::test]
async fn reply_all_includes_an_oversized_cc_participant() {
    let cc_name = "Y".repeat(400);
    let raw = format!(
        "From: sender@example.net\r\nTo: {INBOX}\r\n\
         Cc: {cc_name} <cara@example.com>\r\nSubject: oversized cc\r\n\
         Message-ID: <big-cc-1@example.com>\r\nMIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n"
    );
    let parsed = parse_inbound(raw.as_bytes()).unwrap();
    let stored_cc = parsed.message.cc.clone();
    assert_eq!(stored_cc.len(), 1);
    assert!(
        stored_cc[0].ends_with(" <cara@example.com>"),
        "got {:?}",
        stored_cc[0]
    );

    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), AT)
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-big-cc-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "thread-big-cc");
    AT.clone_into(&mut msg.timestamp);
    "sender@example.net".clone_into(&mut msg.from);
    msg.to = vec![INBOX.to_owned()];
    msg.cc = stored_cc;
    msg.subject = "Oversized cc".to_owned();
    "<original-big-cc@example.com>".clone_into(&mut msg.rfc_message_id);
    h.state.services.insert_message(&msg).await.unwrap();

    let mut request = body();
    request["reply_all"] = json!(true);
    let (status, response) = post(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/reply"),
        &request,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "response body: {response}");
    let spec = spec(&h, response["message_id"].as_str().unwrap());
    // The reply addresses the sender; `reply_all` also copies the oversized
    // `Cc` participant, whose mailbox the fix preserved.
    assert_eq!(spec.envelope.to, vec!["sender@example.net"]);
    assert!(spec.envelope.cc.contains(&"cara@example.com".to_owned()));
}
