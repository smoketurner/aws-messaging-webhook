#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
//! Isolates the read-side fix from the ingest path: seeds the store under the
//! lowercased id the service always writes (ingest lowercases the RCPT), then
//! drives the real axum router with every casing variant of `{inbox_id}`.
//!
//! Before the fix, every variant other than the exact lowercased id 404'd the
//! inbox and silently emptied the messages list (`count: 0`); only the exact
//! lowercased form the service wrote was accepted. After the fix, the API
//! applies the same `trim().to_ascii_lowercase()` canonicalization as ingest,
//! so all variants address the same row.

use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";
const AT: &str = "2026-01-01T09:00:00.000Z";

async fn get(h: &Harness, path: &str) -> (StatusCode, Value) {
    let request = Request::get(path)
        .header("authorization", format!("Bearer {KEY}"))
        .body(Body::empty())
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

/// Seeds the store the way ingest does: an inbox and one message, both under
/// the lowercased id `support@example.com`.
async fn seeded() -> (Harness, String) {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), "2026-01-01T00:00:00.000Z")
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-casing-router", ms).to_string();
    let mut msg = sample_message(INBOX, &id, &id);
    AT.clone_into(&mut msg.timestamp);
    AT.clone_into(&mut msg.created_at);
    AT.clone_into(&mut msg.updated_at);
    h.state.services.insert_message(&msg).await.unwrap();
    (h, id)
}

/// Every spelling a client might carry when its first knowledge of the
/// address is the original casing from an out-of-band source. The whitespace
/// variant is URL-encoded so axum decodes it back to surrounding spaces before
/// the handler trims it.
const CASING_VARIANTS: &[&str] = &[
    "Support@Example.com",
    "Support@example.com",
    "support@Example.com",
    "SUPPORT@EXAMPLE.COM",
    "sUpPoRt@eXaMpLe.CoM",
    "%20Support@Example.com%20",
];

#[tokio::test]
async fn every_casing_variant_resolves_to_the_lowercased_inbox() {
    let (h, _id) = seeded().await;

    for variant in CASING_VARIANTS {
        let (status, body) = get(&h, &format!("/v0/inboxes/{variant}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "`{variant}` should resolve to the inbox (not 404)"
        );
        assert_eq!(
            body["inbox_id"], INBOX,
            "`{variant}` should canonicalize to the lowercased id"
        );
    }

    // The exact lowercased id the service publishes is still accepted.
    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], INBOX);
}

#[tokio::test]
async fn a_genuinely_unknown_inbox_is_still_not_found() {
    // Normalization must not paper over an id the service never stored.
    let (h, _id) = seeded().await;

    let (status, _) = get(&h, "/v0/inboxes/Nobody@Example.com").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = get(&h, "/v0/inboxes/Nobody@Example.com/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0, "a genuinely empty inbox reads as empty");
}
