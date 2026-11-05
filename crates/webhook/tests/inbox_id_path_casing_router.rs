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

use aws_messaging_webhook::mail::{ids, time};
use axum::http::StatusCode;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, TEST_AT, TEST_INBOX, api, grant_api_access, harness};

/// Seeds the store the way ingest does: an inbox and one message, both under
/// the lowercased id `support@example.com`.
async fn seeded() -> (Harness, String) {
    let h = harness().await;
    grant_api_access(&h).await;

    let ms = time::parse(TEST_AT).unwrap();
    let id = ids::inbound_message_id("ses-casing-router", ms).to_string();
    let mut msg = sample_message(TEST_INBOX, &id, &id);
    TEST_AT.clone_into(&mut msg.timestamp);
    TEST_AT.clone_into(&mut msg.created_at);
    TEST_AT.clone_into(&mut msg.updated_at);
    h.state.mailbox().insert_message(&msg, &[]).await.unwrap();
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
        let (status, body) = api::get(&h, &format!("/v0/inboxes/{variant}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "`{variant}` should resolve to the inbox (not 404)"
        );
        assert_eq!(
            body["inbox_id"], TEST_INBOX,
            "`{variant}` should canonicalize to the lowercased id"
        );
    }

    // The exact lowercased id the service publishes is still accepted.
    let (status, body) = api::get(&h, &format!("/v0/inboxes/{TEST_INBOX}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], TEST_INBOX);
}

/// The tuple extractors on the nested routes normalize the inbox segment
/// just as the single-segment route does.
#[tokio::test]
async fn every_casing_variant_resolves_a_message_path() {
    let (h, id) = seeded().await;

    for variant in CASING_VARIANTS {
        let (status, body) = api::get(&h, &format!("/v0/inboxes/{variant}/messages/{id}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "`{variant}` should resolve the message (not 404)"
        );
        assert_eq!(body["inbox_id"], TEST_INBOX);
        assert_eq!(body["message_id"], id.as_str());
    }
}

#[tokio::test]
async fn a_genuinely_unknown_inbox_is_still_not_found() {
    // Normalization must not paper over an id the service never stored.
    let (h, _id) = seeded().await;

    let (status, _) = api::get(&h, "/v0/inboxes/Nobody@Example.com").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = api::get(&h, "/v0/inboxes/Nobody@Example.com/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0, "a genuinely empty inbox reads as empty");
}
