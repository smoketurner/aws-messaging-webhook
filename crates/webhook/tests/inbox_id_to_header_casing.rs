#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
//! The store-level mechanism behind the casing bug, and the `To:`-header
//! client scenario it traps.
//!
//! DynamoDB partition keys are case-sensitive strings: `inbox_pk` does not fold
//! case, so `Support@Example.com` and `support@example.com` address different
//! rows. The fix does not change the keys (it can't — case-sensitivity is
//! DynamoDB's contract); it normalizes the path at the API boundary using
//! `InboxId::from_path`, the same rule ingest uses, so the keys the API
//! builds always match the keys ingest wrote. This suite pins both halves:
//! the keys still differ by casing (why the API must normalize), and
//! `from_path` folds any casing to the stored key (so the API does).

use aws_messaging_webhook::mail::keys;
use aws_messaging_webhook::mail::{InboxId, store::MailStore as _};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";

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

/// DynamoDB string equality is case-sensitive, so the partition keys for two
/// casings of the same address are different rows — not aliases. This is why
/// the read API has to apply the same canonicalization ingest applies; it
/// cannot rely on the store to fold case.
#[test]
fn inbox_pk_does_not_fold_case_so_casing_is_significant_in_the_key() {
    assert_ne!(
        keys::inbox_pk("Support@Example.com"),
        keys::inbox_pk("support@example.com"),
        "distinct casings must address distinct rows"
    );
    assert_eq!(
        keys::inbox_pk("support@example.com"),
        keys::inbox_pk("support@example.com"),
    );
}

/// The client scenario the report names: a caller knows only the verbatim
/// `To:` header of an inbound message (`Support@Example.com`) and uses it as
/// the `{inbox_id}` path. Before the fix it could not see its mail (the inbox
/// 404'd and the messages list came back as a misleading `count: 0`); after
/// the fix the API normalizes the path, so the read resolves.
#[tokio::test]
async fn a_client_using_the_to_header_as_the_inbox_path_can_see_its_mail() {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), "2026-01-01T00:00:00.000Z")
        .await
        .unwrap();

    // A message stored under the lowercased id, but whose `to` field carries
    // the verbatim mixed-case header a client would copy.
    let mut msg = sample_message(INBOX, "mid-1", "mid-1");
    msg.to = vec!["Support@Example.com".to_owned()];
    h.state.services.insert_message(&msg).await.unwrap();

    // The client copies the `To:` header verbatim into the path. Before the
    // fix: inbox 404, messages `count: 0`. After: both resolve.
    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], INBOX);

    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["to"][0], "Support@Example.com");
    assert_eq!(body["messages"][0]["inbox_id"], INBOX);

    // And the single message fetch + thread follow the same normalization.
    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com/messages/mid-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["message_id"], "mid-1");

    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com/threads/mid-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["message_id"], "mid-1");
}
