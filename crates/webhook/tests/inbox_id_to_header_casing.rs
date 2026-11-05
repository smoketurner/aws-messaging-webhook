//! The store-level mechanism behind the casing bug, and the `To:`-header
//! client scenario it traps.
//!
//! DynamoDB partition keys are case-sensitive strings, so `Support@Example.com`
//! and `support@example.com` would address different rows. The fix does not
//! change the keys (it can't — case-sensitivity is DynamoDB's contract); it
//! normalizes the path at the API boundary using `InboxId::new`, the same rule
//! ingest uses, and the key builders take an `InboxId`, so the keys the API
//! builds always match the keys ingest wrote. This suite pins that any casing
//! builds the stored key, and the client scenario end to end.

use aws_messaging_webhook::mail::{InboxId, keys};
use axum::http::StatusCode;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{TEST_INBOX, api, grant_api_access, harness};

/// DynamoDB string equality is case-sensitive, so the partition keys for two
/// casings of the same address would be different rows — not aliases. The key
/// builders take an `InboxId`, so every key is built from the folded form and
/// any casing addresses the row ingest wrote.
#[test]
fn inbox_keys_are_built_from_the_folded_id_whatever_the_casing() {
    assert_eq!(
        keys::inbox(&InboxId::new("Support@Example.com")).pk,
        "INBOX#support@example.com",
    );
    assert_eq!(
        keys::inbox(&InboxId::new("Support@Example.com")),
        keys::inbox(&InboxId::new("support@example.com")),
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
    grant_api_access(&h).await;

    // A message stored under the lowercased id, but whose `to` field carries
    // the verbatim mixed-case header a client would copy.
    let mut msg = sample_message(TEST_INBOX, "mid-1", "mid-1");
    msg.to = vec!["Support@Example.com".to_owned()];
    h.state.mailbox.insert_message(&msg, &[]).await.unwrap();

    // The client copies the `To:` header verbatim into the path. Before the
    // fix: inbox 404, messages `count: 0`. After: both resolve.
    let (status, body) = api::get(&h, "/v0/inboxes/Support@Example.com").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], TEST_INBOX);

    let (status, body) = api::get(&h, "/v0/inboxes/Support@Example.com/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["to"][0], "Support@Example.com");
    assert_eq!(body["messages"][0]["inbox_id"], TEST_INBOX);

    // And the single message fetch + thread follow the same normalization.
    let (status, body) = api::get(&h, "/v0/inboxes/Support@Example.com/messages/mid-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["message_id"], "mid-1");

    let (status, body) = api::get(&h, "/v0/inboxes/Support@Example.com/threads/mid-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["message_id"], "mid-1");
}
