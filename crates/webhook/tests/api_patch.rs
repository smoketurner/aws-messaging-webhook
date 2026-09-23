#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! `PATCH …/messages/{id}`: label changes, and what they do to the message's
//! thread.

use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";

async fn send(h: &Harness, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"));
    let body = match body {
        Some(json) => {
            request = request.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&json).unwrap())
        }
        None => Body::empty(),
    };
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request.body(body).unwrap())
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

async fn patch(h: &Harness, message_id: &str, body: Value) -> (StatusCode, Value) {
    send(
        h,
        "PATCH",
        &format!("/v0/inboxes/{INBOX}/messages/{message_id}"),
        Some(body),
    )
    .await
}

/// Seeds a harness with one inbox and returns the ids of the messages it
/// inserted into `thread`, oldest first.
async fn seeded(thread: &str, count: usize) -> (Harness, Vec<String>) {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), "2026-01-01T00:00:00.000Z")
        .await
        .unwrap();

    let mut ids = Vec::new();
    for index in 0..count {
        let at = format!("2026-01-01T{:02}:00:00.000Z", 9 + index);
        let ms = time::parse(&at).unwrap();
        let id = ids::inbound_message_id(&format!("ses-{at}"), ms).to_string();
        let mut msg = sample_message(INBOX, &id, thread);
        msg.timestamp.clone_from(&at);
        msg.created_at.clone_from(&at);
        msg.updated_at.clone_from(&at);
        h.state.services.insert_message(&msg).await.unwrap();
        ids.push(id);
    }
    (h, ids)
}

#[tokio::test]
async fn removing_unread_is_how_a_client_marks_mail_read() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(&h, &ids[0], json!({"remove_labels": ["unread"]})).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["message_id"], ids[0]);
    assert_eq!(body["labels"], json!(["received"]));

    // The change is durable, not just reflected in the response.
    let (_, message) = send(
        &h,
        "GET",
        &format!("/v0/inboxes/{INBOX}/messages/{}", ids[0]),
        None,
    )
    .await;
    assert_eq!(message["labels"], json!(["received"]));
}

#[tokio::test]
async fn a_bare_string_is_accepted_as_well_as_a_list() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(&h, &ids[0], json!({"add_labels": "urgent"})).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["labels"], json!(["received", "unread", "urgent"]));
}

#[tokio::test]
async fn labels_are_normalized_before_they_are_stored() {
    let (h, ids) = seeded("t1", 1).await;

    let (_, body) = patch(&h, &ids[0], json!({"add_labels": ["  Urgent ", "URGENT"]})).await;

    assert_eq!(body["labels"], json!(["received", "unread", "urgent"]));
}

#[tokio::test]
async fn a_thread_keeps_a_label_until_its_last_carrier_gives_it_up() {
    // The thread's union is reference-counted: two unread messages, and the
    // thread stays unread until both are read.
    let (h, ids) = seeded("t1", 2).await;

    patch(&h, &ids[0], json!({"remove_labels": ["unread"]})).await;
    let (_, thread) = send(&h, "GET", &format!("/v0/inboxes/{INBOX}/threads/t1"), None).await;
    assert!(
        thread["labels"]
            .as_array()
            .unwrap()
            .contains(&json!("unread")),
        "one message is still unread, so the thread is"
    );

    patch(&h, &ids[1], json!({"remove_labels": ["unread"]})).await;
    let (_, thread) = send(&h, "GET", &format!("/v0/inboxes/{INBOX}/threads/t1"), None).await;
    assert!(
        !thread["labels"]
            .as_array()
            .unwrap()
            .contains(&json!("unread")),
        "the last unread message was read, so the thread is not unread"
    );
}

#[tokio::test]
async fn relabelling_does_not_move_the_thread_in_the_list() {
    // The thread list sorts by last activity, and a label edit is not
    // activity: t2 must stay on top.
    let (h, ids) = seeded("t1", 1).await;
    let at = "2026-01-01T15:00:00.000Z";
    let ms = time::parse(at).unwrap();
    let id = ids::inbound_message_id("ses-later", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "t2");
    msg.timestamp = at.to_owned();
    msg.created_at = at.to_owned();
    msg.updated_at = at.to_owned();
    h.state.services.insert_message(&msg).await.unwrap();

    patch(&h, &ids[0], json!({"add_labels": ["urgent"]})).await;

    let (_, list) = send(&h, "GET", &format!("/v0/inboxes/{INBOX}/threads"), None).await;
    assert_eq!(list["threads"][0]["thread_id"], "t2");
}

#[tokio::test]
async fn adding_a_label_makes_the_message_findable_by_it() {
    let (h, ids) = seeded("t1", 1).await;
    patch(&h, &ids[0], json!({"add_labels": ["invoices"]})).await;

    let (_, list) = send(
        &h,
        "GET",
        &format!("/v0/inboxes/{INBOX}/messages?labels=invoices"),
        None,
    )
    .await;

    assert_eq!(list["count"], 1);
    assert_eq!(list["messages"][0]["message_id"], ids[0]);
}

/// Labels a request would push past the thread's cap are the caller's to
/// fix, so they are a 400, not a 500 that invites retrying forever.
#[tokio::test]
async fn labels_past_the_thread_cap_are_a_validation_error() {
    let (h, ids) = seeded("t1", 2).await;
    let first: Vec<String> = (0..15).map(|i| format!("first-{i:02}")).collect();
    let second: Vec<String> = (0..6).map(|i| format!("second-{i:02}")).collect();

    let (status, _) = patch(&h, &ids[0], json!({ "add_labels": first })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = patch(&h, &ids[1], json!({ "add_labels": second })).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["name"], "ValidationError");
    assert_eq!(body["errors"][0]["path"], "labels");
}

/// A thread full of user labels still takes the system labels the pipeline
/// applies, such as a delivery outcome.
#[tokio::test]
async fn a_thread_full_of_user_labels_still_takes_system_labels() {
    let (h, ids) = seeded("t1", 1).await;
    let full: Vec<String> = (0..20).map(|i| format!("mine-{i:02}")).collect();
    let (status, _) = patch(&h, &ids[0], json!({ "add_labels": full })).await;
    assert_eq!(status, StatusCode::OK);

    let labels = h
        .state
        .services
        .update_labels(
            &InboxId(INBOX.to_owned()),
            &ids[0],
            &["bounced".to_owned()],
            &[],
            "2026-01-02T00:00:00.000Z",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(labels.contains(&"bounced".to_owned()));
}

/// A request may name a full set of a caller's own labels plus a state toggle
/// like `spam`: the request cap counts the caller's own labels, not the
/// toggle, mirroring the storage cap that would accept the same state.
/// Before the fix this returned a 400 because the request cap counted the
/// toggle.
#[tokio::test]
async fn a_full_set_of_user_labels_with_a_system_toggle_is_accepted() {
    let (h, ids) = seeded("t1", 1).await;
    let mut labels: Vec<String> = (0..20).map(|i| format!("u{i:02}")).collect();
    labels.push("spam".to_owned());

    let (status, body) = patch(&h, &ids[0], json!({ "add_labels": labels })).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = body["labels"].as_array().unwrap();
    // received + unread (seeded) + spam toggle + 20 user labels.
    assert_eq!(stored.len(), 23, "{body}");
    assert!(
        stored.contains(&json!("spam")),
        "spam toggle was applied: {body}"
    );
    assert!(stored.contains(&json!("u00")), "{body}");
    assert!(stored.contains(&json!("u19")), "{body}");

    // The change is durable, not just reflected in the response.
    let (_, message) = send(
        &h,
        "GET",
        &format!("/v0/inboxes/{INBOX}/messages/{}", ids[0]),
        None,
    )
    .await;
    assert_eq!(message["labels"].as_array().unwrap().len(), 23);
}

/// The cap a request still enforces is on a caller's own labels: naming more
/// than 20 of those in one request is still a 400, with or without a toggle.
#[tokio::test]
async fn more_than_20_user_labels_in_one_request_is_still_rejected() {
    let (h, ids) = seeded("t1", 1).await;
    let labels: Vec<String> = (0..21).map(|i| format!("u{i:02}")).collect();

    let (status, body) = patch(&h, &ids[0], json!({ "add_labels": labels })).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["name"], "ValidationError");
    assert_eq!(body["errors"][0]["path"], "add_labels");
    assert!(
        body["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("at most 20 labels per request"),
        "{body}"
    );
}

#[tokio::test]
async fn service_owned_labels_are_rejected() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(&h, &ids[0], json!({"add_labels": ["received"]})).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["name"], "ValidationError");
    assert_eq!(body["errors"][0]["path"], "add_labels");
}

#[tokio::test]
async fn adding_and_removing_the_same_label_is_rejected() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(
        &h,
        &ids[0],
        json!({"add_labels": ["urgent"], "remove_labels": ["urgent"]}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["name"], "ValidationError");
}

#[tokio::test]
async fn an_empty_request_leaves_the_labels_alone() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(&h, &ids[0], json!({})).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["labels"], json!(["received", "unread"]));
}

#[tokio::test]
async fn removing_a_label_the_message_does_not_have_is_a_no_op() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = patch(&h, &ids[0], json!({"remove_labels": ["nonexistent"]})).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["labels"], json!(["received", "unread"]));
}

#[tokio::test]
async fn patching_an_unknown_message_is_not_found() {
    let (h, _) = seeded("t1", 1).await;

    let (status, body) = patch(&h, "nope", json!({"add_labels": ["urgent"]})).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["name"], "NotFoundError");
}

#[tokio::test]
async fn patching_requires_a_key() {
    let (h, ids) = seeded("t1", 1).await;
    let request = Request::builder()
        .method("PATCH")
        .uri(format!("/v0/inboxes/{INBOX}/messages/{}", ids[0]))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"add_labels":["urgent"]}"#))
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// A patch addressed through a mixed-case `{inbox_id}` path still reaches the
/// message: the API normalizes the path, so the inbox the message lives under
/// resolves (was 404 before the fix).
#[tokio::test]
async fn a_patch_addressing_a_mixed_case_inbox_path_updates_labels() {
    let (h, ids) = seeded("t1", 1).await;

    let (status, body) = send(
        &h,
        "PATCH",
        &format!("/v0/inboxes/Support@Example.com/messages/{}", ids[0]),
        Some(json!({"remove_labels": ["unread"]})),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "mixed-case path should not 404");
    assert_eq!(body["message_id"], ids[0]);
    assert_eq!(body["labels"], json!(["received"]));
}
