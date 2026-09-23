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
const INBOX: &str = "support@example.com";

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

/// Sends a raw body with an optional content type, for the malformed-request
/// cases the typed helper cannot produce.
async fn post_raw(h: &Harness, body: Vec<u8>, content_type: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::post(format!("/v0/inboxes/{INBOX}/messages/send"))
        .header("authorization", format!("Bearer {KEY}"));
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn send_it(h: &Harness, body: &Value, key: Option<&str>) -> (StatusCode, Value) {
    post(h, &format!("/v0/inboxes/{INBOX}/messages/send"), body, key).await
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
    assert_eq!(message.from, INBOX.to_owned());
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
async fn moving_text_between_fields_is_a_different_request() {
    // A fingerprint that runs fields together cannot tell these apart.
    let h = seeded().await;
    let mut first = body();
    first["subject"] = json!("ab");
    first["text"] = json!("c");
    send_it(&h, &first, Some("key-1")).await;

    let mut second = body();
    second["subject"] = json!("a");
    second["text"] = json!("bc");
    let (status, _) = send_it(&h, &second, Some("key-1")).await;

    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn renaming_an_attachment_is_a_different_request() {
    let h = seeded().await;
    let mut first = body();
    first["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "a.txt",
        "content_type": "text/plain",
    }]);
    send_it(&h, &first, Some("key-1")).await;

    let mut second = first.clone();
    second["attachments"][0]["filename"] = json!("b.txt");
    let (status, _) = send_it(&h, &second, Some("key-1")).await;

    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_send_that_fails_to_commit_leaves_no_outbox_uploads_behind() {
    // Nothing under outbox/ expires, so a failed commit must remove the spec
    // and parts it uploaded there. The content document lives under
    // messages/, reclaimed by the bucket's expire-messages lifecycle rule, so
    // discard_uploads leaves it and keeps its deletes scoped to outbox/.
    let h = seeded().await;
    h.state
        .services
        .mail
        .inject(webhook_test_support::mail_memory::Injected::Transient);
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);

    let (status, _) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let objects = &h.state.services.objects;
    let uploaded = objects.put_object_calls();
    assert!(!uploaded.is_empty());

    // Every outbox/ upload is gone: the cleanup is the only thing that reclaims
    // this prefix, so stranding any of it would leak forever.
    for key in uploaded.iter().filter(|k| k.starts_with("outbox/")) {
        assert!(!objects.contains(key), "{key} was left behind");
    }
    // The cleanup only ever reaches into outbox/: touching messages/ would
    // widen the role's delete grant and dodge the lifecycle rule that
    // already reclaims the content document.
    let deleted = objects.delete_object_calls();
    assert!(
        deleted.iter().all(|k| k.starts_with("outbox/")),
        "discard_uploads deleted outside outbox/: {deleted:?}"
    );
    // The content document stays for the expire-messages rule to reclaim.
    let content_key = uploaded
        .iter()
        .find(|k| k.starts_with("messages/"))
        .expect("a send stores a content document");
    assert!(
        objects.contains(content_key),
        "{content_key} should be left for the expire-messages lifecycle rule"
    );
}

#[tokio::test]
async fn the_loser_of_an_idempotency_key_race_cleans_up_its_outbox_uploads() {
    // When two sends race for one Idempotency-Key, the loser's commit is
    // cancelled as KeyExists and discard_uploads must remove what it uploaded
    // under outbox/ — that prefix has no lifecycle rule, so stranding it would
    // leak. The content document under messages/ is left for the
    // expire-messages rule, as on the commit-failure path.
    //
    // The KeyExists cancellation is normally only reachable when two requests
    // overlap; the in-memory store can inject it so a sequential test can
    // exercise the loser's cleanup path.
    let h = seeded().await;
    h.state
        .services
        .mail
        .inject(webhook_test_support::mail_memory::Injected::KeyExists);
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);

    let (status, response) = send_it(&h, &request, Some("key-1")).await;

    // No prior committed key to replay, so the loser answers with the
    // "already in use" conflict.
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["name"], "ConflictError");

    let objects = &h.state.services.objects;
    let uploaded = objects.put_object_calls();
    assert!(!uploaded.is_empty());
    for key in uploaded.iter().filter(|k| k.starts_with("outbox/")) {
        assert!(!objects.contains(key), "{key} was left behind");
    }
    let deleted = objects.delete_object_calls();
    assert!(
        deleted.iter().all(|k| k.starts_with("outbox/")),
        "discard_uploads deleted outside outbox/: {deleted:?}"
    );
    let content_key = uploaded
        .iter()
        .find(|k| k.starts_with("messages/"))
        .expect("a send stores a content document");
    assert!(
        objects.contains(content_key),
        "{content_key} should be left for the expire-messages lifecycle rule"
    );
}

/// When `content::store` fails after `upload_spec` already landed the spec
/// and any inline parts under `outbox/`, the `?` early-return used to skip
/// `discard_uploads`, leaving them orphaned — and `outbox/` has no lifecycle
/// rule. The fix runs `discard_uploads` before the error escapes, so no
/// `outbox/` object survives the failure. The content document under
/// `messages/` was never written (the put failed), and `messages/` is
/// reclaimed by its own lifecycle rule anyway, so it is left untouched.
///
/// One inline attachment makes the put sequence, in order: part(1), spec(2),
/// content-document(3). Failing the 3rd put exercises the cleanup of both the
/// uploaded part and the spec.
#[tokio::test]
async fn a_content_store_failure_discards_the_outbox_uploads() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);
    // Fail the content-document put (the 3rd) — after the spec and part
    // already landed under outbox/.
    h.state.services.objects.inject_nth_put_if_absent(3);

    let (status, response) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    // A transient object failure surfaces as the retryable 502 the contract
    // promises; the body is the generic internal shape (no cause leaked).
    assert_eq!(response["name"], "InternalServerError");

    let objects = &h.state.services.objects;
    let uploaded = objects.put_object_calls();
    assert!(!uploaded.is_empty());

    // Every outbox/ upload (the part and the spec) was reclaimed: the
    // cleanup is the only thing that reclaims this prefix, so stranding
    // any of it would leak forever.
    for key in uploaded.iter().filter(|k| k.starts_with("outbox/")) {
        assert!(!objects.contains(key), "{key} was left behind");
    }
    // discard_uploads ran: it issued a delete for every outbox/ key the spec
    // names, and never reached outside outbox/.
    let deleted = objects.delete_object_calls();
    assert!(
        !deleted.is_empty(),
        "discard_uploads should have issued deletes for the outbox/ uploads"
    );
    assert!(
        deleted.iter().all(|k| k.starts_with("outbox/")),
        "discard_uploads deleted outside outbox/: {deleted:?}"
    );
    // The spec and part keys were both passed to delete_object.
    assert!(
        deleted.iter().any(|k| k.ends_with("/spec.json")),
        "the spec key was not discarded: {deleted:?}"
    );
    assert!(
        deleted.iter().any(|k| k.contains("/parts/")),
        "the inline part key was not discarded: {deleted:?}"
    );
    // The content document under messages/ was never written: its put failed
    // before the store recorded it. (messages/ is also reclaimed by the
    // expire-messages lifecycle rule, so there is nothing to clean there.)
    let content_key = uploaded
        .iter()
        .find(|k| k.starts_with("messages/"))
        .expect("the content-document put was attempted");
    assert!(
        !objects.contains(content_key),
        "{content_key} should not exist — its put failed"
    );
}

/// When `upload_spec` fails partway — the inline parts land, then the spec
/// write (the last put `upload_spec` issues) fails — the `?` early-return used
/// to leave the already-uploaded parts orphaned under `outbox/`. The fix runs
/// `discard_uploads` before the error escapes; `delete_object` treats a
/// missing key as success, so the never-uploaded spec is a no-op delete and
/// the uploaded part is reclaimed.
#[tokio::test]
async fn an_upload_spec_failure_partway_through_discards_the_uploaded_part() {
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode("file bytes"),
        "filename": "notes.txt",
        "content_type": "text/plain",
    }]);
    // Fail the spec write (the 2nd put) — after the inline part already
    // landed under outbox/.../parts/.
    h.state.services.objects.inject_nth_put_if_absent(2);

    let (status, response) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(response["name"], "InternalServerError");

    let objects = &h.state.services.objects;
    let uploaded = objects.put_object_calls();
    assert!(uploaded.iter().any(|k| k.starts_with("outbox/")));
    // The part was attempted (and landed); the spec write was attempted but
    // failed.
    assert!(uploaded.iter().any(|k| k.contains("/parts/")));
    assert!(uploaded.iter().any(|k| k.ends_with("/spec.json")));

    // The leaked part is gone: cleanup reclaimed it. The spec key was never
    // stored (its put failed), so it is trivially absent.
    for key in uploaded.iter().filter(|k| k.starts_with("outbox/")) {
        assert!(!objects.contains(key), "{key} was left behind");
    }
    let deleted = objects.delete_object_calls();
    assert!(
        !deleted.is_empty(),
        "discard_uploads should have issued deletes for the outbox/ uploads"
    );
    assert!(
        deleted.iter().all(|k| k.starts_with("outbox/")),
        "discard_uploads deleted outside outbox/: {deleted:?}"
    );
    assert!(
        deleted.iter().any(|k| k.contains("/parts/")),
        "the inline part key was not discarded: {deleted:?}"
    );
    assert!(
        deleted.iter().any(|k| k.ends_with("/spec.json")),
        "the spec key was not passed to delete_object: {deleted:?}"
    );
    // No content-document put was attempted: content::store is never reached
    // when upload_spec fails first, so messages/ is untouched.
    assert!(
        !uploaded.iter().any(|k| k.starts_with("messages/")),
        "no messages/ put should have run: {uploaded:?}"
    );
}

/// A send with no inline parts still uploads the spec; a `content::store`
/// failure right after it must reclaim that spec. This is the no-attachment
/// variant of the content-store cleanup: the only outbox/ object to leak
/// would be the spec, and `discard_uploads` must remove it.
#[tokio::test]
async fn a_content_store_failure_discards_the_spec_when_there_are_no_parts() {
    let h = seeded().await;
    // No attachments: the put sequence is spec(1), content-document(2).
    h.state.services.objects.inject_nth_put_if_absent(2);

    let (status, _) = send_it(&h, &body(), None).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let objects = &h.state.services.objects;
    let uploaded = objects.put_object_calls();
    assert!(uploaded.iter().any(|k| k.ends_with("/spec.json")));
    for key in uploaded.iter().filter(|k| k.starts_with("outbox/")) {
        assert!(!objects.contains(key), "{key} was left behind");
    }
    let deleted = objects.delete_object_calls();
    assert!(
        deleted.iter().any(|k| k.ends_with("/spec.json")),
        "the spec key was not discarded: {deleted:?}"
    );
    assert!(
        deleted.iter().all(|k| k.starts_with("outbox/")),
        "discard_uploads deleted outside outbox/: {deleted:?}"
    );
}

/// Every failure a client can cause carries the JSON error body, including
/// the ones the framework rejects before a handler runs.
#[tokio::test]
async fn malformed_requests_get_the_json_error_body() {
    let h = seeded().await;

    let (status, response) = post_raw(&h, b"{not json".to_vec(), Some("application/json")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["name"], "ValidationError");
    assert_eq!(response["errors"][0]["path"], "body");

    let (status, response) = post_raw(&h, serde_json::to_vec(&body()).unwrap(), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["name"], "ValidationError");
}

#[tokio::test]
async fn a_send_body_over_the_function_url_limit_is_too_large() {
    let h = seeded().await;
    let (status, body) = post_raw(&h, vec![b' '; 7 * 1024 * 1024], Some("application/json")).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["name"], "PayloadTooLargeError");
}

#[tokio::test]
async fn a_send_body_over_one_mebibyte_is_accepted() {
    // Inline attachments make sends larger than any webhook delivery.
    let h = seeded().await;
    let mut request = body();
    request["attachments"] = json!([{
        "content": STANDARD.encode(vec![b'x'; 2 * 1024 * 1024]),
        "filename": "big.bin",
        "content_type": "application/octet-stream",
    }]);

    let (status, response) = send_it(&h, &request, None).await;

    assert_eq!(status, StatusCode::OK, "{response}");
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

/// A client addressing a mixed-case `{inbox_id}` path still queues a send:
/// the API normalizes the path the same way ingest normalizes the RCPT, so
/// the inbox seeded under the lowercased id resolves (was 404 before the
/// fix).
#[tokio::test]
async fn a_send_addressing_a_mixed_case_inbox_path_is_queued() {
    let h = seeded().await;

    let (status, body) = post(
        &h,
        "/v0/inboxes/Support@Example.com/messages/send",
        &body(),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "mixed-case path should not 404");
    let message_id = body["message_id"].as_str().unwrap().to_owned();
    assert!(!message_id.is_empty());

    // The send lands on the inbox ingest/reads know — the lowercased id.
    let message = h
        .state
        .services
        .get_message(&InboxId(INBOX.to_owned()), &message_id)
        .await
        .unwrap()
        .expect("the queued send is stored under the lowercased id");
    assert_eq!(message.inbox_id, InboxId(INBOX.to_owned()));
}
