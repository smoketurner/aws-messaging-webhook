#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! The presigned download routes: raw MIME and attachments.

use aws_messaging_webhook::mail::AttachmentMeta;
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support";
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

fn attachment(id: &str, filename: Option<&str>, key: Option<&str>) -> AttachmentMeta {
    AttachmentMeta {
        attachment_id: id.to_owned(),
        object_key: key.map(ToOwned::to_owned),
        size: 2_048,
        filename: filename.map(ToOwned::to_owned),
        content_type: "application/pdf".to_owned(),
        content_disposition: "attachment".to_owned(),
        content_id: None,
    }
}

/// Seeds one message, letting the caller shape it, and returns its id.
async fn seeded(
    adjust: impl FnOnce(&mut aws_messaging_webhook::mail::MailMessage),
) -> (Harness, String) {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(&InboxId(INBOX.to_owned()), AT)
        .await
        .unwrap();

    let ms = time::parse(AT).unwrap();
    let id = ids::inbound_message_id("ses-1", ms).to_string();
    let mut msg = sample_message(INBOX, &id, "t1");
    AT.clone_into(&mut msg.timestamp);
    adjust(&mut msg);
    h.state.services.insert_message(&msg).await.unwrap();
    (h, id)
}

#[tokio::test]
async fn raw_returns_a_presigned_url_for_the_stored_mime() {
    let (h, id) = seeded(|m| m.raw_s3_key = Some("inbound/raw-1".to_owned())).await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages/{id}/raw")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["message_id"], id);
    assert_eq!(body["size"], 1_000);
    assert_eq!(body["content_type"], "message/rfc822");
    let url = body["download_url"].as_str().unwrap();
    assert!(url.contains("inbound/raw-1"), "{url}");
    assert!(
        url.contains("response-content-type=message%2Frfc822"),
        "{url}"
    );
    assert!(body["expires_at"].as_str().unwrap() > AT);
}

#[tokio::test]
async fn raw_is_not_found_when_the_message_has_no_stored_object() {
    let (h, id) = seeded(|m| m.raw_s3_key = None).await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages/{id}/raw")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["name"], "NotFoundError");
}

#[tokio::test]
async fn an_attachment_returns_its_metadata_with_the_url() {
    let (h, id) = seeded(|m| {
        m.attachments = vec![attachment(
            "att_1",
            Some("invoice.pdf"),
            Some("attachments/a-1"),
        )];
    })
    .await;

    let (status, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/attachments/att_1"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["attachment_id"], "att_1");
    assert_eq!(body["filename"], "invoice.pdf");
    assert_eq!(body["content_type"], "application/pdf");
    assert_eq!(body["content_disposition"], "attachment");
    assert_eq!(body["size"], 2_048);
    let url = body["download_url"].as_str().unwrap();
    assert!(url.contains("attachments/a-1"), "{url}");
    assert!(url.contains("invoice.pdf"), "{url}");
}

#[tokio::test]
async fn a_hostile_filename_cannot_inject_into_the_signed_headers() {
    // The filename comes from mail an arbitrary sender wrote. It is signed
    // into the URL and echoed back by S3 as a response header, so a quote or
    // a newline reaching it intact would be a header-injection bug.
    let (h, id) = seeded(|m| {
        m.attachments = vec![attachment(
            "att_1",
            Some("evil\"\r\nX-Injected: yes.pdf"),
            Some("attachments/a-1"),
        )];
    })
    .await;

    let (status, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/attachments/att_1"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let url = body["download_url"].as_str().unwrap();
    assert!(!url.contains('\r'), "{url}");
    assert!(!url.contains('\n'), "{url}");
    assert!(!url.contains("X-Injected: yes"), "{url}");

    // The unsanitized name still round-trips as JSON metadata, where it is
    // data rather than a header.
    assert_eq!(body["filename"], "evil\"\r\nX-Injected: yes.pdf");
}

#[tokio::test]
async fn an_unknown_attachment_is_not_found() {
    let (h, id) = seeded(|m| {
        m.attachments = vec![attachment("att_1", None, Some("attachments/a-1"))];
    })
    .await;

    let (status, _) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/attachments/att_missing"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_attachment_dropped_for_size_has_no_download() {
    // Its metadata is real, but nothing was stored, so there is nothing to
    // hand back.
    let (h, id) = seeded(|m| {
        m.attachments = vec![attachment("att_1", Some("huge.bin"), None)];
    })
    .await;

    let (status, _) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages/{id}/attachments/att_1"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn downloads_require_a_key() {
    let (h, id) = seeded(|m| m.raw_s3_key = Some("inbound/raw-1".to_owned())).await;
    let request = Request::get(format!("/v0/inboxes/{INBOX}/messages/{id}/raw"))
        .body(Body::empty())
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
