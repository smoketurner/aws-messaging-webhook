#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
//! End-to-end repro for the inbox-id casing inconsistency: ingest
//! ASCII-lowercases the envelope recipient before storing under `InboxId`,
//! but the `/v0` read API used the `{inbox_id}` path segment verbatim. With
//! the fix, the API applies the same `trim().to_ascii_lowercase()` rule as
//! `ingest::normalized_recipients`, so a client that only knows the original
//! casing (e.g. from a forwarded `To:` header) can read its mail.
//!
//! Drives the full production ingest path (`post` → router → `ingest_inbound`,
//! including `normalized_recipients` and `resolve_recipient`) and then the
//! real axum router for both the lowercased and mixed-case path spellings.

use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, content, ids, time};
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt as _;
use webhook_test_support::{Harness, mail_harness, post, wrapped};

const KEY: &str = "am_live_key";
const BUCKET: &str = "mail-bucket";
const TS: &str = "2026-01-01T00:00:00.000Z";
const SES_ID: &str = "ses-casing-1";

/// A raw inbound MIME whose `To:` header carries the mixed-case address a
/// forwarded copy or a mail client would display. The envelope RCPT carries
/// the same mixed-case form, so ingest's `normalized_recipients` has to
/// lowercase it before matching the configured inbox.
const RAW_MIXED_CASE: &[u8] = b"From: Alice <alice@example.net>\r\n\
To: Support@Example.com\r\n\
Subject: Casing repro\r\n\
Date: Mon, 1 Sep 2025 10:00:00 +0000\r\n\
Message-ID: <casing-1@example.net>\r\n\
MIME-Version: 1.0\r\n\
Content-Type: text/plain; charset=utf-8\r\n\
\r\n\
Hello support,\r\n";

/// A `notificationType: "Received"` SNS payload with an S3 action pointer.
fn ses_inbound_s3(recipients: &[&str]) -> Value {
    json!({
        "notificationType": "Received",
        "mail": {"messageId": SES_ID, "timestamp": TS},
        "receipt": {
            "recipients": recipients,
            "timestamp": TS,
            "spamVerdict": {"status": "PASS"},
            "virusVerdict": {"status": "PASS"},
            "spfVerdict": {"status": "PASS"},
            "dkimVerdict": {"status": "PASS"},
            "dmarcVerdict": {"status": "PASS"},
            "action": {"type": "S3", "bucketName": BUCKET, "objectKey": "inbound/msg-1"},
        }
    })
}

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

async fn seeded() -> Harness {
    let h = mail_harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(RAW_MIXED_CASE),
        "message/rfc822",
    );
    h
}

#[tokio::test]
async fn mixed_case_envelope_rcpt_ingests_under_lowercased_id_and_api_reads_it_back() {
    let h = seeded().await;

    // Ingest the mixed-case RCPT through the full router → `ingest_inbound`
    // path. `normalized_recipients` lowercases `Support@Example.com` to
    // `support@example.com`, passes the `address != mail_config.inbox` gate,
    // and stores the message under `InboxId("support@example.com")`.
    let body = wrapped(&h, &ses_inbound_s3(&["Support@Example.com"]));
    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "ingest should accept the mixed-case RCPT"
    );

    let message_id = ids::inbound_message_id(SES_ID, time::parse(TS).unwrap()).to_string();
    let inbox = InboxId("support@example.com".to_owned());
    let stored = h
        .fake()
        .mail
        .get_message(&inbox, &message_id)
        .await
        .unwrap()
        .expect("message was stored under the lowercased id");

    // The `to` header keeps its verbatim casing while `inbox_id` is lowercased
    // — the two fields disagree on the casing of the same address, which is
    // exactly the trap a client following the `To:` header falls into.
    assert_eq!(stored.inbox_id, inbox);
    assert_eq!(stored.to, vec!["Support@Example.com"]);

    // The content document loads (ingest wrote it under the lowercased id).
    let stored_content = content::load(h.fake(), &stored).await.unwrap();
    assert!(
        stored_content
            .text
            .as_deref()
            .is_some_and(|text| text.contains("Hello support"))
    );

    // Before the fix: mixed-case path → 404 on the inbox, 200 with count: 0 on
    // the messages list. After the fix: both resolve because the API
    // normalizes the path the same way ingest normalizes the RCPT.
    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "mixed-case inbox path should resolve"
    );
    assert_eq!(body["inbox_id"], "support@example.com");

    let (status, body) = get(&h, "/v0/inboxes/Support@Example.com/messages").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "mixed-case messages path should resolve"
    );
    assert_eq!(
        body["count"], 1,
        "mixed-case path must not silently look empty"
    );
    assert_eq!(body["messages"][0]["inbox_id"], "support@example.com");
    assert_eq!(body["messages"][0]["to"][0], "Support@Example.com");

    // The exact lowercased id the service published is still accepted — no
    // regression on the standard integration path.
    let (status, body) = get(&h, "/v0/inboxes/support@example.com").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], "support@example.com");

    let (status, body) = get(&h, "/v0/inboxes/support@example.com/messages").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["message_id"], message_id);

    // Padded variants are normalized too (trim), matching ingest.
    let (status, body) = get(&h, "/v0/inboxes/%20Support@Example.com%20").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a whitespace-padded path should be trimmed"
    );
    assert_eq!(body["inbox_id"], "support@example.com");
}
