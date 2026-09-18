#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
//! Integration tests for inbound ingest: the real router,
//! `FakeServices` (`MailMemoryStore` + `FakeObjectStore`), and the
//! `.eml` fixtures.
//!
//! `webhook_test_support::harness()` always builds a harness with
//! `mail: None` (it is a shared crate this track does not own), so
//! router-driven tests here build their own mail-configured harness
//! (`mail_harness`), mirroring `harness_with`'s wiring with a non-`None`
//! `MailConfig`. The paused-time deadline and resume tests call
//! `mail::ingest::ingest_inbound` directly instead of going through the
//! router: `test_context()` and the Function URL fallback both hard-code a
//! 60 s deadline that this track cannot override without touching the
//! shared crate, and a direct call is the only way to drive
//! `ingest_inbound` with a short, precisely controlled `tokio::time::Instant`
//! deadline.

use std::sync::Arc;
use std::time::Duration;

use aws_messaging_webhook::allowlist::TopicAllowlist;
use aws_messaging_webhook::config::{Config, FunctionMode, MailConfig};
use aws_messaging_webhook::mail::ids::inbound_message_id;
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::time::parse as parse_ts;
use aws_messaging_webhook::mail::{content, ids, ingest};
use aws_messaging_webhook::model::ses_inbound::SesInboundNotification;
use aws_messaging_webhook::state::AppState;
use axum::body::Bytes;
use axum::http::StatusCode;
use serde_json::{Value, json};
use sns_message_verifier::SnsVerifier;
use sns_message_verifier::fixtures::SnsFixture;
use webhook_test_support::objects::ObjectFailure;
use webhook_test_support::{FakeServices, Harness, direct_sns_event, invoke, post, wrapped};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUCKET: &str = "mail-bucket";
const DOMAIN: &str = "example.com";

fn test_mail_config() -> MailConfig {
    MailConfig {
        domain: DOMAIN.to_owned(),
        table_name: "mail-table".to_owned(),
        bucket: BUCKET.to_owned(),
        inbox: "support@example.com".to_owned(),
        configuration_set: "config-set".to_owned(),
        identity_arn: "arn:aws:ses:us-east-1:123456789012:identity/example.com".to_owned(),
        api_keys_parameter: "/example/api-keys".to_owned(),
        attachment_url_ttl: Duration::from_secs(3600),
        region: "us-east-1".to_owned(),
        retention_days: 365,
    }
}

/// Builds a harness with `mail` configured (`webhook_test_support::harness`
/// always sets `mail: None` — see the module doc).
async fn mail_harness() -> Harness {
    let fixture = SnsFixture::new();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/cert.pem"))
        .respond_with(ResponseTemplate::new(200).set_body_string(&fixture.cert_pem))
        .mount(&server)
        .await;
    let cert_url = format!("{}/cert.pem", server.uri());

    let state = Arc::new(AppState {
        services: FakeServices::default(),
        api_keys: aws_messaging_webhook::api::keys::KeyCache::new(),
        verifier: SnsVerifier::builder()
            .dangerous_allow_cert_url_prefix(server.uri())
            .build()
            .unwrap(),
        allowlist: TopicAllowlist::parse(webhook_test_support::ALLOWED_ACCOUNT),
        http: reqwest::Client::new(),
        config: Config {
            table_name: "events".to_owned(),
            event_bus_name: "bus".to_owned(),
            event_source: "aws-messaging-webhook".to_owned(),
            auto_resubscribe: true,
            opt_out_list_name: None,
            raw_event_retention_days: 30,
            aggregate_retention_days: 365,
            mode: FunctionMode::Webhook,
            mail: Some(test_mail_config()),
        },
        dangerous_subscribe_url_prefix: Some(server.uri()),
    });
    Harness {
        state,
        fixture,
        cert_url,
        server,
    }
}

/// A `notificationType: "Received"` SNS payload with an S3 action pointer
/// the fields ingest reads.
#[expect(clippy::too_many_arguments, reason = "one test-fixture builder")]
fn ses_inbound_s3(
    ses_message_id: &str,
    timestamp: &str,
    recipients: &[&str],
    bucket: &str,
    key: &str,
    spam: &str,
    virus: &str,
    auth: &str,
) -> Value {
    json!({
        "notificationType": "Received",
        "mail": {"messageId": ses_message_id, "timestamp": timestamp},
        "receipt": {
            "recipients": recipients,
            "timestamp": timestamp,
            "spamVerdict": {"status": spam},
            "virusVerdict": {"status": virus},
            "spfVerdict": {"status": auth},
            "dkimVerdict": {"status": auth},
            "dmarcVerdict": {"status": auth},
            "action": {"type": "S3", "bucketName": bucket, "objectKey": key},
        }
    })
}

fn expected_message_id(ses_id: &str, timestamp: &str) -> String {
    inbound_message_id(ses_id, parse_ts(timestamp).unwrap()).to_string()
}

const TS: &str = "2026-01-01T00:00:00.000Z";

#[tokio::test]
async fn plain_message_ingests_and_creates_a_thread() {
    let h = mail_harness().await;
    let raw = include_bytes!("fixtures/mail/plain.eml");
    h.fake()
        .objects
        .seed("inbound/msg-1", Bytes::from_static(raw), "message/rfc822");
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;
    assert_eq!(status, StatusCode::OK);

    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let stored = h
        .fake()
        .mail
        .get_message(&inbox, &message_id)
        .await
        .unwrap()
        .expect("message was inserted");
    assert_eq!(
        stored.thread_id, message_id,
        "a new thread roots at its own id"
    );
    assert_eq!(stored.subject, "Plain text hello");
    assert!(stored.labels.contains(&"received".to_owned()));
    assert!(stored.labels.contains(&"unread".to_owned()));
    assert!(!stored.labels.contains(&"spam".to_owned()));

    // The body, headers and verdicts live in the content document, not the
    // item.
    let stored_content = content::load(h.fake(), &stored).await.unwrap();
    assert!(
        stored_content
            .text
            .as_deref()
            .is_some_and(|text| text.contains("Hello Bob"))
    );
    assert!(stored_content.headers.contains_key("Subject"));
    assert_eq!(
        stored_content.verdicts.as_ref().unwrap()["spam"],
        "PASS",
        "verdicts come from the SES receipt"
    );

    // The item and its thread expire with the bucket's lifecycle.
    let retention_secs = 365 * 86_400;
    let now_secs = aws_messaging_webhook::mail::time::now_ms() / 1_000;
    assert!(
        stored.expires_at > now_secs + retention_secs - 60
            && stored.expires_at <= now_secs + retention_secs,
        "expires_at {} is not retention_days from now",
        stored.expires_at
    );
    let thread = h
        .fake()
        .mail
        .get_thread(&inbox, &message_id, 1, None)
        .await
        .unwrap()
        .unwrap()
        .thread;
    assert_eq!(thread.expires_at, stored.expires_at);
    assert!(
        h.fake().calls().contains(&"persist:ses-1".to_owned()),
        "the SES receipt is persisted as the outbox entry, keyed by the SES message id, before ingest runs: {:?}",
        h.fake().calls()
    );
}

/// The direct SNS → Lambda pathway (`entry::dispatch`) runs the same ingest
/// pipeline as the Function URL pathway.
#[tokio::test]
async fn direct_sns_invocation_also_ingests() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let envelope = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );

    let result = invoke(h.state.clone(), direct_sns_event(&envelope)).await;
    assert!(result.is_ok(), "{result:?}");

    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    assert!(
        h.fake()
            .mail
            .get_message(&inbox, &message_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn reply_threads_under_its_parent() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    h.fake().objects.seed(
        "inbound/msg-2",
        Bytes::from_static(include_bytes!("fixtures/mail/reply-references.eml")),
        "message/rfc822",
    );

    let first = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &first).await,
        StatusCode::OK
    );
    let parent_id = expected_message_id("ses-1", TS);

    let reply_ts = "2026-01-01T00:01:00.000Z";
    let second = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-2",
            reply_ts,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-2",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &second).await,
        StatusCode::OK
    );
    let reply_id = expected_message_id("ses-2", reply_ts);

    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let reply = h
        .fake()
        .mail
        .get_message(&inbox, &reply_id)
        .await
        .unwrap()
        .expect("reply was inserted");
    assert_eq!(
        reply.thread_id, parent_id,
        "the reply resolves to the parent's thread via its In-Reply-To"
    );
}

#[tokio::test]
async fn spam_message_gets_the_spam_label() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "FAIL",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );

    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let stored = h
        .fake()
        .mail
        .get_message(&inbox, &message_id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.labels.contains(&"spam".to_owned()));
    assert!(!stored.labels.contains(&"unauthenticated".to_owned()));
}

#[tokio::test]
async fn unauthenticated_message_gets_the_unauthenticated_label() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "FAIL",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );

    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let stored = h
        .fake()
        .mail
        .get_message(&inbox, &message_id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.labels.contains(&"unauthenticated".to_owned()));
    assert!(!stored.labels.contains(&"spam".to_owned()));
}

#[tokio::test]
async fn redelivery_is_idempotent() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );

    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );
    // Redelivery: every target inbox already has the (deterministic) id, so
    // this short-circuits to `ingest_duplicate` without re-fetching.
    h.fake()
        .objects
        .inject("inbound/msg-1", ObjectFailure::Permanent);
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK,
        "a redelivery must not re-fetch the (now-failing) raw object"
    );
}

#[tokio::test]
async fn missing_s3_pointer_skips() {
    let h = mail_harness().await;
    let body = wrapped(
        &h,
        &json!({
            "notificationType": "Received",
            "mail": {"messageId": "ses-1", "timestamp": TS},
            "receipt": {"recipients": ["support@example.com"], "action": {"type": "SNS"}},
        }),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );
    // No S3 pointer means nothing was ever fetched or inserted; a configured
    // inbox is never even created for a receipt that carries no pointer.
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    assert!(h.fake().mail.get_inbox(&inbox).await.unwrap().is_none());
}

#[tokio::test]
async fn wrong_bucket_is_a_permanent_skip() {
    let h = mail_harness().await;
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            "some-other-bucket",
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    // The status is 200 (skips are a success outcome), and nothing is fetched
    // from the (unseeded, and therefore would-404) real bucket key.
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );
    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    assert!(
        h.fake()
            .mail
            .get_message(&inbox, &message_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn missing_object_is_a_permanent_failure_but_still_200() {
    let h = mail_harness().await;
    // Not seeded: `get_object` returns `NotFound`.
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/missing",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK,
        "a permanent ingest failure is logged and counted, not surfaced as a 5xx"
    );
    let message_id = expected_message_id("ses-1", TS);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    assert!(
        h.fake()
            .mail
            .get_message(&inbox, &message_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn transient_s3_error_returns_500() {
    let h = mail_harness().await;
    h.fake()
        .objects
        .inject("inbound/msg-1", ObjectFailure::Transient);
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &["support@example.com"],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

/// SES publishes a test notification whenever a receipt rule changes. It is
/// not mail: nothing is persisted, ingested or published for it.
#[tokio::test]
async fn the_ses_setup_notification_is_acknowledged_and_ignored() {
    let h = mail_harness().await;
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "AMAZON_SES_SETUP_NOTIFICATION",
            TS,
            &["recipient@example.com"],
            BUCKET,
            "inbound/raw/AMAZON_SES_SETUP_NOTIFICATION",
            "PASS",
            "PASS",
            "PASS",
        ),
    );

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake().calls().is_empty(),
        "nothing may be persisted or published: {:?}",
        h.fake().calls()
    );
}

/// Two deliveries of one message — the same `Message-ID` under different SES
/// ids, as a mailing list resend produces — are two messages. The second's
/// alias is already taken, which must not fail its ingest; it joins the
/// first's thread.
#[tokio::test]
async fn a_message_reusing_a_known_message_id_joins_that_thread() {
    let h = mail_harness().await;
    let raw = Bytes::from_static(include_bytes!("fixtures/mail/plain.eml"));
    h.fake()
        .objects
        .seed("inbound/msg-1", raw.clone(), "message/rfc822");
    h.fake()
        .objects
        .seed("inbound/msg-2", raw, "message/rfc822");
    for (ses_id, key) in [("ses-1", "inbound/msg-1"), ("ses-2", "inbound/msg-2")] {
        let body = wrapped(
            &h,
            &ses_inbound_s3(
                ses_id,
                TS,
                &["support@example.com"],
                BUCKET,
                key,
                "PASS",
                "PASS",
                "PASS",
            ),
        );
        assert_eq!(
            post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
            StatusCode::OK
        );
    }

    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let first = expected_message_id("ses-1", TS);
    let second = h
        .fake()
        .mail
        .get_message(&inbox, &expected_message_id("ses-2", TS))
        .await
        .unwrap()
        .expect("the second delivery was stored");
    assert_eq!(second.thread_id, first);
}

#[tokio::test]
async fn only_the_configured_inbox_receives_a_multi_recipient_message() {
    let h = mail_harness().await;
    h.fake().objects.seed(
        "inbound/msg-1",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let body = wrapped(
        &h,
        &ses_inbound_s3(
            "ses-1",
            TS,
            &[
                "support@example.com",
                "sales@example.com",
                "support@example.net",
            ],
            BUCKET,
            "inbound/msg-1",
            "PASS",
            "PASS",
            "PASS",
        ),
    );
    assert_eq!(
        post(h.state.clone(), "/webhooks/ses/inbound", &body).await,
        StatusCode::OK
    );

    let message_id = expected_message_id("ses-1", TS);
    let stored = |local: &str| {
        let inbox = aws_messaging_webhook::mail::InboxId(local.to_owned());
        let fake = h.fake();
        let message_id = message_id.clone();
        async move {
            fake.mail
                .get_message(&inbox, &message_id)
                .await
                .unwrap()
                .is_some()
        }
    };
    assert!(
        stored("support@example.com").await,
        "the configured inbox must receive the message"
    );
    assert!(
        !stored("sales@example.com").await,
        "a recipient other than MAIL_INBOX must be skipped"
    );
    assert!(
        !stored("support@example.net").await,
        "the inbox's local part at another domain is a different address"
    );
    assert!(
        h.fake()
            .mail
            .get_inbox(&aws_messaging_webhook::mail::InboxId(
                "sales@example.com".to_owned()
            ))
            .await
            .unwrap()
            .is_none(),
        "no inbox may be created for an unconfigured recipient"
    );
    let created = h
        .fake()
        .mail
        .get_inbox(&aws_messaging_webhook::mail::InboxId(
            "support@example.com".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created.email, "support@example.com");
}

/// A direct `mail::ingest::ingest_inbound` call — see the module doc for why
/// this test cannot drive the deadline through the router.
fn direct_state(bucket: &str) -> AppState<FakeServices> {
    AppState {
        services: FakeServices::default(),
        api_keys: aws_messaging_webhook::api::keys::KeyCache::new(),
        verifier: SnsVerifier::builder().build().unwrap(),
        allowlist: TopicAllowlist::parse(""),
        http: reqwest::Client::new(),
        config: Config {
            table_name: "events".to_owned(),
            event_bus_name: "bus".to_owned(),
            event_source: "aws-messaging-webhook".to_owned(),
            auto_resubscribe: true,
            opt_out_list_name: None,
            raw_event_retention_days: 30,
            aggregate_retention_days: 365,
            mode: FunctionMode::Webhook,
            mail: Some(MailConfig {
                bucket: bucket.to_owned(),
                ..test_mail_config()
            }),
        },
        dangerous_subscribe_url_prefix: None,
    }
}

fn notification(
    ses_id: &str,
    timestamp: &str,
    recipients: &[&str],
    key: &str,
) -> SesInboundNotification {
    let value = ses_inbound_s3(
        ses_id, timestamp, recipients, BUCKET, key, "PASS", "PASS", "PASS",
    );
    serde_json::from_value(value).unwrap()
}

/// A receipt with neither `mail.timestamp` nor `receipt.timestamp` set, for
/// the timestamp tests: `received_ms` must fall back to the caller-supplied
/// envelope timestamp, and never to wall-clock time.
fn notification_without_timestamps(
    ses_id: &str,
    recipients: &[&str],
    key: &str,
) -> SesInboundNotification {
    let value = json!({
        "notificationType": "Received",
        "mail": {"messageId": ses_id},
        "receipt": {
            "recipients": recipients,
            "spamVerdict": {"status": "PASS"},
            "virusVerdict": {"status": "PASS"},
            "spfVerdict": {"status": "PASS"},
            "dkimVerdict": {"status": "PASS"},
            "dmarcVerdict": {"status": "PASS"},
            "action": {"type": "S3", "bucketName": BUCKET, "objectKey": key},
        }
    });
    serde_json::from_value(value).unwrap()
}

#[tokio::test(start_paused = true)]
async fn deadline_timeout_returns_transient() {
    let state = direct_state(BUCKET);
    state
        .services
        .objects
        .inject("inbound/hang", ObjectFailure::Hang);
    let event = notification("ses-1", TS, &["support@example.com"], "inbound/hang");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let result = ingest::ingest_inbound(&state, &event, deadline, None).await;

    let Err(error) = &result else {
        panic!("expected a transient timeout error, got {result:?}");
    };
    assert_eq!(
        error.kind,
        aws_messaging_webhook::actions::ActionErrorKind::Transient,
        "a deadline elapse must be transient (5xx-equivalent) so it redelivers"
    );
}

#[tokio::test]
async fn resume_skips_an_already_present_part() {
    let state = direct_state(BUCKET);
    let raw = include_bytes!("fixtures/mail/attachment-content-id.eml");
    state
        .services
        .objects
        .seed("inbound/att-1", Bytes::from_static(raw), "message/rfc822");

    let event = notification("ses-1", TS, &["support@example.com"], "inbound/att-1");
    let message_id = inbound_message_id("ses-1", parse_ts(TS).unwrap());
    let attachment_id = ids::attachment_id(&message_id, 0);
    let object_key = format!("attachments/{message_id}/{attachment_id}");

    // Pre-seed the part exactly as the first attempt would have left it —
    // simulating a redelivery that resumes after a prior attempt already put
    // this part.
    let original = Bytes::from_static(b"hello world");
    state
        .services
        .objects
        .seed(&object_key, original.clone(), "image/png");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let result = ingest::ingest_inbound(&state, &event, deadline, None).await;
    assert!(result.is_ok(), "{result:?}");

    // The pre-seeded content is untouched — `put_object_if_absent` never ran
    // against a differing body for this key.
    assert_eq!(state.services.objects.get(&object_key), Some(original));
    // Direct call-count assertions rather than outcome-only. The part
    // is HEADed (once to detect a redelivery, once more in the per-part
    // resume check) but never put, since it was already present.
    assert_eq!(
        state.services.objects.head_object_call_count(&object_key),
        2,
        "resume HEADs every part: once to detect a redelivery, once per part"
    );
    assert_eq!(
        state.services.objects.put_object_call_count(&object_key),
        0,
        "an already-present part is skipped, never put"
    );

    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    let stored = state
        .services
        .mail
        .get_message(&inbox, &message_id.to_string())
        .await
        .unwrap()
        .expect("message was inserted");
    assert_eq!(stored.attachments.len(), 1);
    assert_eq!(stored.attachments[0].attachment_id, attachment_id);
    assert_eq!(stored.attachments[0].content_type, "image/png");
}

/// A raw MIME message carrying two attachment parts, for tests that need to
/// hang a `put_object_if_absent` on one part while the other completes
/// normally — `attachment-content-id.eml` only carries one.
fn two_attachment_email() -> Vec<u8> {
    "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: two parts\r\nMessage-ID: <two-parts-1@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=\"a.bin\"\r\n\r\naGVsbG8=\r\n--b\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=\"b.bin\"\r\n\r\naGVsbG8=\r\n--b--\r\n"
        .as_bytes()
        .to_vec()
}

/// Converts the "no put still pending after the deadline" test from
/// outcome-only (just a transient error) to direct — the hung attachment's
/// put call count freezes the moment the deadline elapses and never grows
/// again, because the whole future tree (including the pending put) is
/// dropped synchronously when the surrounding `tokio::time::timeout` fires
/// (see the doc comment on `mail::ingest::put_kept_parts`).
#[tokio::test(start_paused = true)]
async fn no_put_still_pending_after_the_deadline() {
    let state = direct_state(BUCKET);
    state.services.objects.seed(
        "inbound/two-atts",
        Bytes::from(two_attachment_email()),
        "message/rfc822",
    );

    let event = notification("ses-1", TS, &["support@example.com"], "inbound/two-atts");
    let message_id = inbound_message_id("ses-1", parse_ts(TS).unwrap());
    // The second attachment (ordinal 1) hangs; the first is left alone so
    // the resume check's single HEAD on it (ordinal 0, absent) resolves
    // fast and `put_kept_parts` proceeds straight to putting both parts
    // concurrently.
    let hung_key = format!(
        "attachments/{message_id}/{}",
        ids::attachment_id(&message_id, 1)
    );
    state
        .services
        .objects
        .inject(&hung_key, ObjectFailure::Hang);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let result = ingest::ingest_inbound(&state, &event, deadline, None).await;

    let Err(error) = &result else {
        panic!("expected a transient timeout error, got {result:?}");
    };
    assert_eq!(
        error.kind,
        aws_messaging_webhook::actions::ActionErrorKind::Transient
    );

    let count_at_timeout = state.services.objects.put_object_call_count(&hung_key);
    assert_eq!(
        count_at_timeout, 1,
        "the hung put was attempted exactly once"
    );
    // Advance further paused time: if the future tree were not fully
    // dropped on timeout, the hung put would still be polled and retried.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        state.services.objects.put_object_call_count(&hung_key),
        count_at_timeout,
        "no put is still pending after the deadline: the call count must not keep growing"
    );
    assert!(
        !state.services.objects.contains(&hung_key),
        "the hung part never actually landed"
    );
}

/// When `mail.timestamp` and `receipt.timestamp` are both absent, the
/// envelope timestamp fallback still produces the same deterministic id
/// across a redelivery, so the second delivery is a duplicate rather than a
/// second stored copy.
#[tokio::test]
async fn envelope_timestamp_fallback_stays_deterministic_across_redelivery() {
    let state = direct_state(BUCKET);
    state.services.objects.seed(
        "inbound/no-ts",
        Bytes::from_static(include_bytes!("fixtures/mail/plain.eml")),
        "message/rfc822",
    );
    let event = notification_without_timestamps("ses-1", &["support@example.com"], "inbound/no-ts");
    let envelope_ms = parse_ts(TS).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    let first = ingest::ingest_inbound(&state, &event, deadline, Some(envelope_ms)).await;
    assert_eq!(first.unwrap(), "ingested");

    let message_id = inbound_message_id("ses-1", envelope_ms);
    let inbox = aws_messaging_webhook::mail::InboxId("support@example.com".to_owned());
    assert!(
        state
            .services
            .mail
            .get_message(&inbox, &message_id.to_string())
            .await
            .unwrap()
            .is_some()
    );

    let second = ingest::ingest_inbound(&state, &event, deadline, Some(envelope_ms)).await;
    assert_eq!(
        second.unwrap(),
        "ingest_duplicate",
        "the same envelope timestamp must compute the same id on redelivery"
    );
}

/// With no usable timestamp on any of the three fallbacks, ingest
/// skips the message (`no_timestamp`) rather than inventing one via
/// `time::now_ms()` — never fetching the raw object, since a wall-clock
/// value would make the id non-deterministic across a redelivery.
#[tokio::test]
async fn missing_every_timestamp_source_skips_rather_than_inventing_one() {
    let state = direct_state(BUCKET);
    let event = notification_without_timestamps("ses-1", &["support@example.com"], "inbound/no-ts");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    let result = ingest::ingest_inbound(&state, &event, deadline, None).await;
    assert_eq!(result.unwrap(), "ingest_skipped");
    assert_eq!(
        state
            .services
            .objects
            .get_object_call_count("inbound/no-ts"),
        0,
        "the message is skipped before ever fetching the raw object"
    );
}
