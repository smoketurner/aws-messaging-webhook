// Every `.unwrap()` here lives inside a `#[tokio::test]` function, which
// clippy.toml's `allow-unwrap-in-tests` already exempts; the non-test helper
// functions that needed an explicit `unwrap_used` expectation live in the
// `webhook-test-support` dev-dependency crate.

use std::sync::atomic::Ordering;

use aws_messaging_webhook::actions::ActionErrorKind;
use aws_messaging_webhook::app::app;
use aws_messaging_webhook::store::PersistOutcome;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use sns_message_verifier::fixtures::{notification, subscription_confirmation};
use tower::ServiceExt as _;
use webhook_test_support::{
    HarnessOptions, direct_sns_event, direct_sns_record, function_url_event, harness, harness_with,
    invoke, post, wrapped,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn valid_notification_persists() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    // The request path persists only; the stream relay publishes.
    assert_eq!(
        h.fake().calls(),
        vec!["persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned()]
    );
}

#[tokio::test]
async fn mis_wired_topic_still_classifies_correctly() {
    let h = harness().await;
    // An inbound SMS delivered to the SES events path: the family comes from
    // the payload shape, so it still processes as sms.inbound (with a
    // family_mismatch warning) instead of degrading to unknown.
    let body = wrapped(&h, &inbound_sms("HELLO"));

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    // The family comes from the payload shape, so it persists under the
    // sms.inbound aggregate id even though it arrived on the SES path.
    assert!(h.fake().calls().contains(&"persist:in-msg-1".to_owned()));
}

#[tokio::test]
async fn tampered_signature_rejected_and_nothing_touched() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");
    body["Message"] = json!("tampered");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.fake().calls().is_empty());
}

#[tokio::test]
async fn unlisted_topic_rejected_before_any_verification_work() {
    // expect(0) cert fetches: the allowlist must reject before verify runs.
    let h = harness_with(HarnessOptions {
        allowed_topics: "999999999999",
        cert_fetches: Some(0),
        ..HarnessOptions::default()
    })
    .await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.fake().calls().is_empty());
    h.server.verify().await;
}

#[tokio::test]
async fn missing_sns_header_is_bad_request() {
    let h = harness().await;
    let request = Request::post("/webhooks/ses/events")
        .body(Body::from("{}"))
        .unwrap();
    let status = app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn non_json_body_is_bad_request() {
    let h = harness().await;
    let status = post(
        h.state.clone(),
        "/webhooks/ses/events",
        &json!("not an envelope"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.fake().calls().is_empty());
}

#[tokio::test]
async fn duplicate_delivery_still_runs_the_idempotent_action() {
    let h = harness().await;
    *h.fake().persist_outcome.lock().unwrap() = Some(PersistOutcome::Duplicate);
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    // A redelivery re-runs the (idempotent) action so a crash before the first
    // attempt's action cannot lose it. Publishing is the stream relay's job.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:in-msg-1".to_owned(),
            "opt_out:+14255550182".to_owned(),
        ]
    );
}

#[tokio::test]
async fn persist_failure_returns_500_for_redelivery() {
    let h = harness().await;
    h.fake().fail_persist.store(true, Ordering::SeqCst);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(h.fake().calls().len(), 1);
}

#[tokio::test]
async fn subscription_confirmation_gets_the_subscribe_url() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&h.server)
        .await;
    let mut body = subscription_confirmation(&h.cert_url);
    body["SubscribeURL"] = json!(format!("{}/confirm", h.server.uri()));
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake().calls().is_empty(),
        "confirmations are not persisted"
    );
    h.server.verify().await;
}

#[tokio::test]
async fn unsubscribe_confirmation_resubscribes_and_publishes_notice() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&h.server)
        .await;
    let mut body = subscription_confirmation(&h.cert_url);
    body["Type"] = json!("UnsubscribeConfirmation");
    body["SubscribeURL"] = json!(format!("{}/confirm", h.server.uri()));
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls(),
        vec!["publish:subscription.changed".to_owned()]
    );
    {
        let published = h.fake().published.lock().unwrap();
        assert_eq!(published[0].detail["action"], "resubscribed");
    }
    h.server.verify().await;
}

#[tokio::test]
async fn auto_resubscribe_disabled_leaves_unsubscribe_alone() {
    let h = harness_with(HarnessOptions {
        auto_resubscribe: false,
        ..HarnessOptions::default()
    })
    .await;
    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&h.server)
        .await;
    let mut body = subscription_confirmation(&h.cert_url);
    body["Type"] = json!("UnsubscribeConfirmation");
    body["SubscribeURL"] = json!(format!("{}/confirm", h.server.uri()));
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(h.fake().calls().is_empty());
    h.server.verify().await;
}

#[tokio::test]
async fn failed_confirmation_get_returns_500_so_sns_retries() {
    let h = harness().await;
    Mock::given(method("GET"))
        .and(path("/confirm"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&h.server)
        .await;
    let mut body = subscription_confirmation(&h.cert_url);
    body["SubscribeURL"] = json!(format!("{}/confirm", h.server.uri()));
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn healthz_responds_ok() {
    let h = harness().await;
    let request = Request::get("/healthz").body(Body::empty()).unwrap();
    let response = app(h.state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn inbound_sms(keyword: &str) -> Value {
    json!({
        "originationNumber": "+14255550182",
        "destinationNumber": "+12125550101",
        "messageKeyword": keyword,
        "messageBody": keyword,
        "inboundMessageId": "in-msg-1"
    })
}

#[tokio::test]
async fn stop_keyword_opts_out() {
    let h = harness().await;
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:in-msg-1".to_owned(),
            "opt_out:+14255550182".to_owned(),
        ]
    );
}

#[tokio::test]
async fn start_keyword_opts_back_in() {
    let h = harness().await;
    let body = wrapped(&h, &inbound_sms("START"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(h.fake().calls().contains(&"opt_in:+14255550182".to_owned()));
}

#[tokio::test]
async fn keyword_without_opt_out_list_configured_is_forwarded_only() {
    let h = harness_with(HarnessOptions {
        opt_out_list: false,
        ..HarnessOptions::default()
    })
    .await;
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("opt_out")),
        "no opt-out call: {calls:?}"
    );
    assert!(calls.contains(&"persist:in-msg-1".to_owned()));
}

#[tokio::test]
async fn ordinary_inbound_sms_takes_no_action() {
    let h = harness().await;
    let inner = json!({
        "originationNumber": "+14255550182",
        "messageBody": "hello there",
        "inboundMessageId": "in-msg-2"
    });
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.fake().calls(), vec!["persist:in-msg-2".to_owned()]);
}

#[tokio::test]
async fn delivered_dlr_reports_received_feedback() {
    let h = harness().await;
    let inner = json!({"eventType": "TEXT_DELIVERED", "messageId": "out-1", "isFinal": true});
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/sms/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls.contains(&"feedback:out-1:Received".to_owned()),
        "{calls:?}"
    );
}

#[tokio::test]
async fn failed_dlr_reports_failed_feedback() {
    let h = harness().await;
    let inner =
        json!({"eventType": "TEXT_CARRIER_UNREACHABLE", "messageId": "out-2", "isFinal": true});
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/sms/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake()
            .calls()
            .contains(&"feedback:out-2:Failed".to_owned())
    );
}

#[tokio::test]
async fn intermediate_dlr_sends_no_feedback() {
    let h = harness().await;
    let inner = json!({"eventType": "TEXT_QUEUED", "messageId": "out-3", "isFinal": false});
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/sms/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("feedback")),
        "{calls:?}"
    );
}

fn ses_bounce(bounce_type: &str, recipients: &[&str]) -> Value {
    let bounced: Vec<Value> = recipients
        .iter()
        .map(|r| json!({"emailAddress": r}))
        .collect();
    json!({
        "eventType": "Bounce",
        "bounce": {"bounceType": bounce_type, "bouncedRecipients": bounced},
        "mail": {"messageId": "ses-msg-1"}
    })
}

#[tokio::test]
async fn permanent_bounce_suppresses_every_recipient() {
    let h = harness().await;
    let body = wrapped(
        &h,
        &ses_bounce("Permanent", &["a@example.com", "b@example.com"]),
    );

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls.contains(&"suppress:a@example.com:Bounce".to_owned()),
        "{calls:?}"
    );
    assert!(calls.contains(&"suppress:b@example.com:Bounce".to_owned()));
}

#[tokio::test]
async fn transient_bounce_is_not_suppressed() {
    let h = harness().await;
    let body = wrapped(&h, &ses_bounce("Transient", &["a@example.com"]));

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("suppress")),
        "{calls:?}"
    );
}

#[tokio::test]
async fn complaint_suppresses_recipients() {
    let h = harness().await;
    let inner = json!({
        "notificationType": "Complaint",
        "complaint": {"complainedRecipients": [{"emailAddress": "c@example.com"}]},
        "mail": {"messageId": "ses-msg-2"}
    });
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake()
            .calls()
            .contains(&"suppress:c@example.com:Complaint".to_owned())
    );
}

fn ses_complaint(recipients: &[&str]) -> Value {
    let complained: Vec<Value> = recipients
        .iter()
        .map(|r| json!({"emailAddress": r}))
        .collect();
    json!({
        "notificationType": "Complaint",
        "complaint": {"complainedRecipients": complained},
        "mail": {"messageId": "ses-msg-2"}
    })
}

/// A permanent per-recipient failure (e.g. SES `BadRequestException` for a
/// malformed address in the bounce metadata) must not skip the remaining
/// recipients — each `PutSuppressedDestination` call is independent.
#[tokio::test]
async fn permanent_suppression_failure_for_one_bounce_recipient_continues_to_others() {
    let h = harness().await;
    h.fake()
        .permanent_suppression_failures
        .lock()
        .unwrap()
        .push("bad-address".to_owned());
    let body = wrapped(
        &h,
        &ses_bounce(
            "Permanent",
            &["a@example.com", "bad-address", "b@example.com"],
        ),
    );

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    // A permanent per-recipient failure is logged and swallowed; the event
    // is still persisted (and published by the stream relay).
    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls.contains(&"suppress:a@example.com:Bounce".to_owned()),
        "{calls:?}"
    );
    assert!(
        calls.contains(&"suppress:bad-address:Bounce".to_owned()),
        "the failing recipient must still be attempted: {calls:?}"
    );
    assert!(
        calls.contains(&"suppress:b@example.com:Bounce".to_owned()),
        "the recipient after the failure must not be skipped: {calls:?}"
    );
    assert!(calls.contains(&"persist:ses-msg-1".to_owned()));
}

/// The complaint path shares the same per-recipient failure mode as bounces.
#[tokio::test]
async fn permanent_suppression_failure_for_one_complaint_recipient_continues_to_others() {
    let h = harness().await;
    h.fake()
        .permanent_suppression_failures
        .lock()
        .unwrap()
        .push("bad-address".to_owned());
    let body = wrapped(
        &h,
        &ses_complaint(&["c@example.com", "bad-address", "d@example.com"]),
    );

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(calls.contains(&"suppress:c@example.com:Complaint".to_owned()));
    assert!(calls.contains(&"suppress:bad-address:Complaint".to_owned()));
    assert!(
        calls.contains(&"suppress:d@example.com:Complaint".to_owned()),
        "the recipient after the failure must not be skipped: {calls:?}"
    );
}

/// A transient suppression failure (throttling, 5xx) must still fail fast so
/// SNS redelivery re-runs the idempotent action for every recipient. The
/// recipient that failed is attempted, but later ones are not — redelivery
/// will re-attempt the whole batch.
#[tokio::test]
async fn transient_suppression_failure_fails_fast_and_returns_500() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Transient);
    let body = wrapped(
        &h,
        &ses_bounce("Permanent", &["a@example.com", "b@example.com"]),
    );

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let calls = h.fake().calls();
    assert!(
        calls.contains(&"suppress:a@example.com:Bounce".to_owned()),
        "the first recipient is attempted: {calls:?}"
    );
    assert!(
        !calls.contains(&"suppress:b@example.com:Bounce".to_owned()),
        "the loop must bail out on the transient error, not continue: {calls:?}"
    );
    assert!(calls.contains(&"persist:ses-msg-1".to_owned()));
}

fn ses_inbound(virus_status: &str, content: Option<&str>) -> Value {
    let mut inner = json!({
        "notificationType": "Received",
        "receipt": {
            "spamVerdict": {"status": "PASS"},
            "virusVerdict": {"status": virus_status},
            "action": {"type": "SNS"}
        },
        "mail": {"messageId": "inbound-msg-1"}
    });
    if let Some(content) = content {
        inner["content"] = json!(content);
    }
    inner
}

#[tokio::test]
async fn clean_inbound_email_persists() {
    let h = harness().await;
    let body = wrapped(&h, &ses_inbound("PASS", Some("Subject: hi\r\n\r\nhello")));

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake()
            .calls()
            .contains(&"persist:inbound-msg-1".to_owned())
    );
}

#[tokio::test]
async fn transient_action_failure_returns_500() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Transient);
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        h.fake()
            .calls()
            .contains(&"opt_out:+14255550182".to_owned())
    );
}

#[tokio::test]
async fn redelivery_after_transient_action_failure_reruns_the_action() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Transient);
    let body = wrapped(&h, &inbound_sms("STOP"));
    assert_eq!(
        post(h.state.clone(), "/webhooks/sms/inbound", &body).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );

    // SNS redelivers; the prior attempt persisted, so this is a duplicate.
    *h.fake().action_error.lock().unwrap() = None;
    *h.fake().persist_outcome.lock().unwrap() = Some(PersistOutcome::Duplicate);
    assert_eq!(
        post(h.state.clone(), "/webhooks/sms/inbound", &body).await,
        StatusCode::OK
    );

    let opt_outs = h
        .fake()
        .calls()
        .iter()
        .filter(|c| c.starts_with("opt_out"))
        .count();
    assert_eq!(opt_outs, 2, "action must run once per attempt");
}

#[tokio::test]
async fn permanent_action_failure_still_persists() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Permanent);
    let body = wrapped(&h, &ses_bounce("Permanent", &["a@example.com"]));

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    // A permanent action failure is logged and swallowed; the event is still
    // durably persisted (and will be published by the stream relay).
    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(calls.contains(&"suppress:a@example.com:Bounce".to_owned()));
    assert!(calls.contains(&"persist:ses-msg-1".to_owned()));
}

#[tokio::test]
async fn transient_cert_fetch_failure_returns_500_not_403() {
    // A cold-start cert fetch that fails transiently must not be a permanent
    // 4xx — that would make SNS drop a correctly-signed message. The verifier
    // has no cached cert and the cert server 500s, so verify() -> CertFetch.
    let fixture = sns_message_verifier::fixtures::SnsFixture::new();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/cert.pem"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let cert_url = format!("{}/cert.pem", server.uri());
    let state = std::sync::Arc::new(aws_messaging_webhook::state::AppState {
        services: webhook_test_support::FakeServices::default(),
        api_keys: aws_messaging_webhook::api::keys::KeyCache::new(),
        verifier: sns_message_verifier::SnsVerifier::builder()
            .dangerous_allow_cert_url_prefix(server.uri())
            .build()
            .unwrap(),
        allowlist: aws_messaging_webhook::allowlist::TopicAllowlist::parse(
            webhook_test_support::ALLOWED_ACCOUNT,
        ),
        http: reqwest::Client::new(),
        config: aws_messaging_webhook::config::Config {
            table_name: "events".to_owned(),
            event_bus_name: "bus".to_owned(),
            event_source: "aws-messaging-webhook".to_owned(),
            auto_resubscribe: true,
            opt_out_list_name: Some("opt-out-list".to_owned()),
            raw_event_retention_days: 30,
            aggregate_retention_days: 365,
            mode: aws_messaging_webhook::config::FunctionMode::Webhook,
            mail: None,
        },
        dangerous_subscribe_url_prefix: Some(server.uri()),
    });
    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");

    let status = post(state, "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn direct_invoke_persists() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let result = invoke(h.state.clone(), direct_sns_event(&body))
        .await
        .unwrap();

    assert_eq!(result, Value::Null);
    assert_eq!(
        h.fake().calls(),
        vec!["persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned()]
    );
}

#[tokio::test]
async fn direct_invoke_processes_every_record() {
    let h = harness().await;
    let mut first = notification(&h.cert_url);
    h.fixture.sign(&mut first, "2");
    let mut second = notification(&h.cert_url);
    second["MessageId"] = json!("second-message-id");
    h.fixture.sign(&mut second, "2");
    let payload = json!({ "Records": [direct_sns_record(&first), direct_sns_record(&second)] });

    invoke(h.state.clone(), payload).await.unwrap();

    let persists = h
        .fake()
        .calls()
        .iter()
        .filter(|c| c.starts_with("persist:"))
        .count();
    assert_eq!(persists, 2);
}

#[tokio::test]
async fn direct_invoke_tampered_signature_is_dropped_not_retried() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    body["Message"] = json!("tampered");

    // A 4xx-class rejection must complete the invocation (Ok): failing it
    // would ask Lambda to redeliver a permanently rejected message.
    let result = invoke(h.state.clone(), direct_sns_event(&body)).await;

    assert!(result.is_ok());
    assert!(h.fake().calls().is_empty());
}

#[tokio::test]
async fn direct_invoke_unlisted_topic_rejected_before_any_verification_work() {
    let h = harness_with(HarnessOptions {
        allowed_topics: "999999999999",
        cert_fetches: Some(0),
        ..HarnessOptions::default()
    })
    .await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let result = invoke(h.state.clone(), direct_sns_event(&body)).await;

    assert!(result.is_ok());
    assert!(h.fake().calls().is_empty());
    h.server.verify().await;
}

#[tokio::test]
async fn direct_invoke_classifies_by_payload_shape() {
    let h = harness().await;
    // No routing config exists for the direct pathway: the family (and the
    // canonical path reported to consumers) comes from the payload alone.
    let body = wrapped(&h, &inbound_sms("HELLO"));

    invoke(h.state.clone(), direct_sns_event(&body))
        .await
        .unwrap();

    assert!(h.fake().calls().contains(&"persist:in-msg-1".to_owned()));
}

#[tokio::test]
async fn direct_invoke_transient_failure_fails_the_invocation_for_retry() {
    let h = harness().await;
    h.fake().fail_persist.store(true, Ordering::SeqCst);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let result = invoke(h.state.clone(), direct_sns_event(&body)).await;

    assert!(
        result.is_err(),
        "5xx-class failures must fail the invocation"
    );
}

#[tokio::test]
async fn function_url_payload_dispatches_through_the_router() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    let payload = function_url_event("/webhooks/ses/events", &body);

    let response = invoke(h.state.clone(), payload).await.unwrap();

    assert_eq!(response["statusCode"], 200);
    assert_eq!(
        h.fake().calls(),
        vec!["persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned()]
    );
}

#[tokio::test]
async fn unrecognized_invoke_payload_is_an_error() {
    let h = harness().await;
    let result = invoke(h.state.clone(), json!({"hello": "world"})).await;
    assert!(result.is_err());
    assert!(h.fake().calls().is_empty());
}

#[tokio::test]
async fn stream_publishes_persisted_event() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    let raw = serde_json::to_vec(&body).unwrap();
    let event = webhook_test_support::dynamodb_insert_event(
        &raw,
        "EVT#2026-08-04T00:00:00.000Z#sns-1",
        "seq-1",
    );

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail["schemaVersion"], 1);
}

#[tokio::test]
async fn stream_skips_aggregate_and_non_event_records() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    let raw = serde_json::to_vec(&body).unwrap();
    let event = webhook_test_support::dynamodb_insert_event(&raw, "AGG", "seq-agg");

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    assert!(h.fake().published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn stream_reports_publish_failure_for_retry() {
    let h = harness().await;
    h.fake().fail_publish.store(true, Ordering::SeqCst);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    let raw = serde_json::to_vec(&body).unwrap();
    let event = webhook_test_support::dynamodb_insert_event(&raw, "EVT#t#sns-1", "seq-9");

    let result = invoke(h.state.clone(), event).await.unwrap();

    // The failed record's sequence number is returned so the ESM retries only
    // it (and, past the retry limit, routes it to the DLQ).
    assert_eq!(
        result,
        json!({ "batchItemFailures": [{ "itemIdentifier": "seq-9" }] })
    );
}

#[tokio::test]
async fn stream_publishes_status_change_on_transition() {
    let h = harness().await;
    let event = webhook_test_support::dynamodb_agg_event("MODIFY", Some("delivered"), Some("sent"));

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail_type, "message.status.changed");
    assert_eq!(published[0].detail["schemaVersion"], 1);
    assert_eq!(published[0].detail["meta"]["messageId"], "agg-1");
    assert_eq!(published[0].detail["status"]["current"], "delivered");
}

#[tokio::test]
async fn stream_publishes_initial_status_on_aggregate_insert() {
    let h = harness().await;
    let event = webhook_test_support::dynamodb_agg_event("INSERT", Some("sent"), None);

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail_type, "message.status.changed");
    assert_eq!(published[0].detail["status"]["current"], "sent");
}

#[tokio::test]
async fn stream_skips_status_event_when_status_unchanged() {
    let h = harness().await;
    // An open/click bumped counts but left current_status at "delivered".
    let event =
        webhook_test_support::dynamodb_agg_event("MODIFY", Some("delivered"), Some("delivered"));

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    assert!(h.fake().published.lock().unwrap().is_empty());
}
