#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use aws_messaging_webhook::actions::{
    ActionError, ActionErrorKind, FeedbackStatus, SesApi, SmsVoiceApi, SuppressionReason,
};
use aws_messaging_webhook::allowlist::TopicAllowlist;
use aws_messaging_webhook::app::app;
use aws_messaging_webhook::config::Config;
use aws_messaging_webhook::entry::dispatch;
use aws_messaging_webhook::model::DomainEvent;
use aws_messaging_webhook::publish::{OutboundEvent, PublishError, PublishEvents};
use aws_messaging_webhook::state::AppState;
use aws_messaging_webhook::store::{EventRecord, EventStore, PersistOutcome, StoreError};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use lambda_http::{Context, LambdaEvent};
use serde_json::{Value, json};
use sns_message_verifier::SnsVerifier;
use sns_message_verifier::fixtures::{SnsFixture, notification, subscription_confirmation};
use tower::ServiceExt as _;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ALLOWED_ACCOUNT: &str = "123456789012";

#[derive(Default)]
struct FakeServices {
    calls: Mutex<Vec<String>>,
    published: Mutex<Vec<OutboundEvent>>,
    persist_outcome: Mutex<Option<PersistOutcome>>,
    fail_persist: AtomicBool,
    fail_publish: AtomicBool,
    fail_mark: AtomicBool,
    action_error: Mutex<Option<ActionErrorKind>>,
}

impl FakeServices {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn record(&self, call: impl Into<String>) {
        self.calls.lock().unwrap().push(call.into());
    }

    fn action_result(&self) -> Result<(), ActionError> {
        match *self.action_error.lock().unwrap() {
            Some(ActionErrorKind::Transient) => Err(ActionError::transient(anyhow!(
                "simulated transient action failure"
            ))),
            Some(ActionErrorKind::Permanent) => Err(ActionError::permanent(anyhow!(
                "simulated permanent action failure"
            ))),
            None => Ok(()),
        }
    }
}

impl EventStore for FakeServices {
    async fn persist_new(
        &self,
        record: &EventRecord,
        _event: &DomainEvent,
    ) -> Result<PersistOutcome, StoreError> {
        self.record(format!("persist:{}", record.aggregate_id));
        if self.fail_persist.load(Ordering::SeqCst) {
            return Err(StoreError(anyhow!("simulated persist failure")));
        }
        Ok(self
            .persist_outcome
            .lock()
            .unwrap()
            .unwrap_or(PersistOutcome::Fresh))
    }

    async fn mark_published(&self, _record: &EventRecord) -> Result<(), StoreError> {
        self.record("mark");
        if self.fail_mark.load(Ordering::SeqCst) {
            return Err(StoreError(anyhow!("simulated mark failure")));
        }
        Ok(())
    }
}

impl PublishEvents for FakeServices {
    async fn publish(&self, event: &OutboundEvent) -> Result<(), PublishError> {
        self.record(format!("publish:{}", event.detail_type));
        if self.fail_publish.load(Ordering::SeqCst) {
            return Err(PublishError(anyhow!("simulated publish failure")));
        }
        self.published.lock().unwrap().push(event.clone());
        Ok(())
    }
}

impl SmsVoiceApi for FakeServices {
    async fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> Result<(), ActionError> {
        self.record(format!("feedback:{message_id}:{status:?}"));
        self.action_result()
    }

    async fn put_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.record(format!("opt_out:{phone_number}"));
        self.action_result()
    }

    async fn delete_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.record(format!("opt_in:{phone_number}"));
        self.action_result()
    }
}

impl SesApi for FakeServices {
    async fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> Result<(), ActionError> {
        self.record(format!("suppress:{email_address}:{reason:?}"));
        self.action_result()
    }
}

struct Harness {
    state: Arc<AppState<FakeServices>>,
    fixture: SnsFixture,
    cert_url: String,
    server: MockServer,
}

impl Harness {
    fn fake(&self) -> &FakeServices {
        &self.state.services
    }
}

struct HarnessOptions {
    allowed_topics: &'static str,
    /// `Some(n)`: assert exactly n certificate fetches at teardown.
    cert_fetches: Option<u64>,
    auto_resubscribe: bool,
    opt_out_list: bool,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            allowed_topics: ALLOWED_ACCOUNT,
            cert_fetches: None,
            auto_resubscribe: true,
            opt_out_list: true,
        }
    }
}

async fn harness_with(options: HarnessOptions) -> Harness {
    let fixture = SnsFixture::new();
    let server = MockServer::start().await;
    let cert_mock = Mock::given(method("GET"))
        .and(path("/cert.pem"))
        .respond_with(ResponseTemplate::new(200).set_body_string(&fixture.cert_pem));
    let cert_mock = match options.cert_fetches {
        Some(count) => cert_mock.expect(count),
        None => cert_mock,
    };
    cert_mock.mount(&server).await;
    let cert_url = format!("{}/cert.pem", server.uri());

    let state = Arc::new(AppState {
        services: FakeServices::default(),
        verifier: SnsVerifier::builder()
            .dangerous_allow_cert_url_prefix(server.uri())
            .build()
            .unwrap(),
        allowlist: TopicAllowlist::parse(options.allowed_topics),
        http: reqwest::Client::new(),
        config: Config {
            table_name: "events".to_owned(),
            event_bus_name: "bus".to_owned(),
            event_source: "aws-messaging-webhook".to_owned(),
            auto_resubscribe: options.auto_resubscribe,
            opt_out_list_name: options.opt_out_list.then(|| "opt-out-list".to_owned()),
            raw_event_retention_days: 30,
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

async fn harness() -> Harness {
    harness_with(HarnessOptions::default()).await
}

async fn post(state: Arc<AppState<FakeServices>>, route: &str, body: &Value) -> StatusCode {
    let request = Request::post(route)
        .header("x-amz-sns-message-type", "Notification")
        .body(Body::from(body.to_string()))
        .unwrap();
    app(state).oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn valid_notification_persists_then_publishes_then_marks() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned(),
            "publish:unknown".to_owned(),
            "mark".to_owned(),
        ]
    );
    let published = h.fake().published.lock().unwrap();
    let meta = &published[0].detail["meta"];
    assert_eq!(meta["snsMessageId"], "165545c9-2a5c-472c-8df2-7ff2be2b3b1b");
    assert_eq!(meta["messageId"], "165545c9-2a5c-472c-8df2-7ff2be2b3b1b");
    // The payload matches no family, so it has no canonical path.
    assert_eq!(meta["webhookPath"], Value::Null);
    assert_eq!(published[0].detail["event"], json!({"hello": "world"}));
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
    assert!(h.fake().calls().contains(&"publish:sms.inbound".to_owned()));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(
        published[0].detail["meta"]["webhookPath"],
        "/webhooks/sms/inbound"
    );
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
async fn duplicate_published_is_a_no_op() {
    let h = harness().await;
    *h.fake().persist_outcome.lock().unwrap() = Some(PersistOutcome::DuplicatePublished);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/sms/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls().len(),
        1,
        "only the persist attempt, no publish/mark"
    );
}

#[tokio::test]
async fn duplicate_persisted_resumes_publish_and_mark() {
    let h = harness().await;
    *h.fake().persist_outcome.lock().unwrap() = Some(PersistOutcome::DuplicatePersisted);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/sms/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls[1].starts_with("publish:"),
        "resume must re-publish, got {calls:?}"
    );
    assert_eq!(calls[2], "mark");
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
async fn publish_failure_returns_500_and_never_marks() {
    let h = harness().await;
    h.fake().fail_publish.store(true, Ordering::SeqCst);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let calls = h.fake().calls();
    assert!(
        !calls.contains(&"mark".to_owned()),
        "mark must not run, got {calls:?}"
    );
}

#[tokio::test]
async fn mark_failure_returns_500_after_successful_publish() {
    let h = harness().await;
    h.fake().fail_mark.store(true, Ordering::SeqCst);
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "1");

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(h.fake().published.lock().unwrap().len(), 1);
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

/// Wraps an inner AWS payload as the SNS `Message` of a signed notification.
fn wrapped(h: &Harness, inner: &Value) -> Value {
    let mut body = notification(&h.cert_url);
    body["Message"] = json!(inner.to_string());
    h.fixture.sign(&mut body, "2");
    body
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
async fn stop_keyword_opts_out_and_still_publishes() {
    let h = harness().await;
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:in-msg-1".to_owned(),
            "opt_out:+14255550182".to_owned(),
            "publish:sms.inbound".to_owned(),
            "mark".to_owned(),
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
    assert!(calls.contains(&"publish:sms.inbound".to_owned()));
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
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:in-msg-2".to_owned(),
            "publish:sms.inbound".to_owned(),
            "mark".to_owned(),
        ]
    );
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
    assert!(calls.contains(&"publish:sms.delivery".to_owned()));
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
    assert!(calls.contains(&"publish:ses.bounce".to_owned()));
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
    assert!(calls.contains(&"publish:ses.bounce".to_owned()));
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
async fn clean_inbound_email_publishes_ses_inbound() {
    let h = harness().await;
    let body = wrapped(&h, &ses_inbound("PASS", Some("Subject: hi\r\n\r\nhello")));

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(h.fake().calls().contains(&"publish:ses.inbound".to_owned()));
}

#[tokio::test]
async fn virus_fail_publishes_quarantined_detail_type() {
    let h = harness().await;
    let body = wrapped(&h, &ses_inbound("FAIL", None));

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        h.fake()
            .calls()
            .contains(&"publish:ses.inbound.quarantined".to_owned())
    );
}

#[tokio::test]
async fn oversized_inbound_content_is_stripped_from_bus_event() {
    let h = harness().await;
    let big = "x".repeat(300_000);
    let body = wrapped(&h, &ses_inbound("PASS", Some(&big)));

    let status = post(h.state.clone(), "/webhooks/ses/inbound", &body).await;

    assert_eq!(status, StatusCode::OK);
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published[0].detail["event"]["content"], Value::Null);
    assert_eq!(
        published[0].detail["event"]["mail"]["messageId"],
        "inbound-msg-1"
    );
}

#[tokio::test]
async fn transient_action_failure_returns_500_before_publish() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Transient);
    let body = wrapped(&h, &inbound_sms("STOP"));

    let status = post(h.state.clone(), "/webhooks/sms/inbound", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let calls = h.fake().calls();
    assert!(calls.contains(&"opt_out:+14255550182".to_owned()));
    assert!(!calls.iter().any(|c| c.starts_with("publish")), "{calls:?}");
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

    // SNS redelivers; the prior attempt persisted, so this resumes.
    *h.fake().action_error.lock().unwrap() = None;
    *h.fake().persist_outcome.lock().unwrap() = Some(PersistOutcome::DuplicatePersisted);
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
    assert!(h.fake().calls().contains(&"publish:sms.inbound".to_owned()));
}

#[tokio::test]
async fn permanent_action_failure_still_publishes() {
    let h = harness().await;
    *h.fake().action_error.lock().unwrap() = Some(ActionErrorKind::Permanent);
    let body = wrapped(&h, &ses_bounce("Permanent", &["a@example.com"]));

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(calls.contains(&"suppress:a@example.com:Bounce".to_owned()));
    assert!(calls.contains(&"publish:ses.bounce".to_owned()));
    assert!(calls.contains(&"mark".to_owned()));
}

#[tokio::test]
async fn transient_cert_fetch_failure_returns_500_not_403() {
    // A cold-start cert fetch that fails transiently must not be a permanent
    // 4xx — that would make SNS drop a correctly-signed message. The verifier
    // has no cached cert and the cert server 500s, so verify() -> CertFetch.
    let fixture = SnsFixture::new();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/cert.pem"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let cert_url = format!("{}/cert.pem", server.uri());
    let state = Arc::new(AppState {
        services: FakeServices::default(),
        verifier: SnsVerifier::builder()
            .dangerous_allow_cert_url_prefix(server.uri())
            .build()
            .unwrap(),
        allowlist: TopicAllowlist::parse(ALLOWED_ACCOUNT),
        http: reqwest::Client::new(),
        config: Config {
            table_name: "events".to_owned(),
            event_bus_name: "bus".to_owned(),
            event_source: "aws-messaging-webhook".to_owned(),
            auto_resubscribe: true,
            opt_out_list_name: Some("opt-out-list".to_owned()),
            raw_event_retention_days: 30,
        },
        dangerous_subscribe_url_prefix: Some(server.uri()),
    });
    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");

    let status = post(state, "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// Re-keys a signed envelope to the casing a direct SNS → Lambda record uses.
fn lambda_record_casing(mut envelope: Value) -> Value {
    let record = envelope.as_object_mut().unwrap();
    if let Some(url) = record.remove("SigningCertURL") {
        record.insert("SigningCertUrl".to_owned(), url);
    }
    if let Some(url) = record.remove("UnsubscribeURL") {
        record.insert("UnsubscribeUrl".to_owned(), url);
    }
    envelope
}

fn direct_sns_record(envelope: &Value) -> Value {
    json!({
        "EventSource": "aws:sns",
        "EventVersion": "1.0",
        "EventSubscriptionArn":
            "arn:aws:sns:us-east-1:123456789012:test-topic:11111111-2222-3333-4444-555555555555",
        "Sns": lambda_record_casing(envelope.clone()),
    })
}

fn direct_sns_event(envelope: &Value) -> Value {
    json!({ "Records": [direct_sns_record(envelope)] })
}

/// A Function URL invocation payload (API Gateway v2 shape) that POSTs `body`.
fn function_url_event(path: &str, body: &Value) -> Value {
    json!({
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": "",
        "headers": {
            "content-type": "text/plain; charset=UTF-8",
            "x-amz-sns-message-type": "Notification",
        },
        "requestContext": {
            "accountId": "anonymous",
            "apiId": "url-id",
            "domainName": "url-id.lambda-url.us-east-1.on.aws",
            "domainPrefix": "url-id",
            "http": {
                "method": "POST",
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": "10.0.0.1",
                "userAgent": "Amazon Simple Notification Service Agent",
            },
            "requestId": "request-id",
            "routeKey": "$default",
            "stage": "$default",
            "time": "04/Aug/2026:00:00:00 +0000",
            "timeEpoch": 1_754_265_600_000_i64,
        },
        "body": body.to_string(),
        "isBase64Encoded": false,
    })
}

/// Drives the single-binary entry point exactly as the Lambda runtime would.
async fn invoke(
    state: Arc<AppState<FakeServices>>,
    payload: Value,
) -> Result<Value, lambda_http::Error> {
    let router = app(state.clone());
    dispatch(state, router, LambdaEvent::new(payload, Context::default())).await
}

#[tokio::test]
async fn direct_invoke_runs_the_full_pipeline() {
    let h = harness().await;
    let mut body = notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");

    let result = invoke(h.state.clone(), direct_sns_event(&body))
        .await
        .unwrap();

    assert_eq!(result, Value::Null);
    assert_eq!(
        h.fake().calls(),
        vec![
            "persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned(),
            "publish:unknown".to_owned(),
            "mark".to_owned(),
        ]
    );
    let published = h.fake().published.lock().unwrap();
    // The fixture message matches no family; it still publishes as unknown.
    assert_eq!(published[0].detail["meta"]["webhookPath"], Value::Null);
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

    assert!(h.fake().calls().contains(&"publish:sms.inbound".to_owned()));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(
        published[0].detail["meta"]["webhookPath"],
        "/webhooks/sms/inbound"
    );
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
        vec![
            "persist:165545c9-2a5c-472c-8df2-7ff2be2b3b1b".to_owned(),
            "publish:unknown".to_owned(),
            "mark".to_owned(),
        ]
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
async fn oversized_non_content_payload_publishes_a_pointer_not_a_poison_message() {
    let h = harness().await;
    // A large SES event with no `content` field to strip: must still publish
    // (as a bounded pointer) rather than 5xx-loop forever.
    let big_reason = "x".repeat(300_000);
    let inner = json!({
        "eventType": "Bounce",
        "bounce": {"bounceType": "Transient", "bouncedRecipients": [], "note": big_reason},
        "mail": {"messageId": "huge-1"}
    });
    let body = wrapped(&h, &inner);

    let status = post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published[0].detail["event"]["payloadOmitted"], json!(true));
    // Meta is always preserved so consumers can fetch the full record.
    assert_eq!(published[0].detail["meta"]["messageId"], "huge-1");
}
