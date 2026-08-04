#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use aws_messaging_webhook::actions::{
    ActionError, FeedbackStatus, SesApi, SmsVoiceApi, SuppressionReason,
};
use aws_messaging_webhook::allowlist::TopicAllowlist;
use aws_messaging_webhook::app::app;
use aws_messaging_webhook::config::Config;
use aws_messaging_webhook::model::DomainEvent;
use aws_messaging_webhook::publish::{OutboundEvent, PublishError, PublishEvents};
use aws_messaging_webhook::state::AppState;
use aws_messaging_webhook::store::{EventRecord, EventStore, PersistOutcome, StoreError};
use axum::body::Body;
use axum::http::{Request, StatusCode};
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
}

impl FakeServices {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn record(&self, call: impl Into<String>) {
        self.calls.lock().unwrap().push(call.into());
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
        Ok(())
    }

    async fn put_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.record(format!("opt_out:{phone_number}"));
        Ok(())
    }

    async fn delete_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.record(format!("opt_in:{phone_number}"));
        Ok(())
    }
}

impl SesApi for FakeServices {
    async fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> Result<(), ActionError> {
        self.record(format!("suppress:{email_address}:{reason:?}"));
        Ok(())
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
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            allowed_topics: ALLOWED_ACCOUNT,
            cert_fetches: None,
            auto_resubscribe: true,
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
            opt_out_list_name: Some("opt-out-list".to_owned()),
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
    assert_eq!(meta["webhookPath"], "/webhooks/ses/events");
    assert_eq!(published[0].detail["event"], json!({"hello": "world"}));
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
