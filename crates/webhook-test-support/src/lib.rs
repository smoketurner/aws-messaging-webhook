#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
// Pedantic doc and must_use lints assume a public API with its own contract
// to document; every item here exists only for a `#[tokio::test]` body to
// call directly and unwrap, so they add no signal. Declared at the crate
// root so they cover `mail_memory` and `objects` too.
#![expect(
    clippy::missing_panics_doc,
    reason = "test harness helpers: an internal unwrap is the intended fast-fail on setup failure, not documented API"
)]
#![expect(
    clippy::missing_errors_doc,
    reason = "test harness helpers: callers are `#[tokio::test]` bodies that unwrap the Result directly"
)]
#![expect(
    clippy::must_use_candidate,
    reason = "test harness helpers: callers always use the return value directly"
)]
//! Shared handler-test infrastructure: `FakeServices`, the request
//! harness, and the SNS-envelope / Lambda-invocation builders every handler
//! test suite drives. A dev-dependency crate (not a `tests/support` module)
//! so a test binary that doesn't use every helper isn't flagged for dead
//! code — the per-binary dead-code lint doesn't apply to a library's public
//! API. Depended on by every integration test binary in `crates/webhook`.

pub mod api_keys;
pub mod mail_memory;
pub mod objects;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use api_keys::FakeApiKeys;
use aws_messaging_webhook::actions::{
    ActionError, ActionErrorKind, FeedbackStatus, SesApi, SmsVoiceApi, SuppressionReason,
};
use aws_messaging_webhook::allowlist::TopicAllowlist;
use aws_messaging_webhook::api::keys::{ApiKeyError, ApiKeySource, KeyCache};
use aws_messaging_webhook::app::app;
use aws_messaging_webhook::config::{Config, FunctionMode};
use aws_messaging_webhook::entry::dispatch;
use aws_messaging_webhook::mail::keys::PageKey;
use aws_messaging_webhook::mail::objects::{ObjectError, ObjectStore};
use aws_messaging_webhook::mail::store::{ListQuery, Page, ThreadView};
use aws_messaging_webhook::mail::store::{MailStore, MailStoreError};
use aws_messaging_webhook::mail::thread::ThreadState;
use aws_messaging_webhook::mail::{
    Inbox, InboxId, InsertOutcome, MailMessage, ObjectMeta, PutOutcome, RfcHit,
};
use aws_messaging_webhook::model::DomainEvent;
use aws_messaging_webhook::publish::{OutboundEvent, PublishError, PublishEvents};
use aws_messaging_webhook::state::AppState;
use aws_messaging_webhook::store::{EventRecord, EventStore, PersistOutcome, StoreError};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use lambda_http::{Context, LambdaEvent};
use mail_memory::MailMemoryStore;
use objects::FakeObjectStore;
use serde_json::{Value, json};
use sns_message_verifier::SnsVerifier;
use sns_message_verifier::fixtures::SnsFixture;
use tower::ServiceExt as _;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub const ALLOWED_ACCOUNT: &str = "123456789012";

#[derive(Default)]
pub struct FakeServices {
    pub calls: Mutex<Vec<String>>,
    pub published: Mutex<Vec<OutboundEvent>>,
    pub persist_outcome: Mutex<Option<PersistOutcome>>,
    pub fail_persist: AtomicBool,
    pub fail_publish: AtomicBool,
    pub action_error: Mutex<Option<ActionErrorKind>>,
    /// Email addresses whose suppression call fails with a permanent error,
    /// simulating SES `BadRequestException` for a malformed recipient. Other
    /// recipients still succeed — the action must continue past these.
    pub permanent_suppression_failures: Mutex<Vec<String>>,
    /// In-memory mail store; `MailStore` is delegated to it below.
    pub mail: MailMemoryStore,
    /// Fake object store; `ObjectStore` is delegated to it below.
    pub objects: FakeObjectStore,
    /// The bearer keys the `/v0` surface authenticates against;
    /// `ApiKeySource` is delegated to it below. Unavailable until a test
    /// calls `set_keys`.
    pub api_keys: FakeApiKeys,
}

impl ApiKeySource for FakeServices {
    async fn fetch(&self) -> Result<String, ApiKeyError> {
        self.api_keys.fetch().await
    }
}

impl MailStore for FakeServices {
    async fn get_inbox(&self, inbox: &InboxId) -> Result<Option<Inbox>, MailStoreError> {
        self.mail.get_inbox(inbox).await
    }

    async fn ensure_inbox(&self, inbox: &InboxId, now: &str) -> Result<Inbox, MailStoreError> {
        self.mail.ensure_inbox(inbox, now).await
    }

    async fn message_exists(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> Result<bool, MailStoreError> {
        self.mail.message_exists(inbox, message_id).await
    }

    async fn resolve_rfc_ids(
        &self,
        inbox: &InboxId,
        candidates: &[String],
    ) -> Result<Option<RfcHit>, MailStoreError> {
        self.mail.resolve_rfc_ids(inbox, candidates).await
    }

    async fn insert_message(&self, msg: &MailMessage) -> Result<InsertOutcome, MailStoreError> {
        self.mail.insert_message(msg).await
    }

    async fn get_message(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> Result<Option<MailMessage>, MailStoreError> {
        self.mail.get_message(inbox, message_id).await
    }

    async fn list_inboxes(
        &self,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Page<Inbox>, MailStoreError> {
        self.mail.list_inboxes(limit, start).await
    }

    async fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &str,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> Result<Option<Vec<String>>, MailStoreError> {
        self.mail
            .update_labels(inbox, message_id, add, remove, now)
            .await
    }

    async fn list_messages(&self, query: &ListQuery) -> Result<Page<MailMessage>, MailStoreError> {
        self.mail.list_messages(query).await
    }

    async fn list_threads(&self, query: &ListQuery) -> Result<Page<ThreadState>, MailStoreError> {
        self.mail.list_threads(query).await
    }

    async fn get_thread(
        &self,
        inbox: &InboxId,
        thread_id: &str,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Option<ThreadView>, MailStoreError> {
        self.mail.get_thread(inbox, thread_id, limit, start).await
    }
}

impl ObjectStore for FakeServices {
    async fn get_object(&self, key: &str, max_bytes: u64) -> Result<Bytes, ObjectError> {
        self.objects.get_object(key, max_bytes).await
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectError> {
        self.objects.head_object(key).await
    }

    async fn put_object_if_absent(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<PutOutcome, ObjectError> {
        self.objects
            .put_object_if_absent(key, body, content_type)
            .await
    }
}

impl FakeServices {
    pub fn calls(&self) -> Vec<String> {
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
    fn persist_new(
        &self,
        record: &EventRecord,
        _event: &DomainEvent,
    ) -> impl Future<Output = Result<PersistOutcome, StoreError>> + Send {
        self.record(format!("persist:{}", record.aggregate_id));
        let result = if self.fail_persist.load(Ordering::SeqCst) {
            Err(StoreError(anyhow!("simulated persist failure")))
        } else {
            Ok(self
                .persist_outcome
                .lock()
                .unwrap()
                .unwrap_or(PersistOutcome::Fresh))
        };
        std::future::ready(result)
    }
}

impl PublishEvents for FakeServices {
    fn publish(
        &self,
        event: &OutboundEvent,
    ) -> impl Future<Output = Result<(), PublishError>> + Send {
        self.record(format!("publish:{}", event.detail_type));
        let result = if self.fail_publish.load(Ordering::SeqCst) {
            Err(PublishError(anyhow!("simulated publish failure")))
        } else {
            self.published.lock().unwrap().push(event.clone());
            Ok(())
        };
        std::future::ready(result)
    }
}

impl SmsVoiceApi for FakeServices {
    fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.record(format!("feedback:{message_id}:{status:?}"));
        std::future::ready(self.action_result())
    }

    fn put_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.record(format!("opt_out:{phone_number}"));
        std::future::ready(self.action_result())
    }

    fn delete_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.record(format!("opt_in:{phone_number}"));
        std::future::ready(self.action_result())
    }
}

impl SesApi for FakeServices {
    fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.record(format!("suppress:{email_address}:{reason:?}"));
        let result = if self
            .permanent_suppression_failures
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == email_address)
        {
            Err(ActionError::permanent(anyhow!(
                "simulated permanent suppression failure for {email_address}"
            )))
        } else {
            self.action_result()
        };
        std::future::ready(result)
    }
}

pub struct Harness {
    pub state: Arc<AppState<FakeServices>>,
    pub fixture: SnsFixture,
    pub cert_url: String,
    pub server: MockServer,
}

impl Harness {
    pub fn fake(&self) -> &FakeServices {
        &self.state.services
    }
}

pub struct HarnessOptions {
    pub allowed_topics: &'static str,
    /// `Some(n)`: assert exactly n certificate fetches at teardown.
    pub cert_fetches: Option<u64>,
    pub auto_resubscribe: bool,
    pub opt_out_list: bool,
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

pub async fn harness_with(options: HarnessOptions) -> Harness {
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
        api_keys: KeyCache::new(),
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
            aggregate_retention_days: 365,
            mode: FunctionMode::Webhook,
            mail: None,
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

pub async fn harness() -> Harness {
    harness_with(HarnessOptions::default()).await
}

pub async fn post(state: Arc<AppState<FakeServices>>, route: &str, body: &Value) -> StatusCode {
    let request = Request::post(route)
        .header("x-amz-sns-message-type", "Notification")
        .body(Body::from(body.to_string()))
        .unwrap();
    app(state).oneshot(request).await.unwrap().status()
}

/// Wraps an inner AWS payload as the SNS `Message` of a signed notification.
pub fn wrapped(h: &Harness, inner: &Value) -> Value {
    let mut body = sns_message_verifier::fixtures::notification(&h.cert_url);
    body["Message"] = json!(inner.to_string());
    h.fixture.sign(&mut body, "2");
    body
}

/// Re-keys a signed envelope to the casing a direct SNS → Lambda record uses.
pub fn lambda_record_casing(mut envelope: Value) -> Value {
    let record = envelope.as_object_mut().unwrap();
    if let Some(url) = record.remove("SigningCertURL") {
        record.insert("SigningCertUrl".to_owned(), url);
    }
    if let Some(url) = record.remove("UnsubscribeURL") {
        record.insert("UnsubscribeUrl".to_owned(), url);
    }
    envelope
}

pub fn direct_sns_record(envelope: &Value) -> Value {
    json!({
        "EventSource": "aws:sns",
        "EventVersion": "1.0",
        "EventSubscriptionArn":
            "arn:aws:sns:us-east-1:123456789012:test-topic:11111111-2222-3333-4444-555555555555",
        "Sns": lambda_record_casing(envelope.clone()),
    })
}

pub fn direct_sns_event(envelope: &Value) -> Value {
    json!({ "Records": [direct_sns_record(envelope)] })
}

/// A Function URL invocation payload (API Gateway v2 shape) that POSTs `body`.
pub fn function_url_event(path: &str, body: &Value) -> Value {
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

/// A `Context` with a deadline 60 s in the future, rather than
/// `Context::default()`'s zero deadline — an already-elapsed deadline would
/// make every time-boxed action look instantly expired.
pub fn test_context() -> Context {
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    // `Context` is `#[non_exhaustive]`, so even `..Default::default()` struct
    // update syntax is rejected outside its crate — mutate the field instead.
    let mut ctx = Context::default();
    ctx.deadline = now_ms + 60_000;
    ctx
}

/// Drives the single-binary entry point exactly as the Lambda runtime would.
pub async fn invoke(
    state: Arc<AppState<FakeServices>>,
    payload: Value,
) -> Result<Value, lambda_http::Error> {
    let router: Router = app(state.clone());
    dispatch(state, router, LambdaEvent::new(payload, test_context())).await
}

/// A DynamoDB Streams INSERT event carrying one event item whose `raw_body`
/// is the given (base64-encoded) SNS envelope bytes.
pub fn dynamodb_insert_event(raw_body: &[u8], sk: &str, sequence: &str) -> Value {
    json!({
        "Records": [{
            "awsRegion": "us-east-1",
            "eventID": "evt-1",
            "eventName": "INSERT",
            "eventSource": "aws:dynamodb",
            "dynamodb": {
                "ApproximateCreationDateTime": 1_754_265_600.0,
                "SequenceNumber": sequence,
                "SizeBytes": 42,
                "StreamViewType": "NEW_IMAGE",
                "NewImage": {
                    "pk": {"S": "MSG#agg-1"},
                    "sk": {"S": sk},
                    "received_at": {"S": "2026-08-04T00:00:00.000Z"},
                    "raw_body": {"B": aws_smithy_types::base64::encode(raw_body)},
                }
            }
        }]
    })
}

/// A DynamoDB Streams event for one aggregate (`sk = AGG`) record with the given
/// new/old `current_status` values (None = attribute absent).
pub fn dynamodb_agg_event(
    event_name: &str,
    new_status: Option<&str>,
    old_status: Option<&str>,
) -> Value {
    let mut new_image = json!({
        "pk": {"S": "MSG#agg-1"},
        "sk": {"S": "AGG"},
        "source": {"S": "ses-events"},
        "open_count": {"N": "2"},
    });
    if let Some(status) = new_status {
        new_image["current_status"] = json!({"S": status});
    }
    let mut old_image = json!({ "pk": {"S": "MSG#agg-1"}, "sk": {"S": "AGG"} });
    if let Some(status) = old_status {
        old_image["current_status"] = json!({"S": status});
    }
    json!({
        "Records": [{
            "awsRegion": "us-east-1",
            "eventID": "evt-agg",
            "eventName": event_name,
            "eventSource": "aws:dynamodb",
            "dynamodb": {
                "ApproximateCreationDateTime": 1_754_265_600.0,
                "SequenceNumber": "seq-agg",
                "SizeBytes": 20,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
                "NewImage": new_image,
                "OldImage": old_image,
            }
        }]
    })
}
