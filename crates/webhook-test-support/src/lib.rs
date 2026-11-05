#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
// Pedantic doc and must_use lints assume a public API with its own contract
// to document; every item here exists only for a `#[tokio::test]` body to
// call directly and unwrap, so they add no signal. Declared at the crate
// root so they cover `objects` and `api_keys` too.
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
//! Shared handler-test infrastructure: one fake per seam, the request
//! harness, and the SNS-envelope / Lambda-invocation builders every handler
//! test suite drives. A dev-dependency crate (not a `tests/support` module)
//! so a test binary that doesn't use every helper isn't flagged for dead
//! code — the per-binary dead-code lint doesn't apply to a library's public
//! API. Depended on by every integration test binary in `crates/webhook`.

pub mod api;
pub mod api_keys;
pub mod objects;

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use api_keys::FakeApiKeys;
use aws_messaging_webhook::actions::{
    ActionError, ActionErrorKind, FeedbackStatus, RawSend, SendOutcome, SesApi, SmsVoiceApi,
    SuppressionReason,
};
use aws_messaging_webhook::allowlist::TopicAllowlist;
use aws_messaging_webhook::api::keys::KeyCache;
use aws_messaging_webhook::app::app;
use aws_messaging_webhook::config::{Config, FunctionMode, MailConfig};
use aws_messaging_webhook::entry::dispatch;
use aws_messaging_webhook::mail::fetch::{AttachmentFetcher, FetchError, Fetched};
use aws_messaging_webhook::mail::object_keys;
use aws_messaging_webhook::model::DomainEvent;
use aws_messaging_webhook::publish::{OutboundEvent, PublishError, PublishEvents};
use aws_messaging_webhook::state::{AppState, Backend};
use aws_messaging_webhook::store::{EventRecord, EventStore, PersistOutcome, StoreError};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use lambda_http::{Context, LambdaEvent};
use mailbox::InboxId;
use mailbox::send::SendSpec;
use mailbox_store::mailbox::Mailbox;
use mailbox_store::memory::{self as mail_memory, MailMemoryStore};
use objects::FakeObjectStore;
use serde_json::{Value, json};
use sns_message_verifier::SnsVerifier;
use sns_message_verifier::fixtures::SnsFixture;
use tower::ServiceExt as _;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub const ALLOWED_ACCOUNT: &str = "123456789012";

/// The test [`Backend`]: one in-process fake per seam.
pub struct Fakes;

impl Backend for Fakes {
    type Table = MailMemoryStore;
    type Events = FakeEvents;
    type Publisher = FakePublisher;
    type Sms = FakeSms;
    type Ses = FakeSes;
    type Objects = FakeObjectStore;
    type Fetcher = FakeFetcher;
    type Keys = FakeApiKeys;
}

/// The calls the event, publish, SMS and SES fakes receive, in order. One log
/// is shared by all four so a test can assert on how the calls interleave
/// (persist before publish, say).
#[derive(Debug, Clone, Default)]
pub struct CallLog(Arc<Mutex<Vec<String>>>);

impl CallLog {
    pub fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn record(&self, call: impl Into<String>) {
        self.0.lock().unwrap().push(call.into());
    }
}

/// The scripted outcome of an SMS or SES action call.
fn action_result(action_error: &Mutex<Option<ActionErrorKind>>) -> Result<(), ActionError> {
    match *action_error.lock().unwrap() {
        Some(ActionErrorKind::Transient) => Err(ActionError::transient(anyhow!(
            "simulated transient action failure"
        ))),
        Some(ActionErrorKind::Permanent) => Err(ActionError::permanent(anyhow!(
            "simulated permanent action failure"
        ))),
        None => Ok(()),
    }
}

/// The events table fake.
#[derive(Default)]
pub struct FakeEvents {
    calls: CallLog,
    pub persist_outcome: Mutex<Option<PersistOutcome>>,
    pub fail_persist: AtomicBool,
}

impl EventStore for FakeEvents {
    fn persist_new(
        &self,
        record: &EventRecord,
        _event: &DomainEvent,
    ) -> impl Future<Output = Result<PersistOutcome, StoreError>> + Send {
        self.calls
            .record(format!("persist:{}", record.aggregate_id));
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

/// The `EventBridge` fake.
#[derive(Default)]
pub struct FakePublisher {
    calls: CallLog,
    pub published: Mutex<Vec<OutboundEvent>>,
    pub fail_publish: AtomicBool,
    /// The 0-based publish attempt that fails, for a stream batch whose
    /// middle record fails while the others would succeed. Every other
    /// attempt follows `fail_publish`.
    pub fail_publish_at: Mutex<Option<usize>>,
    /// How many publishes have been attempted, failures included.
    pub publish_attempts: AtomicUsize,
}

impl PublishEvents for FakePublisher {
    fn publish(
        &self,
        event: &OutboundEvent,
    ) -> impl Future<Output = Result<(), PublishError>> + Send {
        self.calls.record(format!("publish:{}", event.detail_type));
        let attempt = self.publish_attempts.fetch_add(1, Ordering::SeqCst);
        let fails_here = *self.fail_publish_at.lock().unwrap() == Some(attempt);
        let result = if fails_here || self.fail_publish.load(Ordering::SeqCst) {
            Err(PublishError(anyhow!("simulated publish failure")))
        } else {
            self.published.lock().unwrap().push(event.clone());
            Ok(())
        };
        std::future::ready(result)
    }
}

/// The End User Messaging SMS fake.
#[derive(Default)]
pub struct FakeSms {
    calls: CallLog,
    pub action_error: Mutex<Option<ActionErrorKind>>,
}

impl SmsVoiceApi for FakeSms {
    fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.calls
            .record(format!("feedback:{message_id}:{status:?}"));
        std::future::ready(action_result(&self.action_error))
    }

    fn put_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.calls.record(format!("opt_out:{phone_number}"));
        std::future::ready(action_result(&self.action_error))
    }

    fn delete_opted_out_number(
        &self,
        _opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.calls.record(format!("opt_in:{phone_number}"));
        std::future::ready(action_result(&self.action_error))
    }
}

/// The SES fake.
#[derive(Default)]
pub struct FakeSes {
    calls: CallLog,
    /// The mail table, so a test can make recording an accepted send fail.
    mail: MailMemoryStore,
    pub action_error: Mutex<Option<ActionErrorKind>>,
    /// Email addresses whose suppression call fails with a permanent error,
    /// simulating SES `BadRequestException` for a malformed recipient. Other
    /// recipients still succeed — the action must continue past these.
    pub permanent_suppression_failures: Mutex<Vec<String>>,
    /// Every message handed to `send_raw`, in order.
    pub sent: Mutex<Vec<SentMessage>>,
    /// The outcome the next `send_raw` returns, consumed once. Defaults to a
    /// successful send.
    pub send_outcome: Mutex<Option<SendOutcome>>,
    /// How many transient mail-store failures to inject as `send_raw`
    /// returns, so a test can make recording an accepted send fail.
    pub store_failures_after_send: Mutex<usize>,
}

impl SesApi for FakeSes {
    fn send_raw(&self, request: &RawSend<'_>) -> impl Future<Output = SendOutcome> + Send {
        self.calls
            .record(format!("send_raw:{}", request.message_id));
        for _ in 0..std::mem::take(&mut *self.store_failures_after_send.lock().unwrap()) {
            self.mail.inject(mail_memory::Injected::Transient);
        }
        self.sent.lock().unwrap().push(SentMessage {
            message_id: request.message_id.to_owned(),
            raw: request.raw.to_vec(),
            to: request.to.to_vec(),
            cc: request.cc.to_vec(),
            bcc: request.bcc.to_vec(),
            from: request.from.to_owned(),
        });
        let outcome =
            self.send_outcome
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| SendOutcome::Sent {
                    ses_message_id: format!("ses-{}", request.message_id),
                });
        std::future::ready(outcome)
    }

    fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> impl Future<Output = Result<(), ActionError>> + Send {
        self.calls
            .record(format!("suppress:{email_address}:{reason:?}"));
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
            action_result(&self.action_error)
        };
        std::future::ready(result)
    }
}

/// One call to [`SesApi::send_raw`], captured for assertions.
#[derive(Debug, Clone)]
pub struct SentMessage {
    pub message_id: String,
    pub raw: Vec<u8>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub from: String,
}

impl SentMessage {
    /// The assembled message as text, for asserting on headers and bodies.
    #[must_use]
    pub fn raw_text(&self) -> String {
        String::from_utf8_lossy(&self.raw).into_owned()
    }
}

/// The attachment-URL fetcher fake.
#[derive(Default)]
pub struct FakeFetcher {
    /// URLs this fake serves, by URL string. A URL that is not here fails
    /// the way an unreachable host would.
    pub fetchable: Mutex<std::collections::HashMap<String, Vec<u8>>>,
    /// Every URL `fetch` was asked for, in order.
    pub fetched: Mutex<Vec<String>>,
}

impl FakeFetcher {
    /// Makes `url` fetchable with `body`.
    pub fn serve_url(&self, url: &str, body: &[u8]) {
        self.fetchable
            .lock()
            .unwrap()
            .insert(url.to_owned(), body.to_vec());
    }
}

impl AttachmentFetcher for FakeFetcher {
    fn fetch(
        &self,
        url: &mailbox::url_policy::AttachmentUrl,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Fetched, FetchError>> + Send {
        self.fetched.lock().unwrap().push(url.as_str().to_owned());
        let result = match self.fetchable.lock().unwrap().get(url.as_str()).cloned() {
            None => Err(FetchError::Rejected { status: 404 }),
            Some(body) if body.len() as u64 > max_bytes => Err(FetchError::TooLarge),
            Some(body) => Ok(Fetched { bytes: body }),
        };
        std::future::ready(result)
    }
}

pub struct Harness {
    pub state: Arc<AppState<Fakes>>,
    /// The log the event, publish, SMS and SES fakes record into.
    pub call_log: CallLog,
    /// The mail table behind `state.mailbox`, for seeding and inspecting
    /// items directly and scripting its failures.
    pub mail: MailMemoryStore,
    pub fixture: SnsFixture,
    pub cert_url: String,
    pub server: MockServer,
}

impl Harness {
    /// Every call the event, publish, SMS and SES fakes received, in order.
    pub fn calls(&self) -> Vec<String> {
        self.call_log.calls()
    }

    /// The stored spec the sender will build `message_id` from.
    pub fn send_spec(&self, message_id: &str) -> SendSpec {
        let bytes = self
            .state
            .objects
            .get(&object_keys::spec(message_id))
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}

/// A fresh state over new fakes, with the call log and mail table they
/// share.
pub fn fake_state(
    config: Config,
    verifier: SnsVerifier,
    allowlist: TopicAllowlist,
    dangerous_subscribe_url_prefix: Option<String>,
) -> (AppState<Fakes>, CallLog, MailMemoryStore) {
    let call_log = CallLog::default();
    let mail = MailMemoryStore::default();
    let state = AppState::<Fakes> {
        mailbox: Mailbox::new(mail.clone()),
        events: FakeEvents {
            calls: call_log.clone(),
            ..FakeEvents::default()
        },
        publisher: FakePublisher {
            calls: call_log.clone(),
            ..FakePublisher::default()
        },
        sms: FakeSms {
            calls: call_log.clone(),
            ..FakeSms::default()
        },
        ses: FakeSes {
            calls: call_log.clone(),
            mail: mail.clone(),
            ..FakeSes::default()
        },
        objects: FakeObjectStore::default(),
        fetcher: FakeFetcher::default(),
        key_source: FakeApiKeys::default(),
        api_keys: KeyCache::new(),
        verifier,
        allowlist,
        http: reqwest::Client::new(),
        config,
        dangerous_subscribe_url_prefix,
    };
    (state, call_log, mail)
}

/// A verifier that trusts certificates `server` serves.
pub fn test_verifier(server: &MockServer) -> SnsVerifier {
    SnsVerifier::builder()
        .dangerous_allow_cert_url_prefix(server.uri())
        .build()
        .unwrap()
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

/// The `Config` [`harness`] runs against: webhook mode with an opt-out list
/// and no mailbox.
#[must_use]
pub fn test_config() -> Config {
    Config {
        table_name: "events".to_owned(),
        event_bus_name: "bus".to_owned(),
        event_source: "aws-messaging-webhook".to_owned(),
        auto_resubscribe: true,
        opt_out_list_name: Some("opt-out-list".to_owned()),
        raw_event_retention_days: 30,
        aggregate_retention_days: 365,
        mode: FunctionMode::Webhook,
        mail: None,
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

    let (state, call_log, mail) = fake_state(
        Config {
            auto_resubscribe: options.auto_resubscribe,
            opt_out_list_name: options.opt_out_list.then(|| "opt-out-list".to_owned()),
            ..test_config()
        },
        test_verifier(&server),
        TopicAllowlist::parse(options.allowed_topics),
        Some(server.uri()),
    );
    Harness {
        state: Arc::new(state),
        call_log,
        mail,
        fixture,
        cert_url,
        server,
    }
}

pub async fn harness() -> Harness {
    harness_with(HarnessOptions::default()).await
}

/// The mail domain [`mail_harness`] configures.
pub const MAIL_DOMAIN: &str = "example.com";

/// The mail bucket [`mail_harness`] configures.
pub const MAIL_BUCKET: &str = "mail-bucket";

/// The `MailConfig` the mailbox tests run against.
#[must_use]
pub fn test_mail_config() -> MailConfig {
    MailConfig {
        domain: MAIL_DOMAIN.to_owned(),
        table_name: "mail-table".to_owned(),
        bucket: MAIL_BUCKET.to_owned(),
        inbox: InboxId::new(TEST_INBOX),
        configuration_set: "config-set".to_owned(),
        identity_arn: "arn:aws:ses:us-east-1:123456789012:identity/example.com".to_owned(),
        api_keys_parameter: "/example/api-keys".to_owned(),
        attachment_url_ttl: Duration::from_secs(3600),
        region: "us-east-1".to_owned(),
        retention_days: 365,
    }
}

/// A harness with the mailbox configured. [`harness`] leaves `mail` unset,
/// which is what a stack without `pMailDomain` looks like, so every mailbox
/// test needs this instead.
pub async fn mail_harness() -> Harness {
    let mut harness = harness_with(HarnessOptions::default()).await;
    let (state, call_log, mail) = fake_state(
        Config {
            mail: Some(test_mail_config()),
            ..harness.state.config.clone()
        },
        test_verifier(&harness.server),
        TopicAllowlist::parse(ALLOWED_ACCOUNT),
        Some(harness.server.uri()),
    );
    harness.state = Arc::new(state);
    harness.call_log = call_log;
    harness.mail = mail;
    harness
}

/// The bearer key [`grant_api_access`] authorizes.
pub const TEST_API_KEY: &str = "am_live_key";

/// The configured inbox, which [`grant_api_access`] creates.
pub const TEST_INBOX: &str = "support@example.com";

/// A fixed timestamp for messages a test seeds.
pub const TEST_AT: &str = "2026-01-01T09:00:00.000Z";

/// When [`grant_api_access`] creates [`TEST_INBOX`].
pub const TEST_INBOX_CREATED_AT: &str = "2026-01-01T00:00:00.000Z";

/// Authorizes [`TEST_API_KEY`] on `h` and creates [`TEST_INBOX`].
pub async fn grant_api_access(h: &Harness) {
    h.state.key_source.set_keys(&[(TEST_API_KEY, "key_1")]);
    h.state
        .mailbox
        .ensure_inbox(&InboxId::new(TEST_INBOX), TEST_INBOX_CREATED_AT)
        .await
        .unwrap();
}

/// A [`mail_harness`] with API access granted: an authorized key and an
/// existing inbox.
pub async fn api_harness() -> Harness {
    let h = mail_harness().await;
    grant_api_access(&h).await;
    h
}

pub async fn post(state: Arc<AppState<Fakes>>, route: &str, body: &Value) -> StatusCode {
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
    state: Arc<AppState<Fakes>>,
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
