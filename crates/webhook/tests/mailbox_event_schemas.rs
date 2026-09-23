#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test code panics on setup failure"
)]
//! Every mailbox event the relay publishes, checked against the reference
//! mailbox API's published webhook schemas in
//! `tests/fixtures/mailbox-events/schemas.json`.
//!
//! The payloads come from the real paths: a send through the API and the
//! sender, the documented AWS SES examples in `tests/fixtures/ses-events/`
//! fed through the events-table relay, and received messages through the
//! mail-table relay. The check is stricter than JSON Schema's default: a
//! property the schema does not define fails, as does any schema keyword the
//! checker does not implement, so a payload passes only if it is exactly the
//! documented shape.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use aws_messaging_webhook::mail::content::{self, MessageContent};
use aws_messaging_webhook::mail::sender::handle_send;
use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{
    AttachmentMeta, Direction, InboxId, MailMessage, ThreadSnapshot, keys, thread,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_dynamo::AttributeValue;
use serde_json::{Map, Value, json};
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::ReadFailure;
use webhook_test_support::{Harness, invoke, mail_harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support@example.com";

/// The schema keywords [`check`] implements. Anything else in a schema fails
/// the run rather than being silently ignored.
const SUPPORTED_KEYWORDS: [&str; 10] = [
    "$ref",
    "type",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "format",
    "title",
    "description",
];

fn schemas() -> Map<String, Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mailbox-events/schemas.json");
    let document: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    document["components"]["schemas"]
        .as_object()
        .unwrap()
        .clone()
}

/// The schema that describes an event type's payload.
fn schema_for(event_type: &str) -> &'static str {
    match event_type {
        "message.received"
        | "message.received.spam"
        | "message.received.unauthenticated"
        | "message.received.blocked" => "events_MessageReceivedEvent",
        "message.sent" => "events_MessageSentEvent",
        "message.delivered" => "events_MessageDeliveredEvent",
        "message.bounced" => "events_MessageBouncedEvent",
        "message.complained" => "events_MessageComplainedEvent",
        "message.rejected" => "events_MessageRejectedEvent",
        "message.opened" => "events_MessageOpenedEvent",
        "domain.verified" => "events_DomainVerifiedEvent",
        other => panic!("no schema for {other}"),
    }
}

/// An RFC 3339 `date-time`: `YYYY-MM-DDTHH:MM:SS`, optional fraction, then
/// `Z` or a `±HH:MM` offset.
fn is_date_time(value: &str) -> bool {
    let bytes = value.as_bytes();
    let digits = |range: std::ops::Range<usize>| {
        bytes
            .get(range)
            .is_some_and(|part| part.iter().all(u8::is_ascii_digit))
    };
    if !(digits(0..4)
        && bytes.get(4) == Some(&b'-')
        && digits(5..7)
        && bytes.get(7) == Some(&b'-')
        && digits(8..10)
        && matches!(bytes.get(10), Some(b'T' | b't'))
        && digits(11..13)
        && bytes.get(13) == Some(&b':')
        && digits(14..16)
        && bytes.get(16) == Some(&b':')
        && digits(17..19))
    {
        return false;
    }
    let mut rest = &value[19..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let len = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if len == 0 {
            return false;
        }
        rest = &fraction[len..];
    }
    match rest.as_bytes() {
        [b'Z' | b'z'] => true,
        [b'+' | b'-', h1, h2, b':', m1, m2] => [h1, h2, m1, m2].iter().all(|b| b.is_ascii_digit()),
        _ => false,
    }
}

/// Collects every way `value` departs from `schema` into `errors`.
fn check(
    value: &Value,
    schema: &Value,
    schemas: &Map<String, Value>,
    path: &str,
    errors: &mut Vec<String>,
) {
    let schema = schema.as_object().unwrap();
    for keyword in schema.keys() {
        assert!(
            SUPPORTED_KEYWORDS.contains(&keyword.as_str()),
            "schema keyword `{keyword}` at {path} is not implemented by this checker"
        );
    }
    if let Some(reference) = schema.get("$ref") {
        let name = reference
            .as_str()
            .and_then(|r| r.strip_prefix("#/components/schemas/"))
            .unwrap();
        check(value, &schemas[name], schemas, path, errors);
        return;
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        errors.push(format!("{path}: {value} is not one of {allowed:?}"));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("string") => match value.as_str() {
            Some(text) => {
                if schema.get("format").and_then(Value::as_str) == Some("date-time")
                    && !is_date_time(text)
                {
                    errors.push(format!("{path}: {text:?} is not an RFC 3339 date-time"));
                }
            }
            None => errors.push(format!("{path}: expected a string, got {value}")),
        },
        Some("integer") => {
            if !(value.is_u64() || value.is_i64()) {
                errors.push(format!("{path}: expected an integer, got {value}"));
            }
        }
        Some("boolean") => {
            if !value.is_boolean() {
                errors.push(format!("{path}: expected a boolean, got {value}"));
            }
        }
        Some("array") => match value.as_array() {
            Some(items) => {
                for (index, item) in items.iter().enumerate() {
                    check(
                        item,
                        &schema["items"],
                        schemas,
                        &format!("{path}[{index}]"),
                        errors,
                    );
                }
            }
            None => errors.push(format!("{path}: expected an array, got {value}")),
        },
        Some("object") => {
            let Some(object) = value.as_object() else {
                errors.push(format!("{path}: expected an object, got {value}"));
                return;
            };
            for required in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let required = required.as_str().unwrap();
                if !object.contains_key(required) {
                    errors.push(format!("{path}: missing required `{required}`"));
                }
            }
            let properties = schema.get("properties").and_then(Value::as_object);
            for (key, field) in object {
                let field_path = format!("{path}.{key}");
                match (
                    properties.and_then(|p| p.get(key)),
                    schema.get("additionalProperties"),
                ) {
                    (Some(field_schema), _) | (None, Some(field_schema)) => {
                        check(field, field_schema, schemas, &field_path, errors);
                    }
                    (None, None) => {
                        errors.push(format!("{field_path}: not defined by the schema"));
                    }
                }
            }
        }
        other => panic!("schema at {path} has unsupported type {other:?}"),
    }
}

/// Every departure of an event detail from its event type's schema.
fn violations(detail: &Value) -> Vec<String> {
    let schemas = schemas();
    let event_type = detail["event_type"].as_str().unwrap();
    let mut errors = Vec::new();
    check(
        detail,
        &json!({ "$ref": format!("#/components/schemas/{}", schema_for(event_type)) }),
        &schemas,
        "$",
        &mut errors,
    );
    errors
}

fn assert_matches_schema(detail: &Value) {
    let errors = violations(detail);
    assert!(
        errors.is_empty(),
        "{} does not match its schema:\n{}\n{detail:#}",
        detail["event_type"],
        errors.join("\n")
    );
}

/// The `message.*` details published so far, in order.
fn mailbox_details(h: &Harness) -> Vec<Value> {
    h.fake()
        .published
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.detail_type.starts_with("message.")
                && event.detail_type != "message.status.changed"
        })
        .map(|event| event.detail.clone())
        .collect()
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

/// Sends one message through the real API and sender and returns it as
/// stored.
async fn sent_message(h: &Harness) -> MailMessage {
    let body = json!({
        "to": ["a@example.net", "Bee <b@example.net>"],
        "cc": "c@example.net",
        "bcc": "d@example.net",
        "subject": "Hello",
        "text": "the body",
    });
    let request = Request::post(format!("/v0/inboxes/{INBOX}/messages/send"))
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let sent: Value = serde_json::from_slice(&bytes).unwrap();
    let message_id = sent["message_id"].as_str().unwrap();
    handle_send(
        &h.state,
        message_id,
        tokio::time::Instant::now() + std::time::Duration::from_secs(120),
    )
    .await
    .unwrap();
    h.state
        .services
        .get_message(&InboxId(INBOX.to_owned()), message_id)
        .await
        .unwrap()
        .unwrap()
}

/// A message item's stream image, as the store writes it.
fn message_image(msg: &MailMessage) -> Value {
    let mut item: serde_dynamo::Item = serde_dynamo::to_item(msg).unwrap();
    item.insert(
        "pk".to_owned(),
        AttributeValue::S(keys::inbox_pk(msg.inbox_id.as_str())),
    );
    item.insert(
        "sk".to_owned(),
        AttributeValue::S(keys::message_sk(&msg.message_id)),
    );
    serde_json::to_value(&item).unwrap()
}

fn mail_record(event_name: &str, new_image: &Value, old_image: Option<&Value>) -> Value {
    let mut change = json!({
        "ApproximateCreationDateTime": 1_754_265_600.0,
        "SequenceNumber": "seq-1",
        "SizeBytes": 42,
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "NewImage": new_image,
    });
    if let Some(old_image) = old_image {
        change["OldImage"] = old_image.clone();
    }
    json!({
        "Records": [{
            "awsRegion": "us-east-1",
            "eventID": "evt-mail-1",
            "eventName": event_name,
            "eventSource": "aws:dynamodb",
            "dynamodb": change,
        }]
    })
}

/// A received message with every optional field populated, so the check
/// covers each one's shape.
fn received_message(labels: &[&str]) -> MailMessage {
    let now = "2026-01-15T09:30:00.000Z".to_owned();
    let mut msg = MailMessage {
        inbox_id: InboxId(INBOX.to_owned()),
        thread_id: "tid-1".to_owned(),
        message_id: "mid-1".to_owned(),
        ses_message_id: Some("ses-in-1".to_owned()),
        direction: Direction::Inbound,
        rfc_message_id: "<orig-1@example.net>".to_owned(),
        in_reply_to: Some("<earlier@example.net>".to_owned()),
        labels: labels.iter().map(|label| (*label).to_owned()).collect(),
        timestamp: now.clone(),
        from: "Sender <sender@example.net>".to_owned(),
        to: vec![INBOX.to_owned()],
        cc: vec!["cc@example.net".to_owned()],
        bcc: Vec::new(),
        subject: "Hello".to_owned(),
        preview: "Hello there".to_owned(),
        size: 4096,
        attachments: vec![AttachmentMeta {
            attachment_id: "att-1".to_owned(),
            object_key: Some("attachments/mid-1/att-1".to_owned()),
            size: 10,
            filename: Some("notes.txt".to_owned()),
            content_type: "text/plain".to_owned(),
            content_disposition: "inline".to_owned(),
            content_id: Some("<notes@example.net>".to_owned()),
        }],
        attachments_truncated: false,
        raw_s3_key: Some("inbound/mid-1".to_owned()),
        thread_snapshot: None,
        delivery: std::collections::BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 1,
        created_at: now.clone(),
        updated_at: now,
        expires_at: 0,
    };
    msg.thread_snapshot = Some(ThreadSnapshot::from(&thread::new_thread(&msg)));
    msg
}

async fn publish_received(h: &Harness, msg: &MailMessage) {
    content::store(
        h.fake(),
        &msg.inbox_id,
        &msg.message_id,
        &MessageContent {
            text: Some("Hello there".to_owned()),
            html: Some("<p>Hello there</p>".to_owned()),
            reply_to: vec!["replies@example.net".to_owned()],
            references: vec!["<earlier@example.net>".to_owned()],
            headers: std::collections::BTreeMap::from([("X-Mailer".to_owned(), "test".to_owned())]),
            ..MessageContent::default()
        },
    )
    .await
    .unwrap();
    let result = invoke(
        h.state.clone(),
        mail_record("INSERT", &message_image(msg), None),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "batchItemFailures": [] }));
}

/// The documented SES example `name`, re-addressed to `ses_message_id`,
/// fed through the events-table relay.
async fn publish_ses_example(h: &Harness, name: &str, ses_message_id: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ses-events")
        .join(name);
    let mut example: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    example["mail"]["messageId"] = json!(ses_message_id);
    let envelope = webhook_test_support::wrapped(h, &example);
    let raw = serde_json::to_vec(&envelope).unwrap();
    let record = webhook_test_support::dynamodb_insert_event(
        &raw,
        "EVT#2026-01-15T09:46:00.000Z#sns-1",
        "seq-ses",
    );
    let result = invoke(h.state.clone(), record).await.unwrap();
    assert_eq!(result, json!({ "batchItemFailures": [] }), "{name}");
}

#[tokio::test]
async fn received_events_match_the_schema() {
    for (labels, event_type) in [
        (vec!["received", "unread"], "message.received"),
        (vec!["received", "unread", "spam"], "message.received.spam"),
        (
            vec!["received", "unread", "unauthenticated"],
            "message.received.unauthenticated",
        ),
    ] {
        let h = seeded().await;
        publish_received(&h, &received_message(&labels)).await;
        let details = mailbox_details(&h);
        assert_eq!(details.len(), 1, "{event_type}");
        assert_eq!(details[0]["event_type"], event_type);
        assert_matches_schema(&details[0]);
        // Every optional field was populated, so each was checked.
        let message = &details[0]["message"];
        for field in [
            "cc",
            "reply_to",
            "subject",
            "preview",
            "text",
            "html",
            "attachments",
            "in_reply_to",
            "references",
            "headers",
        ] {
            assert!(message.get(field).is_some(), "{event_type}: {field} absent");
        }
    }
}

#[tokio::test]
async fn the_sent_event_matches_the_schema() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    let mut queued = sent.clone();
    queued.labels = vec!["queued".to_owned()];

    let result = invoke(
        h.state.clone(),
        mail_record(
            "MODIFY",
            &message_image(&sent),
            Some(&message_image(&queued)),
        ),
    )
    .await
    .unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let details = mailbox_details(&h);
    assert_eq!(details.len(), 1);
    assert_eq!(details[0]["event_type"], "message.sent");
    assert_matches_schema(&details[0]);
    assert_eq!(
        details[0]["send"]["recipients"],
        json!([
            "a@example.net",
            "b@example.net",
            "c@example.net",
            "d@example.net"
        ])
    );
}

/// Every documented SES example that has a mailbox event publishes one that
/// matches its schema, in both SES formats; the rest publish none.
#[tokio::test]
async fn ses_driven_events_match_the_schema() {
    for (example, expected) in [
        ("event-delivery.json", Some("message.delivered")),
        ("notification-delivery.json", Some("message.delivered")),
        ("event-bounce.json", Some("message.bounced")),
        ("notification-bounce-dsn.json", Some("message.bounced")),
        ("notification-bounce-no-dsn.json", Some("message.bounced")),
        ("event-complaint.json", Some("message.complained")),
        (
            "notification-complaint-feedback.json",
            Some("message.complained"),
        ),
        (
            "notification-complaint-no-feedback.json",
            Some("message.complained"),
        ),
        ("event-reject.json", Some("message.rejected")),
        ("event-open.json", Some("message.opened")),
        ("event-send.json", None),
        ("event-click.json", None),
        ("event-delivery-delay.json", None),
        ("event-rendering-failure.json", None),
        ("event-subscription.json", None),
    ] {
        let h = seeded().await;
        let sent = sent_message(&h).await;
        let ses_message_id = sent.ses_message_id.clone().unwrap();

        publish_ses_example(&h, example, &ses_message_id).await;

        let details = mailbox_details(&h);
        match expected {
            Some(event_type) => {
                assert_eq!(details.len(), 1, "{example}");
                assert_eq!(details[0]["event_type"], event_type, "{example}");
                assert_matches_schema(&details[0]);
            }
            None => assert!(details.is_empty(), "{example}: {details:?}"),
        }
    }
}

/// An SES event for mail this mailbox did not send publishes only its
/// `ses.*` event.
#[tokio::test]
async fn ses_events_for_other_senders_publish_no_mailbox_event() {
    let h = seeded().await;
    publish_ses_example(&h, "event-delivery.json", "ses-someone-else").await;
    assert!(mailbox_details(&h).is_empty());
    assert_eq!(h.fake().published.lock().unwrap().len(), 1);
}

/// A transient failure resolving the SES message retries the record before
/// anything publishes, so the retry doesn't publish the `ses.*` event twice.
#[tokio::test]
async fn a_transient_mailbox_lookup_failure_retries_before_publishing() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    h.state
        .services
        .mail
        .fail_next_resolve(ReadFailure::Transient);

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ses-events/event-delivery.json");
    let mut example: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    example["mail"]["messageId"] = json!(sent.ses_message_id.unwrap());
    let raw = serde_json::to_vec(&webhook_test_support::wrapped(&h, &example)).unwrap();
    let record = webhook_test_support::dynamodb_insert_event(
        &raw,
        "EVT#2026-01-15T09:46:00.000Z#sns-1",
        "seq-retry",
    );

    let result = invoke(h.state.clone(), record).await.unwrap();

    assert_eq!(
        result,
        json!({ "batchItemFailures": [{ "itemIdentifier": "seq-retry" }] })
    );
    assert!(h.fake().published.lock().unwrap().is_empty());
}

/// A lookup failure a retry can't fix still publishes the `ses.*` event;
/// only the mailbox event is lost.
#[tokio::test]
async fn a_permanent_mailbox_lookup_failure_still_publishes_the_ses_event() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    h.state
        .services
        .mail
        .fail_next_resolve(ReadFailure::Permanent);

    publish_ses_example(&h, "event-delivery.json", &sent.ses_message_id.unwrap()).await;

    let published = h.fake().published.lock().unwrap();
    let types: Vec<&str> = published
        .iter()
        .map(|event| event.detail_type.as_str())
        .collect();
    assert_eq!(types, vec!["ses.delivery"]);
}

/// A *permanent* delivery-label failure must not skip bounce suppression. The
/// label is informational; the SES account-level suppression write is the
/// deliverability-critical side-effect. `process_notification` acks a
/// permanent `ActionError` with a 200 and no SNS redelivery, so coupling
/// suppression to the label step's `?` would lose the suppression write for
/// good — and the stream relay never recovers it. A permanent
/// `resolve_ses_message` failure (e.g. an `AccessDeniedException` from a mail
/// table IAM regression) must therefore be best-effort: log + count, then
/// still suppress every bounced recipient.
#[tokio::test]
async fn permanent_delivery_label_failure_does_not_skip_bounce_suppression() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    h.state
        .services
        .mail
        .fail_next_resolve(ReadFailure::Permanent); // permanent resolve_ses_message

    let bounce = json!({
        "eventType": "Bounce",
        "bounce": {"bounceType": "Permanent",
                   "bouncedRecipients": [{"emailAddress": "a@example.net"}]},
        "mail": {"messageId": sent.ses_message_id.unwrap()}
    });
    let body = webhook_test_support::wrapped(&h, &bounce);
    let status = webhook_test_support::post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls
            .iter()
            .any(|c| c.starts_with("suppress:a@example.net:Bounce")),
        "a permanent bounce must still suppress its recipients even when the delivery-label step fails permanently: {calls:?}"
    );
}

/// The complaint path shares the suppression arm with bounces, so a permanent
/// delivery-label failure must not skip complaint suppression either.
#[tokio::test]
async fn permanent_delivery_label_failure_does_not_skip_complaint_suppression() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    h.state
        .services
        .mail
        .fail_next_resolve(ReadFailure::Permanent);

    let complaint = json!({
        "notificationType": "Complaint",
        "complaint": {"complainedRecipients": [{"emailAddress": "a@example.net"}]},
        "mail": {"messageId": sent.ses_message_id.unwrap()}
    });
    let body = webhook_test_support::wrapped(&h, &complaint);
    let status = webhook_test_support::post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(status, StatusCode::OK);
    let calls = h.fake().calls();
    assert!(
        calls
            .iter()
            .any(|c| c.starts_with("suppress:a@example.net:Complaint")),
        "a complaint must still suppress its recipients even when the delivery-label step fails permanently: {calls:?}"
    );
}

/// A *transient* delivery-label failure (throttling, 5xx, a dropped DynamoDB
/// connection) is recoverable by redelivery, so it must propagate as a 5xx to
/// recruit SNS redelivery — suppression is *not* attempted on this attempt,
/// since the whole idempotent label+suppress pair re-runs on the redelivery.
#[tokio::test]
async fn transient_delivery_label_failure_returns_500_and_defers_suppression() {
    let h = seeded().await;
    let sent = sent_message(&h).await;
    h.state
        .services
        .mail
        .fail_next_resolve(ReadFailure::Transient);

    let bounce = json!({
        "eventType": "Bounce",
        "bounce": {"bounceType": "Permanent",
                   "bouncedRecipients": [{"emailAddress": "a@example.net"}]},
        "mail": {"messageId": sent.ses_message_id.unwrap()}
    });
    let body = webhook_test_support::wrapped(&h, &bounce);
    let status = webhook_test_support::post(h.state.clone(), "/webhooks/ses/events", &body).await;

    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a transient label failure must drive a 5xx so SNS redelivers"
    );
    let calls = h.fake().calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("suppress")),
        "suppression must not run before the transient label failure clears on redelivery: {calls:?}"
    );
}

/// The fixture covers every event type the relay emits; the ones it does
/// not emit are named here, so a refreshed fixture adding a type fails until
/// this list or the relay accounts for it.
#[test]
fn the_fixture_covers_every_event_type() {
    let schemas = schemas();
    let mut documented = BTreeSet::new();
    for (name, schema) in &schemas {
        if !(name.starts_with("events_") && name.ends_with("Event")) {
            continue;
        }
        let event_type = &schema["properties"]["event_type"];
        let allowed = match event_type.get("$ref").and_then(Value::as_str) {
            Some(reference) => {
                &schemas[reference.trim_start_matches("#/components/schemas/")]["enum"]
            }
            None => &event_type["enum"],
        };
        for value in allowed.as_array().unwrap() {
            documented.insert(value.as_str().unwrap().to_owned());
        }
    }
    let emitted = [
        "message.received",
        "message.received.spam",
        "message.received.unauthenticated",
        "message.sent",
        "message.delivered",
        "message.bounced",
        "message.complained",
        "message.rejected",
        "message.opened",
    ];
    let not_emitted = ["message.received.blocked", "domain.verified"];
    let covered: BTreeSet<String> = emitted
        .iter()
        .chain(&not_emitted)
        .map(|event_type| (*event_type).to_owned())
        .collect();
    assert_eq!(documented, covered);
}

/// The checker itself: each kind of departure is caught.
#[test]
fn the_checker_rejects_departures_from_the_schema() {
    let valid = json!({
        "type": "event",
        "event_type": "message.opened",
        "event_id": "evt_1",
        "open": {
            "inbox_id": INBOX,
            "thread_id": "tid-1",
            "message_id": "mid-1",
            "timestamp": "2026-01-15T09:50:00.000Z",
        },
    });
    assert!(violations(&valid).is_empty());

    let mut extra = valid.clone();
    extra["meta"] = json!({});
    assert_eq!(
        violations(&extra),
        vec!["$.meta: not defined by the schema"]
    );

    let mut missing = valid.clone();
    missing["open"].as_object_mut().unwrap().remove("thread_id");
    assert_eq!(
        violations(&missing),
        vec!["$.open: missing required `thread_id`"]
    );

    let mut wrong_enum = valid.clone();
    wrong_enum["type"] = json!("message.opened");
    assert_eq!(violations(&wrong_enum).len(), 1);

    let mut wrong_type = valid.clone();
    wrong_type["open"]["timestamp"] = json!(1);
    assert_eq!(violations(&wrong_type).len(), 1);

    let mut bad_time = valid;
    bad_time["open"]["timestamp"] = json!("yesterday");
    assert_eq!(violations(&bad_time).len(), 1);
}

#[test]
fn date_times_follow_rfc_3339() {
    for good in [
        "2026-01-15T09:50:00Z",
        "2026-01-15T09:50:00.123Z",
        "2026-01-15T09:50:00+01:00",
    ] {
        assert!(is_date_time(good), "{good}");
    }
    for bad in [
        "2026-01-15",
        "2026-01-15 09:50:00Z",
        "2026-01-15T09:50:00",
        "2026-01-15T09:50:00.Z",
        "2026-01-15T09:50:00+0100",
    ] {
        assert!(!is_date_time(bad), "{bad}");
    }
}
