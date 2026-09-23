//! Builds the mailbox's EventBridge details. Each detail is the reference
//! mailbox API's webhook payload, field for field and nothing more — `type`,
//! `event_type`, `event_id` and one event-specific object — so an
//! EventBridge API destination selecting `$.detail` delivers exactly the
//! body a webhook consumer of that API parses.
//!
//! - `message.received*`, on a received message item's INSERT, carries
//!   `message` and `thread`.
//! - `message.sent`, on the sender's `queued` → `sent` relabel, carries
//!   `send`.
//! - `message.delivered`, `.bounced`, `.complained`, `.rejected` and
//!   `.opened`, one per SES event that resolves to a mailbox message, carry
//!   `delivery`, `bounce`, `complaint`, `reject` and `open`. They are built
//!   from the SES event rather than from the label it adds, because the label
//!   is added once while SES reports each recipient batch and each open
//!   separately, and only the SES event carries the recipients, types and
//!   reason.

use serde::Serialize;
use serde_json::{Value, json};

use crate::mail::content::MessageContent;
use crate::mail::labels::SystemLabel;
use crate::mail::{MailMessage, ids, labels, time, wire};
use crate::model::ses_notification::SesNotification;
use crate::publish::{PUT_EVENTS_ENTRY_CAP_BYTES, detail_bytes};

/// One outbound mailbox event, before publishing.
#[derive(Debug, Clone)]
pub struct MailEvent {
    pub detail_type: &'static str,
    pub detail: Value,
}

/// The `Message` fields the contract requires; an oversized received event
/// is reduced to these.
const MESSAGE_REQUIRED: [&str; 10] = [
    "inbox_id",
    "thread_id",
    "message_id",
    "labels",
    "timestamp",
    "from",
    "to",
    "size",
    "updated_at",
    "created_at",
];

/// The `ThreadItem` fields the contract requires; an oversized received
/// event is reduced to these.
const THREAD_REQUIRED: [&str; 11] = [
    "inbox_id",
    "thread_id",
    "labels",
    "timestamp",
    "senders",
    "recipients",
    "last_message_id",
    "message_count",
    "size",
    "updated_at",
    "created_at",
];

/// Best-effort JSON conversion: `MailEvent`'s wire types are plain owned
/// strings/numbers, so this never actually fails in practice, but a stream
/// record's contents ultimately trace back to an inbound message an attacker
/// controls — falling back to `Null` keeps the relay from ever panicking on
/// it.
fn to_json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Precedence: a spam/virus quarantine verdict beats an authentication
/// failure, which beats a plain receipt. `spam` and `unauthenticated`
/// co-exist with the `received` system label rather than
/// replacing it, so the event type is chosen from the label set.
fn received_event_type(labels: &[String]) -> &'static str {
    if labels::has(labels, SystemLabel::Spam) {
        "message.received.spam"
    } else if labels::has(labels, SystemLabel::Unauthenticated) {
        "message.received.unauthenticated"
    } else {
        "message.received"
    }
}

/// The event type an SES event publishes for a mailbox message, if any.
/// `Send` is absent because the sender's relabel publishes `message.sent`,
/// and `Click` and the rest have no mailbox event.
#[must_use]
pub fn ses_event_type(kind: &str) -> Option<&'static str> {
    match kind {
        "Delivery" => Some("message.delivered"),
        "Bounce" => Some("message.bounced"),
        "Complaint" => Some("message.complained"),
        "Reject" => Some("message.rejected"),
        "Open" => Some("message.opened"),
        _ => None,
    }
}

/// The thread as of this message's arrival, from the snapshot stored on the
/// message item.
fn thread_item(msg: &MailMessage) -> wire::ThreadItem {
    // A received message always carries a snapshot; an absent one falls back
    // to its empty Default rather than failing the whole relay record over a
    // data shape that shouldn't occur.
    let snapshot = msg.thread_snapshot.clone().unwrap_or_default();
    wire::ThreadItem {
        inbox_id: msg.inbox_id.as_str().to_owned(),
        thread_id: msg.thread_id.clone(),
        labels: snapshot.labels,
        timestamp: snapshot.timestamp,
        received_timestamp: snapshot.received_timestamp,
        sent_timestamp: snapshot.sent_timestamp,
        senders: snapshot.senders,
        recipients: snapshot.recipients,
        last_message_id: snapshot.last_message_id,
        message_count: snapshot.message_count,
        size: snapshot.size,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        subject: (!snapshot.subject.is_empty()).then_some(snapshot.subject),
        preview: (!snapshot.preview.is_empty()).then_some(snapshot.preview),
        attachments: snapshot
            .attachments
            .iter()
            .map(wire::Attachment::from)
            .collect(),
    }
}

/// Builds the `message.received*` event for a newly-inserted message item on
/// an INSERT: plain, `.spam`, or `.unauthenticated` depending on the
/// message's labels. `None` for any other INSERT (e.g. a `queued` outbound
/// message), whose lifecycle publishes from [`build_sent_event`] instead.
#[must_use]
pub fn build_received_event(
    msg: &MailMessage,
    content: &MessageContent,
    event_source: &str,
) -> Option<MailEvent> {
    if !labels::has(&msg.labels, SystemLabel::Received) {
        return None;
    }
    let event_type = received_event_type(&msg.labels);
    // ts_ms is the message's own timestamp for a received event.
    let ts_ms = time::parse(&msg.timestamp).unwrap_or(0);
    let event_id = ids::event_id(msg.inbox_id.as_str(), &msg.message_id, event_type, ts_ms);

    let mut message = wire::Message::new(msg, content);
    // A body larger than the whole entry cap cannot survive the ladder
    // below, so it is dropped before the detail is built rather than after:
    // a content document is up to ~270 MB (the 6× worst-case JSON escape of
    // a control-char-heavy body under the raw-mail ceiling — see
    // `content::MAX_CONTENT_BYTES`), and serializing one only to measure it
    // is the relay's most expensive avoidable step.
    let cap = entry_cap(event_type, event_source);
    if message.html.as_ref().is_some_and(|html| html.len() > cap) {
        message.html = None;
    }
    if message.text.as_ref().is_some_and(|text| text.len() > cap) {
        message.text = None;
    }

    let mut detail = json!({
        "type": "event",
        "event_type": event_type,
        "event_id": event_id,
        "message": to_json(&message),
        "thread": to_json(&thread_item(msg)),
    });
    cap_received_detail(&mut detail, event_type, event_source);

    Some(MailEvent {
        detail_type: event_type,
        detail,
    })
}

/// Builds the `message.sent` event when this write is the sender's
/// `queued` → `sent` relabel: `sent` is in `msg`'s labels and was not in
/// `old_labels`. Any other write — a delivery label, a read receipt, a
/// user's own label — builds nothing.
#[must_use]
pub fn build_sent_event(msg: &MailMessage, old_labels: &[String]) -> Option<MailEvent> {
    if !labels::has(&msg.labels, SystemLabel::Sent) || labels::has(old_labels, SystemLabel::Sent) {
        return None;
    }
    let event_type = "message.sent";
    // `sent_at` is written by the same relabel; `updated_at` is that write's
    // time too, and only stands in for an item that somehow lacks it.
    let timestamp = msg.sent_at.as_deref().unwrap_or(&msg.updated_at);
    let ts_ms = time::parse(timestamp).unwrap_or(0);
    let recipients: Vec<&str> = msg
        .to
        .iter()
        .chain(&msg.cc)
        .chain(&msg.bcc)
        .map(String::as_str)
        .collect();
    let detail = json!({
        "type": "event",
        "event_type": event_type,
        "event_id": ids::event_id(msg.inbox_id.as_str(), &msg.message_id, event_type, ts_ms),
        "send": {
            "inbox_id": msg.inbox_id.as_str(),
            "thread_id": msg.thread_id,
            "message_id": msg.message_id,
            "timestamp": timestamp,
            "recipients": recipients,
        },
    });
    Some(MailEvent {
        detail_type: event_type,
        detail,
    })
}

/// Builds the lifecycle event one SES event publishes for the mailbox
/// message `msg` it resolved to. `sns_message_id` and `notified_at` are the
/// SNS envelope's `MessageId` and `Timestamp`: the first keeps the event id
/// stable across redelivery, the second stands in for the event time when
/// the SES object carries none (a `Reject` never does). `None` for an SES
/// event type with no mailbox event, or one missing the object it is named
/// for.
#[must_use]
pub fn build_ses_event(
    ses: &SesNotification,
    msg: &MailMessage,
    sns_message_id: &str,
    notified_at: &str,
) -> Option<MailEvent> {
    let event_type = ses_event_type(&ses.kind)?;
    let (key, event_timestamp, fields) = match ses.kind.as_str() {
        "Delivery" => {
            let delivery = ses.delivery.as_ref()?;
            (
                "delivery",
                delivery.timestamp.as_deref(),
                json!({ "recipients": delivery.recipients }),
            )
        }
        "Bounce" => {
            let bounce = ses.bounce.as_ref()?;
            let recipients: Vec<Value> = bounce
                .bounced_recipients
                .iter()
                .map(|recipient| {
                    json!({
                        "address": recipient.email_address,
                        "status": recipient.status.as_deref().unwrap_or_default(),
                    })
                })
                .collect();
            (
                "bounce",
                bounce.timestamp.as_deref(),
                json!({
                    "type": bounce.bounce_type,
                    "sub_type": bounce.bounce_sub_type.as_deref().unwrap_or_default(),
                    "recipients": recipients,
                }),
            )
        }
        "Complaint" => {
            let complaint = ses.complaint.as_ref()?;
            let recipients: Vec<&str> = complaint
                .complained_recipients
                .iter()
                .map(|recipient| recipient.email_address.as_str())
                .collect();
            (
                "complaint",
                complaint.timestamp.as_deref(),
                json!({
                    "type": complaint.complaint_feedback_type.as_deref().unwrap_or_default(),
                    "sub_type": complaint.complaint_sub_type.as_deref().unwrap_or_default(),
                    "recipients": recipients,
                }),
            )
        }
        "Reject" => {
            let reject = ses.reject.as_ref()?;
            (
                "reject",
                None,
                json!({ "reason": reject.reason.as_deref().unwrap_or_default() }),
            )
        }
        "Open" => {
            let open = ses.open.as_ref()?;
            ("open", open.timestamp.as_deref(), json!({}))
        }
        _ => return None,
    };

    let mut object = json!({
        "inbox_id": msg.inbox_id.as_str(),
        "thread_id": msg.thread_id,
        "message_id": msg.message_id,
        "timestamp": event_timestamp.unwrap_or(notified_at),
    });
    if let (Some(object), Value::Object(fields)) = (object.as_object_mut(), fields) {
        object.extend(fields);
    }
    let ts_ms = time::parse(notified_at).unwrap_or(0);
    let event_id = ids::ses_event_id(
        msg.inbox_id.as_str(),
        &msg.message_id,
        event_type,
        sns_message_id,
        ts_ms,
    );
    let mut detail = json!({
        "type": "event",
        "event_type": event_type,
        "event_id": event_id,
    });
    detail[key] = object;
    Some(MailEvent {
        detail_type: event_type,
        detail,
    })
}

/// The detail budget for one entry: the `PutEvents` cap less the
/// `detail_type` and event `Source` that ride along with it.
fn entry_cap(detail_type: &str, event_source: &str) -> usize {
    PUT_EVENTS_ENTRY_CAP_BYTES.saturating_sub(detail_type.len() + event_source.len())
}

/// Keeps only `keep`'s keys of a JSON object.
fn retain_fields(value: &mut Value, keep: &[&str]) {
    if let Some(object) = value.as_object_mut() {
        object.retain(|key, _| keep.contains(&key.as_str()));
    }
}

/// Reduces an oversized received detail to fit the `PutEvents` entry cap
/// while keeping it a valid payload: drops `message.html`, `message.text`
/// and `message.headers` in turn, then reduces `message` and `thread` to
/// their required fields. The field caps bound those required fields far
/// below the entry cap, so the last step always fits. Reserves
/// `detail_type` plus the configured event `Source` as `PutEvents` entry
/// headroom, as `crate::publish::build_outbound` does.
fn cap_received_detail(detail: &mut Value, detail_type: &str, event_source: &str) {
    let cap = entry_cap(detail_type, event_source);
    for field in ["html", "text", "headers"] {
        if detail_bytes(detail) <= cap {
            return;
        }
        if let Some(message) = detail["message"].as_object_mut() {
            message.remove(field);
        }
    }
    if detail_bytes(detail) <= cap {
        return;
    }
    retain_fields(&mut detail["message"], &MESSAGE_REQUIRED);
    retain_fields(&mut detail["thread"], &THREAD_REQUIRED);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::mail::{AttachmentMeta, Direction, InboxId, ThreadSnapshot};

    const EVENT_SOURCE: &str = "aws-messaging-webhook";
    const NOTIFIED_AT: &str = "2026-01-15T10:00:00.000Z";

    /// The received event a message builds, which every test here expects to
    /// exist.
    fn received(msg: &MailMessage, content: &MessageContent) -> MailEvent {
        build_received_event(msg, content, EVENT_SOURCE).expect("message is labelled received")
    }

    fn sample_content() -> MessageContent {
        MessageContent {
            text: Some("Hello there".to_owned()),
            html: Some("<p>Hello there</p>".to_owned()),
            ..MessageContent::default()
        }
    }

    fn sample_snapshot() -> ThreadSnapshot {
        ThreadSnapshot {
            thread_id: "tid-1".to_owned(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            message_count: 1,
            labels: vec!["received".to_owned(), "unread".to_owned()],
            senders: vec!["sender@example.com".to_owned()],
            recipients: vec!["support@example.com".to_owned()],
            timestamp: "2026-01-15T09:30:00.000Z".to_owned(),
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
            last_message_id: "mid-1".to_owned(),
            size: 1234,
            received_timestamp: Some("2026-01-15T09:30:00.000Z".to_owned()),
            sent_timestamp: None,
            attachments: Vec::new(),
        }
    }

    fn sample_message() -> MailMessage {
        MailMessage {
            inbox_id: InboxId("support@example.com".to_owned()),
            thread_id: "tid-1".to_owned(),
            message_id: "mid-1".to_owned(),
            ses_message_id: Some("ses-1".to_owned()),
            direction: Direction::Inbound,
            rfc_message_id: "<mid-1@example.com>".to_owned(),
            in_reply_to: None,
            labels: vec!["received".to_owned(), "unread".to_owned()],
            timestamp: "2026-01-15T09:30:00.000Z".to_owned(),
            from: "sender@example.com".to_owned(),
            to: vec!["support@example.com".to_owned()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            size: 1234,
            attachments: Vec::new(),
            attachments_truncated: false,
            raw_s3_key: Some("inbound/x".to_owned()),
            thread_snapshot: Some(sample_snapshot()),
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 1,
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
            expires_at: 0,
        }
    }

    /// A message this mailbox sent, as the sender leaves it.
    fn sent_message() -> MailMessage {
        let mut msg = sample_message();
        msg.direction = Direction::Outbound;
        msg.labels = vec!["sent".to_owned()];
        msg.from = "support@example.com".to_owned();
        msg.to = vec!["a@example.net".to_owned()];
        msg.cc = vec!["b@example.net".to_owned()];
        msg.bcc = vec!["c@example.net".to_owned()];
        msg.thread_snapshot = None;
        msg.sent_at = Some("2026-01-15T09:45:00.000Z".to_owned());
        msg
    }

    fn ses(value: &Value) -> SesNotification {
        serde_json::from_value(value.clone()).unwrap()
    }

    fn keys_of(value: &Value) -> Vec<&str> {
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn plain_received_message_builds_the_reference_payload() {
        let msg = sample_message();
        let event = received(&msg, &sample_content());
        let detail = &event.detail;
        assert_eq!(event.detail_type, "message.received");
        // Exactly the contract's top-level fields, nothing added.
        assert_eq!(
            keys_of(detail),
            vec!["event_id", "event_type", "message", "thread", "type"]
        );
        assert_eq!(detail["type"], "event");
        assert_eq!(detail["event_type"], "message.received");
        assert!(
            detail["event_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("evt_"))
        );
        assert_eq!(detail["message"]["message_id"], "mid-1");
        assert_eq!(detail["message"]["html"], "<p>Hello there</p>");
        assert_eq!(
            detail["thread"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "labels": ["received", "unread"],
                "timestamp": "2026-01-15T09:30:00.000Z",
                "received_timestamp": "2026-01-15T09:30:00.000Z",
                "senders": ["sender@example.com"],
                "recipients": ["support@example.com"],
                "subject": "Hello",
                "preview": "Hello there",
                "last_message_id": "mid-1",
                "message_count": 1,
                "size": 1234,
                "created_at": "2026-01-15T09:30:00.000Z",
                "updated_at": "2026-01-15T09:30:00.000Z",
            })
        );
    }

    /// `to`, `labels`, `senders` and `recipients` are required by the
    /// contract, so they serialize even when empty.
    #[test]
    fn required_arrays_serialize_when_empty() {
        let mut msg = sample_message();
        msg.to = Vec::new();
        let mut snapshot = sample_snapshot();
        snapshot.labels = Vec::new();
        snapshot.senders = Vec::new();
        snapshot.recipients = Vec::new();
        msg.thread_snapshot = Some(snapshot);
        let detail = received(&msg, &sample_content()).detail;
        assert_eq!(detail["message"]["to"], json!([]));
        assert_eq!(detail["thread"]["labels"], json!([]));
        assert_eq!(detail["thread"]["senders"], json!([]));
        assert_eq!(detail["thread"]["recipients"], json!([]));
    }

    #[test]
    fn thread_attachments_are_published_in_wire_form() {
        let mut msg = sample_message();
        let mut snapshot = sample_snapshot();
        snapshot.attachments = vec![AttachmentMeta {
            attachment_id: "att-1".to_owned(),
            object_key: Some("attachments/mid-1/att-1".to_owned()),
            size: 10,
            filename: Some("a.txt".to_owned()),
            content_type: "text/plain".to_owned(),
            content_disposition: "attachment".to_owned(),
            content_id: None,
        }];
        msg.thread_snapshot = Some(snapshot);
        let detail = received(&msg, &sample_content()).detail;
        assert_eq!(
            detail["thread"]["attachments"],
            json!([{
                "attachment_id": "att-1",
                "size": 10,
                "filename": "a.txt",
                "content_type": "text/plain",
                "content_disposition": "attachment",
            }])
        );
    }

    #[test]
    fn spam_takes_precedence_over_unauthenticated() {
        let mut msg = sample_message();
        msg.labels = vec![
            "received".to_owned(),
            "spam".to_owned(),
            "unauthenticated".to_owned(),
        ];
        let event = received(&msg, &sample_content());
        assert_eq!(event.detail_type, "message.received.spam");
        assert_eq!(event.detail["event_type"], "message.received.spam");
    }

    #[test]
    fn unauthenticated_without_spam() {
        let mut msg = sample_message();
        msg.labels = vec!["received".to_owned(), "unauthenticated".to_owned()];
        let event = received(&msg, &sample_content());
        assert_eq!(event.detail_type, "message.received.unauthenticated");
    }

    #[test]
    fn no_received_label_builds_nothing() {
        let mut msg = sample_message();
        msg.labels = vec!["queued".to_owned()];
        assert!(build_received_event(&msg, &sample_content(), EVENT_SOURCE).is_none());
    }

    #[test]
    fn event_id_is_stable_across_a_replay_of_the_same_record() {
        let msg = sample_message();
        let a = received(&msg, &sample_content());
        let b = received(&msg, &sample_content());
        assert_eq!(a.detail["event_id"], b.detail["event_id"]);
    }

    #[test]
    fn the_queued_to_sent_relabel_builds_the_send_payload() {
        let msg = sent_message();
        let event = build_sent_event(&msg, &["queued".to_owned()]).expect("a send event");
        assert_eq!(event.detail_type, "message.sent");
        assert_eq!(
            keys_of(&event.detail),
            vec!["event_id", "event_type", "send", "type"]
        );
        assert_eq!(
            event.detail["send"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": "2026-01-15T09:45:00.000Z",
                "recipients": ["a@example.net", "b@example.net", "c@example.net"],
            })
        );
        let replay = build_sent_event(&msg, &["queued".to_owned()]).unwrap();
        assert_eq!(event.detail["event_id"], replay.detail["event_id"]);
    }

    /// Only the relabel that adds `sent` is the send: a later write to a
    /// sent message (a delivery label, a user's own label) builds nothing,
    /// and nor does a write to a message that was never sent.
    #[test]
    fn writes_other_than_the_sent_relabel_build_nothing() {
        let mut msg = sent_message();
        msg.labels = vec!["sent".to_owned(), "delivered".to_owned()];
        assert!(build_sent_event(&msg, &["sent".to_owned()]).is_none());
        let received = sample_message();
        assert!(build_sent_event(&received, &["received".to_owned()]).is_none());
    }

    #[test]
    fn a_delivery_builds_the_delivery_payload() {
        let event = build_ses_event(
            &ses(&json!({
                "eventType": "Delivery",
                "mail": {"messageId": "ses-1"},
                "delivery": {
                    "timestamp": "2026-01-15T09:46:00.000Z",
                    "recipients": ["a@example.net"],
                    "processingTimeMillis": 546,
                    "smtpResponse": "250 ok",
                },
            })),
            &sent_message(),
            "sns-1",
            NOTIFIED_AT,
        )
        .expect("a delivery event");
        assert_eq!(event.detail_type, "message.delivered");
        assert_eq!(
            keys_of(&event.detail),
            vec!["delivery", "event_id", "event_type", "type"]
        );
        assert_eq!(event.detail["type"], "event");
        assert_eq!(
            event.detail["delivery"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": "2026-01-15T09:46:00.000Z",
                "recipients": ["a@example.net"],
            })
        );
    }

    #[test]
    fn a_bounce_builds_the_bounce_payload() {
        let event = build_ses_event(
            &ses(&json!({
                "eventType": "Bounce",
                "mail": {"messageId": "ses-1"},
                "bounce": {
                    "bounceType": "Permanent",
                    "bounceSubType": "General",
                    "timestamp": "2026-01-15T09:46:00.000Z",
                    "bouncedRecipients": [
                        {"emailAddress": "a@example.net", "status": "5.1.1", "action": "failed"},
                        {"emailAddress": "b@example.net"},
                    ],
                },
            })),
            &sent_message(),
            "sns-1",
            NOTIFIED_AT,
        )
        .expect("a bounce event");
        assert_eq!(event.detail_type, "message.bounced");
        assert_eq!(
            event.detail["bounce"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": "2026-01-15T09:46:00.000Z",
                "type": "Permanent",
                "sub_type": "General",
                "recipients": [
                    {"address": "a@example.net", "status": "5.1.1"},
                    {"address": "b@example.net", "status": ""},
                ],
            })
        );
    }

    #[test]
    fn a_complaint_builds_the_complaint_payload() {
        let event = build_ses_event(
            &ses(&json!({
                "notificationType": "Complaint",
                "mail": {"messageId": "ses-1"},
                "complaint": {
                    "complaintFeedbackType": "abuse",
                    "complaintSubType": null,
                    "timestamp": "2026-01-15T09:46:00.000Z",
                    "complainedRecipients": [{"emailAddress": "a@example.net"}],
                },
            })),
            &sent_message(),
            "sns-1",
            NOTIFIED_AT,
        )
        .expect("a complaint event");
        assert_eq!(event.detail_type, "message.complained");
        assert_eq!(
            event.detail["complaint"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": "2026-01-15T09:46:00.000Z",
                "type": "abuse",
                "sub_type": "",
                "recipients": ["a@example.net"],
            })
        );
    }

    /// A `Reject` carries no timestamp of its own, so the notification's
    /// stands in.
    #[test]
    fn a_reject_builds_the_reject_payload_timed_by_the_notification() {
        let event = build_ses_event(
            &ses(&json!({
                "eventType": "Reject",
                "mail": {"messageId": "ses-1"},
                "reject": {"reason": "Bad content"},
            })),
            &sent_message(),
            "sns-1",
            NOTIFIED_AT,
        )
        .expect("a reject event");
        assert_eq!(event.detail_type, "message.rejected");
        assert_eq!(
            event.detail["reject"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": NOTIFIED_AT,
                "reason": "Bad content",
            })
        );
    }

    #[test]
    fn an_open_builds_the_open_payload() {
        let event = build_ses_event(
            &ses(&json!({
                "eventType": "Open",
                "mail": {"messageId": "ses-1"},
                "open": {"timestamp": "2026-01-15T09:50:00.000Z", "ipAddress": "192.0.2.1"},
            })),
            &sent_message(),
            "sns-1",
            NOTIFIED_AT,
        )
        .expect("an open event");
        assert_eq!(event.detail_type, "message.opened");
        assert_eq!(
            event.detail["open"],
            json!({
                "inbox_id": "support@example.com",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "timestamp": "2026-01-15T09:50:00.000Z",
            })
        );
    }

    /// Each SES notification is its own event: a redelivery rebuilds the
    /// same id, a second notification of the same type gets a new one.
    #[test]
    fn ses_event_ids_follow_the_notification() {
        let open = ses(&json!({
            "eventType": "Open",
            "mail": {"messageId": "ses-1"},
            "open": {"timestamp": "2026-01-15T09:50:00.000Z"},
        }));
        let msg = sent_message();
        let first = build_ses_event(&open, &msg, "sns-1", NOTIFIED_AT).unwrap();
        let replay = build_ses_event(&open, &msg, "sns-1", NOTIFIED_AT).unwrap();
        let second = build_ses_event(&open, &msg, "sns-2", NOTIFIED_AT).unwrap();
        assert_eq!(first.detail["event_id"], replay.detail["event_id"]);
        assert_ne!(first.detail["event_id"], second.detail["event_id"]);
    }

    #[test]
    fn ses_events_without_a_mailbox_event_build_nothing() {
        let msg = sent_message();
        for kind in ["Send", "Click", "DeliveryDelay", "Rendering Failure"] {
            let event = ses(&json!({"eventType": kind, "mail": {"messageId": "ses-1"}}));
            assert!(
                build_ses_event(&event, &msg, "sns-1", NOTIFIED_AT).is_none(),
                "{kind}"
            );
        }
        // A delivery missing the object it is named for.
        let bare = ses(&json!({"eventType": "Delivery", "mail": {"messageId": "ses-1"}}));
        assert!(build_ses_event(&bare, &msg, "sns-1", NOTIFIED_AT).is_none());
    }

    fn entry_bytes(event: &MailEvent, event_source: &str) -> usize {
        detail_bytes(&event.detail) + event.detail_type.len() + event_source.len()
    }

    #[test]
    fn oversized_html_is_dropped_first() {
        let msg = sample_message();
        let mut content = sample_content();
        content.html = Some("x".repeat(300_000));
        let event = received(&msg, &content);
        assert!(event.detail["message"].get("html").is_none());
        assert_eq!(event.detail["message"]["text"], "Hello there");
        assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
    }

    #[test]
    fn oversized_bodies_and_headers_are_dropped_in_turn() {
        let msg = sample_message();
        let mut content = sample_content();
        content.html = Some("x".repeat(150_000));
        content.text = Some("y".repeat(250_000));
        content.headers = BTreeMap::from([("X-Big".to_owned(), "z".repeat(50_000))]);
        let event = received(&msg, &content);
        let message = &event.detail["message"];
        assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
        assert!(message.get("html").is_none());
        assert!(message.get("text").is_none());
        // Dropping the bodies was enough: the headers and subject survive.
        assert_eq!(
            message["headers"]["X-Big"].as_str().map(str::len),
            Some(50_000)
        );
        assert_eq!(message["subject"], "Hello");
    }

    #[test]
    fn extreme_oversize_reduces_to_the_required_fields() {
        // Filenames alone (not touched by the html/text/headers steps) stay
        // well over the cap, forcing the required-fields reduction.
        let mut msg = sample_message();
        let mut content = sample_content();
        content.html = Some("x".repeat(500_000));
        content.text = Some("y".repeat(500_000));
        msg.attachments = (0..100)
            .map(|i| AttachmentMeta {
                attachment_id: format!("att-{i}"),
                object_key: Some(format!("attachments/mid-1/att-{i}")),
                size: 100,
                filename: Some("f".repeat(5_000)),
                content_type: "text/plain".to_owned(),
                content_disposition: "attachment".to_owned(),
                content_id: None,
            })
            .collect();
        let event = received(&msg, &content);
        let detail = &event.detail;
        assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
        let mut message_required = MESSAGE_REQUIRED.to_vec();
        message_required.sort_unstable();
        assert_eq!(keys_of(&detail["message"]), message_required);
        let mut thread_required = THREAD_REQUIRED.to_vec();
        thread_required.sort_unstable();
        assert_eq!(keys_of(&detail["thread"]), thread_required);
        assert_eq!(detail["message"]["message_id"], "mid-1");
        assert_eq!(detail["thread"]["thread_id"], "tid-1");
    }

    proptest! {
        /// For any html/text length, the built entry (detail + detail_type +
        /// source) must stay under the `PutEvents` cap — the same guarantee
        /// `publish.rs::build_outbound` gives the SMS/SES pipeline.
        #[test]
        fn a_received_event_never_exceeds_the_putevents_entry_cap(
            html_len in 0usize..600_000,
            text_len in 0usize..600_000,
        ) {
            let msg = sample_message();
            let mut content = sample_content();
            content.html = Some("x".repeat(html_len));
            content.text = Some("y".repeat(text_len));
            let event = build_received_event(&msg, &content, EVENT_SOURCE);
            let event = event.expect("a received message always builds an event");
            prop_assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
        }
    }
}
