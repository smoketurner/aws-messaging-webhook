//! Builds the EventBridge details for mail-table stream records and caps
//! them to the `PutEvents` entry size, mirroring the size-reduction ladder
//! `crate::publish::build_outbound` applies to the SMS and SES pipeline.
//!
//! Two kinds of event are built here: the `message.received*` event for a
//! message item's INSERT, and the `message.<label>` events for the system
//! labels a later MODIFY adds (the sender's `queued` → `sent` relabel and
//! the delivery labels `actions::apply_delivery_label` writes).

use serde::Serialize;
use serde_json::{Value, json};

use crate::mail::content::MessageContent;
use crate::mail::{MailMessage, ids, time, wire};
use crate::publish::{PUT_EVENTS_ENTRY_CAP_BYTES, SCHEMA_VERSION, detail_bytes};

/// One mail-table stream record's outbound EventBridge event, before size
/// capping.
#[derive(Debug, Clone)]
pub struct MailEvent {
    pub detail_type: &'static str,
    pub detail: Value,
}

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
    if labels.iter().any(|label| label == "spam") {
        "message.received.spam"
    } else if labels.iter().any(|label| label == "unauthenticated") {
        "message.received.unauthenticated"
    } else {
        "message.received"
    }
}

/// The `message.<label>` event a newly-added system label publishes, if it
/// publishes one. `received`, `queued`, `unread`, `spam`, `trash` and
/// `unauthenticated` are absent: the first two are part of a message's
/// INSERT (the `received` event covers one and a queued send publishes
/// nothing), and the rest are mailbox state a consumer reads from the item
/// rather than lifecycle transitions.
fn label_event_type(label: &str) -> Option<&'static str> {
    match label {
        "sent" => Some("message.sent"),
        "delivered" => Some("message.delivered"),
        "bounced" => Some("message.bounced"),
        "complained" => Some("message.complained"),
        "rejected" => Some("message.rejected"),
        "opened" => Some("message.opened"),
        _ => None,
    }
}

/// The identifiers every mail event's `meta` carries.
fn event_meta(msg: &MailMessage) -> Value {
    let mut meta = json!({
        "messageId": msg.message_id,
        "inboxId": msg.inbox_id.as_str(),
        "threadId": msg.thread_id,
    });
    if let Some(ses_message_id) = &msg.ses_message_id {
        meta["sesMessageId"] = json!(ses_message_id);
    }
    meta
}

/// Builds the `message.received*` event for a newly-inserted message item on
/// an INSERT: plain, `.spam`, or `.unauthenticated` depending on the
/// message's labels. `None` for any other INSERT (e.g. a `queued` outbound
/// message), whose lifecycle publishes from [`build_label_events`] instead.
#[must_use]
pub fn build_received_event(
    msg: &MailMessage,
    content: &MessageContent,
    event_source: &str,
) -> Option<MailEvent> {
    if !msg.labels.iter().any(|label| label == "received") {
        return None;
    }
    let event_type = received_event_type(&msg.labels);
    // ts_ms is the message's own timestamp for a received event.
    let ts_ms = time::parse(&msg.timestamp).unwrap_or(0);
    let event_id = ids::event_id(msg.inbox_id.as_str(), &msg.message_id, event_type, ts_ms);

    // The stored thread_snapshot as of this message's arrival; a
    // received message always carries one, so an absent
    // snapshot falls back to its empty Default rather than failing the
    // whole relay record over a data shape that shouldn't occur.
    let thread = msg.thread_snapshot.clone().unwrap_or_default();

    let mut message = wire::Message::new(msg, content);
    // A body larger than the whole entry cap cannot survive the ladder
    // below, so it is dropped before the detail is built rather than after:
    // a content document is up to 80 MB, and serializing one only to measure
    // it is the relay's most expensive avoidable step.
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
        "thread": to_json(&thread),
        "schemaVersion": SCHEMA_VERSION,
        "meta": event_meta(msg),
    });
    cap_mail_detail(&mut detail, event_type, event_source);

    Some(MailEvent {
        detail_type: event_type,
        detail,
    })
}

/// Builds the lifecycle events for the labels `msg` gained since
/// `old_labels`: one `message.<label>` per newly-added system label, in the
/// message's label order. A label that was already there, one a PATCH added,
/// and a removed label all publish nothing, so a MODIFY that only touches
/// mailbox state (a read receipt, a user's own label) is silent.
///
/// The event carries the list-view message (no body): these fire on every
/// delivery notification for a message whose content document is already
/// published with its `received`/send event, so re-reading it per label
/// would cost an S3 GET for a payload the consumer has.
#[must_use]
pub fn build_label_events(
    msg: &MailMessage,
    old_labels: &[String],
    event_source: &str,
) -> Vec<MailEvent> {
    let mut events = Vec::new();
    for label in &msg.labels {
        if old_labels.contains(label) {
            continue;
        }
        let Some(event_type) = label_event_type(label) else {
            continue;
        };
        // ts_ms is when the label was applied — `updated_at` is the write
        // that added it, so a replay of the same stream record rebuilds the
        // same event id while a later label gets its own.
        let ts_ms = time::parse(&msg.updated_at).unwrap_or(0);
        let event_id = ids::event_id(msg.inbox_id.as_str(), &msg.message_id, event_type, ts_ms);
        let mut detail = json!({
            "type": "event",
            "event_type": event_type,
            "event_id": event_id,
            "message": to_json(&wire::MessageItem::from(msg)),
            "schemaVersion": SCHEMA_VERSION,
            "meta": event_meta(msg),
        });
        cap_mail_detail(&mut detail, event_type, event_source);
        events.push(MailEvent {
            detail_type: event_type,
            detail,
        });
    }
    events
}

/// The detail budget for one entry: the `PutEvents` cap less the
/// `detail_type` and event `Source` that ride along with it.
fn entry_cap(detail_type: &str, event_source: &str) -> usize {
    PUT_EVENTS_ENTRY_CAP_BYTES.saturating_sub(detail_type.len() + event_source.len())
}

/// Reduces an oversized mail detail to fit the `PutEvents` entry cap:
/// drops `message.html`, `message.text` and `message.headers`, reduces
/// `thread` to `{thread_id}`, then falls back to
/// `message = {payloadOmitted, ids}`. Never drops `meta`. Mirrors
/// `crate::publish::build_outbound`'s ladder, reserving `detail_type` plus
/// the configured event `Source` as `PutEvents` entry headroom the same way.
pub fn cap_mail_detail(detail: &mut Value, detail_type: &str, event_source: &str) {
    let cap = entry_cap(detail_type, event_source);
    if detail_bytes(detail) <= cap {
        return;
    }

    if let Some(html) = detail.pointer_mut("/message/html")
        && !html.is_null()
    {
        *html = Value::Null;
    }
    if detail_bytes(detail) <= cap {
        return;
    }

    if let Some(text) = detail.pointer_mut("/message/text")
        && !text.is_null()
    {
        *text = Value::Null;
    }
    if detail_bytes(detail) <= cap {
        return;
    }

    if let Some(headers) = detail.pointer_mut("/message/headers") {
        *headers = json!({});
    }
    if detail_bytes(detail) <= cap {
        return;
    }

    if let Some(thread_id) = detail.pointer("/thread/thread_id").cloned() {
        detail["thread"] = json!({ "thread_id": thread_id });
    }
    if detail_bytes(detail) <= cap {
        return;
    }

    // The reduced payload is `message = {payloadOmitted, ids}` without
    // naming `ids`' fields; `messageId`/`threadId` mirror `meta`'s keys so a
    // consumer that only reads `message` still gets both identifiers.
    let ids = json!({
        "messageId": detail["meta"]["messageId"].clone(),
        "threadId": detail["meta"]["threadId"].clone(),
    });
    detail["message"] = json!({ "payloadOmitted": true, "ids": ids });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::mail::{AttachmentMeta, Direction, InboxId, ThreadSnapshot};

    const EVENT_SOURCE: &str = "aws-messaging-webhook";

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
            thread_snapshot: Some(ThreadSnapshot {
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
            }),
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 1,
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
            expires_at: 0,
        }
    }

    #[test]
    fn plain_received_message_builds_one_event_with_the_golden_payload() {
        let msg = sample_message();
        let content = sample_content();
        let event = received(&msg, &content);
        assert_eq!(event.detail_type, "message.received");
        assert_eq!(event.detail["type"], "event");
        assert_eq!(event.detail["event_type"], "message.received");
        assert!(
            event.detail["event_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("evt_"))
        );
        assert_eq!(event.detail["schemaVersion"], json!(SCHEMA_VERSION));
        assert_eq!(event.detail["meta"]["messageId"], "mid-1");
        assert_eq!(event.detail["meta"]["inboxId"], "support@example.com");
        assert_eq!(event.detail["meta"]["threadId"], "tid-1");
        assert_eq!(event.detail["meta"]["sesMessageId"], "ses-1");
        assert_eq!(event.detail["message"]["message_id"], "mid-1");
        assert_eq!(event.detail["message"]["html"], "<p>Hello there</p>");
        assert_eq!(event.detail["thread"]["thread_id"], "tid-1");
        assert_eq!(event.detail["thread"]["message_count"], 1);
    }

    /// The relay publishes whatever `thread_snapshot` the message item
    /// carries verbatim, so a reply (whose snapshot `plan_insert` populated
    /// from the thread's full post-insert state) publishes the thread's real
    /// `message_count` and accumulated senders/recipients, not a
    /// single-message stub.
    #[test]
    fn a_reply_publishes_the_accumulated_thread_state() {
        let mut msg = sample_message();
        let content = sample_content();
        msg.message_id = "mid-2".to_owned();
        msg.thread_snapshot = Some(ThreadSnapshot {
            thread_id: "tid-1".to_owned(),
            subject: "Hello".to_owned(),
            preview: "Second message body".to_owned(),
            message_count: 2,
            labels: vec!["received".to_owned(), "unread".to_owned()],
            senders: vec![
                "other@example.com".to_owned(),
                "sender@example.com".to_owned(),
            ],
            recipients: vec!["support@example.com".to_owned()],
            timestamp: "2026-01-15T09:31:00.000Z".to_owned(),
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:31:00.000Z".to_owned(),
        });

        let event = received(&msg, &content);
        let thread = &event.detail["thread"];
        assert_eq!(thread["message_count"], 2);
        assert_eq!(thread["subject"], "Hello");
        assert_eq!(
            thread["senders"],
            json!(["other@example.com", "sender@example.com"])
        );
        assert_eq!(thread["recipients"], json!(["support@example.com"]));
    }

    #[test]
    fn spam_takes_precedence_over_unauthenticated() {
        let mut msg = sample_message();
        let content = sample_content();
        msg.labels = vec![
            "received".to_owned(),
            "spam".to_owned(),
            "unauthenticated".to_owned(),
        ];
        let event = received(&msg, &content);
        assert_eq!(event.detail_type, "message.received.spam");
        assert_eq!(event.detail["event_type"], "message.received.spam");
    }

    #[test]
    fn unauthenticated_without_spam() {
        let mut msg = sample_message();
        let content = sample_content();
        msg.labels = vec!["received".to_owned(), "unauthenticated".to_owned()];
        let event = received(&msg, &content);
        assert_eq!(event.detail_type, "message.received.unauthenticated");
    }

    #[test]
    fn no_received_label_builds_nothing() {
        let mut msg = sample_message();
        let content = sample_content();
        msg.labels = vec!["queued".to_owned()];
        assert!(build_received_event(&msg, &content, EVENT_SOURCE).is_none());
    }

    #[test]
    fn event_id_is_stable_across_a_replay_of_the_same_record() {
        let msg = sample_message();
        let content = sample_content();
        let a = received(&msg, &content);
        let b = received(&msg, &content);
        assert_eq!(a.detail["event_id"], b.detail["event_id"]);
    }

    /// A label a MODIFY added publishes its lifecycle event, carrying the
    /// list-view message (identifiers, labels and metadata, no body).
    #[test]
    fn a_newly_added_delivery_label_builds_its_event() {
        let mut msg = sample_message();
        msg.labels = vec!["sent".to_owned(), "delivered".to_owned()];
        let events = build_label_events(&msg, &["sent".to_owned()], EVENT_SOURCE);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].detail_type, "message.delivered");
        assert_eq!(events[0].detail["event_type"], "message.delivered");
        assert_eq!(events[0].detail["meta"]["messageId"], "mid-1");
        assert_eq!(events[0].detail["message"]["message_id"], "mid-1");
        assert_eq!(
            events[0].detail["message"]["labels"],
            json!(["sent", "delivered"])
        );
        assert_eq!(events[0].detail["message"]["text"], Value::Null);
    }

    /// The sender's `queued` → `sent` relabel is the message's send event.
    #[test]
    fn the_queued_to_sent_relabel_builds_the_sent_event() {
        let mut msg = sample_message();
        msg.labels = vec!["sent".to_owned()];
        let events = build_label_events(&msg, &["queued".to_owned()], EVENT_SOURCE);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].detail_type, "message.sent");
    }

    /// Every delivery label SES events apply has an event of its own, and
    /// several arriving in one write publish one event each.
    #[test]
    fn each_added_delivery_label_builds_its_own_event() {
        let mut msg = sample_message();
        msg.labels = vec![
            "sent".to_owned(),
            "bounced".to_owned(),
            "complained".to_owned(),
            "rejected".to_owned(),
            "opened".to_owned(),
        ];
        let events = build_label_events(&msg, &["sent".to_owned()], EVENT_SOURCE);
        let types: Vec<&str> = events.iter().map(|event| event.detail_type).collect();
        assert_eq!(
            types,
            vec![
                "message.bounced",
                "message.complained",
                "message.rejected",
                "message.opened",
            ]
        );
    }

    /// A write that re-states labels the message already had — the common
    /// case, since every MODIFY carries the whole label list — publishes
    /// nothing.
    #[test]
    fn unchanged_labels_build_nothing() {
        let mut msg = sample_message();
        msg.labels = vec!["sent".to_owned(), "delivered".to_owned()];
        let old = vec!["sent".to_owned(), "delivered".to_owned()];
        assert!(build_label_events(&msg, &old, EVENT_SOURCE).is_empty());
    }

    /// Mailbox state — a user's own label, a read receipt, a trashed or
    /// spam-marked message — is not a lifecycle transition.
    #[test]
    fn user_and_mailbox_state_labels_build_nothing() {
        let mut msg = sample_message();
        msg.labels = vec![
            "received".to_owned(),
            "unread".to_owned(),
            "trash".to_owned(),
            "spam".to_owned(),
            "invoices".to_owned(),
        ];
        let old = vec!["received".to_owned()];
        assert!(build_label_events(&msg, &old, EVENT_SOURCE).is_empty());
    }

    /// The label event's id is derived from the write that applied it, so a
    /// redelivered stream record rebuilds the same id a consumer already
    /// saw, while a later label gets its own.
    #[test]
    fn label_event_ids_are_stable_per_write_and_distinct_per_label() {
        let mut msg = sample_message();
        msg.labels = vec!["sent".to_owned(), "delivered".to_owned()];
        let old = vec!["sent".to_owned()];
        let first = build_label_events(&msg, &old, EVENT_SOURCE);
        let replay = build_label_events(&msg, &old, EVENT_SOURCE);
        assert_eq!(first[0].detail["event_id"], replay[0].detail["event_id"]);

        let mut opened = msg.clone();
        opened.labels.push("opened".to_owned());
        opened.updated_at = "2026-01-15T10:00:00.000Z".to_owned();
        let later = build_label_events(&opened, &msg.labels, EVENT_SOURCE);
        assert_ne!(first[0].detail["event_id"], later[0].detail["event_id"]);
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
        assert_eq!(event.detail["message"]["html"], Value::Null);
        assert_eq!(event.detail["message"]["text"], "Hello there");
        assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
    }

    #[test]
    fn oversized_html_and_text_falls_back_to_headers_then_payload_omitted() {
        let msg = sample_message();
        let mut content = sample_content();
        content.html = Some("x".repeat(150_000));
        content.text = Some("y".repeat(150_000));
        content.headers = BTreeMap::from([("X-Big".to_owned(), "z".repeat(50_000))]);
        let event = received(&msg, &content);
        let detail = &event.detail;
        assert!(entry_bytes(&event, EVENT_SOURCE) <= PUT_EVENTS_ENTRY_CAP_BYTES);
        // meta always survives.
        assert_eq!(detail["meta"]["messageId"], "mid-1");
    }

    #[test]
    fn extreme_oversize_falls_back_to_payload_omitted_with_meta_intact() {
        // Filenames alone (not touched by the html/text/headers/thread
        // reduction steps) stay well over the cap, forcing the final
        // `payloadOmitted` fallback.
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
        assert_eq!(detail["message"]["payloadOmitted"], json!(true));
        assert_eq!(detail["message"]["ids"]["messageId"], "mid-1");
        assert_eq!(detail["meta"]["messageId"], "mid-1");
        assert_eq!(detail["meta"]["threadId"], "tid-1");
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
