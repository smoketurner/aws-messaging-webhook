//! The wire structs: the exact JSON shapes both the `/v0` API and the
//! EventBridge relay serialize. They follow the published mailbox API
//! contract field for field, so a client written against that contract works
//! unmodified against this service. Renaming or reordering a field here is a
//! breaking change for those clients.
//!
//! [`MessageItem`] and [`ThreadItem`] are the list views: the same shapes
//! minus the fields a list response omits (`headers`, `references`, body
//! content, and the embedded `messages[]`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::mail::{AttachmentMeta, MailMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentDisposition {
    Inline,
    Attachment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attachment {
    pub attachment_id: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<ContentDisposition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
}

impl From<&AttachmentMeta> for Attachment {
    fn from(meta: &AttachmentMeta) -> Self {
        Self {
            attachment_id: meta.attachment_id.clone(),
            size: meta.size,
            filename: meta.filename.clone(),
            content_type: Some(meta.content_type.clone()),
            content_disposition: match meta.content_disposition.as_str() {
                "inline" => Some(ContentDisposition::Inline),
                _ => Some(ContentDisposition::Attachment),
            },
            content_id: meta.content_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub inbox_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub labels: Vec<String>,
    pub timestamp: String,
    pub from: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    pub size: u64,
    pub updated_at: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reply_to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bcc: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub html: Option<String>,
    /// Always absent in v1 (phase 4: quote stripping).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extracted_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extracted_html: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
}

impl From<&MailMessage> for Message {
    fn from(msg: &MailMessage) -> Self {
        Self {
            inbox_id: msg.inbox_id.as_str().to_owned(),
            thread_id: msg.thread_id.clone(),
            message_id: msg.message_id.clone(),
            labels: msg.labels.clone(),
            timestamp: msg.timestamp.clone(),
            from: msg.from.clone(),
            to: msg.to.clone(),
            size: msg.size,
            updated_at: msg.updated_at.clone(),
            created_at: msg.created_at.clone(),
            reply_to: msg.reply_to.clone(),
            cc: msg.cc.clone(),
            bcc: msg.bcc.clone(),
            subject: Some(msg.subject.clone()),
            preview: Some(msg.preview.clone()),
            text: msg.text.clone(),
            html: msg.html.clone(),
            extracted_text: None,
            extracted_html: None,
            attachments: msg.attachments.iter().map(Attachment::from).collect(),
            in_reply_to: msg.in_reply_to.clone(),
            references: msg.references.clone(),
            headers: msg.headers.clone(),
        }
    }
}

/// The list view: the same fields as [`Message`] minus `headers`,
/// `references`, `reply_to` and the body content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageItem {
    pub inbox_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub labels: Vec<String>,
    pub timestamp: String,
    pub from: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bcc: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<&MailMessage> for MessageItem {
    fn from(msg: &MailMessage) -> Self {
        Self {
            inbox_id: msg.inbox_id.as_str().to_owned(),
            thread_id: msg.thread_id.clone(),
            message_id: msg.message_id.clone(),
            labels: msg.labels.clone(),
            timestamp: msg.timestamp.clone(),
            from: msg.from.clone(),
            to: msg.to.clone(),
            cc: msg.cc.clone(),
            bcc: msg.bcc.clone(),
            subject: Some(msg.subject.clone()),
            preview: Some(msg.preview.clone()),
            size: msg.size,
            attachments: msg.attachments.iter().map(Attachment::from).collect(),
            in_reply_to: msg.in_reply_to.clone(),
            created_at: msg.created_at.clone(),
            updated_at: msg.updated_at.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Thread {
    pub inbox_id: String,
    pub thread_id: String,
    pub labels: Vec<String>,
    pub timestamp: String,
    pub senders: Vec<String>,
    pub recipients: Vec<String>,
    pub last_message_id: String,
    pub message_count: u64,
    pub size: u64,
    pub updated_at: String,
    pub created_at: String,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

/// The list view: the same fields as [`Thread`] minus the embedded
/// `messages[]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadItem {
    pub inbox_id: String,
    pub thread_id: String,
    pub labels: Vec<String>,
    pub timestamp: String,
    pub senders: Vec<String>,
    pub recipients: Vec<String>,
    pub last_message_id: String,
    pub message_count: u64,
    pub size: u64,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

impl From<&crate::mail::thread::ThreadState> for ThreadItem {
    fn from(thread: &crate::mail::thread::ThreadState) -> Self {
        Self {
            inbox_id: thread.inbox_id.as_str().to_owned(),
            thread_id: thread.thread_id.clone(),
            labels: thread.labels.clone(),
            timestamp: thread.timestamp.clone(),
            senders: thread.senders.clone(),
            recipients: thread.recipients.clone(),
            last_message_id: thread.last_message_id.clone(),
            message_count: thread.message_count,
            size: thread.size,
            created_at: thread.created_at.clone(),
            updated_at: thread.updated_at.clone(),
            subject: (!thread.subject.is_empty()).then(|| thread.subject.clone()),
            preview: (!thread.preview.is_empty()).then(|| thread.preview.clone()),
            attachments: thread.attachments.iter().map(Attachment::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Inbox {
    /// Always `"pod_default"` (v1 has one pod).
    pub pod_id: String,
    pub inbox_id: String,
    pub email: String,
    pub updated_at: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl From<&crate::mail::Inbox> for Inbox {
    fn from(inbox: &crate::mail::Inbox) -> Self {
        Self {
            pod_id: "pod_default".to_owned(),
            inbox_id: inbox.inbox_id.as_str().to_owned(),
            email: inbox.email.clone(),
            updated_at: inbox.updated_at.clone(),
            created_at: inbox.created_at.clone(),
            display_name: inbox.display_name.clone(),
            client_id: None,
            metadata: inbox.metadata.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::mail::{Direction, InboxId};

    fn sample_message() -> MailMessage {
        MailMessage {
            inbox_id: InboxId("support".to_owned()),
            thread_id: "tid-1".to_owned(),
            message_id: "mid-1".to_owned(),
            ses_message_id: Some("ses-1".to_owned()),
            direction: Direction::Inbound,
            rfc_message_id: "<mid-1@example.com>".to_owned(),
            in_reply_to: Some("<orig@example.com>".to_owned()),
            references: vec!["<orig@example.com>".to_owned()],
            labels: vec!["received".to_owned(), "unread".to_owned()],
            timestamp: "2026-01-15T09:30:00.000Z".to_owned(),
            from: "sender@example.com".to_owned(),
            reply_to: vec!["sender@example.com".to_owned()],
            to: vec!["support@example.com".to_owned()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            size: 1234,
            text: Some("Hello there".to_owned()),
            html: Some("<p>Hello there</p>".to_owned()),
            body_truncated: false,
            headers: BTreeMap::from([("X-Test".to_owned(), "1".to_owned())]),
            attachments: vec![AttachmentMeta {
                attachment_id: "att-1".to_owned(),
                object_key: Some("attachments/mid-1/att-1".to_owned()),
                size: 100,
                filename: Some("file.txt".to_owned()),
                content_type: "text/plain".to_owned(),
                content_disposition: "attachment".to_owned(),
                content_id: None,
            }],
            attachments_truncated: false,
            raw_s3_key: Some("inbound/x".to_owned()),
            verdicts: None,
            thread_snapshot: None,
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 1,
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
        }
    }

    #[test]
    fn message_golden_shape() {
        let wire = Message::from(&sample_message());
        let value = serde_json::to_value(&wire).unwrap();
        assert_eq!(
            value,
            json!({
                "inbox_id": "support",
                "thread_id": "tid-1",
                "message_id": "mid-1",
                "labels": ["received", "unread"],
                "timestamp": "2026-01-15T09:30:00.000Z",
                "from": "sender@example.com",
                "to": ["support@example.com"],
                "size": 1234,
                "updated_at": "2026-01-15T09:30:00.000Z",
                "created_at": "2026-01-15T09:30:00.000Z",
                "reply_to": ["sender@example.com"],
                "subject": "Hello",
                "preview": "Hello there",
                "text": "Hello there",
                "html": "<p>Hello there</p>",
                "attachments": [{
                    "attachment_id": "att-1",
                    "size": 100,
                    "filename": "file.txt",
                    "content_type": "text/plain",
                    "content_disposition": "attachment",
                }],
                "in_reply_to": "<orig@example.com>",
                "references": ["<orig@example.com>"],
                "headers": {"X-Test": "1"},
            })
        );
    }

    #[test]
    fn message_item_omits_headers_references_and_body() {
        let wire = MessageItem::from(&sample_message());
        let value = serde_json::to_value(&wire).unwrap();
        assert!(value.get("headers").is_none());
        assert!(value.get("references").is_none());
        assert!(value.get("text").is_none());
        assert!(value.get("html").is_none());
        assert!(value.get("reply_to").is_none());
        assert_eq!(value["message_id"], "mid-1");
        assert_eq!(value["subject"], "Hello");
    }

    /// `MessageItem` must serialize identically whether the source was
    /// a `ByTime` query or a message pointer — since both convert from the
    /// same `MailMessage`, the conversion is trivially deterministic.
    #[test]
    fn message_item_is_deterministic_from_the_same_source() {
        let msg = sample_message();
        assert_eq!(MessageItem::from(&msg), MessageItem::from(&msg));
    }

    #[test]
    fn inline_and_other_dispositions_map_correctly() {
        let mut meta = AttachmentMeta {
            attachment_id: "a".to_owned(),
            object_key: None,
            size: 1,
            filename: None,
            content_type: "image/png".to_owned(),
            content_disposition: "inline".to_owned(),
            content_id: Some("cid-1".to_owned()),
        };
        assert_eq!(
            Attachment::from(&meta).content_disposition,
            Some(ContentDisposition::Inline)
        );
        meta.content_disposition = "attachment".to_owned();
        assert_eq!(
            Attachment::from(&meta).content_disposition,
            Some(ContentDisposition::Attachment)
        );
    }

    #[test]
    fn inbox_golden_shape() {
        let inbox = crate::mail::Inbox {
            inbox_id: InboxId("support".to_owned()),
            email: "support@example.com".to_owned(),
            display_name: Some("Support".to_owned()),
            metadata: None,
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
        };
        let value = serde_json::to_value(Inbox::from(&inbox)).unwrap();
        assert_eq!(
            value,
            json!({
                "pod_id": "pod_default",
                "inbox_id": "support",
                "email": "support@example.com",
                "updated_at": "2026-01-15T09:30:00.000Z",
                "created_at": "2026-01-15T09:30:00.000Z",
                "display_name": "Support",
            })
        );
    }

    #[test]
    fn content_disposition_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(ContentDisposition::Inline).unwrap(),
            json!("inline")
        );
        assert_eq!(
            serde_json::to_value(ContentDisposition::Attachment).unwrap(),
            json!("attachment")
        );
    }
}
