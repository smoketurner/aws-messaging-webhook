//! Shared unit-test fixtures for the mail types.

use std::collections::BTreeMap;

use super::{Direction, InboxId, MailMessage};

/// A received, unread inbound message `mid-1` in thread `tid-1` of
/// `support@example.com`. Tests shape it with struct-update syntax.
#[must_use]
pub fn message() -> MailMessage {
    MailMessage {
        inbox_id: InboxId::new("support@example.com"),
        thread_id: crate::ThreadId::new("tid-1"),
        message_id: crate::MessageId::new("mid-1"),
        ses_message_id: Some(crate::SesMessageId::new("ses-1")),
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
        thread_snapshot: None,
        delivery: BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 1,
        created_at: "2026-01-15T09:30:00.000Z".to_owned(),
        updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
        expires_at: 0,
    }
}
