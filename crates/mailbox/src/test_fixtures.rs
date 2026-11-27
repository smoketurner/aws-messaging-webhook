//! Shared unit-test fixtures for the mail types.

use crate::time;
use std::collections::BTreeMap;

use super::{Direction, InboxId, MailMessage};
use crate::labels::{Label, SystemLabel};
use crate::time::Timestamp;

/// The instant an RFC 3339 literal names.
///
/// # Panics
///
/// When `literal` is not an RFC 3339 timestamp: a typo in a test.
#[must_use]
#[expect(clippy::unwrap_used, reason = "a test fixture panics on a bad literal")]
pub fn at(literal: &str) -> Timestamp {
    time::parse_rfc3339(literal).unwrap()
}

/// The label a literal names.
///
/// # Panics
///
/// When `literal` is not a valid label: a typo in a test.
#[must_use]
#[expect(clippy::unwrap_used, reason = "a test fixture panics on a bad literal")]
pub fn label(literal: &str) -> Label {
    Label::parse(literal).unwrap()
}

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
        labels: vec![
            Label::from(SystemLabel::Received),
            Label::from(SystemLabel::Unread),
        ],
        timestamp: at("2026-01-15T09:30:00.000Z"),
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
        created_at: at("2026-01-15T09:30:00.000Z"),
        updated_at: at("2026-01-15T09:30:00.000Z"),
        expires_at: 0,
    }
}
