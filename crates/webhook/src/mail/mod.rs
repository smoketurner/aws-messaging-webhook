//! The mail inbox: caps, core storage types, size/id/key helpers, the
//! transactional write model, and the store and object-store traits every
//! mail flow is built on.
//!
//! Every field below that carries `skip_serializing_if` must also carry
//! `default`. `serde_dynamo::to_item` omits the attribute when the predicate
//! holds, and without a matching `default`, `serde_dynamo::from_item` fails
//! with "missing field" when reading that item back rather than
//! reconstructing the omitted value — which would break the stream relay and
//! every store read.

pub mod ids;
pub mod keys;
pub mod objects;
pub mod plan;
pub mod send;
pub mod sender;
pub mod store;
pub mod time;
pub mod txn;
pub mod url_policy;
pub mod wire;

pub mod build;
pub mod content;
pub mod events;
pub mod fetch;
pub mod ingest;
pub mod labels;
pub mod mime;
pub mod thread;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Caps
// ---------------------------------------------------------------------------

pub const MESSAGE_USER_LABEL_CAP: usize = 20;
pub const THREAD_USER_LABEL_CAP: usize = 20;
pub const PATCH_LABEL_CHANGES_CAP: usize = 10;
/// Non-empty, no `#`, whitespace or control characters.
pub const LABEL_MAX_BYTES: usize = 64;
pub const SUBJECT_MAX_BYTES: usize = 2_000;
pub const ADDRESS_MAX_BYTES: usize = 320;
pub const INBOUND_ADDRESS_LIST_MAX: usize = 100;
pub const OUTBOUND_RECIPIENTS_MAX: usize = 50;
pub const REFERENCES_MAX: usize = 50;
/// Aliases skip ids over 900 bytes.
pub const RFC_ID_MAX_BYTES: usize = 998;
pub const HEADER_NAME_MAX: usize = 128;
pub const HEADER_VALUE_MAX: usize = 998;
pub const HEADERS_BUDGET: usize = 32_000;
pub const ATTACHMENTS_MAX: usize = 100;
pub const ATTACHMENT_FIELD_MAX: usize = 255;
pub const PREVIEW_CHARS: usize = 256;
/// SES receives up to 40 MB.
pub const MAX_INBOUND_RAW_BYTES: u64 = 45_000_000;
pub const MAX_OUTBOUND_DECODED_BYTES: u64 = 28_000_000;
pub const MAX_OUTBOUND_RAW_BYTES: u64 = 39_500_000;
pub const RESPONSE_BUDGET_BYTES: usize = 5_000_000;
pub const THREAD_ATTACHMENT_SUMMARIES: usize = 20;
pub const THREAD_ATTACHMENT_ROUTE_MAX_KEYS: usize = 500;

// ---------------------------------------------------------------------------
// Core storage types
// ---------------------------------------------------------------------------

/// An inbox identifier: the local part of an inbound address (e.g.
/// `support` for `support@example.com`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InboxId(pub String);

impl InboxId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InboxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for InboxId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Inbound,
    Outbound,
}

/// The `Inbox` item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inbox {
    pub inbox_id: InboxId,
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}

/// A first-writer-wins RFC alias hit: the message the alias points to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RfcHit {
    pub message_id: String,
    pub thread_id: String,
}

/// One kept attachment's metadata on a message item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentMeta {
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    pub content_type: String,
    pub content_disposition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
}

/// The inbound-only `thread_snapshot`: a compact view of the thread as
/// of this message's arrival, so a reader of the message alone (e.g. an
/// EventBridge consumer) doesn't need a second read to show thread context.
///
/// Populated by [`plan::plan_insert`] from the caller-computed
/// `thread_after` state (`mail::thread::ThreadState`), so it always reflects
/// the thread **including** the message it's attached to — a reply's
/// `message_count` is the thread's real count, not 1. `senders`/`recipients`
/// are capped at 20 even though the thread's own address sets may hold up
/// to 50 (`mail::thread::THREAD_ADDRESS_SET_CAP`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThreadSnapshot {
    pub thread_id: String,
    pub subject: String,
    pub preview: String,
    pub message_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub senders: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recipients: Vec<String>,
    pub timestamp: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The `Message` item: what lists, threads, labels and send status read and
/// update. The write-once content (bodies, headers, `References`,
/// `Reply-To`, verdicts) lives in S3 as a [`content::MessageContent`], which
/// keeps this item small. Every field here is bounded by the caps above.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailMessage {
    // Identity and threading.
    pub inbox_id: InboxId,
    pub thread_id: String,
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ses_message_id: Option<String>,
    pub direction: Direction,
    pub rfc_message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,

    // Summary.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub labels: Vec<String>,
    /// Fixed-width (`mail::time::format`).
    pub timestamp: String,
    pub from: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bcc: Vec<String>,
    pub subject: String,
    pub preview: String,
    pub size: u64,

    // Attachments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentMeta>,
    pub attachments_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_s3_key: Option<String>,

    // Inbound only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_snapshot: Option<ThreadSnapshot>,

    // Events.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub delivery: BTreeMap<String, serde_json::Value>,

    // Outbound-only mirror of the send state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<String>,

    // Bookkeeping.
    pub version: u64,
    pub created_at: String,
    pub updated_at: String,
    /// DynamoDB TTL (epoch seconds), matching the mail bucket's lifecycle
    /// expiry so the item and its S3 objects age out together.
    pub expires_at: u64,
}

/// Outcome of [`store::MailStore::insert_message`]. A redelivered ingest
/// (same deterministic message id) is a `Duplicate`, which the ingest flow
/// counts as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Fresh,
    Duplicate,
}

/// Metadata returned by [`objects::ObjectStore::head_object`].
#[derive(Debug, Clone, Copy)]
pub struct ObjectMeta {
    pub size: u64,
}

/// Outcome of [`objects::ObjectStore::put_object_if_absent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyExists,
}

/// A local part valid for an inbound mail address:
/// `^[a-z0-9._+-]{1,64}$`, which `config.rs` requires of `MAIL_INBOX`.
#[must_use]
pub(crate) fn is_valid_local_part(part: &str) -> bool {
    !part.is_empty()
        && part.len() <= 64
        && part.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'+' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_valid_local_part_accepts_the_full_character_class() {
        assert!(is_valid_local_part("support"));
        assert!(is_valid_local_part("a.b_c+d-e0"));
        assert!(is_valid_local_part(&"a".repeat(64)));
    }

    #[test]
    fn is_valid_local_part_rejects_empty_uppercase_whitespace_and_oversized() {
        assert!(!is_valid_local_part(""));
        assert!(!is_valid_local_part("Support"));
        assert!(!is_valid_local_part("Invoices"));
        assert!(!is_valid_local_part("has space"));
        assert!(!is_valid_local_part("has@at"));
        assert!(!is_valid_local_part(&"a".repeat(65)));
    }
}
