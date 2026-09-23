//! A message's write-once content, stored as one JSON document in the mail
//! bucket rather than on the message item.
//!
//! Bodies and headers are most of a message's bytes and never change after
//! the message is stored, while the item is rewritten on every label change,
//! send-status transition and delivery event, and copied into two indexes.
//! Keeping the content out of the item keeps those writes small, and lets a
//! body of any size be stored whole.
//!
//! The document is written before the item, so a stored item always has its
//! document until both expire together.

use std::collections::BTreeMap;

use axum::body::Bytes;
use serde::{Deserialize, Serialize};

use crate::mail::objects::{ObjectError, ObjectStore};
use crate::mail::{
    ADDRESS_MAX_BYTES, HEADERS_BUDGET, INBOUND_ADDRESS_LIST_MAX, InboxId, MAX_INBOUND_RAW_BYTES,
    MailMessage, REFERENCES_MAX,
};

/// The largest document [`store`] writes or [`load`] accepts. The two share
/// one cap so a document the application itself stored can always be read
/// back: [`store`] rejects anything larger before it writes, so a stored
/// item always has a document [`load`] can read — the round-trip invariant
/// the rest of this module relies on.
///
/// The cap is sized for the worst case [`serde_json`] can produce from a
/// body the ingest path accepts under [`MAX_INBOUND_RAW_BYTES`]. Each C0
/// control code in a string escapes to the six-byte `\u00XX` sequence, so a
/// body of control characters inflates 6:1 under `serde_json::to_vec`, not
/// the 2:1 a `"`/`\`-only body would. The same 6:1 worst case applies to
/// the equally attackable envelope — `headers`, `references` and
/// `reply_to`, each bounded above by its own cap — so the limit is `6 ×`
/// the raw-mail ceiling plus `6 ×` that envelope's combined budget.
/// `verdicts` comes from the SES notification rather than the raw mail and
/// is not bounded by these constants, so the [`store`]-time check is what
/// keeps the invariant closed if it or any future field ever exceeds the
/// cap.
const MAX_CONTENT_BYTES: u64 = 6 * MAX_INBOUND_RAW_BYTES
    + 6 * (HEADERS_BUDGET as u64
        + (REFERENCES_MAX as u64) * (ADDRESS_MAX_BYTES as u64)
        + (INBOUND_ADDRESS_LIST_MAX as u64) * (ADDRESS_MAX_BYTES as u64));

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MessageContent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reply_to: Vec<String>,
    /// Inbound only: the spam, virus and authentication verdicts SES
    /// reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdicts: Option<serde_json::Value>,
}

/// Where a message's document lives: per inbox, because one inbound message
/// delivered to two inboxes is two independent messages.
#[must_use]
pub fn content_key(inbox_id: &InboxId, message_id: &str) -> String {
    format!("messages/{}/{message_id}.json", inbox_id.as_str())
}

/// Stores `content` for a message. A document already there is left alone:
/// message ids are deterministic for inbound mail, so a redelivery writes
/// the same bytes.
///
/// The serialized document is rejected up front when it exceeds
/// [`MAX_CONTENT_BYTES`], the same limit [`load`] enforces, so nothing
/// `load` cannot read is ever written: a stored item always has a readable
/// document even for a body of control characters that `serde_json` escapes
/// six bytes to one.
///
/// # Errors
///
/// [`ObjectError::Permanent`] when the document cannot be serialized or
/// exceeds [`MAX_CONTENT_BYTES`]; [`ObjectError`] from the object store
/// when the write fails.
pub async fn store<S: ObjectStore>(
    objects: &S,
    inbox_id: &InboxId,
    message_id: &str,
    content: &MessageContent,
) -> Result<(), ObjectError> {
    let body = serde_json::to_vec(content)
        .map_err(|e| ObjectError::Permanent(anyhow::anyhow!("serializing message content: {e}")))?;
    enforce_content_size(body.len())?;
    objects
        .put_object_if_absent(
            &content_key(inbox_id, message_id),
            Bytes::from(body),
            "application/json",
        )
        .await
        .map(|_outcome| ())
}

/// Rejects a serialized document over [`MAX_CONTENT_BYTES`], the same limit
/// [`load`] enforces, so the two agree on what a stored document can be. A
/// `usize` length that does not fit in a `u64` clamps to `u64::MAX`, which
/// is over any real cap.
fn enforce_content_size(len: usize) -> Result<(), ObjectError> {
    let size = u64::try_from(len).unwrap_or(u64::MAX);
    if size > MAX_CONTENT_BYTES {
        return Err(ObjectError::Permanent(anyhow::anyhow!(
            "serialized message content ({size} bytes) exceeds the {MAX_CONTENT_BYTES}-byte document cap"
        )));
    }
    Ok(())
}

/// Loads a message's document. A missing document reads as empty content:
/// it only happens once retention has removed it, and the item's own fields
/// are still worth returning.
///
/// Reads under [`MAX_CONTENT_BYTES`], the same cap [`store`] enforces, so a
/// document the application stored can always be read back.
///
/// # Errors
///
/// [`ObjectError`] when the document cannot be read or does not parse.
pub async fn load<S: ObjectStore>(
    objects: &S,
    msg: &MailMessage,
) -> Result<MessageContent, ObjectError> {
    let key = content_key(&msg.inbox_id, &msg.message_id);
    let body = match objects.get_object(&key, MAX_CONTENT_BYTES).await {
        Ok(body) => body,
        Err(ObjectError::NotFound) => {
            tracing::warn!(
                inbox_id = msg.inbox_id.as_str(),
                message_id = msg.message_id,
                event = "message_content_missing",
                "message content document is missing; returning the item alone"
            );
            return Ok(MessageContent::default());
        }
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&body)
        .map_err(|e| ObjectError::Permanent(anyhow::anyhow!("parsing {key}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_key_is_scoped_to_the_inbox() {
        assert_eq!(
            content_key(&InboxId("support@example.com".to_owned()), "01a0-msg"),
            "messages/support@example.com/01a0-msg.json"
        );
    }

    #[test]
    fn empty_content_round_trips_as_an_empty_object() {
        let json = serde_json::to_string(&MessageContent::default()).unwrap();
        assert_eq!(json, "{}");
        let back: MessageContent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MessageContent::default());
    }

    /// The cap must cover the 6:1 worst case `serde_json` applies to a body
    /// of control characters, not the 2:1 a `"`/`\`-only body would — that
    /// 2:1 assumption is the bug. The formula pins it so a revert to the
    /// old `2 × MAX_INBOUND_RAW_BYTES` value is caught (it is manifestly
    /// less than `6 × MAX_INBOUND_RAW_BYTES` for a positive raw ceiling).
    #[test]
    fn max_content_bytes_sizes_the_six_x_worst_case_expansion() {
        let envelope = HEADERS_BUDGET as u64
            + (REFERENCES_MAX as u64) * (ADDRESS_MAX_BYTES as u64)
            + (INBOUND_ADDRESS_LIST_MAX as u64) * (ADDRESS_MAX_BYTES as u64);
        assert_eq!(MAX_CONTENT_BYTES, 6 * MAX_INBOUND_RAW_BYTES + 6 * envelope);
    }

    /// `serde_json` escapes a NUL byte as the 6-byte `\u0000` sequence — the
    /// 6:1 inflation the cap is sized for. A `"`/`\`-only body would inflate
    /// only 2:1, the assumption the old `2 ×` cap made.
    #[test]
    fn serde_json_inflates_a_control_char_body_six_fold() {
        let content = MessageContent {
            text: Some("\u{0}".repeat(1000)),
            ..MessageContent::default()
        };
        let json = serde_json::to_vec(&content).unwrap();
        // `{"text":"` (9) + 6 bytes per NUL + `"` + `}` (2) = 6 * 1000 + 11.
        assert_eq!(json.len(), 6 * 1000 + 11);
    }

    /// The store-time check shares `load`'s cap exactly: anything at or under
    /// the cap is accepted, one byte over is a permanent rejection.
    #[test]
    fn enforce_content_size_accepts_the_cap_and_rejects_one_byte_over() {
        let cap = usize::try_from(MAX_CONTENT_BYTES).unwrap();
        assert!(enforce_content_size(cap).is_ok());
        assert!(matches!(
            enforce_content_size(cap.checked_add(1).unwrap()),
            Err(ObjectError::Permanent(_))
        ));
    }
}
