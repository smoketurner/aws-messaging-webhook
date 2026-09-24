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
use crate::mail::{InboxId, MAX_INBOUND_RAW_BYTES, MailMessage};

/// The largest document [`store`] writes and [`load`] reads. Bodies come from
/// a raw message of at most [`MAX_INBOUND_RAW_BYTES`] and JSON escaping grows
/// them, by up to six times for control characters, so the cap is enforced
/// where the document is written rather than assumed from the input size.
const MAX_CONTENT_BYTES: u64 = 2 * MAX_INBOUND_RAW_BYTES;

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
/// A document that would exceed [`MAX_CONTENT_BYTES`] is stored without its
/// bodies, so every stored document can be read back; the bodies stay
/// reachable in the raw message, as attachments past the cap do.
///
/// # Errors
///
/// [`ObjectError`] when the document cannot be serialized or written.
pub async fn store<S: ObjectStore>(
    objects: &S,
    inbox_id: &InboxId,
    message_id: &str,
    content: &MessageContent,
) -> Result<(), ObjectError> {
    let serialize_error =
        |e| ObjectError::Permanent(anyhow::anyhow!("serializing message content: {e}"));
    let (body, bodies_dropped) =
        serialize_within(content, MAX_CONTENT_BYTES).map_err(serialize_error)?;
    if bodies_dropped {
        tracing::warn!(
            inbox_id = inbox_id.as_str(),
            message_id,
            max_bytes = MAX_CONTENT_BYTES,
            event = "message_content_bodies_dropped",
            "message content exceeds the document cap once serialized; storing it without its bodies, which remain in the raw message"
        );
    }
    objects
        .put_object_if_absent(
            &content_key(inbox_id, message_id),
            Bytes::from(body),
            "application/json",
        )
        .await
        .map(|_outcome| ())
}

/// Serializes `content`, leaving out `text` and `html` when the whole
/// document would exceed `max_bytes`. The rest is bounded by the header and
/// address caps, so it always fits. Returns whether the bodies were left out.
fn serialize_within(
    content: &MessageContent,
    max_bytes: u64,
) -> Result<(Vec<u8>, bool), serde_json::Error> {
    let body = serde_json::to_vec(content)?;
    if u64::try_from(body.len()).unwrap_or(u64::MAX) <= max_bytes {
        return Ok((body, false));
    }
    let without_bodies = MessageContent {
        text: None,
        html: None,
        headers: content.headers.clone(),
        references: content.references.clone(),
        reply_to: content.reply_to.clone(),
        verdicts: content.verdicts.clone(),
    };
    Ok((serde_json::to_vec(&without_bodies)?, true))
}

/// Loads a message's document. A missing document reads as empty content:
/// it only happens once retention has removed it, and the item's own fields
/// are still worth returning.
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
    fn a_document_within_the_cap_keeps_its_bodies() {
        let content = MessageContent {
            text: Some("hello".to_owned()),
            html: Some("<p>hello</p>".to_owned()),
            ..MessageContent::default()
        };
        let (body, dropped) = serialize_within(&content, 1_000).unwrap();
        assert!(!dropped);
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert_eq!(back, content);
    }

    #[test]
    fn escaping_past_the_cap_drops_only_the_bodies() {
        // 100 NULs are 100 raw bytes but 600 once JSON-escaped as `\u0000`:
        // the cap has to be checked on the serialized document.
        let content = MessageContent {
            text: Some("\0".repeat(100)),
            html: Some("<p>hi</p>".to_owned()),
            headers: BTreeMap::from([("Subject".to_owned(), "hi".to_owned())]),
            references: vec!["<a@example.com>".to_owned()],
            reply_to: vec!["a@example.com".to_owned()],
            verdicts: Some(serde_json::json!({"spam": "PASS"})),
        };
        let (body, dropped) = serialize_within(&content, 300).unwrap();
        assert!(dropped);
        assert!(body.len() <= 300);
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            back,
            MessageContent {
                text: None,
                html: None,
                ..content
            }
        );
    }

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
}
