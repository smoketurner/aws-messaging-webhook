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

/// The largest document a read accepts. Bodies come from a raw message of at
/// most [`MAX_INBOUND_RAW_BYTES`], and JSON escaping can grow them.
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
/// # Errors
///
/// [`ObjectError`] when the document cannot be serialized or written.
pub async fn store<S: ObjectStore>(
    objects: &S,
    inbox_id: &InboxId,
    message_id: &str,
    content: &MessageContent,
) -> Result<(), ObjectError> {
    let body = serde_json::to_vec(content)
        .map_err(|e| ObjectError::Permanent(anyhow::anyhow!("serializing message content: {e}")))?;
    objects
        .put_object_if_absent(
            &content_key(inbox_id, message_id),
            Bytes::from(body),
            "application/json",
        )
        .await
        .map(|_outcome| ())
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
    fn content_key_is_scoped_to_the_inbox() {
        assert_eq!(
            content_key(&InboxId("support".to_owned()), "01a0-msg"),
            "messages/support/01a0-msg.json"
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
