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
/// bodies (and, if still too large, further fields; see
/// `serialize_within`), so every stored document can be read back; what
/// is dropped stays reachable in the raw message, as attachments past the
/// cap do.
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
    let (body, fields_dropped) =
        serialize_within(content, MAX_CONTENT_BYTES).map_err(serialize_error)?;
    if fields_dropped {
        tracing::warn!(
            inbox_id = inbox_id.as_str(),
            message_id,
            max_bytes = MAX_CONTENT_BYTES,
            event = "message_content_bodies_dropped",
            "message content exceeds the document cap once serialized; storing it without its bodies, and any further fields needed to fit, all of which remain in the raw message"
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

/// Serializes `content` to fit within `max_bytes`, degrading only as much as
/// needed and re-checking after every step, so a stored document is always
/// small enough for [`load`] to read back. The whole document is kept when
/// it fits; otherwise `text` and `html` are dropped (they remain reachable in
/// the raw message), then `references`, `reply_to`, `headers` and `verdicts`
/// in turn until the serialized document fits. The surviving fields are
/// bounded by their parse-time caps, so this fallback is reached only for a
/// pathological id or header; the result is always within `max_bytes`. Returns
/// whether any field was left out.
fn serialize_within(
    content: &MessageContent,
    max_bytes: u64,
) -> Result<(Vec<u8>, bool), serde_json::Error> {
    let within = |bytes: &[u8]| u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= max_bytes;

    let body = serde_json::to_vec(content)?;
    if within(&body) {
        return Ok((body, false));
    }

    // Drop the bodies first: they are the bulk of the bytes and remain
    // reachable in the raw message. Avoid cloning them.
    let mut degraded = MessageContent {
        text: None,
        html: None,
        headers: content.headers.clone(),
        references: content.references.clone(),
        reply_to: content.reply_to.clone(),
        verdicts: content.verdicts.clone(),
    };
    let mut body = serde_json::to_vec(&degraded)?;
    if within(&body) {
        return Ok((body, true));
    }

    // A pathological `References` id JSON-escapes 6× and can push the
    // "without bodies" document past the cap on its own. Keep dropping the
    // remaining fields until it fits; everything dropped is still reachable
    // in the raw message.
    degraded.references = Vec::new();
    body = serde_json::to_vec(&degraded)?;
    if within(&body) {
        return Ok((body, true));
    }
    degraded.reply_to = Vec::new();
    body = serde_json::to_vec(&degraded)?;
    if within(&body) {
        return Ok((body, true));
    }
    degraded.headers = BTreeMap::new();
    body = serde_json::to_vec(&degraded)?;
    if within(&body) {
        return Ok((body, true));
    }
    degraded.verdicts = None;
    // An empty `MessageContent` serializes to "{}" (2 bytes), so this fits
    // for any cap as large as `MAX_CONTENT_BYTES`.
    Ok((serde_json::to_vec(&degraded)?, true))
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
    fn an_oversized_references_id_is_dropped_so_the_document_fits() {
        // A single `References` id of control characters JSON-escapes 6× and
        // can push the "without bodies" document past the cap on its own — the
        // case the bodies-only fallback missed. Dropping `references` next (the
        // largest surviving field after the bodies) must bring it back under.
        let content = MessageContent {
            text: Some("\0".repeat(10_000)),
            html: None,
            references: vec!["<".to_owned() + &"\0".repeat(10_000) + ">"],
            reply_to: vec!["a@example.com".to_owned()],
            headers: BTreeMap::from([("Subject".to_owned(), "hi".to_owned())]),
            verdicts: Some(serde_json::json!({"spam": "PASS"})),
        };
        let (body, dropped) = serialize_within(&content, 20_000).unwrap();
        assert!(dropped);
        assert!(body.len() <= 20_000, "{}", body.len());
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert!(back.text.is_none());
        assert!(
            back.references.is_empty(),
            "the oversized id was dropped to fit"
        );
        // The smaller, bounded fields survive.
        assert_eq!(back.reply_to, content.reply_to);
        assert_eq!(back.headers, content.headers);
        assert_eq!(back.verdicts, content.verdicts);
    }

    #[test]
    fn degradation_drops_fields_in_order_until_the_empty_document_fits() {
        // Every field is oversized, so each is dropped in turn — bodies,
        // references, reply_to, headers, verdicts — until the serialized
        // document fits. The final empty document ("{}") always fits.
        let content = MessageContent {
            text: Some("\0".repeat(10_000)),
            html: Some("\0".repeat(10_000)),
            references: vec!["<".to_owned() + &"\0".repeat(10_000) + ">"],
            reply_to: vec!["x".repeat(10_000)],
            headers: BTreeMap::from([("X-Big".to_owned(), "\0".repeat(10_000))]),
            verdicts: Some(serde_json::Value::String("\0".repeat(10_000))),
        };
        let (body, dropped) = serialize_within(&content, 100).unwrap();
        assert!(dropped);
        assert!(body.len() <= 100, "{}", body.len());
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert_eq!(back, MessageContent::default());
    }

    /// Reproduces the bug report end-to-end against the real parser: a
    /// `References` id of NUL bytes — the raw email is ~16 MB, under the 40 MB
    /// SES receive limit — JSON-escapes sixfold. Before the fix this kept a
    /// ~96 MB "without bodies" document that `load()` rejected; after it, the
    /// id is capped at parse time and the document fits with its bodies kept.
    #[test]
    fn a_giant_references_id_in_a_real_message_stores_within_the_cap() {
        let giant_id = "\0".repeat(16_000_000);
        let raw = format!(
            "From: a@example.com\r\nTo: b@example.com\r\n\
             Subject: repro\r\nMessage-ID: <m@example.com>\r\n\
             References: <{giant_id}>\r\nMIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n"
        );
        assert!(
            raw.len() < 40_000_000,
            "raw email is under the SES receive limit"
        );
        let parsed = crate::mail::mime::parse_inbound(raw.as_bytes()).unwrap();
        assert_eq!(parsed.content.references.len(), 1);
        assert_eq!(
            parsed.content.references[0].len(),
            crate::mail::REFERENCES_MAX_BYTES,
            "the giant id is truncated to the per-entry cap at parse time"
        );
        let (body, dropped) = serialize_within(&parsed.content, MAX_CONTENT_BYTES).unwrap();
        assert!(!dropped, "with the id capped, the bodies are kept");
        assert!(u64::try_from(body.len()).unwrap_or(u64::MAX) <= MAX_CONTENT_BYTES);
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert!(back.text.is_some_and(|t| t.contains("body")));
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
