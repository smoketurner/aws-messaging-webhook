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
//! document until both expire together. This module is the document; the
//! binary stores and loads it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::MAX_INBOUND_RAW_BYTES;

/// The largest document the mailbox writes and reads. Bodies come from a raw
/// message of at most [`MAX_INBOUND_RAW_BYTES`] and JSON escaping grows them,
/// by up to six times for control characters, so the cap is enforced where
/// the document is written rather than assumed from the input size.
pub const MAX_CONTENT_BYTES: u64 = 2 * MAX_INBOUND_RAW_BYTES;

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

/// Serializes `content` to fit within `max_bytes`, degrading only as much as
/// needed and re-checking after every step, so a stored document is always
/// small enough to be read back under [`MAX_CONTENT_BYTES`]. The whole document is kept when
/// it fits; otherwise `text` and `html` are dropped (they remain reachable in
/// the raw message), then `references`, `reply_to`, `headers` and `verdicts`
/// in turn until the serialized document fits. The surviving fields are
/// bounded by their parse-time caps, so this fallback is reached only for a
/// pathological id or header; the result is always within `max_bytes`. Returns
/// whether any field was left out.
///
/// # Errors
///
/// A serialization failure.
pub fn serialize_within(
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
        let parsed = crate::mime::parse_inbound(raw.as_bytes()).unwrap();
        assert_eq!(parsed.content.references.len(), 1);
        assert_eq!(
            parsed.content.references[0].len(),
            crate::REFERENCES_MAX_BYTES,
            "the giant id is truncated to the per-entry cap at parse time"
        );
        let (body, dropped) = serialize_within(&parsed.content, MAX_CONTENT_BYTES).unwrap();
        assert!(!dropped, "with the id capped, the bodies are kept");
        assert!(u64::try_from(body.len()).unwrap_or(u64::MAX) <= MAX_CONTENT_BYTES);
        let back: MessageContent = serde_json::from_slice(&body).unwrap();
        assert!(back.text.is_some_and(|t| t.contains("body")));
    }

    #[test]
    fn empty_content_round_trips_as_an_empty_object() {
        let json = serde_json::to_string(&MessageContent::default()).unwrap();
        assert_eq!(json, "{}");
        let back: MessageContent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MessageContent::default());
    }
}
