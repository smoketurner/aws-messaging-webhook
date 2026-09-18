//! `Idempotency-Key` handling: what a key may look like, what request it
//! stands for, and what a repeat of it means.
//!
//! The fingerprint is a digest of the whole request — every field, every
//! attachment's metadata, the route and the message being replied to — so a
//! key reused with anything different is a conflict rather than a silent
//! second send of the first request.

use axum::http::HeaderMap;
use sha2::{Digest as _, Sha256};

use crate::api::error::ApiError;
use crate::api::send::SendAccepted;
use crate::api::send::validate::{AttachmentSource, ValidatedSend};
use crate::mail::InboxId;
use crate::state::{AppState, Services};

/// How long an `Idempotency-Key` is remembered.
pub(super) const KEY_TTL_SECONDS: u64 = 24 * 60 * 60;

/// The longest `Idempotency-Key` accepted.
pub(super) const IDEMPOTENCY_KEY_MAX_BYTES: usize = 255;

/// Hashes the `Idempotency-Key` header, if one was sent.
///
/// The header value itself is never stored or logged: another caller who
/// learned it could replay someone else's send.
pub(super) fn idempotency_key_hash(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::field("Idempotency-Key", "must be printable ASCII"))?
        .trim();
    if value.is_empty() || value.len() > IDEMPOTENCY_KEY_MAX_BYTES {
        return Err(ApiError::field(
            "Idempotency-Key",
            format!("must be 1 to {IDEMPOTENCY_KEY_MAX_BYTES} characters"),
        ));
    }
    Ok(Some(format!("{:x}", Sha256::digest(value.as_bytes()))))
}

/// A fingerprint of what a request asked for, so the same key presented with
/// a different request can be told apart.
///
/// Built from canonical JSON, so field boundaries are unambiguous (a subject
/// and text of `"ab"` and `"c"` differ from `"a"` and `"bc"`). It covers the
/// route and the message being replied to, so a key reused across `send` and
/// `reply` or across originals is a conflict, and every attachment field,
/// with inline bytes by digest so swapping a file is a conflict too.
pub(super) fn fingerprint(
    inbox: &InboxId,
    route: &str,
    original_message_id: Option<&str>,
    send: &ValidatedSend,
) -> String {
    let attachments: Vec<serde_json::Value> = send
        .attachments
        .iter()
        .map(|attachment| {
            let source = match &attachment.source {
                AttachmentSource::Inline(bytes) => {
                    serde_json::json!({ "sha256": format!("{:x}", Sha256::digest(bytes)) })
                }
                AttachmentSource::Url(url) => serde_json::json!({ "url": url.as_str() }),
            };
            serde_json::json!({
                "source": source,
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "content_disposition": attachment.content_disposition,
                "content_id": attachment.content_id,
            })
        })
        .collect();
    let canonical = serde_json::json!({
        "inbox": inbox.as_str(),
        "route": route,
        "original_message_id": original_message_id,
        "to": send.to,
        "cc": send.cc,
        "bcc": send.bcc,
        "reply_to": send.reply_to,
        "subject": send.subject,
        "text": send.text,
        "html": send.html,
        "labels": send.labels,
        "headers": send.headers,
        "attachments": attachments,
    });
    format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()))
}

/// What a live `Idempotency-Key` recorded: the original ids when `request_hash`
/// matches, a `409` when the key was used for a different request, `None` when
/// there is no live key.
pub(super) async fn replay_key<T: Services>(
    state: &AppState<T>,
    key_hash: &str,
    request_hash: &str,
) -> Result<Option<SendAccepted>, ApiError> {
    let Some(existing) = state
        .services
        .get_send_key(key_hash)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(None);
    };
    if existing.request_hash == request_hash {
        Ok(Some(SendAccepted {
            message_id: existing.message_id,
            thread_id: existing.thread_id,
        }))
    } else {
        Err(ApiError::Conflict(
            "this Idempotency-Key was used for a different request".to_owned(),
        ))
    }
}
