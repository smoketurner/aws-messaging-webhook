//! `POST …/messages/{message_id}/reply`: the same body as a send, with
//! everything the caller leaves out derived from the message being replied
//! to — its recipients, its subject and its `References` chain.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use crate::api::error::ApiError;
use crate::api::json::ApiJson;
use crate::api::send::validate::{Addresses, ReplyRequest};
use crate::api::send::{SendAccepted, enqueue};
use crate::mail::{InboxId, MailMessage, REFERENCES_MAX, content};
use crate::state::{AppState, Services};

/// The `References` chain for a reply: the original's own chain plus the
/// message being replied to, capped so a long thread cannot grow the header
/// without bound.
pub(super) fn threading_references(
    original: &MailMessage,
    original_references: &[String],
) -> Vec<String> {
    let mut references = original_references.to_vec();
    if !references.contains(&original.rfc_message_id) {
        references.push(original.rfc_message_id.clone());
    }
    // Keep the oldest and the most recent, which is what threading actually
    // relies on, and drop the middle when the chain runs long.
    if references.len() > REFERENCES_MAX {
        let keep_from = references.len() - (REFERENCES_MAX - 1);
        let mut trimmed = vec![references[0].clone()];
        trimmed.extend_from_slice(&references[keep_from..]);
        references = trimmed;
    }
    references
}

/// `POST /v0/inboxes/{inbox_id}/messages/{message_id}/reply`
///
/// Derives the threading from the message being replied to, then queues the
/// result exactly as [`send`] does.
///
/// # Errors
///
/// [`ApiError::NotFound`] when the inbox or the original message does not
/// exist; otherwise as [`send`].
pub async fn reply<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path((inbox_id, message_id)): Path<(String, String)>,
    headers: HeaderMap,
    ApiJson(request): ApiJson<ReplyRequest>,
) -> Result<Json<SendAccepted>, ApiError> {
    let inbox = InboxId::from_path(&inbox_id);
    let original = state
        .services
        .get_message(&inbox, &message_id)
        .await
        .map_err(ApiError::from)?
        .ok_or(ApiError::NotFound)?;
    // A missing content document is not an error here: `content::load` logs
    // it and returns empty content, so a reply to a message whose body is
    // gone still goes out, without the original's references or reply-to.
    let original_content = content::load(&state.services, &original)
        .await
        .map_err(ApiError::from)?;

    let mut send = request.send;
    // Recipients default to whoever should receive a reply to the original,
    // unless the caller named its own.
    if send.to.is_none() && send.cc.is_none() && send.bcc.is_none() {
        send.to = Some(Addresses::Many(reply_recipients(
            &inbox,
            &original,
            &original_content.reply_to,
        )));
        if request.reply_all {
            let others = reply_all_recipients(&inbox, &original, &original_content.reply_to);
            if !others.is_empty() {
                send.cc = Some(Addresses::Many(others));
            }
        }
    }
    if send.subject.is_none() {
        send.subject = Some(reply_subject(&original.subject));
    }

    enqueue(
        &state,
        inbox,
        send,
        &headers,
        Some((&original, &original_content)),
        "reply",
    )
    .await
}

/// Who a reply goes to: the original's `Reply-To` if it set one, else its
/// sender.
fn reply_recipients(
    inbox: &InboxId,
    original: &crate::mail::MailMessage,
    original_reply_to: &[String],
) -> Vec<String> {
    if !original_reply_to.is_empty() {
        return original_reply_to.to_vec();
    }
    // A message this inbox sent is replied to by writing to its recipients
    // again, not to itself.
    if original.from.eq_ignore_ascii_case(inbox.as_str()) {
        return original.to.clone();
    }
    vec![original.from.clone()]
}

/// The other participants, for `reply_all`. This inbox is excluded so a reply
/// never arrives back in the inbox that sent it.
fn reply_all_recipients(
    inbox: &InboxId,
    original: &crate::mail::MailMessage,
    original_reply_to: &[String],
) -> Vec<String> {
    let direct = reply_recipients(inbox, original, original_reply_to);
    let mut out = Vec::new();
    for address in original.to.iter().chain(&original.cc) {
        let is_self = address.eq_ignore_ascii_case(inbox.as_str());
        if !is_self && !direct.contains(address) && !out.contains(address) {
            out.push(address.clone());
        }
    }
    out
}

/// Prefixes `Re:` unless the subject already carries one.
fn reply_subject(original: &str) -> String {
    let trimmed = original.trim();
    // `get` rather than indexing: a subject whose third byte falls inside a
    // multi-byte character (an emoji, say) must not panic.
    if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("re:"))
    {
        return trimmed.to_owned();
    }
    format!("Re: {trimmed}")
}
