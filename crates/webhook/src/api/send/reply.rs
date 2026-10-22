//! `POST …/messages/{message_id}/reply`: the same body as a send, with
//! everything the caller leaves out derived from the message being replied
//! to — its recipients, its subject and its `References` chain.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use crate::api::error::ApiError;
use crate::api::json::ApiJson;
use crate::api::send::validate::{Addresses, ReplyRequest, SendRequest};
use crate::api::send::{SendAccepted, enqueue};
use crate::mail::address::Mailbox;
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
        address_reply(
            &mut send,
            &inbox,
            &original,
            &original_content.reply_to,
            request.reply_all,
        );
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

/// Addresses a reply, plus the original's other participants in `cc` for
/// `reply_all`.
///
/// A reply to someone else's message goes to its `Reply-To` if it set one,
/// else its sender. A reply to a message this inbox sent goes to that
/// message's recipients again, and its `Reply-To` plays no part: that header
/// names where the author "suggests replies be sent" (RFC 5322 section
/// 3.6.2), which is advice to the recipients, not to the author following
/// up. This inbox is addressed only when no one else is.
fn address_reply(
    send: &mut SendRequest,
    inbox: &InboxId,
    original: &MailMessage,
    original_reply_to: &[String],
    reply_all: bool,
) {
    let direct = if is_inbox(inbox, &original.from) {
        // A send needs a recipient in only one of `to`, `cc` and `bcc`, so
        // the first list that names someone else is used.
        let to = without_inbox(inbox, &original.to);
        let cc = without_inbox(inbox, &original.cc);
        let bcc = without_inbox(inbox, &original.bcc);
        if !to.is_empty() {
            to
        } else if !cc.is_empty() {
            cc
        } else if !bcc.is_empty() {
            // Blind copies stay blind rather than seeing each other in `To`.
            // With no `to` or `cc` there is no one else for `reply_all` to add.
            send.bcc = Some(Addresses::Many(bcc));
            return;
        } else {
            // A note to itself: there is no one else to write to.
            vec![original.from.clone()]
        }
    } else {
        let reply_to = without_inbox(inbox, original_reply_to);
        if reply_to.is_empty() {
            vec![original.from.clone()]
        } else {
            reply_to
        }
    };
    if reply_all {
        let others = reply_all_recipients(inbox, original, &direct);
        if !others.is_empty() {
            send.cc = Some(Addresses::Many(others));
        }
    }
    send.to = Some(Addresses::Many(direct));
}

/// The other participants, for `reply_all`: each mailbox once, never this
/// inbox and never one already addressed directly. Participants are compared
/// by [`Mailbox`], since the same one is often stored in two spellings.
fn reply_all_recipients(inbox: &InboxId, original: &MailMessage, direct: &[String]) -> Vec<String> {
    let mut seen: Vec<Mailbox> = direct
        .iter()
        .map(|participant| Mailbox::of(participant))
        .collect();
    let mut out = Vec::new();
    for participant in original.to.iter().chain(&original.cc) {
        let mailbox = Mailbox::of(participant);
        if !mailbox.is(inbox.as_str()) && !seen.contains(&mailbox) {
            seen.push(mailbox);
            out.push(participant.clone());
        }
    }
    out
}

/// Whether a stored participant names this inbox.
fn is_inbox(inbox: &InboxId, participant: &str) -> bool {
    Mailbox::of(participant).is(inbox.as_str())
}

/// `participants` without this inbox.
fn without_inbox(inbox: &InboxId, participants: &[String]) -> Vec<String> {
    participants
        .iter()
        .filter(|participant| !is_inbox(inbox, participant))
        .cloned()
        .collect()
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
