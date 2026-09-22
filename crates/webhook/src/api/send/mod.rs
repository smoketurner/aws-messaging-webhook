//! Queuing outbound mail: `POST …/messages/send` and `…/{id}/reply`.
//!
//! Both validate, upload the message's parts and spec to the outbox, and
//! commit a queued message — they never call SES. The sender does that from
//! the table's stream.
//!
//! The work is split by concern: [`validate`] holds the request types and
//! their rules, [`idempotency`] the `Idempotency-Key` contract, [`spec`] the
//! outbox writes, and [`reply`] the route that derives a send from the
//! message being replied to.

pub mod idempotency;
pub mod reply;
pub mod spec;
pub mod validate;

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::Serialize;

use crate::api::error::ApiError;
use crate::api::json::ApiJson;
use crate::mail::send::{Envelope, SendKey, SendState};
use crate::mail::store::EnqueueOutcome;
use crate::mail::{InboxId, MailMessage, content, ids, keys, time};
use crate::state::{AppState, Services};

pub use reply::reply;
pub use validate::{ReplyRequest, SendRequest};

use idempotency::{KEY_TTL_SECONDS, fingerprint, idempotency_key_hash, replay_key};
use reply::threading_references;
use spec::{build_spec, discard_uploads, queued_message, upload_spec};

/// The response both send routes return.
#[derive(Debug, Serialize)]
pub struct SendAccepted {
    pub message_id: String,
    pub thread_id: String,
}

/// `POST /v0/inboxes/{inbox_id}/messages/send`
///
/// Queues the message and returns as soon as it is durably committed; a
/// separate sender calls SES. The response therefore means "this will be
/// sent", not "this has been sent".
///
/// # Errors
///
/// [`ApiError::Validation`] for a request that breaks any rule,
/// [`ApiError::NotFound`] when the inbox does not exist,
/// [`ApiError::Conflict`] when an `Idempotency-Key` is reused with a
/// different request, and a store or object failure otherwise.
pub async fn send<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path(inbox_id): Path<String>,
    headers: HeaderMap,
    ApiJson(request): ApiJson<SendRequest>,
) -> Result<Json<SendAccepted>, ApiError> {
    enqueue(
        &state,
        InboxId::from_path(&inbox_id),
        request,
        &headers,
        None,
        "send",
    )
    .await
}

/// Validates, uploads and commits one outbound message.
///
/// `original`, when present, is the message being replied to, with its
/// content document; together they give the new message its thread and its
/// threading headers.
async fn enqueue<T: Services>(
    state: &AppState<T>,
    inbox: InboxId,
    request: SendRequest,
    headers: &HeaderMap,
    original: Option<(&MailMessage, &content::MessageContent)>,
    route: &str,
) -> Result<Json<SendAccepted>, ApiError> {
    let key_hash = idempotency_key_hash(headers)?;

    let Some(config) = state.config.mail.as_ref() else {
        return Err(ApiError::NotImplemented);
    };
    // An inbox that does not exist cannot send: the address would not be one
    // this domain owns.
    if state
        .services
        .get_inbox(&inbox)
        .await
        .map_err(ApiError::from)?
        .is_none()
    {
        return Err(ApiError::NotFound);
    }

    let validated = validate::validate(request)?;
    let request_hash = fingerprint(
        &inbox,
        route,
        original.map(|(o, _)| o.message_id.as_str()),
        &validated,
    );

    // A live key for this request is answered from what it recorded, without
    // queuing anything a second time.
    if let Some(hash) = &key_hash
        && let Some(replayed) = replay_key(state, hash, &request_hash).await?
    {
        return Ok(Json(replayed));
    }

    let now_ms = time::now_ms();
    let now = time::format(now_ms);
    let message_id = ids::outbound_message_id().to_string();
    // A reply joins the original's thread; anything else starts its own.
    let thread_id = original.map_or_else(|| message_id.clone(), |(o, _)| o.thread_id.clone());

    let mut spec = build_spec(&inbox, &validated, &message_id, &thread_id, &now);
    if let Some((original, original_content)) = original {
        spec.in_reply_to = Some(original.rfc_message_id.clone());
        spec.references = threading_references(original, &original_content.references);
    }
    upload_spec(&state.services, &spec, &validated).await?;
    let message_content = content::MessageContent {
        text: validated.text.clone(),
        html: validated.html.clone(),
        headers: validated.headers.clone(),
        references: spec.references.clone(),
        reply_to: validated.reply_to.clone(),
        verdicts: None,
    };
    content::store(&state.services, &inbox, &message_id, &message_content)
        .await
        .map_err(ApiError::from)?;

    let state_item = SendState::queued(
        inbox.clone(),
        message_id.clone(),
        thread_id.clone(),
        Envelope {
            to: validated.to.clone(),
            cc: validated.cc.clone(),
            bcc: validated.bcc.clone(),
        },
        key_hash.as_ref().map(|hash| keys::send_key_pk(hash)),
        &now,
    );
    let mut message = queued_message(&inbox, &validated, &spec, &now);
    message.expires_at = time::expires_at(now_ms, config.retention_days);
    let key = key_hash.map(|hash| SendKey {
        key_hash: hash,
        inbox_id: inbox.clone(),
        message_id: message_id.clone(),
        thread_id: thread_id.clone(),
        request_hash: request_hash.clone(),
        route: route.to_owned(),
        created_at: now.clone(),
        expires_at: now_ms / 1_000 + KEY_TTL_SECONDS,
    });

    let committed = state
        .services
        .enqueue_send(&message, &state_item, key.as_ref(), now_ms / 1_000)
        .await;
    match committed {
        Ok(EnqueueOutcome::Committed | EnqueueOutcome::AlreadyQueued) => Ok(Json(SendAccepted {
            message_id,
            thread_id,
        })),
        // Another request committed under the same key between the read above
        // and this commit: answer the way that request would be answered on a
        // replay, and drop what this one uploaded.
        Ok(EnqueueOutcome::KeyExists) => {
            discard_uploads(&state.services, &spec).await;
            let hash = key
                .as_ref()
                .map(|key| key.key_hash.as_str())
                .unwrap_or_default();
            match replay_key(state, hash, &request_hash).await? {
                Some(replayed) => Ok(Json(replayed)),
                // Gone again already (expired): a retry will queue it afresh.
                None => Err(ApiError::Conflict(
                    "this Idempotency-Key is already in use".to_owned(),
                )),
            }
        }
        Err(error) => {
            discard_uploads(&state.services, &spec).await;
            Err(ApiError::from(error))
        }
    }
}
