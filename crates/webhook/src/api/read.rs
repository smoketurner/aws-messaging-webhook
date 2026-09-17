//! The read endpoints: listing and fetching inboxes, messages and threads.
//!
//! Every list reads a time-ordered index and applies the request's filters to
//! the page in memory, because labels are stored on the items themselves
//! rather than in per-label index rows. A filtered page can therefore come
//! back short, so [`fill`] keeps asking for more until the page is full, the
//! partition is exhausted, or it has spent its round-trip budget — whichever
//! comes first. A short page with a continuation token is a valid answer, not
//! an error; the client follows the token.

use std::future::Future;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, RawQuery, State};

use crate::api::error::ApiError;
use crate::api::pagination::ListRequest;
use crate::mail::keys::{self, PageKey, decode_page_token, encode_page_token};
use crate::mail::objects::{self, ObjectError};
use crate::mail::store::{ListQuery, MailStoreError, Page};
use crate::mail::thread::ThreadState;
use crate::mail::wire;
use crate::mail::{InboxId, MailMessage, time};
use crate::state::{AppState, Services};

/// How many store round-trips one list request may spend filling a page.
/// Without a bound, a filter matching nothing could walk an entire partition
/// inside a single request.
const MAX_FILL_PAGES: usize = 5;

/// Accumulates up to `limit` items that pass `keep`.
///
/// Each round asks only for what is still missing, so the running total never
/// exceeds `limit` and the continuation can always be the last fetched page's
/// own key — truncating a page in memory would strand the items after the cut
/// with no token pointing at them.
async fn fill<T, F, Fut>(
    query: &ListQuery,
    keep: impl Fn(&T) -> bool,
    mut fetch: F,
) -> Result<(Vec<T>, Option<PageKey>), MailStoreError>
where
    F: FnMut(ListQuery) -> Fut,
    Fut: Future<Output = Result<Page<T>, MailStoreError>>,
{
    let limit = query.limit;
    let mut collected: Vec<T> = Vec::with_capacity(limit);
    let mut start = query.start.clone();

    for _ in 0..MAX_FILL_PAGES {
        let round = ListQuery {
            limit: limit - collected.len(),
            start: start.clone(),
            ..query.clone()
        };
        let page = fetch(round).await?;
        let next = page.next;
        for item in page.items {
            if keep(&item) {
                collected.push(item);
            }
        }
        start = next;
        // Exhausted the partition, or filled the page.
        if start.is_none() || collected.len() >= limit {
            break;
        }
    }

    Ok((collected, start))
}

/// Turns a request's `page_token` into a store cursor, checking it was issued
/// for the partition this request reads.
fn cursor(request: &ListRequest, partition: &str) -> Result<Option<PageKey>, ApiError> {
    request
        .page_token
        .as_deref()
        .map(|token| {
            decode_page_token(token, partition)
                .map_err(|_| ApiError::field("page_token", "not a valid token for this request"))
        })
        .transpose()
}

/// `GET /v0/inboxes`
///
/// # Errors
///
/// [`ApiError::Validation`] for a bad query parameter or a page token issued
/// for another request; a store failure mapped by [`store_failure`].
pub async fn list_inboxes<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    RawQuery(query): RawQuery,
) -> Result<Json<wire::InboxList>, ApiError> {
    let request = ListRequest::parse(query.as_deref().unwrap_or_default())?;
    let start = cursor(&request, keys::inboxes_partition())?;

    let page = state
        .services
        .list_inboxes(request.limit, start)
        .await
        .map_err(store_failure)?;

    let inboxes: Vec<wire::Inbox> = page.items.iter().map(wire::Inbox::from).collect();
    Ok(Json(wire::InboxList {
        count: inboxes.len(),
        limit: request.limit,
        inboxes,
        next_page_token: page.next.as_ref().map(encode_page_token),
    }))
}

/// `GET /v0/inboxes/{inbox_id}`
///
/// # Errors
///
/// [`ApiError::NotFound`] when no such inbox exists; a store failure mapped
/// by [`store_failure`].
pub async fn get_inbox<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path(inbox_id): Path<String>,
) -> Result<Json<wire::Inbox>, ApiError> {
    let inbox = state
        .services
        .get_inbox(&InboxId(inbox_id))
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(wire::Inbox::from(&inbox)))
}

/// `GET /v0/inboxes/{inbox_id}/messages`
///
/// # Errors
///
/// [`ApiError::Validation`] for a bad query parameter or a page token issued
/// for another inbox; a store failure mapped by [`store_failure`].
pub async fn list_messages<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path(inbox_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<wire::MessageList>, ApiError> {
    let request = ListRequest::parse(query.as_deref().unwrap_or_default())?;
    let inbox = InboxId(inbox_id);
    let partition = format!("INBOX#{}#MSG", inbox.as_str());
    let start = cursor(&request, &partition)?;
    let (before, after) = request.message_bounds();

    let query = ListQuery {
        inbox,
        limit: request.limit,
        before,
        after,
        ascending: request.ascending,
        start,
    };
    let services = &state.services;
    let (messages, next) = fill(
        &query,
        |msg: &MailMessage| request.filters.matches_message(msg),
        |round| async move { services.list_messages(&round).await },
    )
    .await
    .map_err(store_failure)?;

    let messages: Vec<wire::MessageItem> = messages.iter().map(wire::MessageItem::from).collect();
    Ok(Json(wire::MessageList {
        count: messages.len(),
        limit: request.limit,
        messages,
        next_page_token: next.as_ref().map(encode_page_token),
    }))
}

/// `GET /v0/inboxes/{inbox_id}/messages/{message_id}`
///
/// # Errors
///
/// [`ApiError::NotFound`] when the inbox holds no such message; a store
/// failure mapped by [`store_failure`].
pub async fn get_message<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path((inbox_id, message_id)): Path<(String, String)>,
) -> Result<Json<wire::Message>, ApiError> {
    let message = state
        .services
        .get_message(&InboxId(inbox_id), &message_id)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(wire::Message::from(&message)))
}

/// `GET /v0/inboxes/{inbox_id}/messages/{message_id}/raw`
///
/// Hands back a presigned URL for the stored raw MIME rather than streaming
/// it through the function, so a large message costs nothing to serve.
///
/// # Errors
///
/// [`ApiError::NotFound`] when the inbox holds no such message, or the
/// message predates retention and its raw object is gone; a store or object
/// failure otherwise.
pub async fn get_raw<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path((inbox_id, message_id)): Path<(String, String)>,
) -> Result<Json<wire::Download>, ApiError> {
    let message = state
        .services
        .get_message(&InboxId(inbox_id), &message_id)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;
    let key = message.raw_s3_key.as_deref().ok_or(ApiError::NotFound)?;

    let disposition = objects::attachment_disposition(Some(&format!("{message_id}.eml")));
    let url = state
        .services
        .presign_get(key, Some(&disposition), Some("message/rfc822"))
        .await
        .map_err(object_failure)?;

    Ok(Json(wire::Download {
        download_url: url,
        expires_at: expires_at(),
        size: message.size,
        message_id: Some(message_id),
        attachment_id: None,
        filename: None,
        content_type: Some("message/rfc822".to_owned()),
        content_disposition: None,
        content_id: None,
    }))
}

/// `GET /v0/inboxes/{inbox_id}/messages/{message_id}/attachments/{attachment_id}`
///
/// # Errors
///
/// [`ApiError::NotFound`] when the message, the attachment, or the stored
/// object does not exist; a store or object failure otherwise.
pub async fn get_attachment<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path((inbox_id, message_id, attachment_id)): Path<(String, String, String)>,
) -> Result<Json<wire::Download>, ApiError> {
    let message = state
        .services
        .get_message(&InboxId(inbox_id), &message_id)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;

    let attachment = message
        .attachments
        .iter()
        .find(|candidate| candidate.attachment_id == attachment_id)
        .ok_or(ApiError::NotFound)?;
    // An attachment dropped for size is recorded without a key: its metadata
    // is real, but there is nothing stored to hand back.
    let key = attachment.object_key.as_deref().ok_or(ApiError::NotFound)?;

    let disposition = objects::attachment_disposition(attachment.filename.as_deref());
    let url = state
        .services
        .presign_get(key, Some(&disposition), Some(&attachment.content_type))
        .await
        .map_err(object_failure)?;

    Ok(Json(wire::Download {
        download_url: url,
        expires_at: expires_at(),
        size: attachment.size,
        message_id: Some(message_id),
        attachment_id: Some(attachment_id),
        filename: attachment.filename.clone(),
        content_type: Some(attachment.content_type.clone()),
        content_disposition: Some(match attachment.content_disposition.as_str() {
            "inline" => wire::ContentDisposition::Inline,
            _ => wire::ContentDisposition::Attachment,
        }),
        content_id: attachment.content_id.clone(),
    }))
}

/// When the URL just issued stops working.
fn expires_at() -> String {
    let ttl_ms = u64::try_from(objects::DOWNLOAD_URL_TTL.as_millis()).unwrap_or(0);
    time::format(time::now_ms().saturating_add(ttl_ms))
}

/// Maps an object-store failure the same way [`store_failure`] maps a table
/// failure: a missing object is a 404, a transient one is a retryable 502.
fn object_failure(error: ObjectError) -> ApiError {
    match error {
        ObjectError::NotFound => ApiError::NotFound,
        ObjectError::Transient(source) => ApiError::BadGateway(source),
        ObjectError::TooLarge { .. } | ObjectError::Permanent(_) => {
            ApiError::Internal(anyhow::Error::new(error))
        }
    }
}

/// `GET /v0/inboxes/{inbox_id}/threads`
///
/// # Errors
///
/// [`ApiError::Validation`] for a bad query parameter or a page token issued
/// for another inbox; a store failure mapped by [`store_failure`].
pub async fn list_threads<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path(inbox_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Json<wire::ThreadList>, ApiError> {
    let request = ListRequest::parse(query.as_deref().unwrap_or_default())?;
    let inbox = InboxId(inbox_id);
    let partition = format!("INBOX#{}#THR", inbox.as_str());
    let start = cursor(&request, &partition)?;
    let (before, after) = request.thread_bounds();

    let query = ListQuery {
        inbox,
        limit: request.limit,
        before,
        after,
        ascending: request.ascending,
        start,
    };
    let services = &state.services;
    let (threads, next) = fill(
        &query,
        |thread: &ThreadState| request.filters.matches_thread(thread),
        |round| async move { services.list_threads(&round).await },
    )
    .await
    .map_err(store_failure)?;

    let threads: Vec<wire::ThreadItem> = threads.iter().map(wire::ThreadItem::from).collect();
    Ok(Json(wire::ThreadList {
        count: threads.len(),
        limit: request.limit,
        threads,
        next_page_token: next.as_ref().map(encode_page_token),
    }))
}

/// `GET /v0/inboxes/{inbox_id}/threads/{thread_id}`
///
/// The thread's messages are embedded in ascending order and paginated
/// independently of the thread list, so a long thread stays within one
/// response.
///
/// # Errors
///
/// [`ApiError::Validation`] for a bad query parameter or page token;
/// [`ApiError::NotFound`] when the inbox holds no such thread; a store
/// failure mapped by [`store_failure`].
pub async fn get_thread<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path((inbox_id, thread_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Result<Json<wire::Thread>, ApiError> {
    let request = ListRequest::parse(query.as_deref().unwrap_or_default())?;
    let inbox = InboxId(inbox_id);
    let partition = format!("THREAD#{}#{}", inbox.as_str(), thread_id);
    let start = cursor(&request, &partition)?;

    let view = state
        .services
        .get_thread(&inbox, &thread_id, request.limit, start)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;

    let messages = view
        .messages
        .items
        .iter()
        .map(wire::Message::from)
        .collect();
    Ok(Json(wire::Thread::new(
        &view.thread,
        messages,
        request.limit,
        view.messages.next.as_ref().map(encode_page_token),
    )))
}

/// Maps a store failure onto the API's retry contract: a throttled or
/// unreachable table is a 502 the client may retry, and anything else is a
/// 500. Neither leaks the underlying error to the caller.
pub(crate) fn store_failure(error: MailStoreError) -> ApiError {
    match error {
        MailStoreError::NotFound => ApiError::NotFound,
        MailStoreError::InvalidPageToken => {
            ApiError::field("page_token", "not a valid token for this request")
        }
        MailStoreError::Transient(source) => ApiError::BadGateway(source),
        MailStoreError::Conflict | MailStoreError::LabelLimit(_) | MailStoreError::Permanent(_) => {
            ApiError::Internal(anyhow::Error::new(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 500 body hides the cause, so the log is the only place it
    /// survives: the conversion must keep the SDK error as a source.
    #[test]
    fn a_permanent_store_failure_keeps_its_cause() {
        let error = store_failure(MailStoreError::Permanent(anyhow::anyhow!(
            "Query(ByTime): AccessDeniedException"
        )));
        let ApiError::Internal(source) = error else {
            panic!("expected an internal error, got {error:?}");
        };
        assert!(
            format!("{source:?}").contains("AccessDeniedException"),
            "cause missing from {source:?}"
        );
    }

    #[test]
    fn a_permanent_object_failure_keeps_its_cause() {
        let error = object_failure(ObjectError::Permanent(anyhow::anyhow!(
            "GetObject: AccessDenied"
        )));
        let ApiError::Internal(source) = error else {
            panic!("expected an internal error, got {error:?}");
        };
        assert!(
            format!("{source:?}").contains("AccessDenied"),
            "cause missing from {source:?}"
        );
    }
}
