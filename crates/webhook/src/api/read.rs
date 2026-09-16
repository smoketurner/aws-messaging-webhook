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
use crate::mail::store::{ListQuery, MailStoreError, Page};
use crate::mail::thread::ThreadState;
use crate::mail::wire;
use crate::mail::{InboxId, MailMessage};
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
            ApiError::Internal(anyhow::anyhow!("{error}"))
        }
    }
}
