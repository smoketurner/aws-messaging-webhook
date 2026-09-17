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
use crate::mail::store::{ListQuery, MAX_LIMIT, MailStoreError, Page};
use crate::mail::thread::ThreadState;
use crate::mail::wire;
use crate::mail::{InboxId, MailMessage, content, time};
use crate::state::{AppState, Services};

/// How many store round-trips one list request may spend filling a page.
/// Without a bound, a filter matching nothing could walk an entire partition
/// inside a single request.
const MAX_FILL_PAGES: usize = 5;

/// Accumulates up to `limit` items that pass `keep`.
///
/// The first round asks for exactly `limit`, which is enough when nothing is
/// filtered out. A later round means the filter is dropping items, so it asks
/// for [`MAX_LIMIT`] at a time rather than only the few still missing — asking
/// for less each round would spend the round budget on shrinking pages. When
/// a round overshoots, the extra items are dropped and the continuation is
/// built from the last item kept (`key_of`), so nothing after the cut is
/// skipped.
async fn fill<T, F, Fut>(
    query: &ListQuery,
    keep: impl Fn(&T) -> bool,
    key_of: impl Fn(&T) -> PageKey,
    mut fetch: F,
) -> Result<(Vec<T>, Option<PageKey>), MailStoreError>
where
    F: FnMut(ListQuery) -> Fut,
    Fut: Future<Output = Result<Page<T>, MailStoreError>>,
{
    let limit = query.limit;
    let mut collected: Vec<T> = Vec::with_capacity(limit);
    let mut start = query.start.clone();

    for round_number in 0..MAX_FILL_PAGES {
        let round = ListQuery {
            limit: if round_number == 0 { limit } else { MAX_LIMIT },
            start: start.clone(),
            ..query.clone()
        };
        let page = fetch(round).await?;
        start = page.next;
        for item in page.items {
            if !keep(&item) {
                continue;
            }
            if collected.len() == limit {
                // A matching item that does not fit: resume right after the
                // last one returned, so this one opens the next page.
                start = collected.last().map(&key_of);
                break;
            }
            collected.push(item);
        }
        // Exhausted the partition, or filled the page.
        if start.is_none() || collected.len() >= limit {
            break;
        }
    }

    Ok((collected, start))
}

/// The token scope for lists that take no time window or sort order: an inbox
/// list and a thread's embedded messages.
const UNSCOPED: &str = "";

/// Turns a request's `page_token` into a store cursor, checking it was issued
/// for the partition and query shape (`scope`) this request reads.
fn cursor(
    request: &ListRequest,
    partition: &str,
    scope: &str,
) -> Result<Option<PageKey>, ApiError> {
    request
        .page_token
        .as_deref()
        .map(|token| {
            decode_page_token(token, partition, scope)
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
    let start = cursor(&request, keys::inboxes_partition(), UNSCOPED)?;

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
        next_page_token: page
            .next
            .as_ref()
            .map(|key| encode_page_token(key, UNSCOPED)),
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
    let partition = keys::messages_partition(inbox.as_str());
    let scope = request.token_scope();
    let start = cursor(&request, &partition, &scope)?;
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
        |msg: &MailMessage| PageKey {
            partition: keys::messages_partition(msg.inbox_id.as_str()),
            sort: msg.message_id.clone(),
            table_pk: keys::inbox_pk(msg.inbox_id.as_str()),
            table_sk: keys::message_sk(&msg.message_id),
        },
        |round| async move { services.list_messages(&round).await },
    )
    .await
    .map_err(store_failure)?;

    let messages: Vec<wire::MessageItem> = messages.iter().map(wire::MessageItem::from).collect();
    Ok(Json(wire::MessageList {
        count: messages.len(),
        limit: request.limit,
        messages,
        next_page_token: next.as_ref().map(|key| encode_page_token(key, &scope)),
    }))
}

/// `GET /v0/inboxes/{inbox_id}/messages/{message_id}`
///
/// # Errors
///
/// [`ApiError::NotFound`] when the inbox holds no such message; a store
/// failure mapped by [`store_failure`], or a content document read failure
/// mapped by [`object_failure`].
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
    let content = content::load(&state.services, &message)
        .await
        .map_err(object_failure)?;
    Ok(Json(wire::Message::new(&message, &content)))
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
    let partition = keys::threads_partition(inbox.as_str());
    let scope = request.token_scope();
    let start = cursor(&request, &partition, &scope)?;
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
        |thread: &ThreadState| PageKey {
            partition: keys::threads_partition(thread.inbox_id.as_str()),
            sort: keys::thread_time_sort(&thread.timestamp, &thread.thread_id),
            table_pk: keys::inbox_pk(thread.inbox_id.as_str()),
            table_sk: keys::thread_sk(&thread.thread_id),
        },
        |round| async move { services.list_threads(&round).await },
    )
    .await
    .map_err(store_failure)?;

    let threads: Vec<wire::ThreadItem> = threads.iter().map(wire::ThreadItem::from).collect();
    Ok(Json(wire::ThreadList {
        count: threads.len(),
        limit: request.limit,
        threads,
        next_page_token: next.as_ref().map(|key| encode_page_token(key, &scope)),
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
    let start = cursor(&request, &partition, UNSCOPED)?;

    let view = state
        .services
        .get_thread(&inbox, &thread_id, request.limit, start)
        .await
        .map_err(store_failure)?
        .ok_or(ApiError::NotFound)?;

    // One document read per message on the page; a page is bounded by the
    // request limit.
    let mut messages = Vec::with_capacity(view.messages.items.len());
    for message in &view.messages.items {
        let content = content::load(&state.services, message)
            .await
            .map_err(object_failure)?;
        messages.push(wire::Message::new(message, &content));
    }
    Ok(Json(wire::Thread::new(
        &view.thread,
        messages,
        request.limit,
        view.messages
            .next
            .as_ref()
            .map(|key| encode_page_token(key, UNSCOPED)),
    )))
}

/// Maps a store failure onto the API's retry contract: a label cap the
/// request would exceed is a 400 the client must change, a throttled or
/// unreachable table is a 502 the client may retry, and anything else is a
/// 500 that does not leak the underlying error.
pub(crate) fn store_failure(error: MailStoreError) -> ApiError {
    match error {
        MailStoreError::NotFound => ApiError::NotFound,
        MailStoreError::InvalidPageToken => {
            ApiError::field("page_token", "not a valid token for this request")
        }
        MailStoreError::LabelLimit(message) => ApiError::field("labels", message),
        MailStoreError::Transient(source) => ApiError::BadGateway(source),
        MailStoreError::Conflict | MailStoreError::Permanent(_) => {
            ApiError::Internal(anyhow::Error::new(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ITEMS: u32 = 30;

    fn key(item: u32) -> PageKey {
        PageKey {
            partition: "p".to_owned(),
            sort: item.to_string(),
            table_pk: "p".to_owned(),
            table_sk: item.to_string(),
        }
    }

    /// Pages through `0..ITEMS` the way the store does: resume after the
    /// start key, return at most `limit`, and a key only when more remain.
    async fn page(round: ListQuery) -> Result<Page<u32>, MailStoreError> {
        let from = round
            .start
            .as_ref()
            .map_or(0, |start| start.sort.parse::<u32>().unwrap() + 1);
        let items: Vec<u32> = (from..ITEMS).take(round.limit).collect();
        let next = items
            .last()
            .filter(|last| **last + 1 < ITEMS)
            .map(|last| key(*last));
        Ok(Page { items, next })
    }

    fn query(limit: usize, start: Option<PageKey>) -> ListQuery {
        ListQuery {
            inbox: InboxId("support@example.com".to_owned()),
            limit,
            before: None,
            after: None,
            ascending: true,
            start,
        }
    }

    /// A rare match must not come back as a short page just because each
    /// round asked for fewer items.
    #[tokio::test]
    async fn a_selective_filter_still_fills_the_page() {
        let rare = |item: &u32| item % 10 == 9;
        let (items, next) = fill(&query(2, None), rare, |item| key(*item), page)
            .await
            .unwrap();
        assert_eq!(items, vec![9, 19]);

        let (items, next) = fill(&query(2, next), rare, |item| key(*item), page)
            .await
            .unwrap();
        assert_eq!(items, vec![29]);
        assert!(next.is_none());
    }

    /// Items a round fetched past the page are not skipped: the next page
    /// resumes right after the last one returned.
    #[tokio::test]
    async fn an_overfull_round_resumes_after_the_last_item_returned() {
        let even = |item: &u32| item.is_multiple_of(2);
        let mut seen = Vec::new();
        let mut start = None;
        loop {
            let (items, next) = fill(&query(3, start), even, |item| key(*item), page)
                .await
                .unwrap();
            assert!(items.len() <= 3);
            seen.extend(items);
            let Some(next) = next else { break };
            start = Some(next);
        }
        assert_eq!(
            seen,
            (0..ITEMS)
                .filter(|item| item.is_multiple_of(2))
                .collect::<Vec<_>>()
        );
    }

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
