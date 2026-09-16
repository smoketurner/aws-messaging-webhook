//! The mail table store trait.
//!
//! Holds the methods ingest needs (`get_inbox`, `ensure_inbox`,
//! `message_exists`, `resolve_rfc_ids`, `insert_message`, `get_message`) and
//! the read-API queries (`list_messages`, `list_threads`, `get_thread`). The
//! send/outbox methods arrive with the sending phase.
//!
//! The list queries read the time-ordered index rather than per-label
//! partitions: one inbox's volume doesn't justify the write amplification of
//! maintaining a pointer row per label, so a `labels` filter is served by
//! filtering the page. See [`ListQuery`] for how the bounds are encoded.

use std::future::Future;

use crate::mail::keys::PageKey;
use crate::mail::thread::ThreadState;
use crate::mail::{Inbox, InboxId, InsertOutcome, MailMessage, RfcHit};

/// Default page size when the caller doesn't ask for one.
pub const DEFAULT_LIMIT: usize = 20;

/// Largest page size a caller may ask for.
pub const MAX_LIMIT: usize = 100;

/// One page of results, plus the key that continues it. `next` is `None` on
/// the last page.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<PageKey>,
}

/// A time-ordered list query over one inbox.
///
/// `before`/`after` are exclusive bounds on the sort key, already translated
/// from the request's RFC 3339 values: message lists compare `UUIDv7` ids
/// (which order by time), thread lists compare `<timestamp>#<thread_id>`
/// because threads order by last activity.
#[derive(Debug, Clone)]
pub struct ListQuery {
    pub inbox: InboxId,
    pub limit: usize,
    pub before: Option<String>,
    pub after: Option<String>,
    /// Oldest first when true; newest first (the default) when false.
    pub ascending: bool,
    /// Where to resume, from a page token the caller presented.
    pub start: Option<PageKey>,
}

/// A thread plus one page of its messages, ascending.
#[derive(Debug, Clone)]
pub struct ThreadView {
    pub thread: ThreadState,
    pub messages: Page<MailMessage>,
}

pub trait MailStore: Send + Sync {
    fn get_inbox(
        &self,
        inbox: &InboxId,
    ) -> impl Future<Output = Result<Option<Inbox>, MailStoreError>> + Send;

    fn ensure_inbox(
        &self,
        inbox: &InboxId,
        now: &str,
    ) -> impl Future<Output = Result<Inbox, MailStoreError>> + Send;

    fn message_exists(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> impl Future<Output = Result<bool, MailStoreError>> + Send;

    /// Resolves an `In-Reply-To`/`References` candidate list to the
    /// thread it belongs to, nearest id first.
    fn resolve_rfc_ids(
        &self,
        inbox: &InboxId,
        candidates: &[String],
    ) -> impl Future<Output = Result<Option<RfcHit>, MailStoreError>> + Send;

    fn insert_message(
        &self,
        msg: &MailMessage,
    ) -> impl Future<Output = Result<InsertOutcome, MailStoreError>> + Send;

    fn get_message(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> impl Future<Output = Result<Option<MailMessage>, MailStoreError>> + Send;

    /// Lists an inbox's messages newest-first by default, from the time-ordered
    /// index. Label and substring filters are applied by the caller on the
    /// returned page, so this returns whatever the bounds select.
    fn list_messages(
        &self,
        query: &ListQuery,
    ) -> impl Future<Output = Result<Page<MailMessage>, MailStoreError>> + Send;

    /// Lists an inbox's threads by last activity, newest-first by default.
    fn list_threads(
        &self,
        query: &ListQuery,
    ) -> impl Future<Output = Result<Page<ThreadState>, MailStoreError>> + Send;

    /// One thread with a page of its messages in ascending order. `None` when
    /// the thread doesn't exist in this inbox.
    fn get_thread(
        &self,
        inbox: &InboxId,
        thread_id: &str,
        limit: usize,
        start: Option<PageKey>,
    ) -> impl Future<Output = Result<Option<ThreadView>, MailStoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum MailStoreError {
    #[error("not found")]
    NotFound,
    #[error("conflict")]
    Conflict,
    #[error("invalid page token")]
    InvalidPageToken,
    #[error("label limit exceeded: {0}")]
    LabelLimit(String),
    #[error("transient mail store error")]
    Transient(#[source] anyhow::Error),
    #[error("permanent mail store error")]
    Permanent(#[source] anyhow::Error),
}
