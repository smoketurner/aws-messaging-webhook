//! The mail table store trait: every read and write the mailbox performs.
//!
//! Three groups. Ingest (`get_inbox`, `ensure_inbox`, `message_exists`,
//! `resolve_rfc_ids`, `insert_message`, `get_message`), the read API
//! (`list_inboxes`, `list_messages`, `list_threads`, `get_thread`,
//! `update_labels`), and sending (`enqueue_send`, `claim_send`, `mark_send`
//! and the queries the sweep uses).
//!
//! The list queries read the time-ordered index rather than per-label
//! partitions: one inbox's volume doesn't justify the write amplification of
//! maintaining a pointer row per label, so a `labels` filter is served by
//! filtering the page. See [`ListQuery`] for how the bounds are encoded.

use std::future::Future;

use crate::mail::keys::PageKey;
use crate::mail::send::{SendFailure, SendKey, SendState, SendStatus};
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

/// What an enqueue attempt resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// The send is queued.
    Committed,
    /// This request's own earlier commit: the message already exists, so the
    /// caller answers with the ids it already derived.
    AlreadyQueued,
    /// A live idempotency key claimed this request hash. The caller reads it
    /// back to decide between replaying its answer and refusing a reused key.
    KeyExists,
}

/// How a send finished, as [`MailStore::mark_send`] is told it.
#[derive(Debug, Clone, Copy)]
pub enum MarkOutcome<'a> {
    /// SES accepted it, and gave back its own id.
    Sent(SesSent<'a>),
    /// It will not be sent.
    Failed(SendFailure),
    /// SES may or may not have it. The message keeps its `queued` label,
    /// because it is neither sent nor known to have failed.
    Unknown,
    /// Hand it back for another attempt.
    Released,
    /// An operator decided this send did go out, without SES telling us so.
    /// There is no SES id to record, which is the difference from `Sent`.
    ClosedSent,
    /// An operator asked for an `unknown` send to be attempted again,
    /// accepting the risk that SES already has it.
    Resumed,
}

/// What SES said about a message it accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SesSent<'a> {
    /// The SES message id.
    pub message_id: &'a str,
    /// The region the message was sent through, which SES puts in the
    /// `Message-ID` it writes over ours.
    pub region: &'a str,
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

    /// Creates the inbox if it doesn't exist. Its `email` is its id.
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

    /// Reads the record an `Idempotency-Key` resolves to, consistently, so a
    /// replay is never answered from a stale read.
    fn get_send_key(
        &self,
        key_hash: &str,
    ) -> impl Future<Output = Result<Option<SendKey>, MailStoreError>> + Send;

    /// Commits a queued send: the message, its send state, its thread, the
    /// alias, and the idempotency key when one was given.
    fn enqueue_send(
        &self,
        msg: &MailMessage,
        state: &SendState,
        key: Option<&SendKey>,
        now_epoch: u64,
    ) -> impl Future<Output = Result<EnqueueOutcome, MailStoreError>> + Send;

    /// Resolves an SES message id back to the mailbox message sent under it.
    ///
    /// `None` when this SES message did not come from the mailbox — most
    /// events on the configuration set will not have.
    fn resolve_ses_message(
        &self,
        ses_message_id: &str,
    ) -> impl Future<Output = Result<Option<(InboxId, String)>, MailStoreError>> + Send;

    /// Lists sends currently in `status`, from the sparse status index.
    ///
    /// Only the index's projection is needed here: the sweep decides what to
    /// do from the status and timestamps alone.
    fn list_by_status(
        &self,
        status: SendStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<SendState>, MailStoreError>> + Send;

    /// Reads one send's state, consistently.
    fn get_send_state(
        &self,
        message_id: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send;

    /// Takes a queued send for this sender.
    ///
    /// `None` means someone else already has it, or it is no longer queued.
    /// That is the ordinary outcome of two senders seeing the same stream
    /// record, not an error.
    fn claim_send(
        &self,
        message_id: &str,
        now: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send;

    /// Records that the send `claimed` describes is about to call SES, and
    /// returns the updated state.
    ///
    /// `None` means the claim was lost: the state is no longer `sending` at
    /// that version, so this sender must not call SES.
    fn note_ses_call(
        &self,
        claimed: &SendState,
        now: &str,
    ) -> impl Future<Output = Result<Option<SendState>, MailStoreError>> + Send;

    /// Records how a send ended, moving the state item and the message's
    /// mirrored status and labels together.
    fn mark_send(
        &self,
        state: &SendState,
        outcome: MarkOutcome<'_>,
        now: &str,
    ) -> impl Future<Output = Result<(), MailStoreError>> + Send;

    /// Adds and removes labels on one message, updating its thread's union in
    /// the same transaction. Returns the message's resulting labels, or
    /// `None` when the inbox holds no such message.
    ///
    /// `add` and `remove` are already validated and sorted; a label in both
    /// is the caller's problem to reject, not this method's to arbitrate.
    fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &str,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> impl Future<Output = Result<Option<Vec<String>>, MailStoreError>> + Send;

    /// Lists every inbox, ordered by id. Inboxes are few and not
    /// inbox-scoped, so this takes only a page size and a continuation.
    fn list_inboxes(
        &self,
        limit: usize,
        start: Option<PageKey>,
    ) -> impl Future<Output = Result<Page<Inbox>, MailStoreError>> + Send;

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
