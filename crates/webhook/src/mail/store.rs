//! The mailbox's result and query types.
//!
//! [`crate::mail::mailbox::Mailbox`] serves three groups. Ingest
//! (`get_inbox`, `ensure_inbox`, `message_exists`, `insert_message`,
//! `get_message`), the read API
//! (`list_inboxes`, `list_messages`, `list_threads`, `get_thread`,
//! `update_labels`), and sending (`enqueue_send`, `get_send_key`,
//! `claim_send`, `note_ses_call`, `mark_send`, `get_send_state`,
//! `resolve_ses_message`, and `list_by_status` for the sweep).
//!
//! The list queries read the time-ordered index rather than per-label
//! partitions: one inbox's volume doesn't justify the write amplification of
//! maintaining a pointer row per label, so a `labels` filter is served by
//! filtering the page. See [`ListQuery`] for how the bounds are encoded.

use crate::mail::keys::PageKey;
use crate::mail::send::SendFailure;
use crate::mail::thread::ThreadState;
use crate::mail::{InboxId, MailMessage, MessageId};

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

/// How a send finished, as [`Mailbox::mark_send`] is told it.
///
/// [`Mailbox::mark_send`]: crate::mail::mailbox::Mailbox::mark_send
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

impl MarkOutcome<'_> {
    /// Whether this outcome settles the send: the record takes its message's
    /// TTL and the message will never be assembled from its outbox again.
    ///
    /// Pairs one-to-one with the arms [`crate::mail::send::mark_transition`]
    /// wraps in `settled(..)`, and is what the sender keys outbox cleanup off:
    /// the `outbox/` prefix has no lifecycle rule, so a settled send that does
    /// not clear its own objects leaks them forever.
    #[must_use]
    pub fn settles(&self) -> bool {
        match self {
            Self::Sent(_) | Self::Failed(_) | Self::ClosedSent => true,
            // A send that is handed back, left for an operator, or resumed
            // may yet be assembled again, so it keeps its outbox.
            Self::Released | Self::Unknown | Self::Resumed => false,
        }
    }
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

/// The SES reference row: which message an SES message id was sent as.
/// Both fields are optional so a row missing either resolves to nothing
/// rather than failing the read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct SesRef {
    pub inbox_id: Option<InboxId>,
    pub message_id: Option<MessageId>,
}

impl SesRef {
    /// The inbox and message the row names, when it names both.
    #[must_use]
    pub fn into_parts(self) -> Option<(InboxId, MessageId)> {
        Some((self.inbox_id?, self.message_id?))
    }
}

/// A thread plus one page of its messages, ascending.
#[derive(Debug, Clone)]
pub struct ThreadView {
    pub thread: ThreadState,
    pub messages: Page<MailMessage>,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The pairing this predicate exists for: the outcomes
    /// [`crate::mail::send::mark_transition`] wraps in `settled(..)` are
    /// exactly the ones whose outbox the sender clears. Adding an outcome
    /// without deciding which side it falls on is how the terminal-failure,
    /// sweep-abandon and operator-close paths each came to leak.
    #[test]
    fn only_the_outcomes_that_take_a_ttl_settle() {
        let sent = SesSent {
            message_id: "0100-ses",
            region: "us-east-1",
        };
        assert!(MarkOutcome::Sent(sent).settles());
        assert!(MarkOutcome::Failed(SendFailure::Rejected).settles());
        assert!(MarkOutcome::ClosedSent.settles());

        assert!(!MarkOutcome::Released.settles());
        assert!(!MarkOutcome::Unknown.settles());
        assert!(!MarkOutcome::Resumed.settles());
    }
}
