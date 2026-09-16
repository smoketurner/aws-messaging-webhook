//! The mail table store trait (§5). Only the P1 methods ingest needs are
//! defined here (`get_inbox`, `ensure_inbox`, `message_exists`,
//! `resolve_rfc_ids`, `insert_message`, `get_message`); the P2 read-API and
//! P3 send/outbox methods are layered on by the phase that implements them.
//!
//! `mail/store.rs` is not on the plan's `§12` shared-files list, so unlike
//! `state.rs`/`config.rs` it is not revisited by a later phase's foundation
//! step in the plan as written — but the P2 and P3 method groups documented
//! in plan §5 belong on this same trait, so a later phase will need to
//! extend it regardless. Flagged to the architect as a deviation (see the F1
//! handoff); not resolved here since it is out of phase-1 scope.

use std::future::Future;

use crate::mail::{Inbox, InboxId, InsertOutcome, MailMessage, RfcHit};

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

    /// Resolves an `In-Reply-To`/`References` candidate list (D2) to the
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
