//! The mail table's write flows, shared by every store that runs them.
//!
//! Each flow is the same shape: read the items the transaction will be
//! conditioned on, compute the new state in Rust, plan the transaction, run
//! it, and turn its cancellation into retry, success or failure. The reads
//! are consistent and repeated on every attempt — a retry means some other
//! writer moved a version, so anything computed from the previous read is
//! stale.
//!
//! Only two things differ between stores: how an item is read and how a
//! transaction is attempted. Those are [`TxnStore`]; everything above them
//! lives here, so the DynamoDB store and the in-memory double cannot drift
//! apart on retry counts, conflict handling or which read conditions which
//! write.

use std::future::Future;

use serde::de::DeserializeOwned;

use crate::mail::plan::{self, PlannedOp};
use crate::mail::send::{SendKey, SendState, SendStatus, label_changes};
use crate::mail::store::{EnqueueOutcome, MailStoreError, MarkOutcome};
use crate::mail::thread::{ThreadState, apply_label_patch, apply_message, new_thread};
use crate::mail::txn::{TxnDecision, drop_taken_aliases};
use crate::mail::{InboxId, InsertOutcome, MailMessage, keys};

/// How many times a cancelled transaction is replanned before the caller is
/// told the write conflicted.
pub const MAX_TXN_RETRIES: u32 = 3;

/// One transaction attempt's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOutcome {
    /// Every condition held; the writes were applied.
    Committed,
    /// The transaction was cancelled, decoded to one decision.
    Cancelled(TxnDecision),
}

/// What a store provides so the flows above can run against it: consistent
/// reads of single items, one transaction attempt, and whatever wait belongs
/// between attempts (a real backoff; nothing, for an in-memory double).
pub trait TxnStore {
    /// Reads one item by key, deserialized into `T`. `None` when absent.
    ///
    /// # Errors
    ///
    /// Returns the store's own error for a failed read or an item that does
    /// not deserialize.
    fn read_item<T: DeserializeOwned + Send>(
        &self,
        pk: &str,
        sk: &str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send;

    /// Runs one transaction attempt.
    ///
    /// # Errors
    ///
    /// Returns the store's own error for a failure that is not a
    /// cancellation (a cancellation is a [`TxnOutcome::Cancelled`]).
    fn run_txn(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<TxnOutcome, MailStoreError>> + Send;

    /// Waits before replanning after attempt `attempt`.
    fn pause(&self, attempt: u32) -> impl Future<Output = ()> + Send;
}

/// Reads a message item.
async fn read_message<S: TxnStore>(
    store: &S,
    inbox: &InboxId,
    message_id: &str,
) -> Result<Option<MailMessage>, MailStoreError> {
    store
        .read_item(
            &keys::inbox_pk(inbox.as_str()),
            &keys::message_sk(message_id),
        )
        .await
}

/// Reads a thread item.
async fn read_thread<S: TxnStore>(
    store: &S,
    inbox: &InboxId,
    thread_id: &str,
) -> Result<Option<ThreadState>, MailStoreError> {
    store
        .read_item(&keys::inbox_pk(inbox.as_str()), &keys::thread_sk(thread_id))
        .await
}

/// Reads a send-state item.
async fn read_send_state<S: TxnStore>(
    store: &S,
    message_id: &str,
) -> Result<Option<SendState>, MailStoreError> {
    store
        .read_item(&keys::outbox_pk(message_id), keys::outbox_sk())
        .await
}

/// The thread this message lands in, before and after: `None` before means a
/// thread this message starts.
fn thread_for(msg: &MailMessage, before: Option<&ThreadState>) -> ThreadState {
    before.map_or_else(|| new_thread(msg), |before| apply_message(before, msg))
}

/// Inserts an inbound message with its thread and `Message-ID` aliases.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted, or the
/// store's own error for a failed read or a cancellation that is neither a
/// duplicate nor retryable.
pub async fn insert_message<S: TxnStore>(
    store: &S,
    msg: &MailMessage,
) -> Result<InsertOutcome, MailStoreError> {
    let mut taken = Vec::new();
    for attempt in 0..=MAX_TXN_RETRIES {
        let thread_before = read_thread(store, &msg.inbox_id, &msg.thread_id).await?;
        let thread_after = thread_for(msg, thread_before.as_ref());

        let mut ops = plan::plan_insert(msg, thread_before.as_ref(), &thread_after)?;
        drop_taken_aliases(&mut ops, &taken);

        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(InsertOutcome::Fresh),
            TxnOutcome::Cancelled(TxnDecision::Duplicate) => return Ok(InsertOutcome::Duplicate),
            TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                store.pause(attempt).await;
            }
            TxnOutcome::Cancelled(TxnDecision::AliasTaken(keys)) => taken.extend(keys),
            TxnOutcome::Cancelled(TxnDecision::Permanent | TxnDecision::KeyExists) => {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "ingest transaction for message {} was cancelled",
                    msg.message_id
                )));
            }
        }
    }
    Err(MailStoreError::Conflict)
}

/// Commits a queued outbound message: the message, its send state, its
/// thread, its aliases and (when the caller gave one) its idempotency key,
/// all in one transaction.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted, or the
/// store's own error for a failed read or a permanent cancellation.
pub async fn enqueue_send<S: TxnStore>(
    store: &S,
    msg: &MailMessage,
    state: &SendState,
    key: Option<&SendKey>,
    now_epoch: u64,
) -> Result<EnqueueOutcome, MailStoreError> {
    let mut taken = Vec::new();
    for attempt in 0..=MAX_TXN_RETRIES {
        let thread_before = read_thread(store, &msg.inbox_id, &msg.thread_id).await?;
        let thread_after = thread_for(msg, thread_before.as_ref());

        let mut ops = plan::plan_enqueue(
            msg,
            state,
            key,
            thread_before.as_ref(),
            &thread_after,
            now_epoch,
        )?;
        drop_taken_aliases(&mut ops, &taken);

        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(EnqueueOutcome::Committed),
            TxnOutcome::Cancelled(TxnDecision::KeyExists) => return Ok(EnqueueOutcome::KeyExists),
            TxnOutcome::Cancelled(TxnDecision::Duplicate) => {
                return Ok(EnqueueOutcome::AlreadyQueued);
            }
            TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                store.pause(attempt).await;
            }
            TxnOutcome::Cancelled(TxnDecision::AliasTaken(keys)) => taken.extend(keys),
            TxnOutcome::Cancelled(TxnDecision::Permanent) => {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "enqueue transaction for message {} was cancelled",
                    msg.message_id
                )));
            }
        }
    }
    Err(MailStoreError::Conflict)
}

/// Hands one queued send to one sender, returning the claimed state.
/// `None` when there is nothing to claim: no send state, or a state another
/// sender holds or has already finished.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted, or the
/// store's own error for a failed read.
pub async fn claim_send<S: TxnStore>(
    store: &S,
    message_id: &str,
    now: &str,
) -> Result<Option<SendState>, MailStoreError> {
    for attempt in 0..=MAX_TXN_RETRIES {
        let Some(before) = read_send_state(store, message_id).await? else {
            return Ok(None);
        };
        // Anything but `queued` means this record is not ours to take:
        // another sender holds it, or it has already finished.
        if before.send_status != SendStatus::Queued {
            return Ok(None);
        }

        let after = before.claimed(now);
        let ops = plan::plan_claim(&before, &after)?;
        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(Some(after)),
            // Lost the race: re-read, and the status check above settles it.
            TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                store.pause(attempt).await;
            }
            TxnOutcome::Cancelled(_) => return Ok(None),
        }
    }
    Err(MailStoreError::Conflict)
}

/// Marks a claim as having called SES, so a sender that dies mid-call leaves
/// a record the sweep can recognize. `None` when the claim is gone.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted, or the
/// store's own error for a failed attempt.
pub async fn note_ses_call<S: TxnStore>(
    store: &S,
    claimed: &SendState,
    now: &str,
) -> Result<Option<SendState>, MailStoreError> {
    let after = claimed.calling_ses(now);
    let ops = plan::plan_ses_call(claimed, &after)?;
    for attempt in 0..=MAX_TXN_RETRIES {
        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(Some(after)),
            // Throttled or conflicting with another transaction: the same
            // write is still the right one.
            TxnOutcome::Cancelled(TxnDecision::Retry) => store.pause(attempt).await,
            // The state moved on under this sender: the claim is gone.
            TxnOutcome::Cancelled(_) => return Ok(None),
        }
    }
    Err(MailStoreError::Conflict)
}

/// Records a send's outcome: the send state, the message's labels, its
/// thread's label counts, and the SES-id alias when there is one.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted or another
/// writer moved this send on, [`MailStoreError::Permanent`] when the send
/// state has no message or the message has no thread.
pub async fn mark_send<S: TxnStore>(
    store: &S,
    state: &SendState,
    outcome: MarkOutcome<'_>,
    now: &str,
) -> Result<(), MailStoreError> {
    let mut taken = Vec::new();
    for attempt in 0..=MAX_TXN_RETRIES {
        let Some(msg) = read_message(store, &state.inbox_id, &state.message_id).await? else {
            return Err(MailStoreError::Permanent(anyhow::anyhow!(
                "send state {} has no message",
                state.message_id
            )));
        };
        let (after, labels, ses) = crate::mail::send::mark_transition(state, &msg, outcome, now);
        let (added, removed) = label_changes(&msg.labels, &labels);
        let thread = if added.is_empty() && removed.is_empty() {
            None
        } else {
            let Some(before) = read_thread(store, &msg.inbox_id, &msg.thread_id).await? else {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "message {} refers to thread {}, which does not exist",
                    msg.message_id,
                    msg.thread_id
                )));
            };
            let after = apply_label_patch(&before, &added, &removed, now);
            Some((before, after))
        };
        let mut ops = plan::plan_mark(
            state,
            &after,
            &msg,
            &labels,
            thread.as_ref().map(|(before, after)| (before, after)),
            ses,
            now,
        )?;
        drop_taken_aliases(&mut ops, &taken);

        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(()),
            TxnOutcome::Cancelled(TxnDecision::Retry) => store.pause(attempt).await,
            TxnOutcome::Cancelled(TxnDecision::VersionConflict) => {
                // A conflict on the send state itself means another writer
                // moved this send on — retrying with the state the caller
                // holds can never succeed, so the claim is reported lost.
                // A conflict on the message or thread is retried.
                let current = read_send_state(store, &state.message_id).await?;
                if current.is_none_or(|current| current.version != state.version) {
                    return Err(MailStoreError::Conflict);
                }
                store.pause(attempt).await;
            }
            TxnOutcome::Cancelled(TxnDecision::AliasTaken(keys)) => taken.extend(keys),
            TxnOutcome::Cancelled(_) => {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "marking send {} was cancelled",
                    state.message_id
                )));
            }
        }
    }
    Err(MailStoreError::Conflict)
}

/// Applies a label patch to one message and its thread's counts, returning
/// the message's labels afterwards. `None` when the message does not exist.
///
/// A patch that changes nothing returns the current labels without spending
/// a transaction and a version bump on a no-op.
///
/// # Errors
///
/// [`MailStoreError::Conflict`] when the retries are exhausted,
/// [`MailStoreError::Permanent`] when the message has no thread.
pub async fn update_labels<S: TxnStore>(
    store: &S,
    inbox: &InboxId,
    message_id: &str,
    add: &[String],
    remove: &[String],
    now: &str,
) -> Result<Option<Vec<String>>, MailStoreError> {
    for attempt in 0..=MAX_TXN_RETRIES {
        let Some(msg) = read_message(store, inbox, message_id).await? else {
            return Ok(None);
        };
        let Some(thread_before) = read_thread(store, inbox, &msg.thread_id).await? else {
            return Err(MailStoreError::Permanent(anyhow::anyhow!(
                "message {message_id} refers to thread {}, which does not exist",
                msg.thread_id
            )));
        };

        let patch = patch_labels(&msg.labels, add, remove);
        if patch.added.is_empty() && patch.removed.is_empty() {
            return Ok(Some(patch.labels));
        }

        let thread_after = apply_label_patch(&thread_before, &patch.added, &patch.removed, now);
        let ops = plan::plan_patch(&msg, &patch.labels, &thread_before, &thread_after, now)?;

        match store.run_txn(&ops).await? {
            TxnOutcome::Committed => return Ok(Some(patch.labels)),
            TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                store.pause(attempt).await;
            }
            TxnOutcome::Cancelled(
                TxnDecision::Permanent
                | TxnDecision::KeyExists
                | TxnDecision::Duplicate
                | TxnDecision::AliasTaken(_),
            ) => {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "label patch for message {message_id} was cancelled"
                )));
            }
        }
    }
    Err(MailStoreError::Conflict)
}

/// A label patch applied to one message's labels: the result, and the
/// changes that actually happened (adding a label twice, or removing one
/// that isn't there, changes nothing).
struct LabelPatch {
    labels: Vec<String>,
    added: Vec<String>,
    removed: Vec<String>,
}

fn patch_labels(current: &[String], add: &[String], remove: &[String]) -> LabelPatch {
    let mut labels = current.to_vec();
    let mut added = Vec::new();
    for label in add {
        if !labels.iter().any(|existing| existing == label) {
            labels.push(label.clone());
            added.push(label.clone());
        }
    }
    let mut removed = Vec::new();
    for label in remove {
        if labels.iter().any(|existing| existing == label) {
            labels.retain(|existing| existing != label);
            removed.push(label.clone());
        }
    }
    labels.sort();
    LabelPatch {
        labels,
        added,
        removed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_patch_reports_only_the_changes_it_made() {
        let current = vec!["received".to_owned(), "unread".to_owned()];
        let patch = patch_labels(
            &current,
            &["unread".to_owned(), "invoices".to_owned()],
            &["spam".to_owned(), "received".to_owned()],
        );
        assert_eq!(patch.labels, vec!["invoices", "unread"]);
        assert_eq!(patch.added, vec!["invoices"]);
        assert_eq!(patch.removed, vec!["received"]);
    }

    #[test]
    fn a_patch_that_changes_nothing_reports_nothing() {
        let current = vec!["received".to_owned()];
        let patch = patch_labels(&current, &["received".to_owned()], &["spam".to_owned()]);
        assert!(patch.added.is_empty());
        assert!(patch.removed.is_empty());
        assert_eq!(patch.labels, current);
    }
}
