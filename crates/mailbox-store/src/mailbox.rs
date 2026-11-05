//! The mailbox: every read and write the mail table serves, written once on
//! top of [`Table`] so the DynamoDB store and the in-memory double cannot
//! drift apart on read behaviour, retry counts, conflict handling or which
//! read conditions which write.
//!
//! Each write flow is the same shape: read the items the transaction will be
//! conditioned on, compute the new state in Rust, plan the transaction, run
//! it, and turn its cancellation into retry, success or failure. The reads
//! are consistent and repeated on every attempt — a retry means some other
//! writer moved a version, so anything computed from the previous read is
//! stale.

use std::collections::HashMap;

use crate::keys::{self, PageKey};
use crate::plan;
use crate::store::{EnqueueOutcome, ListQuery, MailStoreError, Page, SesRef, ThreadView};
use crate::table::{MailIndex, PageQuery, Read, Table, TxnOutcome};
use crate::txn::{TxnDecision, drop_taken_aliases};
use mailbox::send::{MarkOutcome, SendKey, SendState, SendStatus, label_changes};
use mailbox::thread::{AliasLookup, ThreadState, apply_label_patch, apply_message, new_thread};
use mailbox::{
    Inbox, InboxId, InsertOutcome, MailMessage, MessageId, PutOutcome, RfcHit, SesMessageId,
    ThreadId,
};

/// How many times a cancelled transaction is replanned before the caller is
/// told the write conflicted.
pub const MAX_TXN_RETRIES: u32 = 3;

/// The mailbox over one mail table.
pub struct Mailbox<T> {
    table: T,
}

impl<T: Table> Mailbox<T> {
    pub fn new(table: T) -> Self {
        Self { table }
    }

    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn get_inbox(&self, inbox: &InboxId) -> Result<Option<Inbox>, MailStoreError> {
        // Eventually consistent: a stale miss only sends the caller to
        // `ensure_inbox`, whose conditional put settles it.
        self.table
            .get(keys::inbox(inbox), Read::Eventual, "GetItem(inbox)")
            .await
    }

    /// Creates the inbox if it doesn't exist. Its `email` is its id.
    ///
    /// # Errors
    ///
    /// The store's error for a failed write or read, and
    /// [`MailStoreError::Transient`] when the inbox vanished between the
    /// failed condition and the read.
    pub async fn ensure_inbox(&self, inbox: &InboxId, now: &str) -> Result<Inbox, MailStoreError> {
        let (record, item) = plan::inbox_item(inbox, now)?;
        match self.table.put_if_absent(item, "PutItem(inbox)").await? {
            PutOutcome::Created => Ok(record),
            PutOutcome::AlreadyExists => {
                // The failed condition proves the inbox exists, so this rare
                // path reads consistently: a stale replica would report it
                // missing. Absent even so means it was removed between the two
                // calls; a retry recreates it, where a permanent error would
                // ack the notification and drop the message.
                self.table
                    .get(keys::inbox(inbox), Read::Consistent, "GetItem(inbox)")
                    .await?
                    .ok_or_else(|| {
                        MailStoreError::Transient(anyhow::anyhow!(
                            "inbox disappeared after a conditional check failure"
                        ))
                    })
            }
        }
    }

    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn message_exists(
        &self,
        inbox: &InboxId,
        message_id: &MessageId,
    ) -> Result<bool, MailStoreError> {
        let found: Option<serde::de::IgnoredAny> = self
            .table
            .get(
                keys::message(inbox, message_id),
                Read::Eventual,
                "GetItem(message_exists)",
            )
            .await?;
        Ok(found.is_some())
    }

    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn get_message(
        &self,
        inbox: &InboxId,
        message_id: &MessageId,
    ) -> Result<Option<MailMessage>, MailStoreError> {
        // Consistent: the version read here conditions the next write.
        self.table
            .get(
                keys::message(inbox, message_id),
                Read::Consistent,
                "GetItem(message)",
            )
            .await
    }

    /// Reads the record an `Idempotency-Key` resolves to.
    ///
    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn get_send_key(&self, key_hash: &str) -> Result<Option<SendKey>, MailStoreError> {
        // Consistent: a replay must never be answered from a stale read.
        self.table
            .get(
                keys::send_key(key_hash),
                Read::Consistent,
                "GetItem(send key)",
            )
            .await
    }

    /// Resolves an SES message id back to the mailbox message sent under it.
    ///
    /// `None` when this SES message did not come from the mailbox — most
    /// events on the configuration set will not have.
    ///
    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn resolve_ses_message(
        &self,
        ses_message_id: &SesMessageId,
    ) -> Result<Option<(InboxId, MessageId)>, MailStoreError> {
        let row: Option<SesRef> = self
            .table
            .get(
                keys::ses_ref(ses_message_id),
                Read::Eventual,
                "GetItem(ses ref)",
            )
            .await?;
        Ok(row.and_then(SesRef::into_parts))
    }

    /// Lists sends currently in `status`, from the sparse status index.
    ///
    /// # Errors
    ///
    /// The store's error for a failed query.
    pub async fn list_by_status(
        &self,
        status: SendStatus,
        limit: usize,
    ) -> Result<Vec<SendState>, MailStoreError> {
        let partition = keys::send_status_partition(status);
        let page: Page<SendState> = self
            .table
            .query(PageQuery::unbounded(
                MailIndex::ByStatus,
                &partition,
                limit,
                None,
                "listing sends by status",
            ))
            .await?;
        Ok(page.items)
    }

    /// Reads one send's state.
    ///
    /// # Errors
    ///
    /// The store's error for a failed read.
    pub async fn get_send_state(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<SendState>, MailStoreError> {
        // Consistent: the operator resolution this read feeds marks the send
        // under the version read here.
        self.table
            .get(
                keys::send_state(message_id),
                Read::Consistent,
                "GetItem(send state)",
            )
            .await
    }

    /// Lists every inbox, ordered by id. Inboxes are few and not
    /// inbox-scoped, so this takes only a page size and a continuation.
    ///
    /// # Errors
    ///
    /// The store's error for a failed query.
    pub async fn list_inboxes(
        &self,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Page<Inbox>, MailStoreError> {
        self.table
            .query(PageQuery::unbounded(
                MailIndex::ByTime,
                keys::INBOXES_PARTITION,
                limit,
                start.as_ref(),
                "listing inboxes",
            ))
            .await
    }

    /// Lists an inbox's messages newest-first by default, from the time-ordered
    /// index. Label and substring filters are applied by the caller on the
    /// returned page, so this returns whatever the bounds select.
    ///
    /// # Errors
    ///
    /// The store's error for a failed query.
    pub async fn list_messages(
        &self,
        query: &ListQuery,
    ) -> Result<Page<MailMessage>, MailStoreError> {
        let partition = keys::messages_partition(&query.inbox);
        self.table
            .query(PageQuery::scoped(
                MailIndex::ByTime,
                &partition,
                query,
                "listing messages",
            ))
            .await
    }

    /// Lists an inbox's threads by last activity, newest-first by default.
    ///
    /// # Errors
    ///
    /// The store's error for a failed query.
    pub async fn list_threads(
        &self,
        query: &ListQuery,
    ) -> Result<Page<ThreadState>, MailStoreError> {
        let partition = keys::threads_partition(&query.inbox);
        self.table
            .query(PageQuery::scoped(
                MailIndex::ByTime,
                &partition,
                query,
                "listing threads",
            ))
            .await
    }

    /// One thread with a page of its messages in ascending order. `None` when
    /// the thread doesn't exist in this inbox.
    ///
    /// # Errors
    ///
    /// The store's error for a failed read or query.
    pub async fn get_thread(
        &self,
        inbox: &InboxId,
        thread_id: &ThreadId,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Option<ThreadView>, MailStoreError> {
        // Messages of one thread, oldest first: ByThread is keyed by the
        // message id, which orders by time. The thread item and its messages
        // are independent reads, so they go out together rather than one
        // after the other.
        let partition = keys::thread_messages_partition(inbox, thread_id);
        let read_thread = self.table.get(
            keys::thread(inbox, thread_id),
            Read::Eventual,
            "GetItem(thread)",
        );
        let read_messages = self.table.query(PageQuery::unbounded(
            MailIndex::ByThread,
            &partition,
            limit,
            start.as_ref(),
            "listing thread messages",
        ));
        let (thread, messages) = tokio::join!(read_thread, read_messages);
        assemble_thread_view(thread?, messages)
    }

    /// Reads a message item for a write it conditions.
    async fn read_message(
        &self,
        inbox: &InboxId,
        message_id: &MessageId,
    ) -> Result<Option<MailMessage>, MailStoreError> {
        // Consistent: what is read here conditions the next write.
        self.table
            .get(
                keys::message(inbox, message_id),
                Read::Consistent,
                "GetItem",
            )
            .await
    }

    /// Reads a thread item for a write it conditions.
    async fn read_thread(
        &self,
        inbox: &InboxId,
        thread_id: &ThreadId,
    ) -> Result<Option<ThreadState>, MailStoreError> {
        // Consistent: what is read here conditions the next write.
        self.table
            .get(keys::thread(inbox, thread_id), Read::Consistent, "GetItem")
            .await
    }

    /// Reads a send-state item for a write it conditions.
    async fn read_send_state(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<SendState>, MailStoreError> {
        // Consistent: what is read here conditions the next write.
        self.table
            .get(keys::send_state(message_id), Read::Consistent, "GetItem")
            .await
    }

    /// Reads the RFC alias rows for `ids` (angle brackets already stripped),
    /// keyed by id. An id with no row is absent from the map.
    async fn read_aliases(
        &self,
        inbox: &InboxId,
        ids: &[String],
    ) -> Result<HashMap<String, RfcHit>, MailStoreError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let alias_keys = ids.iter().map(|id| keys::rfc_alias(inbox, id)).collect();
        // Consistent: the transaction planned from this read writes a
        // placeholder for every id it misses, conditioned on the row being
        // absent, so a stale miss is a cancelled attempt.
        let mut found: HashMap<_, RfcHit> = self
            .table
            .batch_get(alias_keys, Read::Consistent, "BatchGetItem(rfc_alias)")
            .await?;
        let mut by_id = HashMap::with_capacity(found.len());
        for id in ids {
            if let Some(hit) = found.remove(&keys::rfc_alias(inbox, id)) {
                by_id.insert(id.clone(), hit);
            }
        }
        Ok(by_id)
    }

    /// Inserts an inbound message with its thread and `Message-ID` aliases.
    ///
    /// Every attempt resolves the thread afresh from the alias rows for
    /// `candidates`: the first candidate with a row names the thread, and with
    /// none the message starts the thread `msg.thread_id` names. Losing an
    /// alias check means another message just wrote that row, possibly a
    /// placeholder pointing into a thread this message belongs in, so the
    /// attempt is replanned from a new read rather than committed without it.
    ///
    /// # Errors
    ///
    /// [`MailStoreError::Conflict`] when the retries are exhausted, or the
    /// store's own error for a failed read or a cancellation that is neither a
    /// duplicate nor retryable.
    pub async fn insert_message(
        &self,
        msg: &MailMessage,
        candidates: &[String],
    ) -> Result<InsertOutcome, MailStoreError> {
        let new_thread_id = msg.thread_id.clone();
        let mut msg = msg.clone();
        for attempt in 0..=MAX_TXN_RETRIES {
            let aliases = AliasLookup {
                candidates: candidates.to_vec(),
                found: self.read_aliases(&msg.inbox_id, candidates).await?,
            };
            msg.thread_id = aliases
                .thread_id()
                .cloned()
                .unwrap_or_else(|| new_thread_id.clone());

            let thread_before = self.read_thread(&msg.inbox_id, &msg.thread_id).await?;
            let thread_after = thread_for(&msg, thread_before.as_ref());

            let ops = plan::plan_insert(&msg, thread_before.as_ref(), &thread_after, &aliases)?;

            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(InsertOutcome::Fresh),
                TxnOutcome::Cancelled(TxnDecision::Duplicate) => {
                    return Ok(InsertOutcome::Duplicate);
                }
                TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                    self.table.pause(attempt).await;
                }
                TxnOutcome::Cancelled(TxnDecision::AliasTaken(_)) => {}
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
    pub async fn enqueue_send(
        &self,
        msg: &MailMessage,
        state: &SendState,
        key: Option<&SendKey>,
        now_epoch: u64,
    ) -> Result<EnqueueOutcome, MailStoreError> {
        let mut taken = Vec::new();
        for attempt in 0..=MAX_TXN_RETRIES {
            let thread_before = self.read_thread(&msg.inbox_id, &msg.thread_id).await?;
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

            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(EnqueueOutcome::Committed),
                TxnOutcome::Cancelled(TxnDecision::KeyExists) => {
                    return Ok(EnqueueOutcome::KeyExists);
                }
                TxnOutcome::Cancelled(TxnDecision::Duplicate) => {
                    return Ok(EnqueueOutcome::AlreadyQueued);
                }
                TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                    self.table.pause(attempt).await;
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
    pub async fn claim_send(
        &self,
        message_id: &MessageId,
        now: &str,
    ) -> Result<Option<SendState>, MailStoreError> {
        for attempt in 0..=MAX_TXN_RETRIES {
            let Some(before) = self.read_send_state(message_id).await? else {
                return Ok(None);
            };
            // Anything but `queued` means this record is not ours to take:
            // another sender holds it, or it has already finished.
            if before.send_status != SendStatus::Queued {
                return Ok(None);
            }

            let after = before.claimed(now);
            let ops = plan::plan_state_cas(&before, &after, SendStatus::Queued)?;
            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(Some(after)),
                // Lost the race: re-read, and the status check above settles it.
                TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                    self.table.pause(attempt).await;
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
    pub async fn note_ses_call(
        &self,
        claimed: &SendState,
        now: &str,
    ) -> Result<Option<SendState>, MailStoreError> {
        let after = claimed.calling_ses(now);
        let ops = plan::plan_state_cas(claimed, &after, SendStatus::Sending)?;
        for attempt in 0..=MAX_TXN_RETRIES {
            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(Some(after)),
                // Throttled or conflicting with another transaction: the same
                // write is still the right one.
                TxnOutcome::Cancelled(TxnDecision::Retry) => self.table.pause(attempt).await,
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
    pub async fn mark_send(
        &self,
        state: &SendState,
        outcome: MarkOutcome<'_>,
        now: &str,
    ) -> Result<(), MailStoreError> {
        let mut taken = Vec::new();
        for attempt in 0..=MAX_TXN_RETRIES {
            let Some(msg) = self
                .read_message(&state.inbox_id, &state.message_id)
                .await?
            else {
                return Err(MailStoreError::Permanent(anyhow::anyhow!(
                    "send state {} has no message",
                    state.message_id
                )));
            };
            let (after, labels, ses) = mailbox::send::mark_transition(state, &msg, outcome, now);
            let (added, removed) = label_changes(&msg.labels, &labels);
            let thread = if added.is_empty() && removed.is_empty() {
                None
            } else {
                let Some(before) = self.read_thread(&msg.inbox_id, &msg.thread_id).await? else {
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

            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(()),
                TxnOutcome::Cancelled(TxnDecision::Retry) => self.table.pause(attempt).await,
                TxnOutcome::Cancelled(TxnDecision::VersionConflict) => {
                    // A conflict on the send state itself means another writer
                    // moved this send on — retrying with the state the caller
                    // holds can never succeed, so the claim is reported lost.
                    // A conflict on the message or thread is retried.
                    let current = self.read_send_state(&state.message_id).await?;
                    if current.is_none_or(|current| current.version != state.version) {
                        return Err(MailStoreError::Conflict);
                    }
                    self.table.pause(attempt).await;
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
    pub async fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &MessageId,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> Result<Option<Vec<String>>, MailStoreError> {
        for attempt in 0..=MAX_TXN_RETRIES {
            let Some(msg) = self.read_message(inbox, message_id).await? else {
                return Ok(None);
            };
            let Some(thread_before) = self.read_thread(inbox, &msg.thread_id).await? else {
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

            match self.table.transact(&ops).await? {
                TxnOutcome::Committed => return Ok(Some(patch.labels)),
                TxnOutcome::Cancelled(TxnDecision::Retry | TxnDecision::VersionConflict) => {
                    self.table.pause(attempt).await;
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
}

/// The thread this message lands in, before and after: `None` before means a
/// thread this message starts.
fn thread_for(msg: &MailMessage, before: Option<&ThreadState>) -> ThreadState {
    before.map_or_else(|| new_thread(msg), |before| apply_message(before, msg))
}

/// Assembles a `ThreadView` from the two reads [`Mailbox::get_thread`] runs
/// concurrently via `tokio::join!`: the base-table thread `GetItem` and the
/// `ByThread` GSI `Query` for the thread's messages.
///
/// Extracted pure so the divergent-result decision — what to do when one read
/// succeeded and the other failed — is unit-testable without an AWS client.
/// The load-bearing rule: a `messages` failure coinciding with an absent thread
/// is surfaced, not dropped. `tokio::join!` drives both reads to completion, so
/// by the time the thread item is known to be absent the `messages` `Result` is
/// already resolved; returning `Ok(None)` without applying `?` would mask a
/// retry-exhausted `ByThread` failure as a definitive `404 NotFound` (the API
/// handler maps `Ok(None)` to `NotFound`) instead of `MailStoreError::Transient`
/// /`Permanent` (`502`/`500`), hiding it from the service's internal-error
/// metrics.
fn assemble_thread_view(
    thread: Option<ThreadState>,
    messages: Result<Page<MailMessage>, MailStoreError>,
) -> Result<Option<ThreadView>, MailStoreError> {
    let Some(thread) = thread else {
        // The messages query may have run for nothing; a thread that doesn't
        // exist is the rarer case than one that does — but its result must be
        // surfaced first so a `ByThread` failure coinciding with an absent
        // thread is not masked as a definitive 404.
        messages?;
        return Ok(None);
    };
    let messages = messages?;
    Ok(Some(ThreadView { thread, messages }))
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

    // --- get_thread: assemble_thread_view ---------------------------------
    //
    // `get_thread` runs the base-table thread `GetItem` and the `ByThread` GSI
    // `Query` concurrently via `tokio::join!`, so by the time the thread item
    // is known to be absent the messages `Result` is already resolved. These
    // tests pin the divergent-result contract `get_thread` hands to
    // `assemble_thread_view`: a dependency failure on the messages read must
    // not be masked as a definitive `Ok(None)` (which the `get_thread` API
    // handler maps to a non-retryable `404 NotFound`) just because the thread
    // happened to be absent.

    /// The regression: a retry-exhausted `ByThread` `Query` (throttle/5xx/transport)
    /// that coincides with an absent thread surfaces as `Transient` (`502`),
    /// not a dropped `Result` reported as `Ok(None)` → `404`.
    #[test]
    fn thread_absent_with_transient_messages_failure_surfaces_the_error() {
        let messages: Result<Page<MailMessage>, MailStoreError> = Err(MailStoreError::Transient(
            anyhow::anyhow!("listing thread messages: throttled"),
        ));
        assert!(matches!(
            assemble_thread_view(None, messages),
            Err(MailStoreError::Transient(_))
        ));
    }

    /// A healthy, empty `Query` alongside a genuinely-absent thread is still a
    /// routine `404`: the fix surfaces errors only, it never invents a thread.
    #[test]
    fn thread_absent_with_healthy_empty_messages_returns_none() {
        let empty: Page<MailMessage> = Page {
            items: vec![],
            next: None,
        };
        assert!(matches!(assemble_thread_view(None, Ok(empty)), Ok(None)));
    }

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
        assert_eq!(patch.added, [] as [String; 0]);
        assert_eq!(patch.removed, [] as [String; 0]);
        assert_eq!(patch.labels, current);
    }
}
