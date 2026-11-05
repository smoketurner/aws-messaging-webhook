//! The mail table seam: what a store provides so [`Mailbox`] can run every
//! read and write against it. The DynamoDB store and the in-memory double
//! implement only this; everything above it is written once, in
//! [`crate::mailbox`].
//!
//! [`Mailbox`]: crate::mailbox::Mailbox

use std::collections::HashMap;
use std::future::Future;

use serde::de::DeserializeOwned;

use crate::keys::{PageKey, TableKey};
use crate::plan::PlannedOp;
use crate::store::{ListQuery, MailStoreError, Page};
use crate::txn::TxnDecision;
use mailbox::PutOutcome;

/// How fresh a read must be. A strongly consistent read costs twice the
/// capacity, so [`Read::Consistent`] is only for a read where a stale answer
/// would be wrong rather than merely late; each call site says why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    Eventual,
    Consistent,
}

/// One transaction attempt's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOutcome {
    /// Every condition held; the writes were applied.
    Committed,
    /// The transaction was cancelled, decoded to one decision.
    Cancelled(TxnDecision),
}

/// The mail table's secondary indexes, each with the attribute pair it is
/// keyed by.
///
/// The names match the index names in `template.yaml`, which is why they
/// share a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailIndex {
    /// Inboxes, messages and threads, ordered by time.
    ByTime,
    /// One thread's messages.
    ByThread,
    /// Send states, by status. Sparse: only send-state items carry its keys.
    ByStatus,
}

impl MailIndex {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::ByTime => "ByTime",
            Self::ByThread => "ByThread",
            Self::ByStatus => "ByStatus",
        }
    }

    /// The index's partition and sort key attribute names.
    #[must_use]
    pub fn key_attributes(self) -> (&'static str, &'static str) {
        match self {
            Self::ByTime => ("gsi1pk", "gsi1sk"),
            Self::ByThread => ("gsi2pk", "gsi2sk"),
            Self::ByStatus => ("gsi3pk", "gsi3sk"),
        }
    }
}

/// One page of one index partition, independent of which collection is being
/// listed: inboxes are not inbox-scoped, so this carries the partition rather
/// than deriving it from a [`ListQuery`]'s inbox.
#[derive(Debug, Clone, Copy)]
pub struct PageQuery<'a> {
    /// Which index to read. Named rather than inferred from `partition`:
    /// guessing it from a string prefix silently sent status queries to the
    /// wrong index, which returns an empty page rather than an error.
    pub index: MailIndex,
    pub partition: &'a str,
    pub limit: usize,
    /// Exclusive upper bound on the index sort key.
    pub before: Option<&'a str>,
    /// Exclusive lower bound on the index sort key.
    pub after: Option<&'a str>,
    pub ascending: bool,
    /// Resume after this key.
    pub start: Option<&'a PageKey>,
    /// Names the listing in error messages.
    pub context: &'static str,
}

impl<'a> PageQuery<'a> {
    /// A whole partition, oldest first, from `start`.
    #[must_use]
    pub fn unbounded(
        index: MailIndex,
        partition: &'a str,
        limit: usize,
        start: Option<&'a PageKey>,
        context: &'static str,
    ) -> Self {
        Self {
            index,
            partition,
            limit,
            before: None,
            after: None,
            ascending: true,
            start,
            context,
        }
    }

    /// The inbox-scoped case: a [`ListQuery`] aimed at `partition`.
    #[must_use]
    pub fn scoped(
        index: MailIndex,
        partition: &'a str,
        query: &'a ListQuery,
        context: &'static str,
    ) -> Self {
        Self {
            index,
            partition,
            limit: query.limit,
            before: query.before.as_deref(),
            after: query.after.as_deref(),
            ascending: query.ascending,
            start: query.start.as_ref(),
            context,
        }
    }
}

/// The mail table: single-item reads, a batch read, index queries, a
/// conditional create, one transaction attempt, and whatever wait belongs
/// between attempts (a real backoff; nothing, for an in-memory double).
pub trait Table: Send + Sync {
    /// Reads one item by key, deserialized into `T`; `None` when absent.
    /// `context` names the read in error messages.
    ///
    /// # Errors
    ///
    /// The store's own error for a failed read or an item that does not
    /// deserialize.
    fn get<T: DeserializeOwned + Send>(
        &self,
        key: TableKey,
        read: Read,
        context: &'static str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send;

    /// Reads several items by key. An absent item, or one that does not
    /// deserialize into `T`, is left out of the map.
    ///
    /// # Errors
    ///
    /// The store's own error for a failed read.
    fn batch_get<T: DeserializeOwned + Send>(
        &self,
        keys: Vec<TableKey>,
        read: Read,
        context: &'static str,
    ) -> impl Future<Output = Result<HashMap<TableKey, T>, MailStoreError>> + Send;

    /// One page of an index partition: the items whose index partition key is
    /// `query.partition`, ordered by the index sort key, bounded exclusively by
    /// `before`/`after` and resumed after `start`. `next` is the last item's
    /// key whenever the page is full.
    ///
    /// # Errors
    ///
    /// The store's own error for a failed query or an item that does not
    /// deserialize.
    fn query<T: DeserializeOwned + Send>(
        &self,
        query: PageQuery<'_>,
    ) -> impl Future<Output = Result<Page<T>, MailStoreError>> + Send;

    /// Writes `item` (keys included) unless an item with its key exists.
    ///
    /// # Errors
    ///
    /// The store's own error for a failed write.
    fn put_if_absent(
        &self,
        item: serde_dynamo::Item,
        context: &'static str,
    ) -> impl Future<Output = Result<PutOutcome, MailStoreError>> + Send;

    /// Runs one transaction attempt.
    ///
    /// # Errors
    ///
    /// The store's own error for a failure that is not a cancellation (a
    /// cancellation is a [`TxnOutcome::Cancelled`]).
    fn transact(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<TxnOutcome, MailStoreError>> + Send;

    /// Waits before replanning after attempt `attempt`.
    fn pause(&self, attempt: u32) -> impl Future<Output = ()> + Send;
}

/// A shared table is a table, so a [`Mailbox`] can borrow one.
///
/// [`Mailbox`]: crate::mailbox::Mailbox
impl<S: Table> Table for &S {
    fn get<T: DeserializeOwned + Send>(
        &self,
        key: TableKey,
        read: Read,
        context: &'static str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send {
        (**self).get(key, read, context)
    }

    fn batch_get<T: DeserializeOwned + Send>(
        &self,
        keys: Vec<TableKey>,
        read: Read,
        context: &'static str,
    ) -> impl Future<Output = Result<HashMap<TableKey, T>, MailStoreError>> + Send {
        (**self).batch_get(keys, read, context)
    }

    fn query<T: DeserializeOwned + Send>(
        &self,
        query: PageQuery<'_>,
    ) -> impl Future<Output = Result<Page<T>, MailStoreError>> + Send {
        (**self).query(query)
    }

    fn put_if_absent(
        &self,
        item: serde_dynamo::Item,
        context: &'static str,
    ) -> impl Future<Output = Result<PutOutcome, MailStoreError>> + Send {
        (**self).put_if_absent(item, context)
    }

    fn transact(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<TxnOutcome, MailStoreError>> + Send {
        (**self).transact(ops)
    }

    fn pause(&self, attempt: u32) -> impl Future<Output = ()> + Send {
        (**self).pause(attempt)
    }
}
