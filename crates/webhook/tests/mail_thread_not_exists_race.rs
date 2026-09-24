//! End-to-end regression for `mail::txn::decode_cancellation`'s `Thread`
//! `NotExists` race: a brand-new thread whose `NotExists` check lost to a
//! concurrent writer must decode to a retryable `VersionConflict`, not a
//! `Permanent` drop. Exercises the real `flows::insert_message` and
//! `flows::enqueue_send` retry loops over the real `MailMemoryStore`
//! plan/condition/`decode_cancellation` path (no hardcoded decision): a
//! `TxnStore` wrapper commits a competing message's full insert plan against
//! the inner store between this request's read and its commit, so only this
//! request's `Thread` `NotExists` check fails — exactly the post-TTL-deletion
//! concurrent-reply race the bug report describes. Before the fix both flows
//! returned `MailStoreError::Permanent` here (a silently dropped mail).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::de::DeserializeOwned;
use webhook_test_support::mail_memory::{MailMemoryStore, sample_message};

use aws_messaging_webhook::mail::InsertOutcome;
use aws_messaging_webhook::mail::flows::{self, TxnOutcome, TxnStore};
use aws_messaging_webhook::mail::plan::{self, PlannedOp};
use aws_messaging_webhook::mail::store::{MailStore, MailStoreError};
use aws_messaging_webhook::mail::thread::new_thread;

/// A `TxnStore` wrapper that, on the first `run_txn`, first commits a
/// competing message's full insert plan against the inner store — simulating a
/// concurrent writer that resurrects the thread between this request's
/// consistent read and its commit — then runs this request's transaction
/// against the now-changed state. Reads delegate untouched, so the wrapped
/// request still reads the thread as absent and plans a `Thread` `NotExists`.
struct ConcurrentThreadWinner<'s> {
    inner: &'s MailMemoryStore,
    competing_plan: Vec<PlannedOp>,
    armed: AtomicBool,
}

impl TxnStore for ConcurrentThreadWinner<'_> {
    fn read_item<T: DeserializeOwned + Send>(
        &self,
        pk: &str,
        sk: &str,
    ) -> impl Future<Output = Result<Option<T>, MailStoreError>> + Send {
        self.inner.read_item::<T>(pk, sk)
    }

    fn run_txn(
        &self,
        ops: &[PlannedOp],
    ) -> impl Future<Output = Result<TxnOutcome, MailStoreError>> + Send {
        let armed = self.armed.swap(false, Ordering::SeqCst);
        let competing = if armed {
            Some(self.competing_plan.clone())
        } else {
            None
        };
        let owned_ops = ops.to_vec();
        let inner = self.inner;
        Box::pin(async move {
            if let Some(plan) = competing {
                // A concurrent writer committed a distinct message in this
                // same thread, resurrecting it, between our read and commit.
                let _ = inner.run_txn(&plan).await?;
            }
            inner.run_txn(&owned_ops).await
        })
    }

    fn pause(&self, attempt: u32) -> impl Future<Output = ()> + Send {
        self.inner.pause(attempt)
    }
}

/// Two distinct inbound replies to the same thread; one resurrects it, the
/// other's `Thread` `NotExists` check loses to that resurrection. The loser
/// must retry (re-read the now-existing thread, `apply_message` to it, and
/// commit) rather than decode to `Permanent` and silently drop the mail.
#[tokio::test]
async fn insert_message_recovers_when_a_thread_not_exists_race_loses_to_a_concurrent_writer() {
    let store_inner = MailMemoryStore::default();
    let r1 = sample_message("support@example.com", "mid-r1", "shared-thread");
    let mut r2 = sample_message("support@example.com", "mid-r2", "shared-thread");
    r2.timestamp = "00000002-0000".to_owned();
    let competing_plan = plan::plan_insert(&r1, None, &new_thread(&r1)).unwrap();

    let store = ConcurrentThreadWinner {
        inner: &store_inner,
        competing_plan,
        armed: AtomicBool::new(true),
    };

    let outcome = flows::insert_message(&store, &r2).await.unwrap();
    assert_eq!(outcome, InsertOutcome::Fresh);

    // The losing reply landed, and the resurrecting reply did too — the race
    // cost neither mail.
    assert!(
        store_inner
            .message_exists(&r2.inbox_id, "mid-r2")
            .await
            .unwrap()
    );
    assert!(
        store_inner
            .message_exists(&r1.inbox_id, "mid-r1")
            .await
            .unwrap()
    );

    // The thread ends up carrying both replies, at version 1 (one
    // `apply_message` past the resurrecting commit's 0), proving the retry
    // re-read and branched on the version rather than dropping the loser.
    let thread = store_inner
        .get_thread(&r2.inbox_id, "shared-thread", 100, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread.messages.items.len(), 2);
    assert_eq!(thread.thread.version, 1);
}

/// Same race against the enqueue flow (`flows::enqueue_send`): the queued
/// send's `Thread` `NotExists` must likewise retry, so a concurrent send that
/// resurrects the thread does not cost the loser its committed message.
#[tokio::test]
async fn enqueue_send_recovers_when_a_thread_not_exists_race_loses_to_a_concurrent_writer() {
    use aws_messaging_webhook::mail::send::{Envelope, SendState};
    use aws_messaging_webhook::mail::store::EnqueueOutcome;

    let store_inner = MailMemoryStore::default();
    let r1 = sample_message("support@example.com", "mid-r1", "shared-thread");
    let mut r2 = sample_message("support@example.com", "mid-r2", "shared-thread");
    r2.direction = aws_messaging_webhook::mail::Direction::Outbound;
    r2.labels = vec!["queued".to_owned()];
    r2.timestamp = "00000002-0000".to_owned();
    let competing_plan = plan::plan_insert(&r1, None, &new_thread(&r1)).unwrap();

    let state = SendState::queued(
        r2.inbox_id.clone(),
        r2.message_id.clone(),
        r2.thread_id.clone(),
        Envelope {
            to: vec!["recipient@example.com".to_owned()],
            ..Envelope::default()
        },
        None,
        "2026-01-01T00:00:00.000Z",
    );

    let store = ConcurrentThreadWinner {
        inner: &store_inner,
        competing_plan,
        armed: AtomicBool::new(true),
    };

    let outcome = flows::enqueue_send(&store, &r2, &state, None, 1_800_000_000)
        .await
        .unwrap();
    assert_eq!(outcome, EnqueueOutcome::Committed);
    assert!(
        store_inner
            .message_exists(&r2.inbox_id, "mid-r2")
            .await
            .unwrap()
    );
    assert!(
        store_inner
            .message_exists(&r1.inbox_id, "mid-r1")
            .await
            .unwrap()
    );
}
