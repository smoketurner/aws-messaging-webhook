//! Cancellation decoding for `TransactWriteItems`: turns a
//! transaction's per-item cancellation reasons into one decision, so every
//! flow that runs a transaction shares one retry/conflict policy instead of
//! re-deriving it from raw DynamoDB error codes.

use crate::mail::plan::{Cond, OpRole, PlannedOp, TxnKind, WriteOp};

/// The outcome `decode_cancellation` resolves a transaction cancellation to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnDecision {
    /// `TxnKind::Enqueue` only: the idempotency key already exists.
    KeyExists,
    /// A `NotExists`-conditioned `Message`/`SendState` op lost its check —
    /// a redelivery (`Insert`) or this request's own earlier commit
    /// (`Enqueue`); both count as success.
    Duplicate,
    /// Worth retrying with jitter (throttling, a transaction conflict, a
    /// transient service error).
    Retry,
    /// A version- or status-conditioned check lost: the caller re-reads and
    /// decides.
    VersionConflict,
    /// Only `Message-ID` alias writes lost their check: those aliases already
    /// belong to an earlier message. The first writer keeps an alias, so the
    /// caller drops the ones [`taken_aliases`] names and commits the rest.
    AliasTaken,
    /// Anything else: log and surface as a permanent failure.
    Permanent,
}

/// One transaction item's raw cancellation reason, as reported by
/// `TransactWriteItemsError`'s `CancellationReason` (the non-cancellation
/// errors — `TransactionInProgressException` etc. — are handled by the
/// caller before this, since they carry no per-item reasons).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    None,
    ConditionalCheckFailed,
    TransactionConflict,
    ThrottlingError,
    ProvisionedThroughputExceeded,
    RequestLimitExceeded,
    InternalServerError,
    ValidationError,
    ItemCollectionSizeLimitExceeded,
}

/// Whether `reason` is one of the always-retry reasons.
fn is_retryable(reason: CancellationReason) -> bool {
    matches!(
        reason,
        CancellationReason::TransactionConflict
            | CancellationReason::ThrottlingError
            | CancellationReason::ProvisionedThroughputExceeded
            | CancellationReason::RequestLimitExceeded
            | CancellationReason::InternalServerError
    )
}

/// Whether `op` is conditioned `NotExists` (`Message`/`SendState`
/// only).
fn is_not_exists_conditioned(op: &WriteOp) -> bool {
    matches!(
        op,
        WriteOp::Put {
            cond: Cond::NotExists,
            ..
        }
    )
}

/// Whether `op` carries a version- or status-conditioned check (
/// `Message`, `SendState` or `Thread`).
fn is_version_or_status_conditioned(op: &WriteOp) -> bool {
    match op {
        WriteOp::Put { cond, .. } => {
            matches!(
                cond,
                Cond::VersionEquals(_) | Cond::All(_) | Cond::NotExistsOrExpired { .. }
            )
        }
        WriteOp::Update { cond, .. } => {
            matches!(
                cond,
                Cond::VersionEquals(_) | Cond::All(_) | Cond::NotExistsOrExpired { .. }
            )
        }
        WriteOp::Delete { .. } | WriteOp::AliasFirstWriter { .. } => false,
    }
}

/// Decodes a transaction's cancellation into one [`TxnDecision`], per the
/// precedence documented above. `ops` and `reasons` are parallel: one
/// cancellation reason per planned transaction item.
#[must_use]
pub fn decode_cancellation(
    kind: TxnKind,
    ops: &[PlannedOp],
    reasons: &[CancellationReason],
) -> TxnDecision {
    debug_assert_eq!(ops.len(), reasons.len(), "ops and reasons must be parallel");

    // Step 1: Enqueue's idempotency-key conflict.
    if kind == TxnKind::Enqueue {
        for (op, reason) in ops.iter().zip(reasons) {
            if op.role == OpRole::IdempotencyKey
                && *reason == CancellationReason::ConditionalCheckFailed
            {
                return TxnDecision::KeyExists;
            }
        }
    }

    // Step 2: a NotExists-conditioned Message/SendState op lost its check.
    for (op, reason) in ops.iter().zip(reasons) {
        if matches!(op.role, OpRole::Message | OpRole::SendState)
            && *reason == CancellationReason::ConditionalCheckFailed
            && is_not_exists_conditioned(&op.op)
        {
            return TxnDecision::Duplicate;
        }
    }

    // Step 3: anything retryable, at any index, wins over a later definite
    // outcome only in the sense that a Duplicate above already returned; from
    // here any retryable reason takes precedence over VersionConflict/Permanent.
    if reasons.iter().any(|r| is_retryable(*r)) {
        return TxnDecision::Retry;
    }

    // Step 4: a version- or status-conditioned Message/SendState/Thread check
    // lost.
    for (op, reason) in ops.iter().zip(reasons) {
        if matches!(
            op.role,
            OpRole::Message | OpRole::SendState | OpRole::Thread
        ) && *reason == CancellationReason::ConditionalCheckFailed
            && is_version_or_status_conditioned(&op.op)
        {
            return TxnDecision::VersionConflict;
        }
    }

    // Step 5: the only failed checks are alias writes.
    if !taken_aliases(ops, reasons).is_empty()
        && ops.iter().zip(reasons).all(|(op, reason)| {
            *reason != CancellationReason::ConditionalCheckFailed
                || matches!(op.op, WriteOp::AliasFirstWriter { .. })
        })
    {
        return TxnDecision::AliasTaken;
    }

    // Step 6: anything else, including a failed check on an unconditioned op
    // — which DynamoDB never reports, since it only cancels a conditioned one.
    TxnDecision::Permanent
}

/// The alias keys (`pk`) whose first-writer check failed in a cancelled
/// transaction.
#[must_use]
pub fn taken_aliases(ops: &[PlannedOp], reasons: &[CancellationReason]) -> Vec<String> {
    let mut taken = Vec::new();
    for (op, reason) in ops.iter().zip(reasons) {
        if *reason == CancellationReason::ConditionalCheckFailed
            && let WriteOp::AliasFirstWriter { pk, .. } = &op.op
        {
            taken.push(pk.clone());
        }
    }
    taken
}

/// Removes the alias writes for keys an earlier message already holds.
pub fn drop_taken_aliases(ops: &mut Vec<PlannedOp>, taken: &[String]) {
    ops.retain(|op| match &op.op {
        WriteOp::AliasFirstWriter { pk, .. } => !taken.contains(pk),
        WriteOp::Put { .. } | WriteOp::Update { .. } | WriteOp::Delete { .. } => true,
    });
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::mail::plan::Cond;

    fn op(role: OpRole, op: WriteOp) -> PlannedOp {
        PlannedOp { role, op }
    }

    fn not_exists_put(role: OpRole) -> PlannedOp {
        op(
            role,
            WriteOp::Put {
                item: serde_dynamo::Item::default(),
                cond: Cond::NotExists,
            },
        )
    }

    fn version_put(role: OpRole, version: u64) -> PlannedOp {
        op(
            role,
            WriteOp::Put {
                item: serde_dynamo::Item::default(),
                cond: Cond::VersionEquals(version),
            },
        )
    }

    fn none_put(role: OpRole) -> PlannedOp {
        op(
            role,
            WriteOp::Put {
                item: serde_dynamo::Item::default(),
                cond: Cond::None,
            },
        )
    }

    fn alias(pk: &str) -> PlannedOp {
        op(
            OpRole::RfcAlias,
            WriteOp::AliasFirstWriter {
                pk: pk.to_owned(),
                sk: "RFC".to_owned(),
                message_id: "mid-2".to_owned(),
                thread_id: "tid-2".to_owned(),
                expires_at: 0,
            },
        )
    }

    #[test]
    fn a_taken_alias_alone_is_alias_taken_and_names_its_key() {
        let mut ops = vec![
            not_exists_put(OpRole::Message),
            version_put(OpRole::Thread, 0),
            alias("RFC#support#taken@example.com"),
            alias("RFC#support#free@example.com"),
        ];
        let reasons = vec![
            CancellationReason::None,
            CancellationReason::None,
            CancellationReason::ConditionalCheckFailed,
            CancellationReason::None,
        ];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::AliasTaken
        );
        let taken = taken_aliases(&ops, &reasons);
        assert_eq!(taken, vec!["RFC#support#taken@example.com"]);

        drop_taken_aliases(&mut ops, &taken);
        assert_eq!(ops.len(), 3);
        assert!(ops.iter().any(|o| matches!(
            &o.op,
            WriteOp::AliasFirstWriter { pk, .. } if pk == "RFC#support#free@example.com"
        )));
    }

    #[test]
    fn a_taken_alias_with_another_failed_check_is_not_alias_taken() {
        let ops = vec![
            version_put(OpRole::Thread, 0),
            alias("RFC#support#taken@example.com"),
        ];
        let reasons = vec![
            CancellationReason::ConditionalCheckFailed,
            CancellationReason::ConditionalCheckFailed,
        ];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::VersionConflict
        );

        // A redelivered message's own alias is also taken, but the message
        // itself existing is what decides.
        let ops = vec![
            not_exists_put(OpRole::Message),
            alias("RFC#support#taken@example.com"),
        ];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Duplicate
        );
    }

    #[test]
    fn key_exists_only_for_enqueue() {
        let ops = vec![not_exists_put(OpRole::IdempotencyKey)];
        let reasons = vec![CancellationReason::ConditionalCheckFailed];
        assert_eq!(
            decode_cancellation(TxnKind::Enqueue, &ops, &reasons),
            TxnDecision::KeyExists
        );
        assert_ne!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::KeyExists
        );
    }

    #[test]
    fn duplicate_from_not_exists_message_check() {
        let ops = vec![not_exists_put(OpRole::Message)];
        let reasons = vec![CancellationReason::ConditionalCheckFailed];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Duplicate
        );
    }

    #[test]
    fn retry_beats_a_later_duplicate_candidate() {
        let ops = vec![none_put(OpRole::SesRef), not_exists_put(OpRole::Message)];
        let reasons = vec![
            CancellationReason::TransactionConflict,
            CancellationReason::None,
        ];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Retry
        );
    }

    #[test]
    fn duplicate_beats_retry_when_both_present() {
        // Step 2 (Duplicate) is checked before step 3 (Retry): a definite
        // outcome always beats a mere retry signal elsewhere in the batch.
        let ops = vec![not_exists_put(OpRole::Message), none_put(OpRole::SesRef)];
        let reasons = vec![
            CancellationReason::ConditionalCheckFailed,
            CancellationReason::TransactionConflict,
        ];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Duplicate
        );
    }

    #[test]
    fn version_conflict_from_thread_check() {
        let ops = vec![version_put(OpRole::Thread, 3)];
        let reasons = vec![CancellationReason::ConditionalCheckFailed];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::VersionConflict
        );
    }

    #[test]
    fn permanent_for_validation_error() {
        let ops = vec![none_put(OpRole::SesRef)];
        let reasons = vec![CancellationReason::ValidationError];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Permanent
        );
    }

    #[test]
    fn permanent_when_nothing_matches() {
        let ops = vec![none_put(OpRole::SesRef)];
        let reasons = vec![CancellationReason::None];
        assert_eq!(
            decode_cancellation(TxnKind::Insert, &ops, &reasons),
            TxnDecision::Permanent
        );
    }

    proptest! {
        /// Over random role layouts (with and without a SENDKEY op) and
        /// random reason vectors: KeyExists only for Enqueue, Duplicate only
        /// from a NotExists-conditioned op, a definite outcome always beats
        /// Retry, and everything else falls through to Permanent.
        #[test]
        fn decode_cancellation_precedence_holds(
            kind_is_enqueue in any::<bool>(),
            has_send_key in any::<bool>(),
            role_seed in proptest::collection::vec(0u8..5, 1..8),
            reason_seed in proptest::collection::vec(0u8..9, 1..8),
        ) {
            let kind = if kind_is_enqueue { TxnKind::Enqueue } else { TxnKind::Insert };
            let roles = [
                OpRole::Message,
                OpRole::SendState,
                OpRole::Thread,
                OpRole::RfcAlias,
                OpRole::SesRef,
            ];
            let reasons_pool = [
                CancellationReason::None,
                CancellationReason::ConditionalCheckFailed,
                CancellationReason::TransactionConflict,
                CancellationReason::ThrottlingError,
                CancellationReason::ProvisionedThroughputExceeded,
                CancellationReason::RequestLimitExceeded,
                CancellationReason::InternalServerError,
                CancellationReason::ValidationError,
                CancellationReason::ItemCollectionSizeLimitExceeded,
            ];

            let len = role_seed.len().min(reason_seed.len());
            let mut ops: Vec<PlannedOp> = role_seed[..len]
                .iter()
                .map(|&i| not_exists_put(roles[i as usize % roles.len()]))
                .collect();
            let reasons: Vec<CancellationReason> = reason_seed[..len]
                .iter()
                .map(|&i| reasons_pool[i as usize % reasons_pool.len()])
                .collect();

            if has_send_key {
                ops[0] = not_exists_put(OpRole::IdempotencyKey);
            }

            let decision = decode_cancellation(kind, &ops, &reasons);

            if decision == TxnDecision::KeyExists {
                prop_assert_eq!(kind, TxnKind::Enqueue);
            }
            if decision == TxnDecision::Duplicate {
                let has_not_exists_failure = ops.iter().zip(&reasons).any(|(op, reason)| {
                    matches!(op.role, OpRole::Message | OpRole::SendState)
                        && *reason == CancellationReason::ConditionalCheckFailed
                        && is_not_exists_conditioned(&op.op)
                });
                prop_assert!(has_not_exists_failure);
            }
            if decision != TxnDecision::Retry && decision != TxnDecision::Permanent {
                // A definite outcome (KeyExists/Duplicate/VersionConflict) was
                // reached without falling into the catch-all.
                prop_assert!(decision == TxnDecision::KeyExists
                    || decision == TxnDecision::Duplicate
                    || decision == TxnDecision::VersionConflict);
            }
        }
    }
}
