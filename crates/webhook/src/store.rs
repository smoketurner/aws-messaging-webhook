//! Event persistence: durable raw record + idempotency + aggregate projection
//! in one atomic write.

use std::future::Future;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use aws_smithy_types::date_time::{DateTime, Format};
use axum::body::Bytes;
use sns_message_verifier::SnsEnvelope;

use crate::config::Config;
use crate::model::{DomainEvent, Source};

/// One received SNS notification, ready to persist.
#[derive(Debug, Clone)]
pub struct EventRecord {
    /// Groups every event of one message's lifecycle (DynamoDB partition,
    /// without the `MSG#` prefix).
    pub aggregate_id: String,
    /// The SNS envelope timestamp — content-derived, so a redelivery maps to
    /// the same event item key.
    pub event_timestamp: String,
    pub sns_message_id: String,
    /// The exact HTTP body bytes as received (the full signed envelope).
    pub raw_body: Bytes,
    /// The classified event family; `None` for unrecognized payloads.
    pub source: Option<Source>,
    pub detail_type: String,
    pub topic_arn: String,
    pub received_at: String,
    /// Epoch seconds for the DynamoDB TTL attribute.
    pub expires_at: u64,
    /// Epoch seconds for the aggregate item's TTL — derived from
    /// `aggregate_retention_days`, so the rolled-up state can outlive the raw
    /// event items.
    pub aggregate_expires_at: u64,
}

impl EventRecord {
    /// Assembles a record from a verified envelope and its parsed event.
    ///
    /// # Errors
    ///
    /// Returns an error if the system clock cannot be represented as a
    /// timestamp (practically unreachable).
    pub fn build(
        source: Option<Source>,
        envelope: &SnsEnvelope,
        raw_body: Bytes,
        event: &DomainEvent,
        config: &Config,
    ) -> anyhow::Result<Self> {
        let now = SystemTime::now();
        let received_at = DateTime::from(now)
            .fmt(Format::DateTime)
            .context("failed to format received_at timestamp")?;
        let expires_at = expiry_secs(now, config.raw_event_retention_days)?;
        let aggregate_expires_at = expiry_secs(now, config.aggregate_retention_days)?;

        Ok(Self {
            aggregate_id: event.aggregate_id(&envelope.message_id),
            event_timestamp: envelope.timestamp.clone(),
            sns_message_id: envelope.message_id.clone(),
            raw_body,
            source,
            detail_type: event.detail_type().to_owned(),
            topic_arn: envelope.topic_arn.clone(),
            received_at,
            expires_at,
            aggregate_expires_at,
        })
    }

    /// The family label persisted and logged; `"unknown"` when unclassified.
    #[must_use]
    pub fn source_label(&self) -> &'static str {
        self.source.map_or("unknown", Source::as_str)
    }
}

/// The DynamoDB TTL (epoch seconds) `retention_days` after `now`.
///
/// `retention_days` is `u32` (see `Config::raw_event_retention_days`), so
/// `days × 86_400` is bounded by `u32::MAX × 86_400` and cannot overflow
/// `u64`. A `u64` retention value would let very large inputs overflow the
/// multiply and wrap in release builds, producing a near-zero TTL DynamoDB
/// eventually reaps — the same `u32`-then-widen idiom `crate::mail::time::expires_at`
/// uses for mail retention.
///
/// # Errors
///
/// Returns an error only if `now + retention` cannot be represented as
/// epoch seconds (practically unreachable for any realistic clock value).
fn expiry_secs(now: SystemTime, retention_days: u32) -> anyhow::Result<u64> {
    Ok(now
        .checked_add(Duration::from_secs(
            u64::from(retention_days) * 24 * 60 * 60,
        ))
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .context("failed to compute TTL expiry")?
        .as_secs())
}

/// Result of the conditional persist. The DynamoDB item is the outbox entry;
/// the stream relay publishes it. All the request path needs to know is
/// whether this was the first sighting (aggregate applied) or a redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistOutcome {
    /// First time this SNS message was seen; the aggregate projection was
    /// applied atomically with the event put.
    Fresh,
    /// Already persisted — an SNS redelivery. The aggregate was not
    /// re-applied; idempotent lifecycle actions still re-run.
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
#[error("event store operation failed")]
pub struct StoreError(#[from] pub anyhow::Error);

pub trait EventStore: Send + Sync {
    /// Persists the event record and applies the aggregate projection in one
    /// atomic write, keyed so that an SNS redelivery is detected as a
    /// duplicate rather than re-persisted.
    fn persist_new(
        &self,
        record: &EventRecord,
        event: &DomainEvent,
    ) -> impl Future<Output = Result<PersistOutcome, StoreError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_secs_offsets_now_by_retention_days_in_seconds() {
        let now = SystemTime::UNIX_EPOCH;
        assert_eq!(expiry_secs(now, 0).unwrap(), 0);
        assert_eq!(expiry_secs(now, 1).unwrap(), 24 * 60 * 60);
        assert_eq!(expiry_secs(now, 30).unwrap(), 30 * 24 * 60 * 60);
        assert_eq!(expiry_secs(now, 365).unwrap(), 365 * 24 * 60 * 60);
    }

    /// `u32::MAX × 86_400` fits `u64` without wrapping — the upper bound the
    /// `u32` retention type guarantees. With the pre-fix `u64` retention a
    /// value of `213_503_982_334_602` (well above `u32::MAX`) was accepted by
    /// config and wrapped to ~17h in release builds; it now can't be
    /// represented as a `u32` retention, so it never reaches this function.
    #[test]
    fn expiry_secs_at_u32_max_does_not_wrap() {
        let now = SystemTime::UNIX_EPOCH;
        let expected = u64::from(u32::MAX) * 24 * 60 * 60;
        assert_eq!(expiry_secs(now, u32::MAX).unwrap(), expected);
        // 4_294_967_295 × 86_400 = 371_085_174_288_000 — fits `u64` with room
        // to spare, so the multiply is provably non-overflowing at the type
        // level for every `u32` retention value.
        assert!(expected < u64::MAX, "u32::MAX × 86_400 must fit u64");
    }
}
