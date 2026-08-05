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
        let expiry_secs = |retention_days: u64| -> anyhow::Result<u64> {
            Ok(now
                .checked_add(Duration::from_secs(retention_days * 24 * 60 * 60))
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .context("failed to compute TTL expiry")?
                .as_secs())
        };
        let expires_at = expiry_secs(config.raw_event_retention_days)?;
        let aggregate_expires_at = expiry_secs(config.aggregate_retention_days)?;

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
