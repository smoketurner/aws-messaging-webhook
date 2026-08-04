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
    pub source: Source,
    pub detail_type: String,
    pub topic_arn: String,
    pub received_at: String,
    /// Epoch seconds for the DynamoDB TTL attribute.
    pub expires_at: u64,
}

impl EventRecord {
    /// Assembles a record from a verified envelope and its parsed event.
    ///
    /// # Errors
    ///
    /// Returns an error if the system clock cannot be represented as a
    /// timestamp (practically unreachable).
    pub fn build(
        source: Source,
        envelope: &SnsEnvelope,
        raw_body: Bytes,
        event: &DomainEvent,
        config: &Config,
    ) -> anyhow::Result<Self> {
        let now = SystemTime::now();
        let received_at = DateTime::from(now)
            .fmt(Format::DateTime)
            .context("failed to format received_at timestamp")?;
        let expires_at = now
            .checked_add(Duration::from_secs(
                config.raw_event_retention_days * 24 * 60 * 60,
            ))
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .context("failed to compute expires_at")?
            .as_secs();

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
        })
    }
}

/// Result of the conditional persist, driving the dedup/resume state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistOutcome {
    /// First time this SNS message was seen; aggregate updated atomically.
    Fresh,
    /// Already fully processed — skip everything and return 200.
    DuplicatePublished,
    /// A prior attempt persisted but died before publishing — resume the
    /// actions + publish, skipping the aggregate update (already applied).
    DuplicatePersisted,
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

    /// Marks the event item as published after a successful EventBridge
    /// `PutEvents`.
    fn mark_published(
        &self,
        record: &EventRecord,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
