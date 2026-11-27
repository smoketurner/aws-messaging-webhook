//! Event persistence: durable raw record + idempotency + aggregate projection
//! in one atomic write.

use axum::body::Bytes;
use mailbox::time::{self, Timestamp};
use sns_message_verifier::SnsEnvelope;
use std::future::Future;

use crate::config::Config;
use crate::model::{DomainEvent, Source};

/// When one SNS delivery arrived, and when what it stores starts to age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// When the delivery arrived: the Function URL request's time, or for a
    /// direct invoke the envelope's own `Timestamp`.
    pub at: Timestamp,
    /// What retention counts from. The same instant, except for a direct
    /// invoke of a message published long ago (an event redriven from a
    /// failure destination), which is kept for its full retention from the
    /// invocation that stored it.
    pub retain_from: Timestamp,
}

impl Delivery {
    /// A delivery that arrived at `at` and ages from then.
    #[must_use]
    pub fn at(at: Timestamp) -> Self {
        Self {
            at,
            retain_from: at,
        }
    }
}

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
    /// When the delivery reached this service, as the Lambda event states
    /// it.
    pub received_at: Timestamp,
    /// Epoch seconds for the DynamoDB TTL attribute.
    pub expires_at: u64,
    /// Epoch seconds for the aggregate item's TTL — derived from
    /// `aggregate_retention_days`, so the rolled-up state can outlive the raw
    /// event items.
    pub aggregate_expires_at: u64,
}

impl EventRecord {
    /// Assembles a record from a verified envelope and its parsed event.
    pub(crate) fn build(
        source: Option<Source>,
        envelope: &SnsEnvelope,
        raw_body: Bytes,
        event: &DomainEvent,
        config: &Config,
        delivery: Delivery,
    ) -> Self {
        Self {
            aggregate_id: event.aggregate_id(&envelope.message_id),
            event_timestamp: envelope.timestamp.clone(),
            sns_message_id: envelope.message_id.clone(),
            raw_body,
            source,
            detail_type: event.detail_type().to_owned(),
            topic_arn: envelope.topic_arn.clone(),
            received_at: delivery.at,
            expires_at: time::expires_at(delivery.retain_from, config.raw_event_retention_days),
            aggregate_expires_at: time::expires_at(
                delivery.retain_from,
                config.aggregate_retention_days,
            ),
        }
    }

    /// The family label persisted and logged; `"unknown"` when unclassified.
    #[must_use]
    pub(crate) fn source_label(&self) -> &'static str {
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
    /// duplicate rather than re-persisted. A precedence-guarded status
    /// transition that cannot live in that write is applied after it, on
    /// both outcomes, so a redelivery recovers one that failed.
    fn persist_new(
        &self,
        record: &EventRecord,
        event: &DomainEvent,
    ) -> impl Future<Output = Result<PersistOutcome, StoreError>> + Send;
}
