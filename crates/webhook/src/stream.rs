//! DynamoDB Streams relay: the sole publisher of event-item details to
//! EventBridge.
//!
//! The request path persists the event item (the outbox entry) and returns.
//! This consumer receives the table's stream and, for each newly-inserted
//! event item, rebuilds the normalized detail from the stored SNS envelope and
//! publishes it. Delivery is guaranteed by the event-source mapping's retries
//! and on-failure destination, not by the request path — so a publish that
//! outlives SNS's redelivery budget can no longer be lost.
//!
//! Failures are reported per-record via `ReportBatchItemFailures`: a transient
//! publish error returns that record's sequence number so only it is retried
//! (and, past the retry limit, lands in the DLQ). A record that cannot be
//! reconstructed at all is a deterministic bug, not a transient fault — it is
//! logged and skipped rather than retried forever; the raw item remains in
//! DynamoDB for manual recovery.
//!
//! The stream event is deserialized with `aws_lambda_events::dynamodb` and its
//! images with `serde_dynamo`, so the typed `AttributeValue`s (including the
//! base64 binary `raw_body`) are decoded by the library rather than by hand.

use aws_lambda_events::dynamodb::Event;
use axum::body::Bytes;
use serde_dynamo::{AttributeValue, Item};
use serde_json::{Value, json};
use sns_message_verifier::SnsEnvelope;

use crate::model::DomainEvent;
use crate::publish::build_outbound;
use crate::state::{AppState, Services};
use crate::store::EventRecord;

/// Reads a DynamoDB `String` attribute from a stream image.
fn image_str<'a>(image: &'a Item, key: &str) -> Option<&'a str> {
    match image.get(key) {
        Some(AttributeValue::S(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Handles one DynamoDB stream invocation, publishing each newly-inserted
/// event item. Returns the `ReportBatchItemFailures` response so the
/// event-source mapping retries only the records whose publish failed.
///
/// # Errors
///
/// Returns an error only if the payload is not a well-formed DynamoDB stream
/// event; per-record faults are handled via the returned failure list or
/// skipped.
pub async fn handle_stream<T: Services>(
    state: &AppState<T>,
    payload: Value,
) -> Result<Value, lambda_http::Error> {
    let event: Event = serde_json::from_value(payload)
        .map_err(|e| format!("payload has Records but is not a DynamoDB stream event: {e}"))?;

    let mut failures: Vec<Value> = Vec::new();
    for record in event.records {
        if record.event_name != "INSERT" {
            continue; // Aggregate MODIFYs and TTL REMOVEs are not published here.
        }
        let image = &record.change.new_image;
        // Only event items are relayed; the aggregate (sk = AGG) is not.
        if !image_str(image, "sk").is_some_and(|sk| sk.starts_with("EVT#")) {
            continue;
        }
        // A transient publish failure returns the sequence number so the ESM
        // retries only that record; reconstruction failures are logged and
        // skipped inside publish_record (Ok).
        if publish_record(state, image).await.is_err()
            && let Some(sequence_number) = record.change.sequence_number
        {
            failures.push(json!({ "itemIdentifier": sequence_number }));
        }
    }

    Ok(json!({ "batchItemFailures": failures }))
}

/// Publishes one event-item image. `Err(())` means a transient publish failure
/// to retry; a deterministic reconstruction failure is logged and returns `Ok`
/// so it is skipped rather than retried forever.
async fn publish_record<T: Services>(state: &AppState<T>, image: &Item) -> Result<(), ()> {
    let Some(AttributeValue::B(raw_body)) = image.get("raw_body") else {
        tracing::error!(
            event = "stream_missing_raw_body",
            "event item has no binary raw_body"
        );
        return Ok(());
    };
    let envelope: SnsEnvelope = match serde_json::from_slice(raw_body) {
        Ok(envelope) => envelope,
        Err(error) => {
            tracing::error!(
                ?error,
                event = "stream_bad_envelope",
                "stored raw_body is not an SNS envelope"
            );
            return Ok(());
        }
    };

    let event = DomainEvent::classify(&envelope.message);
    let record = EventRecord {
        aggregate_id: event.aggregate_id(&envelope.message_id),
        event_timestamp: envelope.timestamp.clone(),
        sns_message_id: envelope.message_id.clone(),
        raw_body: Bytes::from(raw_body.clone()),
        source: event.family(),
        detail_type: event.detail_type().to_owned(),
        topic_arn: envelope.topic_arn.clone(),
        received_at: image_str(image, "received_at")
            .unwrap_or_default()
            .to_owned(),
        // TTLs are irrelevant to publishing; the stored item owns them.
        expires_at: 0,
        aggregate_expires_at: 0,
    };

    let outbound = build_outbound(&record, &event);
    match state.services.publish(&outbound).await {
        Ok(()) => {
            tracing::info!(
                source = record.source_label(),
                sns_message_id = record.sns_message_id,
                detail_type = outbound.detail_type,
                outcome = "published",
                "event published to EventBridge"
            );
            Ok(())
        }
        Err(error) => {
            tracing::error!(
                ?error,
                sns_message_id = record.sns_message_id,
                event = "publish_failure",
                "failed to publish to EventBridge; will retry"
            );
            Err(())
        }
    }
}
