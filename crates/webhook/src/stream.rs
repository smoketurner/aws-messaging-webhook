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

use crate::model::{DomainEvent, Source};
use crate::publish::{OutboundEvent, SCHEMA_VERSION, build_outbound};
use crate::state::{AppState, Services};
use crate::store::EventRecord;

/// Reads a DynamoDB `String` attribute from a stream image.
fn image_str<'a>(image: &'a Item, key: &str) -> Option<&'a str> {
    match image.get(key) {
        Some(AttributeValue::S(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Reads a DynamoDB `Number` attribute (stored as a string) as an integer.
fn image_num(image: &Item, key: &str) -> Option<i64> {
    match image.get(key) {
        Some(AttributeValue::N(value)) => value.parse().ok(),
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
        let sequence_number = record.change.sequence_number.clone();
        let new_image = &record.change.new_image;
        let Some(sk) = image_str(new_image, "sk") else {
            continue;
        };
        let published = if sk.starts_with("EVT#") {
            // Event items are write-once, so only their INSERT publishes.
            if record.event_name != "INSERT" {
                continue;
            }
            publish_record(state, new_image).await
        } else if sk == "AGG" {
            // The per-message aggregate: publish a status delta when
            // current_status transitions (INSERT of the first status, or a
            // MODIFY that changes it) — not on count-only bumps.
            publish_status_changed(state, new_image, &record.change.old_image).await
        } else {
            continue;
        };
        // Err(()) is a transient publish failure to retry; reconstruction /
        // no-op cases return Ok and are skipped.
        if published.is_err()
            && let Some(sequence_number) = sequence_number
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

/// Publishes a `message.status.changed` event when the per-message aggregate's
/// `current_status` transitions — the first status (INSERT: no old value) or a
/// MODIFY that changes it. Count-only bumps (opens/clicks leave `current_status`
/// untouched) produce no event. Carries the rolled-up snapshot so consumers get
/// the authoritative status without re-deriving precedence. Same retry contract
/// as `publish_record`: `Err(())` = transient publish failure to retry.
async fn publish_status_changed<T: Services>(
    state: &AppState<T>,
    new_image: &Item,
    old_image: &Item,
) -> Result<(), ()> {
    let Some(current) = image_str(new_image, "current_status") else {
        return Ok(()); // aggregate carries no status yet (e.g. only open/click counts)
    };
    if image_str(old_image, "current_status") == Some(current) {
        return Ok(()); // not a status transition — skip the count-only change
    }
    let Some(message_id) = image_str(new_image, "pk").and_then(|pk| pk.strip_prefix("MSG#")) else {
        tracing::error!(
            event = "stream_agg_missing_pk",
            "aggregate item has no MSG# pk"
        );
        return Ok(());
    };

    let mut status = json!({ "current": current });
    if let Some(bounce_type) = image_str(new_image, "bounce_type") {
        status["bounceType"] = json!(bounce_type);
    }
    if let Some(first_event_at) = image_str(new_image, "first_event_at") {
        status["firstEventAt"] = json!(first_event_at);
    }
    if let Some(last_event_at) = image_str(new_image, "last_event_at") {
        status["lastEventAt"] = json!(last_event_at);
    }
    if let Some(open_count) = image_num(new_image, "open_count") {
        status["openCount"] = json!(open_count);
    }
    if let Some(click_count) = image_num(new_image, "click_count") {
        status["clickCount"] = json!(click_count);
    }

    let webhook_path = image_str(new_image, "source")
        .and_then(Source::from_label)
        .map(Source::webhook_path);
    let detail = json!({
        "schemaVersion": SCHEMA_VERSION,
        "meta": { "messageId": message_id, "webhookPath": webhook_path },
        "status": status,
    });
    let outbound = OutboundEvent {
        detail_type: "message.status.changed".to_owned(),
        detail,
    };
    match state.services.publish(&outbound).await {
        Ok(()) => {
            tracing::info!(
                message_id,
                current_status = current,
                outcome = "status_published",
                "published message.status.changed"
            );
            Ok(())
        }
        Err(error) => {
            tracing::error!(
                ?error,
                message_id,
                event = "publish_failure",
                "failed to publish status change; will retry"
            );
            Err(())
        }
    }
}
