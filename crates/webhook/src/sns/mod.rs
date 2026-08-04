//! The per-message state machine: confirmations, auto-re-subscribe, and the
//! persist → actions → publish notification pipeline.

pub mod confirm;
pub mod extractor;

use anyhow::{Context as _, anyhow};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use sns_message_verifier::{MessageType, SnsEnvelope};

use crate::actions::{self, ActionErrorKind};
use crate::error::AppError;
use crate::model::{DomainEvent, Source};
use crate::publish::OutboundEvent;
use crate::sns::extractor::VerifiedSns;
use crate::state::{AppState, Services};
use crate::store::{EventRecord, PersistOutcome};

/// Entry point for all webhook routes; input is already allowlisted and
/// signature-verified.
///
/// # Errors
///
/// Returns [`AppError`] per the response policy: 4xx for rejected messages,
/// 5xx to recruit SNS redelivery for transient downstream failures.
pub async fn handle_sns<T: Services>(
    state: &AppState<T>,
    source: Source,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    match verified.envelope.message_type {
        MessageType::SubscriptionConfirmation => {
            confirm_subscription(state, source, &verified.envelope).await
        }
        MessageType::UnsubscribeConfirmation => {
            handle_unsubscribe(state, source, &verified.envelope).await
        }
        MessageType::Notification => process_notification(state, source, &verified).await,
    }
}

async fn confirm_subscription<T: Services>(
    state: &AppState<T>,
    source: Source,
    envelope: &SnsEnvelope,
) -> Result<Response, AppError> {
    let subscribe_url = envelope
        .subscribe_url
        .as_deref()
        .ok_or(AppError::SubscribeUrlRejected)?;
    let url = confirm::validate_subscribe_url(
        subscribe_url,
        state.dangerous_subscribe_url_prefix.as_deref(),
    )?;
    confirm::get_subscribe_url(&state.http, url).await?;
    tracing::info!(
        source = source.as_str(),
        topic_arn = envelope.topic_arn,
        sns_message_id = envelope.message_id,
        outcome = "confirmed",
        "confirmed SNS subscription"
    );
    Ok(StatusCode::OK.into_response())
}

/// The `UnsubscribeURL` in every delivered notification is unauthenticated
/// abuse surface: anyone who obtains one can silently detach the pipeline.
/// SNS's `UnsubscribeConfirmation` carries a `SubscribeURL` that undoes it,
/// so by default we immediately re-subscribe and publish a
/// `subscription.changed` event. Deliberate removal via the authenticated
/// `Unsubscribe` API does not emit this message type, so operator intent is
/// respected.
async fn handle_unsubscribe<T: Services>(
    state: &AppState<T>,
    source: Source,
    envelope: &SnsEnvelope,
) -> Result<Response, AppError> {
    if !state.config.auto_resubscribe {
        tracing::warn!(
            source = source.as_str(),
            topic_arn = envelope.topic_arn,
            sns_message_id = envelope.message_id,
            outcome = "unsubscribed",
            event = "subscription_lost",
            "subscription was cancelled and AUTO_RESUBSCRIBE is disabled"
        );
        return Ok(StatusCode::OK.into_response());
    }

    let subscribe_url = envelope
        .subscribe_url
        .as_deref()
        .ok_or(AppError::SubscribeUrlRejected)?;
    let url = confirm::validate_subscribe_url(
        subscribe_url,
        state.dangerous_subscribe_url_prefix.as_deref(),
    )?;
    confirm::get_subscribe_url(&state.http, url).await?;
    tracing::warn!(
        source = source.as_str(),
        topic_arn = envelope.topic_arn,
        sns_message_id = envelope.message_id,
        outcome = "resubscribed",
        event = "resubscribed",
        "unsubscribe attempt detected; re-subscribed"
    );

    // Best-effort: the security action already happened, so a publish failure
    // must not turn into an SNS retry storm over an observability event.
    let notice = OutboundEvent {
        detail_type: "subscription.changed".to_owned(),
        detail: json!({
            "topicArn": envelope.topic_arn,
            "action": "resubscribed",
            "timestamp": envelope.timestamp,
        }),
    };
    if let Err(error) = state.services.publish(&notice).await {
        tracing::error!(error = ?error, "failed to publish subscription.changed event");
    }
    Ok(StatusCode::OK.into_response())
}

async fn process_notification<T: Services>(
    state: &AppState<T>,
    source: Source,
    verified: &VerifiedSns,
) -> Result<Response, AppError> {
    let envelope = &verified.envelope;
    let event = DomainEvent::parse(source, &envelope.message);
    let record = EventRecord::build(
        source,
        envelope,
        verified.raw_body.clone(),
        &event,
        &state.config,
    )?;

    let outcome = state
        .services
        .persist_new(&record, &event)
        .await
        .context("failed to persist event")?;

    if outcome == PersistOutcome::DuplicatePublished {
        tracing::info!(
            source = source.as_str(),
            sns_message_id = record.sns_message_id,
            detail_type = record.detail_type,
            outcome = "duplicate",
            "duplicate SNS delivery; already published"
        );
        return Ok(StatusCode::OK.into_response());
    }

    // Actions run after persistence (the record is durable) and before
    // publishing (a publish failure re-runs only repeat-safe API calls on
    // redelivery, instead of emitting duplicate bus events).
    let action = match actions::run(state, &event).await {
        Ok(action) => action,
        Err(error) => match error.kind {
            ActionErrorKind::Transient => {
                return Err(AppError::Internal(
                    anyhow!(error).context("transient lifecycle action failure"),
                ));
            }
            ActionErrorKind::Permanent => {
                tracing::error!(
                    error = ?error.source,
                    event = "action_failure",
                    "permanent lifecycle action failure; continuing to publish"
                );
                "failed"
            }
        },
    };

    let outbound = build_outbound(source, &record, &event);
    state
        .services
        .publish(&outbound)
        .await
        .context("failed to publish event to EventBridge")?;
    state
        .services
        .mark_published(&record)
        .await
        .context("failed to mark event published")?;

    tracing::info!(
        source = source.as_str(),
        sns_message_id = record.sns_message_id,
        topic_arn = record.topic_arn,
        detail_type = record.detail_type,
        outcome = match outcome {
            PersistOutcome::Fresh => "published",
            PersistOutcome::DuplicatePersisted => "resumed",
            PersistOutcome::DuplicatePublished => unreachable!("handled above"),
        },
        action,
        "event published"
    );
    Ok(StatusCode::OK.into_response())
}

/// `PutEvents` caps an entry at 256 KB; leave headroom for the envelope.
const MAX_DETAIL_BYTES: usize = 250_000;

fn build_outbound(source: Source, record: &EventRecord, event: &DomainEvent) -> OutboundEvent {
    let mut detail = json!({
        "meta": {
            "snsMessageId": record.sns_message_id,
            "messageId": record.aggregate_id,
            "topicArn": record.topic_arn,
            "receivedAt": record.received_at,
            "webhookPath": source.webhook_path(),
        },
        "event": event.payload(),
    });

    // SES inbound notifications from an SNS receipt action embed the raw MIME
    // in `content` and can exceed the PutEvents entry cap. Strip it from the
    // bus event only — the DynamoDB raw record keeps everything.
    let oversized = serde_json::to_string(&detail).is_ok_and(|s| s.len() > MAX_DETAIL_BYTES);
    if oversized
        && let Some(content) = detail
            .get_mut("event")
            .and_then(|event| event.get_mut("content"))
            .filter(|content| !content.is_null())
    {
        *content = serde_json::Value::Null;
        tracing::warn!(
            sns_message_id = record.sns_message_id,
            event = "content_stripped",
            "stripped oversized inbound content from the EventBridge event"
        );
    }

    OutboundEvent {
        detail_type: record.detail_type.clone(),
        detail,
    }
}
