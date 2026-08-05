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
use crate::publish::{OutboundEvent, SCHEMA_VERSION};
use crate::sns::extractor::VerifiedSns;
use crate::state::{AppState, Services};
use crate::store::{EventRecord, PersistOutcome};

/// How a message arrived: a webhook path (which names the event family the
/// operator wired to it) or a direct SNS → Lambda invocation (no path). The
/// family an event actually belongs to comes from [`DomainEvent::classify`];
/// the HTTP expectation is only checked to surface mis-wired topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ingress {
    Http(Source),
    Direct,
}

impl Ingress {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http(source) => source.as_str(),
            Self::Direct => "direct",
        }
    }
}

/// Entry point for both ingress pathways; input is already allowlisted and
/// signature-verified.
///
/// # Errors
///
/// Returns [`AppError`] per the response policy: 4xx for rejected messages,
/// 5xx to recruit SNS redelivery for transient downstream failures.
pub async fn handle_sns<T: Services>(
    state: &AppState<T>,
    ingress: Ingress,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    match verified.envelope.message_type {
        MessageType::SubscriptionConfirmation => {
            confirm_subscription(state, ingress, &verified.envelope).await
        }
        MessageType::UnsubscribeConfirmation => {
            handle_unsubscribe(state, ingress, &verified.envelope).await
        }
        MessageType::Notification => process_notification(state, ingress, &verified).await,
    }
}

async fn confirm_subscription<T: Services>(
    state: &AppState<T>,
    ingress: Ingress,
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
        ingress = ingress.as_str(),
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
    ingress: Ingress,
    envelope: &SnsEnvelope,
) -> Result<Response, AppError> {
    if !state.config.auto_resubscribe {
        tracing::warn!(
            ingress = ingress.as_str(),
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
        ingress = ingress.as_str(),
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
            "schemaVersion": SCHEMA_VERSION,
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
    ingress: Ingress,
    verified: &VerifiedSns,
) -> Result<Response, AppError> {
    let envelope = &verified.envelope;
    let event = DomainEvent::classify(&envelope.message);
    if let Some(family) = event.family() {
        if let Ingress::Http(expected) = ingress
            && family != expected
        {
            tracing::warn!(
                expected = expected.as_str(),
                actual = family.as_str(),
                topic_arn = envelope.topic_arn,
                event = "family_mismatch",
                "topic is wired to a webhook path for a different event family"
            );
        }
    } else {
        tracing::warn!(
            ingress = ingress.as_str(),
            topic_arn = envelope.topic_arn,
            sns_message_id = envelope.message_id,
            event = "unclassified_payload",
            "payload matches no known event family; forwarding as unknown"
        );
    }
    let record = EventRecord::build(
        event.family(),
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

    // Lifecycle actions run after the durable persist. They are idempotent, so
    // a redelivery re-runs them — preserving at-least-once for the action even
    // if a prior attempt persisted but died before acting. Publishing the event
    // to EventBridge is the stream relay's job (see `crate::stream`), not the
    // request path's: the persisted item is the outbox entry.
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
                    "permanent lifecycle action failure; continuing"
                );
                "failed"
            }
        },
    };

    tracing::info!(
        source = record.source_label(),
        sns_message_id = record.sns_message_id,
        topic_arn = record.topic_arn,
        detail_type = record.detail_type,
        outcome = match outcome {
            PersistOutcome::Fresh => "persisted",
            PersistOutcome::Duplicate => "duplicate",
        },
        action,
        "event persisted"
    );
    Ok(StatusCode::OK.into_response())
}
