//! Inline lifecycle actions: mechanical API calls made in response to events,
//! after persistence and before publishing. AWS-native services (the End User
//! Messaging opt-out list, the SES account suppression list) are the source of
//! truth — the persisted raw event is the audit trail. Actions never suppress
//! the EventBridge event.

use std::future::Future;

use crate::model::DomainEvent;
use crate::state::{AppState, Services};

/// How an action failure affects the request, per the SNS retry contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionErrorKind {
    /// Throttling, 5xx, timeouts: return 5xx so SNS redelivers and the resume
    /// path re-runs the action (all action APIs are repeat-safe).
    Transient,
    /// Validation, access denied, misconfiguration: log + metric, then
    /// continue to publish — a bad opt-out list name must not become a retry
    /// storm that also blocks every event from reaching the bus.
    Permanent,
}

#[derive(Debug, thiserror::Error)]
#[error("lifecycle action failed ({kind:?})")]
pub struct ActionError {
    pub kind: ActionErrorKind,
    #[source]
    pub source: anyhow::Error,
}

impl ActionError {
    #[must_use]
    pub fn transient(source: anyhow::Error) -> Self {
        Self {
            kind: ActionErrorKind::Transient,
            source,
        }
    }

    #[must_use]
    pub fn permanent(source: anyhow::Error) -> Self {
        Self {
            kind: ActionErrorKind::Permanent,
            source,
        }
    }
}

/// Message feedback status for `PutMessageFeedback`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackStatus {
    Received,
    Failed,
}

/// Reason for an SES account-level suppression entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressionReason {
    Bounce,
    Complaint,
}

/// The End User Messaging (sms-voice v2) calls the actions need.
pub trait SmsVoiceApi: Send + Sync {
    fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> impl Future<Output = Result<(), ActionError>> + Send;

    fn put_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send;

    fn delete_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> impl Future<Output = Result<(), ActionError>> + Send;
}

/// The SES v2 calls the actions need.
pub trait SesApi: Send + Sync {
    fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> impl Future<Output = Result<(), ActionError>> + Send;
}

/// Dispatches whatever lifecycle action the event calls for. Returns the name
/// of the action taken, for the outcome log line.
///
/// # Errors
///
/// Propagates [`ActionError`] from the underlying API call; the caller maps
/// `Transient` to a 5xx and logs-and-continues on `Permanent`.
#[expect(
    clippy::unused_async,
    reason = "awaits the action API calls once event parsing lands in Phase 3/4"
)]
pub async fn run<T: Services>(
    state: &AppState<T>,
    event: &DomainEvent,
) -> Result<&'static str, ActionError> {
    let _ = state;
    match event {
        DomainEvent::Unknown { .. } => Ok("none"),
    }
}
