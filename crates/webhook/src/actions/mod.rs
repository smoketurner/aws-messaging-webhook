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

/// Carrier-standard keyword families. Constants, not configuration.
const STOP_KEYWORDS: [&str; 9] = [
    "STOP",
    "UNSUBSCRIBE",
    "CANCEL",
    "END",
    "QUIT",
    "OPTOUT",
    "OPT-OUT",
    "REMOVE",
    "ARRET",
];
const START_KEYWORDS: [&str; 3] = ["START", "UNSTOP", "YES"];

/// What an inbound SMS keyword asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordIntent {
    OptOut,
    OptIn,
    None,
}

/// Classifies the registered keyword (preferred) or, absent one, an
/// exact-match message body. No auto-replies here — responses are downstream
/// business logic.
#[must_use]
pub fn keyword_intent(keyword: Option<&str>, body: Option<&str>) -> KeywordIntent {
    let normalized = keyword
        .or(body)
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(str::to_uppercase);
    let Some(word) = normalized else {
        return KeywordIntent::None;
    };
    if STOP_KEYWORDS.contains(&word.as_str()) {
        KeywordIntent::OptOut
    } else if START_KEYWORDS.contains(&word.as_str()) {
        KeywordIntent::OptIn
    } else {
        KeywordIntent::None
    }
}

/// Dispatches whatever lifecycle action the event calls for. Returns the name
/// of the action taken, for the outcome log line.
///
/// # Errors
///
/// Propagates [`ActionError`] from the underlying API call; the caller maps
/// `Transient` to a 5xx and logs-and-continues on `Permanent`.
pub async fn run<T: Services>(
    state: &AppState<T>,
    event: &DomainEvent,
) -> Result<&'static str, ActionError> {
    match event {
        DomainEvent::SmsInbound { event, .. } => {
            let intent = keyword_intent(
                event.message_keyword.as_deref(),
                event.message_body.as_deref(),
            );
            // Opt-out actions are only meaningful with self-managed opt-outs;
            // without a configured list the event still reaches the bus.
            let list = state.config.opt_out_list_name.as_deref();
            match (intent, list) {
                (KeywordIntent::OptOut, Some(list)) => {
                    state
                        .services
                        .put_opted_out_number(list, &event.origination_number)
                        .await?;
                    Ok("opt_out")
                }
                (KeywordIntent::OptIn, Some(list)) => {
                    state
                        .services
                        .delete_opted_out_number(list, &event.origination_number)
                        .await?;
                    Ok("opt_in")
                }
                (KeywordIntent::OptOut | KeywordIntent::OptIn, None) => {
                    tracing::debug!("keyword received but OPT_OUT_LIST_NAME is not set; skipping");
                    Ok("none")
                }
                (KeywordIntent::None, _) => Ok("none"),
            }
        }
        DomainEvent::SmsDelivery { event, .. } => {
            // Feedback closes the loop on messages sent with feedback
            // enabled. The DLR carries no flag saying whether it was, so the
            // call is made on every terminal event; "no feedback record" is
            // treated as a no-op by the API wrapper.
            if !event.is_final {
                return Ok("none");
            }
            let status = if event.is_successful_delivery() {
                FeedbackStatus::Received
            } else {
                FeedbackStatus::Failed
            };
            state
                .services
                .put_message_feedback(&event.message_id, status)
                .await?;
            Ok("feedback")
        }
        DomainEvent::Ses { event, .. } => {
            if let Some(bounce) = &event.bounce {
                if !bounce.is_permanent() {
                    return Ok("none");
                }
                for recipient in &bounce.bounced_recipients {
                    state
                        .services
                        .put_suppressed_destination(
                            &recipient.email_address,
                            SuppressionReason::Bounce,
                        )
                        .await?;
                }
                return Ok("suppression");
            }
            if let Some(complaint) = &event.complaint {
                for recipient in &complaint.complained_recipients {
                    state
                        .services
                        .put_suppressed_destination(
                            &recipient.email_address,
                            SuppressionReason::Complaint,
                        )
                        .await?;
                }
                return Ok("suppression");
            }
            Ok("none")
        }
        DomainEvent::SesInbound { .. } | DomainEvent::Unknown { .. } => Ok("none"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_family_keywords_opt_out() {
        for word in ["STOP", "stop", " Stop ", "UNSUBSCRIBE", "ARRET", "OPT-OUT"] {
            assert_eq!(
                keyword_intent(Some(word), None),
                KeywordIntent::OptOut,
                "{word}"
            );
        }
    }

    #[test]
    fn start_family_keywords_opt_in() {
        for word in ["START", "unstop", "Yes"] {
            assert_eq!(
                keyword_intent(Some(word), None),
                KeywordIntent::OptIn,
                "{word}"
            );
        }
    }

    #[test]
    fn body_is_only_consulted_without_a_keyword() {
        assert_eq!(keyword_intent(None, Some("stop")), KeywordIntent::OptOut);
        // A registered keyword wins over the body.
        assert_eq!(
            keyword_intent(Some("JOIN"), Some("STOP")),
            KeywordIntent::None
        );
    }

    #[test]
    fn ordinary_messages_have_no_intent() {
        assert_eq!(keyword_intent(Some("JOIN"), None), KeywordIntent::None);
        assert_eq!(
            keyword_intent(None, Some("hello, please stop by later")),
            KeywordIntent::None
        );
        assert_eq!(keyword_intent(None, None), KeywordIntent::None);
        assert_eq!(keyword_intent(Some("  "), None), KeywordIntent::None);
    }
}
