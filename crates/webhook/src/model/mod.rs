//! Domain events parsed from the inner SNS `Message` JSON.

use serde_json::Value;

/// Which webhook path (and therefore which SNS wiring) a message arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    SmsInbound,
    SmsEvents,
    SesEvents,
    SesInbound,
}

impl Source {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SmsInbound => "sms-inbound",
            Self::SmsEvents => "sms-events",
            Self::SesEvents => "ses-events",
            Self::SesInbound => "ses-inbound",
        }
    }

    #[must_use]
    pub fn webhook_path(self) -> &'static str {
        match self {
            Self::SmsInbound => "/webhooks/sms/inbound",
            Self::SmsEvents => "/webhooks/sms/events",
            Self::SesEvents => "/webhooks/ses/events",
            Self::SesInbound => "/webhooks/ses/inbound",
        }
    }
}

/// A normalized messaging event. Unrecognized payloads are forwarded as
/// [`DomainEvent::Unknown`] rather than rejected: an at-least-once pipeline
/// degrades to pass-through, and new AWS event shapes appear here first.
#[derive(Debug, Clone)]
pub enum DomainEvent {
    Unknown { raw: Value },
}

impl DomainEvent {
    /// Parses the inner SNS `Message` string for the given source.
    #[must_use]
    pub fn parse(source: Source, message: &str) -> Self {
        let _ = source;
        let raw = serde_json::from_str::<Value>(message)
            .unwrap_or_else(|_| Value::String(message.to_owned()));
        Self::Unknown { raw }
    }

    /// The EventBridge detail-type this event publishes as.
    #[must_use]
    pub fn detail_type(&self) -> &'static str {
        match self {
            Self::Unknown { .. } => "unknown",
        }
    }

    /// The id grouping this event with the rest of its message's lifecycle
    /// (the DynamoDB partition). Falls back to the SNS message id when the
    /// payload carries no originating message id.
    #[must_use]
    pub fn aggregate_id(&self, sns_message_id: &str) -> String {
        match self {
            Self::Unknown { .. } => sns_message_id.to_owned(),
        }
    }

    /// The event payload as forwarded to EventBridge.
    #[must_use]
    pub fn payload(&self) -> &Value {
        match self {
            Self::Unknown { raw } => raw,
        }
    }
}
