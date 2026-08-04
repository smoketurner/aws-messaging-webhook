//! Domain events parsed from the inner SNS `Message` JSON.

pub mod eum_sms;
pub mod ses_inbound;
pub mod ses_notification;

use serde::Deserialize as _;
use serde_json::Value;

use crate::model::eum_sms::{SmsDeliveryEvent, SmsInboundMessage};
use crate::model::ses_inbound::SesInboundNotification;
use crate::model::ses_notification::SesNotification;

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

/// A normalized messaging event. Each variant keeps the typed fields the
/// pipeline acts on plus the raw payload, which is what gets persisted and
/// forwarded. Unrecognized payloads become [`DomainEvent::Unknown`] rather
/// than being rejected: an at-least-once pipeline degrades to pass-through,
/// and new AWS event shapes appear here first.
#[derive(Debug, Clone)]
pub enum DomainEvent {
    SmsInbound {
        event: SmsInboundMessage,
        raw: Value,
    },
    SmsDelivery {
        event: SmsDeliveryEvent,
        raw: Value,
    },
    Ses {
        event: SesNotification,
        raw: Value,
    },
    SesInbound {
        event: SesInboundNotification,
        raw: Value,
    },
    Unknown {
        raw: Value,
    },
}

impl DomainEvent {
    /// Parses the inner SNS `Message` string according to the webhook path it
    /// arrived on. Anything that does not match the expected shape (including
    /// mis-wired topics) is forwarded as `Unknown` with a warning.
    #[must_use]
    pub fn parse(source: Source, message: &str) -> Self {
        let Ok(raw) = serde_json::from_str::<Value>(message) else {
            return Self::Unknown {
                raw: Value::String(message.to_owned()),
            };
        };

        // Deserialize by borrowing `&raw` (no clone of the parsed tree), then
        // move `raw` into the variant exactly once on success.
        match source {
            Source::SmsInbound => match SmsInboundMessage::deserialize(&raw) {
                Ok(event) => Self::SmsInbound { event, raw },
                Err(error) => Self::forward_unknown(source, raw, &error),
            },
            Source::SmsEvents => match SmsDeliveryEvent::deserialize(&raw) {
                Ok(event) => Self::SmsDelivery { event, raw },
                Err(error) => Self::forward_unknown(source, raw, &error),
            },
            Source::SesEvents => match SesNotification::deserialize(&raw) {
                Ok(event) => Self::Ses { event, raw },
                Err(error) => Self::forward_unknown(source, raw, &error),
            },
            // Inbound notifications carry a `receipt`; an SES *sending*
            // notification wired to this path lacks one, so fall through and
            // classify it properly rather than calling it unknown.
            Source::SesInbound => match SesInboundNotification::deserialize(&raw) {
                Ok(event) => Self::SesInbound { event, raw },
                Err(_) => match SesNotification::deserialize(&raw) {
                    Ok(event) => Self::Ses { event, raw },
                    Err(error) => Self::forward_unknown(source, raw, &error),
                },
            },
        }
    }

    fn forward_unknown(source: Source, raw: Value, error: &serde_json::Error) -> Self {
        tracing::warn!(
            source = source.as_str(),
            error = %error,
            "unrecognized payload; forwarding as unknown"
        );
        Self::Unknown { raw }
    }

    /// The EventBridge detail-type this event publishes as.
    #[must_use]
    pub fn detail_type(&self) -> &'static str {
        match self {
            Self::SmsInbound { .. } => "sms.inbound",
            Self::SmsDelivery { event, .. } => event.detail_type(),
            // A parsed-but-unmapped SES kind is a real, routable event (AWS
            // added a type we don't have a slug for), distinct from the
            // Unknown variant's unparseable junk — so downstream rules can
            // still filter it apart.
            Self::Ses { event, .. } => {
                ses_notification::detail_type_for(&event.kind).unwrap_or("ses.unknown")
            }
            Self::SesInbound { event, .. } => {
                if event.receipt.is_quarantined() {
                    "ses.inbound.quarantined"
                } else {
                    "ses.inbound"
                }
            }
            Self::Unknown { .. } => "unknown",
        }
    }

    /// The id grouping this event with the rest of its message's lifecycle
    /// (the DynamoDB partition). Falls back to the SNS message id when the
    /// payload carries no originating message id.
    #[must_use]
    pub fn aggregate_id(&self, sns_message_id: &str) -> String {
        match self {
            Self::SmsInbound { event, .. } => event.inbound_message_id.clone(),
            Self::SmsDelivery { event, .. } => event.message_id.clone(),
            Self::Ses { event, .. } => event.mail.message_id.clone(),
            Self::SesInbound { event, .. } => event.mail.message_id.clone(),
            Self::Unknown { .. } => sns_message_id.to_owned(),
        }
    }

    /// The event payload as forwarded to EventBridge.
    #[must_use]
    pub fn payload(&self) -> &Value {
        match self {
            Self::SmsInbound { raw, .. }
            | Self::SmsDelivery { raw, .. }
            | Self::Ses { raw, .. }
            | Self::SesInbound { raw, .. }
            | Self::Unknown { raw } => raw,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn routes_fix_the_expected_schema() {
        let inbound = DomainEvent::parse(
            Source::SmsInbound,
            r#"{"originationNumber":"+14255550182","inboundMessageId":"in-1"}"#,
        );
        assert!(matches!(inbound, DomainEvent::SmsInbound { .. }));
        assert_eq!(inbound.detail_type(), "sms.inbound");
        assert_eq!(inbound.aggregate_id("sns-1"), "in-1");

        let dlr = DomainEvent::parse(
            Source::SmsEvents,
            r#"{"eventType":"TEXT_DELIVERED","messageId":"out-1","isFinal":true}"#,
        );
        assert_eq!(dlr.detail_type(), "sms.delivery");
        assert_eq!(dlr.aggregate_id("sns-1"), "out-1");
    }

    #[test]
    fn ses_event_detail_type_follows_kind() {
        let event = DomainEvent::parse(
            Source::SesEvents,
            r#"{"eventType":"Open","mail":{"messageId":"m-1"}}"#,
        );
        assert_eq!(event.detail_type(), "ses.open");
        assert_eq!(event.aggregate_id("sns-1"), "m-1");
    }

    #[test]
    fn unmapped_ses_kind_is_ses_unknown_not_unknown() {
        let event = DomainEvent::parse(
            Source::SesEvents,
            r#"{"eventType":"BrandNewThing","mail":{"messageId":"m-2"}}"#,
        );
        assert!(matches!(event, DomainEvent::Ses { .. }));
        // Distinct from the Unknown variant's "unknown": this parsed cleanly.
        assert_eq!(event.detail_type(), "ses.unknown");
        assert_eq!(event.aggregate_id("sns-1"), "m-2");
    }

    #[test]
    fn sending_notification_on_inbound_path_still_classifies() {
        let event = DomainEvent::parse(
            Source::SesInbound,
            r#"{"notificationType":"Bounce",
                "bounce":{"bounceType":"Permanent","bouncedRecipients":[]},
                "mail":{"messageId":"m-3"}}"#,
        );
        assert_eq!(event.detail_type(), "ses.bounce");
    }

    #[test]
    fn wrong_shape_falls_back_to_unknown_with_sns_id() {
        let event = DomainEvent::parse(Source::SmsInbound, r#"{"something":"else"}"#);
        assert!(matches!(event, DomainEvent::Unknown { .. }));
        assert_eq!(event.detail_type(), "unknown");
        assert_eq!(event.aggregate_id("sns-9"), "sns-9");
    }

    #[test]
    fn non_json_message_falls_back_to_unknown_string() {
        let event = DomainEvent::parse(Source::SesEvents, "plain text");
        assert_eq!(event.payload(), &Value::String("plain text".to_owned()));
    }

    proptest! {
        #[test]
        fn parse_never_panics(message in ".{0,256}") {
            for source in [Source::SmsInbound, Source::SmsEvents, Source::SesEvents, Source::SesInbound] {
                drop(DomainEvent::parse(source, &message));
            }
        }
    }
}
