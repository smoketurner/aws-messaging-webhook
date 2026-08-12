//! Domain events parsed from the inner SNS `Message` JSON.

pub mod eum_sms;
pub mod ses_inbound;
pub mod ses_notification;

use serde::Deserialize as _;
use serde_json::Value;

use crate::model::eum_sms::{SmsDeliveryEvent, SmsInboundMessage};
use crate::model::ses_inbound::SesInboundNotification;
use crate::model::ses_notification::SesNotification;

/// An event family. Each HTTP webhook path is wired to one family, but the
/// family a message actually belongs to comes from
/// [`DomainEvent::classify`] — the path only names what the operator expects.
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

    /// Parses the label written by [`Self::as_str`] back into a family;
    /// `None` for `"unknown"` or any unrecognized label.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "sms-inbound" => Some(Self::SmsInbound),
            "sms-events" => Some(Self::SmsEvents),
            "ses-events" => Some(Self::SesEvents),
            "ses-inbound" => Some(Self::SesInbound),
            _ => None,
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
    /// Classifies the inner SNS `Message` string by shape: try-parse each
    /// family, most specific first. The order is load-bearing in exactly one
    /// place — an inbound receipt also satisfies the sending-notification
    /// shape (`notificationType` + `mail`), so `SesInbound` must be tried
    /// before `Ses`; every other pair of families is structurally disjoint
    /// (pinned by the fixture matrix in `tests/model_fixtures.rs`).
    #[must_use]
    pub fn classify(message: &str) -> Self {
        let Ok(raw) = serde_json::from_str::<Value>(message) else {
            return Self::Unknown {
                raw: Value::String(message.to_owned()),
            };
        };

        // Deserialize by borrowing `&raw` (no clone of the parsed tree), then
        // move `raw` into the variant exactly once on success.
        if let Ok(event) = SesInboundNotification::deserialize(&raw) {
            return Self::SesInbound { event, raw };
        }
        if let Ok(event) = SesNotification::deserialize(&raw) {
            return Self::Ses { event, raw };
        }
        if let Ok(event) = SmsInboundMessage::deserialize(&raw) {
            return Self::SmsInbound { event, raw };
        }
        if let Ok(event) = SmsDeliveryEvent::deserialize(&raw) {
            return Self::SmsDelivery { event, raw };
        }
        Self::Unknown { raw }
    }

    /// The family this event classified into; `None` for [`Self::Unknown`].
    #[must_use]
    pub fn family(&self) -> Option<Source> {
        match self {
            Self::SmsInbound { .. } => Some(Source::SmsInbound),
            Self::SmsDelivery { .. } => Some(Source::SmsEvents),
            Self::Ses { .. } => Some(Source::SesEvents),
            Self::SesInbound { .. } => Some(Source::SesInbound),
            Self::Unknown { .. } => None,
        }
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

    /// The id of the outbound message this event is replying to, if any.
    /// Currently only populated for inbound SMS that carries a
    /// `previousPublishedMessageId`.
    #[must_use]
    pub fn previous_message_id(&self) -> Option<&str> {
        match self {
            Self::SmsInbound { event, .. } => event.previous_published_message_id.as_deref(),
            _ => None,
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
    fn shapes_classify_into_their_families() {
        let inbound = DomainEvent::classify(
            r#"{"originationNumber":"+14255550182","inboundMessageId":"in-1"}"#,
        );
        assert_eq!(inbound.family(), Some(Source::SmsInbound));
        assert_eq!(inbound.detail_type(), "sms.inbound");
        assert_eq!(inbound.aggregate_id("sns-1"), "in-1");

        let dlr = DomainEvent::classify(
            r#"{"eventType":"TEXT_DELIVERED","messageId":"out-1","isFinal":true}"#,
        );
        assert_eq!(dlr.family(), Some(Source::SmsEvents));
        assert_eq!(dlr.detail_type(), "sms.delivery");
        assert_eq!(dlr.aggregate_id("sns-1"), "out-1");
    }

    #[test]
    fn ses_event_detail_type_follows_kind() {
        let event = DomainEvent::classify(r#"{"eventType":"Open","mail":{"messageId":"m-1"}}"#);
        assert_eq!(event.family(), Some(Source::SesEvents));
        assert_eq!(event.detail_type(), "ses.open");
        assert_eq!(event.aggregate_id("sns-1"), "m-1");
    }

    #[test]
    fn unmapped_ses_kind_is_ses_unknown_not_unknown() {
        let event =
            DomainEvent::classify(r#"{"eventType":"BrandNewThing","mail":{"messageId":"m-2"}}"#);
        assert!(matches!(event, DomainEvent::Ses { .. }));
        // Distinct from the Unknown variant's "unknown": this parsed cleanly.
        assert_eq!(event.detail_type(), "ses.unknown");
        assert_eq!(event.aggregate_id("sns-1"), "m-2");
    }

    /// The one real overlap: a `Received` notification also satisfies the
    /// sending-notification shape, so the SesInbound-before-Ses try order is
    /// what keeps receipts out of the Ses family.
    #[test]
    fn received_notification_classifies_inbound_not_ses() {
        let event = DomainEvent::classify(
            r#"{"notificationType":"Received","receipt":{},"mail":{"messageId":"m-3"}}"#,
        );
        assert_eq!(event.family(), Some(Source::SesInbound));

        let bounce = DomainEvent::classify(
            r#"{"notificationType":"Bounce",
                "bounce":{"bounceType":"Permanent","bouncedRecipients":[]},
                "mail":{"messageId":"m-4"}}"#,
        );
        assert_eq!(bounce.family(), Some(Source::SesEvents));
        assert_eq!(bounce.detail_type(), "ses.bounce");
    }

    #[test]
    fn unmatched_shape_falls_back_to_unknown_with_sns_id() {
        let event = DomainEvent::classify(r#"{"something":"else"}"#);
        assert!(matches!(event, DomainEvent::Unknown { .. }));
        assert_eq!(event.family(), None);
        assert_eq!(event.detail_type(), "unknown");
        assert_eq!(event.aggregate_id("sns-9"), "sns-9");
    }

    #[test]
    fn previous_message_id_present_on_sms_reply() {
        let event = DomainEvent::classify(
            r#"{"originationNumber":"+1","inboundMessageId":"in-1","previousPublishedMessageId":"out-99"}"#,
        );
        assert_eq!(event.previous_message_id(), Some("out-99"));
    }

    #[test]
    fn previous_message_id_absent_on_unsolicited_sms() {
        let event =
            DomainEvent::classify(r#"{"originationNumber":"+1","inboundMessageId":"in-2"}"#);
        assert_eq!(event.previous_message_id(), None);
    }

    #[test]
    fn previous_message_id_absent_on_non_sms_events() {
        let event = DomainEvent::classify(r#"{"eventType":"Delivery","mail":{"messageId":"m-1"}}"#);
        assert_eq!(event.previous_message_id(), None);
    }

    #[test]
    fn non_json_message_falls_back_to_unknown_string() {
        let event = DomainEvent::classify("plain text");
        assert_eq!(event.payload(), &Value::String("plain text".to_owned()));
    }

    proptest! {
        #[test]
        fn classify_never_panics(message in ".{0,256}") {
            drop(DomainEvent::classify(&message));
        }
    }
}
