//! AWS End User Messaging payloads: two-way inbound SMS and configuration-set
//! delivery events (SMS/MMS/voice), per the two-way-sms-payload and
//! configuration-sets-event-format docs. Fields the pipeline never reads stay
//! in the raw JSON that is persisted and forwarded verbatim.

use serde::Deserialize;

/// Two-way inbound SMS. Only the fields the pipeline reads are typed; the rest
/// (destination number, prior message id, …) ride along in the raw JSON that
/// is persisted and forwarded verbatim. `messageKeyword` carries the matched
/// registered keyword VERBATIM (e.g. "STOP", "JOIN") — no `KEYWORD_` prefix.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SmsInboundMessage {
    pub origination_number: String,
    #[serde(default)]
    pub message_keyword: Option<String>,
    #[serde(default)]
    pub message_body: Option<String>,
    pub inbound_message_id: String,
}

/// A configuration-set event (delivery receipt) for SMS (`TEXT_*`), MMS
/// (`MEDIA_*`), or voice (`VOICE_*`). Only the fields the pipeline reads are
/// typed; phone numbers, timestamps, pricing, etc. ride along in the raw JSON.
/// `event_type` stays a string — the value set grows over time and
/// classification only needs prefixes plus `is_final`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SmsDeliveryEvent {
    pub event_type: String,
    pub message_id: String,
    /// Whether this is the final event for the message. The docs warn that
    /// some failure-looking events (e.g. `TEXT_CARRIER_UNREACHABLE`) can be
    /// transient — this flag, not the event type, decides terminality.
    #[serde(default)]
    pub is_final: bool,
}

/// Event types that mean the message reached the recipient. Everything else
/// that arrives with `is_final = true` is a delivery failure.
const SUCCESS_EVENT_TYPES: [&str; 6] = [
    "TEXT_DELIVERED",
    "TEXT_SUCCESSFUL",
    "MEDIA_DELIVERED",
    "MEDIA_SUCCESSFUL",
    "VOICE_COMPLETED",
    "VOICE_ANSWERED",
];

impl SmsDeliveryEvent {
    #[must_use]
    pub fn is_successful_delivery(&self) -> bool {
        SUCCESS_EVENT_TYPES.contains(&self.event_type.as_str())
    }

    #[must_use]
    pub fn detail_type(&self) -> &'static str {
        if self.event_type.starts_with("MEDIA_") {
            "mms.delivery"
        } else if self.event_type.starts_with("VOICE_") {
            "voice.delivery"
        } else {
            "sms.delivery"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documented_inbound_sms_example() {
        let event: SmsInboundMessage = serde_json::from_str(
            r#"{
              "originationNumber":"+14255550182",
              "destinationNumber":"+12125550101",
              "messageKeyword":"JOIN",
              "messageBody":"EXAMPLE",
              "inboundMessageId":"cae173d2-66b9-564c-8309-21f858e9fb84",
              "previousPublishedMessageId":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            }"#,
        )
        .unwrap();
        assert_eq!(event.origination_number, "+14255550182");
        assert_eq!(event.message_keyword.as_deref(), Some("JOIN"));
        assert_eq!(
            event.inbound_message_id,
            "cae173d2-66b9-564c-8309-21f858e9fb84"
        );
    }

    #[test]
    fn inbound_sms_tolerates_missing_optional_fields() {
        let event: SmsInboundMessage = serde_json::from_str(
            r#"{"originationNumber":"+14255550182","inboundMessageId":"abc"}"#,
        )
        .unwrap();
        assert!(event.message_keyword.is_none());
        assert!(event.message_body.is_none());
    }

    #[test]
    fn parses_documented_text_event_example() {
        let event: SmsDeliveryEvent = serde_json::from_str(
            r#"{
              "eventType": "TEXT_SUCCESSFUL",
              "eventVersion": "1.0",
              "eventTimestamp": 1686975103470,
              "isFinal": true,
              "originationPhoneNumber": "+12065550152",
              "destinationPhoneNumber": "+14255550156",
              "isInternationalSend": false,
              "mcc": "310",
              "mnc": "800",
              "messageId": "862a8790-60c0-4430-9b2b-658bdexample",
              "messageRequestTimestamp": 1686975103170,
              "messageEncoding": "GSM",
              "messageType": "PROMOTIONAL",
              "messageStatus": "SUCCESSFUL",
              "messageStatusDescription": "Message has been accepted by phone carrier",
              "context": { "account": "bar" },
              "totalMessageParts": 1,
              "totalMessagePrice": 0.09582,
              "totalCarrierFee": 0.0
            }"#,
        )
        .unwrap();
        assert!(event.is_final);
        assert!(event.is_successful_delivery());
        assert_eq!(event.detail_type(), "sms.delivery");
        assert_eq!(event.message_id, "862a8790-60c0-4430-9b2b-658bdexample");
    }

    #[test]
    fn classifies_media_and_voice_events() {
        let media: SmsDeliveryEvent = serde_json::from_str(
            r#"{"eventType":"MEDIA_TTL_EXPIRED","messageId":"m1","isFinal":true}"#,
        )
        .unwrap();
        assert_eq!(media.detail_type(), "mms.delivery");
        assert!(!media.is_successful_delivery());

        let voice: SmsDeliveryEvent = serde_json::from_str(
            r#"{"eventType":"VOICE_ANSWERED","messageId":"v1","isFinal":true}"#,
        )
        .unwrap();
        assert_eq!(voice.detail_type(), "voice.delivery");
        assert!(voice.is_successful_delivery());
    }

    #[test]
    fn intermediate_events_are_not_final() {
        let event: SmsDeliveryEvent =
            serde_json::from_str(r#"{"eventType":"TEXT_QUEUED","messageId":"q1"}"#).unwrap();
        assert!(!event.is_final);
    }
}
