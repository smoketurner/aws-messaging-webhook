//! SES sending-event notifications, covering BOTH wire formats: config-set
//! event publishing (top-level `eventType`) and identity feedback
//! notifications (top-level `notificationType`). Only the fields the pipeline
//! acts on are typed; the full payload is persisted and forwarded verbatim.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesNotification {
    /// `eventType` (event publishing) or `notificationType` (identity
    /// notifications) — same value space.
    #[serde(rename = "eventType", alias = "notificationType")]
    pub kind: String,
    pub mail: SesMail,
    #[serde(default)]
    pub bounce: Option<SesBounce>,
    #[serde(default)]
    pub complaint: Option<SesComplaint>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMail {
    /// The SES message id — the aggregate id tying every event of one sent
    /// email together.
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesBounce {
    /// `Undetermined` | `Permanent` | `Transient` (kept as a string — SES
    /// reserves the right to add values).
    pub bounce_type: String,
    #[serde(default)]
    pub bounced_recipients: Vec<SesRecipient>,
}

impl SesBounce {
    /// Only permanent bounces feed the account-level suppression list.
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        self.bounce_type == "Permanent"
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesComplaint {
    #[serde(default)]
    pub complained_recipients: Vec<SesRecipient>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesRecipient {
    pub email_address: String,
}

/// Maps the SES event kind to an EventBridge detail-type. `Rendering Failure`
/// really does contain a space on the wire.
#[must_use]
pub fn detail_type_for(kind: &str) -> Option<&'static str> {
    match kind {
        "Send" => Some("ses.send"),
        "Delivery" => Some("ses.delivery"),
        "Bounce" => Some("ses.bounce"),
        "Complaint" => Some("ses.complaint"),
        "Reject" => Some("ses.reject"),
        "Open" => Some("ses.open"),
        "Click" => Some("ses.click"),
        "Rendering Failure" => Some("ses.rendering-failure"),
        "DeliveryDelay" => Some("ses.delivery-delay"),
        "Subscription" => Some("ses.subscription"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_publishing_bounce() {
        let event: SesNotification = serde_json::from_str(
            r#"{
              "eventType":"Bounce",
              "bounce":{
                "bounceType":"Permanent",
                "bounceSubType":"General",
                "bouncedRecipients":[
                  {"emailAddress":"recipient@example.com","action":"failed","status":"5.1.1",
                   "diagnosticCode":"smtp; 550 5.1.1 user unknown"}
                ],
                "timestamp":"2017-08-05T00:41:02.669Z",
                "feedbackId":"01000157c44f053b-61b59c11-example-000000"
              },
              "mail":{"timestamp":"2017-08-05T00:40:01.123Z","messageId":"EXAMPLE7c191be45",
                      "source":"sender@example.com","destination":["recipient@example.com"]}
            }"#,
        )
        .unwrap();
        assert_eq!(event.kind, "Bounce");
        assert_eq!(event.mail.message_id, "EXAMPLE7c191be45");
        let bounce = event.bounce.unwrap();
        assert!(bounce.is_permanent());
        assert_eq!(
            bounce.bounced_recipients[0].email_address,
            "recipient@example.com"
        );
    }

    #[test]
    fn parses_identity_notification_format() {
        let event: SesNotification = serde_json::from_str(
            r#"{
              "notificationType":"Complaint",
              "complaint":{
                "complainedRecipients":[{"emailAddress":"recipient1@example.com"}],
                "complaintFeedbackType":"abuse",
                "timestamp":"2012-05-25T14:59:38.623Z",
                "feedbackId":"000001378603177f-example"
              },
              "mail":{"timestamp":"2012-05-25T14:59:38.237Z","messageId":"0000013786031775-example",
                      "source":"email_1337@amazon.com","destination":["recipient1@example.com"]}
            }"#,
        )
        .unwrap();
        assert_eq!(event.kind, "Complaint");
        let complaint = event.complaint.unwrap();
        assert_eq!(
            complaint.complained_recipients[0].email_address,
            "recipient1@example.com"
        );
    }

    #[test]
    fn transient_bounce_is_not_permanent() {
        let bounce: SesBounce = serde_json::from_str(
            r#"{"bounceType":"Transient","bouncedRecipients":[{"emailAddress":"a@b.c"}]}"#,
        )
        .unwrap();
        assert!(!bounce.is_permanent());
    }

    #[test]
    fn detail_types_cover_all_documented_kinds() {
        for (kind, expected) in [
            ("Send", "ses.send"),
            ("Delivery", "ses.delivery"),
            ("Bounce", "ses.bounce"),
            ("Complaint", "ses.complaint"),
            ("Reject", "ses.reject"),
            ("Open", "ses.open"),
            ("Click", "ses.click"),
            ("Rendering Failure", "ses.rendering-failure"),
            ("DeliveryDelay", "ses.delivery-delay"),
            ("Subscription", "ses.subscription"),
        ] {
            assert_eq!(detail_type_for(kind), Some(expected), "kind {kind}");
        }
        assert_eq!(detail_type_for("SomethingNew"), None);
    }
}
