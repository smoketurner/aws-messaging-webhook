//! SES sending-event notifications, covering BOTH wire formats: config-set
//! event publishing (top-level `eventType`) and identity feedback
//! notifications (top-level `notificationType`). Only the fields the pipeline
//! acts on are typed; the full payload is persisted and forwarded verbatim.

use serde::{Deserialize, Serialize};

use crate::mail::labels::SystemLabel;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesNotification {
    /// `eventType` (event publishing) or `notificationType` (identity
    /// notifications) — same value space.
    #[serde(rename = "eventType", alias = "notificationType")]
    pub kind: SesEventKind,
    pub mail: SesMail,
    #[serde(default)]
    pub bounce: Option<SesBounce>,
    #[serde(default)]
    pub complaint: Option<SesComplaint>,
    #[serde(default)]
    pub delivery: Option<SesDelivery>,
    #[serde(default)]
    pub reject: Option<SesReject>,
    /// Present only on `Open` events. Typed solely for its `isBotEvent`
    /// signal; the full object is still forwarded verbatim.
    #[serde(default)]
    pub open: Option<SesEngagement>,
    /// Present only on `Click` events.
    #[serde(default)]
    pub click: Option<SesEngagement>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesMail {
    /// The SES message id — the aggregate id tying every event of one sent
    /// email together.
    pub message_id: String,
    /// Envelope sender. Present on inbound receipts and most sending events.
    #[serde(default)]
    pub source: Option<String>,
    /// Envelope recipients.
    #[serde(default)]
    pub destination: Vec<String>,
    /// SES-assigned receipt/send timestamp.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// SES-parsed well-known headers (from/to/subject/date/messageId). Present
    /// on inbound receipts and on sending events when header inclusion is on.
    #[serde(default)]
    pub common_headers: Option<SesCommonHeaders>,
}

/// The subset of `commonHeaders` worth surfacing for routing/filtering.
/// Every field is optional — SES omits headers the message didn't carry.
/// `Serialize` so it can be borrowed straight into the `meta.inbound` summary.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SesCommonHeaders {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// The RFC 5322 `Message-ID` header — distinct from the SES `messageId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesBounce {
    pub bounce_type: BounceType,
    #[serde(default)]
    pub bounce_sub_type: Option<String>,
    #[serde(default)]
    pub bounced_recipients: Vec<SesRecipient>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

impl SesBounce {
    /// Only permanent bounces feed the account-level suppression list.
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        match self.bounce_type {
            BounceType::Permanent => true,
            BounceType::Undetermined | BounceType::Transient | BounceType::Other(_) => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesComplaint {
    #[serde(default)]
    pub complained_recipients: Vec<SesRecipient>,
    /// The feedback report's type (`abuse`, `fraud`, …); absent when the
    /// report carried none.
    #[serde(default)]
    pub complaint_feedback_type: Option<String>,
    /// `OnAccountSuppressionList` when SES itself generated the complaint,
    /// otherwise null.
    #[serde(default)]
    pub complaint_sub_type: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesRecipient {
    pub email_address: String,
    /// The DSN status code (e.g. `5.1.1`), present on a bounced recipient
    /// when an MTA reported one.
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesDelivery {
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub recipients: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesReject {
    #[serde(default)]
    pub reason: Option<String>,
}

/// The `open` / `click` engagement object. `isBotEvent` is what the
/// aggregate acts on and `timestamp` what a mailbox `message.opened` event
/// reports; the rest of the object is forwarded verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesEngagement {
    /// SES's assessment of whether the interaction was generated by an
    /// automated system rather than a human. Absent on events that predate
    /// the feature.
    #[serde(default)]
    pub is_bot_event: Option<BotFlag>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

impl SesEngagement {
    /// True only when SES flags the interaction as `Likely` bot-generated.
    /// A missing or `Unlikely` value counts as human, so pre-feature events
    /// keep accruing to the human counters.
    #[must_use]
    pub fn is_bot(&self) -> bool {
        match &self.is_bot_event {
            Some(BotFlag::Likely) => true,
            Some(BotFlag::Unlikely | BotFlag::Other(_)) | None => false,
        }
    }
}

open_string_enum! {
    /// The SES event kind, from `eventType` or `notificationType`.
    pub enum SesEventKind {
        Send = "Send",
        Delivery = "Delivery",
        Bounce = "Bounce",
        Complaint = "Complaint",
        Reject = "Reject",
        Open = "Open",
        Click = "Click",
        /// Really does contain a space on the wire.
        RenderingFailure = "Rendering Failure",
        DeliveryDelay = "DeliveryDelay",
        Subscription = "Subscription",
    }
}

impl SesEventKind {
    /// The EventBridge detail-type this kind publishes as. `None` for a kind
    /// with no slug yet.
    #[must_use]
    pub fn detail_type(&self) -> Option<&'static str> {
        match self {
            Self::Send => Some("ses.send"),
            Self::Delivery => Some("ses.delivery"),
            Self::Bounce => Some("ses.bounce"),
            Self::Complaint => Some("ses.complaint"),
            Self::Reject => Some("ses.reject"),
            Self::Open => Some("ses.open"),
            Self::Click => Some("ses.click"),
            Self::RenderingFailure => Some("ses.rendering-failure"),
            Self::DeliveryDelay => Some("ses.delivery-delay"),
            Self::Subscription => Some("ses.subscription"),
            Self::Other(_) => None,
        }
    }

    /// The mailbox label this kind records on the message it belongs to.
    ///
    /// `Send` is deliberately absent: the sender already marks the message
    /// sent, and a `Send` event arriving afterwards would say nothing new.
    /// `Click` has no mailbox label in the vocabulary this service uses.
    #[must_use]
    pub fn mailbox_label(&self) -> Option<SystemLabel> {
        match self {
            Self::Delivery => Some(SystemLabel::Delivered),
            Self::Bounce => Some(SystemLabel::Bounced),
            Self::Complaint => Some(SystemLabel::Complained),
            Self::Reject => Some(SystemLabel::Rejected),
            Self::Open => Some(SystemLabel::Opened),
            Self::Send
            | Self::Click
            | Self::RenderingFailure
            | Self::DeliveryDelay
            | Self::Subscription
            | Self::Other(_) => None,
        }
    }

    /// The event type this kind publishes for a mailbox message. `Send` is
    /// absent because the sender's relabel publishes `message.sent`, and
    /// `Click` and the rest have no mailbox event.
    #[must_use]
    pub fn mailbox_event_type(&self) -> Option<&'static str> {
        match self {
            Self::Delivery => Some("message.delivered"),
            Self::Bounce => Some("message.bounced"),
            Self::Complaint => Some("message.complained"),
            Self::Reject => Some("message.rejected"),
            Self::Open => Some("message.opened"),
            Self::Send
            | Self::Click
            | Self::RenderingFailure
            | Self::DeliveryDelay
            | Self::Subscription
            | Self::Other(_) => None,
        }
    }
}

open_string_enum! {
    /// A bounce's `bounceType`.
    pub enum BounceType {
        Undetermined = "Undetermined",
        Permanent = "Permanent",
        Transient = "Transient",
    }
}

open_string_enum! {
    /// An engagement's `isBotEvent`.
    pub enum BotFlag {
        Likely = "Likely",
        Unlikely = "Unlikely",
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
        assert_eq!(event.kind, SesEventKind::Bounce);
        assert_eq!(event.mail.message_id, "EXAMPLE7c191be45");
        let bounce = event.bounce.unwrap();
        assert!(bounce.is_permanent());
        assert_eq!(bounce.bounce_sub_type.as_deref(), Some("General"));
        assert_eq!(
            bounce.timestamp.as_deref(),
            Some("2017-08-05T00:41:02.669Z")
        );
        assert_eq!(
            bounce.bounced_recipients[0].email_address,
            "recipient@example.com"
        );
        assert_eq!(
            bounce.bounced_recipients[0].status.as_deref(),
            Some("5.1.1")
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
        assert_eq!(event.kind, SesEventKind::Complaint);
        let complaint = event.complaint.unwrap();
        assert_eq!(
            complaint.complained_recipients[0].email_address,
            "recipient1@example.com"
        );
        assert_eq!(complaint.complaint_feedback_type.as_deref(), Some("abuse"));
        assert_eq!(complaint.complaint_sub_type, None);
        assert_eq!(
            complaint.timestamp.as_deref(),
            Some("2012-05-25T14:59:38.623Z")
        );
    }

    #[test]
    fn parses_delivery_reject_and_open_details() {
        let delivery: SesNotification = serde_json::from_str(
            r#"{"eventType":"Delivery","mail":{"messageId":"m"},
                "delivery":{"timestamp":"2016-10-19T23:21:04.133Z",
                            "recipients":["recipient@example.com"],
                            "smtpResponse":"250 2.6.0 Message received"}}"#,
        )
        .unwrap();
        let delivery = delivery.delivery.unwrap();
        assert_eq!(
            delivery.timestamp.as_deref(),
            Some("2016-10-19T23:21:04.133Z")
        );
        assert_eq!(delivery.recipients, vec!["recipient@example.com"]);

        let reject: SesNotification = serde_json::from_str(
            r#"{"eventType":"Reject","mail":{"messageId":"m"},"reject":{"reason":"Bad content"}}"#,
        )
        .unwrap();
        assert_eq!(
            reject.reject.unwrap().reason.as_deref(),
            Some("Bad content")
        );

        let open: SesNotification = serde_json::from_str(
            r#"{"eventType":"Open","mail":{"messageId":"m"},
                "open":{"timestamp":"2017-08-09T22:00:19.652Z","ipAddress":"192.0.2.1"}}"#,
        )
        .unwrap();
        assert_eq!(
            open.open.unwrap().timestamp.as_deref(),
            Some("2017-08-09T22:00:19.652Z")
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
    fn open_event_parses_bot_signal() {
        let event: SesNotification = serde_json::from_str(
            r#"{"eventType":"Open","mail":{"messageId":"m"},
                "open":{"ipAddress":"192.0.2.1","isBotEvent":"Likely"}}"#,
        )
        .unwrap();
        assert!(event.open.unwrap().is_bot());
    }

    #[test]
    fn click_unlikely_is_human() {
        let event: SesNotification = serde_json::from_str(
            r#"{"eventType":"Click","mail":{"messageId":"m"},
                "click":{"link":"https://x","isBotEvent":"Unlikely"}}"#,
        )
        .unwrap();
        assert!(!event.click.unwrap().is_bot());
    }

    #[test]
    fn missing_bot_signal_counts_as_human() {
        // Pre-feature events carry no isBotEvent — they must not be treated
        // as bots.
        let engagement: SesEngagement =
            serde_json::from_str(r#"{"ipAddress":"192.0.2.1"}"#).unwrap();
        assert!(!engagement.is_bot());
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
            let parsed = SesEventKind::from(kind.to_owned());
            assert_eq!(parsed.detail_type(), Some(expected), "kind {kind}");
            assert_eq!(parsed.as_str(), kind);
        }
        assert_eq!(
            SesEventKind::from("SomethingNew".to_owned()).detail_type(),
            None
        );
    }

    #[test]
    fn kind_reads_from_either_field_name() {
        for field in ["eventType", "notificationType"] {
            let json = format!(r#"{{"{field}":"Open","mail":{{"messageId":"m"}}}}"#);
            let event: SesNotification = serde_json::from_str(&json).unwrap();
            assert_eq!(event.kind, SesEventKind::Open, "{field}");
        }
    }

    #[test]
    fn open_enums_round_trip_known_and_unknown_values_verbatim() {
        for wire in ["Rendering Failure", "Delivery", "SomethingNew"] {
            let kind: SesEventKind = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(kind.as_str(), wire);
            assert_eq!(
                serde_json::to_value(&kind).unwrap(),
                serde_json::json!(wire)
            );
        }
        let kind: SesEventKind = serde_json::from_str(r#""SomethingNew""#).unwrap();
        assert_eq!(kind, SesEventKind::Other("SomethingNew".to_owned()));

        for wire in ["Permanent", "Transient", "Undetermined", "Soft"] {
            let bounce: BounceType = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(bounce.as_str(), wire);
            assert_eq!(
                serde_json::to_value(&bounce).unwrap(),
                serde_json::json!(wire)
            );
        }
        for wire in ["Likely", "Unlikely", "Maybe"] {
            let flag: BotFlag = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(flag.as_str(), wire);
            assert_eq!(
                serde_json::to_value(&flag).unwrap(),
                serde_json::json!(wire)
            );
        }
    }

    #[test]
    fn unknown_bounce_type_is_not_permanent() {
        let bounce: SesBounce = serde_json::from_str(r#"{"bounceType":"Soft"}"#).unwrap();
        assert_eq!(bounce.bounce_type, BounceType::Other("Soft".to_owned()));
        assert!(!bounce.is_permanent());
    }

    #[test]
    fn unknown_bot_flag_counts_as_human() {
        let engagement: SesEngagement = serde_json::from_str(r#"{"isBotEvent":"Maybe"}"#).unwrap();
        assert!(!engagement.is_bot());
    }

    #[test]
    fn mailbox_mappings_cover_the_labelled_kinds() {
        for (kind, label, event_type) in [
            (
                SesEventKind::Delivery,
                SystemLabel::Delivered,
                "message.delivered",
            ),
            (
                SesEventKind::Bounce,
                SystemLabel::Bounced,
                "message.bounced",
            ),
            (
                SesEventKind::Complaint,
                SystemLabel::Complained,
                "message.complained",
            ),
            (
                SesEventKind::Reject,
                SystemLabel::Rejected,
                "message.rejected",
            ),
            (SesEventKind::Open, SystemLabel::Opened, "message.opened"),
        ] {
            assert_eq!(kind.mailbox_label(), Some(label));
            assert_eq!(kind.mailbox_event_type(), Some(event_type));
        }
        for kind in [
            SesEventKind::Send,
            SesEventKind::Click,
            SesEventKind::Other("Delivery ".to_owned()),
        ] {
            assert_eq!(kind.mailbox_label(), None);
            assert_eq!(kind.mailbox_event_type(), None);
        }
    }
}
