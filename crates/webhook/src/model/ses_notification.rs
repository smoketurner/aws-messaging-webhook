//! SES sending-event notifications, covering BOTH wire formats: config-set
//! event publishing (top-level `eventType`) and identity feedback
//! notifications (top-level `notificationType`). Only the fields the pipeline
//! acts on are typed; the full payload is persisted and forwarded verbatim.

use std::collections::HashMap;

use mailbox::send::SES_MESSAGE_TAG;
use serde::{Deserialize, Serialize};

use mailbox::labels::Label;

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
    /// The message's tags: SES's own (`ses:configuration-set`, …) and the
    /// ones the sender attached. Present on event-publishing events only.
    #[serde(default)]
    pub tags: HashMap<String, Vec<String>>,
}

impl SesMail {
    /// The mailbox message this mail was sent as, when the sender tagged it
    /// ([`SES_MESSAGE_TAG`]). Absent for mail this service did not send.
    #[must_use]
    pub fn mailbox_message(&self) -> Option<&str> {
        self.tags
            .get(SES_MESSAGE_TAG)
            .and_then(|values| values.first())
            .map(String::as_str)
    }
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
    pub(crate) fn is_permanent(&self) -> bool {
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
    pub(crate) fn is_bot(&self) -> bool {
        match &self.is_bot_event {
            Some(BotFlag::Likely) => true,
            Some(BotFlag::Unlikely | BotFlag::Other(_)) | None => false,
        }
    }
}

/// The SES event kind, from `eventType` or `notificationType`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum SesEventKind {
    Send,
    Delivery,
    Bounce,
    Complaint,
    Reject,
    Open,
    Click,
    /// Really does contain a space on the wire.
    RenderingFailure,
    DeliveryDelay,
    Subscription,
    /// A value this service has no variant for, kept verbatim.
    Other(String),
}

impl SesEventKind {
    /// The value as AWS sent it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Send => "Send",
            Self::Delivery => "Delivery",
            Self::Bounce => "Bounce",
            Self::Complaint => "Complaint",
            Self::Reject => "Reject",
            Self::Open => "Open",
            Self::Click => "Click",
            Self::RenderingFailure => "Rendering Failure",
            Self::DeliveryDelay => "DeliveryDelay",
            Self::Subscription => "Subscription",
            Self::Other(value) => value,
        }
    }
}

impl From<String> for SesEventKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Send" => Self::Send,
            "Delivery" => Self::Delivery,
            "Bounce" => Self::Bounce,
            "Complaint" => Self::Complaint,
            "Reject" => Self::Reject,
            "Open" => Self::Open,
            "Click" => Self::Click,
            "Rendering Failure" => Self::RenderingFailure,
            "DeliveryDelay" => Self::DeliveryDelay,
            "Subscription" => Self::Subscription,
            _ => Self::Other(value),
        }
    }
}

impl From<SesEventKind> for String {
    fn from(value: SesEventKind) -> Self {
        match value {
            SesEventKind::Send => "Send".to_owned(),
            SesEventKind::Delivery => "Delivery".to_owned(),
            SesEventKind::Bounce => "Bounce".to_owned(),
            SesEventKind::Complaint => "Complaint".to_owned(),
            SesEventKind::Reject => "Reject".to_owned(),
            SesEventKind::Open => "Open".to_owned(),
            SesEventKind::Click => "Click".to_owned(),
            SesEventKind::RenderingFailure => "Rendering Failure".to_owned(),
            SesEventKind::DeliveryDelay => "DeliveryDelay".to_owned(),
            SesEventKind::Subscription => "Subscription".to_owned(),
            SesEventKind::Other(value) => value,
        }
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
    pub fn mailbox_label(&self) -> Option<Label> {
        match self {
            Self::Delivery => Some(Label::Delivered),
            Self::Bounce => Some(Label::Bounced),
            Self::Complaint => Some(Label::Complained),
            Self::Reject => Some(Label::Rejected),
            Self::Open => Some(Label::Opened),
            Self::Send
            | Self::Click
            | Self::RenderingFailure
            | Self::DeliveryDelay
            | Self::Subscription
            | Self::Other(_) => None,
        }
    }

    /// Whether an event of this kind shows that SES accepted the message.
    /// Every kind does except the two SES raises for a message it took but
    /// never tried to deliver.
    #[must_use]
    pub fn confirms_send(&self) -> bool {
        match self {
            Self::Send
            | Self::Delivery
            | Self::Bounce
            | Self::Complaint
            | Self::Open
            | Self::Click
            | Self::DeliveryDelay
            | Self::Subscription => true,
            Self::Reject | Self::RenderingFailure | Self::Other(_) => false,
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

/// A bounce's `bounceType`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum BounceType {
    Undetermined,
    Permanent,
    Transient,
    /// A value this service has no variant for, kept verbatim.
    Other(String),
}

impl BounceType {
    /// The value as AWS sent it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Undetermined => "Undetermined",
            Self::Permanent => "Permanent",
            Self::Transient => "Transient",
            Self::Other(value) => value,
        }
    }
}

impl From<String> for BounceType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Undetermined" => Self::Undetermined,
            "Permanent" => Self::Permanent,
            "Transient" => Self::Transient,
            _ => Self::Other(value),
        }
    }
}

impl From<BounceType> for String {
    fn from(value: BounceType) -> Self {
        match value {
            BounceType::Undetermined => "Undetermined".to_owned(),
            BounceType::Permanent => "Permanent".to_owned(),
            BounceType::Transient => "Transient".to_owned(),
            BounceType::Other(value) => value,
        }
    }
}

/// An engagement's `isBotEvent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum BotFlag {
    Likely,
    Unlikely,
    /// A value this service has no variant for, kept verbatim.
    Other(String),
}

impl BotFlag {
    /// The value as AWS sent it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Likely => "Likely",
            Self::Unlikely => "Unlikely",
            Self::Other(value) => value,
        }
    }
}

impl From<String> for BotFlag {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Likely" => Self::Likely,
            "Unlikely" => Self::Unlikely,
            _ => Self::Other(value),
        }
    }
}

impl From<BotFlag> for String {
    fn from(value: BotFlag) -> Self {
        match value {
            BotFlag::Likely => "Likely".to_owned(),
            BotFlag::Unlikely => "Unlikely".to_owned(),
            BotFlag::Other(value) => value,
        }
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
                Label::Delivered,
                "message.delivered",
            ),
            (SesEventKind::Bounce, Label::Bounced, "message.bounced"),
            (
                SesEventKind::Complaint,
                Label::Complained,
                "message.complained",
            ),
            (SesEventKind::Reject, Label::Rejected, "message.rejected"),
            (SesEventKind::Open, Label::Opened, "message.opened"),
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
