use serde::Deserialize;

/// The `Type` field of an SNS HTTP(S) delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum MessageType {
    Notification,
    SubscriptionConfirmation,
    UnsubscribeConfirmation,
}

impl MessageType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Notification => "Notification",
            Self::SubscriptionConfirmation => "SubscriptionConfirmation",
            Self::UnsubscribeConfirmation => "UnsubscribeConfirmation",
        }
    }
}

/// The raw JSON body SNS POSTs to an HTTP(S) subscriber, or the `Sns` record
/// object of a direct SNS → Lambda invocation — the same envelope, except the
/// Lambda shape spells the URL fields `SigningCertUrl`/`UnsubscribeUrl`
/// (accepted via aliases).
///
/// Unknown fields are ignored so AWS can add fields without breaking parsing.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsEnvelope {
    #[serde(rename = "Type")]
    pub message_type: MessageType,
    pub message_id: String,
    pub topic_arn: String,
    pub message: String,
    pub timestamp: String,
    pub signature_version: String,
    pub signature: String,
    #[serde(rename = "SigningCertURL", alias = "SigningCertUrl")]
    pub signing_cert_url: String,
    /// Present on some `Notification` messages.
    pub subject: Option<String>,
    /// Present on `SubscriptionConfirmation` and `UnsubscribeConfirmation`.
    #[serde(rename = "SubscribeURL")]
    pub subscribe_url: Option<String>,
    /// Present on `SubscriptionConfirmation` and `UnsubscribeConfirmation`.
    pub token: Option<String>,
    /// Present on `Notification` messages.
    #[serde(rename = "UnsubscribeURL", alias = "UnsubscribeUrl")]
    pub unsubscribe_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn lambda_record_casing_parses_via_aliases() {
        let record = r#"{
            "Type": "Notification",
            "MessageId": "id-1",
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:t",
            "Message": "hello",
            "Timestamp": "2026-08-03T19:12:52.000Z",
            "SignatureVersion": "1",
            "Signature": "sig",
            "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/cert.pem",
            "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
            "MessageAttributes": {}
        }"#;
        let envelope: SnsEnvelope = serde_json::from_str(record).unwrap();
        assert_eq!(
            envelope.signing_cert_url,
            "https://sns.us-east-1.amazonaws.com/cert.pem"
        );
        assert_eq!(
            envelope.unsubscribe_url.as_deref(),
            Some("https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe")
        );
    }

    proptest! {
        #[test]
        fn deserialize_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            drop(serde_json::from_slice::<SnsEnvelope>(&bytes));
        }

        #[test]
        fn deserialize_never_panics_on_json(value in "\\{.*\\}") {
            drop(serde_json::from_str::<SnsEnvelope>(&value));
        }
    }
}
