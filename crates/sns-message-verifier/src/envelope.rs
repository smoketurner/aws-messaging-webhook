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

/// The raw JSON body SNS POSTs to an HTTP(S) subscriber.
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
    #[serde(rename = "SigningCertURL")]
    pub signing_cert_url: String,
    /// Present on some `Notification` messages.
    pub subject: Option<String>,
    /// Present on `SubscriptionConfirmation` and `UnsubscribeConfirmation`.
    #[serde(rename = "SubscribeURL")]
    pub subscribe_url: Option<String>,
    /// Present on `SubscriptionConfirmation` and `UnsubscribeConfirmation`.
    pub token: Option<String>,
    /// Present on `Notification` messages.
    #[serde(rename = "UnsubscribeURL")]
    pub unsubscribe_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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
