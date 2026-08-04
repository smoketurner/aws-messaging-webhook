use crate::envelope::{MessageType, SnsEnvelope};
use crate::error::VerifyError;

/// Builds the canonical string SNS signs, per the AWS signature verification
/// procedure (the same field order and `Name\nValue\n` layout as the official
/// `aws-js-sns-message-validator`).
///
/// # Errors
///
/// Returns [`VerifyError::MissingField`] if a confirmation message lacks
/// `SubscribeURL` or `Token`.
pub fn build_string_to_sign(envelope: &SnsEnvelope) -> Result<String, VerifyError> {
    let mut canonical = String::new();
    match envelope.message_type {
        MessageType::Notification => {
            push_pair(&mut canonical, "Message", &envelope.message);
            push_pair(&mut canonical, "MessageId", &envelope.message_id);
            if let Some(subject) = &envelope.subject {
                push_pair(&mut canonical, "Subject", subject);
            }
            push_pair(&mut canonical, "Timestamp", &envelope.timestamp);
            push_pair(&mut canonical, "TopicArn", &envelope.topic_arn);
        }
        MessageType::SubscriptionConfirmation | MessageType::UnsubscribeConfirmation => {
            let subscribe_url = envelope
                .subscribe_url
                .as_deref()
                .ok_or(VerifyError::MissingField("SubscribeURL"))?;
            let token = envelope
                .token
                .as_deref()
                .ok_or(VerifyError::MissingField("Token"))?;
            push_pair(&mut canonical, "Message", &envelope.message);
            push_pair(&mut canonical, "MessageId", &envelope.message_id);
            push_pair(&mut canonical, "SubscribeURL", subscribe_url);
            push_pair(&mut canonical, "Timestamp", &envelope.timestamp);
            push_pair(&mut canonical, "Token", token);
            push_pair(&mut canonical, "TopicArn", &envelope.topic_arn);
        }
    }
    push_pair(&mut canonical, "Type", envelope.message_type.as_str());
    Ok(canonical)
}

fn push_pair(canonical: &mut String, name: &str, value: &str) {
    canonical.push_str(name);
    canonical.push('\n');
    canonical.push_str(value);
    canonical.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn envelope(message_type: MessageType) -> SnsEnvelope {
        SnsEnvelope {
            message_type,
            message_id: "mid".into(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:t".into(),
            message: "hello".into(),
            timestamp: "2026-08-03T00:00:00.000Z".into(),
            signature_version: "1".into(),
            signature: String::new(),
            signing_cert_url: String::new(),
            subject: None,
            subscribe_url: Some("https://sns.us-east-1.amazonaws.com/confirm".into()),
            token: Some("tok".into()),
            unsubscribe_url: None,
        }
    }

    #[test]
    fn notification_field_order() {
        let mut e = envelope(MessageType::Notification);
        e.subject = Some("subj".into());
        let s = build_string_to_sign(&e).unwrap();
        assert_eq!(
            s,
            "Message\nhello\nMessageId\nmid\nSubject\nsubj\nTimestamp\n\
             2026-08-03T00:00:00.000Z\nTopicArn\narn:aws:sns:us-east-1:123456789012:t\n\
             Type\nNotification\n"
        );
    }

    #[test]
    fn notification_without_subject_omits_it() {
        let s = build_string_to_sign(&envelope(MessageType::Notification)).unwrap();
        assert!(!s.contains("Subject"));
    }

    #[test]
    fn confirmation_field_order() {
        let s = build_string_to_sign(&envelope(MessageType::SubscriptionConfirmation)).unwrap();
        assert_eq!(
            s,
            "Message\nhello\nMessageId\nmid\nSubscribeURL\n\
             https://sns.us-east-1.amazonaws.com/confirm\nTimestamp\n\
             2026-08-03T00:00:00.000Z\nToken\ntok\nTopicArn\n\
             arn:aws:sns:us-east-1:123456789012:t\nType\nSubscriptionConfirmation\n"
        );
    }

    #[test]
    fn confirmation_missing_token_errors() {
        let mut e = envelope(MessageType::UnsubscribeConfirmation);
        e.token = None;
        assert!(matches!(
            build_string_to_sign(&e),
            Err(VerifyError::MissingField("Token"))
        ));
    }

    proptest! {
        #[test]
        fn never_panics_and_is_deterministic(
            message in ".*",
            message_id in ".*",
            subject in proptest::option::of(".*"),
            timestamp in ".*",
            topic_arn in ".*",
        ) {
            let mut e = envelope(MessageType::Notification);
            e.message = message;
            e.message_id = message_id;
            e.subject = subject;
            e.timestamp = timestamp;
            e.topic_arn = topic_arn;
            let first = build_string_to_sign(&e).unwrap();
            let second = build_string_to_sign(&e).unwrap();
            prop_assert_eq!(first, second);
        }
    }
}
