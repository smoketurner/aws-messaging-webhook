//! SES inbound email notifications (receipt rule → SNS), per
//! receiving-email-notifications-contents. The verdicts drive quarantine
//! classification — nothing is dropped, and the full payload is forwarded.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesInboundNotification {
    /// Always `Received`.
    pub notification_type: String,
    pub mail: super::ses_notification::SesMail,
    pub receipt: SesReceipt,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesReceipt {
    #[serde(default)]
    pub spam_verdict: Option<Verdict>,
    #[serde(default)]
    pub virus_verdict: Option<Verdict>,
    #[serde(default)]
    pub spf_verdict: Option<Verdict>,
    #[serde(default)]
    pub dkim_verdict: Option<Verdict>,
    #[serde(default)]
    pub dmarc_verdict: Option<Verdict>,
    /// Present only when the message failed DMARC: `none` | `quarantine` |
    /// `reject`.
    #[serde(default)]
    pub dmarc_policy: Option<String>,
}

/// Verdict status: `PASS` | `FAIL` | `GRAY` | `PROCESSING_FAILED` — kept as a
/// string because the documented value set is not closed.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    pub status: String,
}

impl SesReceipt {
    /// Spam or virus verdict FAIL selects the quarantined detail-type.
    /// Classification only — the event still persists and publishes.
    #[must_use]
    pub fn is_quarantined(&self) -> bool {
        let failed =
            |verdict: &Option<Verdict>| verdict.as_ref().is_some_and(|v| v.status == "FAIL");
        failed(&self.spam_verdict) || failed(&self.virus_verdict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLEAN: &str = r#"{
      "notificationType":"Received",
      "receipt":{
        "timestamp":"2015-09-11T20:32:33.936Z",
        "processingTimeMillis":222,
        "recipients":["recipient@example.com"],
        "spamVerdict":{"status":"PASS"},
        "virusVerdict":{"status":"PASS"},
        "spfVerdict":{"status":"PASS"},
        "dkimVerdict":{"status":"PASS"},
        "action":{"type":"SNS","topicArn":"arn:aws:sns:us-east-1:012345678912:example-topic"}
      },
      "mail":{"timestamp":"2015-09-11T20:32:33.936Z","messageId":"d6iitobk75ur44p8kdnnp7g2n800",
              "source":"a@example.com","destination":["recipient@example.com"]},
      "content":"Return-Path: <a@example.com>\r\n\r\nExample content\r\n"
    }"#;

    #[test]
    fn parses_documented_sns_action_example() {
        let event: SesInboundNotification = serde_json::from_str(CLEAN).unwrap();
        assert_eq!(event.notification_type, "Received");
        assert_eq!(event.mail.message_id, "d6iitobk75ur44p8kdnnp7g2n800");
        assert!(!event.receipt.is_quarantined());
    }

    #[test]
    fn virus_fail_quarantines() {
        let raw = CLEAN.replace(
            r#""virusVerdict":{"status":"PASS"}"#,
            r#""virusVerdict":{"status":"FAIL"}"#,
        );
        let event: SesInboundNotification = serde_json::from_str(&raw).unwrap();
        assert!(event.receipt.is_quarantined());
    }

    #[test]
    fn gray_and_processing_failed_do_not_quarantine() {
        let raw = CLEAN
            .replace(
                r#""spamVerdict":{"status":"PASS"}"#,
                r#""spamVerdict":{"status":"GRAY"}"#,
            )
            .replace(
                r#""virusVerdict":{"status":"PASS"}"#,
                r#""virusVerdict":{"status":"PROCESSING_FAILED"}"#,
            );
        let event: SesInboundNotification = serde_json::from_str(&raw).unwrap();
        assert!(!event.receipt.is_quarantined());
    }

    #[test]
    fn missing_verdicts_do_not_quarantine() {
        let event: SesInboundNotification = serde_json::from_str(
            r#"{"notificationType":"Received","receipt":{},"mail":{"messageId":"m1"}}"#,
        )
        .unwrap();
        assert!(!event.receipt.is_quarantined());
    }

    #[test]
    fn spf_dkim_dmarc_failures_alone_do_not_quarantine() {
        let raw = CLEAN
            .replace(
                r#""spfVerdict":{"status":"PASS"}"#,
                r#""spfVerdict":{"status":"FAIL"}"#,
            )
            .replace(
                r#""dkimVerdict":{"status":"PASS"}"#,
                r#""dkimVerdict":{"status":"FAIL"}"#,
            );
        let event: SesInboundNotification = serde_json::from_str(&raw).unwrap();
        assert!(!event.receipt.is_quarantined());
    }
}
