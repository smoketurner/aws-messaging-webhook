//! SES inbound email notifications (receipt rule → SNS), per
//! receiving-email-notifications-contents. The verdicts drive quarantine
//! classification — nothing is dropped, and the full payload is forwarded.

use serde::{Deserialize, Serialize};

use super::ses_notification::{SesCommonHeaders, SesMail};

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
    /// The receipt-rule action. On the recommended path this is the S3 action,
    /// whose pointer tells a consumer where the raw MIME landed.
    #[serde(default)]
    pub action: Option<SesReceiptAction>,
}

/// The receipt-rule action. Only the S3 fields are typed — they carry the
/// pointer to the stored message; every other action type deserializes with
/// them absent and is still forwarded verbatim in the raw payload.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SesReceiptAction {
    /// `S3` | `SNS` | `Lambda` | … — kept as a string, SES may add values.
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    /// S3 action only: the destination bucket.
    #[serde(default)]
    pub bucket_name: Option<String>,
    /// S3 action only: the stored object key (prefix already applied by SES).
    #[serde(default)]
    pub object_key: Option<String>,
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

    /// The S3 pointer to the stored message, when the receipt rule used the S3
    /// action and SES populated both the bucket and key. `None` for any other
    /// action or an incomplete pointer.
    #[must_use]
    pub fn s3_pointer(&self) -> Option<(&str, &str)> {
        let action = self.action.as_ref()?;
        match (action.bucket_name.as_deref(), action.object_key.as_deref()) {
            (Some(bucket), Some(key)) => Some((bucket, key)),
            _ => None,
        }
    }
}

/// The `meta.inbound` summary: parsed headers and auth verdicts lifted out of
/// an inbound receipt so consumers can route without fetching from S3. Built
/// by borrowing from the parsed structs and serialized with serde —
/// `skip_serializing_if` omits absent sub-fields rather than emitting null.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundSummary<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<&'a SesCommonHeaders>,
    #[serde(skip_serializing_if = "AuthSummary::is_empty")]
    auth: AuthSummary<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthSummary<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    spf: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dkim: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dmarc: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    spam: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    virus: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dmarc_policy: Option<&'a str>,
}

impl AuthSummary<'_> {
    fn is_empty(&self) -> bool {
        self.spf.is_none()
            && self.dkim.is_none()
            && self.dmarc.is_none()
            && self.spam.is_none()
            && self.virus.is_none()
            && self.dmarc_policy.is_none()
    }
}

impl<'a> InboundSummary<'a> {
    #[must_use]
    pub fn from_receipt(mail: &'a SesMail, receipt: &'a SesReceipt) -> Self {
        let status = |verdict: &'a Option<Verdict>| verdict.as_ref().map(|v| v.status.as_str());
        Self {
            headers: mail.common_headers.as_ref(),
            auth: AuthSummary {
                spf: status(&receipt.spf_verdict),
                dkim: status(&receipt.dkim_verdict),
                dmarc: status(&receipt.dmarc_verdict),
                spam: status(&receipt.spam_verdict),
                virus: status(&receipt.virus_verdict),
                dmarc_policy: receipt.dmarc_policy.as_deref(),
            },
        }
    }

    /// True when neither headers nor any verdict is present, so the whole
    /// summary should be omitted from `meta`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.headers.is_none() && self.auth.is_empty()
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

    const S3_ACTION: &str = r#"{
      "notificationType":"Received",
      "receipt":{
        "timestamp":"2015-09-11T20:32:33.936Z",
        "recipients":["recipient@example.com"],
        "spamVerdict":{"status":"PASS"},
        "virusVerdict":{"status":"PASS"},
        "spfVerdict":{"status":"PASS"},
        "dkimVerdict":{"status":"GRAY"},
        "dmarcVerdict":{"status":"FAIL"},
        "dmarcPolicy":"reject",
        "action":{"type":"S3","topicArn":"arn:aws:sns:us-east-1:012345678912:t",
                  "bucketName":"inbound-mail","objectKey":"prefix/d6iitobk75ur44p8kdnnp7g2n800"}
      },
      "mail":{"timestamp":"2015-09-11T20:32:33.936Z","messageId":"d6iitobk75ur44p8kdnnp7g2n800",
              "source":"sender@example.com","destination":["recipient@example.com"],
              "commonHeaders":{"from":["Sender <sender@example.com>"],
                               "to":["recipient@example.com"],"subject":"Hello",
                               "date":"Fri, 11 Sep 2015 20:32:33 +0000",
                               "messageId":"<abc@mail.example.com>"}}
    }"#;

    #[test]
    fn parses_s3_action_pointer() {
        let event: SesInboundNotification = serde_json::from_str(S3_ACTION).unwrap();
        assert_eq!(
            event.receipt.s3_pointer(),
            Some(("inbound-mail", "prefix/d6iitobk75ur44p8kdnnp7g2n800"))
        );
        let action = event.receipt.action.unwrap();
        assert_eq!(action.kind.as_deref(), Some("S3"));
    }

    #[test]
    fn parses_common_headers_and_verdicts() {
        let event: SesInboundNotification = serde_json::from_str(S3_ACTION).unwrap();
        let headers = event.mail.common_headers.unwrap();
        assert_eq!(headers.subject.as_deref(), Some("Hello"));
        assert_eq!(headers.from, vec!["Sender <sender@example.com>"]);
        assert_eq!(
            headers.message_id.as_deref(),
            Some("<abc@mail.example.com>")
        );
        assert_eq!(event.receipt.dmarc_verdict.as_ref().unwrap().status, "FAIL");
        assert_eq!(event.receipt.dmarc_policy.as_deref(), Some("reject"));
    }

    #[test]
    fn sns_action_has_no_s3_pointer() {
        // The classic SNS-delivery receipt carries no S3 pointer.
        let event: SesInboundNotification = serde_json::from_str(CLEAN).unwrap();
        assert_eq!(event.receipt.s3_pointer(), None);
    }

    #[test]
    fn partial_s3_action_yields_no_pointer() {
        // A bucket without a key (or vice versa) is not a usable pointer.
        let raw = r#"{"notificationType":"Received","receipt":{
            "action":{"type":"S3","bucketName":"b"}},"mail":{"messageId":"m"}}"#;
        let event: SesInboundNotification = serde_json::from_str(raw).unwrap();
        assert_eq!(event.receipt.s3_pointer(), None);
    }
}
