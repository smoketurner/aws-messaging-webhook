//! The normalized EventBridge detail and the trait that publishes it. The
//! stream relay ([`crate::stream`]) is the sole publisher of event-item
//! details; the request path only publishes control-plane notices
//! (`subscription.changed`).

use std::future::Future;

use serde_json::{Value, json};

use crate::model::{DomainEvent, Source};
use crate::store::EventRecord;

/// Schema version stamped on every EventBridge detail this service emits, so
/// downstream consumers have a stable field to switch on as the contract
/// evolves. Bump only on a breaking change to the emitted detail shape.
pub const SCHEMA_VERSION: u32 = 1;

/// `PutEvents` caps an entry at 256 KB; leave headroom for the envelope.
const MAX_DETAIL_BYTES: usize = 250_000;

/// The hard `PutEvents` limit on a single *entry* (256 KiB = 262,144 bytes).
/// `PutEvents` sizes the whole entry — the sum of `Detail` + `DetailType` +
/// `Source` (plus `Time`/`Resources` if set), per the [API reference][1] — not
/// `Detail` alone, so any reduction threshold must reserve room for the
/// envelope. [`MAX_DETAIL_BYTES`] starts reduction below this boundary; Step 3
/// drops `meta.inbound` when reducing `event` alone can't keep the detail under
/// this cap *minus the actual envelope* (e.g. an attacker-controlled
/// `commonHeaders.subject` duplicated into `meta.inbound.headers` exceeds it on
/// its own).
///
/// `pub(crate)`: [`crate::mail::events::cap_mail_detail`] reserves the same
/// envelope headroom for the mail-table relay's `message.received*` events.
///
/// [1]: https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEventsRequestEntry.html
pub(crate) const PUT_EVENTS_ENTRY_CAP_BYTES: usize = 262_144;

/// One event ready for `PutEvents`.
#[derive(Debug, Clone)]
pub struct OutboundEvent {
    pub detail_type: String,
    pub detail: Value,
}

#[derive(Debug, thiserror::Error)]
#[error("event publish failed")]
pub struct PublishError(#[from] pub anyhow::Error);

pub trait PublishEvents: Send + Sync {
    fn publish(
        &self,
        event: &OutboundEvent,
    ) -> impl Future<Output = Result<(), PublishError>> + Send;
}

/// The would-be `Detail` byte length of `detail` if `PutEvents` serialized it.
/// `pub(crate)` so [`crate::mail::events::cap_mail_detail`] can reuse the same
/// measurement for the mail-table relay's size-reduction ladder.
pub(crate) fn detail_bytes(detail: &Value) -> usize {
    serde_json::to_vec(detail).map_or(usize::MAX, |bytes| bytes.len())
}

/// Builds the EventBridge detail for a persisted event, guaranteeing it stays
/// under the `PutEvents` entry cap so an oversized payload can never become a
/// poison record. Oversized events are reduced in up to three steps — raw MIME
/// is stripped, then `event` is replaced with a pointer, then `meta.inbound`
/// is dropped if the duplicated parsed headers alone still exceed the cap; the
/// full payload always remains in the DynamoDB raw record.
///
/// `event_source` is the configured `Source` field stamped on every
/// `PutEventsRequestEntry` ([`AwsServices`](crate::aws::AwsServices) sets it
/// from `Config::event_source`). It counts toward the same 256 KiB entry cap
/// as `Detail` and `DetailType`, so Step 3 reserves it as envelope headroom.
#[must_use]
pub fn build_outbound(
    record: &EventRecord,
    event: &DomainEvent,
    event_source: &str,
) -> OutboundEvent {
    let mut meta = json!({
        "snsMessageId": record.sns_message_id,
        "messageId": record.aggregate_id,
        "topicArn": record.topic_arn,
        "receivedAt": record.received_at,
        // The classified family's canonical path; null for unknown events.
        // A label for consumers, not the literal arrival path — a direct
        // SNS → Lambda delivery never had one.
        "webhookPath": record.source.map(Source::webhook_path),
    });
    // Conversation threading: if the inbound event is a reply to a previously
    // sent message, surface the correlation id so consumers can link the two
    // without parsing the event payload.
    if let Some(prev) = event.previous_message_id() {
        meta["previousMessageId"] = Value::String(prev.to_owned());
    }
    // Inbound email: surface the S3 pointer to the stored raw MIME so a
    // consumer can GetObject it without parsing the payload — the point of
    // the recommended SES → S3 receipt path. Absent for non-S3 receipts.
    if let Some((bucket, key)) = event.s3_pointer() {
        meta["s3"] = json!({ "bucket": bucket, "key": key });
    }
    // Inbound email: parsed headers + auth verdicts, so consumers can route on
    // subject/from/DMARC without fetching from S3. Absent for other families.
    if let Some(inbound) = event.inbound_meta() {
        meta["inbound"] = inbound;
    }
    let mut detail =
        json!({ "schemaVersion": SCHEMA_VERSION, "meta": meta, "event": event.payload() });

    if detail_bytes(&detail) <= MAX_DETAIL_BYTES {
        return OutboundEvent {
            detail_type: record.detail_type.clone(),
            detail,
        };
    }

    // Step 1: drop embedded raw MIME (SES inbound `content`), the usual cause.
    if let Some(content) = detail
        .get_mut("event")
        .and_then(|event| event.get_mut("content"))
        .filter(|content| !content.is_null())
    {
        *content = Value::Null;
        tracing::warn!(
            sns_message_id = record.sns_message_id,
            event = "content_stripped",
            "stripped oversized inbound content from the EventBridge event"
        );
    }

    // Step 2: if still over the cap, replace the payload with a pointer so the
    // bus event stays publishable. Consumers fetch the full record from
    // DynamoDB by meta.messageId + meta.snsMessageId.
    if detail_bytes(&detail) > MAX_DETAIL_BYTES {
        detail["event"] = json!({
            "payloadOmitted": true,
            "reason": "event payload exceeds the EventBridge entry size limit",
        });
        tracing::warn!(
            sns_message_id = record.sns_message_id,
            event = "payload_omitted",
            "event payload too large for EventBridge; published a pointer only"
        );
    }

    // Step 3: hard-cap safety net. Step 2 buys headroom by replacing `event`
    // with a pointer, but `meta.inbound.headers` (parsed `commonHeaders`) is
    // copied verbatim and never reduced — an attacker-controlled ~261 KiB
    // `subject` alone can keep the detail over the `PutEvents` 256 KiB cap
    // even after the payload is dropped. Drop the inbound summary so the bus
    // event stays publishable; `detail_type` still signals any quarantine
    // routing, `meta.s3` still locates the raw MIME, and the full record
    // (headers + verdicts) remains in DynamoDB. A published event without
    // routing metadata is strictly better than a poison record that reaches
    // no consumer, and this path only fires on the oversized edge case.
    //
    // `PutEvents` counts the whole entry — `Detail` + `DetailType` + `Source`
    // — toward the 256 KiB cap, not `Detail` alone. Compare `detail` against
    // the cap *minus the actual envelope* (`detail_type` + the configured
    // `source`), or a `detail` in the just-under-the-cap window yields an
    // entry just over it that AWS rejects (the bug fixed here). `saturating_sub`
    // keeps an absurdly large `source` from underflowing into a huge threshold.
    let envelope = record.detail_type.len() + event_source.len();
    if detail_bytes(&detail) > PUT_EVENTS_ENTRY_CAP_BYTES.saturating_sub(envelope)
        && let Some(meta) = detail.get_mut("meta").and_then(Value::as_object_mut)
    {
        meta.remove("inbound");
        tracing::warn!(
            sns_message_id = record.sns_message_id,
            event = "inbound_meta_dropped",
            "dropped meta.inbound to keep the EventBridge entry under the PutEvents cap"
        );
    }

    OutboundEvent {
        detail_type: record.detail_type.clone(),
        detail,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use proptest::prelude::*;

    use super::*;
    use crate::model::{DomainEvent, Source};
    use crate::store::EventRecord;

    /// The default `Config::event_source`, mirroring `config.rs`'s fallback so
    /// the entry-size assertions model the envelope `AwsServices::publish`
    /// actually stamps on the `PutEventsRequestEntry`.
    const DEFAULT_EVENT_SOURCE: &str = "aws-messaging-webhook";

    /// `PutEvents` entry size = `detail` + `detail_type` + `source` (the three
    /// fields `AwsServices::publish` always sets on the builder), mirroring the
    /// 256 KiB *entry* cap rather than the `detail`-only size the old tests used.
    fn entry_bytes(detail: &Value, detail_type: &str, source: &str) -> usize {
        detail_bytes(detail) + detail_type.len() + source.len()
    }

    fn record(source: Option<Source>, detail_type: &str) -> EventRecord {
        EventRecord {
            aggregate_id: "agg-1".to_owned(),
            event_timestamp: "2026-08-03T19:12:52.000Z".to_owned(),
            sns_message_id: "sns-1".to_owned(),
            raw_body: Bytes::from_static(b"{}"),
            source,
            detail_type: detail_type.to_owned(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:t".to_owned(),
            received_at: "2026-08-03T19:12:53.000Z".to_owned(),
            expires_at: 1_800_000_000,
            aggregate_expires_at: 1_900_000_000,
        }
    }

    /// A DynamoDB-stream record with production-length meta fields (UUID SNS
    /// message id, 28-char SES message id, full topic ARN), used by the
    /// cap-bounding tests to reproduce the SNS-conformant oversize window where
    /// `meta.inbound` alone can exceed the `PutEvents` cap.
    fn realistic_inbound_record() -> EventRecord {
        EventRecord {
            aggregate_id: "d6iitobk75ur44p8kdnnp7g2n800".to_owned(),
            event_timestamp: "2026-08-03T19:12:52.000Z".to_owned(),
            sns_message_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_owned(),
            raw_body: Bytes::from_static(b"{}"),
            source: Some(Source::SesInbound),
            detail_type: "ses.inbound".to_owned(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:my-ses-inbound-topic".to_owned(),
            received_at: "2026-08-03T19:12:53.456Z".to_owned(),
            expires_at: 1_800_000_000,
            aggregate_expires_at: 1_900_000_000,
        }
    }

    /// Builds an SES inbound notification (S3 action, no `headers` array) whose
    /// `commonHeaders.subject` is sized so the JSON is exactly 262,144 bytes —
    /// the SNS `Message` cap. The subject fills the slack left by the fixed
    /// overhead, computed by measuring the empty-subject template.
    fn sns_conformant_inbound_with_max_subject() -> String {
        let mut notification = json!({
            "notificationType": "Received",
            "mail": {
                "messageId": "d6iitobk75ur44p8kdnnp7g2n800",
                "commonHeaders": {"subject": ""}
            },
            "receipt": {
                "action": {
                    "type": "S3",
                    "bucketName": "prod-inbound-mail",
                    "objectKey": "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
                },
                "spfVerdict": {"status": "PASS"},
                "dkimVerdict": {"status": "PASS"},
                "dmarcVerdict": {"status": "FAIL"},
                "dmarcPolicy": "reject",
                "virusVerdict": {"status": "PASS"},
                "spamVerdict": {"status": "PASS"}
            }
        });
        let overhead = notification.to_string().len();
        notification["mail"]["commonHeaders"]["subject"] = json!("x".repeat(262_144 - overhead));
        notification.to_string()
    }

    #[test]
    fn stamps_schema_version_meta_and_webhook_path() {
        let event = DomainEvent::classify(r#"{"eventType":"Open","mail":{"messageId":"m-1"}}"#);
        let out = build_outbound(
            &record(Some(Source::SesEvents), "ses.open"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail_type, "ses.open");
        assert_eq!(out.detail["schemaVersion"], json!(SCHEMA_VERSION));
        let meta = &out.detail["meta"];
        assert_eq!(meta["snsMessageId"], "sns-1");
        assert_eq!(meta["messageId"], "agg-1");
        assert_eq!(meta["topicArn"], "arn:aws:sns:us-east-1:123456789012:t");
        assert_eq!(meta["webhookPath"], "/webhooks/ses/events");
        assert_eq!(out.detail["event"]["eventType"], "Open");
    }

    #[test]
    fn unknown_family_has_null_webhook_path() {
        let event = DomainEvent::classify("plain text");
        let out = build_outbound(&record(None, "unknown"), &event, DEFAULT_EVENT_SOURCE);
        assert_eq!(out.detail["meta"]["webhookPath"], Value::Null);
    }

    #[test]
    fn oversized_inbound_content_is_stripped() {
        let big = "x".repeat(300_000);
        let message = json!({
            "notificationType": "Received",
            "receipt": {
                "spamVerdict": {"status": "PASS"},
                "virusVerdict": {"status": "PASS"},
                "action": {"type": "SNS"}
            },
            "mail": {"messageId": "in-1"},
            "content": big
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesInbound), "ses.inbound"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail["event"]["content"], Value::Null);
        assert_eq!(out.detail["event"]["mail"]["messageId"], "in-1");
        // The reduced entry (detail + detail_type + source) must fit under the
        // PutEvents entry cap, not just change shape — the guarantee that keeps
        // oversized records off the DLQ.
        assert!(
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
                <= PUT_EVENTS_ENTRY_CAP_BYTES
        );
    }

    #[test]
    fn oversized_payload_without_content_becomes_a_pointer() {
        let big = "x".repeat(300_000);
        let message = json!({
            "eventType": "Bounce",
            "bounce": {"bounceType": "Transient", "bouncedRecipients": [], "note": big},
            "mail": {"messageId": "huge-1"}
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesEvents), "ses.bounce"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail["event"]["payloadOmitted"], json!(true));
        // Meta is always preserved so consumers can fetch the full record.
        assert_eq!(out.detail["meta"]["messageId"], "agg-1");
        // The reduced entry (detail + detail_type + source) must fit under the
        // PutEvents 256 KiB entry cap.
        assert!(
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
                <= PUT_EVENTS_ENTRY_CAP_BYTES
        );
    }

    #[test]
    fn sms_inbound_reply_includes_previous_message_id_in_meta() {
        let message = json!({
            "originationNumber": "+14255550182",
            "destinationNumber": "+12125550101",
            "messageKeyword": "REPLY",
            "messageBody": "Got it, thanks",
            "inboundMessageId": "cae173d2-66b9-564c-8309-21f858e9fb84",
            "previousPublishedMessageId": "outbound-msg-001"
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SmsInbound), "sms.inbound"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail["meta"]["previousMessageId"], "outbound-msg-001");
    }

    #[test]
    fn sms_inbound_without_previous_message_has_no_previous_in_meta() {
        let message = json!({
            "originationNumber": "+14255550182",
            "inboundMessageId": "abc-123"
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SmsInbound), "sms.inbound"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        // previousMessageId should be absent (not null) when there's no prior message.
        assert!(out.detail["meta"].get("previousMessageId").is_none());
    }

    #[test]
    fn non_sms_events_have_no_previous_message_id_in_meta() {
        let message = json!({
            "eventType": "Delivery",
            "mail": {"messageId": "m-1"}
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesEvents), "ses.delivery"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert!(out.detail["meta"].get("previousMessageId").is_none());
    }

    #[test]
    fn inbound_s3_action_surfaces_pointer_and_headers_in_meta() {
        let message = json!({
            "notificationType": "Received",
            "mail": {
                "messageId": "in-9",
                "commonHeaders": {"from": ["a@b.c"], "subject": "Invoice"}
            },
            "receipt": {
                "spfVerdict": {"status": "PASS"},
                "dmarcVerdict": {"status": "PASS"},
                "action": {"type": "S3", "bucketName": "inbound-mail", "objectKey": "p/in-9"}
            }
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesInbound), "ses.inbound"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail["meta"]["s3"]["bucket"], "inbound-mail");
        assert_eq!(out.detail["meta"]["s3"]["key"], "p/in-9");
        assert_eq!(
            out.detail["meta"]["inbound"]["headers"]["subject"],
            "Invoice"
        );
        assert_eq!(out.detail["meta"]["inbound"]["auth"]["spf"], "PASS");
    }

    #[test]
    fn non_inbound_events_have_no_s3_or_inbound_meta() {
        let message = json!({"eventType": "Open", "mail": {"messageId": "m-1"}}).to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesEvents), "ses.open"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert!(out.detail["meta"].get("s3").is_none());
        assert!(out.detail["meta"].get("inbound").is_none());
    }

    #[test]
    fn oversized_inbound_keeps_s3_pointer_after_content_stripped() {
        // The whole point of the S3 path: even when the raw MIME is too big
        // and gets stripped, the pointer to where it lives stays in meta.
        let big = "x".repeat(300_000);
        let message = json!({
            "notificationType": "Received",
            "mail": {"messageId": "big-in", "commonHeaders": {"subject": "Big"}},
            "receipt": {
                "action": {"type": "S3", "bucketName": "b", "objectKey": "k"}
            },
            "content": big
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(
            &record(Some(Source::SesInbound), "ses.inbound"),
            &event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(out.detail["event"]["content"], Value::Null);
        assert_eq!(out.detail["meta"]["s3"]["bucket"], "b");
        assert_eq!(out.detail["meta"]["s3"]["key"], "k");
        assert_eq!(out.detail["meta"]["inbound"]["headers"]["subject"], "Big");
        // The reduced entry (detail + detail_type + source) must fit under the
        // PutEvents 256 KiB entry cap.
        assert!(
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
                <= PUT_EVENTS_ENTRY_CAP_BYTES
        );
    }

    /// Regression for the `meta.inbound` size-bounding bug: an SNS-conformant
    /// inbound receipt (Message exactly 262,144 bytes) with an
    /// attacker-controlled `commonHeaders.subject` near the SNS cap yields an
    /// EventBridge detail that exceeds the `PutEvents` 256 KiB entry cap *after*
    /// Step 2 reduces `event` to a pointer — because `meta.inbound.headers`
    /// duplicates the subject verbatim and is never bounded by Step 1/2. Step 3
    /// drops `meta.inbound` so the entry (detail + `detail_type` + `source`)
    /// stays publishable.
    #[test]
    fn sns_conformant_oversized_subject_fits_under_putevents_cap() {
        let message = sns_conformant_inbound_with_max_subject();
        assert_eq!(
            message.len(),
            262_144,
            "notification is exactly at the SNS cap"
        );

        let event = DomainEvent::classify(&message);
        let out = build_outbound(&realistic_inbound_record(), &event, DEFAULT_EVENT_SOURCE);

        // The fix's guarantee: the whole entry (detail + detail_type + source)
        // fits under the PutEvents 256 KiB entry cap.
        assert!(
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
                <= PUT_EVENTS_ENTRY_CAP_BYTES,
            "entry is {} bytes; exceeds the PutEvents 262,144-byte entry cap",
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
        );
        // The bug scenario is real: had Step 3 NOT dropped `meta.inbound`, the
        // post-Step-2 entry would still exceed the cap. Re-insert the inbound
        // summary (the same one build_outbound built) and assert that.
        let mut without_step3 = out.detail.clone();
        without_step3["meta"]["inbound"] = event
            .inbound_meta()
            .expect("inbound meta present before Step 3");
        assert!(
            entry_bytes(&without_step3, &out.detail_type, DEFAULT_EVENT_SOURCE)
                > PUT_EVENTS_ENTRY_CAP_BYTES,
            "post-Step-2 entry is {} bytes; bug scenario not reproduced",
            entry_bytes(&without_step3, &out.detail_type, DEFAULT_EVENT_SOURCE)
        );
        // Step 3 dropped `meta.inbound`; Step 2 replaced `event` with a pointer.
        assert!(out.detail["meta"].get("inbound").is_none());
        assert_eq!(out.detail["event"]["payloadOmitted"], json!(true));
        // The S3 pointer survives so a consumer can still GetObject the raw MIME,
        // and the detail_type still routes the receipt (DMARC FAIL here).
        assert_eq!(out.detail_type, "ses.inbound");
        assert_eq!(out.detail["meta"]["s3"]["bucket"], "prod-inbound-mail");
        assert_eq!(
            out.detail["meta"]["s3"]["key"],
            "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
        );
    }

    /// Step 3 only drops `meta.inbound` when the *entry* (detail plus
    /// `detail_type` plus `source`) would exceed the `PutEvents` cap; if Step
    /// 2's pointer already brings it under the cap, the routing metadata
    /// (headers and auth) is preserved so consumers can route without fetching
    /// from S3.
    #[test]
    fn inbound_meta_preserved_when_reduction_fits_under_cap() {
        // A subject large enough to trigger Step 2 (the duplicated headers push
        // the pre-Step-2 detail past MAX_DETAIL_BYTES) but small enough that the
        // post-Step-2 detail still fits under the 256 KiB cap.
        let subject = "x".repeat(200_000);
        let message = json!({
            "notificationType": "Received",
            "mail": {
                "messageId": "d6iitobk75ur44p8kdnnp7g2n800",
                "commonHeaders": {"subject": subject}
            },
            "receipt": {
                "action": {
                    "type": "S3",
                    "bucketName": "prod-inbound-mail",
                    "objectKey": "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
                },
                "spfVerdict": {"status": "PASS"},
                "dmarcVerdict": {"status": "FAIL"},
                "dmarcPolicy": "reject"
            }
        })
        .to_string();
        let event = DomainEvent::classify(&message);
        let out = build_outbound(&realistic_inbound_record(), &event, DEFAULT_EVENT_SOURCE);
        assert_eq!(out.detail["event"]["payloadOmitted"], json!(true));
        assert!(
            entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE)
                <= PUT_EVENTS_ENTRY_CAP_BYTES
        );
        // Step 3 did NOT fire: the full routing metadata survives.
        assert_eq!(
            out.detail["meta"]["inbound"]["headers"]["subject"],
            "x".repeat(200_000)
        );
        assert_eq!(out.detail["meta"]["inbound"]["auth"]["dmarc"], "FAIL");
        assert_eq!(out.detail["meta"]["s3"]["bucket"], "prod-inbound-mail");
    }

    /// Regression for the just-under-the-cap window: an SNS-conformant inbound
    /// receipt (Message under the 262,144-byte SNS cap) whose post-Step-2
    /// `detail` lands in `(cap - |source| - |detail_type|, cap]`. The old Step 3
    /// compared `detail` alone to the raw entry cap, so it skipped this window
    /// and published an entry (`detail` plus `detail_type` plus `source`) one
    /// byte over the cap — AWS rejected it, the record retried to exhaustion,
    /// and it landed in the DLQ. The fix reserves the actual envelope, so Step 3
    /// fires and the entry fits.
    #[test]
    fn step3_reserves_envelope_headroom_in_just_under_cap_window() {
        // After Step 2 replaces `event` with a pointer, the subject survives in
        // the detail only once (`meta.inbound.headers.subject`), so the
        // post-Step-2, pre-Step-3 detail grows exactly 1 byte per subject char.
        // Measure that fixed overhead with a probe subject long enough to trip
        // Step 2 (and short enough that Step 3 still skips), then pick the
        // subject length that lands the detail at `cap - |envelope| + 1` — the
        // smallest value in the bug window, where the *detail* is at/under the
        // raw cap (old Step 3 skips) but the *entry* exceeds it by exactly 1.
        let notice = |subject: String| {
            json!({
                "notificationType": "Received",
                "mail": {
                    "messageId": "d6iitobk75ur44p8kdnnp7g2n800",
                    "commonHeaders": {"subject": subject}
                },
                "receipt": {
                    "action": {
                        "type": "S3",
                        "bucketName": "prod-inbound-mail",
                        "objectKey": "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
                    },
                    "spfVerdict": {"status": "PASS"},
                    "dmarcVerdict": {"status": "FAIL"},
                    "dmarcPolicy": "reject"
                }
            })
            .to_string()
        };

        // Probe: 200 KB subject trips Step 2 (the subject is duplicated in the
        // pre-Step-2 event payload) but the ~200 KB post-Step-2 detail is well
        // under the cap, so Step 3 skips and `out.detail` keeps `meta.inbound`.
        let probe_subject_len = 200_000;
        let probe_event = DomainEvent::classify(&notice("x".repeat(probe_subject_len)));
        let probe_out = build_outbound(
            &realistic_inbound_record(),
            &probe_event,
            DEFAULT_EVENT_SOURCE,
        );
        assert_eq!(
            probe_out.detail["event"]["payloadOmitted"],
            json!(true),
            "probe must trip Step 2"
        );
        let probe_detail = detail_bytes(&probe_out.detail);
        assert!(
            probe_detail <= PUT_EVENTS_ENTRY_CAP_BYTES,
            "probe must be small enough that Step 3 skips"
        );
        // Fixed (non-subject) bytes of the post-Step-2, pre-Step-3 detail.
        let fixed_overhead = probe_detail - probe_subject_len;
        let envelope = probe_out.detail_type.len() + DEFAULT_EVENT_SOURCE.len();
        // Smallest subject length inside the bug window: detail = cap - envelope + 1.
        let cap_minus_envelope = PUT_EVENTS_ENTRY_CAP_BYTES.saturating_sub(envelope);
        let target_subject_len = (cap_minus_envelope + 1).saturating_sub(fixed_overhead);

        // Build the window notification and run `build_outbound` with the fix.
        let message = notice("x".repeat(target_subject_len));
        let event = DomainEvent::classify(&message);
        let out = build_outbound(&realistic_inbound_record(), &event, DEFAULT_EVENT_SOURCE);

        // Reachable from a valid SNS notification under the 262,144-byte cap.
        assert!(
            message.len() <= 262_144,
            "window notification must fit the SNS Message cap; len={}",
            message.len()
        );
        assert_eq!(
            out.detail["event"]["payloadOmitted"],
            json!(true),
            "Step 2 fired"
        );

        // Reconstruct the pre-Step-3 detail the buggy code measured, with the
        // actual (huge-subject) inbound summary still in place.
        let mut pre_step3 = out.detail.clone();
        pre_step3["meta"]["inbound"] = event
            .inbound_meta()
            .expect("ses.inbound carries meta.inbound");
        let pre_detail = detail_bytes(&pre_step3);
        let pre_entry = entry_bytes(&pre_step3, &out.detail_type, DEFAULT_EVENT_SOURCE);

        // The window is real: the *detail* alone is at/under the raw cap, so the
        // old Step 3 (`detail > cap`) would have skipped it.
        assert!(
            pre_detail <= PUT_EVENTS_ENTRY_CAP_BYTES,
            "old Step 3 would have fired on detail {pre_detail}, not the bug window"
        );
        // ...yet the pre-Step-3 *entry* exceeds the cap by exactly 1 — the bug.
        assert_eq!(
            pre_entry,
            PUT_EVENTS_ENTRY_CAP_BYTES + 1,
            "pre-Step-3 entry should exceed the cap by exactly 1"
        );

        // The fix's guarantee: the *published* entry (Step 3 dropped inbound) fits.
        let published_entry = entry_bytes(&out.detail, &out.detail_type, DEFAULT_EVENT_SOURCE);
        assert!(
            published_entry <= PUT_EVENTS_ENTRY_CAP_BYTES,
            "published entry {published_entry} exceeds the PutEvents cap"
        );
        // Step 3 fired precisely because the envelope was reserved.
        assert!(
            out.detail["meta"].get("inbound").is_none(),
            "Step 3 dropped meta.inbound"
        );
        // The S3 pointer survives so a consumer can still GetObject the raw MIME.
        assert_eq!(out.detail_type, "ses.inbound");
        assert_eq!(out.detail["meta"]["s3"]["bucket"], "prod-inbound-mail");
        assert_eq!(
            out.detail["meta"]["s3"]["key"],
            "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
        );
    }

    proptest! {
        /// For any attacker-controlled subject length (including beyond the SNS
        /// cap) and any configured `EVENT_SOURCE` up to ~200 bytes longer than
        /// the default, `build_outbound` must keep the EventBridge *entry*
        /// (detail + `detail_type` + `source`) under the `PutEvents` 256 KiB
        /// entry cap — the guarantee that prevents a poison record. Exercises
        /// the Step 2 / Step 3 boundary across the whole subject range and the
        /// envelope-sensitivity the fix reserves headroom for.
        #[test]
        fn build_outbound_never_exceeds_putevents_entry_cap(
            subject_len in 0usize..270_000,
            source_extra in 0usize..200,
        ) {
            let source = format!("{DEFAULT_EVENT_SOURCE}{}", "s".repeat(source_extra));
            let subject = "x".repeat(subject_len);
            let message = json!({
                "notificationType": "Received",
                "mail": {
                    "messageId": "d6iitobk75ur44p8kdnnp7g2n800",
                    "commonHeaders": {"subject": subject}
                },
                "receipt": {
                    "action": {
                        "type": "S3",
                        "bucketName": "prod-inbound-mail",
                        "objectKey": "ses-receipts/d6iitobk75ur44p8kdnnp7g2n800"
                    },
                    "spfVerdict": {"status": "PASS"},
                    "dmarcVerdict": {"status": "FAIL"},
                    "dmarcPolicy": "reject"
                }
            })
            .to_string();
            let event = DomainEvent::classify(&message);
            let out = build_outbound(&realistic_inbound_record(), &event, &source);
            prop_assert!(
                entry_bytes(&out.detail, &out.detail_type, &source) <= PUT_EVENTS_ENTRY_CAP_BYTES
            );
        }
    }
}
