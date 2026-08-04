//! Production [`Services`](crate::state::Services) implementation wrapping
//! the AWS SDK clients.

use std::time::SystemTime;

use anyhow::{Context as _, anyhow};
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue, Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem, Update,
};
use aws_sdk_eventbridge::types::PutEventsRequestEntry;
use aws_sdk_pinpointsmsvoicev2::types::MessageFeedbackStatus;
use aws_sdk_sesv2::types::SuppressionListReason;
use aws_smithy_types::Blob;
use aws_smithy_types::date_time::{DateTime, Format};
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::actions::{ActionError, FeedbackStatus, SesApi, SmsVoiceApi, SuppressionReason};
use crate::config::Config;
use crate::model::DomainEvent;
use crate::model::ses_notification::SesBounce;
use crate::publish::{OutboundEvent, PublishError, PublishEvents};
use crate::store::{EventRecord, EventStore, PersistOutcome, StoreError};

pub struct AwsServices {
    dynamo: aws_sdk_dynamodb::Client,
    events: aws_sdk_eventbridge::Client,
    sms: aws_sdk_pinpointsmsvoicev2::Client,
    ses: aws_sdk_sesv2::Client,
    config: Config,
}

impl AwsServices {
    #[must_use]
    pub fn new(sdk_config: &aws_config::SdkConfig, config: Config) -> Self {
        Self {
            dynamo: aws_sdk_dynamodb::Client::new(sdk_config),
            events: aws_sdk_eventbridge::Client::new(sdk_config),
            sms: aws_sdk_pinpointsmsvoicev2::Client::new(sdk_config),
            ses: aws_sdk_sesv2::Client::new(sdk_config),
            config,
        }
    }
}

fn partition_key(record: &EventRecord) -> String {
    format!("MSG#{}", record.aggregate_id)
}

fn event_sort_key(record: &EventRecord) -> String {
    format!("EVT#{}#{}", record.event_timestamp, record.sns_message_id)
}

fn set_status(clauses: &mut Vec<&'static str>, status: &str, overwrite: bool) -> AttributeValue {
    clauses.push(if overwrite {
        "current_status = :status"
    } else {
        "current_status = if_not_exists(current_status, :status)"
    });
    AttributeValue::S(status.to_owned())
}

/// The aggregate projection applied atomically with the event put. The base
/// expression maintains first/last timestamps; per-event clauses materialize
/// the message's current state (status transitions, open/click counts).
fn aggregate_update(
    table_name: &str,
    record: &EventRecord,
    event: &DomainEvent,
) -> anyhow::Result<Update> {
    let mut set_clauses = vec![
        "#source = if_not_exists(#source, :source)",
        "first_event_at = if_not_exists(first_event_at, :ts)",
        "last_event_at = :ts",
        "expires_at = :expires",
    ];
    let mut add_clause = None;
    let mut builder = Update::builder()
        .table_name(table_name)
        .key("pk", AttributeValue::S(partition_key(record)))
        .key("sk", AttributeValue::S("AGG".to_owned()))
        .expression_attribute_names("#source", "source")
        .expression_attribute_values(":source", AttributeValue::S(record.source.as_str().into()))
        .expression_attribute_values(":ts", AttributeValue::S(record.event_timestamp.clone()))
        .expression_attribute_values(":expires", AttributeValue::N(record.expires_at.to_string()));

    let status_value = match event {
        DomainEvent::SmsInbound { .. } | DomainEvent::SesInbound { .. } => {
            Some(set_status(&mut set_clauses, "received", true))
        }
        DomainEvent::SmsDelivery { event, .. } => {
            if event.is_final {
                let status = if event.is_successful_delivery() {
                    "delivered"
                } else {
                    "failed"
                };
                Some(set_status(&mut set_clauses, status, true))
            } else {
                None
            }
        }
        DomainEvent::Ses { event, .. } => match event.kind.as_str() {
            // Send never overwrites a terminal status: events arrive out of
            // order under at-least-once delivery.
            "Send" => Some(set_status(&mut set_clauses, "sent", false)),
            "Delivery" => Some(set_status(&mut set_clauses, "delivered", true)),
            "Bounce" => {
                let permanent = event.bounce.as_ref().is_some_and(SesBounce::is_permanent);
                if let Some(bounce) = &event.bounce {
                    set_clauses.push("bounce_type = :bounce_type");
                    builder = builder.expression_attribute_values(
                        ":bounce_type",
                        AttributeValue::S(bounce.bounce_type.clone()),
                    );
                }
                // Only a permanent bounce is terminal — matching the
                // suppression action. A transient bounce is retryable, so it
                // records bounce_type but must not clobber a delivered status.
                permanent.then(|| set_status(&mut set_clauses, "bounced", true))
            }
            "Complaint" => {
                set_clauses.push("complained_at = :ts");
                Some(set_status(&mut set_clauses, "complained", true))
            }
            "Open" => {
                set_clauses.push("last_opened_at = :ts");
                add_clause = Some("open_count :one");
                None
            }
            "Click" => {
                set_clauses.push("last_clicked_at = :ts");
                add_clause = Some("click_count :one");
                None
            }
            _ => None,
        },
        DomainEvent::Unknown { .. } => None,
    };
    if let Some(status) = status_value {
        builder = builder.expression_attribute_values(":status", status);
    }

    let mut expression = format!("SET {}", set_clauses.join(", "));
    if let Some(add) = add_clause {
        expression.push_str(" ADD ");
        expression.push_str(add);
        builder = builder.expression_attribute_values(":one", AttributeValue::N("1".to_owned()));
    }

    let update = builder
        .update_expression(expression)
        .build()
        .context("failed to build aggregate update")?;
    Ok(update)
}

impl EventStore for AwsServices {
    async fn persist_new(
        &self,
        record: &EventRecord,
        event: &DomainEvent,
    ) -> Result<PersistOutcome, StoreError> {
        let put = Put::builder()
            .table_name(&self.config.table_name)
            .item("pk", AttributeValue::S(partition_key(record)))
            .item("sk", AttributeValue::S(event_sort_key(record)))
            .item(
                "raw_body",
                AttributeValue::B(Blob::new(record.raw_body.to_vec())),
            )
            .item(
                "source",
                AttributeValue::S(record.source.as_str().to_owned()),
            )
            .item("detail_type", AttributeValue::S(record.detail_type.clone()))
            .item("topic_arn", AttributeValue::S(record.topic_arn.clone()))
            .item(
                "sns_message_id",
                AttributeValue::S(record.sns_message_id.clone()),
            )
            .item(
                "sns_timestamp",
                AttributeValue::S(record.event_timestamp.clone()),
            )
            .item("received_at", AttributeValue::S(record.received_at.clone()))
            .item("status", AttributeValue::S("PERSISTED".to_owned()))
            .item(
                "expires_at",
                AttributeValue::N(record.expires_at.to_string()),
            )
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .build()
            .context("failed to build event put")?;

        let update = aggregate_update(&self.config.table_name, record, event)?;

        let result = self
            .dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(put).build())
            .transact_items(TransactWriteItem::builder().update(update).build())
            .send()
            .await;

        match result {
            Ok(_) => Ok(PersistOutcome::Fresh),
            Err(error) => duplicate_outcome(&error).map_err(StoreError),
        }
    }

    async fn mark_published(&self, record: &EventRecord) -> Result<(), StoreError> {
        let published_at = DateTime::from(SystemTime::now())
            .fmt(Format::DateTime)
            .context("failed to format published_at")?;
        self.dynamo
            .update_item()
            .table_name(&self.config.table_name)
            .key("pk", AttributeValue::S(partition_key(record)))
            .key("sk", AttributeValue::S(event_sort_key(record)))
            .update_expression("SET #status = :published, published_at = :at")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":published", AttributeValue::S("PUBLISHED".to_owned()))
            .expression_attribute_values(":at", AttributeValue::S(published_at))
            .send()
            .await
            .map_err(|e| StoreError(anyhow!("{}", DisplayErrorContext(&e))))?;
        Ok(())
    }
}

/// Decides the dedup outcome from a `TransactWriteItems` failure. The Put is
/// transaction entry 0: if (and only if) its condition check failed, the
/// message was seen before, and the returned old item's `status` says how far
/// the prior attempt got — no second read needed.
fn duplicate_outcome(error: &SdkError<TransactWriteItemsError>) -> anyhow::Result<PersistOutcome> {
    let cancellation = match &error {
        SdkError::ServiceError(ctx) => match ctx.err() {
            TransactWriteItemsError::TransactionCanceledException(cancelled) => {
                cancelled.cancellation_reasons().first().cloned()
            }
            _ => None,
        },
        _ => None,
    };
    let Some(reason) = cancellation else {
        return Err(anyhow!(
            "transact_write_items failed: {}",
            DisplayErrorContext(&error)
        ));
    };
    if reason.code() != Some("ConditionalCheckFailed") {
        return Err(anyhow!(
            "transaction cancelled: {}",
            DisplayErrorContext(&error)
        ));
    }
    let prior_status = reason
        .item()
        .and_then(|item| item.get("status"))
        .and_then(|value| value.as_s().ok().cloned());
    match prior_status.as_deref() {
        Some("PUBLISHED") => Ok(PersistOutcome::DuplicatePublished),
        // PERSISTED, or a missing/unreadable old item: resume. The bias is
        // at-least-once — a spurious resume re-runs repeat-safe calls and at
        // worst duplicates a bus event; the other direction loses events.
        _ => Ok(PersistOutcome::DuplicatePersisted),
    }
}

impl PublishEvents for AwsServices {
    async fn publish(&self, event: &OutboundEvent) -> Result<(), PublishError> {
        let entry = PutEventsRequestEntry::builder()
            .event_bus_name(&self.config.event_bus_name)
            .source(&self.config.event_source)
            .detail_type(&event.detail_type)
            .detail(event.detail.to_string())
            .build();
        let response = self
            .events
            .put_events()
            .entries(entry)
            .send()
            .await
            .map_err(|e| PublishError(anyhow!("{}", DisplayErrorContext(&e))))?;

        if response.failed_entry_count() > 0 {
            let failure = response.entries().first().map_or_else(
                || "no entry detail".to_owned(),
                |e| {
                    format!(
                        "{}: {}",
                        e.error_code().unwrap_or("unknown"),
                        e.error_message().unwrap_or("no message")
                    )
                },
            );
            return Err(PublishError(anyhow!("PutEvents entry failed: {failure}")));
        }
        Ok(())
    }
}

/// Maps an SDK failure onto the action retry policy: network faults,
/// timeouts, throttling, and 5xx are transient (worth an SNS redelivery);
/// everything else — validation, access denied, bad configuration — is
/// permanent (log and move on).
fn classify_action_error<E>(context: &'static str, error: &SdkError<E>) -> ActionError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    const THROTTLING_CODES: [&str; 3] = [
        "ThrottlingException",
        "TooManyRequestsException",
        "RequestThrottled",
    ];
    let transient = match error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(ctx) => {
            let status = ctx.raw().status();
            status.is_server_error()
                || status.as_u16() == 429
                || ctx
                    .err()
                    .code()
                    .is_some_and(|code| THROTTLING_CODES.contains(&code))
        }
        _ => false,
    };
    let source = anyhow!("{context}: {}", DisplayErrorContext(error));
    if transient {
        ActionError::transient(source)
    } else {
        ActionError::permanent(source)
    }
}

impl SmsVoiceApi for AwsServices {
    async fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> Result<(), ActionError> {
        let status = match status {
            FeedbackStatus::Received => MessageFeedbackStatus::Received,
            FeedbackStatus::Failed => MessageFeedbackStatus::Failed,
        };
        let result = self
            .sms
            .put_message_feedback()
            .message_id(message_id)
            .message_feedback_status(status)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // DLRs carry no flag saying whether the message was sent with
            // feedback enabled, so this call is made for every terminal
            // event; "no such feedback record" is the expected no-op.
            Err(SdkError::ServiceError(ctx)) if ctx.err().is_resource_not_found_exception() => {
                tracing::debug!(message_id, "message has no feedback record; skipping");
                Ok(())
            }
            Err(error) => Err(classify_action_error("PutMessageFeedback", &error)),
        }
    }

    async fn put_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.sms
            .put_opted_out_number()
            .opt_out_list_name(opt_out_list_name)
            .opted_out_number(phone_number)
            .send()
            .await
            .map_err(|e| classify_action_error("PutOptedOutNumber", &e))?;
        Ok(())
    }

    async fn delete_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        let result = self
            .sms
            .delete_opted_out_number()
            .opt_out_list_name(opt_out_list_name)
            .opted_out_number(phone_number)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // Not opted out = the desired end state; treat as success.
            Err(SdkError::ServiceError(ctx)) if ctx.err().is_resource_not_found_exception() => {
                Ok(())
            }
            Err(error) => Err(classify_action_error("DeleteOptedOutNumber", &error)),
        }
    }
}

impl SesApi for AwsServices {
    async fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> Result<(), ActionError> {
        let reason = match reason {
            SuppressionReason::Bounce => SuppressionListReason::Bounce,
            SuppressionReason::Complaint => SuppressionListReason::Complaint,
        };
        self.ses
            .put_suppressed_destination()
            .email_address(email_address)
            .reason(reason)
            .send()
            .await
            .map_err(|e| classify_action_error("PutSuppressedDestination", &e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;

    use super::*;
    use crate::model::Source;

    fn record(source: Source, aggregate_id: &str) -> EventRecord {
        EventRecord {
            aggregate_id: aggregate_id.to_owned(),
            event_timestamp: "2026-08-03T19:12:52.000Z".to_owned(),
            sns_message_id: "sns-1".to_owned(),
            raw_body: Bytes::from_static(b"{}"),
            source,
            detail_type: "test".to_owned(),
            topic_arn: "arn:aws:sns:us-east-1:123456789012:t".to_owned(),
            received_at: "2026-08-03T19:12:53.000Z".to_owned(),
            expires_at: 1_800_000_000,
        }
    }

    fn expression_for(source: Source, message: &str) -> (String, Vec<String>) {
        let event = DomainEvent::parse(source, message);
        let update = aggregate_update("t", &record(source, "agg-1"), &event).unwrap();
        let expression = update.update_expression.clone();
        let mut value_keys: Vec<String> = update
            .expression_attribute_values
            .unwrap_or_default()
            .keys()
            .cloned()
            .collect();
        value_keys.sort();
        (expression, value_keys)
    }

    #[test]
    fn base_expression_tracks_first_and_last_event() {
        let (expr, keys) = expression_for(Source::SesEvents, "not json");
        assert!(expr.contains("first_event_at = if_not_exists(first_event_at, :ts)"));
        assert!(expr.contains("last_event_at = :ts"));
        assert!(!expr.contains("current_status"));
        assert_eq!(keys, [":expires", ":source", ":ts"]);
    }

    #[test]
    fn open_event_increments_count_and_sets_last_opened() {
        let (expr, keys) = expression_for(
            Source::SesEvents,
            r#"{"eventType":"Open","mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("ADD open_count :one"));
        assert!(expr.contains("last_opened_at = :ts"));
        assert!(keys.contains(&":one".to_owned()));
    }

    #[test]
    fn click_event_increments_click_count() {
        let (expr, _) = expression_for(
            Source::SesEvents,
            r#"{"eventType":"Click","mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("ADD click_count :one"));
        assert!(expr.contains("last_clicked_at = :ts"));
    }

    #[test]
    fn send_never_overwrites_a_terminal_status() {
        let (expr, _) = expression_for(
            Source::SesEvents,
            r#"{"eventType":"Send","mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("current_status = if_not_exists(current_status, :status)"));
    }

    #[test]
    fn permanent_bounce_overwrites_status_and_records_bounce_type() {
        let (expr, keys) = expression_for(
            Source::SesEvents,
            r#"{"eventType":"Bounce","bounce":{"bounceType":"Permanent","bouncedRecipients":[]},
                "mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("current_status = :status"));
        assert!(!expr.contains("if_not_exists(current_status"));
        assert!(expr.contains("bounce_type = :bounce_type"));
        assert!(keys.contains(&":bounce_type".to_owned()));
        assert!(keys.contains(&":status".to_owned()));
    }

    #[test]
    fn transient_bounce_records_type_but_does_not_set_status() {
        // A transient bounce is retryable, so it must not clobber a prior
        // delivered/sent status — matching the suppression action's gate.
        let (expr, keys) = expression_for(
            Source::SesEvents,
            r#"{"eventType":"Bounce","bounce":{"bounceType":"Transient","bouncedRecipients":[]},
                "mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("bounce_type = :bounce_type"));
        assert!(!expr.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }

    #[test]
    fn final_dlr_sets_delivered_or_failed() {
        let (delivered, _) = expression_for(
            Source::SmsEvents,
            r#"{"eventType":"TEXT_DELIVERED","messageId":"m","isFinal":true}"#,
        );
        assert!(delivered.contains("current_status = :status"));

        let (queued, keys) = expression_for(
            Source::SmsEvents,
            r#"{"eventType":"TEXT_QUEUED","messageId":"m","isFinal":false}"#,
        );
        assert!(!queued.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }
}
