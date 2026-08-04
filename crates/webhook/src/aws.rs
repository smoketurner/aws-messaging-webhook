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

/// The aggregate projection applied atomically with the event put. The base
/// expression maintains first/last timestamps; event-specific fields (counts,
/// status transitions) extend it per [`DomainEvent`] variant.
fn aggregate_update(
    table_name: &str,
    record: &EventRecord,
    event: &DomainEvent,
) -> anyhow::Result<Update> {
    let DomainEvent::Unknown { .. } = event;
    let update = Update::builder()
        .table_name(table_name)
        .key("pk", AttributeValue::S(partition_key(record)))
        .key("sk", AttributeValue::S("AGG".to_owned()))
        .update_expression(
            "SET #source = if_not_exists(#source, :source), \
             first_event_at = if_not_exists(first_event_at, :ts), \
             last_event_at = :ts, expires_at = :expires",
        )
        .expression_attribute_names("#source", "source")
        .expression_attribute_values(":source", AttributeValue::S(record.source.as_str().into()))
        .expression_attribute_values(":ts", AttributeValue::S(record.event_timestamp.clone()))
        .expression_attribute_values(":expires", AttributeValue::N(record.expires_at.to_string()))
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
        self.sms
            .put_message_feedback()
            .message_id(message_id)
            .message_feedback_status(status)
            .send()
            .await
            .map_err(|e| classify_action_error("PutMessageFeedback", &e))?;
        Ok(())
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
