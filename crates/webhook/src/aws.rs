//! Production [`Services`](crate::state::Services) implementation wrapping
//! the AWS SDK clients.

// Mail inbox trait implementations.
pub mod api_keys;
pub mod mail_store;
pub mod objects;

use anyhow::{Context as _, anyhow};
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use aws_sdk_eventbridge::types::PutEventsRequestEntry;
use aws_sdk_pinpointsmsvoicev2::types::MessageFeedbackStatus;
use aws_sdk_sesv2::config::Builder as SesConfigBuilder;
use aws_sdk_sesv2::config::retry::RetryConfig;
use aws_sdk_sesv2::config::timeout::TimeoutConfig;
use aws_sdk_sesv2::operation::send_email::SendEmailOutput;
use aws_sdk_sesv2::types::{
    Destination as SesDestination, EmailContent, MessageTag, RawMessage, SuppressionListReason,
};
use aws_smithy_types::Blob;
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::actions::{
    ActionError, FeedbackStatus, RawSend, SendOutcome, SesApi, SmsVoiceApi, SuppressionReason,
};
use crate::config::{Config, MailConfig};
use crate::mail::fetch::{AttachmentFetcher, FetchError, Fetched, HttpAttachmentFetcher};
use crate::metrics::names;
use crate::model::DomainEvent;
use crate::model::ses_notification::{SesBounce, SesEngagement};
use crate::publish::{OutboundEvent, PublishError, PublishEvents};
use crate::store::{EventRecord, EventStore, PersistOutcome, StoreError};

pub struct AwsServices {
    dynamo: aws_sdk_dynamodb::Client,
    events: aws_sdk_eventbridge::Client,
    sms: aws_sdk_pinpointsmsvoicev2::Client,
    ses: aws_sdk_sesv2::Client,
    /// Mail bodies and attachments (`MailConfig.bucket`). Constructed
    /// unconditionally: it is cheap, and whether mail is configured is a
    /// per-invocation question rather than a per-client one.
    s3: aws_sdk_s3::Client,
    /// Reads the `SecureString` holding the API key hashes.
    ssm: aws_sdk_ssm::Client,
    /// Fetches URL-backed attachments, behind the SSRF guards.
    fetcher: HttpAttachmentFetcher,
    config: Config,
}

impl AttachmentFetcher for AwsServices {
    async fn fetch(
        &self,
        url: &crate::mail::url_policy::AttachmentUrl,
        max_bytes: u64,
    ) -> Result<Fetched, FetchError> {
        self.fetcher.fetch(url, max_bytes).await
    }
}

impl AwsServices {
    #[must_use]
    pub fn new(sdk_config: &aws_config::SdkConfig, config: Config) -> Self {
        Self {
            dynamo: aws_sdk_dynamodb::Client::new(sdk_config),
            events: aws_sdk_eventbridge::Client::new(sdk_config),
            sms: aws_sdk_pinpointsmsvoicev2::Client::new(sdk_config),
            ses: aws_sdk_sesv2::Client::new(sdk_config),
            s3: aws_sdk_s3::Client::new(sdk_config),
            ssm: aws_sdk_ssm::Client::new(sdk_config),
            fetcher: HttpAttachmentFetcher::new(),
            config,
        }
    }

    /// The mail bucket/table configuration (`Config.mail`), when mail
    /// ingestion is enabled. `None` when it isn't — every mail store/object
    /// store method is only ever reached when the caller has already gated
    /// on mail being configured; each maps `None` into its own error type.
    pub(crate) fn mail_config(&self) -> Option<&MailConfig> {
        self.config.mail.as_ref()
    }

    /// Applies a precedence-guarded `current_status` transition (built by
    /// [`status_transition`]) as a standalone `UpdateItem` after the
    /// raw-event/aggregate `TransactWriteItems` has committed.
    ///
    /// A `ConditionalCheckFailed` is the benign, expected outcome for a stale
    /// out-of-order retry: the event cannot regress the more-terminal
    /// `current_status` a sibling already set, so no `message.status.changed`
    /// regression is published and the raw event stays durable. Any other
    /// failure is logged and swallowed — the raw event is already committed,
    /// and surfacing the error as a `StoreError` (5xx) would only recruit a
    /// redelivery that re-rolls a `Duplicate` `Put` with no chance to re-apply
    /// this transition.
    async fn apply_status_transition(&self, sns_message_id: &str, transition: Update) {
        let result = self
            .dynamo
            .update_item()
            .set_table_name(Some(transition.table_name))
            .set_key(Some(transition.key))
            .set_update_expression(Some(transition.update_expression))
            .set_condition_expression(transition.condition_expression)
            .set_expression_attribute_values(transition.expression_attribute_values)
            .send()
            .await;
        match result {
            Ok(_) => {}
            Err(SdkError::ServiceError(ctx))
                if ctx.err().is_conditional_check_failed_exception() =>
            {
                metrics::counter!(names::STATUS_TRANSITIONS_SUPPRESSED).increment(1);
                tracing::debug!(
                    sns_message_id,
                    event = "status_transition_suppressed",
                    "stale out-of-order event cannot regress current_status; raw event durable",
                );
            }
            Err(error) => {
                tracing::error!(
                    ?error,
                    sns_message_id,
                    event = "status_transition_failed",
                    "precedence-guarded status update failed; raw event durable",
                );
            }
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
        .expression_attribute_values(":source", AttributeValue::S(record.source_label().into()))
        .expression_attribute_values(":ts", AttributeValue::S(record.event_timestamp.clone()))
        .expression_attribute_values(
            ":expires",
            AttributeValue::N(record.aggregate_expires_at.to_string()),
        );

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
        // SES `current_status` is set three ways:
        //  - `Send` uses an `if_not_exists` guard here — it is the initial
        //    rung and must not overwrite a terminal status set by an
        //    out-of-order sibling.
        //  - permanent `Bounce`/final `SmsDelivery` clobber here deliberately
        //    (test-pinned).
        //  - `Delivery` and `Complaint` set `current_status` in a separate,
        //    precedence-guarded `UpdateItem` *after* the transaction (see
        //    [`status_transition`]). An unconditional clobber here let a stale
        //    out-of-order retry regress a more-terminal status a sibling had
        //    already set (e.g. a `Delivery` retry overwriting `complained`),
        //    and a guard inside the `TransactWriteItems` would cancel the
        //    raw-event `Put` on failure and loop. So Delivery contributes
        //    nothing to this aggregate (it falls through to the wildcard
        //    below), and Complaint records only its idempotent timestamp here.
        DomainEvent::Ses { event, .. } => match event.kind.as_str() {
            "Send" => Some(set_status(&mut set_clauses, "sent", false)),
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
                // `complained_at` is an idempotent timestamp — safe in the
                // transaction. `current_status` moves to the post-transaction
                // guarded update (see [`status_transition`]).
                set_clauses.push("complained_at = :ts");
                None
            }
            "Open" => {
                set_clauses.push("last_opened_at = :ts");
                // SES's isBotEvent (Likely) routes automated opens to a
                // separate counter so human engagement stays uncontaminated.
                add_clause = Some(if event.open.as_ref().is_some_and(SesEngagement::is_bot) {
                    "bot_open_count :one"
                } else {
                    "open_count :one"
                });
                None
            }
            "Click" => {
                set_clauses.push("last_clicked_at = :ts");
                add_clause = Some(if event.click.as_ref().is_some_and(SesEngagement::is_bot) {
                    "bot_click_count :one"
                } else {
                    "click_count :one"
                });
                None
            }
            // Delivery (its `current_status` is applied post-transaction by
            // [`status_transition`]) and non-status SES kinds
            // (Reject/DeliveryDelay/Subscription/…) set no in-transaction
            // current_status.
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

/// A `current_status` transition applied as a standalone `UpdateItem` *after*
/// the raw-event/aggregate `TransactWriteItems` commits — for `Delivery` and
/// `Complaint`, the only status-setting SES branches that neither guard
/// `current_status` (like `Send`) nor deliberately pin an unconditional clobber
/// (like permanent `Bounce`/final `SmsDelivery`).
///
/// The transition is precedence-guarded: it takes effect only when
/// `current_status` is unset or one of the less-terminal rungs below it on the
/// SES ladder
/// (`sent` → `delivered` → `{complained}`), so a stale out-of-order retry —
/// whose first delivery failed pre-persist, then lands after a sibling event
/// has already set `current_status` — cannot regress the rolled-up status. The
/// `UpdateItem` returns `ConditionalCheckFailed`, a benign no-op: the raw event
/// is already durable and the relay sees no `current_status` transition, so no
/// `message.status.changed` regression (e.g. `complained` → `delivered`) is
/// published for the retry.
///
/// The guard lives on a separate `UpdateItem`, not the in-transaction
/// `Update`: a condition failure inside `TransactWriteItems` cancels the
/// raw-event `Put` too, and the dedup path (`duplicate_outcome`) inspects only
/// cancellation index 0, so the stale retry would resurface as an unhandled
/// `StoreError` (5xx) and loop instead of settling. Splitting the update keeps
/// the raw event durable unconditionally and degrades a status failure
/// gracefully.
fn status_transition(
    table_name: &str,
    record: &EventRecord,
    event: &DomainEvent,
) -> Option<Update> {
    // `bounced` and `complained` are both terminal, so neither appears in the
    // other's allowed priors: a stale `Delivery`/`Complaint` retry arriving
    // after either settles as a `ConditionalCheckFailed` no-op.
    let DomainEvent::Ses { event, .. } = event else {
        return None;
    };
    let (status, priors): (&'static str, &'static [&'static str]) = match event.kind.as_str() {
        "Delivery" => ("delivered", &["sent"]),
        "Complaint" => ("complained", &["sent", "delivered"]),
        _ => return None,
    };
    let mut condition = "attribute_not_exists(current_status)".to_owned();
    let mut builder = Update::builder()
        .table_name(table_name)
        .key("pk", AttributeValue::S(partition_key(record)))
        .key("sk", AttributeValue::S("AGG".to_owned()))
        .update_expression("SET current_status = :status")
        .expression_attribute_values(":status", AttributeValue::S(status.to_owned()));
    for (i, prior) in priors.iter().enumerate() {
        let placeholder = format!(":prev{i}");
        condition.push_str(" OR current_status = ");
        condition.push_str(&placeholder);
        builder = builder
            .expression_attribute_values(placeholder, AttributeValue::S((*prior).to_owned()));
    }
    match builder.condition_expression(condition).build() {
        Ok(update) => Some(update),
        Err(error) => {
            tracing::error!(
                ?error,
                event = "status_transition_build_failure",
                "failed to build precedence-guarded status update",
            );
            None
        }
    }
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
                AttributeValue::S(record.source_label().to_owned()),
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
            .item(
                "expires_at",
                AttributeValue::N(record.expires_at.to_string()),
            )
            .condition_expression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
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
            Ok(_) => {
                // The precedence-guarded `current_status` transition for
                // Delivery/Complaint runs only on the fresh path — a
                // redelivery is a Duplicate and never re-applies the aggregate.
                // It is best-effort: the raw event is already durable, a
                // `ConditionalCheckFailed` is a benign suppression, and any
                // other failure is logged rather than surfaced as a 5xx (a
                // retry would only re-roll a `Duplicate` Put that cannot
                // re-apply this transition).
                if let Some(transition) = status_transition(&self.config.table_name, record, event)
                {
                    self.apply_status_transition(&record.sns_message_id, transition)
                        .await;
                }
                Ok(PersistOutcome::Fresh)
            }
            Err(error) => duplicate_outcome(&error).map_err(StoreError),
        }
    }
}

/// Decides the dedup outcome from a `TransactWriteItems` failure. The Put is
/// transaction entry 0: if its condition check failed, the event was already
/// persisted — an SNS redelivery.
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
    Ok(PersistOutcome::Duplicate)
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

/// Throttling error codes for DynamoDB/Pinpoint/SES actions and the mail
/// store. S3 (`aws::objects`) uses a different vocabulary and keeps its own
/// list.
pub(crate) const THROTTLING_CODES: [&str; 3] = [
    "ThrottlingException",
    "TooManyRequestsException",
    "RequestThrottled",
];

/// Maps an SDK failure onto the transient/permanent retry policy shared by
/// actions, the mail store, and the object store: network faults, timeouts,
/// and 5xx/429/throttling responses are transient (worth a retry);
/// everything else is permanent. `throttling_codes` is the calling API's own
/// vocabulary for its throttling error code, since it differs per service.
pub(crate) fn sdk_error_is_transient<E>(error: &SdkError<E>, throttling_codes: &[&str]) -> bool
where
    E: ProvideErrorMetadata,
{
    match error {
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
                    .is_some_and(|code| throttling_codes.contains(&code))
        }
        _ => false,
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
    let source = anyhow!("{context}: {}", DisplayErrorContext(error));
    if sdk_error_is_transient(error, &THROTTLING_CODES) {
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

    async fn send_raw(&self, request: &RawSend<'_>) -> SendOutcome {
        let mut destination = SesDestination::builder();
        for address in request.to {
            destination = destination.to_addresses(address);
        }
        for address in request.cc {
            destination = destination.cc_addresses(address);
        }
        for address in request.bcc {
            destination = destination.bcc_addresses(address);
        }

        // A builder that will not build means the request was never sent, so
        // this is a definite failure rather than an ambiguous one.
        let raw = match RawMessage::builder()
            .data(Blob::new(request.raw.to_vec()))
            .build()
        {
            Ok(raw) => raw,
            Err(error) => {
                return SendOutcome::Failed {
                    reason: format!("building the raw message: {error}"),
                };
            }
        };
        let tag = match MessageTag::builder()
            .name("mailbox_message")
            .value(request.message_id)
            .build()
        {
            Ok(tag) => tag,
            Err(error) => {
                return SendOutcome::Failed {
                    reason: format!("building the message tag: {error}"),
                };
            }
        };

        let result = self
            .ses
            .send_email()
            .from_email_address(request.from)
            .from_email_address_identity_arn(request.identity_arn)
            .configuration_set_name(request.configuration_set)
            .destination(destination.build())
            .content(EmailContent::builder().raw(raw).build())
            .email_tags(tag)
            .customize()
            // One attempt, always. A retry of a call that already succeeded
            // would send the message a second time, and SendEmail has no
            // idempotency token to prevent that. The timeout bounds the call
            // well inside the time the sender keeps back for it; running out
            // is an unknown outcome, never a retry.
            .config_override(
                SesConfigBuilder::new()
                    .retry_config(RetryConfig::disabled())
                    .timeout_config(
                        TimeoutConfig::builder()
                            .operation_timeout(SES_CALL_TIMEOUT)
                            .build(),
                    ),
            )
            .send()
            .await;

        classify_send_result(result)
    }
}

/// How long one `SendEmail` call may take before its outcome is unknown.
pub const SES_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Turns one `SendEmail` result into a [`SendOutcome`].
///
/// The split that matters is between a definite refusal and an answer that
/// may have come after the message was accepted. See
/// [`classify_send_status`] for service errors. Everything else — a timeout,
/// a dropped connection, a response that could not be parsed — means the
/// request may have been received and acted on, so it is `Unknown` and the
/// message is never resent without an operator saying so.
fn classify_send_result<E>(result: Result<SendEmailOutput, SdkError<E>>) -> SendOutcome
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let error = match result {
        Ok(output) => {
            return SendOutcome::Sent {
                ses_message_id: output.message_id().unwrap_or_default().to_owned(),
            };
        }
        Err(error) => error,
    };

    let reason = DisplayErrorContext(&error).to_string();
    match &error {
        SdkError::ServiceError(context) => {
            classify_send_status(context.raw().status().as_u16(), reason)
        }
        // Rejected before anything was sent: nothing reached SES.
        SdkError::ConstructionFailure(_) => SendOutcome::Failed { reason },
        // The request may be in flight or already handled.
        _ => SendOutcome::Unknown { reason },
    }
}

/// Classifies an HTTP status SES answered `SendEmail` with.
///
/// 429 and 503 are refusals to take the request at all, so retrying cannot
/// send twice. 500, 502 and 504 can come back after SES accepted the message,
/// so they are `Unknown`. Any other 5xx is treated the same way. Other 4xx
/// are definite, permanent refusals.
fn classify_send_status(status: u16, reason: String) -> SendOutcome {
    match status {
        429 | 503 => SendOutcome::Retryable { reason },
        500..=599 => SendOutcome::Unknown { reason },
        _ => SendOutcome::Failed { reason },
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sesv2::operation::send_email::SendEmailError;
    use axum::body::Bytes;

    use super::*;
    use crate::model::Source;

    /// A send whose answer never arrived must never be reported as a
    /// failure. Doing so would let the sender give up on — or worse, resend —
    /// a message SES may already have accepted.
    #[test]
    fn an_unanswered_send_is_unknown_rather_than_failed() {
        let timeout: SdkError<SendEmailError> =
            SdkError::timeout_error(Box::new(std::io::Error::other("timed out")));
        assert!(
            matches!(
                classify_send_result(Err(timeout)),
                SendOutcome::Unknown { .. }
            ),
            "a timeout leaves the outcome genuinely unknown"
        );
    }

    /// Only statuses that mean SES did not take the request are retried; one
    /// that may follow an accepted message is unknown.
    #[test]
    fn send_statuses_retry_only_on_a_clear_refusal() {
        for status in [429, 503] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Retryable { .. }
                ),
                "{status} should be retryable"
            );
        }
        for status in [500, 502, 504] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Unknown { .. }
                ),
                "{status} should be unknown"
            );
        }
        for status in [400, 403, 404] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Failed { .. }
                ),
                "{status} should be a failure"
            );
        }
    }

    /// A request that was never built cannot have reached SES, so it is a
    /// definite failure and safe to stop on.
    #[test]
    fn a_request_that_was_never_sent_is_a_definite_failure() {
        let construction: SdkError<SendEmailError> =
            SdkError::construction_failure(Box::new(std::io::Error::other("bad field")));
        assert!(matches!(
            classify_send_result(Err(construction)),
            SendOutcome::Failed { .. }
        ));
    }

    fn record(source: Option<Source>, aggregate_id: &str) -> EventRecord {
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
            aggregate_expires_at: 1_900_000_000,
        }
    }

    fn expression_for(message: &str) -> (String, Vec<String>) {
        let event = DomainEvent::classify(message);
        let update = aggregate_update("t", &record(event.family(), "agg-1"), &event).unwrap();
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
        let (expr, keys) = expression_for("not json");
        assert!(expr.contains("first_event_at = if_not_exists(first_event_at, :ts)"));
        assert!(expr.contains("last_event_at = :ts"));
        assert!(!expr.contains("current_status"));
        assert_eq!(keys, [":expires", ":source", ":ts"]);
    }

    #[test]
    fn aggregate_ttl_uses_the_aggregate_expiry_not_the_raw_ttl() {
        let event = DomainEvent::classify("not json");
        let update = aggregate_update("t", &record(event.family(), "agg-1"), &event).unwrap();
        let expires = update
            .expression_attribute_values
            .unwrap()
            .get(":expires")
            .cloned()
            .unwrap();
        // record() sets expires_at = 1_800_000_000 and
        // aggregate_expires_at = 1_900_000_000; the aggregate must use the latter.
        assert_eq!(expires, AttributeValue::N("1900000000".to_owned()));
    }

    #[test]
    fn open_event_increments_count_and_sets_last_opened() {
        let (expr, keys) = expression_for(r#"{"eventType":"Open","mail":{"messageId":"m"}}"#);
        assert!(expr.contains("ADD open_count :one"));
        assert!(expr.contains("last_opened_at = :ts"));
        assert!(keys.contains(&":one".to_owned()));
    }

    #[test]
    fn bot_open_routes_to_bot_counter() {
        let (expr, _) = expression_for(
            r#"{"eventType":"Open","mail":{"messageId":"m"},"open":{"isBotEvent":"Likely"}}"#,
        );
        assert!(expr.contains("ADD bot_open_count :one"));
        assert!(!expr.contains("ADD open_count"));
        assert!(expr.contains("last_opened_at = :ts"));
    }

    #[test]
    fn unlikely_open_stays_on_the_human_counter() {
        let (expr, _) = expression_for(
            r#"{"eventType":"Open","mail":{"messageId":"m"},"open":{"isBotEvent":"Unlikely"}}"#,
        );
        assert!(expr.contains("ADD open_count :one"));
        assert!(!expr.contains("bot_open_count"));
    }

    #[test]
    fn click_event_increments_click_count() {
        let (expr, _) = expression_for(r#"{"eventType":"Click","mail":{"messageId":"m"}}"#);
        assert!(expr.contains("ADD click_count :one"));
        assert!(expr.contains("last_clicked_at = :ts"));
    }

    #[test]
    fn bot_click_routes_to_bot_counter() {
        let (expr, _) = expression_for(
            r#"{"eventType":"Click","mail":{"messageId":"m"},"click":{"isBotEvent":"Likely"}}"#,
        );
        assert!(expr.contains("ADD bot_click_count :one"));
        assert!(!expr.contains("ADD click_count"));
        assert!(expr.contains("last_clicked_at = :ts"));
    }

    #[test]
    fn send_never_overwrites_a_terminal_status() {
        let (expr, _) = expression_for(r#"{"eventType":"Send","mail":{"messageId":"m"}}"#);
        assert!(expr.contains("current_status = if_not_exists(current_status, :status)"));
    }

    #[test]
    fn permanent_bounce_overwrites_status_and_records_bounce_type() {
        let (expr, keys) = expression_for(
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
            r#"{"eventType":"Bounce","bounce":{"bounceType":"Transient","bouncedRecipients":[]},
                "mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("bounce_type = :bounce_type"));
        assert!(!expr.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }

    #[test]
    fn final_dlr_sets_delivered_or_failed() {
        let (delivered, _) =
            expression_for(r#"{"eventType":"TEXT_DELIVERED","messageId":"m","isFinal":true}"#);
        assert!(delivered.contains("current_status = :status"));

        let (queued, keys) =
            expression_for(r#"{"eventType":"TEXT_QUEUED","messageId":"m","isFinal":false}"#);
        assert!(!queued.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }

    // Delivery and Complaint no longer set `current_status` in the
    // in-transaction aggregate: their transition is applied post-transaction
    // by a precedence-guarded `UpdateItem` (see `status_transition`) so a
    // stale out-of-order retry cannot regress a more-terminal sibling status.

    #[test]
    fn delivery_does_not_set_current_status_in_transaction() {
        let (expr, keys) = expression_for(r#"{"eventType":"Delivery","mail":{"messageId":"m"}}"#);
        assert!(!expr.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }

    #[test]
    fn complaint_records_timestamp_but_not_current_status_in_transaction() {
        // complained_at is an idempotent timestamp that stays in the
        // transaction; current_status moves to the post-transaction guard.
        let (expr, keys) = expression_for(
            r#"{"eventType":"Complaint","complaint":{"complainedRecipients":[]},
                "mail":{"messageId":"m"}}"#,
        );
        assert!(expr.contains("complained_at = :ts"));
        assert!(!expr.contains("current_status"));
        assert!(!keys.contains(&":status".to_owned()));
    }

    #[test]
    fn only_delivery_and_complaint_have_status_transitions() {
        // Branches whose current_status stays in the transaction (or is not
        // set at all) produce no post-transaction transition.
        for message in [
            r#"{"eventType":"Send","mail":{"messageId":"m"}}"#,
            r#"{"eventType":"Bounce","bounce":{"bounceType":"Permanent","bouncedRecipients":[]},"mail":{"messageId":"m"}}"#,
            r#"{"eventType":"Open","mail":{"messageId":"m"}}"#,
            r#"{"eventType":"Click","mail":{"messageId":"m"}}"#,
            r#"{"eventType":"TEXT_DELIVERED","messageId":"m","isFinal":true}"#,
            "not json",
        ] {
            let event = DomainEvent::classify(message);
            assert!(
                status_transition("t", &record(event.family(), "agg-1"), &event).is_none(),
                "no transition expected for: {message}"
            );
        }
        for message in [
            r#"{"eventType":"Delivery","mail":{"messageId":"m"}}"#,
            r#"{"eventType":"Complaint","complaint":{"complainedRecipients":[]},"mail":{"messageId":"m"}}"#,
        ] {
            let event = DomainEvent::classify(message);
            assert!(
                status_transition("t", &record(event.family(), "agg-1"), &event).is_some(),
                "transition expected for: {message}"
            );
        }
    }

    #[test]
    fn delivery_status_transition_advances_from_unset_or_sent_only() {
        let event = DomainEvent::classify(r#"{"eventType":"Delivery","mail":{"messageId":"m"}}"#);
        let update = status_transition("t", &record(event.family(), "agg-1"), &event).unwrap();
        assert_eq!(update.update_expression, "SET current_status = :status");
        let condition = update.condition_expression.unwrap();
        assert!(condition.contains("attribute_not_exists(current_status)"));
        assert!(condition.contains("current_status = :prev0"));
        assert!(!condition.contains(":prev1"));
        let values = update.expression_attribute_values.unwrap();
        assert_eq!(values[":status"], AttributeValue::S("delivered".to_owned()));
        assert_eq!(values[":prev0"], AttributeValue::S("sent".to_owned()));
        assert!(!values.contains_key(":prev1"));
        // No terminal (delivered/complained/bounced) is whitelisted as a
        // prior, so a stale Delivery retry landing after any of them settles
        // as a benign ConditionalCheckFailed instead of regressing the status.
        assert!(!values.contains_key(":prev2"));
    }

    #[test]
    fn complaint_status_transition_advances_from_unset_sent_or_delivered_only() {
        let event = DomainEvent::classify(
            r#"{"eventType":"Complaint","complaint":{"complainedRecipients":[]},
                "mail":{"messageId":"m"}}"#,
        );
        let update = status_transition("t", &record(event.family(), "agg-1"), &event).unwrap();
        let condition = update.condition_expression.unwrap();
        assert!(condition.contains("attribute_not_exists(current_status)"));
        assert!(condition.contains("current_status = :prev0"));
        assert!(condition.contains("current_status = :prev1"));
        assert!(!condition.contains(":prev2"));
        let values = update.expression_attribute_values.unwrap();
        assert_eq!(
            values[":status"],
            AttributeValue::S("complained".to_owned())
        );
        assert_eq!(values[":prev0"], AttributeValue::S("sent".to_owned()));
        assert_eq!(values[":prev1"], AttributeValue::S("delivered".to_owned()));
        assert!(!values.contains_key(":prev2"));
    }
}
