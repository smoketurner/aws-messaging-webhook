//! [`EventStore`] for the events table: each SNS notification's raw event
//! item plus the per-message aggregate it rolls up into.

use anyhow::{Context as _, anyhow};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
use aws_smithy_types::Blob;
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::events_keys;
use crate::metrics::names;
use crate::model::DomainEvent;
use crate::model::ses_notification::{SesBounce, SesEngagement, SesEventKind};
use crate::store::{EventRecord, EventStore, PersistOutcome, StoreError};

/// The events table.
pub struct EventsTable {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

impl EventsTable {
    #[must_use]
    pub(crate) fn new(client: aws_sdk_dynamodb::Client, table_name: String) -> Self {
        Self { client, table_name }
    }

    /// Applies a precedence-guarded `current_status` transition (built by
    /// [`status_transition`]) as a standalone `UpdateItem` after the
    /// raw-event/aggregate `TransactWriteItems` has committed.
    ///
    /// A `ConditionalCheckFailed` is the benign, expected outcome for a stale
    /// out-of-order retry: the event cannot regress the more-terminal
    /// `current_status` a sibling already set, so no `message.status.changed`
    /// regression is published and the raw event stays durable. (A redelivery
    /// of an event whose transition already applied settles the same way.)
    ///
    /// Any other failure is a `StoreError` (5xx): the raw event is committed,
    /// but the redelivery it recruits resolves as a `Duplicate` `Put` and
    /// re-applies this guarded — hence repeat-safe — transition, so a
    /// throttle or timeout between the two writes cannot silently lose the
    /// status.
    async fn apply_status_transition(
        &self,
        sns_message_id: &str,
        transition: Update,
    ) -> Result<(), StoreError> {
        let result = self
            .client
            .update_item()
            .set_table_name(Some(transition.table_name))
            .set_key(Some(transition.key))
            .set_update_expression(Some(transition.update_expression))
            .set_condition_expression(transition.condition_expression)
            .set_expression_attribute_values(transition.expression_attribute_values)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(ctx))
                if ctx.err().is_conditional_check_failed_exception() =>
            {
                metrics::counter!(names::STATUS_TRANSITIONS_SUPPRESSED).increment(1);
                tracing::debug!(
                    sns_message_id,
                    event = "status_transition_suppressed",
                    "stale out-of-order event cannot regress current_status; raw event durable",
                );
                Ok(())
            }
            Err(error) => Err(StoreError(anyhow!(
                "status transition failed: {}",
                DisplayErrorContext(&error)
            ))),
        }
    }
}

/// The per-message aggregate's `current_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateStatus {
    Received,
    Sent,
    Delivered,
    Failed,
    Bounced,
    Complained,
}

impl AggregateStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Sent => "sent",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
            Self::Bounced => "bounced",
            Self::Complained => "complained",
        }
    }

    /// The SES ladder (`sent` → `delivered` → `{complained}`): the statuses a
    /// precedence-guarded [`status_transition`] to `self` may replace, besides
    /// an unset one. `bounced` and `complained` are both terminal, so neither
    /// appears in the other's priors: a stale `Delivery`/`Complaint` retry
    /// arriving after either settles as a `ConditionalCheckFailed` no-op.
    fn allowed_priors(self) -> &'static [Self] {
        match self {
            Self::Delivered => &[Self::Sent],
            Self::Complained => &[Self::Sent, Self::Delivered],
            Self::Received | Self::Sent | Self::Failed | Self::Bounced => &[],
        }
    }
}

/// An in-transaction clause setting `current_status` whatever it was.
fn overwrite_status(clauses: &mut Vec<&'static str>, status: AggregateStatus) -> AttributeValue {
    clauses.push("current_status = :status");
    AttributeValue::S(status.as_str().to_owned())
}

/// An in-transaction clause setting `current_status` only when it is unset.
fn status_if_unset(clauses: &mut Vec<&'static str>, status: AggregateStatus) -> AttributeValue {
    clauses.push("current_status = if_not_exists(current_status, :status)");
    AttributeValue::S(status.as_str().to_owned())
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
        .key(
            "pk",
            AttributeValue::S(events_keys::partition(&record.aggregate_id)),
        )
        .key(
            "sk",
            AttributeValue::S(events_keys::AGGREGATE_SK.to_owned()),
        )
        .expression_attribute_names("#source", "source")
        .expression_attribute_values(":source", AttributeValue::S(record.source_label().into()))
        .expression_attribute_values(":ts", AttributeValue::S(record.event_timestamp.clone()))
        .expression_attribute_values(
            ":expires",
            AttributeValue::N(record.aggregate_expires_at.to_string()),
        );

    let status_value = match event {
        DomainEvent::SmsInbound { .. } | DomainEvent::SesInbound { .. } => Some(overwrite_status(
            &mut set_clauses,
            AggregateStatus::Received,
        )),
        DomainEvent::SmsDelivery { event, .. } => {
            if event.is_final {
                let status = if event.is_successful_delivery() {
                    AggregateStatus::Delivered
                } else {
                    AggregateStatus::Failed
                };
                Some(overwrite_status(&mut set_clauses, status))
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
        //    nothing to this aggregate (it shares the no-status arm below),
        //    and Complaint records only its idempotent timestamp here.
        DomainEvent::Ses { event, .. } => match &event.kind {
            SesEventKind::Send => Some(status_if_unset(&mut set_clauses, AggregateStatus::Sent)),
            SesEventKind::Bounce => {
                let permanent = event.bounce.as_ref().is_some_and(SesBounce::is_permanent);
                if let Some(bounce) = &event.bounce {
                    set_clauses.push("bounce_type = :bounce_type");
                    builder = builder.expression_attribute_values(
                        ":bounce_type",
                        AttributeValue::S(bounce.bounce_type.as_str().to_owned()),
                    );
                }
                // Only a permanent bounce is terminal — matching the
                // suppression action. A transient bounce is retryable, so it
                // records bounce_type but must not clobber a delivered status.
                permanent.then(|| overwrite_status(&mut set_clauses, AggregateStatus::Bounced))
            }
            SesEventKind::Complaint => {
                // `complained_at` is an idempotent timestamp — safe in the
                // transaction. `current_status` moves to the post-transaction
                // guarded update (see [`status_transition`]).
                set_clauses.push("complained_at = :ts");
                None
            }
            SesEventKind::Open => {
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
            SesEventKind::Click => {
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
            // (Reject/DeliveryDelay/Subscription/unknown) set no in-transaction
            // current_status.
            SesEventKind::Delivery
            | SesEventKind::Reject
            | SesEventKind::RenderingFailure
            | SesEventKind::DeliveryDelay
            | SesEventKind::Subscription
            | SesEventKind::Other(_) => None,
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
/// the raw event durable unconditionally; a transient failure of the split
/// update is a 5xx whose redelivery (a `Duplicate`) re-applies it.
fn status_transition(
    table_name: &str,
    record: &EventRecord,
    event: &DomainEvent,
) -> Option<Update> {
    let DomainEvent::Ses { event, .. } = event else {
        return None;
    };
    let status = match &event.kind {
        SesEventKind::Delivery => AggregateStatus::Delivered,
        SesEventKind::Complaint => AggregateStatus::Complained,
        SesEventKind::Send
        | SesEventKind::Bounce
        | SesEventKind::Reject
        | SesEventKind::Open
        | SesEventKind::Click
        | SesEventKind::RenderingFailure
        | SesEventKind::DeliveryDelay
        | SesEventKind::Subscription
        | SesEventKind::Other(_) => return None,
    };
    let mut condition = "attribute_not_exists(current_status)".to_owned();
    let mut builder = Update::builder()
        .table_name(table_name)
        .key(
            "pk",
            AttributeValue::S(events_keys::partition(&record.aggregate_id)),
        )
        .key(
            "sk",
            AttributeValue::S(events_keys::AGGREGATE_SK.to_owned()),
        )
        .update_expression("SET current_status = :status")
        .expression_attribute_values(":status", AttributeValue::S(status.as_str().to_owned()));
    for (i, prior) in status.allowed_priors().iter().enumerate() {
        let placeholder = format!(":prev{i}");
        condition.push_str(" OR current_status = ");
        condition.push_str(&placeholder);
        builder = builder
            .expression_attribute_values(placeholder, AttributeValue::S(prior.as_str().to_owned()));
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

impl EventStore for EventsTable {
    async fn persist_new(
        &self,
        record: &EventRecord,
        event: &DomainEvent,
    ) -> Result<PersistOutcome, StoreError> {
        let put = Put::builder()
            .table_name(&self.table_name)
            .item(
                "pk",
                AttributeValue::S(events_keys::partition(&record.aggregate_id)),
            )
            .item(
                "sk",
                AttributeValue::S(events_keys::event_sk(
                    &record.event_timestamp,
                    &record.sns_message_id,
                )),
            )
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

        let update = aggregate_update(&self.table_name, record, event)?;

        let result = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(put).build())
            .transact_items(TransactWriteItem::builder().update(update).build())
            .send()
            .await;

        let outcome = match result {
            Ok(_) => PersistOutcome::Fresh,
            Err(error) => duplicate_outcome(&error).map_err(StoreError)?,
        };
        // The precedence-guarded `current_status` transition for
        // Delivery/Complaint runs on the fresh *and* the duplicate path: it is
        // not part of the transaction, so a failure (or a crash) after the
        // commit is recovered only by the redelivery re-applying it. The guard
        // makes that repeat-safe.
        if let Some(transition) = status_transition(&self.table_name, record, event) {
            self.apply_status_transition(&record.sns_message_id, transition)
                .await?;
        }
        Ok(outcome)
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

#[cfg(test)]
mod tests {
    use axum::body::Bytes;

    use super::*;
    use crate::model::Source;

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

    // `persist_new` against a stub DynamoDB endpoint: the guarded transition
    // is applied on the duplicate path too, and a transient failure of it is
    // surfaced (5xx) rather than swallowed — otherwise a throttle or crash
    // between the transaction and the split update would lose the status for
    // good, since every redelivery resolves as a `Duplicate`.
    mod persist {
        use wiremock::matchers::{header, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::*;

        const TRANSACT: &str = "DynamoDB_20120810.TransactWriteItems";
        const UPDATE: &str = "DynamoDB_20120810.UpdateItem";
        const DELIVERY: &str = r#"{"eventType":"Delivery","mail":{"messageId":"m"}}"#;

        fn json(status: u16, body: &str) -> ResponseTemplate {
            ResponseTemplate::new(status)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_string(body.to_owned())
        }

        fn dynamo_error(status: u16, kind: &str) -> ResponseTemplate {
            json(
                status,
                &format!(r#"{{"__type":"com.amazonaws.dynamodb.v20120810#{kind}","message":"x"}}"#),
            )
        }

        fn duplicate_put() -> ResponseTemplate {
            json(
                400,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                    "Message":"cancelled",
                    "CancellationReasons":[{"Code":"ConditionalCheckFailed"},{"Code":"None"}]}"#,
            )
        }

        async fn mount(server: &MockServer, target: &str, response: ResponseTemplate) {
            Mock::given(method("POST"))
                .and(header("x-amz-target", target))
                .respond_with(response)
                .mount(server)
                .await;
        }

        fn aws(server: &MockServer) -> EventsTable {
            let dynamo_conf = aws_sdk_dynamodb::Config::builder()
                .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
                .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                    "AKID", "secret", None, None, "test",
                ))
                .endpoint_url(server.uri())
                .retry_config(aws_smithy_types::retry::RetryConfig::disabled())
                .build();
            EventsTable::new(
                aws_sdk_dynamodb::Client::from_conf(dynamo_conf),
                "events".to_owned(),
            )
        }

        async fn persist(server: &MockServer, message: &str) -> Result<PersistOutcome, StoreError> {
            let event = DomainEvent::classify(message);
            aws(server)
                .persist_new(&record(event.family(), "agg-1"), &event)
                .await
        }

        async fn update_calls(server: &MockServer) -> usize {
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .filter(|r| r.headers.get("x-amz-target").is_some_and(|v| v == UPDATE))
                .count()
        }

        #[tokio::test]
        async fn a_fresh_delivery_applies_the_guarded_transition() {
            let server = MockServer::start().await;
            mount(&server, TRANSACT, json(200, "{}")).await;
            mount(&server, UPDATE, json(200, "{}")).await;
            assert_eq!(
                persist(&server, DELIVERY).await.unwrap(),
                PersistOutcome::Fresh
            );
            assert_eq!(update_calls(&server).await, 1);
        }

        #[tokio::test]
        async fn a_duplicate_delivery_re_applies_the_guarded_transition() {
            let server = MockServer::start().await;
            mount(&server, TRANSACT, duplicate_put()).await;
            mount(&server, UPDATE, json(200, "{}")).await;
            assert_eq!(
                persist(&server, DELIVERY).await.unwrap(),
                PersistOutcome::Duplicate
            );
            assert_eq!(update_calls(&server).await, 1);
        }

        #[tokio::test]
        async fn a_suppressed_stale_transition_is_benign() {
            let server = MockServer::start().await;
            mount(&server, TRANSACT, json(200, "{}")).await;
            mount(
                &server,
                UPDATE,
                dynamo_error(400, "ConditionalCheckFailedException"),
            )
            .await;
            assert_eq!(
                persist(&server, DELIVERY).await.unwrap(),
                PersistOutcome::Fresh
            );
        }

        #[tokio::test]
        async fn a_failed_transition_is_surfaced_for_redelivery() {
            let server = MockServer::start().await;
            mount(&server, TRANSACT, json(200, "{}")).await;
            mount(&server, UPDATE, dynamo_error(500, "InternalServerError")).await;
            assert!(persist(&server, DELIVERY).await.is_err());
        }

        #[tokio::test]
        async fn events_without_a_transition_send_no_update() {
            let server = MockServer::start().await;
            mount(&server, TRANSACT, duplicate_put()).await;
            let open = r#"{"eventType":"Open","mail":{"messageId":"m"}}"#;
            assert_eq!(
                persist(&server, open).await.unwrap(),
                PersistOutcome::Duplicate
            );
            assert_eq!(update_calls(&server).await, 0);
        }
    }
}
