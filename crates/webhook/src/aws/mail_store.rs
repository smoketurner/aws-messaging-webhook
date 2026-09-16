//! [`MailStore`] for [`AwsServices`](crate::aws::AwsServices): the mail
//! table's DynamoDB access.
//!
//! Every write goes through the D27 transaction model
//! ([`crate::mail::plan`]/[`crate::mail::txn`]): a planner builds
//! role-tagged [`PlannedOp`]s from a pure, in-memory computation, this module
//! renders them to a `TransactWriteItems` call, and
//! [`decode_cancellation`] turns a cancellation into one of a handful of
//! decisions the caller retries or surfaces. Reads and writes use
//! `serde_dynamo::to_item`/`from_item` on the same structs the planner
//! builds (N18); this module only adds the key attributes those structs
//! don't carry themselves.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue as DynamoAv, Delete, KeysAndAttributes, Put, TransactWriteItem, Update,
};
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::aws::{self, AwsServices};
use crate::mail::keys;
use crate::mail::plan::{Check, Cond, PlannedOp, TxnKind, WriteOp};
use crate::mail::store::{MailStore, MailStoreError};
use crate::mail::thread::{ThreadState, apply_message, new_thread};
use crate::mail::txn::{CancellationReason, TxnDecision, decode_cancellation};
use crate::mail::{Inbox, InboxId, InsertOutcome, MailMessage, RfcHit};

/// D27: `Retry`/`VersionConflict` loop at most this many times before giving
/// up with `MailStoreError::Conflict`.
const MAX_TXN_RETRIES: u32 = 3;

/// M11: `BatchGetItem` loops on `UnprocessedKeys` at most this many times
/// (matching D19's `ByThread` `BatchGetItem` loop) before surfacing a
/// transient error.
const MAX_BATCH_GET_ATTEMPTS: u32 = 5;

impl AwsServices {
    /// The mail table name, or a permanent error when mail is disabled —
    /// every `MailStore` method is only ever called when it is enabled, but
    /// this keeps that assumption from becoming a panic if it's ever
    /// violated.
    fn mail_table_name(&self) -> Result<&str, MailStoreError> {
        self.mail_config()
            .map(|config| config.table_name.as_str())
            .ok_or_else(|| MailStoreError::Permanent(anyhow!("mail is not configured")))
    }

    /// Reads the thread's current state, consistently (D3: "read the thread
    /// consistently, compute its new state in Rust").
    async fn get_thread_state(
        &self,
        inbox: &InboxId,
        thread_id: &str,
    ) -> Result<Option<ThreadState>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::inbox_pk(inbox.as_str())))
            .key("sk", DynamoAv::S(keys::thread_sk(thread_id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(thread)", &e))?;
        match output.item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item)
                .map(Some)
                .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing thread: {e}"))),
        }
    }
}

/// Maps an SDK failure to the mail-store retry policy: network faults,
/// timeouts, throttling and 5xx are transient; everything else is permanent.
/// Shares its transient/permanent classification with `aws.rs`'s
/// `classify_action_error` via `aws::sdk_error_is_transient`.
fn store_error_from_sdk<E>(context: &'static str, error: &SdkError<E>) -> MailStoreError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let source = anyhow!("{context}: {}", DisplayErrorContext(error));
    if aws::sdk_error_is_transient(error, &aws::THROTTLING_CODES) {
        MailStoreError::Transient(source)
    } else {
        MailStoreError::Permanent(source)
    }
}

/// Maps a `CancellationReason`'s `code()` (D27) to our enum.
fn map_cancellation_reason(code: Option<&str>) -> CancellationReason {
    match code {
        Some("ConditionalCheckFailed") => CancellationReason::ConditionalCheckFailed,
        Some("TransactionConflict") => CancellationReason::TransactionConflict,
        Some("ThrottlingError") => CancellationReason::ThrottlingError,
        Some("ProvisionedThroughputExceeded") => CancellationReason::ProvisionedThroughputExceeded,
        Some("RequestLimitExceeded") => CancellationReason::RequestLimitExceeded,
        Some("InternalServerError") => CancellationReason::InternalServerError,
        Some("ValidationError") => CancellationReason::ValidationError,
        Some("ItemCollectionSizeLimitExceeded") => {
            CancellationReason::ItemCollectionSizeLimitExceeded
        }
        _ => CancellationReason::None,
    }
}

/// The rendered form of a [`Cond`]: a `ConditionExpression` plus the
/// placeholder names/values it references.
#[derive(Default)]
struct RenderedCond {
    expression: Option<String>,
    names: Vec<(String, String)>,
    values: Vec<(String, DynamoAv)>,
}

/// Renders a [`Cond`] to a `ConditionExpression` plus its placeholder names
/// and values.
fn render_cond(cond: &Cond) -> RenderedCond {
    match cond {
        Cond::None => RenderedCond::default(),
        Cond::NotExists => RenderedCond {
            expression: Some("attribute_not_exists(pk)".to_owned()),
            ..RenderedCond::default()
        },
        Cond::VersionEquals(version) => RenderedCond {
            expression: Some("version = :cond_version".to_owned()),
            values: vec![(":cond_version".to_owned(), DynamoAv::N(version.to_string()))],
            ..RenderedCond::default()
        },
        Cond::NotExistsOrExpired { now_epoch } => RenderedCond {
            expression: Some("attribute_not_exists(pk) OR expires_at < :cond_now".to_owned()),
            values: vec![(":cond_now".to_owned(), DynamoAv::N(now_epoch.to_string()))],
            ..RenderedCond::default()
        },
        Cond::All(checks) => {
            let mut clauses = Vec::with_capacity(checks.len());
            let mut names = Vec::new();
            let mut values = Vec::new();
            for (index, check) in checks.iter().enumerate() {
                match check {
                    Check::Eq(name, value) => {
                        let name_placeholder = format!("#c{index}");
                        let value_placeholder = format!(":c{index}");
                        names.push((name_placeholder.clone(), (*name).to_owned()));
                        values.push((value_placeholder.clone(), value.clone().into()));
                        clauses.push(format!("{name_placeholder} = {value_placeholder}"));
                    }
                    Check::In(name, options) => {
                        let name_placeholder = format!("#c{index}");
                        names.push((name_placeholder.clone(), (*name).to_owned()));
                        let mut option_placeholders = Vec::with_capacity(options.len());
                        for (option_index, option) in options.iter().enumerate() {
                            let value_placeholder = format!(":c{index}_{option_index}");
                            values.push((value_placeholder.clone(), option.clone().into()));
                            option_placeholders.push(value_placeholder);
                        }
                        clauses.push(format!(
                            "{name_placeholder} IN ({})",
                            option_placeholders.join(", ")
                        ));
                    }
                    Check::Exists(name) => {
                        let name_placeholder = format!("#c{index}");
                        names.push((name_placeholder.clone(), (*name).to_owned()));
                        clauses.push(format!("attribute_exists({name_placeholder})"));
                    }
                    Check::NotExists(name) => {
                        let name_placeholder = format!("#c{index}");
                        names.push((name_placeholder.clone(), (*name).to_owned()));
                        clauses.push(format!("attribute_not_exists({name_placeholder})"));
                    }
                }
            }
            RenderedCond {
                expression: Some(clauses.join(" AND ")),
                names,
                values,
            }
        }
    }
}

/// Renders one [`WriteOp`] to a `TransactWriteItem`.
fn to_transact_item(table_name: &str, op: &WriteOp) -> Result<TransactWriteItem, MailStoreError> {
    match op {
        WriteOp::Put { item, cond } => put_transact_item(table_name, item, cond),
        WriteOp::Update {
            pk,
            sk,
            set,
            remove,
            cond,
        } => update_transact_item(table_name, pk, sk, set, remove, cond),
        WriteOp::Delete { pk, sk } => delete_transact_item(table_name, pk, sk),
        WriteOp::AliasFirstWriter {
            pk,
            sk,
            message_id,
            thread_id,
        } => alias_transact_item(table_name, pk, sk, message_id, thread_id),
    }
}

fn put_transact_item(
    table_name: &str,
    item: &serde_dynamo::Item,
    cond: &Cond,
) -> Result<TransactWriteItem, MailStoreError> {
    let RenderedCond {
        expression,
        names,
        values,
    } = render_cond(cond);
    let mut builder = Put::builder().table_name(table_name);
    for (name, value) in item.inner() {
        builder = builder.item(name.clone(), value.clone().into());
    }
    if let Some(expression) = expression {
        builder = builder.condition_expression(expression);
    }
    for (name, value) in names {
        builder = builder.expression_attribute_names(name, value);
    }
    for (name, value) in values {
        builder = builder.expression_attribute_values(name, value);
    }
    let put = builder
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building Put: {e}")))?;
    Ok(TransactWriteItem::builder().put(put).build())
}

fn update_transact_item(
    table_name: &str,
    pk: &str,
    sk: &str,
    set: &[(String, serde_dynamo::AttributeValue)],
    remove: &[String],
    cond: &Cond,
) -> Result<TransactWriteItem, MailStoreError> {
    let RenderedCond {
        expression: cond_expression,
        mut names,
        mut values,
    } = render_cond(cond);
    let mut set_clauses = Vec::with_capacity(set.len());
    for (index, (name, value)) in set.iter().enumerate() {
        let name_placeholder = format!("#s{index}");
        let value_placeholder = format!(":s{index}");
        names.push((name_placeholder.clone(), name.clone()));
        values.push((value_placeholder.clone(), value.clone().into()));
        set_clauses.push(format!("{name_placeholder} = {value_placeholder}"));
    }
    let mut remove_clauses = Vec::with_capacity(remove.len());
    for (index, name) in remove.iter().enumerate() {
        let name_placeholder = format!("#r{index}");
        names.push((name_placeholder.clone(), name.clone()));
        remove_clauses.push(name_placeholder);
    }
    let mut expression_parts = Vec::new();
    if !set_clauses.is_empty() {
        expression_parts.push(format!("SET {}", set_clauses.join(", ")));
    }
    if !remove_clauses.is_empty() {
        expression_parts.push(format!("REMOVE {}", remove_clauses.join(", ")));
    }
    let mut builder = Update::builder()
        .table_name(table_name)
        .key("pk", DynamoAv::S(pk.to_owned()))
        .key("sk", DynamoAv::S(sk.to_owned()))
        .update_expression(expression_parts.join(" "));
    if let Some(expression) = cond_expression {
        builder = builder.condition_expression(expression);
    }
    for (name, value) in names {
        builder = builder.expression_attribute_names(name, value);
    }
    for (name, value) in values {
        builder = builder.expression_attribute_values(name, value);
    }
    let update = builder
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building Update: {e}")))?;
    Ok(TransactWriteItem::builder().update(update).build())
}

fn delete_transact_item(
    table_name: &str,
    pk: &str,
    sk: &str,
) -> Result<TransactWriteItem, MailStoreError> {
    let delete = Delete::builder()
        .table_name(table_name)
        .key("pk", DynamoAv::S(pk.to_owned()))
        .key("sk", DynamoAv::S(sk.to_owned()))
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building Delete: {e}")))?;
    Ok(TransactWriteItem::builder().delete(delete).build())
}

fn alias_transact_item(
    table_name: &str,
    pk: &str,
    sk: &str,
    message_id: &str,
    thread_id: &str,
) -> Result<TransactWriteItem, MailStoreError> {
    let put = Put::builder()
        .table_name(table_name)
        .item("pk", DynamoAv::S(pk.to_owned()))
        .item("sk", DynamoAv::S(sk.to_owned()))
        .item("message_id", DynamoAv::S(message_id.to_owned()))
        .item("thread_id", DynamoAv::S(thread_id.to_owned()))
        .condition_expression("attribute_not_exists(pk)")
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building alias Put: {e}")))?;
    Ok(TransactWriteItem::builder().put(put).build())
}

/// A `TransactWriteItems` attempt's outcome once the SDK call has returned.
enum Attempt {
    /// Every condition held; the writes were applied.
    Committed,
    /// The transaction was cancelled or is in progress; decoded per D27.
    Decision(TxnDecision),
}

impl AwsServices {
    async fn attempt_transaction(
        &self,
        kind: TxnKind,
        ops: &[PlannedOp],
    ) -> Result<Attempt, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let mut builder = self.dynamo.transact_write_items();
        for planned in ops {
            builder = builder.transact_items(to_transact_item(&table_name, &planned.op)?);
        }

        match builder.send().await {
            Ok(_) => Ok(Attempt::Committed),
            Err(error) => {
                if let SdkError::ServiceError(ctx) = &error {
                    match ctx.err() {
                        TransactWriteItemsError::TransactionCanceledException(cancelled) => {
                            let reasons: Vec<CancellationReason> = cancelled
                                .cancellation_reasons()
                                .iter()
                                .map(|reason| map_cancellation_reason(reason.code()))
                                .collect();
                            return Ok(Attempt::Decision(decode_cancellation(kind, ops, &reasons)));
                        }
                        TransactWriteItemsError::TransactionInProgressException(_) => {
                            return Ok(Attempt::Decision(TxnDecision::Retry));
                        }
                        TransactWriteItemsError::IdempotentParameterMismatchException(_) => {
                            return Err(MailStoreError::Permanent(anyhow!(
                                "transact_write_items: idempotent parameter mismatch"
                            )));
                        }
                        _ => {}
                    }
                }
                Err(store_error_from_sdk("TransactWriteItems", &error))
            }
        }
    }
}

fn item_attribute_string(item: &HashMap<String, DynamoAv>, name: &str) -> Option<String> {
    match item.get(name) {
        Some(DynamoAv::S(value)) => Some(value.clone()),
        _ => None,
    }
}

impl MailStore for AwsServices {
    async fn get_inbox(&self, inbox: &InboxId) -> Result<Option<Inbox>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::inbox_pk(inbox.as_str())))
            .key("sk", DynamoAv::S(keys::inbox_sk().to_owned()))
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(inbox)", &e))?;
        match output.item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item)
                .map(Some)
                .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing inbox: {e}"))),
        }
    }

    async fn ensure_inbox(&self, inbox: &InboxId, now: &str) -> Result<Inbox, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let record = Inbox {
            inbox_id: inbox.clone(),
            email: inbox.as_str().to_owned(),
            display_name: None,
            metadata: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        };
        let mut item: serde_dynamo::Item = serde_dynamo::to_item(&record)
            .map_err(|e| MailStoreError::Permanent(anyhow!("serializing inbox: {e}")))?;
        item.inner_mut().insert(
            "pk".to_owned(),
            DynamoAv::S(keys::inbox_pk(inbox.as_str())).into(),
        );
        item.inner_mut().insert(
            "sk".to_owned(),
            DynamoAv::S(keys::inbox_sk().to_owned()).into(),
        );

        let mut builder = self.dynamo.put_item().table_name(table_name.clone());
        for (name, value) in item.inner() {
            builder = builder.item(name.clone(), value.clone().into());
        }
        let result = builder
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match result {
            Ok(_) => Ok(record),
            Err(SdkError::ServiceError(ctx))
                if ctx.err().is_conditional_check_failed_exception() =>
            {
                self.get_inbox(inbox).await?.ok_or_else(|| {
                    MailStoreError::Permanent(anyhow!(
                        "inbox disappeared after a conditional check failure"
                    ))
                })
            }
            Err(error) => Err(store_error_from_sdk("PutItem(inbox)", &error)),
        }
    }

    async fn message_exists(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> Result<bool, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::inbox_pk(inbox.as_str())))
            .key("sk", DynamoAv::S(keys::message_sk(message_id)))
            .projection_expression("pk")
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(message_exists)", &e))?;
        Ok(output.item.is_some())
    }

    async fn resolve_rfc_ids(
        &self,
        inbox: &InboxId,
        candidates: &[String],
    ) -> Result<Option<RfcHit>, MailStoreError> {
        if candidates.is_empty() {
            return Ok(None);
        }
        let table_name = self.mail_table_name()?.to_owned();

        let mut pending_keys: Vec<HashMap<String, DynamoAv>> = candidates
            .iter()
            .map(|candidate| {
                HashMap::from([
                    (
                        "pk".to_owned(),
                        DynamoAv::S(keys::rfc_alias_pk(inbox.as_str(), candidate)),
                    ),
                    (
                        "sk".to_owned(),
                        DynamoAv::S(keys::rfc_alias_sk().to_owned()),
                    ),
                ])
            })
            .collect();

        let mut found: HashMap<String, RfcHit> = HashMap::new();
        let mut attempts = 0u32;
        while !pending_keys.is_empty() {
            if attempts >= MAX_BATCH_GET_ATTEMPTS {
                return Err(MailStoreError::Transient(anyhow!(
                    "resolve_rfc_ids: BatchGetItem left unprocessed keys after {MAX_BATCH_GET_ATTEMPTS} attempts"
                )));
            }
            if attempts > 0 {
                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempts))).await;
            }
            attempts += 1;

            let request_items = KeysAndAttributes::builder()
                .set_keys(Some(pending_keys.clone()))
                .consistent_read(true)
                .build()
                .map_err(|e| {
                    MailStoreError::Permanent(anyhow!("building KeysAndAttributes: {e}"))
                })?;
            let output = self
                .dynamo
                .batch_get_item()
                .request_items(table_name.clone(), request_items)
                .send()
                .await
                .map_err(|e| store_error_from_sdk("BatchGetItem(rfc_alias)", &e))?;

            if let Some(mut responses) = output.responses
                && let Some(items) = responses.remove(&table_name)
            {
                for item in items {
                    let Some(pk) = item_attribute_string(&item, "pk") else {
                        continue;
                    };
                    let Some(message_id) = item_attribute_string(&item, "message_id") else {
                        continue;
                    };
                    let Some(thread_id) = item_attribute_string(&item, "thread_id") else {
                        continue;
                    };
                    found.insert(
                        pk,
                        RfcHit {
                            message_id,
                            thread_id,
                        },
                    );
                }
            }

            pending_keys = output
                .unprocessed_keys
                .and_then(|mut m| m.remove(&table_name))
                .map(|attrs| attrs.keys().to_vec())
                .unwrap_or_default();
        }

        Ok(candidates
            .iter()
            .find_map(|candidate| found.get(&keys::rfc_alias_pk(inbox.as_str(), candidate)))
            .cloned())
    }

    async fn insert_message(&self, msg: &MailMessage) -> Result<InsertOutcome, MailStoreError> {
        for _attempt in 0..=MAX_TXN_RETRIES {
            // D3: read the thread consistently, compute its new state in
            // Rust, then plan the whole transaction fresh — re-read on every
            // retry, since a concurrent writer may have moved the thread's
            // version.
            let thread_before: Option<ThreadState> =
                self.get_thread_state(&msg.inbox_id, &msg.thread_id).await?;
            let thread_after = match &thread_before {
                Some(before) => apply_message(before, msg),
                None => new_thread(msg),
            };

            let ops = crate::mail::plan::plan_insert(msg, thread_before.as_ref(), &thread_after)?;

            match self.attempt_transaction(TxnKind::Insert, &ops).await? {
                Attempt::Committed => return Ok(InsertOutcome::Fresh),
                Attempt::Decision(TxnDecision::Duplicate) => return Ok(InsertOutcome::Duplicate),
                Attempt::Decision(TxnDecision::Retry | TxnDecision::VersionConflict) => {}
                Attempt::Decision(TxnDecision::Permanent | TxnDecision::KeyExists) => {
                    return Err(MailStoreError::Permanent(anyhow!(
                        "ingest transaction for message {} was cancelled",
                        msg.message_id
                    )));
                }
            }
        }
        Err(MailStoreError::Conflict)
    }

    async fn get_message(
        &self,
        inbox: &InboxId,
        message_id: &str,
    ) -> Result<Option<MailMessage>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::inbox_pk(inbox.as_str())))
            .key("sk", DynamoAv::S(keys::message_sk(message_id)))
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(message)", &e))?;
        match output.item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item)
                .map(Some)
                .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing message: {e}"))),
        }
    }
}
