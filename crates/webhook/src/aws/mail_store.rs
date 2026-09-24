//! [`MailStore`] for [`AwsServices`](crate::aws::AwsServices): the mail
//! table's DynamoDB access.
//!
//! Every write goes through the transaction model
//! ([`crate::mail::plan`]/[`crate::mail::txn`]): a planner builds
//! role-tagged [`PlannedOp`]s from a pure, in-memory computation, this module
//! renders them to a `TransactWriteItems` call, and
//! [`decode_cancellation`] turns a cancellation into one of a handful of
//! decisions the caller retries or surfaces. Reads and writes use
//! `serde_dynamo::to_item`/`from_item` on the same structs the planner
//! builds; this module only adds the key attributes those structs
//! don't carry themselves.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue as DynamoAv, KeysAndAttributes, Put, TransactWriteItem, Update,
};
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::aws::{self, AwsServices};
use crate::mail::flows;
use crate::mail::keys::{self, PageKey};
use crate::mail::plan::{Check, Cond, PlannedOp, WriteOp};
use crate::mail::send::{SendKey, SendState, SendStatus};
use crate::mail::store::{
    EnqueueOutcome, ListQuery, MailStore, MailStoreError, MarkOutcome, Page, ThreadView,
};
use crate::mail::thread::ThreadState;
use crate::mail::txn::{CancellationReason, TxnDecision, decode_cancellation};
use crate::mail::{Inbox, InboxId, InsertOutcome, MailMessage, RfcHit};

/// Waits before retrying a cancelled transaction: exponential from 25 ms,
/// with jitter so writers that collided do not collide again in step.
async fn backoff(attempt: u32) {
    let base_ms = 25_u64 << attempt.min(4);
    let jitter_ms = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    ) % base_ms;
    tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
}

/// `BatchGetItem` loops on `UnprocessedKeys` at most this many times before
/// surfacing a transient error.
const MAX_BATCH_GET_ATTEMPTS: u32 = 5;

impl AwsServices {
    /// The mail table name, or a permanent error when mail is disabled —
    /// every `MailStore` method is only ever called when it is enabled, but
    /// this keeps that assumption from becoming a panic if it's ever
    /// violated.
    /// One page of a time-ordered index partition, deserialized into `T`.
    ///
    /// Both list indexes are keyed the same way — a partition string plus a
    /// sort key that orders by time — so messages, threads and thread
    /// membership share this. `before`/`after` become an exclusive sort-key
    /// range, and the returned `next` is the last item's key so the caller's
    /// post-filtering can stop mid-page.
    async fn query_page<T: serde::de::DeserializeOwned>(
        &self,
        query: PageQuery<'_>,
        context: &'static str,
    ) -> Result<Page<T>, MailStoreError> {
        let partition = query.partition;
        let table_name = self.mail_table_name()?.to_owned();
        let index = query.index.name();
        let (pk_attr, sk_attr) = query.index.key_attributes();

        let key = KeyCondition::new(pk_attr, sk_attr, partition, query.after, query.before);

        let mut request = self
            .dynamo
            .query()
            .table_name(&table_name)
            .index_name(index)
            .key_condition_expression(key.expression)
            .set_expression_attribute_names(Some(key.names))
            .set_expression_attribute_values(Some(key.values))
            .scan_index_forward(query.ascending)
            .limit(i32::try_from(query.limit).unwrap_or(i32::MAX));
        if let Some(start) = query.start {
            request = request
                .exclusive_start_key("pk", DynamoAv::S(start.table_pk.clone()))
                .exclusive_start_key("sk", DynamoAv::S(start.table_sk.clone()))
                .exclusive_start_key(pk_attr, DynamoAv::S(start.partition.clone()))
                .exclusive_start_key(sk_attr, DynamoAv::S(start.sort.clone()));
        }

        let output = request
            .send()
            .await
            .map_err(|e| store_error_from_sdk(context, &e))?;

        // The continuation comes from DynamoDB's own LastEvaluatedKey, which
        // already holds both the index and table keys in the exact form the
        // next ExclusiveStartKey needs.
        let next = output
            .last_evaluated_key
            .as_ref()
            .and_then(|key| page_key_from_last_evaluated(key, pk_attr, sk_attr));

        let mut items = Vec::new();
        for item in output.items.unwrap_or_default() {
            items.push(
                serde_dynamo::from_item(item)
                    .map_err(|e| MailStoreError::Permanent(anyhow!("{context}: {e}")))?,
            );
        }
        Ok(Page { items, next })
    }

    fn mail_table_name(&self) -> Result<&str, MailStoreError> {
        self.mail_config()
            .map(|config| config.table_name.as_str())
            .ok_or_else(|| MailStoreError::Permanent(anyhow!("mail is not configured")))
    }
}

/// A `Query` key condition with exactly the placeholders it uses: DynamoDB
/// rejects a request whose `ExpressionAttributeNames` or
/// `ExpressionAttributeValues` hold an entry the expression never mentions,
/// so the sort-key name is declared only when a range bound needs it.
struct KeyCondition {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, DynamoAv>,
}

impl KeyCondition {
    fn new(
        pk_attr: &str,
        sk_attr: &str,
        partition: &str,
        after: Option<&str>,
        before: Option<&str>,
    ) -> Self {
        let mut expression = "#pk = :pk".to_owned();
        let mut names = HashMap::from([("#pk".to_owned(), pk_attr.to_owned())]);
        let mut values = HashMap::from([(":pk".to_owned(), DynamoAv::S(partition.to_owned()))]);
        let range = match (after, before) {
            (Some(after), Some(before)) => {
                values.insert(":after".to_owned(), DynamoAv::S(after.to_owned()));
                values.insert(":before".to_owned(), DynamoAv::S(before.to_owned()));
                // Not BETWEEN, which includes both ends: `ListQuery`'s bounds
                // are exclusive, and they are exclusive in the one-sided
                // cases below, so an item exactly on a bound must not depend
                // on whether the caller gave the other one.
                Some(" AND #sk > :after AND #sk < :before")
            }
            (Some(after), None) => {
                values.insert(":after".to_owned(), DynamoAv::S(after.to_owned()));
                Some(" AND #sk > :after")
            }
            (None, Some(before)) => {
                values.insert(":before".to_owned(), DynamoAv::S(before.to_owned()));
                Some(" AND #sk < :before")
            }
            (None, None) => None,
        };
        if let Some(range) = range {
            expression.push_str(range);
            names.insert("#sk".to_owned(), sk_attr.to_owned());
        }
        Self {
            expression,
            names,
            values,
        }
    }
}

/// One page of one index partition, independent of which collection is being
/// listed: inboxes are not inbox-scoped, so this carries the partition rather
/// than deriving it from a [`ListQuery`]'s inbox.
struct PageQuery<'a> {
    /// Which index to read. Named rather than inferred from `partition`:
    /// guessing it from a string prefix silently sent status queries to the
    /// wrong index, which returns an empty page rather than an error.
    index: MailIndex,
    partition: &'a str,
    limit: usize,
    before: Option<&'a str>,
    after: Option<&'a str>,
    ascending: bool,
    start: Option<&'a PageKey>,
}

/// The mail table's secondary indexes, each with the attribute pair it is
/// keyed by.
///
/// The names match the index names in `template.yaml`, which is why they
/// share a prefix.
#[expect(
    clippy::enum_variant_names,
    reason = "these are the index names the template declares"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MailIndex {
    /// Inboxes, messages and threads, ordered by time.
    ByTime,
    /// One thread's messages.
    ByThread,
    /// Send states, by status. Sparse: only send-state items carry its keys.
    ByStatus,
}

impl MailIndex {
    fn name(self) -> &'static str {
        match self {
            Self::ByTime => "ByTime",
            Self::ByThread => "ByThread",
            Self::ByStatus => "ByStatus",
        }
    }

    fn key_attributes(self) -> (&'static str, &'static str) {
        match self {
            Self::ByTime => ("gsi1pk", "gsi1sk"),
            Self::ByThread => ("gsi2pk", "gsi2sk"),
            Self::ByStatus => ("gsi3pk", "gsi3sk"),
        }
    }
}

impl<'a> PageQuery<'a> {
    /// The inbox-scoped case: a [`ListQuery`] aimed at `partition`.
    fn scoped(index: MailIndex, partition: &'a str, query: &'a ListQuery) -> Self {
        Self {
            index,
            partition,
            limit: query.limit,
            before: query.before.as_deref(),
            after: query.after.as_deref(),
            ascending: query.ascending,
            start: query.start.as_ref(),
        }
    }
}

/// Turns a `LastEvaluatedKey` into a [`PageKey`].
///
/// Returns `None` when any of the four attributes is missing or is not a
/// string, which would make the continuation unusable; the caller then
/// reports the page as the last one rather than handing back a token that
/// would fail on presentation.
fn page_key_from_last_evaluated(
    key: &HashMap<String, DynamoAv>,
    pk_attr: &str,
    sk_attr: &str,
) -> Option<PageKey> {
    let string = |name: &str| match key.get(name) {
        Some(DynamoAv::S(value)) => Some(value.clone()),
        _ => None,
    };
    Some(PageKey {
        partition: string(pk_attr)?,
        sort: string(sk_attr)?,
        table_pk: string("pk")?,
        table_sk: string("sk")?,
    })
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

/// Maps a `CancellationReason`'s `code()` to our enum.
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
                let Check::Eq(name, value) = check;
                let name_placeholder = format!("#c{index}");
                let value_placeholder = format!(":c{index}");
                names.push((name_placeholder.clone(), (*name).to_owned()));
                values.push((value_placeholder.clone(), value.clone().into()));
                clauses.push(format!("{name_placeholder} = {value_placeholder}"));
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
        WriteOp::AliasFirstWriter {
            pk,
            sk,
            message_id,
            thread_id,
            expires_at,
        } => alias_transact_item(table_name, pk, sk, message_id, thread_id, *expires_at),
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

fn alias_transact_item(
    table_name: &str,
    pk: &str,
    sk: &str,
    message_id: &str,
    thread_id: &str,
    expires_at: u64,
) -> Result<TransactWriteItem, MailStoreError> {
    let put = Put::builder()
        .table_name(table_name)
        .item("pk", DynamoAv::S(pk.to_owned()))
        .item("sk", DynamoAv::S(sk.to_owned()))
        .item("message_id", DynamoAv::S(message_id.to_owned()))
        .item("thread_id", DynamoAv::S(thread_id.to_owned()))
        .item("expires_at", DynamoAv::N(expires_at.to_string()))
        .condition_expression("attribute_not_exists(pk)")
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building alias Put: {e}")))?;
    Ok(TransactWriteItem::builder().put(put).build())
}

impl AwsServices {
    async fn attempt_transaction(
        &self,
        ops: &[PlannedOp],
    ) -> Result<flows::TxnOutcome, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let mut builder = self.dynamo.transact_write_items();
        for planned in ops {
            builder = builder.transact_items(to_transact_item(&table_name, &planned.op)?);
        }

        match builder.send().await {
            Ok(_) => Ok(flows::TxnOutcome::Committed),
            Err(error) => {
                if let SdkError::ServiceError(ctx) = &error {
                    match ctx.err() {
                        TransactWriteItemsError::TransactionCanceledException(cancelled) => {
                            let reasons: Vec<CancellationReason> = cancelled
                                .cancellation_reasons()
                                .iter()
                                .map(|reason| map_cancellation_reason(reason.code()))
                                .collect();
                            return Ok(flows::TxnOutcome::Cancelled(decode_cancellation(
                                ops, &reasons,
                            )));
                        }
                        TransactWriteItemsError::TransactionInProgressException(_) => {
                            return Ok(flows::TxnOutcome::Cancelled(TxnDecision::Retry));
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

impl flows::TxnStore for AwsServices {
    async fn read_item<T: serde::de::DeserializeOwned + Send>(
        &self,
        pk: &str,
        sk: &str,
    ) -> Result<Option<T>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(pk.to_owned()))
            .key("sk", DynamoAv::S(sk.to_owned()))
            // Consistent: what is read here conditions the next write.
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem", &e))?;
        output
            .item
            .map(|item| {
                serde_dynamo::from_item(item)
                    .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing item: {e}")))
            })
            .transpose()
    }

    async fn run_txn(&self, ops: &[PlannedOp]) -> Result<flows::TxnOutcome, MailStoreError> {
        self.attempt_transaction(ops).await
    }

    async fn pause(&self, attempt: u32) {
        backoff(attempt).await;
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
            // Consistent: `ensure_inbox`'s conditional-check-failed arm relies on
            // this read observing the inbox the strongly-consistent
            // `ConditionExpression` just proved exists. An eventually-consistent
            // read can hit a stale replica during replication lag and return
            // `None`, tripping the "inbox disappeared" guard.
            .consistent_read(true)
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
        let (record, item) = crate::mail::plan::inbox_item(inbox, now)?;

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
        flows::insert_message(self, msg).await
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
            // Consistent: the version read here conditions the next write.
            .consistent_read(true)
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

    async fn get_send_key(&self, key_hash: &str) -> Result<Option<SendKey>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::send_key_pk(key_hash)))
            .key("sk", DynamoAv::S(keys::send_key_sk().to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(send key)", &e))?;
        match output.item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item)
                .map(Some)
                .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing send key: {e}"))),
        }
    }

    async fn enqueue_send(
        &self,
        msg: &MailMessage,
        state: &SendState,
        key: Option<&SendKey>,
        now_epoch: u64,
    ) -> Result<EnqueueOutcome, MailStoreError> {
        flows::enqueue_send(self, msg, state, key, now_epoch).await
    }

    async fn resolve_ses_message(
        &self,
        ses_message_id: &str,
    ) -> Result<Option<(InboxId, String)>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::ses_ref_pk(ses_message_id)))
            .key("sk", DynamoAv::S(keys::ses_ref_sk().to_owned()))
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(ses ref)", &e))?;
        let Some(item) = output.item else {
            return Ok(None);
        };
        let inbox = match item.get("inbox_id") {
            Some(DynamoAv::S(value)) => value.clone(),
            _ => return Ok(None),
        };
        let message_id = match item.get("message_id") {
            Some(DynamoAv::S(value)) => value.clone(),
            _ => return Ok(None),
        };
        Ok(Some((InboxId(inbox), message_id)))
    }

    async fn list_by_status(
        &self,
        status: SendStatus,
        limit: usize,
    ) -> Result<Vec<SendState>, MailStoreError> {
        let page: Page<SendState> = self
            .query_page(
                PageQuery {
                    index: MailIndex::ByStatus,
                    partition: &format!("SENDSTATUS#{}", status.as_str()),
                    limit,
                    before: None,
                    after: None,
                    ascending: true,
                    start: None,
                },
                "listing sends by status",
            )
            .await?;
        Ok(page.items)
    }

    async fn get_send_state(&self, message_id: &str) -> Result<Option<SendState>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .dynamo
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(keys::outbox_pk(message_id)))
            .key("sk", DynamoAv::S(keys::outbox_sk().to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| store_error_from_sdk("GetItem(send state)", &e))?;
        match output.item {
            None => Ok(None),
            Some(item) => serde_dynamo::from_item(item)
                .map(Some)
                .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing send state: {e}"))),
        }
    }

    async fn claim_send(
        &self,
        message_id: &str,
        now: &str,
    ) -> Result<Option<SendState>, MailStoreError> {
        flows::claim_send(self, message_id, now).await
    }

    async fn note_ses_call(
        &self,
        claimed: &SendState,
        now: &str,
    ) -> Result<Option<SendState>, MailStoreError> {
        flows::note_ses_call(self, claimed, now).await
    }

    async fn mark_send(
        &self,
        state: &SendState,
        outcome: MarkOutcome<'_>,
        now: &str,
    ) -> Result<(), MailStoreError> {
        flows::mark_send(self, state, outcome, now).await
    }

    async fn update_labels(
        &self,
        inbox: &InboxId,
        message_id: &str,
        add: &[String],
        remove: &[String],
        now: &str,
    ) -> Result<Option<Vec<String>>, MailStoreError> {
        flows::update_labels(self, inbox, message_id, add, remove, now).await
    }

    async fn list_inboxes(
        &self,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Page<Inbox>, MailStoreError> {
        self.query_page(
            PageQuery {
                index: MailIndex::ByTime,
                partition: keys::inboxes_partition(),
                limit,
                before: None,
                after: None,
                ascending: true,
                start: start.as_ref(),
            },
            "listing inboxes",
        )
        .await
    }

    async fn list_messages(&self, query: &ListQuery) -> Result<Page<MailMessage>, MailStoreError> {
        let partition = keys::messages_partition(query.inbox.as_str());
        self.query_page(
            PageQuery::scoped(MailIndex::ByTime, &partition, query),
            "listing messages",
        )
        .await
    }

    async fn list_threads(&self, query: &ListQuery) -> Result<Page<ThreadState>, MailStoreError> {
        let partition = keys::threads_partition(query.inbox.as_str());
        self.query_page(
            PageQuery::scoped(MailIndex::ByTime, &partition, query),
            "listing threads",
        )
        .await
    }

    async fn get_thread(
        &self,
        inbox: &InboxId,
        thread_id: &str,
        limit: usize,
        start: Option<PageKey>,
    ) -> Result<Option<ThreadView>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        // Messages of one thread, oldest first: ByThread is keyed by the
        // message id, which orders by time. The thread item and its messages
        // are independent reads, so they go out together rather than one
        // after the other.
        let partition = keys::thread_messages_partition(inbox.as_str(), thread_id);
        let read_thread = self
            .dynamo
            .get_item()
            .table_name(&table_name)
            .key("pk", DynamoAv::S(keys::inbox_pk(inbox.as_str())))
            .key("sk", DynamoAv::S(keys::thread_sk(thread_id)))
            .send();
        let read_messages = self.query_page(
            PageQuery {
                index: MailIndex::ByThread,
                partition: &partition,
                limit,
                before: None,
                after: None,
                ascending: true,
                start: start.as_ref(),
            },
            "listing thread messages",
        );
        let (thread, messages) = tokio::join!(read_thread, read_messages);

        let thread = thread
            .map_err(|e| store_error_from_sdk("GetItem(thread)", &e))?
            .item;
        assemble_thread_view(thread.map(serde_dynamo::Item::from), messages)
    }
}

/// Assembles a `ThreadView` from the two reads [`AwsServices::get_thread`] runs
/// concurrently via `tokio::join!`: the base-table thread `GetItem` and the
/// `ByThread` GSI `Query` for the thread's messages.
///
/// Extracted pure so the divergent-result decision — what to do when one read
/// succeeded and the other failed — is unit-testable without an AWS client.
/// The load-bearing rule: a `messages` failure coinciding with an absent thread
/// is surfaced, not dropped. `tokio::join!` drives both reads to completion, so
/// by the time the thread item is known to be absent the `messages` `Result` is
/// already resolved; returning `Ok(None)` without applying `?` would mask a
/// retry-exhausted `ByThread` failure as a definitive `404 NotFound` (the API
/// handler maps `Ok(None)` to `NotFound`) instead of `MailStoreError::Transient`
/// /`Permanent` (`502`/`500`), hiding it from the service's internal-error
/// metrics.
fn assemble_thread_view(
    thread: Option<serde_dynamo::Item>,
    messages: Result<Page<MailMessage>, MailStoreError>,
) -> Result<Option<ThreadView>, MailStoreError> {
    let Some(thread) = thread else {
        // The messages query may have run for nothing; a thread that doesn't
        // exist is the rarer case than one that does — but its result must be
        // surfaced first so a `ByThread` failure coinciding with an absent
        // thread is not masked as a definitive 404.
        messages?;
        return Ok(None);
    };
    let thread: ThreadState = serde_dynamo::from_item(thread)
        .map_err(|e| MailStoreError::Permanent(anyhow!("deserializing thread: {e}")))?;
    let messages = messages?;
    Ok(Some(ThreadView { thread, messages }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every declared placeholder must appear in the expression, and every
    /// placeholder in the expression must be declared.
    fn assert_placeholders_match(key: &KeyCondition) {
        for name in key.names.keys() {
            assert!(
                key.expression.contains(name.as_str()),
                "{name} declared but unused in {:?}",
                key.expression
            );
        }
        for value in key.values.keys() {
            assert!(
                key.expression.contains(value.as_str()),
                "{value} declared but unused in {:?}",
                key.expression
            );
        }
        for token in key.expression.split_whitespace() {
            if token.starts_with('#') {
                assert!(key.names.contains_key(token), "{token} undeclared");
            }
            if token.starts_with(':') {
                assert!(key.values.contains_key(token), "{token} undeclared");
            }
        }
    }

    #[test]
    fn an_unbounded_page_declares_only_the_partition_key() {
        let key = KeyCondition::new("gsi1pk", "gsi1sk", "INBOXES", None, None);
        assert_eq!(key.expression, "#pk = :pk");
        assert!(!key.names.contains_key("#sk"));
        assert_placeholders_match(&key);
    }

    /// `ListQuery`'s bounds are exclusive, and a caller who gives both must
    /// get the same items they would get from two one-sided queries —
    /// `BETWEEN` would quietly include the endpoints instead.
    #[test]
    fn both_bounds_stay_exclusive() {
        let key = KeyCondition::new("gsi1pk", "gsi1sk", "INBOX#x#MSG", Some("a"), Some("b"));
        assert_eq!(
            key.expression,
            "#pk = :pk AND #sk > :after AND #sk < :before"
        );
        assert!(!key.expression.contains("BETWEEN"));
        assert_placeholders_match(&key);
    }

    #[test]
    fn every_range_bound_declares_the_sort_key() {
        for (after, before) in [(Some("a"), None), (None, Some("b")), (Some("a"), Some("b"))] {
            let key = KeyCondition::new("gsi1pk", "gsi1sk", "INBOX#x#MSG", after, before);
            assert_eq!(key.names.get("#sk").map(String::as_str), Some("gsi1sk"));
            assert_placeholders_match(&key);
        }
    }

    // --- get_thread: assemble_thread_view ---------------------------------
    //
    // `get_thread` runs the base-table thread `GetItem` and the `ByThread` GSI
    // `Query` concurrently via `tokio::join!`, so by the time the thread item
    // is known to be absent the messages `Result` is already resolved. These
    // tests pin the divergent-result contract the AWS client hands to
    // `assemble_thread_view`: a dependency failure on the messages read must
    // not be masked as a definitive `Ok(None)` (which the `get_thread` API
    // handler maps to a non-retryable `404 NotFound`) just because the thread
    // happened to be absent.

    /// The regression: a retry-exhausted `ByThread` `Query` (throttle/5xx/transport)
    /// that coincides with an absent thread surfaces as `Transient` (`502`),
    /// not a dropped `Result` reported as `Ok(None)` → `404`.
    #[test]
    fn thread_absent_with_transient_messages_failure_surfaces_the_error() {
        let messages: Result<Page<MailMessage>, MailStoreError> = Err(MailStoreError::Transient(
            anyhow!("listing thread messages: throttled"),
        ));
        assert!(matches!(
            assemble_thread_view(None, messages),
            Err(MailStoreError::Transient(_))
        ));
    }

    /// A healthy, empty `Query` alongside a genuinely-absent thread is still a
    /// routine `404`: the fix surfaces errors only, it never invents a thread.
    #[test]
    fn thread_absent_with_healthy_empty_messages_returns_none() {
        let empty: Page<MailMessage> = Page {
            items: vec![],
            next: None,
        };
        assert!(matches!(assemble_thread_view(None, Ok(empty)), Ok(None)));
    }

    // --- get_inbox: consistent_read regression guard ----------------------
    //
    // `ensure_inbox`'s `PutItem` `ConditionExpression` is evaluated strongly
    // consistently, so a `ConditionalCheckFailedException` proves the inbox
    // exists. The recovery read in `ensure_inbox` is `get_inbox`, so it must
    // also read strongly consistently — otherwise an eventually-consistent
    // `GetItem` can hit a stale replica during DynamoDB replication lag and
    // return `Ok(None)`, tripping the "inbox disappeared after a conditional
    // check failure" guard. That spurious `Permanent` surfaces from ingest as
    // `Ok("ingest_failed")` (HTTP 200), which the SNS→Lambda ingress treats
    // as success, silently dropping the inbound email with no redelivery.
    //
    // The stale-read race itself can't be reproduced in a unit test
    // (DynamoDB-Local/localstack are single-node; the in-memory test double
    // has no replica), so this pins the *property* the fix restores directly
    // against the AWS-backed `MailStore`: the `GetItem` `get_inbox` emits
    // must carry `consistent_read = true`. The seam is a request interceptor
    // on a real `aws_sdk_dynamodb::Client`: it records the typed `GetItemInput`
    // in `read_before_execution` (always available) and short-circuits the
    // execution with an error so the request never reaches a real DynamoDB
    // endpoint (no network, no retries, deterministic). This guards the
    // exact asymmetry that let the bug land: every other read in this file
    // that conditions a follow-up write sets `.consistent_read(true)`;
    // `get_inbox` must not regress to eventually-consistent.

    use aws_sdk_dynamodb::config::Intercept;
    use aws_sdk_dynamodb::config::interceptors::BeforeSerializationInterceptorContextRef;
    use aws_sdk_dynamodb::operation::get_item::GetItemInput;
    use std::sync::{Arc, Mutex};

    /// Mail table name used by the capturing-dynamo fixtures below.
    const MAIL_TABLE_NAME: &str = "mail-table";

    /// Interceptor that records the typed `GetItemInput` for every `GetItem`
    /// and then aborts the execution so the test never touches the network.
    #[derive(Debug)]
    struct CaptureGetItemInput(Arc<Mutex<Vec<GetItemInput>>>);

    impl Intercept for CaptureGetItemInput {
        fn name(&self) -> &'static str {
            "CaptureGetItemInput"
        }

        fn read_before_execution(
            &self,
            context: &BeforeSerializationInterceptorContextRef<'_>,
            _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
        ) -> Result<(), aws_sdk_dynamodb::error::BoxError> {
            if let Some(input) = context.input().downcast_ref::<GetItemInput>()
                && let Ok(mut guard) = self.0.lock()
            {
                guard.push(input.clone());
            }
            // Short-circuit after capture: a real DynamoDB endpoint isn't
            // available in a unit test, and retries/endpoint resolution would
            // make the test slow and non-deterministic. The captured input is
            // all we assert on, so the surfaced (permanent) error is discarded.
            let err: aws_sdk_dynamodb::error::BoxError =
                String::from("interceptor short-circuit: request captured for assertion").into();
            Err(err)
        }
    }

    /// Builds an `AwsServices` whose DynamoDB client records every `GetItem`
    /// input it would send and short-circuits before transmission.
    fn aws_with_capturing_dynamo(captured: Arc<Mutex<Vec<GetItemInput>>>) -> AwsServices {
        let dynamo_conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .retry_config(aws_smithy_types::retry::RetryConfig::disabled())
            .interceptor(CaptureGetItemInput(captured))
            .build();
        let sdk = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let mut aws = AwsServices::new(
            &sdk,
            crate::config::Config {
                table_name: "events".to_owned(),
                event_bus_name: "bus".to_owned(),
                event_source: "aws-messaging-webhook".to_owned(),
                auto_resubscribe: false,
                opt_out_list_name: None,
                raw_event_retention_days: 30,
                aggregate_retention_days: 365,
                mode: crate::config::FunctionMode::Webhook,
                mail: Some(crate::config::MailConfig {
                    domain: "example.com".to_owned(),
                    table_name: MAIL_TABLE_NAME.to_owned(),
                    bucket: "mail-bucket".to_owned(),
                    inbox: "support@example.com".to_owned(),
                    configuration_set: "config-set".to_owned(),
                    identity_arn: "arn:aws:ses:us-east-1:123456789012:identity/example.com"
                        .to_owned(),
                    api_keys_parameter: "/example/api-keys".to_owned(),
                    attachment_url_ttl: std::time::Duration::from_secs(3600),
                    region: "us-east-1".to_owned(),
                    retention_days: 365,
                }),
            },
        );
        aws.dynamo = aws_sdk_dynamodb::Client::from_conf(dynamo_conf);
        aws
    }

    /// The `GetItem` emitted by `get_inbox` reads strongly consistently, so the
    /// conditional-check recovery in `ensure_inbox` cannot observe a stale
    /// replica after a `PutItem` conditional-check failure.
    #[tokio::test]
    async fn get_inbox_reads_strongly_consistently() {
        let captured = Arc::new(Mutex::new(Vec::<GetItemInput>::new()));
        let aws = aws_with_capturing_dynamo(Arc::clone(&captured));

        let inbox = InboxId("support@example.com".to_owned());
        // The interceptor short-circuits every request with an error, so the
        // call surfaces a (permanent) store error; only the emitted request
        // is asserted on.
        let _ = MailStore::get_inbox(&aws, &inbox).await;

        let requests: Vec<GetItemInput> = captured
            .lock()
            .map(|guard| guard.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        assert_eq!(
            requests.len(),
            1,
            "get_inbox must emit exactly one GetItem request"
        );
        let request = &requests[0];
        assert_eq!(
            request.consistent_read(),
            Some(true),
            "get_inbox must read strongly consistently so the conditional-check \
             recovery in ensure_inbox cannot observe a stale replica"
        );
        assert_eq!(request.table_name(), Some(MAIL_TABLE_NAME));
        let key = request.key();
        let pk = key.and_then(|k| k.get("pk"));
        let sk = key.and_then(|k| k.get("sk"));
        assert_eq!(pk, Some(&DynamoAv::S(keys::inbox_pk(inbox.as_str()))));
        assert_eq!(sk, Some(&DynamoAv::S(keys::inbox_sk().to_owned())));
    }
}
