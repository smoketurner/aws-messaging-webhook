//! [`Table`] over `DynamoDB`: the mail table's `GetItem`, `BatchGetItem`,
//! `Query`, conditional `PutItem` and `TransactWriteItems`.
//!
//! Every write goes through the transaction model
//! ([`crate::plan`]/[`crate::txn`]): a planner builds
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
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue as DynamoAv, KeysAndAttributes, Put, TransactWriteItem, Update,
};

use crate::keys::{PageKey, TableKey};
use crate::plan::{Check, Cond, PlannedOp, WriteOp};
use crate::sdk_error::FromSdkFailure;
use crate::store::{MailStoreError, Page};
use crate::table::{PageQuery, Read, Table, TxnOutcome};
use crate::txn::{CancellationReason, TxnDecision, decode_cancellation};
use mailbox::PutOutcome;

/// `BatchGetItem` loops on `UnprocessedKeys` at most this many times before
/// surfacing a transient error.
const MAX_BATCH_GET_ATTEMPTS: u32 = 5;

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

impl FromSdkFailure for MailStoreError {
    fn transient(source: anyhow::Error) -> Self {
        Self::Transient(source)
    }

    fn permanent(source: anyhow::Error) -> Self {
        Self::Permanent(source)
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
        } => alias_transact_item(
            table_name,
            pk,
            sk,
            message_id.as_deref(),
            thread_id,
            *expires_at,
        ),
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
    message_id: Option<&str>,
    thread_id: &str,
    expires_at: u64,
) -> Result<TransactWriteItem, MailStoreError> {
    let mut put = Put::builder()
        .table_name(table_name)
        .item("pk", DynamoAv::S(pk.to_owned()))
        .item("sk", DynamoAv::S(sk.to_owned()))
        .item("thread_id", DynamoAv::S(thread_id.to_owned()))
        .item("expires_at", DynamoAv::N(expires_at.to_string()));
    put = match message_id {
        // A real alias takes over a placeholder (a row with no message id)
        // but never another message's alias.
        Some(message_id) => put
            .item("message_id", DynamoAv::S(message_id.to_owned()))
            .condition_expression("attribute_not_exists(pk) OR attribute_not_exists(message_id)"),
        None => put.condition_expression("attribute_not_exists(pk)"),
    };
    let put = put
        .build()
        .map_err(|e| MailStoreError::Permanent(anyhow!("building alias Put: {e}")))?;
    Ok(TransactWriteItem::builder().put(put).build())
}

/// The mail table.
pub struct DynamoTable {
    client: aws_sdk_dynamodb::Client,
    /// `None` when mail is not configured.
    table_name: Option<String>,
}

impl DynamoTable {
    #[must_use]
    pub fn new(client: aws_sdk_dynamodb::Client, table_name: Option<String>) -> Self {
        Self { client, table_name }
    }

    /// The mail table name, or a permanent error when mail is disabled —
    /// every `Table` method is only ever called when it is enabled, but
    /// this keeps that assumption from becoming a panic if it's ever
    /// violated.
    fn mail_table_name(&self) -> Result<&str, MailStoreError> {
        self.table_name
            .as_deref()
            .ok_or_else(|| MailStoreError::Permanent(anyhow!("mail is not configured")))
    }
}

/// The `pk`/`sk` an item returned by a read carries, so a batch read can
/// say which key each item answers.
fn item_key(item: &HashMap<String, DynamoAv>) -> Option<TableKey> {
    let string = |name: &str| match item.get(name) {
        Some(DynamoAv::S(value)) => Some(value.clone()),
        _ => None,
    };
    Some(TableKey {
        pk: string("pk")?,
        sk: string("sk")?,
    })
}

impl Table for DynamoTable {
    async fn get<T: serde::de::DeserializeOwned + Send>(
        &self,
        TableKey { pk, sk }: TableKey,
        read: Read,
        context: &'static str,
    ) -> Result<Option<T>, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let output = self
            .client
            .get_item()
            .table_name(table_name)
            .key("pk", DynamoAv::S(pk))
            .key("sk", DynamoAv::S(sk))
            .consistent_read(read == Read::Consistent)
            .send()
            .await
            .map_err(|e| MailStoreError::from_sdk(context, &e))?;
        output
            .item
            .map(|item| {
                serde_dynamo::from_item(item)
                    .map_err(|e| MailStoreError::Permanent(anyhow!("{context}: {e}")))
            })
            .transpose()
    }

    async fn batch_get<T: serde::de::DeserializeOwned + Send>(
        &self,
        keys: Vec<TableKey>,
        read: Read,
        context: &'static str,
    ) -> Result<HashMap<TableKey, T>, MailStoreError> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let table_name = self.mail_table_name()?.to_owned();

        let mut pending_keys: Vec<HashMap<String, DynamoAv>> = keys
            .into_iter()
            .map(|TableKey { pk, sk }| {
                HashMap::from([
                    ("pk".to_owned(), DynamoAv::S(pk)),
                    ("sk".to_owned(), DynamoAv::S(sk)),
                ])
            })
            .collect();

        let mut found = HashMap::new();
        let mut attempts = 0u32;
        while !pending_keys.is_empty() {
            if attempts >= MAX_BATCH_GET_ATTEMPTS {
                return Err(MailStoreError::Transient(anyhow!(
                    "{context}: BatchGetItem left unprocessed keys after {MAX_BATCH_GET_ATTEMPTS} attempts"
                )));
            }
            if attempts > 0 {
                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempts))).await;
            }
            attempts += 1;

            let request_items = KeysAndAttributes::builder()
                .set_keys(Some(pending_keys.clone()))
                .consistent_read(read == Read::Consistent)
                .build()
                .map_err(|e| {
                    MailStoreError::Permanent(anyhow!("building KeysAndAttributes: {e}"))
                })?;
            let output = self
                .client
                .batch_get_item()
                .request_items(table_name.clone(), request_items)
                .send()
                .await
                .map_err(|e| MailStoreError::from_sdk(context, &e))?;

            if let Some(mut responses) = output.responses
                && let Some(items) = responses.remove(&table_name)
            {
                for item in items {
                    let Some(key) = item_key(&item) else {
                        continue;
                    };
                    let Ok(value) = serde_dynamo::from_item(item) else {
                        continue;
                    };
                    found.insert(key, value);
                }
            }

            pending_keys = output
                .unprocessed_keys
                .and_then(|mut m| m.remove(&table_name))
                .map(|attrs| attrs.keys().to_vec())
                .unwrap_or_default();
        }
        Ok(found)
    }

    async fn query<T: serde::de::DeserializeOwned + Send>(
        &self,
        query: PageQuery<'_>,
    ) -> Result<Page<T>, MailStoreError> {
        let context = query.context;
        let table_name = self.mail_table_name()?.to_owned();
        let index = query.index.name();
        let (pk_attr, sk_attr) = query.index.key_attributes();

        let key = KeyCondition::new(pk_attr, sk_attr, query.partition, query.after, query.before);

        let mut request = self
            .client
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
            .map_err(|e| MailStoreError::from_sdk(context, &e))?;

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

    async fn put_if_absent(
        &self,
        item: serde_dynamo::Item,
        context: &'static str,
    ) -> Result<PutOutcome, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let mut builder = self.client.put_item().table_name(table_name);
        for (name, value) in item.into_inner() {
            builder = builder.item(name, value.into());
        }
        let result = builder
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match result {
            Ok(_) => Ok(PutOutcome::Created),
            Err(SdkError::ServiceError(ctx))
                if ctx.err().is_conditional_check_failed_exception() =>
            {
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(MailStoreError::from_sdk(context, &error)),
        }
    }

    async fn transact(&self, ops: &[PlannedOp]) -> Result<TxnOutcome, MailStoreError> {
        let table_name = self.mail_table_name()?.to_owned();
        let mut builder = self.client.transact_write_items();
        for planned in ops {
            builder = builder.transact_items(to_transact_item(&table_name, &planned.op)?);
        }

        match builder.send().await {
            Ok(_) => Ok(TxnOutcome::Committed),
            Err(error) => {
                if let SdkError::ServiceError(ctx) = &error {
                    match ctx.err() {
                        TransactWriteItemsError::TransactionCanceledException(cancelled) => {
                            let reasons: Vec<CancellationReason> = cancelled
                                .cancellation_reasons()
                                .iter()
                                .map(|reason| map_cancellation_reason(reason.code()))
                                .collect();
                            return Ok(TxnOutcome::Cancelled(decode_cancellation(ops, &reasons)));
                        }
                        TransactWriteItemsError::TransactionInProgressException(_) => {
                            return Ok(TxnOutcome::Cancelled(TxnDecision::Retry));
                        }
                        TransactWriteItemsError::IdempotentParameterMismatchException(_) => {
                            return Err(MailStoreError::Permanent(anyhow!(
                                "transact_write_items: idempotent parameter mismatch"
                            )));
                        }
                        _ => {}
                    }
                }
                Err(MailStoreError::from_sdk("TransactWriteItems", &error))
            }
        }
    }

    /// Waits before retrying a cancelled transaction: exponential from 25 ms,
    /// with jitter so writers that collided do not collide again in step.
    async fn pause(&self, attempt: u32) {
        let base_ms = 25_u64 << attempt.min(4);
        let jitter_ms = u64::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos(),
        ) % base_ms;
        tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys;
    use crate::mailbox::Mailbox;
    use mailbox::{Inbox, InboxId};
    use mailbox::{MessageId, SesMessageId, ThreadId};

    fn alias_put(message_id: Option<&str>) -> aws_sdk_dynamodb::types::Put {
        let op = WriteOp::AliasFirstWriter {
            pk: "RFC#support@example.com#a@example.com".to_owned(),
            sk: "RFC".to_owned(),
            message_id: message_id.map(str::to_owned),
            thread_id: "tid-1".to_owned(),
            expires_at: 1_900_000_000,
        };
        to_transact_item("mail", &op)
            .unwrap()
            .put
            .expect("an alias renders as a Put")
    }

    #[test]
    fn a_real_alias_may_land_on_a_placeholder_but_not_on_another_alias() {
        let put = alias_put(Some("mid-1"));
        assert_eq!(
            put.condition_expression(),
            Some("attribute_not_exists(pk) OR attribute_not_exists(message_id)")
        );
        assert_eq!(
            put.item().get("message_id"),
            Some(&DynamoAv::S("mid-1".to_owned()))
        );
    }

    #[test]
    fn a_placeholder_lands_only_where_no_row_exists_and_carries_no_message_id() {
        let put = alias_put(None);
        assert_eq!(put.condition_expression(), Some("attribute_not_exists(pk)"));
        assert!(!put.item().contains_key("message_id"));
        assert_eq!(
            put.item().get("thread_id"),
            Some(&DynamoAv::S("tid-1".to_owned()))
        );
    }

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

    // --- inbox reads: consistent_read guard ---------------------------------
    //
    // `ensure_inbox`'s `PutItem` condition is evaluated strongly consistently,
    // so a `ConditionalCheckFailedException` proves the inbox exists, and the
    // recovery read that follows must be strongly consistent too: a stale
    // replica would report it missing. Every other inbox read tolerates a
    // stale miss (it only leads to `ensure_inbox`), and a consistent read
    // costs twice the capacity, so `get_inbox` stays eventually consistent.
    //
    // The seam is a request interceptor on a real `aws_sdk_dynamodb::Client`:
    // it records the typed `GetItemInput` in `read_before_execution` and
    // short-circuits the execution with an error, so no request reaches a
    // real endpoint (no network, no retries, deterministic).

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

    /// Builds a `DynamoTable` whose client records every `GetItem` input it
    /// would send and short-circuits before transmission.
    fn aws_with_capturing_dynamo(captured: Arc<Mutex<Vec<GetItemInput>>>) -> DynamoTable {
        let dynamo_conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .retry_config(aws_smithy_types::retry::RetryConfig::disabled())
            .interceptor(CaptureGetItemInput(captured))
            .build();
        DynamoTable::new(
            aws_sdk_dynamodb::Client::from_conf(dynamo_conf),
            Some(MAIL_TABLE_NAME.to_owned()),
        )
    }

    /// Only the `ensure_inbox` recovery read pays for strong consistency.
    #[tokio::test]
    async fn only_the_ensure_inbox_recovery_reads_strongly_consistently() {
        let captured = Arc::new(Mutex::new(Vec::<GetItemInput>::new()));
        let aws = aws_with_capturing_dynamo(Arc::clone(&captured));

        let inbox = InboxId::new("support@example.com");
        // The interceptor short-circuits every request with an error; only
        // the emitted requests are asserted on.
        let _ = Mailbox::new(&aws).get_inbox(&inbox).await;
        let _ = Table::get::<Inbox>(
            &aws,
            keys::inbox(&inbox),
            Read::Consistent,
            "GetItem(inbox)",
        )
        .await;

        let requests: Vec<GetItemInput> = captured
            .lock()
            .map(|guard| guard.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].consistent_read(),
            Some(false),
            "get_inbox tolerates a stale miss and must not pay for a consistent read"
        );
        assert_eq!(
            requests[1].consistent_read(),
            Some(true),
            "the recovery read after a conditional-check failure must be consistent"
        );
        let request = &requests[1];
        assert_eq!(request.table_name(), Some(MAIL_TABLE_NAME));
        let key = request.key();
        let pk = key.and_then(|k| k.get("pk"));
        let sk = key.and_then(|k| k.get("sk"));
        assert_eq!(
            pk,
            Some(&DynamoAv::S("INBOX#support@example.com".to_owned()))
        );
        assert_eq!(sk, Some(&DynamoAv::S("META".to_owned())));
    }

    /// Each single-item read keeps its key and its consistency: the reads
    /// that condition a write are consistent, the others are not.
    #[tokio::test]
    async fn single_item_reads_keep_their_keys_and_consistency() {
        let captured = Arc::new(Mutex::new(Vec::<GetItemInput>::new()));
        let aws = aws_with_capturing_dynamo(Arc::clone(&captured));
        let inbox = InboxId::new("support@example.com");

        let mailbox = Mailbox::new(&aws);
        let _ = mailbox.get_message(&inbox, &MessageId::new("mid-1")).await;
        let _ = mailbox.get_send_key("keyhash").await;
        let _ = mailbox.get_send_state(&MessageId::new("mid-1")).await;
        let _ = mailbox
            .resolve_ses_message(&SesMessageId::new("ses-1"))
            .await;
        let _ = mailbox
            .get_thread(&inbox, &ThreadId::new("tid-1"), 10, None)
            .await;
        let _ = mailbox
            .message_exists(&inbox, &MessageId::new("mid-1"))
            .await;
        let _ = mailbox
            .update_labels(
                &inbox,
                &MessageId::new("mid-1"),
                &[],
                &[],
                "2026-01-01T00:00:00.000Z",
            )
            .await;

        let requests: Vec<GetItemInput> = captured
            .lock()
            .map(|guard| guard.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let seen: Vec<(String, String, Option<bool>)> = requests
            .iter()
            .map(|request| {
                let string = |name: &str| match request.key().and_then(|key| key.get(name)) {
                    Some(DynamoAv::S(value)) => value.clone(),
                    _ => String::new(),
                };
                (string("pk"), string("sk"), request.consistent_read())
            })
            .collect();
        let expected = [
            ("INBOX#support@example.com", "MSG#mid-1", Some(true)),
            ("SENDKEY#keyhash", "KEY", Some(true)),
            ("OUTBOX#mid-1", "STATE", Some(true)),
            ("SESMSG#ses-1", "REF", Some(false)),
            ("INBOX#support@example.com", "THR#tid-1", Some(false)),
            ("INBOX#support@example.com", "MSG#mid-1", Some(false)),
            ("INBOX#support@example.com", "MSG#mid-1", Some(true)),
        ]
        .map(|(pk, sk, consistent)| (pk.to_owned(), sk.to_owned(), consistent));
        assert_eq!(seen, expected);
    }

    // --- MailStoreError::from_sdk: classifying DynamoDB throttles ------------
    //
    // `MailStoreError::from_sdk` stamps every non-transient SDK error
    // `Permanent`; `actions::run`'s `Ses` arm then swallows a permanent
    // delivery-label failure with a 200, so a misclassified throttle
    // silently drops the label with no SNS redelivery. DynamoDB serves its
    // throughput throttles over HTTP 400, so the `code()`-list branch is the
    // only one that can catch them. These tests pin both legs of
    // `apply_delivery_label`: the `GetItem` read (`resolve_ses_message`) and
    // the top-level `TransactWriteItems` write (`RequestLimitExceeded` falls
    // through `transact`'s explicit variant arms to
    // `MailStoreError::from_sdk`).

    use aws_sdk_dynamodb::config::http::HttpResponse;
    use aws_sdk_dynamodb::operation::get_item::GetItemError;
    use aws_sdk_dynamodb::types::error::{
        ProvisionedThroughputExceededException, RequestLimitExceeded,
    };
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::error::ErrorMetadata;

    /// One `GetItem` service error whose typed variant carries `code` (so
    /// `ProvideErrorMetadata::code()` returns it, as a real DynamoDB
    /// response deserializes), served over `status`. The two DynamoDB
    /// throughput codes use their modeled variants; any other code falls
    /// back to an unhandled error that still carries `code`.
    fn get_item_service_error(code: &str, status: u16) -> SdkError<GetItemError> {
        let meta = ErrorMetadata::builder().code(code).build();
        let err = match code {
            "ProvisionedThroughputExceededException" => {
                GetItemError::ProvisionedThroughputExceededException(
                    ProvisionedThroughputExceededException::builder()
                        .meta(meta)
                        .build(),
                )
            }
            "RequestLimitExceeded" => GetItemError::RequestLimitExceeded(
                RequestLimitExceeded::builder().meta(meta).build(),
            ),
            _ => GetItemError::generic(meta),
        };
        let response = HttpResponse::new(status.try_into().unwrap(), SdkBody::empty());
        SdkError::service_error(err, response)
    }

    /// The account-level `RequestLimitExceeded` as a top-level
    /// `TransactWriteItems` error: the realistic write-leg trigger, surfaced
    /// on the request itself (not as a cancellation reason) and so falling
    /// through `transact`'s explicit variant arms to
    /// `MailStoreError::from_sdk`.
    fn transact_write_request_limit_exceeded(status: u16) -> SdkError<TransactWriteItemsError> {
        let meta = ErrorMetadata::builder()
            .code("RequestLimitExceeded")
            .build();
        let response = HttpResponse::new(status.try_into().unwrap(), SdkBody::empty());
        SdkError::service_error(
            TransactWriteItemsError::RequestLimitExceeded(
                RequestLimitExceeded::builder().meta(meta).build(),
            ),
            response,
        )
    }

    /// Read leg: a `ProvisionedThroughputExceededException` throttling the
    /// `GetItem` in `resolve_ses_message` is transient, not a permanent label
    /// failure the `Ses` arm would swallow with a 200.
    #[test]
    fn provisioned_throughput_exceeded_on_the_read_leg_is_transient() {
        let error = get_item_service_error("ProvisionedThroughputExceededException", 400);
        assert!(
            matches!(
                MailStoreError::from_sdk("GetItem", &error),
                MailStoreError::Transient(_)
            ),
            "a throttled GetItem must be transient so the delivery label is retried"
        );
    }

    /// Read leg's realistic trigger is the account-level ceiling: a
    /// `RequestLimitExceeded` on the `GetItem` is transient too.
    #[test]
    fn request_limit_exceeded_on_the_read_leg_is_transient() {
        let error = get_item_service_error("RequestLimitExceeded", 400);
        assert!(
            matches!(
                MailStoreError::from_sdk("GetItem", &error),
                MailStoreError::Transient(_)
            ),
            "an account-level request limit on GetItem must be transient"
        );
    }

    /// Write leg: a top-level `RequestLimitExceeded` on `TransactWriteItems`
    /// is transient. This is the realistic write-leg trigger — it surfaces on
    /// the request, not as a `TransactionCanceledException` cancellation
    /// reason, so it reaches `MailStoreError::from_sdk` via the fall-through arm.
    #[test]
    fn request_limit_exceeded_on_the_write_leg_is_transient() {
        let error = transact_write_request_limit_exceeded(400);
        assert!(
            matches!(
                MailStoreError::from_sdk("TransactWriteItems", &error),
                MailStoreError::Transient(_)
            ),
            "an account-level request limit on TransactWriteItems must be transient"
        );
    }

    /// `PutItem(inbox)` racing a transaction on the same item gets a 400
    /// `TransactionConflictException`; it must be transient, or the ingest
    /// notification is acked and the message dropped.
    #[test]
    fn a_transaction_conflict_on_a_plain_write_is_transient() {
        let error = get_item_service_error("TransactionConflictException", 400);
        assert!(matches!(
            MailStoreError::from_sdk("PutItem(inbox)", &error),
            MailStoreError::Transient(_)
        ));
    }

    /// A non-throttling `GetItem` 400 keeps classifying permanent: the fix
    /// adds throttling codes only, so a genuinely-bad request (validation,
    /// access denied) still surfaces as permanent rather than retrying.
    #[test]
    fn a_non_throttling_get_item_failure_stays_permanent() {
        let error = get_item_service_error("ValidationException", 400);
        assert!(
            matches!(
                MailStoreError::from_sdk("GetItem", &error),
                MailStoreError::Permanent(_)
            ),
            "a non-throttling 400 must remain permanent"
        );
    }

    // --- DynamoTable against a stub DynamoDB endpoint -----------------------
    //
    // A real client pointed at wiremock: the request tests pin the exact JSON
    // DynamoDB would receive, and the response tests feed back canned
    // `application/x-amz-json-1.0` bodies, success and error alike, so every
    // branch that reads a response runs against the wire format the SDK
    // actually deserializes.
    mod wire {
        use serde_json::{Value, json};
        use wiremock::matchers::{header, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::*;
        use crate::keys::PageKey;
        use crate::plan::OpRole;
        use crate::table::MailIndex;

        const TABLE: &str = "mail-table";

        /// One row as the tests read it back.
        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct Row {
            pk: String,
            sk: String,
            n: u32,
        }

        fn row(pk: &str, sk: &str, n: u32) -> Value {
            json!({"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": n.to_string()}})
        }

        fn key(pk: &str, sk: &str) -> TableKey {
            TableKey {
                pk: pk.to_owned(),
                sk: sk.to_owned(),
            }
        }

        fn ok(body: &Value) -> ResponseTemplate {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_string(body.to_string())
        }

        /// A DynamoDB error response. The `__type` value is the
        /// `com.amazonaws.dynamodb.v20120810#<ExceptionName>` form shown in
        /// "Error handling with DynamoDB" (Error components):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html>
        fn dynamo_error(status: u16, kind: &str) -> ResponseTemplate {
            ResponseTemplate::new(status)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_string(
                    json!({
                        "__type": format!("com.amazonaws.dynamodb.v20120810#{kind}"),
                        "message": "stub",
                    })
                    .to_string(),
                )
        }

        /// A `TransactionCanceledException` carrying one `CancellationReasons`
        /// entry per transaction item, in item order, each with a `Code`; an
        /// item with no error has the literal code `"None"`. Shape and codes
        /// from the `TransactWriteItems` API reference (Errors,
        /// `TransactionCanceledException`):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
        fn cancelled(codes: &[&str]) -> ResponseTemplate {
            let reasons: Vec<Value> = codes.iter().map(|code| json!({"Code": code})).collect();
            ResponseTemplate::new(400)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_string(
                    json!({
                        "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                        "Message": "Transaction cancelled",
                        "CancellationReasons": reasons,
                    })
                    .to_string(),
                )
        }

        fn target(operation: &str) -> String {
            format!("DynamoDB_20120810.{operation}")
        }

        async fn mount(server: &MockServer, operation: &str, response: ResponseTemplate) {
            Mock::given(method("POST"))
                .and(header("x-amz-target", target(operation).as_str()))
                .respond_with(response)
                .mount(server)
                .await;
        }

        /// Serves `response` to the next matching request only, so a sequence
        /// of these answers successive calls in mount order.
        async fn mount_once(server: &MockServer, operation: &str, response: ResponseTemplate) {
            Mock::given(method("POST"))
                .and(header("x-amz-target", target(operation).as_str()))
                .respond_with(response)
                .up_to_n_times(1)
                .mount(server)
                .await;
        }

        fn client(server: &MockServer) -> aws_sdk_dynamodb::Client {
            let conf = aws_sdk_dynamodb::Config::builder()
                .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
                .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                    "AKID", "secret", None, None, "test",
                ))
                .endpoint_url(server.uri())
                .retry_config(aws_smithy_types::retry::RetryConfig::disabled())
                .build();
            aws_sdk_dynamodb::Client::from_conf(conf)
        }

        fn table(server: &MockServer) -> DynamoTable {
            DynamoTable::new(client(server), Some(TABLE.to_owned()))
        }

        /// The JSON bodies of every request the stub saw for `operation`.
        async fn bodies(server: &MockServer, operation: &str) -> Vec<Value> {
            let target = target(operation);
            let mut bodies = Vec::new();
            for request in server.received_requests().await.unwrap_or_default() {
                if request
                    .headers
                    .get("x-amz-target")
                    .is_some_and(|value| value == target.as_str())
                {
                    bodies.push(serde_json::from_slice(&request.body).unwrap());
                }
            }
            bodies
        }

        fn planned(role: OpRole, op: WriteOp) -> PlannedOp {
            PlannedOp { role, op }
        }

        fn put(cond: Cond) -> WriteOp {
            WriteOp::Put {
                item: serde_dynamo::Item::from(HashMap::from([
                    (
                        "pk".to_owned(),
                        serde_dynamo::AttributeValue::S("P".to_owned()),
                    ),
                    (
                        "sk".to_owned(),
                        serde_dynamo::AttributeValue::S("S".to_owned()),
                    ),
                ])),
                cond,
            }
        }

        fn error_text(error: &MailStoreError) -> String {
            match error {
                MailStoreError::Transient(source) | MailStoreError::Permanent(source) => {
                    format!("{source:#}")
                }
                other => other.to_string(),
            }
        }

        // --- TransactWriteItems --------------------------------------------

        /// Runs `ops` as one committed transaction and returns the
        /// `TransactItems` the stub received.
        async fn sent_transact_items(ops: &[PlannedOp]) -> Value {
            let server = MockServer::start().await;
            mount(&server, "TransactWriteItems", ok(&json!({}))).await;
            let outcome = table(&server).transact(ops).await.unwrap();
            assert_eq!(outcome, TxnOutcome::Committed);
            let mut sent = bodies(&server, "TransactWriteItems").await;
            assert_eq!(sent.len(), 1);
            let mut body = sent.remove(0);
            // The SDK fills in a random idempotency token for every call.
            let token = body
                .as_object_mut()
                .and_then(|body| body.remove("ClientRequestToken"));
            assert!(token.is_some_and(|token| token.is_string()));
            let mut keys: Vec<&String> = body.as_object().unwrap().keys().collect();
            keys.sort();
            assert_eq!(keys, ["TransactItems"]);
            body["TransactItems"].take()
        }

        /// Every `Cond` on a `Put`: the condition expression with exactly
        /// the placeholder names and values it references.
        #[tokio::test]
        async fn put_conditions_render_with_their_placeholders() {
            let ops = [
                planned(OpRole::Message, put(Cond::None)),
                planned(OpRole::Message, put(Cond::NotExists)),
                planned(
                    OpRole::SendState,
                    put(Cond::NotExistsOrExpired { now_epoch: 42 }),
                ),
                planned(
                    OpRole::SendState,
                    put(Cond::All(vec![
                        Check::Eq(
                            "status",
                            serde_dynamo::AttributeValue::S("queued".to_owned()),
                        ),
                        Check::Eq("version", serde_dynamo::AttributeValue::N("3".to_owned())),
                    ])),
                ),
                planned(OpRole::Thread, put(Cond::VersionEquals(7))),
            ];
            let item = json!({"pk": {"S": "P"}, "sk": {"S": "S"}});
            assert_eq!(
                sent_transact_items(&ops).await,
                json!([
                    {"Put": {"TableName": TABLE, "Item": item}},
                    {"Put": {
                        "TableName": TABLE,
                        "Item": item,
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }},
                    {"Put": {
                        "TableName": TABLE,
                        "Item": item,
                        "ConditionExpression": "attribute_not_exists(pk) OR expires_at < :cond_now",
                        "ExpressionAttributeValues": {":cond_now": {"N": "42"}},
                    }},
                    {"Put": {
                        "TableName": TABLE,
                        "Item": item,
                        "ConditionExpression": "#c0 = :c0 AND #c1 = :c1",
                        "ExpressionAttributeNames": {"#c0": "status", "#c1": "version"},
                        "ExpressionAttributeValues": {":c0": {"S": "queued"}, ":c1": {"N": "3"}},
                    }},
                    {"Put": {
                        "TableName": TABLE,
                        "Item": item,
                        "ConditionExpression": "version = :cond_version",
                        "ExpressionAttributeValues": {":cond_version": {"N": "7"}},
                    }},
                ])
            );
        }

        fn update(sk: &str, set: &[(&str, &str)], remove: &[&str], cond: Cond) -> PlannedOp {
            let mut pairs = Vec::new();
            for (name, value) in set {
                pairs.push((
                    (*name).to_owned(),
                    serde_dynamo::AttributeValue::S((*value).to_owned()),
                ));
            }
            planned(
                OpRole::Thread,
                WriteOp::Update {
                    pk: "THR".to_owned(),
                    sk: sk.to_owned(),
                    set: pairs,
                    remove: remove.iter().map(|name| (*name).to_owned()).collect(),
                    cond,
                },
            )
        }

        /// An update's SET and REMOVE clauses, alone and together, each
        /// with its own placeholders beside the condition's.
        #[tokio::test]
        async fn updates_render_set_and_remove_clauses() {
            let ops = [
                update(
                    "T1",
                    &[("subject", "hi"), ("note", "n")],
                    &["draft", "stale"],
                    Cond::VersionEquals(7),
                ),
                update("T2", &[("subject", "only-set")], &[], Cond::None),
                update("T3", &[], &["draft"], Cond::None),
            ];
            assert_eq!(
                sent_transact_items(&ops).await,
                json!([
                    {"Update": {
                        "TableName": TABLE,
                        "Key": {"pk": {"S": "THR"}, "sk": {"S": "T1"}},
                        "UpdateExpression": "SET #s0 = :s0, #s1 = :s1 REMOVE #r0, #r1",
                        "ConditionExpression": "version = :cond_version",
                        "ExpressionAttributeNames": {
                            "#s0": "subject", "#s1": "note", "#r0": "draft", "#r1": "stale",
                        },
                        "ExpressionAttributeValues": {
                            ":cond_version": {"N": "7"}, ":s0": {"S": "hi"}, ":s1": {"S": "n"},
                        },
                    }},
                    {"Update": {
                        "TableName": TABLE,
                        "Key": {"pk": {"S": "THR"}, "sk": {"S": "T2"}},
                        "UpdateExpression": "SET #s0 = :s0",
                        "ExpressionAttributeNames": {"#s0": "subject"},
                        "ExpressionAttributeValues": {":s0": {"S": "only-set"}},
                    }},
                    {"Update": {
                        "TableName": TABLE,
                        "Key": {"pk": {"S": "THR"}, "sk": {"S": "T3"}},
                        "UpdateExpression": "REMOVE #r0",
                        "ExpressionAttributeNames": {"#r0": "draft"},
                    }},
                ])
            );
        }

        fn alias(pk: &str, message_id: Option<&str>) -> PlannedOp {
            planned(
                OpRole::RfcAlias,
                WriteOp::AliasFirstWriter {
                    pk: pk.to_owned(),
                    sk: "RFC".to_owned(),
                    message_id: message_id.map(str::to_owned),
                    thread_id: "tid-1".to_owned(),
                    expires_at: 1_900_000_000,
                },
            )
        }

        #[tokio::test]
        async fn alias_puts_render_their_item_and_first_writer_condition() {
            let ops = [alias("RFC#a", Some("mid-1")), alias("RFC#b", None)];
            assert_eq!(
                sent_transact_items(&ops).await,
                json!([
                    {"Put": {
                        "TableName": TABLE,
                        "Item": {
                            "pk": {"S": "RFC#a"},
                            "sk": {"S": "RFC"},
                            "thread_id": {"S": "tid-1"},
                            "expires_at": {"N": "1900000000"},
                            "message_id": {"S": "mid-1"},
                        },
                        "ConditionExpression":
                            "attribute_not_exists(pk) OR attribute_not_exists(message_id)",
                    }},
                    {"Put": {
                        "TableName": TABLE,
                        "Item": {
                            "pk": {"S": "RFC#b"},
                            "sk": {"S": "RFC"},
                            "thread_id": {"S": "tid-1"},
                            "expires_at": {"N": "1900000000"},
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }},
                ])
            );
        }

        /// Each documented cancellation reason code maps to its own variant;
        /// anything else, `"None"` included, is `None`. Codes from the
        /// `TransactWriteItems` API reference (Errors,
        /// `TransactionCanceledException`):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
        #[test]
        fn every_documented_cancellation_code_maps_to_its_reason() {
            let cases = [
                (
                    Some("ConditionalCheckFailed"),
                    CancellationReason::ConditionalCheckFailed,
                ),
                (
                    Some("TransactionConflict"),
                    CancellationReason::TransactionConflict,
                ),
                (Some("ThrottlingError"), CancellationReason::ThrottlingError),
                (
                    Some("ProvisionedThroughputExceeded"),
                    CancellationReason::ProvisionedThroughputExceeded,
                ),
                (
                    Some("RequestLimitExceeded"),
                    CancellationReason::RequestLimitExceeded,
                ),
                (
                    Some("InternalServerError"),
                    CancellationReason::InternalServerError,
                ),
                (Some("ValidationError"), CancellationReason::ValidationError),
                (
                    Some("ItemCollectionSizeLimitExceeded"),
                    CancellationReason::ItemCollectionSizeLimitExceeded,
                ),
                (Some("None"), CancellationReason::None),
                (Some("SomethingNew"), CancellationReason::None),
                (None, CancellationReason::None),
            ];
            for (code, expected) in cases {
                assert_eq!(map_cancellation_reason(code), expected, "{code:?}");
            }
        }

        async fn transact_against(
            response: ResponseTemplate,
            ops: &[PlannedOp],
        ) -> Result<TxnOutcome, MailStoreError> {
            let server = MockServer::start().await;
            mount(&server, "TransactWriteItems", response).await;
            table(&server).transact(ops).await
        }

        /// The reasons on the wire reach `decode_cancellation` in item order:
        /// the same failed check means a duplicate on a `NotExists` message
        /// put and a version conflict on a versioned thread put.
        #[tokio::test]
        async fn cancellation_reasons_are_decoded_against_their_ops() {
            let ops = [
                planned(OpRole::Message, put(Cond::NotExists)),
                planned(OpRole::Thread, put(Cond::VersionEquals(1))),
            ];
            let duplicate = transact_against(cancelled(&["ConditionalCheckFailed", "None"]), &ops)
                .await
                .unwrap();
            assert_eq!(duplicate, TxnOutcome::Cancelled(TxnDecision::Duplicate));

            let conflict = transact_against(cancelled(&["None", "ConditionalCheckFailed"]), &ops)
                .await
                .unwrap();
            assert_eq!(
                conflict,
                TxnOutcome::Cancelled(TxnDecision::VersionConflict)
            );

            for retryable in ["TransactionConflict", "ThrottlingError"] {
                let retry = transact_against(cancelled(&["None", retryable]), &ops)
                    .await
                    .unwrap();
                assert_eq!(
                    retry,
                    TxnOutcome::Cancelled(TxnDecision::Retry),
                    "{retryable}"
                );
            }
        }

        /// `TransactionInProgressException` (a request token already in
        /// flight) is a retry, not an error. Listed in the `TransactWriteItems`
        /// API reference (Errors):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
        #[tokio::test]
        async fn a_transaction_in_progress_is_retried() {
            let ops = [planned(OpRole::Message, put(Cond::NotExists))];
            let outcome =
                transact_against(dynamo_error(400, "TransactionInProgressException"), &ops)
                    .await
                    .unwrap();
            assert_eq!(outcome, TxnOutcome::Cancelled(TxnDecision::Retry));
        }

        /// `IdempotentParameterMismatchException` (same token, different
        /// payload) cannot succeed on retry. Listed in the `TransactWriteItems`
        /// API reference (Errors):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
        #[tokio::test]
        async fn an_idempotent_parameter_mismatch_is_permanent() {
            let ops = [planned(OpRole::Message, put(Cond::NotExists))];
            let error = transact_against(
                dynamo_error(400, "IdempotentParameterMismatchException"),
                &ops,
            )
            .await
            .unwrap_err();
            assert!(matches!(error, MailStoreError::Permanent(_)), "{error:?}");
            assert!(
                error_text(&error).contains("idempotent parameter mismatch"),
                "{error:?}"
            );
        }

        /// Any other failure goes through the shared SDK classification: a
        /// 5xx is transient, a validation error permanent.
        #[tokio::test]
        async fn other_transaction_failures_use_the_sdk_classification() {
            let ops = [planned(OpRole::Message, put(Cond::NotExists))];
            let transient = transact_against(dynamo_error(500, "InternalServerError"), &ops)
                .await
                .unwrap_err();
            assert!(
                matches!(transient, MailStoreError::Transient(_)),
                "{transient:?}"
            );
            let permanent = transact_against(dynamo_error(400, "ValidationException"), &ops)
                .await
                .unwrap_err();
            assert!(
                matches!(permanent, MailStoreError::Permanent(_)),
                "{permanent:?}"
            );
            assert!(
                error_text(&permanent).contains("TransactWriteItems"),
                "{permanent:?}"
            );
        }

        // --- PutItem -------------------------------------------------------

        #[tokio::test]
        async fn put_if_absent_sends_the_item_under_a_not_exists_condition() {
            let server = MockServer::start().await;
            mount(&server, "PutItem", ok(&json!({}))).await;
            let item = serde_dynamo::Item::from(HashMap::from([
                (
                    "pk".to_owned(),
                    serde_dynamo::AttributeValue::S("INBOX#a".to_owned()),
                ),
                (
                    "sk".to_owned(),
                    serde_dynamo::AttributeValue::S("META".to_owned()),
                ),
            ]));
            let outcome = table(&server)
                .put_if_absent(item, "PutItem(inbox)")
                .await
                .unwrap();
            assert_eq!(outcome, PutOutcome::Created);
            assert_eq!(
                bodies(&server, "PutItem").await,
                vec![json!({
                    "TableName": TABLE,
                    "Item": {"pk": {"S": "INBOX#a"}, "sk": {"S": "META"}},
                    "ConditionExpression": "attribute_not_exists(pk)",
                })]
            );
        }

        async fn put_against(response: ResponseTemplate) -> Result<PutOutcome, MailStoreError> {
            let server = MockServer::start().await;
            mount(&server, "PutItem", response).await;
            table(&server)
                .put_if_absent(serde_dynamo::Item::default(), "PutItem(inbox)")
                .await
        }

        /// A failed condition is the item already existing, not an error;
        /// `ConditionalCheckFailedException` is the `PutItem` error for it
        /// ("Error handling with DynamoDB", HTTP status code 400):
        /// <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html>
        #[tokio::test]
        async fn a_failed_put_condition_means_the_item_already_exists() {
            let outcome = put_against(dynamo_error(400, "ConditionalCheckFailedException"))
                .await
                .unwrap();
            assert_eq!(outcome, PutOutcome::AlreadyExists);
        }

        #[tokio::test]
        async fn other_put_failures_use_the_sdk_classification() {
            let transient = put_against(dynamo_error(500, "InternalServerError"))
                .await
                .unwrap_err();
            assert!(
                matches!(transient, MailStoreError::Transient(_)),
                "{transient:?}"
            );
            let permanent = put_against(dynamo_error(400, "ValidationException"))
                .await
                .unwrap_err();
            assert!(
                matches!(permanent, MailStoreError::Permanent(_)),
                "{permanent:?}"
            );
            assert!(
                error_text(&permanent).contains("PutItem(inbox)"),
                "{permanent:?}"
            );
        }

        // --- BatchGetItem --------------------------------------------------
        //
        // Response shape (`Responses` keyed by table name, `UnprocessedKeys`
        // in the same form as `RequestItems`) from the `BatchGetItem` API
        // reference (Response Syntax, Response Elements):
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html

        #[tokio::test]
        async fn a_batch_read_requests_every_key_with_its_consistency() {
            for (read, consistent) in [(Read::Consistent, true), (Read::Eventual, false)] {
                let server = MockServer::start().await;
                mount(
                    &server,
                    "BatchGetItem",
                    ok(&json!({"Responses": {TABLE: []}})),
                )
                .await;
                let found = table(&server)
                    .batch_get::<Row>(vec![key("A", "1"), key("B", "2")], read, "BatchGetItem")
                    .await
                    .unwrap();
                assert!(found.is_empty());
                assert_eq!(
                    bodies(&server, "BatchGetItem").await,
                    vec![json!({"RequestItems": {TABLE: {
                        "Keys": [
                            {"pk": {"S": "A"}, "sk": {"S": "1"}},
                            {"pk": {"S": "B"}, "sk": {"S": "2"}},
                        ],
                        "ConsistentRead": consistent,
                    }}})]
                );
            }
        }

        #[tokio::test]
        async fn an_empty_batch_sends_no_request() {
            let server = MockServer::start().await;
            let found = table(&server)
                .batch_get::<Row>(Vec::new(), Read::Eventual, "BatchGetItem")
                .await
                .unwrap();
            assert!(found.is_empty());
            assert!(
                server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .is_empty()
            );
        }

        /// Items map back to the key they carry, in whatever order DynamoDB
        /// returns them; an item without a string `pk`/`sk` or one that does
        /// not deserialize is left out rather than failing the batch.
        #[tokio::test]
        async fn batch_items_map_back_by_key_and_bad_rows_are_skipped() {
            let server = MockServer::start().await;
            mount(
                &server,
                "BatchGetItem",
                ok(&json!({
                    "Responses": {TABLE: [
                        row("B", "2", 2),
                        row("A", "1", 1),
                        {"pk": {"S": "C"}, "sk": {"S": "3"}, "n": {"S": "not-a-number"}},
                        {"pk": {"S": "D"}, "n": {"N": "4"}},
                        {"pk": {"S": "E"}, "sk": {"N": "5"}, "n": {"N": "5"}},
                    ]},
                    "UnprocessedKeys": {},
                })),
            )
            .await;
            let found = table(&server)
                .batch_get::<Row>(
                    vec![key("A", "1"), key("B", "2"), key("C", "3"), key("D", "4")],
                    Read::Eventual,
                    "BatchGetItem",
                )
                .await
                .unwrap();
            let expected = HashMap::from([
                (
                    key("A", "1"),
                    Row {
                        pk: "A".to_owned(),
                        sk: "1".to_owned(),
                        n: 1,
                    },
                ),
                (
                    key("B", "2"),
                    Row {
                        pk: "B".to_owned(),
                        sk: "2".to_owned(),
                        n: 2,
                    },
                ),
            ]);
            assert_eq!(found, expected);
        }

        /// A response for some other table contributes nothing.
        #[tokio::test]
        async fn responses_for_another_table_are_ignored() {
            let server = MockServer::start().await;
            mount(
                &server,
                "BatchGetItem",
                ok(&json!({"Responses": {"other-table": [row("A", "1", 1)]}})),
            )
            .await;
            let found = table(&server)
                .batch_get::<Row>(vec![key("A", "1")], Read::Eventual, "BatchGetItem")
                .await
                .unwrap();
            assert!(found.is_empty());
        }

        /// Unprocessed keys are re-requested, and only they are; the
        /// results of both rounds are merged.
        #[tokio::test]
        async fn unprocessed_keys_are_retried_until_drained() {
            let server = MockServer::start().await;
            mount_once(
                &server,
                "BatchGetItem",
                ok(&json!({
                    "Responses": {TABLE: [row("A", "1", 1)]},
                    "UnprocessedKeys": {TABLE: {
                        "Keys": [{"pk": {"S": "B"}, "sk": {"S": "2"}}],
                        "ConsistentRead": true,
                    }},
                })),
            )
            .await;
            mount_once(
                &server,
                "BatchGetItem",
                ok(&json!({"Responses": {TABLE: [row("B", "2", 2)]}, "UnprocessedKeys": {}})),
            )
            .await;
            let started = std::time::Instant::now();
            let found = table(&server)
                .batch_get::<Row>(
                    vec![key("A", "1"), key("B", "2")],
                    Read::Consistent,
                    "BatchGetItem",
                )
                .await
                .unwrap();
            assert_eq!(found.len(), 2);
            assert_eq!(found[&key("B", "2")].n, 2);
            // One backoff of 50 ms before the second round.
            assert!(started.elapsed() >= Duration::from_millis(50));
            let sent = bodies(&server, "BatchGetItem").await;
            assert_eq!(sent.len(), 2);
            assert_eq!(
                sent[1],
                json!({"RequestItems": {TABLE: {
                    "Keys": [{"pk": {"S": "B"}, "sk": {"S": "2"}}],
                    "ConsistentRead": true,
                }}})
            );
        }

        /// Keys DynamoDB never processes surface as a transient error after
        /// `MAX_BATCH_GET_ATTEMPTS` requests, with a growing backoff between
        /// them, rather than looping forever.
        #[tokio::test]
        async fn keys_left_unprocessed_give_up_after_the_attempt_cap() {
            let server = MockServer::start().await;
            mount(
                &server,
                "BatchGetItem",
                ok(&json!({
                    "Responses": {TABLE: []},
                    "UnprocessedKeys": {TABLE: {
                        "Keys": [{"pk": {"S": "A"}, "sk": {"S": "1"}}],
                    }},
                })),
            )
            .await;
            let started = std::time::Instant::now();
            // Bounded, so a loop that never counts its attempts fails the
            // test instead of hanging it.
            let error = tokio::time::timeout(
                Duration::from_secs(10),
                table(&server).batch_get::<Row>(
                    vec![key("A", "1")],
                    Read::Eventual,
                    "BatchGetItem",
                ),
            )
            .await
            .expect("the attempt cap ends the loop")
            .unwrap_err();
            assert!(matches!(error, MailStoreError::Transient(_)), "{error:?}");
            assert_eq!(
                bodies(&server, "BatchGetItem").await.len(),
                usize::try_from(MAX_BATCH_GET_ATTEMPTS).unwrap()
            );
            // 50 + 100 + 150 + 200 ms between the five attempts.
            assert!(started.elapsed() >= Duration::from_millis(500));
        }

        #[tokio::test]
        async fn a_failed_batch_read_uses_the_sdk_classification() {
            let server = MockServer::start().await;
            mount(
                &server,
                "BatchGetItem",
                dynamo_error(400, "ProvisionedThroughputExceededException"),
            )
            .await;
            let error = table(&server)
                .batch_get::<Row>(vec![key("A", "1")], Read::Eventual, "BatchGetItem(x)")
                .await
                .unwrap_err();
            assert!(matches!(error, MailStoreError::Transient(_)), "{error:?}");
            assert!(error_text(&error).contains("BatchGetItem(x)"), "{error:?}");
        }

        // --- GetItem -------------------------------------------------------

        #[tokio::test]
        async fn a_read_item_is_deserialized_and_a_bad_one_is_permanent() {
            let server = MockServer::start().await;
            mount_once(&server, "GetItem", ok(&json!({"Item": row("A", "1", 1)}))).await;
            mount_once(&server, "GetItem", ok(&json!({}))).await;
            mount_once(
                &server,
                "GetItem",
                ok(&json!({"Item": {"pk": {"S": "A"}, "sk": {"S": "1"}, "n": {"S": "x"}}})),
            )
            .await;
            let table = table(&server);
            let found = table
                .get::<Row>(key("A", "1"), Read::Eventual, "GetItem(x)")
                .await
                .unwrap();
            assert_eq!(found.map(|row| row.n), Some(1));
            let absent = table
                .get::<Row>(key("A", "1"), Read::Eventual, "GetItem(x)")
                .await
                .unwrap();
            assert!(absent.is_none());
            let error = table
                .get::<Row>(key("A", "1"), Read::Eventual, "GetItem(x)")
                .await
                .unwrap_err();
            assert!(matches!(error, MailStoreError::Permanent(_)), "{error:?}");
            assert!(error_text(&error).contains("GetItem(x)"), "{error:?}");
        }

        // --- Query ---------------------------------------------------------

        fn query_response(last_evaluated: Option<Value>) -> ResponseTemplate {
            let mut body = json!({"Items": [row("A", "1", 1), row("B", "2", 2)], "Count": 2});
            if let Some(last) = last_evaluated {
                body["LastEvaluatedKey"] = last;
            }
            ok(&body)
        }

        #[tokio::test]
        async fn a_query_sends_its_index_bounds_order_limit_and_start() {
            let server = MockServer::start().await;
            mount(&server, "Query", query_response(None)).await;
            let start = PageKey {
                partition: "INBOX#a#MSG".to_owned(),
                sort: "2026-01-01".to_owned(),
                table_pk: "INBOX#a".to_owned(),
                table_sk: "MSG#m".to_owned(),
            };
            let query = PageQuery {
                index: MailIndex::ByThread,
                partition: "INBOX#a#MSG",
                limit: 25,
                before: Some("z"),
                after: Some("a"),
                ascending: false,
                start: Some(&start),
                context: "Query(x)",
            };
            let page = table(&server).query::<Row>(query).await.unwrap();
            assert_eq!(page.items.len(), 2);
            assert!(page.next.is_none());
            assert_eq!(
                bodies(&server, "Query").await,
                vec![json!({
                    "TableName": TABLE,
                    "IndexName": "ByThread",
                    "KeyConditionExpression": "#pk = :pk AND #sk > :after AND #sk < :before",
                    "ExpressionAttributeNames": {"#pk": "gsi2pk", "#sk": "gsi2sk"},
                    "ExpressionAttributeValues": {
                        ":pk": {"S": "INBOX#a#MSG"},
                        ":after": {"S": "a"},
                        ":before": {"S": "z"},
                    },
                    "ScanIndexForward": false,
                    "Limit": 25,
                    "ExclusiveStartKey": {
                        "pk": {"S": "INBOX#a"},
                        "sk": {"S": "MSG#m"},
                        "gsi2pk": {"S": "INBOX#a#MSG"},
                        "gsi2sk": {"S": "2026-01-01"},
                    },
                })]
            );
        }

        /// A first page carries no `ExclusiveStartKey`, and a limit beyond
        /// `i32` saturates rather than wrapping.
        #[tokio::test]
        async fn a_first_page_has_no_start_key_and_a_huge_limit_saturates() {
            let server = MockServer::start().await;
            mount(&server, "Query", query_response(None)).await;
            let query = PageQuery::unbounded(MailIndex::ByTime, "INBOXES", usize::MAX, None, "Q");
            table(&server).query::<Row>(query).await.unwrap();
            assert_eq!(
                bodies(&server, "Query").await,
                vec![json!({
                    "TableName": TABLE,
                    "IndexName": "ByTime",
                    "KeyConditionExpression": "#pk = :pk",
                    "ExpressionAttributeNames": {"#pk": "gsi1pk"},
                    "ExpressionAttributeValues": {":pk": {"S": "INBOXES"}},
                    "ScanIndexForward": true,
                    "Limit": i32::MAX,
                })]
            );
        }

        #[tokio::test]
        async fn a_last_evaluated_key_becomes_the_next_page_key() {
            let server = MockServer::start().await;
            mount(
                &server,
                "Query",
                query_response(Some(json!({
                    "pk": {"S": "INBOX#a"},
                    "sk": {"S": "MSG#b"},
                    "gsi1pk": {"S": "INBOX#a#MSG"},
                    "gsi1sk": {"S": "2026-01-02"},
                }))),
            )
            .await;
            let query = PageQuery::unbounded(MailIndex::ByTime, "INBOX#a#MSG", 2, None, "Q");
            let page = table(&server).query::<Row>(query).await.unwrap();
            assert_eq!(
                page.next,
                Some(PageKey {
                    partition: "INBOX#a#MSG".to_owned(),
                    sort: "2026-01-02".to_owned(),
                    table_pk: "INBOX#a".to_owned(),
                    table_sk: "MSG#b".to_owned(),
                })
            );
            assert_eq!(
                page.items,
                vec![
                    Row {
                        pk: "A".to_owned(),
                        sk: "1".to_owned(),
                        n: 1,
                    },
                    Row {
                        pk: "B".to_owned(),
                        sk: "2".to_owned(),
                        n: 2,
                    },
                ]
            );
        }

        /// A `LastEvaluatedKey` missing an attribute, or holding a non-string
        /// one, yields no continuation rather than an unusable one.
        #[test]
        fn an_incomplete_last_evaluated_key_gives_no_page_key() {
            let full = HashMap::from([
                ("pk".to_owned(), DynamoAv::S("P".to_owned())),
                ("sk".to_owned(), DynamoAv::S("S".to_owned())),
                ("gsi1pk".to_owned(), DynamoAv::S("IP".to_owned())),
                ("gsi1sk".to_owned(), DynamoAv::S("IS".to_owned())),
            ]);
            assert!(page_key_from_last_evaluated(&full, "gsi1pk", "gsi1sk").is_some());
            for name in ["pk", "sk", "gsi1pk", "gsi1sk"] {
                let mut missing = full.clone();
                missing.remove(name);
                assert!(
                    page_key_from_last_evaluated(&missing, "gsi1pk", "gsi1sk").is_none(),
                    "{name} missing"
                );
                let mut wrong_type = full.clone();
                wrong_type.insert(name.to_owned(), DynamoAv::N("1".to_owned()));
                assert!(
                    page_key_from_last_evaluated(&wrong_type, "gsi1pk", "gsi1sk").is_none(),
                    "{name} not a string"
                );
            }
        }

        #[tokio::test]
        async fn a_query_item_that_does_not_deserialize_is_permanent() {
            let server = MockServer::start().await;
            mount(
                &server,
                "Query",
                ok(&json!({"Items": [{"pk": {"S": "A"}, "sk": {"S": "1"}, "n": {"S": "x"}}]})),
            )
            .await;
            let query = PageQuery::unbounded(MailIndex::ByTime, "INBOXES", 10, None, "Query(x)");
            let error = table(&server).query::<Row>(query).await.unwrap_err();
            assert!(matches!(error, MailStoreError::Permanent(_)), "{error:?}");
            assert!(error_text(&error).contains("Query(x)"), "{error:?}");
        }

        #[tokio::test]
        async fn a_failed_query_uses_the_sdk_classification() {
            let server = MockServer::start().await;
            mount(&server, "Query", dynamo_error(500, "InternalServerError")).await;
            let query = PageQuery::unbounded(MailIndex::ByStatus, "STATUS#queued", 10, None, "Q");
            let error = table(&server).query::<Row>(query).await.unwrap_err();
            assert!(matches!(error, MailStoreError::Transient(_)), "{error:?}");
        }

        // --- pause ---------------------------------------------------------

        /// The wait between transaction attempts is at least the exponential
        /// base (25 ms doubling, capped at 400 ms from the fifth attempt on)
        /// and less than twice it, whatever the jitter.
        #[tokio::test(start_paused = true)]
        async fn the_retry_pause_is_its_base_plus_less_than_one_base_of_jitter() {
            let server = MockServer::start().await;
            let table = table(&server);
            for (attempt, base_ms) in [
                (0, 25),
                (1, 50),
                (2, 100),
                (3, 200),
                (4, 400),
                (5, 400),
                (10, 400),
            ] {
                let started = tokio::time::Instant::now();
                table.pause(attempt).await;
                let elapsed = started.elapsed();
                let base = Duration::from_millis(base_ms);
                assert!(
                    elapsed >= base && elapsed < base * 2,
                    "attempt {attempt}: waited {elapsed:?}"
                );
            }
        }

        // --- mail disabled -------------------------------------------------

        /// With no table name every call fails permanently before sending
        /// anything, except an empty batch, which has nothing to read.
        #[tokio::test]
        async fn every_call_fails_permanently_when_mail_is_not_configured() {
            let server = MockServer::start().await;
            let table = DynamoTable::new(client(&server), None);
            let permanent = |result: Result<(), MailStoreError>| {
                assert!(
                    matches!(result, Err(MailStoreError::Permanent(_))),
                    "{result:?}"
                );
            };
            permanent(
                table
                    .get::<Row>(key("A", "1"), Read::Eventual, "G")
                    .await
                    .map(drop),
            );
            permanent(
                table
                    .batch_get::<Row>(vec![key("A", "1")], Read::Eventual, "B")
                    .await
                    .map(drop),
            );
            permanent(
                table
                    .query::<Row>(PageQuery::unbounded(MailIndex::ByTime, "P", 1, None, "Q"))
                    .await
                    .map(drop),
            );
            permanent(
                table
                    .put_if_absent(serde_dynamo::Item::default(), "P")
                    .await
                    .map(drop),
            );
            permanent(table.transact(&[]).await.map(drop));
            assert!(
                table
                    .batch_get::<Row>(Vec::new(), Read::Eventual, "B")
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .is_empty()
            );
        }
    }
}
