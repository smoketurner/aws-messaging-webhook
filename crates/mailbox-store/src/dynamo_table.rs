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
}
