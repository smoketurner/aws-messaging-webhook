//! DynamoDB item sizing and the item budget (`fit_item`).
//!
//! [`dynamo_item_size`] follows AWS's documented item-size rules
//! (<https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html>):
//! attribute name bytes + attribute value bytes, with a fixed 3-byte overhead
//! per nested list/map. Numbers are approximated by their string-literal byte
//! length (DynamoDB's real number encoding is more compact but never larger),
//! so this is a safe over-estimate, never an under-estimate — exactly what a
//! budget check needs.

use serde_dynamo::{AttributeValue, Item};

use crate::mail::{ITEM_BUDGET_BYTES, MailMessage};

/// A fixed per-nested-collection overhead DynamoDB charges for `L` and `M`
/// attribute values, on top of their members' sizes.
const COLLECTION_OVERHEAD_BYTES: usize = 3;

fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => b.len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::M(map) => {
            COLLECTION_OVERHEAD_BYTES
                + map
                    .iter()
                    .map(|(k, v)| k.len() + attribute_value_size(v))
                    .sum::<usize>()
        }
        AttributeValue::L(list) => {
            COLLECTION_OVERHEAD_BYTES + list.iter().map(attribute_value_size).sum::<usize>()
        }
        AttributeValue::Ss(set) | AttributeValue::Ns(set) => set.iter().map(String::len).sum(),
        AttributeValue::Bs(set) => set.iter().map(Vec::len).sum(),
    }
}

/// The DynamoDB item size: the sum of every top-level attribute
/// name's byte length plus its value's size.
#[must_use]
pub fn dynamo_item_size(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + attribute_value_size(value))
        .sum()
}

/// The combined size of every item in one `TransactWriteItems` call, bounded
/// at 4 MB.
#[must_use]
pub fn transaction_bytes(items: &[Item]) -> usize {
    items.iter().map(dynamo_item_size).sum()
}

/// Serializes `msg` the same way the store does, for a size check.
fn item_of(msg: &MailMessage) -> Item {
    // `MailMessage` has no non-serializable fields (plain strings, numbers,
    // and JSON values), so this cannot fail in practice.
    serde_dynamo::to_item(msg).unwrap_or_default()
}

/// Shrinks `msg` in place until its DynamoDB item size is at most
/// [`ITEM_BUDGET_BYTES`], in this order:
///
/// 1. `html`, truncated at a `char` boundary (sets `body_truncated`).
/// 2. `text`, truncated at a `char` boundary (sets `body_truncated`).
/// 3. `headers`, dropping the largest-value entry first, one at a time.
/// 4. `attachments`, dropping from the tail one at a time (sets
///    `attachments_truncated`).
/// 5. `thread_snapshot.recipients`, dropping from the tail one at a time.
/// 6. `thread_snapshot.senders`, dropping from the tail one at a time (
///    after `recipients`, both still last).
///
/// Each step re-measures after every edit and stops as soon as the item
/// fits; a step that runs out of material to shrink (empty string, no
/// headers, no attachments, no recipients, no senders) falls through to the
/// next one. Ingest and send never fail on size — by construction, this
/// always reaches budget: every unbounded field is eventually emptied.
pub fn fit_item(msg: &mut MailMessage) {
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }

    shrink_body(msg, Field::Html);
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }
    shrink_body(msg, Field::Text);
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }
    shrink_headers(msg);
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }
    shrink_attachments(msg);
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }
    shrink_thread_snapshot_recipients(msg);
    if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
        return;
    }
    shrink_thread_snapshot_senders(msg);
}

#[derive(Clone, Copy)]
enum Field {
    Html,
    Text,
}

/// Halves `msg.html`/`msg.text` (at a `char` boundary) until either the item
/// fits or the field is empty, then clears it and marks `body_truncated`.
fn shrink_body(msg: &mut MailMessage, field: Field) {
    loop {
        let body = match field {
            Field::Html => &mut msg.html,
            Field::Text => &mut msg.text,
        };
        let Some(text) = body else { return };
        if text.is_empty() {
            *body = None;
            return;
        }
        let half = floor_char_boundary(text, text.len() / 2);
        text.truncate(half);
        msg.body_truncated = true;
        if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
            return;
        }
    }
}

/// The largest `char` boundary at or before `index`. `str::floor_char_boundary`
/// is nightly-only as of this MSRV, so this mirrors it directly.
fn floor_char_boundary(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Drops the largest-value header, one at a time, until the item fits or
/// `headers` is empty.
fn shrink_headers(msg: &mut MailMessage) {
    while !msg.headers.is_empty() {
        if let Some(largest_key) = msg
            .headers
            .iter()
            .max_by_key(|(k, v)| k.len() + v.len())
            .map(|(k, _)| k.clone())
        {
            msg.headers.remove(&largest_key);
        }
        if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
            return;
        }
    }
}

/// Drops attachments from the tail, one at a time, until the item fits or
/// none remain.
fn shrink_attachments(msg: &mut MailMessage) {
    while msg.attachments.pop().is_some() {
        msg.attachments_truncated = true;
        if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
            return;
        }
    }
}

/// Drops `thread_snapshot.recipients` from the tail, one at a time, until the
/// item fits or none remain.
fn shrink_thread_snapshot_recipients(msg: &mut MailMessage) {
    loop {
        let popped = match &mut msg.thread_snapshot {
            Some(snapshot) => snapshot.recipients.pop().is_some(),
            None => return,
        };
        if !popped {
            return;
        }
        if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
            return;
        }
    }
}

/// Drops `thread_snapshot.senders` from the tail, one at a time, until the
/// item fits or none remain; shrinks after `recipients`.
fn shrink_thread_snapshot_senders(msg: &mut MailMessage) {
    loop {
        let popped = match &mut msg.thread_snapshot {
            Some(snapshot) => snapshot.senders.pop().is_some(),
            None => return,
        };
        if !popped {
            return;
        }
        if dynamo_item_size(&item_of(msg)) <= ITEM_BUDGET_BYTES {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use serde_dynamo::{AttributeValue, Item};

    use super::*;
    use crate::mail::{AttachmentMeta, Direction, InboxId, ThreadSnapshot};

    fn item(entries: Vec<(&str, AttributeValue)>) -> Item {
        let mut item = Item::default();
        for (k, v) in entries {
            item.inner_mut().insert(k.to_owned(), v);
        }
        item
    }

    #[test]
    fn scalar_sizes_match_documented_rules() {
        assert_eq!(attribute_value_size(&AttributeValue::S("hello".into())), 5);
        assert_eq!(attribute_value_size(&AttributeValue::N("123".into())), 3);
        assert_eq!(attribute_value_size(&AttributeValue::B(vec![1, 2, 3])), 3);
        assert_eq!(attribute_value_size(&AttributeValue::Bool(true)), 1);
        assert_eq!(attribute_value_size(&AttributeValue::Null(true)), 1);
    }

    #[test]
    fn map_and_list_add_collection_overhead() {
        let list = AttributeValue::L(vec![
            AttributeValue::S("ab".into()),
            AttributeValue::S("cd".into()),
        ]);
        assert_eq!(attribute_value_size(&list), 3 + 2 + 2);

        let mut map = std::collections::HashMap::new();
        map.insert("k".to_owned(), AttributeValue::S("v".into()));
        assert_eq!(attribute_value_size(&AttributeValue::M(map)), 3 + 1 + 1);
    }

    #[test]
    fn item_size_sums_name_and_value() {
        let item = item(vec![
            ("pk", AttributeValue::S("INBOX#a".into())),
            ("size", AttributeValue::N("100".into())),
        ]);
        // "pk"(2) + "INBOX#a"(7) + "size"(4) + "100"(3) = 16
        assert_eq!(dynamo_item_size(&item), 16);
    }

    #[test]
    fn transaction_bytes_sums_every_item() {
        let a = item(vec![("a", AttributeValue::S("x".into()))]);
        let b = item(vec![("bb", AttributeValue::S("yy".into()))]);
        assert_eq!(transaction_bytes(&[a, b]), (1 + 1) + (2 + 2));
    }

    fn base_message() -> MailMessage {
        MailMessage {
            inbox_id: InboxId("support".to_owned()),
            thread_id: "tid-1".to_owned(),
            message_id: "mid-1".to_owned(),
            ses_message_id: None,
            direction: Direction::Inbound,
            rfc_message_id: "<mid-1@example.com>".to_owned(),
            in_reply_to: None,
            references: Vec::new(),
            labels: vec!["received".to_owned(), "unread".to_owned()],
            timestamp: crate::mail::time::format(1_700_000_000_000),
            from: "sender@example.com".to_owned(),
            reply_to: Vec::new(),
            to: vec!["support@example.com".to_owned()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            size: 1_000,
            text: Some("hello there".to_owned()),
            html: Some("<p>hello there</p>".to_owned()),
            body_truncated: false,
            headers: BTreeMap::new(),
            attachments: Vec::new(),
            attachments_truncated: false,
            raw_s3_key: Some("inbound/x".to_owned()),
            verdicts: None,
            thread_snapshot: None,
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 0,
            created_at: crate::mail::time::format(1_700_000_000_000),
            updated_at: crate::mail::time::format(1_700_000_000_000),
        }
    }

    #[test]
    fn fit_item_is_a_no_op_under_budget() {
        let mut msg = base_message();
        let before = item_of(&msg);
        fit_item(&mut msg);
        assert_eq!(item_of(&msg), before);
        assert!(!msg.body_truncated);
    }

    #[test]
    fn fit_item_shrinks_html_first() {
        let mut msg = base_message();
        msg.html = Some("x".repeat(500_000));
        fit_item(&mut msg);
        assert!(dynamo_item_size(&item_of(&msg)) <= ITEM_BUDGET_BYTES);
        assert!(msg.body_truncated);
        // text was untouched: html alone was enough to shrink under budget.
        assert_eq!(msg.text.as_deref(), Some("hello there"));
    }

    #[test]
    fn fit_item_falls_through_to_headers_then_attachments() {
        let mut msg = base_message();
        msg.html = Some("x".repeat(200_000));
        msg.text = Some("y".repeat(200_000));
        for i in 0..50 {
            msg.headers
                .insert(format!("X-Custom-{i}"), "z".repeat(2_000));
        }
        for i in 0..20 {
            msg.attachments.push(AttachmentMeta {
                attachment_id: format!("att-{i}"),
                object_key: Some(format!("attachments/mid-1/att-{i}")),
                size: 1_000,
                filename: Some(format!("file-{i}.txt")),
                content_type: "text/plain".to_owned(),
                content_disposition: "attachment".to_owned(),
                content_id: None,
            });
        }
        fit_item(&mut msg);
        assert!(dynamo_item_size(&item_of(&msg)) <= ITEM_BUDGET_BYTES);
        assert!(msg.body_truncated);
    }

    fn thread_snapshot(recipients: Vec<String>, senders: Vec<String>) -> ThreadSnapshot {
        ThreadSnapshot {
            thread_id: "tid-1".to_owned(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            message_count: 1,
            labels: vec!["received".to_owned()],
            senders,
            recipients,
            timestamp: crate::mail::time::format(1_700_000_000_000),
            created_at: crate::mail::time::format(1_700_000_000_000),
            updated_at: crate::mail::time::format(1_700_000_000_000),
        }
    }

    #[test]
    fn fit_item_shrinks_thread_snapshot_recipients_before_senders() {
        let mut msg = base_message();
        msg.html = None;
        msg.text = None;
        msg.thread_snapshot = Some(thread_snapshot(
            (0..5000).map(|i| format!("user{i}@example.com")).collect(),
            Vec::new(),
        ));
        fit_item(&mut msg);
        assert!(dynamo_item_size(&item_of(&msg)) <= ITEM_BUDGET_BYTES);
    }

    #[test]
    fn fit_item_shrinks_thread_snapshot_senders_last() {
        let mut msg = base_message();
        msg.html = None;
        msg.text = None;
        msg.thread_snapshot = Some(thread_snapshot(
            Vec::new(),
            (0..5000).map(|i| format!("user{i}@example.com")).collect(),
        ));
        fit_item(&mut msg);
        assert!(dynamo_item_size(&item_of(&msg)) <= ITEM_BUDGET_BYTES);
    }

    proptest! {
        /// At every bounded maximum (large html/text, many headers, many
        /// attachments, a large thread snapshot), `fit_item` always reaches
        /// budget and never panics.
        #[test]
        fn fit_item_always_reaches_budget_at_maxima(
            html_len in 0usize..600_000,
            text_len in 0usize..600_000,
            header_count in 0usize..80,
            attachment_count in 0usize..100,
            recipient_count in 0usize..6000,
            sender_count in 0usize..6000,
        ) {
            let mut msg = base_message();
            msg.html = Some("h".repeat(html_len));
            msg.text = Some("t".repeat(text_len));
            for i in 0..header_count {
                msg.headers.insert(format!("X-Header-{i}"), "v".repeat(900));
            }
            for i in 0..attachment_count {
                msg.attachments.push(AttachmentMeta {
                    attachment_id: format!("att-{i}"),
                    object_key: Some(format!("attachments/mid-1/att-{i}")),
                    size: 1_000,
                    filename: Some(format!("file-{i}.txt")),
                    content_type: "application/octet-stream".to_owned(),
                    content_disposition: "attachment".to_owned(),
                    content_id: None,
                });
            }
            msg.thread_snapshot = Some(thread_snapshot(
                (0..recipient_count).map(|i| format!("user{i}@example.com")).collect(),
                (0..sender_count).map(|i| format!("sender{i}@example.com")).collect(),
            ));

            fit_item(&mut msg);

            prop_assert!(dynamo_item_size(&item_of(&msg)) <= ITEM_BUDGET_BYTES);
        }
    }
}
