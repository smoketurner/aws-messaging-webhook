//! The mail table's DynamoDB Streams relay: `message.received*`
//! on a `MSG#` INSERT, nothing on any other mail-table item or on a MODIFY,
//! and the existing events-table relay left unaffected.
//!
//! Stream images are built with `serde_dynamo::to_item` on
//! [`aws_messaging_webhook::mail::MailMessage`] — never hand-parsed
//! attribute JSON — matching what the store actually writes.

// This binary's own helper functions never fail except on a bug in the test
// itself (mirrors `webhook_test_support`'s file-level expectation, which
// doesn't extend to code outside that crate).
#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

use std::collections::BTreeMap;

use aws_messaging_webhook::mail::content::{self, MessageContent};
use aws_messaging_webhook::mail::{Direction, InboxId, MailMessage, ThreadSnapshot, keys};
use serde_dynamo::AttributeValue;
use serde_json::{Value, json};
use webhook_test_support::{Harness, harness, invoke};

/// Stores a message's content document where the relay reads it.
async fn store_content(h: &Harness, msg: &MailMessage, message_content: MessageContent) {
    content::store(h.fake(), &msg.inbox_id, &msg.message_id, &message_content)
        .await
        .unwrap();
}

fn sample_message() -> MailMessage {
    MailMessage {
        inbox_id: InboxId("support".to_owned()),
        thread_id: "tid-1".to_owned(),
        message_id: "mid-1".to_owned(),
        ses_message_id: Some("ses-1".to_owned()),
        direction: Direction::Inbound,
        rfc_message_id: "<mid-1@example.com>".to_owned(),
        in_reply_to: None,
        labels: vec!["received".to_owned(), "unread".to_owned()],
        timestamp: "2026-01-15T09:30:00.000Z".to_owned(),
        from: "sender@example.com".to_owned(),
        to: vec!["support@example.com".to_owned()],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_owned(),
        preview: "Hello there".to_owned(),
        size: 1234,
        attachments: Vec::new(),
        attachments_truncated: false,
        raw_s3_key: Some("inbound/x".to_owned()),
        thread_snapshot: Some(ThreadSnapshot {
            thread_id: "tid-1".to_owned(),
            subject: "Hello".to_owned(),
            preview: "Hello there".to_owned(),
            message_count: 1,
            labels: vec!["received".to_owned(), "unread".to_owned()],
            senders: vec!["sender@example.com".to_owned()],
            recipients: vec!["support@example.com".to_owned()],
            timestamp: "2026-01-15T09:30:00.000Z".to_owned(),
            created_at: "2026-01-15T09:30:00.000Z".to_owned(),
            updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
        }),
        delivery: BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 1,
        created_at: "2026-01-15T09:30:00.000Z".to_owned(),
        updated_at: "2026-01-15T09:30:00.000Z".to_owned(),
        expires_at: 0,
    }
}

/// Builds a `MailMessage`'s stream `NewImage` the way the real store writes
/// it: `serde_dynamo::to_item` on the struct, plus the `pk`/`sk` key
/// attributes the store adds separately. Serializing the resulting
/// `Item` reproduces the DynamoDB JSON wire shape (`{"S": "..."}`, …) a real
/// stream record carries.
fn message_new_image(msg: &MailMessage) -> Value {
    let mut item: serde_dynamo::Item = serde_dynamo::to_item(msg).unwrap();
    item.insert(
        "pk".to_owned(),
        AttributeValue::S(keys::inbox_pk(msg.inbox_id.as_str())),
    );
    item.insert(
        "sk".to_owned(),
        AttributeValue::S(keys::message_sk(&msg.message_id)),
    );
    serde_json::to_value(&item).unwrap()
}

/// One DynamoDB Streams record for a mail-table item. `new_image` is omitted
/// (`None`) for a MODIFY/REMOVE test that only needs the key attributes to
/// route on `sk`; `OldImage` is never included (an INSERT's absent old
/// image must not fail deserialization, and the P1 relay never reads it).
fn mail_stream_event(event_name: &str, new_image: &Value, sequence: &str) -> Value {
    json!({
        "Records": [{
            "awsRegion": "us-east-1",
            "eventID": "evt-mail-1",
            "eventName": event_name,
            "eventSource": "aws:dynamodb",
            "dynamodb": {
                "ApproximateCreationDateTime": 1_754_265_600.0,
                "SequenceNumber": sequence,
                "SizeBytes": 42,
                "StreamViewType": "NEW_IMAGE",
                "NewImage": new_image,
            }
        }]
    })
}

/// A minimal non-`MSG#` mail-table item image: only the key attributes the
/// relay's `sk` dispatch reads, since these items are skipped before any
/// `MailMessage` deserialization is attempted.
fn key_only_image(pk: &str, sk: &str) -> Value {
    json!({ "pk": {"S": pk}, "sk": {"S": sk} })
}

#[tokio::test]
async fn insert_received_publishes_one_event_with_the_golden_payload() {
    let h = harness().await;
    let msg = sample_message();
    store_content(
        &h,
        &msg,
        MessageContent {
            text: Some("Hello there".to_owned()),
            ..MessageContent::default()
        },
    )
    .await;
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-1");

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    let detail = &published[0].detail;
    assert_eq!(published[0].detail_type, "message.received");
    assert_eq!(detail["type"], "event");
    assert_eq!(detail["event_type"], "message.received");
    assert!(
        detail["event_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("evt_"))
    );
    assert_eq!(detail["schemaVersion"], 1);
    assert_eq!(detail["meta"]["messageId"], "mid-1");
    assert_eq!(detail["meta"]["inboxId"], "support");
    assert_eq!(detail["meta"]["threadId"], "tid-1");
    assert_eq!(detail["meta"]["sesMessageId"], "ses-1");
    assert_eq!(detail["message"]["message_id"], "mid-1");
    assert_eq!(detail["message"]["from"], "sender@example.com");
    assert_eq!(detail["message"]["text"], "Hello there");
    assert_eq!(detail["thread"]["thread_id"], "tid-1");
    assert_eq!(detail["thread"]["message_count"], 1);
    assert_eq!(
        h.fake().calls(),
        vec!["publish:message.received".to_owned()]
    );
}

/// The relay publishes the stored `thread_snapshot` verbatim, so a
/// reply into an existing thread publishes the thread's real accumulated
/// state, not a single-message stub.
#[tokio::test]
async fn insert_of_a_reply_publishes_the_accumulated_thread_state() {
    let h = harness().await;
    let mut msg = sample_message();
    msg.message_id = "mid-2".to_owned();
    msg.thread_snapshot = Some(ThreadSnapshot {
        thread_id: "tid-1".to_owned(),
        subject: "Hello".to_owned(),
        preview: "Second message body".to_owned(),
        message_count: 2,
        labels: vec!["received".to_owned(), "unread".to_owned()],
        senders: vec![
            "other@example.com".to_owned(),
            "sender@example.com".to_owned(),
        ],
        recipients: vec!["support@example.com".to_owned()],
        timestamp: "2026-01-15T09:31:00.000Z".to_owned(),
        created_at: "2026-01-15T09:30:00.000Z".to_owned(),
        updated_at: "2026-01-15T09:31:00.000Z".to_owned(),
    });
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-reply");

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    let thread = &published[0].detail["thread"];
    assert_eq!(thread["message_count"], 2);
    assert_eq!(
        thread["senders"],
        json!(["other@example.com", "sender@example.com"])
    );
}

#[tokio::test]
async fn insert_spam_publishes_the_spam_variant() {
    let h = harness().await;
    let mut msg = sample_message();
    msg.labels = vec!["received".to_owned(), "spam".to_owned()];
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-2");

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail_type, "message.received.spam");
}

#[tokio::test]
async fn insert_unauthenticated_publishes_the_unauthenticated_variant() {
    let h = harness().await;
    let mut msg = sample_message();
    msg.labels = vec!["received".to_owned(), "unauthenticated".to_owned()];
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-3");

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail_type, "message.received.unauthenticated");
}

#[tokio::test]
async fn spam_takes_precedence_over_unauthenticated_on_the_stream_path() {
    let h = harness().await;
    let mut msg = sample_message();
    msg.labels = vec![
        "received".to_owned(),
        "spam".to_owned(),
        "unauthenticated".to_owned(),
    ];
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-4");

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published[0].detail_type, "message.received.spam");
}

#[tokio::test]
async fn modify_on_a_message_item_publishes_nothing() {
    let h = harness().await;
    let msg = sample_message();
    let event = mail_stream_event("MODIFY", &message_new_image(&msg), "seq-5");

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    assert!(h.fake().published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn send_state_marker_ref_and_key_items_publish_nothing() {
    for (pk, sk) in [
        ("OUTBOX#mid-1", "STATE"),
        ("SESCALL#mid-1", "CALL"),
        ("SESMSG#ses-1", "REF"),
        ("SENDKEY#deadbeef", "KEY"),
        ("INBOX#support", "THR#tid-1"),
        ("INBOX#support#LABEL#received", "MSGAT#mid-1"),
    ] {
        let h = harness().await;
        let event = mail_stream_event("INSERT", &key_only_image(pk, sk), "seq-key");
        let result = invoke(h.state.clone(), event).await.unwrap();
        assert_eq!(
            result,
            json!({ "batchItemFailures": [] }),
            "sk={sk} should not fail"
        );
        assert!(
            h.fake().published.lock().unwrap().is_empty(),
            "sk={sk} should not publish"
        );
    }
}

#[tokio::test]
async fn event_id_is_stable_across_a_replay_of_the_same_record() {
    let h = harness().await;
    let msg = sample_message();

    let first = mail_stream_event("INSERT", &message_new_image(&msg), "seq-a");
    invoke(h.state.clone(), first).await.unwrap();
    let second = mail_stream_event("INSERT", &message_new_image(&msg), "seq-b");
    invoke(h.state.clone(), second).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 2);
    assert_eq!(
        published[0].detail["event_id"],
        published[1].detail["event_id"]
    );
}

#[tokio::test]
async fn oversized_html_is_dropped_before_falling_back() {
    let h = harness().await;
    let msg = sample_message();
    store_content(
        &h,
        &msg,
        MessageContent {
            text: Some("Hello there".to_owned()),
            html: Some("x".repeat(300_000)),
            ..MessageContent::default()
        },
    )
    .await;
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-big");

    invoke(h.state.clone(), event).await.unwrap();

    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail["message"]["html"], Value::Null);
    assert_eq!(published[0].detail["message"]["text"], "Hello there");
    assert_eq!(published[0].detail["meta"]["messageId"], "mid-1");
}

#[tokio::test]
async fn a_transient_publish_failure_is_retried() {
    let h = harness().await;
    h.fake()
        .fail_publish
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let msg = sample_message();
    let event = mail_stream_event("INSERT", &message_new_image(&msg), "seq-fail");

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(
        result,
        json!({ "batchItemFailures": [{ "itemIdentifier": "seq-fail" }] })
    );
}

#[tokio::test]
async fn events_table_records_are_unaffected_by_the_mail_branch() {
    let h = harness().await;
    let mut body = sns_message_verifier::fixtures::notification(&h.cert_url);
    h.fixture.sign(&mut body, "2");
    let raw = serde_json::to_vec(&body).unwrap();
    let event = webhook_test_support::dynamodb_insert_event(
        &raw,
        "EVT#2026-08-04T00:00:00.000Z#sns-1",
        "seq-evt",
    );

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_ne!(published[0].detail_type, "message.received");
}

/// The events-table aggregate-status relay (unrelated to the mail branch)
/// still runs unchanged once the `sk`-prefix dispatch also checks for `MSG#`.
#[tokio::test]
async fn aggregate_status_relay_is_unaffected_by_the_mail_branch() {
    let h = harness().await;
    let event = webhook_test_support::dynamodb_agg_event("INSERT", Some("sent"), None);

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    let published = h.fake().published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].detail_type, "message.status.changed");
}

/// A record whose `NewImage` doesn't deserialize into `MailMessage` (a
/// malformed/partial mail item) settles without a retry — a deterministic
/// bug, not a transient fault — rather than looping forever.
#[tokio::test]
async fn a_message_item_that_fails_to_deserialize_settles_without_retry() {
    let h = harness().await;
    let bad_image = json!({
        "pk": {"S": "INBOX#support"},
        "sk": {"S": "MSG#mid-broken"},
        // Missing every other required MailMessage field.
    });
    let event = mail_stream_event("INSERT", &bad_image, "seq-broken");

    let result = invoke(h.state.clone(), event).await.unwrap();

    assert_eq!(result, json!({ "batchItemFailures": [] }));
    assert!(h.fake().published.lock().unwrap().is_empty());
}
