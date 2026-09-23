//! Round-trip tests for `content::store` / `content::load` against the
//! in-memory `FakeObjectStore`, which honors the same `get_object`
//! `max_bytes` contract the production `AwsServices` impl does — so a
//! `TooLarge` here is the same failure mode the read path sees in
//! production.
//!
//! The store/load pair shares one cap (`content::MAX_CONTENT_BYTES`), so a
//! document `store` writes can always be read back. The regression this
//! guards is a body of control characters: `serde_json` escapes each C0
//! code as the 6-byte `\u00XX` sequence, so such a body inflates 6:1 and
//! the old `2 × MAX_INBOUND_RAW_BYTES` read cap rejected documents `store`
//! had happily written, breaking the round-trip and the parsed-content
//! read paths (`get_message`, `get_thread`, `reply`) and silently
//! stripping the body from the stream relay's `message.received*` event.

use aws_messaging_webhook::mail::content::{self, MessageContent};
use aws_messaging_webhook::mail::{Direction, InboxId, MAX_INBOUND_RAW_BYTES, MailMessage};
use std::collections::BTreeMap;
use webhook_test_support::objects::FakeObjectStore;

/// A `MailMessage` carrying only the fields `content::load` reads —
/// `inbox_id` and `message_id`, which key the content document.
fn message_for(inbox: &InboxId, message_id: &str) -> MailMessage {
    MailMessage {
        inbox_id: inbox.clone(),
        thread_id: String::new(),
        message_id: message_id.to_owned(),
        ses_message_id: None,
        direction: Direction::Inbound,
        rfc_message_id: String::new(),
        in_reply_to: None,
        labels: Vec::new(),
        timestamp: String::new(),
        from: String::new(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: String::new(),
        preview: String::new(),
        size: 0,
        attachments: Vec::new(),
        attachments_truncated: false,
        raw_s3_key: None,
        thread_snapshot: None,
        delivery: BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 0,
        created_at: String::new(),
        updated_at: String::new(),
        expires_at: 0,
    }
}

/// A small control-character body round-trips unchanged, a fast canary that
/// the store/load paths agree for the kind of body that triggers the 6:1 JSON
/// escape — the case the old `2 ×` cap was silently corrupting once the body
/// grew past its boundary.
#[tokio::test]
async fn a_small_control_char_body_round_trips() {
    let store = FakeObjectStore::default();
    let inbox = InboxId("support@example.com".to_owned());
    let content = MessageContent {
        text: Some("\u{0}".repeat(1000)),
        html: Some("\u{1}".repeat(500)),
        ..MessageContent::default()
    };

    content::store(&store, &inbox, "mid-1", &content)
        .await
        .unwrap();

    let key = "messages/support@example.com/mid-1.json";
    assert!(store.contains(key), "store writes the content document");

    let loaded = content::load(&store, &message_for(&inbox, "mid-1"))
        .await
        .unwrap();
    assert_eq!(loaded, content);
}

/// The repro: a NUL-byte body large enough that `serde_json` inflates it
/// past the old `2 × MAX_INBOUND_RAW_BYTES` read cap but still under the
/// new `6 ×` cap. Under the bug, `store` wrote this document and `load`
/// rejected it as `TooLarge`; now it round-trips, so every parsed-content
/// read path (`get_message`, `get_thread`, `reply`) and the stream relay
/// see the body instead of HTTP 500 / a silently emptied webhook event.
#[tokio::test]
async fn a_control_char_body_that_exceeded_the_old_cap_now_round_trips() {
    let store = FakeObjectStore::default();
    let inbox = InboxId("support@example.com".to_owned());

    // One-third of the raw-mail ceiling of NUL bytes inflates to just over
    // the old `2 ×` cap: 6 × (MAX_INBOUND_RAW_BYTES / 3) = 2 ×
    // MAX_INBOUND_RAW_BYTES, plus one byte to cross it.
    let nul_bytes = usize::try_from(MAX_INBOUND_RAW_BYTES / 3).unwrap() + 1;
    let content = MessageContent {
        text: Some("\u{0}".repeat(nul_bytes)),
        ..MessageContent::default()
    };

    let serialized = serde_json::to_vec(&content).unwrap();
    assert!(
        serialized.len() as u64 > 2 * MAX_INBOUND_RAW_BYTES,
        "the repro must exceed the old 2x cap; serialized = {}",
        serialized.len()
    );

    content::store(&store, &inbox, "nul-1", &content)
        .await
        .unwrap();
    let loaded = content::load(&store, &message_for(&inbox, "nul-1"))
        .await
        .unwrap();
    assert_eq!(loaded, content);
}
