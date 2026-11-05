//! What insertion leaves in the RFC alias rows, read back from the
//! in-memory store's raw items: alias claiming and skipping, placeholder
//! expiry, and the alias id length cap.

use mailbox::{InboxId, MailMessage, REFERENCES_MAX_BYTES};
use serde_dynamo::{AttributeValue, Item};

use crate::keys;
use crate::memory::{MailMemoryStore, sample_message};
use crate::plan::ALIAS_ID_MAX_BYTES;

const INBOX: &str = "support@example.com";

fn inbox() -> InboxId {
    InboxId::new(INBOX)
}

fn message(message_id: &str, thread_id: &str) -> MailMessage {
    let mut msg = sample_message(INBOX, message_id, thread_id);
    msg.expires_at = 1_900_000_000;
    msg
}

fn alias_row(store: &MailMemoryStore, id: &str) -> Option<Item> {
    store.raw_item(&keys::rfc_alias(&inbox(), id))
}

fn string(item: &Item, name: &str) -> Option<String> {
    match item.inner().get(name) {
        Some(AttributeValue::S(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Placeholders cover exactly the referenced ids no row maps: never the
/// message's own id, which gets a real alias, and never an id another row
/// already maps.
#[tokio::test]
async fn placeholders_are_written_only_for_unmapped_references() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let root = message("mid-0", "tid-0");
    mailbox
        .insert_message(&root, &["mid-0@example.com".to_owned()])
        .await
        .unwrap();

    let reply = message("mid-1", "tid-1");
    mailbox
        .insert_message(
            &reply,
            &[
                "mid-1@example.com".to_owned(),
                "parent@example.com".to_owned(),
                "mid-0@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();

    let own = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&own, "message_id").as_deref(), Some("mid-1"));
    let placeholder = alias_row(&store, "parent@example.com").unwrap();
    assert_eq!(string(&placeholder, "message_id"), None);
    let mapped = alias_row(&store, "mid-0@example.com").unwrap();
    assert_eq!(string(&mapped, "message_id").as_deref(), Some("mid-0"));
}

/// A message takes over the placeholder an earlier reference left for it,
/// but never another message's alias for the same id.
#[tokio::test]
async fn a_message_claims_its_own_placeholder_but_not_another_messages_alias() {
    let store = MailMemoryStore::default();
    let mailbox = store.mailbox();
    let referrer = message("mid-0", "tid-0");
    mailbox
        .insert_message(
            &referrer,
            &[
                "mid-0@example.com".to_owned(),
                "mid-1@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();
    let placeholder = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&placeholder, "message_id"), None);

    let late = message("mid-1", "tid-1");
    mailbox
        .insert_message(&late, &["mid-1@example.com".to_owned()])
        .await
        .unwrap();
    let claimed = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&claimed, "message_id").as_deref(), Some("mid-1"));

    let mut duplicate_id = message("mid-2", "tid-2");
    duplicate_id.rfc_message_id = "<mid-1@example.com>".to_owned();
    mailbox
        .insert_message(&duplicate_id, &["mid-1@example.com".to_owned()])
        .await
        .unwrap();
    let kept = alias_row(&store, "mid-1@example.com").unwrap();
    assert_eq!(string(&kept, "message_id").as_deref(), Some("mid-1"));
}

#[tokio::test]
async fn an_oversized_placeholder_is_skipped() {
    let store = MailMemoryStore::default();
    let oversized = format!("{}@example.com", "x".repeat(ALIAS_ID_MAX_BYTES));
    let msg = message("mid-1", "tid-1");
    store
        .mailbox()
        .insert_message(&msg, &["mid-1@example.com".to_owned(), oversized.clone()])
        .await
        .unwrap();
    assert!(alias_row(&store, &oversized).is_none());
    assert!(alias_row(&store, "mid-1@example.com").is_some());
}

/// A placeholder outliving the message whose reference it records would
/// thread a later reply into a thread that no longer exists.
#[tokio::test]
async fn a_placeholder_points_at_the_messages_thread_and_expires_with_it() {
    let store = MailMemoryStore::default();
    let msg = message("mid-1", "tid-1");
    store
        .mailbox()
        .insert_message(
            &msg,
            &[
                "mid-1@example.com".to_owned(),
                "parent@example.com".to_owned(),
            ],
        )
        .await
        .unwrap();
    let placeholder = alias_row(&store, "parent@example.com").unwrap();
    assert_eq!(string(&placeholder, "thread_id").as_deref(), Some("tid-1"));
    assert_eq!(
        placeholder.inner().get("expires_at"),
        Some(&AttributeValue::N(msg.expires_at.to_string()))
    );
}

#[tokio::test]
async fn an_oversized_message_id_gets_no_alias() {
    let store = MailMemoryStore::default();
    let mut msg = message("mid-1", "tid-1");
    let id = format!("{}@example.com", "x".repeat(ALIAS_ID_MAX_BYTES + 10));
    msg.rfc_message_id = format!("<{id}>");
    store.mailbox().insert_message(&msg, &[]).await.unwrap();
    assert!(alias_row(&store, &id).is_none());
}

/// A `Message-ID` truncated to `REFERENCES_MAX_BYTES` by `mime::parse_inbound`
/// is still too long to carry an alias row once its angle brackets are
/// stripped, so truncating never changes which ids carry one.
#[tokio::test]
async fn a_message_id_truncated_to_the_cap_still_gets_no_alias() {
    let store = MailMemoryStore::default();
    let mut msg = message("mid-1", "tid-1");
    let id = "a".repeat(REFERENCES_MAX_BYTES.saturating_sub(2));
    msg.rfc_message_id = format!("<{id}>");
    assert_eq!(msg.rfc_message_id.len(), REFERENCES_MAX_BYTES);
    store.mailbox().insert_message(&msg, &[]).await.unwrap();
    assert!(alias_row(&store, &id).is_none());
}

/// An id exactly at the cap is short enough for an alias row.
#[tokio::test]
async fn a_message_id_exactly_at_the_cap_gets_an_alias() {
    let store = MailMemoryStore::default();
    let mut msg = message("mid-1", "tid-1");
    let domain = "@example.com";
    let id = format!("{}{domain}", "x".repeat(ALIAS_ID_MAX_BYTES - domain.len()));
    assert_eq!(id.len(), ALIAS_ID_MAX_BYTES);
    msg.rfc_message_id = format!("<{id}>");
    store.mailbox().insert_message(&msg, &[]).await.unwrap();
    assert!(alias_row(&store, &id).is_some());
}
