//! Mail table key builders, and the opaque page token every list endpoint
//! returns.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use mailbox::InboxId;
use mailbox::send::SendStatus;

/// A mail-table item's primary key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKey {
    pub pk: String,
    pub sk: String,
}

pub const INBOX_SK: &str = "META";

/// The index partition holding every inbox, so listing them is one query
/// rather than a table scan.
pub const INBOXES_PARTITION: &str = "INBOXES";

pub const SEND_STATE_SK: &str = "STATE";

pub const RFC_ALIAS_SK: &str = "RFC";

pub const SES_REF_SK: &str = "REF";

pub const SEND_KEY_SK: &str = "KEY";

/// The prefix of a message item's `sk`.
pub const MESSAGE_SK_PREFIX: &str = "MSG#";

const OUTBOX_PK_PREFIX: &str = "OUTBOX#";

fn inbox_pk(inbox: &InboxId) -> String {
    format!("INBOX#{}", inbox.as_str())
}

/// The inbox item's key.
#[must_use]
pub fn inbox(inbox: &InboxId) -> TableKey {
    TableKey {
        pk: inbox_pk(inbox),
        sk: INBOX_SK.to_owned(),
    }
}

/// A message item's key, in its inbox's partition.
#[must_use]
pub fn message(inbox: &InboxId, message_id: &(impl AsRef<str> + ?Sized)) -> TableKey {
    let message_id = message_id.as_ref();
    TableKey {
        pk: inbox_pk(inbox),
        sk: format!("{MESSAGE_SK_PREFIX}{message_id}"),
    }
}

/// A thread item's key, in its inbox's partition.
#[must_use]
pub fn thread(inbox: &InboxId, thread_id: &(impl AsRef<str> + ?Sized)) -> TableKey {
    let thread_id = thread_id.as_ref();
    TableKey {
        pk: inbox_pk(inbox),
        sk: format!("THR#{thread_id}"),
    }
}

/// The send-state item's key: a message's send state lives outside its
/// inbox partition so the sender never needs the inbox id to find it.
#[must_use]
pub fn send_state(message_id: &(impl AsRef<str> + ?Sized)) -> TableKey {
    let message_id = message_id.as_ref();
    TableKey {
        pk: format!("{OUTBOX_PK_PREFIX}{message_id}"),
        sk: SEND_STATE_SK.to_owned(),
    }
}

/// The message id a send-state item's `pk` names; `None` for any other `pk`.
#[must_use]
pub fn parse_outbox_pk(pk: &str) -> Option<&str> {
    pk.strip_prefix(OUTBOX_PK_PREFIX)
}

/// The RFC alias item's key: `rfc_id` is the `Message-ID` value with the
/// surrounding `<`/`>` stripped.
#[must_use]
pub fn rfc_alias(inbox: &InboxId, rfc_id: &str) -> TableKey {
    TableKey {
        pk: format!("RFC#{}#{rfc_id}", inbox.as_str()),
        sk: RFC_ALIAS_SK.to_owned(),
    }
}

/// The item mapping an SES message id back to the message it sent.
#[must_use]
pub fn ses_ref(ses_message_id: &(impl AsRef<str> + ?Sized)) -> TableKey {
    let ses_message_id = ses_message_id.as_ref();
    TableKey {
        pk: format!("SESMSG#{ses_message_id}"),
        sk: SES_REF_SK.to_owned(),
    }
}

/// The `Idempotency-Key` item's `pk`: `key_hash` is the lowercase hex
/// SHA-256 of the header value.
#[must_use]
pub fn send_key_pk(key_hash: &str) -> String {
    format!("SENDKEY#{key_hash}")
}

/// The `Idempotency-Key` item's key.
#[must_use]
pub fn send_key(key_hash: &str) -> TableKey {
    TableKey {
        pk: send_key_pk(key_hash),
        sk: SEND_KEY_SK.to_owned(),
    }
}

/// The `ByTime` partition listing an inbox's messages, sorted by message id.
#[must_use]
pub fn messages_partition(inbox: &InboxId) -> String {
    format!("INBOX#{}#MSG", inbox.as_str())
}

/// The `ByTime` partition listing an inbox's threads, sorted by
/// [`thread_time_sort`].
#[must_use]
pub fn threads_partition(inbox: &InboxId) -> String {
    format!("INBOX#{}#THR", inbox.as_str())
}

/// The `ByThread` partition listing one thread's messages, sorted by
/// message id.
#[must_use]
pub fn thread_messages_partition(
    inbox: &InboxId,
    thread_id: &(impl AsRef<str> + ?Sized),
) -> String {
    let thread_id = thread_id.as_ref();
    format!("THREAD#{}#{thread_id}", inbox.as_str())
}

/// A thread's `ByTime` sort key: last activity, then id, so a thread list
/// orders by most recent activity.
#[must_use]
pub fn thread_time_sort(timestamp: &str, thread_id: &(impl AsRef<str> + ?Sized)) -> String {
    let thread_id = thread_id.as_ref();
    format!("{timestamp}#{thread_id}")
}

/// The `ByStatus` partition listing the sends in `status`.
#[must_use]
pub fn send_status_partition(status: SendStatus) -> String {
    format!("SENDSTATUS#{}", status.as_str())
}

/// A decoded page-token key: where in an index to resume.
///
/// Both halves are needed. A `Query` against a secondary index takes an
/// `ExclusiveStartKey` holding the index's own key *and* the table's key for
/// the same item, because the index's key is not unique on its own. Carrying
/// the table key here keeps it exact rather than re-derived from the sort
/// value, whose shape differs per index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageKey {
    /// The index partition the token was issued for, re-validated against the
    /// request that presents it.
    pub partition: String,
    /// The index sort key of the last returned item.
    pub sort: String,
    pub table_pk: String,
    pub table_sk: String,
}

/// The opaque page token: base64url JSON of the last *returned* item's
/// key, plus the query shape it was issued for. Both are re-validated against
/// the request that presents it; a mismatch is the caller's 400. Without the
/// shape, a token reused with a different time window or sort order would hand
/// DynamoDB a start key outside the new key condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageTokenPayload {
    scope: String,
    /// Flattened, so the token's JSON keeps the shape it has always had
    /// while the key's fields are declared once, on [`PageKey`].
    #[serde(flatten)]
    key: PageKey,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid page token")]
pub struct PageTokenError;

/// Encodes a page key as an opaque token for the `next`/`before`/`after`
/// response field, bound to `scope`: the sort order and time window of the
/// query that produced it.
///
/// # Panics
///
/// Never panics in practice: `PageTokenPayload` is plain owned strings, which
/// `serde_json` always serializes successfully.
#[must_use]
pub fn encode_page_token(key: &PageKey, scope: &str) -> String {
    let payload = PageTokenPayload {
        scope: scope.to_owned(),
        key: key.clone(),
    };
    // Infallible: `PageTokenPayload` is plain owned strings.
    #[expect(
        clippy::unwrap_used,
        reason = "PageTokenPayload has no non-serializable fields"
    )]
    let json = serde_json::to_vec(&payload).unwrap();
    URL_SAFE_NO_PAD.encode(json)
}

/// Decodes a page token, checking it was issued for `expected_partition` and
/// `expected_scope`.
///
/// # Errors
///
/// Returns [`PageTokenError`] for malformed base64/JSON, or a token issued
/// for a different partition or query shape, which the caller sees as a 400.
pub fn decode_page_token(
    token: &str,
    expected_partition: &str,
    expected_scope: &str,
) -> Result<PageKey, PageTokenError> {
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| PageTokenError)?;
    let payload: PageTokenPayload = serde_json::from_slice(&bytes).map_err(|_| PageTokenError)?;
    if payload.key.partition != expected_partition || payload.scope != expected_scope {
        return Err(PageTokenError);
    }
    Ok(payload.key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn key(pk: &str, sk: &str) -> TableKey {
        TableKey {
            pk: pk.to_owned(),
            sk: sk.to_owned(),
        }
    }

    /// Every key is a storage format: these are the exact strings items
    /// already in the table were written under.
    #[test]
    fn key_builders_match_the_stored_shapes() {
        let inbox_id = InboxId::new("support@example.com");
        assert_eq!(inbox(&inbox_id), key("INBOX#support@example.com", "META"));
        assert_eq!(
            message(&inbox_id, "mid-1"),
            key("INBOX#support@example.com", "MSG#mid-1")
        );
        assert_eq!(
            thread(&inbox_id, "tid-1"),
            key("INBOX#support@example.com", "THR#tid-1")
        );
        assert_eq!(send_state("mid-1"), key("OUTBOX#mid-1", "STATE"));
        assert_eq!(
            rfc_alias(&inbox_id, "abc@x"),
            key("RFC#support@example.com#abc@x", "RFC")
        );
        assert_eq!(ses_ref("ses-1"), key("SESMSG#ses-1", "REF"));
        assert_eq!(send_key("deadbeef"), key("SENDKEY#deadbeef", "KEY"));
        assert_eq!(send_key_pk("deadbeef"), "SENDKEY#deadbeef");
    }

    #[test]
    fn index_keys_match_the_stored_shapes() {
        let inbox_id = InboxId::new("support@example.com");
        assert_eq!(INBOXES_PARTITION, "INBOXES");
        assert_eq!(
            messages_partition(&inbox_id),
            "INBOX#support@example.com#MSG"
        );
        assert_eq!(
            threads_partition(&inbox_id),
            "INBOX#support@example.com#THR"
        );
        assert_eq!(
            thread_messages_partition(&inbox_id, "tid-1"),
            "THREAD#support@example.com#tid-1"
        );
        assert_eq!(
            thread_time_sort("2026-01-01T00:00:00.000Z", "tid-1"),
            "2026-01-01T00:00:00.000Z#tid-1"
        );
        for (status, partition) in [
            (SendStatus::Queued, "SENDSTATUS#queued"),
            (SendStatus::Sending, "SENDSTATUS#sending"),
            (SendStatus::Sent, "SENDSTATUS#sent"),
            (SendStatus::Failed, "SENDSTATUS#failed"),
            (SendStatus::Unknown, "SENDSTATUS#unknown"),
        ] {
            assert_eq!(send_status_partition(status), partition);
        }
    }

    #[test]
    fn an_outbox_pk_parses_back_to_its_message_id() {
        assert_eq!(parse_outbox_pk(&send_state("mid-1").pk), Some("mid-1"));
        assert_eq!(parse_outbox_pk("INBOX#support@example.com"), None);
        assert_eq!(parse_outbox_pk("OUTBOX#"), Some(""));
    }

    /// Tokens are handed to callers, so their JSON is a wire format: the
    /// key's fields sit beside `scope`, not nested under it.
    #[test]
    fn a_page_token_keeps_its_flat_json_shape() {
        let key = PageKey {
            partition: "INBOX#support#MSG".to_owned(),
            sort: "mid-1".to_owned(),
            table_pk: "INBOX#support".to_owned(),
            table_sk: "MSG#mid-1".to_owned(),
        };
        let token = encode_page_token(&key, "asc=false;before=;after=");
        let json = URL_SAFE_NO_PAD.decode(token).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value["scope"], "asc=false;before=;after=");
        assert_eq!(value["partition"], "INBOX#support#MSG");
        assert_eq!(value["sort"], "mid-1");
        assert_eq!(value["table_pk"], "INBOX#support");
        assert_eq!(value["table_sk"], "MSG#mid-1");
    }

    #[test]
    fn a_page_token_is_refused_for_a_different_query_shape() {
        let key = PageKey {
            partition: "INBOX#support#MSG".to_owned(),
            sort: "mid-9".to_owned(),
            table_pk: "INBOX#support".to_owned(),
            table_sk: "MSG#mid-9".to_owned(),
        };
        let token = encode_page_token(&key, "scope-a");
        assert!(decode_page_token(&token, &key.partition, "scope-b").is_err());
    }

    #[test]
    fn page_token_round_trips() {
        let key = PageKey {
            partition: "INBOX#support#MSG".to_owned(),
            sort: "mid-9".to_owned(),
            table_pk: "INBOX#support".to_owned(),
            table_sk: "MSG#mid-9".to_owned(),
        };
        let token = encode_page_token(&key, "scope-a");
        let decoded = decode_page_token(&token, &key.partition, "scope-a").unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn page_token_rejects_a_partition_mismatch() {
        let key = PageKey {
            partition: "INBOX#support#MSG".to_owned(),
            sort: "mid-9".to_owned(),
            table_pk: "INBOX#support".to_owned(),
            table_sk: "MSG#mid-9".to_owned(),
        };
        let token = encode_page_token(&key, "scope-a");
        assert!(decode_page_token(&token, "INBOX#billing#MSG", "scope-a").is_err());
    }

    #[test]
    fn page_token_rejects_malformed_input() {
        assert!(decode_page_token("not-base64!!!", "p", "").is_err());
        assert!(decode_page_token(&URL_SAFE_NO_PAD.encode("not json"), "p", "").is_err());
        assert!(decode_page_token("", "p", "").is_err());
    }

    proptest! {
        #[test]
        fn page_token_round_trip_holds_for_any_strings(
            partition in "[A-Za-z0-9#_-]{1,80}",
            sort in "[A-Za-z0-9#_-]{1,80}",
            table_pk in "[A-Za-z0-9#_-]{1,80}",
            table_sk in "[A-Za-z0-9#_-]{1,80}",
        ) {
            let key = PageKey { partition: partition.clone(), sort, table_pk, table_sk };
            let token = encode_page_token(&key, "scope-a");
            prop_assert_eq!(decode_page_token(&token, &partition, "scope-a").unwrap(), key);
        }
    }
}
