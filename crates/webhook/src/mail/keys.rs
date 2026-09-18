//! Mail table key builders, and the opaque page token every list endpoint
//! returns.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// Builds the `pk` for an inbox-scoped item.
#[must_use]
pub fn inbox_pk(inbox_id: &str) -> String {
    format!("INBOX#{inbox_id}")
}

#[must_use]
pub fn inbox_sk() -> &'static str {
    "META"
}

/// The index partition holding every inbox, so listing them is one query
/// rather than a table scan.
#[must_use]
pub fn inboxes_partition() -> &'static str {
    "INBOXES"
}

/// The `ByTime` partition listing an inbox's messages, sorted by message id.
#[must_use]
pub fn messages_partition(inbox_id: &str) -> String {
    format!("INBOX#{inbox_id}#MSG")
}

/// The `ByTime` partition listing an inbox's threads, sorted by
/// [`thread_time_sort`].
#[must_use]
pub fn threads_partition(inbox_id: &str) -> String {
    format!("INBOX#{inbox_id}#THR")
}

/// A thread's `ByTime` sort key: last activity, then id, so a thread list
/// orders by most recent activity.
#[must_use]
pub fn thread_time_sort(timestamp: &str, thread_id: &str) -> String {
    format!("{timestamp}#{thread_id}")
}

#[must_use]
pub fn message_sk(message_id: &str) -> String {
    format!("MSG#{message_id}")
}

/// The send-state item's `pk`: a message's send state lives outside its
/// inbox partition so the sender never needs the inbox id to find it.
#[must_use]
pub fn outbox_pk(message_id: &str) -> String {
    format!("OUTBOX#{message_id}")
}

#[must_use]
pub fn outbox_sk() -> &'static str {
    "STATE"
}

#[must_use]
pub fn thread_sk(thread_id: &str) -> String {
    format!("THR#{thread_id}")
}

/// The RFC alias item's `pk`: `rfc_id` is the `Message-ID` value with the
/// surrounding `<`/`>` stripped.
#[must_use]
pub fn rfc_alias_pk(inbox_id: &str, rfc_id: &str) -> String {
    format!("RFC#{inbox_id}#{rfc_id}")
}

#[must_use]
pub fn rfc_alias_sk() -> &'static str {
    "RFC"
}

#[must_use]
pub fn ses_ref_pk(ses_message_id: &str) -> String {
    format!("SESMSG#{ses_message_id}")
}

#[must_use]
pub fn ses_ref_sk() -> &'static str {
    "REF"
}

/// The `Idempotency-Key` item's `pk`: `key_hash` is the lowercase hex
/// SHA-256 of the header value.
#[must_use]
pub fn send_key_pk(key_hash: &str) -> String {
    format!("SENDKEY#{key_hash}")
}

#[must_use]
pub fn send_key_sk() -> &'static str {
    "KEY"
}

/// A decoded page-token key: where in an index to resume.
///
/// Both halves are needed. A `Query` against a secondary index takes an
/// `ExclusiveStartKey` holding the index's own key *and* the table's key for
/// the same item, because the index's key is not unique on its own. Carrying
/// the table key here keeps it exact rather than re-derived from the sort
/// value, whose shape differs per index.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    partition: String,
    sort: String,
    table_pk: String,
    table_sk: String,
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
        partition: key.partition.clone(),
        sort: key.sort.clone(),
        table_pk: key.table_pk.clone(),
        table_sk: key.table_sk.clone(),
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
    if payload.partition != expected_partition || payload.scope != expected_scope {
        return Err(PageTokenError);
    }
    Ok(PageKey {
        partition: payload.partition,
        sort: payload.sort,
        table_pk: payload.table_pk,
        table_sk: payload.table_sk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn key_builders_match_the_documented_shapes() {
        assert_eq!(inbox_pk("support"), "INBOX#support");
        assert_eq!(message_sk("mid-1"), "MSG#mid-1");
        assert_eq!(outbox_pk("mid-1"), "OUTBOX#mid-1");
        assert_eq!(thread_sk("tid-1"), "THR#tid-1");
        assert_eq!(rfc_alias_pk("support", "abc@x"), "RFC#support#abc@x");
        assert_eq!(ses_ref_pk("ses-1"), "SESMSG#ses-1");
        assert_eq!(send_key_pk("deadbeef"), "SENDKEY#deadbeef");
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
