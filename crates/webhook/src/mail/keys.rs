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

/// The message pointer's `pk` (per-inbox, per-label list view).
#[must_use]
pub fn label_pk(inbox_id: &str, label: &str) -> String {
    format!("INBOX#{inbox_id}#LABEL#{label}")
}

#[must_use]
pub fn message_pointer_sk(message_id: &str) -> String {
    format!("MSGAT#{message_id}")
}

#[must_use]
pub fn thread_pointer_sk(timestamp: &str, thread_id: &str) -> String {
    format!("THRAT#{timestamp}#{thread_id}")
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

/// The SES-call marker's `pk`.
#[must_use]
pub fn ses_call_pk(message_id: &str) -> String {
    format!("SESCALL#{message_id}")
}

#[must_use]
pub fn ses_call_sk() -> &'static str {
    "CALL"
}

/// A decoded `before`/`after`/page-token key: the partition it was issued for
/// plus the sort key of the boundary item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageKey {
    pub partition: String,
    pub sort: String,
}

/// The opaque page token: base64url JSON of the last *returned* item's
/// key. Its partition is re-validated against the request that presents it;
/// a mismatch is the caller's 400.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageTokenPayload {
    partition: String,
    sort: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid page token")]
pub struct PageTokenError;

/// Encodes a page key as an opaque token for the `next`/`before`/`after`
/// response field.
///
/// # Panics
///
/// Never panics in practice: `PageTokenPayload` is plain owned strings, which
/// `serde_json` always serializes successfully.
#[must_use]
pub fn encode_page_token(key: &PageKey) -> String {
    let payload = PageTokenPayload {
        partition: key.partition.clone(),
        sort: key.sort.clone(),
    };
    // Infallible: `PageTokenPayload` is plain owned strings.
    #[expect(
        clippy::unwrap_used,
        reason = "PageTokenPayload has no non-serializable fields"
    )]
    let json = serde_json::to_vec(&payload).unwrap();
    URL_SAFE_NO_PAD.encode(json)
}

/// Decodes a page token, checking it was issued for `expected_partition`.
///
/// # Errors
///
/// Returns [`PageTokenError`] for malformed base64/JSON, or a token issued
/// for a different partition, which the caller sees as a 400.
pub fn decode_page_token(token: &str, expected_partition: &str) -> Result<PageKey, PageTokenError> {
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| PageTokenError)?;
    let payload: PageTokenPayload = serde_json::from_slice(&bytes).map_err(|_| PageTokenError)?;
    if payload.partition != expected_partition {
        return Err(PageTokenError);
    }
    Ok(PageKey {
        partition: payload.partition,
        sort: payload.sort,
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
        assert_eq!(label_pk("support", "sent"), "INBOX#support#LABEL#sent");
        assert_eq!(message_pointer_sk("mid-1"), "MSGAT#mid-1");
        assert_eq!(
            thread_pointer_sk("00000001-0002", "tid-1"),
            "THRAT#00000001-0002#tid-1"
        );
        assert_eq!(rfc_alias_pk("support", "abc@x"), "RFC#support#abc@x");
        assert_eq!(ses_ref_pk("ses-1"), "SESMSG#ses-1");
        assert_eq!(send_key_pk("deadbeef"), "SENDKEY#deadbeef");
        assert_eq!(ses_call_pk("mid-1"), "SESCALL#mid-1");
    }

    #[test]
    fn page_token_round_trips() {
        let key = PageKey {
            partition: "INBOX#support#LABEL#unread".to_owned(),
            sort: "MSGAT#mid-9".to_owned(),
        };
        let token = encode_page_token(&key);
        let decoded = decode_page_token(&token, &key.partition).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn page_token_rejects_a_partition_mismatch() {
        let key = PageKey {
            partition: "INBOX#support#LABEL#unread".to_owned(),
            sort: "MSGAT#mid-9".to_owned(),
        };
        let token = encode_page_token(&key);
        assert!(decode_page_token(&token, "INBOX#billing#LABEL#unread").is_err());
    }

    #[test]
    fn page_token_rejects_malformed_input() {
        assert!(decode_page_token("not-base64!!!", "p").is_err());
        assert!(decode_page_token(&URL_SAFE_NO_PAD.encode("not json"), "p").is_err());
        assert!(decode_page_token("", "p").is_err());
    }

    proptest! {
        #[test]
        fn page_token_round_trip_holds_for_any_strings(
            partition in "[A-Za-z0-9#_-]{1,80}",
            sort in "[A-Za-z0-9#_-]{1,80}",
        ) {
            let key = PageKey { partition: partition.clone(), sort };
            let token = encode_page_token(&key);
            prop_assert_eq!(decode_page_token(&token, &partition).unwrap(), key);
        }
    }
}
