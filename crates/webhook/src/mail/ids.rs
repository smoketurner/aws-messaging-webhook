//! Message, attachment and event ids (D38, D23).
//!
//! Every id in the mail feature is a `UUIDv7` (`Builder::from_unix_timestamp_millis`,
//! hyphenated lowercase): sortable by creation time, but with a
//! *deterministic* random component for ids that must be reproducible from
//! their source content (inbound message ids, attachment ids, event ids) so a
//! redelivery or a resumed operation lands on the same id rather than
//! creating a duplicate. Outbound message ids have no source content to
//! derive from, so they use the standard random `now_v7()`.

use sha2::{Digest as _, Sha256};
use uuid::{Builder, Uuid};

/// The first 10 bytes of `SHA-256(input)`, the random-bytes slot
/// `Builder::from_unix_timestamp_millis` takes in place of the CSPRNG output
/// a plain `Uuid::now_v7()` would use — this is what makes the id
/// deterministic in its source content.
fn deterministic_seed(input: &str) -> [u8; 10] {
    let digest = Sha256::digest(input.as_bytes());
    let mut seed = [0u8; 10];
    seed.copy_from_slice(&digest[..10]);
    seed
}

/// The deterministic inbound message id: reproducible from the SES message id
/// and the receipt timestamp alone, so redelivery of the same SES receipt
/// (and, therefore, of the same S3 object) always resolves to the same
/// message id, and attachments are stored once under it.
#[must_use]
pub fn inbound_message_id(ses_id: &str, received_ms: u64) -> Uuid {
    let seed = deterministic_seed(&format!("inbound\n{ses_id}"));
    Builder::from_unix_timestamp_millis(received_ms, &seed).into_uuid()
}

/// A fresh outbound message id: randomly seeded (nothing to derive
/// determinism from at enqueue time), computed once per API request and
/// reused across retries of that request (D17 m6).
#[must_use]
pub fn outbound_message_id() -> Uuid {
    Uuid::now_v7()
}

/// A deterministic attachment id for the `ordinal`-th kept part of
/// `message_id` (AT4, N17): `"att_"` + 32 lowercase hex (a simple-form
/// `UUIDv7`), timestamped at the message id's own embedded creation time so
/// attachment ids sort next to their message. `ordinal` is the 0-based
/// position in the kept attachment list (AT11 selection order for inbound,
/// the request's `attachments` array index for outbound) — not a raw MIME
/// part index. Determinism (same message, same ordinal, same id) is what
/// lets ingest's resume check (D48 m2) and the outbound object puts
/// (`put_object_if_absent`) treat a redelivery or a retried request as a
/// no-op rather than a duplicate write.
#[must_use]
pub fn attachment_id(message_id: &Uuid, ordinal: usize) -> String {
    // `message_id` is always a `UUIDv7` minted by this crate
    // (`inbound_message_id`/`outbound_message_id`), so `get_timestamp` is
    // always `Some`; falling back to the Unix epoch keeps this function
    // total rather than panicking on a hypothetical non-v7 input.
    let ts_ms = message_id.get_timestamp().map_or(0, |ts| {
        let (secs, nanos) = ts.to_unix();
        secs.saturating_mul(1000) + u64::from(nanos) / 1_000_000
    });
    let seed = deterministic_seed(&format!("att\n{message_id}\n{ordinal}"));
    let uuid = Builder::from_unix_timestamp_millis(ts_ms, &seed).into_uuid();
    format!("att_{}", uuid.simple())
}

/// The sort-key time prefix (D38): `ms` split into an 8-hex-digit high half
/// and a 4-hex-digit low half, so `ByTime`/`ByThread`-style range bounds
/// (`before`/`after`) can be built as plain string comparisons against a
/// `UUIDv7` sort key, which begins with the same encoding of its timestamp.
#[must_use]
pub fn time_prefix(ms: u64) -> String {
    format!("{:08x}-{:04x}", ms >> 16, ms & 0xffff)
}

/// This inbox's RFC `Message-ID` for a message we sent (D2): the message id
/// wrapped in angle brackets on our mail domain.
#[must_use]
pub fn our_rfc_message_id(message_id: &str, domain: &str) -> String {
    format!("<{message_id}@{domain}>")
}

/// The two RFC `Message-ID` forms SES may address a sent message by (D2),
/// registered as aliases at `mark_sent` so a reply referencing either one
/// resolves back to it.
#[must_use]
pub fn ses_rfc_ids(ses_message_id: &str, region: &str) -> [String; 2] {
    [
        format!("<{ses_message_id}@email.amazonses.com>"),
        format!("<{ses_message_id}@{region}.amazonses.com>"),
    ]
}

/// The EventBridge `event_id` (D23): `evt_` followed by a deterministic
/// simple-form `UUIDv7` seeded from `(inbox, message_id, event_type)`, so
/// redelivery of the same underlying event produces the same `event_id`.
/// `ts_ms` is the message timestamp for a received event, `sent_at` for
/// sent, and the delivery timestamp for a delivery event.
#[must_use]
pub fn event_id(inbox: &str, message_id: &str, event_type: &str, ts_ms: u64) -> String {
    let seed = deterministic_seed(&format!("{inbox}\n{message_id}\n{event_type}"));
    let uuid = Builder::from_unix_timestamp_millis(ts_ms, &seed).into_uuid();
    format!("evt_{uuid}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn is_v7(uuid: Uuid) -> bool {
        uuid.get_version_num() == 7
    }

    #[test]
    fn inbound_message_id_is_deterministic() {
        let a = inbound_message_id("ses-1", 1_700_000_000_000);
        let b = inbound_message_id("ses-1", 1_700_000_000_000);
        assert_eq!(a, b);
        assert!(is_v7(a));
    }

    #[test]
    fn inbound_message_id_differs_on_ses_id_or_timestamp() {
        let base = inbound_message_id("ses-1", 1_700_000_000_000);
        assert_ne!(base, inbound_message_id("ses-2", 1_700_000_000_000));
        assert_ne!(base, inbound_message_id("ses-1", 1_700_000_000_001));
    }

    #[test]
    fn outbound_message_id_is_random_and_v7() {
        let a = outbound_message_id();
        let b = outbound_message_id();
        assert_ne!(a, b);
        assert!(is_v7(a));
        assert!(is_v7(b));
    }

    #[test]
    fn attachment_id_is_deterministic_and_unique_per_ordinal() {
        let mid = inbound_message_id("ses-1", 1_700_000_000_000);
        let other_mid = inbound_message_id("ses-2", 1_700_000_000_000);
        let a0 = attachment_id(&mid, 0);
        let a0_again = attachment_id(&mid, 0);
        let a1 = attachment_id(&mid, 1);
        let other_message = attachment_id(&other_mid, 0);
        assert_eq!(a0, a0_again);
        assert_ne!(a0, a1);
        assert_ne!(a0, other_message);
    }

    #[test]
    fn attachment_id_has_the_documented_shape() {
        let mid = inbound_message_id("ses-1", 1_700_000_000_000);
        let id = attachment_id(&mid, 0);
        assert!(id.starts_with("att_"));
        assert_eq!(id.len(), 36);
        let hex = &id[4..];
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        let uuid = Uuid::parse_str(hex).unwrap();
        assert!(is_v7(uuid));
    }

    #[test]
    fn attachment_id_embeds_the_message_id_timestamp() {
        let mid = inbound_message_id("ses-1", 1_700_000_000_000);
        let id = attachment_id(&mid, 0);
        let uuid = Uuid::parse_str(&id[4..]).unwrap();
        let (secs, nanos) = uuid.get_timestamp().unwrap().to_unix();
        let ts_ms = secs * 1000 + u64::from(nanos) / 1_000_000;
        assert_eq!(ts_ms, 1_700_000_000_000);
    }

    /// A golden vector computed from the N17 formula, pinned so a change to
    /// the seed shape or the UUID encoding is caught rather than silently
    /// re-deriving a new "correct" value.
    #[test]
    fn attachment_id_golden_vector() {
        let mid = inbound_message_id("ses-1", 1_700_000_000_000);
        assert_eq!(mid.to_string(), "018bcfe5-6800-7bd1-8167-61246ff575b4");
        let id = attachment_id(&mid, 0);
        assert_eq!(id, "att_018bcfe5680070649b53ffdbc1b145b6");
    }

    #[test]
    fn time_prefix_has_the_documented_shape() {
        // ms = 0x0001_0002 -> high = 0x0001, low = 0x0002
        assert_eq!(time_prefix(0x0001_0002), "00000001-0002");
        assert_eq!(time_prefix(0), "00000000-0000");
    }

    #[test]
    fn our_rfc_message_id_wraps_in_angle_brackets() {
        assert_eq!(
            our_rfc_message_id("mid-1", "example.com"),
            "<mid-1@example.com>"
        );
    }

    #[test]
    fn ses_rfc_ids_covers_both_forms() {
        let ids = ses_rfc_ids("0100abc", "us-east-1");
        assert_eq!(
            ids,
            [
                "<0100abc@email.amazonses.com>".to_owned(),
                "<0100abc@us-east-1.amazonses.com>".to_owned(),
            ]
        );
    }

    #[test]
    fn event_id_is_deterministic_and_prefixed() {
        let a = event_id("inbox-1", "mid-1", "received", 1_700_000_000_000);
        let b = event_id("inbox-1", "mid-1", "received", 1_700_000_000_000);
        assert_eq!(a, b);
        assert!(a.starts_with("evt_"));
        let uuid = Uuid::parse_str(a.trim_start_matches("evt_")).unwrap();
        assert!(is_v7(uuid));
    }

    #[test]
    fn event_id_differs_by_event_type() {
        let received = event_id("inbox-1", "mid-1", "received", 1_700_000_000_000);
        let sent = event_id("inbox-1", "mid-1", "sent", 1_700_000_000_000);
        assert_ne!(received, sent);
    }

    proptest! {
        #[test]
        fn time_prefix_is_monotonic_within_the_same_high_half(
            base in 0u64..(1u64 << 48),
            delta in 1u64..0xffff,
        ) {
            let low = base & !0xffff; // clear the low 16 bits so base+delta can't overflow into the high half
            let a = time_prefix(low);
            let b = time_prefix(low + delta);
            prop_assert!(a < b);
        }

        #[test]
        fn attachment_ids_never_collide_across_a_small_ordinal_range(
            ses_id in "[a-z0-9-]{1,20}",
            ts in 0u64..(1u64 << 48),
        ) {
            let message_id = inbound_message_id(&ses_id, ts);
            let ids: Vec<String> = (0..8).map(|i| attachment_id(&message_id, i)).collect();
            let unique: std::collections::HashSet<_> = ids.iter().collect();
            prop_assert_eq!(unique.len(), ids.len());
        }
    }
}
