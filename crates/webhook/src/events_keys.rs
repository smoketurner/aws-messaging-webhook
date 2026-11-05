//! Events table key builders: one partition per aggregate (`MSG#<id>`)
//! holding its event items (`EVT#…`) and its rolled-up aggregate (`AGG`).

/// The aggregate item's `sk`.
pub(crate) const AGGREGATE_SK: &str = "AGG";

/// The prefix of an event item's `sk`.
pub(crate) const EVENT_SK_PREFIX: &str = "EVT#";

const PARTITION_PREFIX: &str = "MSG#";

/// The partition holding every item for `aggregate_id`.
#[must_use]
pub(crate) fn partition(aggregate_id: &str) -> String {
    format!("{PARTITION_PREFIX}{aggregate_id}")
}

/// The aggregate id a partition `pk` names; `None` for any other `pk`.
#[must_use]
pub(crate) fn parse_partition(pk: &str) -> Option<&str> {
    pk.strip_prefix(PARTITION_PREFIX)
}

/// An event item's `sk`: ordered by event time, made unique by the SNS
/// message id.
#[must_use]
pub(crate) fn event_sk(event_timestamp: &str, sns_message_id: &str) -> String {
    format!("{EVENT_SK_PREFIX}{event_timestamp}#{sns_message_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Storage formats: items already in the table were written under these.
    #[test]
    fn keys_match_the_stored_shapes() {
        assert_eq!(partition("0100abc"), "MSG#0100abc");
        assert_eq!(
            event_sk("2026-01-01T00:00:00.000Z", "sns-1"),
            "EVT#2026-01-01T00:00:00.000Z#sns-1"
        );
        assert_eq!(AGGREGATE_SK, "AGG");
    }

    #[test]
    fn a_partition_parses_back_to_its_aggregate_id() {
        assert_eq!(parse_partition(&partition("0100abc")), Some("0100abc"));
        assert_eq!(parse_partition("OUTBOX#mid-1"), None);
    }
}
