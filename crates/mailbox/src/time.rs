//! Instants and their stored form. An instant is a [`jiff::Timestamp`]; mail
//! table attributes (`timestamp`, `created_at`, `updated_at`, …) hold it as a
//! fixed-width UTC string, `YYYY-MM-DDTHH:MM:SS.sssZ`, always exactly 24
//! bytes, so DynamoDB string comparison (and page-key/prefix bounds) sorts by
//! wall-clock order.
//!
//! Nothing here reads the clock. This crate is pure: every instant it handles
//! is handed in, taken by the binary from the Lambda event being served.

pub use jiff::Timestamp;

/// The DynamoDB TTL (epoch seconds) for an item written at `at`,
/// `retention_days` later: for a mail item, the same span the mail bucket's
/// lifecycle rules keep its objects, so both age out together.
///
/// `retention_days` is `u32`, so `days × 86_400` cannot overflow `u64`; a
/// wider type would let a huge value wrap to a near-zero TTL.
#[must_use]
pub fn expires_at(at: Timestamp, retention_days: u32) -> u64 {
    unix_seconds(at).saturating_add(u64::from(retention_days).saturating_mul(86_400))
}

/// `at` as whole seconds since the Unix epoch, the unit of a DynamoDB TTL.
/// An instant before the epoch clamps to zero.
#[must_use]
pub fn unix_seconds(at: Timestamp) -> u64 {
    u64::try_from(at.as_second()).unwrap_or(0)
}

/// `at` as milliseconds since the Unix epoch, the resolution ids and sort
/// keys carry. An instant before the epoch, which nothing here produces,
/// clamps to zero.
#[must_use]
pub fn unix_ms(at: Timestamp) -> u64 {
    u64::try_from(at.as_millisecond()).unwrap_or(0)
}

/// Formats `at` as a fixed-width, millisecond-precision UTC timestamp:
/// `YYYY-MM-DDTHH:MM:SS.sssZ` (24 bytes), truncating anything finer. Holds
/// the fixed-width guarantee for every year in 0000–9999, and a
/// [`Timestamp`] cannot hold a later one.
#[must_use]
pub fn format(at: Timestamp) -> String {
    format!("{at:.3}")
}

/// Parses a string in exactly [`format`](fn@format)'s shape. `None` for any other shape
/// (a missing or extra fraction digit, an offset other than `Z`, a
/// non-existent calendar date such as Feb 30, a leap second) and for an
/// instant before the Unix epoch. Strict because these strings are compared
/// bytewise as sort keys: accepting a second spelling of an instant would
/// accept a key that sorts somewhere else.
#[must_use]
pub fn parse(s: &str) -> Option<Timestamp> {
    let at = parse_rfc3339(s)?;
    (at.as_millisecond() >= 0 && format(at) == s).then_some(at)
}

/// Parses [`format`](fn@format)'s shape without its fraction, `YYYY-MM-DDTHH:MM:SSZ`,
/// as strictly as [`parse`] reads the full one.
#[must_use]
pub fn parse_whole_second(s: &str) -> Option<Timestamp> {
    let at = parse_rfc3339(s)?;
    let canonical = format(at);
    // The canonical form with its `.000` removed must be the input itself.
    let whole = canonical.strip_suffix(".000Z")?;
    (at.as_millisecond() >= 0 && s.strip_suffix('Z') == Some(whole)).then_some(at)
}

/// Parses any RFC 3339 timestamp, as AWS writes them in the events it
/// delivers (an SNS envelope's `Timestamp`, a scheduled event's `time`),
/// where the number of fraction digits varies by service. Anything finer
/// than a millisecond is dropped: see [`to_millis`].
#[must_use]
pub fn parse_rfc3339(s: &str) -> Option<Timestamp> {
    s.parse().ok().map(to_millis)
}

/// `at` with anything finer than a millisecond dropped. Every instant enters
/// the program through this, so one that has been stored and read back
/// equals the one that was written.
#[must_use]
pub fn to_millis(at: Timestamp) -> Timestamp {
    Timestamp::from_millisecond(at.as_millisecond()).unwrap_or(at)
}

/// Serde for an instant stored or sent as a string: `#[serde(with =
/// "time::fixed")]`. This is the one place an instant becomes text or text
/// an instant; between a read and a write it stays a [`Timestamp`].
///
/// Writes [`format`](fn@format)'s fixed-width shape, which sort keys and clients rely
/// on. Reads any RFC 3339 timestamp, so an item written by something less
/// strict still loads.
pub mod fixed {
    use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

    use super::Timestamp;

    /// # Errors
    ///
    /// Only what the serializer itself returns.
    pub fn serialize<S: Serializer>(at: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::format(*at))
    }

    /// # Errors
    ///
    /// When the value is not a string holding an RFC 3339 timestamp.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
        let text = String::deserialize(deserializer)?;
        super::parse_rfc3339(&text)
            .ok_or_else(|| D::Error::custom(format_args!("not an RFC 3339 timestamp: {text}")))
    }

    /// The same for an optional instant: `#[serde(default, skip_serializing_if
    /// = "Option::is_none", with = "time::fixed::option")]`.
    pub mod option {
        use serde::{Deserialize as _, Deserializer, Serializer};

        use super::super::Timestamp;

        /// # Errors
        ///
        /// Only what the serializer itself returns.
        pub fn serialize<S: Serializer>(
            at: &Option<Timestamp>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match at {
                Some(at) => serializer.serialize_some(&super::super::format(*at)),
                None => serializer.serialize_none(),
            }
        }

        /// A string that is not a timestamp reads as absent rather than
        /// failing the item: an optional instant is never what an item is
        /// keyed by, and one unreadable mark must not make a send
        /// unlistable to the sweep that would recover it.
        ///
        /// # Errors
        ///
        /// When the value is neither null nor a string.
        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Timestamp>, D::Error> {
            let text = Option::<String>::deserialize(deserializer)?;
            Ok(text.as_deref().and_then(super::super::parse_rfc3339))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The latest instant a [`Timestamp`] holds, 9999-12-30T22:00:00.999Z, in
    /// epoch milliseconds: the upper bound of what [`format`](fn@format) can be given.
    const MAX_MS: u64 = 253_402_207_200_999;

    #[test]
    fn max_ms_is_the_latest_timestamp() {
        assert_eq!(unix_ms(Timestamp::MAX), MAX_MS);
    }

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_millisecond(i64::try_from(ms).unwrap()).unwrap()
    }

    fn format_ms(ms: u64) -> String {
        format(at(ms))
    }

    fn parse_ms(s: &str) -> Option<u64> {
        parse(s).map(unix_ms)
    }

    fn days_in_month(year: i64, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
            _ => 28,
        }
    }

    #[test]
    fn formats_a_known_instant() {
        // 2015-09-11T20:32:33.936Z, from the SES documentation examples.
        assert_eq!(format_ms(1_442_003_553_936), "2015-09-11T20:32:33.936Z");
    }

    #[test]
    fn formats_the_epoch() {
        assert_eq!(format_ms(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn formats_are_always_24_bytes() {
        for ms in [0, 1, 999, 1000, 1_442_003_553_936, MAX_MS] {
            assert_eq!(format_ms(ms).len(), 24, "ms={ms}");
        }
    }

    #[test]
    fn parses_a_known_instant() {
        assert_eq!(
            parse_ms("2015-09-11T20:32:33.936Z"),
            Some(1_442_003_553_936)
        );
    }

    #[test]
    fn parse_rejects_malformed_input() {
        assert_eq!(parse_ms(""), None);
        assert_eq!(parse_ms("2015-09-11T20:32:33.936"), None); // missing Z
        assert_eq!(parse_ms("2015-09-11 20:32:33.936Z"), None); // missing T
        assert_eq!(parse_ms("2015-13-11T20:32:33.936Z"), None); // month 13
        assert_eq!(parse_ms("2015-09-11T24:32:33.936Z"), None); // hour 24
        assert_eq!(parse_ms("not a timestamp, just text"), None);
        assert_eq!(parse_ms("2015-09-11T20:32:33Z"), None); // no fraction
        assert_eq!(parse_ms("2015-09-11T20:32:33.9360Z"), None); // four digits
        assert_eq!(parse_ms("2015-09-11T20:32:33.936+00:00"), None); // offset
        assert_eq!(parse_ms("2015-09-11t20:32:33.936z"), None); // lowercase
        assert_eq!(parse_ms("2015-06-30T23:59:60.000Z"), None); // leap second
        assert_eq!(parse_ms("1969-12-31T23:59:59.999Z"), None); // pre-epoch
    }

    #[test]
    fn parse_rejects_days_past_the_end_of_the_month() {
        // Non-existent calendar dates the old `1..=31` bound accepted and
        // `days_from_civil` silently rolled into the next month, returning a
        // real but different epoch millisecond value.
        assert_eq!(parse_ms("2015-02-30T00:00:00.000Z"), None); // used to roll to Mar 2
        assert_eq!(parse_ms("2015-02-29T00:00:00.000Z"), None); // 2015 non-leap -> Mar 1
        assert_eq!(parse_ms("2015-04-31T00:00:00.000Z"), None); // used to roll to May 1
        assert_eq!(parse_ms("2015-06-31T00:00:00.000Z"), None);
        assert_eq!(parse_ms("2015-09-31T00:00:00.000Z"), None);
        assert_eq!(parse_ms("2015-11-31T00:00:00.000Z"), None);
        assert_eq!(parse_ms("2100-02-29T00:00:00.000Z"), None); // century non-leap
        // Upper bound of a 31-day month and day zero are still rejected.
        assert_eq!(parse_ms("2015-01-32T00:00:00.000Z"), None);
        assert_eq!(parse_ms("2015-01-00T00:00:00.000Z"), None);
    }

    #[test]
    fn parse_accepts_the_last_day_of_every_short_month_and_leap_days() {
        fn round_trips(s: &str) {
            assert_eq!(parse(s).map(format), Some(s.to_owned()), "{s}");
        }
        // Last valid day of each short month in a non-leap year.
        round_trips("2015-02-28T00:00:00.000Z");
        round_trips("2015-04-30T23:59:59.999Z");
        round_trips("2015-06-30T00:00:00.000Z");
        round_trips("2015-09-30T00:00:00.000Z");
        round_trips("2015-11-30T00:00:00.000Z");
        // Feb 29 round-trips in a leap year, but rejects in a non-leap year.
        round_trips("2024-02-29T00:00:00.000Z"); // divisible by 4, not a century
        round_trips("2000-02-29T00:00:00.000Z"); // divisible by 400
        assert_eq!(parse_ms("2015-02-29T00:00:00.000Z"), None);
    }

    #[test]
    fn parse_round_trips_every_valid_calendar_date_and_rejects_overflow() {
        for &year in &[1970i64, 1999, 2000, 2001, 2004, 2015, 2024, 2100, 2400] {
            for month in 1u32..=12 {
                let dim = days_in_month(year, month);
                for day in 1..=dim {
                    for &(h, m, s, ms) in &[(0u64, 0u64, 0u64, 0u64), (23, 59, 59, 999)] {
                        let s_str =
                            format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z");
                        assert_eq!(parse(&s_str).map(format), Some(s_str.clone()));
                    }
                }
                // One day past the end of the month is now rejected rather
                // than rolled into the next month.
                if dim < 31 {
                    let bad = format!("{year:04}-{month:02}-{}T00:00:00.000Z", dim + 1);
                    assert_eq!(parse_ms(&bad), None, "expected None for {bad}");
                }
            }
        }
    }

    #[test]
    fn round_trips_millisecond_boundaries() {
        for ms in [0, 999, 1000, 86_399_999, 86_400_000, MAX_MS] {
            assert_eq!(parse_ms(&format_ms(ms)), Some(ms), "ms={ms}");
        }
    }

    #[test]
    fn parse_whole_second_takes_only_the_canonical_shape() {
        assert_eq!(
            parse_whole_second("2015-09-11T20:32:33Z"),
            Some(at(1_442_003_553_000))
        );
        for rejected in [
            "2015-09-11T20:32:33.000Z", // that is `parse`'s shape
            "2015-09-11t20:32:33Z",     // lowercase separator
            "2015-09-11T20:32:60Z",     // leap second
            "2015-09-11T203233Z",       // basic time
            "2015-09-11T20:32:33+00:00",
            "1969-12-31T23:59:59Z",
            "2015-09-11 20:32:33Z",
        ] {
            assert_eq!(parse_whole_second(rejected), None, "{rejected}");
        }
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Marks {
        #[serde(with = "fixed")]
        at: Timestamp,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[serde(with = "fixed::option")]
        mark: Option<Timestamp>,
    }

    #[test]
    fn fixed_writes_the_fixed_width_form_and_reads_it_back() {
        let marks = Marks {
            at: at(1_442_003_553_000),
            mark: Some(at(1_442_003_553_936)),
        };
        let json = serde_json::to_value(&marks).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "at": "2015-09-11T20:32:33.000Z",
                "mark": "2015-09-11T20:32:33.936Z",
            })
        );
        assert_eq!(serde_json::from_value::<Marks>(json).unwrap(), marks);
    }

    #[test]
    fn an_unreadable_optional_instant_reads_as_absent_and_a_required_one_fails() {
        let read = |json| serde_json::from_value::<Marks>(json);
        let at = "2015-09-11T20:32:33.000Z";
        assert_eq!(
            read(serde_json::json!({ "at": at, "mark": "garbage" }))
                .unwrap()
                .mark,
            None
        );
        assert_eq!(read(serde_json::json!({ "at": at })).unwrap().mark, None);
        assert!(read(serde_json::json!({ "at": "garbage" })).is_err());
    }

    #[test]
    fn format_truncates_below_a_millisecond() {
        let at = "2015-09-11T20:32:33.936999999Z".parse().unwrap();
        assert_eq!(format(at), "2015-09-11T20:32:33.936Z");
    }

    #[test]
    fn parse_rfc3339_accepts_the_shapes_aws_events_carry() {
        let expected = Some(at(1_442_003_553_000));
        assert_eq!(parse_rfc3339("2015-09-11T20:32:33Z"), expected);
        assert_eq!(parse_rfc3339("2015-09-11T20:32:33.000Z"), expected);
        assert_eq!(parse_rfc3339("2015-09-11T22:32:33+02:00"), expected);
        assert_eq!(parse_rfc3339("yesterday"), None);
    }

    #[test]
    fn expires_at_offsets_by_retention_days_in_seconds() {
        assert_eq!(expires_at(Timestamp::UNIX_EPOCH, 0), 0);
        assert_eq!(expires_at(Timestamp::UNIX_EPOCH, 30), 30 * 86_400);
        assert_eq!(expires_at(at(1_999), 1), 1 + 86_400);
        // The widest retention fits `u64` with room to spare.
        assert_eq!(
            expires_at(Timestamp::UNIX_EPOCH, u32::MAX),
            u64::from(u32::MAX) * 86_400
        );
    }

    proptest! {
        #[test]
        fn round_trip_holds_for_any_representable_instant(ms in 0u64..=MAX_MS) {
            prop_assert_eq!(parse_ms(&format_ms(ms)), Some(ms));
        }

        #[test]
        fn format_is_always_24_bytes_and_lexicographic_order_matches_chronological(
            a in 0u64..=MAX_MS,
            b in 0u64..=MAX_MS,
        ) {
            let (fa, fb) = (format_ms(a), format_ms(b));
            prop_assert_eq!(fa.len(), 24);
            prop_assert_eq!(fb.len(), 24);
            prop_assert_eq!(a.cmp(&b), fa.cmp(&fb));
        }
    }
}
