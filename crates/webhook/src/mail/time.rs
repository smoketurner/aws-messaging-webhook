//! Fixed-width UTC timestamps for mail table attributes (`timestamp`,
//! `created_at`, `updated_at`, …): `YYYY-MM-DDTHH:MM:SS.sssZ`, always exactly
//! 24 bytes, so DynamoDB string comparison (and page-key/prefix bounds) sorts
//! by wall-clock order.
//!
//! `aws_smithy_types::date_time`'s RFC 3339 formatter is not fixed-width — it
//! omits the fractional-second component entirely at whole seconds and
//! trims trailing zero digits otherwise — so it is not reused here. Calendar
//! conversion uses Howard Hinnant's `days_from_civil` / `civil_from_days`
//! algorithm (a small, well-known, dependency-free proleptic-Gregorian
//! calendar), avoiding a `chrono` dependency the plan does not call for.

use std::time::{SystemTime, UNIX_EPOCH};

const MS_PER_DAY: u64 = 86_400_000;
const MS_PER_HOUR: u64 = 3_600_000;
const MS_PER_MINUTE: u64 = 60_000;

/// The current time as milliseconds since the Unix epoch. Saturates to
/// `u64::MAX` in the astronomically unreachable case of a value that
/// overflows `u64` milliseconds (the year 584,942,417).
#[must_use]
pub fn now_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Formats `epoch_ms` as a fixed-width, millisecond-precision UTC timestamp:
/// `YYYY-MM-DDTHH:MM:SS.sssZ` (24 bytes). Holds the fixed-width guarantee for
/// any year in 0000–9999 (any epoch millisecond value this application will
/// ever produce).
#[must_use]
pub fn format(epoch_ms: u64) -> String {
    let days = epoch_ms / MS_PER_DAY;
    let ms_of_day = epoch_ms % MS_PER_DAY;
    #[expect(
        clippy::cast_possible_wrap,
        reason = "days-since-epoch fits i64 for any u64 millisecond value this app produces"
    )]
    let (year, month, day) = civil_from_days(days as i64);
    let hour = ms_of_day / MS_PER_HOUR;
    let minute = (ms_of_day / MS_PER_MINUTE) % 60;
    let second = (ms_of_day / 1000) % 60;
    let millis = ms_of_day % 1000;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Parses a string in exactly [`format`]'s shape back into epoch
/// milliseconds. `None` for any other shape (wrong length, wrong separators,
/// out-of-range field, or a date this pre-epoch check rejects).
#[must_use]
pub fn parse(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 24
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'.'
        || b[23] != b'Z'
    {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u64 = s.get(11..13)?.parse().ok()?;
    let minute: u64 = s.get(14..16)?.parse().ok()?;
    let second: u64 = s.get(17..19)?.parse().ok()?;
    let millis: u64 = s.get(20..23)?.parse().ok()?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let days: u64 = days.try_into().ok()?;
    Some(days * MS_PER_DAY + hour * MS_PER_HOUR + minute * MS_PER_MINUTE + second * 1000 + millis)
}

/// Days since the Unix epoch (1970-01-01) for a UTC civil date. Howard
/// Hinnant's `days_from_civil`: <https://howardhinnant.github.io/date_algorithms.html>.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (i64::from(m) + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`]: the UTC civil date for `z` days since
/// the Unix epoch.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    #[expect(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "doy - (153*mp+2)/5 + 1 is in [1, 31] by construction"
    )]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    #[expect(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "mp is in [0, 11] by construction, so month is in [1, 12]"
    )]
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if month <= 2 { y + 1 } else { y };
    (y, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The maximum epoch millisecond value whose formatted year stays within
    /// RFC 3339's 0-9999 range (9999-12-31T23:59:59.999Z), bounding the
    /// proptest to the range where [`format`]'s fixed-width guarantee holds.
    const MAX_MS: u64 = 253_402_300_799_999;

    #[test]
    fn formats_a_known_instant() {
        // 2015-09-11T20:32:33.936Z, from the SES documentation examples.
        assert_eq!(format(1_442_003_553_936), "2015-09-11T20:32:33.936Z");
    }

    #[test]
    fn formats_the_epoch() {
        assert_eq!(format(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn formats_are_always_24_bytes() {
        for ms in [0, 1, 999, 1000, 1_442_003_553_936, MAX_MS] {
            assert_eq!(format(ms).len(), 24, "ms={ms}");
        }
    }

    #[test]
    fn parses_a_known_instant() {
        assert_eq!(parse("2015-09-11T20:32:33.936Z"), Some(1_442_003_553_936));
    }

    #[test]
    fn parse_rejects_malformed_input() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("2015-09-11T20:32:33.936"), None); // missing Z
        assert_eq!(parse("2015-09-11 20:32:33.936Z"), None); // missing T
        assert_eq!(parse("2015-13-11T20:32:33.936Z"), None); // month 13
        assert_eq!(parse("2015-09-11T24:32:33.936Z"), None); // hour 24
        assert_eq!(parse("not a timestamp, just text"), None);
    }

    #[test]
    fn round_trips_millisecond_boundaries() {
        for ms in [0, 999, 1000, 86_399_999, 86_400_000, MAX_MS] {
            assert_eq!(parse(&format(ms)), Some(ms), "ms={ms}");
        }
    }

    proptest! {
        #[test]
        fn round_trip_holds_for_any_representable_instant(ms in 0u64..=MAX_MS) {
            prop_assert_eq!(parse(&format(ms)), Some(ms));
        }

        #[test]
        fn format_is_always_24_bytes_and_lexicographic_order_matches_chronological(
            a in 0u64..=MAX_MS,
            b in 0u64..=MAX_MS,
        ) {
            let (fa, fb) = (format(a), format(b));
            prop_assert_eq!(fa.len(), 24);
            prop_assert_eq!(fb.len(), 24);
            prop_assert_eq!(a.cmp(&b), fa.cmp(&fb));
        }
    }
}
