//! List-endpoint query parameters: parsing, validation, and the translation
//! from a request's instants to index sort-key bounds.
//!
//! Two things make this more than a `serde` derive. The contract allows
//! `from`, `to`, `subject` and `labels` to repeat, which
//! `serde_urlencoded` cannot express, so the query string is walked directly.
//! And `before`/`after` arrive as timestamps but are compared against sort
//! keys that are not timestamps: a message list sorts by `UUIDv7` message id,
//! which begins with its own millisecond timestamp, while a thread list sorts
//! by `<timestamp>#<thread_id>`. Each gets its own boundary encoding.

use crate::api::error::{ApiError, FieldError};
use crate::mail::store::{DEFAULT_LIMIT, MAX_LIMIT};
use crate::mail::thread::ThreadState;
use crate::mail::{MailMessage, ids, time};

/// The `include_*` flags and the label each one admits. A flag that is absent
/// or false excludes items carrying its label.
///
/// `blocked` has no system label in this service — nothing applies it — but
/// the flag is honored so a client that sets it behaves the same here as
/// against the reference implementation.
const INCLUDE_FLAGS: [(&str, &str); 4] = [
    ("include_spam", "spam"),
    ("include_blocked", "blocked"),
    ("include_unauthenticated", "unauthenticated"),
    ("include_trash", "trash"),
];

/// A parsed, validated list request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    pub limit: usize,
    /// Oldest first when true; newest first (the default) when false.
    pub ascending: bool,
    /// Exclusive upper bound as epoch milliseconds: items strictly older.
    pub before_ms: Option<u64>,
    /// Inclusive lower bound as epoch milliseconds: items at or after it.
    pub after_ms: Option<u64>,
    pub page_token: Option<String>,
    pub filters: Filters,
}

/// The post-query filters. The index is time-ordered, so these are applied to
/// each page as it comes back rather than pushed into the key condition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    /// Every one of these labels must be present for an item to match.
    pub labels: Vec<String>,
    /// Any one of these labels excludes an item, from the `include_*` flags.
    pub excluded: Vec<&'static str>,
    /// Case-insensitive substrings; an item matches a field if *any* value
    /// matches, and must match every field that was given.
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub subject: Vec<String>,
}

impl Filters {
    fn labels_match(&self, labels: &[String]) -> bool {
        if self
            .excluded
            .iter()
            .any(|excluded| labels.iter().any(|label| label == excluded))
        {
            return false;
        }
        self.labels
            .iter()
            .all(|wanted| labels.iter().any(|label| label == wanted))
    }

    #[must_use]
    pub fn matches_message(&self, msg: &MailMessage) -> bool {
        self.labels_match(&msg.labels)
            && any_contains(&self.from, std::slice::from_ref(&msg.from))
            && any_contains(&self.to, &msg.to)
            && any_contains(&self.subject, std::slice::from_ref(&msg.subject))
    }

    #[must_use]
    pub fn matches_thread(&self, thread: &ThreadState) -> bool {
        self.labels_match(&thread.labels)
            && any_contains(&self.from, &thread.senders)
            && any_contains(&self.to, &thread.recipients)
            && any_contains(&self.subject, std::slice::from_ref(&thread.subject))
    }
}

/// Whether any `needle` appears case-insensitively in any `haystack` value.
/// An empty needle list is "no filter on this field" and always matches.
fn any_contains(needles: &[String], haystacks: &[String]) -> bool {
    if needles.is_empty() {
        return true;
    }
    needles.iter().any(|needle| {
        let needle = needle.to_lowercase();
        haystacks
            .iter()
            .any(|value| value.to_lowercase().contains(&needle))
    })
}

impl ListRequest {
    /// The sort-key bounds for a message list.
    ///
    /// A `UUIDv7` renders as its 48-bit millisecond timestamp first, so
    /// [`ids::time_prefix`] is a valid lexical boundary against a whole id.
    /// `after` lands below every id in its millisecond and `before` below
    /// every id in its own, giving a half-open `[after, before)` range.
    #[must_use]
    pub fn message_bounds(&self) -> (Option<String>, Option<String>) {
        (
            self.before_ms.map(ids::time_prefix),
            self.after_ms.map(ids::time_prefix),
        )
    }

    /// The sort-key bounds for a thread list, whose keys are
    /// `<timestamp>#<thread_id>`. A bare timestamp sorts below every key that
    /// extends it, so the same half-open range holds.
    #[must_use]
    pub fn thread_bounds(&self) -> (Option<String>, Option<String>) {
        (
            self.before_ms.map(time::format),
            self.after_ms.map(time::format),
        )
    }

    /// The query shape a page token is bound to: the sort order and time
    /// window. A token is only valid for a request with the same shape, since
    /// its start key must fall inside that request's key range.
    #[must_use]
    pub fn token_scope(&self) -> String {
        format!(
            "asc={};before={};after={}",
            self.ascending,
            self.before_ms.map_or_else(String::new, |ms| ms.to_string()),
            self.after_ms.map_or_else(String::new, |ms| ms.to_string()),
        )
    }

    /// Parses and validates a raw query string.
    ///
    /// # Errors
    ///
    /// [`ApiError::Validation`] listing every offending parameter at once, so
    /// a caller fixing a request sees all of its problems in one response.
    pub fn parse(query: &str) -> Result<Self, ApiError> {
        let mut errors = Vec::new();
        let mut request = Self {
            limit: DEFAULT_LIMIT,
            ascending: false,
            before_ms: None,
            after_ms: None,
            page_token: None,
            filters: Filters::default(),
        };
        // Absent flags default to false, which excludes the label.
        let mut include: [bool; 4] = [false; 4];

        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            let value = value.trim().to_owned();
            match key.as_ref() {
                "limit" => match value.parse::<usize>() {
                    Ok(limit) if (1..=MAX_LIMIT).contains(&limit) => request.limit = limit,
                    _ => errors.push(field(
                        "limit",
                        format!("must be a whole number from 1 to {MAX_LIMIT}"),
                    )),
                },
                "ascending" => match parse_bool(&value) {
                    Some(ascending) => request.ascending = ascending,
                    None => errors.push(field("ascending", "must be true or false")),
                },
                "before" => match parse_instant(&value) {
                    Some(ms) => request.before_ms = Some(ms),
                    None => errors.push(field("before", TIMESTAMP_HELP)),
                },
                "after" => match parse_instant(&value) {
                    Some(ms) => request.after_ms = Some(ms),
                    None => errors.push(field("after", TIMESTAMP_HELP)),
                },
                "page_token" => {
                    if !value.is_empty() {
                        request.page_token = Some(value);
                    }
                }
                "labels" => push_non_empty(&mut request.filters.labels, value),
                "from" => push_non_empty(&mut request.filters.from, value),
                "to" => push_non_empty(&mut request.filters.to, value),
                "subject" => push_non_empty(&mut request.filters.subject, value),
                other => {
                    if let Some(index) = INCLUDE_FLAGS.iter().position(|(flag, _)| *flag == other) {
                        match parse_bool(&value) {
                            Some(on) => include[index] = on,
                            None => errors.push(field(other.to_owned(), "must be true or false")),
                        }
                    }
                    // Unrecognized parameters are ignored: a client sending a
                    // parameter this service does not implement gets the
                    // unfiltered result, not a rejection.
                }
            }
        }

        if let (Some(after), Some(before)) = (request.after_ms, request.before_ms)
            && after >= before
        {
            errors.push(field("after", "must be earlier than `before`"));
        }

        for (index, (_, label)) in INCLUDE_FLAGS.iter().enumerate() {
            if !include[index] {
                request.filters.excluded.push(label);
            }
        }
        // An explicitly requested label always wins over the flag that would
        // otherwise hide it: `?labels=trash` means the caller wants trash.
        request
            .filters
            .excluded
            .retain(|excluded| !request.filters.labels.iter().any(|label| label == excluded));

        if errors.is_empty() {
            Ok(request)
        } else {
            Err(ApiError::Validation(errors))
        }
    }
}

const TIMESTAMP_HELP: &str = "must be a UTC timestamp, for example 2026-01-15T09:30:00.000Z";

fn field(path: impl Into<String>, message: impl Into<String>) -> FieldError {
    FieldError {
        path: path.into(),
        message: message.into(),
    }
}

fn push_non_empty(values: &mut Vec<String>, value: String) {
    if !value.is_empty() {
        values.push(value);
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// Parses a UTC timestamp into epoch milliseconds, accepting both the
/// millisecond form this service emits and the whole-second form clients
/// commonly send. Offsets other than `Z` are rejected rather than silently
/// misread as UTC.
fn parse_instant(value: &str) -> Option<u64> {
    if let Some(ms) = time::parse(value) {
        return Some(ms);
    }
    // `YYYY-MM-DDTHH:MM:SSZ` — the same shape without the fraction.
    let seconds = value.strip_suffix('Z')?;
    if seconds.len() != 19 {
        return None;
    }
    time::parse(&format!("{seconds}.000Z"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(query: &str) -> ListRequest {
        ListRequest::parse(query).expect("query should parse")
    }

    fn errors(query: &str) -> Vec<String> {
        match ListRequest::parse(query) {
            Err(ApiError::Validation(errors)) => {
                errors.into_iter().map(|error| error.path).collect()
            }
            other => panic!("expected a validation failure, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_query_is_the_documented_defaults() {
        let request = parse_ok("");
        assert_eq!(request.limit, DEFAULT_LIMIT);
        assert!(!request.ascending);
        assert_eq!(request.before_ms, None);
        assert_eq!(request.after_ms, None);
        assert_eq!(request.page_token, None);
        // Every include_* flag defaults off, so all four labels are excluded.
        assert_eq!(
            request.filters.excluded,
            vec!["spam", "blocked", "unauthenticated", "trash"]
        );
    }

    #[test]
    fn repeated_parameters_accumulate() {
        let request = parse_ok("from=alice&from=bob&to=support&subject=invoice&labels=a&labels=b");
        assert_eq!(request.filters.from, vec!["alice", "bob"]);
        assert_eq!(request.filters.to, vec!["support"]);
        assert_eq!(request.filters.subject, vec!["invoice"]);
        assert_eq!(request.filters.labels, vec!["a", "b"]);
    }

    #[test]
    fn include_flags_admit_their_labels() {
        let request = parse_ok("include_spam=true&include_trash=true");
        assert_eq!(request.filters.excluded, vec!["blocked", "unauthenticated"]);
    }

    #[test]
    fn asking_for_a_label_overrides_the_flag_that_would_hide_it() {
        // Without this, `?labels=trash` would always return nothing.
        let request = parse_ok("labels=trash");
        assert!(!request.filters.excluded.contains(&"trash"));
        assert!(request.filters.excluded.contains(&"spam"));
    }

    #[test]
    fn limit_is_bounded_on_both_sides() {
        assert_eq!(parse_ok("limit=1").limit, 1);
        assert_eq!(parse_ok(&format!("limit={MAX_LIMIT}")).limit, MAX_LIMIT);
        assert_eq!(errors("limit=0"), vec!["limit"]);
        assert_eq!(errors(&format!("limit={}", MAX_LIMIT + 1)), vec!["limit"]);
        assert_eq!(errors("limit=abc"), vec!["limit"]);
        assert_eq!(errors("limit=-1"), vec!["limit"]);
    }

    #[test]
    fn booleans_reject_anything_but_true_or_false() {
        assert!(parse_ok("ascending=true").ascending);
        assert!(!parse_ok("ascending=false").ascending);
        assert_eq!(errors("ascending=yes"), vec!["ascending"]);
        assert_eq!(errors("include_spam=1"), vec!["include_spam"]);
    }

    #[test]
    fn timestamps_accept_both_precisions_and_reject_offsets() {
        let with_millis = parse_ok("after=2026-01-15T09:30:00.250Z");
        assert_eq!(
            with_millis.after_ms,
            time::parse("2026-01-15T09:30:00.250Z")
        );

        let whole_second = parse_ok("after=2026-01-15T09:30:00Z");
        assert_eq!(
            whole_second.after_ms,
            time::parse("2026-01-15T09:30:00.000Z")
        );

        assert_eq!(errors("after=2026-01-15T09:30:00+01:00"), vec!["after"]);
        assert_eq!(errors("before=yesterday"), vec!["before"]);
        assert_eq!(errors("before=2026-01-15"), vec!["before"]);
    }

    #[test]
    fn an_inverted_range_is_rejected() {
        assert_eq!(
            errors("after=2026-01-15T10:00:00.000Z&before=2026-01-15T09:00:00.000Z"),
            vec!["after"]
        );
        // Equal bounds select nothing, so they are rejected too.
        assert_eq!(
            errors("after=2026-01-15T10:00:00.000Z&before=2026-01-15T10:00:00.000Z"),
            vec!["after"]
        );
    }

    #[test]
    fn every_problem_is_reported_in_one_response() {
        let paths = errors("limit=999&ascending=maybe&before=nope");
        assert_eq!(paths, vec!["limit", "ascending", "before"]);
    }

    #[test]
    fn unrecognized_parameters_are_ignored() {
        let request = parse_ok("cursor=abc&limit=5");
        assert_eq!(request.limit, 5);
    }

    #[test]
    fn an_empty_page_token_is_treated_as_absent() {
        assert_eq!(parse_ok("page_token=").page_token, None);
    }

    #[test]
    fn message_bounds_order_the_same_way_message_ids_do() {
        let request = parse_ok("after=2026-01-15T09:00:00.000Z&before=2026-01-15T10:00:00.000Z");
        let (before, after) = request.message_bounds();
        let (before, after) = (before.unwrap(), after.unwrap());
        assert!(after < before, "{after} should sort below {before}");

        // A real id minted inside the window falls between the two bounds.
        let inside = time::parse("2026-01-15T09:30:00.000Z").unwrap();
        let id = crate::mail::ids::inbound_message_id("ses-1", inside).to_string();
        assert!(after < id && id < before, "{id} should fall inside");
    }

    #[test]
    fn thread_bounds_sort_below_the_keys_that_extend_them() {
        let request = parse_ok("after=2026-01-15T09:00:00.000Z");
        let (_, after) = request.thread_bounds();
        let after = after.unwrap();
        assert_eq!(after, "2026-01-15T09:00:00.000Z");
        // The sort key appends `#<thread_id>`, which sorts after the bare
        // bound, so a thread at exactly this instant is included.
        assert!(after < format!("{after}#tid-1"));
    }
}
