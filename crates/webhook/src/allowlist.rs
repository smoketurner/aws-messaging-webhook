//! Topic allowlist: the second half of the security boundary.
//!
//! Signature verification proves a message came from SNS — from *any* AWS
//! account's topic. Without this check, the public auto-confirming Function
//! URL would accept subscriptions from (and persist events for) anyone who
//! discovers it.

/// Parsed `ALLOWED_TOPICS`: comma-separated entries, each either a bare
/// 12-digit account id (allows every topic that account owns) or a full
/// `TopicArn` glob with `*` wildcards.
#[derive(Debug, Clone)]
pub struct TopicAllowlist {
    entries: Vec<Entry>,
}

#[derive(Debug, Clone)]
enum Entry {
    Account(String),
    ArnGlob(String),
}

impl TopicAllowlist {
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        let entries = raw
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let is_account = entry.len() == 12 && entry.bytes().all(|b| b.is_ascii_digit());
                if is_account {
                    Entry::Account(entry.to_owned())
                } else {
                    Entry::ArnGlob(entry.to_owned())
                }
            })
            .collect();
        Self { entries }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether a (signature-covered) `TopicArn` is allowed. An empty
    /// allowlist allows everything — deployment docs mark that dev-only, and
    /// startup logs a loud warning.
    #[must_use]
    pub fn allows(&self, topic_arn: &str) -> bool {
        if self.entries.is_empty() {
            return true;
        }
        let account = arn_account(topic_arn);
        for entry in &self.entries {
            let matched = match entry {
                Entry::Account(id) => account == Some(id.as_str()),
                Entry::ArnGlob(pattern) => glob_match(pattern, topic_arn),
            };
            if matched {
                return true;
            }
        }
        false
    }
}

/// The account field of `arn:<partition>:sns:<region>:<account>:<topic>`.
fn arn_account(topic_arn: &str) -> Option<&str> {
    topic_arn.split(':').nth(4)
}

/// `*`-wildcard match without a regex dependency: fixed segments must appear
/// in order, anchored at both ends.
pub(crate) fn glob_match(pattern: &str, value: &str) -> bool {
    let segments: Vec<&str> = pattern.split('*').collect();
    let [only] = segments.as_slice() else {
        let mut rest = value;
        let first = segments[0];
        let Some(after_first) = rest.strip_prefix(first) else {
            return false;
        };
        rest = after_first;
        for middle in &segments[1..segments.len() - 1] {
            if middle.is_empty() {
                continue;
            }
            let Some(index) = rest.find(middle) else {
                return false;
            };
            rest = &rest[index + middle.len()..];
        }
        return rest.ends_with(segments[segments.len() - 1]);
    };
    *only == value
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARN: &str = "arn:aws:sns:us-east-1:123456789012:prod-ses-events";

    #[test]
    fn account_id_entry_matches_any_topic_in_account() {
        let list = TopicAllowlist::parse("123456789012");
        assert!(list.allows(ARN));
        assert!(list.allows("arn:aws:sns:eu-west-1:123456789012:other"));
        assert!(!list.allows("arn:aws:sns:us-east-1:999999999999:prod-ses-events"));
    }

    #[test]
    fn glob_entries_match_start_middle_and_end() {
        assert!(TopicAllowlist::parse("arn:aws:sns:us-east-1:123456789012:prod-*").allows(ARN));
        assert!(TopicAllowlist::parse("arn:aws:sns:*:123456789012:prod-ses-events").allows(ARN));
        assert!(TopicAllowlist::parse("*prod-ses-events").allows(ARN));
        assert!(TopicAllowlist::parse("arn:aws:sns:*:123456789012:*").allows(ARN));
    }

    #[test]
    fn exact_arn_entry_requires_exact_match() {
        let list = TopicAllowlist::parse(ARN);
        assert!(list.allows(ARN));
        assert!(!list.allows("arn:aws:sns:us-east-1:123456789012:prod-ses-events-2"));
    }

    #[test]
    fn non_matching_entries_reject() {
        let list = TopicAllowlist::parse("999999999999,arn:aws:sns:*:111111111111:*");
        assert!(!list.allows(ARN));
    }

    #[test]
    fn multiple_entries_any_match_wins() {
        let list = TopicAllowlist::parse("999999999999, arn:aws:sns:us-east-1:123456789012:prod-*");
        assert!(list.allows(ARN));
    }

    #[test]
    fn malformed_arn_never_matches_account_entries() {
        let list = TopicAllowlist::parse("123456789012");
        assert!(!list.allows("not-an-arn"));
        assert!(!list.allows(""));
        assert!(!list.allows("arn:aws:sns:us-east-1"));
    }

    #[test]
    fn empty_allowlist_allows_everything() {
        assert!(TopicAllowlist::parse("").allows(ARN));
        assert!(TopicAllowlist::parse(" , ,").is_empty());
    }

    #[test]
    fn is_empty_reflects_entry_presence() {
        assert!(TopicAllowlist::parse("").is_empty());
        assert!(!TopicAllowlist::parse("123456789012").is_empty());
    }

    #[test]
    fn only_exactly_twelve_digits_is_an_account_entry() {
        // 13 digits: not an account, treated as an exact-match glob.
        assert!(!TopicAllowlist::parse("1234567890123").allows(ARN));
        // 10 digits: also not an account. If the account-detection used OR
        // instead of AND (len==12 || all-digit), this would wrongly match the
        // account field; as an exact glob it cannot match a full ARN.
        assert!(!TopicAllowlist::parse("1234567890").allows("arn:aws:sns:us-east-1:1234567890:t"));
        // 12 non-digit chars: not an account either, exact glob only.
        assert!(!TopicAllowlist::parse("abcdefghijkl").allows(ARN));
    }
}
