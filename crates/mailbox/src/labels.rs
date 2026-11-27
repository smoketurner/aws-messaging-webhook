//! Labels, system labels, and inbound verdict classification.
//!
//! A message's labels are an open set — the system labels the pipeline
//! applies plus whatever the caller adds — held as [`Label`]s and stored,
//! indexed and published as strings.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::LABEL_MAX_BYTES;

/// One label: 1 to [`LABEL_MAX_BYTES`] bytes of `a-z`, `0-9`, `_` and `-`.
///
/// A variant per label the pipeline applies itself, so the compiler asks
/// what a new one means everywhere it matters, and [`Label::User`] for the
/// caller's own. Keeping `#` out matters beyond tidiness: it separates the
/// parts of every mail-table key. Deserializing applies the same check, so a
/// stored or requested label that breaks it fails to read rather than
/// spreading.
///
/// Compares, orders and hashes by its string — the order items store their
/// labels in — not by declaration order.
#[derive(Debug, Clone)]
pub enum Label {
    /// Inbound mail, set by ingest.
    Received,
    /// The send reached SES.
    Sent,
    /// Accepted by the API, not yet sent.
    Queued,
    /// Nobody has read it.
    Unread,
    /// SES quarantined it, or a caller marked it spam.
    Spam,
    /// A caller trashed it.
    Trash,
    /// SPF, DKIM or DMARC failed.
    Unauthenticated,
    /// SES delivered it to the recipient's provider.
    Delivered,
    /// It bounced.
    Bounced,
    /// The recipient reported it as spam.
    Complained,
    /// SES refused it outright.
    Rejected,
    /// The recipient opened it.
    Opened,
    /// A caller's own label.
    User(UserLabel),
}

/// A caller's own label: valid, and never a system label's name, so each
/// string has exactly one [`Label`].
#[derive(Debug, Clone)]
pub struct UserLabel(String);

/// Why a string is not a [`Label`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a label is 1 to {LABEL_MAX_BYTES} bytes of `a-z`, `0-9`, `_` and `-`")]
pub struct InvalidLabel;

impl Label {
    /// Every system label: each variant but [`Label::User`]. A new variant
    /// goes here too, or [`Label::parse`] reads its string as a user label.
    pub const SYSTEM: [Self; 12] = [
        Self::Received,
        Self::Sent,
        Self::Queued,
        Self::Unread,
        Self::Spam,
        Self::Trash,
        Self::Unauthenticated,
        Self::Delivered,
        Self::Bounced,
        Self::Complained,
        Self::Rejected,
        Self::Opened,
    ];

    /// `raw` as a label, unchanged: no trimming or case folding, which is
    /// the request layer's job.
    ///
    /// # Errors
    ///
    /// [`InvalidLabel`] when `raw` is empty, longer than
    /// [`LABEL_MAX_BYTES`], or uses any other character.
    pub fn parse(raw: &str) -> Result<Self, InvalidLabel> {
        if let Some(system) = Self::SYSTEM.into_iter().find(|label| label.as_str() == raw) {
            return Ok(system);
        }
        let valid = !raw.is_empty()
            && raw.len() <= LABEL_MAX_BYTES
            && raw
                .bytes()
                .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
        if valid {
            Ok(Self::User(UserLabel(raw.to_owned())))
        } else {
            Err(InvalidLabel)
        }
    }

    /// The stored and published form.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        match self {
            Self::Received => "received",
            Self::Sent => "sent",
            Self::Queued => "queued",
            Self::Unread => "unread",
            Self::Spam => "spam",
            Self::Trash => "trash",
            Self::Unauthenticated => "unauthenticated",
            Self::Delivered => "delivered",
            Self::Bounced => "bounced",
            Self::Complained => "complained",
            Self::Rejected => "rejected",
            Self::Opened => "opened",
            Self::User(label) => label.0.as_str(),
        }
    }

    /// Whether the pipeline applies this label, as opposed to a caller.
    #[must_use]
    pub const fn is_system(&self) -> bool {
        match self {
            Self::User(_) => false,
            Self::Received
            | Self::Sent
            | Self::Queued
            | Self::Unread
            | Self::Spam
            | Self::Trash
            | Self::Unauthenticated
            | Self::Delivered
            | Self::Bounced
            | Self::Complained
            | Self::Rejected
            | Self::Opened => true,
        }
    }

    /// Whether a request is barred from setting or clearing this label.
    /// `unread`, `spam` and `trash` are the mailbox state a caller owns;
    /// every other system label is a record of something that happened,
    /// which a request must not be able to invent.
    #[must_use]
    pub const fn is_reserved(&self) -> bool {
        match self {
            Self::Unread | Self::Spam | Self::Trash | Self::User(_) => false,
            Self::Received
            | Self::Sent
            | Self::Queued
            | Self::Unauthenticated
            | Self::Delivered
            | Self::Bounced
            | Self::Complained
            | Self::Rejected
            | Self::Opened => true,
        }
    }
}

impl PartialEq for Label {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Label {}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

/// Hashes as its string, which [`Borrow<str>`] requires.
impl Hash for Label {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl PartialEq<str> for Label {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Label {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Lets a label-keyed map be read by `&str`. Sound because `Label`'s
/// `Eq`, `Ord` and `Hash` are its string's.
impl Borrow<str> for Label {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for Label {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Label {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// How many of `labels` are the caller's own rather than system labels.
#[must_use]
pub fn user_label_count(labels: &[Label]) -> usize {
    labels.iter().filter(|label| !label.is_system()).count()
}

/// The inbound classification label an ingested message gets beyond
/// `received`/`unread`: spam takes precedence over an authentication
/// failure, which takes precedence over neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundVerdict {
    Spam,
    Unauthenticated,
    Clean,
}

impl InboundVerdict {
    /// The extra label this verdict adds, if any.
    #[must_use]
    pub fn label(self) -> Option<Label> {
        match self {
            Self::Spam => Some(Label::Spam),
            Self::Unauthenticated => Some(Label::Unauthenticated),
            Self::Clean => None,
        }
    }
}

/// Classifies an inbound receipt's verdicts: spam/virus `FAIL` → spam
/// (quarantined); SPF/DKIM/DMARC `FAIL` → unauthenticated; otherwise clean.
/// Nothing is ever dropped — this only selects the label and detail-type.
/// Event precedence: spam > unauthenticated > received.
#[must_use]
pub fn classify_inbound(spam_or_virus_failed: bool, auth_failed: bool) -> InboundVerdict {
    if spam_or_virus_failed {
        InboundVerdict::Spam
    } else if auth_failed {
        InboundVerdict::Unauthenticated
    } else {
        InboundVerdict::Clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(raw: &str) -> Label {
        Label::parse(raw).unwrap()
    }

    #[test]
    fn only_lowercase_ascii_letters_digits_underscore_and_hyphen_parse() {
        for raw in ["billing", "follow-up", "q3_2026", "a", "0"] {
            assert_eq!(Label::parse(raw).map(|l| l.to_string()), Ok(raw.to_owned()));
        }
        let longest = "x".repeat(LABEL_MAX_BYTES);
        assert!(Label::parse(&longest).is_ok());
        for raw in [
            "",
            "bad#label",
            "two words",
            "tab\t",
            "new\nline",
            "nul\0",
            "Upper",
            "dot.ted",
            "slash/ed",
            "café",
            "emoji🙂",
            &"x".repeat(LABEL_MAX_BYTES + 1),
        ] {
            assert_eq!(Label::parse(raw), Err(InvalidLabel), "{raw:?}");
        }
    }

    #[test]
    fn deserializing_applies_the_same_rule() {
        let parsed: Vec<Label> = serde_json::from_str(r#"["sent","follow-up"]"#).unwrap();
        assert_eq!(parsed, vec![Label::Sent, label("follow-up")]);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            r#"["sent","follow-up"]"#
        );
        assert!(serde_json::from_str::<Label>(r#""bad#label""#).is_err());
        assert!(serde_json::from_str::<Label>(r#""""#).is_err());
    }

    /// Round-tripping every system label is what keeps the strings and the
    /// variants from drifting when one is added.
    #[test]
    fn every_system_label_parses_back_to_its_variant() {
        for system in Label::SYSTEM {
            let parsed = Label::parse(system.as_str()).unwrap();
            assert!(parsed.is_system(), "{system}");
            assert_eq!(parsed, system);
        }
        assert!(!label("invoices").is_system());
    }

    #[test]
    fn a_user_label_never_holds_a_system_name() {
        assert!(matches!(label("received"), Label::Received));
        assert!(matches!(label("invoices"), Label::User(_)));
    }

    #[test]
    fn unread_spam_trash_are_not_reserved() {
        assert!(!Label::Unread.is_reserved());
        assert!(!Label::Spam.is_reserved());
        assert!(!Label::Trash.is_reserved());
    }

    #[test]
    fn other_system_labels_are_reserved() {
        assert!(Label::Received.is_reserved());
        assert!(Label::Sent.is_reserved());
        assert!(Label::Delivered.is_reserved());
    }

    #[test]
    fn a_users_own_label_is_never_reserved() {
        assert!(!label("invoices").is_reserved());
    }

    #[test]
    fn system_holds_each_label_once() {
        let mut seen: Vec<&str> = Label::SYSTEM.iter().map(Label::as_str).collect();
        seen.sort_unstable();
        let count = seen.len();
        seen.dedup();
        assert_eq!(seen.len(), count, "a label is listed twice in SYSTEM");
    }

    #[test]
    fn labels_order_by_their_string() {
        let mut labels = vec![Label::Unread, label("invoices"), Label::Received];
        labels.sort();
        assert_eq!(labels, ["invoices", "received", "unread"]);
    }

    #[test]
    fn user_labels_are_the_ones_outside_the_system_set() {
        let labels = vec![
            Label::Received,
            label("invoices"),
            Label::Unread,
            label("q3"),
        ];
        assert_eq!(user_label_count(&labels), 2);
    }

    #[test]
    fn classify_inbound_prefers_spam_over_auth_failure() {
        assert_eq!(classify_inbound(true, true), InboundVerdict::Spam);
        assert_eq!(classify_inbound(true, false), InboundVerdict::Spam);
        assert_eq!(
            classify_inbound(false, true),
            InboundVerdict::Unauthenticated
        );
        assert_eq!(classify_inbound(false, false), InboundVerdict::Clean);
    }

    #[test]
    fn a_verdict_names_the_label_it_adds() {
        assert_eq!(InboundVerdict::Spam.label(), Some(Label::Spam), "spam");
        assert_eq!(
            InboundVerdict::Unauthenticated.label(),
            Some(Label::Unauthenticated)
        );
        assert_eq!(InboundVerdict::Clean.label(), None);
    }
}
