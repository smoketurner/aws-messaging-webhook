//! Labels, system labels, and inbound verdict classification.
//!
//! A message's labels are an open set — the system labels below plus
//! whatever the caller adds — held as [`Label`]s and stored, indexed and
//! published as strings. [`SystemLabel`] is the closed half: every label the
//! pipeline itself applies, so the compiler asks what a new one means
//! everywhere it matters instead of leaving a literal to be typo'd.

use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::LABEL_MAX_BYTES;

/// One label: 1 to [`LABEL_MAX_BYTES`] bytes of `a-z`, `0-9`, `_` and `-`.
///
/// Keeping `#` out matters beyond tidiness: it separates the parts of every
/// mail-table key, so a label could never be a key segment if it could
/// contain one. Deserializing applies the same check, so a stored or
/// requested label that breaks it fails to read rather than spreading.
/// Orders by its string, which is the order items store their labels in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Label(String);

/// Why a string is not a [`Label`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a label is 1 to {LABEL_MAX_BYTES} bytes of `a-z`, `0-9`, `_` and `-`")]
pub struct InvalidLabel;

impl Label {
    /// `raw` as a label, unchanged: no trimming or case folding, which is
    /// the request layer's job.
    ///
    /// # Errors
    ///
    /// [`InvalidLabel`] when `raw` is empty, longer than
    /// [`LABEL_MAX_BYTES`], or uses any other character.
    pub fn parse(raw: &str) -> Result<Self, InvalidLabel> {
        let valid = !raw.is_empty()
            && raw.len() <= LABEL_MAX_BYTES
            && raw
                .bytes()
                .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
        if valid {
            Ok(Self(raw.to_owned()))
        } else {
            Err(InvalidLabel)
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The system label this is, or `None` for a caller's own label.
    #[must_use]
    pub fn system(&self) -> Option<SystemLabel> {
        SystemLabel::ALL
            .into_iter()
            .find(|known| known.as_str() == self.0)
    }

    /// Every system label except `unread`, `spam` and `trash` — rejected on
    /// a PATCH request and in a send's `labels`.
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        self.system()
            .is_some_and(|label| !label.is_settable_by_request())
    }
}

impl From<SystemLabel> for Label {
    fn from(label: SystemLabel) -> Self {
        Self(label.as_str().to_owned())
    }
}

impl PartialEq<SystemLabel> for Label {
    fn eq(&self, other: &SystemLabel) -> bool {
        self.0 == other.as_str()
    }
}

impl PartialEq<str> for Label {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Label {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// Lets a label-keyed map be read by `&str`. Sound because `Label`'s
/// `Eq`, `Ord` and `Hash` are its string's.
impl Borrow<str> for Label {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Label {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Label {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

string_enum! {
    /// A label the pipeline applies itself.
    ///
    /// Deliberately not `Ord`: items store their labels sorted by the stored
    /// string, and ordering the variants would sort by declaration order
    /// instead.
    pub enum SystemLabel {
        /// Inbound mail, set by ingest.
        Received = "received",
        /// The send reached SES.
        Sent = "sent",
        /// Accepted by the API, not yet sent.
        Queued = "queued",
        /// Nobody has read it.
        Unread = "unread",
        /// SES quarantined it, or a caller marked it spam.
        Spam = "spam",
        /// A caller trashed it.
        Trash = "trash",
        /// SPF, DKIM or DMARC failed.
        Unauthenticated = "unauthenticated",
        /// SES delivered it to the recipient's provider.
        Delivered = "delivered",
        /// It bounced.
        Bounced = "bounced",
        /// The recipient reported it as spam.
        Complained = "complained",
        /// SES refused it outright.
        Rejected = "rejected",
        /// The recipient opened it.
        Opened = "opened",
    }
}

impl SystemLabel {
    /// Whether a request may set or clear this label. `unread`, `spam` and
    /// `trash` are the mailbox state a caller owns; every other system label
    /// is a record of something that happened, which a request must not be
    /// able to invent.
    #[must_use]
    pub(crate) fn is_settable_by_request(self) -> bool {
        match self {
            Self::Unread | Self::Spam | Self::Trash => true,
            Self::Received
            | Self::Sent
            | Self::Queued
            | Self::Unauthenticated
            | Self::Delivered
            | Self::Bounced
            | Self::Complained
            | Self::Rejected
            | Self::Opened => false,
        }
    }
}

impl fmt::Display for SystemLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether `labels` carries `label`.
#[must_use]
pub fn has(labels: &[Label], label: SystemLabel) -> bool {
    labels.iter().any(|existing| *existing == label)
}

/// How many of `labels` are the caller's own rather than system labels.
#[must_use]
pub fn user_label_count(labels: &[Label]) -> usize {
    labels
        .iter()
        .filter(|label| label.system().is_none())
        .count()
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
    pub fn label(self) -> Option<SystemLabel> {
        match self {
            Self::Spam => Some(SystemLabel::Spam),
            Self::Unauthenticated => Some(SystemLabel::Unauthenticated),
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
        assert_eq!(parsed, vec![label("sent"), label("follow-up")]);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            r#"["sent","follow-up"]"#
        );
        assert!(serde_json::from_str::<Label>(r#""bad#label""#).is_err());
        assert!(serde_json::from_str::<Label>(r#""""#).is_err());
    }

    /// Round-tripping every variant is what keeps the stored strings and the
    /// enum from drifting when a variant is added.
    #[test]
    fn every_system_label_is_a_label_that_names_itself() {
        for system in SystemLabel::ALL {
            let parsed = Label::parse(system.as_str()).unwrap();
            assert_eq!(parsed, Label::from(system));
            assert_eq!(parsed.system(), Some(system));
            assert_eq!(parsed, system);
        }
        assert_eq!(label("invoices").system(), None);
    }

    #[test]
    fn unread_spam_trash_are_not_reserved() {
        assert!(!label("unread").is_reserved());
        assert!(!label("spam").is_reserved());
        assert!(!label("trash").is_reserved());
    }

    #[test]
    fn other_system_labels_are_reserved() {
        assert!(label("received").is_reserved());
        assert!(label("sent").is_reserved());
        assert!(label("delivered").is_reserved());
    }

    #[test]
    fn a_users_own_label_is_never_reserved() {
        assert!(!label("invoices").is_reserved());
    }

    #[test]
    fn all_holds_each_variant_once() {
        let mut seen: Vec<&str> = SystemLabel::ALL.iter().map(|l| l.as_str()).collect();
        seen.sort_unstable();
        let count = seen.len();
        seen.dedup();
        assert_eq!(seen.len(), count, "a label is listed twice in ALL");
    }

    #[test]
    fn user_labels_are_the_ones_outside_the_enum() {
        let labels = vec![
            label("received"),
            label("invoices"),
            label("unread"),
            label("q3"),
        ];
        assert_eq!(user_label_count(&labels), 2);
        assert!(has(&labels, SystemLabel::Received));
        assert!(!has(&labels, SystemLabel::Sent));
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
        assert_eq!(
            InboundVerdict::Spam.label(),
            Some(SystemLabel::Spam),
            "spam"
        );
        assert_eq!(
            InboundVerdict::Unauthenticated.label(),
            Some(SystemLabel::Unauthenticated)
        );
        assert_eq!(InboundVerdict::Clean.label(), None);
    }
}
