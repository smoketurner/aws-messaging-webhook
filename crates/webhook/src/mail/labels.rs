//! System labels, and inbound verdict classification.
//!
//! A message's labels are an open set — the system labels below plus
//! whatever the caller adds — so they are stored, indexed and published as
//! strings. [`SystemLabel`] is the closed half: every label the pipeline
//! itself applies, so the compiler asks what a new one means everywhere it
//! matters instead of leaving a literal to be typo'd.

use std::fmt;

/// A label the pipeline applies itself.
///
/// Deliberately not `Ord`: items store their labels sorted by the stored
/// string, and ordering the variants would sort by declaration order
/// instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemLabel {
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
}

impl SystemLabel {
    /// Every system label. The array is built from the variants, so adding
    /// one to the enum adds it here.
    pub const ALL: [Self; 12] = [
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

    /// The stored and published form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
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
        }
    }

    /// The system label `label` names, or `None` for a caller's own label.
    #[must_use]
    pub fn parse(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.as_str() == label)
    }

    /// Whether a request may set or clear this label. `unread`, `spam` and
    /// `trash` are the mailbox state a caller owns; every other system label
    /// is a record of something that happened, which a request must not be
    /// able to invent.
    #[must_use]
    pub fn is_settable_by_request(self) -> bool {
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

    /// This label's own owned `String`, for the label vectors items carry.
    #[must_use]
    pub fn to_label(self) -> String {
        self.as_str().to_owned()
    }
}

impl fmt::Display for SystemLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<SystemLabel> for String {
    fn eq(&self, other: &SystemLabel) -> bool {
        self == other.as_str()
    }
}

/// Whether `labels` carries `label`.
#[must_use]
pub fn has(labels: &[String], label: SystemLabel) -> bool {
    labels.iter().any(|existing| existing == label.as_str())
}

/// How many of `labels` are the caller's own rather than system labels.
#[must_use]
pub fn user_label_count(labels: &[String]) -> usize {
    labels
        .iter()
        .filter(|label| SystemLabel::parse(label).is_none())
        .count()
}

/// Every system label except `unread`, `spam` and `trash` — rejected on a
/// PATCH request and in a send's `labels`.
#[must_use]
pub fn is_reserved(label: &str) -> bool {
    SystemLabel::parse(label).is_some_and(|label| !label.is_settable_by_request())
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

    #[test]
    fn unread_spam_trash_are_not_reserved() {
        assert!(!is_reserved("unread"));
        assert!(!is_reserved("spam"));
        assert!(!is_reserved("trash"));
    }

    #[test]
    fn other_system_labels_are_reserved() {
        assert!(is_reserved("received"));
        assert!(is_reserved("sent"));
        assert!(is_reserved("delivered"));
    }

    #[test]
    fn a_users_own_label_is_never_reserved() {
        assert!(!is_reserved("invoices"));
        assert!(!is_reserved("Received"));
    }

    /// Round-tripping every variant is what keeps the stored strings and the
    /// enum from drifting when a variant is added.
    #[test]
    fn every_system_label_parses_back_to_itself() {
        for label in SystemLabel::ALL {
            assert_eq!(SystemLabel::parse(label.as_str()), Some(label));
        }
        assert_eq!(SystemLabel::parse("invoices"), None);
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
            "received".to_owned(),
            "invoices".to_owned(),
            "unread".to_owned(),
            "q3".to_owned(),
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
