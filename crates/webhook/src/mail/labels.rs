//! System/reserved label sets and inbound verdict classification (D16, D26).

/// The 12 system labels (D26): applied by the pipeline itself, never
/// directly settable by a PATCH request (except `unread`/`spam`/`trash`,
/// which are user-toggleable).
pub const SYSTEM_LABELS: [&str; 12] = [
    "received",
    "sent",
    "queued",
    "unread",
    "spam",
    "trash",
    "unauthenticated",
    "delivered",
    "bounced",
    "complained",
    "rejected",
    "opened",
];

/// Every system label except `unread`, `spam` and `trash` — rejected on a
/// PATCH request and in a send's `labels` (D26).
#[must_use]
pub fn is_reserved(label: &str) -> bool {
    SYSTEM_LABELS.contains(&label) && !matches!(label, "unread" | "spam" | "trash")
}

/// The inbound classification label an ingested message gets beyond
/// `received`/`unread` (D16): spam takes precedence over an authentication
/// failure, which takes precedence over neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundVerdict {
    Spam,
    Unauthenticated,
    Clean,
}

/// Classifies an inbound receipt's verdicts (D16): spam/virus `FAIL` → spam
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
    fn user_labels_are_not_reserved() {
        assert!(!is_reserved("invoices"));
        assert!(!is_reserved("customer-a"));
    }

    #[test]
    fn classify_inbound_verdict_matrix() {
        // Precedence (D16): spam > unauthenticated > clean, over every
        // combination of the two inputs.
        assert_eq!(classify_inbound(false, false), InboundVerdict::Clean);
        assert_eq!(
            classify_inbound(false, true),
            InboundVerdict::Unauthenticated
        );
        assert_eq!(classify_inbound(true, false), InboundVerdict::Spam);
        assert_eq!(classify_inbound(true, true), InboundVerdict::Spam);
    }
}
