//! The outbound send state: the item the sender claims, and the spec it
//! builds a message from.
//!
//! Sending is a transactional outbox. The API validates a request, uploads
//! its parts and a spec to the mail bucket, and commits a queued message plus
//! this state item; it never calls SES. A separate sender function consumes
//! the table's stream, claims the state item, and calls SES once.
//!
//! The split exists because `SESv2` `SendEmail` has no idempotency token and
//! the SDKs retry 5xx on their own, so a synchronous send could deliver the
//! same mail more than once. Here the claim is what makes a send happen at
//! most once.
//!
//! The state item is deliberately separate from the message item. The sender
//! addresses it by message id alone, without knowing the inbox, and updating
//! it during a send does not touch the message — so the relay sees at most
//! one message change per outcome rather than one per attempt.

use crate::time::Timestamp;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::InboxId;
use crate::labels::Label;
use crate::wire::ContentDisposition;
use crate::{MessageId, ThreadId};

/// Where a send has got to.
///
/// `sending` never reaches the message item's mirrored copy: it changes on
/// every attempt, and mirroring it would make the relay publish an event for
/// each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendStatus {
    Queued,
    Sending,
    Sent,
    Failed,
    /// SES may have accepted the message, or may not: the call was made and
    /// no usable answer came back. Never resent without an operator saying
    /// so, because the alternative risks sending twice.
    Unknown,
}

impl SendStatus {
    /// The stored form, identical to the serde name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Sending => "sending",
            Self::Sent => "sent",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

impl SendStatus {
    /// Whether a send in this state belongs in the `ByStatus` index. The
    /// index serves the sweep alone, which looks for sends stuck `sending`
    /// and for the `unknown` ones awaiting an operator; `queued` is there
    /// because a send is queued before any sender sees it, and only the
    /// settled states (`sent`, `failed`) are left out.
    #[must_use]
    pub fn is_swept(self) -> bool {
        match self {
            Self::Queued | Self::Sending | Self::Unknown => true,
            Self::Sent | Self::Failed => false,
        }
    }
}

/// The SES message tag every send carries its message id under. SES repeats
/// a message's tags on each event it publishes for it, which is how an event
/// names the send it belongs to even when SES's own id was never recorded.
pub const SES_MESSAGE_TAG: &str = "mailbox_message";

/// The real recipient lists, as given.
///
/// Kept apart from the message item's display lists, which are capped for
/// item size: truncating who a message is actually addressed to would send it
/// to the wrong people, so this is never shrunk.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cc: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bcc: Vec<String>,
}

impl Envelope {
    /// Every recipient across all three lists.
    #[must_use]
    pub(crate) fn recipient_count(&self) -> usize {
        self.to
            .len()
            .saturating_add(self.cc.len())
            .saturating_add(self.bcc.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.recipient_count() == 0
    }
}

/// Why a send stopped for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendFailure {
    /// The spec object is gone, so there is nothing to build from.
    SendSpecMissing,
    /// A URL-backed attachment could not be fetched, permanently.
    AttachmentFetchFailed,
    /// Repeated transient failures fetching an attachment.
    AttachmentFetchUnavailable,
    /// The object store refused the spec or a stored part (for example
    /// access denied), or stayed unavailable across every attempt.
    OutboxUnavailable,
    /// The assembled message exceeds what SES accepts.
    MessageTooLarge,
    /// The message could not be assembled from its spec and parts.
    BuildFailed,
    /// SES refused it: a bad address, an unverified identity, a paused
    /// account.
    Rejected,
    /// SES stayed unavailable across every attempt.
    SesUnavailable,
    /// An operator closed it by hand.
    ClosedByOperator,
    /// The send was claimed and abandoned by its sender too many times in a
    /// row; it never reached SES. A sender that is killed before it records
    /// `ses_call_at` leaves a stale claim the sweep releases, and after the
    /// transient-failure cap is exceeded that release would loop forever, so
    /// the sweep fails the send instead.
    SenderAbandoned,
}

impl SendFailure {
    /// The stored form, identical to the serde name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SendSpecMissing => "send_spec_missing",
            Self::AttachmentFetchFailed => "attachment_fetch_failed",
            Self::AttachmentFetchUnavailable => "attachment_fetch_unavailable",
            Self::OutboxUnavailable => "outbox_unavailable",
            Self::MessageTooLarge => "message_too_large",
            Self::BuildFailed => "build_failed",
            Self::Rejected => "rejected",
            Self::SesUnavailable => "ses_unavailable",
            Self::ClosedByOperator => "closed_by_operator",
            Self::SenderAbandoned => "sender_abandoned",
        }
    }
}

/// The `OUTBOX#<message_id>` / `STATE` item.
///
/// Mirrors the item's attributes one-to-one so it round-trips through
/// `serde_dynamo`, the same way [`crate::thread::ThreadState`] does.
/// It is never deleted explicitly. Once the send settles it takes its
/// message's TTL, so the two age out together; until then it has none, since
/// a queued, sending or unknown send must stay findable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendState {
    pub inbox_id: InboxId,
    pub message_id: MessageId,
    pub thread_id: ThreadId,
    pub send_status: SendStatus,
    pub version: u64,
    pub envelope: Envelope,
    /// When the current claim was taken. The sweep uses it to tell a live
    /// send from one whose sender died.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::time::fixed::option")]
    pub sending_at: Option<Timestamp>,
    /// When the sender, holding the current claim, was about to call SES.
    /// Written before the call, so a claim that went stale with this set may
    /// already have been sent, and the sweep treats it as `unknown` rather
    /// than sending it again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::time::fixed::option")]
    pub ses_call_at: Option<Timestamp>,
    /// Set by a release to hand the record back for another attempt; its
    /// absent-to-present transition is what re-triggers the sender.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::time::fixed::option")]
    pub requeued_at: Option<Timestamp>,
    /// Consecutive transient failures, so repeated unavailability eventually
    /// stops rather than looping forever.
    #[serde(default)]
    pub transient_failures: u32,
    /// When an operator resent this message by hand, as a breadcrumb on the
    /// item: nothing reads it, but a duplicate delivery is exactly the sort
    /// of thing someone later has to explain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::time::fixed::option")]
    pub operator_resend_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<SendFailure>,
    /// The `SENDKEY#` partition this send was committed under, recorded so
    /// an operator can trace a send back to the request that made it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_pk: Option<String>,
    #[serde(with = "crate::time::fixed")]
    pub created_at: Timestamp,
    #[serde(with = "crate::time::fixed")]
    pub updated_at: Timestamp,
    /// DynamoDB TTL (epoch seconds), set only once the send has settled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl SendState {
    /// The state a newly enqueued send starts in.
    #[must_use]
    pub fn queued(
        inbox_id: InboxId,
        message_id: MessageId,
        thread_id: ThreadId,
        envelope: Envelope,
        idempotency_key_pk: Option<String>,
        now: Timestamp,
    ) -> Self {
        Self {
            inbox_id,
            message_id,
            thread_id,
            send_status: SendStatus::Queued,
            version: 0,
            envelope,
            sending_at: None,
            ses_call_at: None,
            requeued_at: None,
            transient_failures: 0,
            operator_resend_at: None,
            failure: None,
            idempotency_key_pk,
            created_at: now,
            updated_at: now,
            expires_at: None,
        }
    }
}

/// The `SENDKEY#<sha256>` / `KEY` item: what an `Idempotency-Key` resolves
/// to once its request has been committed.
///
/// `request_hash` is what makes a replay safe to answer from here: the same
/// key with the same request returns the original ids, while the same key
/// with a *different* request is a client bug and is refused rather than
/// silently sending something else.
///
/// It expires, so a key is not remembered forever; `expires_at` is a TTL
/// timestamp in epoch seconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendKey {
    /// SHA-256 of the `Idempotency-Key` header value, which is what the item
    /// is keyed by. The header itself is never stored: it is a client secret
    /// in the sense that it must not be guessable by another caller.
    pub key_hash: String,
    pub inbox_id: InboxId,
    pub message_id: MessageId,
    pub thread_id: ThreadId,
    /// A fingerprint of the request body, so the same key presented with a
    /// *different* request is refused instead of silently sending something
    /// the caller did not ask for.
    pub request_hash: String,
    pub route: String,
    #[serde(with = "crate::time::fixed")]
    pub created_at: Timestamp,
    pub expires_at: u64,
}

/// One attachment as the spec records it: either bytes already uploaded to
/// the outbox, or a URL the sender fetches when it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecAttachment {
    pub attachment_id: String,
    /// Where the bytes live once uploaded. Absent for a URL-backed part that
    /// has not been fetched yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    /// Fetched by the sender, behind the SSRF guards, when `object_key` is
    /// absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    pub content_type: String,
    pub content_disposition: ContentDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
    /// Zero until the bytes exist.
    #[serde(default)]
    pub size: u64,
}

/// Everything the sender needs to build the MIME, stored as
/// `outbox/<message_id>/spec.json` rather than on the item, so a large
/// message does not have to fit in a table row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendSpec {
    pub message_id: MessageId,
    pub thread_id: ThreadId,
    pub inbox_id: InboxId,
    pub from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub envelope: Envelope,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reply_to: Vec<String>,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html: Option<String>,
    pub rfc_message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    /// Caller-supplied headers, already filtered: none of them can override a
    /// header the service controls.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<SpecAttachment>,
    #[serde(with = "crate::time::fixed")]
    pub created_at: Timestamp,
}

impl SendState {
    /// The state after a sender takes this record.
    ///
    /// Claiming is what makes a send happen at most once: the write is
    /// conditioned on the status still being `queued`, so of two senders
    /// looking at the same stream record, exactly one proceeds.
    #[must_use]
    pub fn claimed(&self, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Sending,
            version: self.version.saturating_add(1),
            sending_at: Some(now),
            ses_call_at: None,
            // Cleared so a later release can set it again and re-trigger the
            // sender through its absent-to-present transition.
            requeued_at: None,
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state just before the claimed send calls SES.
    #[must_use]
    pub fn calling_ses(&self, now: Timestamp) -> Self {
        Self {
            version: self.version.saturating_add(1),
            ses_call_at: Some(now),
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state after SES accepted the message.
    #[must_use]
    pub fn sent(&self, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Sent,
            version: self.version.saturating_add(1),
            sending_at: None,
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state after SES refused the message for good.
    #[must_use]
    pub fn failed(&self, failure: SendFailure, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Failed,
            version: self.version.saturating_add(1),
            sending_at: None,
            failure: Some(failure),
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state after a send whose outcome never came back.
    ///
    /// Nothing is cleaned up: the outbox objects stay, because an operator
    /// may later decide to resend, and that has to rebuild the same message.
    #[must_use]
    pub fn unknown(&self, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Unknown,
            version: self.version.saturating_add(1),
            sending_at: None,
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state after an operator asks for an `unknown` send to go out
    /// again.
    ///
    /// The transient-failure count resets: the operator has looked at this
    /// send and decided, so the attempts that led to `unknown` should not
    /// count against the fresh one.
    #[must_use]
    pub fn resumed(&self, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Queued,
            version: self.version.saturating_add(1),
            sending_at: None,
            ses_call_at: None,
            requeued_at: Some(now),
            transient_failures: 0,
            operator_resend_at: Some(now),
            updated_at: now,
            ..self.clone()
        }
    }

    /// The state after handing the record back for another attempt.
    #[must_use]
    pub fn released(&self, now: Timestamp) -> Self {
        Self {
            send_status: SendStatus::Queued,
            version: self.version.saturating_add(1),
            sending_at: None,
            ses_call_at: None,
            requeued_at: Some(now),
            transient_failures: self.transient_failures.saturating_add(1),
            updated_at: now,
            ..self.clone()
        }
    }
}

/// Labels present in `after` but not `before`, and the reverse: the patch
/// a status transition applies to the message's thread.
#[must_use]
pub fn label_changes(before: &[Label], after: &[Label]) -> (Vec<Label>, Vec<Label>) {
    let mut added = Vec::new();
    for label in after {
        if !before.contains(label) {
            added.push(label.clone());
        }
    }
    let mut removed = Vec::new();
    for label in before {
        if !after.contains(label) {
            removed.push(label.clone());
        }
    }
    (added, removed)
}

/// How a send finished, as the mailbox is told it when marking the send.
#[derive(Debug, Clone, Copy)]
pub enum MarkOutcome<'a> {
    /// SES accepted it, and gave back its own id.
    Sent(SesSent<'a>),
    /// It will not be sent.
    Failed(SendFailure),
    /// SES may or may not have it. The message keeps its `queued` label,
    /// because it is neither sent nor known to have failed.
    Unknown,
    /// Hand it back for another attempt.
    Released,
    /// An operator decided this send did go out, without SES telling us so.
    /// There is no SES id to record, which is the difference from `Sent`.
    ClosedSent,
    /// An operator asked for an `unknown` send to be attempted again,
    /// accepting the risk that SES already has it.
    Resumed,
}

impl MarkOutcome<'_> {
    /// Whether this outcome settles the send: the record takes its message's
    /// TTL and the message will never be assembled from its outbox again.
    ///
    /// Pairs one-to-one with the arms [`mark_transition`]
    /// wraps in `settled(..)`, and is what the sender keys outbox cleanup off:
    /// the `outbox/` prefix has no lifecycle rule, so a settled send that does
    /// not clear its own objects leaks them forever.
    #[must_use]
    pub fn settles(&self) -> bool {
        match self {
            Self::Sent(_) | Self::Failed(_) | Self::ClosedSent => true,
            // A send that is handed back, left for an operator, or resumed
            // may yet be assembled again, so it keeps its outbox.
            Self::Released | Self::Unknown | Self::Resumed => false,
        }
    }
}

/// What SES said about a message it accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SesSent<'a> {
    /// The SES message id.
    pub message_id: &'a str,
    /// The region the message was sent through, which SES puts in the
    /// `Message-ID` it writes over ours.
    pub region: &'a str,
}

/// Works out the new send state, the message's new labels, and the SES id to
/// record, for one outcome.
///
/// Used by the mailbox's send marking, so every store
/// agrees on what a given outcome means.
#[must_use]
pub fn mark_transition<'a>(
    state: &SendState,
    msg: &crate::MailMessage,
    outcome: MarkOutcome<'a>,
    now: Timestamp,
) -> (SendState, Vec<Label>, Option<SesSent<'a>>) {
    let relabel = |remove: Label, add: Label| {
        let mut labels: Vec<Label> = msg
            .labels
            .iter()
            .filter(|label| **label != remove)
            .cloned()
            .collect();
        if !labels.contains(&add) {
            labels.push(add);
        }
        labels.sort();
        labels
    };

    // A settled send ages out with its message.
    let settled = |next: SendState| SendState {
        expires_at: Some(msg.expires_at),
        ..next
    };

    match outcome {
        MarkOutcome::Sent(sent) => (
            settled(state.sent(now)),
            relabel(Label::Queued, Label::Sent),
            Some(sent),
        ),
        MarkOutcome::Failed(failure) => (
            settled(state.failed(failure, now)),
            relabel(Label::Queued, Label::Rejected),
            None,
        ),
        // Neither sent nor known to have failed, so the labels do not move:
        // claiming either would be a statement this service cannot support.
        MarkOutcome::Unknown => (state.unknown(now), msg.labels.clone(), None),
        MarkOutcome::Released => (state.released(now), msg.labels.clone(), None),
        // The operator is asserting the outcome SES never gave us, so the
        // message is labelled as if it had.
        MarkOutcome::ClosedSent => (
            settled(state.sent(now)),
            relabel(Label::Queued, Label::Sent),
            None,
        ),
        MarkOutcome::Resumed => (state.resumed(now), msg.labels.clone(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures;

    #[test]
    fn statuses_serialize_as_the_stored_strings() {
        for (status, expected) in [
            (SendStatus::Queued, "queued"),
            (SendStatus::Sending, "sending"),
            (SendStatus::Sent, "sent"),
            (SendStatus::Failed, "failed"),
            (SendStatus::Unknown, "unknown"),
        ] {
            assert_eq!(status.as_str(), expected);
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{expected}\"")
            );
        }
    }

    #[test]
    fn failures_round_trip_as_the_stored_strings() {
        for (failure, wire) in [
            (SendFailure::SendSpecMissing, "send_spec_missing"),
            (
                SendFailure::AttachmentFetchFailed,
                "attachment_fetch_failed",
            ),
            (
                SendFailure::AttachmentFetchUnavailable,
                "attachment_fetch_unavailable",
            ),
            (SendFailure::OutboxUnavailable, "outbox_unavailable"),
            (SendFailure::MessageTooLarge, "message_too_large"),
            (SendFailure::BuildFailed, "build_failed"),
            (SendFailure::Rejected, "rejected"),
            (SendFailure::SesUnavailable, "ses_unavailable"),
            (SendFailure::ClosedByOperator, "closed_by_operator"),
            (SendFailure::SenderAbandoned, "sender_abandoned"),
        ] {
            assert_eq!(failure.as_str(), wire);
            assert_eq!(
                serde_json::to_value(failure).unwrap(),
                serde_json::json!(wire)
            );
            let back: SendFailure = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(back, failure);
        }
    }

    #[test]
    fn a_queued_state_starts_at_version_zero_with_no_claim() {
        let state = SendState::queued(
            InboxId::new("support@example.com"),
            MessageId::new("mid-1"),
            ThreadId::new("tid-1"),
            Envelope {
                to: vec!["a@example.com".to_owned()],
                ..Envelope::default()
            },
            None,
            test_fixtures::at("2026-01-01T00:00:00.000Z"),
        );
        assert_eq!(state.send_status, SendStatus::Queued);
        assert_eq!(state.version, 0);
        assert_eq!(state.transient_failures, 0);
        assert!(state.sending_at.is_none());
        assert!(state.requeued_at.is_none());
    }

    #[test]
    fn a_send_state_round_trips_through_the_item_encoding() {
        let state = SendState::queued(
            InboxId::new("support@example.com"),
            MessageId::new("mid-1"),
            ThreadId::new("tid-1"),
            Envelope {
                to: vec!["a@example.com".to_owned()],
                cc: vec!["b@example.com".to_owned()],
                bcc: Vec::new(),
            },
            Some("SENDKEY#abc".to_owned()),
            test_fixtures::at("2026-01-01T00:00:00.000Z"),
        );
        let item: serde_dynamo::Item = serde_dynamo::to_item(&state).unwrap();
        let back: SendState = serde_dynamo::from_item(item).unwrap();
        assert_eq!(back, state);
    }

    #[test]
    fn envelope_counts_every_list() {
        let envelope = Envelope {
            to: vec!["a@example.com".to_owned(), "b@example.com".to_owned()],
            cc: vec!["c@example.com".to_owned()],
            bcc: vec!["d@example.com".to_owned()],
        };
        assert_eq!(envelope.recipient_count(), 4);
        assert!(!envelope.is_empty());
        assert!(Envelope::default().is_empty());
    }

    /// The pairing this predicate exists for: the outcomes
    /// [`mark_transition`] wraps in `settled(..)` are
    /// exactly the ones whose outbox the sender clears. Adding an outcome
    /// without deciding which side it falls on is how the terminal-failure,
    /// sweep-abandon and operator-close paths each came to leak.
    #[test]
    fn only_the_outcomes_that_take_a_ttl_settle() {
        let sent = SesSent {
            message_id: "0100-ses",
            region: "us-east-1",
        };
        assert!(MarkOutcome::Sent(sent).settles());
        assert!(MarkOutcome::Failed(SendFailure::Rejected).settles());
        assert!(MarkOutcome::ClosedSent.settles());

        assert!(!MarkOutcome::Released.settles());
        assert!(!MarkOutcome::Unknown.settles());
        assert!(!MarkOutcome::Resumed.settles());
    }
}
