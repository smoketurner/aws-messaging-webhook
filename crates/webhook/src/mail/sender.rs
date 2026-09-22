//! The sender: turning one queued send into one SES call.
//!
//! Runs in the same binary under `FUNCTION_MODE=sender`, driven by the mail
//! table's stream. Each record names a message; this module claims it, builds
//! it, sends it once, and records what happened.
//!
//! The ordering is what makes a send happen at most once:
//!
//! 1. **Claim** — a conditional write moves `queued` to `sending`. Two
//!    senders handed the same record race here, and exactly one wins. The
//!    loser stops, which is a normal outcome and not an error.
//! 2. **Assemble** — read the spec and every part. A message is never sent
//!    with some of its attachments missing.
//! 3. **Send** — one SES call, with the SDK's own retries off.
//! 4. **Mark** — record the outcome, which is the only write that touches the
//!    message item, so the relay publishes one event per send rather than one
//!    per attempt.
//!
//! When the outcome is [`SendOutcome::Unknown`], nothing is retried. SES may
//! already hold the message, and sending it again is worse than leaving it
//! for an operator to resolve.

use std::time::Duration;

use tokio::time::Instant;

use crate::actions::{RawSend, SendOutcome};
use crate::mail::build::{BuildError, BuiltPart, build_outbound};
use crate::mail::fetch::{AttachmentFetcher, FetchError};
use crate::mail::objects::ObjectError;
use crate::mail::send::{self, SendFailure, SendSpec, SendState, SendStatus};
use crate::mail::store::{MailStoreError, MarkOutcome, SesSent};
use crate::mail::url_policy;
use crate::mail::{MAX_OUTBOUND_DECODED_BYTES, MAX_OUTBOUND_RAW_BYTES, time};
use crate::metrics::names;
use crate::state::{AppState, Services};

/// What one stream record came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// The message reached SES and was recorded as sent.
    Sent,
    /// It will not be sent, and that is recorded.
    Failed,
    /// The outcome is genuinely unknown; an operator decides.
    Unknown,
    /// Nothing to do: another sender holds it, or it is already finished.
    Skipped,
}

/// Anything that should make the record be retried by the event source.
///
/// Only genuinely transient store and object failures reach here. A message
/// that cannot be built, or that SES refuses, is recorded as failed and the
/// record is settled: retrying it would fail the same way forever.
#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error("the mail store was unavailable")]
    Store(#[source] anyhow::Error),
    #[error("mail is not configured")]
    NotConfigured,
}

/// Drives one batch of mail-table stream records.
///
/// Only send-state items matter here, and only two transitions on them: a new
/// queued send, and a release, which is visible as `requeued_at` appearing.
/// Everything else on this stream — messages, threads, aliases — belongs to
/// the relay running in the other mode.
///
/// Failures are reported per record through `batchItemFailures`, so one
/// unavailable dependency does not force the whole batch to be redelivered
/// and re-attempted.
///
/// # Errors
///
/// Only when the payload is not a stream event at all.
pub async fn handle_sender_stream<T: Services>(
    state: &AppState<T>,
    payload: serde_json::Value,
    deadline: Instant,
) -> Result<serde_json::Value, lambda_http::Error> {
    let event: aws_lambda_events::dynamodb::Event = serde_json::from_value(payload)
        .map_err(|e| format!("payload has Records but is not a DynamoDB stream event: {e}"))?;

    let mut failures = Vec::new();
    for record in event.records {
        let Some(message_id) = queued_send_id(&record) else {
            continue;
        };
        if let Err(error) = handle_send(state, &message_id, deadline).await {
            tracing::error!(
                message_id,
                error = ?error,
                event = "send_record_failed",
                "returning this record for redelivery"
            );
            if let Some(sequence_number) = record.change.sequence_number.clone() {
                failures.push(serde_json::json!({ "itemIdentifier": sequence_number }));
            }
        }
    }
    Ok(serde_json::json!({ "batchItemFailures": failures }))
}

/// The message id a record is asking to be sent, if it is asking at all.
fn queued_send_id(record: &aws_lambda_events::dynamodb::EventRecord) -> Option<String> {
    let new_image = &record.change.new_image;
    if crate::stream::image_str(new_image, "sk")? != "STATE" {
        return None;
    }
    if crate::stream::image_str(new_image, "send_status")? != SendStatus::Queued.as_str() {
        return None;
    }

    let message_id = crate::stream::image_str(new_image, "pk")?
        .strip_prefix("OUTBOX#")?
        .to_owned();

    match record.event_name.as_str() {
        "INSERT" => Some(message_id),
        // A release is the only MODIFY worth acting on, and it is visible as
        // `requeued_at` appearing. Acting on every MODIFY would re-enter the
        // sender on its own claim and mark writes.
        "MODIFY" => {
            let was_absent =
                crate::stream::image_str(&record.change.old_image, "requeued_at").is_none();
            let is_present = crate::stream::image_str(new_image, "requeued_at").is_some();
            (was_absent && is_present).then_some(message_id)
        }
        _ => None,
    }
}

/// An operator instruction, sent by invoking the sender function directly.
///
/// Deliberately not an HTTP route: these are rare, destructive and
/// account-scoped, so `lambda:InvokeFunction` is a better gate than a bearer
/// key, and the public API surface stays the contract it mirrors.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    /// Send an `unknown` message again.
    Resend { message_id: String },
    /// Record an `unknown` message as having gone out.
    CloseSent { message_id: String },
    /// Record an `unknown` message as not having gone out.
    CloseFailed { message_id: String },
}

impl Command {
    fn message_id(&self) -> &str {
        match self {
            Self::Resend { message_id }
            | Self::CloseSent { message_id }
            | Self::CloseFailed { message_id } => message_id,
        }
    }

    fn resolution(&self) -> Resolution {
        match self {
            Self::Resend { .. } => Resolution::Resend,
            Self::CloseSent { .. } => Resolution::CloseSent,
            Self::CloseFailed { .. } => Resolution::CloseFailed,
        }
    }
}

/// Runs one operator instruction.
///
/// # Errors
///
/// [`SenderError`] when the send is missing, is not `unknown`, or the store
/// cannot be written.
pub async fn handle_command<T: Services>(
    state: &AppState<T>,
    command: &Command,
) -> Result<(), SenderError> {
    resolve_unknown(state, command.message_id(), command.resolution()).await
}

/// What an operator decided about a send whose outcome SES never confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// Send it again, accepting that SES may already have it.
    Resend,
    /// It did go out; record it as sent without sending anything.
    CloseSent,
    /// It did not go out; record it as failed.
    CloseFailed,
}

/// Applies an operator's decision to a send stuck in `unknown`.
///
/// This is the one place that can move a send out of `unknown`, and it exists
/// because nothing automatic should: SES may hold the message, so resending
/// risks delivering it twice and closing it asserts an outcome this service
/// never observed. Both are judgements only a person can make.
///
/// # Errors
///
/// [`SenderError::Store`] when the send does not exist or is not `unknown` —
/// resolving a send that is still in flight would race the sender holding it.
pub async fn resolve_unknown<T: Services>(
    state: &AppState<T>,
    message_id: &str,
    resolution: Resolution,
) -> Result<(), SenderError> {
    let now = time::format(time::now_ms());
    let Some(send) = state
        .services
        .get_send_state(message_id)
        .await
        .map_err(store_error)?
    else {
        return Err(SenderError::Store(anyhow::anyhow!(
            "no send state for {message_id}"
        )));
    };
    if send.send_status != SendStatus::Unknown {
        return Err(SenderError::Store(anyhow::anyhow!(
            "send {message_id} is {}, not unknown; only an unknown send can be resolved",
            send.send_status.as_str()
        )));
    }

    let outcome = match resolution {
        Resolution::Resend => MarkOutcome::Resumed,
        Resolution::CloseSent => MarkOutcome::ClosedSent,
        Resolution::CloseFailed => MarkOutcome::Failed(SendFailure::ClosedByOperator),
    };
    record(state, &send, outcome, &now)
        .await
        .map_err(store_error)?;

    tracing::warn!(
        message_id,
        resolution = ?resolution,
        event = "send_resolved_by_operator",
        "an operator resolved a send whose outcome SES never confirmed"
    );
    Ok(())
}

/// How long a send may sit claimed before the sweep assumes its sender died.
///
/// Comfortably longer than the sender's own timeout, so a send that is merely
/// slow is never taken away from a sender that is still working on it — doing
/// that is how the same message gets sent twice.
const CLAIM_STALE_AFTER_MS: u64 = 15 * 60 * 1_000;

/// How many stuck sends one sweep looks at.
const SWEEP_LIMIT: usize = 100;

/// Time kept back from the invocation deadline for calling SES and recording
/// its answer: loading a send stops early enough to leave it. Covers
/// [`crate::aws::SES_CALL_TIMEOUT`] plus a few store writes.
const SEND_RESERVE: Duration = Duration::from_secs(35);

/// Time kept back from the deadline when waiting before a hand-back, so the
/// release itself still has room to run.
const RELEASE_RESERVE: Duration = Duration::from_secs(5);

/// How many times a send may be handed back before it is abandoned.
const MAX_TRANSIENT_FAILURES: u32 = 5;

/// How many times recording a send SES accepted is attempted before giving
/// up and leaving it to the sweep.
const MARK_SENT_ATTEMPTS: u32 = 3;

/// What one sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Claims released because the sender holding them appears to be gone
    /// before it called SES, and the transient-failure cap was not yet
    /// reached.
    pub released: usize,
    /// Claims whose sender repeatedly vanished before it called SES, past
    /// the transient-failure cap. Failed so they stop looping and age out,
    /// as `hand_back` does for the live retry path.
    pub abandoned: usize,
    /// Claims whose sender appears to be gone after it was about to call
    /// SES, moved to `unknown` because the message may have gone out.
    pub marked_unknown: usize,
    /// Claims left alone because they are still plausibly live.
    pub still_working: usize,
    /// Stuck sends the sweep could not update; the next sweep tries again.
    pub errors: usize,
    /// Sends whose outcome is unknown. Reported, never touched.
    pub unknown: usize,
}

/// Recovers sends whose sender died mid-flight.
///
/// A Lambda can be killed between claiming a send and recording its outcome,
/// which leaves the state item `sending` with nobody working on it and no
/// stream record to re-trigger. Nothing else would ever notice.
///
/// A stale claim is released only when its sender never got as far as
/// calling SES. One whose sender recorded that it was about to call SES may
/// already have been delivered, so it becomes `unknown` for an operator:
/// releasing it could send the message twice. A release that never reaches
/// SES bumps `transient_failures`, and once that would exceed
/// [`MAX_TRANSIENT_FAILURES`] the send is failed instead of requeued — the
/// same cap [`hand_back`] enforces on the live retry path — so a send whose
/// sender is repeatedly killed during load cannot loop forever.
///
/// Sends in `unknown` are counted and left alone. A stuck send that cannot be
/// updated — its state moved on since the index was read, or the store is
/// briefly unavailable — is logged and skipped, so one does not stop the
/// rest from being recovered.
///
/// # Errors
///
/// [`SenderError`] when the store cannot be listed.
pub async fn sweep<T: Services>(state: &AppState<T>) -> Result<SweepReport, SenderError> {
    let now_ms = time::now_ms();
    let now = time::format(now_ms);
    let mut report = SweepReport::default();

    for stuck in state
        .services
        .list_by_status(SendStatus::Sending, SWEEP_LIMIT)
        .await
        .map_err(store_error)?
    {
        let claimed_ms = stuck
            .sending_at
            .as_deref()
            .and_then(time::parse)
            .unwrap_or(now_ms);
        if now_ms.saturating_sub(claimed_ms) < CLAIM_STALE_AFTER_MS {
            report.still_working += 1;
            continue;
        }

        let result = if stuck.ses_call_at.is_some() {
            tracing::error!(
                message_id = %stuck.message_id,
                ses_call_at = stuck.ses_call_at.as_deref().unwrap_or_default(),
                event = "send_claim_stale_after_ses_call",
                "a sender died after it was about to call SES; the message may have gone out"
            );
            record(state, &stuck, MarkOutcome::Unknown, &now)
                .await
                .map(|()| report.marked_unknown += 1)
        } else if stuck.transient_failures + 1 >= MAX_TRANSIENT_FAILURES {
            // The sender vanished before it called SES, and this is not the
            // first time: a release would requeue and the next sweep would
            // release again, looping forever. `hand_back` enforces this same
            // cap on the live retry path; the sweep must too, or a poison
            // message that kills every sender during load churns indefinitely.
            tracing::warn!(
                message_id = %stuck.message_id,
                transient_failures = stuck.transient_failures,
                event = "send_claim_abandoned",
                "a send has been abandoned past the failure cap; failing it"
            );
            state
                .services
                .mark_send(
                    &stuck,
                    MarkOutcome::Failed(SendFailure::SenderAbandoned),
                    &now,
                )
                .await
                .map(|()| {
                    report.abandoned += 1;
                    metrics::counter!(names::SEND_FAILURES).increment(1);
                })
        } else {
            tracing::warn!(
                message_id = %stuck.message_id,
                sending_at = stuck.sending_at.as_deref().unwrap_or("unknown"),
                event = "send_claim_released",
                "releasing a claim whose sender appears to be gone"
            );
            record(state, &stuck, MarkOutcome::Released, &now)
                .await
                .map(|()| report.released += 1)
        };
        if let Err(error) = result {
            report.errors += 1;
            tracing::warn!(
                message_id = %stuck.message_id,
                error = ?error,
                event = "sweep_update_failed",
                "could not recover a stuck send; the next sweep will try again"
            );
        }
    }

    report.unknown = state
        .services
        .list_by_status(SendStatus::Unknown, SWEEP_LIMIT)
        .await
        .map_err(store_error)?
        .len();
    if report.unknown > 0 {
        tracing::warn!(
            count = report.unknown,
            event = "sends_outcome_unknown",
            "sends whose outcome SES never confirmed; an operator must resolve them"
        );
    }

    Ok(report)
}

/// Claims `message_id`, sends it, and records the outcome.
///
/// `deadline` is when the invocation ends. Loading the send must finish
/// [`SEND_RESERVE`] before it, which is what keeps a slow attachment from
/// getting the invocation killed while it holds the claim.
///
/// # Errors
///
/// [`SenderError`] only when the store could not record where the send got
/// to; every other outcome, including a hand-back for another attempt, is
/// recorded and returns `Ok`.
pub async fn handle_send<T: Services>(
    state: &AppState<T>,
    message_id: &str,
    deadline: Instant,
) -> Result<Handled, SenderError> {
    let config = state
        .config
        .mail
        .as_ref()
        .ok_or(SenderError::NotConfigured)?;
    let now = time::format(time::now_ms());

    let Some(claimed) = state
        .services
        .claim_send(message_id, &now)
        .await
        .map_err(store_error)?
    else {
        tracing::debug!(
            message_id,
            event = "send_not_claimed",
            "another sender holds this send, or it is already finished"
        );
        return Ok(Handled::Skipped);
    };

    // From here the claim is held, so every path must record an outcome.
    let load_by = deadline
        .checked_sub(SEND_RESERVE)
        .unwrap_or_else(Instant::now);
    let (spec, parts) = match tokio::time::timeout_at(load_by, load(state, message_id)).await {
        Ok(Ok(loaded)) => loaded,
        Ok(Err(LoadError::Permanent(failure))) => {
            return fail(state, &claimed, failure, &now).await;
        }
        Ok(Err(LoadError::Transient { failure, error })) => {
            tracing::warn!(
                message_id,
                error = ?error,
                event = "send_load_unavailable",
                "could not load the send; handing it back"
            );
            return hand_back(state, &claimed, failure, &now, deadline).await;
        }
        Err(_elapsed) => {
            tracing::warn!(
                message_id,
                event = "send_load_timed_out",
                "loading the send did not finish in the time available; handing it back"
            );
            return hand_back(
                state,
                &claimed,
                SendFailure::AttachmentFetchUnavailable,
                &now,
                deadline,
            )
            .await;
        }
    };

    let built = match build(&spec, &parts) {
        Ok(built) => built,
        Err(failure) => return fail(state, &claimed, failure, &now).await,
    };

    // Recorded before the call, so a sender that dies from here on leaves a
    // claim the sweep will not release: it may already have been sent.
    let Some(calling) = state
        .services
        .note_ses_call(&claimed, &now)
        .await
        .map_err(store_error)?
    else {
        tracing::warn!(
            message_id,
            event = "send_claim_lost",
            "the claim moved on before SES was called; not sending"
        );
        return Ok(Handled::Skipped);
    };

    let outcome = state
        .services
        .send_raw(&RawSend {
            raw: &built,
            from: &spec.from,
            to: &spec.envelope.to,
            cc: &spec.envelope.cc,
            bcc: &spec.envelope.bcc,
            configuration_set: &config.configuration_set,
            identity_arn: &config.identity_arn,
            message_id,
        })
        .await;

    record_outcome(state, &calling, outcome, &now, deadline).await
}

/// Assembles the raw message, or the failure a send that cannot be built
/// comes to.
fn build(spec: &SendSpec, parts: &[LoadedPart]) -> Result<Vec<u8>, SendFailure> {
    let message_id = spec.message_id.as_str();
    build_outbound(
        spec,
        &parts.iter().map(part_ref).collect::<Vec<_>>(),
        MAX_OUTBOUND_RAW_BYTES,
    )
    .map_err(|error| match error {
        BuildError::TooLarge { size, limit } => {
            tracing::warn!(
                message_id,
                size,
                limit,
                event = "send_too_large",
                "assembled message exceeds what SES accepts"
            );
            SendFailure::MessageTooLarge
        }
        BuildError::Failed(error) => {
            tracing::error!(
                message_id,
                error = ?error,
                event = "send_build_failed",
                "could not assemble the message"
            );
            SendFailure::BuildFailed
        }
    })
}

/// Records what SES said, which is the only write that touches the message.
async fn record_outcome<T: Services>(
    state: &AppState<T>,
    calling: &SendState,
    outcome: SendOutcome,
    now: &str,
    deadline: Instant,
) -> Result<Handled, SenderError> {
    let message_id = calling.message_id.as_str();
    match outcome {
        SendOutcome::Sent { ses_message_id } => {
            mark_sent(state, calling, &ses_message_id, now, deadline).await?;
            metrics::counter!(names::MESSAGES_SENT).increment(1);
            tracing::info!(
                message_id,
                ses_message_id,
                inbox_id = %calling.inbox_id.as_str(),
                event = "message_sent",
                "sent"
            );
            clear_outbox(state, calling).await;
            Ok(Handled::Sent)
        }
        SendOutcome::Failed { reason } => {
            tracing::warn!(
                message_id,
                reason,
                event = "send_rejected",
                "SES refused the message"
            );
            fail(state, calling, SendFailure::Rejected, now).await
        }
        SendOutcome::Retryable { reason } => {
            tracing::warn!(
                message_id,
                reason,
                event = "send_retryable",
                "SES did not take the request; handing it back for another attempt"
            );
            hand_back(state, calling, SendFailure::SesUnavailable, now, deadline).await
        }
        SendOutcome::Unknown { reason } => {
            // Deliberately terminal. SES may hold the message; sending it
            // again could deliver it twice, so this waits for an operator.
            tracing::error!(
                message_id,
                reason,
                event = "send_outcome_unknown",
                "SES may or may not have accepted this message; not resending"
            );
            record(state, calling, MarkOutcome::Unknown, now)
                .await
                .map_err(store_error)?;
            metrics::counter!(names::SEND_OUTCOME_UNKNOWN).increment(1);
            Ok(Handled::Unknown)
        }
    }
}

/// Records that SES accepted the message, retrying briefly.
///
/// SES already has the message, so this is worth trying hard: if it cannot
/// be recorded, the claim is left carrying its SES-call mark, and the sweep
/// moves it to `unknown` rather than sending it again.
async fn mark_sent<T: Services>(
    state: &AppState<T>,
    calling: &SendState,
    ses_message_id: &str,
    now: &str,
    deadline: Instant,
) -> Result<(), SenderError> {
    let config = state
        .config
        .mail
        .as_ref()
        .ok_or(SenderError::NotConfigured)?;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = state
            .services
            .mark_send(
                calling,
                MarkOutcome::Sent(SesSent {
                    message_id: ses_message_id,
                    region: &config.region,
                }),
                now,
            )
            .await;
        let error = match result {
            Ok(()) => return Ok(()),
            // Another writer moved this send on; retrying cannot help.
            Err(error @ MailStoreError::Conflict) => error,
            Err(error) if attempt >= MARK_SENT_ATTEMPTS => error,
            Err(error) => {
                tracing::warn!(
                    message_id = %calling.message_id,
                    attempt,
                    error = ?error,
                    event = "send_mark_retrying",
                    "SES accepted the message but recording it failed; retrying"
                );
                wait_before_retry(Duration::from_secs(u64::from(attempt)), deadline).await;
                continue;
            }
        };
        tracing::error!(
            message_id = %calling.message_id,
            ses_message_id,
            error = ?error,
            event = "send_mark_failed",
            "SES accepted the message but it could not be recorded; the sweep will mark it unknown"
        );
        return Err(store_error(error));
    }
}

/// Hands a send back for another attempt after a transient failure, waiting
/// first so the next attempt is not immediate, or records `failure` once it
/// has been handed back too often.
///
/// Returns `Ok`: the release sets `requeued_at`, which is itself the stream
/// record that re-triggers the sender. Also failing the invocation would
/// redeliver the original record on top, multiplying attempts.
async fn hand_back<T: Services>(
    state: &AppState<T>,
    claimed: &SendState,
    failure: SendFailure,
    now: &str,
    deadline: Instant,
) -> Result<Handled, SenderError> {
    if claimed.transient_failures + 1 >= MAX_TRANSIENT_FAILURES {
        return fail(state, claimed, failure, now).await;
    }
    let delay = Duration::from_secs(1 << claimed.transient_failures.min(4));
    wait_before_retry(delay, deadline).await;
    record(state, claimed, MarkOutcome::Released, now)
        .await
        .map_err(store_error)?;
    Ok(Handled::Skipped)
}

/// Sleeps for `delay`, cut short so [`RELEASE_RESERVE`] of the invocation
/// remains.
async fn wait_before_retry(delay: Duration, deadline: Instant) {
    let latest = deadline
        .checked_sub(RELEASE_RESERVE)
        .unwrap_or_else(Instant::now);
    tokio::time::sleep_until((Instant::now() + delay).min(latest)).await;
}

/// Records an outcome for a send and disposes of what that outcome makes
/// dead.
///
/// The sender's one way to call [`Services::mark_send`], because the write and
/// the cleanup belong together: any outcome [`MarkOutcome::settles`] leaves a
/// send that will never be assembled again, and `outbox/` has no lifecycle
/// rule, so objects not cleared here are leaked for good. Routing every record
/// through one place is what stops a new settling call site from leaking the
/// way the terminal-failure and operator-close paths each did.
///
/// The one settling write that does not come through here is `Sent`: SES
/// already has the message, so [`mark_sent`] retries that write, and
/// [`record_outcome`] pairs it with the same [`clear_outbox`] call.
///
/// Cleanup is best effort and never reported: only the store write can fail
/// the caller.
async fn record<T: Services>(
    state: &AppState<T>,
    send: &SendState,
    outcome: MarkOutcome<'_>,
    now: &str,
) -> Result<(), MailStoreError> {
    state.services.mark_send(send, outcome, now).await?;
    if outcome.settles() {
        clear_outbox(state, send).await;
    }
    Ok(())
}

/// Removes the outbox objects for a send whose outcome has settled.
///
/// Best effort, and deliberately after the outcome is recorded: the send has
/// already happened, so a failure here must not undo it or make the record be
/// retried. The `outbox/` prefix has no bucket lifecycle rule, so anything
/// left behind would never expire — a settled send must clear its own
/// objects, because nothing else reaps them.
///
/// Called for every outcome [`MarkOutcome::settles`] reports: a `Sent` send, a
/// terminal `Failed` one from any path, and a send an operator closes. None
/// of them will be assembled again, so none of them needs its outbox. A send
/// in `unknown`, released or resumed keeps its objects, because it may yet go
/// out.
async fn clear_outbox<T: Services>(state: &AppState<T>, finished: &SendState) {
    let message_id = &finished.message_id;
    let mut keys = vec![send::spec_key(message_id)];

    // The spec names the parts, so it is read before it is removed.
    if let Ok(bytes) = state
        .services
        .get_object(&send::spec_key(message_id), MAX_OUTBOUND_RAW_BYTES)
        .await
        && let Ok(spec) = serde_json::from_slice::<SendSpec>(&bytes)
    {
        for attachment in &spec.attachments {
            keys.push(send::part_key(message_id, &attachment.attachment_id));
        }
    }

    for key in keys {
        if let Err(error) = state.services.delete_object(&key).await {
            tracing::warn!(
                message_id,
                key,
                error = ?error,
                event = "outbox_cleanup_failed",
                "could not remove an outbox object; the `outbox/` prefix has no retention rule, so it will need manual cleanup"
            );
        }
    }
}

/// Why a send could not be loaded.
enum LoadError {
    /// A retry would fail the same way: record this failure.
    Permanent(SendFailure),
    /// A retry may succeed: hand the send back, and record `failure` once it
    /// has been handed back too often.
    Transient {
        failure: SendFailure,
        error: anyhow::Error,
    },
}

/// Classifies an object-store failure while loading `key`. `missing` is the
/// failure a key that is not there comes to.
fn object_load_error(
    message_id: &str,
    key: &str,
    error: ObjectError,
    missing: SendFailure,
) -> LoadError {
    match error {
        ObjectError::NotFound => LoadError::Permanent(missing),
        ObjectError::Transient(source) => LoadError::Transient {
            failure: SendFailure::OutboxUnavailable,
            error: source,
        },
        // Access denied or an object over the limit: the same answer every
        // time, so retrying would only loop.
        error @ (ObjectError::Permanent(_) | ObjectError::TooLarge { .. }) => {
            tracing::error!(
                message_id,
                key,
                error = ?error,
                event = "send_outbox_refused",
                "the object store refused an outbox object"
            );
            LoadError::Permanent(SendFailure::OutboxUnavailable)
        }
    }
}

/// Reads the spec and every part it names.
async fn load<T: Services>(
    state: &AppState<T>,
    message_id: &str,
) -> Result<(SendSpec, Vec<LoadedPart>), LoadError> {
    let spec = load_spec(state, message_id).await?;
    let parts = load_parts(state, &spec).await?;
    Ok((spec, parts))
}

async fn load_spec<T: Services>(
    state: &AppState<T>,
    message_id: &str,
) -> Result<SendSpec, LoadError> {
    let key = send::spec_key(message_id);
    let bytes = state
        .services
        .get_object(&key, MAX_OUTBOUND_RAW_BYTES)
        .await
        .map_err(|error| {
            object_load_error(message_id, &key, error, SendFailure::SendSpecMissing)
        })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        // A spec that will not parse will not parse on a retry either.
        tracing::error!(message_id, error = ?e, event = "send_spec_unreadable", "unreadable spec");
        LoadError::Permanent(SendFailure::SendSpecMissing)
    })
}

/// One attachment's bytes, owned so the borrow checker does not tie them to
/// the loop that fetched them.
struct LoadedPart {
    spec: crate::mail::send::SpecAttachment,
    bytes: Vec<u8>,
}

fn part_ref(part: &LoadedPart) -> BuiltPart<'_> {
    BuiltPart {
        spec: &part.spec,
        bytes: &part.bytes,
    }
}

/// Reads every attachment named by the spec: bytes already in the outbox, or
/// a URL fetched once and stored so a retry reuses it.
async fn load_parts<T: Services>(
    state: &AppState<T>,
    spec: &SendSpec,
) -> Result<Vec<LoadedPart>, LoadError> {
    let message_id = spec.message_id.as_str();
    let mut parts = Vec::with_capacity(spec.attachments.len());
    let mut budget = MAX_OUTBOUND_DECODED_BYTES;

    for attachment in &spec.attachments {
        // Bytes already in the outbox: either inlined at enqueue, or fetched
        // by an earlier attempt at this send, which is what stops a retry
        // re-fetching everything.
        if let Some(key) = &attachment.object_key {
            let bytes = state
                .services
                .get_object(key, MAX_OUTBOUND_RAW_BYTES)
                .await
                .map_err(|error| {
                    object_load_error(message_id, key, error, SendFailure::AttachmentFetchFailed)
                })?;
            budget = budget.saturating_sub(bytes.len() as u64);
            parts.push(LoadedPart {
                spec: attachment.clone(),
                bytes: bytes.to_vec(),
            });
            continue;
        }

        // An earlier attempt may already have fetched and stored it. The
        // spec is written once at enqueue and never rewritten, so the stored
        // object is the only record that the fetch happened.
        let stored_key = send::part_key(message_id, &attachment.attachment_id);
        match state
            .services
            .get_object(&stored_key, MAX_OUTBOUND_RAW_BYTES)
            .await
        {
            Ok(bytes) => {
                budget = budget.saturating_sub(bytes.len() as u64);
                let mut spec_attachment = attachment.clone();
                spec_attachment.object_key = Some(stored_key);
                spec_attachment.size = bytes.len() as u64;
                parts.push(LoadedPart {
                    spec: spec_attachment,
                    bytes: bytes.to_vec(),
                });
                continue;
            }
            Err(ObjectError::NotFound) => {}
            Err(error) => {
                return Err(object_load_error(
                    message_id,
                    &stored_key,
                    error,
                    SendFailure::AttachmentFetchFailed,
                ));
            }
        }

        let Some(raw_url) = &attachment.url else {
            tracing::error!(
                message_id,
                attachment_id = %attachment.attachment_id,
                event = "send_part_has_no_source",
                "attachment names neither stored bytes nor a URL"
            );
            return Err(LoadError::Permanent(SendFailure::AttachmentFetchFailed));
        };
        // Re-checked here rather than trusted from the spec: the shape rules
        // are cheap, and the spec has been sitting in S3 since enqueue.
        let url = url_policy::parse_attachment_url(raw_url).map_err(|rejected| {
            tracing::warn!(
                message_id,
                attachment_id = %attachment.attachment_id,
                rejected = %rejected,
                event = "send_part_url_blocked",
                "attachment URL is not allowed"
            );
            LoadError::Permanent(SendFailure::AttachmentFetchFailed)
        })?;

        let fetched = AttachmentFetcher::fetch(&state.services, &url, budget)
            .await
            .map_err(|error| fetch_error(message_id, &attachment.attachment_id, error))?;
        budget = budget.saturating_sub(fetched.bytes.len() as u64);

        // Stored under the attachment's own key so a retry reuses these
        // bytes instead of fetching a URL whose content may have changed.
        state
            .services
            .put_object_if_absent(
                &stored_key,
                axum::body::Bytes::from(fetched.bytes.clone()),
                &attachment.content_type,
            )
            .await
            .map_err(|error| {
                object_load_error(
                    message_id,
                    &stored_key,
                    error,
                    SendFailure::AttachmentFetchFailed,
                )
            })?;

        let mut spec_attachment = attachment.clone();
        spec_attachment.object_key = Some(stored_key);
        spec_attachment.size = fetched.bytes.len() as u64;
        parts.push(LoadedPart {
            spec: spec_attachment,
            bytes: fetched.bytes,
        });
    }
    Ok(parts)
}

/// Maps a fetch failure onto whether another attempt could succeed.
fn fetch_error(message_id: &str, attachment_id: &str, error: FetchError) -> LoadError {
    if error.is_transient() {
        return LoadError::Transient {
            failure: SendFailure::AttachmentFetchUnavailable,
            error: anyhow::Error::new(error).context(format!("fetching {attachment_id}")),
        };
    }
    // The URL is never logged: it came from a caller and may carry a
    // credential in its path or query.
    tracing::warn!(
        message_id,
        attachment_id,
        error = %error,
        event = "send_part_fetch_failed",
        "attachment could not be fetched"
    );
    LoadError::Permanent(SendFailure::AttachmentFetchFailed)
}

async fn fail<T: Services>(
    state: &AppState<T>,
    claimed: &SendState,
    failure: SendFailure,
    now: &str,
) -> Result<Handled, SenderError> {
    record(state, claimed, MarkOutcome::Failed(failure), now)
        .await
        .map_err(store_error)?;
    metrics::counter!(names::SEND_FAILURES).increment(1);
    tracing::warn!(
        message_id = %claimed.message_id,
        failure = failure.as_str(),
        event = "send_failed",
        "send will not be retried"
    );
    Ok(Handled::Failed)
}

fn store_error(error: MailStoreError) -> SenderError {
    SenderError::Store(anyhow::Error::new(error))
}
