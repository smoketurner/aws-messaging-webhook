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

use crate::actions::{RawSend, SendOutcome};
use crate::mail::build::{BuildError, BuiltPart, build_outbound};
use crate::mail::fetch::{AttachmentFetcher, FetchError};
use crate::mail::objects::ObjectError;
use crate::mail::send::{self, SendFailure, SendSpec, SendState, SendStatus};
use crate::mail::store::{MailStoreError, MarkOutcome};
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
    #[error("the object store was unavailable")]
    Objects(#[source] anyhow::Error),
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
) -> Result<serde_json::Value, lambda_http::Error> {
    let event: aws_lambda_events::dynamodb::Event = serde_json::from_value(payload)
        .map_err(|e| format!("payload has Records but is not a DynamoDB stream event: {e}"))?;

    let mut failures = Vec::new();
    for record in event.records {
        let Some(message_id) = queued_send_id(&record) else {
            continue;
        };
        if let Err(error) = handle_send(state, &message_id).await {
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
    if crate::stream::image_str(new_image, "send_status")? != "queued" {
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
    state
        .services
        .mark_send(&send, outcome, &now)
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

/// What one sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Claims released because the sender holding them appears to be gone.
    pub released: usize,
    /// Claims left alone because they are still plausibly live.
    pub still_working: usize,
    /// Sends whose outcome is unknown. Reported, never touched.
    pub unknown: usize,
}

/// Recovers sends whose sender died mid-flight.
///
/// A Lambda can be killed between claiming a send and recording its outcome,
/// which leaves the state item `sending` with nobody working on it and no
/// stream record to re-trigger. Nothing else would ever notice.
///
/// Sends in `unknown` are counted and left alone. SES may hold those
/// messages, so releasing one could deliver it twice; only an operator can
/// decide.
///
/// # Errors
///
/// [`SenderError`] when the store cannot be read or written.
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

        tracing::warn!(
            message_id = %stuck.message_id,
            sending_at = stuck.sending_at.as_deref().unwrap_or("unknown"),
            event = "send_claim_released",
            "releasing a claim whose sender appears to be gone"
        );
        release(state, &stuck, &now).await?;
        report.released += 1;
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
/// # Errors
///
/// [`SenderError`] only for failures worth another delivery of the same
/// record; every permanent outcome is recorded and returns `Ok`.
pub async fn handle_send<T: Services>(
    state: &AppState<T>,
    message_id: &str,
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
    let spec = match load_spec(state, message_id).await {
        Ok(spec) => spec,
        Err(LoadError::Missing) => {
            return fail(state, &claimed, SendFailure::SendSpecMissing, &now).await;
        }
        Err(LoadError::Unavailable(error)) => {
            // Hand the record back rather than holding a claim nobody will
            // release, then let the event source redeliver it.
            release(state, &claimed, &now).await?;
            return Err(SenderError::Objects(error));
        }
    };

    let parts = match load_parts(state, &spec).await {
        Ok(parts) => parts,
        Err(LoadError::Missing) => {
            return fail(state, &claimed, SendFailure::AttachmentFetchFailed, &now).await;
        }
        Err(LoadError::Unavailable(error)) => {
            release(state, &claimed, &now).await?;
            return Err(SenderError::Objects(error));
        }
    };

    let built = match build_outbound(
        &spec,
        &parts.iter().map(part_ref).collect::<Vec<_>>(),
        MAX_OUTBOUND_RAW_BYTES,
    ) {
        Ok(built) => built,
        Err(BuildError::TooLarge { size, limit }) => {
            tracing::warn!(
                message_id,
                size,
                limit,
                event = "send_too_large",
                "assembled message exceeds what SES accepts"
            );
            return fail(state, &claimed, SendFailure::MessageTooLarge, &now).await;
        }
        Err(BuildError::Failed(error)) => {
            tracing::error!(
                message_id,
                error = ?error,
                event = "send_build_failed",
                "could not assemble the message"
            );
            return fail(state, &claimed, SendFailure::MessageTooLarge, &now).await;
        }
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

    record_outcome(state, &claimed, outcome, &now).await
}

/// Records what SES said, which is the only write that touches the message.
async fn record_outcome<T: Services>(
    state: &AppState<T>,
    claimed: &SendState,
    outcome: SendOutcome,
    now: &str,
) -> Result<Handled, SenderError> {
    let message_id = claimed.message_id.as_str();
    match outcome {
        SendOutcome::Sent { ses_message_id } => {
            state
                .services
                .mark_send(
                    claimed,
                    MarkOutcome::Sent {
                        ses_message_id: &ses_message_id,
                    },
                    now,
                )
                .await
                .map_err(store_error)?;
            metrics::counter!(names::MESSAGES_SENT).increment(1);
            tracing::info!(
                message_id,
                ses_message_id,
                inbox_id = %claimed.inbox_id.as_str(),
                event = "message_sent",
                "sent"
            );
            clear_outbox(state, claimed).await;
            Ok(Handled::Sent)
        }
        SendOutcome::Failed { reason } => {
            tracing::warn!(
                message_id,
                reason,
                event = "send_rejected",
                "SES refused the message"
            );
            fail(state, claimed, SendFailure::Rejected, now).await
        }
        SendOutcome::Retryable { reason } => {
            tracing::warn!(
                message_id,
                reason,
                event = "send_retryable",
                "SES was unavailable; releasing for another attempt"
            );
            // Give up for good once a send has been handed back too often,
            // rather than looping on it forever.
            if claimed.transient_failures + 1 >= MAX_TRANSIENT_FAILURES {
                return fail(state, claimed, SendFailure::SesUnavailable, now).await;
            }
            release(state, claimed, now).await?;
            Ok(Handled::Skipped)
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
            state
                .services
                .mark_send(claimed, MarkOutcome::Unknown, now)
                .await
                .map_err(store_error)?;
            metrics::counter!(names::SEND_OUTCOME_UNKNOWN).increment(1);
            Ok(Handled::Unknown)
        }
    }
}

/// How many times a send may be handed back before it is abandoned.
const MAX_TRANSIENT_FAILURES: u32 = 5;

/// Removes the outbox objects for a finished send.
///
/// Best effort, and deliberately after the outcome is recorded: the send has
/// already happened, so a failure here must not undo it or make the record be
/// retried. Anything left behind expires with the bucket's retention rule.
///
/// Only ever called for a send that is finished and will not be rebuilt. A
/// send in `unknown` keeps its objects, because an operator may yet ask for it
/// to go out again.
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
                error = %error,
                event = "outbox_cleanup_failed",
                "could not remove an outbox object; it will expire with retention"
            );
        }
    }
}

/// Why a spec or part could not be loaded.
enum LoadError {
    /// It is not there, and will not appear: retention removed it, or it was
    /// never written.
    Missing,
    /// The object store could not answer.
    Unavailable(anyhow::Error),
}

impl From<ObjectError> for LoadError {
    fn from(error: ObjectError) -> Self {
        match error {
            ObjectError::NotFound => Self::Missing,
            ObjectError::Transient(source) => Self::Unavailable(source),
            other => Self::Unavailable(anyhow::anyhow!("{other}")),
        }
    }
}

async fn load_spec<T: Services>(
    state: &AppState<T>,
    message_id: &str,
) -> Result<SendSpec, LoadError> {
    let bytes = state
        .services
        .get_object(&send::spec_key(message_id), MAX_OUTBOUND_RAW_BYTES)
        .await?;
    serde_json::from_slice(&bytes).map_err(|e| {
        // A spec that will not parse will not parse on a retry either.
        tracing::error!(message_id, error = ?e, event = "send_spec_unreadable", "unreadable spec");
        LoadError::Missing
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

/// Reads every attachment named by the spec.
///
/// A URL-backed part that has not been fetched yet has no object key; those
/// are not supported by this sender yet and stop the send rather than
/// silently producing a message missing its attachment.
async fn load_parts<T: Services>(
    state: &AppState<T>,
    spec: &SendSpec,
) -> Result<Vec<LoadedPart>, LoadError> {
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
                .await?;
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
        let stored_key = send::part_key(&spec.message_id, &attachment.attachment_id);
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
            Err(other) => return Err(other.into()),
        }

        let Some(raw_url) = &attachment.url else {
            tracing::error!(
                message_id = %spec.message_id,
                attachment_id = %attachment.attachment_id,
                event = "send_part_has_no_source",
                "attachment names neither stored bytes nor a URL"
            );
            return Err(LoadError::Missing);
        };
        // Re-checked here rather than trusted from the spec: the shape rules
        // are cheap, and the spec has been sitting in S3 since enqueue.
        let url = url_policy::parse_attachment_url(raw_url).map_err(|rejected| {
            tracing::warn!(
                message_id = %spec.message_id,
                attachment_id = %attachment.attachment_id,
                rejected = %rejected,
                event = "send_part_url_blocked",
                "attachment URL is not allowed"
            );
            LoadError::Missing
        })?;

        let fetched = AttachmentFetcher::fetch(&state.services, &url, budget)
            .await
            .map_err(|error| fetch_error(&spec.message_id, &attachment.attachment_id, &error))?;
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
            .await?;

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

/// Maps a fetch failure onto the two outcomes the caller distinguishes: worth
/// another attempt, or not.
fn fetch_error(message_id: &str, attachment_id: &str, error: &FetchError) -> LoadError {
    if error.is_transient() {
        return LoadError::Unavailable(anyhow::anyhow!("fetching {attachment_id}: {error}"));
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
    LoadError::Missing
}

async fn fail<T: Services>(
    state: &AppState<T>,
    claimed: &SendState,
    failure: SendFailure,
    now: &str,
) -> Result<Handled, SenderError> {
    state
        .services
        .mark_send(claimed, MarkOutcome::Failed(failure), now)
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

async fn release<T: Services>(
    state: &AppState<T>,
    claimed: &SendState,
    now: &str,
) -> Result<(), SenderError> {
    state
        .services
        .mark_send(claimed, MarkOutcome::Released, now)
        .await
        .map_err(store_error)
}

fn store_error(error: MailStoreError) -> SenderError {
    SenderError::Store(anyhow::Error::new(error))
}
