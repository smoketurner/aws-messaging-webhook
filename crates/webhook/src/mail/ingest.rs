//! Inbound ingest: fetch → parse (`spawn_blocking`) → extract attachments →
//! per-inbox thread resolution and insert, all bounded by the invocation
//! deadline.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::body::Bytes;
use uuid::Uuid;

use crate::actions::ActionError;
use crate::config::MailConfig;
use crate::mail::labels::SystemLabel;
use crate::mail::mime::{ParseError, ParsedAttachment, ParsedMessage, parse_inbound};
use crate::mail::objects::ObjectStore as _;
use crate::mail::store::MailStoreError;
use crate::mail::{
    AttachmentMeta, Direction, InboxId, MAX_INBOUND_RAW_BYTES, MailMessage, MessageId,
    SesMessageId, ThreadId, content, ids, labels, object_keys, strip_angle_brackets, thread, time,
};
use crate::metrics::names;
use crate::model::ses_inbound::{SesInboundNotification, SesReceipt, Verdict};
use crate::state::{AppState, Backend};

/// Headroom held back from the invocation deadline: ingest runs under
/// `deadline − now − DEADLINE_MARGIN`, leaving room for the timeout's own
/// bookkeeping and for tearing down in-flight uploads.
const DEADLINE_MARGIN: Duration = Duration::from_secs(10);

/// What an inbound ingest did, reported on the action outcome log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// Mail is not configured, so there is nowhere to ingest into.
    MailDisabled,
    /// Nothing to ingest: no usable S3 pointer, timestamp or inbox.
    Skipped,
    /// A permanent failure, already logged and counted.
    Failed,
    /// Every target inbox already holds the message.
    Duplicate,
    Ingested,
}

impl IngestOutcome {
    /// The log form.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MailDisabled => "none",
            Self::Skipped => "ingest_skipped",
            Self::Failed => "ingest_failed",
            Self::Duplicate => "ingest_duplicate",
            Self::Ingested => "ingested",
        }
    }
}

/// Runs the full ingest flow for one SES inbound receipt.
///
/// # Errors
///
/// Returns [`ActionError::Transient`] for an S3 or store transient failure,
/// or when the deadline elapses — a 5xx, so SNS or the async-invoke queue
/// redelivers. Returns [`ActionError::Permanent`] for a missing or malformed
/// object, a size or label cap violation, or throttling that outlasts the
/// transaction retry budget; those are logged and counted, and the inbound
/// event still publishes.
pub async fn ingest_inbound<B: Backend>(
    state: &AppState<B>,
    event: &SesInboundNotification,
    deadline: tokio::time::Instant,
    envelope_ts_ms: Option<u64>,
) -> Result<IngestOutcome, ActionError> {
    let Some(mail_config) = state.config.mail.as_ref() else {
        return Ok(IngestOutcome::MailDisabled);
    };
    let Some(key) = guarded_s3_pointer(event, mail_config) else {
        return Ok(IngestOutcome::Skipped);
    };
    let Some(received_ms) = received_ms(event, envelope_ts_ms) else {
        metrics::counter!(names::INGEST_SKIPPED).increment(1);
        tracing::warn!(
            event = "ingest_skipped",
            reason = "no_timestamp",
            "none of mail.timestamp, receipt.timestamp or the SNS envelope Timestamp parsed \
             (wall-clock time is never a fallback: it would make the message id \
             differ across a redelivery)"
        );
        return Ok(IngestOutcome::Skipped);
    };

    let mut resolution = InboxResolution::default();
    for recipient in normalized_recipients(&event.receipt) {
        resolve_recipient(state, mail_config, &recipient, &mut resolution).await;
    }
    if resolution.inboxes.is_empty() {
        if resolution.had_permanent {
            // Already logged and counted as a failure by
            // `record_resolution_error`: the message was addressed to an
            // inbox we serve but couldn't read, not to one we don't serve.
            return finish(resolution.had_transient, IngestOutcome::Failed);
        }
        metrics::counter!(names::INGEST_SKIPPED).increment(1);
        tracing::info!(
            event = "ingest_skipped",
            reason = "no_resolvable_inbox",
            "no recipient resolved to the configured inbox"
        );
        return finish(resolution.had_transient, IngestOutcome::Skipped);
    }

    let uuid = ids::inbound_message_id(&event.mail.message_id, received_ms);
    let message_id = MessageId::from(uuid);

    let inboxes = std::mem::take(&mut resolution.inboxes);
    let targets = fresh_targets(state, inboxes, &message_id, &mut resolution).await;
    if targets.is_empty() {
        let outcome = if resolution.had_permanent {
            IngestOutcome::Failed
        } else {
            IngestOutcome::Duplicate
        };
        return finish(resolution.had_transient, outcome);
    }

    let budget = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .saturating_sub(DEADLINE_MARGIN);
    let outcome = tokio::time::timeout(
        budget,
        ingest_into_targets(state, key, event, targets, uuid, &message_id, received_ms),
    )
    .await;

    if let Ok(result) = outcome {
        let result = result
            .map_err(|error| record_ingest_error(error, &event.mail.message_id, &message_id))?;
        finish(resolution.had_transient, result)
    } else {
        metrics::counter!(names::INGEST_TIMEOUTS).increment(1);
        tracing::warn!(
            event = "ingest_deadline",
            %message_id,
            "ingest exceeded its deadline budget"
        );
        Err(ActionError::transient(anyhow::anyhow!(
            "ingest exceeded its deadline budget"
        )))
    }
}

/// The two message-level skip guards: no S3 pointer, or a bucket other than
/// `MailConfig.bucket`. Returns the inbound object key on success.
fn guarded_s3_pointer<'a>(
    event: &'a SesInboundNotification,
    mail_config: &MailConfig,
) -> Option<&'a str> {
    let Some((bucket, key)) = event.receipt.s3_pointer() else {
        metrics::counter!(names::INGEST_SKIPPED).increment(1);
        tracing::info!(
            event = "ingest_skipped",
            reason = "no_s3_pointer",
            "inbound receipt carries no S3 pointer; nothing to ingest"
        );
        return None;
    };
    if bucket != mail_config.bucket {
        metrics::counter!(names::INGEST_SKIPPED).increment(1);
        tracing::warn!(
            event = "ingest_skipped",
            reason = "foreign_bucket",
            bucket,
            "inbound receipt points at a bucket other than MAIL_BUCKET"
        );
        return None;
    }
    Some(key)
}

/// Drops inboxes where `message_exists` is already true (a redelivery),
/// aggregating any store error into `resolution` the same way
/// [`resolve_recipient`] does.
async fn fresh_targets<B: Backend>(
    state: &AppState<B>,
    inboxes: Vec<InboxId>,
    message_id: &MessageId,
    resolution: &mut InboxResolution,
) -> Vec<InboxId> {
    let mut targets = Vec::with_capacity(inboxes.len());
    for inbox_id in inboxes {
        match state.mailbox.message_exists(&inbox_id, message_id).await {
            Ok(false) => targets.push(inbox_id),
            Ok(true) => {}
            Err(error) => record_resolution_error(resolution, error),
        }
    }
    targets
}

/// Logs and counts an ingest that ended in a permanent failure — a missing
/// or oversized object, malformed MIME, a denied store — naming both the SES
/// message id (what an operator has from the bounce or the SES console) and
/// the message id this service would have given it. A transient failure is
/// left alone: the redelivery it recruits is the outcome, not a loss.
fn record_ingest_error(
    error: ActionError,
    ses_message_id: &str,
    message_id: &MessageId,
) -> ActionError {
    if error.kind == crate::actions::ActionErrorKind::Permanent {
        metrics::counter!(names::INGEST_FAILURES).increment(1);
        tracing::error!(
            error = ?error.source,
            ses_message_id,
            %message_id,
            event = "ingest_failure",
            "inbound message could not be ingested"
        );
    }
    error
}

/// Combines the per-recipient resolution outcome with the fetch/parse/insert
/// outcome: a transient failure anywhere in the action wins, even when every
/// inbox that did complete succeeded.
fn finish(had_transient: bool, outcome: IngestOutcome) -> Result<IngestOutcome, ActionError> {
    if had_transient {
        return Err(ActionError::transient(anyhow::anyhow!(
            "ingest failed transiently for at least one recipient inbox"
        )));
    }
    Ok(outcome)
}

#[derive(Default)]
struct InboxResolution {
    inboxes: Vec<InboxId>,
    had_transient: bool,
    had_permanent: bool,
}

/// Normalizes `receipt.recipients`: lowercase, deduplicated, in their
/// original order.
fn normalized_recipients(receipt: &SesReceipt) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    receipt
        .recipients
        .iter()
        .map(|r| r.trim().to_ascii_lowercase())
        .filter(|r| seen.insert(r.clone()))
        .collect()
}

/// Resolves one normalized recipient address to an inbox, appending it to
/// `resolution.inboxes` on success. An address other than `MAIL_INBOX` is
/// skipped rather than failing the message, logged at WARN with a `reason`:
/// a
/// recipient SES accepted but we don't store is a configuration error worth
/// seeing in a default INFO deployment. This is a per-recipient
/// skip, so unlike the whole-message skips above it does not increment
/// `IngestSkipped` — that metric keeps meaning "a message we didn't store".
async fn resolve_recipient<B: Backend>(
    state: &AppState<B>,
    mail_config: &MailConfig,
    address: &str,
    resolution: &mut InboxResolution,
) {
    if address != mail_config.inbox.as_str() {
        tracing::warn!(
            event = "ingest_skipped",
            reason = "unknown_inbox",
            recipient = address,
            "recipient is not MAIL_INBOX"
        );
        return;
    }
    let inbox_id = InboxId::new(address);

    match state.mailbox.get_inbox(&inbox_id).await {
        Ok(Some(_)) => {
            resolution.inboxes.push(inbox_id);
            return;
        }
        Ok(None) => {}
        Err(error) => {
            record_resolution_error(resolution, error);
            return;
        }
    }

    let now = time::format(time::now_ms());
    match state.mailbox.ensure_inbox(&inbox_id, &now).await {
        Ok(_) => resolution.inboxes.push(inbox_id),
        Err(error) => record_resolution_error(resolution, error),
    }
}

/// Records a store failure met while resolving an inbox. A transient one
/// fails the action so the message is redelivered; a permanent one (a denied
/// table, a malformed item) cannot be retried into success, so it is logged
/// and counted here — it would otherwise be indistinguishable from "this
/// message was addressed to an inbox we don't serve".
fn record_resolution_error(resolution: &mut InboxResolution, error: MailStoreError) {
    let error = ActionError::from(error);
    match error.kind {
        crate::actions::ActionErrorKind::Transient => resolution.had_transient = true,
        crate::actions::ActionErrorKind::Permanent => {
            resolution.had_permanent = true;
            metrics::counter!(names::INGEST_FAILURES).increment(1);
            tracing::error!(
                error = ?error.source,
                event = "ingest_failure",
                reason = "inbox_resolution",
                "permanent store error resolving the recipient's inbox"
            );
        }
    }
}

/// `received_ms` = `mail.timestamp`, else `receipt.timestamp`, else
/// `envelope_ts_ms` (the verified SNS envelope's `Timestamp`, parsed and
/// threaded in from `sns::mod::process_notification` via `actions::run`).
/// [`time::now_ms`] is never a fallback: `received_ms` seeds
/// `inbound_message_id`, so a wall-clock value would make the id
/// non-deterministic across a redelivery, defeating `message_exists`'s
/// dedup. `None` when all three sources are missing or fail to parse — the
/// caller skips the message (`no_timestamp`) rather than inventing one; a
/// real SES receipt always populates `mail.timestamp`, so this is
/// unreachable in practice.
fn received_ms(event: &SesInboundNotification, envelope_ts_ms: Option<u64>) -> Option<u64> {
    event
        .mail
        .timestamp
        .as_deref()
        .and_then(time::parse)
        .or_else(|| event.receipt.timestamp.as_deref().and_then(time::parse))
        .or(envelope_ts_ms)
}

/// Fetches, parses and extracts the raw MIME once, then inserts the built
/// item into every target inbox.
async fn ingest_into_targets<B: Backend>(
    state: &AppState<B>,
    key: &str,
    event: &SesInboundNotification,
    targets: Vec<InboxId>,
    uuid: Uuid,
    message_id: &MessageId,
    received_ms: u64,
) -> Result<IngestOutcome, ActionError> {
    let (parsed, mut content, attachments) =
        fetch_parse_and_extract(state, key, uuid, message_id).await?;

    let verdict =
        labels::classify_inbound(event.receipt.is_quarantined(), auth_failed(&event.receipt));
    let now_ms = time::now_ms();
    content.verdicts = Some(verdicts_json(&event.receipt));
    let draft = InboundDraft {
        parsed,
        message_id: message_id.clone(),
        ses_message_id: SesMessageId::new(event.mail.message_id.as_str()),
        labels: verdict_labels(verdict),
        timestamp: time::format(received_ms),
        now: time::format(now_ms),
        raw_s3_key: key.to_owned(),
        attachments,
        expires_at: state
            .config
            .mail
            .as_ref()
            .map_or(0, |mail| time::expires_at(now_ms, mail.retention_days)),
    };

    if !insert_into_all_targets(state, &draft, &content, targets).await? {
        return Ok(IngestOutcome::Failed);
    }
    metrics::counter!(names::MESSAGES_INGESTED).increment(1);
    tracing::info!(
        event = "ingest_complete",
        %message_id,
        "ingested inbound message"
    );
    Ok(IngestOutcome::Ingested)
}

/// `GetObject`, then a `spawn_blocking` parse, then extraction of the kept
/// attachment parts, returning the parsed fields, the content template and
/// its attachments' persisted metadata.
async fn fetch_parse_and_extract<B: Backend>(
    state: &AppState<B>,
    key: &str,
    uuid: Uuid,
    message_id: &MessageId,
) -> Result<(ParsedMessage, content::MessageContent, Vec<AttachmentMeta>), ActionError> {
    let raw = state
        .objects
        .get_object(key, MAX_INBOUND_RAW_BYTES)
        .await
        .map_err(ActionError::from)?;

    let parsed = tokio::task::spawn_blocking(move || parse_inbound(&raw))
        .await
        .map_err(|error| {
            ActionError::permanent(anyhow::anyhow!("ingest parse task panicked: {error}"))
        })?
        .map_err(|ParseError::Malformed| {
            ActionError::permanent(anyhow::anyhow!("malformed inbound MIME message"))
        })?;

    let kept_attachments: Vec<(String, ParsedAttachment)> = parsed
        .attachments
        .into_iter()
        .enumerate()
        .map(|(ordinal, attachment)| (ids::attachment_id(&uuid, ordinal), attachment))
        .collect();
    put_kept_parts(state, message_id, &kept_attachments).await?;

    let attachment_metas: Vec<AttachmentMeta> = kept_attachments
        .iter()
        .map(|(attachment_id, attachment)| AttachmentMeta {
            attachment_id: attachment_id.clone(),
            object_key: Some(object_keys::attachment(message_id, attachment_id)),
            size: u64::try_from(attachment.bytes.len()).unwrap_or(u64::MAX),
            filename: attachment.filename.clone(),
            content_type: attachment.content_type.clone(),
            content_disposition: attachment.content_disposition,
            content_id: attachment.content_id.clone(),
        })
        .collect();

    Ok((parsed.message, parsed.content, attachment_metas))
}

/// Everything one inbound message's item is built from, before it is
/// addressed to an inbox: one draft is inserted into every target inbox.
struct InboundDraft {
    parsed: ParsedMessage,
    message_id: MessageId,
    ses_message_id: SesMessageId,
    labels: Vec<String>,
    /// Fixed-width (`mail::time::format`) receipt time.
    timestamp: String,
    now: String,
    raw_s3_key: String,
    attachments: Vec<AttachmentMeta>,
    expires_at: u64,
}

impl InboundDraft {
    /// The message as `inbox_id` stores it. Its thread is the one it starts
    /// when no candidate resolves; `insert_message` moves it otherwise.
    ///
    /// `thread_snapshot` is left `None` here — `plan_insert` populates it
    /// from `thread_after` (the thread's full state including this message,
    /// computed from the versioned retry loop's consistent read), so a reply
    /// publishes the thread's real `message_count` rather than 1.
    fn to_message(&self, inbox_id: &InboxId) -> MailMessage {
        let parsed = self.parsed.clone();
        MailMessage {
            inbox_id: inbox_id.clone(),
            thread_id: ThreadId::from(&self.message_id),
            message_id: self.message_id.clone(),
            ses_message_id: Some(self.ses_message_id.clone()),
            direction: Direction::Inbound,
            rfc_message_id: parsed.rfc_message_id,
            in_reply_to: parsed.in_reply_to,
            labels: self.labels.clone(),
            timestamp: self.timestamp.clone(),
            from: parsed.from,
            to: parsed.to,
            cc: parsed.cc,
            bcc: parsed.bcc,
            subject: parsed.subject,
            preview: parsed.preview,
            size: parsed.size,
            attachments: self.attachments.clone(),
            attachments_truncated: parsed.attachments_truncated,
            raw_s3_key: Some(self.raw_s3_key.clone()),
            thread_snapshot: None,
            delivery: BTreeMap::new(),
            send_status: None,
            sent_at: None,
            version: 0,
            created_at: self.now.clone(),
            updated_at: self.now.clone(),
            expires_at: self.expires_at,
        }
    }
}

/// Inserts into every target inbox: each is attempted regardless of an
/// earlier permanent failure; a transient failure anywhere
/// fails the whole action once every inbox has been attempted. Returns
/// whether at least one inbox succeeded.
async fn insert_into_all_targets<B: Backend>(
    state: &AppState<B>,
    draft: &InboundDraft,
    content: &content::MessageContent,
    targets: Vec<InboxId>,
) -> Result<bool, ActionError> {
    let mut had_transient = false;
    let mut inserted_any = false;

    for inbox_id in targets {
        match insert_into_inbox(state, draft, content, &inbox_id).await {
            Ok(()) => inserted_any = true,
            Err(ActionError {
                kind: crate::actions::ActionErrorKind::Transient,
                ..
            }) => {
                had_transient = true;
            }
            Err(error) => {
                metrics::counter!(names::INGEST_FAILURES).increment(1);
                tracing::error!(
                    error = ?error.source,
                    message_id = %draft.message_id,
                    inbox_id = inbox_id.as_str(),
                    event = "ingest_failure",
                    "permanent error inserting inbound message; continuing with remaining inboxes"
                );
            }
        }
    }

    if had_transient {
        return Err(ActionError::transient(anyhow::anyhow!(
            "ingest failed transiently for one or more target inboxes"
        )));
    }
    Ok(inserted_any)
}

/// Stores this message's content document and inserts its item for one
/// target inbox: the document, then `insert_message`, which resolves the
/// thread (`Duplicate` counts as success). The document goes first so an
/// item never exists without it.
async fn insert_into_inbox<B: Backend>(
    state: &AppState<B>,
    draft: &InboundDraft,
    content: &content::MessageContent,
    inbox_id: &InboxId,
) -> Result<(), ActionError> {
    let msg = draft.to_message(inbox_id);
    // `parse_inbound` re-wraps `In-Reply-To`/`References` in angle brackets
    // (matching `our_rfc_message_id`/`ses_rfc_ids`'s output shape), but the
    // RFC alias key `plan_insert` writes strips them (`strip_angle_brackets`
    // in `mail/plan.rs`) — candidates must be stripped the same way or every
    // lookup misses.
    let in_reply_to = msg.in_reply_to.as_deref().map(strip_angle_brackets);
    let references: Vec<String> = content
        .references
        .iter()
        .map(|id| strip_angle_brackets(id).to_owned())
        .collect();
    // The message's own `Message-ID` goes first: when this inbox already
    // holds a row for that id — its own send arriving back, since SES writes
    // the same id over ours on both copies, or a placeholder an earlier reply
    // left for it — the message joins that row's thread. `insert_message`
    // resolves the thread from these and leaves a placeholder for each
    // referenced id that no row maps yet.
    let own_id = strip_angle_brackets(&msg.rfc_message_id);
    let mut candidates = Vec::new();
    if !own_id.is_empty() {
        candidates.push(own_id.to_owned());
    }
    for candidate in thread::candidate_ids(in_reply_to, &references) {
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }

    content::store(&state.objects, inbox_id, &draft.message_id, content)
        .await
        .map_err(ActionError::from)?;

    state
        .mailbox
        .insert_message(&msg, &candidates)
        .await
        .map(|_outcome| ())
        .map_err(ActionError::from)
}

/// The labels an inbound message carries: `received` and `unread`, plus
/// whatever the verdict adds, sorted (matching `plan_insert`'s sorted-labels
/// invariant).
fn verdict_labels(verdict: labels::InboundVerdict) -> Vec<String> {
    let mut labels: Vec<String> = [SystemLabel::Received, SystemLabel::Unread]
        .into_iter()
        .chain(verdict.label())
        .map(SystemLabel::to_label)
        .collect();
    labels.sort_unstable();
    labels
}

/// Whether SPF, DKIM or DMARC returned `FAIL`.
fn auth_failed(receipt: &SesReceipt) -> bool {
    let failed = |verdict: &Option<Verdict>| verdict.as_ref().is_some_and(Verdict::failed);
    failed(&receipt.spf_verdict) || failed(&receipt.dkim_verdict) || failed(&receipt.dmarc_verdict)
}

/// A compact JSON summary of every verdict SES reported, stored in the
/// message's content document.
fn verdicts_json(receipt: &SesReceipt) -> serde_json::Value {
    let status = |verdict: &Option<Verdict>| verdict.as_ref().map(|v| v.status.as_str().to_owned());
    serde_json::json!({
        "spam": status(&receipt.spam_verdict),
        "virus": status(&receipt.virus_verdict),
        "spf": status(&receipt.spf_verdict),
        "dkim": status(&receipt.dkim_verdict),
        "dmarc": status(&receipt.dmarc_verdict),
        "dmarcPolicy": receipt.dmarc_policy,
    })
}

/// Extracts the kept attachment parts into `attachments/<mid>/<att_id>`,
/// resuming a redelivery by skipping parts a prior attempt already put, in
/// chunks of 4 concurrent puts.
///
/// `tokio::join!` rather than a `JoinSet`: a `JoinSet` requires its spawned
/// futures to be `'static`, which this function's borrowed `&AppState<B>` is
/// not (the pipeline never passes an `Arc<AppState<B>>` down to this depth).
/// `join!` gives the same bounded 4-way concurrency and, holding no
/// independent task handles, a stronger cancellation guarantee when the
/// surrounding `tokio::time::timeout` elapses: every pending put is dropped
/// synchronously with the future tree, so no separate shutdown step is
/// needed to be sure no put is still running.
async fn put_kept_parts<B: Backend>(
    state: &AppState<B>,
    message_id: &MessageId,
    attachments: &[(String, ParsedAttachment)],
) -> Result<(), ActionError> {
    if attachments.is_empty() {
        return Ok(());
    }

    let to_put = resume_filtered(state, message_id, attachments).await?;
    for chunk in to_put.chunks(4) {
        put_chunk(state, message_id, chunk).await?;
    }
    Ok(())
}

/// If the first kept part is already present, this is a redelivery — HEAD
/// every part and skip the ones already stored. Otherwise, put
/// everything (a race with a concurrent attempt still gets a harmless 412,
/// surfaced as `PutOutcome::AlreadyExists`).
async fn resume_filtered<'a, B: Backend>(
    state: &AppState<B>,
    message_id: &MessageId,
    attachments: &'a [(String, ParsedAttachment)],
) -> Result<Vec<&'a (String, ParsedAttachment)>, ActionError> {
    let (first_id, _) = &attachments[0];
    let first_present = state
        .objects
        .head_object(&object_keys::attachment(message_id, first_id))
        .await
        .map_err(ActionError::from)?
        .is_some();

    if !first_present {
        return Ok(attachments.iter().collect());
    }

    let mut kept = Vec::with_capacity(attachments.len());
    for entry @ (attachment_id, _) in attachments {
        let present = state
            .objects
            .head_object(&object_keys::attachment(message_id, attachment_id))
            .await
            .map_err(ActionError::from)?
            .is_some();
        if !present {
            kept.push(entry);
        }
    }
    Ok(kept)
}

async fn put_chunk<B: Backend>(
    state: &AppState<B>,
    message_id: &MessageId,
    chunk: &[&(String, ParsedAttachment)],
) -> Result<(), ActionError> {
    match chunk {
        [] => Ok(()),
        [a] => put_one(state, message_id, a).await,
        [a, b] => {
            let (r1, r2) =
                tokio::join!(put_one(state, message_id, a), put_one(state, message_id, b));
            r1.and(r2)
        }
        [a, b, c] => {
            let (r1, r2, r3) = tokio::join!(
                put_one(state, message_id, a),
                put_one(state, message_id, b),
                put_one(state, message_id, c)
            );
            r1.and(r2).and(r3)
        }
        [a, b, c, d, ..] => {
            let (r1, r2, r3, r4) = tokio::join!(
                put_one(state, message_id, a),
                put_one(state, message_id, b),
                put_one(state, message_id, c),
                put_one(state, message_id, d)
            );
            r1.and(r2).and(r3).and(r4)
        }
    }
}

async fn put_one<B: Backend>(
    state: &AppState<B>,
    message_id: &MessageId,
    (attachment_id, attachment): &(String, ParsedAttachment),
) -> Result<(), ActionError> {
    let key = object_keys::attachment(message_id, attachment_id);
    state
        .objects
        .put_object_if_absent(
            &key,
            Bytes::clone(&attachment.bytes),
            &attachment.content_type,
        )
        .await
        .map(|_outcome| ())
        .map_err(ActionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mail::labels::InboundVerdict;
    use crate::model::ses_inbound::SesReceipt;

    #[test]
    fn normalized_recipients_lowercases_trims_and_dedupes() {
        let receipt = SesReceipt {
            recipients: vec![
                " Support@Example.com ".to_owned(),
                "support@example.com".to_owned(),
                "sales@example.com".to_owned(),
            ],
            timestamp: None,
            spam_verdict: None,
            virus_verdict: None,
            spf_verdict: None,
            dkim_verdict: None,
            dmarc_verdict: None,
            dmarc_policy: None,
            action: None,
        };
        assert_eq!(
            normalized_recipients(&receipt),
            vec!["support@example.com", "sales@example.com"]
        );
    }

    #[test]
    fn verdict_labels_are_sorted_per_verdict() {
        assert_eq!(
            verdict_labels(InboundVerdict::Clean),
            vec!["received", "unread"]
        );
        assert_eq!(
            verdict_labels(InboundVerdict::Spam),
            vec!["received", "spam", "unread"]
        );
        assert_eq!(
            verdict_labels(InboundVerdict::Unauthenticated),
            vec!["received", "unauthenticated", "unread"]
        );
    }

    /// A store failure while resolving the recipient's inbox is either
    /// worth a redelivery or a failure we report — never the silent
    /// "addressed to an inbox we don't serve" skip it used to look like.
    #[test]
    fn a_resolution_failure_is_recorded_by_kind() {
        let mut resolution = InboxResolution::default();
        record_resolution_error(
            &mut resolution,
            MailStoreError::Transient(anyhow::anyhow!("throttled")),
        );
        assert!(resolution.had_transient);
        assert!(!resolution.had_permanent);

        let mut resolution = InboxResolution::default();
        record_resolution_error(&mut resolution, MailStoreError::NotFound);
        assert!(resolution.had_permanent);
        assert!(!resolution.had_transient);
    }

    fn notification_with_timestamps(
        mail_ts: Option<&str>,
        receipt_ts: Option<&str>,
    ) -> SesInboundNotification {
        serde_json::from_value(serde_json::json!({
            "notificationType": "Received",
            "mail": {"messageId": "m1", "timestamp": mail_ts},
            "receipt": {"recipients": [], "timestamp": receipt_ts},
        }))
        .unwrap()
    }

    /// `mail.timestamp` beats `receipt.timestamp` beats the envelope
    /// timestamp; `time::now_ms()` is never consulted, so all three missing
    /// is `None` rather than a wall-clock value.
    #[test]
    fn received_ms_follows_the_fallback_order_and_never_falls_back_to_now() {
        let both = notification_with_timestamps(
            Some("2026-01-01T00:00:00.000Z"),
            Some("2026-01-02T00:00:00.000Z"),
        );
        assert_eq!(
            received_ms(&both, Some(999)),
            time::parse("2026-01-01T00:00:00.000Z")
        );

        let receipt_only = notification_with_timestamps(None, Some("2026-01-02T00:00:00.000Z"));
        assert_eq!(
            received_ms(&receipt_only, Some(999)),
            time::parse("2026-01-02T00:00:00.000Z")
        );

        let neither = notification_with_timestamps(None, None);
        assert_eq!(received_ms(&neither, Some(999)), Some(999));
        assert_eq!(received_ms(&neither, None), None);
    }
}
