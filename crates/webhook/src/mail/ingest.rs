//! Inbound ingest: fetch → parse (`spawn_blocking`) → extract attachments →
//! per-inbox thread resolution and insert, all bounded by the invocation
//! deadline.

use std::future::Future;
use std::time::Duration;

use axum::body::Bytes;
use uuid::Uuid;

use crate::actions::ActionError;
use crate::config::MailConfig;
use crate::mail::mime::{ParseError, ParsedAttachment, parse_inbound};
use crate::mail::objects::ObjectError;
use crate::mail::store::MailStoreError;
use crate::mail::{
    AttachmentMeta, InboxId, MAX_INBOUND_RAW_BYTES, MailMessage, ids, labels, size, thread, time,
};
use crate::metrics::names;
use crate::model::ses_inbound::{SesInboundNotification, SesReceipt, Verdict};
use crate::state::{AppState, Services};

/// Headroom held back from the invocation deadline: ingest runs under
/// `deadline − now − DEADLINE_MARGIN`, leaving room for the timeout's own
/// bookkeeping and for tearing down in-flight uploads.
const DEADLINE_MARGIN: Duration = Duration::from_secs(10);

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
pub fn ingest_inbound<T: Services>(
    state: &AppState<T>,
    event: &SesInboundNotification,
    deadline: tokio::time::Instant,
    envelope_ts_ms: Option<u64>,
) -> impl Future<Output = Result<&'static str, ActionError>> {
    run(state, event, deadline, envelope_ts_ms)
}

async fn run<T: Services>(
    state: &AppState<T>,
    event: &SesInboundNotification,
    deadline: tokio::time::Instant,
    envelope_ts_ms: Option<u64>,
) -> Result<&'static str, ActionError> {
    let Some(mail_config) = state.config.mail.as_ref() else {
        return Ok("none");
    };
    let Some(key) = guarded_s3_pointer(event, mail_config) else {
        return Ok("ingest_skipped");
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
        return Ok("ingest_skipped");
    };

    let mut resolution = InboxResolution::default();
    for recipient in normalized_recipients(&event.receipt) {
        resolve_recipient(state, mail_config, &recipient, &mut resolution).await;
    }
    if resolution.inboxes.is_empty() {
        metrics::counter!(names::INGEST_SKIPPED).increment(1);
        tracing::info!(
            event = "ingest_skipped",
            reason = "no_resolvable_inbox",
            "no recipient resolved to the configured inbox"
        );
        return finish(resolution.had_transient, "ingest_skipped");
    }

    let message_id = ids::inbound_message_id(&event.mail.message_id, received_ms);
    let message_id_str = message_id.to_string();

    let inboxes = std::mem::take(&mut resolution.inboxes);
    let targets = fresh_targets(state, inboxes, &message_id_str, &mut resolution).await;
    if targets.is_empty() {
        return finish(resolution.had_transient, "ingest_duplicate");
    }

    let budget = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .saturating_sub(DEADLINE_MARGIN);
    let outcome = tokio::time::timeout(
        budget,
        ingest_into_targets(
            state,
            key,
            event,
            targets,
            message_id,
            &message_id_str,
            received_ms,
        ),
    )
    .await;

    if let Ok(result) = outcome {
        finish(resolution.had_transient, result?)
    } else {
        metrics::counter!(names::INGEST_TIMEOUTS).increment(1);
        tracing::warn!(
            event = "ingest_deadline",
            message_id = %message_id_str,
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
async fn fresh_targets<T: Services>(
    state: &AppState<T>,
    inboxes: Vec<InboxId>,
    message_id: &str,
    resolution: &mut InboxResolution,
) -> Vec<InboxId> {
    let mut targets = Vec::with_capacity(inboxes.len());
    for inbox_id in inboxes {
        match state.services.message_exists(&inbox_id, message_id).await {
            Ok(false) => targets.push(inbox_id),
            Ok(true) => {}
            Err(error) => record_resolution_error(resolution, error),
        }
    }
    targets
}

/// Combines the per-recipient resolution outcome with the fetch/parse/insert
/// outcome: a transient failure anywhere in the action wins, even when every
/// inbox that did complete succeeded.
fn finish(had_transient: bool, outcome: &'static str) -> Result<&'static str, ActionError> {
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
/// `resolution.inboxes` on success. A domain mismatch or a local part other
/// than `MAIL_INBOX` is skipped rather than failing the message, logged at
/// WARN with a `reason`: a
/// recipient SES accepted but we don't store is a configuration error worth
/// seeing in a default INFO deployment. This is a per-recipient
/// skip, so unlike the whole-message skips above it does not increment
/// `IngestSkipped` — that metric keeps meaning "a message we didn't store".
async fn resolve_recipient<T: Services>(
    state: &AppState<T>,
    mail_config: &MailConfig,
    address: &str,
    resolution: &mut InboxResolution,
) {
    let Some((local, domain)) = address.split_once('@') else {
        return;
    };
    if !domain.eq_ignore_ascii_case(&mail_config.domain) {
        tracing::warn!(
            event = "ingest_skipped",
            reason = "domain_mismatch",
            recipient = address,
            "recipient domain does not match MAIL_DOMAIN"
        );
        return;
    }
    if local != mail_config.inbox {
        tracing::warn!(
            event = "ingest_skipped",
            reason = "unknown_inbox",
            recipient = address,
            "recipient local part is not MAIL_INBOX"
        );
        return;
    }
    let inbox_id = InboxId(local.to_owned());

    match state.services.get_inbox(&inbox_id).await {
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
    match state.services.ensure_inbox(&inbox_id, address, &now).await {
        Ok(_) => resolution.inboxes.push(inbox_id),
        Err(error) => record_resolution_error(resolution, error),
    }
}

fn record_resolution_error(resolution: &mut InboxResolution, error: MailStoreError) {
    match map_store_error(error).kind {
        crate::actions::ActionErrorKind::Transient => resolution.had_transient = true,
        crate::actions::ActionErrorKind::Permanent => resolution.had_permanent = true,
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
async fn ingest_into_targets<T: Services>(
    state: &AppState<T>,
    key: &str,
    event: &SesInboundNotification,
    targets: Vec<InboxId>,
    message_id: Uuid,
    message_id_str: &str,
    received_ms: u64,
) -> Result<&'static str, ActionError> {
    let (parsed_message, attachment_metas) =
        fetch_parse_and_extract(state, key, message_id, message_id_str).await?;

    let verdict =
        labels::classify_inbound(event.receipt.is_quarantined(), auth_failed(&event.receipt));
    let message_labels = verdict_labels(verdict);
    let now = time::format(time::now_ms());
    let timestamp = time::format(received_ms);

    let inserted = insert_into_all_targets(
        state,
        &parsed_message,
        targets,
        message_id_str,
        &message_labels,
        &timestamp,
        &now,
        key,
        event,
        &attachment_metas,
    )
    .await?;

    if !inserted {
        return Ok("ingest_failed");
    }
    metrics::counter!(names::MESSAGES_INGESTED).increment(1);
    tracing::info!(
        event = "ingest_complete",
        message_id = %message_id_str,
        "ingested inbound message"
    );
    Ok("ingested")
}

/// `GetObject`, then a `spawn_blocking` parse, then extraction of the kept
/// attachment parts, returning the parsed content template and its
/// attachments' persisted metadata.
async fn fetch_parse_and_extract<T: Services>(
    state: &AppState<T>,
    key: &str,
    message_id: Uuid,
    message_id_str: &str,
) -> Result<(MailMessage, Vec<AttachmentMeta>), ActionError> {
    let raw = state
        .services
        .get_object(key, MAX_INBOUND_RAW_BYTES)
        .await
        .map_err(map_object_error)?;

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
        .map(|(ordinal, attachment)| (ids::attachment_id(&message_id, ordinal), attachment))
        .collect();
    put_kept_parts(state, message_id_str, &kept_attachments).await?;

    let attachment_metas: Vec<AttachmentMeta> = kept_attachments
        .iter()
        .map(|(attachment_id, attachment)| AttachmentMeta {
            attachment_id: attachment_id.clone(),
            object_key: Some(object_key(message_id_str, attachment_id)),
            size: u64::try_from(attachment.bytes.len()).unwrap_or(u64::MAX),
            filename: attachment.filename.clone(),
            content_type: attachment.content_type.clone(),
            content_disposition: attachment.content_disposition.clone(),
            content_id: attachment.content_id.clone(),
        })
        .collect();

    Ok((parsed.message, attachment_metas))
}

/// Inserts into every target inbox: each is attempted regardless of an
/// earlier permanent failure; a transient failure anywhere
/// fails the whole action once every inbox has been attempted. Returns
/// whether at least one inbox succeeded.
#[expect(
    clippy::too_many_arguments,
    reason = "one call site; see `ingest_into_targets`"
)]
async fn insert_into_all_targets<T: Services>(
    state: &AppState<T>,
    template: &MailMessage,
    targets: Vec<InboxId>,
    message_id_str: &str,
    message_labels: &[String],
    timestamp: &str,
    now: &str,
    raw_key: &str,
    event: &SesInboundNotification,
    attachment_metas: &[AttachmentMeta],
) -> Result<bool, ActionError> {
    let mut had_transient = false;
    let mut inserted_any = false;

    for inbox_id in targets {
        match insert_into_inbox(
            state,
            template,
            &inbox_id,
            message_id_str,
            message_labels,
            timestamp,
            now,
            raw_key,
            event,
            attachment_metas,
        )
        .await
        {
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
                    message_id = %message_id_str,
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

/// Builds and inserts this message's item for one target inbox: thread
/// resolution, label assignment, `fit_item` to the item budget, then
/// `insert_message` (`Duplicate` counts as success).
#[expect(
    clippy::too_many_arguments,
    reason = "one call site; see `ingest_into_targets`"
)]
async fn insert_into_inbox<T: Services>(
    state: &AppState<T>,
    template: &MailMessage,
    inbox_id: &InboxId,
    message_id: &str,
    message_labels: &[String],
    timestamp: &str,
    now: &str,
    raw_key: &str,
    event: &SesInboundNotification,
    attachment_metas: &[AttachmentMeta],
) -> Result<(), ActionError> {
    // `parse_inbound` re-wraps `In-Reply-To`/`References` in angle brackets
    // (matching `our_rfc_message_id`/`ses_rfc_ids`'s output shape), but the
    // RFC alias key `plan_insert` writes strips them (`strip_angle_brackets`
    // in `mail/plan.rs`) — candidates must be stripped the same way or every
    // lookup misses.
    let in_reply_to = template.in_reply_to.as_deref().map(strip_angle_brackets);
    let references: Vec<String> = template
        .references
        .iter()
        .map(|id| strip_angle_brackets(id).to_owned())
        .collect();
    let candidates = thread::candidate_ids(in_reply_to, &references);
    let thread_hit = state
        .services
        .resolve_rfc_ids(inbox_id, &candidates)
        .await
        .map_err(map_store_error)?;
    let thread_id = thread_hit.map_or_else(|| message_id.to_owned(), |hit| hit.thread_id);

    let mut msg = template.clone();
    msg.inbox_id = inbox_id.clone();
    msg.thread_id = thread_id;
    msg.message_id = message_id.to_owned();
    msg.ses_message_id = Some(event.mail.message_id.clone());
    msg.labels = message_labels.to_vec();
    msg.timestamp = timestamp.to_owned();
    msg.attachments = attachment_metas.to_vec();
    msg.raw_s3_key = Some(raw_key.to_owned());
    msg.verdicts = Some(verdicts_json(&event.receipt));
    // `thread_snapshot` is left `None` here — `plan_insert` populates it
    // from `thread_after` (the thread's full state including this message,
    // computed from the versioned retry loop's consistent read), so a reply
    // publishes the thread's real `message_count` rather than 1.
    msg.version = 0;
    msg.created_at = now.to_owned();
    msg.updated_at = now.to_owned();

    size::fit_item(&mut msg);

    state
        .services
        .insert_message(&msg)
        .await
        .map(|_outcome| ())
        .map_err(map_store_error)
}

/// The labels an inbound message carries, sorted (matching `plan_insert`'s
/// sorted-labels invariant).
fn verdict_labels(verdict: labels::InboundVerdict) -> Vec<String> {
    match verdict {
        labels::InboundVerdict::Spam => {
            vec![
                "received".to_owned(),
                "spam".to_owned(),
                "unread".to_owned(),
            ]
        }
        labels::InboundVerdict::Unauthenticated => vec![
            "received".to_owned(),
            "unauthenticated".to_owned(),
            "unread".to_owned(),
        ],
        labels::InboundVerdict::Clean => vec!["received".to_owned(), "unread".to_owned()],
    }
}

/// Whether SPF, DKIM or DMARC returned `FAIL`.
fn auth_failed(receipt: &SesReceipt) -> bool {
    let failed = |verdict: &Option<Verdict>| verdict.as_ref().is_some_and(|v| v.status == "FAIL");
    failed(&receipt.spf_verdict) || failed(&receipt.dkim_verdict) || failed(&receipt.dmarc_verdict)
}

/// A compact JSON summary of every verdict SES reported, stored on the
/// message item's `verdicts` attribute.
fn verdicts_json(receipt: &SesReceipt) -> serde_json::Value {
    let status = |verdict: &Option<Verdict>| verdict.as_ref().map(|v| v.status.clone());
    serde_json::json!({
        "spam": status(&receipt.spam_verdict),
        "virus": status(&receipt.virus_verdict),
        "spf": status(&receipt.spf_verdict),
        "dkim": status(&receipt.dkim_verdict),
        "dmarc": status(&receipt.dmarc_verdict),
        "dmarcPolicy": receipt.dmarc_policy,
    })
}

/// Strips a `Message-ID`-shaped header value's surrounding `<`/`>`, if
/// present — mirrors the private helper of the same name in `mail/plan.rs`,
/// which is what the RFC alias key is actually keyed on.
fn strip_angle_brackets(rfc_id: &str) -> &str {
    rfc_id
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(rfc_id)
}

fn object_key(message_id: &str, attachment_id: &str) -> String {
    format!("attachments/{message_id}/{attachment_id}")
}

/// Extracts the kept attachment parts into `attachments/<mid>/<att_id>`,
/// resuming a redelivery by skipping parts a prior attempt already put, in
/// chunks of 4 concurrent puts.
///
/// `tokio::join!` rather than a `JoinSet`: a `JoinSet` requires its spawned
/// futures to be `'static`, which this function's borrowed `&AppState<T>` is
/// not (the pipeline never passes an `Arc<AppState<T>>` down to this depth).
/// `join!` gives the same bounded 4-way concurrency and, holding no
/// independent task handles, a stronger cancellation guarantee when the
/// surrounding `tokio::time::timeout` elapses: every pending put is dropped
/// synchronously with the future tree, so no separate shutdown step is
/// needed to be sure no put is still running.
async fn put_kept_parts<T: Services>(
    state: &AppState<T>,
    message_id: &str,
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
async fn resume_filtered<'a, T: Services>(
    state: &AppState<T>,
    message_id: &str,
    attachments: &'a [(String, ParsedAttachment)],
) -> Result<Vec<&'a (String, ParsedAttachment)>, ActionError> {
    let (first_id, _) = &attachments[0];
    let first_present = state
        .services
        .head_object(&object_key(message_id, first_id))
        .await
        .map_err(map_object_error)?
        .is_some();

    if !first_present {
        return Ok(attachments.iter().collect());
    }

    let mut kept = Vec::with_capacity(attachments.len());
    for entry @ (attachment_id, _) in attachments {
        let present = state
            .services
            .head_object(&object_key(message_id, attachment_id))
            .await
            .map_err(map_object_error)?
            .is_some();
        if !present {
            kept.push(entry);
        }
    }
    Ok(kept)
}

async fn put_chunk<T: Services>(
    state: &AppState<T>,
    message_id: &str,
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

async fn put_one<T: Services>(
    state: &AppState<T>,
    message_id: &str,
    (attachment_id, attachment): &(String, ParsedAttachment),
) -> Result<(), ActionError> {
    let key = object_key(message_id, attachment_id);
    state
        .services
        .put_object_if_absent(
            &key,
            Bytes::clone(&attachment.bytes),
            &attachment.content_type,
        )
        .await
        .map(|_outcome| ())
        .map_err(map_object_error)
}

fn map_store_error(error: MailStoreError) -> ActionError {
    match error {
        MailStoreError::Transient(source) => ActionError::transient(source),
        MailStoreError::Conflict => ActionError::transient(anyhow::anyhow!("mail store conflict")),
        other => ActionError::permanent(anyhow::Error::from(other)),
    }
}

fn map_object_error(error: ObjectError) -> ActionError {
    match error {
        ObjectError::Transient(source) => ActionError::transient(source),
        other => ActionError::permanent(anyhow::Error::from(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::ActionErrorKind;
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

    #[test]
    fn strip_angle_brackets_removes_a_matched_pair_only() {
        assert_eq!(strip_angle_brackets("<id@example.com>"), "id@example.com");
        assert_eq!(strip_angle_brackets("id@example.com"), "id@example.com");
        assert_eq!(strip_angle_brackets("<id@example.com"), "<id@example.com");
    }

    #[test]
    fn object_key_has_the_documented_shape() {
        assert_eq!(object_key("mid-1", "att-1"), "attachments/mid-1/att-1");
    }

    #[test]
    fn map_store_error_classifies_transient_and_permanent() {
        assert_eq!(
            map_store_error(MailStoreError::Transient(anyhow::anyhow!("x"))).kind,
            ActionErrorKind::Transient
        );
        assert_eq!(
            map_store_error(MailStoreError::Conflict).kind,
            ActionErrorKind::Transient
        );
        assert_eq!(
            map_store_error(MailStoreError::NotFound).kind,
            ActionErrorKind::Permanent
        );
    }

    #[test]
    fn map_object_error_classifies_transient_and_permanent() {
        assert_eq!(
            map_object_error(ObjectError::Transient(anyhow::anyhow!("x"))).kind,
            ActionErrorKind::Transient
        );
        assert_eq!(
            map_object_error(ObjectError::NotFound).kind,
            ActionErrorKind::Permanent
        );
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
