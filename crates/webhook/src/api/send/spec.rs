//! Turning a validated send into what the outbox holds: the spec and its
//! inline parts in S3, and the queued message item the sender picks up.

use std::collections::BTreeMap;

use axum::body::Bytes;

use crate::api::error::ApiError;
use crate::api::send::validate::{AttachmentSource, ValidatedSend};
use crate::mail::labels::SystemLabel;
use crate::mail::send::{self as send_mod, Envelope, SendSpec, SendStatus, SpecAttachment};
use crate::mail::{AttachmentMeta, Direction, InboxId, MailMessage, PREVIEW_CHARS, content, ids};
use crate::state::Services;

/// Builds the spec the sender will assemble the message from.
pub(super) fn build_spec(
    inbox: &InboxId,
    send: &ValidatedSend,
    message_id: &str,
    thread_id: &str,
    now: &str,
) -> SendSpec {
    let uuid = uuid::Uuid::parse_str(message_id).unwrap_or_else(|_| uuid::Uuid::nil());
    let attachments = send
        .attachments
        .iter()
        .enumerate()
        .map(|(ordinal, attachment)| {
            let attachment_id = ids::attachment_id(&uuid, ordinal);
            let (object_key, url, size) = match &attachment.source {
                // Inline bytes are uploaded under the id before the send is
                // queued, so the sender finds them already there.
                AttachmentSource::Inline(bytes) => (
                    Some(send_mod::part_key(message_id, &attachment_id)),
                    None,
                    bytes.len() as u64,
                ),
                AttachmentSource::Url(url) => (None, Some(url.as_str().to_owned()), 0),
            };
            SpecAttachment {
                attachment_id,
                object_key,
                url,
                filename: attachment.filename.clone(),
                content_type: attachment.content_type.clone(),
                content_disposition: attachment.content_disposition.clone(),
                content_id: attachment.content_id.clone(),
                size,
            }
        })
        .collect();

    SendSpec {
        message_id: message_id.to_owned(),
        thread_id: thread_id.to_owned(),
        inbox_id: inbox.clone(),
        from: inbox.as_str().to_owned(),
        display_name: None,
        envelope: Envelope {
            to: send.to.clone(),
            cc: send.cc.clone(),
            bcc: send.bcc.clone(),
        },
        reply_to: send.reply_to.clone(),
        subject: send.subject.clone(),
        text: send.text.clone(),
        html: send.html.clone(),
        rfc_message_id: ids::our_rfc_message_id(message_id, inbox.domain()),
        in_reply_to: None,
        references: Vec::new(),
        headers: send.headers.clone(),
        attachments,
        created_at: now.to_owned(),
    }
}

/// Uploads every inline part, then the spec.
///
/// The spec goes last on purpose: the sender treats it as the signal that a
/// send is ready to build, so it must never be visible before the parts it
/// refers to.
pub(super) async fn upload_spec<T: Services>(
    services: &T,
    spec: &SendSpec,
    send: &ValidatedSend,
) -> Result<(), ApiError> {
    for (attachment, spec_attachment) in send.attachments.iter().zip(&spec.attachments) {
        let AttachmentSource::Inline(bytes) = &attachment.source else {
            continue;
        };
        let Some(key) = &spec_attachment.object_key else {
            continue;
        };
        services
            .put_object_if_absent(
                key,
                Bytes::from(bytes.clone()),
                &spec_attachment.content_type,
            )
            .await
            .map_err(ApiError::from)?;
    }

    let body = serde_json::to_vec(spec)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("serializing the send spec: {e}")))?;
    services
        .put_object_if_absent(
            &send_mod::spec_key(&spec.message_id),
            Bytes::from(body),
            "application/json",
        )
        .await
        .map_err(ApiError::from)?;
    Ok(())
}

/// The message item a queued send writes: everything a reader needs, with the
/// body kept as the caller gave it. `size` is zero until the sender has built
/// the real MIME.
pub(super) fn queued_message(
    inbox: &InboxId,
    send: &ValidatedSend,
    spec: &SendSpec,
    now: &str,
) -> MailMessage {
    let mut labels = send.labels.clone();
    labels.push(SystemLabel::Queued.to_label());
    labels.sort();
    labels.dedup();

    MailMessage {
        inbox_id: inbox.clone(),
        thread_id: spec.thread_id.clone(),
        message_id: spec.message_id.clone(),
        ses_message_id: None,
        direction: Direction::Outbound,
        rfc_message_id: spec.rfc_message_id.clone(),
        in_reply_to: spec.in_reply_to.clone(),
        labels,
        timestamp: now.to_owned(),
        from: spec.from.clone(),
        to: send.to.clone(),
        cc: send.cc.clone(),
        bcc: send.bcc.clone(),
        subject: send.subject.clone(),
        preview: preview_of(send),
        size: 0,
        attachments: spec
            .attachments
            .iter()
            .map(|attachment| AttachmentMeta {
                attachment_id: attachment.attachment_id.clone(),
                object_key: attachment.object_key.clone(),
                size: attachment.size,
                filename: attachment.filename.clone(),
                content_type: attachment.content_type.clone(),
                content_disposition: attachment.content_disposition.clone(),
                content_id: attachment.content_id.clone(),
            })
            .collect(),
        attachments_truncated: false,
        raw_s3_key: None,
        thread_snapshot: None,
        delivery: BTreeMap::new(),
        send_status: Some(SendStatus::Queued.as_str().to_owned()),
        sent_at: None,
        version: 0,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
        expires_at: 0,
    }
}

pub(super) fn preview_of(send: &ValidatedSend) -> String {
    send.text
        .as_deref()
        .unwrap_or_default()
        .chars()
        .take(PREVIEW_CHARS)
        .collect()
}

/// Removes what a send uploaded before its commit failed: the spec, inline
/// parts and content document. Nothing under `outbox/` expires, so leaving
/// them would keep them forever. Best effort: the request has already failed,
/// and a leftover object is harmless.
pub(super) async fn discard_uploads<T: Services>(services: &T, inbox: &InboxId, spec: &SendSpec) {
    let mut keys = vec![
        send_mod::spec_key(&spec.message_id),
        content::content_key(inbox, &spec.message_id),
    ];
    keys.extend(
        spec.attachments
            .iter()
            .filter_map(|attachment| attachment.object_key.clone()),
    );
    for key in keys {
        if let Err(error) = services.delete_object(&key).await {
            tracing::warn!(
                message_id = %spec.message_id,
                key,
                error = ?error,
                event = "send_upload_cleanup_failed",
                "could not remove an object uploaded for a send that was not queued"
            );
        }
    }
}
