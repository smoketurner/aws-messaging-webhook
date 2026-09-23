//! Turning a validated send into what the outbox holds: the spec and its
//! inline parts in S3, and the queued message item the sender picks up.

use std::collections::BTreeMap;

use axum::body::Bytes;
use mail_parser::parsers::preview::{preview_html, preview_text};

use crate::api::error::ApiError;
use crate::api::send::validate::{AttachmentSource, ValidatedSend};
use crate::mail::labels::SystemLabel;
use crate::mail::send::{self as send_mod, Envelope, SendSpec, SendStatus, SpecAttachment};
use crate::mail::{AttachmentMeta, Direction, InboxId, MailMessage, PREVIEW_CHARS, ids};
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

/// The snippet stored on the queued message, derived the same way the
/// inbound path derives it: `mail_parser`'s `preview_text` over `send.text`,
/// falling back to `preview_html` over `send.html` when there is no text
/// body. An HTML-only send is a first-class, validation-accepted shape
/// (`text.is_none() && html.is_none()` is the only case validation rejects),
/// so it must get a non-empty snippet — and matching the inbound
/// `body_preview` derivation keeps the two listings consistent for the same
/// body, instead of hand-rolling a tag stripper that would not collapse
/// whitespace or decode entities the way `preview_html` does.
pub(super) fn preview_of(send: &ValidatedSend) -> String {
    let preview = match send.text.as_deref() {
        Some(text) => preview_text(text.into(), PREVIEW_CHARS),
        None => send
            .html
            .as_deref()
            .map(|html| preview_html(html.into(), PREVIEW_CHARS))
            .unwrap_or_default(),
    };
    preview.into_owned()
}

/// Removes what a send uploaded under `outbox/` before its commit failed: the
/// spec and its inline parts. Nothing under `outbox/` expires, so leaving
/// them would keep them forever; the outbox-only delete grant this role holds
/// is exactly what makes these deletes succeed. The content document under
/// `messages/` is deliberately not touched: that prefix is reclaimed by the
/// bucket's `expire-messages` lifecycle rule, so deleting it here would only
/// add a no-op call (and a noisy warning under a narrower grant). Best effort:
/// the request has already failed, and a leftover object is harmless.
pub(super) async fn discard_uploads<T: Services>(services: &T, spec: &SendSpec) {
    let mut keys = vec![send_mod::spec_key(&spec.message_id)];
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

#[cfg(test)]
mod tests {
    use super::preview_of;
    use crate::api::send::validate::ValidatedSend;
    use crate::mail::PREVIEW_CHARS;

    /// A `ValidatedSend` carrying only the bodies `preview_of` reads; every
    /// other field is empty because `preview_of` ignores it. Mirrors the
    /// `body()` fixture shape used by `tests/api_send.rs`, minimized to the
    /// two fields under test.
    fn send(text: Option<&str>, html: Option<&str>) -> ValidatedSend {
        ValidatedSend {
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            reply_to: Vec::new(),
            subject: String::new(),
            text: text.map(ToOwned::to_owned),
            html: html.map(ToOwned::to_owned),
            labels: Vec::new(),
            headers: std::collections::BTreeMap::new(),
            attachments: Vec::new(),
        }
    }

    /// When both bodies are present, text wins — matching the inbound
    /// `body_preview`, which prefers the text part over the HTML part. This is
    /// the only case the old text-only implementation happened to get right;
    /// it must not regress.
    #[test]
    fn text_takes_precedence_over_html_when_both_present() {
        let preview = preview_of(&send(Some("plain body"), Some("<p>html body</p>")));
        assert_eq!(preview, "plain body");
    }

    /// `preview_html` strips tags and decodes entities (collapsing whitespace
    /// the way the inbound path does), rather than naively truncating the
    /// raw markup — a hand-rolled tag strip would not match inbound.
    #[test]
    fn html_only_send_preview_strips_tags_and_decodes_entities() {
        let preview = preview_of(&send(None, Some("<p>Hi &amp; <b>welcome</b></p>")));
        assert!(
            preview.contains("Hi & welcome"),
            "expected entity-decoded, tag-stripped text, got {preview:?}"
        );
        assert!(
            !preview.contains('<') && !preview.contains("&amp;"),
            "preview should not retain raw HTML tags or entities, got {preview:?}"
        );
    }

    /// Long text is truncated the same way the inbound path truncates it:
    /// `mail_parser`'s `preview_text` caps the result at `PREVIEW_CHARS` and
    /// appends `...` when the body exceeds the cap. The old implementation took
    /// `PREVIEW_CHARS` chars with no ellipsis, so inbound and outbound
    /// previews differed for a long body; this pins them together.
    #[test]
    fn long_text_preview_is_truncated_with_ellipsis_like_inbound() {
        let body = "a".repeat(PREVIEW_CHARS + 44);
        let preview = preview_of(&send(Some(&body), None));
        assert!(
            preview.ends_with("..."),
            "long text preview should end with '...', got {preview:?}"
        );
        assert_eq!(
            preview.len(),
            PREVIEW_CHARS,
            "long text preview should cap at PREVIEW_CHARS (253 chars + '...')"
        );
    }

    /// Outbound and inbound must derive the same preview for the same body.
    /// Inbound parses raw MIME and calls `mail_parser::Message::body_preview`
    /// (`mime.rs`), which for an HTML-only message runs `preview_html`; the
    /// outbound path now runs the same `preview_html`. Feeding the same HTML
    /// to both must produce identical snippets — the strongest form of the
    /// parity guarantee, and a regression guard against any future divergence
    /// between the two derivations.
    #[test]
    fn outbound_preview_matches_inbound_preview_for_the_same_html_body() {
        use crate::mail::mime::parse_inbound;

        let html = "<p>Hello <strong>there</strong> &amp; welcome</p>";
        let outbound = preview_of(&send(None, Some(html)));

        // A minimal HTML-only MIME message carrying the same body.
        let raw = format!(
            "From: alice@example.com\r\n\
             To: support@example.com\r\n\
             Subject: HTML only\r\n\
             Message-ID: <html-only-1@example.com>\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/html; charset=utf-8\r\n\
             \r\n\
             {html}"
        );
        let inbound = parse_inbound(raw.as_bytes()).unwrap().message.preview;

        assert_eq!(
            outbound, inbound,
            "outbound and inbound previews must match for the same HTML body"
        );
        assert!(
            !outbound.is_empty(),
            "the matched preview must be non-empty for an HTML-only body"
        );
    }
}
