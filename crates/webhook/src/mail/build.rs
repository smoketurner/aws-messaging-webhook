//! Assembling the outbound MIME document from a send spec and its parts.
//!
//! The sender does this, not the API, so a message is built from the bytes
//! that are actually in the outbox at send time — including URL-backed
//! attachments, which do not exist when the request is accepted.
//!
//! The structure is chosen by what the message actually contains, because an
//! unnecessary wrapper changes how some clients render it: a `multipart/*`
//! with a single child is always collapsed to that child.
//!
//! `Bcc` is never written as a header. It reaches its recipients because SES
//! is given the envelope separately; writing it would disclose the hidden
//! recipients to everyone else.

use mail_builder::MessageBuilder;
use mail_builder::headers::address::Address;
use mail_builder::headers::content_type::ContentType;
use mail_builder::mime::MimePart;

use crate::mail::send::{SendSpec, SpecAttachment};

/// One attachment's bytes, paired with the spec entry describing it.
pub struct BuiltPart<'a> {
    pub spec: &'a SpecAttachment,
    pub bytes: &'a [u8],
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("the assembled message is {size} bytes, over the limit of {limit}")]
    TooLarge { size: u64, limit: u64 },
    #[error("building the message failed")]
    Failed(#[source] anyhow::Error),
}

/// Builds the RFC 5322 document for `spec`.
///
/// `parts` must be in the spec's own order and cover every attachment: a
/// message is never sent with some of its attachments missing.
///
/// # Errors
///
/// [`BuildError::TooLarge`] when the result exceeds `max_bytes`, and
/// [`BuildError::Failed`] if serialization fails.
pub fn build_outbound(
    spec: &SendSpec,
    parts: &[BuiltPart<'_>],
    max_bytes: u64,
) -> Result<Vec<u8>, BuildError> {
    let body = body_part(spec, parts);

    let mut builder = MessageBuilder::new()
        .from(address(&spec.from, spec.display_name.as_deref()))
        .message_id(strip_angle_brackets(&spec.rfc_message_id))
        .subject(spec.subject.as_str())
        .body(body);

    if !spec.envelope.to.is_empty() {
        builder = builder.to(addresses(&spec.envelope.to));
    }
    if !spec.envelope.cc.is_empty() {
        builder = builder.cc(addresses(&spec.envelope.cc));
    }
    // `envelope.bcc` is deliberately not written: see the module docs.
    if !spec.reply_to.is_empty() {
        builder = builder.reply_to(addresses(&spec.reply_to));
    }
    if let Some(in_reply_to) = &spec.in_reply_to {
        builder = builder.in_reply_to(strip_angle_brackets(in_reply_to));
    }
    if !spec.references.is_empty() {
        let references: Vec<&str> = spec
            .references
            .iter()
            .map(|id| strip_angle_brackets(id))
            .collect();
        builder = builder.references(references);
    }
    // Caller headers last, but they can only ever be ones the API allowed
    // through, so this cannot overwrite anything set above.
    for (name, value) in &spec.headers {
        builder = builder.header(name.as_str(), mail_builder::headers::raw::Raw::new(value));
    }

    let built = builder
        .write_to_vec()
        .map_err(|e| BuildError::Failed(anyhow::anyhow!("serializing the message: {e}")))?;

    let size = built.len() as u64;
    if size > max_bytes {
        return Err(BuildError::TooLarge {
            size,
            limit: max_bytes,
        });
    }
    Ok(built)
}

/// Builds the body tree.
///
/// `multipart/alternative` holds the two body representations,
/// `multipart/related` adds the parts an HTML body references by `cid`, and
/// `multipart/mixed` adds ordinary attachments. Each wrapper appears only
/// when it has something to wrap.
fn body_part<'a>(spec: &'a SendSpec, parts: &'a [BuiltPart<'a>]) -> MimePart<'a> {
    let (inline, attached): (Vec<_>, Vec<_>) = parts
        .iter()
        .partition(|part| part.spec.content_disposition == "inline");

    let body = alternative(spec);

    let related = if inline.is_empty() {
        body
    } else {
        let mut children = vec![body];
        children.extend(inline.iter().map(|part| {
            let mut child = attachment_part(part);
            if let Some(cid) = &part.spec.content_id {
                child = child.inline().cid(cid.as_str());
            }
            child
        }));
        MimePart::new("multipart/related", children)
    };

    if attached.is_empty() {
        related
    } else {
        let mut children = vec![related];
        children.extend(attached.iter().map(|part| {
            let child = attachment_part(part);
            match &part.spec.filename {
                Some(filename) => child.attachment(filename.as_str()),
                None => child.header("Content-Disposition", ContentType::new("attachment")),
            }
        }));
        MimePart::new("multipart/mixed", children)
    }
}

/// The text and/or HTML body. Both present means `multipart/alternative`,
/// ordered worst-to-best as the standard requires: a client shows the last
/// part it understands.
fn alternative(spec: &SendSpec) -> MimePart<'_> {
    match (spec.text.as_deref(), spec.html.as_deref()) {
        (Some(text), Some(html)) => MimePart::new(
            "multipart/alternative",
            vec![
                MimePart::new("text/plain", text),
                MimePart::new("text/html", html),
            ],
        ),
        (Some(text), None) => MimePart::new("text/plain", text),
        (None, Some(html)) => MimePart::new("text/html", html),
        // Validation requires one of them, so this is unreachable from the
        // API; an empty body is still a valid document rather than a panic.
        (None, None) => MimePart::new("text/plain", ""),
    }
}

fn attachment_part<'a>(part: &'a BuiltPart<'a>) -> MimePart<'a> {
    MimePart::new(part.spec.content_type.as_str(), part.bytes)
}

fn address<'a>(email: &'a str, display_name: Option<&'a str>) -> Address<'a> {
    match display_name {
        Some(name) => Address::new_address(Some(name), email),
        None => Address::new_address(None::<&str>, email),
    }
}

fn addresses(list: &[String]) -> Address<'_> {
    Address::new_list(
        list.iter()
            .map(|email| Address::new_address(None::<&str>, email.as_str()))
            .collect(),
    )
}

/// `mail-builder` adds the angle brackets itself, so a stored id keeps only
/// its inner value here.
fn strip_angle_brackets(id: &str) -> &str {
    id.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use mail_parser::MessageParser;

    use std::collections::BTreeMap;

    use super::*;
    use crate::mail::InboxId;
    use crate::mail::send::Envelope;

    fn spec() -> SendSpec {
        SendSpec {
            message_id: "mid-1".to_owned(),
            thread_id: "tid-1".to_owned(),
            inbox_id: InboxId("support@example.com".to_owned()),
            from: "support@example.com".to_owned(),
            display_name: None,
            envelope: Envelope {
                to: vec!["to@example.net".to_owned()],
                cc: Vec::new(),
                bcc: Vec::new(),
            },
            reply_to: Vec::new(),
            subject: "Hello".to_owned(),
            text: Some("the body".to_owned()),
            html: None,
            rfc_message_id: "<mid-1@example.com>".to_owned(),
            in_reply_to: None,
            references: Vec::new(),
            headers: BTreeMap::new(),
            attachments: Vec::new(),
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
        }
    }

    fn part(id: &str, content_type: &str, disposition: &str, cid: Option<&str>) -> SpecAttachment {
        SpecAttachment {
            attachment_id: id.to_owned(),
            object_key: Some(format!("outbox/mid-1/parts/{id}")),
            url: None,
            filename: Some(format!("{id}.bin")),
            content_type: content_type.to_owned(),
            content_disposition: disposition.to_owned(),
            content_id: cid.map(ToOwned::to_owned),
            size: 3,
        }
    }

    fn build(spec: &SendSpec, parts: &[BuiltPart<'_>]) -> String {
        let bytes = build_outbound(spec, parts, 10_000_000).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn a_text_only_message_has_no_multipart_wrapper() {
        let raw = build(&spec(), &[]);
        let parsed = MessageParser::default().parse(raw.as_bytes()).unwrap();

        assert_eq!(parsed.subject(), Some("Hello"));
        assert_eq!(parsed.text_body_count(), 1);
        assert!(
            !raw.contains("multipart/"),
            "a single body needs no wrapper: {raw}"
        );
    }

    #[test]
    fn text_and_html_become_one_alternative_with_html_last() {
        // A client shows the last part it understands, so the richer
        // representation has to come second.
        let mut spec = spec();
        spec.html = Some("<p>the body</p>".to_owned());
        let raw = build(&spec, &[]);

        assert!(raw.contains("multipart/alternative"), "{raw}");
        let text_at = raw.find("text/plain").unwrap();
        let html_at = raw.find("text/html").unwrap();
        assert!(text_at < html_at, "html must come after text");
    }

    #[test]
    fn bcc_never_appears_as_a_header() {
        // The whole point of Bcc is that the other recipients cannot see it.
        let mut spec = spec();
        spec.envelope.bcc = vec!["hidden@example.net".to_owned()];
        let raw = build(&spec, &[]);

        assert!(!raw.contains("hidden@example.net"), "{raw}");
        assert!(!raw.to_lowercase().contains("bcc:"), "{raw}");
    }

    #[test]
    fn an_attachment_is_wrapped_in_mixed_and_keeps_its_filename() {
        let mut spec = spec();
        let attachment = part("att_1", "application/pdf", "attachment", None);
        spec.attachments = vec![attachment.clone()];
        let parts = vec![BuiltPart {
            spec: &attachment,
            bytes: b"pdf",
        }];

        let raw = build(&spec, &parts);
        let parsed = MessageParser::default().parse(raw.as_bytes()).unwrap();

        assert!(raw.contains("multipart/mixed"), "{raw}");
        assert_eq!(parsed.attachment_count(), 1);
        assert!(raw.contains("att_1.bin"), "{raw}");
    }

    /// A filename-less attachment is still an attachment, so it has to carry
    /// `Content-Disposition: attachment` — without it, RFC 2183's default
    /// disposition is `inline`, and a strict client renders the part in the
    /// body instead of offering it as a download. `mail-builder`'s
    /// `.attachment()` requires a filename, so the disposition is written as a
    /// raw header in that branch.
    #[test]
    fn a_filenameless_attachment_still_emits_a_disposition_header() {
        let mut spec = spec();
        let mut attachment = part("att_1", "application/pdf", "attachment", None);
        attachment.filename = None;
        spec.attachments = vec![attachment.clone()];
        let parts = vec![BuiltPart {
            spec: &attachment,
            bytes: b"pdf",
        }];

        let raw = build(&spec, &parts);
        let parsed = MessageParser::default().parse(raw.as_bytes()).unwrap();

        assert!(raw.contains("Content-Disposition: attachment"), "{raw}");
        assert_eq!(
            raw.matches("Content-Disposition").count(),
            1,
            "one disposition header per part: {raw}"
        );
        assert_eq!(
            parsed.attachment_count(),
            1,
            "the part must parse as an attachment: {raw}"
        );
        assert!(raw.contains("multipart/mixed"), "{raw}");
    }

    /// The fix writes the disposition header only in the `None` arm, so the
    /// `Some(filename)` path still goes through `.attachment(filename)` — and
    /// because `mail-builder` pushes rather than replaces the header, that
    /// part must keep exactly one `Content-Disposition` (no duplicates that a
    /// strict client would reject or that `mail-parser` would misread).
    #[test]
    fn an_attachment_with_a_filename_has_exactly_one_disposition_header() {
        let mut spec = spec();
        let attachment = part("att_1", "application/pdf", "attachment", None);
        spec.attachments = vec![attachment.clone()];
        let parts = vec![BuiltPart {
            spec: &attachment,
            bytes: b"pdf",
        }];

        let raw = build(&spec, &parts);

        assert_eq!(
            raw.matches("Content-Disposition").count(),
            1,
            "exactly one disposition header even with a filename: {raw}"
        );
        assert!(
            raw.contains("Content-Disposition: attachment;"),
            "the disposition should carry the filename: {raw}"
        );
    }

    #[test]
    fn an_inline_part_is_related_to_the_body_and_keeps_its_cid() {
        // An HTML body references it by cid, so the reference has to survive
        // and the part has to sit in the same `related` tree as the body.
        let mut spec = spec();
        spec.html = Some("<img src=\"cid:logo\">".to_owned());
        let attachment = part("att_1", "image/png", "inline", Some("logo"));
        spec.attachments = vec![attachment.clone()];
        let parts = vec![BuiltPart {
            spec: &attachment,
            bytes: b"png",
        }];

        let raw = build(&spec, &parts);

        assert!(raw.contains("multipart/related"), "{raw}");
        assert!(raw.contains("logo"), "the cid must survive: {raw}");
        assert!(!raw.contains("multipart/mixed"), "nothing to mix: {raw}");
    }

    #[test]
    fn inline_and_attached_parts_nest_rather_than_flatten() {
        let mut spec = spec();
        spec.html = Some("<img src=\"cid:logo\">".to_owned());
        let inline = part("att_1", "image/png", "inline", Some("logo"));
        let attached = part("att_2", "application/pdf", "attachment", None);
        spec.attachments = vec![inline.clone(), attached.clone()];
        let parts = vec![
            BuiltPart {
                spec: &inline,
                bytes: b"png",
            },
            BuiltPart {
                spec: &attached,
                bytes: b"pdf",
            },
        ];

        let raw = build(&spec, &parts);

        assert!(raw.contains("multipart/mixed"), "{raw}");
        assert!(raw.contains("multipart/related"), "{raw}");
        // mixed wraps related, not the other way round.
        assert!(raw.find("multipart/mixed").unwrap() < raw.find("multipart/related").unwrap());
    }

    #[test]
    fn threading_headers_are_written_without_double_brackets() {
        let mut spec = spec();
        spec.in_reply_to = Some("<original@example.net>".to_owned());
        spec.references = vec!["<first@example.net>".to_owned()];

        let raw = build(&spec, &[]);
        let parsed = MessageParser::default().parse(raw.as_bytes()).unwrap();

        assert!(!raw.contains("<<"), "brackets must not be doubled: {raw}");
        assert_eq!(parsed.in_reply_to().as_text(), Some("original@example.net"));
        assert_eq!(parsed.message_id(), Some("mid-1@example.com"));
    }

    #[test]
    fn a_permitted_custom_header_survives() {
        let mut spec = spec();
        spec.headers
            .insert("X-Campaign".to_owned(), "spring".to_owned());

        let raw = build(&spec, &[]);
        let parsed = MessageParser::default().parse(raw.as_bytes()).unwrap();

        assert_eq!(
            parsed.header("X-Campaign").and_then(|h| h.as_text()),
            Some("spring")
        );
    }

    #[test]
    fn an_oversized_message_is_refused_rather_than_sent() {
        let mut spec = spec();
        spec.text = Some("x".repeat(1_000));
        let error = build_outbound(&spec, &[], 100).unwrap_err();
        assert!(matches!(error, BuildError::TooLarge { .. }));
    }
}
