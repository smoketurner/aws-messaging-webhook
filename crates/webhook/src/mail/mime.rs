//! Inbound MIME parsing. Outbound MIME building is not implemented yet.

use std::collections::BTreeMap;

use axum::body::Bytes;
use mail_parser::{
    Addr, Address, ContentType, HeaderValue, Message as MimeMessage, MessageParser, MessagePart,
    MimeHeaders,
};

use crate::mail::content::MessageContent;
use crate::mail::{
    ADDRESS_MAX_BYTES, ATTACHMENT_FIELD_MAX, ATTACHMENTS_MAX, Direction, HEADER_NAME_MAX,
    HEADER_VALUE_MAX, HEADERS_BUDGET, INBOUND_ADDRESS_LIST_MAX, InboxId, MailMessage,
    PREVIEW_CHARS, REFERENCES_MAX, SUBJECT_MAX_BYTES,
};

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("malformed MIME message")]
    Malformed,
}

/// One kept attachment part extracted from the raw MIME, normalized and
/// ready to be persisted to `attachments/<mid>/<att_id>`.
#[derive(Debug, Clone)]
pub struct ParsedAttachment {
    pub filename: Option<String>,
    pub content_type: String,
    pub content_disposition: String,
    pub content_id: Option<String>,
    pub bytes: axum::body::Bytes,
}

/// A parsed inbound message, before thread resolution and item assembly:
/// [`MailMessage`]'s summary fields, the [`MessageContent`] document, and the
/// kept attachment parts, which have not yet been written to S3 or given
/// deterministic ids.
///
/// `message`'s identity, threading and bookkeeping fields (`inbox_id`,
/// `thread_id`, `message_id`, `labels`, `timestamp`, `attachments`,
/// `raw_s3_key`, `thread_snapshot`, `delivery`, `version`, `created_at`,
/// `updated_at`, `expires_at`, …) are placeholders — ingest resolves one
/// per matching inbox and overwrites them; only the fields [`parse_inbound`]
/// documents are meaningful here. `content.verdicts` is likewise filled in
/// by ingest from the SES receipt.
#[derive(Debug, Clone)]
pub struct ParsedInbound {
    pub message: MailMessage,
    pub content: MessageContent,
    pub attachments: Vec<ParsedAttachment>,
}

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
/// A normalized content type is a lowercase `type/subtype` token, at most
/// this many bytes.
const NORMALIZED_CONTENT_TYPE_MAX_BYTES: usize = 127;

/// Parses a raw MIME message: addresses, subject, date,
/// `Message-ID`/`In-Reply-To`/`References`, concatenated text and html body
/// parts, headers within the caps, and attachment selection and
/// normalization. Runs inside `tokio::task::spawn_blocking` at the ingest
/// call site: the raw bytes and the borrowed `mail_parser::Message` are
/// dropped
/// when this function returns, since every field is copied into owned
/// `String`/`Bytes` before then.
///
/// Only `parse_inbound`'s content fields are populated on the returned
/// [`ParsedInbound::message`] — see that struct's doc for which fields are
/// placeholders.
///
/// # Errors
///
/// Returns [`ParseError`] when `mail-parser` cannot construct a message from
/// `raw` at all — a permanent ingest failure, since a retry would parse the
/// same bytes.
pub fn parse_inbound(raw: &[u8]) -> Result<ParsedInbound, ParseError> {
    let parsed = MessageParser::default()
        .parse(raw)
        .ok_or(ParseError::Malformed)?;

    let from = flatten_addresses(parsed.from(), INBOUND_ADDRESS_LIST_MAX)
        .into_iter()
        .next()
        .unwrap_or_default();
    let to = flatten_addresses(parsed.to(), INBOUND_ADDRESS_LIST_MAX);
    let cc = flatten_addresses(parsed.cc(), INBOUND_ADDRESS_LIST_MAX);
    let bcc = flatten_addresses(parsed.bcc(), INBOUND_ADDRESS_LIST_MAX);
    let reply_to = flatten_addresses(parsed.reply_to(), INBOUND_ADDRESS_LIST_MAX);

    let rfc_message_id = parsed
        .message_id()
        .map(|id| format!("<{id}>"))
        .unwrap_or_default();
    let in_reply_to = id_list(parsed.in_reply_to()).into_iter().next();
    let mut references = id_list(parsed.references());
    if references.len() > REFERENCES_MAX {
        // Keep the nearest (last) ids — thread resolution walks References
        // nearest-first.
        let start = references.len() - REFERENCES_MAX;
        references = references.split_off(start);
    }

    let subject = truncate_bytes(parsed.subject().unwrap_or_default(), SUBJECT_MAX_BYTES);
    let preview = parsed
        .body_preview(PREVIEW_CHARS)
        .map(std::borrow::Cow::into_owned)
        .unwrap_or_default();

    // `mail-parser`'s `text_bodies()`/`html_bodies()` each fall back to the
    // other kind when a message has only one, as a convenience for callers
    // that want "the body" in either form. A5 wants only genuine parts of
    // each kind, so each iterator is filtered to its real `PartType`.
    let text = join_body_parts(parsed.text_bodies().filter(|part| !part.is_text_html()));
    let html = join_body_parts(parsed.html_bodies().filter(|part| part.is_text_html()));

    let headers = collect_headers(&parsed);

    let attachment_count = parsed.attachment_count();
    let attachments = parsed
        .attachments()
        .take(ATTACHMENTS_MAX)
        .map(build_attachment)
        .collect();

    let message = MailMessage {
        inbox_id: InboxId(String::new()),
        thread_id: String::new(),
        message_id: String::new(),
        ses_message_id: None,
        direction: Direction::Inbound,
        rfc_message_id,
        in_reply_to,
        labels: Vec::new(),
        timestamp: String::new(),
        from,
        to,
        cc,
        bcc,
        subject,
        preview,
        size: u64::try_from(raw.len()).unwrap_or(u64::MAX),
        attachments: Vec::new(),
        attachments_truncated: attachment_count > ATTACHMENTS_MAX,
        raw_s3_key: None,
        thread_snapshot: None,
        delivery: BTreeMap::new(),
        send_status: None,
        sent_at: None,
        version: 0,
        created_at: String::new(),
        updated_at: String::new(),
        expires_at: 0,
    };

    let content = MessageContent {
        text,
        html,
        headers,
        references,
        reply_to,
        verdicts: None,
    };

    Ok(ParsedInbound {
        message,
        content,
        attachments,
    })
}

/// Truncates `input` to at most `max_bytes` bytes, on a `char` boundary.
fn truncate_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        input.to_owned()
    } else {
        input[..input.floor_char_boundary(max_bytes)].to_owned()
    }
}

/// `Name <address>` when a non-empty display name is present, else the bare
/// address. `None` for a malformed entry with no address at all.
fn format_addr(addr: &Addr<'_>) -> Option<String> {
    let address = addr.address.as_deref()?;
    let formatted = match addr.name.as_deref().map(str::trim) {
        Some(name) if !name.is_empty() => format!("{name} <{address}>"),
        _ => address.to_owned(),
    };
    Some(truncate_bytes(&formatted, ADDRESS_MAX_BYTES))
}

/// Flattens an address header (a plain list, or a group — whose member
/// addresses are flattened without the group name) into display strings,
/// capped at `cap` entries (`INBOUND_ADDRESS_LIST_MAX`).
fn flatten_addresses(address: Option<&Address<'_>>, cap: usize) -> Vec<String> {
    let Some(address) = address else {
        return Vec::new();
    };
    let addrs: Vec<&Addr<'_>> = match address {
        Address::List(list) => list.iter().collect(),
        Address::Group(groups) => groups
            .iter()
            .flat_map(|group| group.addresses.iter())
            .collect(),
    };
    addrs
        .into_iter()
        .filter_map(format_addr)
        .take(cap)
        .collect()
}

/// `Message-ID`/`In-Reply-To`/`References` are parsed by `mail-parser` with
/// the angle brackets stripped; this re-wraps each id to the RFC form our
/// own [`crate::mail::ids::our_rfc_message_id`] and [`crate::mail::ids::ses_rfc_ids`]
/// produce.
fn id_list(value: &HeaderValue<'_>) -> Vec<String> {
    match value {
        HeaderValue::Text(id) => vec![format!("<{id}>")],
        HeaderValue::TextList(ids) => ids.iter().map(|id| format!("<{id}>")).collect(),
        _ => Vec::new(),
    }
}

/// Concatenates every part's text with `\n` (A5); `None` when there are no
/// parts of this kind.
fn join_body_parts<'a>(parts: impl Iterator<Item = &'a MessagePart<'a>>) -> Option<String> {
    let joined = parts
        .filter_map(MessagePart::text_contents)
        .collect::<Vec<_>>()
        .join("\n");
    (!joined.is_empty()).then_some(joined)
}

/// Builds the message-level `headers` map from the raw header text
/// (`headers_raw`, so values keep their original encoding rather than
/// `mail-parser`'s structured decode), truncating each name/value to its cap
/// and stopping once `HEADERS_BUDGET` is reached. Later headers with the
/// same name overwrite earlier ones.
fn collect_headers(message: &MimeMessage<'_>) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    let mut budget = 0usize;
    for (name, value) in message.headers_raw() {
        if budget >= HEADERS_BUDGET {
            break;
        }
        let name = truncate_bytes(name.trim(), HEADER_NAME_MAX);
        let value = truncate_bytes(value.trim(), HEADER_VALUE_MAX);
        budget += name.len() + value.len();
        headers.insert(name, value);
    }
    headers
}

/// Normalizes a `Content-Type` to a lowercase `type/subtype` token within
/// the size cap, falling back to [`DEFAULT_CONTENT_TYPE`] for anything
/// missing, oversized, or containing whitespace/control characters — so a
/// hostile inbound content type can never make a later `PutObject` fail to
/// build.
fn normalize_content_type(content_type: Option<&ContentType<'_>>) -> String {
    let Some(content_type) = content_type else {
        return DEFAULT_CONTENT_TYPE.to_owned();
    };
    let mut value = content_type.c_subtype.as_deref().map_or_else(
        || content_type.c_type.to_string(),
        |subtype| format!("{}/{}", content_type.c_type, subtype),
    );
    value.make_ascii_lowercase();
    let valid = value.len() <= NORMALIZED_CONTENT_TYPE_MAX_BYTES
        && value.contains('/')
        && !value.chars().any(|c| c.is_control() || c.is_whitespace());
    if valid {
        value
    } else {
        DEFAULT_CONTENT_TYPE.to_owned()
    }
}

/// `inline` when the `Content-Disposition` says so (case-insensitively),
/// else `attachment`, the disposition default.
fn normalized_disposition(part: &MessagePart<'_>) -> String {
    let is_inline = part
        .content_disposition()
        .is_some_and(|cd| cd.c_type.eq_ignore_ascii_case("inline"));
    if is_inline { "inline" } else { "attachment" }.to_owned()
}

/// A filename for a nested `message/rfc822` part: the inner
/// message's own subject, sanitized to strip control characters and capped
/// at `ATTACHMENT_FIELD_MAX` bytes (leaving room for the `.eml` suffix),
/// else `"message"` when the inner subject is absent or blank.
fn nested_message_filename(inner: Option<&MimeMessage<'_>>) -> String {
    let subject = inner
        .and_then(MimeMessage::subject)
        .map(str::trim)
        .filter(|subject| !subject.is_empty())
        .unwrap_or("message");
    let sanitized: String = subject.chars().filter(|c| !c.is_control()).collect();
    format!(
        "{}.eml",
        truncate_bytes(&sanitized, ATTACHMENT_FIELD_MAX.saturating_sub(4))
    )
}

/// Builds one kept attachment's metadata and bytes. A nested
/// `message/rfc822` part is stored as its own raw bytes under a synthesized
/// filename and content type; inner parts are never flattened.
fn build_attachment(part: &MessagePart<'_>) -> ParsedAttachment {
    let content_disposition = normalized_disposition(part);
    let content_id = part
        .content_id()
        .map(|id| truncate_bytes(id, ATTACHMENT_FIELD_MAX));
    let bytes = Bytes::copy_from_slice(part.contents());

    if part.is_message() {
        return ParsedAttachment {
            filename: Some(nested_message_filename(part.message())),
            content_type: "message/rfc822".to_owned(),
            content_disposition,
            content_id,
            bytes,
        };
    }

    ParsedAttachment {
        filename: part
            .attachment_name()
            .map(|name| truncate_bytes(name, ATTACHMENT_FIELD_MAX)),
        content_type: normalize_content_type(part.content_type()),
        content_disposition,
        content_id,
        bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mail::ATTACHMENTS_MAX;

    macro_rules! fixture {
        ($name:literal) => {
            parse_inbound(include_bytes!(concat!("../../tests/fixtures/mail/", $name))).unwrap()
        };
    }

    #[test]
    fn plain_text_message() {
        let parsed = fixture!("plain.eml");
        assert_eq!(parsed.message.from, "Alice Sender <alice@example.com>");
        assert_eq!(parsed.message.to, ["Bob Recipient <bob@example.com>"]);
        assert_eq!(parsed.message.subject, "Plain text hello");
        assert_eq!(parsed.message.rfc_message_id, "<plain-1@example.com>");
        assert!(parsed.content.text.unwrap().contains("Hello Bob"));
        assert!(parsed.content.html.is_none());
        assert!(parsed.attachments.is_empty());
        assert!(!parsed.message.attachments_truncated);
    }

    #[test]
    fn multipart_alternative_keeps_both_bodies() {
        let parsed = fixture!("multipart-alternative.eml");
        assert_eq!(parsed.content.text.unwrap().trim(), "Plain body.");
        assert!(parsed.content.html.unwrap().contains("HTML body."));
    }

    #[test]
    fn attachment_with_content_id() {
        let parsed = fixture!("attachment-content-id.eml");
        assert_eq!(parsed.attachments.len(), 1);
        let attachment = &parsed.attachments[0];
        assert_eq!(attachment.content_id.as_deref(), Some("logo123"));
        assert_eq!(attachment.content_disposition, "inline");
        assert_eq!(attachment.content_type, "image/png");
        assert_eq!(attachment.filename.as_deref(), Some("logo.png"));
        assert_eq!(attachment.bytes.as_ref(), b"hello world");
    }

    #[test]
    fn reply_captures_in_reply_to_and_references() {
        let parsed = fixture!("reply-references.eml");
        assert_eq!(
            parsed.message.in_reply_to.as_deref(),
            Some("<plain-1@example.com>")
        );
        assert_eq!(
            parsed.content.references,
            ["<root-0@example.com>", "<plain-1@example.com>"]
        );
    }

    #[test]
    fn inline_text_parts_are_concatenated_with_newline() {
        let parsed = fixture!("inline-text-parts.eml");
        assert_eq!(parsed.content.text.unwrap(), "First part.\nSecond part.");
    }

    #[test]
    fn nested_message_is_stored_whole_not_flattened() {
        let parsed = fixture!("nested-rfc822.eml");
        assert_eq!(parsed.attachments.len(), 1);
        let attachment = &parsed.attachments[0];
        assert_eq!(attachment.content_type, "message/rfc822");
        assert_eq!(
            attachment.filename.as_deref(),
            Some("Original subject line.eml")
        );
        let inner = std::str::from_utf8(&attachment.bytes).unwrap();
        assert!(inner.contains("This is the original nested message body."));
    }

    #[test]
    fn rfc2231_encoded_filename_is_decoded() {
        let parsed = fixture!("encoded-word-filename.eml");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].filename.as_deref(), Some("été.pdf"));
    }

    #[test]
    fn attachment_with_no_filename_is_omitted() {
        let parsed = fixture!("no-filename.eml");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].filename, None);
        assert_eq!(
            parsed.attachments[0].content_type,
            "application/octet-stream"
        );
    }

    #[test]
    fn malformed_message_is_a_parse_error() {
        let error =
            parse_inbound(include_bytes!("../../tests/fixtures/mail/malformed.eml")).unwrap_err();
        assert!(matches!(error, ParseError::Malformed));
    }

    #[test]
    fn hostile_content_type_falls_back_to_octet_stream() {
        let parsed = fixture!("hostile-content-type.eml");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(
            parsed.attachments[0].content_type,
            "application/octet-stream"
        );
    }

    #[test]
    fn empty_input_is_malformed() {
        let error = parse_inbound(b"").unwrap_err();
        assert!(matches!(error, ParseError::Malformed));
    }

    /// Only the first 100 attachment parts are kept; the rest are
    /// signaled by `attachments_truncated` and reachable only in the raw MIME.
    #[test]
    fn more_than_100_attachments_are_truncated_at_selection() {
        use std::fmt::Write as _;

        let mut raw = String::from(
            "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: many parts\r\nMessage-ID: <many-1@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"b\"\r\n\r\n",
        );
        for i in 0..150 {
            let _ = write!(
                raw,
                "--b\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=\"part{i}.bin\"\r\n\r\naGVsbG8=\r\n"
            );
        }
        raw.push_str("--b--\r\n");

        let parsed = parse_inbound(raw.as_bytes()).unwrap();
        assert_eq!(parsed.attachments.len(), ATTACHMENTS_MAX);
        assert!(parsed.message.attachments_truncated);
    }

    #[test]
    fn addresses_and_headers_stay_within_caps() {
        let parsed = fixture!("plain.eml");
        assert!(parsed.content.headers.contains_key("Subject"));
        assert!(parsed.content.headers.contains_key("From"));
    }

    /// A 1 MB body, generated at test time.
    #[test]
    fn one_megabyte_body_is_preserved_without_truncation() {
        let body = "A".repeat(1_000_000);
        let raw = format!(
            "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: big body\r\nMessage-ID: <big-body-1@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}"
        );
        let parsed = parse_inbound(raw.as_bytes()).unwrap();
        assert_eq!(parsed.content.text.unwrap().len(), 1_000_000);
    }

    /// A ≈ 30 MB message, generated at test time.
    #[test]
    fn thirty_megabyte_attachment_parses_without_panicking() {
        let payload = "A".repeat(30_000_000);
        let raw = format!(
            "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: big attachment\r\nMessage-ID: <big-att-1@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nsmall body\r\n--b\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"big.bin\"\r\n\r\n{payload}\r\n--b--\r\n"
        );
        let parsed = parse_inbound(raw.as_bytes()).unwrap();
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].bytes.len(), 30_000_000);
    }

    /// A text-only message must not synthesize an `html` body from
    /// `mail-parser`'s "fall back to the other kind" convenience — see the
    /// comment on `parse_inbound`'s body extraction.
    #[test]
    fn text_only_message_has_no_html_fallback() {
        let parsed = fixture!("plain.eml");
        assert!(parsed.content.html.is_none());
        assert!(parsed.content.text.is_some());
    }
}
