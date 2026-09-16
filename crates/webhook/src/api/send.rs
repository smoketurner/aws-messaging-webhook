//! Validating a send request.
//!
//! Everything a caller supplies is checked here, before anything is uploaded
//! or queued, and every problem is reported at once rather than one per
//! round-trip.
//!
//! Two of the rules are load-bearing rather than cosmetic. Addresses, header
//! names and header values are rejected outright if they contain CR or LF,
//! because all three end up in a MIME document where a newline starts a new
//! header — that is how a caller would otherwise inject `Bcc` into someone
//! else's message. And the header names this service controls cannot be
//! overridden at all, so a caller cannot rewrite `From` and send as another
//! inbox.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;

use crate::api::error::{ApiError, FieldError};
use crate::mail::url_policy::{self, AttachmentUrl};
use crate::mail::{
    ADDRESS_MAX_BYTES, ATTACHMENT_FIELD_MAX, ATTACHMENTS_MAX, HEADER_NAME_MAX, HEADER_VALUE_MAX,
    HEADERS_BUDGET, LABEL_MAX_BYTES, MAX_OUTBOUND_DECODED_BYTES, MESSAGE_USER_LABEL_CAP,
    OUTBOUND_RECIPIENTS_MAX, SUBJECT_MAX_BYTES, labels,
};

/// Headers the service sets itself. A caller-supplied value for any of these
/// is refused rather than ignored, so a request never appears to have been
/// honored when it was not.
///
/// `From` and `Return-Path` are the reason this list is not advisory: without
/// them a caller could send as any address on the domain. The `Content-*`
/// family is excluded because the MIME builder owns the body structure.
const CONTROLLED_HEADERS: [&str; 13] = [
    "from",
    "sender",
    "to",
    "cc",
    "bcc",
    "reply-to",
    "subject",
    "date",
    "message-id",
    "in-reply-to",
    "references",
    "return-path",
    "mime-version",
];

/// The longest a `content_id` may be.
const CONTENT_ID_MAX_BYTES: usize = 250;

/// The longest a `content_type` may be.
const CONTENT_TYPE_MAX_BYTES: usize = 127;

/// One address, or several. The contract allows both spellings everywhere a
/// list of addresses appears.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Addresses {
    One(String),
    Many(Vec<String>),
}

impl Addresses {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(one) => vec![one],
            Self::Many(many) => many,
        }
    }
}

/// An attachment as the request gives it: bytes inline, or a URL for the
/// sender to fetch.
#[derive(Debug, Clone, Deserialize)]
pub struct AttachmentInput {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub content_disposition: Option<String>,
    #[serde(default)]
    pub content_id: Option<String>,
}

/// `POST …/messages/send`.
#[derive(Debug, Clone, Deserialize)]
pub struct SendRequest {
    #[serde(default)]
    pub to: Option<Addresses>,
    #[serde(default)]
    pub cc: Option<Addresses>,
    #[serde(default)]
    pub bcc: Option<Addresses>,
    #[serde(default)]
    pub reply_to: Option<Addresses>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub html: Option<String>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentInput>>,
}

/// One attachment that passed validation. Inline bytes are already decoded;
/// a URL has already been vetted for shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachmentSource {
    Inline(Vec<u8>),
    Url(AttachmentUrl),
}

/// A validated attachment, before it is given an id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedAttachment {
    pub source: AttachmentSource,
    pub filename: Option<String>,
    pub content_type: String,
    pub content_disposition: String,
    pub content_id: Option<String>,
}

/// A send request that has passed every rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedSend {
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub reply_to: Vec<String>,
    pub subject: String,
    pub text: Option<String>,
    pub html: Option<String>,
    pub labels: Vec<String>,
    pub headers: BTreeMap<String, String>,
    pub attachments: Vec<ValidatedAttachment>,
}

/// The default content type for an attachment that does not name one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Validates a send request.
///
/// # Errors
///
/// [`ApiError::Validation`] listing every offending field.
pub fn validate(request: SendRequest) -> Result<ValidatedSend, ApiError> {
    let mut errors = Vec::new();

    let to = addresses(request.to, "to", &mut errors);
    let cc = addresses(request.cc, "cc", &mut errors);
    let bcc = addresses(request.bcc, "bcc", &mut errors);
    let reply_to = addresses(request.reply_to, "reply_to", &mut errors);

    let recipients = to.len() + cc.len() + bcc.len();
    if recipients == 0 {
        errors.push(field("to", "at least one of to, cc or bcc is required"));
    } else if recipients > OUTBOUND_RECIPIENTS_MAX {
        errors.push(field(
            "to",
            format!("at most {OUTBOUND_RECIPIENTS_MAX} recipients across to, cc and bcc"),
        ));
    }

    let subject = request.subject.unwrap_or_default();
    if subject.len() > SUBJECT_MAX_BYTES {
        errors.push(field(
            "subject",
            format!("must be at most {SUBJECT_MAX_BYTES} bytes"),
        ));
    }
    if contains_newline(&subject) {
        errors.push(field("subject", "must not contain a line break"));
    }

    let text = non_empty(request.text);
    let html = non_empty(request.html);
    if text.is_none() && html.is_none() {
        errors.push(field("text", "at least one of text or html is required"));
    }

    let labels = user_labels(request.labels, &mut errors);
    let headers = headers(request.headers, &mut errors);
    let attachments = attachments(request.attachments, &mut errors);

    if errors.is_empty() {
        Ok(ValidatedSend {
            to,
            cc,
            bcc,
            reply_to,
            subject,
            text,
            html,
            labels,
            headers,
            attachments,
        })
    } else {
        Err(ApiError::Validation(errors))
    }
}

fn field(path: impl Into<String>, message: impl Into<String>) -> FieldError {
    FieldError {
        path: path.into(),
        message: message.into(),
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.is_empty())
}

/// A CR or LF anywhere in a value that becomes a header is header injection.
fn contains_newline(value: &str) -> bool {
    value.contains('\r') || value.contains('\n')
}

fn addresses(
    input: Option<Addresses>,
    path: &'static str,
    errors: &mut Vec<FieldError>,
) -> Vec<String> {
    let mut out = Vec::new();
    for address in input.map(Addresses::into_vec).unwrap_or_default() {
        let address = address.trim().to_owned();
        if address.is_empty() {
            errors.push(field(path, "an address cannot be empty"));
            continue;
        }
        if address.len() > ADDRESS_MAX_BYTES {
            errors.push(field(
                path,
                format!("an address must be at most {ADDRESS_MAX_BYTES} bytes"),
            ));
            continue;
        }
        if contains_newline(&address) {
            errors.push(field(path, "an address must not contain a line break"));
            continue;
        }
        // Not a full RFC 5322 parse: SES is the authority on deliverability.
        // This only rejects what cannot be an address at all.
        if !address.contains('@') {
            errors.push(field(path, format!("`{address}` is not an email address")));
            continue;
        }
        if !out.contains(&address) {
            out.push(address);
        }
    }
    out
}

fn user_labels(input: Option<Vec<String>>, errors: &mut Vec<FieldError>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for label in input.unwrap_or_default() {
        let label = label.trim().to_lowercase();
        if label.is_empty() {
            errors.push(field("labels", "a label cannot be empty"));
            continue;
        }
        if label.len() > LABEL_MAX_BYTES {
            errors.push(field(
                "labels",
                format!("`{label}` is longer than {LABEL_MAX_BYTES} bytes"),
            ));
            continue;
        }
        if labels::is_reserved(&label) {
            errors.push(field(
                "labels",
                format!("`{label}` is set by the service and cannot be applied"),
            ));
            continue;
        }
        if !out.contains(&label) {
            out.push(label);
        }
    }
    if out.len() > MESSAGE_USER_LABEL_CAP {
        errors.push(field(
            "labels",
            format!("at most {MESSAGE_USER_LABEL_CAP} labels"),
        ));
    }
    out.sort();
    out
}

/// Whether `name` is an RFC 5322 header field name: printable ASCII with no
/// space and no colon.
fn is_header_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b':' && b != b';')
}

fn headers(
    input: Option<BTreeMap<String, String>>,
    errors: &mut Vec<FieldError>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut budget = 0usize;

    for (name, value) in input.unwrap_or_default() {
        let trimmed = name.trim();
        if !is_header_name(trimmed) {
            errors.push(field("headers", format!("`{name}` is not a header name")));
            continue;
        }
        if trimmed.len() > HEADER_NAME_MAX {
            errors.push(field(
                "headers",
                format!("`{trimmed}` is longer than {HEADER_NAME_MAX} bytes"),
            ));
            continue;
        }
        if CONTROLLED_HEADERS.contains(&trimmed.to_lowercase().as_str())
            || trimmed.to_lowercase().starts_with("content-")
        {
            errors.push(field(
                "headers",
                format!("`{trimmed}` is set by the service and cannot be overridden"),
            ));
            continue;
        }
        if value.len() > HEADER_VALUE_MAX {
            errors.push(field(
                "headers",
                format!("the value of `{trimmed}` is longer than {HEADER_VALUE_MAX} bytes"),
            ));
            continue;
        }
        if contains_newline(&value) {
            errors.push(field(
                "headers",
                format!("the value of `{trimmed}` must not contain a line break"),
            ));
            continue;
        }
        budget += trimmed.len() + value.len();
        out.insert(trimmed.to_owned(), value);
    }

    if budget > HEADERS_BUDGET {
        errors.push(field(
            "headers",
            format!("all headers together must be at most {HEADERS_BUDGET} bytes"),
        ));
    }
    out
}

/// A content type is `token/token`, optionally with one `; charset=token`.
fn is_content_type(value: &str) -> bool {
    if value.len() > CONTENT_TYPE_MAX_BYTES || contains_newline(value) {
        return false;
    }
    let (essence, parameters) = match value.split_once(';') {
        Some((essence, parameters)) => (essence, Some(parameters)),
        None => (value, None),
    };
    let Some((kind, subtype)) = essence.trim().split_once('/') else {
        return false;
    };
    let is_token = |token: &str| {
        !token.is_empty()
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+' | b'_'))
    };
    if !is_token(kind.trim()) || !is_token(subtype.trim()) {
        return false;
    }
    match parameters {
        None => true,
        Some(parameters) => match parameters.trim().split_once('=') {
            Some((name, value)) => {
                name.trim().eq_ignore_ascii_case("charset")
                    && is_token(value.trim().trim_matches('"'))
            }
            None => false,
        },
    }
}

/// Resolves the one source an attachment is allowed to have.
///
/// Inline bytes are decoded here rather than at send time so a bad encoding
/// is the caller's `400` instead of a queued message that can never be built.
fn attachment_source(
    attachment: &AttachmentInput,
    path: &str,
    errors: &mut Vec<FieldError>,
) -> Option<AttachmentSource> {
    match (&attachment.content, &attachment.url) {
        (Some(content), None) => {
            // Mail clients wrap base64 at 76 columns, so whitespace is
            // expected rather than an error.
            let stripped: String = content
                .chars()
                .filter(|c| !c.is_ascii_whitespace())
                .collect();
            let Ok(bytes) = STANDARD.decode(stripped.as_bytes()) else {
                errors.push(field(path, "`content` is not valid base64"));
                return None;
            };
            Some(AttachmentSource::Inline(bytes))
        }
        (None, Some(url)) => match url_policy::parse_attachment_url(url) {
            Ok(url) => Some(AttachmentSource::Url(url)),
            Err(rejected) => {
                // The reason stays generic: a caller learns its URL was
                // refused, not what the network behind this service looks
                // like.
                errors.push(field(path, format!("`url` was refused: {rejected}")));
                None
            }
        },
        (Some(_), Some(_)) | (None, None) => {
            errors.push(field(path, "give exactly one of `content` or `url`"));
            None
        }
    }
}

fn attachments(
    input: Option<Vec<AttachmentInput>>,
    errors: &mut Vec<FieldError>,
) -> Vec<ValidatedAttachment> {
    let input = input.unwrap_or_default();
    if input.len() > ATTACHMENTS_MAX {
        errors.push(field(
            "attachments",
            format!("at most {ATTACHMENTS_MAX} attachments"),
        ));
        return Vec::new();
    }

    let mut out = Vec::with_capacity(input.len());
    let mut content_ids: BTreeSet<String> = BTreeSet::new();
    let mut decoded_bytes: u64 = 0;

    for (index, attachment) in input.into_iter().enumerate() {
        let path = format!("attachments[{index}]");

        let source = attachment_source(&attachment, &path, errors);
        if let Some(AttachmentSource::Inline(bytes)) = &source {
            decoded_bytes += bytes.len() as u64;
        }

        if let Some(filename) = &attachment.filename {
            if filename.len() > ATTACHMENT_FIELD_MAX {
                errors.push(field(
                    &path,
                    format!("`filename` is longer than {ATTACHMENT_FIELD_MAX} bytes"),
                ));
            }
            if filename.chars().any(char::is_control) {
                errors.push(field(
                    &path,
                    "`filename` must not contain control characters",
                ));
            }
        }

        let content_type = attachment
            .content_type
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned());
        if !is_content_type(&content_type) {
            errors.push(field(&path, "`content_type` is not a media type"));
        }

        let disposition = attachment
            .content_disposition
            .unwrap_or_else(|| "attachment".to_owned());
        if !matches!(disposition.as_str(), "inline" | "attachment") {
            errors.push(field(
                &path,
                "`content_disposition` must be `inline` or `attachment`",
            ));
        }

        if let Some(content_id) = &attachment.content_id {
            let invalid = content_id.is_empty()
                || content_id.len() > CONTENT_ID_MAX_BYTES
                || content_id
                    .bytes()
                    .any(|b| !b.is_ascii_graphic() || matches!(b, b'<' | b'>'));
            if invalid {
                errors.push(field(
                    &path,
                    "`content_id` must be printable ASCII without `<`, `>` or spaces",
                ));
            } else if !content_ids.insert(content_id.clone()) {
                // Two parts sharing a cid make an HTML reference ambiguous.
                errors.push(field(&path, "`content_id` is used by another attachment"));
            }
        } else if disposition == "inline" {
            errors.push(field(
                &path,
                "an `inline` attachment needs a `content_id` to reference it by",
            ));
        }

        if let Some(source) = source {
            out.push(ValidatedAttachment {
                source,
                filename: attachment.filename,
                content_type,
                content_disposition: disposition,
                content_id: attachment.content_id,
            });
        }
    }

    if decoded_bytes > MAX_OUTBOUND_DECODED_BYTES {
        errors.push(field(
            "attachments",
            format!("decoded attachments must total at most {MAX_OUTBOUND_DECODED_BYTES} bytes"),
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> SendRequest {
        SendRequest {
            to: Some(Addresses::One("to@example.com".to_owned())),
            cc: None,
            bcc: None,
            reply_to: None,
            subject: Some("Hello".to_owned()),
            text: Some("body".to_owned()),
            html: None,
            labels: None,
            headers: None,
            attachments: None,
        }
    }

    fn paths(request: SendRequest) -> Vec<String> {
        match validate(request) {
            Err(ApiError::Validation(errors)) => errors.into_iter().map(|e| e.path).collect(),
            other => panic!("expected a validation failure, got {other:?}"),
        }
    }

    #[test]
    fn a_minimal_request_is_accepted() {
        let send = validate(minimal()).unwrap();
        assert_eq!(send.to, vec!["to@example.com"]);
        assert_eq!(send.subject, "Hello");
        assert_eq!(send.text.as_deref(), Some("body"));
        assert!(send.attachments.is_empty());
    }

    #[test]
    fn a_recipient_and_a_body_are_both_required() {
        let mut request = minimal();
        request.to = None;
        assert_eq!(paths(request), vec!["to"]);

        let mut request = minimal();
        request.text = None;
        request.html = None;
        assert_eq!(paths(request), vec!["text"]);

        // html alone is enough.
        let mut request = minimal();
        request.text = None;
        request.html = Some("<p>hi</p>".to_owned());
        assert!(validate(request).is_ok());
    }

    #[test]
    fn recipients_are_counted_across_all_three_lists() {
        let mut request = minimal();
        let many: Vec<String> = (0..OUTBOUND_RECIPIENTS_MAX)
            .map(|i| format!("user{i}@example.com"))
            .collect();
        request.to = Some(Addresses::Many(many));
        request.cc = Some(Addresses::One("one-too-many@example.com".to_owned()));
        assert_eq!(paths(request), vec!["to"]);
    }

    #[test]
    fn an_address_with_a_line_break_is_rejected() {
        // This is the header-injection case: a newline in an address would
        // start a new header in the assembled message.
        let mut request = minimal();
        request.to = Some(Addresses::One(
            "ok@example.com\r\nBcc: victim@example.com".to_owned(),
        ));
        assert_eq!(paths(request), vec!["to", "to"]);
    }

    #[test]
    fn duplicate_recipients_collapse() {
        let mut request = minimal();
        request.to = Some(Addresses::Many(vec![
            "a@example.com".to_owned(),
            "a@example.com".to_owned(),
        ]));
        assert_eq!(validate(request).unwrap().to, vec!["a@example.com"]);
    }

    #[test]
    fn controlled_headers_cannot_be_overridden() {
        for name in ["From", "from", "BCC", "Message-ID", "Content-Type"] {
            let mut request = minimal();
            request.headers = Some(BTreeMap::from([(name.to_owned(), "x".to_owned())]));
            assert_eq!(paths(request), vec!["headers"], "{name} should be refused");
        }
    }

    #[test]
    fn a_custom_header_is_kept_but_a_newline_in_one_is_not() {
        let mut request = minimal();
        request.headers = Some(BTreeMap::from([(
            "X-Campaign".to_owned(),
            "spring".to_owned(),
        )]));
        let send = validate(request).unwrap();
        assert_eq!(
            send.headers.get("X-Campaign").map(String::as_str),
            Some("spring")
        );

        let mut request = minimal();
        request.headers = Some(BTreeMap::from([(
            "X-Campaign".to_owned(),
            "a\r\nBcc: victim@example.com".to_owned(),
        )]));
        assert_eq!(paths(request), vec!["headers"]);
    }

    #[test]
    fn service_owned_labels_cannot_be_applied() {
        let mut request = minimal();
        request.labels = Some(vec!["sent".to_owned()]);
        assert_eq!(paths(request), vec!["labels"]);

        let mut request = minimal();
        request.labels = Some(vec!["Campaign".to_owned(), "campaign".to_owned()]);
        assert_eq!(validate(request).unwrap().labels, vec!["campaign"]);
    }

    #[test]
    fn an_attachment_needs_exactly_one_source() {
        let mut request = minimal();
        request.attachments = Some(vec![AttachmentInput {
            content: None,
            url: None,
            filename: None,
            content_type: None,
            content_disposition: None,
            content_id: None,
        }]);
        assert_eq!(paths(request), vec!["attachments[0]"]);

        let mut request = minimal();
        request.attachments = Some(vec![AttachmentInput {
            content: Some(STANDARD.encode("hi")),
            url: Some("https://example.com/a.pdf".to_owned()),
            filename: None,
            content_type: None,
            content_disposition: None,
            content_id: None,
        }]);
        assert_eq!(paths(request), vec!["attachments[0]"]);
    }

    #[test]
    fn inline_content_is_decoded_and_whitespace_is_tolerated() {
        let mut request = minimal();
        request.attachments = Some(vec![AttachmentInput {
            // Wrapped the way a mail client would wrap it.
            content: Some(format!(
                "{}\r\n{}",
                &STANDARD.encode("hello world")[..8],
                &STANDARD.encode("hello world")[8..]
            )),
            url: None,
            filename: Some("a.txt".to_owned()),
            content_type: Some("text/plain".to_owned()),
            content_disposition: None,
            content_id: None,
        }]);
        let send = validate(request).unwrap();
        assert_eq!(
            send.attachments[0].source,
            AttachmentSource::Inline(b"hello world".to_vec())
        );
        assert_eq!(send.attachments[0].content_disposition, "attachment");
    }

    #[test]
    fn a_private_url_is_refused_at_the_api() {
        // The sender checks again on every hop, but a URL that can never be
        // fetched should never be queued in the first place.
        for url in [
            "http://example.com/a.pdf",
            "https://169.254.169.254/latest/meta-data/",
            "https://127.0.0.1/a.pdf",
            "https://user:pw@example.com/a.pdf",
        ] {
            let mut request = minimal();
            request.attachments = Some(vec![AttachmentInput {
                content: None,
                url: Some(url.to_owned()),
                filename: None,
                content_type: None,
                content_disposition: None,
                content_id: None,
            }]);
            assert_eq!(paths(request), vec!["attachments[0]"], "{url}");
        }
    }

    #[test]
    fn an_inline_disposition_needs_a_unique_content_id() {
        let inline = |cid: Option<&str>| AttachmentInput {
            content: Some(STANDARD.encode("x")),
            url: None,
            filename: None,
            content_type: Some("image/png".to_owned()),
            content_disposition: Some("inline".to_owned()),
            content_id: cid.map(ToOwned::to_owned),
        };

        let mut request = minimal();
        request.attachments = Some(vec![inline(None)]);
        assert_eq!(paths(request), vec!["attachments[0]"]);

        let mut request = minimal();
        request.attachments = Some(vec![inline(Some("logo")), inline(Some("logo"))]);
        assert_eq!(paths(request), vec!["attachments[1]"]);

        let mut request = minimal();
        request.attachments = Some(vec![inline(Some("logo")), inline(Some("banner"))]);
        assert!(validate(request).is_ok());
    }

    #[test]
    fn a_content_id_cannot_smuggle_angle_brackets_or_spaces() {
        for cid in ["<logo>", "a b", "a\r\nb", ""] {
            let mut request = minimal();
            request.attachments = Some(vec![AttachmentInput {
                content: Some(STANDARD.encode("x")),
                url: None,
                filename: None,
                content_type: None,
                content_disposition: None,
                content_id: Some(cid.to_owned()),
            }]);
            assert_eq!(paths(request), vec!["attachments[0]"], "{cid:?}");
        }
    }

    #[test]
    fn content_types_are_checked() {
        assert!(is_content_type("text/plain"));
        assert!(is_content_type("application/pdf"));
        assert!(is_content_type("text/plain; charset=utf-8"));
        assert!(is_content_type("text/plain; charset=\"utf-8\""));
        assert!(!is_content_type("text"));
        assert!(!is_content_type("text/"));
        assert!(!is_content_type("/plain"));
        assert!(!is_content_type("text/plain\r\nX-Evil: yes"));
        assert!(!is_content_type("text/plain; boundary=x"));
        assert!(!is_content_type(&format!(
            "text/{}",
            "x".repeat(CONTENT_TYPE_MAX_BYTES)
        )));
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let request = SendRequest {
            to: None,
            cc: None,
            bcc: None,
            reply_to: None,
            subject: Some("x".repeat(SUBJECT_MAX_BYTES + 1)),
            text: None,
            html: None,
            labels: Some(vec!["sent".to_owned()]),
            headers: None,
            attachments: None,
        };
        assert_eq!(paths(request), vec!["to", "subject", "text", "labels"]);
    }
}
