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
use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::api::error::{ApiError, FieldError};
use crate::config::MailConfig;
use crate::mail::objects::ObjectError;
use crate::mail::send::{
    self as send_mod, Envelope, SendKey, SendSpec, SendState, SendStatus, SpecAttachment,
};
use crate::mail::store::EnqueueOutcome;
use crate::mail::url_policy::{self, AttachmentUrl};
use crate::mail::{
    ADDRESS_MAX_BYTES, ATTACHMENT_FIELD_MAX, ATTACHMENTS_MAX, AttachmentMeta, Direction,
    HEADER_NAME_MAX, HEADER_VALUE_MAX, HEADERS_BUDGET, InboxId, LABEL_MAX_BYTES,
    MAX_OUTBOUND_DECODED_BYTES, MESSAGE_USER_LABEL_CAP, MailMessage, OUTBOUND_RECIPIENTS_MAX,
    PREVIEW_CHARS, SUBJECT_MAX_BYTES, ids, keys, labels, time,
};
use crate::state::{AppState, Services};

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

/// How long an `Idempotency-Key` is remembered.
const KEY_TTL_SECONDS: u64 = 24 * 60 * 60;

/// The longest `Idempotency-Key` accepted.
const IDEMPOTENCY_KEY_MAX_BYTES: usize = 255;

/// The response both send routes return.
#[derive(Debug, Serialize)]
pub struct SendAccepted {
    pub message_id: String,
    pub thread_id: String,
}

/// `POST /v0/inboxes/{inbox_id}/messages/send`
///
/// Queues the message and returns as soon as it is durably committed; a
/// separate sender calls SES. The response therefore means "this will be
/// sent", not "this has been sent".
///
/// # Errors
///
/// [`ApiError::Validation`] for a request that breaks any rule,
/// [`ApiError::NotFound`] when the inbox does not exist,
/// [`ApiError::Conflict`] when an `Idempotency-Key` is reused with a
/// different request, and a store or object failure otherwise.
pub async fn send<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Path(inbox_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SendRequest>,
) -> Result<Json<SendAccepted>, ApiError> {
    let inbox = InboxId(inbox_id);
    let key_hash = idempotency_key_hash(&headers)?;

    let Some(config) = state.config.mail.as_ref() else {
        return Err(ApiError::NotImplemented);
    };
    // An inbox that does not exist cannot send: the address would not be one
    // this domain owns.
    if state
        .services
        .get_inbox(&inbox)
        .await
        .map_err(crate::api::read::store_failure)?
        .is_none()
    {
        return Err(ApiError::NotFound);
    }

    let validated = validate(request)?;
    let request_hash = fingerprint(&inbox, &validated);

    // A live key for this request is answered from what it recorded, without
    // queuing anything a second time.
    if let Some(hash) = &key_hash
        && let Some(existing) = state
            .services
            .get_send_key(hash)
            .await
            .map_err(crate::api::read::store_failure)?
    {
        return if existing.request_hash == request_hash {
            Ok(Json(SendAccepted {
                message_id: existing.message_id,
                thread_id: existing.thread_id,
            }))
        } else {
            Err(ApiError::Conflict(
                "this Idempotency-Key was used for a different request".to_owned(),
            ))
        };
    }

    let now_ms = time::now_ms();
    let now = time::format(now_ms);
    let message_id = ids::outbound_message_id();
    // A send with no reply context starts its own thread.
    let thread_id = message_id.to_string();
    let message_id = message_id.to_string();

    let spec = build_spec(&inbox, config, &validated, &message_id, &thread_id, &now);
    upload_spec(&state.services, &spec, &validated).await?;

    let state_item = SendState::queued(
        inbox.clone(),
        message_id.clone(),
        thread_id.clone(),
        Envelope {
            to: validated.to.clone(),
            cc: validated.cc.clone(),
            bcc: validated.bcc.clone(),
        },
        key_hash.as_ref().map(|hash| keys::send_key_pk(hash)),
        &now,
    );
    let message = queued_message(&inbox, &validated, &spec, &now);
    let key = key_hash.map(|hash| SendKey {
        key_hash: hash,
        inbox_id: inbox,
        message_id: message_id.clone(),
        thread_id: thread_id.clone(),
        request_hash,
        route: "send".to_owned(),
        created_at: now.clone(),
        expires_at: now_ms / 1_000 + KEY_TTL_SECONDS,
    });

    match state
        .services
        .enqueue_send(&message, &state_item, key.as_ref(), now_ms / 1_000)
        .await
        .map_err(crate::api::read::store_failure)?
    {
        EnqueueOutcome::Committed | EnqueueOutcome::AlreadyQueued => Ok(Json(SendAccepted {
            message_id,
            thread_id,
        })),
        // Another request won the key between the read above and the commit.
        EnqueueOutcome::KeyExists => Err(ApiError::Conflict(
            "this Idempotency-Key is already in use".to_owned(),
        )),
    }
}

/// Hashes the `Idempotency-Key` header, if one was sent.
///
/// The header value itself is never stored or logged: another caller who
/// learned it could replay someone else's send.
fn idempotency_key_hash(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::field("Idempotency-Key", "must be printable ASCII"))?
        .trim();
    if value.is_empty() || value.len() > IDEMPOTENCY_KEY_MAX_BYTES {
        return Err(ApiError::field(
            "Idempotency-Key",
            format!("must be 1 to {IDEMPOTENCY_KEY_MAX_BYTES} characters"),
        ));
    }
    Ok(Some(sha256_hex(value.as_bytes())))
}

/// A fingerprint of what a request asked for, so the same key presented with
/// a different request can be told apart. Attachment bytes are included, so
/// swapping a file while reusing a key is a conflict rather than a replay.
fn fingerprint(inbox: &InboxId, send: &ValidatedSend) -> String {
    let mut hasher = Sha256::new();
    hasher.update(inbox.as_str().as_bytes());
    for list in [&send.to, &send.cc, &send.bcc, &send.reply_to, &send.labels] {
        for value in list {
            hasher.update(b"\x00");
            hasher.update(value.as_bytes());
        }
        hasher.update(b"\x01");
    }
    hasher.update(send.subject.as_bytes());
    hasher.update(send.text.as_deref().unwrap_or_default().as_bytes());
    hasher.update(send.html.as_deref().unwrap_or_default().as_bytes());
    for (name, value) in &send.headers {
        hasher.update(name.as_bytes());
        hasher.update(b"\x00");
        hasher.update(value.as_bytes());
    }
    for attachment in &send.attachments {
        match &attachment.source {
            AttachmentSource::Inline(bytes) => hasher.update(bytes),
            AttachmentSource::Url(url) => hasher.update(url.as_str().as_bytes()),
        }
    }
    hex(&hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Builds the spec the sender will assemble the message from.
fn build_spec(
    inbox: &InboxId,
    config: &MailConfig,
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
        from: format!("{}@{}", inbox.as_str(), config.domain),
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
        rfc_message_id: ids::our_rfc_message_id(message_id, &config.domain),
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
async fn upload_spec<T: Services>(
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
            .map_err(object_failure)?;
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
        .map_err(object_failure)?;
    Ok(())
}

/// The message item a queued send writes: everything a reader needs, with the
/// body kept as the caller gave it. `size` is zero until the sender has built
/// the real MIME.
fn queued_message(
    inbox: &InboxId,
    send: &ValidatedSend,
    spec: &SendSpec,
    now: &str,
) -> MailMessage {
    let mut labels = send.labels.clone();
    labels.push("queued".to_owned());
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
        references: spec.references.clone(),
        labels,
        timestamp: now.to_owned(),
        from: spec.from.clone(),
        reply_to: send.reply_to.clone(),
        to: send.to.clone(),
        cc: send.cc.clone(),
        bcc: send.bcc.clone(),
        subject: send.subject.clone(),
        preview: preview_of(send),
        size: 0,
        text: send.text.clone(),
        html: send.html.clone(),
        body_truncated: false,
        headers: send.headers.clone(),
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
        verdicts: None,
        thread_snapshot: None,
        delivery: BTreeMap::new(),
        send_status: Some(SendStatus::Queued.as_str().to_owned()),
        sent_at: None,
        version: 0,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    }
}

fn preview_of(send: &ValidatedSend) -> String {
    send.text
        .as_deref()
        .unwrap_or_default()
        .chars()
        .take(PREVIEW_CHARS)
        .collect()
}

fn object_failure(error: ObjectError) -> ApiError {
    match error {
        ObjectError::Transient(source) => ApiError::BadGateway(source),
        ObjectError::NotFound | ObjectError::TooLarge { .. } | ObjectError::Permanent(_) => {
            ApiError::Internal(anyhow::anyhow!("{error}"))
        }
    }
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
