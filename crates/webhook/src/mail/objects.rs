//! The object store trait: S3 access for mail bodies, attachments and
//! send specs. The bucket is always `MailConfig.bucket` — never a parameter,
//! so no caller can address another bucket.
//!
//! Objects are written once and never modified. The one deletion is
//! [`ObjectStore::delete_object`], which the sender uses to clear an outbox
//! entry once its send's outcome has settled; everything else expires on the
//! bucket's own schedule. The `outbox/` prefix has no lifecycle rule, so a
//! settled send clears its own objects — nothing else reaps them.

use std::future::Future;
use std::time::Duration;

use axum::body::Bytes;

use crate::mail::{ObjectMeta, PutOutcome};

/// How long a download URL stays valid. Long enough for a client to follow
/// the link it was just handed, short enough that a leaked URL — it carries
/// its own authorization, and the API's bearer key is not needed to use it —
/// stops working quickly.
pub const DOWNLOAD_URL_TTL: Duration = Duration::from_mins(15);

pub trait ObjectStore: Send + Sync {
    /// Fetches `key`, rejecting a response over `max_bytes` (the S3 client
    /// streams and aborts rather than buffering an oversized object first).
    fn get_object(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Bytes, ObjectError>> + Send;

    /// `None` for a missing object — not an error, since ingest's resume
    /// check uses this to distinguish "already put" from "not yet".
    fn head_object(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<Option<ObjectMeta>, ObjectError>> + Send;

    /// A conditional put (`if_none_match: *`): `Created` on the first write,
    /// `AlreadyExists` when the key is already present. Never overwrites —
    /// this is what makes a redelivered ingest or a resumed send idempotent.
    fn put_object_if_absent(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> impl Future<Output = Result<PutOutcome, ObjectError>> + Send;

    /// Removes `key`. A key that is already gone is not an error: the caller
    /// wanted it absent, and it is.
    fn delete_object(&self, key: &str) -> impl Future<Output = Result<(), ObjectError>> + Send;

    /// A presigned `GET` URL for `key`, valid for [`DOWNLOAD_URL_TTL`].
    ///
    /// `disposition` and `content_type`, when given, are signed in as
    /// response-header overrides so the browser names the download and treats
    /// it as the right type. They are part of the signature, so a recipient
    /// cannot alter them without invalidating the URL.
    ///
    /// Presigning is a local computation: it does not check that the object
    /// exists, so a caller that cares must have established that already.
    fn presign_get(
        &self,
        key: &str,
        disposition: Option<&str>,
        content_type: Option<&str>,
    ) -> impl Future<Output = Result<String, ObjectError>> + Send;
}

/// Builds a `Content-Disposition` value that is safe to sign into a URL and
/// echo back in a response header.
///
/// The filename reaches us from a `Content-Disposition` header in mail an
/// arbitrary sender wrote, so it is treated as hostile: anything outside a
/// conservative allowlist is replaced, keeping quotes, control characters,
/// semicolons and path separators out of the header value entirely. A name
/// left with nothing usable falls back to `download`.
#[must_use]
pub fn attachment_disposition(filename: Option<&str>) -> String {
    let cleaned: String = filename
        .unwrap_or_default()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.');
    let name = if cleaned.is_empty() {
        "download"
    } else {
        cleaned
    };
    // Bounded so one absurd filename can't bloat every signed URL.
    let name: String = name.chars().take(128).collect();
    format!("attachment; filename=\"{name}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_filename_survives() {
        assert_eq!(
            attachment_disposition(Some("invoice 42.pdf")),
            "attachment; filename=\"invoice 42.pdf\""
        );
    }

    #[test]
    fn header_injection_and_path_traversal_are_neutralized() {
        // A quote would end the quoted-string, CR/LF would start a new
        // header, and `../` would matter if anything ever wrote this to disk
        // by name.
        for hostile in [
            "a\"; rm -rf /; x=\"",
            "a\r\nX-Evil: yes",
            "../../etc/passwd",
            "a;b",
        ] {
            let value = attachment_disposition(Some(hostile));
            // The prefix is ours and contains a `;` of its own, so the
            // assertions are about the filename the sender controls.
            let name = value
                .strip_prefix("attachment; filename=\"")
                .and_then(|rest| rest.strip_suffix('"'))
                .unwrap_or_else(|| panic!("unexpected shape: {value}"));
            assert!(!name.contains('\r'), "{name}");
            assert!(!name.contains('\n'), "{name}");
            assert!(!name.contains(';'), "{name}");
            assert!(!name.contains('/'), "{name}");
            assert!(!name.contains('"'), "{name}");
        }
    }

    #[test]
    fn an_unusable_name_falls_back() {
        assert_eq!(
            attachment_disposition(None),
            "attachment; filename=\"download\""
        );
        assert_eq!(
            attachment_disposition(Some("...")),
            "attachment; filename=\"download\""
        );
    }

    #[test]
    fn a_very_long_name_is_bounded() {
        let value = attachment_disposition(Some(&"x".repeat(1000)));
        assert!(value.len() < 200, "{}", value.len());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    #[error("object not found")]
    NotFound,
    #[error("object too large ({size} bytes)")]
    TooLarge { size: u64 },
    #[error("transient object store error")]
    Transient(#[source] anyhow::Error),
    #[error("permanent object store error")]
    Permanent(#[source] anyhow::Error),
}
