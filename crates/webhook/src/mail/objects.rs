//! The object store trait (§5): S3 access for mail bodies, attachments and
//! send specs. The bucket is always `MailConfig.bucket` — never a parameter,
//! so no caller can address another bucket.
//!
//! Only the P1 methods this phase's ingest flow needs are defined here
//! (`get_object`, `head_object`, `put_object_if_absent`). `copy_object`,
//! `delete_object` and `presign_get` are P3/P2 additions (promotion, the
//! attachment presign route) layered on by the tracks that implement them.

use std::future::Future;

use axum::body::Bytes;

use crate::mail::{ObjectMeta, PutOutcome};

pub trait ObjectStore: Send + Sync {
    /// Fetches `key`, rejecting a response over `max_bytes` (the S3 client
    /// streams and aborts rather than buffering an oversized object first).
    fn get_object(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Bytes, ObjectError>> + Send;

    /// `None` for a missing object — not an error, since ingest's resume
    /// check (D48 m2) uses this to distinguish "already put" from "not yet".
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
