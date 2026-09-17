//! [`ObjectStore`] for [`AwsServices`](crate::aws::AwsServices): S3 access
//! for mail bodies, attachments and send specs, with the client
//! timeouts (`operation_attempt_timeout` 20 s, `operation_timeout` 45 s).

use std::time::Duration;

use anyhow::anyhow;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::error::display::DisplayErrorContext;
use axum::body::Bytes;

use crate::aws::{self, AwsServices};
use crate::mail::ObjectMeta;
use crate::mail::PutOutcome;
use crate::mail::objects::{DOWNLOAD_URL_TTL, ObjectError, ObjectStore};

/// The per-attempt and per-operation S3 timeouts, applied to every call
/// via `.customize().config_override(...)` since the shared `s3` client
/// (`aws.rs`) is built without them.
fn timeout_override() -> S3ConfigBuilder {
    let timeout_config = TimeoutConfig::builder()
        .operation_attempt_timeout(Duration::from_secs(20))
        .operation_timeout(Duration::from_secs(45))
        .build();
    S3ConfigBuilder::new().timeout_config(timeout_config)
}

/// S3's throttling error codes — a different vocabulary from the
/// DynamoDB/Pinpoint/SES actions and mail store (`aws::THROTTLING_CODES`).
const THROTTLING_CODES: [&str; 2] = ["ThrottlingException", "SlowDown"];

/// Maps an S3 SDK failure onto [`ObjectError`]: a 404 (missing key — the
/// `s3:ListBucket` grant is what makes this a real 404 rather than a 403) is
/// [`ObjectError::NotFound`]; a 403 is [`ObjectError::Permanent`]; timeouts,
/// dispatch/response failures, throttling and 5xx are
/// [`ObjectError::Transient`]; everything else is permanent. Shares its
/// transient/permanent classification with `aws.rs`'s
/// `classify_action_error` via `aws::sdk_error_is_transient`.
fn classify_object_error<E>(context: &'static str, error: &SdkError<E>) -> ObjectError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    if let SdkError::ServiceError(ctx) = error {
        match ctx.raw().status().as_u16() {
            404 => return ObjectError::NotFound,
            403 => {
                return ObjectError::Permanent(anyhow!(
                    "{context}: {}",
                    DisplayErrorContext(error)
                ));
            }
            _ => {}
        }
    }
    let source = anyhow!("{context}: {}", DisplayErrorContext(error));
    if aws::sdk_error_is_transient(error, &THROTTLING_CODES) {
        ObjectError::Transient(source)
    } else {
        ObjectError::Permanent(source)
    }
}

/// The outcome of a conditional `PutObject` that came back an error: a 412
/// is the idempotent "another writer got there first", and a 409
/// `ConditionalRequestConflict` — S3's answer when two conditional writes to
/// the same key overlap — is transient, since the write never reached a
/// decision and a retry is what settles it. Everything else classifies as
/// any other S3 failure does.
fn put_if_absent_outcome<E>(error: &SdkError<E>) -> Result<PutOutcome, ObjectError>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let status = match error {
        SdkError::ServiceError(ctx) => Some(ctx.raw().status().as_u16()),
        _ => None,
    };
    match status {
        Some(412) => Ok(PutOutcome::AlreadyExists),
        Some(409) => Err(ObjectError::Transient(anyhow!(
            "PutObject: {}",
            DisplayErrorContext(error)
        ))),
        _ => Err(classify_object_error("PutObject", error)),
    }
}

impl AwsServices {
    /// The mail bucket name (`MailConfig.bucket`) — the only bucket the
    /// object store ever addresses. Missing mail config here is a caller
    /// bug: every ingress path guards on `mail` being configured before an
    /// `ObjectStore` method can be reached.
    fn mail_bucket(&self) -> Result<&str, ObjectError> {
        self.mail_config()
            .map(|config| config.bucket.as_str())
            .ok_or_else(|| {
                ObjectError::Permanent(anyhow!("ObjectStore called with mail not configured"))
            })
    }
}

impl ObjectStore for AwsServices {
    async fn get_object(&self, key: &str, max_bytes: u64) -> Result<Bytes, ObjectError> {
        let bucket = self.mail_bucket()?;
        let output = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(key)
            .customize()
            .config_override(timeout_override())
            .send()
            .await
            .map_err(|error| classify_object_error("GetObject", &error))?;

        if let Some(len) = output.content_length() {
            let len = u64::try_from(len).unwrap_or(u64::MAX);
            if len > max_bytes {
                return Err(ObjectError::TooLarge { size: len });
            }
        }

        let mut body = output.body;
        let mut buf = Vec::new();
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|error| ObjectError::Transient(anyhow!("GetObject body: {error}")))?
        {
            buf.extend_from_slice(&chunk);
            if buf.len() as u64 > max_bytes {
                return Err(ObjectError::TooLarge {
                    size: buf.len() as u64,
                });
            }
        }
        Ok(Bytes::from(buf))
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectError> {
        let bucket = self.mail_bucket()?;
        let result = self
            .s3
            .head_object()
            .bucket(bucket)
            .key(key)
            .customize()
            .config_override(timeout_override())
            .send()
            .await;
        match result {
            Ok(output) => {
                let size = output
                    .content_length()
                    .and_then(|len| u64::try_from(len).ok())
                    .unwrap_or(0);
                Ok(Some(ObjectMeta { size }))
            }
            Err(error) => match classify_object_error("HeadObject", &error) {
                ObjectError::NotFound => Ok(None),
                other => Err(other),
            },
        }
    }

    async fn put_object_if_absent(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<PutOutcome, ObjectError> {
        let bucket = self.mail_bucket()?;
        let result = self
            .s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .content_type(content_type)
            .body(ByteStream::from(body))
            .customize()
            .config_override(timeout_override())
            .send()
            .await;
        match result {
            Ok(_) => Ok(PutOutcome::Created),
            Err(error) => put_if_absent_outcome(&error),
        }
    }

    async fn delete_object(&self, key: &str) -> Result<(), ObjectError> {
        let bucket = self.mail_bucket()?;
        let result = self
            .s3
            .delete_object()
            .bucket(bucket)
            .key(key)
            .customize()
            .config_override(timeout_override())
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // S3 reports deleting a missing key as success, but a 404 from a
            // bucket policy path is the same outcome for the caller.
            Err(error) => match classify_object_error("DeleteObject", &error) {
                ObjectError::NotFound => Ok(()),
                other => Err(other),
            },
        }
    }

    async fn presign_get(
        &self,
        key: &str,
        disposition: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<String, ObjectError> {
        let bucket = self.mail_bucket()?;
        let config = PresigningConfig::expires_in(DOWNLOAD_URL_TTL)
            .map_err(|e| ObjectError::Permanent(anyhow!("building the presigning config: {e}")))?;

        let mut request = self.s3.get_object().bucket(bucket).key(key);
        if let Some(disposition) = disposition {
            request = request.response_content_disposition(disposition);
        }
        if let Some(content_type) = content_type {
            request = request.response_content_type(content_type);
        }

        let presigned = request
            .presigned(config)
            .await
            .map_err(|e| classify_object_error("GetObject(presign)", &e))?;
        Ok(presigned.uri().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::config::http::HttpResponse;
    use aws_sdk_s3::operation::put_object::PutObjectError;
    use aws_smithy_types::body::SdkBody;

    use super::*;

    /// One S3 service error with the given HTTP status, as the SDK hands it
    /// back from a failed `PutObject`.
    fn service_error(status: u16) -> SdkError<PutObjectError> {
        let response = HttpResponse::new(status.try_into().unwrap(), SdkBody::empty());
        SdkError::service_error(PutObjectError::unhandled("s3 error"), response)
    }

    #[test]
    fn a_lost_if_none_match_race_is_the_already_written_outcome() {
        assert!(matches!(
            put_if_absent_outcome(&service_error(412)),
            Ok(PutOutcome::AlreadyExists)
        ));
    }

    /// Two conditional writes to one key overlap: S3 answers 409 without
    /// deciding either, so the caller must retry rather than treat the
    /// object as unwritable — which for ingest would drop the message.
    #[test]
    fn a_conditional_write_conflict_is_transient() {
        assert!(matches!(
            put_if_absent_outcome(&service_error(409)),
            Err(ObjectError::Transient(_))
        ));
    }

    #[test]
    fn other_failures_classify_as_any_other_put_does() {
        assert!(matches!(
            put_if_absent_outcome(&service_error(403)),
            Err(ObjectError::Permanent(_))
        ));
        assert!(matches!(
            put_if_absent_outcome(&service_error(503)),
            Err(ObjectError::Transient(_))
        ));
    }
}
