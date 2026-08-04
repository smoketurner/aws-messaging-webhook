use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sns_message_verifier::VerifyError;

/// Request-handling failure, mapped onto the SNS retry contract: SNS retries
/// deliveries on 5xx and treats other 4xx as permanent, so transient
/// downstream failures MUST be 5xx (to recruit redelivery) and
/// rejected-by-policy messages MUST be 4xx (to avoid retry storms).
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("missing x-amz-sns-message-type header")]
    MissingSnsHeader,

    #[error("request body could not be read")]
    UnreadableBody,

    #[error("topic is not in the allowlist: {0}")]
    TopicNotAllowed(String),

    #[error("SNS message verification failed: {0}")]
    Verification(#[from] VerifyError),

    #[error("SubscribeURL is not an SNS endpoint")]
    SubscribeUrlRejected,

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::MissingSnsHeader | Self::UnreadableBody | Self::SubscribeUrlRejected => {
                StatusCode::BAD_REQUEST
            }
            Self::TopicNotAllowed(_) => StatusCode::FORBIDDEN,
            Self::Verification(error) => verification_status(error),
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Maps a verification failure onto the SNS retry contract. Client/attacker
/// faults are permanent 4xx (don't ask SNS to redeliver garbage); a failure to
/// fetch, parse, or validate the certificate is transient infrastructure —
/// return 5xx so a correctly-signed message survives a cold-start network blip
/// or a cert rotation instead of being dropped as a permanent 4xx.
fn verification_status(error: &VerifyError) -> StatusCode {
    match error {
        VerifyError::MalformedEnvelope(_)
        | VerifyError::MissingField(_)
        | VerifyError::InvalidCertUrl { .. }
        | VerifyError::UnsupportedSignatureVersion(_)
        | VerifyError::InvalidSignatureEncoding(_) => StatusCode::BAD_REQUEST,
        VerifyError::SignatureMismatch => StatusCode::FORBIDDEN,
        VerifyError::CertFetch(_) | VerifyError::CertParse(_) | VerifyError::CertValidity => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        match &self {
            Self::TopicNotAllowed(topic_arn) => {
                tracing::warn!(
                    topic_arn,
                    event = "allowlist_rejection",
                    "rejected SNS message"
                );
            }
            // A verification failure is a signature rejection only when it's a
            // permanent 4xx; a transient cert-fetch 5xx is infrastructure, and
            // must not inflate the signature-rejection metric.
            Self::Verification(error) if status.is_client_error() => {
                tracing::error!(
                    error = %error,
                    event = "signature_rejection",
                    "rejected SNS message"
                );
            }
            Self::Verification(error) => {
                tracing::error!(
                    error = %error,
                    event = "internal_error",
                    "certificate could not be fetched or validated; returning 5xx so SNS redelivers"
                );
            }
            Self::Internal(error) => {
                tracing::error!(
                    error = ?error,
                    event = "internal_error",
                    "processing failed; returning 5xx so SNS redelivers"
                );
            }
            other => {
                tracing::warn!(error = %other, event = "bad_request", "rejected request");
            }
        }
        // Public endpoint: never echo error details to the caller.
        (status, status.canonical_reason().unwrap_or("error")).into_response()
    }
}
