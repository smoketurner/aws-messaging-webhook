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
            Self::Verification(
                VerifyError::MalformedEnvelope(_) | VerifyError::MissingField(_),
            ) => StatusCode::BAD_REQUEST,
            Self::TopicNotAllowed(_) | Self::Verification(_) => StatusCode::FORBIDDEN,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match &self {
            Self::TopicNotAllowed(topic_arn) => {
                tracing::warn!(
                    topic_arn,
                    event = "allowlist_rejection",
                    "rejected SNS message"
                );
            }
            Self::Verification(error) => {
                tracing::error!(
                    error = %error,
                    event = "signature_rejection",
                    "rejected SNS message"
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
        let status = self.status();
        (status, status.canonical_reason().unwrap_or("error")).into_response()
    }
}
