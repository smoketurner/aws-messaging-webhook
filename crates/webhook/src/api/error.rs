//! Error responses for the `/v0` API.
//!
//! Two body shapes: `{name, message, code?, fix?, docs?}` for most failures,
//! and a validation variant carrying a per-field `errors` array. Clients
//! match on `name`, so the names here are part of the wire contract and must
//! not be renamed casually.
//!
//! Status codes carry the retry meaning clients assume: 401 is never retried
//! (so a cold key cache must not use it — see [`ApiError::Unavailable`]), 429
//! and 5xx are retried, and 4xx otherwise is permanent.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::metrics::names;

/// The standard error body. `code`, `fix` and `docs` are omitted rather than
/// serialized as null when absent, matching the wire contract.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub name: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
}

/// One field-level problem in a [`ValidationBody`].
#[derive(Debug, Serialize)]
pub struct FieldError {
    pub path: String,
    pub message: String,
}

/// The validation error body: the standard shape plus `errors`.
#[derive(Debug, Serialize)]
pub struct ValidationBody {
    pub name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub errors: Vec<FieldError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// Every failure the `/v0` surface can return.
///
/// Internal detail never reaches the caller: [`Self::Internal`] carries an
/// `anyhow::Error` for the log line only, and its response body is a fixed
/// string.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Bad input: caps exceeded, reserved labels, malformed page token, bad
    /// `Idempotency-Key`, and so on. One entry per offending field.
    #[error("validation failed")]
    Validation(Vec<FieldError>),

    /// Missing or unrecognized bearer key. Never returned when the key cache
    /// simply could not be loaded — see [`Self::Unavailable`].
    #[error("unauthorized")]
    Unauthorized,

    #[error("not found")]
    NotFound,

    /// The same `Idempotency-Key` with a different request.
    #[error("conflict: {0}")]
    Conflict(String),

    #[error("payload too large")]
    TooLarge,

    /// A route the API contract defines but this service does not implement.
    #[error("not implemented")]
    NotImplemented,

    /// The key cache has never loaded and the parameter store is unreachable.
    /// 503 rather than 401, because the SDKs retry 503 and do not retry 401:
    /// a transient SSM outage must not look like a bad key.
    #[error("service unavailable")]
    Unavailable,

    /// An upstream AWS call failed in a way the caller cannot act on.
    #[error("upstream failure")]
    BadGateway(#[source] anyhow::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl ApiError {
    /// A single-field validation failure, the common case.
    pub fn field(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation(vec![FieldError {
            path: path.into(),
            message: message.into(),
        }])
    }

    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::BadGateway(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The `name` field SDK clients match on.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Validation(_) => "ValidationError",
            Self::Unauthorized => "UnauthorizedError",
            Self::NotFound => "NotFoundError",
            Self::Conflict(_) => "ConflictError",
            Self::TooLarge => "PayloadTooLargeError",
            Self::NotImplemented => "NotImplementedError",
            Self::Unavailable => "ServiceUnavailableError",
            Self::BadGateway(_) | Self::Internal(_) => "InternalServerError",
        }
    }

    /// The caller-visible message. Internal and upstream failures collapse to
    /// a fixed string so nothing about the infrastructure leaks.
    fn public_message(&self) -> String {
        match self {
            Self::Validation(_) => "Request validation failed.".to_owned(),
            Self::Unauthorized => "Missing or invalid API key.".to_owned(),
            Self::Conflict(detail) => detail.clone(),
            Self::NotFound => "Resource not found.".to_owned(),
            Self::TooLarge => "Request body is too large.".to_owned(),
            Self::NotImplemented => "This endpoint is not implemented.".to_owned(),
            Self::Unavailable => "API keys are temporarily unavailable; retry shortly.".to_owned(),
            Self::BadGateway(_) | Self::Internal(_) => "Internal server error.".to_owned(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        record(&self, status);

        let mut response = match self {
            Self::Validation(errors) => {
                let body = ValidationBody {
                    name: "ValidationError",
                    message: Some("Request validation failed.".to_owned()),
                    errors,
                    code: None,
                };
                (status, Json(body)).into_response()
            }
            ref other => {
                let body = ErrorBody {
                    name: other.name(),
                    message: other.public_message(),
                    code: None,
                    fix: None,
                    docs: None,
                };
                (status, Json(body)).into_response()
            }
        };

        // The SDKs honour Retry-After on 503; without it they back off blindly.
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
        }
        response
    }
}

/// Logs and counts a failure. Key material never appears here: auth failures
/// are counted, never described.
fn record(error: &ApiError, status: StatusCode) {
    match error {
        ApiError::Unauthorized => {
            metrics::counter!(names::API_AUTH_FAILURES).increment(1);
            tracing::warn!(event = "api_auth_failure", "rejected API request");
        }
        ApiError::Internal(source) | ApiError::BadGateway(source) => {
            metrics::counter!(names::INTERNAL_ERRORS).increment(1);
            tracing::error!(
                error = ?source,
                status = status.as_u16(),
                event = "api_internal_error",
                "API request failed"
            );
        }
        other => {
            tracing::warn!(
                error = %other,
                status = status.as_u16(),
                event = "api_error",
                "API request rejected"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::{Value, json};

    use super::*;

    async fn body_of(error: ApiError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn not_found_matches_the_wire_shape() {
        let (status, body) = body_of(ApiError::NotFound).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            body,
            json!({"name": "NotFoundError", "message": "Resource not found."})
        );
    }

    #[tokio::test]
    async fn validation_carries_per_field_errors() {
        let (status, body) = body_of(ApiError::field("limit", "must be 1-100")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            json!({
                "name": "ValidationError",
                "message": "Request validation failed.",
                "errors": [{"path": "limit", "message": "must be 1-100"}],
            })
        );
    }

    #[tokio::test]
    async fn internal_detail_never_reaches_the_caller() {
        let (status, body) = body_of(ApiError::Internal(anyhow::anyhow!(
            "dynamodb table arn:aws:dynamodb:us-east-1:123456789012:table/secret"
        )))
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["name"], "InternalServerError");
        assert_eq!(body["message"], "Internal server error.");
        assert!(!body.to_string().contains("secret"));
    }

    #[tokio::test]
    async fn cold_key_cache_is_retryable_503_not_401() {
        let (status, body) = body_of(ApiError::Unavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["name"], "ServiceUnavailableError");
    }

    #[tokio::test]
    async fn unavailable_sets_retry_after() {
        let response = ApiError::Unavailable.into_response();
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            HeaderValue::from_static("5")
        );
    }

    #[test]
    fn optional_fields_are_omitted_not_null() {
        let body = ErrorBody {
            name: "NotFoundError",
            message: "Resource not found.".to_owned(),
            code: None,
            fix: None,
            docs: None,
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(
            json,
            r#"{"name":"NotFoundError","message":"Resource not found."}"#
        );
    }
}
