//! Handlers for routes the API contract defines but this service does not
//! implement, and for requests that match no route at all.
//!
//! These matter for client compatibility: a client calling an endpoint this
//! service doesn't implement must still get a body it can parse, not axum's
//! bare status line. Unknown paths *outside* `/v0` keep axum's default 404 —
//! only the `/v0` surface promises the contract's shape.

use std::future::{Ready, ready};

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::api::error::{ApiError, ErrorBody};

/// A route the API contract defines that this service does not implement:
/// drafts, labels, pods, domains, webhooks, and the inbox-management and
/// delete routes.
pub fn not_implemented() -> Ready<ApiError> {
    ready(ApiError::NotImplemented)
}

/// Fallback for any unmatched path under `/v0`.
pub fn unknown_route() -> Ready<ApiError> {
    ready(ApiError::NotFound)
}

/// Fallback for a known `/v0` path used with the wrong method. Built inline
/// rather than as an [`ApiError`] variant because 405 is the only status the
/// router raises on the caller's behalf, and it carries no detail beyond the
/// name.
pub fn method_not_allowed() -> Ready<Response> {
    let body = ErrorBody {
        name: "MethodNotAllowedError",
        message: "This method is not allowed on this endpoint.".to_owned(),
        code: None,
        fix: None,
        docs: None,
    };
    ready((StatusCode::METHOD_NOT_ALLOWED, Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::{Value, json};

    use super::*;

    async fn json_of(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn unimplemented_routes_answer_501_with_a_parseable_body() {
        let (status, body) = json_of(not_implemented().await.into_response()).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            body,
            json!({
                "name": "NotImplementedError",
                "message": "This endpoint is not implemented.",
            })
        );
    }

    #[tokio::test]
    async fn unknown_v0_paths_answer_404_with_a_parseable_body() {
        let (status, body) = json_of(unknown_route().await.into_response()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["name"], "NotFoundError");
    }

    #[tokio::test]
    async fn wrong_method_answers_405_with_a_parseable_body() {
        let (status, body) = json_of(method_not_allowed().await).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["name"], "MethodNotAllowedError");
    }
}
