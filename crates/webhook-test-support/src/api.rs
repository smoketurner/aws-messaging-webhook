//! A `/v0` client over the real router: the request helpers every mailbox
//! API test drives. [`get`], [`post`] and [`patch`] authenticate with
//! [`TEST_API_KEY`](crate::TEST_API_KEY); [`call`] sends exactly the headers
//! it is given, for the unauthenticated cases.

use axum::body::{Body, Bytes};
use axum::http::{Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;

use crate::{Harness, TEST_API_KEY};

/// Sends `request` through the router and returns the status and raw body.
pub async fn send(h: &Harness, request: Request<Body>) -> (StatusCode, Bytes) {
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes)
}

/// Sends `method path` with exactly `headers`, and `body` as JSON when
/// given. The response body is parsed as JSON, or `Null` when it isn't.
pub async fn call(
    h: &Harness,
    method: Method,
    path: &str,
    body: Option<&Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let body = match body {
        Some(json) => {
            request = request.header("content-type", "application/json");
            Body::from(serde_json::to_vec(json).unwrap())
        }
        None => Body::empty(),
    };
    let (status, bytes) = send(h, request.body(body).unwrap()).await;
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The `Authorization` header value for [`TEST_API_KEY`].
pub fn bearer() -> String {
    format!("Bearer {TEST_API_KEY}")
}

/// An authenticated `GET`.
pub async fn get(h: &Harness, path: &str) -> (StatusCode, Value) {
    call(h, Method::GET, path, None, &[("authorization", &bearer())]).await
}

/// An authenticated `POST` of `body` as JSON.
pub async fn post(h: &Harness, path: &str, body: &Value) -> (StatusCode, Value) {
    post_with(h, path, body, &[]).await
}

/// An authenticated `POST` of `body` as JSON, with `extra` headers.
pub async fn post_with(
    h: &Harness,
    path: &str,
    body: &Value,
    extra: &[(&str, &str)],
) -> (StatusCode, Value) {
    let bearer = bearer();
    let mut headers = vec![("authorization", bearer.as_str())];
    headers.extend_from_slice(extra);
    call(h, Method::POST, path, Some(body), &headers).await
}

/// An authenticated `PATCH` of `body` as JSON.
pub async fn patch(h: &Harness, path: &str, body: &Value) -> (StatusCode, Value) {
    call(
        h,
        Method::PATCH,
        path,
        Some(body),
        &[("authorization", &bearer())],
    )
    .await
}
