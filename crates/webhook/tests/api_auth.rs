#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! Bearer auth on the `/v0` surface (D20), driven through the real router.
//!
//! These tests pin the three rules an SDK client depends on: an unknown key
//! is rejected, a parameter-store outage is retryable rather than a
//! rejection, and the webhook paths stay unauthenticated.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;
use webhook_test_support::harness;

const KEY: &str = "am_live_key";

/// Sends a `/v0` request, optionally with an `Authorization` header.
async fn get(
    harness: &webhook_test_support::Harness,
    path: &str,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::get(path);
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    let response = aws_messaging_webhook::app::app(harness.state.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn a_request_without_a_key_is_rejected() {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);

    let (status, body) = get(&h, "/v0/pods", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["name"], "UnauthorizedError");
}

#[tokio::test]
async fn an_unknown_key_is_rejected() {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);

    let (status, body) = get(&h, "/v0/pods", Some("am_wrong")).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["name"], "UnauthorizedError");
}

#[tokio::test]
async fn a_cold_cache_that_cannot_load_answers_503_not_401() {
    // The SDKs retry 503 and never retry 401, so an SSM outage must not look
    // like a bad key.
    let h = harness().await;
    h.state.services.api_keys.make_unavailable();

    let (status, body) = get(&h, "/v0/pods", Some(KEY)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["name"], "ServiceUnavailableError");
}

#[tokio::test]
async fn an_authenticated_call_to_an_unimplemented_route_answers_501() {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);

    let (status, body) = get(&h, "/v0/pods", Some(KEY)).await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["name"], "NotImplementedError");
}

#[tokio::test]
async fn an_unknown_v0_path_answers_the_agentmail_404_body() {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);

    let (status, body) = get(&h, "/v0/nope", Some(KEY)).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["name"], "NotFoundError");
}

#[tokio::test]
async fn auth_does_not_leak_which_v0_paths_exist() {
    // Without a key, a real path and a nonsense path must be indistinguishable.
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);

    let (real, _) = get(&h, "/v0/pods", None).await;
    let (nonsense, _) = get(&h, "/v0/nope", None).await;

    assert_eq!(real, StatusCode::UNAUTHORIZED);
    assert_eq!(nonsense, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_webhook_paths_stay_unauthenticated() {
    // SNS cannot present a bearer key; /v0 auth must not cover /webhooks/*.
    let h = harness().await;
    h.state.services.api_keys.make_unavailable();

    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(
            Request::post("/webhooks/ses/events")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    // 400 for the missing SNS header — reached the handler, was not rejected
    // by auth.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn healthz_stays_open() {
    let h = harness().await;
    h.state.services.api_keys.make_unavailable();

    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
