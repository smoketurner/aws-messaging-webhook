use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, FromRequestParts, State};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::response::Response;
use axum::routing::{get, post};
use lambda_http::RequestExt as _;
use tower_http::sensitive_headers::SetSensitiveRequestHeadersLayer;
use tower_http::trace::TraceLayer;

use crate::entry::context_deadline;
use crate::error::AppError;
use crate::model::Source;
use crate::sns::extractor::VerifiedSns;
use crate::sns::{Ingress, handle_sns};
use crate::state::{AppState, Services};

/// SNS caps messages at 256 KiB; 1 MiB bounds abuse with headroom.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// A real Function URL invocation, served through `Adapter`, always carries
/// the Lambda invocation context as a request extension (`Adapter::call`
/// applies `with_lambda_context` before the router runs). Only a plain axum
/// `Request` built directly — the handler test harness — lacks it, so this
/// fallback is a test-only path, never a production one.
const FALLBACK_DEADLINE_SECS: u64 = 60;

/// The invocation deadline (D48), read from the Lambda context Function URL
/// requests carry as a request extension. Falls back to now + 60 s when the
/// extension is absent (only handler tests that build a plain axum
/// `Request`).
struct Deadline(tokio::time::Instant);

impl<S> FromRequestParts<S> for Deadline
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> {
        let deadline = parts.lambda_context_ref().map_or_else(
            || tokio::time::Instant::now() + Duration::from_secs(FALLBACK_DEADLINE_SECS),
            context_deadline,
        );
        std::future::ready(Ok(Self(deadline)))
    }
}

pub fn app<T: Services>(state: Arc<AppState<T>>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/webhooks/sms/inbound", post(sms_inbound::<T>))
        .route("/webhooks/sms/events", post(sms_events::<T>))
        .route("/webhooks/ses/events", post(ses_events::<T>))
        .route("/webhooks/ses/inbound", post(ses_inbound::<T>))
        .with_state(Arc::clone(&state))
        .merge(Router::new().nest("/v0", crate::api::router(state)))
        .layer(TraceLayer::new_for_http())
        // Outside the trace layer, so a bearer key can never reach a log
        // line: the header is redacted before tracing records the request.
        .layer(SetSensitiveRequestHeadersLayer::new([AUTHORIZATION]))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn sms_inbound<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Deadline(deadline): Deadline,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(
        &state,
        Ingress::Http(Source::SmsInbound),
        verified,
        deadline,
    )
    .await
}

async fn sms_events<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Deadline(deadline): Deadline,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Ingress::Http(Source::SmsEvents), verified, deadline).await
}

async fn ses_events<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Deadline(deadline): Deadline,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Ingress::Http(Source::SesEvents), verified, deadline).await
}

async fn ses_inbound<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    Deadline(deadline): Deadline,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(
        &state,
        Ingress::Http(Source::SesInbound),
        verified,
        deadline,
    )
    .await
}
