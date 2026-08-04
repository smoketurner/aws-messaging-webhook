use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::Response;
use axum::routing::{get, post};
use tower_http::trace::TraceLayer;

use crate::error::AppError;
use crate::model::Source;
use crate::sns::extractor::VerifiedSns;
use crate::sns::handle_sns;
use crate::state::{AppState, Services};

/// SNS caps messages at 256 KiB; 1 MiB bounds abuse with headroom.
const MAX_BODY_BYTES: usize = 1024 * 1024;

pub fn app<T: Services>(state: Arc<AppState<T>>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/webhooks/sms/inbound", post(sms_inbound::<T>))
        .route("/webhooks/sms/events", post(sms_events::<T>))
        .route("/webhooks/ses/events", post(ses_events::<T>))
        .route("/webhooks/ses/inbound", post(ses_inbound::<T>))
        .layer(TraceLayer::new_for_http())
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn sms_inbound<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Source::SmsInbound, verified).await
}

async fn sms_events<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Source::SmsEvents, verified).await
}

async fn ses_events<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Source::SesEvents, verified).await
}

async fn ses_inbound<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    verified: VerifiedSns,
) -> Result<Response, AppError> {
    handle_sns(&state, Source::SesInbound, verified).await
}
