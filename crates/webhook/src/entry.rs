//! Single-binary ingress dispatch: one Lambda serves both delivery pathways.
//!
//! A Function URL invocation arrives as an API Gateway v2 payload (Function
//! URLs reuse API Gateway's payload format 2.0 on the wire — no API Gateway
//! service is involved) and is served by the Axum router through
//! `lambda_http`'s `Adapter` — the same machinery `lambda_http::run` uses. A direct SNS → Lambda invocation
//! arrives as a `Records[].Sns` event; each record runs through the identical
//! verify → persist → act → publish pipeline. Neither pathway needs routing
//! config: the event family comes from
//! [`DomainEvent::classify`](crate::model::DomainEvent::classify) on the
//! payload shape.
//!
//! The response-code retry protocol maps onto the async-invoke contract:
//! 2xx/4xx outcomes complete the invocation (no redelivery), 5xx outcomes
//! fail it so Lambda's async retry redelivers. `error.rs` remains the single
//! place that classifies failures. NOTE: the async-invoke queue retries twice
//! and then drops the event unless the function has an on-failure
//! destination; the HTTP pathway's SNS delivery policy retries far longer.
//!
//! The `Sns` record is deliberately NOT parsed with `aws_lambda_events`'
//! `SnsMessage`: that type parses `Timestamp` into `chrono::DateTime`, whose
//! serialization drops trailing `.000` subseconds — rebuilding the signed
//! canonical string from it would reject every message published on a whole
//! second. The record deserializes straight into the verifier's envelope,
//! keeping all signed field values verbatim.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::response::{IntoResponse as _, Response};
use lambda_http::aws_lambda_events::apigw::ApiGatewayV2httpRequest;
use lambda_http::request::LambdaRequest;
use lambda_http::tower::ServiceExt as _;
use lambda_http::{Adapter, Context, LambdaEvent, lambda_runtime, service_fn};
use serde::Deserialize;
use serde_json::Value;

use crate::app::app;
use crate::sns::extractor::VerifiedSns;
use crate::sns::{Ingress, handle_sns};
use crate::state::{AppState, Services};

/// A direct SNS → Lambda invocation (`Records[].Sns`). Each record's `Sns`
/// object is the same envelope SNS posts over HTTPS (with `SigningCertUrl` /
/// `UnsubscribeUrl` casing); it stays a [`Value`] here so the signed field
/// values reach [`VerifiedSns::verify`] verbatim.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsEvent {
    pub records: Vec<SnsRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsRecord {
    pub sns: Value,
}

/// Starts the Lambda runtime with the dual-pathway dispatcher.
///
/// # Errors
///
/// Returns an error if the runtime cannot start or its event loop fails.
pub async fn run<T: Services>(state: Arc<AppState<T>>) -> Result<(), lambda_http::Error> {
    let router = app(Arc::clone(&state));
    // Boxing keeps the per-invoke future off the stack and caps the type
    // nesting the compiler must lay out (runtime → dispatch → router).
    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        Box::pin(dispatch(Arc::clone(&state), router.clone(), event))
    }))
    .await
}

/// Handles one invocation, dispatching on the payload shape. Public so the
/// handler test suite can drive both pathways without a Lambda runtime.
///
/// # Errors
///
/// Returns an error when the payload matches neither pathway, or when a
/// direct SNS delivery hit a transient (5xx-class) failure — failing the
/// invocation is what recruits the async-invoke retry.
pub async fn dispatch<T: Services>(
    state: Arc<AppState<T>>,
    router: Router,
    event: LambdaEvent<Value>,
) -> Result<Value, lambda_http::Error> {
    let LambdaEvent { payload, context } = event;
    // Unambiguous: every SNS invocation has a top-level `Records` array and a
    // Function URL payload never does — an attacker-controlled HTTP body only
    // ever appears as a JSON *string field* inside the API Gateway envelope,
    // so it cannot fake this shape.
    if payload.get("Records").is_some() {
        // Both DynamoDB streams and direct SNS deliver a `Records` array; the
        // stream records carry `eventSource: aws:dynamodb` (and a `dynamodb`
        // object), while SNS records carry `Sns`.
        if is_dynamodb_stream(&payload) {
            return crate::stream::handle_stream(&state, payload).await;
        }
        let event = SnsEvent::deserialize(payload)
            .map_err(|e| format!("payload has Records but is not an SNS event: {e}"))?;
        handle_direct(&state, event).await?;
        Ok(Value::Null)
    } else {
        serve_http(router, payload, context).await
    }
}

/// Distinguishes a DynamoDB Streams invocation from a direct SNS one — both
/// arrive as a `Records` array.
fn is_dynamodb_stream(payload: &Value) -> bool {
    payload
        .get("Records")
        .and_then(Value::as_array)
        .and_then(|records| records.first())
        .is_some_and(|first| {
            first.get("eventSource").and_then(Value::as_str) == Some("aws:dynamodb")
                || first.get("dynamodb").is_some()
        })
}

async fn serve_http(
    router: Router,
    payload: Value,
    context: Context,
) -> Result<Value, lambda_http::Error> {
    let request = ApiGatewayV2httpRequest::deserialize(payload)
        .map_err(|e| format!("payload is neither an SNS event nor a Function URL request: {e}"))?;
    let event = LambdaEvent::new(LambdaRequest::ApiGatewayV2(request), context);
    let response = match Adapter::from(router).oneshot(event).await {
        Ok(response) => response,
        Err(infallible) => match infallible {},
    };
    Ok(serde_json::to_value(response)?)
}

async fn handle_direct<T: Services>(
    state: &AppState<T>,
    event: SnsEvent,
) -> Result<(), lambda_http::Error> {
    for record in event.records {
        let raw_body = Bytes::from(serde_json::to_vec(&record.sns)?);
        let status = process_record(state, raw_body).await.status();
        if status.is_server_error() {
            // Fail the whole invocation: any unprocessed records redeliver
            // with it, and the idempotent persist makes re-runs safe.
            return Err(format!(
                "transient failure ({status}) processing direct SNS delivery; \
                 failing the invocation so it retries"
            )
            .into());
        }
    }
    Ok(())
}

async fn process_record<T: Services>(state: &AppState<T>, raw_body: Bytes) -> Response {
    let verified = match VerifiedSns::verify(state, raw_body).await {
        Ok(verified) => verified,
        Err(error) => return error.into_response(),
    };
    match handle_sns(state, Ingress::Direct, verified).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
