//! `CloudWatch` Embedded Metrics Format (EMF) integration.
//!
//! Replaces the previous approach of log metric filters in the SAM template
//! with inline EMF emission via the [`metrics`] facade. `CloudWatch` extracts
//! metric datapoints directly from the structured JSON log line — no filter
//! pattern matching required, and dimensions/values are available immediately.
//!
//! The [`metrics_cloudwatch_embedded`] backend handles batching, the EMF JSON
//! schema, and per-invocation flushing (via its `lambda` feature which wraps
//! the Lambda runtime's tower service).

use metrics_cloudwatch_embedded::Collector;

/// Metric names — constants so callers and dashboards stay in sync.
pub mod names {
    pub const MESSAGES_RECEIVED: &str = "MessagesReceived";
    pub const SIGNATURE_REJECTIONS: &str = "SignatureRejections";
    pub const ALLOWLIST_REJECTIONS: &str = "AllowlistRejections";
    pub const UNCLASSIFIED_PAYLOADS: &str = "UnclassifiedPayloads";
    pub const DUPLICATES: &str = "Duplicates";
    pub const EVENTS_PUBLISHED: &str = "EventsPublished";
    pub const PUBLISH_FAILURES: &str = "PublishFailures";
    pub const INTERNAL_ERRORS: &str = "InternalErrors";
    pub const ACTION_FAILURES: &str = "ActionFailures";
    pub const RESUBSCRIBES: &str = "Resubscribes";
    pub const SUBSCRIPTIONS_LOST: &str = "SubscriptionsLost";
    pub const COLD_START: &str = "ColdStart";
    pub const LATENCY: &str = "Latency";
}

/// Initialize the EMF collector. Returns a `&'static Collector` handle which
/// must be flushed at the end of each Lambda invocation.
///
/// The collector emits a `ColdStart` metric (Count = 1) on the first
/// invocation only — handled internally by the library.
///
/// # Errors
///
/// Returns an error if a metrics recorder is already installed (should not
/// happen in normal operation).
pub fn init(namespace: String) -> Result<&'static Collector, metrics_cloudwatch_embedded::Error> {
    metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace(namespace)
        .with_dimension("function", function_name())
        .lambda_cold_start_metric(names::COLD_START)
        .init()
}

/// The Lambda function name from the environment, or a fallback for local
/// development.
fn function_name() -> String {
    std::env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or_else(|_| "local".to_owned())
}
