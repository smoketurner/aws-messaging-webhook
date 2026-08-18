//! `CloudWatch` Embedded Metrics Format (EMF) integration.
//!
//! Replaces the previous approach of log metric filters in the SAM template
//! with inline EMF emission via the [`metrics`] facade. `CloudWatch` extracts
//! metric datapoints directly from the structured JSON log line — no filter
//! pattern matching required, and dimensions/values are available immediately.
//!
//! The [`metrics_cloudwatch_embedded`] backend handles batching and the EMF
//! JSON schema. Per-invocation metrics are flushed to stdout at the end of
//! each Lambda invocation (see [`crate::entry`]); the `ColdStart` metric is
//! emitted once per execution environment via [`emit_cold_start`] on the first
//! invocation. The library only emits `ColdStart` through its own tower
//! middleware (`MetricsLayer`/`MetricsService`), which this application does
//! not use, so it is emitted explicitly here instead.

use std::sync::atomic::{AtomicBool, Ordering};

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
/// The `ColdStart` metric is **not** emitted automatically by this collector.
/// The `metrics_cloudwatch_embedded` library only emits it when its
/// `MetricsLayer`/`MetricsService` tower middleware wraps the Lambda handler,
/// which this application does not use; [`emit_cold_start`] writes it once per
/// execution environment on the first invocation instead.
///
/// # Errors
///
/// Returns an error if a metrics recorder is already installed (should not
/// happen in normal operation).
pub fn init(namespace: String) -> Result<&'static Collector, metrics_cloudwatch_embedded::Error> {
    metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace(namespace)
        .with_dimension("function", function_name())
        .init()
}

/// Emit the `ColdStart` metric (Count = 1) the first time this is called for
/// the given `emitted` flag.
///
/// `emitted` tracks whether the cold-start metric has already been recorded for
/// this execution environment. The first caller to observe `false` atomically
/// flips it to `true` and writes the metric; all later callers are no-ops. In
/// production `emitted` is a process-global `static AtomicBool`; tests pass a
/// local flag so the first/subsequent-call behavior is exercisable without
/// process-global state.
///
/// The metric is written as a standalone EMF document via
/// [`Collector::write_single`], separate from the per-invocation
/// [`Collector::flush`], mirroring the layout the library's own
/// `MetricsService` produces.
pub fn emit_cold_start(emitted: &AtomicBool, collector: &Collector, writer: impl std::io::Write) {
    if emitted
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        // A prior invocation already recorded cold start for this execution
        // environment; nothing to emit.
        return;
    }
    if let Err(error) =
        collector.write_single(names::COLD_START, Some(metrics::Unit::Count), 1, writer)
    {
        tracing::error!(error = %error, "failed to write ColdStart metric");
    }
}

/// The Lambda function name from the environment, or a fallback for local
/// development.
fn function_name() -> String {
    std::env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or_else(|_| "local".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// All tests share a single leaked `Collector`. `Builder::init` installs the
    /// process-global recorder, so it can only succeed once per test binary;
    /// `OnceLock` guarantees that.
    fn collector() -> &'static Collector {
        static COLLECTOR: std::sync::OnceLock<&'static Collector> = std::sync::OnceLock::new();
        COLLECTOR.get_or_init(|| {
            metrics_cloudwatch_embedded::Builder::new()
                .cloudwatch_namespace("TestNamespace")
                .with_dimension("function", "test-function")
                .with_timestamp(0)
                .init()
                .expect("test collector init")
        })
    }

    /// Parses the EMF JSON written by `write_single`, trimming the trailing
    /// newline that `writeln!` appends.
    fn parse_emf(output: &[u8]) -> Value {
        let text = std::str::from_utf8(output).expect("EMF output is UTF-8");
        serde_json::from_str(text.trim()).expect("valid EMF JSON")
    }

    #[test]
    fn first_call_emits_a_valid_cold_start_emf_document() {
        let collector = collector();
        let emitted = AtomicBool::new(false);
        let mut output = Vec::new();

        emit_cold_start(&emitted, collector, &mut output);

        assert!(!output.is_empty(), "first invocation must emit ColdStart");
        let doc = parse_emf(&output);

        // Value and dimension.
        assert_eq!(doc["ColdStart"], json!(1));
        assert_eq!(doc["function"], json!("test-function"));
        // Namespace.
        assert_eq!(
            doc["_aws"]["CloudWatchMetrics"][0]["Namespace"],
            json!("TestNamespace")
        );
        // Dimensions: a single member `["function"]`.
        let dims = doc["_aws"]["CloudWatchMetrics"][0]["Dimensions"][0].as_array();
        let dims = dims.expect("dimensions array");
        assert_eq!(dims, &[json!("function")]);
        // The single metric directive.
        let metrics = doc["_aws"]["CloudWatchMetrics"][0]["Metrics"].as_array();
        let metrics = metrics.expect("metrics array");
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["Name"], json!("ColdStart"));
        assert_eq!(metrics[0]["Unit"], json!("Count"));
    }

    #[test]
    fn second_call_emits_nothing() {
        let collector = collector();
        let emitted = AtomicBool::new(false);
        let mut first = Vec::new();
        let mut second = Vec::new();

        emit_cold_start(&emitted, collector, &mut first);
        emit_cold_start(&emitted, collector, &mut second);

        assert!(!first.is_empty(), "first invocation emitted");
        assert!(
            second.is_empty(),
            "second invocation must not re-emit ColdStart"
        );
    }

    #[test]
    fn already_emitted_flag_never_emits() {
        // Simulates an execution environment that already recorded its cold start.
        let collector = collector();
        let emitted = AtomicBool::new(true);
        let mut output = Vec::new();

        emit_cold_start(&emitted, collector, &mut output);

        assert!(output.is_empty(), "must not emit when flag is already set");
    }
}
