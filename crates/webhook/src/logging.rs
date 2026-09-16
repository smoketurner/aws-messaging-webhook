//! Tracing subscriber setup, replacing `lambda_http::tracing::init_default_subscriber`
//! with a filter that never lets a dependency emit at `TRACE`.
//!
//! `lambda_runtime`'s raw-invoke logging (`api_response.rs`) and
//! `aws-smithy-runtime`'s request/response logging (`orchestrator.rs`) both
//! log full request/response bodies at `TRACE` — a source of credential and
//! PII leakage this application must never enable, even if an operator (or a
//! stack that predates this restriction) sets `AWS_LAMBDA_LOG_LEVEL=TRACE`.
//! [`directives`] builds an `EnvFilter` directive string that caps the
//! untargeted (dependency) default at `DEBUG` while letting this crate's own
//! target run at the configured level.

use lambda_http::tracing::subscriber::EnvFilter;
use tracing::Level;

/// Builds the `EnvFilter` directive string for `level` (parsed case-insensitively;
/// an unrecognized value falls back to `INFO`): the untargeted default capped at
/// `DEBUG` (so `TRACE` can never reach a dependency), `reqwest` and the hyper
/// crates fixed at `WARN`, and this crate's own target left at the requested
/// level uncapped.
#[must_use]
pub fn directives(level: &str) -> String {
    let level = level.parse::<Level>().unwrap_or(Level::INFO);
    let default = level.min(Level::DEBUG);
    format!("{default},reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook={level}")
}

/// Installs the process-global tracing subscriber.
///
/// Reads `AWS_LAMBDA_LOG_LEVEL` only — `RUST_LOG` is ignored, so the filter is
/// fully determined by [`directives`] rather than an operator-supplied
/// directive string that could re-enable `TRACE` on a dependency. Emits JSON
/// when `AWS_LAMBDA_LOG_FORMAT=json`, matching Lambda's advanced logging
/// controls.
///
/// # Panics
///
/// Panics if a global subscriber is already installed (should not happen in
/// normal operation: this is called once, at process start).
pub fn init() {
    let level = std::env::var("AWS_LAMBDA_LOG_LEVEL").unwrap_or_else(|_| "INFO".to_owned());
    let json = std::env::var("AWS_LAMBDA_LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"));
    let filter = EnvFilter::builder().parse_lossy(directives(&level));
    let subscriber = lambda_http::tracing::subscriber::fmt()
        .with_target(false)
        .without_time()
        .with_env_filter(filter);
    if json {
        subscriber.json().init();
    } else {
        subscriber.init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_leaves_the_default_uncapped() {
        assert_eq!(
            directives("INFO"),
            "INFO,reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook=INFO"
        );
    }

    #[test]
    fn debug_leaves_the_default_uncapped() {
        assert_eq!(
            directives("DEBUG"),
            "DEBUG,reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook=DEBUG"
        );
    }

    #[test]
    fn warn_and_error_pass_through() {
        assert_eq!(
            directives("WARN"),
            "WARN,reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook=WARN"
        );
        assert_eq!(
            directives("ERROR"),
            "ERROR,reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook=ERROR"
        );
    }

    /// The one case the cap exists for: a stack still configured with the
    /// pre-migration `TRACE` level must never let a dependency log at TRACE,
    /// even though this crate's own target still honors it.
    #[test]
    fn trace_caps_the_default_to_debug_but_not_this_crate() {
        assert_eq!(
            directives("TRACE"),
            "DEBUG,reqwest=warn,hyper=warn,hyper_util=warn,aws_messaging_webhook=TRACE"
        );
    }

    #[test]
    fn unrecognized_level_falls_back_to_info() {
        assert_eq!(directives("bogus"), directives("INFO"));
        assert_eq!(directives(""), directives("INFO"));
    }

    #[test]
    fn parsing_is_case_insensitive() {
        assert_eq!(directives("debug"), directives("DEBUG"));
        assert_eq!(directives("Warn"), directives("WARN"));
    }
}
