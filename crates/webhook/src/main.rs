use std::sync::Arc;
use std::time::Duration;

use aws_messaging_webhook::aws::AwsServices;
use aws_messaging_webhook::config::Config;
use aws_messaging_webhook::entry;
use aws_messaging_webhook::state::AppState;
use sns_message_verifier::SnsVerifier;

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    lambda_http::tracing::init_default_subscriber();

    let (config, allowlist) = Config::from_env()?;

    // Initialize EMF metrics collector. The namespace matches the CloudWatch
    // metric namespace previously populated by log metric filters (the stack
    // name). Falls back to the event_source when STACK_NAME is unset (local
    // dev).
    let namespace = std::env::var("STACK_NAME").unwrap_or_else(|_| config.event_source.clone());
    let collector = aws_messaging_webhook::metrics::init(namespace)
        .map_err(|e| format!("failed to initialize metrics collector: {e}"))?;

    let sdk_config = aws_config::load_from_env().await;
    let services = AwsServices::new(&sdk_config, config.clone());

    let mut verifier = SnsVerifier::builder();
    let mut dangerous_subscribe_url_prefix = None;
    // Local development against a fake SNS (`cargo lambda watch`): honored in
    // debug builds only, so no release binary can bypass verification.
    #[cfg(debug_assertions)]
    if let Ok(prefix) = std::env::var("SNS_CERT_HOST_OVERRIDE") {
        tracing::warn!(prefix, "SNS cert host override active (debug build only)");
        verifier = verifier.dangerous_allow_cert_url_prefix(prefix.clone());
        dangerous_subscribe_url_prefix = Some(prefix);
    }

    let state = Arc::new(AppState {
        services,
        verifier: verifier.build()?,
        allowlist,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            // Never follow redirects: validate_subscribe_url restricts the
            // initial host to SNS, but following a 3xx off that host would be
            // SSRF from the Lambda's network context.
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        config,
        dangerous_subscribe_url_prefix,
    });

    entry::run(state, collector).await
}
