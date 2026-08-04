use std::sync::Arc;
use std::time::Duration;

use aws_messaging_webhook::app::app;
use aws_messaging_webhook::aws::AwsServices;
use aws_messaging_webhook::config::Config;
use aws_messaging_webhook::state::AppState;
use sns_message_verifier::SnsVerifier;

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    lambda_http::tracing::init_default_subscriber();

    let (config, allowlist) = Config::from_env()?;

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

    lambda_http::run(app(state)).await
}
