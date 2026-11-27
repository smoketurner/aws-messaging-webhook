use aws_messaging_webhook::logging;
use aws_messaging_webhook::metrics;
use std::sync::Arc;

use aws_messaging_webhook::api::keys::KeyCache;
use aws_messaging_webhook::aws::{Aws, Components};
use aws_messaging_webhook::config::Config;
use aws_messaging_webhook::entry;
use aws_messaging_webhook::state::AppState;
use sns_message_verifier::SnsVerifier;
#[cfg(debug_assertions)]
use sns_message_verifier::SnsVerifierBuilder;

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    logging::init();

    let (config, allowlist) = Config::from_env()?;

    // The EMF collector's namespace is the stack name, falling back to the
    // event source when STACK_NAME is unset (local dev).
    let namespace = std::env::var("STACK_NAME").unwrap_or_else(|_| config.event_source.clone());
    let collector = metrics::init(namespace)
        .map_err(|e| format!("failed to initialize metrics collector: {e}"))?;

    let sdk_config = aws_config::load_from_env().await;
    let Components {
        mailbox,
        events,
        publisher,
        sms,
        ses,
        objects,
        fetcher,
        key_source,
    } = Components::new(&sdk_config, &config);

    #[cfg(debug_assertions)]
    let (verifier, dangerous_subscribe_url_prefix) = cert_host_override(SnsVerifier::builder());
    #[cfg(not(debug_assertions))]
    let (verifier, dangerous_subscribe_url_prefix) = (SnsVerifier::builder(), None);

    // One client for both users — fetching signing certificates and
    // confirming subscriptions — so they share a connection pool. It never
    // follows redirects, which both of them depend on.
    let http = sns_message_verifier::no_redirect_client()?;
    let state = Arc::new(AppState::<Aws> {
        mailbox,
        events,
        publisher,
        sms,
        ses,
        objects,
        fetcher,
        key_source,
        api_keys: KeyCache::new(),
        verifier: verifier.http_client(http.clone()).build()?,
        allowlist,
        http,
        config,
        dangerous_subscribe_url_prefix,
    });

    entry::run(state, collector).await
}

/// Local development against a fake SNS (`cargo lambda watch`): honored in
/// debug builds only, so no release binary can bypass verification.
#[cfg(debug_assertions)]
fn cert_host_override(verifier: SnsVerifierBuilder) -> (SnsVerifierBuilder, Option<String>) {
    let Ok(prefix) = std::env::var("SNS_CERT_HOST_OVERRIDE") else {
        return (verifier, None);
    };
    tracing::warn!(prefix, "SNS cert host override active (debug build only)");
    (
        verifier.dangerous_allow_cert_url_prefix(prefix.clone()),
        Some(prefix),
    )
}
