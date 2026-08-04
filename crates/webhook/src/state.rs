use std::sync::Arc;

use sns_message_verifier::SnsVerifier;

use crate::actions::{SesApi, SmsVoiceApi};
use crate::allowlist::TopicAllowlist;
use crate::config::Config;
use crate::publish::PublishEvents;
use crate::store::EventStore;

/// Everything the pipeline calls out to, as one bound so handlers carry a
/// single type parameter. Production implements it on one struct wrapping the
/// AWS SDK clients; tests implement it on one recording fake.
pub trait Services:
    EventStore + PublishEvents + SmsVoiceApi + SesApi + Send + Sync + 'static
{
}

impl<T> Services for T where
    T: EventStore + PublishEvents + SmsVoiceApi + SesApi + Send + Sync + 'static
{
}

/// Shared application state.
pub struct AppState<T: Services> {
    pub services: Arc<T>,
    pub verifier: Arc<SnsVerifier>,
    pub allowlist: Arc<TopicAllowlist>,
    /// Client for `SubscribeURL` GETs (confirm and re-subscribe).
    pub http: reqwest::Client,
    pub config: Arc<Config>,
}

impl<T: Services> Clone for AppState<T> {
    fn clone(&self) -> Self {
        Self {
            services: Arc::clone(&self.services),
            verifier: Arc::clone(&self.verifier),
            allowlist: Arc::clone(&self.allowlist),
            http: self.http.clone(),
            config: Arc::clone(&self.config),
        }
    }
}
