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

/// Shared application state; the router holds it behind one `Arc`.
pub struct AppState<T: Services> {
    pub services: T,
    pub verifier: SnsVerifier,
    pub allowlist: TopicAllowlist,
    /// Client for `SubscribeURL` GETs (confirm and re-subscribe).
    pub http: reqwest::Client,
    pub config: Config,
    /// Accept `SubscribeURL`s with this prefix, bypassing the SNS host
    /// policy. Tests and debug builds only.
    pub dangerous_subscribe_url_prefix: Option<String>,
}
