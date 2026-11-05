use sns_message_verifier::SnsVerifier;

use crate::actions::{SesApi, SmsVoiceApi};
use crate::allowlist::TopicAllowlist;
use crate::api::keys::{ApiKeySource, KeyCache};
use crate::config::Config;
use crate::mail::fetch::AttachmentFetcher;
use crate::mail::objects::ObjectStore;
use crate::publish::PublishEvents;
use crate::store::EventStore;
use mailbox_store::Mailbox;
use mailbox_store::Table;

/// The component types one deployment wires together, one per external
/// system, so handlers carry a single type parameter. Production is
/// [`crate::aws::Aws`]; tests supply in-process fakes.
pub trait Backend: Send + Sync + 'static {
    type Table: Table + 'static;
    type Events: EventStore + 'static;
    type Publisher: PublishEvents + 'static;
    type Sms: SmsVoiceApi + 'static;
    type Ses: SesApi + 'static;
    type Objects: ObjectStore + 'static;
    type Fetcher: AttachmentFetcher + 'static;
    type Keys: ApiKeySource + 'static;
}

/// Shared application state; the router holds it behind one `Arc`.
pub struct AppState<B: Backend> {
    pub mailbox: Mailbox<B::Table>,
    pub events: B::Events,
    pub publisher: B::Publisher,
    pub sms: B::Sms,
    pub ses: B::Ses,
    pub objects: B::Objects,
    pub fetcher: B::Fetcher,
    /// Where the bearer API key hashes are read from.
    pub key_source: B::Keys,
    /// Bearer API keys, cached for the life of this execution
    /// environment and shared by every `/v0` request.
    pub api_keys: KeyCache,
    pub verifier: SnsVerifier,
    pub allowlist: TopicAllowlist,
    /// Client for `SubscribeURL` GETs (confirm and re-subscribe).
    pub http: reqwest::Client,
    pub config: Config,
    /// Accept `SubscribeURL`s with this prefix, bypassing the SNS host
    /// policy. Tests and debug builds only.
    pub dangerous_subscribe_url_prefix: Option<String>,
}
