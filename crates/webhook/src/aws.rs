//! Production components wrapping the AWS SDK clients, one per seam. The
//! SDK error classification they share is `mailbox_store::sdk_error`.

pub mod api_keys;
pub mod events_table;
pub mod objects;
pub mod publisher;
pub mod ses;
pub mod sms;

use mailbox_store::dynamo_table::DynamoTable;
use mailbox_store::mailbox::Mailbox;
use mailbox_store::sdk_error::FromSdkFailure;

use crate::actions::{ActionError, ActionErrorKind};
use crate::config::Config;
use crate::mail::fetch::HttpAttachmentFetcher;
use crate::state::Backend;

/// The production [`Backend`].
pub struct Aws;

impl Backend for Aws {
    type Table = DynamoTable;
    type Events = events_table::EventsTable;
    type Publisher = publisher::EventBridgePublisher;
    type Sms = sms::SmsClient;
    type Ses = ses::SesClient;
    type Objects = objects::S3Objects;
    type Fetcher = HttpAttachmentFetcher;
    type Keys = api_keys::SsmKeys;
}

/// Every production component, built from one SDK configuration.
pub struct Components {
    pub mailbox: Mailbox<DynamoTable>,
    pub events: events_table::EventsTable,
    pub publisher: publisher::EventBridgePublisher,
    pub sms: sms::SmsClient,
    pub ses: ses::SesClient,
    pub objects: objects::S3Objects,
    pub fetcher: HttpAttachmentFetcher,
    pub key_source: api_keys::SsmKeys,
}

impl Components {
    /// Builds every client once from `sdk_config`. The mail components carry
    /// no table, bucket or parameter when mail is not configured, and fail
    /// permanently if they are ever called that way.
    #[must_use]
    pub fn new(sdk_config: &aws_config::SdkConfig, config: &Config) -> Self {
        let dynamo = aws_sdk_dynamodb::Client::new(sdk_config);
        let mail = config.mail.as_ref();
        Self {
            mailbox: Mailbox::new(DynamoTable::new(
                dynamo.clone(),
                mail.map(|mail| mail.table_name.clone()),
            )),
            events: events_table::EventsTable::new(dynamo, config.table_name.clone()),
            publisher: publisher::EventBridgePublisher::new(
                aws_sdk_eventbridge::Client::new(sdk_config),
                config.event_bus_name.clone(),
                config.event_source.clone(),
            ),
            sms: sms::SmsClient::new(aws_sdk_pinpointsmsvoicev2::Client::new(sdk_config)),
            ses: ses::SesClient::new(aws_sdk_sesv2::Client::new(sdk_config)),
            objects: objects::S3Objects::new(
                aws_sdk_s3::Client::new(sdk_config),
                mail.map(|mail| mail.bucket.clone()),
            ),
            fetcher: HttpAttachmentFetcher::new(),
            key_source: api_keys::SsmKeys::new(
                aws_sdk_ssm::Client::new(sdk_config),
                mail.map(|mail| mail.api_keys_parameter.clone()),
            ),
        }
    }
}

/// A transient action failure is worth an SNS redelivery; a permanent one is
/// logged and the event still publishes.
impl FromSdkFailure for ActionError {
    fn transient(source: anyhow::Error) -> Self {
        Self {
            kind: ActionErrorKind::Transient,
            source,
        }
    }

    fn permanent(source: anyhow::Error) -> Self {
        Self {
            kind: ActionErrorKind::Permanent,
            source,
        }
    }
}
