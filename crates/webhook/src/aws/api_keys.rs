//! The production [`ApiKeySource`]: the SSM `SecureString` holding the API
//! key hashes.
//!
//! The parameter name comes from configuration and is never caller-supplied,
//! and the decrypted value is passed straight to the cache — it is never
//! logged, and no error here carries the parameter's contents.

use aws_sdk_ssm::error::ProvideErrorMetadata as _;

use crate::api::keys::{ApiKeyError, ApiKeySource};
use crate::aws::AwsServices;

impl ApiKeySource for AwsServices {
    async fn fetch(&self) -> Result<String, ApiKeyError> {
        let Some(mail) = self.mail_config() else {
            return Err(ApiKeyError::Fetch(anyhow::anyhow!(
                "mail is not configured, so there is no API key parameter"
            )));
        };

        let response = self
            .ssm
            .get_parameter()
            .name(&mail.api_keys_parameter)
            .with_decryption(true)
            .send()
            .await
            .map_err(|error| {
                ApiKeyError::Fetch(anyhow::anyhow!(
                    "GetParameter failed: {}",
                    error.code().unwrap_or("unknown")
                ))
            })?;

        response
            .parameter
            .and_then(|parameter| parameter.value)
            .ok_or_else(|| {
                ApiKeyError::Malformed(anyhow::anyhow!("API key parameter has no value"))
            })
    }
}
