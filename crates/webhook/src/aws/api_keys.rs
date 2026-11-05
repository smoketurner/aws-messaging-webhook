//! The production [`ApiKeySource`]: the SSM `SecureString` holding the API
//! key hashes.
//!
//! The parameter name comes from configuration and is never caller-supplied,
//! and the decrypted value is passed straight to the cache — it is never
//! logged, and no error here carries the parameter's contents.

use aws_sdk_ssm::error::ProvideErrorMetadata as _;

use crate::api::keys::{ApiKeyError, ApiKeySource};
/// The SSM parameter holding the API key hashes.
pub struct SsmKeys {
    client: aws_sdk_ssm::Client,
    /// `None` when mail is not configured.
    parameter: Option<String>,
}

impl SsmKeys {
    #[must_use]
    pub fn new(client: aws_sdk_ssm::Client, parameter: Option<String>) -> Self {
        Self { client, parameter }
    }
}

impl ApiKeySource for SsmKeys {
    async fn fetch(&self) -> Result<String, ApiKeyError> {
        let Some(parameter) = &self.parameter else {
            return Err(ApiKeyError::Fetch(anyhow::anyhow!(
                "mail is not configured, so there is no API key parameter"
            )));
        };

        let response = self
            .client
            .get_parameter()
            .name(parameter)
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
