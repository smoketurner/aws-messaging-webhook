//! Verification of AWS SNS message signatures for HTTPS webhook endpoints.
//!
//! SNS signs every message it delivers over HTTP(S). This crate parses the raw
//! POST body into an [`SnsEnvelope`] and verifies the signature against the
//! certificate referenced by `SigningCertURL`, supporting `SignatureVersion` 1
//! (`SHA1withRSA`) and 2 (`SHA256withRSA`).
//!
//! # Trust model
//!
//! The trust anchor is the `SigningCertURL` host policy: the certificate is
//! only fetched from `https://sns.<region>.amazonaws.com(.cn)/...pem` on port
//! 443. There is no chain-to-CA verification — the same model as AWS's own
//! validator libraries. Signature verification proves a message came from SNS;
//! it does NOT prove it came from a topic you trust. Callers must separately
//! check `TopicArn` against an allowlist.

mod canonical;
mod cert;
mod envelope;
mod error;
mod signature;

use std::sync::Arc;
use std::time::Duration;

pub use canonical::build_string_to_sign;
pub use envelope::{MessageType, SnsEnvelope};
pub use error::{CertUrlRejection, VerifyError};
pub use signature::verify_with_cert;

/// Verifies SNS messages, fetching and caching signing certificates.
pub struct SnsVerifier {
    http: reqwest::Client,
    cache: cert::CertCache,
    dangerous_allow_prefix: Option<String>,
}

impl SnsVerifier {
    #[must_use]
    pub fn builder() -> SnsVerifierBuilder {
        SnsVerifierBuilder {
            http: None,
            dangerous_allow_prefix: None,
        }
    }

    /// Parses a raw HTTP POST body as an SNS envelope and verifies its
    /// signature, returning the parsed envelope on success.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the body is not a valid envelope or the
    /// signature does not verify; see [`SnsVerifier::verify`].
    pub async fn verify_body(&self, body: &[u8]) -> Result<SnsEnvelope, VerifyError> {
        let envelope: SnsEnvelope = serde_json::from_slice(body)?;
        self.verify(&envelope).await?;
        Ok(envelope)
    }

    /// Verifies an already-parsed envelope's signature.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError`] if the `SigningCertURL` violates the SNS host
    /// policy, the certificate cannot be fetched or parsed or is outside its
    /// validity window, the `SignatureVersion` is unsupported, or the
    /// signature does not match.
    pub async fn verify(&self, envelope: &SnsEnvelope) -> Result<(), VerifyError> {
        let url = cert::validate_cert_url(
            &envelope.signing_cert_url,
            self.dangerous_allow_prefix.as_deref(),
        )?;
        let cache_key = url.as_str();

        let key = if let Some(cached) = self.cache.get(cache_key) {
            cached
        } else {
            let fetched = Arc::new(cert::fetch_and_parse(&self.http, &url).await?);
            self.cache
                .insert(cache_key.to_owned(), Arc::clone(&fetched));
            fetched
        };
        signature::verify_with_key(envelope, &key)
    }
}

/// Builder for [`SnsVerifier`].
pub struct SnsVerifierBuilder {
    http: Option<reqwest::Client>,
    dangerous_allow_prefix: Option<String>,
}

impl SnsVerifierBuilder {
    /// Overrides the HTTP client used to fetch signing certificates.
    #[must_use]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http = Some(client);
        self
    }

    /// DANGEROUS: additionally accepts any `SigningCertURL` starting with the
    /// given prefix, bypassing the SNS host policy for those URLs. This
    /// disables the scheme's trust anchor for matching URLs — never enable it
    /// with attacker-reachable input. Intended solely for tests and local
    /// development against a fake SNS endpoint.
    #[must_use]
    pub fn dangerous_allow_cert_url_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.dangerous_allow_prefix = Some(prefix.into());
        self
    }

    /// Builds the verifier. Without a custom client, uses one with a 5 second
    /// overall timeout.
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::CertFetch`] if the default HTTP client cannot
    /// be constructed.
    pub fn build(self) -> Result<SnsVerifier, VerifyError> {
        let http = match self.http {
            Some(client) => client,
            None => reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
        };
        Ok(SnsVerifier {
            http,
            cache: cert::CertCache::default(),
            dangerous_allow_prefix: self.dangerous_allow_prefix,
        })
    }
}
