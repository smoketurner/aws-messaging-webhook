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
#[cfg(feature = "test-fixtures")]
pub mod fixtures;
mod signature;

use std::sync::Arc;
use std::time::Duration;

pub use canonical::build_string_to_sign;
pub use cert::validate_sns_url;
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
            dangerous_allow_prefix: None,
            http: None,
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

/// An HTTP client with a 5 second overall timeout that never follows
/// redirects.
///
/// Both users of one need exactly this: fetching a signing certificate,
/// where a redirect off the allowed host would defeat the host policy that
/// anchors the whole scheme, and confirming a subscription, where following
/// a 3xx off SNS would be SSRF from the Lambda's network context.
///
/// # Errors
///
/// Returns [`VerifyError::CertFetch`] if the client cannot be constructed.
pub fn no_redirect_client() -> Result<reqwest::Client, VerifyError> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

/// Builder for [`SnsVerifier`].
pub struct SnsVerifierBuilder {
    dangerous_allow_prefix: Option<String>,
    http: Option<reqwest::Client>,
}

impl SnsVerifierBuilder {
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

    /// Fetches certificates with `http` instead of a client of its own, so a
    /// caller that already has one shares its connection pool.
    ///
    /// The client must not follow redirects: the `SigningCertURL` host policy
    /// is this scheme's trust anchor, and a redirect off an allowed host
    /// would defeat it. [`no_redirect_client`] builds one that satisfies
    /// this.
    #[must_use]
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Builds the verifier. Without [`Self::http_client`], it gets its own
    /// client from [`no_redirect_client`].
    ///
    /// # Errors
    ///
    /// Returns [`VerifyError::CertFetch`] if the default HTTP client cannot
    /// be constructed.
    pub fn build(self) -> Result<SnsVerifier, VerifyError> {
        let http = match self.http {
            Some(http) => http,
            None => no_redirect_client()?,
        };
        Ok(SnsVerifier {
            http,
            cache: cert::CertCache::default(),
            dangerous_allow_prefix: self.dangerous_allow_prefix,
        })
    }
}
