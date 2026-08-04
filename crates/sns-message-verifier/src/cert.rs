use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use rsa::RsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use url::Url;
use x509_cert::Certificate;
use x509_cert::der::{DecodePem, Encode};

use crate::error::{CertUrlRejection, VerifyError};

/// Certificates are ~1.5 KB; anything much larger is not an SNS signing cert.
const MAX_CERT_RESPONSE_BYTES: usize = 64 * 1024;

const MAX_CACHED_CERTS: usize = 32;

/// Validates that `raw` is an SNS service URL: `https`, port 443, and a host
/// matching `sns.<region>.amazonaws.com(.cn)`. This is the SSRF trust anchor
/// shared by certificate fetching and `SubscribeURL` confirmation, so both
/// guards stay identical.
///
/// `dangerous_allow_prefix` accepts URLs beginning with that prefix, bypassing
/// the policy — honored ONLY in debug builds (tests, local development against
/// a fake SNS), never in a release binary.
///
/// # Errors
///
/// Returns the specific [`CertUrlRejection`] for the first failed check.
pub fn validate_sns_url(
    raw: &str,
    dangerous_allow_prefix: Option<&str>,
) -> Result<Url, CertUrlRejection> {
    let Ok(url) = Url::parse(raw) else {
        return Err(CertUrlRejection::Unparseable);
    };
    if cfg!(debug_assertions)
        && dangerous_allow_prefix.is_some_and(|prefix| raw.starts_with(prefix))
    {
        return Ok(url);
    }
    if url.scheme() != "https" {
        return Err(CertUrlRejection::NotHttps);
    }
    if !matches!(url.port(), None | Some(443)) {
        return Err(CertUrlRejection::InvalidPort);
    }
    if !url.host_str().is_some_and(is_sns_host) {
        return Err(CertUrlRejection::InvalidHost);
    }
    Ok(url)
}

/// Validates a `SigningCertURL`: the SNS URL policy plus a `.pem` path. The
/// trust anchor of the whole verification scheme — a lax check here lets an
/// attacker serve their own certificate.
pub(crate) fn validate_cert_url(
    raw: &str,
    dangerous_allow_prefix: Option<&str>,
) -> Result<Url, VerifyError> {
    let reject = |reason| VerifyError::InvalidCertUrl {
        url: raw.to_owned(),
        reason,
    };
    let url = validate_sns_url(raw, dangerous_allow_prefix).map_err(reject)?;
    let is_pem = std::path::Path::new(url.path())
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("pem"));
    if !is_pem {
        return Err(reject(CertUrlRejection::NotPem));
    }
    Ok(url)
}

/// Whether `host` is an SNS service endpoint: `sns.<region>.amazonaws.com` or
/// `sns.<region>.amazonaws.com.cn`, where `<region>` is one label of
/// `[a-z0-9-]`. ISO partitions are unsupported.
///
fn is_sns_host(host: &str) -> bool {
    let Some(rest) = host.strip_prefix("sns.") else {
        return false;
    };
    let region = rest
        .strip_suffix(".amazonaws.com")
        .or_else(|| rest.strip_suffix(".amazonaws.com.cn"));
    let Some(region) = region else {
        return false;
    };
    !region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

pub(crate) async fn fetch_and_parse(
    client: &reqwest::Client,
    url: &Url,
) -> Result<RsaPublicKey, VerifyError> {
    let mut response = client.get(url.clone()).send().await?.error_for_status()?;
    // Enforce the size cap while streaming so a malicious or misbehaving cert
    // server can't OOM the function by returning a huge body — checking after
    // `bytes()` would already have buffered all of it.
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > MAX_CERT_RESPONSE_BYTES {
            return Err(VerifyError::CertParse(
                "certificate response too large".to_owned(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    parse_cert_pem(&body)
}

/// Parses a PEM certificate, checks its validity window, and extracts the RSA
/// public key. No chain-to-CA verification is performed — the URL policy in
/// [`validate_cert_url`] is the trust anchor, matching AWS's own validators.
pub(crate) fn parse_cert_pem(pem: &[u8]) -> Result<RsaPublicKey, VerifyError> {
    let cert = Certificate::from_pem(pem)
        .map_err(|e| VerifyError::CertParse(format!("not a PEM certificate: {e}")))?;

    let validity = &cert.tbs_certificate.validity;
    let now = SystemTime::now();
    let not_before = validity.not_before.to_system_time();
    let not_after = validity.not_after.to_system_time();
    if now < not_before || now > not_after {
        return Err(VerifyError::CertValidity);
    }

    let spki_der = cert
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .map_err(|e| VerifyError::CertParse(format!("invalid SubjectPublicKeyInfo: {e}")))?;
    RsaPublicKey::from_public_key_der(&spki_der)
        .map_err(|e| VerifyError::CertParse(format!("not an RSA public key: {e}")))
}

/// In-memory cache of parsed signing keys, keyed by cert URL. Validity is
/// checked at parse time only — entries live for the process lifetime, which
/// is hours in a Lambda sandbox versus years of cert validity.
#[derive(Default)]
pub(crate) struct CertCache {
    keys: RwLock<HashMap<String, Arc<RsaPublicKey>>>,
}

impl CertCache {
    pub(crate) fn get(&self, url: &str) -> Option<Arc<RsaPublicKey>> {
        let Ok(keys) = self.keys.read() else {
            tracing::warn!("certificate cache lock poisoned; treating as a cache miss");
            return None;
        };
        keys.get(url).cloned()
    }

    pub(crate) fn insert(&self, url: String, key: Arc<RsaPublicKey>) {
        let Ok(mut keys) = self.keys.write() else {
            tracing::warn!("certificate cache lock poisoned; skipping cache insert");
            return;
        };
        if keys.len() >= MAX_CACHED_CERTS && !keys.contains_key(&url) {
            keys.clear();
        }
        keys.insert(url, key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejection(raw: &str) -> CertUrlRejection {
        match validate_cert_url(raw, None) {
            Err(VerifyError::InvalidCertUrl { reason, .. }) => reason,
            other => panic!("expected InvalidCertUrl, got {other:?}"),
        }
    }

    #[test]
    fn accepts_commercial_govcloud_and_china_hosts() {
        for url in [
            "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-abc.pem",
            "https://sns.us-gov-west-1.amazonaws.com/SimpleNotificationService-abc.pem",
            "https://sns.cn-north-1.amazonaws.com.cn/SimpleNotificationService-abc.pem",
            "https://sns.us-east-1.amazonaws.com:443/SimpleNotificationService-abc.pem",
        ] {
            assert!(validate_cert_url(url, None).is_ok(), "should accept {url}");
        }
    }

    #[test]
    fn rejects_http_scheme() {
        assert_eq!(
            rejection("http://sns.us-east-1.amazonaws.com/cert.pem"),
            CertUrlRejection::NotHttps
        );
    }

    #[test]
    fn rejects_lookalike_and_malformed_hosts() {
        for url in [
            "https://sns.us-east-1.amazonaws.com.evil.com/cert.pem",
            "https://evil.com/sns.us-east-1.amazonaws.com/cert.pem",
            "https://sns.us-east-1.notamazonaws.com/cert.pem",
            "https://xsns.us-east-1.amazonaws.com/cert.pem",
            "https://sns..amazonaws.com/cert.pem",
            "https://sns.us.east.amazonaws.com/cert.pem",
            "https://amazonaws.com/cert.pem",
        ] {
            assert_eq!(
                rejection(url),
                CertUrlRejection::InvalidHost,
                "should reject {url}"
            );
        }
    }

    #[test]
    fn rejects_non_443_port() {
        assert_eq!(
            rejection("https://sns.us-east-1.amazonaws.com:8443/cert.pem"),
            CertUrlRejection::InvalidPort
        );
    }

    #[test]
    fn rejects_non_pem_path() {
        assert_eq!(
            rejection("https://sns.us-east-1.amazonaws.com/cert.txt"),
            CertUrlRejection::NotPem
        );
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(rejection("not a url"), CertUrlRejection::Unparseable);
    }

    #[test]
    fn prefix_override_bypasses_policy_only_for_matching_urls() {
        let prefix = Some("http://127.0.0.1:9999/");
        assert!(validate_cert_url("http://127.0.0.1:9999/cert.pem", prefix).is_ok());
        assert_eq!(
            match validate_cert_url("http://evil.com/cert.pem", prefix) {
                Err(VerifyError::InvalidCertUrl { reason, .. }) => reason,
                other => panic!("expected InvalidCertUrl, got {other:?}"),
            },
            CertUrlRejection::NotHttps
        );
    }
}
