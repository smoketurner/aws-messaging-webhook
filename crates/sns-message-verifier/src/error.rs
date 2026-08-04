use std::fmt;

/// Reason a `SigningCertURL` was rejected before any network request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertUrlRejection {
    /// The URL could not be parsed at all.
    Unparseable,
    /// The scheme was not `https`.
    NotHttps,
    /// The host did not match `sns.<region>.amazonaws.com(.cn)`.
    InvalidHost,
    /// A port other than 443 was specified.
    InvalidPort,
    /// The path did not end in `.pem`.
    NotPem,
}

impl fmt::Display for CertUrlRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Unparseable => "URL could not be parsed",
            Self::NotHttps => "scheme is not https",
            Self::InvalidHost => "host is not an SNS endpoint",
            Self::InvalidPort => "port is not 443",
            Self::NotPem => "path does not end in .pem",
        };
        f.write_str(reason)
    }
}

/// Failure verifying an SNS message.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("malformed SNS envelope JSON: {0}")]
    MalformedEnvelope(#[from] serde_json::Error),

    #[error("missing required field {0}")]
    MissingField(&'static str),

    #[error("rejected SigningCertURL {url}: {reason}")]
    InvalidCertUrl {
        url: String,
        reason: CertUrlRejection,
    },

    #[error("failed to fetch signing certificate: {0}")]
    CertFetch(#[from] reqwest::Error),

    #[error("failed to parse signing certificate: {0}")]
    CertParse(String),

    #[error("signing certificate is expired or not yet valid")]
    CertValidity,

    #[error("unsupported SignatureVersion {0:?} (expected \"1\" or \"2\")")]
    UnsupportedSignatureVersion(String),

    #[error("signature is not valid base64: {0}")]
    InvalidSignatureEncoding(#[from] base64::DecodeError),

    #[error("signature verification failed")]
    SignatureMismatch,
}
