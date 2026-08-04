//! `SubscribeURL` handling, shared by subscription confirmation and
//! auto-re-subscribe. The URL is re-validated against the SNS endpoint policy
//! before any GET — never fetch an arbitrary URL, even out of a
//! signature-verified message.

use anyhow::Context as _;
use url::Url;

use crate::error::AppError;

/// Validates that a `SubscribeURL` points at a real SNS endpoint over HTTPS.
/// `dangerous_allow_prefix` additionally accepts URLs starting with that
/// prefix — for tests and local development against a fake SNS only; the
/// binary populates it solely in debug builds.
///
/// # Errors
///
/// Returns [`AppError::SubscribeUrlRejected`] for any URL that is not
/// `https://sns.<region>.amazonaws.com(.cn)` on port 443.
pub fn validate_subscribe_url(
    raw: &str,
    dangerous_allow_prefix: Option<&str>,
) -> Result<Url, AppError> {
    let url = Url::parse(raw).map_err(|_| AppError::SubscribeUrlRejected)?;
    if dangerous_allow_prefix.is_some_and(|prefix| raw.starts_with(prefix)) {
        return Ok(url);
    }
    let accepted = url.scheme() == "https"
        && matches!(url.port(), None | Some(443))
        && url
            .host_str()
            .is_some_and(sns_message_verifier::is_sns_host);
    if accepted {
        Ok(url)
    } else {
        Err(AppError::SubscribeUrlRejected)
    }
}

/// GETs a validated `SubscribeURL`, confirming (or re-confirming) the
/// subscription.
///
/// # Errors
///
/// Returns [`AppError::Internal`] (a 5xx, so SNS redelivers the confirmation)
/// if the request fails or SNS answers with an error status.
pub async fn get_subscribe_url(http: &reqwest::Client, url: Url) -> Result<(), AppError> {
    let response = http
        .get(url)
        .send()
        .await
        .context("GET SubscribeURL failed")?;
    response
        .error_for_status()
        .context("SNS rejected the subscription confirmation")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_sns_https_urls() {
        for url in [
            "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=abc",
            "https://sns.cn-north-1.amazonaws.com.cn/?Action=ConfirmSubscription",
        ] {
            assert!(
                validate_subscribe_url(url, None).is_ok(),
                "should accept {url}"
            );
        }
    }

    #[test]
    fn rejects_non_sns_urls() {
        for url in [
            "http://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription",
            "https://sns.us-east-1.amazonaws.com:8443/?Action=ConfirmSubscription",
            "https://evil.example.com/?Action=ConfirmSubscription",
            "https://sns.us-east-1.amazonaws.com.evil.com/x",
            "not a url",
        ] {
            assert!(
                matches!(
                    validate_subscribe_url(url, None),
                    Err(AppError::SubscribeUrlRejected)
                ),
                "should reject {url}"
            );
        }
    }
}
