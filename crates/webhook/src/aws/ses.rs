//! [`SesApi`] over SES v2: suppression-list writes and raw sends.

use aws_sdk_sesv2::config::Builder as SesConfigBuilder;
use aws_sdk_sesv2::config::retry::RetryConfig;
use aws_sdk_sesv2::config::timeout::TimeoutConfig;
use aws_sdk_sesv2::error::SdkError;
use aws_sdk_sesv2::operation::send_email::SendEmailOutput;
use aws_sdk_sesv2::types::{
    Destination as SesDestination, EmailContent, MessageTag, RawMessage, SuppressionListReason,
};
use aws_smithy_types::Blob;
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::actions::{ActionError, RawSend, SendOutcome, SesApi, SuppressionReason};
use crate::aws::FromSdkFailure as _;
use aws_sdk_sesv2::error::ProvideErrorMetadata;

/// The SES v2 client.
pub struct SesClient {
    client: aws_sdk_sesv2::Client,
}

impl SesClient {
    #[must_use]
    pub(crate) fn new(client: aws_sdk_sesv2::Client) -> Self {
        Self { client }
    }
}

impl SesApi for SesClient {
    async fn put_suppressed_destination(
        &self,
        email_address: &str,
        reason: SuppressionReason,
    ) -> Result<(), ActionError> {
        let reason = match reason {
            SuppressionReason::Bounce => SuppressionListReason::Bounce,
            SuppressionReason::Complaint => SuppressionListReason::Complaint,
        };
        self.client
            .put_suppressed_destination()
            .email_address(email_address)
            .reason(reason)
            .send()
            .await
            .map_err(|e| ActionError::from_sdk("PutSuppressedDestination", &e))?;
        Ok(())
    }

    async fn send_raw(&self, request: &RawSend<'_>) -> SendOutcome {
        let mut destination = SesDestination::builder();
        for address in request.to {
            destination = destination.to_addresses(address);
        }
        for address in request.cc {
            destination = destination.cc_addresses(address);
        }
        for address in request.bcc {
            destination = destination.bcc_addresses(address);
        }

        // A builder that will not build means the request was never sent, so
        // this is a definite failure rather than an ambiguous one.
        let raw = match RawMessage::builder()
            .data(Blob::new(request.raw.to_vec()))
            .build()
        {
            Ok(raw) => raw,
            Err(error) => {
                return SendOutcome::Failed {
                    reason: format!("building the raw message: {error}"),
                };
            }
        };
        let tag = match MessageTag::builder()
            .name("mailbox_message")
            .value(request.message_id)
            .build()
        {
            Ok(tag) => tag,
            Err(error) => {
                return SendOutcome::Failed {
                    reason: format!("building the message tag: {error}"),
                };
            }
        };

        let result = self
            .client
            .send_email()
            .from_email_address(request.from)
            .from_email_address_identity_arn(request.identity_arn)
            .configuration_set_name(request.configuration_set)
            .destination(destination.build())
            .content(EmailContent::builder().raw(raw).build())
            .email_tags(tag)
            .customize()
            // One attempt, always. A retry of a call that already succeeded
            // would send the message a second time, and SendEmail has no
            // idempotency token to prevent that. The timeout bounds the call
            // well inside the time the sender keeps back for it; running out
            // is an unknown outcome, never a retry.
            .config_override(
                SesConfigBuilder::new()
                    .retry_config(RetryConfig::disabled())
                    .timeout_config(
                        TimeoutConfig::builder()
                            .operation_timeout(SES_CALL_TIMEOUT)
                            .build(),
                    ),
            )
            .send()
            .await;

        classify_send_result(result)
    }
}

/// How long one `SendEmail` call may take before its outcome is unknown.
pub(crate) const SES_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Turns one `SendEmail` result into a [`SendOutcome`].
///
/// The split that matters is between a definite refusal and an answer that
/// may have come after the message was accepted. See
/// [`classify_send_status`] for service errors. Everything else — a timeout,
/// a dropped connection, a response that could not be parsed — means the
/// request may have been received and acted on, so it is `Unknown` and the
/// message is never resent without an operator saying so.
fn classify_send_result<E>(result: Result<SendEmailOutput, SdkError<E>>) -> SendOutcome
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let error = match result {
        Ok(output) => {
            return SendOutcome::Sent {
                ses_message_id: output.message_id().unwrap_or_default().to_owned(),
            };
        }
        Err(error) => error,
    };

    let reason = DisplayErrorContext(&error).to_string();
    match &error {
        SdkError::ServiceError(context) => {
            classify_send_status(context.raw().status().as_u16(), reason)
        }
        // Rejected before anything was sent: nothing reached SES.
        SdkError::ConstructionFailure(_) => SendOutcome::Failed { reason },
        // The request may be in flight or already handled.
        _ => SendOutcome::Unknown { reason },
    }
}

/// Classifies an HTTP status SES answered `SendEmail` with.
///
/// 429 and 503 are refusals to take the request at all, so retrying cannot
/// send twice. 500, 502 and 504 can come back after SES accepted the message,
/// so they are `Unknown`. Any other 5xx is treated the same way. Other 4xx
/// are definite, permanent refusals.
fn classify_send_status(status: u16, reason: String) -> SendOutcome {
    match status {
        429 | 503 => SendOutcome::Retryable { reason },
        500..=599 => SendOutcome::Unknown { reason },
        _ => SendOutcome::Failed { reason },
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sesv2::operation::send_email::SendEmailError;

    use super::*;

    /// A send whose answer never arrived must never be reported as a
    /// failure. Doing so would let the sender give up on — or worse, resend —
    /// a message SES may already have accepted.
    #[test]
    fn an_unanswered_send_is_unknown_rather_than_failed() {
        let timeout: SdkError<SendEmailError> =
            SdkError::timeout_error(Box::new(std::io::Error::other("timed out")));
        assert!(
            matches!(
                classify_send_result(Err(timeout)),
                SendOutcome::Unknown { .. }
            ),
            "a timeout leaves the outcome genuinely unknown"
        );
    }

    /// Only statuses that mean SES did not take the request are retried; one
    /// that may follow an accepted message is unknown.
    #[test]
    fn send_statuses_retry_only_on_a_clear_refusal() {
        for status in [429, 503] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Retryable { .. }
                ),
                "{status} should be retryable"
            );
        }
        for status in [500, 502, 504] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Unknown { .. }
                ),
                "{status} should be unknown"
            );
        }
        for status in [400, 403, 404] {
            assert!(
                matches!(
                    classify_send_status(status, String::new()),
                    SendOutcome::Failed { .. }
                ),
                "{status} should be a failure"
            );
        }
    }

    /// A request that was never built cannot have reached SES, so it is a
    /// definite failure and safe to stop on.
    #[test]
    fn a_request_that_was_never_sent_is_a_definite_failure() {
        let construction: SdkError<SendEmailError> =
            SdkError::construction_failure(Box::new(std::io::Error::other("bad field")));
        assert!(matches!(
            classify_send_result(Err(construction)),
            SendOutcome::Failed { .. }
        ));
    }
}
