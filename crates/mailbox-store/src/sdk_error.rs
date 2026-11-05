//! The transient/permanent classification every SDK failure maps onto, shared
//! by the mail table here and the binary's other AWS clients.

use anyhow::anyhow;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_smithy_types::error::display::DisplayErrorContext;

/// AWS error codes that mean "throttled or briefly unavailable — try again",
/// whatever the HTTP status they arrive with. Several services send these as
/// a 400 (DynamoDB's throughput and request-rate throttles, its
/// `TransactionConflictException`, S3's `RequestTimeout`), so the status
/// check alone misses them.
///
/// One service-agnostic list, shared by every classifier, so a new call site
/// cannot pick up an incomplete per-service vocabulary. It mirrors the SDK's
/// own retry classifier (`aws_runtime::retries::classifiers`'
/// `THROTTLING_ERRORS` and `TRANSIENT_ERRORS`) plus DynamoDB's
/// `TransactionConflictException`, minus `LimitExceededException`, which
/// SES and others also use for account quotas a retry cannot clear — that
/// must stay permanent rather than become a retry storm.
pub const TRANSIENT_ERROR_CODES: [&str; 15] = [
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "RequestThrottled",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "TransactionConflictException",
    "TransactionInProgressException",
    "BandwidthLimitExceeded",
    "SlowDown",
    "PriorRequestNotComplete",
    "RequestTimeout",
    "RequestTimeoutException",
];

/// Maps an SDK failure onto the transient/permanent retry policy shared by
/// the mail store and the binary's actions and object store: network faults, timeouts,
/// 5xx/429 responses, and any [`TRANSIENT_ERROR_CODES`] code are transient
/// (worth a retry); everything else is permanent.
pub fn sdk_error_is_transient<E>(error: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    match error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(ctx) => {
            let status = ctx.raw().status();
            status.is_server_error()
                || status.as_u16() == 429
                || ctx
                    .err()
                    .code()
                    .is_some_and(|code| TRANSIENT_ERROR_CODES.contains(&code))
        }
        _ => false,
    }
}

/// An error type with a transient and a permanent case, which an SDK failure
/// maps onto through [`sdk_error_is_transient`].
pub trait FromSdkFailure: Sized {
    fn transient(source: anyhow::Error) -> Self;

    fn permanent(source: anyhow::Error) -> Self;

    /// Maps an SDK failure onto the shared retry policy: network faults,
    /// timeouts, throttling, and 5xx are transient (worth a retry);
    /// everything else — validation, access denied, bad configuration — is
    /// permanent.
    fn from_sdk<E>(context: &'static str, error: &SdkError<E>) -> Self
    where
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        let source = anyhow!("{context}: {}", DisplayErrorContext(error));
        if sdk_error_is_transient(error) {
            Self::transient(source)
        } else {
            Self::permanent(source)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- sdk_error_is_transient: DynamoDB throughput throttles ------------
    //
    // `sdk_error_is_transient` recognises transport failures, HTTP 5xx and
    // 429 directly; service-specific throttling is matched by `code()`
    // against `TRANSIENT_ERROR_CODES`. DynamoDB serves its throughput throttles
    // (`ProvisionedThroughputExceededException`, `RequestLimitExceeded`)
    // over HTTP 400, not 429/5xx, so only the `code()`-list branch can catch
    // them. Before the fix those codes were missing from the list and
    // classed permanent, which dropped the delivery label with no SNS
    // redelivery. These tests pin both that they now classify transient and
    // that a genuinely-permanent 400 stays permanent.

    use aws_sdk_dynamodb::config::http::HttpResponse;
    use aws_sdk_dynamodb::operation::get_item::GetItemError;
    use aws_sdk_dynamodb::types::error::{
        ProvisionedThroughputExceededException, RequestLimitExceeded, ResourceNotFoundException,
        ThrottlingException,
    };
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::error::ErrorMetadata;

    /// One `GetItem` service error whose typed variant carries `code` (so
    /// `ProvideErrorMetadata::code()` returns exactly that string, the way
    /// a real DynamoDB response deserializes), served over `status`.
    fn get_item_service_error(code: &str, status: u16) -> SdkError<GetItemError> {
        let meta = ErrorMetadata::builder().code(code).build();
        let err = match code {
            "ProvisionedThroughputExceededException" => {
                GetItemError::ProvisionedThroughputExceededException(
                    ProvisionedThroughputExceededException::builder()
                        .meta(meta)
                        .build(),
                )
            }
            "RequestLimitExceeded" => GetItemError::RequestLimitExceeded(
                RequestLimitExceeded::builder().meta(meta).build(),
            ),
            "ThrottlingException" => {
                GetItemError::ThrottlingException(ThrottlingException::builder().meta(meta).build())
            }
            "ResourceNotFoundException" => GetItemError::ResourceNotFoundException(
                ResourceNotFoundException::builder().meta(meta).build(),
            ),
            _ => GetItemError::generic(meta),
        };
        let response = HttpResponse::new(status.try_into().unwrap(), SdkBody::empty());
        SdkError::service_error(err, response)
    }

    /// An unhandled `GetItem` error with no `code()`, served over `status`:
    /// exercises the status-code branches of `sdk_error_is_transient` in
    /// isolation from the `code()`-list branch.
    fn unhandled_get_item_error(status: u16) -> SdkError<GetItemError> {
        let response = HttpResponse::new(status.try_into().unwrap(), SdkBody::empty());
        SdkError::service_error(GetItemError::unhandled("dynamo error"), response)
    }

    /// DynamoDB's per-partition throughput throttle is HTTP 400, so only the
    /// `code()`-list branch can catch it. Misclassifying it permanent would
    /// silently drop the delivery label with no SNS redelivery.
    #[test]
    fn dynamodb_provisioned_throughput_exceeded_is_transient() {
        let error = get_item_service_error("ProvisionedThroughputExceededException", 400);
        assert!(
            sdk_error_is_transient(&error),
            "a DynamoDB throughput throttle (HTTP 400) must be transient so SNS redelivers"
        );
    }

    /// The account-level request ceiling is the realistic trigger for both
    /// legs of `apply_delivery_label` (the `GetItem` read and the
    /// `TransactWriteItems` write) and is served over HTTP 400.
    #[test]
    fn dynamodb_request_limit_exceeded_is_transient() {
        let error = get_item_service_error("RequestLimitExceeded", 400);
        assert!(
            sdk_error_is_transient(&error),
            "an account-level DynamoDB request limit (HTTP 400) must be transient"
        );
    }

    /// The pre-existing DynamoDB throttle code must keep classifying
    /// transient once the throughput codes join it.
    #[test]
    fn dynamodb_throttling_exception_stays_transient() {
        let error = get_item_service_error("ThrottlingException", 400);
        assert!(sdk_error_is_transient(&error));
    }

    /// A genuinely-permanent DynamoDB 400 (missing table, validation, access
    /// denied) must stay permanent: the fix adds throttling codes only.
    #[test]
    fn a_genuinely_permanent_dynamodb_400_stays_permanent() {
        let error = get_item_service_error("ResourceNotFoundException", 400);
        assert!(
            !sdk_error_is_transient(&error),
            "a non-throttling 400 must remain permanent"
        );
    }

    /// A non-transactional write that collides with an in-flight
    /// transaction on the same item (the mail store's `PutItem(inbox)`
    /// against a `TransactWriteItems` touching the inbox) is a 400
    /// `TransactionConflictException`: a retry settles it.
    #[test]
    fn dynamodb_transaction_conflict_is_transient() {
        let error = get_item_service_error("TransactionConflictException", 400);
        assert!(sdk_error_is_transient(&error));
    }

    /// Every classifier shares one code list, so each code in it classifies
    /// transient over a plain 400 — the per-service lists this replaced were
    /// how DynamoDB's throttles went missing.
    #[test]
    fn every_listed_transient_code_is_transient_over_http_400() {
        for code in TRANSIENT_ERROR_CODES {
            assert!(
                sdk_error_is_transient(&get_item_service_error(code, 400)),
                "{code} over HTTP 400 must be transient"
            );
        }
    }

    /// `LimitExceededException` is deliberately left off the list: SES and
    /// others use it for account quotas a retry cannot clear.
    #[test]
    fn a_quota_limit_exceeded_stays_permanent() {
        let error = get_item_service_error("LimitExceededException", 400);
        assert!(!sdk_error_is_transient(&error));
    }

    /// HTTP 5xx and 429 are transient regardless of `code()`, so they must
    /// not depend on the throttling-code list.
    #[test]
    fn http_5xx_and_429_are_transient_without_a_throttling_code() {
        assert!(sdk_error_is_transient(&unhandled_get_item_error(500)));
        assert!(sdk_error_is_transient(&unhandled_get_item_error(503)));
        assert!(sdk_error_is_transient(&unhandled_get_item_error(429)));
    }
}
