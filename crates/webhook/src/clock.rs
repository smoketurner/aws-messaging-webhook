//! Where the time comes from.
//!
//! An invocation's "now" is the timestamp AWS put on the event being served,
//! not a clock reading taken while serving it: a Function URL request's
//! `requestContext.timeEpoch` ([`RequestTime`]), a direct SNS delivery's
//! envelope `Timestamp`, a scheduled sweep's `time` ([`scheduled_time`]).
//! Stamping what an invocation writes with its event's time makes a
//! redelivery of the same event write the same values, and leaves nothing
//! for a test to race.
//!
//! [`now`] reads the clock, and is for the few instants no event can supply:
//!
//! - the sender's claim on a send and what it records about it. A claim is a
//!   lease the sweep judges by age, and the stream record that triggered the
//!   sender says when the send was queued, which under a retry or a lagging
//!   stream is long before it was claimed — stamping the claim with it would
//!   make a live claim look abandoned and get the message sent twice;
//! - an operator command, whose payload carries no timestamp;
//! - an event that arrives without its timestamp, which AWS does not send.

use std::convert::Infallible;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use lambda_http::RequestExt as _;
use lambda_http::request::RequestContext;
use mailbox::time::{self, Timestamp};
use serde_json::Value;

/// The current time, read from the clock. See the module docs for when that
/// is the right source; everywhere else, take the event's time.
pub(crate) fn now() -> Timestamp {
    Timestamp::now()
}

/// When the Function URL received the request being served: its
/// `requestContext.timeEpoch`, which `lambda_http` carries as a request
/// extension. Falls back to [`now`] when the extension is absent — only
/// handler tests that build a plain axum `Request`.
pub(crate) struct RequestTime(pub Timestamp);

impl<S> FromRequestParts<S> for RequestTime
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> {
        let at = match parts.request_context_ref() {
            Some(RequestContext::ApiGatewayV2(context)) => epoch_ms(context.time_epoch),
            _ => None,
        };
        std::future::ready(Ok(Self(at.unwrap_or_else(now))))
    }
}

/// `timeEpoch` as an instant. `None` for zero, which is what the field
/// deserializes to when the payload omits it.
fn epoch_ms(ms: i64) -> Option<Timestamp> {
    (ms > 0)
        .then(|| Timestamp::from_millisecond(ms).ok())
        .flatten()
}

/// When a scheduled rule fired: the `time` of the event it delivers. `None`
/// for a payload without one, such as a sweep invoked by hand.
pub(crate) fn scheduled_time(payload: &Value) -> Option<Timestamp> {
    payload
        .get("time")
        .and_then(Value::as_str)
        .and_then(time::parse_rfc3339)
}

#[cfg(test)]
mod tests {
    use lambda_http::aws_lambda_events::apigw::ApiGatewayV2httpRequestContext;

    use super::*;

    #[test]
    fn a_scheduled_event_carries_its_fire_time() {
        // The documented scheduled-event shape: whole seconds, no fraction.
        let payload = serde_json::json!({
            "version": "0",
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "time": "2026-08-04T00:10:00Z",
            "detail": {},
        });
        assert_eq!(
            scheduled_time(&payload).map(time::format).as_deref(),
            Some("2026-08-04T00:10:00.000Z")
        );
    }

    #[test]
    fn a_payload_without_a_usable_time_has_none() {
        assert_eq!(scheduled_time(&serde_json::json!({})), None);
        assert_eq!(scheduled_time(&serde_json::json!({ "time": 5 })), None);
        assert_eq!(scheduled_time(&serde_json::json!({ "time": "soon" })), None);
    }

    #[tokio::test]
    async fn the_request_time_is_read_from_the_function_url_context() {
        let mut context = ApiGatewayV2httpRequestContext::default();
        context.time_epoch = 1_754_265_600_250;
        let request = axum::http::Request::new(())
            .with_request_context(RequestContext::ApiGatewayV2(context));
        let (mut parts, ()) = request.into_parts();

        let Ok(RequestTime(at)) = RequestTime::from_request_parts(&mut parts, &()).await;

        assert_eq!(time::format(at), "2025-08-04T00:00:00.250Z");
    }

    #[test]
    fn a_request_time_is_its_epoch_milliseconds() {
        assert_eq!(
            epoch_ms(1_754_265_600_000).map(time::format).as_deref(),
            Some("2025-08-04T00:00:00.000Z")
        );
        assert_eq!(epoch_ms(0), None);
        assert_eq!(epoch_ms(i64::MAX), None);
    }
}
