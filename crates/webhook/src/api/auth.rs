//! Bearer authentication for the `/v0` surface (D20).
//!
//! Every `/v0` route sits behind this middleware. Three rules are
//! load-bearing:
//!
//! - A missing, malformed or unrecognized key is `401`, and the presented
//!   value is never logged.
//! - A cache that has never loaded is `503`, not `401`: the SDKs retry `503`
//!   and do not retry `401`, so a parameter-store outage must not look like a
//!   bad key.
//! - The matched key's id (not the key) is attached to the request and the
//!   log line, so an operator can tell which key an agent used.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;

use crate::api::error::ApiError;
use crate::api::keys::Verdict;
use crate::state::{AppState, Services};

/// The id of the key a request authenticated with, attached as a request
/// extension. Ids are operator-facing labels, not secrets.
#[derive(Debug, Clone)]
pub struct KeyId(pub String);

/// Rejects the request unless it carries a recognized bearer key.
///
/// # Errors
///
/// [`ApiError::Unauthorized`] when the `Authorization` header is missing,
/// uses another scheme, or presents a key no configured hash matches;
/// [`ApiError::Unavailable`] when the key cache has never loaded and the
/// parameter store cannot be read, so the key can be neither confirmed nor
/// denied.
pub async fn require_bearer<T: Services>(
    State(state): State<Arc<AppState<T>>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token)
        .ok_or(ApiError::Unauthorized)?;

    match state.api_keys.verify(&state.services, presented).await {
        Verdict::Allowed(key_id) => {
            tracing::debug!(key_id, event = "api_authenticated", "authenticated request");
            request.extensions_mut().insert(KeyId(key_id));
            Ok(next.run(request).await)
        }
        Verdict::Denied => Err(ApiError::Unauthorized),
        Verdict::Unavailable => Err(ApiError::Unavailable),
    }
}

/// Extracts the token from an `Authorization` value. The scheme is matched
/// case-insensitively, per RFC 7235; an empty token is not a token.
fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_bearer_token_in_any_case() {
        assert_eq!(bearer_token("Bearer am_key"), Some("am_key"));
        assert_eq!(bearer_token("bearer am_key"), Some("am_key"));
        assert_eq!(bearer_token("BEARER am_key"), Some("am_key"));
    }

    #[test]
    fn rejects_other_schemes_and_empty_tokens() {
        assert_eq!(bearer_token("Basic am_key"), None);
        assert_eq!(bearer_token("Bearer "), None);
        assert_eq!(bearer_token("Bearer    "), None);
        assert_eq!(bearer_token("am_key"), None);
        assert_eq!(bearer_token(""), None);
    }
}
