//! A JSON body extractor whose rejections use the API's error body.
//!
//! Axum's own `Json` answers a malformed body, a missing content type or an
//! oversized request with a plain-text response. A client that parses every
//! failure as `{name, message}` cannot read those, so this maps them onto
//! [`ApiError`].

use axum::Json;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use serde::de::DeserializeOwned;

use crate::api::error::ApiError;

/// One value, or several: the contract allows both spellings for a list.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    #[must_use]
    pub fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(one) => vec![one],
            Self::Many(many) => many,
        }
    }
}

/// A request body deserialized from JSON.
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(ApiError::TooLarge)
            }
            Err(rejection) => Err(ApiError::field("body", rejection.body_text())),
        }
    }
}
