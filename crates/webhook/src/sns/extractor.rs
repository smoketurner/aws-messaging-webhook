use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use sns_message_verifier::SnsEnvelope;

use crate::error::AppError;
use crate::state::{AppState, Services};

/// An SNS delivery that passed the topic allowlist and signature
/// verification. Both ingress paths — this extractor on the HTTP routes and
/// `entry::dispatch` on direct SNS → Lambda invokes — construct it only via
/// [`VerifiedSns::verify`], so no handler can see an unverified message.
pub struct VerifiedSns {
    pub envelope: SnsEnvelope,
    /// The envelope bytes that get persisted: the exact HTTP body on the
    /// webhook path; on the direct-invoke path, the `Sns` record re-serialized
    /// from the invocation payload (every field value preserved verbatim).
    pub raw_body: Bytes,
}

impl VerifiedSns {
    /// The security boundary for a raw envelope body: parse, topic allowlist,
    /// signature verification.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the body is not an envelope, the topic is
    /// not allowlisted, or the signature does not verify.
    pub(crate) async fn verify<T: Services>(
        state: &AppState<T>,
        raw_body: Bytes,
    ) -> Result<Self, AppError> {
        let envelope: SnsEnvelope =
            serde_json::from_slice(&raw_body).map_err(|e| AppError::Verification(e.into()))?;

        // INVARIANT: checking the allowlist BEFORE signature verification is
        // sound only because TopicArn is part of the signed canonical string —
        // a message lying about its TopicArn passes here and then fails
        // verification below. The early check just rejects unwanted topics
        // before any certificate fetch or RSA work. Do not reorder these
        // without revisiting that reasoning.
        if !state.allowlist.allows(&envelope.topic_arn) {
            return Err(AppError::TopicNotAllowed(envelope.topic_arn));
        }
        state.verifier.verify(&envelope).await?;

        Ok(Self { envelope, raw_body })
    }
}

impl<T: Services> FromRequest<Arc<AppState<T>>> for VerifiedSns {
    type Rejection = AppError;

    async fn from_request(req: Request, state: &Arc<AppState<T>>) -> Result<Self, Self::Rejection> {
        if req.headers().get("x-amz-sns-message-type").is_none() {
            return Err(AppError::MissingSnsHeader);
        }
        let raw_body = Bytes::from_request(req, state)
            .await
            .map_err(|_| AppError::UnreadableBody)?;
        Self::verify(state, raw_body).await
    }
}
