use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use sns_message_verifier::SnsEnvelope;

use crate::error::AppError;
use crate::state::{AppState, Services};

/// An SNS delivery that passed the topic allowlist and signature
/// verification. Handlers take this as their body extractor, so no handler
/// can see an unverified message.
pub struct VerifiedSns {
    pub envelope: SnsEnvelope,
    /// The exact bytes received — these get persisted, not a re-serialization.
    pub raw_body: Bytes,
}

impl<T: Services> FromRequest<AppState<T>> for VerifiedSns {
    type Rejection = AppError;

    async fn from_request(req: Request, state: &AppState<T>) -> Result<Self, Self::Rejection> {
        if req.headers().get("x-amz-sns-message-type").is_none() {
            return Err(AppError::MissingSnsHeader);
        }
        let raw_body = Bytes::from_request(req, state)
            .await
            .map_err(|_| AppError::UnreadableBody)?;
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
