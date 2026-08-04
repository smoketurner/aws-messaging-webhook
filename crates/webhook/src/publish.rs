//! Re-publishing normalized events to the EventBridge bus.

use std::future::Future;

use serde_json::Value;

/// One event ready for `PutEvents`.
#[derive(Debug, Clone)]
pub struct OutboundEvent {
    pub detail_type: String,
    pub detail: Value,
}

#[derive(Debug, thiserror::Error)]
#[error("event publish failed")]
pub struct PublishError(#[from] pub anyhow::Error);

pub trait PublishEvents: Send + Sync {
    fn publish(
        &self,
        event: &OutboundEvent,
    ) -> impl Future<Output = Result<(), PublishError>> + Send;
}
