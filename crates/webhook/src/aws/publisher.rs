//! [`PublishEvents`] over `EventBridge` `PutEvents`.

use anyhow::anyhow;
use aws_sdk_eventbridge::types::PutEventsRequestEntry;
use aws_smithy_types::error::display::DisplayErrorContext;

use crate::publish::{OutboundEvent, PublishError, PublishEvents};

/// Publishes to one event bus under one source.
pub struct EventBridgePublisher {
    client: aws_sdk_eventbridge::Client,
    bus_name: String,
    source: String,
}

impl EventBridgePublisher {
    #[must_use]
    pub fn new(client: aws_sdk_eventbridge::Client, bus_name: String, source: String) -> Self {
        Self {
            client,
            bus_name,
            source,
        }
    }
}

impl PublishEvents for EventBridgePublisher {
    async fn publish(&self, event: &OutboundEvent) -> Result<(), PublishError> {
        let entry = PutEventsRequestEntry::builder()
            .event_bus_name(&self.bus_name)
            .source(&self.source)
            .detail_type(&event.detail_type)
            .detail(event.detail.to_string())
            .build();
        let response = self
            .client
            .put_events()
            .entries(entry)
            .send()
            .await
            .map_err(|e| PublishError(anyhow!("{}", DisplayErrorContext(&e))))?;

        if response.failed_entry_count() > 0 {
            let failure = response.entries().first().map_or_else(
                || "no entry detail".to_owned(),
                |e| {
                    format!(
                        "{}: {}",
                        e.error_code().unwrap_or("unknown"),
                        e.error_message().unwrap_or("no message")
                    )
                },
            );
            return Err(PublishError(anyhow!("PutEvents entry failed: {failure}")));
        }
        Ok(())
    }
}
