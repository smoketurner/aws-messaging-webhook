use anyhow::Context as _;

use crate::allowlist::TopicAllowlist;

/// Runtime configuration from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub table_name: String,
    pub event_bus_name: String,
    /// The `source` field on published EventBridge events.
    pub event_source: String,
    pub auto_resubscribe: bool,
    /// End User Messaging opt-out list for the STOP/START action; `None`
    /// disables that action.
    pub opt_out_list_name: Option<String>,
    pub raw_event_retention_days: u64,
    /// TTL for the per-message aggregate item. Kept separate from (and
    /// typically longer than) `raw_event_retention_days` so a message's
    /// rolled-up current state outlives its bulky raw event items.
    pub aggregate_retention_days: u64,
}

impl Config {
    /// Loads configuration and the topic allowlist from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable when a required variable is
    /// missing or a value fails to parse.
    pub fn from_env() -> anyhow::Result<(Self, TopicAllowlist)> {
        let table_name = require("TABLE_NAME")?;
        let event_bus_name = require("EVENT_BUS_NAME")?;
        let event_source =
            optional("EVENT_SOURCE").unwrap_or_else(|| "aws-messaging-webhook".to_owned());

        let allowlist = TopicAllowlist::parse(&optional("ALLOWED_TOPICS").unwrap_or_default());
        if allowlist.is_empty() {
            tracing::warn!(
                "ALLOWED_TOPICS is empty: ANY AWS account's SNS topic may subscribe and \
                 deliver events to this endpoint. Do not run this way outside development."
            );
        }

        let auto_resubscribe = match optional("AUTO_RESUBSCRIBE") {
            None => true,
            Some(raw) => raw
                .parse::<bool>()
                .with_context(|| format!("AUTO_RESUBSCRIBE must be true or false, got {raw:?}"))?,
        };

        let opt_out_list_name = optional("OPT_OUT_LIST_NAME");
        if opt_out_list_name.is_none() {
            tracing::warn!(
                "OPT_OUT_LIST_NAME is not set: inbound STOP/START keywords will be forwarded \
                 to EventBridge but not applied to any End User Messaging opt-out list"
            );
        }

        let raw_event_retention_days = match optional("RAW_EVENT_RETENTION_DAYS") {
            None => 30,
            Some(raw) => {
                let days = raw.parse::<u64>().with_context(|| {
                    format!("RAW_EVENT_RETENTION_DAYS must be a positive integer, got {raw:?}")
                })?;
                // 0 would set a TTL of "now", purging every audit record almost
                // immediately — reject it rather than silently destroy data.
                anyhow::ensure!(
                    days > 0,
                    "RAW_EVENT_RETENTION_DAYS must be at least 1, got 0"
                );
                days
            }
        };

        let aggregate_retention_days = match optional("AGGREGATE_RETENTION_DAYS") {
            None => 365,
            Some(raw) => {
                let days = raw.parse::<u64>().with_context(|| {
                    format!("AGGREGATE_RETENTION_DAYS must be a positive integer, got {raw:?}")
                })?;
                anyhow::ensure!(
                    days > 0,
                    "AGGREGATE_RETENTION_DAYS must be at least 1, got 0"
                );
                days
            }
        };

        Ok((
            Self {
                table_name,
                event_bus_name,
                event_source,
                auto_resubscribe,
                opt_out_list_name,
                raw_event_retention_days,
                aggregate_retention_days,
            },
            allowlist,
        ))
    }
}

fn require(name: &str) -> anyhow::Result<String> {
    optional(name).with_context(|| format!("required environment variable {name} is not set"))
}

/// Reads a variable, treating unset and empty as absent.
fn optional(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        Ok(_) | Err(_) => None,
    }
}
