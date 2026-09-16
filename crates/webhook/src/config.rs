use std::time::Duration;

use anyhow::Context as _;

use crate::allowlist::TopicAllowlist;
use crate::mail::is_valid_local_part;

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
    /// Which half of the single binary this invocation runs: the webhook
    /// (Function URL + direct SNS ingress) or the mail sender (stream
    /// consumer, sweep, redrive, close). Only meaningful when [`mail`] is
    /// configured; the webhook-only deployment always runs in
    /// [`FunctionMode::Webhook`].
    ///
    /// [`mail`]: Config::mail
    pub mode: FunctionMode,
    /// The AgentMail-compatible mail inbox feature. `None` disables every
    /// mail feature (ingest, the `/v0` API, and the sender).
    pub mail: Option<MailConfig>,
}

/// Which half of the single binary is running (`FUNCTION_MODE`). The sender
/// dispatch this drives lands in phase 3; phase 1 always constructs
/// [`FunctionMode::Webhook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionMode {
    Webhook,
    Sender,
}

/// The AgentMail-compatible mail inbox configuration, present only when
/// `MAIL_DOMAIN` is set.
#[derive(Debug, Clone)]
pub struct MailConfig {
    pub domain: String,
    /// The mail table's name — a separate DynamoDB table from
    /// [`Config::table_name`], which holds only SMS/SES messaging events.
    pub table_name: String,
    pub bucket: String,
    /// Explicit inbound local parts (before `@domain`). May be empty only
    /// when `catch_all` is true.
    pub inboxes: Vec<String>,
    pub catch_all: bool,
    pub auto_create_inboxes: bool,
    pub configuration_set: String,
    pub identity_arn: String,
    /// SSM parameter name holding the bearer API key hashes; must start with
    /// `/` (D8).
    pub api_keys_parameter: String,
    /// Presigned attachment URL lifetime; 60 s – 3,600 s (D37).
    pub attachment_url_ttl: Duration,
    pub region: String,
    /// `MailSenderMaxSendRate`: minimum spacing between sends in one
    /// execution environment, expressed as sends/second (default 1, min 1).
    pub send_rate: u32,
    /// `MailUnknownOutboxRetentionDays`: how long an `unknown` send outcome
    /// stays resolvable before its outbox objects expire (default 30, min 1).
    pub unknown_outbox_retention_days: u32,
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

        let mode = parse_function_mode(optional("FUNCTION_MODE").as_deref())?;
        let mail = MailConfig::from_env()?;

        Ok((
            Self {
                table_name,
                event_bus_name,
                event_source,
                auto_resubscribe,
                opt_out_list_name,
                raw_event_retention_days,
                aggregate_retention_days,
                mode,
                mail,
            },
            allowlist,
        ))
    }
}

impl MailConfig {
    /// Loads the mail configuration from the environment. Returns `None`
    /// (every mail feature disabled) when `MAIL_DOMAIN` is unset or empty.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable when a required mail variable is
    /// missing, or when a value fails validation.
    fn from_env() -> anyhow::Result<Option<Self>> {
        let Some(domain) = optional("MAIL_DOMAIN") else {
            return Ok(None);
        };

        let table_name = require("MAIL_TABLE_NAME")?;
        let bucket = require("MAIL_BUCKET")?;
        let catch_all = parse_bool_flag("MAIL_CATCH_ALL", optional("MAIL_CATCH_ALL").as_deref())?;
        let auto_create_inboxes = parse_bool_flag(
            "MAIL_AUTO_CREATE_INBOXES",
            optional("MAIL_AUTO_CREATE_INBOXES").as_deref(),
        )?;
        let inboxes = match optional("MAIL_INBOXES") {
            Some(raw) => parse_inbox_list(&raw)?,
            None => Vec::new(),
        };
        anyhow::ensure!(
            !inboxes.is_empty() || catch_all,
            "MAIL_INBOXES must list at least one inbox unless MAIL_CATCH_ALL is true"
        );

        let configuration_set = require("SES_CONFIGURATION_SET")?;
        let identity_arn = require("MAIL_IDENTITY_ARN")?;
        let api_keys_parameter = require("API_KEYS_PARAMETER")?;
        validate_api_keys_parameter(&api_keys_parameter)?;

        let attachment_url_ttl = match optional("ATTACHMENT_URL_TTL_SECONDS") {
            Some(raw) => parse_ttl_seconds(&raw)?,
            None => Duration::from_secs(3600),
        };

        let region = require("AWS_REGION")?;

        let send_rate = match optional("MAIL_SEND_RATE") {
            Some(raw) => parse_positive_u32("MAIL_SEND_RATE", &raw)?,
            None => 1,
        };
        let unknown_outbox_retention_days = match optional("MAIL_UNKNOWN_OUTBOX_RETENTION_DAYS") {
            Some(raw) => parse_positive_u32("MAIL_UNKNOWN_OUTBOX_RETENTION_DAYS", &raw)?,
            None => 30,
        };

        Ok(Some(Self {
            domain,
            table_name,
            bucket,
            inboxes,
            catch_all,
            auto_create_inboxes,
            configuration_set,
            identity_arn,
            api_keys_parameter,
            attachment_url_ttl,
            region,
            send_rate,
            unknown_outbox_retention_days,
        }))
    }
}

/// Parses `FUNCTION_MODE`: absent or `"webhook"` → [`FunctionMode::Webhook`];
/// `"sender"` → [`FunctionMode::Sender`]; anything else is an error.
fn parse_function_mode(raw: Option<&str>) -> anyhow::Result<FunctionMode> {
    match raw {
        None | Some("webhook") => Ok(FunctionMode::Webhook),
        Some("sender") => Ok(FunctionMode::Sender),
        Some(other) => {
            anyhow::bail!("FUNCTION_MODE must be \"webhook\" or \"sender\", got {other:?}")
        }
    }
}

/// Maximum `MAIL_INBOXES` entries (N13): the template expands the list into
/// 10 conditional `!Select` slots (`Fn::Join`'s delimiter must be a literal,
/// so `CloudFormation` can't map over an arbitrary-length list).
const MAX_INBOXES: usize = 10;

/// Splits a comma-separated `MAIL_INBOXES` value into validated local parts
/// (N13): at most 10 entries, each matching `^[a-z0-9._+-]{1,64}$` exactly —
/// no trimming, so a whitespace-padded or empty entry is a hard error rather
/// than silently dropped, matching the template's `AllowedPattern`.
fn parse_inbox_list(raw: &str) -> anyhow::Result<Vec<String>> {
    if raw.is_empty() {
        // `optional("MAIL_INBOXES")` already treats an empty env var as
        // absent (so `MailConfig::from_env` never reaches this function with
        // ""), but handle it here too so this function means the same thing
        // to any caller: zero entries, not a one-element list holding "".
        return Ok(Vec::new());
    }
    let parts: Vec<&str> = raw.split(',').collect();
    anyhow::ensure!(
        parts.len() <= MAX_INBOXES,
        "MAIL_INBOXES must list at most {MAX_INBOXES} inboxes, got {}",
        parts.len()
    );
    parts
        .into_iter()
        .map(|part| {
            anyhow::ensure!(
                is_valid_local_part(part),
                "MAIL_INBOXES entry {part:?} is not a valid local part \
                 (expected 1-64 bytes of [a-z0-9._+-], no whitespace, not empty)"
            );
            Ok(part.to_owned())
        })
        .collect()
}

fn parse_bool_flag(name: &str, raw: Option<&str>) -> anyhow::Result<bool> {
    match raw {
        None => Ok(false),
        Some(raw) => raw
            .parse::<bool>()
            .with_context(|| format!("{name} must be true or false, got {raw:?}")),
    }
}

/// `ApiKeysParameterName` must be an absolute SSM parameter path (D8).
fn validate_api_keys_parameter(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        name.starts_with('/'),
        "API_KEYS_PARAMETER must start with \"/\", got {name:?}"
    );
    Ok(())
}

/// Parses `ATTACHMENT_URL_TTL_SECONDS`, bounded to 60–3,600 s (D37).
fn parse_ttl_seconds(raw: &str) -> anyhow::Result<Duration> {
    let seconds: u64 = raw.parse().with_context(|| {
        format!("ATTACHMENT_URL_TTL_SECONDS must be a positive integer, got {raw:?}")
    })?;
    anyhow::ensure!(
        (60..=3600).contains(&seconds),
        "ATTACHMENT_URL_TTL_SECONDS must be between 60 and 3600, got {seconds}"
    );
    Ok(Duration::from_secs(seconds))
}

/// Parses a positive (≥ 1) `u32` environment value, naming `name` on failure.
fn parse_positive_u32(name: &str, raw: &str) -> anyhow::Result<u32> {
    let value: u32 = raw
        .parse()
        .with_context(|| format!("{name} must be a positive integer, got {raw:?}"))?;
    anyhow::ensure!(value >= 1, "{name} must be at least 1, got 0");
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_mode_defaults_to_webhook() {
        assert_eq!(parse_function_mode(None).unwrap(), FunctionMode::Webhook);
        assert_eq!(
            parse_function_mode(Some("webhook")).unwrap(),
            FunctionMode::Webhook
        );
    }

    #[test]
    fn function_mode_parses_sender() {
        assert_eq!(
            parse_function_mode(Some("sender")).unwrap(),
            FunctionMode::Sender
        );
    }

    #[test]
    fn function_mode_rejects_unknown_values() {
        assert!(parse_function_mode(Some("worker")).is_err());
    }

    #[test]
    fn inbox_list_accepts_zero_entries() {
        assert_eq!(parse_inbox_list("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn inbox_list_accepts_one_entry() {
        assert_eq!(parse_inbox_list("support").unwrap(), vec!["support"]);
    }

    #[test]
    fn inbox_list_validates_each_entry() {
        let inboxes = parse_inbox_list("support,billing,sales").unwrap();
        assert_eq!(inboxes, vec!["support", "billing", "sales"]);
    }

    /// N13: an empty entry (a stray or trailing comma) is a hard error, not
    /// silently dropped.
    #[test]
    fn inbox_list_rejects_a_blank_entry() {
        assert!(parse_inbox_list("support,,billing").is_err());
        assert!(parse_inbox_list("support,billing,").is_err());
    }

    /// N13: entries are not trimmed, so surrounding whitespace is a local-part
    /// validation failure rather than being silently stripped.
    #[test]
    fn inbox_list_rejects_whitespace_padded_entries() {
        assert!(parse_inbox_list(" support ,billing").is_err());
    }

    #[test]
    fn inbox_list_rejects_a_bad_character() {
        assert!(parse_inbox_list("support,bad!char").is_err());
    }

    #[test]
    fn inbox_list_rejects_an_uppercase_entry() {
        assert!(parse_inbox_list("support,Billing").is_err());
    }

    /// A full address (rather than a bare local part) is rejected: `@` is
    /// not in the local-part character class.
    #[test]
    fn inbox_list_rejects_a_full_address_entry() {
        assert!(parse_inbox_list("support,billing@example.com").is_err());
    }

    #[test]
    fn inbox_list_rejects_an_invalid_entry() {
        assert!(parse_inbox_list("support,Not Valid").is_err());
    }

    /// N13: at most 10 entries; the template expands into 10 `!Select` slots.
    #[test]
    fn inbox_list_accepts_ten_entries_and_rejects_eleven() {
        let ten = (0..10)
            .map(|i| format!("inbox{i}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(parse_inbox_list(&ten).unwrap().len(), 10);

        let eleven = (0..11)
            .map(|i| format!("inbox{i}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_inbox_list(&eleven).is_err());
    }

    #[test]
    fn api_keys_parameter_requires_leading_slash() {
        assert!(validate_api_keys_parameter("/prod/api-keys").is_ok());
        assert!(validate_api_keys_parameter("prod/api-keys").is_err());
    }

    #[test]
    fn ttl_seconds_accepts_the_boundary_values() {
        assert_eq!(parse_ttl_seconds("60").unwrap(), Duration::from_secs(60));
        assert_eq!(
            parse_ttl_seconds("3600").unwrap(),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn ttl_seconds_rejects_out_of_range_and_non_numeric() {
        assert!(parse_ttl_seconds("59").is_err());
        assert!(parse_ttl_seconds("3601").is_err());
        assert!(parse_ttl_seconds("soon").is_err());
    }

    #[test]
    fn positive_u32_rejects_zero_and_non_numeric() {
        assert!(parse_positive_u32("MAIL_SEND_RATE", "0").is_err());
        assert!(parse_positive_u32("MAIL_SEND_RATE", "-1").is_err());
        assert!(parse_positive_u32("MAIL_SEND_RATE", "fast").is_err());
        assert_eq!(parse_positive_u32("MAIL_SEND_RATE", "5").unwrap(), 5);
    }
}
