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
    /// The mail inbox feature. `None` disables every
    /// mail feature (ingest, the `/v0` API, and the sender).
    pub mail: Option<MailConfig>,
}

/// Which half of the single binary is running (`FUNCTION_MODE`). The sender
/// half is not implemented yet, so this is always
/// [`FunctionMode::Webhook`] today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionMode {
    Webhook,
    Sender,
}

/// The mail inbox configuration, present only when
/// `MAIL_DOMAIN` is set.
#[derive(Debug, Clone)]
pub struct MailConfig {
    pub domain: String,
    /// The mail table's name — a separate DynamoDB table from
    /// [`Config::table_name`], which holds only SMS/SES messaging events.
    pub table_name: String,
    pub bucket: String,
    /// The one inbox's local part (before `@domain`): the only recipient the
    /// receipt rule accepts.
    pub inbox: String,
    pub configuration_set: String,
    pub identity_arn: String,
    /// SSM parameter name holding the bearer API key hashes; must start with
    /// `/`.
    pub api_keys_parameter: String,
    /// Presigned attachment URL lifetime; 60 s – 3,600 s.
    pub attachment_url_ttl: Duration,
    pub region: String,
    /// `MailSenderMaxSendRate`: minimum spacing between sends in one
    /// execution environment, expressed as sends/second (default 1, min 1).
    pub send_rate: u32,
    /// `MailUnknownOutboxRetentionDays`: how long an `unknown` send outcome
    /// stays resolvable before its outbox objects expire (default 30, min 1).
    pub unknown_outbox_retention_days: u32,
    /// `pMailRetentionDays`: how long mail items and their S3 objects are
    /// kept. Items carry a DynamoDB TTL this far out, matching the bucket's
    /// lifecycle expiry.
    pub retention_days: u32,
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
        let inbox = require("MAIL_INBOX")?;
        validate_inbox(&inbox)?;

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
        let retention_days =
            parse_positive_u32("MAIL_RETENTION_DAYS", &require("MAIL_RETENTION_DAYS")?)?;
        let unknown_outbox_retention_days = match optional("MAIL_UNKNOWN_OUTBOX_RETENTION_DAYS") {
            Some(raw) => parse_positive_u32("MAIL_UNKNOWN_OUTBOX_RETENTION_DAYS", &raw)?,
            None => 30,
        };

        Ok(Some(Self {
            domain,
            table_name,
            bucket,
            inbox,
            configuration_set,
            identity_arn,
            api_keys_parameter,
            attachment_url_ttl,
            region,
            send_rate,
            unknown_outbox_retention_days,
            retention_days,
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

/// `MAIL_INBOX` must be a bare local part matching `^[a-z0-9._+-]{1,64}$`
/// exactly — no trimming, so a padded value is a hard error rather than
/// silently fixed, matching the template's `AllowedPattern`.
fn validate_inbox(inbox: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_valid_local_part(inbox),
        "MAIL_INBOX {inbox:?} is not a valid local part \
         (expected 1-64 bytes of [a-z0-9._+-], no whitespace, not empty)"
    );
    Ok(())
}

/// `pApiKeysParameterName` must be an absolute SSM parameter path outside
/// the `aws` and `ssm` prefixes, which SSM reserves in any case.
fn validate_api_keys_parameter(name: &str) -> anyhow::Result<()> {
    let Some(path) = name.strip_prefix('/') else {
        anyhow::bail!("API_KEYS_PARAMETER must start with \"/\", got {name:?}");
    };
    let reserved = ["aws", "ssm"].iter().any(|prefix| {
        path.get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    });
    anyhow::ensure!(
        !reserved,
        "API_KEYS_PARAMETER must not start with \"/aws\" or \"/ssm\" (reserved by SSM), got {name:?}"
    );
    Ok(())
}

/// Parses `ATTACHMENT_URL_TTL_SECONDS`, bounded to 60–3,600 s.
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
    fn inbox_accepts_a_local_part() {
        assert!(validate_inbox("support").is_ok());
    }

    /// A full address is rejected: `@` is not in the local-part character
    /// class, and the domain comes from `MAIL_DOMAIN`.
    #[test]
    fn inbox_rejects_a_full_address() {
        assert!(validate_inbox("support@example.com").is_err());
    }

    #[test]
    fn inbox_rejects_a_list() {
        assert!(validate_inbox("support,billing").is_err());
    }

    #[test]
    fn inbox_rejects_empty_padded_and_uppercase_values() {
        assert!(validate_inbox("").is_err());
        assert!(validate_inbox(" support ").is_err());
        assert!(validate_inbox("Support").is_err());
    }

    #[test]
    fn api_keys_parameter_requires_leading_slash() {
        assert!(validate_api_keys_parameter("/prod/api-keys").is_ok());
        assert!(validate_api_keys_parameter("prod/api-keys").is_err());
    }

    #[test]
    fn api_keys_parameter_rejects_reserved_prefixes() {
        assert!(validate_api_keys_parameter("/aws-messaging-webhook/dev/api-keys").is_err());
        assert!(validate_api_keys_parameter("/AWS/api-keys").is_err());
        assert!(validate_api_keys_parameter("/ssm/api-keys").is_err());
        assert!(validate_api_keys_parameter("/SsM-keys").is_err());
        assert!(validate_api_keys_parameter("/messaging-webhook/dev/api-keys").is_ok());
        assert!(validate_api_keys_parameter("/a").is_ok());
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
