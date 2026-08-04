#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test code panics on setup failure"
)]
//! Classification matrix: every documented AWS example payload in
//! `tests/fixtures/` must classify into its directory's event family with no
//! path or routing hint. These are the real wire shapes (full field sets, not
//! trimmed stubs), so this suite is what pins the try-parse ordering in
//! `DomainEvent::classify` and catches a model change that breaks parsing of
//! a payload AWS actually sends.

use std::fs;
use std::path::PathBuf;

use aws_messaging_webhook::model::{DomainEvent, Source};

/// Sentinel SNS message id: `aggregate_id` returning it means the payload's
/// own message id was not extracted.
const SNS_FALLBACK_ID: &str = "sns-fallback-id";

/// Loads `(file_name, contents)` for every fixture under the route directory.
fn fixtures(route: &str) -> Vec<(String, String)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(route);
    let mut paths: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("missing fixture dir {}: {error}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no fixtures in {}", dir.display());
    paths
        .into_iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let contents = fs::read_to_string(&path).unwrap();
            (name, contents)
        })
        .collect()
}

#[test]
fn sms_inbound_fixtures_parse() {
    for (name, payload) in fixtures("sms-inbound") {
        let event = DomainEvent::classify(&payload);
        assert_eq!(
            event.family(),
            Some(Source::SmsInbound),
            "{name}: expected SmsInbound, got {event:?}"
        );
        assert_eq!(event.detail_type(), "sms.inbound", "{name}");
        assert_ne!(
            event.aggregate_id(SNS_FALLBACK_ID),
            SNS_FALLBACK_ID,
            "{name}: aggregate id fell back to the SNS message id"
        );
    }
}

#[test]
fn sms_event_fixtures_parse() {
    for (name, payload) in fixtures("sms-events") {
        let event = DomainEvent::classify(&payload);
        assert_eq!(
            event.family(),
            Some(Source::SmsEvents),
            "{name}: expected SmsDelivery, got {event:?}"
        );
        assert!(
            ["sms.delivery", "mms.delivery", "voice.delivery"].contains(&event.detail_type()),
            "{name}: unexpected detail type {}",
            event.detail_type()
        );
        assert_ne!(
            event.aggregate_id(SNS_FALLBACK_ID),
            SNS_FALLBACK_ID,
            "{name}: aggregate id fell back to the SNS message id"
        );
    }
}

#[test]
fn ses_event_fixtures_parse() {
    for (name, payload) in fixtures("ses-events") {
        let event = DomainEvent::classify(&payload);
        assert_eq!(
            event.family(),
            Some(Source::SesEvents),
            "{name}: expected Ses, got {event:?}"
        );
        // Every documented kind has a detail-type mapping; `ses.unknown`
        // appearing here means a kind slipped out of `detail_type_for`.
        let detail_type = event.detail_type();
        assert!(
            detail_type.starts_with("ses.") && detail_type != "ses.unknown",
            "{name}: unexpected detail type {detail_type}"
        );
        assert_ne!(
            event.aggregate_id(SNS_FALLBACK_ID),
            SNS_FALLBACK_ID,
            "{name}: aggregate id fell back to the SNS message id"
        );
    }
}

#[test]
fn ses_inbound_fixtures_parse() {
    for (name, payload) in fixtures("ses-inbound") {
        let event = DomainEvent::classify(&payload);
        assert_eq!(
            event.family(),
            Some(Source::SesInbound),
            "{name}: expected SesInbound, got {event:?}"
        );
        // Neither documented example fails a spam/virus verdict.
        assert_eq!(event.detail_type(), "ses.inbound", "{name}");
        assert_ne!(
            event.aggregate_id(SNS_FALLBACK_ID),
            SNS_FALLBACK_ID,
            "{name}: aggregate id fell back to the SNS message id"
        );
    }
}
