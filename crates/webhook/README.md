# aws-messaging-webhook

The Lambda binary. A single executable that runs as two functions: the webhook
(`FUNCTION_MODE=webhook`, the default), which ingests SNS over a Function URL
(Axum) and direct invoke, runs verify then persist then act then publish, serves
the bearer-authenticated `/v0` mailbox API, and relays the events table's
DynamoDB stream to EventBridge; and the mail sender (`FUNCTION_MODE=sender`),
which consumes the mail table's stream to send, sweep, redrive and close. It is
the sole EventBridge publisher.

Dependencies point one way: `aws-messaging-webhook` → `mailbox-store` →
`mailbox`. This crate persists and serves the mailbox types through a `Mailbox`
and owns every AWS integration; the two crates below it know nothing about it.
`webhook-test-support` sits above it, depending on the binary's public modules
to drive its integration tests.

## API

This crate is a binary (`publish = false`), not a published library, but its
modules are public so the integration test binaries and `webhook-test-support`
can reach them. The seam everything is generic over is the `Backend` trait and
`AppState<B>` in `state.rs`.

- `state`: the `Backend` trait (associated types `Table`, `Events`,
  `Publisher`, `Sms`, `Ses`, `Objects`, `Fetcher`, `Keys`) and `AppState<B>`,
  the state the router holds behind one `Arc`. Production is `aws::Aws`; tests
  supply `webhook_test_support::Fakes`.
- `config`: `Config`, `MailConfig` and `FunctionMode` (`Webhook` or `Sender`),
  read from the environment.
- `entry`: the single-binary entry point that dispatches a Function URL request,
  a direct SNS event or a stream batch.
- `app`: the Axum router, the 1 MiB body limit, and `Authorization` redaction.
- `sns`: the request-body extractor, subscription confirmation, and the SNS
  ingress handler (`extractor`, `confirm`, `mod`).
- `model`: the `Source` families, payload classification, and
  `DomainEvent::detail_type` (the EventBridge detail types).
- `store`: the `EventStore` seam over the events table.
- `actions`: the `SmsVoiceApi` and `SesApi` seams, the lifecycle actions.
- `publish`: the `PublishEvents` seam onto EventBridge.
- `stream`: the DynamoDB stream relay that publishes `message.status.changed`
  and the mailbox events.
- `metrics`, `logging`: the CloudWatch EMF metric catalog and tracing setup.
- `allowlist`: `TopicAllowlist`, the `TopicArn` allowlist check.
- `error`: the shared error type.
- `api`: the `/v0` mailbox API (`auth`, `read`, `send`, `labels`,
  `pagination`, `keys`, the `unimplemented` 501 groups).
- `aws`: the production `Backend` implementations (`events_table`, `publisher`,
  `sms`, `ses`, `objects`, `api_keys`).
- `mail`: the mail feature (`ingest`, `sender`, `fetch`, `events`, `objects`,
  `content`, `object_keys`).

### HTTP routes

- `GET /healthz`
- `POST /webhooks/sms/inbound`, `POST /webhooks/sms/events`,
  `POST /webhooks/ses/events`, `POST /webhooks/ses/inbound` (each wired to one
  `Source` family through `Source::webhook_path`)
- the nested `/v0` mailbox API, behind bearer auth

### Environment variables

Read in `config.rs`. General: `FUNCTION_MODE`, `EVENT_SOURCE`,
`ALLOWED_TOPICS`, `TABLE_NAME` (required in webhook mode), `EVENT_BUS_NAME`
(required in webhook mode), `AUTO_RESUBSCRIBE`, `OPT_OUT_LIST_NAME`,
`RAW_EVENT_RETENTION_DAYS`, `AGGREGATE_RETENTION_DAYS`, `STACK_NAME`,
`SNS_CERT_HOST_OVERRIDE` (debug builds only), `RUST_LOG`. The mail feature is
disabled unless `MAIL_DOMAIN` is set; when it is, `MAIL_TABLE_NAME`,
`MAIL_BUCKET`, `MAIL_INBOX`, `SES_CONFIGURATION_SET`, `MAIL_IDENTITY_ARN`,
`API_KEYS_PARAMETER`, `ATTACHMENT_URL_TTL_SECONDS`, `MAIL_RETENTION_DAYS` and
`AWS_REGION` apply. See `docs/` for deploy, events and operations detail rather
than duplicating it here.

## Features

This crate defines no Cargo features.

## Tests

Inline `#[cfg(test)]` modules cover `config.rs`, `model/` and the rest. The
handler and integration suites in `tests/` drive the real Axum router and the
Lambda entry against the in-process fakes from `webhook-test-support`:
`handlers.rs`, `model_fixtures.rs`, the `api_*` suites (`api_auth`, `api_read`,
`api_send`, `api_reply`, `api_patch`, `api_download`), the `mail_*` suites
(`mail_ingest`, `mail_sender`, `mail_stream`), `mailbox_event_schemas.rs`, and
the inbox-id casing suites.

```bash
cargo test -p aws-messaging-webhook
cargo test -p aws-messaging-webhook --test handlers   # one suite
```

## Invariants

- The whole stack is generic over `Backend`. New external systems go through a
  seam type, so the tests stay free of AWS.
- Exactly one source publishes to EventBridge: this binary's stream relay and,
  on the request path, the control-plane `subscription.changed` notice. A
  signature proves a message came from SNS, so `TopicArn` is always checked
  against `ALLOWED_TOPICS` before anything is persisted or published.
- The `Authorization` header is redacted before tracing records a request, so a
  bearer key never reaches a log line.
- Repository-wide rules are in [CLAUDE.md](../../CLAUDE.md).
