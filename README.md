# aws-messaging-webhook

[![CI](https://img.shields.io/github/actions/workflow/status/smoketurner/aws-messaging-webhook/ci.yml?branch=main)](https://github.com/smoketurner/aws-messaging-webhook/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.98.0-blue)](rust-toolchain.toml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](#license)

One Rust Lambda function that receives AWS messaging events from Amazon SNS, records them in
DynamoDB, and republishes them to an EventBridge bus your applications subscribe to. It handles:

- AWS End User Messaging (EUM): inbound SMS and delivery receipts.
- Amazon SES: sending events (bounces, complaints, opens, clicks) and inbound mail.

Optionally, it also runs a mailbox on SES with an HTTP API to read and send mail.

## How it works

```
EUM / SES ─► SNS ─► Function URL (HTTPS) or direct invoke
                      │
                      ├─ verify   SNS signature + topic allowlist
                      ├─ persist  DynamoDB, one conditional write per event
                      ├─ act      opt-outs, suppression, delivery feedback
                      │
                      └─ DynamoDB stream ─► relay ─► EventBridge ─► your apps
```

- **Verify.** Every message must carry a valid SNS signature and come from an allowlisted topic.
- **Persist.** A conditional write makes SNS redeliveries harmless.
- **Act.** `STOP`/`START` update the EUM opt-out list. Hard bounces and complaints go on the SES
  suppression list. Delivery receipts send message feedback.
- **Publish.** A stream relay publishes each saved event, with retries and a dead-letter queue.

Subscriptions confirm themselves. If someone abuses an `UnsubscribeURL`, the function
re-subscribes.

## Quick start

Install Rust, [`cargo-lambda`](https://cargo-lambda.info) and the AWS SAM CLI, then:

```bash
sam build
sam deploy --guided   # set pAllowedTopics to your account id
```

Subscribe your SNS topics to the endpoints the stack outputs. See [docs/deploy.md](docs/deploy.md).

> [!IMPORTANT]
> Set `pAllowedTopics`. A valid signature proves only that a message came from SNS in *some*
> account. Keep raw message delivery off on every subscription.

## Mailbox

Set `pMailDomain` and `pMailInbox` to receive mail at `<inbox>@<domain>`. Inbound mail is stored
in S3 and DynamoDB, threaded, and exposed through a `/v0` API that also sends mail.

- [Setup](docs/mailbox/setup.md): parameters and five one-time steps after the first deploy
- [API](docs/mailbox/api.md): routes, labels, filtering and sending
- [Events](docs/mailbox/events.md): `message.*` events and webhook delivery
- [Tracking](docs/mailbox/tracking.md): opens, clicks and custom tracking domains
- [Storage](docs/mailbox/storage.md): mail table, bucket layout and retention
- [Runbook](docs/mailbox/runbook.md): stuck sends and failed ingests

## Documentation

| Doc | Covers |
|---|---|
| [Deploy](docs/deploy.md) | Parameters, topic subscriptions, retry behavior |
| [Events](docs/events.md) | EventBridge detail types and payloads |
| [Data model](docs/data-model.md) | Events table and cross-account read access |
| [Operations](docs/operations.md) | Logs, metrics, alarms and an end-to-end check |
| [Mailbox design](docs/design/mailbox-on-ses.md) | Why the mailbox is built the way it is |

## Development

```bash
cargo test --workspace   # no AWS account needed
cargo clippy --all-targets --all-features -- -D warnings
prek run                 # fmt, clippy, deny, actionlint, zizmor
```

| Crate | Purpose |
|---|---|
| `crates/webhook` | The Lambda function |
| `crates/sns-message-verifier` | SNS signature verification. The `test-fixtures` feature signs test messages |

Debug builds read `SNS_CERT_HOST_OVERRIDE` to run against a local fake SNS under
`cargo lambda watch`. Release builds always verify.

## License

Licensed under either of the [Apache License 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
