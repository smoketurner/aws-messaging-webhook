---
description: How to build, test, run, and deploy the project; toolchain requirements and local development workflow
inclusion: auto
---

# Development

## Prerequisites

- Rust toolchain: pinned in `rust-toolchain.toml` (1.97.1 with rustfmt and clippy)
- `cargo-lambda` for local Lambda execution and SAM builds
- AWS SAM CLI for deployment
- `prek` for pre-commit hooks (optional but recommended)
- `cargo-deny` for dependency auditing
- `actionlint` and `zizmor` for CI workflow linting

## Build

```bash
sam build --config-env dev          # Full Lambda build (uses cargo-lambda)
cargo build --workspace             # Local workspace build
```

## Test

```bash
cargo test --workspace                                      # All tests (no AWS needed)
cargo test -p aws-messaging-webhook --test handlers <name>  # One handler test
cargo test -p sns-message-verifier <name>                   # One verifier test
```

Handler tests drive the real router end-to-end with properly signed SNS envelopes. The `sns-message-verifier` crate's `test-fixtures` feature generates throwaway keys and certificates for this purpose.

## Lint and check

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo deny check                    # Advisories, licenses, bans
prek run                            # All pre-commit hooks at once
```

## Local execution

```bash
cargo lambda watch                  # Starts local Lambda runtime
```

In debug builds, set `SNS_CERT_HOST_OVERRIDE` to point at a local fake SNS for testing. This env var is compiled out in release builds.

## Deploy

```bash
sam build --config-env dev && sam deploy --config-env dev
```

Key parameters: `Stage`, `AllowedTopics`, `AutoResubscribe`, `OptOutListName`, `EventSource`, `RawEventRetentionDays`, `LogLevel`, `LogRetentionDays`.

## End-to-end verification (deployed stack)

Send a probe through the SES mailbox simulator:
```bash
aws sesv2 send-email --from-email-address <verified-sender> \
  --destination ToAddresses=bounce@simulator.amazonses.com \
  --content "Simple={Subject={Data=probe},Body={Text={Data=probe}}}"
```
Then confirm: `ses.bounce` event arrives on the bus, DynamoDB event item reaches `status = PUBLISHED`, simulator address lands on the SES suppression list.

## Pre-commit hooks (via prek)

Configured in `.pre-commit-config.yaml`:
1. `cargo fmt --check`
2. `cargo clippy --all-targets --all-features -- -D warnings`
3. `cargo deny check`
4. `actionlint` (GitHub workflow files only)
5. `zizmor` (GitHub workflow files only)
