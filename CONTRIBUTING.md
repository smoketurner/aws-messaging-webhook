# Contributing

## Prerequisites

- **Rust** — auto-installed via `rustup` from `rust-toolchain.toml` (1.97.1, edition 2024)
- **cargo-lambda** — local Lambda execution and SAM builds ([install](https://cargo-lambda.info))
- **AWS SAM CLI** — deployment ([install](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html))
- **prek** — pre-commit hooks ([install](https://github.com/nicholasgasior/prek))
- **cargo-deny** — supply chain auditing (`cargo install cargo-deny`)

## Getting started

```bash
git clone https://github.com/smoketurner/aws-messaging-webhook
cd aws-messaging-webhook
make check   # fmt + clippy + deny + tests
```

## Running tests

No AWS account needed — handler tests use properly signed test fixtures.

```bash
make test              # all tests
make test-handlers     # handler integration tests only
make test-verifier     # SNS verifier tests only
```

Run a single test by name:

```bash
cargo test -p aws-messaging-webhook --test handlers <test_name>
cargo test -p sns-message-verifier <test_name>
```

## Code style

Enforced by `rustfmt` (100-char line width) and `clippy` (pedantic + strict denial of
`unwrap`, `panic`, `todo`, `dbg!`, `print` in non-test code). Run:

```bash
make lint   # fmt-check + clippy + deny
```

## Adding dependencies

1. Pin exact versions in workspace `Cargo.toml`: `version = "=x.y.z"`
2. Set `default-features = false` on every dependency
3. Enable features in the **member crate's** `Cargo.toml`, not the workspace root
4. Run `cargo deny check` to verify licenses and advisories

## Branch workflow

1. Create a feature branch off `main`
2. Make changes, run `make check`
3. Open a PR — CI must pass (lint, test, deny, sam validate, actionlint, zizmor)
4. Squash merge to `main`

Never push directly to `main`.

## Local development

```bash
cp .env.example .env   # adjust values
make watch             # starts cargo lambda watch
```

Test with sample payloads (direct SNS invoke pathway):

```bash
cargo lambda invoke aws-messaging-webhook --data-file events/sms-inbound.json
```

In debug builds, set `SNS_CERT_HOST_OVERRIDE` in `.env` to point at a local fake SNS
(e.g., LocalStack). This env var is compiled out in release builds.

## Pre-commit hooks

```bash
prek install   # one-time setup
prek run       # or: make prek
```

Hooks run: `cargo fmt --check`, `cargo clippy`, `cargo deny check`, `actionlint`, `zizmor`.

## SAM template

Validate template changes locally before pushing:

```bash
make validate   # sam validate --lint
```
