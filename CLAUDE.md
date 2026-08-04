# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo test --workspace                                  # unit + handler tests; no AWS account needed
cargo test -p aws-messaging-webhook --test handlers <name>   # one handler test
cargo test -p sns-message-verifier <name>                    # one verifier test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo deny check                                        # advisories/licenses/bans (runs in CI)
prek run                                                # all pre-commit hooks: fmt, clippy, deny, actionlint, zizmor
```

Deploy (needs `cargo-lambda` and the AWS SAM CLI; toolchain pinned in `rust-toolchain.toml`):

```bash
sam build --config-env dev && sam deploy --config-env dev
scripts/subscribe.sh <stack-name> <sms/inbound|sms/events|ses/events|ses/inbound> <topic-arn>
scripts/e2e-ses-bounce.sh <stack> <verified-sender>     # deployed end-to-end check via SES simulator
```

## Architecture

One Rust Lambda (Axum behind a Lambda Function URL) receiving AWS messaging events over SNS.
The pipeline for every notification is **verify → persist → act → publish**, in that order, and
the order is load-bearing (see below). Two workspace crates:

- **`crates/sns-message-verifier`** — standalone SNS signature verification (versions 1 and 2),
  no AWS SDK dependency. Trust anchor is the `SigningCertURL` host policy (`https://sns.<region>.amazonaws.com`
  only, no redirects followed, no CA-chain check — same model as AWS's own validators). The
  `test-fixtures` feature generates throwaway keys/certs so consumers can sign test envelopes.
- **`crates/webhook`** (package `aws-messaging-webhook`) — the Lambda itself.

### Request flow through the webhook crate

1. `app.rs` routes the four `/webhooks/...` paths; each path expects one event family
   (`model::Source`) matching the SNS topic wired to it.
2. `sns/extractor.rs` (`VerifiedSns`, an Axum extractor) is the security boundary: it enforces
   the topic allowlist (`allowlist.rs`) and signature verification before any handler runs.
   Signature proof alone means "came from SNS in *some* account" — the allowlist is what makes
   the public URL safe.
3. `sns/mod.rs` is the per-message state machine: auto-confirms `SubscriptionConfirmation`,
   auto-re-subscribes on `UnsubscribeConfirmation` (the unauthenticated `UnsubscribeURL` is
   abuse surface; authenticated unsubscribes don't emit this message type), and runs the
   notification pipeline.
4. `model/` parses the inner SNS `Message` per source path. Unrecognized payloads become
   `DomainEvent::Unknown` and are forwarded, never rejected — the pipeline degrades to
   pass-through for new AWS event shapes.
5. `store.rs` persists to DynamoDB (event items + per-message aggregate). The conditional write
   is the idempotency mechanism; `PersistOutcome` (`Fresh` / `DuplicatePersisted` /
   `DuplicatePublished`) tells the handler whether to skip, resume, or run fully.
6. `actions/` runs inline lifecycle calls (delivery feedback, STOP/START opt-outs, bounce and
   complaint suppression). AWS-native lists are the source of truth.
7. `publish.rs` + `build_outbound` in `sns/mod.rs` emit to EventBridge, capping detail size so
   an oversized payload can never become a poison message that 5xx-loops forever.

### Invariants to preserve

- **Response codes are the retry protocol.** 4xx = permanently rejected; 5xx = deliberate,
  recruiting SNS redelivery for transient failures. The persist-before-act-before-publish
  ordering plus `PERSISTED`/`PUBLISHED` status means a retry resumes exactly where it died
  without duplicate bus events; action APIs must stay repeat-safe. `ActionErrorKind` splits
  transient (5xx, retry) from permanent (log + metric, still publish) — a misconfigured
  opt-out list must not become a retry storm.
- **No verification bypass in release builds.** `SNS_CERT_HOST_OVERRIDE` (for
  `cargo lambda watch` against a fake SNS) is compiled in under `#[cfg(debug_assertions)]`
  only. The HTTP clients never follow redirects — that's part of the trust model, not a nicety.
- **Handler tests are the integration suite.** `crates/webhook/tests/handlers.rs` drives the
  real router with properly signed envelopes against one `FakeServices` implementing the
  `Services` trait (`state.rs` — the single bound aggregating `EventStore + PublishEvents +
  SmsVoiceApi + SesApi`). New downstream calls go through that trait so tests stay AWS-free.
- SNS topics and subscriptions deliberately live outside the SAM stack; `template.yaml` maps
  CloudFormation parameters to the env vars `config.rs` reads.

## Dependencies

Workspace `Cargo.toml` pins exact versions (`=x.y.z`) with `default-features = false` on
everything; each crate opts into only the features it uses. The crypto stack is
version-coupled (rsa 0.9 ↔ digest 0.10 / spki 0.7 / x509-cert 0.2, rand 0.8) — the comments in
`Cargo.toml` explain each pin; don't bump those lines independently. Workspace lints deny
`unwrap`/`panic`/etc.; `clippy.toml` relaxes them for test code.
