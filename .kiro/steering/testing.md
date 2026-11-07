---
description: Testing philosophy, test structure, how to write new tests, and fixture system
inclusion: auto
---

# Testing

## Philosophy

- Handler tests ARE the integration suite - they drive the real Axum router with properly signed SNS envelopes
- No AWS account needed for any test
- The `Backend` trait + `AppState<B>` seam in `state.rs` names one component type per external system; tests supply in-process fakes (`webhook_test_support::Fakes`)
- All new downstream calls must go through a `Backend` seam (`EventStore`, `PublishEvents`, `SmsVoiceApi`, `SesApi`, `ObjectStore`, `AttachmentFetcher`, `ApiKeySource`, or the `Table` seam) so tests stay AWS-free

## Test structure

### Handler tests (`crates/webhook/tests/handlers.rs`)

- End-to-end tests against the real router
- Use `webhook_test_support::Fakes` as the `Backend` (one fake per seam), via the harness and request helpers in the `webhook-test-support` crate
- SNS envelopes are properly signed using the `sns-message-verifier` crate's `test-fixtures` feature
- Cover both Function URL (HTTPS) and direct-invoke pathways
- Run a specific test: `cargo test -p aws-messaging-webhook --test handlers <test_name>`

### Model fixture tests (`crates/webhook/tests/model_fixtures.rs`)

- Pin the classification ordering with verbatim AWS-documented payloads
- Ensure `DomainEvent::classify` try-parse ordering stays correct (SesInbound before Ses, etc.)
- Located in `crates/webhook/tests/fixtures/` directory

### Mailbox API and mail tests (`crates/webhook/tests/`)

- The `api_*` suites (`api_auth.rs`, `api_read.rs`, `api_send.rs`, `api_reply.rs`, `api_patch.rs`, `api_download.rs`) exercise the bearer-authenticated `/v0` mailbox API against fakes
- The `mail_*` suites (`mail_ingest.rs`, `mail_sender.rs`, `mail_stream.rs`) cover inbound ingest, the sender, and the mail-table stream relay
- `mailbox_event_schemas.rs` pins the emitted mailbox event schemas
- Run a specific test: `cargo test -p aws-messaging-webhook --test <suite> <test_name>`

### Store-behaviour tests (`crates/mailbox-store`)

- `crates/mailbox-store/tests/mailbox_behaviour.rs` drives the mailbox through its public API against `MailMemoryStore` (the `memory` feature)
- Run a specific test: `cargo test -p mailbox-store --test mailbox_behaviour <test_name>`

### Verifier tests (`crates/sns-message-verifier`)

- Unit tests for signature verification (v1 and v2)
- Run a specific test: `cargo test -p sns-message-verifier <test_name>`

## Test fixtures feature

The `sns-message-verifier` crate has a `test-fixtures` Cargo feature that:
- Generates throwaway RSA keys and X.509 certificates
- Allows consumers to create properly signed SNS envelopes for testing
- Used by the webhook crate's handler tests

## Writing new tests

1. Add new downstream calls through a `Backend` seam trait
2. Implement the new behaviour in the matching fake in `webhook-test-support`
3. Write handler tests that exercise the full pipeline (verify, persist, act, publish)
4. For new event families: add a fixture in `tests/fixtures/`, add a classification test in `model_fixtures.rs`

## Lint relaxations for tests

`clippy.toml` relaxes workspace lints for test code (e.g., `unwrap_used` is allowed in tests).

## Running tests

```bash
cargo test --workspace                                      # All tests
cargo test -p aws-messaging-webhook --test handlers         # All handler tests
cargo test -p aws-messaging-webhook --test handlers <name>  # One handler test
cargo test -p sns-message-verifier                          # All verifier tests
cargo test -p sns-message-verifier <name>                   # One verifier test
```
