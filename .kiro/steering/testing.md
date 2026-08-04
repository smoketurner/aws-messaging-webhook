---
description: Testing philosophy, test structure, how to write new tests, and fixture system
inclusion: auto
---

# Testing

## Philosophy

- Handler tests ARE the integration suite - they drive the real Axum router with properly signed SNS envelopes
- No AWS account needed for any test
- Trait-based services (`Services` trait in `state.rs`) enable `FakeServices` test doubles
- All new downstream calls must go through the `Services` trait so tests stay AWS-free

## Test structure

### Handler tests (`crates/webhook/tests/handlers.rs`)

- End-to-end tests against the real router
- Use `FakeServices` implementing the `Services` trait
- SNS envelopes are properly signed using the `sns-message-verifier` crate's `test-fixtures` feature
- Cover both Function URL (HTTPS) and direct-invoke pathways
- Run a specific test: `cargo test -p aws-messaging-webhook --test handlers <test_name>`

### Model fixture tests (`crates/webhook/tests/model_fixtures.rs`)

- Pin the classification ordering with verbatim AWS-documented payloads
- Ensure `DomainEvent::classify` try-parse ordering stays correct (SesInbound before Ses, etc.)
- Located in `crates/webhook/tests/fixtures/` directory

### Verifier tests (`crates/sns-message-verifier`)

- Unit tests for signature verification (v1 and v2)
- Run a specific test: `cargo test -p sns-message-verifier <test_name>`

## Test fixtures feature

The `sns-message-verifier` crate has a `test-fixtures` Cargo feature that:
- Generates throwaway RSA keys and X.509 certificates
- Allows consumers to create properly signed SNS envelopes for testing
- Used by the webhook crate's handler tests

## Writing new tests

1. Add new downstream calls through the `Services` trait
2. Implement the new trait method in `FakeServices`
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
