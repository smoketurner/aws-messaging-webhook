---
description: Coding standards, naming patterns, error handling idioms, and dependency management rules
inclusion: auto
---

# Conventions

## Language and edition

- Rust edition 2024, MSRV 1.97.1 (pinned in `rust-toolchain.toml`)
- Target: `aarch64-unknown-linux-gnu` (Lambda provided.al2023 arm64)

## Dependency management

- **Exact version pins** (`=x.y.z`) in workspace `Cargo.toml`
- **`default-features = false`** on everything; each crate opts into only the features it uses
- Crypto stack is version-coupled: rsa 0.9, digest 0.10, spki 0.7, x509-cert 0.2, rand 0.8 - do not bump independently
- Comments in `Cargo.toml` explain each pin; read them before changing versions

## Workspace lints

Enforced via `[workspace.lints.clippy]`:
- **Deny:** `unwrap_used`, `panic`, `panic_in_result_fn`, `unimplemented`, `allow_attributes`, `dbg_macro`, `todo`, `print_stdout`, `print_stderr`, `await_holding_lock`, `large_futures`, `exit`, `mem_forget`
- **Warn:** `expect_used`, clippy pedantic (with `module_name_repetitions` and `similar_names` allowed)
- `clippy.toml` relaxes these for test code

## Error handling

- 4xx = permanent rejection (message will not be retried)
- 5xx = deliberate, triggers SNS redelivery for transient failures
- `ActionErrorKind` splits transient vs permanent errors in lifecycle actions
- Use `thiserror` for error types; `anyhow` where appropriate
- Never let a permanent configuration error (e.g., missing opt-out list) produce 5xx retry storms

## Naming

- Module names: snake_case, match their domain concept
- Types: PascalCase, descriptive (e.g., `DomainEvent`, `PersistOutcome`, `VerifiedSns`)
- Trait-based services pattern: trait in `state.rs`, implementations separate

## Code style

- `rustfmt.toml` controls formatting (run `cargo fmt`)
- No `unwrap()` or `panic!()` in non-test code
- Structured logging via `tracing` (JSON output in Lambda)
- All new downstream calls must go through the `Services` trait for testability

## Security rules

- `SNS_CERT_HOST_OVERRIDE` must stay behind `#[cfg(debug_assertions)]`
- HTTP clients must never follow redirects (part of the SNS cert URL trust model)
- Topic allowlist enforcement is mandatory - signature alone only proves "came from SNS in some account"
- Raw message delivery must stay disabled on subscriptions (strips the signed envelope)
