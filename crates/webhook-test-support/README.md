# webhook-test-support

Shared fakes and harness for `aws-messaging-webhook`'s integration test
binaries. It is a dev-support library (not a `tests/support` module) so a test
binary that doesn't use every helper isn't flagged for dead code; the per-binary
dead-code lint doesn't apply to a library's public API.

It sits above the binary in the dependency graph: it depends on
`aws-messaging-webhook` and reaches its public modules, and the binary's
integration test binaries depend on it in turn. It is `publish = false` and
defines no features of its own.

## API

- `Fakes`: the test `Backend`, one in-process fake per seam. `Table` is
  `mailbox_store::MailMemoryStore`; the rest are `FakeEvents`, `FakePublisher`,
  `FakeSms`, `FakeSes`, `FakeObjectStore`, `FakeFetcher` and `FakeApiKeys`. A
  shared `CallLog` records the event, publish, SMS and SES calls in order so a
  test can assert on how they interleave.
- `Harness` and the state builders: `fake_state`, `harness`/`harness_with`
  (webhook mode, no mailbox), `mail_harness` (mailbox configured) and
  `api_harness` (mailbox plus an authorized bearer key and an existing inbox).
  `test_config` and `test_mail_config` build the `Config`/`MailConfig` they run
  against; `test_verifier` builds an `SnsVerifier` that trusts the harness's
  certificate endpoint.
- SNS envelope and Lambda-invocation builders: `wrapped` (wrap an inner AWS
  payload as a signed SNS notification), `direct_sns_event`/`direct_sns_record`,
  `function_url_event`, `dynamodb_insert_event` and `dynamodb_agg_event`;
  `invoke` drives the single-binary entry point as the Lambda runtime would,
  and `post` sends a notification through the router.
- `api`: the `/v0` request helpers over the real router: `get`, `post`,
  `post_with` and `patch` authenticate with `TEST_API_KEY`; `call` sends exactly
  the headers it is given (for the unauthenticated cases); `send` runs a prebuilt
  request.
- `api_keys`: `FakeApiKeys`, the `ApiKeySource` fake.
- `objects`: `FakeObjectStore`, the `ObjectStore` fake, with per-key error
  injection (including a never-resolving `Hang` for the paused-time deadline
  tests) and call recording.

It enables `sns-message-verifier`'s `test-fixtures` feature and
`mailbox-store`'s `memory` feature.

## Features

This crate defines no Cargo features of its own.

## Tests

The fakes are exercised by the `aws-messaging-webhook` integration suites that
depend on this crate. This crate's own tests are the inline `#[cfg(test)]`
module in `src/objects.rs`, which covers `FakeObjectStore`'s put/get roundtrip,
idempotent puts, the injection modes and the presign TTL.

```bash
cargo test -p webhook-test-support
```

Repository-wide rules are in [CLAUDE.md](../../CLAUDE.md).
