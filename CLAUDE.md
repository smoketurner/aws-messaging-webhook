# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo test --workspace                                  # unit + handler tests; no AWS account needed
cargo test -p aws-messaging-webhook --test handlers <name>   # one handler test
cargo test -p sns-message-verifier <name>                    # one verifier test
cargo test -p mailbox-store --test mailbox_behaviour <name>  # one store-behaviour test
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps --all-features   # rustdoc lints (runs in CI)
cargo fmt --all -- --check
cargo deny check                                        # advisories/licenses/bans (runs in CI)
prek run                                                # all pre-commit hooks: fmt, clippy, doc, deny, actionlint, zizmor
```

Deploy (needs `cargo-lambda` and the AWS SAM CLI; toolchain pinned in `rust-toolchain.toml`):

```bash
sam build && sam deploy
```

Topic wiring is plain AWS CLI (subscribe over https to a webhook path, or lambda protocol +
`add-permission` for direct invoke) — the commands live in `docs/deploy.md`; the SES-simulator
end-to-end probe is in `docs/operations.md`.

## Architecture

One Rust binary deployed as two Lambda functions. The webhook function receives AWS messaging
events over SNS via two ingress pathways — HTTPS POSTs to a Lambda Function URL (Axum) and
direct SNS→Lambda invocations — and serves the bearer-authenticated `/v0` mailbox API (`api/`)
on the same Function URL. The mail-sender function (created only when `pMailDomain` is set) is
the same binary in `FunctionMode::Sender`, reached through the mail table's stream, a scheduled
sweep, or an operator command. `entry.rs` dispatches each invocation by mode and payload shape:
a top-level `Records` array is either a DynamoDB stream batch (`eventSource: aws:dynamodb`) or
an SNS event; anything else is a Function URL request served through `lambda_http::Adapter`
(in sender mode, a sweep or command). The pipeline for every notification is
**verify → persist → act → publish**, in that order, and the order is load-bearing (see below).
Five workspace crates; dependency direction is `aws-messaging-webhook` → `mailbox-store` →
`mailbox`:

- **`crates/mailbox`** — the pure mailbox domain, with no AWS SDK, axum or tokio: the stored
  message, thread and send types and their caps, the id newtypes, labels, MIME parsing
  (`mime`) and building (`build`), the content document, attachment-URL policy and the wire
  shapes. Its `test-fixtures` feature exposes a sample message (and the plain-http URL parser)
  to other crates' tests.
- **`crates/mailbox-store`** — the mail table: keys, the transaction planner (`plan`) and its
  cancellation decoding (`txn`), the `Table` seam with its `DynamoTable` implementation,
  `Mailbox` (every mailbox read and write, written once on top of `Table`), and the SDK error
  classification the binary's clients share (`sdk_error`). The `memory` feature, enabled only
  by dev-dependencies, adds `MailMemoryStore`.
- **`crates/sns-message-verifier`** — standalone SNS signature verification (versions 1 and 2),
  no AWS SDK dependency. Trust anchor is the `SigningCertURL` host policy (`https://sns.<region>.amazonaws.com`
  only, no redirects followed, no CA-chain check — same model as AWS's own validators). The
  `test-fixtures` feature generates throwaway keys/certs so consumers can sign test envelopes.
- **`crates/webhook`** (package `aws-messaging-webhook`) — the Lambda itself, including the
  binary's side of the mailbox (`mail/`: ingest, the sender, the relay's mail events,
  attachment fetching, and the mail bucket).
- **`crates/webhook-test-support`** — the request harness, SNS envelope signing and the
  non-mail fakes (one per seam) shared by the webhook crate's integration tests.

Tests live where the code they exercise lives: `mailbox` has inline unit tests only;
`mailbox-store` tests behaviour through its public API in `crates/mailbox-store/tests/`
against `MailMemoryStore`, keeps tests that need crate internals inline (alias rows, the
thread race, planning), and pins the exact DynamoDB requests inline beside `DynamoTable`;
`crates/webhook/tests/` only drives the router or the Lambda entry. Crates expose only what
another crate uses: `unreachable_pub` is a workspace lint, so a test needing an internal item
moves inline rather than making the item public.

### Request flow through the webhook crate

1. `app.rs` routes the four `/webhooks/...` paths; the direct-invoke pathway has no path.
   Neither determines the event family: `DomainEvent::classify` (`model/`) does, by
   shape-based try-parse, most specific first. The ordering is load-bearing in one place —
   an SES inbound receipt also satisfies the sending-notification shape, so `SesInbound` is
   tried before `Ses` — and is pinned by the fixture matrix in `tests/model_fixtures.rs`
   (verbatim AWS-documented payloads). A path only names the family the operator wired to
   it: a mismatch logs `family_mismatch` but the event still processes as what it is.
2. `sns/extractor.rs` (`VerifiedSns`) is the security boundary: `VerifiedSns::verify` enforces
   the topic allowlist (`allowlist.rs`) and signature verification, and is the only constructor
   both ingress paths use. Signature proof alone means "came from SNS in *some* account" — the
   allowlist is what makes the public URL safe (direct invokes are additionally gated by the
   per-topic function resource policy installed when wiring the subscription).
3. `sns/mod.rs` is the per-message state machine: auto-confirms `SubscriptionConfirmation`,
   auto-re-subscribes on `UnsubscribeConfirmation` (the unauthenticated `UnsubscribeURL` is
   abuse surface; authenticated unsubscribes don't emit this message type), and runs the
   notification pipeline.
4. Payloads matching no family become `DomainEvent::Unknown` and are forwarded, never
   rejected (source `unknown`, null `webhookPath` in the outbound meta, `unclassified_payload`
   log/metric) — the pipeline degrades to pass-through for new AWS event shapes.
5. `store.rs` persists to DynamoDB (event items + per-message aggregate). The conditional write
   is the idempotency mechanism; `PersistOutcome` (`Fresh` / `Duplicate`) tells the request path
   whether the aggregate was applied (idempotent actions run regardless).
6. `actions.rs` runs inline lifecycle calls (delivery feedback, STOP/START opt-outs, bounce and
   complaint suppression). AWS-native lists are the source of truth.
7. `stream.rs` is the **sole publisher of event details**: a DynamoDB Streams consumer (same
   binary) rebuilds each
   newly-persisted event via `publish.rs::build_outbound` and emits it to EventBridge, capping
   detail size so an oversized payload can't become a poison record. The event-source mapping's
   retries + on-failure DLQ make delivery durable independently of the request path.

### Invariants to preserve

- **Response codes are the retry protocol.** 4xx = permanently rejected; 5xx = deliberate,
  recruiting SNS redelivery for transient failures. `entry.rs` maps the same protocol onto the
  direct-invoke contract (2xx/4xx → `Ok`, no retry; 5xx → `Err`, async-invoke retry) — `error.rs`
  stays the single place that classifies failures. Note the direct pathway's async queue retries
  only twice; the HTTPS delivery policy retries far longer.
- **Never parse the direct-invoke `Sns` record with `aws_lambda_events`' `SnsMessage`.** It
  parses `Timestamp` into a `chrono::DateTime`, whose serialization drops trailing `.000`
  subseconds — rebuilding the signed canonical string from it rejects every message published
  on a whole second. `entry.rs` deserializes the record straight into `SnsEnvelope` (which
  aliases the Lambda-shape `SigningCertUrl`/`UnsubscribeUrl` casing) so signed values stay
  verbatim.
- **"Now" is the Lambda event's timestamp, not a clock reading.** What an invocation writes
  is stamped with the time AWS put on the event it is serving — a Function URL request's
  `requestContext.timeEpoch` (the `RequestTime` extractor), a direct SNS delivery's envelope
  `Timestamp`, a scheduled sweep's `time` — and threaded down as a `jiff::Timestamp`
  (`mailbox::time::Timestamp`); the `mailbox` crate never reads the clock. Text becomes a
  `Timestamp` once, where it enters (the event, a query parameter, a stored item), and stays
  one: stored and wire structs hold `Timestamp` fields under `#[serde(with = "time::fixed")]`,
  which writes the fixed-width `YYYY-MM-DDTHH:MM:SS.sssZ` form sort keys depend on (an
  unreadable *optional* instant reads as absent; a required one fails the item). Don't pass
  a formatted string between functions, and never `Display` a `Timestamp` (its width varies)
  — `time::format` is for a key or a JSON literal being built by hand. `clock.rs` says
  where each comes from and holds the one `now()`, for what no event can supply: the sender's
  claim on a send (a lease the sweep judges by age — the stream record that triggered the
  sender may be far older than the attempt, so dating the claim by it would make a live claim
  look abandoned and send the message twice), an operator command, and tests that build a
  request without a Lambda context. Monotonic deadlines and the verifier's certificate-validity
  check use the real clock too; a signed `Timestamp` must never decide whether the certificate
  that signed it is current. The inbound message id is seeded from the mail's own timestamps
  (content), never from the delivery time, which differs on every HTTPS redelivery. A direct
  delivery is dated by its envelope but *retained* from the invocation's deadline when that is
  later (`clock::Arrival`), so an event redriven long after it was published is not stored
  already expired. The sender reads the clock again for each mark it records (`ses_call_at`,
  `sent_at`, a release): loading a send can take most of an invocation.
- **A send is called once; SES's events are its second witness.** `SendEmail` has no
  idempotency token, so the sender marks `ses_call_at` before the call and nothing automatic
  ever calls SES again for a send carrying that mark. If the write recording SES's answer is
  lost, the send waits as `sending` then `unknown` — and the first SES event for the message
  (matched by the `mailbox_message` tag, `sender::confirm_sent`) records it as sent. Only a
  send with the mark may be settled from an event: the tag is the mail sender's claim, and
  the configuration set is shared.
- **Persist before acting; actions stay repeat-safe.** The request path persists (the outbox
  entry) before running idempotent actions; a 5xx redelivery re-runs only the repeat-safe
  actions, so action APIs must stay repeat-safe.
  Publishing is decoupled — the stream relay publishes every event detail, with its own
  retries + DLQ (the request path publishes only `subscription.changed`).
  `ActionErrorKind` splits transient (5xx, retry) from permanent (log + metric, still persist) —
  a misconfigured opt-out list must not become a retry storm.
- **No verification bypass in release builds.** `SNS_CERT_HOST_OVERRIDE` (for
  `cargo lambda watch` against a fake SNS) is compiled in under `#[cfg(debug_assertions)]`
  only; `mail/fetch.rs`'s plain-http mode is `#[cfg(test)]`, and the URL parser behind it
  (`mailbox::url_policy`) is test builds or the dev-only `test-fixtures` feature, for the same
  reason. Nothing a non-dev dependency enables (`mailbox-store`'s `memory`,
  `webhook-test-support`'s dependencies) may turn that feature on. The SNS HTTP
  clients never follow redirects — that's part of the trust model, not a nicety. The one
  exception is `mail/fetch.rs`, which fetches caller-supplied attachment URLs and therefore
  *must* handle redirects; it follows them by hand, at most five hops, re-running the full
  `mailbox::url_policy` shape and address checks on every `Location`, because the first check
  says nothing about where a redirect leads.
- **Strongly consistent DynamoDB reads cost twice the capacity; use them only when a stale read
  would be wrong, not merely late.** Default to eventually consistent. A stale miss that a
  conditional write downstream already catches, or that a retry corrects, does not justify one;
  a read that must observe what a failed condition just proved exists (the `ensure_inbox`
  recovery) or that feeds a `VersionEquals` write does. Say why at each `consistent_read(true)`.
- **Integration tests drive the real router against fakes.** `crates/webhook/tests/handlers.rs`
  sends properly signed SNS envelopes, and the `api_*`/`mail_*` suites exercise the mailbox,
  all against fakes from `crates/webhook-test-support`. `AppState<B: Backend>` (`state.rs`)
  holds one component per external system — `Table`, `EventStore`, `PublishEvents`,
  `SmsVoiceApi`, `SesApi`, `ObjectStore`, `AttachmentFetcher` and `ApiKeySource` — and
  `Backend` names their types: `aws::Aws` in production (one small struct per client under
  `aws/`), `webhook_test_support::Fakes` in tests. Mailbox reads and writes are written once,
  in `mailbox_store::mailbox`, on top of the `Table` seam (`mailbox_store::table`) that
  `DynamoTable` and `MailMemoryStore` implement. New downstream calls go through those traits so tests stay
  AWS-free.
- SNS topics and subscriptions deliberately live outside the SAM stack, except the two
  stack-owned mail topics (SES receipts and SES configuration-set events, created only when
  `pMailDomain` is set); `template.yaml` maps CloudFormation parameters to the env vars
  `config.rs` reads.

## Dependencies

Workspace `Cargo.toml` pins exact versions (`=x.y.z`) with `default-features = false` on
everything, listed alphabetically and without commentary; each crate opts into only the
features it uses.

`[workspace.lints]` denies panicking code (`unwrap`/`expect`/`panic`, indexing and string
slicing, `arithmetic_side_effects`, lossy casts), silently dropped results, and `#[allow]` —
an opt-out is a narrow `#[expect(lint, reason = "...")]` on the item that needs it. Use
`saturating_*`/`checked_*` arithmetic and `.get()`/`split_at`/`strip_prefix` rather than
`expect`ing the lint, and reserve `#[expect]` for code that is provably in range (a JSON
literal being filled in, say). `clippy.toml` relaxes unwrap, expect, panic and indexing inside
`#[test]` functions and `#[cfg(test)]` modules; integration-test helpers outside a `#[test]`
function and `webhook-test-support` opt out per item. The rustdoc link lints run only under
`cargo doc`, which the `cargo-doc` hook and CI run with warnings denied: a public doc may not
link to a private item or to another crate's internals.

Items from the workspace's own crates are imported, not spelled inline: `clippy::absolute_paths`
is denied with `absolute-paths-max-segments = 2`, so `crate::mail::sender::sweep` or
`mailbox::time::Timestamp` in an expression is an error — `use` the type, or a function's
parent module. External crates are exempt through `absolute-paths-allowed-crates` in
`clippy.toml`; a new dependency named by an absolute path is added there.

Dates and times are `jiff`, never `chrono`. `chrono` is in the build only as a dependency of
`aws_lambda_events`, whose DynamoDB stream types (`stream.rs`, the sender) carry one field of
it that this code never reads; `deny.toml` bans it for everything else. `jiff` is built with
`std` only — every instant here is UTC, so no time-zone database is compiled in.

Four pins are not free to move, and a bump that ignores them fails to resolve or reintroduces
an advisory:

- **The crypto stack is version-coupled.** rsa 0.9 speaks digest 0.10 / spki 0.7, so sha1 and
  sha2 stay on 0.10.x and x509-cert on 0.2.x — the 0.11 / 0.3 releases are trait-incompatible.
  rand stays on 0.8, the last line implementing the rand_core 0.6 traits rsa 0.9 needs.
- **`aws-sdk-s3`, `aws-sdk-ssm`, and `aws-smithy-types` move together.** Bump them as a set;
  check with `cargo tree`/`cargo deny` before moving any of the three.
- **Defaults off on the AWS SDKs** drops the legacy rustls 0.21 connector
  (RUSTSEC-2026-0098/0099/0104); crates enable `default-https-client` (hyper 1) instead.
- **`mail-builder`'s `gethostname` feature stays off**: `Message-ID` is minted from the message
  id, never from the host.

`[profile.dev.package]` optimizes rsa and num-bigint-dig because test harnesses generate a
2048-bit RSA key per harness, which takes seconds unoptimized. `[profile.release]` deliberately
omits `panic = "abort"` (lambda_runtime turns an unwound handler panic into a 502 and keeps the
sandbox warm) and `opt-level = "z"` (per-message RSA verification and JSON parsing are billed
CPU time).
