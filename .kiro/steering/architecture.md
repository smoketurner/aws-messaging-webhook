---
description: System architecture, component relationships, request flow, and invariants
inclusion: auto
---

# Architecture

## Workspace layout

Five workspace crates; dependency direction is `aws-messaging-webhook` -> `mailbox-store` -> `mailbox`.

| Crate | Package name | Purpose |
|---|---|---|
| `crates/mailbox` | `mailbox` | Pure mailbox domain (no AWS SDK, axum or tokio): stored message/thread/send types, id newtypes, labels, MIME parsing and building, content document, attachment-URL policy and wire shapes |
| `crates/mailbox-store` | `mailbox-store` | The mail table: keys, transaction planner, the `Table` seam (`DynamoTable` impl), `Mailbox` over `Table`, and SDK error classification. The `memory` feature adds `MailMemoryStore` for tests |
| `crates/sns-message-verifier` | `sns-message-verifier` | Standalone SNS signature verification (v1 and v2), no AWS SDK dependency |
| `crates/webhook` | `aws-messaging-webhook` | The Lambda: routing, allowlist, persistence, lifecycle actions, publishing, and the binary's side of the mailbox (`mail/`) and the `/v0` API (`api/`) |
| `crates/webhook-test-support` | `webhook-test-support` | Request harness, SNS envelope signing, and the fakes (one per `Backend` seam) shared by the webhook crate's integration tests |

## Ingress pathways

1. **Function URL (HTTPS)** - Axum router at `/healthz`, the four `/webhooks/...` paths, and the nested bearer-authenticated `/v0` mailbox API; `lambda_http::Adapter` bridges Lambda events to Axum
2. **Direct SNS invoke** - `entry.rs` detects the `Records` array shape and dispatches directly; no path, no confirmation handshake

## Request flow through the webhook crate

1. `entry.rs` - Dispatches by payload shape: `Records` with `eventSource: aws:dynamodb` = DynamoDB stream (the publish relay); other top-level `Records` = direct SNS event; otherwise = Function URL request served through Axum
2. `app.rs` - Routes `GET /healthz`, the four `/webhooks/...` paths (`/webhooks/sms/inbound`, `/webhooks/sms/events`, `/webhooks/ses/events`, `/webhooks/ses/inbound`), and the nested `/v0` mailbox API; path does NOT determine event family
3. `sns/extractor.rs` (`VerifiedSns`) - Security boundary: enforces topic allowlist + signature verification; single constructor used by both ingress paths
4. `model/` - `DomainEvent::classify` does shape-based try-parse, most specific first (SesInbound before Ses - ordering is load-bearing and pinned by fixture matrix)
5. `sns/mod.rs` - Per-message state machine: auto-confirms subscriptions, auto-re-subscribes on abuse, runs the request-path pipeline (persist + inline actions)
6. `store.rs` - DynamoDB persistence with conditional writes for idempotency; `PersistOutcome` (Fresh/Duplicate) tells the request path whether the aggregate was applied
7. `actions.rs` - Inline lifecycle calls (delivery feedback, opt-outs, suppression), run on both Fresh and Duplicate (idempotent)
8. `stream.rs` + `publish.rs::build_outbound` - the DynamoDB Streams consumer is the **sole publisher** to EventBridge, handling two record kinds: an `EVT#` INSERT rebuilds the normalized event from its stored SNS envelope and emits it (with size capping), while an `AGG` MODIFY (or first INSERT) emits `message.status.changed` when `current_status` transitions - count-only bumps (opens/clicks) produce no event

## Critical invariants

- **Response codes are the retry protocol.** 4xx = permanent rejection; 5xx = deliberate, recruiting SNS redelivery for transient failures. `error.rs` is the single place that classifies failures.
- **Never parse direct-invoke `Sns` records with `aws_lambda_events`' `SnsMessage`.** It parses `Timestamp` into `chrono::DateTime` which drops trailing `.000` subseconds, breaking signature verification. Use `SnsEnvelope` which keeps signed values verbatim.
- **No verification bypass in release builds.** `SNS_CERT_HOST_OVERRIDE` is `#[cfg(debug_assertions)]` only.
- **Persist-before-act, publish-by-stream.** The request path persists the event item (the outbox entry) then runs idempotent actions; the DynamoDB Streams relay is the sole publisher. A 5xx redelivery re-runs only the repeat-safe actions, and the stream's event-source-mapping retries + DLQ make publishing durable independently.
- **`ActionErrorKind` splits transient from permanent.** Transient = 5xx/retry; permanent = log + metric, still persist (the stream will publish it). A misconfigured opt-out list must not become a retry storm.
- **HTTP clients never follow redirects** - part of the trust model for cert URL validation.

## DynamoDB data model

Single table with two item types sharing the same partition key:
- **Event items** - `pk = MSG#<messageId>`, `sk = EVT#<timestamp>#<snsMessageId>`: raw body, parse metadata, TTL; each insert is what the stream relay publishes
- **Aggregate item** - same `pk`, `sk = AGG`: current_status, first/last_event_at, open_count, click_count, bot_open_count, bot_click_count (opens/clicks SES flags isBotEvent=Likely), bounce_type. A stream MODIFY that transitions `current_status` is what the relay turns into a `message.status.changed` event

## Consumer read access

Cross-account consumers read the store through an assumable role, not raw table grants. When `pConsumerAccountIds` is set at deploy time, `template.yaml` creates `rConsumerReadRole` (an unnamed IAM role, exposed via the `ConsumerReadRoleArn` output) scoped to `GetItem` / `BatchGetItem` / `Query` on the events table only - no writes, no `Scan`, no index access. Consumers use it to fetch a message's current state (`GetItem` on `pk = MSG#<messageId>`, `sk = AGG`) or full timeline (`Query` on `pk`), typically after receiving a `payloadOmitted` EventBridge detail. Empty `pConsumerAccountIds` (the default) creates no role. This is a SAM-template capability - no Rust code is involved.

## Backend trait and AppState

`state.rs` defines the dependency-injection seam: the `Backend` trait names one component type per external system, and `AppState<B: Backend>` holds one value per component behind a single `Arc`. `Backend`'s associated types are `Table` (`mailbox_store::Table`), `Events` (`EventStore`), `Publisher` (`PublishEvents`), `Sms` (`SmsVoiceApi`), `Ses` (`SesApi`), `Objects` (`ObjectStore`), `Fetcher` (`AttachmentFetcher`), and `Keys` (`ApiKeySource`). Every downstream call goes through these traits so tests stay AWS-free: production uses `aws::Aws` (one small struct per client under `aws/`), tests use `webhook_test_support::Fakes`.

## Observability (metrics)

Metrics are emitted via CloudWatch Embedded Metrics Format (EMF) using the [`metrics`](https://crates.io/crates/metrics) facade backed by [`metrics_cloudwatch_embedded`](https://crates.io/crates/metrics_cloudwatch_embedded). The collector is initialized once in `main.rs` and flushed to stdout at the end of every Lambda invocation in `entry.rs`; CloudWatch extracts metric datapoints directly from the structured JSON log line.

- **Module:** `metrics.rs` — `init()` sets up the global recorder; `names` submodule holds all metric name constants
- **Namespace:** the deployed stack name (`STACK_NAME` env var), matching the namespace previously used by log metric filters
- **Dimension:** `function` (the Lambda function name) on every metric
- **Flush:** end of each invocation in `entry::run`, before the execution environment freezes

### Metric catalog

Metric name constants live in the `names` submodule of `metrics.rs`.

| Name | Type | Emitted from | Meaning |
|---|---|---|---|
| `ColdStart` | Count | `metrics.rs` (`emit_cold_start`, first invocation only) | New execution environment started |
| `Latency` | Histogram (ms) | `entry.rs` | Wall-clock time of `dispatch()` per invocation |
| `MessagesReceived` | Count | `sns/mod.rs` | Request-path deliveries (persisted + duplicate) |
| `Duplicates` | Count | `sns/mod.rs` | SNS redeliveries (subset of MessagesReceived) |
| `SignatureRejections` | Count | `error.rs` | Permanent verification failures (4xx) |
| `AllowlistRejections` | Count | `error.rs` | Topic not in allowlist |
| `InternalErrors` | Count | `error.rs`, `api/error.rs` | Transient infra failures (5xx, recruits retry) |
| `UnclassifiedPayloads` | Count | `sns/mod.rs` | Payload matched no known family (forwarded as `unknown`) |
| `StatusTransitionsSuppressed` | Count | `aws/events_table.rs` | Stale out-of-order `current_status` transitions suppressed by the precedence guard (raw event still durable) |
| `ActionFailures` | Count | `sns/mod.rs`, `actions.rs` | Permanent lifecycle action failure (counted per failed recipient for suppression) |
| `EventsPublished` | Count | `stream.rs` | Successful EventBridge publishes (relay) |
| `PublishFailures` | Count | `stream.rs` | Failed publishes (will retry via stream ESM) |
| `Resubscribes` | Count | `sns/mod.rs` | Auto-re-subscribe after abuse |
| `SubscriptionsLost` | Count | `sns/mod.rs` | Subscription cancelled, not re-attached (alarm on this) |

Mail metrics (emitted only when the mailbox feature is enabled):

| Name | Type | Emitted from | Meaning |
|---|---|---|---|
| `MessagesIngested` | Count | `mail/ingest.rs` | Inbound mail messages successfully ingested |
| `IngestFailures` | Count | `mail/ingest.rs` | Inbound ingest failures |
| `IngestSkipped` | Count | `mail/ingest.rs` | Inbound messages skipped (not ingested) |
| `IngestTimeouts` | Count | `mail/ingest.rs` | Inbound ingest timed out |
| `ApiAuthFailures` | Count | `api/error.rs` | `/v0` bearer authentication failures |
| `MessagesSent` | Count | `mail/sender.rs` | Outbound mail messages sent |
| `SendFailures` | Count | `mail/sender.rs` | Outbound send failures |
| `SendOutcomeUnknown` | Count | `mail/sender.rs` | Outbound send with an indeterminate outcome |

### Testing note

In tests, no global recorder is installed. `metrics::counter!` / `metrics::histogram!` calls no-op when the recorder is absent — this is by design in the `metrics` facade, so no test code changes are needed.
