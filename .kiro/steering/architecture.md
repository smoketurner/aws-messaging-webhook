---
description: System architecture, component relationships, request flow, and invariants
inclusion: auto
---

# Architecture

## Workspace layout

| Crate | Package name | Purpose |
|---|---|---|
| `crates/webhook` | `aws-messaging-webhook` | The Lambda: routing, allowlist, persistence, lifecycle actions, publishing |
| `crates/sns-message-verifier` | `sns-message-verifier` | Standalone SNS signature verification (v1 and v2), no AWS SDK dependency |

## Ingress pathways

1. **Function URL (HTTPS)** - Axum router at `/webhooks/{family}` paths; `lambda_http::Adapter` bridges Lambda events to Axum
2. **Direct SNS invoke** - `entry.rs` detects the `Records` array shape and dispatches directly; no path, no confirmation handshake

## Request flow through the webhook crate

1. `entry.rs` - Dispatches by payload shape: `Records` with `eventSource: aws:dynamodb` = DynamoDB stream (the publish relay); other top-level `Records` = direct SNS event; otherwise = Function URL request served through Axum
2. `app.rs` - Routes four `/webhooks/...` paths; path does NOT determine event family
3. `sns/extractor.rs` (`VerifiedSns`) - Security boundary: enforces topic allowlist + signature verification; single constructor used by both ingress paths
4. `model/` - `DomainEvent::classify` does shape-based try-parse, most specific first (SesInbound before Ses - ordering is load-bearing and pinned by fixture matrix)
5. `sns/mod.rs` - Per-message state machine: auto-confirms subscriptions, auto-re-subscribes on abuse, runs the request-path pipeline (persist + inline actions)
6. `store.rs` - DynamoDB persistence with conditional writes for idempotency; `PersistOutcome` (Fresh/Duplicate) tells the request path whether the aggregate was applied
7. `actions/` - Inline lifecycle calls (delivery feedback, opt-outs, suppression), run on both Fresh and Duplicate (idempotent)
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

Cross-account consumers read the store through an assumable role, not raw table grants. When `ConsumerAccountIds` is set at deploy time, `template.yaml` creates `<stack-name>-consumer-read` (`ConsumerReadRoleArn` output) scoped to `GetItem` / `BatchGetItem` / `Query` on the events table only - no writes, no `Scan`, no index access. Consumers use it to fetch a message's current state (`GetItem` on `pk = MSG#<messageId>`, `sk = AGG`) or full timeline (`Query` on `pk`), typically after receiving a `payloadOmitted` EventBridge detail. Empty `ConsumerAccountIds` (the default) creates no role. This is a SAM-template capability - no Rust code is involved.

## Services trait

`state.rs` defines the `Services` trait aggregating `EventStore + PublishEvents + SmsVoiceApi + SesApi`. All downstream calls go through this trait so handler tests stay AWS-free using `FakeServices`.
