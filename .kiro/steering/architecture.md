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

1. `entry.rs` - Dispatches by payload shape: top-level `Records` array = direct SNS event; otherwise = Function URL request served through Axum
2. `app.rs` - Routes four `/webhooks/...` paths; path does NOT determine event family
3. `sns/extractor.rs` (`VerifiedSns`) - Security boundary: enforces topic allowlist + signature verification; single constructor used by both ingress paths
4. `model/` - `DomainEvent::classify` does shape-based try-parse, most specific first (SesInbound before Ses - ordering is load-bearing and pinned by fixture matrix)
5. `sns/mod.rs` - Per-message state machine: auto-confirms subscriptions, auto-re-subscribes on abuse, runs notification pipeline
6. `store.rs` - DynamoDB persistence with conditional writes for idempotency; `PersistOutcome` (Fresh/DuplicatePersisted/DuplicatePublished) determines next steps
7. `actions/` - Inline lifecycle calls (delivery feedback, opt-outs, suppression)
8. `publish.rs` + `build_outbound` - EventBridge emission with size capping

## Critical invariants

- **Response codes are the retry protocol.** 4xx = permanent rejection; 5xx = deliberate, recruiting SNS redelivery for transient failures. `error.rs` is the single place that classifies failures.
- **Never parse direct-invoke `Sns` records with `aws_lambda_events`' `SnsMessage`.** It parses `Timestamp` into `chrono::DateTime` which drops trailing `.000` subseconds, breaking signature verification. Use `SnsEnvelope` which keeps signed values verbatim.
- **No verification bypass in release builds.** `SNS_CERT_HOST_OVERRIDE` is `#[cfg(debug_assertions)]` only.
- **Persist-before-act-before-publish ordering** is load-bearing: a retry resumes exactly where it died without duplicate bus events.
- **`ActionErrorKind` splits transient from permanent.** Transient = 5xx/retry; permanent = log + metric, still publish. A misconfigured opt-out list must not become a retry storm.
- **HTTP clients never follow redirects** - part of the trust model for cert URL validation.

## DynamoDB data model

Single table with two item types sharing the same partition key:
- **Event items** - `pk = MSG#<messageId>`, `sk = EVT#<timestamp>#<snsMessageId>`: raw body, parse metadata, status (PERSISTED/PUBLISHED), TTL
- **Aggregate item** - same `pk`, `sk = AGG`: current_status, first/last_event_at, open_count, click_count, bounce_type

## Services trait

`state.rs` defines the `Services` trait aggregating `EventStore + PublishEvents + SmsVoiceApi + SesApi`. All downstream calls go through this trait so handler tests stay AWS-free using `FakeServices`.
