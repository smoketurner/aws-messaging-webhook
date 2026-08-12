---
description: Product overview, purpose, and high-level goals of the aws-messaging-webhook project
inclusion: auto
---

# Product

## What it does

A single Rust AWS Lambda that receives AWS messaging events delivered over SNS and processes them through a verify-persist-act-publish pipeline. Supports two ingress pathways: HTTPS POSTs to a Lambda Function URL (Axum router), and direct SNS-to-Lambda invocations.

## Event sources

- End User Messaging (EUM) two-way SMS inbound
- EUM configuration-set event destinations (delivery receipts)
- SES configuration-set event destinations (bounce, complaint, delivery, open, click, etc.)
- SES receipt-rule SNS notifications (inbound email)

## Pipeline stages

1. **Verify** - SNS signature (v1 and v2) plus topic allowlist enforcement
2. **Persist** - Event-sourced DynamoDB store (event items + per-message aggregates); conditional write provides idempotency
3. **Act** - Inline lifecycle actions: delivery feedback, STOP/START opt-out management, bounce/complaint suppression
4. **Publish** - Normalized events to a custom EventBridge bus for downstream consumers

## Key design goals

- Security: public URL protected by signature verification AND topic allowlist (both are required)
- Reliability: 5xx responses recruit SNS redelivery for transient failures; the persisted event item is an outbox entry that a DynamoDB Streams relay publishes with its own retries and on-failure DLQ
- Observability: structured JSON logs, CloudWatch metrics via Embedded Metrics Format (EMF)
- Testability: trait-based services allow full handler tests with properly signed envelopes against fake implementations, no AWS account needed
