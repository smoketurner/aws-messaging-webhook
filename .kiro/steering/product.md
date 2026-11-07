---
description: Product overview, purpose, and high-level goals of the aws-messaging-webhook project
inclusion: auto
---

# Product

## What it does

A single Rust binary, deployed as two AWS Lambda functions (a webhook receiver and a mail sender), that receives AWS messaging events delivered over SNS and processes them through a verify-persist-act-publish pipeline. The webhook function supports two ingress pathways: HTTPS POSTs to a Lambda Function URL (Axum router) and direct SNS-to-Lambda invocations. When a mail domain is configured, it also runs a mailbox-on-SES feature: inbound email is ingested into a mail table and served through the bearer-authenticated `/v0` mailbox API, and the mail sender function sends and replies.

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
- Testability: the `Backend` trait + `AppState<B>` seam (one component type per external system) allows full handler tests with properly signed envelopes against in-process fakes (`webhook_test_support::Fakes`), no AWS account needed
