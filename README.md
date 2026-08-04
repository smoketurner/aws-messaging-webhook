# aws-messaging-webhook

One Rust Lambda function (Axum behind a Lambda Function URL) that receives AWS messaging events
delivered over SNS — End User Messaging two-way SMS and delivery receipts, SES sending events,
and SES inbound email notifications — then:

1. **Verifies** the SNS message signature (`SignatureVersion` 1 and 2) and enforces a topic
   allowlist. Together these are the security boundary for the public URL.
2. **Auto-confirms** SNS subscriptions from allowlisted topics, and auto-re-subscribes if an
   unauthenticated `UnsubscribeURL` is abused (`AUTO_RESUBSCRIBE=false` disables).
3. **Persists** every event to DynamoDB as an event-sourced store: one item per event keyed by
   the originating message, plus a per-message aggregate (delivery status, open/click counts).
   The conditional write doubles as idempotency — SNS redeliveries never double-process.
4. **Acts** on lifecycle events inline: delivery receipts → `PutMessageFeedback`; STOP/START
   keywords → `PutOptedOutNumber`/`DeleteOptedOutNumber`; hard bounces and complaints →
   `PutSuppressedDestination`. AWS-native lists stay the source of truth.
5. **Re-publishes** normalized events to a custom EventBridge bus for downstream applications.

## Architecture

```
EUM two-way SMS ──► SNS ─┐
EUM config set (DLR) ──► SNS ─┤   POST /webhooks/…      ┌─► DynamoDB (events + aggregates)
SES config set ──► SNS ─┼──► Lambda Function URL ──┼─► lifecycle actions (EUM/SES APIs)
SES receipt rule ──► SNS ─┘   verify → persist →    └─► EventBridge bus ─► your apps
                              act → publish
```

Each webhook path expects one event family, so wire each SNS topic to its own path:

| Path | Subscribe this topic |
|---|---|
| `/webhooks/sms/inbound` | EUM two-way SMS inbound topic |
| `/webhooks/sms/events` | EUM configuration-set event destination topic (delivery receipts) |
| `/webhooks/ses/events` | SES configuration-set event destination topic (bounce/complaint/…) |
| `/webhooks/ses/inbound` | SES receipt-rule SNS topic (inbound email notifications) |

## Deploy

Prerequisites: Rust (see `rust-toolchain.toml`), [`cargo-lambda`](https://cargo-lambda.info),
AWS SAM CLI.

```bash
sam build --config-env dev
sam deploy --config-env dev --parameter-overrides \
  "Stage=dev AllowedTopics=<your-account-id> OptOutListName=<your-opt-out-list>"
scripts/subscribe.sh aws-messaging-webhook-dev ses/events <topic-arn>
```

### Deployment contract

- **`AllowedTopics` is load-bearing security.** Signature verification proves a message came
  from SNS — from *any* AWS account. The allowlist (12-digit account ids and/or TopicArn globs,
  comma-separated) is what stops strangers from subscribing your public endpoint to their
  topics. Empty = accept everything = development only.
- **Raw message delivery must stay disabled** on subscriptions (the default). Raw delivery
  strips the signed JSON envelope, and the webhook rejects the request.
- **`SignatureVersion: 2`** (SHA256) is recommended per topic; `subscribe.sh` sets it. Version 1
  (the SNS default) is also supported.
- **SES inbound email**: use the receipt rule **S3 action** to store message content, with the
  SNS notification carrying the pointer. Full content over SNS is size-limited and discouraged;
  oversized inbound payloads have their embedded content stripped from the EventBridge event
  (the DynamoDB raw record keeps whatever SNS delivered).
- **SMS opt-out handling** fires only with self-managed opt-outs enabled on your numbers
  (AWS-managed opt-outs intercept STOP before SNS ever sees it) and requires `OptOutListName`.
- SNS topics and subscriptions deliberately live *outside* this stack, next to your EUM/SES
  configuration; `scripts/subscribe.sh` bridges the two after deploy.

## EventBridge contract

Events publish to the `<stack-name>-events` bus with `source` = `EventSource` parameter
(default `aws-messaging-webhook`) and these detail-types:

`sms.inbound`, `sms.delivery`, `ses.bounce`, `ses.complaint`, `ses.delivery`, `ses.send`,
`ses.reject`, `ses.open`, `ses.click`, `ses.rendering-failure`, `ses.delivery-delay`,
`ses.subscription`, `ses.inbound`, `ses.inbound.quarantined` (spam/virus verdict FAIL —
classification only, nothing dropped), `subscription.changed` (auto-re-subscribe fired),
`unknown` (unrecognized payload, forwarded verbatim).

Detail shape:

```json
{
  "meta": {
    "snsMessageId": "…",
    "messageId": "…",        // aggregate id: query DynamoDB with pk = MSG#<messageId>
    "topicArn": "…",
    "receivedAt": "…",
    "webhookPath": "/webhooks/ses/events"
  },
  "event": { /* the inner AWS payload, verbatim */ }
}
```

## Data model

One DynamoDB table (`TableName` output):

- **Event items** — `pk = MSG#<messageId>`, `sk = EVT#<timestamp>#<snsMessageId>`: the exact
  raw body received, parse metadata, `status` (`PERSISTED`/`PUBLISHED`), TTL via `expires_at`
  (`RawEventRetentionDays`, default 30).
- **Aggregate item** — same `pk`, `sk = AGG`: `current_status`, `first/last_event_at`,
  `open_count`, `last_opened_at`, `click_count`, `last_clicked_at`, `bounce_type`.

A message's full timeline is one `Query` on `pk`; its current state is one `GetItem` on
`pk` + `AGG`.

## Operations

- Structured JSON logs; one INFO line per message with `outcome`
  (`published|duplicate|resumed|confirmed|resubscribed`) and `action`.
- CloudWatch metrics (namespace = stack name) via log metric filters: `MessagesReceived`,
  `SignatureRejections`, `AllowlistRejections`, `Duplicates`, `EventsPublished`,
  `InternalErrors`, `ActionFailures`, `Resubscribes`.
- Transient downstream failures return 5xx on purpose: SNS redelivers, and the store's
  `PERSISTED`/`PUBLISHED` status makes the retry resume exactly where it died. Consumers must
  tolerate rare duplicate bus events (SNS is at-least-once end to end).
- End-to-end check: `scripts/e2e-ses-bounce.sh <stack> <verified-sender>` drives the SES
  mailbox simulator through the full pipeline.

## Development

```bash
cargo test --workspace                 # unit + handler tests (no AWS needed)
cargo clippy --all-targets --all-features -- -D warnings
prek run                               # fmt, clippy, deny, actionlint, zizmor
```

Local run: `cargo lambda watch`, then POST signed fixtures. Debug builds honor
`SNS_CERT_HOST_OVERRIDE` to trust a local fake SNS certificate server; release builds have no
bypass.

## License

MIT or Apache-2.0, at your option.
