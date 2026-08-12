# aws-messaging-webhook

[![CI](https://img.shields.io/github/actions/workflow/status/smoketurner/aws-messaging-webhook/ci.yml?branch=main)](https://github.com/smoketurner/aws-messaging-webhook/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.97.1-blue)](rust-toolchain.toml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](#license)

One Rust Lambda function that receives AWS messaging events delivered over SNS — End User
Messaging two-way SMS and delivery receipts, SES sending events, and SES inbound email
notifications — over either pathway: HTTPS to a Lambda Function URL (Axum), or direct
SNS → Lambda invocation. Then it:

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

- [Architecture](#architecture)
- [Deploy](#deploy)
- [EventBridge contract](#eventbridge-contract)
- [Data model](#data-model)
- [Operations](#operations)
- [Development](#development)
- [License](#license)

## Architecture

```
EUM two-way SMS ──────► SNS ─┐  POST /webhooks/… (https)   ┌─► DynamoDB (events + aggregates)
EUM config set (DLR) ─► SNS ─┤  ─► Lambda Function URL ─┐  │        │ stream (NEW_IMAGE)
SES config set ───────► SNS ─┤  or direct invoke        ├──┤        ▼
SES receipt rule ─────► SNS ─┘  (lambda protocol) ──────┘  │   stream relay ─► EventBridge bus ─► your apps
                                verify → persist → act     └─► lifecycle actions (EUM/SES APIs)
```

The request path is a durable outbox writer: it verifies, persists the event item (the outbox
entry), and runs inline lifecycle actions. A DynamoDB Streams consumer in the same function is
the **sole publisher** — it reads each newly-persisted event and emits it to EventBridge, with
the stream event-source mapping's retries and on-failure DLQ guaranteeing delivery.

The event family is classified from each payload's shape, so no routing configuration exists.
Wire each SNS topic to its matching path anyway — a topic delivering a different family than
its path logs a `family_mismatch` warning (the event still processes correctly). Direct
SNS → Lambda subscriptions carry no path and need no substitute for one:

| Path | Subscribe this topic |
|---|---|
| `/webhooks/sms/inbound` | EUM two-way SMS inbound topic |
| `/webhooks/sms/events` | EUM configuration-set event destination topic (delivery receipts) |
| `/webhooks/ses/events` | SES configuration-set event destination topic (bounce/complaint/…) |
| `/webhooks/ses/inbound` | SES receipt-rule SNS topic (inbound email notifications) |

### Workspace

| Crate | Purpose |
|---|---|
| `crates/webhook` (`aws-messaging-webhook`) | The Lambda: routing, allowlist, persistence, lifecycle actions, publishing |
| `crates/sns-message-verifier` | Standalone SNS signature verification (versions 1 and 2) with no AWS SDK dependency; its `test-fixtures` feature generates throwaway keys and certs so consumers can sign test envelopes |

## Deploy

Prerequisites: Rust (see `rust-toolchain.toml`), [`cargo-lambda`](https://cargo-lambda.info),
AWS SAM CLI.

```bash
git clone https://github.com/smoketurner/aws-messaging-webhook
cd aws-messaging-webhook
sam build --config-env dev
sam deploy --config-env dev --parameter-overrides \
  "Stage=dev AllowedTopics=<your-account-id> OptOutListName=<your-opt-out-list>"
```

> [!IMPORTANT]
> **`AllowedTopics` is load-bearing security.** Signature verification proves a message came
> from SNS — from *any* AWS account. The allowlist (12-digit account ids and/or TopicArn globs,
> comma-separated) is what stops strangers from subscribing your public endpoint to their
> topics. Empty = accept everything = development only.

> [!WARNING]
> **Raw message delivery must stay disabled** on subscriptions (the default). Raw delivery
> strips the signed JSON envelope, and the webhook rejects the request. This applies to both
> pathways.

### Wire up topics

Topics and subscriptions live outside the stack, next to your EUM/SES configuration. To wire
a topic to the Function URL (the HTTPS pathway), subscribe it to the matching webhook path:

```bash
webhook_url=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookUrl'].OutputValue" --output text)
aws sns set-topic-attributes --topic-arn <topic-arn> \
  --attribute-name SignatureVersion --attribute-value 2
aws sns subscribe --topic-arn <topic-arn> --protocol https \
  --notification-endpoint "${webhook_url%/}/webhooks/<path>"
```

The function auto-confirms the subscription: `PendingConfirmation` on the new subscription
flips to `false` within seconds. If it stays pending, check the function logs — is the topic
allowlisted? Is raw message delivery disabled? `SignatureVersion` 2 (SHA256) is recommended;
version 1, the SNS default, also verifies.

### Direct SNS → Lambda (optional)

Topics can also invoke the function directly instead of POSTing to the Function URL. The same
signature verification and allowlist apply; there is no confirmation handshake (Lambda
subscriptions confirm via IAM) and no routing to configure — the event family comes from the
payload shape. Grant the topic permission to invoke the function (keep the `--source-arn`
scope: it is the per-topic gate on this pathway), then subscribe:

```bash
function_arn=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookFunctionArn'].OutputValue" --output text)
aws lambda add-permission --function-name "${function_arn}" \
  --statement-id "sns-<topic-name>" --action lambda:InvokeFunction \
  --principal sns.amazonaws.com --source-arn <topic-arn>
aws sns subscribe --topic-arn <topic-arn> --protocol lambda \
  --notification-endpoint "${function_arn}"
```

The retry behavior differs: SNS hands direct deliveries to Lambda's async-invoke queue, which
retries a failed invocation **twice** and then drops it, while the HTTPS delivery policy
retries far longer. If you need durability past two retries, configure an on-failure
destination (SQS) on the function.

### Parameters

| Parameter | Default | Notes |
|---|---|---|
| `Stage` | `dev` | `dev` or `prod`; `prod` enables DynamoDB deletion protection |
| `AllowedTopics` | *(empty)* | Comma-separated account ids and/or TopicArn globs — see above |
| `AutoResubscribe` | `true` | Re-subscribe when an unauthenticated `UnsubscribeURL` is abused |
| `OptOutListName` | *(empty)* | EUM opt-out list updated by STOP/START keywords; empty disables that action |
| `EventSource` | `aws-messaging-webhook` | `source` field on published EventBridge events |
| `RawEventRetentionDays` | `30` | DynamoDB TTL for raw event items |
| `AggregateRetentionDays` | `365` | DynamoDB TTL for the per-message aggregate item; kept longer than raw events so current state outlives them |
| `LogLevel` | `INFO` | `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| `LogRetentionDays` | `30` | CloudWatch log retention |
| `ConsumerAccountIds` | *(empty)* | Comma-separated 12-digit account ids allowed to assume the read-only consumer role (see [Consumer read access](#consumer-read-access)); empty grants none |

### Deployment contract

- **`SignatureVersion: 2`** (SHA256) is recommended per topic (the wiring commands above set
  it). Version 1 (the SNS default) is also supported.
- **SES inbound email**: use the receipt rule **S3 action** to store message content, with the
  SNS notification carrying the pointer. Full content over SNS is size-limited and discouraged;
  oversized inbound payloads have their embedded content stripped from the EventBridge event
  (the DynamoDB raw record keeps whatever SNS delivered).
- **SMS opt-out handling** fires only with self-managed opt-outs enabled on your numbers
  (AWS-managed opt-outs intercept STOP before SNS ever sees it) and requires `OptOutListName`.
- SNS topics and subscriptions deliberately live *outside* this stack, next to your EUM/SES
  configuration; the wiring commands above bridge the two after deploy.

## EventBridge contract

Events publish to the `<stack-name>-events` bus with `source` = `EventSource` parameter
(default `aws-messaging-webhook`) and these detail-types:

`sms.inbound`, `sms.delivery`, `mms.delivery`, `voice.delivery`, `ses.bounce`, `ses.complaint`, `ses.delivery`, `ses.send`,
`ses.reject`, `ses.open`, `ses.click`, `ses.rendering-failure`, `ses.delivery-delay`,
`ses.subscription`, `ses.inbound`, `ses.inbound.quarantined` (spam/virus verdict FAIL —
classification only, nothing dropped), `ses.unknown` (a valid SES event of a kind this version
doesn't map yet), `message.status.changed` (the per-message aggregate's `current_status`
transitioned), `subscription.changed` (auto-re-subscribe fired), `unknown` (unparseable
payload, forwarded verbatim).

An event whose payload exceeds the EventBridge 256 KB entry limit is published with its `event`
replaced by `{ "payloadOmitted": true, ... }`; `meta` is always preserved, so consumers fetch the
full record from DynamoDB by `meta.messageId`. (SES inbound raw MIME is dropped first; this
pointer form is the fallback.)

Detail shape:

```json
{
  "schemaVersion": 1,       // detail contract version; bumped only on a breaking shape change
  "meta": {
    "snsMessageId": "…",
    "messageId": "…",        // aggregate id: query DynamoDB with pk = MSG#<messageId>
    "previousMessageId": "…", // present only on sms.inbound replies; the outbound message this reply answers
    "topicArn": "…",
    "receivedAt": "…",
    "webhookPath": "/webhooks/ses/events"
  },
  "event": { /* the inner AWS payload, verbatim */ }
}
```

`previousMessageId` is present only on `sms.inbound` events where the inbound message is a
reply to a previously sent outbound message (i.e. the EUM payload carries a
`previousPublishedMessageId`). Consumers can use it to correlate a reply with the sent message
that triggered it without parsing the event payload. It is absent (not null) on unsolicited
inbound contacts and on all other event families.

`schemaVersion` is present on every published detail, including the `subscription.changed`
event, so consumers have a stable field to switch on as the contract evolves.

The stream relay also emits `message.status.changed` when a message's aggregate `current_status`
transitions (e.g. `sent` → `delivered` → `bounced`) — not on count-only bumps like opens/clicks —
so consumers can track the authoritative rolled-up status without re-deriving precedence:

```json
{
  "schemaVersion": 1,
  "meta": { "messageId": "…", "webhookPath": "/webhooks/ses/events" },
  "status": {
    "current": "delivered",   // bounced | complained | failed | received | sent | …
    "bounceType": "…",         // present on a bounce
    "firstEventAt": "…", "lastEventAt": "…",
    "openCount": 0, "clickCount": 0,
    "botOpenCount": 0, "botClickCount": 0
  }
}
```

Open and click counts are split by SES's `isBotEvent` signal: an interaction SES
flags `Likely` bot-generated (Apple Mail Privacy Protection prefetch, security
scanners) accrues to `botOpenCount` / `botClickCount`, and everything else —
including events that predate the feature and carry no `isBotEvent` — accrues to
the human `openCount` / `clickCount`. Consumers wanting the raw signal per event
read `detail.event.open.isBotEvent` (or `.click.isBotEvent`) off the `ses.open` /
`ses.click` detail, which is forwarded verbatim.

## Data model

One DynamoDB table (`TableName` output):

- **Event items** — `pk = MSG#<messageId>`, `sk = EVT#<timestamp>#<snsMessageId>`: the exact
  raw body received, parse metadata, TTL via `expires_at` (`RawEventRetentionDays`, default 30).
  The insert of each event item is what the stream relay turns into an EventBridge publish.
- **Aggregate item** — same `pk`, `sk = AGG`: `current_status`, `first/last_event_at`,
  `open_count`, `last_opened_at`, `click_count`, `last_clicked_at`, `bot_open_count`,
  `bot_click_count` (opens/clicks SES flags `isBotEvent=Likely`), `bounce_type`. Its TTL
  (`AggregateRetentionDays`, default 365) is kept longer than the raw events' so the rolled-up
  current state outlives them.

A message's full timeline is one `Query` on `pk`; its current state is one `GetItem` on
`pk` + `AGG`.

### Consumer read access

When an EventBridge detail is published with `payloadOmitted` (over the 256 KB entry limit),
or a consumer wants a message's full timeline, it fetches directly from DynamoDB. Cross-account
consumers get read access through a role, not raw table grants: set `ConsumerAccountIds` to the
12-digit account ids at deploy time and the stack creates `<stack-name>-consumer-read`
(`ConsumerReadRoleArn` output), a role those accounts may assume. It allows `GetItem` /
`BatchGetItem` / `Query` on the table only — no writes, no `Scan`, and no access to internal
indexes. A consumer assumes the role, then:

- current state: `GetItem` on `pk = MSG#<messageId>`, `sk = AGG`
- full timeline: `Query` on `pk = MSG#<messageId>`

`meta.messageId` on every published detail is the `<messageId>`. Empty `ConsumerAccountIds`
(the default) creates no role and grants no cross-account access.

## Operations

- Structured JSON logs; one INFO line per message. The request path logs an `outcome`
  (`persisted|duplicate|confirmed|resubscribed`) and `action`; the stream relay logs
  `outcome=published` per event emitted to EventBridge.
- CloudWatch metrics (namespace = stack name) emitted inline via CloudWatch Embedded Metrics
  Format (EMF): `MessagesReceived` (request-path deliveries: persisted + duplicate),
  `SignatureRejections`, `AllowlistRejections`, `UnclassifiedPayloads` (events forwarded as
  `unknown` — a sustained rate means a new AWS event shape or junk on a topic), `Duplicates`,
  `EventsPublished` (from the stream relay), `PublishFailures` (a relay publish that will be
  retried), `InternalErrors`, `ActionFailures`, `Resubscribes`, `SubscriptionsLost` (alarm on
  this — a subscription was cancelled and, with `AutoResubscribe=false`, not re-attached),
  `ColdStart` (Count = 1 on the first invocation of a new execution environment), and
  `Latency` (histogram, milliseconds per invocation — CloudWatch derives p50/p90/p99). All
  metrics carry a `function` dimension (the Lambda function name). Also alarm on the native
  Lambda stream `IteratorAge` and the `PublishDlq` queue depth (`PublishDlqUrl` output): a
  non-empty DLQ means events exhausted their publish retries.
- The request path and the stream relay have independent durability. A transient failure in the
  request path (persist or a lifecycle action) returns 5xx so SNS redelivers; the conditional
  write dedupes the redelivery and re-runs only the idempotent actions. Publishing is decoupled:
  the event item is the outbox entry, and the stream event-source mapping retries the publish
  (bisecting poison batches, reporting per-record failures) and routes anything past the retry
  limit to the DLQ — so a publish can never be silently lost. Consumers must tolerate rare
  duplicate bus events (at-least-once end to end).
- End-to-end check on a deployed stack: send a probe through the SES mailbox simulator —
  `aws sesv2 send-email --from-email-address <verified-sender> --destination
  ToAddresses=bounce@simulator.amazonses.com --content
  "Simple={Subject={Data=probe},Body={Text={Data=probe}}}"` — then confirm a `ses.bounce`
  event arrives on the bus (temporary rule → SQS, or CloudWatch), an event item is written to
  DynamoDB under `pk = MSG#<messageId>`, and the simulator address lands on the SES account
  suppression list.

## Development

```bash
cargo test --workspace                 # unit + handler tests (no AWS needed)
cargo clippy --all-targets --all-features -- -D warnings
prek run                               # fmt, clippy, deny, actionlint, zizmor
```

The handler tests drive the real router end to end with properly signed SNS envelopes (the
verifier crate's `test-fixtures` feature generates throwaway keys and certificates), so no AWS
account is needed for development. For a deployed end-to-end check see the SES-simulator probe
under Observability.

> [!NOTE]
> Debug builds honor `SNS_CERT_HOST_OVERRIDE` for running against a local fake SNS under
> `cargo lambda watch`; release builds have no bypass.

## License

Licensed under either of the [Apache License 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
