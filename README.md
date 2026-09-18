# aws-messaging-webhook

[![CI](https://img.shields.io/github/actions/workflow/status/smoketurner/aws-messaging-webhook/ci.yml?branch=main)](https://github.com/smoketurner/aws-messaging-webhook/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.98.0-blue)](rust-toolchain.toml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](#license)

One Rust Lambda receives AWS messaging events over SNS: End User Messaging two-way SMS and
delivery receipts, SES sending events, and SES inbound mail. Topics reach it over HTTPS to a
Lambda Function URL, or by invoking it directly. Every event then:

1. **Verifies.** The SNS signature (`SignatureVersion` 1 and 2) and a topic allowlist. These
   two are the security boundary for the public URL.
2. **Confirms.** Subscriptions from allowlisted topics confirm themselves, and re-subscribe if
   an unauthenticated `UnsubscribeURL` is abused (`pAutoResubscribe=false` disables this).
3. **Persists.** One DynamoDB item per event, keyed by the originating message, plus a
   per-message aggregate holding delivery status and open/click counts. The conditional write
   is the idempotency: an SNS redelivery never double-processes.
4. **Acts.** Delivery receipts call `PutMessageFeedback`. STOP/START keywords call
   `PutOptedOutNumber`/`DeleteOptedOutNumber`. Hard bounces and complaints call
   `PutSuppressedDestination`. AWS-native lists stay the source of truth.
5. **Publishes.** Normalized events go to a custom EventBridge bus.

- [Architecture](#architecture)
- [Deploy](#deploy)
- [Mailbox](#mailbox)
- [EventBridge contract](#eventbridge-contract)
- [Data model](#data-model)
- [Operations](#operations)
- [Development](#development)
- [License](#license)

## Architecture

```
EUM two-way SMS ──────► SNS ─┐  POST /webhooks/… (https)   ┌─► DynamoDB (events + aggregates)
EUM config set (DLR) ─► SNS ─┤  ─► Lambda Function URL ─┐  │        │ stream (NEW_AND_OLD_IMAGES)
SES config set ───────► SNS ─┤  or direct invoke        ├──┤        ▼
SES receipt rule ─────► SNS ─┘  (lambda protocol) ──────┘  │   stream relay ─► EventBridge bus ─► your apps
                                verify → persist → act     └─► lifecycle actions (EUM/SES APIs)
```

The request path is a durable outbox writer: verify, persist the event item, run the inline
lifecycle actions. A DynamoDB Streams consumer in the same function publishes every event
detail, and the stream mapping's retries and on-failure DLQ make that delivery durable. The
request path publishes one event of its own, `subscription.changed`, which has no event item
behind it.

Each payload's shape decides its event family, so there is no routing to configure. Wire each
topic to its matching path anyway: a mismatch logs `family_mismatch` and still processes the
event correctly. Direct SNS → Lambda subscriptions carry no path and need none.

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
sam build
sam deploy --parameter-overrides \
  "pAllowedTopics=<your-account-id> pOptOutListName=<your-opt-out-list>"
```

> [!IMPORTANT]
> **`pAllowedTopics` is security, not configuration.** A valid signature proves a message came
> from SNS in *some* AWS account. The allowlist — comma-separated 12-digit account ids and/or
> TopicArn globs — is what stops a stranger subscribing your public endpoint to their topic.
> Empty accepts everything, which is for development only.

> [!WARNING]
> **Raw message delivery must stay disabled** on subscriptions (the default). Raw delivery
> strips the signed JSON envelope, and the webhook rejects the request. This applies to both
> pathways.

### Wire up topics

Topics and subscriptions live outside the stack, next to your EUM/SES configuration. A mailbox
stack owns two of its own — see [README_MAILBOX.md](README_MAILBOX.md). To wire a topic to the
HTTPS pathway, subscribe it to the matching endpoint. The stack outputs each one as a URL:

| Output | Subscribe this topic |
|---|---|
| `SmsInboundEndpoint` | EUM two-way SMS inbound topic |
| `SmsEventsEndpoint` | EUM configuration-set event destination topic (delivery receipts) |
| `SesEventsEndpoint` | SES configuration-set event destination topic (bounce/complaint/…) |
| `SesInboundEndpoint` | SES receipt-rule SNS topic (inbound email notifications) |

```bash
endpoint=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='SesEventsEndpoint'].OutputValue" --output text)
aws sns set-topic-attributes --topic-arn <topic-arn> \
  --attribute-name SignatureVersion --attribute-value 2
aws sns subscribe --topic-arn <topic-arn> --protocol https \
  --notification-endpoint "$endpoint"
```

`PendingConfirmation` flips to `false` within seconds. If it stays pending, read the function
logs: the topic is usually not allowlisted, or raw message delivery is on.

### Direct SNS → Lambda (optional)

A topic can invoke the function directly instead of POSTing to the Function URL. Signature
verification and the allowlist apply the same way. There is no confirmation handshake, since
Lambda subscriptions confirm through IAM. Grant the topic permission to invoke the function,
then subscribe. Keep `--source-arn` scoped: it is the per-topic gate on this pathway.

```bash
function_arn=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookFunctionArn'].OutputValue" --output text)
aws lambda add-permission --function-name "${function_arn}" \
  --statement-id "sns-<topic-name>" --action lambda:InvokeFunction \
  --principal sns.amazonaws.com --source-arn <topic-arn>
aws sns subscribe --topic-arn <topic-arn> --protocol lambda \
  --notification-endpoint "${function_arn}"
```

Retries differ between the pathways. Lambda's async-invoke queue retries a direct delivery
**twice**; the HTTPS delivery policy retries far longer. Anything past those two retries lands
in `rAsyncInvokeDlq` (`AsyncInvokeDlqUrl` output) with the original event, so nothing is lost.
Alarm on its depth and replay from it — see [Operations](#operations). Do not add your own
on-failure destination: a function has one, and a second replaces the stack's.

### Parameters

| Parameter | Default | Notes |
|---|---|---|
| `pStage` | `dev` | `dev` or `prod`; `prod` enables DynamoDB deletion protection |
| `pAllowedTopics` | *(empty)* | Comma-separated account ids and/or TopicArn globs — see above |
| `pAutoResubscribe` | `true` | Re-subscribe when an unauthenticated `UnsubscribeURL` is abused |
| `pOptOutListName` | *(empty)* | EUM opt-out list updated by STOP/START keywords; empty disables that action |
| `pEventSource` | `aws-messaging-webhook` | `source` field on published EventBridge events |
| `pRawEventRetentionDays` | `30` | DynamoDB TTL for raw event items |
| `pAggregateRetentionDays` | `365` | DynamoDB TTL for the per-message aggregate item; kept longer than raw events so current state outlives them |
| `pLogLevel` | `INFO` | `DEBUG`/`INFO`/`WARN`/`ERROR`; `TRACE` is refused, since the runtime and the AWS SDK log raw payloads at that level |
| `pLogRetentionDays` | `30` | CloudWatch log retention |
| `pConsumerAccountIds` | *(empty)* | Comma-separated 12-digit account ids allowed to assume the read-only consumer role (see [Consumer read access](#consumer-read-access)); empty grants none |

### Deployment contract

- **Signatures.** `SignatureVersion: 2` (SHA256) per topic, which the commands above set.
  Version 1, the SNS default, also verifies.
- **SES inbound.** Use the receipt rule's S3 action and let the SNS notification carry the
  pointer. Content delivered over SNS is size-limited; an oversized payload loses its embedded
  content in the EventBridge event, though DynamoDB keeps whatever SNS delivered.
- **SMS opt-outs.** STOP/START handling needs self-managed opt-outs on your numbers and
  `pOptOutListName`. AWS-managed opt-outs intercept STOP before SNS sees it.
- **Topics live outside this stack**, next to your EUM/SES configuration. The exception is the
  two topics a mailbox stack owns (see [README_MAILBOX.md](README_MAILBOX.md)).

## Mailbox

Setting `pMailDomain` turns the stack into a mailbox on SES. Inbound mail lands in DynamoDB and
S3, a `/v0` API reads and sends it, and mailbox events publish to the same bus. Empty is the
default: no mail resources, and the function stays at a 10 s timeout and 256 MB.

[README_MAILBOX.md](README_MAILBOX.md) has its parameters, post-deploy steps, API, sending
contract, storage layout, runbook and events.

## EventBridge contract

Events publish to the `<stack-name>-events` bus with `source` = `pEventSource` (default
`aws-messaging-webhook`).

| Detail-type | Fires on |
|---|---|
| `sms.inbound`, `sms.delivery`, `mms.delivery`, `voice.delivery` | End User Messaging |
| `ses.send`, `ses.delivery`, `ses.bounce`, `ses.complaint`, `ses.reject`, `ses.open`, `ses.click`, `ses.rendering-failure`, `ses.delivery-delay`, `ses.subscription` | SES sending events |
| `ses.inbound` | An inbound receipt |
| `ses.inbound.quarantined` | The same, spam or virus verdict `FAIL`. Classification only — nothing is dropped |
| `ses.unknown` | A valid SES event this version doesn't map yet |
| `message.status.changed` | A per-message aggregate's `current_status` transitioned |
| `subscription.changed` | Auto-re-subscribe fired |
| `unknown` | An unparseable payload, forwarded verbatim |

An event over EventBridge's 256 KB entry limit publishes with its `event` replaced by
`{ "payloadOmitted": true, … }`. `meta.messageId` always survives, so consumers fetch the full
record from DynamoDB. SES inbound raw MIME drops first; the pointer form is the fallback.

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
    "webhookPath": "/webhooks/ses/events",
    "s3": {                   // present only on ses.inbound receipts stored via the S3 action
      "bucket": "…", "key": "…"
    },
    "inbound": {              // present only on ses.inbound receipts, when SES supplied either piece
      "headers": { "from": ["…"], "to": ["…"], "subject": "…", "date": "…", "messageId": "<…>" },
      "auth": { "spf": "PASS", "dkim": "PASS", "dmarc": "FAIL", "spam": "PASS", "virus": "PASS", "dmarcPolicy": "reject" }
    }
  },
  "event": { /* the inner AWS payload, verbatim */ }
}
```

Three `meta` fields are conditional, and absent rather than null when they don't apply:

- **`previousMessageId`** appears on an `sms.inbound` event whose payload carries a
  `previousPublishedMessageId` — a reply to a message you sent. It correlates the two without
  parsing the payload.
- **`s3`** `{bucket, key}` appears on `ses.inbound` and `ses.inbound.quarantined` when the
  receipt rule used the S3 action. It survives the oversized-payload strip, so a consumer can
  still `GetObject` the message when `content` is gone.
- **`inbound`** appears on the same two, so consumers route without fetching from S3. It holds
  `headers` (SES's parsed `commonHeaders`) and `auth` (the `spf`/`dkim`/`dmarc`/`spam`/`virus`
  statuses and `dmarcPolicy`). The block is absent when the receipt carried neither.

`schemaVersion` is on every detail, including `subscription.changed`, so consumers have one
field to switch on as the contract evolves. These three additions are meta-only, so it stays 1.

The relay also emits `message.status.changed` when an aggregate's `current_status` transitions:
`sent` → `delivered` → `bounced`. Count-only bumps, such as opens and clicks, do not fire it.

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

SES's `isBotEvent` signal splits the counts. An interaction flagged `Likely` — Apple Mail
Privacy Protection prefetch, security scanners — accrues to `botOpenCount`/`botClickCount`.
Everything else, including older events carrying no `isBotEvent`, accrues to
`openCount`/`clickCount`. The raw per-event signal is on the forwarded `ses.open`/`ses.click`
detail at `detail.event.open.isBotEvent`.

### Mailbox events

A mailbox stack publishes `message.received`, `message.sent` and the delivery events on the same
bus. See [README_MAILBOX.md](README_MAILBOX.md#mailbox-events).

## Data model

One DynamoDB table (`TableName` output):

- **Event items** — `pk = MSG#<messageId>`, `sk = EVT#<timestamp>#<snsMessageId>`: the exact
  raw body received, parse metadata, TTL via `expires_at` (`pRawEventRetentionDays`, default 30).
  The insert of each event item is what the stream relay turns into an EventBridge publish.
- **Aggregate item** — same `pk`, `sk = AGG`: `current_status`, `first/last_event_at`,
  `open_count`, `last_opened_at`, `click_count`, `last_clicked_at`, `bot_open_count`,
  `bot_click_count` (opens/clicks SES flags `isBotEvent=Likely`), `bounce_type`. Its TTL
  (`pAggregateRetentionDays`, default 365) is kept longer than the raw events' so the rolled-up
  current state outlives them.

A message's full timeline is one `Query` on `pk`; its current state is one `GetItem` on
`pk` + `AGG`.

### Mail table

A mailbox stack adds a second table, described in
[README_MAILBOX.md](README_MAILBOX.md#mail-table).

### Consumer read access

A consumer reads DynamoDB directly when a detail arrives with `payloadOmitted`, or when it
wants a message's full timeline. Cross-account consumers get a role rather than table grants:
set `pConsumerAccountIds` to the 12-digit account ids and the stack creates a role for them,
published as `ConsumerReadRoleArn`. It allows `GetItem`, `BatchGetItem` and `Query` on the table
only — no writes, no `Scan`, no indexes. Assume the role, then:

- current state: `GetItem` on `pk = MSG#<messageId>`, `sk = AGG`
- full timeline: `Query` on `pk = MSG#<messageId>`

`meta.messageId` on every published detail is the `<messageId>`. Empty `pConsumerAccountIds`
(the default) creates no role and grants no cross-account access.

## Operations

- **Logs.** Structured JSON, one INFO line per message. The request path logs an `outcome`
  (`persisted|duplicate|confirmed|resubscribed`) and an `action`; the relay logs
  `outcome=published` per event.
- **Metrics.** EMF, in the stack-name namespace, each with a `function` dimension:
  `MessagesReceived` (persisted + duplicate), `SignatureRejections`, `AllowlistRejections`,
  `UnclassifiedPayloads`, `Duplicates`, `EventsPublished`, `PublishFailures`, `InternalErrors`,
  `ActionFailures`, `Resubscribes`, `SubscriptionsLost`, `ColdStart`, and `Latency` (a
  histogram in milliseconds, so CloudWatch derives p50/p90/p99).
- **What to alarm on.** `SubscriptionsLost` means a subscription was cancelled and, with
  `pAutoResubscribe=false`, not re-attached. A non-empty `rPublishDlq` (`PublishDlqUrl`) means
  events exhausted their publish retries. A sustained `UnclassifiedPayloads` rate means a new
  AWS event shape or junk on a topic. Also watch the native Lambda stream `IteratorAge`.
- **Durability.** The request path and the relay fail independently. A transient failure while
  persisting or acting returns 5xx, SNS redelivers, the conditional write dedupes it, and only
  the idempotent actions re-run. Publishing is decoupled: the event item is the outbox entry,
  and the stream mapping retries, bisects poison batches, reports per-record failures and routes
  the rest to the DLQ. Delivery is at-least-once end to end, so consumers tolerate rare
  duplicates.
- **End-to-end probe.** Send through the SES mailbox simulator, then confirm three things: a
  `ses.bounce` event on the bus, an event item under `pk = MSG#<messageId>`, and the simulator
  address on the SES account suppression list.

  ```bash
  aws sesv2 send-email --from-email-address <verified-sender> \
    --destination ToAddresses=bounce@simulator.amazonses.com \
    --content "Simple={Subject={Data=probe},Body={Text={Data=probe}}}"
  ```

## Development

```bash
cargo test --workspace                 # unit + handler tests (no AWS needed)
cargo clippy --all-targets --all-features -- -D warnings
prek run                               # fmt, clippy, deny, actionlint, zizmor
```

The handler tests drive the real router with properly signed SNS envelopes. The verifier
crate's `test-fixtures` feature generates throwaway keys and certificates, so development needs
no AWS account. For a deployed check, see the probe under [Operations](#operations).

> [!NOTE]
> Debug builds honor `SNS_CERT_HOST_OVERRIDE`, for running against a local fake SNS under
> `cargo lambda watch`. Release builds have no bypass.

## License

Licensed under either of the [Apache License 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
