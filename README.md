# aws-messaging-webhook

[![CI](https://img.shields.io/github/actions/workflow/status/smoketurner/aws-messaging-webhook/ci.yml?branch=main)](https://github.com/smoketurner/aws-messaging-webhook/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.98.0-blue)](rust-toolchain.toml)
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
- [Mailbox](#mailbox)
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

Topics and subscriptions live outside the stack, next to your EUM/SES configuration. (The one
exception is the two mail topics the stack creates and subscribes itself when `MailDomain` is
set; see [Mailbox](#mailbox).) To wire
a topic to the Function URL (the HTTPS pathway), subscribe it to the matching webhook
endpoint. The stack outputs each endpoint as a ready-to-use URL:

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
| `LogLevel` | `INFO` | `DEBUG`/`INFO`/`WARN`/`ERROR` (no `TRACE`; see [Upgrading from `LogLevel=TRACE`](#upgrading-from-logleveltrace)) |
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
  configuration; the wiring commands above bridge the two after deploy. The exception is the
  two mail topics a mailbox stack owns (see [Mailbox](#mailbox)).

### Upgrading from `LogLevel=TRACE`

`TRACE` is no longer an allowed `LogLevel`: at that level the Lambda runtime logs raw
invocation payloads and the AWS SDK logs full requests and responses, which leaks secrets.
CloudFormation reuses a stack's previous parameter values on update, so a stack deployed with
`LogLevel=TRACE` fails parameter validation on its next deploy. Pass a new level once:

```bash
sam deploy --config-env dev --parameter-overrides "Stage=dev LogLevel=DEBUG"
```

## Mailbox

Setting `MailDomain` also makes the stack a mailbox on SES. The stack
creates the SES identity and configuration set, a receipt rule that stores inbound mail in S3,
the mail bucket and mail table, two SNS topics subscribed to the function, and (with
`HostedZoneId`) the DNS records.

With `MailDomain` empty (the default), none of those resources exist, and the function keeps
its 10 s timeout, 256 MB of memory and its current environment. A mailbox stack runs the
function with a 90 s timeout and 512 MB, so one invocation can parse a 40 MB message.

> [!IMPORTANT]
> **Region.** SES email receiving is offered only in some regions. Deploy a mailbox stack in a
> region that has an email receiving endpoint in the
> [SES endpoints list](https://docs.aws.amazon.com/general/latest/gr/ses.html); elsewhere the
> receipt rule set can't be created.

```bash
sam deploy --config-env dev --parameter-overrides \
  "Stage=dev AllowedTopics=<your-account-id> MailDomain=mail.example.com MailAddresses=hello,support \
   ApiKeysParameterName=/aws-messaging-webhook/dev/api-keys HostedZoneId=<zone-id>"
```

### Mailbox parameters

| Parameter | Default | Notes |
|---|---|---|
| `MailDomain` | *(empty)* | Receiving domain and sending identity, e.g. `mail.example.com`. Empty disables every mail resource |
| `MailAddresses` | *(empty)* | Comma-separated local parts, **at most 10** (e.g. `hello,support`). Each becomes the inbox `<local>@<MailDomain>`. Required unless `MailCatchAll=true`. The cap exists because CloudFormation can't map over a list, so the template builds the recipient addresses from ten fixed slots |
| `MailCatchAll` | `false` | `true` makes the receipt rule accept every address at the domain |
| `MailAutoCreateInboxes` | `false` | With catch-all, whether mail to an unknown local part creates an inbox |
| `MailBucketName` | *(empty)* | Empty lets CloudFormation generate the bucket name |
| `MailRetentionDays` | `365` | S3 expiration for inbound raw MIME, attachments and sent raw MIME |
| `HostedZoneId` | *(empty)* | Route 53 zone for the domain. Set it and the stack publishes the DNS records; leave it empty and the `DnsRecords` output lists them |
| `DmarcPolicy` | `quarantine` | `none`, `quarantine` or `reject` in the `_dmarc` record |
| `ReceiptTlsPolicy` | `Optional` | `Require` rejects inbound mail that wasn't delivered over TLS |
| `ExistingReceiptRuleSetName` | *(empty)* | Empty creates a rule set. Set it to add the rule to a rule set that is already active in the region |
| `ApiKeysParameterName` | *(empty)* | Name of the SecureString SSM parameter holding the API key hashes. Must start with `/` |
| `ApiKeysKmsKeyArn` | *(empty)* | Customer-managed KMS key that encrypts that parameter; empty means `aws/ssm` |
| `AttachmentUrlTtlSeconds` | `900` | Lifetime of presigned download URLs, 60–3600 |

### After the first deploy

CloudFormation can't do the steps below, so run them once. They use this helper:

```bash
stack=aws-messaging-webhook-dev
output() { aws cloudformation describe-stacks --stack-name "$stack" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
```

1. **Publish DNS** (skip if you set `HostedZoneId`). `output DnsRecords` lists the records:
   - the domain's MX to `inbound-smtp.<region>.amazonaws.com`;
   - three DKIM CNAMEs;
   - the MAIL FROM domain `bounce.<domain>`, with an MX to `feedback-smtp.<region>.amazonses.com`
     and `v=spf1 include:amazonses.com ~all`;
   - the domain's SPF record `v=spf1 include:amazonses.com -all`;
   - `_dmarc.<domain>`.
2. **Wait for DKIM `SUCCESS`** before sending mail from the identity:

   ```bash
   aws sesv2 get-email-identity --email-identity mail.example.com \
     --query '{dkim: DkimAttributes.Status, mailFrom: MailFromAttributes.MailFromDomainStatus}'
   ```

3. **Activate the receipt rule set.** A region has exactly one active rule set, so activating
   this one deactivates any other. If a rule set is already active, redeploy with
   `ExistingReceiptRuleSetName` set to its name instead; the stack then only adds its rule.

   ```bash
   aws ses set-active-receipt-rule-set --rule-set-name "$(output ReceiptRuleSetName)"
   ```

4. **Create the API key parameter.** CloudFormation can't create a SecureString parameter. Its
   name must start with `/` and equal `ApiKeysParameterName`. The value holds SHA-256 hashes,
   never the keys:

   ```bash
   key="am_$(openssl rand -hex 24)"
   hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
   aws ssm put-parameter --name /aws-messaging-webhook/dev/api-keys --type SecureString \
     --value "{\"keys\":[{\"id\":\"key_1\",\"sha256\":\"$hash\"}]}"
   echo "$key"   # hand this to the client; it isn't stored anywhere
   ```

   Add `--key-id <ApiKeysKmsKeyArn>` when you use a customer-managed key. To rotate, overwrite
   the parameter with both entries, move clients to the new key, then remove the old entry.

The API is served under the `ApiBaseUrl` output; `InboxIds` lists the configured inboxes.

### How the mailbox is wired

- **Topology.** The stack owns two SNS topics, `MailInboundTopicArn` (receipt notifications)
  and `MailEventsTopicArn` (configuration-set events for sent mail). Both are subscribed to the
  function over the direct (lambda) pathway with per-topic invoke permissions, so there's no
  wiring step. Every other topic stays outside the stack. When `AllowedTopics` is non-empty,
  the two mail topic ARNs are appended to the function's allowlist automatically.
- **Retries.** Lambda's async queue retries a failed mail delivery twice, then sends it to
  `MailIngestDlq` (`MailIngestDlqUrl` output). That on-failure destination applies to every
  asynchronous invocation of the function, including direct SNS subscriptions you wired by hand.
- **Mail table stream.** The mail table's stream has at most two readers, the stream relay and
  the mail sender, which is DynamoDB's recommended ceiling. A third consumer needs Kinesis Data
  Streams for DynamoDB.
- **Retention.** Objects under `inbound/`, `attachments/` and `sent/` expire after
  `MailRetentionDays`, but mail table items stay. After that, raw-message and attachment
  downloads for older messages return 404. Nothing under `outbox/` expires.
- The mail bucket and mail table are retained when the stack or the mailbox is deleted.

### Mail metrics

Mail ingest emits `MessagesIngested`, `IngestFailures`, `IngestSkipped` and `IngestTimeouts` as
CloudWatch Embedded Metrics Format (EMF) in the stack-name namespace, each carrying a `function`
dimension. The stack defines no alarms; build your own alarms or dashboards on these metrics and
on the `MailIngestDlq` and `PublishDlq` queue depths.

### Disabling the mailbox

SES refuses to delete the active rule set, so clearing `MailDomain` on a stack whose created
rule set is active fails the update. Deactivate it first:

```bash
aws ses set-active-receipt-rule-set   # no name: deactivates the active rule set
```

With `ExistingReceiptRuleSetName`, only the stack's rule is removed and no deactivation is
needed, but mail to the mailbox addresses then matches no rule. The mail bucket and mail table
are retained, so delete them by hand if you don't need them. If you re-enable the mailbox with
the same explicit `MailBucketName`, delete the retained bucket first.

### Known follow-ups

- Re-ingesting permanently failed ingests. Deliveries in `MailIngestDlq` aren't replayed
  automatically; their raw MIME stays under `inbound/` until `MailRetentionDays`.

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
replaced by `{ "payloadOmitted": true, ... }`; `meta.messageId` is always preserved, so
consumers fetch the full record from DynamoDB. (SES inbound raw MIME is dropped first; this
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

`previousMessageId` is present only on `sms.inbound` events where the inbound message is a
reply to a previously sent outbound message (i.e. the EUM payload carries a
`previousPublishedMessageId`). Consumers can use it to correlate a reply with the sent message
that triggered it without parsing the event payload. It is absent (not null) on unsolicited
inbound contacts and on all other event families.

`meta.s3` and `meta.inbound` appear only on `ses.inbound` (and `ses.inbound.quarantined`)
events. `meta.s3` `{ bucket, key }` is the pointer to the stored raw MIME, present when the
receipt rule used the S3 action and SES populated both the bucket and the object key — the
recommended SES → S3 receipt path. It **survives the oversized-payload content-strip**: when an
inbound message is too large and its embedded `content` is dropped, the pointer stays, so a
consumer can still `GetObject` the message from S3. `meta.inbound` carries a summary lifted out
of the receipt so consumers can route without fetching from S3: `headers` (the SES-parsed
`commonHeaders` — `from`/`to`/`subject`/`date`/`messageId`) and `auth` (the `spf`/`dkim`/`dmarc`/
`spam`/`virus` verdict statuses plus `dmarcPolicy`). Every sub-field is omitted (not null) when
SES did not supply it, and the whole `inbound` block is absent when the receipt carried neither
headers nor verdicts. All three additions are meta-only and additive, so `schemaVersion` stays 1.

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

### Mailbox events

A mailbox stack's mail table stream additionally publishes mailbox detail-types on the same
bus, `source` and `schemaVersion` contract as above. Only the inbound side is implemented;
`message.sent`, `message.delivered`, `message.bounced`, `message.complained`,
`message.rejected` and `message.opened` arrive with the sender and SES-event pipeline.

`message.received`, `message.received.spam` (spam/virus verdict `FAIL`) or
`message.received.unauthenticated` (SPF/DKIM/DMARC `FAIL`, spam/virus clean) fires once per
inbox a newly-ingested message lands in — a message to two inboxes publishes two events, one per
inbox. Precedence is spam over unauthenticated over plain; nothing is ever dropped, only
classified. Shape:

```json
{
  "schemaVersion": 1,
  "meta": {
    "messageId": "…", "inboxId": "…", "threadId": "…", "sesMessageId": "…"
  },
  "type": "message.received",
  "event_type": "message.received",
  "event_id": "evt_…",
  "message": {
    "inbox_id": "…", "thread_id": "…", "message_id": "…",
    "labels": ["received", "unread"],
    "timestamp": "…", "from": "…", "to": ["…"], "size": 1234,
    "updated_at": "…", "created_at": "…",
    "subject": "…", "preview": "…", "text": "…", "html": "…",
    "attachments": [{ "attachment_id": "…", "size": 1234, "filename": "…", "content_type": "…", "content_disposition": "attachment" }],
    "in_reply_to": "…", "references": ["…"], "headers": { "X-…": "…" }
  },
  "thread": { "thread_id": "…", "subject": "…", "message_count": 1, "recipients": ["…"] }
}
```

`message` is the same `Message` object the `/v0` read API returns.
`thread` is the compact snapshot taken as of this message's arrival (`thread_id`, `subject`,
`message_count`, `recipients`), not a full thread fetch — a consumer wanting the thread's current
state re-reads it, since the snapshot is only ever as fresh as the message that carries it. An
oversized detail is reduced the same way the SMS/SES pipeline's details are: `message.html`,
`.text` and `.headers` drop first, then `thread` shrinks to `{thread_id}`, then `message` falls
back to `{payloadOmitted, ids}`; `meta` is never dropped.

A payload matching no known family, and mail-table writes that aren't a message INSERT (send
state, marker, key and RFC-alias items, and thread housekeeping writes), publish nothing from
the mail table stream.

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

### Mail table

A mailbox stack additionally creates a second, separate DynamoDB table (`MailTableName`
output) holding the mailbox — a distinct partition space from the messaging
events table above, keyed by inbox and message rather than by SNS message id:

| Item | `pk` | `sk` | Holds |
|---|---|---|---|
| Inbox | `INBOX#<inbox>` | `META` | email, display name, metadata, timestamps |
| Message | `INBOX#<inbox>` | `MSG#<messageId>` | the full message: addresses, subject, body, labels, attachments, headers |
| Thread | `INBOX#<inbox>` | `THR#<threadId>` | rolled-up subject/preview/senders/recipients/labels, message count, size, newest attachments |
| RFC alias | `RFC#<inbox>#<rfc-id>` | `RFC` | maps an inbound or outbound `Message-ID` to the message/thread it belongs to, for reply threading |

Labels live in the message and thread items themselves, not in per-label index rows: a list
filtered by label is served by reading the time-ordered index and filtering the page. That keeps
ingest to a fixed three writes per message, at the cost of reading past non-matching messages
when a label is rare.

This release populates only the Inbox, Message and Thread items from inbound ingest.
The send-state, send-key and SES-reference items the send path adds live on the same table
under their own `pk`s (`OUTBOX#<messageId>`, `SENDKEY#<sha256>`, `SESMSG#<sesMessageId>`,
`SESCALL#<messageId>`) and are not written by this release. `message_id` and `thread_id` are UUIDv7s: inbound ids are deterministic (derived from
the SES message id and receipt timestamp), so a redelivered SES notification always resolves to
the same message rather than creating a duplicate. A message's raw MIME and attachments live in
the mail bucket, not the table; the message item's `raw_s3_key`/attachment `object_key`s point at
them.

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
