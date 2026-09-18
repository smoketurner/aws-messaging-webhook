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

Topics and subscriptions live outside the stack, next to your EUM/SES configuration. (The one
exception is the two mail topics the stack creates and subscribes itself when `pMailDomain` is
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
  two topics a mailbox stack owns (see [Mailbox](#mailbox)).

## Mailbox

Set `pMailDomain` and the stack becomes a mailbox on SES. It creates the SES identity and
configuration set, a receipt rule storing inbound mail in S3, the mail bucket and mail table,
two SNS topics subscribed to the function, and the DNS records when `pHostedZoneId` is set.

Empty `pMailDomain` (the default) creates none of it, and the function keeps its 10 s timeout
and 256 MB. A mailbox stack runs it with a 90 s timeout and 512 MB, enough to parse a 40 MB
message in one invocation.

> [!IMPORTANT]
> **Region.** SES receives email in some regions only. Deploy a mailbox stack where the
> [SES endpoints list](https://docs.aws.amazon.com/general/latest/gr/ses.html) shows an email
> receiving endpoint. Elsewhere the receipt rule set cannot be created.

```bash
sam deploy --parameter-overrides \
  "pAllowedTopics=<your-account-id> pMailDomain=mail.example.com pMailInbox=hello \
   pApiKeysParameterName=/messaging-webhook/dev/api-keys pHostedZoneId=<zone-id>"
```

### Mailbox parameters

| Parameter | Default | Notes |
|---|---|---|
| `pMailDomain` | *(empty)* | Receiving domain and sending identity, e.g. `mail.example.com`. Empty disables every mail resource |
| `pMailInbox` | *(empty)* | Local part of the one inbox, e.g. `hello` for `hello@<pMailDomain>`. Required when `pMailDomain` is set; the receipt rule accepts only that address |
| `pMailBucketName` | *(empty)* | Empty lets CloudFormation generate the bucket name |
| `pMailRetentionDays` | `365` | How long a message is kept: S3 expiration for raw MIME, attachments and message content, and the TTL on its mail table items |
| `pHostedZoneId` | *(empty)* | Route 53 zone for the domain. Set it and the stack publishes the DNS records; leave it empty and the `DnsRecords` output lists them |
| `pDmarcPolicy` | `quarantine` | `none`, `quarantine` or `reject` in the `_dmarc` record |
| `pReceiptTlsPolicy` | `Optional` | `Require` rejects inbound mail that wasn't delivered over TLS |
| `pExistingReceiptRuleSetName` | *(empty)* | Empty creates a rule set. Set it to add the rule to a rule set that is already active in the region |
| `pApiKeysParameterName` | *(empty)* | Name of the SecureString SSM parameter holding the API key hashes. Required when `pMailDomain` is set. Must start with `/`, and not with `/aws` or `/ssm`, which SSM reserves |
| `pApiKeysKmsKeyArn` | *(empty)* | Customer-managed KMS key that encrypts that parameter; empty means `aws/ssm` |
| `pAttachmentUrlTtlSeconds` | `900` | Lifetime of presigned download URLs, 60–3600 |

### After the first deploy

CloudFormation cannot do these four. Run them once, using this helper:

```bash
stack=aws-messaging-webhook-dev
output() { aws cloudformation describe-stacks --stack-name "$stack" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
```

1. **Publish DNS** (skip if you set `pHostedZoneId`). `output DnsRecords` lists the records:
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

3. **Activate the receipt rule set.** A region has one active rule set, so activating this one
   deactivates any other. If one is already active, redeploy with `pExistingReceiptRuleSetName`
   set to its name and the stack adds only its rule.

   Inbound mail stops silently whenever the stack's rule set is not the active one, including
   after a deploy that replaces it. Check with `aws ses describe-active-receipt-rule-set`.

   ```bash
   aws ses set-active-receipt-rule-set --rule-set-name "$(output ReceiptRuleSetName)"
   ```

4. **Create the API key parameter.** CloudFormation cannot create a SecureString parameter.
   Name it exactly `pApiKeysParameterName`, starting with `/`. The value holds SHA-256 hashes,
   never the keys:

   ```bash
   key="am_$(openssl rand -hex 24)"
   hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
   aws ssm put-parameter --name /messaging-webhook/dev/api-keys --type SecureString \
     --value "{\"keys\":[{\"id\":\"key_1\",\"sha256\":\"$hash\"}]}"
   echo "$key"   # hand this to the client; it isn't stored anywhere
   ```

   Add `--key-id <pApiKeysKmsKeyArn>` for a customer-managed key. Rotate by writing both
   entries, moving clients to the new key, then removing the old entry.

The API is served under the `ApiBaseUrl` output. `InboxAddress` is the inbox's address and its
`inbox_id`, for example `/v0/inboxes/hello@mail.example.com/messages`.

### Mailbox API

Every `/v0` route needs `Authorization: Bearer <key>`. Anything under `/v0` that isn't listed
below answers `501`. Every error carries a JSON body with a `name` and `message`, including a
malformed body, a missing content type and an oversized request.

| Route | Returns |
|---|---|
| `GET /v0/inboxes` | `{count, limit, inboxes[], next_page_token?}` |
| `GET /v0/inboxes/{inbox_id}` | one inbox |
| `GET /v0/inboxes/{inbox_id}/messages` | `{count, limit, messages[], next_page_token?}`, newest first |
| `GET /v0/inboxes/{inbox_id}/messages/{message_id}` | the full message, including `text`, `html`, `headers` and `references`, which the list view omits |
| `GET /v0/inboxes/{inbox_id}/threads` | `{count, limit, threads[], next_page_token?}`, by last activity |
| `GET /v0/inboxes/{inbox_id}/threads/{thread_id}` | one thread with its `messages[]` embedded oldest first, paginated independently |
| `GET …/messages/{message_id}/raw` | `{message_id, size, download_url, expires_at}` for the stored raw MIME |
| `GET …/messages/{message_id}/attachments/{attachment_id}` | the same, plus `filename`, `content_type`, `content_disposition` and `content_id` |
| `PATCH /v0/inboxes/{inbox_id}/messages/{message_id}` | `{message_id, labels}` after applying `add_labels`/`remove_labels` |
| `POST /v0/inboxes/{inbox_id}/messages/send` | `{message_id, thread_id}` once the send is durably queued |
| `POST …/messages/{message_id}/reply` | the same, joining the original's thread |

Downloads are presigned S3 URLs valid for 15 minutes, not bytes streamed through the function.
The URL carries its own authorization: following it needs no API key, and anyone holding it can
fetch the object until it expires. A raw message or attachment past `pMailRetentionDays`, and an
attachment dropped for size, answer `404`.

`PATCH` takes `{"add_labels": …, "remove_labels": …}`, each one label or a list. Labels are
lowercased, trimmed and deduplicated. Removing the `unread` label is how a client marks mail
read; there is no separate endpoint for it.

- A message carries at most 20 of your labels, a thread at most 20 across its messages. The
  service's own labels don't count. Exceeding either cap is a `400` on `labels`, as is naming
  more than 20 labels in one request.
- The service's own labels — everything but `unread`, `spam`, `trash` and yours — are a `400`,
  as is the same label in both fields.
- A thread keeps a label until its last message carrying it gives it up. Relabelling is not
  thread activity, so it does not reorder a thread list.

List parameters: `limit` (default 20, max 100), `page_token`, `ascending` (default false),
`before`, `after`, `labels`, `from`, `to`, `subject`, and the four `include_*` flags
(`include_spam`, `include_blocked`, `include_unauthenticated`, `include_trash`).

- `before` and `after` are UTC timestamps, accepted either as `2026-01-15T09:30:00.000Z` or
  `2026-01-15T09:30:00Z`. The window is half-open: `after` includes its own instant, `before`
  excludes it. Any other offset is rejected rather than read as UTC.
- `labels`, `from`, `to` and `subject` may repeat. An item must carry *every* requested label;
  for `from`/`to`/`subject` it must match *any* value of each field given, case-insensitively,
  as a substring.
- The `include_*` flags default to false and drop items carrying that label. Naming a label
  explicitly overrides the flag that would hide it, so `?labels=trash` returns trashed mail.
- `page_token` is opaque and bound to the inbox, sort order and time window that issued it.
  Presenting one with a different inbox, `ascending`, `before` or `after` is a `400`. Label and
  address filters may change between pages.
- A filtered list can return fewer than `limit` items *and* a `next_page_token`. Follow the
  token.
- `GET /v0/inboxes` and the messages inside `GET …/threads/{thread_id}` take only `limit` and
  `page_token`. A thread view includes every message in the thread, whatever its labels.

### Sending

`POST …/messages/send` takes `to`/`cc`/`bcc` (one address or a list), `subject`, `text` and/or
`html`, and optionally `reply_to`, `labels`, `headers` and `attachments`. Each attachment gives
exactly one of `content` (base64) or `url`. A send or reply request body may be up to 6 MiB, the
Function URL's own limit (every other route takes 1 MiB); base64 makes inline content about a
third larger than the file, so send larger attachments by `url`.

**It queues; it does not send.** The response means the message is durably recorded, not that
SES took it. A separate sender consumes the mail table's stream and makes the SES call. SESv2
`SendEmail` has no idempotency token and the AWS SDKs retry 5xx themselves, so a synchronous
send could deliver the same mail twice.

Send an `Idempotency-Key` to make a retry safe:

- The same key with the same request returns the original ids.
- The same key with any difference — a field, an attachment's content or metadata, a different
  route or original message — is a `409`. Sending something the caller didn't ask for is worse
  than refusing.
- Keys are remembered for 24 hours. Without one, a retry after a lost response queues the
  message twice.

Headers the service controls are refused rather than ignored, so a caller cannot send as
another inbox: `From`, `Sender`, `To`, `Cc`, `Bcc`, `Reply-To`, `Subject`, `Date`,
`Message-ID`, `In-Reply-To`, `References`, `Return-Path`, `MIME-Version` and every `Content-*`.
A CR or LF anywhere in an address, header name or header value is also refused — all three end
up in a MIME document, where a newline starts a new header.

`…/reply` takes the same body plus `reply_all`, and derives what you leave out from the message
it answers: the thread, `In-Reply-To`, the `References` chain, a `Re:` subject (never doubled),
and the recipient — the original's `Reply-To`, or its sender. `reply_all` copies the original's
other recipients, minus this inbox, so a reply never arrives back where it came from. Naming
`to`, `cc` or `bcc` overrides the derived recipients and keeps the threading.

An attachment `url` must be `https`, on the default port, without embedded credentials, and
must not resolve to a private or link-local address. The API checks that when it accepts the
request, and the sender checks it again on the URL and on every redirect `Location`, at most
five hops. The sender is the only HTTP client here that follows redirects, and it does so by
hand because the first check says nothing about where a redirect leads. The host resolves once
and the connection pins to the addresses that passed, so the name cannot change underneath it.
A compressed response is refused, since nothing here decompresses.

Fetched bytes go to the outbox under the attachment's id, so a retried send reuses them instead
of re-fetching a URL whose content may have changed.

A sweep runs every ten minutes. A sender killed between claiming a send and recording its
outcome leaves that send `sending`, with nobody working on it and no stream record to re-trigger
it. The sweep takes claims older than fifteen minutes: one whose sender never reached SES is
released for another attempt, and one whose sender recorded that it was about to call SES moves
to `unknown`, since the message may already have gone out. Fifteen minutes sits well beyond the
sender's own timeout — taking a send from a sender still working on it is how mail gets
delivered twice. `unknown` sends are counted and left for an operator.

SES events on the configuration set then label the message `delivered`, `bounced`,
`complained`, `rejected` or `opened`. Labels are added, never removed: these events arrive out
of order, and a message that both bounced and was opened should say both. An event for mail
this service did not send resolves to nothing, which is the ordinary case on a shared
configuration set.

### How the mailbox is wired

- **Topics.** The stack owns `MailInboundTopicArn` (receipt notifications) and
  `MailEventsTopicArn` (configuration-set events for sent mail), both subscribed to the function
  over the direct pathway with per-topic invoke permissions. No wiring step. A non-empty
  `pAllowedTopics` gains both ARNs automatically.
- **Retries.** Lambda's async queue retries a failed mail delivery twice, then writes it to
  `rAsyncInvokeDlq` (`AsyncInvokeDlqUrl`). That destination covers every asynchronous
  invocation, including direct SNS subscriptions you wired by hand.
- **Stream readers.** The mail table's stream has two: the relay and the sender. That is
  DynamoDB's recommended ceiling; a third needs Kinesis Data Streams for DynamoDB.
- **Storage.** SES writes raw MIME under `inbound/raw/`. Bodies, headers, `References`,
  `Reply-To` and verdicts go to `messages/<inbox>/<message_id>.json`. The table item holds only
  what lists, threads, labels and send status need.
- **Retention.** Objects under `inbound/`, `attachments/`, `messages/` and `sent/` expire after
  `pMailRetentionDays`, and the message's items carry the same TTL, so a message ages out whole.
  DynamoDB removes expired items within a few days. Nothing under `outbox/` expires.
- **Deletion.** The mail bucket and mail table survive deleting the stack or the mailbox.

### Mail metrics

Ingest emits `MessagesIngested`, `IngestFailures`, `IngestSkipped` and `IngestTimeouts` as EMF
in the stack-name namespace, each with a `function` dimension. The stack defines no alarms.
Build them on these metrics and on the `rAsyncInvokeDlq` and `rPublishDlq` queue depths.

### Disabling the mailbox

SES refuses to delete the active rule set, so clearing `pMailDomain` fails while the stack's
rule set is active. Deactivate it first:

```bash
aws ses set-active-receipt-rule-set   # no name: deactivates the active rule set
```

With `pExistingReceiptRuleSetName`, only the stack's rule goes and no deactivation is needed,
but mail to the mailbox then matches no rule. The bucket and table are retained; delete them by
hand if you don't want them. Re-enabling the mailbox with the same explicit `pMailBucketName`
needs the retained bucket deleted first.

### Operator runbook

Three situations need a person. The first two invoke the sender directly rather than going
through the API: they are rare, destructive and account-scoped, so `lambda:InvokeFunction` is a
better gate than a bearer key.

**A send whose outcome is `unknown`.** SES was called and gave no usable answer, or the sender
died after calling it, so SES may or may not hold the message. Nothing automatic touches it,
because resending risks delivering twice. Find them in the sweep's `sends_outcome_unknown` log
line or by querying `ByStatus`, then decide:

```bash
sender=$(output MailSenderFunctionName)

# Send it again, accepting that the recipient may get two copies.
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"resend","message_id":"<id>"}' /dev/stdout

# Or record the outcome without sending anything.
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"close_sent","message_id":"<id>"}' /dev/stdout
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"close_failed","message_id":"<id>"}' /dev/stdout
```

Only an `unknown` send resolves this way. Anything else is refused: resolving a send in flight
races the sender holding it, and resolving a finished one rewrites a settled outcome. Closing as
sent labels the message `sent` with no SES id. Closing as failed labels it `rejected` with
reason `closed_by_operator`.

**Mail that failed to ingest.** A delivery that exhausted its retries lands in
`rAsyncInvokeDlq` with the original event. Replaying it means re-invoking the function:

```bash
queue=$(output AsyncInvokeDlqUrl)
msg=$(aws sqs receive-message --queue-url "$queue" --max-number-of-messages 1)
echo "$msg" | jq -r '.Messages[0].Body' | jq '.requestPayload' > /tmp/replay.json

aws lambda invoke --function-name "$(output WebhookFunctionName)" \
  --payload file:///tmp/replay.json /dev/stdout

# Once it succeeds, drop the DLQ message.
aws sqs delete-message --queue-url "$queue" \
  --receipt-handle "$(echo "$msg" | jq -r '.Messages[0].ReceiptHandle')"
```

Ingest is idempotent: the message id comes from the SES message id and receipt timestamp, so a
replay of a partly-successful attempt resolves to the same ids. The signature is re-verified on
replay, which holds as long as the signing certificate is valid — comfortably within the queue's
14-day retention.

**A send stuck in `queued`.** `rMailSenderDlq` gets a record when the sender exhausts its
retries. Its messages carry the stream position (`DDBStreamBatchInfo`), not the send, so they
cannot be replayed. The send state in the table is the source of truth: fix the cause, then
re-trigger the sender by stamping `requeued_at`, which is the change it acts on. The condition
below leaves a send that has moved on untouched.

```bash
aws dynamodb update-item --table-name "$(output MailTableName)" \
  --key '{"pk":{"S":"OUTBOX#<message-id>"},"sk":{"S":"STATE"}}' \
  --update-expression 'SET requeued_at = :now' \
  --condition-expression 'send_status = :queued AND attribute_not_exists(requeued_at)' \
  --expression-attribute-values "{\":now\":{\"S\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\"},\":queued\":{\"S\":\"queued\"}}"
```

The sender reacts to `requeued_at` appearing, so a send that already carries one needs a
`REMOVE requeued_at` update first. Then delete the DLQ message.

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
- **`inbound`** appears on the same two, carrying what the receipt already knew so consumers can
  route without fetching from S3: `headers` (SES's parsed `commonHeaders`) and `auth` (the
  `spf`/`dkim`/`dmarc`/`spam`/`virus` statuses and `dmarcPolicy`). The block is absent when the
  receipt carried neither.

`schemaVersion` is on every detail, including `subscription.changed`, so consumers have one
field to switch on as the contract evolves. These three additions are meta-only, so it stays 1.

The relay also emits `message.status.changed` when an aggregate's `current_status` transitions
(`sent` → `delivered` → `bounced`), but not on count-only bumps like opens and clicks:

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

A mailbox stack publishes mailbox detail-types from the mail table's stream, on the same bus,
`source` and `schemaVersion` contract.

`message.received`, `message.received.spam` (spam or virus verdict `FAIL`) or
`message.received.unauthenticated` (SPF/DKIM/DMARC `FAIL`, spam and virus clean) fires once per
inbox a new message lands in. Spam beats unauthenticated beats plain. Nothing is dropped, only
classified.

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

`message` is the `Message` object the `/v0` read API returns. `thread` is the snapshot taken as
this message arrived — `thread_id`, `subject`, `message_count`, `recipients` — not a fresh
fetch, so a consumer wanting current thread state re-reads it. An oversized detail reduces the
way the SMS/SES details do: `message.html`, `.text` and `.headers` first, then `thread` to
`{thread_id}`, then `message` to `{payloadOmitted, ids}`. `meta` never drops.

`message.sent` fires when the sender relabels a queued message `sent`. `message.delivered`,
`message.bounced`, `message.complained`, `message.rejected` and `message.opened` fire when an
SES event adds that label. Each carries the same `schemaVersion`, `meta`, `type`, `event_type`
and `event_id`, with `message` in its list form: identifiers, labels, addresses, subject and
preview, no body — the consumer has that from the message's own event or the read API.

Delivery labels are added, never removed, so a message that bounced and was opened publishes
both. A label arriving twice — an SES event redelivered, a stream record replayed — rebuilds the
same `event_id`, which is what a consumer deduplicates on.

The mail stream publishes nothing for an unrecognized payload, for writes to anything but a
message item (send state, markers, keys, RFC aliases, thread housekeeping), or for a message
write adding no system label (a read receipt, your own label, a metadata write).

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

A mailbox stack additionally creates a second, separate DynamoDB table (`MailTableName`
output) holding the mailbox — a distinct partition space from the messaging
events table above, keyed by inbox and message rather than by SNS message id:

| Item | `pk` | `sk` | Holds |
|---|---|---|---|
| Inbox | `INBOX#<inbox>` | `META` | email, display name, metadata, timestamps |
| Message | `INBOX#<inbox>` | `MSG#<messageId>` | addresses, subject, preview, labels, attachment metadata, send status; the body and headers are in S3 |
| Thread | `INBOX#<inbox>` | `THR#<threadId>` | rolled-up subject/preview/senders/recipients/labels, message count, size, newest attachments |
| RFC alias | `RFC#<inbox>#<rfc-id>` | `RFC` | maps an inbound or outbound `Message-ID` to the message/thread it belongs to, for reply threading |

Labels live on the message and thread items, not in per-label index rows. A label-filtered list
reads the time-ordered index and filters the page. Ingest stays at three writes per message, and
a rare label costs reading past non-matching ones.

Two indexes serve the reads:

- **`ByTime`** (`gsi1pk`/`gsi1sk`) — `INBOX#<inbox>#MSG` by message id, `INBOX#<inbox>#THR` by
  `<timestamp>#<threadId>`, and `INBOXES` by inbox id.
- **`ByThread`** (`gsi2pk`/`gsi2sk`) — `THREAD#<inbox>#<threadId>` by message id, for one
  thread's messages.

Message ids are UUIDv7s, so sorting by id sorts by time, which turns a `before`/`after` window
into a plain key range. Inbound ids are deterministic, derived from the SES message id and
receipt timestamp, so a redelivered notification resolves to the same message instead of a
duplicate.

Ingest writes the Inbox, Message and Thread items. The send path adds its own under
`OUTBOX#<messageId>`, `SENDKEY#<sha256>` and `SESMSG#<sesMessageId>`. Raw MIME and attachments
live in the mail bucket; `raw_s3_key` and each attachment's `object_key` point at them.

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

The handler tests drive the real router with properly signed SNS envelopes — the verifier
crate's `test-fixtures` feature generates throwaway keys and certificates — so development needs
no AWS account. For a deployed check, see the probe under [Operations](#operations).

> [!NOTE]
> Debug builds honor `SNS_CERT_HOST_OVERRIDE`, for running against a local fake SNS under
> `cargo lambda watch`. Release builds have no bypass.

## License

Licensed under either of the [Apache License 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.
