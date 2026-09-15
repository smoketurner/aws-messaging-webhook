# Design: AgentMail-compatible inbox on SES

Status: **proposal** — nothing in this document is implemented yet.

## Goal

Replace an [AgentMail](https://agentmail.to) inbox (concretely `hello@mail.smoketurner.com`)
with this project, so that:

1. The existing SAM template takes a **hostname** and one or more **inbound addresses** as
   parameters and provisions everything AWS-side: the SES domain identity, DNS, an S3 bucket
   for raw mail, the receipt rule set, the SNS topics, and the wiring into the webhook. No
   second tool: `sam deploy` remains the whole deployment.
2. The Lambda **ingests** each received email: parses the MIME from S3, stores message, thread,
   and attachment metadata, and emits AgentMail-shaped `message.*` events on the EventBridge bus.
3. The Lambda exposes an **AgentMail-compatible HTTP API** (`/v0/...`, bearer auth) so existing
   AgentMail SDK code — list threads, read a message, reply — works against it with only the
   base URL and API key changed.

### Non-goals (v1)

- Multi-tenant pods, per-inbox client ids, AgentMail's `drafts`, `labels`, `domains`, and
  `webhooks` resource groups. The EventBridge bus is the outbound notification mechanism; an HTTP
  webhook consumer is an EventBridge API destination (see [Events](#events)).
- A UI. The API and the bus are the product.
- Byte-for-byte parity with every AgentMail field. The compatibility target is the subset an
  agent uses to *read and respond to mail*; every endpoint's status is tabulated below.

## How it fits the existing pipeline

Nothing about the current verify → persist → act → publish pipeline changes. Inbound email
already arrives as an SES receipt notification over SNS (`DomainEvent::SesInbound`), already
persists to the events table, and already publishes `ses.inbound` with the S3 pointer in
`meta.s3`. The enhancement adds:

- **a new lifecycle action** for `SesInbound` (the *act* stage): fetch the MIME from S3, parse
  it, and write mailbox items to a new table — the "mailbox ingest" action;
- **a second table** (the mail table) modelled for AgentMail's read patterns, with its own
  stream feeding the existing relay so `message.*` events are published the same way every other
  event is (the relay stays the sole publisher);
- **an authenticated `/v0` API** on the same Axum router and Function URL;
- **an outbound send path** through SES v2 that writes the sent message into the same mail table
  so replies thread correctly and delivery events join up.

```
                     template.yaml (Condition: HasMailDomain)
   ┌───────────────────────────────────────────────────────────────────────┐
   │ MX/DKIM/SPF/DMARC ─► SES receipt rule ─► S3 (raw MIME)                │
   │                                      └─► SNS topic ─► webhook          │
   │ SES configuration set (outbound) ─► SNS topic ─► webhook               │
   └───────────────────────────────────────────────────────────────────────┘
                                             │
   SNS notification ──► verify ──► persist (events table) ──► act ──► (relay ► ses.inbound)
                                                               │
                                       mailbox ingest: GetObject ► parse MIME ► extract
                                       attachments ► resolve thread ► TransactWrite
                                                               │
                                                        mail table (stream)
                                                               │
                                              relay ──► EventBridge `message.received`
                                                               ▲
   Agent ──► GET /v0/inboxes/{id}/threads ──► mail table       │
   Agent ──► POST /v0/inboxes/{id}/messages/{id}/reply ──► SESv2 SendEmail(Raw) ─► S3 + mail table
```

## SAM template changes

Everything lives in `template.yaml`, gated by one condition, `HasMailDomain` (`MailDomain`
non-empty), so a stack deployed without the mail parameters is byte-for-byte what it is today.
This relaxes the current "topics and subscriptions live outside the stack" rule for the two
**mail** topics only: an SES receipt rule and a configuration-set event destination must name
their topic, and the topic must exist before the rule, so the stack owns them. The EUM and
SES-sending topics operators already wire by hand are unaffected.

### Parameters

| Parameter | Env var | Purpose |
|---|---|---|
| `MailDomain` | `MAIL_DOMAIN` | e.g. `mail.smoketurner.com`; the receiving domain and the sending identity. Empty (default) disables every mail resource |
| `MailAddresses` | `MAIL_INBOXES` | `CommaDelimitedList` of local parts, e.g. `hello`; each becomes an inbox (`hello@mail.smoketurner.com`) |
| `MailCatchAll` | `MAIL_CATCH_ALL` | `false` (default) receives only `MailAddresses`; `true` makes the receipt rule match the whole domain |
| `MailAutoCreateInboxes` | `MAIL_AUTO_CREATE_INBOXES` | `false` (default); with catch-all on, whether an unknown recipient creates an inbox |
| `MailBucketName` | `MAIL_BUCKET` | Empty (default) derives `<stack-name>-mail`; set it to control the name |
| `MailRetentionDays` | — | S3 lifecycle expiration for raw MIME and attachments (default 365) |
| `HostedZoneId` | — | Optional Route 53 zone; when set, the stack publishes every DNS record itself |
| `DmarcPolicy` | — | `none` / `quarantine` (default) / `reject` for the `_dmarc` TXT |
| `ReceiptTlsPolicy` | — | `Optional` (default) / `Require` on the receipt rule |
| `ExistingReceiptRuleSetName` | — | Empty (default) creates a rule set; set it to add the rule to a rule set that already exists and is active in this region |
| `ApiKeysParameterName` | `API_KEYS_PARAMETER` | Name of the SecureString SSM parameter holding the API key hashes (see [Authentication](#authentication)) |
| `ApiKeysKmsKeyArn` | — | Optional customer-managed key the parameter is encrypted with; empty means the AWS-managed `aws/ssm` key |
| `AttachmentUrlTtlSeconds` | `ATTACHMENT_URL_TTL_SECONDS` | Presigned URL lifetime (default 900) |

`MAIL_INBOXES` is the inbox control plane in v1: the function treats every configured address
as an inbox, upserting its `META` item on first use, and `GET /v0/inboxes` returns configured
inboxes plus any created through the API in a later phase. No table seeding step exists.

### Resources (all under `HasMailDomain`)

- **`MailIdentity`** (`AWS::SES::EmailIdentity`): `EmailIdentity: !Ref MailDomain`, Easy DKIM
  on (`DkimAttributes.SigningEnabled`), `MailFromAttributes.MailFromDomain: bounce.<domain>`,
  and `ConfigurationSetAttributes.ConfigurationSetName` pointing at the outbound set below so
  every send from the identity is stamped even if a caller forgets. The identity exposes
  `DkimDNSTokenName1..3` / `DkimDNSTokenValue1..3` as attributes, which is what makes fully
  in-template DNS possible.
- **`MailBucket`** (`AWS::S3::Bucket`): all public access blocked, SSE-S3, versioning off,
  lifecycle expiration at `MailRetentionDays` on `inbound/`, `attachments/`, `sent/`,
  `DeletionPolicy: Retain` (mail outlives a stack teardown).
- **`MailBucketPolicy`**: `ses.amazonaws.com` may `s3:PutObject` under `inbound/*` when
  `aws:SourceAccount` is this account and `aws:SourceArn` is the receipt rule. The rule ARN is
  built with `!Sub` from the rule-set and rule *names*, not `!Ref` — a `!Ref` to the rule
  while the rule references the bucket would be a circular dependency.
- **`MailInboundTopic`** (`AWS::SNS::Topic`, `SignatureVersion: "2"`) plus a topic policy
  allowing `ses.amazonaws.com` (`aws:SourceAccount` condition) to publish.
- **`MailReceiptRuleSet`** (`AWS::SES::ReceiptRuleSet`, only when `ExistingReceiptRuleSetName`
  is empty) and **`MailReceiptRule`** (`AWS::SES::ReceiptRule`): `Recipients` = the full
  addresses, or `[MailDomain]` when `MailCatchAll`; `ScanEnabled: true`; `TlsPolicy`; one
  `S3Action { BucketName, ObjectKeyPrefix: inbound/, TopicArn }`. `DependsOn: MailBucketPolicy`
  — SES verifies it can write to the bucket when the rule is created. The S3 action's
  `TopicArn` is what produces the receipt notification with `receipt.action.type = "S3"` and the
  bucket/key pointer that `model/ses_inbound.rs` already parses.
- **Inbound subscription** as a SAM event on the function: `Events.MailInbound: { Type: SNS,
  Topic: !Ref MailInboundTopic }` — SAM emits the `AWS::SNS::Subscription` (`lambda` protocol)
  and the `AWS::Lambda::Permission` scoped to that topic, the same per-topic gate the README's
  manual `add-permission` step provides today. The direct pathway is deliberate; see
  [Timeouts](#timeouts-and-the-delivery-pathway). The function also gets an async-invoke
  on-failure destination (`EventInvokeConfig` → `MailIngestDlq`, SQS) for durability past the
  async queue's two retries.
- **`MailConfigurationSet`** (`AWS::SES::ConfigurationSet`) and
  **`MailEventDestination`** (`AWS::SES::ConfigurationSetEventDestination`) for `SEND`,
  `DELIVERY`, `BOUNCE`, `COMPLAINT`, `REJECT`, `OPEN` to **`MailEventsTopic`** (SNS,
  `SignatureVersion: "2"`), subscribed to the function the same way. This is what turns SES
  delivery events for sent mail into `message.delivered` / `message.bounced` / …
- **Route 53** (when `HostedZoneId` is set): `AWS::Route53::RecordSet` for
  `MX 10 inbound-smtp.${AWS::Region}.amazonaws.com`, the three DKIM CNAMEs from the identity's
  attributes, MAIL FROM `MX`/`TXT` on `bounce.<domain>`, `TXT "v=spf1 include:amazonses.com
  -all"`, and `_dmarc.<domain> TXT` with `DmarcPolicy`. Without a zone id, a `DnsRecords`
  output lists the same records for manual publication.
- **`MailTable`** (`AWS::DynamoDB::Table`, on-demand, `pk`/`sk`, the three GSIs in the data
  model, stream `NEW_AND_OLD_IMAGES`, deletion protection in prod, **no TTL** — mailbox data
  is not an audit buffer; retention is the S3 lifecycle plus explicit deletes).
- A second **stream event-source mapping** on `MailTable` into the same function, same
  settings as the events-table one (bisect, `ReportBatchItemFailures`, on-failure to the
  existing `PublishDlq`). Filter to `INSERT` and `MODIFY` records whose image `sk` begins with
  `MSG#`; thread items, inbox items, and the label pointer items below never reach the relay.
- **IAM** additions to the function role: `s3:GetObject` on `inbound/*`, `s3:PutObject` /
  `GetObject` on `attachments/*` and `sent/*` of the bucket; `ses:SendEmail` /
  `ses:SendRawEmail` on the identity and configuration-set ARNs with a `ses:FromAddress`
  `StringLike` condition of `*@${MailDomain}`; DynamoDB read/write on `MailTable` and its
  indexes; `ssm:GetParameter` on `arn:…:parameter/${ApiKeysParameterName}` plus `kms:Decrypt`
  on `ApiKeysKmsKeyArn` when set (the AWS-managed `aws/ssm` key needs no explicit statement);
  `sqs:SendMessage` on `MailIngestDlq`.
- Function `Timeout` raised from 10 to **60 s** (ingest of a 40 MB message; see below) and
  `MemorySize` to 512 to give the parser headroom. Both apply stack-wide, which is harmless for
  the existing paths.

New outputs: `MailTableName`, `MailBucketName`, `MailInboundTopicArn`, `MailEventsTopicArn`,
`MailConfigurationSetName`, `InboxIds`, `ApiBaseUrl` (`<FunctionUrl>v0`), `DnsRecords`, and
`ReceiptRuleSetName`.

### Two things CloudFormation cannot do

Both are one CLI command after the first deploy, documented next to the README's existing
wiring commands:

1. **Activate the receipt rule set.** There is exactly one active rule set per region and no
   CloudFormation resource or property activates one:

   ```bash
   aws ses set-active-receipt-rule-set --rule-set-name "$(aws cloudformation describe-stacks \
     --stack-name aws-messaging-webhook-prod \
     --query "Stacks[0].Outputs[?OutputKey=='ReceiptRuleSetName'].OutputValue" --output text)"
   ```

   Operators who already have an active rule set pass its name as
   `ExistingReceiptRuleSetName` instead, and the stack only adds its rule.

2. **Create the encrypted API-key parameter.** `AWS::SSM::Parameter` supports only `String`
   and `StringList`; `SecureString` must be created outside the template. The value is a JSON
   document of key ids and SHA-256 hashes (never the keys themselves):

   ```bash
   key="am_$(openssl rand -hex 24)"
   hash=$(printf '%s' "$key" | sha256sum | cut -d' ' -f1)
   aws ssm put-parameter --name /aws-messaging-webhook/prod/api-keys --type SecureString \
     --value "{\"keys\":[{\"id\":\"key_1\",\"sha256\":\"$hash\"}]}"
   echo "$key"   # hand this to the agent; it is not stored anywhere
   ```

   Rotation is `put-parameter --overwrite` with a second entry, roll clients, then remove the
   first. The parameter name is passed as `ApiKeysParameterName`. Standard tier (4 KB) holds
   dozens of keys.

Two constraints to state in the README: SES email receiving is offered in a subset of regions,
so the stack must live in one of them; and the identity must show DKIM `SUCCESS` before mail is
sent from it.

## Data model: the mail table

Single table, keyed for AgentMail's access patterns. `inbox_id` **is the email address**, as in
AgentMail. `message_id` is the **SES message id** — the same value that is the events table's
aggregate id (`pk = MSG#<messageId>`) and the S3 object key suffix, so an audit query on the
events table, the raw object, and the mailbox item all join on one id. For outbound mail it is
the id `SendEmail` returns, which is also the `mail.messageId` on every subsequent delivery,
bounce, and complaint event. `thread_id` is the message id of the thread's first message.

| Item | `pk` | `sk` | Notable attributes |
|---|---|---|---|
| Inbox | `INBOX#<inbox_id>` | `META` | `email`, `display_name`, `metadata`, `created_at`, `updated_at` |
| Message | `INBOX#<inbox_id>` | `MSG#<message_id>` | `thread_id`, `timestamp`, `labels` (SS), `from`, `to`, `cc`, `bcc`, `reply_to`, `subject`, `preview` (first 256 chars of text), `size`, `text`, `html`, `headers` (M), `in_reply_to`, `references` (L), `attachments` (L of M), `rfc_message_id`, `raw_s3_key`, `verdicts` (M), `created_at`, `updated_at` |
| Thread | `INBOX#<inbox_id>` | `THR#<thread_id>` | `timestamp`, `subject`, `preview`, `senders` (SS), `recipients` (SS), `labels` (SS, union of member labels), `last_message_id`, `message_count`, `size`, `received_timestamp`, `sent_timestamp`, `attachments` (L) |
| Message label pointer | `INBOX#<inbox_id>#LABEL#<label>` | `MSGAT#<timestamp>#<message_id>` | One per label on the message: the list-view projection (`thread_id`, `timestamp`, `from`, `to`, `cc`, `subject`, `preview`, `size`, `attachments` summary, `labels`) |
| Thread label pointer | `INBOX#<inbox_id>#LABEL#<label>` | `THRAT#<timestamp>#<thread_id>` | One per label in the thread's union: the thread list-view projection |

Global secondary indexes (all project `ALL`; the table is small and reads dominate):

| Index | Key | Serves |
|---|---|---|
| `ByTime` | `gsi1pk = INBOX#<inbox_id>#MSG` or `#THR`, `gsi1sk = <timestamp>#<id>` | Unfiltered list messages / list threads with `before`/`after`/`ascending` as a sort-key range, `page_token` = base64 of `LastEvaluatedKey` |
| `ByThread` | `gsi2pk = THREAD#<inbox_id>#<thread_id>`, `gsi2sk = <timestamp>#<message_id>` | Get thread: the embedded `messages` array |
| `ByRfcId` | `gsi3pk = RFC#<inbox_id>#<rfc-message-id>` | Thread resolution from `In-Reply-To` / `References` |

**Labels are the folders.** There is no folder attribute: a message's location is whatever its
labels say (`received`, `sent`, `spam`, `trash`, `unread`, and anything an agent adds such as
`needs-reply`). A global secondary index key must be one scalar attribute, so it cannot index
membership in the `labels` set; the label pointer items are how every label gets a direct query.
`GET …/messages?labels=sent` is one bounded `Query` on `INBOX#<inbox_id>#LABEL#sent` with the
same `before`/`after`/`ascending` sort-key range as `ByTime`, and it behaves exactly like a Sent
folder; `labels=unread` is an unread view for free. Pointers are written in the same
transaction as the message (ingest step 7, send step 5) and maintained by `PATCH` (below), so
they are never out of step with the message item. The cost is one extra write per label per
message, bounded by a cap of 20 labels per message, and duplication of the list-view fields,
which never change after ingest except `labels` itself. Multiple `labels` in one request are
served by querying the smallest label and intersecting with the message items' sets.

Bodies live on the message item, capped: `text` and `html` are each stored up to 200 KB (an item
is 400 KB max); a body over the cap is truncated in the item and flagged `body_truncated = true`,
and the full body is always available from the raw MIME (`GET …/raw`). Extracted attachments go
to `s3://<bucket>/attachments/<message_id>/<attachment_id>`; `attachment_id` is a per-message
ordinal (`att_1`, `att_2`) so it is stable across re-ingest.

Why a second table instead of the events table: the access patterns (per-inbox time ordering,
thread membership, RFC-id lookup) need indexes the events table has no reason to carry; the
events table's TTL would silently delete mail; and its stream is filtered to be the outbox for
raw AWS-shaped events. Keeping them apart keeps the existing table's invariants untouched.

## Ingest

The new `actions::run` arm for `DomainEvent::SesInbound`:

1. **Resolve inboxes** from `receipt.recipients` (the envelope recipients matched by the rule).
   For each recipient, `GetItem INBOX#<addr>/META`. See [Inbox resolution](#inbox-resolution).
2. **Fetch** the raw MIME with `GetObject` on `meta.s3` (`bucket`, `key`). A receipt with no S3
   pointer (an SNS-action rule) is not ingested: log `ingest_skipped reason=no_s3_pointer` and
   return `Ok("none")` — the existing `ses.inbound` event still flows. Wrong bucket (not
   `MAIL_BUCKET`) is a permanent skip with a warning: never follow a pointer the operator did not
   configure.
3. **Parse** with [`mail-parser`](https://crates.io/crates/mail-parser) (pure Rust, MIT/Apache,
   the Stalwart parser): addresses, subject, date, `Message-ID`, `In-Reply-To`, `References`,
   text and HTML bodies, attachments (name, content type, disposition, `Content-ID`, size).
4. **Extract attachments** to S3 (`PutObject`, idempotent by key).
5. **Resolve the thread**: look up each id in `In-Reply-To` then `References` (nearest first)
   on `ByRfcId` for this inbox; first hit wins and supplies `thread_id`. No hit → new thread,
   `thread_id = message_id`. No subject-based merging: false joins are worse than split threads.
6. **Classify labels**: `received` + `unread` always; `spam` when the receipt is quarantined
   (spam or virus verdict `FAIL`, the existing `is_quarantined`); `unauthenticated` when SPF,
   DKIM, or DMARC is `FAIL`. These select the event type (`message.received`,
   `.spam`, `.unauthenticated`) exactly as AgentMail does, and drive the list endpoints'
   `include_spam` / `include_unauthenticated` filters.
7. **Persist** with one `TransactWriteItems` per inbox: `Put` message
   (`attribute_not_exists(pk) AND attribute_not_exists(sk)`), `Update` thread (`ADD
   message_count :one`, `ADD senders/recipients`, `ADD labels`, `SET last_message_id,
   timestamp, size` guarded by `timestamp >= if_not_exists(...)`, `SET subject/preview =
   if_not_exists(...)`), one `Put` message label pointer per label, and one `Put` thread label
   pointer per label newly added to the thread's union. A `ConditionalCheckFailed` on the
   message put is the idempotency signal (`Duplicate`): the whole transaction cancels, the
   thread is not double-counted, no pointer is duplicated, and the action returns `Ok`.

Failure classification follows `ActionErrorKind`: S3/DynamoDB throttling, 5xx, and timeouts are
`Transient` (→ 5xx → SNS redelivery re-runs the idempotent ingest); a MIME that fails to parse,
an object that is missing, or an unknown recipient with `MailCatchAll` off are `Permanent` (log,
`IngestFailures` metric, the `ses.inbound` event still publishes with the S3 pointer so nothing
is lost).

Ingest is a *new* `Services` sub-trait, `MailStore + ObjectStore`, so the handler tests keep
running with `FakeServices` and MIME fixtures (`tests/fixtures/mail/*.eml`).

### Inbox resolution

- Recipient has an inbox item → ingest into it.
- Recipient is in `MAIL_INBOXES` but has no `META` item yet → upsert the item, then ingest.
- Recipient has no inbox and `MailCatchAll` is off → the receipt rule would not have matched,
  so this only happens if the operator edited the rule by hand; treat as permanent skip.
- Recipient has no inbox and `MAIL_AUTO_CREATE_INBOXES=true` (default `false`) → create the
  inbox item and ingest. Off by default so a catch-all domain does not become an unbounded
  inbox factory for spam.
- Otherwise → skip with `unknown_inbox`, and the message stays discoverable through the
  `ses.inbound` event and the raw object.

### Timeouts and the delivery pathway

A 40 MB message (the SES receiving ceiling) means one `GetObject`, a parse, and several
`PutObject`s inside the request. Over HTTPS, SNS applies a short, fixed response timeout to the
endpoint (AWS documents it as 15 seconds; it is not configurable) before it counts the delivery
as failed and retries, which would make a slow ingest look like a failure loop. The template
therefore subscribes the mail topics with the **`lambda` protocol** (async invoke, no response
timeout, function timeout 60 s), with an on-failure SQS destination on the function for
durability beyond the async queue's two retries. An operator who prefers the longer SNS HTTPS
retry policy can still subscribe the topic to `SesInboundEndpoint` by hand and accept the
ceiling.

## The `/v0` API

Mounted on the existing router under `/v0`, behind a bearer-auth middleware. Same Function URL
as the webhook paths (one URL per function). Request bodies on the send/reply routes are limited
to **6 MB** (the Function URL payload ceiling, which happens to equal AgentMail's documented
limit); the 1 MiB `DefaultBodyLimit` stays on every other route.

### Authentication

`Authorization: Bearer <key>`. The middleware SHA-256-hashes the presented key and compares it
in constant time (`subtle::ConstantTimeEq`) against every hash in the cached parameter, read
with `ssm:GetParameter` (`WithDecryption: true`) from the SecureString named by
`API_KEYS_PARAMETER`. The cache refreshes every 5 minutes and on any miss (so a freshly added
key works immediately at the cost of one `GetParameter`); a `GetParameter` failure keeps the
last good cache rather than failing open or locking everyone out, and an empty cache rejects
everything. Missing or wrong key → `401` with the AgentMail error body. The header is redacted
from `TraceLayer` logs. Keys never appear in logs or metrics; `ApiAuthFailures` is a counter
only. Parameter Store rather than Secrets Manager: no per-secret charge, the same KMS
encryption, and the rotation story (overwrite with two hashes, roll, drop one) needs none of
Secrets Manager's rotation machinery.

Deliberately not IAM SigV4: the AgentMail SDKs send a bearer token, and drop-in compatibility is
the point. A second `AuthType: AWS_IAM` URL is impossible on the same function, and API Gateway
in front would be a separate phase if WAF or usage plans are ever needed.

### Compatibility matrix

Paths and field names follow AgentMail's v0 reference exactly; snake_case JSON; datetimes RFC
3339 UTC; errors are `{ "name": "...", "message": "...", "code"?, "fix"?, "docs"? }`.

| Endpoint | v1 | Notes |
|---|---|---|
| `GET /v0/inboxes` | ✅ | `count`, `inboxes[]`, `limit`, `next_page_token`; Query `sk = META` via a `ByTime` variant (`gsi1pk = INBOXES`) |
| `GET /v0/inboxes/{inbox_id}` | ✅ | `pod_id` is a fixed `"pod_default"` |
| `POST /v0/inboxes` | phase 4 | `username` + `domain == MAIL_DOMAIN` → inbox item; requires `MailCatchAll` so the receipt rule already matches the new address |
| `DELETE /v0/inboxes/{inbox_id}` | phase 4 | |
| `GET /v0/inboxes/{inbox_id}/threads` | ✅ | `limit`, `page_token`, `before`, `after`, `ascending` on `ByTime`; `labels` switches to the thread label pointer partition (a direct query per label); `include_spam` / `include_blocked` / `include_unauthenticated` / `include_trash` default to false and drop threads carrying that label; `senders`, `recipients`, `subject` are post-filters on the page |
| `GET /v0/inboxes/{inbox_id}/threads/{thread_id}` | ✅ | Embedded `messages[]` in ascending order from `ByThread`; thread-level `count`/`limit`/`next_page_token` for long threads |
| `DELETE /v0/inboxes/{inbox_id}/threads/{thread_id}` | phase 4 | |
| `GET /v0/inboxes/{inbox_id}/messages` | ✅ | Same shape as threads: `ByTime` unfiltered, the message label pointer partition for `labels` (so `labels=sent` is the Sent folder and `labels=unread` the unread view), `include_*` exclusions, `from`/`to`/`subject` post-filters |
| `GET /v0/inboxes/{inbox_id}/messages/{message_id}` | ✅ | Full message: `text`, `html`, `headers`, `attachments`, `in_reply_to`, `references`; `extracted_text`/`extracted_html` absent in v1 (phase 4: quote stripping) |
| `GET …/messages/{message_id}/raw` | ✅ | `{ message_id, size, download_url, expires_at }` — presigned `GetObject` on the raw key |
| `GET …/messages/{message_id}/attachments/{attachment_id}` | ✅ | `{ attachment_id, size, filename, content_type, content_disposition, content_id, download_url, expires_at }` — presigned GET with `response-content-disposition` |
| `PATCH …/messages/{message_id}` | ✅ | One `TransactWriteItems`: `ADD`/`DELETE` on the message's set, `Put` a pointer per added label, `Delete` one per removed label, refresh `labels` on the pointers that remain, and `ADD` to the thread's union (a thread label is removed only when no member message still carries it). Response `{ message_id, labels }`. Removing `unread` is how a client marks read |
| `DELETE …/messages/{message_id}` | phase 4 | Adds the `trash` label rather than deleting; a purge is a separate operator concern |
| `POST …/messages/send` | ✅ | See [Sending](#sending) |
| `POST …/messages/{message_id}/reply` | ✅ | See [Sending](#sending) |
| `/v0/inboxes/{inbox_id}/drafts/*` | ❌ 501 | |
| `/v0/inboxes/{inbox_id}/labels`, `/v0/pods/*`, `/v0/domains/*` | ❌ 501 | |
| `/v0/webhooks/*` | ❌ 501 | EventBridge is the delivery mechanism; see [Events](#events) |

Unknown `/v0` paths return AgentMail's 404 body. Unknown paths outside `/v0` keep Axum's
default 404, as today.

### Pagination

`page_token` is the base64url-encoded `LastEvaluatedKey`. It is not signed: the partition key is
always re-derived from the path's `inbox_id` and the request's `labels`, so a token from another
inbox or label can only ever yield an empty page, never another inbox's data. Default `limit`
20, maximum 100.

### Sending

`POST /v0/inboxes/{inbox_id}/messages/send` and `…/messages/{message_id}/reply` share one
implementation:

1. Validate: at least one of `to`/`cc`/`bcc`; at least one of `text`/`html`; total request ≤
   6 MB; every `attachments[].content` is base64 (`attachments[].url` returns `400 unsupported`
   in v1 — fetching arbitrary URLs from inside the function is SSRF surface, and the SDK path
   that needs it can pass a presigned URL later if we decide to allowlist the mail bucket).
2. For a reply: load the original; `In-Reply-To` = its RFC id; `References` = its references +
   its id; subject = `Re: <original>` unless already prefixed; `to` defaults to the original's
   `reply_to` or `from`, and `reply_all` adds the original `to`/`cc` minus this inbox.
3. Build the MIME with [`mail-builder`](https://crates.io/crates/mail-builder): `From` = inbox
   `display_name <inbox_id>`; user `headers` are merged last but cannot override `From`,
   `Message-ID`, `Date`, or `Return-Path`.
4. `SESv2 SendEmail` with `Content.Raw`, `ConfigurationSetName = SES_CONFIGURATION_SET`, and
   `FromEmailAddressIdentityArn` of the domain identity. A `MessageRejected` /
   `MailFromDomainNotVerified` / `AccountSendingPaused` is `400`/`403` with the SES message in
   `message`; throttling is `429`; anything else `502`.
5. Persist the sent message (labels `["sent"]`) into the mail table with its `sent` label
   pointers (message and thread) in the same transaction as ingest uses, and the raw MIME to
   `sent/<message_id>`, in the sender's inbox, in the original's thread for a reply.
6. Respond `{ message_id, thread_id }`. The mail-table insert makes the relay publish
   `message.sent`.

`track_opens` is accepted and ignored in v1 (SES open tracking is a configuration-set setting,
not per message); `message.opened` events still arrive if the config set has it on.

## Events

The mail table's stream is a second source for the existing relay. `stream.rs` distinguishes
the two tables by the `pk` prefix (`MSG#` vs `INBOX#`) and, for `INBOX#` message records,
builds an AgentMail-shaped detail. A `MODIFY` publishes only when a *delivery* label
(`delivered`, `bounced`, `complained`, `rejected`, `opened`) was added; a client's own label
edits through `PATCH` change the item but emit nothing, since AgentMail has no event for them.

```json
{
  "schemaVersion": 1,
  "meta": { "messageId": "…", "inboxId": "hello@mail.smoketurner.com", "threadId": "…" },
  "event_type": "message.received",
  "event_id": "evt_<ulid>",
  "message": { /* the AgentMail message object, minus text/html when over the size cap */ }
}
```

`detail-type` = `event_type`. The `event_type`/`event_id`/`message` triple **is** AgentMail's
webhook payload, so a consumer that wants an HTTP webhook uses an EventBridge API destination
with an input transformer selecting `$.detail` — no code, and the receiver sees what an
AgentMail webhook would have sent, plus the additive `schemaVersion`/`meta` block.

| Event | Emitted when |
|---|---|
| `message.received` / `.spam` / `.unauthenticated` | Mail-table message INSERT with label `received` |
| `message.sent` | Mail-table message INSERT with label `sent` |
| `message.delivered`, `message.bounced`, `message.complained`, `message.rejected`, `message.opened` | The existing `SesEvents` action, extended: when `mail.messageId` matches a mail-table message (a `GetItem` on the sender inbox recorded in the events table aggregate), it adds the label (`delivered`, `bounced`, …); the MODIFY on the mail-table stream publishes the event |
| `message.received.blocked` | Never in v1; there is no block list. Reserved |

The existing `ses.inbound`, `ses.delivery`, … events keep publishing unchanged from the events
table; consumers that only care about mailbox semantics filter on `message.*`. Both carry the
same `meta.messageId`.

The 256 KB `PutEvents` cap is handled the way `publish.rs` already handles inbound content:
drop `message.html`, then `message.text`, then fall back to `{ "payloadOmitted": true }` with
`meta` intact; the API is the fetch path.

## Observability

New metrics (same EMF namespace): `MessagesIngested`, `IngestFailures`, `IngestSkipped`,
`ApiRequests` (with `route` and `status` dimensions), `ApiAuthFailures`, `MessagesSent`,
`SendFailures`. Alarm on `IngestFailures` and `SendFailures`. The per-message INFO line gains
`inbox_id` and `thread_id`.

## Security notes

- The `/v0` surface is the first thing on this URL that *returns data*. Bearer keys are hashed
  at rest, compared in constant time, cached in memory only, and never logged.
- Presigned URLs are short-lived (`ATTACHMENT_URL_TTL_SECONDS`, default 15 min) and scoped to
  one object. `download_url` is the only way the API hands out S3 access.
- S3 pointers from a receipt are followed only when the bucket equals `MAIL_BUCKET`.
- Sending is scoped by IAM to `*@<MailDomain>` and the stack's configuration set; the API can
  only ever send as an inbox it stores.
- No WAF is attachable to a Function URL directly. If abuse becomes a concern, CloudFront in
  front (with the `/webhooks/*` paths bypassing auth) is the documented next step, not part of v1.

## Testing

- `tests/handlers.rs` grows a `/v0` section driving the real router with a fake key in a fake
  `ApiKeySource`; `FakeServices` implements the new `MailStore`, `ObjectStore`, `SesSend`, and
  `ApiKeySource` traits with in-memory maps.
- `tests/fixtures/mail/` holds `.eml` fixtures: plain text, multipart alternative, attachments
  with `Content-ID`, a reply with `References`, a 30 MB synthetic to pin memory use, and a
  malformed message to prove the permanent-failure path.
- Property tests: thread resolution never merges messages whose id chains do not intersect;
  `page_token` round-trips; every AgentMail response type serializes with exactly the documented
  field names (a golden JSON test per type).
- `cargo deny` gates the new crates: `aws-sdk-s3`, `aws-sdk-ssm`, `mail-parser`,
  `mail-builder`, `subtle`, `ulid` — all MIT/Apache, pinned per the workspace rule.
- `sam validate --lint` covers the template; a `cfn-lint` run with the mail parameters set and
  unset proves the `HasMailDomain` condition leaves the no-mail stack unchanged.

## Phasing

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Template: mail parameters, identity, DNS, bucket, topics, receipt rule, configuration set, mail table, IAM, stream mapping; README post-deploy steps | Mail to `hello@<staging host>` lands in S3 and a `ses.inbound` event carries `meta.s3` |
| 1 | Ingest action, mail table writes, `message.received*` on the bus | A reply email threads under its parent; a spam-verdict message publishes `message.received.spam` |
| 2 | Read API + auth: inboxes, threads, messages, raw, attachment, PATCH labels | The AgentMail Python SDK, pointed at the `ApiBaseUrl` output, lists threads and reads a message unmodified |
| 3 | Send + reply, `message.sent/delivered/bounced/complained/rejected/opened` | A reply sent through the API appears in the thread, and its SES delivery event adds `delivered` |
| 4 | Create/delete inbox, delete-as-trash, `extracted_text`, webhooks via API destinations guide | Optional; each independently shippable |

## Cutover runbook for `hello@mail.smoketurner.com`

1. Stand up phases 0–3 on `mail-staging.smoketurner.com` and run the SDK smoke test.
2. Lower the `mail.smoketurner.com` MX TTL to 300 s at least a day ahead.
3. Deploy the prod stack with `MailDomain=mail.smoketurner.com MailAddresses=hello`, activate
   the rule set, create the API-key parameter, and wait for the SES identity to show DKIM
   `SUCCESS` *before* touching MX — receiving and sending identities are the same domain, and an
   unverified identity cannot send.
4. Flip MX to `inbound-smtp.<region>.amazonaws.com`; replace AgentMail's DKIM/SPF records with
   SES's. From this moment AgentMail stops receiving; keep its inbox readable for history until
   the old thread context is no longer needed.
5. Point the agent at the `ApiBaseUrl` output with the key generated when the parameter was
   created. Thread ids and message ids are new; anything the agent stored about AgentMail ids
   is stale.
6. Watch `IngestFailures`, `SendFailures`, the two DLQs, and SES reputation metrics for a week,
   then restore the MX TTL.

## Open questions

1. **Ids.** This design uses the SES message id as `message_id` (URL-safe, joins the audit
   table, S3 key, and delivery events). AgentMail's ids are opaque; a client that assumes a
   specific format would break either way. Confirm, or prefer the RFC `Message-ID` (needs URL
   encoding and is sender-controlled).
2. **Catch-all vs explicit addresses** for `mail.smoketurner.com`, and whether `POST
   /v0/inboxes` (phase 4) matters for the workflow that replaces AgentMail.
3. **Bearer keys in Parameter Store** (SDK-compatible) vs. IAM SigV4 (nothing to rotate, but
   no SDK compatibility). The design assumes bearer.
4. **DNS ownership.** Is `smoketurner.com` in Route 53, so the stack can publish records via
   `HostedZoneId`, or does the stack only output them?
5. **SDK host override.** The official SDKs are Fern-generated; the Python client's constructor
   takes an `environment` (an enum whose `PROD` member is the AgentMail host) rather than a plain
   `base_url`. Fern clients generally accept a custom environment value or a `base_url`, but this
   must be verified against the pinned SDK versions (Python and TypeScript) before phase 2 is
   called done; the fallback is the clients' `httpx_client`/`fetch` hooks, which can rewrite the
   host.
6. **Same function or a second one.** Serving the API from the webhook function is the smallest
   change; a second function from the same binary (`MODE=api`) would isolate API latency and
   concurrency from SNS ingest. v1 assumes one function; the router split makes the second easy.
