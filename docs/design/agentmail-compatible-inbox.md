# Design: AgentMail-compatible inbox on SES

Status: **proposal** — nothing in this document is implemented yet.

## Goal

Replace an [AgentMail](https://agentmail.to) inbox (concretely `hello@mail.smoketurner.com`)
with this project, so that:

1. A Terraform module takes a **hostname** and one or more **inbound addresses** and provisions
   everything AWS-side: the SES domain identity, DNS, an S3 bucket for raw mail, the receipt
   rule set, the SNS topic, and the wiring into the webhook.
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
                          Terraform module `ses-inbox`
   ┌───────────────────────────────────────────────────────────────────────┐
   │ MX/DKIM/SPF/DMARC ─► SES receipt rule ─► S3 (raw MIME)                │
   │                                      └─► SNS topic ─► webhook          │
   │ SESv2 configuration set (outbound) ─► SNS topic ─► webhook             │
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

## Terraform module: `terraform/modules/ses-inbox`

Lives in this repo (a sibling of `template.yaml`), and keeps the existing rule that SNS topics
and subscriptions live *outside* the SAM stack: the module owns the mail-side resources and
reads the SAM stack's outputs to wire them in.

### Inputs

| Variable | Example | Notes |
|---|---|---|
| `hostname` | `mail.smoketurner.com` | The receiving domain; also the sending identity |
| `addresses` | `["hello"]` | Local parts (or full addresses) to receive; each becomes an inbox |
| `catch_all` | `false` | Receive `*@hostname`; unknown recipients are still stored (see [Inbox resolution](#inbox-resolution)) |
| `bucket_name` | `smoketurner-mail` | Raw MIME + extracted attachments; must match the SAM `MailBucketName` parameter |
| `webhook_stack_name` | `aws-messaging-webhook-prod` | Read via `data.aws_cloudformation_stack` for the function ARN/URL and mail table name |
| `subscription_protocol` | `lambda` | `lambda` (recommended for ingest, see [Timeouts](#timeouts-and-the-delivery-pathway)) or `https` |
| `route53_zone_id` | `Z…` | Optional; when set, the module publishes MX, DKIM CNAMEs, MAIL FROM, SPF and DMARC records |
| `dmarc_policy` | `quarantine` | `none` / `quarantine` / `reject` for the published `_dmarc` TXT |
| `retention_days` | `365` | S3 lifecycle expiration for raw MIME and attachments |
| `tls_policy` | `Optional` | Receipt rule TLS policy; `Require` rejects plaintext SMTP senders |

### Resources

- `aws_sesv2_email_identity` for `hostname` with Easy DKIM, plus
  `aws_sesv2_email_identity_mail_from_attributes` (`bounce.<hostname>`) so bounces return to SES.
- `aws_s3_bucket` (`bucket_name`): public access blocked, SSE-S3 (or a KMS key input), versioning
  off, lifecycle expiration at `retention_days`, and a bucket policy that lets `ses.amazonaws.com`
  `PutObject` under `inbound/*` only when `aws:SourceAccount` is this account and `aws:SourceArn`
  is the receipt rule.
- `aws_sns_topic` `inbound` with `SignatureVersion = 2` and a policy allowing `ses.amazonaws.com`
  to publish from this account.
- `aws_ses_receipt_rule_set` + `aws_ses_active_receipt_rule_set` + `aws_ses_receipt_rule`:
  recipients = the full addresses (or `hostname` when `catch_all`), `scan_enabled = true`, one
  `s3_action { bucket_name, object_key_prefix = "inbound/", topic_arn }`. The S3 action's own
  `topic_arn` is what produces the receipt notification with `receipt.action.type = "S3"` and the
  bucket/key pointer that `model/ses_inbound.rs` already parses.
- Subscription to the webhook. `lambda` protocol: `aws_sns_topic_subscription` plus
  `aws_lambda_permission` scoped with `source_arn` to the topic (the per-topic gate the README
  describes). `https` protocol: subscribe to the stack's `SesInboundEndpoint`; the function
  auto-confirms.
- Outbound: `aws_sesv2_configuration_set` (name = an input, default `<stack>-mail`) with an
  event destination for `SEND, DELIVERY, BOUNCE, COMPLAINT, REJECT, OPEN` to a second SNS topic
  `events`, subscribed to the stack's `SesEventsEndpoint`. This is what turns SES delivery
  events for sent mail into `message.delivered` / `message.bounced` / … (see [Events](#events)).
- One `aws_dynamodb_table_item` per address in the mail table: the inbox record. Terraform is the
  inbox control plane in v1; `POST /v0/inboxes` is a later phase.
- Route 53 records when `route53_zone_id` is set: `MX 10 inbound-smtp.<region>.amazonaws.com`,
  three DKIM CNAMEs, MAIL FROM `MX`/`TXT`, `TXT "v=spf1 include:amazonses.com -all"`, and
  `_dmarc TXT`. Without a zone id the module outputs the records for manual publication.

Two constraints to state in the module README: SES email receiving is only offered in a subset of
regions and there is exactly **one active receipt rule set per region** — the module must be
told when to adopt an existing rule set instead of creating and activating a new one
(`existing_rule_set_name` input).

### Outputs

`bucket_name`, `inbound_topic_arn`, `events_topic_arn`, `configuration_set_name`, `inbox_ids`
(`["hello@mail.smoketurner.com"]`), `dns_records` (for the no-Route-53 case), `api_base_url`
(`<FunctionUrl>v0`).

### Ordering

1. `sam deploy` with the new parameters (below). IAM statements name the bucket and domain by
   string, so the bucket does not need to exist yet.
2. `terraform apply` the module; it reads the stack outputs, creates the mail resources, and
   subscribes them. The topics are in the same account, so the existing `AllowedTopics` account
   id entry already admits them.

## SAM template changes

New parameters, all mapped to env vars read by `config.rs`:

| Parameter | Env var | Purpose |
|---|---|---|
| `MailBucketName` | `MAIL_BUCKET` | Grants `s3:GetObject` on `inbound/*` and `s3:PutObject`/`GetObject` on `attachments/*`, `sent/*` of `arn:aws:s3:::<bucket>` |
| `MailDomains` | `MAIL_DOMAINS` | Comma-separated hostnames the API may send from and create inboxes under; scopes `ses:SendEmail`/`ses:SendRawEmail` with a `ses:FromAddress` `StringLike` condition of `*@<domain>` |
| `SesConfigurationSet` | `SES_CONFIGURATION_SET` | Config set stamped on every send; the IAM resource list includes its ARN |
| `AttachmentUrlTtlSeconds` | `ATTACHMENT_URL_TTL_SECONDS` | Presigned URL lifetime (default 900) |

New resources:

- **`MailTable`** (`AWS::DynamoDB::Table`, on-demand, `pk`/`sk`, three GSIs below, stream
  `NEW_AND_OLD_IMAGES`, deletion protection in prod, **no TTL** — mailbox data is not an audit
  buffer; retention is the operator's S3 lifecycle plus explicit deletes).
- **`ApiKeysSecret`** (`AWS::SecretsManager::Secret`) seeded with one generated key. Value is a
  JSON document `{"keys":[{"id":"key_…","sha256":"…","created_at":"…"}]}`; the function is
  granted `secretsmanager:GetSecretValue` on it only. Operators add a second key, roll clients,
  then remove the first.
- A second **stream event-source mapping** on `MailTable` into the same function, same settings
  as the events-table one (bisect, `ReportBatchItemFailures`, on-failure DLQ). Filter to
  `INSERT` and to `MODIFY` where the image's `sk` begins with `MSG#` (label/status changes).
- Function `Timeout` raised from 10 to **60 s** (ingest of a 40 MB message; see below) and
  `MemorySize` to 512 to give the parser headroom.

New outputs: `MailTableName`, `ApiBaseUrl` (`<FunctionUrl>v0`), `ApiKeysSecretArn`.

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

Global secondary indexes (all project `ALL`; the table is small and reads dominate):

| Index | Key | Serves |
|---|---|---|
| `ByTime` | `gsi1pk = INBOX#<inbox_id>#MSG` or `#THR`, `gsi1sk = <timestamp>#<id>` | List messages / list threads with `before`/`after`/`ascending` as a sort-key range, `page_token` = base64 of `LastEvaluatedKey` |
| `ByThread` | `gsi2pk = THREAD#<inbox_id>#<thread_id>`, `gsi2sk = <timestamp>#<message_id>` | Get thread: the embedded `messages` array |
| `ByRfcId` | `gsi3pk = RFC#<inbox_id>#<rfc-message-id>` | Thread resolution from `In-Reply-To` / `References` |

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
   message_count :one`, `ADD senders/recipients`, `SET last_message_id, timestamp, size` guarded
   by `timestamp >= if_not_exists(...)`, `SET subject/preview = if_not_exists(...)`). A
   `ConditionalCheckFailed` on the put is the idempotency signal (`Duplicate`): the whole
   transaction cancels, the thread is not double-counted, and the action returns `Ok`.

Failure classification follows `ActionErrorKind`: S3/DynamoDB throttling, 5xx, and timeouts are
`Transient` (→ 5xx → SNS redelivery re-runs the idempotent ingest); a MIME that fails to parse,
an object that is missing, or an unknown recipient with `catch_all` off are `Permanent` (log,
`IngestFailures` metric, the `ses.inbound` event still publishes with the S3 pointer so nothing
is lost).

Ingest is a *new* `Services` sub-trait, `MailStore + ObjectStore`, so the handler tests keep
running with `FakeServices` and MIME fixtures (`tests/fixtures/mail/*.eml`).

### Inbox resolution

- Recipient has an inbox item → ingest into it.
- Recipient has no inbox item and `catch_all` is off → the receipt rule would not have matched,
  so this only happens if the operator edited the rule by hand; treat as permanent skip.
- Recipient has no inbox item and `MAIL_AUTO_CREATE_INBOXES=true` (default `false`) → create the
  inbox item and ingest. Off by default so a catch-all domain does not become an unbounded
  inbox factory for spam.
- Otherwise → skip with `unknown_inbox`, and the message stays discoverable through the
  `ses.inbound` event and the raw object.

### Timeouts and the delivery pathway

A 40 MB message (the SES receiving ceiling) means one `GetObject`, a parse, and several
`PutObject`s inside the request. Over HTTPS, SNS applies a short, fixed response timeout to the
endpoint (AWS documents it as 15 seconds; it is not configurable) before it counts the delivery
as failed and retries, which would make a slow ingest look like a failure loop. The module
therefore defaults inbox topics to the **`lambda` protocol** (async invoke, no response
timeout, function timeout 60 s), with an on-failure SQS destination configured on the function
for durability beyond the async queue's two retries. The `https` option remains for operators
who want the longer SNS retry policy and can accept the ceiling.

## The `/v0` API

Mounted on the existing router under `/v0`, behind a bearer-auth middleware. Same Function URL
as the webhook paths (one URL per function). Request bodies on the send/reply routes are limited
to **6 MB** (the Function URL payload ceiling, which happens to equal AgentMail's documented
limit); the 1 MiB `DefaultBodyLimit` stays on every other route.

### Authentication

`Authorization: Bearer <key>`. The middleware SHA-256-hashes the presented key and compares it
in constant time (`subtle::ConstantTimeEq`) against every hash in the cached secret. The cache
refreshes every 5 minutes and on any miss (so a freshly added key works immediately at the cost
of one `GetSecretValue`). Missing or wrong key → `401` with the AgentMail error body. The header
is redacted from `TraceLayer` logs. Keys never appear in logs or metrics; `ApiAuthFailures` is a
counter only.

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
| `POST /v0/inboxes` | phase 4 | `username` + `domain ∈ MAIL_DOMAINS` → inbox item; requires `catch_all` on the domain |
| `DELETE /v0/inboxes/{inbox_id}` | phase 4 | |
| `GET /v0/inboxes/{inbox_id}/threads` | ✅ | `limit`, `page_token`, `labels`, `before`, `after`, `ascending`, `include_spam`, `include_blocked`, `include_unauthenticated`, `include_trash`, `senders`, `recipients`, `subject` (the last three as post-filters) |
| `GET /v0/inboxes/{inbox_id}/threads/{thread_id}` | ✅ | Embedded `messages[]` in ascending order from `ByThread`; thread-level `count`/`limit`/`next_page_token` for long threads |
| `DELETE /v0/inboxes/{inbox_id}/threads/{thread_id}` | phase 4 | |
| `GET /v0/inboxes/{inbox_id}/messages` | ✅ | Same filters as threads plus `from`/`to`/`subject` post-filters |
| `GET /v0/inboxes/{inbox_id}/messages/{message_id}` | ✅ | Full message: `text`, `html`, `headers`, `attachments`, `in_reply_to`, `references`; `extracted_text`/`extracted_html` absent in v1 (phase 4: quote stripping) |
| `GET …/messages/{message_id}/raw` | ✅ | `{ message_id, size, download_url, expires_at }` — presigned `GetObject` on the raw key |
| `GET …/messages/{message_id}/attachments/{attachment_id}` | ✅ | `{ attachment_id, size, filename, content_type, content_disposition, content_id, download_url, expires_at }` — presigned GET with `response-content-disposition` |
| `PATCH …/messages/{message_id}` | ✅ | `add_labels` / `remove_labels` → `ADD`/`DELETE` on the string set; response `{ message_id, labels }`. Removing `unread` is how a client marks read |
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
always re-derived from the path's `inbox_id`, so a token from another inbox can only ever yield
an empty page, never another inbox's data. Default `limit` 20, maximum 100.

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
5. Persist the sent message (labels `["sent"]`) into the mail table and the raw MIME to
   `sent/<message_id>`, in the sender's inbox, in the original's thread for a reply.
6. Respond `{ message_id, thread_id }`. The mail-table insert makes the relay publish
   `message.sent`.

`track_opens` is accepted and ignored in v1 (SES open tracking is a configuration-set setting,
not per message); `message.opened` events still arrive if the config set has it on.

## Events

The mail table's stream is a second source for the existing relay. `stream.rs` distinguishes
the two tables by the `pk` prefix (`MSG#` vs `INBOX#`) and, for `INBOX#` records, builds an
AgentMail-shaped detail:

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
- Sending is scoped by IAM to `*@<MAIL_DOMAINS>` and the named configuration set; the API can
  only ever send as an inbox it stores.
- No WAF is attachable to a Function URL directly. If abuse becomes a concern, CloudFront in
  front (with the `/webhooks/*` paths bypassing auth) is the documented next step, not part of v1.

## Testing

- `tests/handlers.rs` grows a `/v0` section driving the real router with a fake key in a fake
  `SecretsProvider`; `FakeServices` implements the new `MailStore`, `ObjectStore`,
  `SesSend`, and `SecretsProvider` traits with in-memory maps.
- `tests/fixtures/mail/` holds `.eml` fixtures: plain text, multipart alternative, attachments
  with `Content-ID`, a reply with `References`, a 30 MB synthetic to pin memory use, and a
  malformed message to prove the permanent-failure path.
- Property tests: thread resolution never merges messages whose id chains do not intersect;
  `page_token` round-trips; every AgentMail response type serializes with exactly the documented
  field names (a golden JSON test per type).
- `cargo deny` gates the new crates: `aws-sdk-s3`, `aws-sdk-secretsmanager`, `mail-parser`,
  `mail-builder`, `subtle`, `ulid` — all MIT/Apache, pinned per the workspace rule.

## Phasing

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Terraform module + SAM parameters/resources (mail table, secret, IAM, stream mapping) | Mail to `hello@<staging host>` lands in S3 and a `ses.inbound` event carries `meta.s3` |
| 1 | Ingest action, mail table writes, `message.received*` on the bus | A reply email threads under its parent; a spam-verdict message publishes `message.received.spam` |
| 2 | Read API + auth: inboxes, threads, messages, raw, attachment, PATCH labels | The AgentMail Python SDK, pointed at `api_base_url`, lists threads and reads a message unmodified |
| 3 | Send + reply, `message.sent/delivered/bounced/complained/rejected/opened` | A reply sent through the API appears in the thread, and its SES delivery event adds `delivered` |
| 4 | Create/delete inbox, delete-as-trash, `extracted_text`, webhooks via API destinations guide | Optional; each independently shippable |

## Cutover runbook for `hello@mail.smoketurner.com`

1. Stand up phases 0–3 on `mail-staging.smoketurner.com` and run the SDK smoke test.
2. Lower the `mail.smoketurner.com` MX TTL to 300 s at least a day ahead.
3. Deploy the module for `mail.smoketurner.com` with `addresses = ["hello"]`; verify the SES
   identity (DKIM `SUCCESS`) *before* touching MX — receiving and sending identities are the
   same domain, and an unverified identity cannot send.
4. Flip MX to `inbound-smtp.<region>.amazonaws.com`; replace AgentMail's DKIM/SPF records with
   SES's. From this moment AgentMail stops receiving; keep its inbox readable for history until
   the old thread context is no longer needed.
5. Point the agent at `api_base_url` with a key from `ApiKeysSecret`. Thread ids and message ids
   are new; anything the agent stored about AgentMail ids is stale.
6. Watch `IngestFailures`, `SendFailures`, the two DLQs, and SES reputation metrics for a week,
   then restore the MX TTL.

## Open questions

1. **Ids.** This design uses the SES message id as `message_id` (URL-safe, joins the audit
   table, S3 key, and delivery events). AgentMail's ids are opaque; a client that assumes a
   specific format would break either way. Confirm, or prefer the RFC `Message-ID` (needs URL
   encoding and is sender-controlled).
2. **Catch-all vs explicit addresses** for `mail.smoketurner.com`, and whether `POST
   /v0/inboxes` (phase 4) matters for the workflow that replaces AgentMail.
3. **API keys in Secrets Manager** (bearer, SDK-compatible) vs. IAM SigV4 (no secret to
   rotate, but no SDK compatibility). The design assumes bearer.
4. **DNS ownership.** Is `smoketurner.com` in Route 53, so the module can publish records, or
   does the module only output them?
5. **SDK host override.** The official SDKs are Fern-generated; the Python client's constructor
   takes an `environment` (an enum whose `PROD` member is the AgentMail host) rather than a plain
   `base_url`. Fern clients generally accept a custom environment value or a `base_url`, but this
   must be verified against the pinned SDK versions (Python and TypeScript) before phase 2 is
   called done; the fallback is the clients' `httpx_client`/`fetch` hooks, which can rewrite the
   host.
6. **Same function or a second one.** Serving the API from the webhook function is the smallest
   change; a second function from the same binary (`MODE=api`) would isolate API latency and
   concurrency from SNS ingest. v1 assumes one function; the router split makes the second easy.
