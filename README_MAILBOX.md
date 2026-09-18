# Mailbox

Setting `pMailDomain` builds a mailbox on SES: inbound mail into DynamoDB and S3, a `/v0` API to
read and send it, and mailbox events on the same EventBridge bus. Leave `pMailDomain` empty and
none of these resources exist. [README.md](README.md) covers what runs either way.

- [Parameters](#parameters)
- [After the first deploy](#after-the-first-deploy)
- [Mailbox API](#mailbox-api)
- [Sending](#sending)
- [Open and click tracking](#open-and-click-tracking)
- [How the mailbox is wired](#how-the-mailbox-is-wired)
- [Mail metrics](#mail-metrics)
- [Disabling the mailbox](#disabling-the-mailbox)
- [Operator runbook](#operator-runbook)
- [Mail table](#mail-table)
- [Mailbox events](#mailbox-events)

Set `pMailDomain` and the stack creates:

- the SES identity and configuration set;
- a receipt rule storing inbound mail in S3;
- the mail bucket and mail table;
- two SNS topics, already subscribed to the function;
- the DNS records, when `pHostedZoneId` is set.

Empty `pMailDomain` is the default and creates none of it. A mailbox stack also runs the
function with a 90 s timeout and 512 MB, enough to parse a 40 MB message in one invocation.
Without a mailbox it stays at 10 s and 256 MB.

> [!IMPORTANT]
> **Region.** SES receives email in some regions only. Deploy a mailbox stack where the
> [SES endpoints list](https://docs.aws.amazon.com/general/latest/gr/ses.html) shows an email
> receiving endpoint. Elsewhere the receipt rule set cannot be created.

```bash
sam deploy --parameter-overrides \
  "pAllowedTopics=<your-account-id> pMailDomain=mail.example.com pMailInbox=hello \
   pApiKeysParameterName=/messaging-webhook/dev/api-keys pHostedZoneId=<zone-id>"
```

## Parameters

| Parameter | Default | Notes |
|---|---|---|
| `pMailDomain` | *(empty)* | Receiving domain and sending identity, e.g. `mail.example.com`. Empty disables every mail resource |
| `pMailInbox` | *(empty)* | Local part of the one inbox, e.g. `hello` for `hello@<pMailDomain>`. Required when `pMailDomain` is set; the receipt rule accepts only that address |
| `pMailBucketName` | *(empty)* | Empty lets CloudFormation generate the bucket name |
| `pMailRetentionDays` | `365` | How long a message is kept: S3 expiration for raw MIME, attachments and message content, and the TTL on its mail table items |
| `pHostedZoneId` | *(empty)* | Route 53 zone for the domain. Set it and the stack publishes the DNS records; leave it empty and the `DnsRecords` output lists them |
| `pDmarcPolicy` | `quarantine` | `none`, `quarantine` or `reject` in the `_dmarc` record |
| `pMailTrackingDomain` | *(empty)* | Subdomain SES wraps open and click tracking links in, e.g. `click.mail.example.com`. Empty leaves them on SES's `awstrack.me`; opens and clicks are reported either way. See [Open and click tracking](#open-and-click-tracking) |
| `pMailTrackingHttpsPolicy` | `OPTIONAL` | `OPTIONAL`, `REQUIRE` or `REQUIRE_OPEN_ONLY`. The two HTTPS values need a CDN in front of the subdomain |
| `pMailTrackingCnameTarget` | *(empty)* | Host the tracking subdomain's CNAME points at. Empty uses `r.<region>.awstrack.me`; set it to your CDN's hostname |
| `pReceiptTlsPolicy` | `Optional` | `Require` rejects inbound mail that wasn't delivered over TLS |
| `pExistingReceiptRuleSetName` | *(empty)* | Empty creates a rule set. Set it to add the rule to a rule set that is already active in the region |
| `pApiKeysParameterName` | *(empty)* | Name of the SecureString SSM parameter holding the API key hashes. Required when `pMailDomain` is set. Must start with `/`, and not with `/aws` or `/ssm`, which SSM reserves |
| `pApiKeysKmsKeyArn` | *(empty)* | Customer-managed KMS key that encrypts that parameter; empty means `aws/ssm` |
| `pAttachmentUrlTtlSeconds` | `900` | Lifetime of presigned download URLs, 60–3600 |

## After the first deploy

CloudFormation cannot do these four. Run them once, using this helper:

```bash
stack=aws-messaging-webhook-dev
output() { aws cloudformation describe-stacks --stack-name "$stack" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
```

1. **Publish DNS** (skip if you set `pHostedZoneId`). `output DnsRecords` lists the records:
   - the domain's MX to `inbound-smtp.<region>.amazonaws.com`;
   - three DomainKeys Identified Mail (DKIM) CNAMEs;
   - the MAIL FROM domain `bounce.<domain>`, with an MX to `feedback-smtp.<region>.amazonses.com`
     and a Sender Policy Framework (SPF) record, `v=spf1 include:amazonses.com ~all`;
   - the domain's SPF record, `v=spf1 include:amazonses.com -all`;
   - `_dmarc.<domain>`, the Domain-based Message Authentication, Reporting and Conformance
     (DMARC) policy.

   With `pMailTrackingDomain` set, `output TrackingDnsRecord` gives one more: the tracking
   subdomain's CNAME. It is a separate output because where it points depends on whether a CDN
   fronts the subdomain.
2. **Wait for DKIM `SUCCESS`** before sending mail from the identity:

   ```bash
   aws sesv2 get-email-identity --email-identity mail.example.com \
     --query '{dkim: DkimAttributes.Status, mailFrom: MailFromAttributes.MailFromDomainStatus}'
   ```

3. **Activate the receipt rule set.** A region has one active rule set, so activating this one
   deactivates any other. If one is already active, redeploy with `pExistingReceiptRuleSetName`
   set to its name and the stack adds only its rule.

   Inbound mail stops whenever the stack's rule set is not the active one, including after a
   deploy that replaces it, and nothing reports it: SES never calls the function. Check with
   `aws ses describe-active-receipt-rule-set`.

   ```bash
   aws ses set-active-receipt-rule-set --rule-set-name "$(output ReceiptRuleSetName)"
   ```

4. **Create the API key parameter.** CloudFormation cannot create a Systems Manager (SSM)
   SecureString parameter.
   Name it exactly `pApiKeysParameterName`, starting with `/`. The value holds SHA-256 hashes,
   never the keys:

   ```bash
   key="am_$(openssl rand -hex 24)"
   hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
   aws ssm put-parameter --name /messaging-webhook/dev/api-keys --type SecureString \
     --value "{\"keys\":[{\"id\":\"key_1\",\"sha256\":\"$hash\"}]}"
   echo "$key"   # hand this to the client; it isn't stored anywhere
   ```

   Add `--key-id <pApiKeysKmsKeyArn>` for a customer-managed Key Management Service (KMS) key. Rotate by writing both
   entries, moving clients to the new key, then removing the old entry.

The API is served under the `ApiBaseUrl` output. `InboxAddress` is the inbox's address and its
`inbox_id`, for example `/v0/inboxes/hello@mail.example.com/messages`.

## Mailbox API

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

## Sending

`POST …/messages/send` takes `to`/`cc`/`bcc` (one address or a list), `subject`, `text` and/or
`html`, and optionally `reply_to`, `labels`, `headers` and `attachments`. Each attachment gives
exactly one of `content` (base64) or `url`.

A send or reply body may reach 6 MiB, the Function URL's limit; every other route takes 1 MiB.
Base64 inflates inline content by 33%, so send attachments over 4.5 MiB by `url`.

**It queues; it does not send.** The response means the message is durably recorded, not that
SES took it. A separate sender consumes the mail table's stream and makes the SES call. SESv2
`SendEmail` has no idempotency token and the AWS SDKs retry 5xx themselves, so a synchronous
send could deliver the same mail twice.

Send an `Idempotency-Key` to make a retry safe:

- The same key with the same request returns the original ids.
- The same key with any difference — a field, an attachment's content or metadata, a different
  route or original message — is a `409`. The alternative is sending mail the caller did not
  ask for.
- Keys are remembered for 24 hours. Without one, a retry after a lost response queues the
  message twice.

These headers are refused rather than ignored, so a caller cannot send as another inbox:
`From`, `Sender`, `To`, `Cc`, `Bcc`, `Reply-To`, `Subject`, `Date`, `Message-ID`,
`In-Reply-To`, `References`, `Return-Path`, `MIME-Version` and every `Content-*`.

A CR or LF in an address, header name or header value is refused too. All three end up in a
MIME document, where a newline starts a new header.

`…/reply` takes the same body plus `reply_all`. It derives what you leave out from the message
it answers: the thread, `In-Reply-To`, the `References` chain, a `Re:` subject (never doubled),
and the recipient. The recipient is the original's `Reply-To`, or its sender.

`reply_all` copies the original's other recipients, minus this inbox, so a reply never arrives
back where it came from. Naming `to`, `cc` or `bcc` overrides the derived recipients and keeps
the threading.

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
it. The sweep takes claims older than fifteen minutes:

- a sender that never reached SES releases its claim for another attempt;
- a sender that recorded it was about to call SES moves to `unknown`, since the message may
  already have gone out.

Fifteen minutes is 7.5 times the sender's own 120-second timeout. Taking a send from a sender
still working on it is how mail gets delivered twice. `unknown` sends are counted and left for
an operator.

SES events on the configuration set then label the message `delivered`, `bounced`,
`complained`, `rejected` or `opened`. Labels are added, never removed: these events arrive out
of order, and a message that both bounced and was opened should say both. An event for mail
this service did not send resolves to nothing, which is the ordinary case on a shared
configuration set.

## Open and click tracking

Both are on, unconditionally: the configuration set matches every event type SES defines, and
every send names it. SES appends a 1×1 transparent pixel to the `html` body — fetching it is an
`Open` — and rewrites each link into a redirect it counts as a `Click`. A text-only message is
tracked for neither: both work by changing the HTML.

What `pMailTrackingDomain` changes is whose domain those links wear, not whether they are
reported. Out of the box they are served from SES's own `awstrack.me`, visible to the recipient
in the status bar of every link they hover. Set the parameter and they wear a subdomain of
yours instead:

```bash
sam deploy --parameter-overrides "… pMailTrackingDomain=click.mail.example.com"
```

The subdomain is a CNAME to SES's regional tracking host, `r.<region>.awstrack.me` — SES still
serves the redirects, under your name. The stack publishes the record when `pHostedZoneId` is
set, and prints it as `TrackingDnsRecord` when it isn't. A subdomain of `pMailDomain` is covered
by that domain's identity and needs no verification of its own; any other domain must already be
a verified SES identity. A dedicated subdomain per sending region is what SES recommends.

`pMailTrackingHttpsPolicy` is `OPTIONAL` by default: the pixel loads over HTTP and each click
link keeps the scheme the original link had. `REQUIRE` and `REQUIRE_OPEN_ONLY` wrap links in
HTTPS, which a bare CNAME cannot serve — a certificate for your subdomain has to exist
somewhere. Put a CDN holding one in front, with `r.<region>.awstrack.me` as its origin and the
`Host` header forwarded, and set `pMailTrackingCnameTarget` to the CDN's hostname so the record
points there instead. Check it with
`curl --head https://click.mail.example.com/favicon.ico`: the response carries
`x-amz-ses-region` and `x-amz-ses-request-protocol`.

Two markers in the HTML steer tracking per message. SES acts on both and removes them before
the message goes out:

- `{{ses:openTracker}}` anywhere in the `html` body puts the pixel there instead of at the end,
  where a client that clips long messages may never load it. One per message — a second is a
  `400` from SES, which the sender records as a failed send.
- `<a ses:no-track href="…">` leaves that one link alone. SES rewrites at most 250 links in a
  message and skips any URL that isn't RFC 3986-encoded.

Opens and clicks reach the webhook over `MailEventsTopicArn` like every other sending event, and
land on the bus as `ses.open` and `ses.click`. They accrue to `open_count` and `click_count` on
the message's aggregate, or to `bot_open_count`/`bot_click_count` when SES flags the interaction
`isBotEvent=Likely` — Apple Mail Privacy Protection prefetches and scanners. An `Open` also
labels the mailbox message `opened`; a click adds no label.

The event destination matches all ten SES event types: `send`, `reject`, `bounce`, `complaint`,
`delivery`, `open`, `click`, `renderingFailure`, `deliveryDelay` and `subscription`. A type left
out here is the one way an event is lost outright, so none is. The last three carry no aggregate
status rule and no mailbox label — they are persisted and forwarded as `ses.rendering-failure`,
`ses.delivery-delay` and `ses.subscription`, which is the pass-through the pipeline is built to
degrade to.

## How the mailbox is wired

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

## Mail metrics

Ingest emits `MessagesIngested`, `IngestFailures`, `IngestSkipped` and `IngestTimeouts` as
CloudWatch Embedded Metrics Format (EMF) in the stack-name namespace, each with a `function`
dimension. The stack defines no alarms.
Build them on these metrics and on the `rAsyncInvokeDlq` and `rPublishDlq` queue depths.

## Disabling the mailbox

SES refuses to delete the active rule set, so clearing `pMailDomain` fails while the stack's
rule set is active. Deactivate it first:

```bash
aws ses set-active-receipt-rule-set   # no name: deactivates the active rule set
```

With `pExistingReceiptRuleSetName`, only the stack's rule goes and no deactivation is needed,
but mail to the mailbox then matches no rule. The bucket and table are retained; delete them by
hand if you don't want them. Re-enabling the mailbox with the same explicit `pMailBucketName`
needs the retained bucket deleted first.

## Operator runbook

Three situations need a person. The first two invoke the sender directly rather than going
through the API. Each one resends or closes a message that SES may already hold, so
`lambda:InvokeFunction` gates them instead of a bearer key.

**A send whose outcome is `unknown`.** SES was called and gave no usable answer, or the sender
died after calling it. SES may or may not hold the message. Nothing automatic touches it:
resending risks delivering twice. Find them in the sweep's `sends_outcome_unknown` log
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
replay of a partial attempt resolves to the same ids. The replay re-verifies the signature, so
it works while SES still serves the signing certificate. The queue holds the message 14 days.

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

## Mail table

A mailbox stack creates a second table, `MailTableName`. It is a separate partition space from the events table, keyed by
inbox and message rather than by SNS message id:

| Item | `pk` | `sk` | Holds |
|---|---|---|---|
| Inbox | `INBOX#<inbox>` | `META` | email, display name, metadata, timestamps |
| Message | `INBOX#<inbox>` | `MSG#<messageId>` | addresses, subject, preview, labels, attachment metadata, send status; the body and headers are in S3 |
| Thread | `INBOX#<inbox>` | `THR#<threadId>` | rolled-up subject/preview/senders/recipients/labels, message count, size, newest attachments |
| RFC alias | `RFC#<inbox>#<rfc-id>` | `RFC` | maps an inbound or outbound `Message-ID` to the message/thread it belongs to, for reply threading |

Labels live on the message and thread items, not in per-label index rows. A label-filtered list
reads the time-ordered index and filters the page. Ingest stays at three writes per message. The
cost is reading past non-matching messages, which grows as a label gets less common.

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

## Mailbox events

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
SES event adds that label.

Each carries the same `schemaVersion`, `meta`, `type`, `event_type` and `event_id`. `message` is
in its list form: identifiers, labels, addresses, subject and preview, no body. The consumer has
the body from the message's own event or the read API.

Delivery labels are added, never removed, so a message that bounced and was opened publishes
both. A label arriving twice — an SES event redelivered, a stream record replayed — rebuilds the
same `event_id`, which is what a consumer deduplicates on.

The mail stream publishes nothing for:

- an unrecognized payload;
- a write to anything but a message item — send state, markers, keys, RFC aliases, thread
  housekeeping;
- a message write adding no system label, such as a read receipt, your own label or a metadata
  write.

