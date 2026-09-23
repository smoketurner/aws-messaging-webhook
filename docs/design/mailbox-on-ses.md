# Design: a mailbox on SES

This doc explains why the mailbox is built the way it is. For how to run it, see
[docs/mailbox/](../mailbox/setup.md).

## Goal

Give an agent a mailbox with one `sam deploy`: receive mail at a domain, read and label it over an
authenticated API, send and reply, and publish events for each step.

The HTTP API mirrors a third-party mailbox API's `/v0` contract (paths, field names, error
bodies). A client written for that contract works here after changing the base URL and key. The
contract constrains the wire format only; storage and processing are this project's own design.

## Tenets

1. **Never send the same mail twice.** A lost or stuck send waits for an operator rather than
   risking a duplicate.
2. **Never lose inbound mail.** A message that can't be parsed still publishes `ses.inbound` with
   its S3 pointer.
3. **Every step is safe to repeat.** SNS, DynamoDB Streams and EventBridge all deliver at least
   once.

## Architecture

The mailbox extends the existing **verify → persist → act → publish** pipeline. Inbound mail
already arrived as an SES receipt over SNS. The mailbox adds an ingest action, a mail table with
its own stream, the `/v0` API, and a sender function.

```
 SES receipt ─► S3 (raw MIME) + SNS ─► webhook fn: verify ─► persist ─► ingest
                                                                          │
 Agent ─► /v0 API (webhook fn) ─► mail table ◄────────────────────────────┘
                                      │ stream
                     ┌────────────────┴────────────────┐
              relay (webhook fn)                 sender fn
                     │                           claim ► build ► SES ► record
                     ▼
              EventBridge message.*
```

Both functions run the same binary, selected by `FUNCTION_MODE`. The sender is a separate
function so its concurrency, memory and IAM permissions cover sending only. A slow send can't
tie up the function serving the API.

## Decision: sending is queued, not synchronous

The API writes the send to the mail table and returns. The sender function reads the stream and
calls SES.

**Why:** SESv2 `SendEmail` has no idempotency token, and AWS SDKs retry 5xx responses
automatically. A synchronous send could turn one API request into three deliveries.

Four mechanisms keep each send to at most one delivery:

1. **Claim.** A conditional write moves the send from `queued` to `sending`. When the stream
   delivers a record twice, one sender wins and the other stops.
2. **SES-call mark.** The sender records `ses_call_at` just before calling SES. A stale claim
   without the mark was never sent and can be released. A claim with the mark may have been sent,
   so it never retries automatically.
3. **No SDK retries on the send call.** The sender retries only 429 and 503, which mean SES
   didn't accept the request. A 500, 502 or 504 can follow an accepted message.
4. **Ambiguous outcomes stop.** A timeout, dropped connection or ambiguous 5xx marks the send
   `unknown`. Only an operator resolves it. Treating `unknown` as `failed` would eventually cause
   a duplicate.

A transient failure (SES throttling, S3 or an attachment host briefly down) hands the send back
with `requeued_at` set, which re-triggers the sender. The sender backs off from 1 to 16 seconds.
After five hand-backs the send fails. A permanent failure (access denied, object too large, host
rejecting the request) fails the send at once.

The sender must finish loading the send 35 seconds before its deadline. That leaves room for the
SES call (capped at 20 seconds) and recording the result.

A sweep runs every 10 minutes for sends stuck in `sending` longer than 15 minutes, 7.5 times the
sender's 120-second timeout. The margin matters: taking a send from a live sender causes a
duplicate.

`Idempotency-Key` is stored as a hash, with a fingerprint of the request. A reused key with a
different request returns `409`, because sending something the caller didn't ask for is worse
than refusing.

## Decision: message content lives in S3

Bodies, headers, `References`, `Reply-To` and verdicts go to one S3 object per message. The
table item keeps only what lists, threads, labels and send status need.

**Why:** Content is most of a message's bytes and never changes. The item is rewritten on every
label change, status change and delivery event, and copied into two indexes. DynamoDB bills each
write on the full item size. Keeping content out holds items to a few KB, and bodies of any size
are stored whole.

The object is written before the table transaction, with `if-none-match: *`. A retried
transaction finds its own object and keeps it.

## Decision: labels are filtered, not indexed

Labels live on message and thread items. A filtered list reads the time index and filters each
page, re-reading up to five times to fill it.

**Why:** Per-label index rows cost four extra writes per reply, because thread rows embed the
last-activity time in their sort key. The `include_spam` and `include_trash` filters force a
filtering read anyway. Ingest stays at three writes per message.

**Cost:** A rare label reads past many messages. A page can return fewer than `limit` items plus a
continuation token. Per-label rows can be added and backfilled later if one inbox needs them.

## Decision: ids are time-ordered and deterministic

`message_id` and `thread_id` are UUIDv7s, so sorting by id sorts by time. A `before`/`after`
window becomes a key range on the existing index.

Inbound ids derive from the SES message id and receipt time, never the clock. A redelivered
notification produces the same id, so the message is stored once.

## Ingest

For each matched recipient, ingest fetches the raw MIME, parses it, extracts attachments,
resolves the thread and commits one transaction. The transaction writes the message (only if it
doesn't exist), the thread (only if unchanged since read) and the `Message-ID` alias. A
redelivery fails the message condition and cancels the whole transaction, so the thread count
stays correct.

**Threading** checks the message's own `Message-ID`, then `In-Reply-To`, then `References`
(nearest first) against known aliases. No match starts a new thread. There is no subject
matching, because wrongly joining threads is worse than splitting one.

SES replaces a sent message's `Message-ID` with its own. When a send succeeds, both SES forms
(`<id@email.amazonses.com>` and the regional form) become aliases, so replies thread correctly.

**Failures** follow the existing split. Throttling, 5xx and timeouts trigger an SNS redelivery.
A message that won't parse is logged and counted, and `ses.inbound` still publishes.

**The mail topics use direct invoke, not HTTPS.** SNS gives HTTPS endpoints a short fixed
timeout. Fetching and parsing a 40 MB message, SES's maximum, can take longer.

**Guards:** ingest follows an S3 pointer only when the bucket is `MAIL_BUCKET`. SES setup
notifications (`AMAZON_SES_SETUP_NOTIFICATION`) are acknowledged and ignored.

## API authentication

Clients send a bearer key. The function hashes it with SHA-256 and compares it in constant time
with hashes from an SSM SecureString. The cache refreshes every five minutes and on a miss, so a
new key works at once.

If SSM fails, the last good cache stays in use. If the cache has never loaded, the API answers
`503`, not `401`: SDKs retry `503`, and an outage must not look like a bad key.

**Why SSM, not Secrets Manager:** the same KMS encryption with no per-secret charge. Rotation is
writing two hashes.

**Why bearer keys, not SigV4:** compatibility with existing clients is the goal, and the function
can't have a second Function URL that uses `AWS_IAM` auth.

## Security

- **Keys** are hashed at rest, compared in constant time, cached in memory only, and never logged.
  `TRACE` logging is refused because the runtime logs raw payloads, including `Authorization`.
- **Downloads** are presigned URLs scoped to one object, valid for 15 minutes by default.
  Attachment filenames come from arbitrary senders and are signed into the URL, so they are
  reduced to a safe character set first.
- **Headers** containing CR or LF are refused. `From`, `Return-Path` and the other
  service-controlled headers can't be overridden, so a caller can't send as another inbox.
- **`Bcc`** reaches recipients through the SES envelope only, never as a header.
- **Sending** is limited by IAM to this domain's identity and this stack's configuration set.
- **URL attachments** are a server-side request forgery risk: the function can reach the EC2
  metadata endpoint. One module checks `https`, the default port, no credentials and public
  addresses only, at request time and again at fetch time. IP-literal hosts are checked directly,
  which catches decimal, octal, hex, IPv4-mapped and 6to4 spellings. Redirects are followed by
  hand, at most five, re-checking each hop. The host resolves once, and the connection is pinned
  to the checked addresses. Size is capped before and during the read, and compressed responses
  are refused.
- **Abuse protection:** a Function URL takes no WAF. If abuse appears, the next step is CloudFront
  in front, with `/webhooks/*` bypassing auth.

## Events

The relay publishes mailbox events from the mail table's stream. Each detail matches the
reference API's webhook payload exactly, checked by `tests/mailbox_event_schemas.rs`. Setting
`pMailWebhookUrl` delivers them over HTTPS through an EventBridge API destination.

- `message.received*` carries the thread as it was at ingest, stored on the message item, so the
  relay never reads the thread item.
- The SES-driven events are built from the SES event itself. Only the SES event holds the
  recipients, bounce and complaint types, and reject reason. The relay resolves the SES id before
  publishing, so a failed lookup retries instead of publishing a partial event.
- Delivery labels are added, never removed. Events arrive out of order, and a message that
  bounced and was opened should show both.
- Recording a send's outcome is the only write to the message item. The relay publishes one
  `message.sent` per send, not one per attempt.

## Status

Receiving, reading, labeling, downloads, sending and the operator paths are implemented and
tested against in-memory fakes. `POST /v0/inboxes`, the `DELETE` routes, and the `drafts`,
`labels`, `domains` and `webhooks` groups answer `501`.

**Nothing here has been deployed.** Two bugs found during the build (a continuation key missing
its table half, and a status query sent to the wrong index) are the kind only real DynamoDB
reveals. Both returned plausible empty results instead of errors. Deploying to staging and
moving real mail in both directions is the next step.

Accepted limits:

- A filtered list stops after five reads and returns a short page with a continuation token.
- `GET /v0/inboxes` reads one index partition. That is fine at the inbox counts this targets.
- Metrics are emitted, but alarms are the operator's to build. Nothing pages.
