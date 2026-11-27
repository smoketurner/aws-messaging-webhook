# Mailbox architecture

How the mailbox receives, stores, sends and publishes mail. For setup, see [setup.md](setup.md).

The `/v0` API follows a third-party mailbox API's contract (paths, field names, error bodies), so
a client written for it works here after changing the base URL and key.

## Guarantees

- **A send is never delivered twice by this service.** A send whose outcome is unclear waits for
  an operator.
- **Inbound mail is never dropped.** A message that can't be parsed still publishes `ses.inbound`
  with its S3 pointer.
- **Every step is safe to repeat.** SNS, DynamoDB Streams and EventBridge all deliver at least
  once.

## Components

The mailbox runs inside the **verify → persist → act → publish** pipeline. Inbound mail arrives
as an SES receipt over SNS, and ingest is one of the pipeline's actions. The mail table has its
own stream.

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

Both functions run the same binary, selected by `FUNCTION_MODE`. The sender has its own
concurrency, memory and IAM permissions, which cover sending only.

## Sending

The API writes the send to the mail table and returns. The sender function reads the stream and
calls SES. Four mechanisms keep each send to at most one delivery:

1. **Claim.** A conditional write moves the send from `queued` to `sending`. When the stream
   delivers a record twice, one sender wins and the other stops.
2. **SES-call mark.** The sender records `ses_call_at` just before calling SES. A stale claim
   without the mark was never sent and is released. A claim with the mark may have been sent, so
   it never retries automatically.
3. **No SDK retries on the send call.** The sender retries only 429 and 503, which mean SES
   didn't accept the request.
4. **Ambiguous outcomes stop.** A timeout, dropped connection or other 5xx marks the send
   `unknown`. Nothing resends it; an operator resolves it unless SES does first.
5. **SES's events confirm a send.** Every send is tagged `mailbox_message=<message id>`, and SES
   repeats the tag on each event it publishes for the message. When an event arrives for a send
   that is `unknown`, or still `sending` with its SES-call mark, and SES's id was never recorded,
   the event is SES saying it has the message: the send is recorded as sent with that id, dated
   by when SES accepted it. `Reject` and `Rendering Failure` confirm nothing. A send that never
   carried the SES-call mark cannot be settled this way, since other senders share the
   configuration set and a tag is only their claim.

A transient failure (SES throttling, S3 or an attachment host briefly down) hands the send back
with `requeued_at` set, which re-triggers the sender. The sender backs off from 1 to 16 seconds.
After five hand-backs the send fails. A permanent failure (access denied, object too large, host
rejecting the request) fails the send at once.

The sender must finish loading the send 35 seconds before its deadline, leaving room for the SES
call (capped at 20 seconds) and recording the result.

A sweep runs every 10 minutes for sends stuck in `sending` longer than 15 minutes, 7.5 times the
sender's 120-second timeout.

`Idempotency-Key` is stored as a hash, with a fingerprint of the request. A reused key with a
different request returns `409`.

## Storage

Bodies, headers, `References`, `Reply-To` and verdicts go to one S3 object per message. The table
item keeps only what lists, threads, labels and send status need. The object is written before
the table transaction, with `if-none-match: *`; a retried transaction finds its own object and
keeps it.

Labels live on message and thread items; there is no per-label index. A filtered list reads the
time index and filters each page, re-reading up to five times to fill it, so a page can return
fewer than `limit` items plus a continuation token.

`message_id` and `thread_id` are UUIDv7s, so sorting by id sorts by time, and a `before`/`after`
window is a key range on the time index. Inbound ids derive from the SES message id and receipt
time, never the clock, so a redelivered notification maps to the same message.

The table layout is in [storage.md](storage.md).

## Ingest

For each matched recipient, ingest fetches the raw MIME, parses it, extracts attachments,
resolves the thread and commits one transaction. The transaction writes the message (only if it
doesn't exist), the thread (only if unchanged since read) and the `Message-ID` alias. A
redelivery fails the message condition and cancels the whole transaction, so the thread count
stays correct.

**Threading** checks the message's own `Message-ID`, then `In-Reply-To`, then `References`
(nearest first) against known aliases. No match starts a new thread. Subjects are never matched.

Each referenced id that no alias maps yet gets a placeholder alias pointing at the message's
thread, like the empty containers in Jamie Zawinski's
[threading algorithm](https://www.jwz.org/doc/threading.html). A reply that arrives before its
parent, and replies to a parent this inbox never received, therefore share one thread. When the
parent arrives, its real alias replaces the placeholder. A placeholder is written only where no
row exists; a message that loses that race re-reads the aliases and joins the thread that won.
Two threads that a later message links stay separate.

SES replaces a sent message's `Message-ID` with its own. When a send succeeds, both SES forms
(`<id@email.amazonses.com>` and the regional form) become aliases, so replies thread correctly.

**Failures:** throttling, 5xx and timeouts trigger an SNS redelivery. A message that won't parse
is logged and counted, and `ses.inbound` still publishes.

The mail topics invoke the function directly rather than over HTTPS, because SNS gives HTTPS
endpoints a short fixed timeout and a 40 MB message (SES's maximum) can take longer.

**Guards:** ingest follows an S3 pointer only when the bucket is `MAIL_BUCKET`. SES setup
notifications (`AMAZON_SES_SETUP_NOTIFICATION`) are acknowledged and ignored.

## API authentication

Clients send a bearer key. The function hashes it with SHA-256 and compares it in constant time
with hashes from an SSM SecureString. The cache refreshes every five minutes and on a miss, so a
new key works at once.

If SSM fails, the last good cache stays in use and the next refresh waits out a short backoff. If
the cache has never loaded, the API answers `503`, not `401`.

## Security

- **Keys** are hashed at rest, compared in constant time, cached in memory only, and never logged.
  `TRACE` logging is refused because the runtime logs raw payloads, including `Authorization`.
- **Downloads** are presigned URLs scoped to one object, valid for 15 minutes by default.
  Attachment filenames are reduced to a safe character set before they're signed into the URL.
- **Headers** containing CR or LF are refused. `From`, `Return-Path` and the other
  service-controlled headers can't be overridden, so a caller can't send as another inbox.
- **`Bcc`** reaches recipients through the SES envelope only, never as a header.
- **Sending** is limited by IAM to this domain's identity and this stack's configuration set.
- **URL attachments:** one module checks `https`, the default port, no credentials and public
  addresses only, at request time and again at fetch time. IP-literal hosts are checked directly,
  which catches decimal, octal, hex, IPv4-mapped and 6to4 spellings. Redirects are followed by
  hand, at most five, re-checking each hop. The host resolves once, and the connection is pinned
  to the checked addresses. Size is capped before and during the read, and compressed responses
  are refused.
- **No WAF:** a Function URL can't take one.

## Events

The relay publishes mailbox events from the mail table's stream. Each detail matches the
reference API's webhook payload, checked by `crates/webhook/tests/mailbox_event_schemas.rs`.
Setting `pMailWebhookUrl` delivers them over HTTPS through an EventBridge API destination. See
[events.md](events.md).

- `message.received*` carries the thread as it was at ingest, stored on the message item, so the
  relay never reads the thread item.
- The SES-driven events are built from the SES event itself. The relay resolves the SES id before
  publishing, so a failed lookup retries instead of publishing a partial event.
- Delivery labels are added, never removed, so a message that bounced and was opened shows both.
- Recording a send's outcome is the only write to the message item, so the relay publishes one
  `message.sent` per send, not one per attempt.

## Limits

- `POST /v0/inboxes`, the `DELETE` routes, and the `drafts`, `labels`, `pods`, `domains` and
  `webhooks` groups answer `501`.
- A filtered list stops after five reads and returns a short page with a continuation token.
- `GET /v0/inboxes` reads one index partition.
- Metrics are emitted, but alarms are the operator's to build.
