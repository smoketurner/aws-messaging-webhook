# Design: a mailbox on SES

How the mailbox works and why it is shaped this way. The README covers running it — parameters,
post-deploy steps, the endpoint reference. This covers the decisions behind them, including the
ones that look odd until you know what they are defending against.

The HTTP surface deliberately mirrors a third-party mailbox API's `/v0` contract — paths, field
names, error bodies — so an agent written against that contract works here with only the base
URL and key changed. That constrains the wire format and nothing else; everything below the
serialization boundary is this project's own design.

## What it does

1. **Receives.** The SAM template takes a domain and a list of local parts and provisions the
   AWS side: the SES domain identity, DNS, an S3 bucket for raw mail, the receipt rule set, the
   SNS topics and the wiring into the function. `sam deploy` remains the whole deployment.
2. **Ingests.** Each received message is fetched from S3, parsed, and stored as message, thread
   and attachment metadata, then published to EventBridge as `message.received*`.
3. **Serves.** An authenticated `/v0` API lists and reads inboxes, threads and messages, changes
   labels, and hands out presigned download URLs.
4. **Sends.** `POST …/messages/send` and `…/reply` queue outbound mail; a separate sender
   function delivers it through SES and records what happened.

Out of scope, and answering `501`: multi-tenant pods, per-inbox client ids, and the `drafts`,
`labels`, `domains` and `webhooks` resource groups. EventBridge is the outbound notification
mechanism; an HTTP webhook consumer is an EventBridge API destination with an input transformer
selecting `$.detail`, which needs no code here.

## How it fits the existing pipeline

The **verify → persist → act → publish** pipeline is unchanged. Inbound email already arrived as
an SES receipt over SNS, already persisted to the events table, and already published
`ses.inbound`. The mailbox adds a lifecycle action in the *act* stage, a second table with its
own stream, an authenticated API on the same router, and a second function for sending.

```
                 template.yaml (Condition: cHasMailDomain)
 ┌──────────────────────────────────────────────────────────────────────┐
 │ MX/DKIM/SPF/DMARC ─► SES receipt rule ─► S3 (raw MIME)               │
 │                                       └─► SNS topic ─► webhook fn    │
 │ SES configuration set (outbound) ─────────► SNS topic ─► webhook fn  │
 └──────────────────────────────────────────────────────────────────────┘
                                          │
 SNS ─► verify ─► persist (events table) ─► act ─► (relay ► ses.inbound)
                                          │
                     ingest: GetObject ► parse MIME ► extract attachments
                             ► resolve thread ► TransactWriteItems
                                          │
                                   mail table (stream)
                                    │              │
              webhook fn: relay ────┘              └──── sender fn: claim ► build
                    │                                            ► SES ► mark
                    ▼
            EventBridge message.*

 Agent ─► GET  /v0/inboxes/{id}/threads      ─► mail table
 Agent ─► POST /v0/inboxes/{id}/messages/send ─► outbox (S3 + mail table), returns queued
```

Two functions run the same binary, selected by `FUNCTION_MODE`. They read the same stream and
each ignores what the other owns: the relay publishes message inserts, the sender acts on
send-state items. The sender is separate so its concurrency, memory and IAM are scoped to
sending, and so a slow send cannot occupy the function serving the API.

## The mail table

A second DynamoDB table, keyed by inbox and message rather than by SNS message id. Every item
lives in one of these shapes:

| Item | `pk` | `sk` | Holds |
|---|---|---|---|
| Inbox | `INBOX#<inbox>` | `META` | email, display name, metadata, timestamps |
| Message | `INBOX#<inbox>` | `MSG#<messageId>` | addresses, subject, preview, labels, attachment metadata, send status — not the body |
| Thread | `INBOX#<inbox>` | `THR#<threadId>` | rolled-up subject, preview, senders, recipients, labels, counts, newest attachments |
| RFC alias | `RFC#<inbox>#<rfc-id>` | `RFC` | maps a `Message-ID` to its message and thread, for threading |
| Send state | `OUTBOX#<messageId>` | `STATE` | status, envelope, claim and failure bookkeeping |
| Send key | `SENDKEY#<sha256>` | `KEY` | what an `Idempotency-Key` resolves to |
| SES reference | `SESMSG#<sesMessageId>` | `REF` | maps an SES id back to a message, for delivery events |

Message, thread, RFC alias and SES reference items carry an `expires_at` TTL of
`pMailRetentionDays`, the same span the bucket keeps the message's objects, so a message ages
out whole. A thread takes the TTL of its newest message.

### Message content lives in S3

A message's bodies, headers, `References`, `Reply-To` and verdicts are written once to
`messages/<inbox>/<messageId>.json` rather than onto the message item. They are most of a
message's bytes and never change, while the item is rewritten on every label change, send-status
transition and delivery event, and copied into both list indexes; DynamoDB bills each of those
writes on the whole item. Keeping the content out keeps the item to a few KB whatever the mail
looks like, and a body of any size is stored whole rather than truncated to fit.

The document is written before the item's transaction, so an item never exists without it; an
orphaned document from a transaction that failed is overwritten by the retry, since inbound ids
are deterministic. Three readers need it: `GET` for one message or one thread, a reply (for the
original's `References` and `Reply-To`), and the stream relay (for the `message.received` event
body). List endpoints read only items.

Three indexes: **ByTime** (`gsi1`) over inboxes, messages and threads ordered by time;
**ByThread** (`gsi2`) over one thread's messages; **ByStatus** (`gsi3`, sparse) over send states
by status, which is how the sweep finds work. The index is named explicitly at each query rather
than inferred from the partition string — inferring it once sent status queries to the wrong
index, and a query against the wrong index returns an empty page rather than an error, so
nothing complains.

### Labels are not indexed

An earlier revision gave every label its own pointer rows, so a label was a direct query. That
cost four extra writes on a typical reply — thread pointers embed the thread's last-activity
timestamp in their sort key, so every new message deleted and rewrote them — and nothing ever
read them, because the `include_spam`/`include_trash` flags force a filtering read anyway.

Labels now live on the message and thread items. A filtered list reads the time-ordered index
and filters the page, re-reading up to five times to fill it. Ingest is a fixed three writes.
The cost is reading past non-matching messages when a label is rare: a response can come back
with fewer than `limit` items *and* a continuation token, which is a normal result rather than
an error. Pointer rows are derived data and can be reintroduced and backfilled if one inbox's
volume ever makes the filtered read too expensive.

### Ids

`message_id` and `thread_id` are UUIDv7s, so sorting by id *is* sorting by time — which is what
lets a `before`/`after` window become a plain key range instead of a separate index.

Inbound ids are **derived deterministically** from the SES message id and the receipt timestamp,
never from wall-clock time. SNS delivers at least once; an id minted from the clock would store
the same mail twice on a redelivery.

### Pagination

A page token is the base64url-encoded continuation, and it carries the *table* key alongside the
*index* key. A `Query` against a secondary index needs an `ExclusiveStartKey` holding both,
because an index key is not unique on its own. The token's partition is re-checked against the
request that presents it, so a token from one inbox cannot be replayed against another. Default
page size 20, maximum 100.

## Ingest

For each envelope recipient the rule matched: resolve the inbox, fetch the raw MIME from
`inbound/raw/`, parse it, extract attachments to S3, resolve the thread, write the content
document, and commit.

Thread resolution walks `In-Reply-To` then `References`, nearest first, against the RFC aliases
for that inbox; first hit wins. No hit starts a new thread. There is deliberately no
subject-based merging: a false join is worse than a split thread.

Labels are assigned from the receipt's verdicts — `received` and `unread` always, `spam` when
quarantined, `unauthenticated` when SPF, DKIM or DMARC failed. These select the event type and
drive the list endpoints' `include_*` filters.

The commit is one transaction: the message conditioned on not existing, the thread
version-conditioned on what was read, and the `Message-ID` alias. The failed condition on the
message *is* the idempotency signal — a redelivery cancels the whole transaction, so the thread
is not double-counted.

An S3 pointer is followed only when the bucket equals `MAIL_BUCKET`. A receipt naming another
bucket is a permanent skip: never follow a pointer the operator did not configure.

Failures follow the existing `ActionErrorKind` split. Throttling, 5xx and timeouts are transient
and recruit SNS redelivery, which re-runs the idempotent ingest. A MIME that will not parse is
permanent: it is logged and counted, and the `ses.inbound` event still publishes with the S3
pointer, so nothing is lost.

The mail topics are subscribed with the **lambda** protocol rather than HTTPS. SNS applies a
fixed, short response timeout to an HTTPS endpoint, and a 40 MB message — the SES receiving
ceiling — can take longer than that to fetch and parse, which would make a slow ingest look like
a failure loop.

## The `/v0` API

Bearer auth on every route, on the same Function URL as the webhook paths.

The presented key is SHA-256-hashed and compared in constant time against hashes read from an
SSM SecureString. Keys are never stored, logged or emitted as metrics. The cache refreshes every
five minutes and on a miss, so a newly added key works immediately. A parameter-store failure
keeps the last good cache rather than failing open or locking everyone out — and a cache that
has *never* loaded answers `503`, not `401`, because the SDKs retry `503` and never retry `401`:
a transient outage must not look like a bad key.

Parameter Store rather than Secrets Manager: same KMS encryption, no per-secret charge, and
rotation is overwrite-with-two-hashes rather than Secrets Manager's rotation machinery. Bearer
rather than SigV4 because SDK compatibility is the point, and a second `AWS_IAM` Function URL on
the same function is not possible.

Reads, label changes and downloads are implemented. Downloads are presigned S3 URLs valid for
fifteen minutes rather than bytes streamed through the function, so message size costs the
function nothing. A presigned URL carries its own authorization — the API key is not needed to
follow it — which is why the lifetime is short.

An attachment's filename reaches us from a header an arbitrary sender wrote, and it is signed
into the URL and echoed back by S3 as a response header, so it is reduced to a conservative
allowlist first. A quote would end the quoted string and a CRLF would start a new header.

## Sending

The API **queues**; it never calls SES. A separate sender function consumes the table's stream,
claims the send, assembles the message, calls SES once, and records the outcome.

This is the design's central decision, and it exists because a synchronous send could deliver
the same mail more than once. SESv2 `SendEmail` has no idempotency token, and the AWS SDKs retry
5xx on their own, so one API request could become three deliveries.

At most once is enforced in three places:

1. **The claim.** A conditional write moves `queued` to `sending`. The stream delivers at least
   once, so two senders will be handed the same record; exactly one wins and the other stops,
   which is an ordinary outcome and not an error.
2. **SDK retries are disabled** on the send call. What a failure means is this service's
   decision, not the SDK's.
3. **An ambiguous outcome is terminal.** A timeout or dropped connection means SES *may* hold
   the message. The send stops as `unknown` and keeps its `queued` label, because it is neither
   sent nor known to have failed and claiming either would be a statement this service cannot
   support. Only an operator resolves it. Collapsing `unknown` into `failed` is the bug that
   would eventually double-send.

Marking the outcome is the only write that touches the message item, so the relay publishes one
event per send rather than one per attempt; `sending` is never mirrored onto the message for the
same reason.

A transient failure hands the record back with `requeued_at` set, whose appearance re-triggers
the sender, and gives up after five attempts so a lastingly unavailable SES cannot keep one
message circulating forever.

`Idempotency-Key` is hashed, never stored, and the stored record carries a fingerprint of the
request body. The same key with the same request replays the original ids; the same key with a
*different* request is a `409`, because silently sending something the caller did not ask for is
worse than refusing.

### Assembling the message

The MIME tree is shaped by what the message actually holds: `multipart/related` only when an
inline part needs to sit with the body that references it, `multipart/mixed` only when there is
an ordinary attachment. A multipart with one child is collapsed to that child, and an
unnecessary wrapper changes how some clients render it.

**`Bcc` is never written as a header.** Its recipients are reached through the envelope handed to
SES separately; writing the header would disclose them to everyone else on the message.

### Fetching URL attachments

A send may name a URL instead of inlining bytes, which means this service makes an HTTP request
to an address a caller chose. That is server-side request forgery surface: the function can
reach the EC2 metadata endpoint and any public host.

One module holds the rules, applied both when the request is accepted and again at fetch time:
`https` only, default port, no embedded credentials, and no address outside the public ranges.
IP-literal hosts never reach a resolver, so they are checked directly — which is what catches
the obfuscated spellings of loopback and the metadata endpoint (decimal, octal, hex, shortened,
IPv4-mapped, 6to4-wrapped).

The fetcher is the only HTTP client here that follows redirects, and it follows them by hand, at
most five hops, re-running the full checks on every `Location`. The first check says nothing
about where a redirect leads, which is exactly how an attacker would reach an internal address
after passing it. The host is resolved once and the connection pinned to the addresses that
passed, so a name cannot resolve to something else between the check and the connect. Size is
capped before the body is read and again while reading, because the header can lie, and a
compressed response is refused rather than stored as-is, because nothing here decompresses.

Fetched bytes are stored in the outbox under the attachment's id, so a retried send reuses the
message that was already built rather than a URL whose content may have changed.

### Recovering stuck sends

A sender killed between claiming a send and recording its outcome leaves the state `sending`
with nobody working on it and no stream record to re-trigger it. Nothing else would notice, so a
scheduled sweep releases claims older than fifteen minutes.

That threshold is well beyond the sender's own timeout on purpose: taking a send away from a
sender still working on it is how the same message gets delivered twice. Sends in `unknown` are
counted and left alone.

## Events

The mail table's stream is a second source for the existing relay, which remains the sole
publisher. `detail-type` is the event type, and the `event_type`/`event_id`/`message` triple is
the reference API's webhook payload, so a consumer wanting an HTTP webhook uses an EventBridge
API destination selecting `$.detail` — no code — plus an additive `schemaVersion`/`meta` block.

| Event | Emitted when |
|---|---|
| `message.received` / `.spam` / `.unauthenticated` | A message INSERT labelled `received` |
| `message.sent` | A message INSERT labelled `sent` |
| `message.delivered`, `.bounced`, `.complained`, `.rejected`, `.opened` | An SES event resolves to a mailbox message and adds its label |

Delivery labels are **added, never removed**. These events arrive out of order under
at-least-once delivery, so a message that both bounced and was opened should say both rather
than whichever landed last. An SES event that resolves to nothing is ignored: most events on a
shared configuration set are for mail this service did not send.

A client's own label edits through `PATCH` change the item but publish nothing — the reference
API has no event for them.

The 256 KB `PutEvents` cap is handled the way the existing pipeline handles inbound content:
drop `html`, then `text`, then fall back to a payload-omitted marker with `meta` intact. The API
is the fetch path.

## Observability

Metrics in the existing EMF namespace: `MessagesIngested`, `IngestFailures`, `IngestSkipped`,
`IngestTimeouts`, `MessagesSent`, `SendFailures`, `SendOutcomeUnknown` and `ApiAuthFailures`.
The per-message INFO line carries `inbox_id` and `thread_id`.

**The stack defines no alarms.** Metrics are emitted; alarms and dashboards are the operator's to
build against them. The consequence is accepted: nothing pages when ingest fails, a sweep dies,
or sends sit stuck.

## Security notes

- The `/v0` surface is the first thing on this URL that returns data. Keys are hashed at rest,
  compared in constant time, cached in memory only, and never logged.
- `TRACE` is not an allowed log level: at `TRACE` the Lambda runtime logs raw invoke payloads,
  including the `Authorization` header.
- Presigned URLs are short-lived and scoped to one object; `download_url` is the only way the API
  hands out S3 access.
- S3 pointers from a receipt are followed only when the bucket equals `MAIL_BUCKET`.
- Sending is scoped by IAM to this domain's identity and this stack's configuration set, so the
  sender cannot send as another domain.
- Header injection is refused rather than escaped: any CR or LF in an address, header name or
  header value is rejected, and the headers the service controls — `From` and `Return-Path` above
  all — cannot be overridden, so a caller cannot send as another inbox.
- No WAF attaches to a Function URL directly. If abuse becomes a concern, CloudFront in front
  (with the `/webhooks/*` paths bypassing auth) is the next step, not part of this work.

## Status and what is left

Receiving, reading, labelling, downloading, sending and the operator paths are implemented and
tested. The routes still answering `501` are the out-of-scope resource groups above, plus
`POST /v0/inboxes` and the `DELETE` routes.

**Nothing here has been deployed.** Every guarantee above is verified against in-memory doubles.
Two of the bugs found while building this — a continuation key missing its table half, and a
status query routed to the wrong index — are exactly the class that only real DynamoDB surfaces,
and both returned plausible-looking empty results rather than errors. Deploying to staging and
watching real mail move in both directions is the next step, and it outranks everything below.

The two situations that need a person — a send stuck in `unknown`, and mail that failed to
ingest — are driven by invoking a function directly rather than through the API, because they
are rare, destructive and account-scoped: `lambda:InvokeFunction` gates them better than a
bearer key, and the public surface stays the contract it mirrors. The README carries the
commands.

Deliberate remaining limits, none of which is unfinished work:

- A filtered list gives up after five store round-trips and returns a short page with a
  continuation token, so a rare label over a large inbox costs the client several requests.
- `GET /v0/inboxes` reads one index partition. Fine at the inbox counts this targets; it would
  need sharding well beyond them.
- Nothing pages. Metrics are emitted and alarms are the operator's to build.
