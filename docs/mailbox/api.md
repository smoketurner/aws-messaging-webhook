# Mailbox API

Every `/v0` route needs `Authorization: Bearer <key>`. Unlisted `/v0` routes answer `501`. Errors
carry a JSON body with `name` and `message`.

| Route | Returns |
|---|---|
| `GET /v0/inboxes` | `{count, limit, inboxes[], next_page_token?}` |
| `GET /v0/inboxes/{inbox_id}` | One inbox |
| `GET /v0/inboxes/{inbox_id}/messages` | `{count, limit, messages[], next_page_token?}`, newest first |
| `GET /v0/inboxes/{inbox_id}/messages/{message_id}` | The full message, including `text`, `html`, `headers` and `references` |
| `GET /v0/inboxes/{inbox_id}/threads` | `{count, limit, threads[], next_page_token?}`, by last activity |
| `GET /v0/inboxes/{inbox_id}/threads/{thread_id}` | One thread with its `messages[]`, oldest first |
| `GET …/messages/{message_id}/raw` | `{message_id, size, download_url, expires_at}` |
| `GET …/messages/{message_id}/attachments/{attachment_id}` | The same, plus `filename`, `content_type`, `content_disposition`, `content_id` |
| `PATCH /v0/inboxes/{inbox_id}/messages/{message_id}` | `{message_id, labels}` |
| `POST /v0/inboxes/{inbox_id}/messages/send` | `{message_id, thread_id}` once queued |
| `POST …/messages/{message_id}/reply` | The same, in the original's thread |

## Downloads

`download_url` is a presigned S3 URL valid for `pAttachmentUrlTtlSeconds` (15 minutes by
default). Anyone holding it can fetch the object until it expires; no API key is needed. Objects
past retention, and attachments dropped for size, answer `404`.

## Labels

`PATCH` takes `{"add_labels": …, "remove_labels": …}`, each a label or a list. Labels are
lowercased, trimmed and deduplicated. Remove `unread` to mark a message read.

- A message holds at most 20 of your labels, and a thread 20 across its messages. A request
  names at most 20 of your labels. Exceeding a cap is a `400`.
- Your labels plus `unread`, `spam` and `trash` are editable. Service labels, or the same label
  in both fields, are a `400`.
- A thread keeps a label while any of its messages has it. Relabeling doesn't reorder threads.

## Listing and filtering

| Parameter | Behavior |
|---|---|
| `limit` | Default 20, maximum 100 |
| `page_token` | Opaque. Bound to the inbox, sort order and time window; changing any of them is a `400` |
| `ascending` | Default `false` |
| `before`, `after` | UTC timestamps (`2026-01-15T09:30:00Z`, with or without milliseconds). `after` is inclusive, `before` exclusive |
| `labels` | Repeatable. An item must carry every label |
| `from`, `to`, `subject` | Repeatable. Case-insensitive substring; any value matches |
| `include_spam`, `include_blocked`, `include_unauthenticated`, `include_trash` | Default `false`. Naming the label in `labels` overrides the flag |

A filtered page can hold fewer than `limit` items and still return a `next_page_token`. Follow
the token.

`GET /v0/inboxes` and a thread's messages take only `limit` and `page_token`. A thread shows all
its messages regardless of labels.

## Sending

`POST …/messages/send` takes `to`, `cc`, `bcc` (an address or a list), `subject`, `text` and/or
`html`, and optionally `reply_to`, `labels`, `headers` and `attachments`. Each attachment gives
either `content` (base64) or `url`.

**A success response means the message is queued, not sent.** A separate sender function calls SES. Watch
for `message.sent` or a delivery event to learn the outcome.

| Limit | Value |
|---|---|
| Send or reply body | 6 MiB. Send attachments over 4.5 MiB by `url`, since base64 adds 33% |
| Any other body | 1 MiB |
| Refused headers | `From`, `Sender`, `To`, `Cc`, `Bcc`, `Reply-To`, `Subject`, `Date`, `Message-ID`, `In-Reply-To`, `References`, `Return-Path`, `MIME-Version`, `Content-*` |
| CR or LF | Refused in any address, header name or header value |

### Retrying safely

Send an `Idempotency-Key` header:

- Same key, same request: returns the original ids.
- Same key, any difference: `409`.
- Keys are kept for 24 hours. Without one, a retry after a lost response sends twice.

### Replies

`…/reply` takes the same body plus `reply_all`. It fills in the thread, `In-Reply-To`,
`References`, a `Re:` subject (never doubled) and the recipient: the original's `Reply-To`, or
its sender. `reply_all` adds the original's other recipients, minus this inbox. Setting `to`,
`cc` or `bcc` overrides the recipients and keeps the threading.

### URL attachments

A `url` must be `https`, on the default port, with no credentials, and resolve to a public
address. The sender checks again at fetch time, on every redirect (at most five), and refuses
compressed responses. Fetched bytes are kept, so a retried send reuses them.

### Delivery status

SES events add these labels to a sent message: `delivered`, `bounced`, `complained`, `rejected`
and `opened`. Labels are added, never removed, because events arrive out of order.
