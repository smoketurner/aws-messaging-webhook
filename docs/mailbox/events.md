# Mailbox events

Mailbox events publish on the same bus and `source` as the other events. The detail type is the
event's `event_type`. The detail is `type` (always `"event"`), `event_type`, `event_id` and one
event object.

| Event | Published when | Object |
|---|---|---|
| `message.received`, `.spam`, `.unauthenticated` | A new message lands in an inbox | `message`, `thread` |
| `message.sent` | The sender marks a queued message `sent` | `send` |
| `message.delivered` | SES reports a `Delivery` for a sent message | `delivery` |
| `message.bounced` | SES reports a `Bounce` | `bounce` |
| `message.complained` | SES reports a `Complaint` | `complaint` |
| `message.rejected` | SES reports a `Reject` | `reject` |
| `message.opened` | SES reports an `Open` | `open` |

Other SES events, label edits and internal writes publish no mailbox event.

`event_id` is deterministic: redeliveries and replays produce the same id. Deduplicate on it.
`tests/fixtures/mailbox-events/schemas.json` holds each event's schema, and
`tests/mailbox_event_schemas.rs` checks every event against it.

## Received mail

`.spam` means the spam or virus verdict was `FAIL`. `.unauthenticated` means SPF, DKIM or DMARC
failed on otherwise clean mail. Spam wins over unauthenticated. Nothing is dropped.

```json
{
  "type": "event",
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
  "thread": {
    "inbox_id": "…", "thread_id": "…", "labels": ["received", "unread"],
    "timestamp": "…", "received_timestamp": "…",
    "senders": ["…"], "recipients": ["…"], "subject": "…", "preview": "…",
    "last_message_id": "…", "message_count": 1, "size": 1234,
    "created_at": "…", "updated_at": "…"
  }
}
```

`message` matches the API's message object. `thread` is the thread as of this message's arrival;
re-read it for current state. `senders`, `recipients` and `attachments` hold at most 20 entries.

Over 256 KB, the event drops `message.html`, then `message.text`, then `message.headers`. If it
still doesn't fit, `message` and `thread` shrink to their required fields.

## Sent mail

```json
{
  "type": "event",
  "event_type": "message.bounced",
  "event_id": "evt_…",
  "bounce": {
    "inbox_id": "…", "thread_id": "…", "message_id": "…", "timestamp": "…",
    "type": "Permanent", "sub_type": "General",
    "recipients": [{ "address": "…", "status": "5.1.1" }]
  }
}
```

Every object carries `inbox_id`, `thread_id`, `message_id` and `timestamp`, plus:

| Object | Fields |
|---|---|
| `send` | `recipients`: the `to`, `cc` and `bcc` addresses. `timestamp` is when it was sent |
| `delivery` | `recipients`: SES's delivered recipients |
| `bounce` | `type`, `sub_type`, and each recipient's DSN `status` (empty when none was reported) |
| `complaint` | `type`, `sub_type` (empty when absent), and the complaining `recipients` |
| `reject` | `reason`. `timestamp` is when SNS published the notification |
| `open` | Identifiers and `timestamp` only |

One event publishes per SES notification. A bounce reported in two notifications publishes two
`message.bounced` events, and every open publishes `message.opened`. SES events for mail this
service didn't send publish nothing.

## Webhook delivery

Set `pMailWebhookUrl` and the stack POSTs every mailbox event to it through an EventBridge API
destination. The body is the event detail.

Store the secret in SSM and pass it at deploy time. Don't put it in `samconfig.toml`:

```bash
aws ssm put-parameter --name /messaging-webhook/dev/mail-webhook-secret --type SecureString \
  --value "$(openssl rand -hex 32)"

sam deploy --parameter-overrides \
  pMailWebhookUrl=https://… \
  pMailWebhookSecret="$(aws ssm get-parameter --name /messaging-webhook/dev/mail-webhook-secret \
    --with-decryption --query Parameter.Value --output text)"
```

On an existing stack, parameters you don't pass keep their previous values.

| Header | Use |
|---|---|
| `x-webhook-secret` | Compare in constant time to authenticate the request |
| `webhook-id` | The `event_id`. Deduplicate on it |

The receiver has 5 seconds to answer, so acknowledge first and work after. EventBridge retries
`401`, `407`, `409`, `429`, `5xx` and timeouts for 24 hours, up to 185 attempts. Other `4xx`
responses drop the event, so answer a wrong secret with `401`, not `403`. Delivery is at least
once and unordered.

To rotate the secret, overwrite the SSM parameter and redeploy. Accept both values during the
switch.
