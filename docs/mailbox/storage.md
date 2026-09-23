# Mailbox storage

## Mail table

The mail table (output `MailTableName`) is keyed by inbox and message.

| Item | `pk` | `sk` | Holds |
|---|---|---|---|
| Inbox | `INBOX#<inbox>` | `META` | Address, display name, metadata, timestamps |
| Message | `INBOX#<inbox>` | `MSG#<messageId>` | Addresses, subject, preview, labels, attachment metadata, send status |
| Thread | `INBOX#<inbox>` | `THR#<threadId>` | Rolled-up subject, preview, senders, recipients, labels, counts |
| RFC alias | `RFC#<inbox>#<rfc-id>` | `RFC` | Maps a `Message-ID` to its message and thread |
| Send state | `OUTBOX#<messageId>` | `STATE` | Send status, envelope, claim and failure details |
| Send key | `SENDKEY#<sha256>` | `KEY` | What an `Idempotency-Key` resolves to |
| SES reference | `SESMSG#<sesMessageId>` | `REF` | Maps an SES message id to a mailbox message |

| Index | Keys | Serves |
|---|---|---|
| `ByTime` | `gsi1pk`/`gsi1sk` | Messages, threads and inboxes in time order |
| `ByThread` | `gsi2pk`/`gsi2sk` | One thread's messages |
| `ByStatus` | `gsi3pk`/`gsi3sk` | Send states by status (sparse) |

Message and thread ids are UUIDv7s, so they sort by time. Inbound ids derive from the SES message
id and receipt time, so a redelivery maps to the same message.

## Mail bucket

| Prefix | Holds |
|---|---|
| `inbound/raw/` | Raw MIME, written by SES |
| `messages/<inbox>/<message_id>.json` | Bodies, headers, `References`, `Reply-To` and verdicts |
| `attachments/<message_id>/<attachment_id>` | Attachments extracted from inbound mail |
| `outbox/<message_id>/` | A send's `spec.json` and its attachment parts |

## Retention

Objects under `inbound/`, `attachments/` and `messages/` expire after `pMailRetentionDays`. The message's table items carry the same TTL, so a message ages out whole.
Objects under `outbox/` don't expire. The bucket and table survive stack deletion.

## Wiring

- The stack owns two topics, `MailInboundTopicArn` and `MailEventsTopicArn`, subscribed to the
  function directly. A non-empty `pAllowedTopics` includes them automatically.
- The mail table's stream has two readers: the relay and the sender. That is DynamoDB's
  recommended maximum.
