# EventBridge events

Events publish to the `<stack-name>-events` bus with `source` set to `pEventSource`
(default `aws-messaging-webhook`). Mailbox events use a different format; see
[mailbox/events.md](mailbox/events.md).

| Detail type | Published when |
|---|---|
| `sms.inbound`, `sms.delivery`, `mms.delivery`, `voice.delivery` | EUM delivers an inbound message or delivery receipt |
| `ses.send`, `ses.delivery`, `ses.bounce`, `ses.complaint`, `ses.reject`, `ses.open`, `ses.click`, `ses.rendering-failure`, `ses.delivery-delay`, `ses.subscription` | SES reports a sending event |
| `ses.inbound` | SES receives a message |
| `ses.inbound.quarantined` | SES receives a message with a spam or virus verdict of `FAIL`. It is labeled, not dropped |
| `ses.unknown` | A valid SES event of a type this version doesn't map |
| `message.status.changed` | A message's `current_status` changes |
| `subscription.changed` | The function re-subscribed a topic |
| `unknown` | A payload matched no known shape. It is forwarded unchanged |

## Detail

```json
{
  "schemaVersion": 1,       // bumped only on a breaking shape change
  "meta": {
    "snsMessageId": "…",
    "messageId": "…",        // DynamoDB key: pk = MSG#<messageId>
    "previousMessageId": "…", // sms.inbound replies only
    "topicArn": "…",
    "receivedAt": "…",
    "webhookPath": "/webhooks/ses/events",
    "s3": { "bucket": "…", "key": "…" },  // ses.inbound via the S3 action only
    "inbound": {                           // ses.inbound only
      "headers": { "from": ["…"], "to": ["…"], "subject": "…", "date": "…", "messageId": "<…>" },
      "auth": { "spf": "PASS", "dkim": "PASS", "dmarc": "FAIL", "spam": "PASS", "virus": "PASS", "dmarcPolicy": "reject" }
    }
  },
  "event": { /* the AWS payload, verbatim */ }
}
```

Conditional `meta` fields are absent, not null, when they don't apply:

- `previousMessageId` links an `sms.inbound` reply to the message you sent.
- `s3` points at the stored message on `ses.inbound` and `ses.inbound.quarantined`. It survives
  size reduction.
- `inbound` carries SES's parsed headers and auth verdicts, so consumers can route mail without
  fetching it.

## Status changes

`message.status.changed` fires when `current_status` changes (for example `sent` → `delivered`).
Opens and clicks only bump counts and don't fire it.

```json
{
  "schemaVersion": 1,
  "meta": { "messageId": "…", "webhookPath": "/webhooks/ses/events" },
  "status": {
    "current": "delivered",   // bounced | complained | failed | received | sent | …
    "bounceType": "…",         // on a bounce
    "firstEventAt": "…", "lastEventAt": "…",
    "openCount": 0, "clickCount": 0,
    "botOpenCount": 0, "botClickCount": 0
  }
}
```

Opens and clicks SES flags `isBotEvent: Likely` (Apple Mail Privacy Protection, security
scanners) count as bot opens and clicks. The raw flag is at `detail.event.open.isBotEvent` on
`ses.open` and `ses.click`.

## Oversized events

EventBridge accepts entries up to 256 KB. A larger event shrinks in steps until it fits:

1. Drop the raw MIME of an SES inbound message.
2. Replace `event` with `{ "payloadOmitted": true, … }`.
3. Keep only `messageId`, `snsMessageId`, `webhookPath` and `s3` in `meta`. These are bounded,
   so this step always fits.

The full payload stays in DynamoDB under `meta.messageId`.
