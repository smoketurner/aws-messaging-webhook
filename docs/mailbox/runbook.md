# Mailbox runbook

The commands use the `output` helper from [setup.md](setup.md#after-the-first-deploy).

## Metrics

Mail metrics use the same EMF namespace and `function` dimension as the
[pipeline metrics](../operations.md#metrics):

| Metric | Counts |
|---|---|
| `MessagesIngested`, `IngestFailures`, `IngestSkipped`, `IngestTimeouts` | Inbound mail processing |
| `MessagesSent`, `SendFailures`, `SendOutcomeUnknown` | Send outcomes |
| `ApiAuthFailures` | Rejected API keys |

Alarm on `SendOutcomeUnknown`, `IngestFailures`, and the depth of `AsyncInvokeDlqUrl` and
`MailSenderDlqUrl`.

## How a send can get stuck

The sender claims a queued send, calls SES once, and records the outcome. A sweep runs every 10
minutes and handles claims older than 15 minutes (7.5 times the sender's 120-second timeout):

- If the sender never reached SES, the sweep releases the claim for another attempt. Repeated
  releases fail the send as `sender_abandoned`.
- If the sender may have reached SES, the sweep marks the send `unknown`.

The first two situations below invoke the sender directly. `lambda:InvokeFunction` gates them,
not an API key.

## A send is `unknown`

SES may or may not hold the message. Nothing retries it automatically, because a resend can
deliver twice. Find these sends in the sweep's `sends_outcome_unknown` log line or the `ByStatus`
index, then choose:

```bash
sender=$(output MailSenderFunctionName)

# Send again, accepting the recipient may get two copies.
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"resend","message_id":"<id>"}' /dev/stdout

# Or record the outcome without sending.
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"close_sent","message_id":"<id>"}' /dev/stdout
aws lambda invoke --function-name "$sender" --payload \
  '{"command":"close_failed","message_id":"<id>"}' /dev/stdout
```

These commands accept only `unknown` sends. `close_sent` labels the message `sent` with no SES
id. `close_failed` labels it `rejected` with reason `closed_by_operator`.

## Inbound mail failed to ingest

After two retries, Lambda writes the event to `AsyncInvokeDlqUrl`. Replay it:

```bash
queue=$(output AsyncInvokeDlqUrl)
msg=$(aws sqs receive-message --queue-url "$queue" --max-number-of-messages 1)
echo "$msg" | jq -r '.Messages[0].Body' | jq '.requestPayload' > /tmp/replay.json

aws lambda invoke --function-name "$(output WebhookFunctionName)" \
  --payload file:///tmp/replay.json /dev/stdout

# Once it succeeds, delete the queue message.
aws sqs delete-message --queue-url "$queue" \
  --receipt-handle "$(echo "$msg" | jq -r '.Messages[0].ReceiptHandle')"
```

Replays are safe: ingest derives the same ids every time. The replay re-verifies the signature,
so it works only while SES still serves the signing certificate. The queue keeps messages for 14
days.

## A send is stuck in `queued`

`MailSenderDlqUrl` receives a record when the sender runs out of retries. The record holds a
stream position, not the send, so it can't be replayed. Fix the cause, then re-trigger the
sender by setting `requeued_at`:

```bash
aws dynamodb update-item --table-name "$(output MailTableName)" \
  --key '{"pk":{"S":"OUTBOX#<message-id>"},"sk":{"S":"STATE"}}' \
  --update-expression 'SET requeued_at = :now' \
  --condition-expression 'send_status = :queued AND attribute_not_exists(requeued_at)' \
  --expression-attribute-values "{\":now\":{\"S\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\"},\":queued\":{\"S\":\"queued\"}}"
```

If the send already has `requeued_at`, remove it first (`REMOVE requeued_at`). Then delete the
queue message.
