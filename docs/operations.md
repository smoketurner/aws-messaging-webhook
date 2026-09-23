# Operations

Mailbox operations are in [mailbox/runbook.md](mailbox/runbook.md).

## Logs

Structured JSON, one INFO line per message. The request path logs `outcome` (`persisted`,
`duplicate`, `confirmed` or `resubscribed`) and `action`. The relay logs `outcome=published`.

## Metrics

CloudWatch Embedded Metric Format (EMF), in the stack-name namespace, with a `function`
dimension:

| Metric | Counts |
|---|---|
| `MessagesReceived`, `Duplicates` | Messages handled; redeliveries caught by the conditional write |
| `SignatureRejections`, `AllowlistRejections` | Messages refused at the security boundary |
| `UnclassifiedPayloads` | Payloads matching no known shape |
| `EventsPublished`, `PublishFailures` | Relay publishes to EventBridge |
| `ActionFailures`, `InternalErrors` | Failed lifecycle actions; unexpected errors |
| `Resubscribes`, `SubscriptionsLost` | Re-subscriptions; cancellations left in place |
| `ColdStart`, `Latency` | New execution environments; handling time (ms histogram) |

## What to alarm on

Alarms are yours to build on the metrics and queues. These signals are worth one:

| Signal | Means |
|---|---|
| `SubscriptionsLost` > 0 | A subscription was cancelled and not re-attached (`pAutoResubscribe=false`) |
| Messages in `PublishDlqUrl` | Events used up their publish retries |
| Messages in `AsyncInvokeDlqUrl` | Direct invocations used up their retries |
| Sustained `UnclassifiedPayloads` | A new AWS event shape, or junk on a topic |
| Rising stream `IteratorAge` | The relay is falling behind |

## Failure handling

- **Request path.** A transient failure returns 5xx and SNS redelivers. The conditional write
  catches the repeat, and only repeat-safe actions run again.
- **Relay.** The stream mapping retries, bisects failing batches and reports per-record
  failures. Records that still fail go to the publish DLQ.

Delivery is at least once end to end. Consumers must tolerate duplicates.

## End-to-end check

```bash
aws sesv2 send-email --from-email-address <verified-sender> \
  --destination ToAddresses=bounce@simulator.amazonses.com \
  --content "Simple={Subject={Data=probe},Body={Text={Data=probe}}}"
```

Expect a `ses.bounce` event on the bus, an event item under `pk = MSG#<messageId>`, and the
simulator address on the SES account suppression list.
