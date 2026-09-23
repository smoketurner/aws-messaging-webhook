# Data model

The events table (output `TableName`) holds two items per message key. The mail table is in
[mailbox/storage.md](mailbox/storage.md).

| Item | `pk` | `sk` | Holds | TTL |
|---|---|---|---|---|
| Event | `MSG#<messageId>` | `EVT#<timestamp>#<snsMessageId>` | Raw body as received, parse metadata | `pRawEventRetentionDays` (30) |
| Summary | `MSG#<messageId>` | `AGG` | `current_status`, first/last event time, open and click counts and times, bot open and click counts, `bounce_type` | `pAggregateRetentionDays` (365) |

- **Full history:** `Query` on `pk`.
- **Current state:** `GetItem` on `pk` and `sk = AGG`.

Each new event item is what the stream relay publishes to EventBridge.

## Consumer read access

Consumers read the table when an event arrives with `payloadOmitted`, or to get a message's
history. Set `pConsumerAccountIds` and the stack creates a role (`ConsumerReadRoleArn`) those
accounts can assume. It allows `GetItem`, `BatchGetItem` and `Query` on the table only. Use
`meta.messageId` from any event as `<messageId>`.
