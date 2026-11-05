# mailbox

The mailbox domain: the stored message, thread and send types, their caps, MIME parsing and
building, and the JSON wire shapes. It does no I/O: no AWS SDK, HTTP framework or async
runtime.

Dependencies point one way: `aws-messaging-webhook` → `mailbox-store` → `mailbox`. The store
persists these types and the binary serves and sends them; nothing here knows about either.

## API

The crate root holds the stored item types and the caps every layer enforces:

- `MailMessage`: one stored message, bounded by the caps below.
- `InboxId`, `Inbox`: an inbox's normalized id and its record.
- `ThreadSnapshot`, `RfcHit`, `AttachmentMeta`: what a message records about its thread,
  an alias row's target, and one attachment.
- `InsertOutcome`, `PutOutcome`: the results of an insert and of a conditional create.
- `MESSAGE_USER_LABEL_CAP`, `THREAD_USER_LABEL_CAP`, `REFERENCES_MAX_BYTES` and the other
  size and count caps.

The rest is organised by module:

- `address`: envelope addresses and mailbox comparison.
- `build`: `build_outbound`, the outbound MIME document from a send spec and its parts.
- `content`: `MessageContent`, a message's write-once body document.
- `ids`: message, attachment and event ids (`UUIDv7`), and our `Message-ID` forms.
- `labels`: system labels, user label counting and inbound verdict classification.
- `mime`: `parse_inbound`, inbound MIME into a `ParsedInbound`.
- `send`: `SendState`, `SendSpec`, `SendKey` and `mark_transition`, the outbound send.
- `thread`: `ThreadState`, `AliasLookup`, `new_thread` and `apply_message`, thread
  resolution.
- `time`: fixed-width UTC timestamps that sort as strings.
- `url_policy`: `parse_attachment_url`, which outbound URLs may be fetched.
- `wire`: the JSON shapes the `/v0` API and the event relay serialize.

## Features

- `test-fixtures`: adds `test_fixtures::message`, a sample `MailMessage` at
  `support@example.com`. `mailbox-store` and `aws-messaging-webhook` enable it for their
  tests only.

## Tests

The tests are inline `#[cfg(test)]` modules, with `proptest` for ids, timestamps and thread
resolution.
MIME parsing reads the sample messages in `tests/fixtures/mail/`.

```bash
cargo test -p mailbox
cargo test -p mailbox mime::   # one module
```

## Invariants

- Stored and wire formats are pinned by tests that compare against literal strings: stored
  status and failure strings in `send.rs`, typed item fields in `lib.rs`, the 24-byte
  timestamp in `time.rs`, and the wire golden shapes in `wire.rs`. A failing one means stored
  items or API clients would break; change the code, not the expected string.
- A field with `skip_serializing_if` must also carry `default`. `serde_dynamo` omits the
  attribute when the predicate holds, and reading the item back fails with "missing field"
  unless the field has a default.
- Repository-wide rules are in [CLAUDE.md](../../CLAUDE.md).
