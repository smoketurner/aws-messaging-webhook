# mailbox-store

The mail table: its keys, the transaction planner, the `Table` seam with its DynamoDB
implementation, and `Mailbox`, every mailbox read and write written once on top of that seam.

Dependencies point one way: `aws-messaging-webhook` → `mailbox-store` → `mailbox`. The binary
builds a `Mailbox` over `DynamoTable`; its tests build one over the in-memory table. Both run
the same read, retry and conflict logic, because that logic lives in `Mailbox`, not in a
store.

## API

- `Mailbox<T: Table>`: ingest, the read API and sending (`insert_message`, `list_messages`,
  `get_thread`, `update_labels`, `enqueue_send`, `claim_send`, `mark_send`, and the rest).
- `Table`: the seam a store implements: `get`, `batch_get`, `query`, `put_if_absent`, one
  `transact` attempt, and the `pause` between attempts.
- `DynamoTable`: `Table` over `aws_sdk_dynamodb`.
- `PlannedOp`: one write of a planned transaction, opaque outside the crate.
- `Read`, `PageQuery`, `MailIndex`, `TxnOutcome`, `TxnDecision`: the types in `Table`'s
  signatures.
- `ListQuery`, `Page`, `ThreadView`, `EnqueueOutcome`, `DEFAULT_LIMIT`, `MAX_LIMIT`: query and
  result types.
- `MailStoreError`: `Transient` (worth a retry) or `Permanent`, plus the not-found, conflict,
  page-token and label-limit cases.
- `FromSdkFailure`: maps any SDK failure onto transient or permanent; the binary's other AWS
  clients share it.
- `keys`: `TableKey`, `PageKey`, the key builders (`keys::inbox`, `keys::message`, ...) and
  `encode_page_token` / `decode_page_token`.

The planner (`WriteOp`, `Cond`, the `plan_*` functions) and cancellation decoding are
crate-private.

## Features

- `memory`: adds `MailMemoryStore`, an in-memory `Table` with scripted failures (`Injected`,
  `ReadFailure`), and `sample_message`. `webhook-test-support` enables it, as do the dev
  dependencies of this crate and `aws-messaging-webhook`.

## Tests

- Inline `#[cfg(test)]` modules: keys and page tokens, planning, cancellation decoding, the
  in-memory table, alias rows (`src/alias_tests.rs`) and the thread race
  (`src/thread_race_tests.rs`). `dynamo_table.rs` drives `DynamoTable` against a stub
  endpoint and pins the exact JSON each DynamoDB call sends.
- `tests/mailbox_behaviour.rs`: `Mailbox` over `MailMemoryStore`, through the public API only.

```bash
cargo test -p mailbox-store
cargo mutants -p mailbox-store --jobs 3
```

Mutation testing should leave no missed mutants. `.cargo/mutants.toml` excludes the few that
are equivalent to the original code, each with its reason.

## Invariants

- Key formats and page tokens are stored data. `keys.rs` and `plan.rs` pin them against
  literal strings; a failing one means existing items or issued tokens would stop resolving.
- Every write is planned first and run as one transaction. Cancellations decode in one place,
  `txn.rs`, so every flow shares the retry and conflict policy.
- Reads default to eventually consistent. A consistent read needs a comment saying why a stale
  read would be wrong; see the consistent-read rule in [CLAUDE.md](../../CLAUDE.md).
- New store calls go through `Table`, so the in-memory table keeps the tests free of AWS.
