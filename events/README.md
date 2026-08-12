# Sample Event Payloads

These JSON files are example SNS event payloads for use with `cargo lambda invoke`
during local development.

## Usage

```bash
cargo lambda watch &
cargo lambda invoke aws-messaging-webhook --data-file events/sms-inbound.json
cargo lambda invoke aws-messaging-webhook --data-file events/ses-bounce.json
cargo lambda invoke aws-messaging-webhook --data-file events/ses-delivery.json
```

## Notes

- These payloads use the **direct SNS invoke** pathway (the `Records` array shape).
- Signatures are placeholder values (`EXAMPLE`) — they will fail verification unless
  `ALLOWED_TOPICS` is empty (accept-all mode) or you use properly signed envelopes
  from the test-fixtures feature.
- For Function URL (HTTPS pathway) testing, wrap the SNS JSON in a Lambda Function
  URL event envelope or use `curl` against the local server.
