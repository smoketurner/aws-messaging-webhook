# Sample Event Payloads

These JSON files are example SNS event payloads for use with `cargo lambda invoke`
during local development.

## Usage

### Direct SNS invoke (Records array)

```bash
cargo lambda watch &
cargo lambda invoke aws-messaging-webhook --data-file events/sms-inbound.json
cargo lambda invoke aws-messaging-webhook --data-file events/ses-bounce.json
cargo lambda invoke aws-messaging-webhook --data-file events/ses-delivery.json
```

### Function URL (HTTPS pathway)

```bash
cargo lambda watch &
cargo lambda invoke aws-messaging-webhook --data-file events/function-url-sms-inbound.json
```

## Notes

- The direct SNS invoke payloads use the `Records` array shape and are dispatched
  without going through the Axum router.
- The Function URL payloads wrap the SNS JSON in a Lambda Function URL event envelope
  (API Gateway V2 / HTTP API format) and are routed through the Axum router at the
  `/webhooks/{family}` paths.
- Signatures are placeholder values (`EXAMPLE`) -- they will fail verification unless
  `ALLOWED_TOPICS` is empty (accept-all mode) or you use properly signed envelopes
  from the test-fixtures feature.
- For live Function URL testing with `curl`, point at the local server started by
  `cargo lambda watch` and POST the raw SNS JSON body with the appropriate
  `x-amz-sns-message-type` header.
