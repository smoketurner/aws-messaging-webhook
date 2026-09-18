# AWS documentation example payloads

Verbatim example events from the AWS documentation, one file per documented
example, organized by the webhook route that receives them. `model_fixtures.rs`
parses every file and asserts it classifies into its directory's event family —
if a model change breaks parsing of a documented payload, that test fails.

Retrieved 2026-08-04 from:

| Directory | Source page |
|---|---|
| `sms-inbound/` | <https://docs.aws.amazon.com/sms-voice/latest/userguide/two-way-sms-payload.html> |
| `sms-events/` | <https://docs.aws.amazon.com/sms-voice/latest/userguide/configuration-sets-event-format.html> |
| `ses-events/event-*` | <https://docs.aws.amazon.com/ses/latest/dg/event-publishing-retrieving-sns-examples.html> |
| `ses-events/notification-*` | <https://docs.aws.amazon.com/ses/latest/dg/notification-examples.html> |
| `ses-inbound/` | <https://docs.aws.amazon.com/ses/latest/dg/receiving-email-notifications-examples.html> |

Values are unmodified; whitespace is normalized to 2-space-indented JSON. Two
typos in the docs' examples were repaired to make them valid JSON:

- `sms-events/text-protect-blocked.json` — missing comma after
  `"protectRecommendation": "BLOCK"`.
- `ses-inbound/received-alert-s3-action.json` — invalid escape
  `"objectKey": "\email"` changed to `"email"`.

`sms-events/rcs-text-successful.json` is the docs' RCS example: RCS delivery
events reuse the `TEXT_*` event types with an RCS agent id as
`originationPhoneNumber`. The docs show two RCS examples identical in shape;
only the first is kept.

## Mailbox event schemas

`mailbox-events/schemas.json` holds the reference mailbox API's published webhook event schemas,
retrieved 2026-09-18 from its OpenAPI 3.1 specification. It contains the eight webhook event
payload schemas (`events_*Event`) and every schema they reference, in the spec's own
`components.schemas` layout so each `$ref` resolves unchanged. Values are unmodified and
whitespace is normalized to 2-space-indented JSON. `mailbox_event_schemas.rs` validates every
mailbox event the relay publishes against them and fails if the file documents an event type
the test doesn't account for, so a refreshed copy surfaces new types.
