# aws-messaging-webhook

A single Rust Lambda function (Axum behind a Lambda Function URL) that receives AWS messaging
events delivered over SNS — End User Messaging two-way SMS and delivery receipts, SES sending
events, and SES inbound email notifications — then:

1. Verifies the SNS message signature (the Function URL is public; the signature plus a topic
   allowlist are the security boundary)
2. Auto-confirms SNS subscriptions from allowlisted topics
3. Durably persists every event to DynamoDB as an event-sourced store with a per-message
   aggregate (delivery status, opens, clicks)
4. Performs mechanical lifecycle actions inline: SMS message feedback (`PutMessageFeedback`),
   STOP/START opt-out handling (`PutOptedOutNumber`/`DeleteOptedOutNumber`), and SES
   account-level suppression on hard bounces and complaints (`PutSuppressedDestination`)
5. Re-publishes normalized events to a custom EventBridge bus for downstream applications

Status: under construction. Deployment contract, event taxonomy, and operations docs land with
the SAM template.
