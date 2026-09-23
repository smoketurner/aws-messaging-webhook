# Deploy

You need Rust (the version in `rust-toolchain.toml`), [`cargo-lambda`](https://cargo-lambda.info)
and the AWS SAM CLI.

```bash
sam build
sam deploy --guided
```

The guided deploy asks for a stack name, region and parameters, and saves them to
`samconfig.toml` (gitignored). Later deploys are plain `sam deploy`. The stack needs
`CAPABILITY_IAM`.

> [!IMPORTANT]
> **Set `pAllowedTopics`.** A valid signature proves only that a message came from SNS in *some*
> AWS account. The allowlist stops strangers subscribing your public endpoint to their topics.
> Empty accepts every topic; use that only in development.

> [!WARNING]
> **Keep raw message delivery off** (the default) on every subscription. Raw delivery strips the
> signed envelope, and the function rejects the message.

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `pStage` | `dev` | `dev` or `prod`. `prod` turns on DynamoDB deletion protection |
| `pAllowedTopics` | *(empty)* | Comma-separated 12-digit account ids and topic ARN globs |
| `pAutoResubscribe` | `true` | Re-subscribe when someone abuses an unauthenticated `UnsubscribeURL` |
| `pOptOutListName` | *(empty)* | EUM opt-out list that `STOP`/`START` update. Empty disables the action |
| `pEventSource` | `aws-messaging-webhook` | `source` on published EventBridge events |
| `pRawEventRetentionDays` | `30` | TTL of raw event items |
| `pAggregateRetentionDays` | `365` | TTL of per-message summary items |
| `pLogLevel` | `INFO` | `DEBUG`, `INFO`, `WARN` or `ERROR`. `TRACE` is refused: the runtime and SDK log raw payloads at that level |
| `pLogRetentionDays` | `30` | CloudWatch log retention |
| `pConsumerAccountIds` | *(empty)* | Accounts allowed to assume the read-only consumer role. See [data-model.md](data-model.md#consumer-read-access) |

Mailbox parameters are in [mailbox/setup.md](mailbox/setup.md).

## Subscribe topics over HTTPS

Topics and subscriptions live outside the stack, next to your EUM and SES configuration. Subscribe
each topic to the endpoint output that matches it:

| Output | Topic |
|---|---|
| `SmsInboundEndpoint` | EUM two-way SMS inbound |
| `SmsEventsEndpoint` | EUM configuration-set events (delivery receipts) |
| `SesEventsEndpoint` | SES configuration-set events |
| `SesInboundEndpoint` | SES receipt rule (inbound mail) |

```bash
endpoint=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='SesEventsEndpoint'].OutputValue" --output text)
aws sns set-topic-attributes --topic-arn <topic-arn> \
  --attribute-name SignatureVersion --attribute-value 2
aws sns subscribe --topic-arn <topic-arn> --protocol https \
  --notification-endpoint "$endpoint"
```

The subscription confirms within seconds. If `PendingConfirmation` stays `true`, the topic is
not allowlisted or raw delivery is on; the function logs say which.

The payload's shape decides how an event is handled, not the path. A topic on the wrong path
logs `family_mismatch` and is still processed.

## Or subscribe the function directly

A topic can invoke the function instead of calling the URL. Verification and the allowlist apply
the same way; confirmation happens through IAM. Always scope `--source-arn` to one topic: it is
this path's per-topic gate.

```bash
function_arn=$(aws cloudformation describe-stacks --stack-name aws-messaging-webhook-dev \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookFunctionArn'].OutputValue" --output text)
aws lambda add-permission --function-name "${function_arn}" \
  --statement-id "sns-<topic-name>" --action lambda:InvokeFunction \
  --principal sns.amazonaws.com --source-arn <topic-arn>
aws sns subscribe --topic-arn <topic-arn> --protocol lambda \
  --notification-endpoint "${function_arn}"
```

| Path | SNS retries delivery | On a function error |
|---|---|---|
| HTTPS | 3 times, 20 s apart (default policy; configurable up to 100 retries over 3,600 s) | The 5xx response triggers SNS's retries |
| Direct | 100,015 times over 23 days (AWS-managed policy) | Lambda retries twice, then writes the event to `AsyncInvokeDlqUrl` |

Don't add your own on-failure destination: a function has one, and yours replaces the stack's.

## Requirements on your side

- **Signature version.** Use `SignatureVersion` 2 (SHA-256). Version 1 also verifies.
- **SES inbound.** Use the receipt rule's S3 action. SNS limits message size, and an oversized
  message loses its embedded content in the EventBridge event.
- **SMS opt-outs.** `STOP`/`START` handling needs self-managed opt-outs and `pOptOutListName`.
  With AWS-managed opt-outs, AWS handles `STOP` before SNS sees it.
