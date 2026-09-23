# Mailbox setup

Set `pMailDomain` and the stack creates:

- the SES domain identity and configuration set;
- a receipt rule that stores inbound mail in S3;
- the mail bucket and mail table;
- two SNS topics, already subscribed to the function;
- the DNS records, when `pHostedZoneId` is set.

A mailbox stack runs the function with a 90 s timeout and 512 MB, enough to parse a 40 MB
message. Without a mailbox it runs with 10 s and 256 MB.

> [!IMPORTANT]
> Deploy in a region with an SES email receiving endpoint (see the
> [SES endpoints list](https://docs.aws.amazon.com/general/latest/gr/ses.html)). Elsewhere the
> receipt rule set can't be created.

```bash
sam deploy --parameter-overrides \
  "pAllowedTopics=<your-account-id> pMailDomain=mail.example.com pMailInbox=hello \
   pApiKeysParameterName=/messaging-webhook/dev/api-keys pHostedZoneId=<zone-id>"
```

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `pMailDomain` | *(empty)* | Receiving domain and sending identity. Empty disables every mail resource |
| `pMailInbox` | *(empty)* | Local part of the one inbox (`hello` for `hello@<pMailDomain>`). Required with `pMailDomain` |
| `pMailBucketName` | *(empty)* | Empty lets CloudFormation name the bucket |
| `pMailRetentionDays` | `365` | How long a message is kept, in S3 and in the mail table |
| `pHostedZoneId` | *(empty)* | Route 53 zone. Set it and the stack publishes DNS; otherwise the `DnsRecords` output lists the records |
| `pDmarcPolicy` | `quarantine` | `none`, `quarantine` or `reject` |
| `pMailTrackingDomain` | *(empty)* | Custom domain for open and click tracking links. See [tracking.md](tracking.md) |
| `pMailTrackingHttpsPolicy` | `REQUIRE` | `REQUIRE`, `REQUIRE_OPEN_ONLY` or `OPTIONAL` |
| `pReceiptTlsPolicy` | `Optional` | `Require` rejects inbound mail not delivered over TLS |
| `pExistingReceiptRuleSetName` | *(empty)* | Add the rule to this already-active rule set instead of creating one |
| `pApiKeysParameterName` | *(empty)* | SecureString SSM parameter holding API key hashes. Required with `pMailDomain`. Must start with `/`, but not `/aws` or `/ssm` |
| `pApiKeysKmsKeyArn` | *(empty)* | KMS key for that parameter. Empty means `aws/ssm` |
| `pAttachmentUrlTtlSeconds` | `900` | Lifetime of download URLs, 60–3600 |
| `pMailWebhookUrl` | *(empty)* | HTTPS endpoint that receives mailbox events. See [events.md](events.md#webhook-delivery) |
| `pMailWebhookSecret` | *(empty)* | `NoEcho`. Sent as `x-webhook-secret`. Keep it out of `samconfig.toml` |

## After the first deploy

CloudFormation can't do these steps. Run them once. The helper reads stack outputs:

```bash
stack=aws-messaging-webhook-dev
output() { aws cloudformation describe-stacks --stack-name "$stack" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
```

1. **Publish DNS** (skip with `pHostedZoneId`). `output DnsRecords` lists the MX, three DKIM
   CNAMEs, the `bounce.<domain>` MAIL FROM records, SPF and DMARC.

2. **Wait for DKIM `SUCCESS`** before sending:

   ```bash
   aws sesv2 get-email-identity --email-identity mail.example.com \
     --query '{dkim: DkimAttributes.Status, mailFrom: MailFromAttributes.MailFromDomainStatus}'
   ```

3. **Activate the receipt rule set.** A region has one active rule set, so this deactivates any
   other. If one is already active, redeploy with `pExistingReceiptRuleSetName` instead.

   ```bash
   aws ses set-active-receipt-rule-set --rule-set-name "$(output ReceiptRuleSetName)"
   ```

   Inbound mail stops silently whenever this rule set isn't active, including after a deploy that
   replaces it. Check with `aws ses describe-active-receipt-rule-set`.

4. **Create the API key parameter.** Store SHA-256 hashes, never the keys:

   ```bash
   key="am_$(openssl rand -hex 24)"
   hash=$(printf '%s' "$key" | openssl dgst -sha256 -r | cut -d' ' -f1)
   aws ssm put-parameter --name /messaging-webhook/dev/api-keys --type SecureString \
     --value "{\"keys\":[{\"id\":\"key_1\",\"sha256\":\"$hash\"}]}"
   echo "$key"   # give this to the client; it isn't stored anywhere
   ```

   Add `--key-id <pApiKeysKmsKeyArn>` for a customer-managed key. To rotate, add the new hash,
   move clients over, then remove the old one.

5. **Verify the inbox address.** The stack registers it as its own SES identity. SES mails a
   verification link on the first deploy, before step 3 activates receiving, so that mail is
   lost. Send it again:

   ```bash
   aws ses verify-email-identity --email-address "$(output InboxAddress)"
   ```

   Open the link in the message from `no-reply-aws@amazon.com`
   (`GET /v0/inboxes/<address>/messages` lists it). Sends fail until the address is verified.

The API is at the `ApiBaseUrl` output. The `InboxAddress` output is also the `inbox_id`, as in
`/v0/inboxes/hello@mail.example.com/messages`.

## Disabling the mailbox

SES won't delete the active rule set, so deactivate it before clearing `pMailDomain`:

```bash
aws ses set-active-receipt-rule-set   # no name: deactivates the active rule set
```

With `pExistingReceiptRuleSetName`, only the stack's rule is removed and no deactivation is
needed.

The mail bucket and table are retained. Delete them by hand if you don't want them. To re-enable
with the same `pMailBucketName`, delete the retained bucket first.
