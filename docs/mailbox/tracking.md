# Open and click tracking

Tracking is always on. SES adds a 1×1 pixel to the `html` body (an **open**) and rewrites each
link into a counted redirect (a **click**). Text-only messages aren't tracked.

Opens and clicks publish as `ses.open` and `ses.click` and update the message's counts (see
[events.md](../events.md#status-changes)). An open also labels the mailbox message `opened`.

## Use your own tracking domain

By default, tracking links use SES's `awstrack.me`, which recipients see when hovering a link. Set
`pMailTrackingDomain` to use a subdomain of yours:

```bash
sam deploy --parameter-overrides "… pMailTrackingDomain=click.mail.example.com"
```

The stack follows [SES's HTTPS setup](https://docs.aws.amazon.com/ses/latest/dg/configure-custom-open-click-domains.html):
a CloudFront distribution in front of `r.<region>.awstrack.me`, an ACM certificate, and A, AAAA
and HTTPS alias records. This needs:

- `pHostedZoneId`, to validate the certificate and publish the records;
- a stack in us-east-1, the only region CloudFront takes certificates from;
- a subdomain of `pMailDomain`, or another domain already verified in SES.

`pMailTrackingHttpsPolicy` controls which links use HTTPS: `REQUIRE` (default, all),
`REQUIRE_OPEN_ONLY` (pixel only) or `OPTIONAL`.

Check it works:

```bash
curl --head https://click.mail.example.com/favicon.ico
```

Expect `x-amz-ses-region` to be the stack's region and `x-amz-ses-request-protocol` to be `https`.

## Per-message control

- `{{ses:openTracker}}` places the pixel there instead of at the end, where clients that clip
  long messages may never load it. Use one per message; a second makes the send fail.
- `<a ses:no-track href="…">` leaves that link untracked.

SES rewrites at most 250 links per message and skips URLs that aren't RFC 3986-encoded.
