# sns-message-verifier

Standalone verification of AWS SNS message signatures for HTTPS webhook
endpoints. It parses a raw POST body into an `SnsEnvelope` and verifies the
signature against the certificate named by `SigningCertURL`, covering
`SignatureVersion` 1 (`SHA1withRSA`) and 2 (`SHA256withRSA`). It has no AWS SDK
dependency: it fetches the certificate over HTTPS itself and does the signature
math directly.

This crate stands on its own; nothing in the workspace sits below it, and only
`aws-messaging-webhook` depends on it. The binary builds one `SnsVerifier` and
holds it in `AppState`.

## Trust model

The trust anchor is the `SigningCertURL` host policy: a certificate is only
fetched from `https://sns.<region>.amazonaws.com(.cn)/...pem` on port 443, and
the HTTP client never follows redirects. There is no chain-to-CA verification,
the same model AWS's own validator libraries use. A verified signature proves a
message came from SNS; it does NOT prove it came from a topic you trust.
Callers must separately check `TopicArn` against an allowlist.

## API

The verifier and its builder:

- `SnsVerifier`: verifies SNS messages, fetching and caching signing
  certificates (a certificate is cached only once a signature has verified
  against it).
- `SnsVerifier::builder` returns `SnsVerifierBuilder`, with
  `http_client` (share an existing non-redirecting `reqwest::Client`),
  `dangerous_allow_cert_url_prefix` (accept a `SigningCertURL` with the given
  prefix, bypassing the host policy; tests and local development only), and
  `build`.
- `SnsVerifier::verify_body` parses a raw POST body and verifies it;
  `SnsVerifier::verify` verifies an already-parsed envelope.
- `no_redirect_client`: builds the `reqwest::Client` the scheme needs, with a
  5 second timeout and redirects disabled, so a redirect off an allowed host
  cannot defeat the host policy.

Re-exported from the internal modules:

- `SnsEnvelope`, `MessageType`: the parsed envelope and its message-type field.
- `VerifyError`, `CertUrlRejection`: the error type and the reasons a
  `SigningCertURL` is rejected.
- `build_string_to_sign`: the canonical string-to-sign builder.
- `validate_sns_url`: the `SigningCertURL` host-policy check.
- `verify_with_cert`: verifies an envelope against an already-parsed
  certificate.

## Features

- `test-fixtures`: adds the `fixtures` module (throwaway RSA keys, self-signed
  certificates, and envelope signing) for tests of code that consumes this
  crate. It pulls in `rand`, `rsa/sha1`, `rsa/sha2` and `x509-cert/builder`.
  Never enable it in production builds. `webhook-test-support` and the
  `aws-messaging-webhook` dev dependencies enable it for their tests only.

## Tests

Inline `#[cfg(test)]` modules cover canonicalization, certificate parsing and
the signature math, with `proptest` where it fits. `tests/verify.rs` drives the
public `SnsVerifier` against a `wiremock` certificate endpoint.

```bash
cargo test -p sns-message-verifier
```

## Invariants

- A certificate is fetched only from the SNS host policy's allowed hosts over
  HTTPS, and the HTTP client never follows redirects. Both are the scheme's
  trust anchor; neither is relaxed outside the `dangerous_allow_cert_url_prefix`
  escape hatch, which is for tests and local development only.
- A certificate is cached only after a signature has verified against it, so
  bad-signature envelopes cannot fill the cache or flush a live entry.
- Signature verification does not establish topic trust. A caller must check
  `TopicArn` against its own allowlist.
- Repository-wide rules are in [CLAUDE.md](../../CLAUDE.md).
