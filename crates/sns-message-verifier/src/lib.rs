//! Verification of AWS SNS message signatures for HTTPS webhook endpoints.
//!
//! SNS signs every message it delivers over HTTP(S). This crate parses the raw
//! POST body into an [`SnsEnvelope`] and verifies the signature against the
//! certificate referenced by `SigningCertURL`, supporting `SignatureVersion` 1
//! (`SHA1withRSA`) and 2 (`SHA256withRSA`).
//!
//! # Trust model
//!
//! The trust anchor is the `SigningCertURL` host policy: the certificate is
//! only fetched from `https://sns.<region>.amazonaws.com(.cn)/...pem` on port
//! 443. There is no chain-to-CA verification — the same model as AWS's own
//! validator libraries. Signature verification proves a message came from SNS;
//! it does NOT prove it came from a topic you trust. Callers must separately
//! check `TopicArn` against an allowlist.
