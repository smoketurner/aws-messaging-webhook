# Security Policy

## Supported Versions

Only the latest release on the `main` branch is supported with security updates.

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |
| < latest | :x:               |

## Reporting a Vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

Please report vulnerabilities privately by emailing **security@smoketurner.com**.

Include as much of the following as possible:

- Description of the vulnerability
- Steps to reproduce or proof of concept
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Acknowledgment:** within 72 hours of report
- **Resolution target:** within 30 days of acknowledgment

We will keep you informed of progress toward a fix and may ask for additional
information or guidance.

## Security Model

This project is an internet-facing webhook endpoint. Its security boundary relies on:

1. **SNS signature verification** — all inbound messages are cryptographically
   verified against AWS SNS signing certificates (v1 and v2 signatures).
2. **Topic allowlist enforcement** — signature verification alone only proves a
   message originated from *some* SNS topic in *some* account. The mandatory
   allowlist restricts accepted topics to an explicit set, preventing
   unauthorized sources from injecting events.

Both controls are required and enforced on every request. Bypassing either is
considered a critical vulnerability.

## Credit and Attribution

Reporters will be credited in the security advisory unless they indicate a
preference not to be named.
