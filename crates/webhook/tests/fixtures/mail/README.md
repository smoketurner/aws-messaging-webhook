# Mail parse fixtures (AT11, §11 P1)

Synthetic `.eml` messages hand-written for `mail::mime::parse_inbound` tests
— not sourced from any external documentation. `plan.md` §11 lists the
scenarios these cover; large messages (150 tiny parts, a ~30 MB message, a
1 MB body) are generated at test time rather than committed here.

| File | Scenario |
|---|---|
| `plain.eml` | Plain `text/plain` message, no attachments |
| `multipart-alternative.eml` | `multipart/alternative` text + HTML |
| `attachment-content-id.eml` | Inline image with a `Content-ID` |
| `reply-references.eml` | `In-Reply-To` and multi-entry `References` |
| `inline-text-parts.eml` | Multiple `text/plain` body parts (A5 concatenation) |
| `nested-rfc822.eml` | A `message/rfc822` attachment (not flattened) |
| `encoded-word-filename.eml` | RFC 2231 continuation-encoded filename |
| `no-filename.eml` | An attachment with neither a Content-Disposition nor Content-Type filename |
| `malformed.eml` | No headers at all — `mail-parser` returns `None` |
| `hostile-content-type.eml` | A `Content-Type` over the 127-byte normalization cap |
