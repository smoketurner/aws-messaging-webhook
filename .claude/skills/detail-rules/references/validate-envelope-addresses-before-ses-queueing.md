# Validate Envelope Addresses Before SES Queueing

Every SES destination or Reply-To address must pass through `envelope_address` before being queued; `mail-parser`'s `Addr::address` field must never be used directly as an envelope address.

## What to look for

### 1. Bypassing `envelope_address` for any SES-bound address

Every address that will appear as a destination, Reply-To, or Return-Path in an SES send call must go through `envelope_address` in `crates/webhook/src/api/send/validate.rs`. Using `Addr::address` directly, trimming and passing it on, or doing only a `contains('@')` check are all violations.

### 2. Trusting mail-parser's `Addr::address` as a ready-made mailbox

`mail-parser`'s `Addr::address` field is not an RFC 5321 mailbox. In version 0.11.9 it silently drops quoted local parts (`"a b"@example.com` → `@example.com`) and passes embedded whitespace (`ada@ example.com`) and double-`@` (`a@b@example.com`) verbatim. Passing `addr.address.as_deref()` or `addr.address.as_ref()` directly to SES is a violation.

### 3. Insufficient shape-checks on the reduced address

A check of the form `!address.contains('@')` is not sufficient — it accepts `@example.com` (empty local), `ada@ example.com` (whitespace), and `a@b@example.com` (double `@`). Any gate weaker than the full `envelope_address` two-pass parse is a violation.

### 4. Incorrectly applying `ADDRESS_MAX_BYTES` to a `"Name <address>"` string

`format_addr` in `crates/webhook/src/mail/mime.rs` stores inbound participants in `"Name <address>"` form. Applying `truncate_bytes` to the combined string can remove the `<address>` portion, leaving a string that no longer parses back to a mailbox. The display name must give way to the cap, never the address itself.

### 5. Comparing stored participants to an inbox address without parsing

A stored participant string is in display form (`Support <support@example.com>`). Comparing it with `eq_ignore_ascii_case` to a bare inbox address is always false for non-bare entries. The mailbox must be extracted with `envelope_address` before comparing.

### 6. Missing ASCII check on the local part

SES has no SMTPUTF8 support. Accepting a non-ASCII local part (e.g., `üser@example.com`) without rejecting it with a 400 is a violation — it produces a 200, queues the message, and then receives a terminal SES rejection.

### 7. Passing a non-ASCII domain without Punycode encoding

A non-ASCII domain must be encoded with `structured-email-address`'s `canonical()` (which applies IDNA) before reaching the SES envelope. Queueing a non-ASCII domain as-is causes a terminal SES rejection after the caller was told 200.

## Violation examples

**Trusting mail-parser's Addr::address directly (pre-ffa6247 pattern in mime.rs)**
```rust
fn format_addr(addr: &Addr<'_>) -> Option<String> {
    let address = addr.address.as_deref()?;
    let formatted = match addr.name.as_deref().map(str::trim) {
        Some(name) if !name.is_empty() => format!("{name} <{address}>"),
        _ => address.to_owned(),
    };
    Some(truncate_bytes(&formatted, ADDRESS_MAX_BYTES))
    // BUG: truncates combined string; can remove the <address> portion entirely
}
```

**Insufficient shape-check after mail-parser reduction (pre-bb1d297 pattern in validate.rs)**
```rust
// In envelope_address: returns the raw parser output after only a non-empty check
addr.address
    .as_deref()
    .map(str::trim)
    .filter(|address| !address.is_empty())  // BUG: @example.com passes, spaces pass
    .map(ToOwned::to_owned)
    .ok_or("is not an email address")
```

**Only `contains('@')` gating in addresses() (pre-bb1d297 pattern in validate.rs)**
```rust
if !address.contains('@') {  // BUG: accepts ada@ example.com, a@b@example.com, @example.com
    errors.push(field(path, format!("`{address}` is not an email address")));
    continue;
}
```

**Inbox comparison without parsing (pre-ffa6247 pattern in reply.rs)**
```rust
// Direct string comparison fails for "Support <support@example.com>"
if original.from.eq_ignore_ascii_case(inbox.as_str()) {
    return original.to.clone();
}
```

**Tab in quoted local part reaching the envelope (pre-7667a0f pattern)**

Using only `Strictness::Standard` (RFC 5322) to parse the entry is insufficient: RFC 5322 allows a tab inside a quoted string, but RFC 5321's `qtextSMTP` does not. A single-pass parse with `ENTRY_CONFIG` alone would let `"a\tb"@example.com` through.

## Correct patterns

**Two-pass parse in `envelope_address` (current validate.rs:292–301)**
```rust
pub(super) fn envelope_address(value: &str) -> Result<String, String> {
    let address = EmailAddress::parse_with(value, &ENTRY_CONFIG)
        .map_err(|error| format!("is not an email address: {error}"))?;
    if !address.local_part().is_ascii() {
        return Err("has a non-ASCII local part, which SES cannot deliver to".to_owned());
    }
    let address = address.canonical();
    EmailAddress::parse_with(&address, &ENVELOPE_CONFIG)
        .map_err(|error| format!("is not an SMTP mailbox: {error}"))?;
    Ok(address)
}
```

- `ENTRY_CONFIG` uses `Strictness::Standard` with `allow_display_name()` and `allow_domain_literal()` — RFC 5322 syntax for the full entry.
- `ENVELOPE_CONFIG` uses `Strictness::Strict` with `allow_quoted_local_part()` and `allow_domain_literal()` — RFC 5321 `Mailbox` grammar, re-parsed against the canonical form to reject tabs in quoted local parts and enforce SMTP length limits.
- `.canonical()` returns minimum quoting, lowercase domain, Punycode for non-ASCII domains.

**Name-preserving truncation in `format_addr` (current mime.rs:192–203)**
```rust
fn format_addr(addr: &Addr<'_>) -> Option<String> {
    let address = addr.address.as_deref()?;
    let framing = " <".len() + address.len() + ">".len();
    let name = addr
        .name
        .as_deref()
        .zip(ADDRESS_MAX_BYTES.checked_sub(framing))
        .and_then(|(name, budget)| display_name(name, budget));
    Some(match name {
        Some(name) => format!("{name} <{address}>"),
        None => truncate_bytes(address, ADDRESS_MAX_BYTES),
    })
}
```
The address portion is never truncated; only the display name gives way to the cap.

**Inbox comparison after parsing (current reply.rs:146–148)**
```rust
fn is_inbox(inbox: &InboxId, participant: &str) -> bool {
    envelope_address(participant).is_ok_and(|address| address.eq_ignore_ascii_case(inbox.as_str()))
}
```

## Scope

- `crates/webhook/src/api/send/validate.rs` — primary: `envelope_address`, `ENTRY_CONFIG`, `ENVELOPE_CONFIG`, and the `addresses()` helper.
- `crates/webhook/src/mail/mime.rs` — `format_addr` and `display_name`: stored participant strings must round-trip through `envelope_address`.
- `crates/webhook/src/api/send/reply.rs` — `is_inbox` and `address_reply`: any comparison between a stored participant and an inbox address must parse through `envelope_address` first.
- Any future code that constructs an SES `Destination`, `ReplyToAddresses`, or `ReturnPath` field must call `envelope_address` and use the returned canonical string, not a raw API input or mail-parser field.
