//! Mail addresses: the envelope form SES delivers to, and the mailbox a
//! stored participant names.
//!
//! Participants are stored the way they arrived — `Alice <alice@example.net>`
//! from one header, `alice@example.net` from another — so the read API shows
//! what the sender wrote. Two participants are the same recipient when
//! [`same_mailbox`] says so, never because their strings are equal.

use std::sync::LazyLock;

use structured_email_address::{Config, EmailAddress, Strictness};

/// Reads one recipient entry: RFC 5322 syntax around the address (display
/// name, comments, whitespace between tokens) is accepted, and groups are
/// refused because an entry names one recipient.
static ENTRY_CONFIG: LazyLock<Config> = LazyLock::new(|| {
    Config::builder()
        .strictness(Strictness::Standard)
        .allow_domain_literal()
        .allow_display_name()
        .build()
});

/// The RFC 5321 section 4.1.2 `Mailbox` grammar, which is narrower than the
/// RFC 5322 one the entry is read with: a quoted local part may not hold a
/// tab, for one (`qtextSMTP`, `quoted-pairSMTP`).
static ENVELOPE_CONFIG: LazyLock<Config> = LazyLock::new(|| {
    Config::builder()
        .strictness(Strictness::Strict)
        .allow_quoted_local_part()
        .allow_domain_literal()
        .build()
});

/// The address the envelope carries for one recipient entry: the address
/// itself, with any display name or comment around it removed, in the form
/// SES accepts.
///
/// A caller may write `Ada Lovelace <ada@example.com>`, and a reply derives
/// its recipients from the stored `From` of the message it answers, which is
/// kept in that form for the read API to show. What goes into the envelope
/// has to be the address by itself, because that is the list SES delivers
/// to; a display form reaches it as `BadRequestException: Missing '<'`,
/// after the message is already queued.
///
/// The result uses the minimum quoting possible, as RFC 5321 section 4.1.2
/// asks of a sender (`"ada"@example.com` becomes `ada@example.com`), with
/// the domain lowercased and a non-ASCII domain in Punycode.
///
/// SES does not support SMTPUTF8, so "the local part of a destination email
/// address … may only contain 7-bit ASCII characters", and a non-ASCII domain
/// "must be encoded using Punycode"
/// (<https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_Destination.html>).
///
/// # Errors
///
/// `Err` says what is wrong, for the caller's error list.
pub fn envelope_address(value: &str) -> Result<String, String> {
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

/// Reads a stored participant for comparison: the entry grammar, with the
/// local part lowercased as well as the domain, matching how
/// [`InboxId`](crate::mail::InboxId) treats an address.
static PARTICIPANT_CONFIG: LazyLock<Config> = LazyLock::new(|| {
    Config::builder()
        .strictness(Strictness::Standard)
        .allow_domain_literal()
        .allow_display_name()
        .lowercase_all()
        .build()
});

/// Whether two stored participants name the same mailbox, however each
/// header wrote it. [`EmailAddress`] equality is the mailbox alone, display
/// name excluded. A participant that does not parse matches only its own
/// spelling.
#[must_use]
pub fn same_mailbox(a: &str, b: &str) -> bool {
    match (
        EmailAddress::parse_with(a, &PARTICIPANT_CONFIG),
        EmailAddress::parse_with(b, &PARTICIPANT_CONFIG),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => a.eq_ignore_ascii_case(b),
    }
}

#[cfg(test)]
mod tests {
    use super::same_mailbox;

    #[test]
    fn spellings_of_one_mailbox_are_the_same_mailbox() {
        for (a, b) in [
            ("Bob <bob@example.net>", "bob@example.net"),
            ("bob@example.net", "BOB@Example.NET"),
            ("\"Bob, B.\" <bob@example.net>", "Robert <bob@example.net>"),
            ("\"bob\"@example.net", "bob@example.net"),
            ("bob@bücher.example", "bob@xn--bcher-kva.example"),
        ] {
            assert!(same_mailbox(a, b), "{a} / {b}");
        }
    }

    #[test]
    fn different_mailboxes_and_unparsable_participants_do_not_match() {
        assert!(!same_mailbox("bob@example.net", "bob@example.com"));
        assert!(!same_mailbox("bob+tag@example.net", "bob@example.net"));
        assert!(same_mailbox("not an address", "NOT AN ADDRESS"));
        assert!(!same_mailbox("not an address", "bob@example.net"));
    }
}
