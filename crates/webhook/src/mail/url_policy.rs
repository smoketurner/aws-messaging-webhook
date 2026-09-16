//! The one place that decides whether an outbound URL may be fetched.
//!
//! A send may name a URL instead of inlining an attachment's bytes, which
//! means this service makes an HTTP request to an address a caller chose.
//! That is server-side request forgery surface: the function runs inside a
//! VPC-less Lambda that can still reach the EC2 metadata endpoint and any
//! public host, so "fetch what the caller asked for" has to be narrowed to
//! "fetch a public HTTPS host, and keep checking".
//!
//! Two rules do the work. [`parse_attachment_url`] vets the URL's shape, and
//! [`is_public_ip`] vets every address it resolves to. Both are applied twice:
//! once at the API, so a bad URL is rejected before anything is queued, and
//! again on every redirect hop at fetch time, because the first check says
//! nothing about where a redirect leads.
//!
//! An IP-literal host never reaches a resolver, so [`parse_attachment_url`]
//! checks those addresses itself rather than assuming the resolver will.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use url::{Host, Url};

/// The longest URL accepted, in bytes.
const URL_MAX_BYTES: usize = 2_048;

/// A URL that has passed every shape rule. Holding one does not mean the
/// host's addresses are public — that is [`is_public_ip`]'s job at connect
/// time, since a name resolves to an address only then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentUrl(Url);

impl AttachmentUrl {
    #[must_use]
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The host to resolve. Always present: a URL without one is rejected.
    #[must_use]
    pub fn host(&self) -> Host<&str> {
        // A parsed `AttachmentUrl` always has a host; `parse_attachment_url`
        // rejects the alternative.
        self.0.host().unwrap_or(Host::Domain(""))
    }
}

/// Why a URL may not be fetched.
///
/// The variants are deliberately coarse. A caller is told its URL was
/// refused, not which internal address it nearly reached, so this cannot be
/// used to map a private network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UrlRejected {
    #[error("the URL could not be parsed")]
    Malformed,
    #[error("the URL is longer than the limit")]
    TooLong,
    #[error("only https URLs can be fetched")]
    NotHttps,
    #[error("a URL with embedded credentials cannot be fetched")]
    HasUserinfo,
    #[error("only the default https port can be fetched")]
    PortNotAllowed,
    #[error("the URL has no host")]
    NoHost,
    #[error("that host is not a public address")]
    HostNotPublic,
}

/// Vets a URL's shape.
///
/// # Errors
///
/// [`UrlRejected`] when the URL is malformed, over-long, not `https`, carries
/// userinfo, names a port other than 443, has no host, or is an IP literal
/// that is not a public address.
pub fn parse_attachment_url(raw: &str) -> Result<AttachmentUrl, UrlRejected> {
    parse_with(raw, false)
}

/// [`parse_attachment_url`] with the transport rules relaxed, for tests whose
/// mock server speaks plain http on an arbitrary port. Every other rule,
/// including the address rules, still applies — those are what the SSRF tests
/// actually exercise.
///
/// # Errors
///
/// As [`parse_attachment_url`], except that `http` and any port are accepted.
#[cfg(test)]
pub fn parse_attachment_url_allowing_http(raw: &str) -> Result<AttachmentUrl, UrlRejected> {
    parse_with(raw, true)
}

fn parse_with(raw: &str, relaxed_transport: bool) -> Result<AttachmentUrl, UrlRejected> {
    if raw.len() > URL_MAX_BYTES {
        return Err(UrlRejected::TooLong);
    }
    let url = Url::parse(raw).map_err(|_| UrlRejected::Malformed)?;

    let scheme_ok = url.scheme() == "https" || (relaxed_transport && url.scheme() == "http");
    if !scheme_ok {
        return Err(UrlRejected::NotHttps);
    }
    // Credentials in a URL are a redirect-laundering trick as often as a real
    // intent, and nothing legitimate needs them here.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(UrlRejected::HasUserinfo);
    }
    // `port()` is None when the port is the scheme default, so this allows
    // 443 written either way and nothing else.
    if !relaxed_transport {
        match url.port() {
            None | Some(443) => {}
            Some(_) => return Err(UrlRejected::PortNotAllowed),
        }
    }

    let host = url.host().ok_or(UrlRejected::NoHost)?;
    match host {
        // A literal address never reaches the resolver, so it is judged here.
        // `url` has already normalized the many spellings of an address —
        // decimal, octal, hex, shortened — into a real one.
        Host::Ipv4(addr) => {
            if !is_public_ip(IpAddr::V4(addr)) {
                return Err(UrlRejected::HostNotPublic);
            }
        }
        Host::Ipv6(addr) => {
            if !is_public_ip(IpAddr::V6(addr)) {
                return Err(UrlRejected::HostNotPublic);
            }
        }
        Host::Domain(name) => {
            if name.is_empty() {
                return Err(UrlRejected::NoHost);
            }
        }
    }

    Ok(AttachmentUrl(url))
}

/// Whether `addr` is a public address this service may connect to.
///
/// Written as an allowlist for IPv6 and a denylist for IPv4, which is how the
/// two address spaces are actually organized: IPv4's special ranges are
/// enumerable, while IPv6's global unicast space is one prefix with a handful
/// of documented holes in it. Anything not positively known to be public is
/// denied.
#[must_use]
pub fn is_public_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => {
            // An IPv4-mapped address is an IPv4 address wearing a costume;
            // judging it as IPv6 would let `::ffff:169.254.169.254` through.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_ipv4(v4);
            }
            is_public_ipv6(v6)
        }
    }
}

fn is_public_ipv4(addr: Ipv4Addr) -> bool {
    let [a, b, c, _] = addr.octets();
    let bc = u16::from(b);

    // "This network", loopback, link-local (the metadata endpoint lives at
    // 169.254.169.254), the three private ranges, carrier-grade NAT, the
    // IETF protocol assignments, the documentation ranges, benchmarking,
    // multicast and the reserved top of the space.
    let denied = a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..128).contains(&bc))
        || (a == 169 && b == 254)
        || (a == 172 && (16..32).contains(&bc))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (18..20).contains(&bc))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224;

    !denied
}

fn is_public_ipv6(addr: Ipv6Addr) -> bool {
    let segments = addr.segments();
    let first = segments[0];

    // Global unicast is 2000::/3 and nothing else is allowed through.
    if !(0x2000..0x4000).contains(&first) {
        return false;
    }

    // The documented exceptions inside global unicast: Teredo, the ORCHID
    // and benchmarking blocks, documentation prefixes, and 6to4 — which
    // embeds an arbitrary IPv4 address and would otherwise be a clean tunnel
    // to a private one.
    let excluded = first == 0x2002
        || (first == 0x2001 && segments[1] == 0x0000)
        || (first == 0x2001 && segments[1] == 0x0002 && segments[2] == 0x0000)
        || (first == 0x2001 && (0x0010..0x0040).contains(&segments[1]))
        || (first == 0x2001 && segments[1] == 0x0db8)
        || (0x3fff..0x4000).contains(&first) && (first & 0xfff0) == 0x3ff0;

    !excluded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn rejected(raw: &str) -> UrlRejected {
        match parse_attachment_url(raw) {
            Err(error) => error,
            Ok(url) => panic!("{raw} should have been rejected, got {}", url.as_str()),
        }
    }

    #[test]
    fn a_plain_https_url_is_accepted() {
        let url = parse_attachment_url("https://example.com/a.pdf").unwrap();
        assert_eq!(url.as_str(), "https://example.com/a.pdf");
        assert!(parse_attachment_url("https://example.com:443/a.pdf").is_ok());
    }

    #[test]
    fn only_https_on_the_default_port_without_credentials() {
        assert_eq!(rejected("http://example.com/"), UrlRejected::NotHttps);
        assert_eq!(rejected("ftp://example.com/"), UrlRejected::NotHttps);
        assert_eq!(rejected("file:///etc/passwd"), UrlRejected::NotHttps);
        assert_eq!(
            rejected("https://user:pw@example.com/"),
            UrlRejected::HasUserinfo
        );
        assert_eq!(
            rejected("https://user@example.com/"),
            UrlRejected::HasUserinfo
        );
        assert_eq!(
            rejected("https://example.com:8443/"),
            UrlRejected::PortNotAllowed
        );
        assert_eq!(rejected("not a url"), UrlRejected::Malformed);
        assert_eq!(
            rejected(&format!(
                "https://example.com/{}",
                "x".repeat(URL_MAX_BYTES)
            )),
            UrlRejected::TooLong
        );
    }

    /// Every spelling of a loopback or metadata address that a naive string
    /// check would miss. `url` normalizes the encodings; the address rules
    /// reject what they normalize to.
    #[test]
    fn obfuscated_private_addresses_are_rejected() {
        for raw in [
            "https://2130706433/",         // decimal 127.0.0.1
            "https://0x7f.1/",             // hex, shortened
            "https://0177.0.0.1/",         // octal
            "https://127.1/",              // shortened
            "https://127.0.0.1/",          // plain
            "https://169.254.169.254/",    // EC2 instance metadata
            "https://[::ffff:a9fe:a9fe]/", // mapped 169.254.169.254
            "https://[::ffff:127.0.0.1]/", // mapped loopback
            "https://[::127.0.0.1]/",      // ::/96
            "https://[::1]/",              // loopback
            "https://[fd00:ec2::254]/",    // unique local
            "https://[fe80::1]/",          // link-local
            "https://10.0.0.1/",
            "https://192.168.1.1/",
            "https://172.16.0.1/",
            "https://100.64.0.1/", // carrier-grade NAT
        ] {
            let error = rejected(raw);
            assert!(
                matches!(
                    error,
                    UrlRejected::HostNotPublic | UrlRejected::Malformed | UrlRejected::NoHost
                ),
                "{raw} was rejected as {error:?}"
            );
        }
    }

    /// A zone id must never be fetched. Whether `url` refuses to parse it or
    /// the address rules reject it does not matter; not fetching does.
    #[test]
    fn an_ipv6_zone_id_is_never_fetched() {
        assert!(parse_attachment_url("https://[fe80::1%25eth0]/").is_err());
    }

    #[test]
    fn public_addresses_are_allowed() {
        for addr in ["1.1.1.1", "8.8.8.8", "93.184.216.34", "203.0.114.1"] {
            assert!(
                is_public_ip(addr.parse().unwrap()),
                "{addr} should be public"
            );
        }
        for addr in ["2606:4700:4700::1111", "2a00:1450:4001:80e::200e"] {
            assert!(
                is_public_ip(addr.parse().unwrap()),
                "{addr} should be public"
            );
        }
    }

    #[test]
    fn every_reserved_ipv4_range_is_denied() {
        for addr in [
            "0.0.0.0",
            "0.1.2.3",
            "10.255.255.255",
            "100.64.0.0",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.0",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.0.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "239.255.255.255",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(
                !is_public_ip(addr.parse().unwrap()),
                "{addr} should be denied"
            );
        }
    }

    #[test]
    fn ipv4_ranges_adjacent_to_reserved_ones_stay_public() {
        // The boundaries are where an off-by-one would hide.
        for addr in [
            "9.255.255.255",
            "11.0.0.0",
            "100.63.255.255",
            "100.128.0.0",
            "126.255.255.255",
            "128.0.0.1",
            "169.253.255.255",
            "169.255.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.0.1.1",
            "192.0.3.1",
            "192.167.255.255",
            "192.169.0.0",
            "198.17.255.255",
            "198.20.0.0",
            "223.255.255.255",
        ] {
            assert!(
                is_public_ip(addr.parse().unwrap()),
                "{addr} should be public"
            );
        }
    }

    #[test]
    fn ipv6_outside_global_unicast_is_denied() {
        for addr in [
            "::",
            "::1",
            "64:ff9b::1",
            "64:ff9b:1::1",
            "fc00::1",
            "fd00:ec2::254",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "5f00::1",
            "1000::1",
            "4000::1",
        ] {
            assert!(
                !is_public_ip(addr.parse().unwrap()),
                "{addr} should be denied"
            );
        }
    }

    #[test]
    fn documented_and_tunnelling_ipv6_prefixes_are_denied() {
        for addr in [
            "2001::1",        // Teredo
            "2001:2::1",      // benchmarking
            "2001:10::1",     // ORCHID
            "2001:20::1",     // ORCHIDv2
            "2001:3f::1",     // top of the ORCHIDv2 range
            "2001:db8::1",    // documentation
            "2002::1",        // 6to4
            "2002:7f00:1::1", // 6to4 wrapping 127.0.0.1
            "3fff::1",        // documentation
        ] {
            assert!(
                !is_public_ip(addr.parse().unwrap()),
                "{addr} should be denied"
            );
        }
        // 2001:db9 is not the documentation prefix and stays public.
        assert!(is_public_ip("2001:db9::1".parse().unwrap()));
    }
}
