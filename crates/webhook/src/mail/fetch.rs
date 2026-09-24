//! Fetching a URL-backed attachment.
//!
//! This is the only HTTP client in the service that follows redirects, and it
//! does so manually rather than letting the client do it, because every hop
//! has to be re-checked. CLAUDE.md's "the HTTP clients never follow
//! redirects" holds for the SNS clients, whose trust anchor is the host
//! policy on the initial URL; here the initial URL is caller-supplied and a
//! redirect is exactly how an attacker would try to reach an internal
//! address after passing the first check.
//!
//! Three guards, in order, each of which a caller could otherwise walk past:
//!
//! 1. **Shape** — [`url_policy::parse_attachment_url`] on the original URL
//!    and on every `Location`.
//! 2. **Address** — the host is resolved here, every address is checked with
//!    [`url_policy::is_public_ip`], and the connection is pinned to the
//!    addresses that passed. Resolving and then connecting to the resolved
//!    address is what stops DNS rebinding between the check and the connect.
//! 3. **Size** — `Content-Length` is refused before the body is read, and the
//!    body is capped again while streaming, since the header can lie.

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use url::Host;

use crate::mail::url_policy::{self, AttachmentUrl, UrlRejected};

/// How many hops a redirect chain may take.
const MAX_REDIRECTS: usize = 5;

/// Per-request timeouts. The whole fetch is bounded so one slow host cannot
/// hold a send open until the invocation deadline.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(60);

/// What a fetch produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub bytes: Vec<u8>,
}

/// Why a fetch did not produce bytes.
///
/// Everything but [`Self::Transient`] is permanent: retrying would fail the
/// same way, so the send stops rather than looping.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("that URL is not allowed")]
    Blocked(#[source] UrlRejected),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("the attachment is larger than the limit")]
    TooLarge,
    #[error("the server answered {status}")]
    Rejected { status: u16 },
    #[error("the response is compressed, which this service does not decode")]
    BadEncoding,
    #[error("the host could not be resolved")]
    NotFound,
    #[error("fetching failed")]
    Transient(#[source] anyhow::Error),
}

impl FetchError {
    /// Whether another attempt could succeed.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// Fetches an attachment named by URL.
pub trait AttachmentFetcher: Send + Sync {
    /// Reads at most `max_bytes` from `url`.
    ///
    /// # Errors
    ///
    /// [`FetchError`] for a blocked URL, an over-sized or compressed body, a
    /// refusal, or a transient network failure.
    fn fetch(
        &self,
        url: &AttachmentUrl,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Fetched, FetchError>> + Send;
}

/// The real fetcher.
pub struct HttpAttachmentFetcher {
    /// Addresses exempted from the public-address rule, for tests pointing at
    /// a local mock server. Always empty in production.
    extra_public: Vec<IpAddr>,
    /// Whether plain http is accepted. Compiled in only under `cfg(test)`,
    /// where the mock server has no TLS, so a release build cannot relax the
    /// scheme at all — the same shape the SNS certificate override uses.
    #[cfg(test)]
    allow_http: bool,
}

impl Default for HttpAttachmentFetcher {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpAttachmentFetcher {
    #[must_use]
    pub fn new() -> Self {
        Self {
            extra_public: Vec::new(),
            #[cfg(test)]
            allow_http: false,
        }
    }

    /// A fetcher that also accepts `extra_public`, so a test can point at a
    /// mock server on loopback. Every other address still goes through
    /// [`url_policy::is_public_ip`].
    #[cfg(test)]
    #[must_use]
    pub fn for_tests(extra_public: Vec<IpAddr>) -> Self {
        Self {
            extra_public,
            allow_http: true,
        }
    }

    /// Applies the shape rules to a redirect target. Only a test build can
    /// relax the scheme; everything else is checked either way.
    #[cfg_attr(
        not(test),
        expect(
            clippy::unused_self,
            reason = "the test build reads self.allow_http; a release build has no such field"
        )
    )]
    fn vet(&self, raw: &str) -> Result<AttachmentUrl, UrlRejected> {
        #[cfg(test)]
        if self.allow_http {
            return url_policy::parse_attachment_url_allowing_http(raw);
        }
        url_policy::parse_attachment_url(raw)
    }

    fn address_allowed(&self, address: IpAddr) -> bool {
        url_policy::is_public_ip(address) || self.extra_public.contains(&address)
    }

    /// The addresses to connect to for `url`, each vetted, and the domain
    /// they were resolved for.
    ///
    /// An IP-literal host is connected to as is; only a domain goes to the
    /// resolver, bounded by [`CONNECT_TIMEOUT`]. The domain is `None` for a
    /// literal, which has nothing to pin.
    ///
    /// # Errors
    ///
    /// [`FetchError::NotFound`] when nothing resolves, and
    /// [`FetchError::Blocked`] when any address is not public — any, not all:
    /// a name that resolves to both a public and a private address is a
    /// rebinding attempt, not a usable host.
    async fn addresses(
        &self,
        url: &AttachmentUrl,
    ) -> Result<(Option<String>, Vec<SocketAddr>), FetchError> {
        let port = url.as_url().port_or_known_default().unwrap_or(443);
        let (domain, resolved) = match url.host() {
            Host::Ipv4(ip) => (None, vec![SocketAddr::new(IpAddr::V4(ip), port)]),
            Host::Ipv6(ip) => (None, vec![SocketAddr::new(IpAddr::V6(ip), port)]),
            Host::Domain(domain) => {
                let resolved: Vec<SocketAddr> =
                    tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::lookup_host((domain, port)))
                        .await
                        .map_err(|_| {
                            FetchError::Transient(anyhow::anyhow!("resolving {domain} timed out"))
                        })?
                        .map_err(|e| {
                            FetchError::Transient(anyhow::anyhow!("resolving {domain}: {e}"))
                        })?
                        .collect();
                (Some(domain.to_owned()), resolved)
            }
        };

        if resolved.is_empty() {
            return Err(FetchError::NotFound);
        }
        for address in &resolved {
            if !self.address_allowed(address.ip()) {
                return Err(FetchError::Blocked(UrlRejected::HostNotPublic));
            }
        }
        Ok((domain, resolved))
    }

    /// A client pinned to `addresses` for `host`, so the connection goes to
    /// an address that was actually checked rather than to whatever the
    /// resolver returns a second time.
    #[expect(
        clippy::unused_self,
        reason = "kept a method so the https_only decision stays with the fetcher's other rules"
    )]
    fn client(
        &self,
        domain: Option<&str>,
        addresses: &[SocketAddr],
    ) -> Result<reqwest::Client, FetchError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(TOTAL_TIMEOUT)
            // Redirects are followed by hand so each hop is re-checked.
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .user_agent(concat!("aws-messaging-webhook/", env!("CARGO_PKG_VERSION")));
        #[cfg(not(test))]
        {
            builder = builder.https_only(true);
        }
        if let Some(domain) = domain {
            builder = builder.resolve_to_addrs(domain, addresses);
        }
        builder
            .build()
            .map_err(|e| FetchError::Transient(anyhow::anyhow!("building the client: {e}")))
    }
}

impl AttachmentFetcher for HttpAttachmentFetcher {
    async fn fetch(&self, url: &AttachmentUrl, max_bytes: u64) -> Result<Fetched, FetchError> {
        let mut current = url.clone();

        for _hop in 0..=MAX_REDIRECTS {
            let (domain, addresses) = self.addresses(&current).await?;
            let client = self.client(domain.as_deref(), &addresses)?;

            let response = client
                .get(current.as_str())
                .send()
                .await
                .map_err(|e| FetchError::Transient(anyhow::anyhow!("fetching: {e}")))?;

            let status = response.status();
            if status.is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(FetchError::TooManyRedirects)?;
                // Relative locations are resolved against the current URL,
                // then put through the same shape rules as the original.
                let joined = current
                    .as_url()
                    .join(location)
                    .map_err(|_| FetchError::Blocked(UrlRejected::Malformed))?;
                current = self.vet(joined.as_str()).map_err(FetchError::Blocked)?;
                continue;
            }

            if !status.is_success() {
                return Err(match status.as_u16() {
                    // Worth another attempt; everything else the server said
                    // it will keep saying.
                    408 | 429 => FetchError::Transient(anyhow::anyhow!("server said {status}")),
                    code if code >= 500 => {
                        FetchError::Transient(anyhow::anyhow!("server said {status}"))
                    }
                    code => FetchError::Rejected { status: code },
                });
            }

            // Nothing here decompresses, so a compressed body would be stored
            // and sent as-is under the wrong content type.
            if response
                .headers()
                .get_all(reqwest::header::CONTENT_ENCODING)
                .iter()
                .any(|encoding| {
                    !encoding
                        .to_str()
                        .is_ok_and(|value| value.trim().eq_ignore_ascii_case("identity"))
                })
            {
                return Err(FetchError::BadEncoding);
            }

            // Refuse before reading anything, when the server is honest about
            // the size.
            if response.content_length().is_some_and(|len| len > max_bytes) {
                return Err(FetchError::TooLarge);
            }

            // And again while reading, because the header can lie.
            let mut bytes = Vec::new();
            let mut response = response;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| FetchError::Transient(anyhow::anyhow!("reading: {e}")))?
            {
                if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                    return Err(FetchError::TooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }

            return Ok(Fetched { bytes });
        }

        Err(FetchError::TooManyRedirects)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// A fetcher that trusts the mock server's loopback address and nothing
    /// else, so every other address still goes through the real rules.
    fn fetcher() -> HttpAttachmentFetcher {
        // `localhost` resolves to both loopback families, and every resolved
        // address has to pass, so both are exempted.
        HttpAttachmentFetcher::for_tests(vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ])
    }

    /// Addresses the mock server by name rather than by its loopback
    /// literal, so the URL passes the shape rules and only the *resolved*
    /// address needs the exemption. An IP literal would be rejected outright,
    /// which is what the SSRF tests below rely on.
    fn local(server: &MockServer, path: &str) -> AttachmentUrl {
        let port = server.address().port();
        let raw = format!("http://localhost:{port}{path}");
        url_policy::parse_attachment_url_allowing_http(&raw).unwrap()
    }

    /// `Url::host_str` keeps the brackets on an IPv6 literal, which no
    /// resolver accepts; a literal is connected to directly instead.
    #[tokio::test]
    async fn an_ip_literal_host_is_used_without_resolving() {
        let fetcher = HttpAttachmentFetcher::new();
        for (raw, expected) in [
            (
                "https://[2606:4700:4700::1111]/a.pdf",
                "[2606:4700:4700::1111]:443",
            ),
            ("https://1.1.1.1/a.pdf", "1.1.1.1:443"),
        ] {
            let url = url_policy::parse_attachment_url(raw).unwrap();
            let (domain, addresses) = fetcher.addresses(&url).await.unwrap();
            assert_eq!(domain, None, "{raw}");
            assert_eq!(addresses, vec![expected.parse::<SocketAddr>().unwrap()]);
        }
    }

    #[tokio::test]
    async fn a_plain_response_is_returned() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/a.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"hello".to_vec())
                    .insert_header("content-type", "application/pdf"),
            )
            .mount(&server)
            .await;

        let fetched = fetcher()
            .fetch(&local(&server, "/a.pdf"), 1_000)
            .await
            .unwrap();

        assert_eq!(fetched.bytes, b"hello");
    }

    #[tokio::test]
    async fn a_body_over_the_cap_is_refused_even_without_a_content_length() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/big"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 1_000]))
            .mount(&server)
            .await;

        let error = fetcher()
            .fetch(&local(&server, "/big"), 10)
            .await
            .unwrap_err();

        assert!(matches!(error, FetchError::TooLarge), "{error:?}");
    }

    #[tokio::test]
    async fn a_compressed_response_is_refused_rather_than_stored_as_is() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gz"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"compressed".to_vec())
                    .insert_header("content-encoding", "gzip"),
            )
            .mount(&server)
            .await;

        let error = fetcher()
            .fetch(&local(&server, "/gz"), 1_000)
            .await
            .unwrap_err();

        assert!(matches!(error, FetchError::BadEncoding), "{error:?}");
    }

    #[tokio::test]
    async fn a_duplicate_content_encoding_header_is_refused_when_any_value_is_non_identity() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dup"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"\x1f\x8bcompressed".to_vec())
                    .insert_header("content-encoding", "identity")
                    .append_header("content-encoding", "gzip"),
            )
            .mount(&server)
            .await;

        let error = fetcher()
            .fetch(&local(&server, "/dup"), 1_000)
            .await
            .unwrap_err();

        assert!(matches!(error, FetchError::BadEncoding), "{error:?}");
    }

    #[tokio::test]
    async fn duplicate_identity_content_encoding_headers_are_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/id-id"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"plain".to_vec())
                    .insert_header("content-encoding", "identity")
                    .append_header("content-encoding", "identity"),
            )
            .mount(&server)
            .await;

        let fetched = fetcher()
            .fetch(&local(&server, "/id-id"), 1_000)
            .await
            .unwrap();

        assert_eq!(fetched.bytes, b"plain");
    }

    #[tokio::test]
    async fn a_client_error_is_permanent_but_a_server_error_is_not() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gone"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/broken"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let permanent = fetcher()
            .fetch(&local(&server, "/gone"), 1_000)
            .await
            .unwrap_err();
        assert!(matches!(permanent, FetchError::Rejected { status: 404 }));
        assert!(!permanent.is_transient());

        let transient = fetcher()
            .fetch(&local(&server, "/broken"), 1_000)
            .await
            .unwrap_err();
        assert!(transient.is_transient(), "{transient:?}");
    }

    #[tokio::test]
    async fn a_redirect_to_a_private_address_is_blocked() {
        // The first URL passes every check; the redirect is the attack.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/start"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "https://169.254.169.254/latest/meta-data/"),
            )
            .mount(&server)
            .await;

        let error = fetcher()
            .fetch(&local(&server, "/start"), 1_000)
            .await
            .unwrap_err();

        assert!(matches!(error, FetchError::Blocked(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_redirect_loop_gives_up() {
        let server = MockServer::start().await;
        let target = local(&server, "/loop");
        Mock::given(method("GET"))
            .and(path("/loop"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", target.as_str()))
            .mount(&server)
            .await;

        let error = fetcher().fetch(&target, 1_000).await.unwrap_err();

        assert!(matches!(error, FetchError::TooManyRedirects), "{error:?}");
    }
}
