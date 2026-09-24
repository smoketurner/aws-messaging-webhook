#![expect(clippy::unwrap_used, reason = "test code panics on fixture failure")]

use serde_json::json;
use sns_message_verifier::fixtures::{SnsFixture, notification, subscription_confirmation};
use sns_message_verifier::{
    CertUrlRejection, SnsEnvelope, SnsVerifier, VerifyError, verify_with_cert,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn serve_cert(pem: &str, expected_fetches: u64) -> (MockServer, String) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/cert.pem"))
        .respond_with(ResponseTemplate::new(200).set_body_string(pem))
        .expect(expected_fetches)
        .mount(&server)
        .await;
    let cert_url = format!("{}/cert.pem", server.uri());
    (server, cert_url)
}

fn verifier(server: &MockServer) -> SnsVerifier {
    SnsVerifier::builder()
        .dangerous_allow_cert_url_prefix(server.uri())
        .build()
        .unwrap()
}

#[tokio::test]
async fn accepts_valid_v1_and_v2_notifications() {
    let fixture = SnsFixture::new();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    for version in ["1", "2"] {
        let mut body = notification(&cert_url);
        fixture.sign(&mut body, version);
        let envelope = sns.verify_body(body.to_string().as_bytes()).await.unwrap();
        assert_eq!(envelope.message, "{\"hello\":\"world\"}");
    }
}

#[tokio::test]
async fn accepts_valid_subscription_confirmation() {
    let fixture = SnsFixture::new();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = subscription_confirmation(&cert_url);
    fixture.sign(&mut body, "2");
    let envelope = sns.verify_body(body.to_string().as_bytes()).await.unwrap();
    assert_eq!(envelope.token.as_deref(), Some("abc123"));
}

#[tokio::test]
async fn rejects_tampered_fields() {
    let fixture = SnsFixture::new();
    // expect(3): a certificate is cached only after a signature verifies
    // against it, so each tampered envelope fetches it afresh.
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 3).await;
    let sns = verifier(&server);

    for (field, value) in [
        ("Message", json!("tampered")),
        ("Timestamp", json!("2026-08-04T00:00:00.000Z")),
        ("TopicArn", json!("arn:aws:sns:us-east-1:999999999999:evil")),
    ] {
        let mut body = notification(&cert_url);
        fixture.sign(&mut body, "1");
        body[field] = value;
        let err = sns
            .verify_body(body.to_string().as_bytes())
            .await
            .unwrap_err();
        assert!(
            matches!(err, VerifyError::SignatureMismatch),
            "tampering {field} should fail signature verification, got {err:?}"
        );
    }
}

#[tokio::test]
async fn rejects_signature_from_wrong_key() {
    let served = SnsFixture::new();
    let attacker = SnsFixture::new();
    let (server, cert_url) = serve_cert(&served.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    attacker.sign(&mut body, "2");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::SignatureMismatch));
}

#[tokio::test]
async fn rejects_expired_certificate() {
    let fixture = SnsFixture::expired();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "1");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::CertValidity));
}

#[tokio::test]
async fn rejects_not_yet_valid_certificate() {
    let fixture = SnsFixture::not_yet_valid();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "1");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::CertValidity));
}

#[tokio::test]
async fn rejects_oversized_certificate_response() {
    // A body well over the cap must be rejected as "too large" specifically,
    // proving the streaming size check fires. The fetch fails before the
    // signature is checked, so the signing fixture is irrelevant here.
    let oversized = "x".repeat(100 * 1024);
    let (server, cert_url) = serve_cert(&oversized, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    SnsFixture::new().sign(&mut body, "2");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    match err {
        VerifyError::CertParse(message) => {
            assert!(
                message.contains("too large"),
                "expected size error, got {message}"
            );
        }
        other => panic!("expected CertParse(too large), got {other:?}"),
    }
}

#[test]
fn verify_with_cert_accepts_valid_and_rejects_tampered() {
    let fixture = SnsFixture::new();
    let mut body = notification("https://sns.us-east-1.amazonaws.com/cert.pem");
    fixture.sign(&mut body, "2");

    let envelope: SnsEnvelope = serde_json::from_value(body.clone()).unwrap();
    verify_with_cert(&envelope, fixture.cert_pem.as_bytes()).unwrap();

    body["Message"] = json!("tampered after signing");
    let tampered: SnsEnvelope = serde_json::from_value(body).unwrap();
    assert!(matches!(
        verify_with_cert(&tampered, fixture.cert_pem.as_bytes()),
        Err(VerifyError::SignatureMismatch)
    ));
}

#[tokio::test]
async fn rejects_garbage_base64_signature() {
    let fixture = SnsFixture::new();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "1");
    body["Signature"] = json!("!!! not base64 !!!");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::InvalidSignatureEncoding(_)));
}

#[tokio::test]
async fn rejects_unsupported_signature_version() {
    let fixture = SnsFixture::new();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "1");
    body["SignatureVersion"] = json!("3");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::UnsupportedSignatureVersion(v) if v == "3"));
}

#[tokio::test]
async fn rejects_confirmation_missing_token() {
    let fixture = SnsFixture::new();
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = subscription_confirmation(&cert_url);
    fixture.sign(&mut body, "1");
    body.as_object_mut().unwrap().remove("Token");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(err, VerifyError::MissingField("Token")));
}

#[tokio::test]
async fn rejects_malformed_envelope_json() {
    let server = MockServer::start().await;
    let sns = verifier(&server);
    let err = sns.verify_body(b"not json at all").await.unwrap_err();
    assert!(matches!(err, VerifyError::MalformedEnvelope(_)));
}

#[tokio::test]
async fn rejects_cert_url_outside_override_prefix() {
    let server = MockServer::start().await;
    let sns = verifier(&server);

    let fixture = SnsFixture::new();
    let mut body = notification("https://evil.example.com/cert.pem");
    fixture.sign(&mut body, "1");
    let err = sns
        .verify_body(body.to_string().as_bytes())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        VerifyError::InvalidCertUrl {
            reason: CertUrlRejection::InvalidHost,
            ..
        }
    ));
}

#[tokio::test]
async fn caches_certificate_across_verifications() {
    let fixture = SnsFixture::new();
    // expect(1): the second verify must hit the cache, not the server.
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    for _ in 0..2 {
        let mut body = notification(&cert_url);
        fixture.sign(&mut body, "2");
        sns.verify_body(body.to_string().as_bytes()).await.unwrap();
    }
    server.verify().await;
}

#[tokio::test]
async fn cache_dedupes_urls_differing_only_by_query_string() {
    let fixture = SnsFixture::new();
    // expect(1): two URLs differing only by query must dedup to a single
    // fetch — query strings do not change which cert is served and must
    // not produce distinct cache keys.
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    for n in 0..2 {
        let qurl = format!("{cert_url}?k={n}");
        let mut body = notification(&qurl);
        fixture.sign(&mut body, "2");
        sns.verify_body(body.to_string().as_bytes()).await.unwrap();
    }
    server.verify().await;
}

#[tokio::test]
async fn cache_dedupes_urls_differing_only_by_fragment() {
    let fixture = SnsFixture::new();
    // expect(1): two URLs differing only by fragment must dedup to a
    // single fetch — reqwest strips fragments before sending, so both
    // fetches would hit the same mock endpoint, and the cache must treat
    // them as the same key.
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    for n in 0..2 {
        let furl = format!("{cert_url}#frag{n}");
        let mut body = notification(&furl);
        fixture.sign(&mut body, "2");
        sns.verify_body(body.to_string().as_bytes()).await.unwrap();
    }
    server.verify().await;
}

#[tokio::test]
async fn cache_does_not_evict_legit_entry_under_query_flood() {
    let fixture = SnsFixture::new();
    // expect(1): one warmup fetch, then 32 query-appended verifies that all
    // cache-hit on the canonical key, then a final verify that also
    // cache-hits. Without the fix, the 32 distinct query keys would have
    // tripped `CertCache::insert`'s 32-distinct-key `clear()` rule and
    // evicted the legit entry, forcing a re-fetch on the final verify
    // (34 total fetches).
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
    let sns = verifier(&server);

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");
    sns.verify_body(body.to_string().as_bytes()).await.unwrap(); // 1: warm legit entry
    for n in 0..32 {
        let qurl = format!("{cert_url}?k={n}");
        let mut body = notification(&qurl);
        fixture.sign(&mut body, "2");
        sns.verify_body(body.to_string().as_bytes()).await.unwrap(); // 32: all cache-hit under the canonical key
    }
    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");
    sns.verify_body(body.to_string().as_bytes()).await.unwrap(); // +1: legit cache-hit
    server.verify().await;
}

#[tokio::test]
async fn bad_signature_envelopes_cannot_evict_a_verified_certificate() {
    let fixture = SnsFixture::new();
    let server = MockServer::start().await;
    // Every path serves the genuine cert, as a host that decodes
    // percent-escapes or serves several regions' certs effectively does.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture.cert_pem.clone()))
        .mount(&server)
        .await;
    let sns = verifier(&server);
    let cert_url = format!("{}/cert.pem", server.uri());

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");
    sns.verify_body(body.to_string().as_bytes()).await.unwrap();

    // More distinct URLs than the cache holds, each with a signature that
    // fails: none of them may take a cache slot.
    for n in 0..40 {
        let mut body = notification(&format!("{}/cert{n}.pem", server.uri()));
        fixture.sign(&mut body, "2");
        body["Message"] = json!("forged");
        let err = sns
            .verify_body(body.to_string().as_bytes())
            .await
            .unwrap_err();
        assert!(matches!(err, VerifyError::SignatureMismatch));
    }

    let mut body = notification(&cert_url);
    fixture.sign(&mut body, "2");
    sns.verify_body(body.to_string().as_bytes()).await.unwrap();

    let fetches_of_the_verified_url = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/cert.pem")
        .count();
    assert_eq!(
        fetches_of_the_verified_url, 1,
        "the verified entry was evicted"
    );
}
