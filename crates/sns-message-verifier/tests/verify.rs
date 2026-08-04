#![expect(clippy::unwrap_used, reason = "test code panics on fixture failure")]

mod support;

use serde_json::json;
use sns_message_verifier::{CertUrlRejection, SnsVerifier, VerifyError};
use support::{SnsFixture, notification, subscription_confirmation};
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
    let (server, cert_url) = serve_cert(&fixture.cert_pem, 1).await;
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
