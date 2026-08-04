//! Fixture factory: throwaway RSA keys, self-signed certificates, and signed
//! SNS envelope bodies, so verification is tested end-to-end without AWS.

#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rsa::RsaPrivateKey;
use rsa::pkcs8::EncodePublicKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde_json::{Value, json};
use sha1::Sha1;
use sha2::Sha256;
use sns_message_verifier::{SnsEnvelope, build_string_to_sign};
use x509_cert::builder::{Builder, CertificateBuilder, Profile};
use x509_cert::der::EncodePem;
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::spki::SubjectPublicKeyInfoOwned;
use x509_cert::time::{Time, Validity};

pub struct SnsFixture {
    pub private_key: RsaPrivateKey,
    pub cert_pem: String,
}

impl SnsFixture {
    pub fn new() -> Self {
        Self::with_validity(Validity::from_now(Duration::from_hours(1)).unwrap())
    }

    pub fn expired() -> Self {
        let not_before = SystemTime::now() - Duration::from_hours(2);
        let not_after = SystemTime::now() - Duration::from_hours(1);
        Self::with_validity(Validity {
            not_before: Time::try_from(not_before).unwrap(),
            not_after: Time::try_from(not_after).unwrap(),
        })
    }

    fn with_validity(validity: Validity) -> Self {
        let mut rng = rand::thread_rng();
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();

        let public_key_der = private_key.to_public_key().to_public_key_der().unwrap();
        let spki = SubjectPublicKeyInfoOwned::try_from(public_key_der.as_bytes()).unwrap();
        let signer = rsa::pkcs1v15::SigningKey::<Sha256>::new(private_key.clone());
        let builder = CertificateBuilder::new(
            Profile::Root,
            SerialNumber::from(1u32),
            validity,
            Name::from_str("CN=sns-message-verifier test").unwrap(),
            spki,
            &signer,
        )
        .unwrap();
        let cert = builder.build::<rsa::pkcs1v15::Signature>().unwrap();
        let cert_pem = cert.to_pem(x509_cert::der::pem::LineEnding::LF).unwrap();

        Self {
            private_key,
            cert_pem,
        }
    }

    /// Signs `envelope` in place: computes the canonical string, signs it with
    /// this fixture's key at the given `SignatureVersion`, and sets the
    /// `Signature`/`SignatureVersion` fields.
    pub fn sign(&self, envelope: &mut Value, signature_version: &str) {
        envelope["SignatureVersion"] = json!(signature_version);
        let parsed: SnsEnvelope = serde_json::from_value(envelope.clone()).unwrap();
        let canonical = build_string_to_sign(&parsed).unwrap();

        let signature_b64 = if signature_version == "1" {
            let key = rsa::pkcs1v15::SigningKey::<Sha1>::new(self.private_key.clone());
            let signature: rsa::pkcs1v15::Signature = key.sign(canonical.as_bytes());
            STANDARD.encode(signature.to_bytes())
        } else {
            let key = rsa::pkcs1v15::SigningKey::<Sha256>::new(self.private_key.clone());
            let signature: rsa::pkcs1v15::Signature = key.sign(canonical.as_bytes());
            STANDARD.encode(signature.to_bytes())
        };
        envelope["Signature"] = json!(signature_b64);
    }
}

pub fn notification(cert_url: &str) -> Value {
    json!({
        "Type": "Notification",
        "MessageId": "165545c9-2a5c-472c-8df2-7ff2be2b3b1b",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "Subject": "test subject",
        "Message": "{\"hello\":\"world\"}",
        "Timestamp": "2026-08-03T19:12:52.000Z",
        "SignatureVersion": "1",
        "Signature": "",
        "SigningCertURL": cert_url,
        "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe"
    })
}

pub fn subscription_confirmation(cert_url: &str) -> Value {
    json!({
        "Type": "SubscriptionConfirmation",
        "MessageId": "436b8b2e-9b0f-49c8-9b6e-5d1a2f7c8e9a",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "Message": "You have chosen to subscribe to the topic.",
        "Timestamp": "2026-08-03T19:12:52.000Z",
        "SignatureVersion": "1",
        "Signature": "",
        "SigningCertURL": cert_url,
        "SubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=abc",
        "Token": "abc123"
    })
}
