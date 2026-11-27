use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rsa::RsaPublicKey;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::signature::Verifier as _;
use sha1::Sha1;
use sha2::Sha256;

use crate::canonical::build_string_to_sign;
use crate::envelope::SnsEnvelope;
use crate::error::VerifyError;

pub(crate) fn verify_with_key(
    envelope: &SnsEnvelope,
    key: &RsaPublicKey,
) -> Result<(), VerifyError> {
    let canonical = build_string_to_sign(envelope)?;
    let signature_bytes = STANDARD.decode(envelope.signature.as_bytes())?;
    let signature = Signature::try_from(signature_bytes.as_slice())
        .map_err(|_| VerifyError::SignatureMismatch)?;

    let verified = match envelope.signature_version.as_str() {
        "1" => VerifyingKey::<Sha1>::new(key.clone())
            .verify(canonical.as_bytes(), &signature)
            .is_ok(),
        "2" => VerifyingKey::<Sha256>::new(key.clone())
            .verify(canonical.as_bytes(), &signature)
            .is_ok(),
        other => {
            return Err(VerifyError::UnsupportedSignatureVersion(other.to_owned()));
        }
    };
    if verified {
        Ok(())
    } else {
        Err(VerifyError::SignatureMismatch)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::verify_with_key;
    use crate::cert;
    use crate::envelope::SnsEnvelope;
    use crate::error::VerifyError;
    use crate::fixtures::{SnsFixture, notification};

    #[test]
    fn accepts_a_valid_signature_and_rejects_a_tampered_message() {
        let fixture = SnsFixture::new();
        let key = cert::parse_cert_pem(fixture.cert_pem.as_bytes()).unwrap();
        let mut body = notification("https://sns.us-east-1.amazonaws.com/cert.pem");
        fixture.sign(&mut body, "2");

        let envelope: SnsEnvelope = serde_json::from_value(body.clone()).unwrap();
        verify_with_key(&envelope, &key).unwrap();

        body["Message"] = json!("tampered after signing");
        let tampered: SnsEnvelope = serde_json::from_value(body).unwrap();
        assert!(matches!(
            verify_with_key(&tampered, &key),
            Err(VerifyError::SignatureMismatch)
        ));
    }
}
