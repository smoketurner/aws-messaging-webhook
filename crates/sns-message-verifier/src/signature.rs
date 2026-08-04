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

/// Verifies an envelope's signature against an already-obtained PEM
/// certificate, without any network access. The async
/// [`SnsVerifier`](crate::SnsVerifier) methods are the production surface;
/// this exists for offline use and tests.
///
/// # Errors
///
/// Returns [`VerifyError`] if the certificate is unparseable or outside its
/// validity window, the `SignatureVersion` is unsupported, the signature is
/// not valid base64, or verification fails.
pub fn verify_with_cert(envelope: &SnsEnvelope, cert_pem: &[u8]) -> Result<(), VerifyError> {
    let key = crate::cert::parse_cert_pem(cert_pem)?;
    verify_with_key(envelope, &key)
}

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
