//! Signature and checksum verification.
//!
//! The installer is the trust boundary because it is the component that writes
//! executables to disk. The Hub verifies nothing and holds no trust anchor, so
//! all cryptography lives here.
//!
//! `ring` supplies both SHA-256 and Ed25519 verification and is already in
//! `Cargo.lock` transitively through rustls/webrtc, so this adds no new
//! compiled dependency weight.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::digest::{SHA256, digest};
use ring::signature::{ED25519, UnparsedPublicKey};

use botster_hub_installation::{ReleaseDocument, ReleaseManifest};

use crate::error::{InstallerError, InstallerResult};

/// The manifest bytes the signature actually covered, plus their digest.
#[derive(Debug)]
pub struct VerifiedManifest {
    pub manifest: ReleaseManifest,
    /// Digest of the exact bytes passed to Ed25519 verification.
    ///
    /// Not a digest of the whole release document: the envelope is unsigned, so
    /// a whole-document digest would record something the signature never
    /// covered and imply an authentication that did not happen. The receipt has
    /// to say unambiguously *which payload was verified*.
    pub signed_manifest_sha256: String,
}

/// Lowercase hex of the SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Parse a trust anchor: base64 of a raw 32-byte Ed25519 public key.
pub fn parse_trust_anchor(contents: &str) -> InstallerResult<Vec<u8>> {
    let encoded: String = contents.split_whitespace().collect();
    if encoded.is_empty() {
        return Err(InstallerError::new(
            "invalid_trust_anchor",
            "the trust anchor file is empty",
        ));
    }
    let key = BASE64.decode(encoded.as_bytes()).map_err(|_| {
        InstallerError::new(
            "invalid_trust_anchor",
            "the trust anchor is not valid base64",
        )
    })?;
    if key.len() != 32 {
        return Err(InstallerError::new(
            "invalid_trust_anchor",
            format!(
                "an ed25519 trust anchor is 32 bytes; this one is {}",
                key.len()
            ),
        ));
    }
    Ok(key)
}

/// Verify the document's signature and decode the manifest it covers.
///
/// Fails closed on every path: an unknown algorithm, undecodable base64, a
/// tampered payload, and a wrong key all abort rather than degrade.
pub fn verify_document(
    document: &ReleaseDocument,
    trust_anchor: &[u8],
) -> InstallerResult<VerifiedManifest> {
    if document.signature.algorithm != "ed25519" {
        return Err(InstallerError::new(
            "unsupported_signature_algorithm",
            format!(
                "release signature algorithm {} is not supported",
                document.signature.algorithm
            ),
        ));
    }
    if document.signature.value.trim().is_empty() {
        return Err(InstallerError::new(
            "missing_release_signature",
            "the release document carries no signature value",
        ));
    }
    let manifest_bytes = BASE64
        .decode(document.install_manifest.as_bytes())
        .map_err(|_| {
            InstallerError::new(
                "invalid_release_manifest",
                "install_manifest is not valid base64",
            )
        })?;
    let signature = BASE64
        .decode(document.signature.value.as_bytes())
        .map_err(|_| {
            InstallerError::new(
                "invalid_release_signature",
                "the release signature is not valid base64",
            )
        })?;

    UnparsedPublicKey::new(&ED25519, trust_anchor)
        .verify(&manifest_bytes, &signature)
        .map_err(|_| {
            InstallerError::new(
                "release_signature_rejected",
                "the release manifest signature did not verify against the trust anchor",
            )
        })?;

    let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        InstallerError::new(
            "invalid_release_manifest",
            format!("the signed manifest is not a supported manifest: {error}"),
        )
    })?;

    Ok(VerifiedManifest {
        signed_manifest_sha256: sha256_hex(&manifest_bytes),
        manifest,
    })
}

/// Enforce the signed/unsigned authority boundary.
///
/// The verified manifest is the sole authority. The envelope's copies exist
/// only so the Hub — which verifies nothing — can read version identity for
/// `check-update`, and every duplicated field must match exactly.
///
/// Without this rule an attacker who cannot forge a signature can still wrap a
/// legitimately signed *old* manifest in an envelope advertising a *new*
/// version: the Hub would report that newer version as available, and the
/// installer would silently install something else.
pub fn enforce_envelope_agreement(
    document: &ReleaseDocument,
    manifest: &ReleaseManifest,
) -> InstallerResult<()> {
    for (field, envelope, signed) in [
        ("product_id", &document.product_id, &manifest.product_id),
        (
            "release_channel",
            &document.release_channel,
            &manifest.release_channel,
        ),
        ("version", &document.version, &manifest.version),
        (
            "build_revision",
            &document.build_revision,
            &manifest.build_revision,
        ),
    ] {
        if envelope != signed {
            return Err(InstallerError::new(
                "release_envelope_disagreement",
                format!(
                    "unsigned envelope {field} {envelope:?} disagrees with the verified manifest {signed:?}"
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_installation::ReleaseSignature;

    fn document(manifest: &str, signature: &str) -> ReleaseDocument {
        ReleaseDocument {
            schema_version: 2,
            product_id: "botster-hub".to_string(),
            release_channel: "stable".to_string(),
            version: "0.2.0".to_string(),
            build_revision: "0".repeat(40),
            install_manifest: manifest.to_string(),
            signature: ReleaseSignature {
                algorithm: "ed25519".to_string(),
                key_id: "test-only-do-not-trust".to_string(),
                value: signature.to_string(),
            },
        }
    }

    #[test]
    fn a_trust_anchor_must_be_exactly_a_32_byte_ed25519_key() {
        assert!(parse_trust_anchor(&BASE64.encode([7_u8; 32])).is_ok());
        assert_eq!(
            parse_trust_anchor("").expect_err("empty").kind(),
            "invalid_trust_anchor"
        );
        assert_eq!(
            parse_trust_anchor("not base64!!")
                .expect_err("bad base64")
                .kind(),
            "invalid_trust_anchor"
        );
        assert_eq!(
            parse_trust_anchor(&BASE64.encode([7_u8; 16]))
                .expect_err("short key")
                .kind(),
            "invalid_trust_anchor"
        );
    }

    #[test]
    fn an_unknown_algorithm_or_absent_signature_aborts_before_any_verification() {
        let mut unknown = document("aGk=", "c2ln");
        unknown.signature.algorithm = "rsa-pss".to_string();
        assert_eq!(
            verify_document(&unknown, &[0_u8; 32])
                .expect_err("unknown algorithm")
                .kind(),
            "unsupported_signature_algorithm"
        );

        let mut absent = document("aGk=", "");
        absent.signature.value = "   ".to_string();
        assert_eq!(
            verify_document(&absent, &[0_u8; 32])
                .expect_err("absent signature")
                .kind(),
            "missing_release_signature"
        );
    }

    #[test]
    fn every_duplicated_field_must_match_the_verified_manifest_exactly() {
        let manifest = || ReleaseManifest {
            product_id: "botster-hub".to_string(),
            release_channel: "stable".to_string(),
            version: "0.2.0".to_string(),
            build_revision: "0".repeat(40),
            source_revisions: botster_hub_installation::ManifestSourceRevisions {
                botster_hub: "0".repeat(40),
                botster_core: "1".repeat(40),
            },
            artifacts: Vec::new(),
        };
        assert!(enforce_envelope_agreement(&document("", ""), &manifest()).is_ok());

        // The `version` case is the attack this rule exists to stop: a validly
        // signed old manifest advertised inside an envelope claiming a new one.
        for mutate in [
            (|d: &mut ReleaseDocument| d.product_id = "botster-core".to_string())
                as fn(&mut ReleaseDocument),
            |d: &mut ReleaseDocument| d.release_channel = "beta".to_string(),
            |d: &mut ReleaseDocument| d.version = "99.0.0".to_string(),
            |d: &mut ReleaseDocument| d.build_revision = "9".repeat(40),
        ] {
            let mut tampered = document("", "");
            mutate(&mut tampered);
            assert_eq!(
                enforce_envelope_agreement(&tampered, &manifest())
                    .expect_err("envelope disagreement must abort")
                    .kind(),
                "release_envelope_disagreement"
            );
        }
    }

    #[test]
    fn sha256_hex_is_canonical_lowercase() {
        assert_eq!(
            sha256_hex(b"botster"),
            sha256_hex(b"botster").to_ascii_lowercase()
        );
        assert_eq!(sha256_hex(b"").len(), 64);
        assert!(botster_hub_installation::is_sha256_hex(&sha256_hex(b"x")));
    }
}
