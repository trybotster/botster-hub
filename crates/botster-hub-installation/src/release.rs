//! The signed release document and the manifest it carries.
//!
//! # Why this reader is forward-tolerant and the receipt is not
//!
//! This repository's standing posture is cold-cut replacement, and relaxing a
//! validator needs its exception argued rather than assumed.
//!
//! Cold-cut applies where we control both ends and can replace them together.
//! Release metadata is read by binaries already in the field that we cannot
//! reach. A Hub that cannot parse a newer release document cannot tell its user
//! that an update exists — so strictness there disables the exact mechanism that
//! would ship a fix for the strictness. That is bricking the updater.
//!
//! The receipt is the opposite case: local state written by our own installer,
//! both ends controlled, and the upgrade ordering *depends* on an older Hub
//! rejecting a receipt schema it does not know. So the receipt stays strict.
//!
//! # Why the signature covers transported bytes
//!
//! The signature covers the decoded `install_manifest` bytes exactly as
//! transported. Signing a JSON object in place would require a canonical-JSON
//! implementation agreed between signer and verifier — a well-known bug class.
//! The signed bytes travel verbatim, so signer and verifier cannot disagree
//! about what was signed.

use serde::{Deserialize, Serialize};

/// Release schema this revision publishes.
pub const RELEASE_SCHEMA_VERSION: u16 = 2;
/// Lowest release schema this revision accepts.
///
/// Compared with `>=`, not `==`: a Hub that understands schema 2 must still
/// parse a schema 3 document well enough to read `version` and `build_revision`
/// and answer available/current.
pub const MINIMUM_RELEASE_SCHEMA_VERSION: u16 = 2;
/// Upper bound on a release document that will be read at all.
pub const MAX_RELEASE_BYTES: u64 = 64 * 1024;

/// The outer, **unsigned** release document.
///
/// Unknown fields are ignored rather than rejected, so a newer publisher cannot
/// strand an older reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseDocument {
    pub schema_version: u16,
    pub product_id: String,
    pub release_channel: String,
    pub version: String,
    pub build_revision: String,
    /// Base64 of the exact manifest JSON bytes the signature covers.
    pub install_manifest: String,
    pub signature: ReleaseSignature,
}

/// The detached signature over the decoded manifest bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseSignature {
    pub algorithm: String,
    pub key_id: String,
    /// Base64 of the raw signature bytes.
    pub value: String,
}

/// The signed manifest: the sole authority for the installer.
///
/// The envelope's copies of `product_id`, `release_channel`, `version`, and
/// `build_revision` exist only so the Hub — which verifies nothing — can read
/// version identity for `check-update`. `build_revision` is carried here
/// specifically so the installer's equality rule can cover it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub product_id: String,
    pub release_channel: String,
    pub version: String,
    pub build_revision: String,
    pub source_revisions: ManifestSourceRevisions,
    pub artifacts: Vec<ManifestArtifact>,
}

/// The two distinct source identities behind the revision-coupled pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSourceRevisions {
    pub botster_hub: String,
    pub botster_core: String,
}

/// One downloadable artifact and the checksum that authenticates it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestArtifact {
    pub name: String,
    pub url: String,
    pub size: u64,
    pub sha256: String,
}
