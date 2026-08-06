//! The managed-installation receipt: the contract shared by the installer that
//! writes it and the Hub that reads it.
//!
//! Sharing this shape is what keeps writer and reader from disagreeing about
//! the one file they both touch. The receipt is *local* state produced by our
//! own installer, so unlike release metadata it stays strict: unknown fields
//! are rejected and the schema version is matched exactly. See
//! [`crate::release`] for why the release document does the opposite.

use serde::{Deserialize, Serialize};

use crate::safety::{DirectoryHandle, InstallationProblem, random_suffix};
use crate::source::validate_release_source;

/// Receipt schema written and accepted by this revision.
pub const RECEIPT_SCHEMA_VERSION: u16 = 2;
/// `$HOME`-relative path of the managed installation receipt.
pub const RECEIPT_RELATIVE_PATH: &str = ".botster/installations/botster-hub.json";
/// First `$HOME`-relative component of the receipt directory.
pub const RECEIPT_ROOT_DIRECTORY: &str = ".botster";
/// Second `$HOME`-relative component of the receipt directory.
pub const RECEIPT_INSTALLATIONS_DIRECTORY: &str = "installations";
/// Receipt file name inside the installations directory.
pub const RECEIPT_FILE_NAME: &str = "botster-hub.json";
/// Upper bound on a receipt that will be read at all.
pub const MAX_RECEIPT_BYTES: u64 = 64 * 1024;
/// Product identifier this receipt contract covers.
pub const PRODUCT_ID: &str = "botster-hub";
/// Artifact names a managed botster-hub installation may record.
pub const KNOWN_ARTIFACT_NAMES: [&str; 2] = ["botster-hub", "botster-session-worker"];
/// Signature algorithms the receipt may record as a fact.
pub const KNOWN_SIGNATURE_ALGORITHMS: [&str; 1] = ["ed25519"];

const RECEIPT_TEMP_PREFIX: &str = "botster-hub.json.";
const RECEIPT_TEMP_SUFFIX: &str = ".tmp";
const RECEIPT_FILE_MODE: libc::mode_t = 0o600;
const RECEIPT_DIRECTORY_MODE: libc::mode_t = 0o700;

/// The installed-artifact checksum facts for one binary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptArtifact {
    pub name: String,
    pub sha256: String,
    pub size: u64,
}

/// The two distinct source identities behind a revision-coupled generation.
///
/// Filesystem colocation of the Hub and its worker does not collapse their
/// provenance: the Hub revision is this repository's checkout, and the Core
/// revision is what `Cargo.lock` pins for `botster-session-worker`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptSourceRevisions {
    pub botster_hub: String,
    pub botster_core: String,
}

/// Signature *facts*, not a signature the Hub checks.
///
/// `signed_manifest_sha256` is the digest of the exact bytes passed to Ed25519
/// verification — the decoded install manifest — not a digest of the whole
/// release document. The envelope is unsigned, so recording a whole-document
/// digest here would imply an authentication that never happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptSignature {
    pub algorithm: String,
    pub key_id: String,
    pub signed_manifest_sha256: String,
}

/// Identity of the installer that produced this receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptInstaller {
    pub id: String,
    pub version: String,
}

/// The receipt itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationReceipt {
    pub schema_version: u16,
    pub product_id: String,
    pub binary_version: String,
    pub installation_mode: String,
    pub release_channel: String,
    pub provider: String,
    pub source_url: String,
    pub build_revision: String,
    pub artifacts: Vec<ReceiptArtifact>,
    pub source_revisions: ReceiptSourceRevisions,
    pub signature: ReceiptSignature,
    pub installer: ReceiptInstaller,
}

impl InstallationReceipt {
    /// Validate everything about a receipt that does not require the reader's
    /// own compile-time identity.
    ///
    /// `embedded_build_revision` is the running binary's embedded revision.
    /// `None` means the binary carries no embedded revision — a development
    /// build — and the agreement check is skipped rather than failed, because a
    /// value cannot disagree with the absence of one.
    pub fn validate(
        &self,
        expected_binary_version: &str,
        embedded_build_revision: Option<&str>,
    ) -> Result<(), InstallationProblem> {
        if self.schema_version != RECEIPT_SCHEMA_VERSION {
            return Err(InstallationProblem::new(
                "unsupported_receipt_schema",
                "installation receipt schema is unsupported",
            ));
        }
        if self.product_id != PRODUCT_ID {
            return Err(InstallationProblem::new(
                "receipt_product_mismatch",
                "installation receipt names a different product",
            ));
        }
        if self.binary_version != expected_binary_version {
            return Err(InstallationProblem::new(
                "receipt_binary_mismatch",
                "installation receipt does not match the running binary version",
            ));
        }
        if self.installation_mode != "managed" {
            return Err(InstallationProblem::new(
                "unsupported_installation_mode",
                "installation receipt mode is unsupported",
            ));
        }
        if !is_supported_channel(&self.release_channel) {
            return Err(InstallationProblem::new(
                "unsupported_release_channel",
                "installation receipt release channel is unsupported",
            ));
        }
        if self.provider != "http_json" {
            return Err(InstallationProblem::new(
                "unsupported_release_provider",
                "installation receipt provider is unsupported",
            ));
        }
        validate_release_source(&self.source_url)?;
        if !is_sanitized_revision(&self.build_revision) {
            return Err(InstallationProblem::new(
                "malformed_receipt_revision",
                "installation receipt build revision is not a sanitized value",
            ));
        }
        if let Some(embedded) = embedded_build_revision
            && self.build_revision != embedded
        {
            return Err(InstallationProblem::new(
                "receipt_build_revision_mismatch",
                "installation receipt does not match the running binary build revision",
            ));
        }
        self.validate_artifacts()?;
        if !is_canonical_object_id(&self.source_revisions.botster_hub)
            || !is_canonical_object_id(&self.source_revisions.botster_core)
        {
            return Err(InstallationProblem::new(
                "malformed_receipt_revision",
                "installation receipt source revisions are not canonical object ids",
            ));
        }
        if !KNOWN_SIGNATURE_ALGORITHMS.contains(&self.signature.algorithm.as_str()) {
            return Err(InstallationProblem::new(
                "unsupported_signature_algorithm",
                "installation receipt signature algorithm is unsupported",
            ));
        }
        if !is_sanitized_label(&self.signature.key_id) {
            return Err(InstallationProblem::new(
                "malformed_receipt_field",
                "installation receipt signature key id is not a sanitized value",
            ));
        }
        if !is_sha256_hex(&self.signature.signed_manifest_sha256) {
            return Err(InstallationProblem::new(
                "malformed_receipt_checksum",
                "installation receipt signed manifest digest is not a SHA-256 hex digest",
            ));
        }
        if !is_sanitized_label(&self.installer.id) || !is_sanitized_label(&self.installer.version) {
            return Err(InstallationProblem::new(
                "malformed_receipt_field",
                "installation receipt installer identity is not a sanitized value",
            ));
        }
        Ok(())
    }

    fn validate_artifacts(&self) -> Result<(), InstallationProblem> {
        if self.artifacts.len() != KNOWN_ARTIFACT_NAMES.len() {
            return Err(InstallationProblem::new(
                "unknown_receipt_artifact",
                "installation receipt must record exactly the known artifacts",
            ));
        }
        for expected in KNOWN_ARTIFACT_NAMES {
            let matches = self
                .artifacts
                .iter()
                .filter(|artifact| artifact.name == expected)
                .count();
            if matches != 1 {
                return Err(InstallationProblem::new(
                    "unknown_receipt_artifact",
                    "installation receipt artifact names are unrecognized",
                ));
            }
        }
        for artifact in &self.artifacts {
            if !is_sha256_hex(&artifact.sha256) {
                return Err(InstallationProblem::new(
                    "malformed_receipt_checksum",
                    "installation receipt artifact checksum is not a SHA-256 hex digest",
                ));
            }
        }
        Ok(())
    }
}

/// A validated `$HOME/.botster/installations` descriptor.
///
/// Every read and write below goes through this descriptor. The public API
/// takes the handle rather than a path, so a path-based write — and the
/// check/use race it would reintroduce — is structurally unavailable.
#[derive(Debug)]
pub struct InstallationsDirectory {
    handle: DirectoryHandle,
}

impl InstallationsDirectory {
    /// Open the installations directory for reading. `Ok(None)` means no
    /// managed installation state exists under this home.
    pub fn open(home: &std::path::Path) -> Result<Option<Self>, InstallationProblem> {
        let home = DirectoryHandle::open_root(home, "home directory")?;
        let Some(botster) = home.open_directory(RECEIPT_ROOT_DIRECTORY, "receipt_directory")?
        else {
            return Ok(None);
        };
        let Some(installations) =
            botster.open_directory(RECEIPT_INSTALLATIONS_DIRECTORY, "receipt_directory")?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            handle: installations,
        }))
    }

    /// Open the installations directory, creating both components when absent.
    pub fn open_or_create(home: &std::path::Path) -> Result<Self, InstallationProblem> {
        let home = DirectoryHandle::open_root(home, "home directory")?;
        let botster = home.open_or_create_directory(
            RECEIPT_ROOT_DIRECTORY,
            RECEIPT_DIRECTORY_MODE,
            "receipt_directory",
        )?;
        let installations = botster.open_or_create_directory(
            RECEIPT_INSTALLATIONS_DIRECTORY,
            RECEIPT_DIRECTORY_MODE,
            "receipt_directory",
        )?;
        Ok(Self {
            handle: installations,
        })
    }

    /// Read the receipt bytes. `Ok(None)` means no receipt is present.
    pub fn read_receipt_bytes(&self) -> Result<Option<Vec<u8>>, InstallationProblem> {
        let Some(file) = self
            .handle
            .open_regular_file(RECEIPT_FILE_NAME, "receipt")?
        else {
            return Ok(None);
        };
        file.read_bounded(MAX_RECEIPT_BYTES, "receipt").map(Some)
    }

    /// Read and parse the receipt. `Ok(None)` means no receipt is present.
    pub fn read_receipt(&self) -> Result<Option<InstallationReceipt>, InstallationProblem> {
        let Some(bytes) = self.read_receipt_bytes()? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|_| {
            InstallationProblem::new(
                "malformed_receipt",
                "installation receipt is not valid supported JSON",
            )
        })
    }

    /// Atomically replace the receipt.
    ///
    /// Durability order is file `fsync` → `renameat` → directory `fsync`. An
    /// earlier draft `fsync`ed the directory *before* the rename, which does not
    /// commit the rename at all.
    pub fn write_receipt(&self, receipt: &InstallationReceipt) -> Result<(), InstallationProblem> {
        let bytes = serde_json::to_vec_pretty(receipt).map_err(|error| {
            InstallationProblem::new(
                "receipt_io_error",
                format!("installation receipt could not be serialized: {error}"),
            )
        })?;
        self.sweep_stale_temporaries();

        let temp_name = format!(
            "{RECEIPT_TEMP_PREFIX}{}{RECEIPT_TEMP_SUFFIX}",
            random_suffix()
        );
        let file = self.handle.create_exclusive_file(
            &temp_name,
            RECEIPT_FILE_MODE,
            "receipt temporary",
        )?;
        let staged = file
            .write_all(&bytes, "receipt temporary")
            .and_then(|()| file.sync("receipt temporary"));
        if let Err(problem) = staged {
            let _ = self.handle.unlink_file(&temp_name, "receipt temporary");
            return Err(problem);
        }
        drop(file);

        self.handle
            .rename_into(&temp_name, &self.handle, RECEIPT_FILE_NAME, "receipt")
            .inspect_err(|_| {
                let _ = self.handle.unlink_file(&temp_name, "receipt temporary");
            })?;
        self.handle.sync("receipt_directory")
    }

    /// Remove stale receipt temporaries left by an interrupted write.
    ///
    /// Bounded and fail-safe: only this writer's own name pattern is
    /// considered, each candidate is reopened `O_NOFOLLOW` and must be a
    /// regular file owned by the effective uid and not world-writable, and
    /// anything failing a check is left alone rather than removed. The sweep
    /// never follows a symlink and never deletes a file it cannot prove is its
    /// own.
    pub fn sweep_stale_temporaries(&self) {
        let Ok(names) = self.handle.entry_names("receipt_directory") else {
            return;
        };
        for name in names {
            if !name.starts_with(RECEIPT_TEMP_PREFIX) || !name.ends_with(RECEIPT_TEMP_SUFFIX) {
                continue;
            }
            let Ok(Some(_file)) = self.handle.open_regular_file(&name, "receipt temporary") else {
                continue;
            };
            let _ = self.handle.unlink_file(&name, "receipt temporary");
        }
    }

    /// The validated directory descriptor, for callers that need to inspect it.
    #[must_use]
    pub const fn handle(&self) -> &DirectoryHandle {
        &self.handle
    }
}

/// Release channels a managed installation may use.
#[must_use]
pub fn is_supported_channel(channel: &str) -> bool {
    matches!(channel, "stable" | "beta" | "nightly")
}

/// A display/comparison label: alphanumerics plus `.`, `_`, `-`.
///
/// Deliberately looser than [`is_canonical_object_id`]. This guards a value
/// that is only ever shown or compared, never used as a path component.
#[must_use]
pub fn is_sanitized_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// A sanitized build revision label.
#[must_use]
pub fn is_sanitized_revision(revision: &str) -> bool {
    is_sanitized_label(revision)
}

/// Canonical lowercase-hex Git object-id form.
///
/// Stricter than [`is_sanitized_label`] on purpose. A revision that becomes a
/// path component must not carry `/`, `..`, or anything else that could escape
/// a single component — and a valid signature over a manifest proves *who*
/// wrote a value, not that the value is a safe path.
#[must_use]
pub fn is_canonical_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Canonical lowercase 64-character SHA-256 hex.
#[must_use]
pub fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
