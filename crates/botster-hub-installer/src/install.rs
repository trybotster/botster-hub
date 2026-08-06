//! The installation transaction.
//!
//! # Generations: the pair is one indivisible unit
//!
//! The Hub and its locked-Core worker are **one revision-coupled generation**,
//! never two independently replaceable files. Replacing them as two renames
//! makes a mixed pair — Hub at N+1 beside worker at N — reachable by `SIGKILL`
//! or power loss, and that is exactly the state "revision-coupled artifacts"
//! exists to forbid. So both binaries live inside one generation directory and
//! are only ever reachable through one pointer that flips atomically.
//!
//! # Two different strengths of durability claim
//!
//! * **`SIGKILL`-safe — demonstrated.** Every crash boundary is proven
//!   empirically by killing the installer there.
//! * **Power-loss-safe — argued, not demonstrated.** The `fsync` sequence below
//!   is implemented exactly as specified, but no fault-injection harness exists
//!   in this repository, so durability across power loss rests on the ordering
//!   argument. The phrase "power-loss safe" must not be used unqualified about
//!   this code.

use std::path::{Path, PathBuf};

use botster_hub_installation::layout::{
    BIN_DIRECTORY, BIN_HUB_SYMLINK_TARGET, CURRENT_POINTER, GENERATIONS_DIRECTORY, HUB_BINARY_NAME,
    STAGING_PREFIX, WORKER_BINARY_NAME, generation_name,
};
use botster_hub_installation::safety::{DirectoryHandle, effective_uid, random_suffix};
use botster_hub_installation::{
    InstallationReceipt, InstallationsDirectory, KNOWN_ARTIFACT_NAMES, LeaseMode, LeaseOutcome,
    MAX_RELEASE_BYTES, MINIMUM_RELEASE_SCHEMA_VERSION, ManifestArtifact, PRODUCT_ID,
    RECEIPT_SCHEMA_VERSION, ReceiptArtifact, ReceiptInstaller, ReceiptSignature,
    ReceiptSourceRevisions, ReleaseDocument, ReleaseManifest, is_canonical_object_id,
    is_sha256_hex, is_supported_channel, lease, validate_release_source,
};

use crate::error::{InstallerError, InstallerResult};
use crate::fetch::{self, MAX_ARTIFACT_BYTES};
use crate::inject::{self, Point};
use crate::run::{self, RUN_DEADLINE};
use crate::verify;

const INSTALLER_ID: &str = "botster-hub-installer";
const ARTIFACT_MODE: libc::mode_t = 0o755;
const DIRECTORY_MODE: libc::mode_t = 0o755;

/// Everything the operator supplied.
pub struct InstallRequest {
    pub prefix: PathBuf,
    pub home: PathBuf,
    pub source_url: String,
    pub release_channel: String,
    pub trust_anchor: Vec<u8>,
}

/// What a completed install put on disk.
pub struct InstallSummary {
    pub generation: String,
    pub version: String,
    pub build_revision: String,
    pub reused_generation: bool,
    pub previous_generation: Option<String>,
}

/// Fetch, verify, install, and record a managed release.
pub fn install(request: &InstallRequest) -> InstallerResult<InstallSummary> {
    // ---- Read-only phase. Nothing below the prefix is touched yet, so a
    // ---- malformed or unsigned document costs the operator nothing.
    let (document, manifest, signed_manifest_sha256) = resolve_release(request)?;
    let generation = generation_name(
        &manifest.source_revisions.botster_hub,
        &manifest.source_revisions.botster_core,
    )
    .ok_or_else(|| {
        InstallerError::new(
            "malformed_manifest_revision",
            "manifest source revisions are not canonical object ids, so no generation name can be built from them",
        )
    })?;

    std::fs::create_dir_all(&request.prefix).map_err(|error| {
        InstallerError::new(
            "prefix_unavailable",
            format!(
                "install prefix {} could not be created: {error}",
                request.prefix.display()
            ),
        )
    })?;
    let prefix = DirectoryHandle::open_root(&request.prefix, "prefix")?;

    // ---- One exclusive lease, taken before any installation-state mutation and
    // ---- held on the same descriptor through switch, verification, and receipt
    // ---- commit or rollback. Acquiring it and releasing it before mutating
    // ---- would be check-then-act: a managed daemon could start in the gap, and
    // ---- two installers could interleave their switches.
    let _lease = match lease::acquire(&request.prefix, LeaseMode::Exclusive)? {
        LeaseOutcome::Acquired(lease) => lease,
        LeaseOutcome::Contended => {
            return Err(InstallerError::new(
                "installation_busy",
                format!(
                    "a managed Hub daemon or another installer holds the lease on {}; stop every daemon from this installation and try again",
                    request.prefix.display()
                ),
            ));
        }
    };

    let generations =
        prefix.open_or_create_directory(GENERATIONS_DIRECTORY, DIRECTORY_MODE, "generations")?;
    sweep_stale_staging(&generations);

    let reused_generation = match generations.open_directory(&generation, "generation")? {
        Some(existing) => {
            // Fail closed: reuse only on an exact match of every artifact's
            // ownership, mode, size, and checksum. Never delete or overwrite a
            // generation this run cannot prove it produced.
            verify_generation_contents(&existing, &manifest)?;
            true
        }
        None => {
            stage_generation(&generations, &generation, &manifest)?;
            false
        }
    };

    let staged_hub = request
        .prefix
        .join(GENERATIONS_DIRECTORY)
        .join(&generation)
        .join(HUB_BINARY_NAME);
    verify_binary_identity(&staged_hub, &manifest, "staged")?;

    let previous_generation = read_pointer(&prefix)?;
    let mut rollback = Rollback {
        previous_generation: previous_generation.clone(),
        created_pointer: false,
        created_bin_symlink: false,
    };

    let outcome = commit(request, &prefix, &generation, &manifest, &mut rollback);
    match outcome {
        Ok(()) => {}
        Err(error) => {
            rollback.undo(&prefix);
            return Err(error);
        }
    }

    let receipt = build_receipt(request, &document, &manifest, &signed_manifest_sha256)?;
    if let Err(error) = write_receipt(request, &receipt) {
        rollback.undo(&prefix);
        return Err(error);
    }

    Ok(InstallSummary {
        generation,
        version: manifest.version,
        build_revision: manifest.build_revision,
        reused_generation,
        previous_generation,
    })
}

/// The switch, the bootstrap entrypoint, and post-switch verification.
fn commit(
    request: &InstallRequest,
    prefix: &DirectoryHandle,
    generation: &str,
    manifest: &ReleaseManifest,
    rollback: &mut Rollback,
) -> InstallerResult<()> {
    inject::check(Point::BeforeSwitch)?;

    // The switch is exactly one atomic operation: `symlinkat` to a unique temp
    // name, then `renameat` over `current`. `rename(2)` over an existing symlink
    // is atomic — a concurrent resolver sees the old target or the new one,
    // never neither and never a blend.
    let pointer_temp = format!(".current-{}", random_suffix());
    let target = format!("{GENERATIONS_DIRECTORY}/{generation}");
    prefix.create_symlink(&pointer_temp, &target, "generation pointer")?;
    if let Err(problem) =
        prefix.rename_into(&pointer_temp, prefix, CURRENT_POINTER, "generation pointer")
    {
        let _ = prefix.unlink_file(&pointer_temp, "generation pointer");
        return Err(problem.into());
    }
    // fsync the pointer's parent so the switch itself is committed, not merely
    // visible to a live observer.
    prefix.sync("prefix")?;
    if rollback.previous_generation.is_none() {
        rollback.created_pointer = true;
        inject::check(Point::AfterCurrent)?;
    }
    inject::check(Point::AfterSwitch)?;

    publish_entrypoint(prefix, rollback)?;

    inject::check(Point::PostSwitchVerify)?;
    // Verify through `bin/botster-hub` — the production launch path — rather
    // than through the generation directory, so what is proven is what an
    // operator actually runs.
    let entrypoint = request.prefix.join(BIN_DIRECTORY).join(HUB_BINARY_NAME);
    verify_binary_identity(&entrypoint, manifest, "installed")?;
    Ok(())
}

/// Publish or reuse `bin/botster-hub`.
///
/// An existing symlink is the **expected** state after an abrupt prior attempt,
/// so it gets a rule rather than only an abort-on-surprise:
///
/// * not a symlink → abort; the installer does not clobber an object it cannot
///   prove it owns;
/// * a symlink at the canonical target → reuse, which is what makes a re-run
///   after a crashed bootstrap converge;
/// * a symlink pointing anywhere else → fail closed, neither followed nor
///   replaced. Blind reuse would let post-switch verification execute an
///   attacker-chosen binary; blind replacement would clobber something the
///   installer cannot prove is its own.
fn publish_entrypoint(prefix: &DirectoryHandle, rollback: &mut Rollback) -> InstallerResult<()> {
    let bin = prefix.open_or_create_directory(BIN_DIRECTORY, DIRECTORY_MODE, "bin directory")?;
    match bin.read_symlink(HUB_BINARY_NAME, "entrypoint") {
        Ok(Some(target)) if target == BIN_HUB_SYMLINK_TARGET => {}
        Ok(Some(target)) => {
            return Err(InstallerError::new(
                "entrypoint_foreign_symlink",
                format!(
                    "{BIN_DIRECTORY}/{HUB_BINARY_NAME} points at {target:?} instead of {BIN_HUB_SYMLINK_TARGET:?}; it is neither followed nor replaced"
                ),
            ));
        }
        Ok(None) => {
            let temp = format!(".{HUB_BINARY_NAME}-{}", random_suffix());
            bin.create_symlink(&temp, BIN_HUB_SYMLINK_TARGET, "entrypoint")?;
            if let Err(problem) = bin.rename_into(&temp, &bin, HUB_BINARY_NAME, "entrypoint") {
                let _ = bin.unlink_file(&temp, "entrypoint");
                return Err(problem.into());
            }
            rollback.created_bin_symlink = true;
        }
        Err(problem) => return Err(problem.into()),
    }
    // `bin` is a third directory: fsyncing `generations` and the pointer's
    // parent does not cover it, and without this a durable receipt could survive
    // while its own entrypoint's directory entry was lost.
    bin.sync("bin directory")?;
    inject::check(Point::AfterBin)?;
    Ok(())
}

/// Write the receipt last, so no reachable state has a schema-2 receipt beside
/// an old generation.
fn write_receipt(request: &InstallRequest, receipt: &InstallationReceipt) -> InstallerResult<()> {
    inject::check(Point::BeforeReceipt)?;
    let installations = InstallationsDirectory::open_or_create(&request.home)?;
    if inject::armed(Point::ReceiptWrite).is_some() {
        // Model a crash *during* the write by leaving exactly the on-disk state
        // an interrupted write leaves — one unique stale temporary, no rename —
        // rather than intercepting the shared crate's writer, which would put a
        // test hook in the contract the Hub also links.
        let _ = installations.handle().create_exclusive_file(
            &format!("botster-hub.json.{}.tmp", random_suffix()),
            0o600,
            "receipt temporary",
        );
        inject::check(Point::ReceiptWrite)?;
    }
    installations.write_receipt(receipt)?;
    Ok(())
}

/// What a recoverable failure has to undo.
struct Rollback {
    previous_generation: Option<String>,
    created_pointer: bool,
    created_bin_symlink: bool,
}

impl Rollback {
    /// Reverse the switch, or unmake a partial bootstrap.
    ///
    /// On an upgrade this is the same single atomic operation pointed back at
    /// the retained previous generation — which is what makes rollback cheap and
    /// genuinely testable rather than a narrative claim.
    ///
    /// On a first install there is no previous generation to reverse to, so the
    /// objects this run created are removed instead, leaving no installation
    /// and, critically, **no receipt**. There is never a receipt without a
    /// complete generation behind it.
    fn undo(&self, prefix: &DirectoryHandle) {
        if let Some(previous) = &self.previous_generation {
            let temp = format!(".current-rollback-{}", random_suffix());
            if prefix
                .create_symlink(&temp, previous, "generation pointer")
                .is_ok()
                && prefix
                    .rename_into(&temp, prefix, CURRENT_POINTER, "generation pointer")
                    .is_err()
            {
                let _ = prefix.unlink_file(&temp, "generation pointer");
            }
            let _ = prefix.sync("prefix");
            return;
        }
        if self.created_bin_symlink
            && let Ok(Some(bin)) = prefix.open_directory(BIN_DIRECTORY, "bin directory")
        {
            let _ = bin.unlink_file(HUB_BINARY_NAME, "entrypoint");
            let _ = bin.sync("bin directory");
        }
        if self.created_pointer {
            let _ = prefix.unlink_file(CURRENT_POINTER, "generation pointer");
            let _ = prefix.sync("prefix");
        }
    }
}

/// Fetch the document, verify its signature, and enforce the authority boundary.
fn resolve_release(
    request: &InstallRequest,
) -> InstallerResult<(ReleaseDocument, ReleaseManifest, String)> {
    let body = fetch::fetch(&request.source_url, MAX_RELEASE_BYTES, "release metadata")?;
    let document: ReleaseDocument = serde_json::from_slice(&body).map_err(|error| {
        InstallerError::new(
            "invalid_release_metadata",
            format!("release metadata is not a supported document: {error}"),
        )
    })?;
    if document.schema_version < MINIMUM_RELEASE_SCHEMA_VERSION {
        return Err(InstallerError::new(
            "invalid_release_metadata",
            format!(
                "release schema {} is below the minimum {MINIMUM_RELEASE_SCHEMA_VERSION}",
                document.schema_version
            ),
        ));
    }
    if document.product_id != PRODUCT_ID || document.release_channel != request.release_channel {
        return Err(InstallerError::new(
            "invalid_release_metadata",
            "release metadata identity does not match this installation",
        ));
    }

    let verified = verify::verify_document(&document, &request.trust_anchor)?;
    verify::enforce_envelope_agreement(&document, &verified.manifest)?;
    validate_manifest(&verified.manifest, &request.release_channel)?;
    Ok((document, verified.manifest, verified.signed_manifest_sha256))
}

/// A valid signature proves authorship, not shape.
///
/// Every manifest string that reaches the filesystem or a comparison needs its
/// own validator, and the generation-name components need a stricter one than a
/// display-label sanitizer.
fn validate_manifest(manifest: &ReleaseManifest, channel: &str) -> InstallerResult<()> {
    if manifest.release_channel != channel || !is_supported_channel(&manifest.release_channel) {
        return Err(InstallerError::new(
            "invalid_release_manifest",
            "the signed manifest is not for this release channel",
        ));
    }
    if !is_canonical_object_id(&manifest.source_revisions.botster_hub)
        || !is_canonical_object_id(&manifest.source_revisions.botster_core)
    {
        return Err(InstallerError::new(
            "malformed_manifest_revision",
            "manifest source revisions must be canonical lowercase-hex object ids before they can become path components",
        ));
    }
    if manifest.artifacts.len() != KNOWN_ARTIFACT_NAMES.len() {
        return Err(InstallerError::new(
            "invalid_release_manifest",
            "the manifest must carry exactly the known artifacts",
        ));
    }
    for expected in KNOWN_ARTIFACT_NAMES {
        if manifest
            .artifacts
            .iter()
            .filter(|artifact| artifact.name == expected)
            .count()
            != 1
        {
            return Err(InstallerError::new(
                "invalid_release_manifest",
                format!("the manifest does not name artifact {expected} exactly once"),
            ));
        }
    }
    for artifact in &manifest.artifacts {
        if !is_sha256_hex(&artifact.sha256) {
            return Err(InstallerError::new(
                "invalid_release_manifest",
                format!("artifact {} has a malformed checksum", artifact.name),
            ));
        }
        if artifact.size == 0 || artifact.size > MAX_ARTIFACT_BYTES {
            return Err(InstallerError::new(
                "invalid_release_manifest",
                format!("artifact {} declares an unusable size", artifact.name),
            ));
        }
        validate_release_source(&artifact.url)?;
    }
    Ok(())
}

/// Stage into a unique directory, then rename the finished thing into place.
///
/// Staging never writes into the final generation name. A crash mid-download can
/// therefore only ever leave a partial *staging* directory, never a partial
/// generation: directory rename is atomic, so the final generation name is
/// complete by construction. Staging directly into the deterministic name would
/// have left a half-written generation that a re-run could not distinguish from
/// a good one.
fn stage_generation(
    generations: &DirectoryHandle,
    generation: &str,
    manifest: &ReleaseManifest,
) -> InstallerResult<()> {
    let staging_name = format!("{STAGING_PREFIX}{}", random_suffix());
    generations.create_directory(&staging_name, DIRECTORY_MODE, "staging directory")?;
    let staging = generations
        .open_directory(&staging_name, "staging directory")?
        .ok_or_else(|| {
            InstallerError::new(
                "staging_unavailable",
                "the staging directory vanished immediately after creation",
            )
        })?;

    let staged = stage_artifacts(&staging, manifest);
    if let Err(error) = staged {
        remove_staging(generations, &staging_name);
        return Err(error);
    }

    // 1. each artifact fsynced above, 2. fsync the staging directory,
    // 3. rename it into the final name, 4. fsync `generations` so the
    // generation's existence is committed before anything points at it.
    if let Err(error) = staging
        .sync("staging directory")
        .map_err(InstallerError::from)
        .and_then(|()| inject::check(Point::BeforeStagingRename))
    {
        remove_staging(generations, &staging_name);
        return Err(error);
    }
    generations.rename_into(&staging_name, generations, generation, "generation")?;
    generations.sync("generations")?;
    Ok(())
}

fn stage_artifacts(staging: &DirectoryHandle, manifest: &ReleaseManifest) -> InstallerResult<()> {
    for artifact in &manifest.artifacts {
        // One byte past the declared size, so an oversized body is *detected*
        // rather than silently truncated to a passing length.
        let bytes = fetch::fetch(
            &artifact.url,
            artifact.size + 1,
            &format!("artifact {}", artifact.name),
        )?;
        if bytes.len() as u64 != artifact.size {
            return Err(InstallerError::new(
                "artifact_size_mismatch",
                format!(
                    "artifact {} is {} bytes but the signed manifest declares {}",
                    artifact.name,
                    bytes.len(),
                    artifact.size
                ),
            ));
        }
        inject::check(Point::ArtifactWrite)?;
        let file = staging.create_exclusive_file(&artifact.name, ARTIFACT_MODE, "artifact")?;
        file.write_all(&bytes, "artifact")?;
        file.set_mode(ARTIFACT_MODE, "artifact")?;
        file.sync("artifact")?;
    }
    // Verify what is actually on disk, not the buffer that was written, so the
    // checksum covers the staged artifact itself.
    verify_generation_contents(staging, manifest)
}

/// Verify every artifact's ownership, mode, size, and SHA-256 against the
/// signed manifest.
fn verify_generation_contents(
    directory: &DirectoryHandle,
    manifest: &ReleaseManifest,
) -> InstallerResult<()> {
    for artifact in &manifest.artifacts {
        let file = directory
            .open_regular_file(&artifact.name, "artifact")?
            .ok_or_else(|| {
                InstallerError::new(
                    "artifact_missing",
                    format!("artifact {} is absent from the generation", artifact.name),
                )
            })?;
        let facts = file.facts("artifact")?;
        if facts.uid != effective_uid() || facts.mode != u32::from(ARTIFACT_MODE) {
            return Err(InstallerError::new(
                "artifact_unsafe_ownership",
                format!(
                    "artifact {} is not owned by this user with mode {ARTIFACT_MODE:o}",
                    artifact.name
                ),
            ));
        }
        if facts.size != artifact.size {
            return Err(InstallerError::new(
                "artifact_size_mismatch",
                format!(
                    "artifact {} is {} bytes on disk but the signed manifest declares {}",
                    artifact.name, facts.size, artifact.size
                ),
            ));
        }
        let bytes = file.read_bounded(MAX_ARTIFACT_BYTES, "artifact")?;
        let digest = verify::sha256_hex(&bytes);
        if digest != artifact.sha256 {
            return Err(InstallerError::new(
                "artifact_checksum_mismatch",
                format!(
                    "artifact {} hashes to {digest} but the signed manifest declares {}",
                    artifact.name, artifact.sha256
                ),
            ));
        }
    }
    Ok(())
}

/// Remove installer-owned stale staging directories.
///
/// Same bounded, fail-safe discipline as the receipt temp sweep: only this
/// installer's own name pattern is considered, each candidate is opened
/// `O_NOFOLLOW` with owner and mode validated, and anything failing a check is
/// left alone rather than removed.
fn sweep_stale_staging(generations: &DirectoryHandle) {
    let Ok(names) = generations.entry_names("generations") else {
        return;
    };
    for name in names {
        if !name.starts_with(STAGING_PREFIX) {
            continue;
        }
        remove_staging(generations, &name);
    }
}

fn remove_staging(generations: &DirectoryHandle, name: &str) {
    let Ok(Some(staging)) = generations.open_directory(name, "staging directory") else {
        return;
    };
    let Ok(entries) = staging.entry_names("staging directory") else {
        return;
    };
    for entry in entries {
        let _ = staging.unlink_file(&entry, "staged artifact");
    }
    drop(staging);
    let _ = generations.unlink_directory(name, "staging directory");
}

fn read_pointer(prefix: &DirectoryHandle) -> InstallerResult<Option<String>> {
    match prefix.read_symlink(CURRENT_POINTER, "generation pointer") {
        Ok(target) => Ok(target),
        // A non-symlink at `current` is an object this installer cannot prove it
        // owns, so it is never replaced.
        Err(problem) => Err(problem.into()),
    }
}

/// Run the binary's own `version` subcommand and require it to agree with the
/// signed manifest.
fn verify_binary_identity(
    binary: &Path,
    manifest: &ReleaseManifest,
    stage: &str,
) -> InstallerResult<()> {
    let output = run::run_bounded(binary, &["version"], RUN_DEADLINE)?;
    let pairs = run::parse_key_value(&output.stdout)?;
    let value = |key: &str| {
        pairs
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    for (key, expected) in [
        ("product_id", PRODUCT_ID),
        ("version", manifest.version.as_str()),
        ("build_revision", manifest.build_revision.as_str()),
    ] {
        let actual = value(key).ok_or_else(|| {
            InstallerError::new(
                "identity_verification_failed",
                format!("the {stage} Hub binary reported no {key}"),
            )
        })?;
        if actual != expected {
            return Err(InstallerError::new(
                "identity_verification_failed",
                format!(
                    "the {stage} Hub binary reports {key}={actual:?} but the signed manifest declares {expected:?}"
                ),
            ));
        }
    }
    Ok(())
}

fn build_receipt(
    request: &InstallRequest,
    document: &ReleaseDocument,
    manifest: &ReleaseManifest,
    signed_manifest_sha256: &str,
) -> InstallerResult<InstallationReceipt> {
    let receipt = InstallationReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        product_id: PRODUCT_ID.to_string(),
        binary_version: manifest.version.clone(),
        installation_mode: "managed".to_string(),
        release_channel: manifest.release_channel.clone(),
        provider: "http_json".to_string(),
        source_url: request.source_url.clone(),
        build_revision: manifest.build_revision.clone(),
        artifacts: manifest
            .artifacts
            .iter()
            .map(|artifact: &ManifestArtifact| ReceiptArtifact {
                name: artifact.name.clone(),
                sha256: artifact.sha256.clone(),
                size: artifact.size,
            })
            .collect(),
        source_revisions: ReceiptSourceRevisions {
            botster_hub: manifest.source_revisions.botster_hub.clone(),
            botster_core: manifest.source_revisions.botster_core.clone(),
        },
        signature: ReceiptSignature {
            algorithm: document.signature.algorithm.clone(),
            key_id: document.signature.key_id.clone(),
            signed_manifest_sha256: signed_manifest_sha256.to_string(),
        },
        installer: ReceiptInstaller {
            id: INSTALLER_ID.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
    };
    // Validate what is about to be written with the same rules the Hub reads it
    // by, so the installer can never produce a receipt the Hub will reject.
    receipt
        .validate(&manifest.version, Some(&manifest.build_revision))
        .map_err(InstallerError::from)?;
    Ok(receipt)
}

/// Where the worker of the live generation resolves, for diagnostics.
#[must_use]
pub fn worker_path(prefix: &Path, generation: &str) -> PathBuf {
    prefix
        .join(GENERATIONS_DIRECTORY)
        .join(generation)
        .join(WORKER_BINARY_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_installation::{ManifestSourceRevisions, ReleaseSignature};

    fn manifest() -> ReleaseManifest {
        ReleaseManifest {
            product_id: PRODUCT_ID.to_string(),
            release_channel: "stable".to_string(),
            version: "0.2.0".to_string(),
            build_revision: "0".repeat(40),
            source_revisions: ManifestSourceRevisions {
                botster_hub: "0".repeat(40),
                botster_core: "1".repeat(40),
            },
            artifacts: vec![
                ManifestArtifact {
                    name: "botster-hub".to_string(),
                    url: "https://releases.example.invalid/botster-hub".to_string(),
                    size: 10,
                    sha256: "a".repeat(64),
                },
                ManifestArtifact {
                    name: "botster-session-worker".to_string(),
                    url: "https://releases.example.invalid/botster-session-worker".to_string(),
                    size: 20,
                    sha256: "b".repeat(64),
                },
            ],
        }
    }

    #[test]
    fn a_signed_manifest_is_still_subjected_to_path_component_validation() {
        for hostile in ["../escape", "a/b", "", &"a".repeat(41), "ABCDEF"] {
            let mut tampered = manifest();
            tampered.source_revisions.botster_hub = hostile.to_string();
            assert_eq!(
                validate_manifest(&tampered, "stable")
                    .expect_err("a hostile revision must be rejected")
                    .kind(),
                "malformed_manifest_revision",
                "hostile={hostile}"
            );
            assert_eq!(
                generation_name(hostile, &"1".repeat(40)),
                None,
                "hostile={hostile}"
            );
        }
    }

    /// Every artifact URL is a network coordinate in its own right, so the
    /// HTTPS rule covers it too.
    #[test]
    fn artifact_urls_are_validated_independently_of_the_metadata_url() {
        let mut plaintext = manifest();
        plaintext.artifacts[1].url = "http://192.0.2.10/botster-session-worker".to_string();
        assert_eq!(
            validate_manifest(&plaintext, "stable")
                .expect_err("a non-loopback plaintext artifact URL must be rejected")
                .kind(),
            "insecure_release_source"
        );

        let mut loopback = manifest();
        loopback.artifacts[0].url = "http://127.0.0.1:9/botster-hub".to_string();
        assert!(validate_manifest(&loopback, "stable").is_ok());

        // A separate artifact host is allowed: each URL is validated on its own
        // and every artifact is checksum-verified against the signed manifest.
        let mut cdn = manifest();
        cdn.artifacts[0].url = "https://cdn.example.invalid/botster-hub".to_string();
        assert!(validate_manifest(&cdn, "stable").is_ok());
    }

    #[test]
    fn manifest_artifact_names_checksums_and_sizes_are_validated() {
        let mut unknown = manifest();
        unknown.artifacts[1].name = "botster-unexpected".to_string();
        assert_eq!(
            validate_manifest(&unknown, "stable")
                .expect_err("unknown artifact")
                .kind(),
            "invalid_release_manifest"
        );

        let mut malformed = manifest();
        malformed.artifacts[0].sha256 = "nothex".to_string();
        assert_eq!(
            validate_manifest(&malformed, "stable")
                .expect_err("bad checksum")
                .kind(),
            "invalid_release_manifest"
        );

        let mut oversized = manifest();
        oversized.artifacts[0].size = MAX_ARTIFACT_BYTES + 1;
        assert_eq!(
            validate_manifest(&oversized, "stable")
                .expect_err("oversized")
                .kind(),
            "invalid_release_manifest"
        );

        let mut wrong_channel = manifest();
        wrong_channel.release_channel = "beta".to_string();
        assert_eq!(
            validate_manifest(&wrong_channel, "stable")
                .expect_err("channel")
                .kind(),
            "invalid_release_manifest"
        );
    }

    #[test]
    fn a_built_receipt_always_satisfies_the_rules_the_hub_reads_it_by() {
        let manifest = manifest();
        let document = ReleaseDocument {
            schema_version: 2,
            product_id: PRODUCT_ID.to_string(),
            release_channel: "stable".to_string(),
            version: manifest.version.clone(),
            build_revision: manifest.build_revision.clone(),
            install_manifest: String::new(),
            signature: ReleaseSignature {
                algorithm: "ed25519".to_string(),
                key_id: "test-only-do-not-trust".to_string(),
                value: String::new(),
            },
        };
        let request = InstallRequest {
            prefix: PathBuf::from("/tmp/prefix"),
            home: PathBuf::from("/tmp/home"),
            source_url: "https://releases.example.invalid/botster-hub.json".to_string(),
            release_channel: "stable".to_string(),
            trust_anchor: vec![0_u8; 32],
        };
        let receipt = build_receipt(&request, &document, &manifest, &"c".repeat(64))
            .expect("a receipt built from a valid manifest validates");
        assert_eq!(receipt.schema_version, RECEIPT_SCHEMA_VERSION);
        assert_eq!(
            receipt.source_revisions.botster_hub,
            manifest.source_revisions.botster_hub
        );
        assert_ne!(
            receipt.source_revisions.botster_hub, receipt.source_revisions.botster_core,
            "Hub and locked-Core provenance stay distinct"
        );
        assert_eq!(receipt.signature.signed_manifest_sha256, "c".repeat(64));
    }
}
