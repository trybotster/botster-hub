//! Authoritative Hub software identity and installation-aware update checks.
//!
//! Binary identity is embedded at build time. Installation provenance comes
//! only from the cold-turkey receipt at `$HOME/.botster/installations/botster-hub.json`,
//! whose shape, safety rules, and atomic write live in `botster-hub-installation`
//! so the installer that writes it and the Hub that reads it cannot disagree.
//!
//! The Hub verifies **no** signature and holds **no** trust anchor. The
//! installer is the trust boundary because it is the component that writes
//! executables to disk; `check-update` is read-only and non-destructive, so
//! forged metadata yields at worst a misleading "update available" that the
//! installer then refuses. The receipt's signature fields are recorded facts,
//! not something checked here.

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use botster_hub_client::{
    DaemonHubUpdate, DaemonHubUpdateState, DaemonInstallationDiagnostic,
    DaemonInstallationIdentity, DaemonInstallationMode, DaemonSoftwareIdentity,
};
use botster_hub_installation::{
    InstallationProblem, InstallationReceipt, InstallationsDirectory, MAX_RELEASE_BYTES,
    MINIMUM_RELEASE_SCHEMA_VERSION,
};
use semver::Version;
use serde::Deserialize;

const PRODUCT_ID: &str = "botster-hub";
const PRODUCT_NAME: &str = "Botster Hub";
const RELEASE_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HubUpdateCheckPlan {
    Immediate(DaemonHubUpdate),
    Managed(ManagedReleaseCheck),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedReleaseCheck {
    source_url: String,
    release_channel: String,
}

/// The Hub's forward-tolerant read of a release document.
///
/// Deliberately *not* `deny_unknown_fields`, and deliberately compared with
/// `>=` rather than `==` on `schema_version`. Release metadata is read by
/// binaries already in the field that we cannot reach, and a Hub that cannot
/// parse a newer document cannot tell its user an update exists — so strictness
/// here would disable the very channel that ships the fix for the strictness.
///
/// The asymmetry with the receipt is intentional. `RECEIPT_SCHEMA_VERSION`
/// stays exact because the receipt is local state written by our own installer,
/// both ends are controlled, and the ticket's upgrade ordering *depends* on an
/// older Hub rejecting a receipt schema it does not know.
///
/// `product_id` and `release_channel` stay **exact** matches: those are
/// identity, not versioning, and a mismatch means the document is not for this
/// installation.
#[derive(Debug, Deserialize)]
struct ReleaseMetadata {
    schema_version: u16,
    product_id: String,
    release_channel: String,
    version: String,
    #[serde(default)]
    build_revision: Option<String>,
}

struct ReceiptResolution {
    identity: DaemonInstallationIdentity,
    receipt: Option<InstallationReceipt>,
}

#[must_use]
pub fn software_identity() -> DaemonSoftwareIdentity {
    DaemonSoftwareIdentity {
        product_id: PRODUCT_ID.to_string(),
        product_name: PRODUCT_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_revision: embedded_build_revision().map(str::to_string),
    }
}

/// The build revision compiled into this binary, if any.
///
/// `None` in a development build. The receipt's `build_revision` agreement
/// check is skipped in that case rather than failed: a value cannot disagree
/// with the absence of one, and failing would perturb development and unmanaged
/// behavior that this ticket does not touch.
#[must_use]
pub fn embedded_build_revision() -> Option<&'static str> {
    option_env!("BOTSTER_EMBEDDED_BUILD_REVISION")
}

#[must_use]
pub fn installation_identity() -> DaemonInstallationIdentity {
    resolve_receipt().identity
}

#[must_use]
pub(crate) fn plan_hub_update_check() -> HubUpdateCheckPlan {
    plan_hub_update_check_for_resolution(resolve_receipt())
}

fn plan_hub_update_check_for_resolution(resolution: ReceiptResolution) -> HubUpdateCheckPlan {
    let current_version = software_identity().version;
    let Some(receipt) = resolution.receipt else {
        let invalid = !resolution.identity.diagnostics.is_empty();
        return HubUpdateCheckPlan::Immediate(DaemonHubUpdate {
            state: DaemonHubUpdateState::Unavailable,
            current_version,
            available_version: None,
            build_revision: None,
            reason: Some(unavailable_reason(resolution.identity.mode, invalid).to_string()),
            action: Some("manual".to_string()),
        });
    };

    HubUpdateCheckPlan::Managed(ManagedReleaseCheck {
        source_url: receipt.source_url,
        release_channel: receipt.release_channel,
    })
}

#[must_use]
pub(crate) fn execute_managed_update_check(check: ManagedReleaseCheck) -> DaemonHubUpdate {
    execute_managed_update_check_with_fetch(check, fetch_release_metadata)
}

fn execute_managed_update_check_with_fetch(
    check: ManagedReleaseCheck,
    fetch: impl FnOnce(&str) -> Result<ReleaseMetadata, &'static str>,
) -> DaemonHubUpdate {
    let current_version = software_identity().version;
    let metadata = match fetch(&check.source_url) {
        Ok(metadata) => metadata,
        Err(reason) => return unavailable_update(current_version, reason, "retry"),
    };

    if metadata.schema_version < MINIMUM_RELEASE_SCHEMA_VERSION
        || metadata.product_id != PRODUCT_ID
        || metadata.release_channel != check.release_channel
        || !botster_hub_installation::is_supported_channel(&metadata.release_channel)
        || metadata
            .build_revision
            .as_deref()
            .is_some_and(|revision| !botster_hub_installation::is_sanitized_revision(revision))
    {
        return unavailable_update(
            current_version,
            "invalid_release_metadata",
            "contact_provider",
        );
    }

    let Ok(current) = Version::parse(&current_version) else {
        return unavailable_update(current_version, "invalid_embedded_version", "reinstall");
    };
    let Ok(available) = Version::parse(&metadata.version) else {
        return unavailable_update(
            current_version,
            "invalid_release_version",
            "contact_provider",
        );
    };

    use std::cmp::Ordering;
    match available.cmp(&current) {
        Ordering::Greater => DaemonHubUpdate {
            state: DaemonHubUpdateState::Available,
            current_version,
            available_version: Some(metadata.version),
            build_revision: metadata.build_revision,
            reason: Some("newer_release_available".to_string()),
            action: Some("run_managed_installer".to_string()),
        },
        Ordering::Equal => DaemonHubUpdate {
            state: DaemonHubUpdateState::Current,
            current_version,
            available_version: Some(metadata.version),
            build_revision: metadata.build_revision,
            reason: Some("up_to_date".to_string()),
            action: None,
        },
        Ordering::Less => DaemonHubUpdate {
            state: DaemonHubUpdateState::Current,
            current_version,
            available_version: Some(metadata.version),
            build_revision: metadata.build_revision,
            reason: Some("source_behind".to_string()),
            action: Some("no_downgrade".to_string()),
        },
    }
}

fn unavailable_update(
    current_version: String,
    reason: impl Into<String>,
    action: impl Into<String>,
) -> DaemonHubUpdate {
    DaemonHubUpdate {
        state: DaemonHubUpdateState::Unavailable,
        current_version,
        available_version: None,
        build_revision: None,
        reason: Some(reason.into()),
        action: Some(action.into()),
    }
}

fn fetch_release_metadata(source_url: &str) -> Result<ReleaseMetadata, &'static str> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(Some(RELEASE_CHECK_TIMEOUT))
        .timeout_connect(Some(RELEASE_CHECK_TIMEOUT))
        .timeout_recv_response(Some(RELEASE_CHECK_TIMEOUT))
        .timeout_recv_body(Some(RELEASE_CHECK_TIMEOUT))
        .build()
        .into();
    let request = ureq::http::Request::builder()
        .method(ureq::http::Method::GET)
        .uri(source_url)
        .body(Vec::new())
        .map_err(|_| "invalid_release_source")?;
    let mut response = agent.run(request).map_err(|error| match error {
        ureq::Error::Timeout(_) => "release_source_timeout",
        _ => "release_source_unreachable",
    })?;
    if !response.status().is_success() {
        return Err("release_source_unavailable");
    }
    let body = response
        .body_mut()
        .with_config()
        .limit(MAX_RELEASE_BYTES)
        .read_to_vec()
        .map_err(|_| "invalid_release_metadata")?;
    serde_json::from_slice(&body).map_err(|_| "invalid_release_metadata")
}

fn resolve_receipt() -> ReceiptResolution {
    let Some(home) = env::var_os("HOME").filter(|home| !home.is_empty()) else {
        return fallback_resolution(Some(diagnostic(
            "home_unavailable",
            "HOME is required to resolve the Hub installation receipt",
        )));
    };
    resolve_receipt_under_home(&PathBuf::from(home))
}

/// Resolve the receipt below `home` through the shared descriptor-relative
/// reader.
///
/// The reader moved to the shared crate's `openat`/`O_NOFOLLOW` walk with
/// `fstat` on the opened descriptors. The previous path-stat-then-read had the
/// same check/use race on the read side that the write side needed closing;
/// fixing it is cleanup made necessary by sharing the code, not opportunistic
/// refactoring.
fn resolve_receipt_under_home(home: &Path) -> ReceiptResolution {
    let directory = match InstallationsDirectory::open(home) {
        Ok(Some(directory)) => directory,
        Ok(None) => return fallback_resolution(None),
        Err(problem) => return fallback_resolution(Some(problem_diagnostic(&problem))),
    };
    let receipt = match directory.read_receipt() {
        Ok(Some(receipt)) => receipt,
        Ok(None) => return fallback_resolution(None),
        Err(problem) => return fallback_resolution(Some(problem_diagnostic(&problem))),
    };
    if let Err(problem) = receipt.validate(env!("CARGO_PKG_VERSION"), embedded_build_revision()) {
        return fallback_resolution(Some(problem_diagnostic(&problem)));
    }

    ReceiptResolution {
        identity: DaemonInstallationIdentity {
            mode: DaemonInstallationMode::Managed,
            provenance: "managed_receipt".to_string(),
            release_channel: Some(receipt.release_channel.clone()),
            provider: Some(receipt.provider.clone()),
            diagnostics: Vec::new(),
        },
        receipt: Some(receipt),
    }
}

fn fallback_resolution(diagnostic: Option<DaemonInstallationDiagnostic>) -> ReceiptResolution {
    let (mode, provenance) = fallback_installation(cfg!(debug_assertions));
    ReceiptResolution {
        identity: DaemonInstallationIdentity {
            mode,
            provenance: provenance.to_string(),
            release_channel: None,
            provider: None,
            diagnostics: diagnostic.into_iter().collect(),
        },
        receipt: None,
    }
}

fn fallback_installation(debug_build: bool) -> (DaemonInstallationMode, &'static str) {
    if debug_build {
        (DaemonInstallationMode::Development, "development_build")
    } else {
        (DaemonInstallationMode::Unmanaged, "manual_install")
    }
}

fn unavailable_reason(mode: DaemonInstallationMode, invalid: bool) -> &'static str {
    if invalid {
        "invalid_installation_receipt"
    } else {
        match mode {
            DaemonInstallationMode::Development => "development_checkout",
            DaemonInstallationMode::Unmanaged => "unmanaged_installation",
            DaemonInstallationMode::Managed => unreachable!("managed identity has receipt"),
        }
    }
}

fn diagnostic(kind: impl Into<String>, message: impl Into<String>) -> DaemonInstallationDiagnostic {
    DaemonInstallationDiagnostic {
        kind: kind.into(),
        message: message.into(),
    }
}

fn problem_diagnostic(problem: &InstallationProblem) -> DaemonInstallationDiagnostic {
    diagnostic(problem.kind(), problem.message())
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_installation::{
        RECEIPT_SCHEMA_VERSION, ReceiptArtifact, ReceiptInstaller, ReceiptSignature,
        ReceiptSourceRevisions,
    };
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::{AtomicU64, Ordering};

    static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct Fixture {
        root: PathBuf,
        receipt: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = env::temp_dir().join(format!(
                "botster-hub-maintenance-{}-{}",
                std::process::id(),
                FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            let receipt = root.join(botster_hub_installation::RECEIPT_RELATIVE_PATH);
            fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create fixture");
            Self { root, receipt }
        }

        fn write(&self, value: serde_json::Value) {
            fs::write(
                &self.receipt,
                serde_json::to_vec(&value).expect("serialize receipt"),
            )
            .expect("write receipt");
        }

        fn resolve(&self) -> ReceiptResolution {
            resolve_receipt_under_home(&self.root)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn valid_receipt() -> serde_json::Value {
        serde_json::to_value(InstallationReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            product_id: PRODUCT_ID.to_string(),
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
            installation_mode: "managed".to_string(),
            release_channel: "stable".to_string(),
            provider: "http_json".to_string(),
            source_url: "https://releases.example.invalid/botster-hub.json".to_string(),
            build_revision: "release1".to_string(),
            artifacts: vec![
                ReceiptArtifact {
                    name: "botster-hub".to_string(),
                    sha256: "a".repeat(64),
                    size: 1024,
                },
                ReceiptArtifact {
                    name: "botster-session-worker".to_string(),
                    sha256: "b".repeat(64),
                    size: 2048,
                },
            ],
            source_revisions: ReceiptSourceRevisions {
                botster_hub: "0".repeat(40),
                botster_core: "1".repeat(40),
            },
            signature: ReceiptSignature {
                algorithm: "ed25519".to_string(),
                key_id: "test-only-do-not-trust".to_string(),
                signed_manifest_sha256: "c".repeat(64),
            },
            installer: ReceiptInstaller {
                id: "botster-hub-installer".to_string(),
                version: "0.1.0".to_string(),
            },
        })
        .expect("serialize valid receipt")
    }

    #[test]
    fn software_identity_uses_embedded_package_version_without_paths() {
        let identity = software_identity();
        assert_eq!(identity.product_id, PRODUCT_ID);
        assert_eq!(identity.version, env!("CARGO_PKG_VERSION"));
        assert!(
            !serde_json::to_string(&identity)
                .expect("serialize identity")
                .contains(std::path::MAIN_SEPARATOR)
        );
    }

    #[test]
    fn valid_receipt_is_managed_and_binary_authoritative() {
        let fixture = Fixture::new();
        fixture.write(valid_receipt());
        let resolution = fixture.resolve();
        assert_eq!(resolution.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            resolution.identity.release_channel.as_deref(),
            Some("stable")
        );
        assert!(resolution.identity.diagnostics.is_empty());
        assert!(resolution.receipt.is_some());
    }

    /// Cold turkey: schema 1 is not accepted alongside schema 2, it is rejected
    /// as unsupported and the installation degrades to unmanaged. That rejection
    /// is load-bearing — the ticket's upgrade ordering depends on an older Hub
    /// refusing a receipt schema it does not know.
    #[test]
    fn schema_one_receipts_are_rejected_as_unsupported_and_treated_as_unmanaged() {
        let fixture = Fixture::new();
        fixture.write(serde_json::json!({
            "schema_version": 1,
            "product_id": PRODUCT_ID,
            "binary_version": env!("CARGO_PKG_VERSION"),
            "installation_mode": "managed",
            "release_channel": "stable",
            "provider": "http_json",
            "source_url": "https://releases.example.invalid/botster-hub.json"
        }));
        let resolution = fixture.resolve();
        assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
        // A schema-1 document lacks every schema-2 field, so the strict receipt
        // reader rejects the shape before the version check ever runs. Either
        // way it never becomes managed, which is the property that matters.
        assert_eq!(resolution.identity.diagnostics[0].kind, "malformed_receipt");

        let mut future = valid_receipt();
        future["schema_version"] = serde_json::json!(3);
        fixture.write(future);
        let resolution = fixture.resolve();
        assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            resolution.identity.diagnostics[0].kind,
            "unsupported_receipt_schema"
        );
    }

    #[test]
    fn malformed_and_mismatched_receipts_never_become_managed() {
        let fixture = Fixture::new();
        fs::write(&fixture.receipt, b"{").expect("write malformed receipt");
        let malformed = fixture.resolve();
        assert_ne!(malformed.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(malformed.identity.diagnostics[0].kind, "malformed_receipt");

        let mut mismatched = valid_receipt();
        mismatched["binary_version"] = serde_json::json!("999.0.0");
        fixture.write(mismatched);
        let mismatched = fixture.resolve();
        assert_ne!(mismatched.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            mismatched.identity.diagnostics[0].kind,
            "receipt_binary_mismatch"
        );
    }

    #[test]
    fn unsupported_receipt_fields_are_diagnosed() {
        for (field, value, expected) in [
            (
                "provider",
                serde_json::json!("git"),
                "unsupported_release_provider",
            ),
            (
                "release_channel",
                serde_json::json!("custom"),
                "unsupported_release_channel",
            ),
            (
                "installation_mode",
                serde_json::json!("linked"),
                "unsupported_installation_mode",
            ),
        ] {
            let fixture = Fixture::new();
            let mut receipt = valid_receipt();
            receipt[field] = value;
            fixture.write(receipt);
            let resolution = fixture.resolve();
            assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
            assert_eq!(resolution.identity.diagnostics[0].kind, expected);
        }
    }

    /// The additive schema-2 fields get shape validation, not trust: malformed
    /// hex, an unknown algorithm, and an unrecognized artifact name each
    /// diagnose rather than being accepted as facts.
    #[test]
    fn additive_schema_two_fields_are_shape_validated() {
        for (pointer, value, expected) in [
            (
                "/artifacts/0/sha256",
                serde_json::json!("not-hex"),
                "malformed_receipt_checksum",
            ),
            (
                "/signature/algorithm",
                serde_json::json!("rsa-pss"),
                "unsupported_signature_algorithm",
            ),
            (
                "/artifacts/1/name",
                serde_json::json!("botster-unexpected"),
                "unknown_receipt_artifact",
            ),
            (
                "/source_revisions/botster_core",
                serde_json::json!("../escape"),
                "malformed_receipt_revision",
            ),
            (
                "/signature/signed_manifest_sha256",
                serde_json::json!("abc"),
                "malformed_receipt_checksum",
            ),
            (
                "/installer/id",
                serde_json::json!("installer/../../etc"),
                "malformed_receipt_field",
            ),
        ] {
            let fixture = Fixture::new();
            let mut receipt = valid_receipt();
            *receipt.pointer_mut(pointer).expect("pointer exists") = value;
            fixture.write(receipt);
            let resolution = fixture.resolve();
            assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
            assert_eq!(
                resolution.identity.diagnostics[0].kind, expected,
                "pointer={pointer}"
            );
        }
    }

    /// `build_revision` agreement applies only when the running binary carries
    /// an embedded revision. A development build has none, and skipping the
    /// check there is what keeps this ticket from perturbing development or
    /// unmanaged behavior.
    #[test]
    fn build_revision_agreement_applies_only_to_a_binary_with_an_embedded_revision() {
        let receipt: InstallationReceipt =
            serde_json::from_value(valid_receipt()).expect("parse receipt");
        assert_eq!(receipt.validate(env!("CARGO_PKG_VERSION"), None), Ok(()));
        assert_eq!(
            receipt.validate(env!("CARGO_PKG_VERSION"), Some("release1")),
            Ok(())
        );
        assert_eq!(
            receipt
                .validate(env!("CARGO_PKG_VERSION"), Some("release2"))
                .expect_err("a disagreeing embedded revision diagnoses")
                .kind(),
            "receipt_build_revision_mismatch"
        );
    }

    #[test]
    fn managed_release_sources_require_https_except_for_explicit_loopback_fixtures() {
        let fixture = Fixture::new();
        let mut receipt = valid_receipt();
        receipt["source_url"] = serde_json::json!("http://192.0.2.10/botster-hub.json");
        fixture.write(receipt);
        let resolution = fixture.resolve();
        assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            resolution.identity.diagnostics[0].kind,
            "insecure_release_source"
        );

        let fixture = Fixture::new();
        let mut receipt = valid_receipt();
        receipt["source_url"] = serde_json::json!("http://127.0.0.1:8123/botster-hub.json");
        fixture.write(receipt);
        assert_eq!(
            fixture.resolve().identity.mode,
            DaemonInstallationMode::Managed
        );
    }

    #[test]
    fn fallback_modes_and_update_reasons_cover_development_unmanaged_and_invalid_receipts() {
        assert_eq!(
            fallback_installation(true),
            (DaemonInstallationMode::Development, "development_build")
        );
        assert_eq!(
            fallback_installation(false),
            (DaemonInstallationMode::Unmanaged, "manual_install")
        );
        assert_eq!(
            unavailable_reason(DaemonInstallationMode::Development, false),
            "development_checkout"
        );
        assert_eq!(
            unavailable_reason(DaemonInstallationMode::Unmanaged, false),
            "unmanaged_installation"
        );
        assert_eq!(
            unavailable_reason(DaemonInstallationMode::Development, true),
            "invalid_installation_receipt"
        );

        let update = plan_hub_update_check_for_resolution(ReceiptResolution {
            identity: DaemonInstallationIdentity {
                mode: DaemonInstallationMode::Unmanaged,
                provenance: "manual_install".to_string(),
                release_channel: None,
                provider: None,
                diagnostics: Vec::new(),
            },
            receipt: None,
        });
        let HubUpdateCheckPlan::Immediate(update) = update else {
            panic!("unmanaged installation must not query a provider");
        };
        assert_eq!(update.state, DaemonHubUpdateState::Unavailable);
        assert_eq!(update.reason.as_deref(), Some("unmanaged_installation"));
        assert_eq!(update.action.as_deref(), Some("manual"));
    }

    #[test]
    fn unsafe_receipt_file_and_directory_are_rejected() {
        let fixture = Fixture::new();
        fixture.write(valid_receipt());
        let mut permissions = fs::metadata(&fixture.receipt)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(&fixture.receipt, permissions).expect("set permissions");
        let world_writable = fixture.resolve();
        assert_eq!(
            world_writable.identity.diagnostics[0].kind,
            "receipt_world_writable"
        );

        fs::remove_file(&fixture.receipt).expect("remove receipt");
        let target = fixture.root.join("target.json");
        fs::write(&target, serde_json::to_vec(&valid_receipt()).expect("json"))
            .expect("write target");
        symlink(&target, &fixture.receipt).expect("symlink receipt");
        let symlinked = fixture.resolve();
        assert_eq!(symlinked.identity.diagnostics[0].kind, "receipt_symlink");
    }

    #[test]
    fn non_regular_permission_denied_and_world_writable_directory_receipts_are_rejected() {
        let fixture = Fixture::new();
        fs::remove_file(&fixture.receipt).ok();
        fs::create_dir(&fixture.receipt).expect("create receipt directory");
        let non_regular = fixture.resolve();
        assert_eq!(
            non_regular.identity.diagnostics[0].kind,
            "receipt_not_regular_file"
        );

        fs::remove_dir(&fixture.receipt).expect("remove receipt directory");
        fixture.write(valid_receipt());
        let mut permissions = fs::metadata(&fixture.receipt)
            .expect("receipt metadata")
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&fixture.receipt, permissions).expect("deny receipt reads");
        let denied = fixture.resolve();
        assert_eq!(
            denied.identity.diagnostics[0].kind,
            "receipt_permission_denied"
        );

        let mut permissions = fs::metadata(fixture.receipt.parent().expect("receipt parent"))
            .expect("receipt parent metadata")
            .permissions();
        permissions.set_mode(0o777);
        fs::set_permissions(
            fixture.receipt.parent().expect("receipt parent"),
            permissions,
        )
        .expect("make receipt parent world-writable");
        let unsafe_directory = fixture.resolve();
        assert_eq!(
            unsafe_directory.identity.diagnostics[0].kind,
            "receipt_world_writable"
        );
    }

    #[test]
    fn managed_update_comparison_covers_current_available_and_source_behind() {
        let check = || ManagedReleaseCheck {
            source_url: "https://example.invalid/releases.json".to_string(),
            release_channel: "stable".to_string(),
        };
        let result = |version: &str| ReleaseMetadata {
            schema_version: MINIMUM_RELEASE_SCHEMA_VERSION,
            product_id: PRODUCT_ID.to_string(),
            release_channel: "stable".to_string(),
            version: version.to_string(),
            build_revision: Some("abc123".to_string()),
        };

        let current = execute_managed_update_check_with_fetch(check(), |_| {
            Ok(result(env!("CARGO_PKG_VERSION")))
        });
        assert_eq!(current.state, DaemonHubUpdateState::Current);
        assert_eq!(current.reason.as_deref(), Some("up_to_date"));

        let available = execute_managed_update_check_with_fetch(check(), |_| Ok(result("99.0.0")));
        assert_eq!(available.state, DaemonHubUpdateState::Available);

        let behind = execute_managed_update_check_with_fetch(check(), |_| Ok(result("0.0.1")));
        assert_eq!(behind.state, DaemonHubUpdateState::Current);
        assert_eq!(behind.reason.as_deref(), Some("source_behind"));
        assert_eq!(behind.action.as_deref(), Some("no_downgrade"));
    }

    /// The guarantee test for forward tolerance. Deleting `deny_unknown_fields`
    /// is only a deleted attribute until something asserts that a *newer* schema
    /// carrying an *unknown* field still produces the right answer.
    #[test]
    fn a_newer_schema_with_unknown_fields_still_answers_available_or_current() {
        let document = |version: &str| {
            serde_json::json!({
                "schema_version": MINIMUM_RELEASE_SCHEMA_VERSION + 1,
                "product_id": PRODUCT_ID,
                "release_channel": "stable",
                "version": version,
                "build_revision": "release99",
                "install_manifest": "aGVsbG8=",
                "signature": { "algorithm": "ed25519", "key_id": "k", "value": "sig" },
                "delta_updates": { "from": ["0.1.0"], "patch_url": "https://example.invalid/p" },
                "platform_matrix": ["aarch64-apple-darwin"]
            })
        };
        let check = || ManagedReleaseCheck {
            source_url: "https://example.invalid/releases.json".to_string(),
            release_channel: "stable".to_string(),
        };

        let available = execute_managed_update_check_with_fetch(check(), |_| {
            serde_json::from_value(document("99.0.0")).map_err(|_| "invalid_release_metadata")
        });
        assert_eq!(available.state, DaemonHubUpdateState::Available);
        assert_eq!(available.available_version.as_deref(), Some("99.0.0"));
        assert_eq!(available.action.as_deref(), Some("run_managed_installer"));

        let current = execute_managed_update_check_with_fetch(check(), |_| {
            serde_json::from_value(document(env!("CARGO_PKG_VERSION")))
                .map_err(|_| "invalid_release_metadata")
        });
        assert_eq!(current.state, DaemonHubUpdateState::Current);
        assert_eq!(current.reason.as_deref(), Some("up_to_date"));
    }

    /// Forward tolerance is bounded. Identity stays exact and a schema below the
    /// minimum is still refused, so nobody can read "forward tolerant" as
    /// licence to loosen the whole validator.
    #[test]
    fn identity_stays_exact_and_a_schema_below_the_minimum_is_still_refused() {
        let check = || ManagedReleaseCheck {
            source_url: "https://example.invalid/releases.json".to_string(),
            release_channel: "stable".to_string(),
        };
        let metadata = |schema: u16, product: &str, channel: &str| ReleaseMetadata {
            schema_version: schema,
            product_id: product.to_string(),
            release_channel: channel.to_string(),
            version: "99.0.0".to_string(),
            build_revision: Some("release99".to_string()),
        };

        for (schema, product, channel) in [
            (MINIMUM_RELEASE_SCHEMA_VERSION - 1, PRODUCT_ID, "stable"),
            (MINIMUM_RELEASE_SCHEMA_VERSION, "botster-core", "stable"),
            (MINIMUM_RELEASE_SCHEMA_VERSION, PRODUCT_ID, "beta"),
        ] {
            let update = execute_managed_update_check_with_fetch(check(), |_| {
                Ok(metadata(schema, product, channel))
            });
            assert_eq!(
                update.reason.as_deref(),
                Some("invalid_release_metadata"),
                "schema={schema} product={product} channel={channel}"
            );
        }
    }

    #[test]
    fn provider_failures_are_typed_unavailable() {
        let result = execute_managed_update_check_with_fetch(
            ManagedReleaseCheck {
                source_url: "https://example.invalid/releases.json".to_string(),
                release_channel: "stable".to_string(),
            },
            |_| Err("release_source_timeout"),
        );
        assert_eq!(result.state, DaemonHubUpdateState::Unavailable);
        assert_eq!(result.reason.as_deref(), Some("release_source_timeout"));
        assert_eq!(result.action.as_deref(), Some("retry"));
    }
}
