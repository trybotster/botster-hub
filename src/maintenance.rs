//! Authoritative Hub software identity and installation-aware update checks.
//!
//! Binary identity is embedded at build time. Installation provenance comes
//! only from the cold-turkey receipt at `$HOME/.botster/installations/botster-hub.json`.

use std::env;
use std::fs;
use std::net::IpAddr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use botster_hub_client::{
    DaemonHubUpdate, DaemonHubUpdateState, DaemonInstallationDiagnostic,
    DaemonInstallationIdentity, DaemonInstallationMode, DaemonSoftwareIdentity,
};
use semver::Version;
use serde::Deserialize;

const PRODUCT_ID: &str = "botster-hub";
const PRODUCT_NAME: &str = "Botster Hub";
const RECEIPT_SCHEMA_VERSION: u16 = 1;
const RELEASE_SCHEMA_VERSION: u16 = 1;
const RECEIPT_RELATIVE_PATH: &str = ".botster/installations/botster-hub.json";
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;
const MAX_RELEASE_BYTES: u64 = 64 * 1024;
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallationReceipt {
    schema_version: u16,
    product_id: String,
    binary_version: String,
    installation_mode: String,
    release_channel: String,
    provider: String,
    source_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
        build_revision: option_env!("BOTSTER_EMBEDDED_BUILD_REVISION").map(str::to_string),
    }
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

    if metadata.schema_version != RELEASE_SCHEMA_VERSION
        || metadata.product_id != PRODUCT_ID
        || metadata.release_channel != check.release_channel
        || !is_supported_channel(&metadata.release_channel)
        || metadata
            .build_revision
            .as_deref()
            .is_some_and(|revision| !is_sanitized_revision(revision))
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
    resolve_receipt_at(&PathBuf::from(home).join(RECEIPT_RELATIVE_PATH))
}

fn resolve_receipt_at(path: &Path) -> ReceiptResolution {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return fallback_resolution(None);
        }
        Err(error) => {
            return fallback_resolution(Some(diagnostic(
                io_diagnostic_kind(&error),
                "installation receipt could not be inspected",
            )));
        }
    };

    if let Err(problem) = validate_receipt_path(path, &metadata) {
        return fallback_resolution(Some(problem));
    }
    if metadata.len() > MAX_RECEIPT_BYTES {
        return fallback_resolution(Some(diagnostic(
            "receipt_too_large",
            "installation receipt exceeds the size limit",
        )));
    }

    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return fallback_resolution(Some(diagnostic(
                io_diagnostic_kind(&error),
                "installation receipt could not be read",
            )));
        }
    };
    let receipt: InstallationReceipt = match serde_json::from_slice(&bytes) {
        Ok(receipt) => receipt,
        Err(_) => {
            return fallback_resolution(Some(diagnostic(
                "malformed_receipt",
                "installation receipt is not valid supported JSON",
            )));
        }
    };

    if let Some(problem) = validate_receipt(&receipt) {
        return fallback_resolution(Some(problem));
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

fn validate_receipt(receipt: &InstallationReceipt) -> Option<DaemonInstallationDiagnostic> {
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION {
        return Some(diagnostic(
            "unsupported_receipt_schema",
            "installation receipt schema is unsupported",
        ));
    }
    if receipt.product_id != PRODUCT_ID {
        return Some(diagnostic(
            "receipt_product_mismatch",
            "installation receipt names a different product",
        ));
    }
    if receipt.binary_version != env!("CARGO_PKG_VERSION") {
        return Some(diagnostic(
            "receipt_binary_mismatch",
            "installation receipt does not match the running binary version",
        ));
    }
    if receipt.installation_mode != "managed" {
        return Some(diagnostic(
            "unsupported_installation_mode",
            "installation receipt mode is unsupported",
        ));
    }
    if !is_supported_channel(&receipt.release_channel) {
        return Some(diagnostic(
            "unsupported_release_channel",
            "installation receipt release channel is unsupported",
        ));
    }
    if receipt.provider != "http_json" {
        return Some(diagnostic(
            "unsupported_release_provider",
            "installation receipt provider is unsupported",
        ));
    }
    if let Err(kind) = validate_release_source(&receipt.source_url) {
        return Some(diagnostic(
            kind,
            if kind == "insecure_release_source" {
                "installation receipt release source must use HTTPS or loopback HTTP"
            } else {
                "installation receipt release source is invalid"
            },
        ));
    }
    None
}

fn validate_release_source(source_url: &str) -> Result<(), &'static str> {
    let uri = source_url
        .parse::<ureq::http::Uri>()
        .map_err(|_| "invalid_release_source")?;
    let scheme = uri.scheme_str().ok_or("invalid_release_source")?;
    let host = uri.host().ok_or("invalid_release_source")?;
    match scheme {
        "https" => Ok(()),
        "http" if is_loopback_host(host) => Ok(()),
        "http" => Err("insecure_release_source"),
        _ => Err("invalid_release_source"),
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn validate_receipt_path(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), DaemonInstallationDiagnostic> {
    if metadata.file_type().is_symlink() {
        return Err(diagnostic(
            "receipt_symlink",
            "installation receipt must not be a symbolic link",
        ));
    }
    if !metadata.file_type().is_file() {
        return Err(diagnostic(
            "receipt_not_regular_file",
            "installation receipt must be a regular file",
        ));
    }
    validate_owned_private_metadata(metadata, "receipt")?;

    let mut directory = path.parent();
    for _ in 0..2 {
        let Some(path) = directory else { break };
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            diagnostic(
                io_diagnostic_kind(&error),
                "installation receipt directory could not be inspected",
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(diagnostic(
                "unsafe_receipt_directory",
                "installation receipt directory is not a safe regular directory",
            ));
        }
        validate_owned_private_metadata(&metadata, "receipt_directory")?;
        directory = path.parent();
    }
    Ok(())
}

fn validate_owned_private_metadata(
    metadata: &fs::Metadata,
    subject: &'static str,
) -> Result<(), DaemonInstallationDiagnostic> {
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(diagnostic(
            "receipt_wrong_owner",
            format!("installation {subject} is not owned by the current user"),
        ));
    }
    if metadata.mode() & 0o002 != 0 {
        return Err(diagnostic(
            "receipt_world_writable",
            format!("installation {subject} must not be world-writable"),
        ));
    }
    Ok(())
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

fn io_diagnostic_kind(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::PermissionDenied => "receipt_permission_denied",
        _ => "receipt_io_error",
    }
}

fn is_supported_channel(channel: &str) -> bool {
    matches!(channel, "stable" | "beta" | "nightly")
}

fn is_sanitized_revision(revision: &str) -> bool {
    !revision.is_empty()
        && revision.len() <= 64
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let receipt = root.join(RECEIPT_RELATIVE_PATH);
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
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn valid_receipt() -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "product_id": PRODUCT_ID,
            "binary_version": env!("CARGO_PKG_VERSION"),
            "installation_mode": "managed",
            "release_channel": "stable",
            "provider": "http_json",
            "source_url": "https://releases.example.invalid/botster-hub.json"
        })
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
        let resolution = resolve_receipt_at(&fixture.receipt);
        assert_eq!(resolution.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            resolution.identity.release_channel.as_deref(),
            Some("stable")
        );
        assert!(resolution.identity.diagnostics.is_empty());
        assert!(resolution.receipt.is_some());
    }

    #[test]
    fn malformed_and_mismatched_receipts_never_become_managed() {
        let fixture = Fixture::new();
        fs::write(&fixture.receipt, b"{").expect("write malformed receipt");
        let malformed = resolve_receipt_at(&fixture.receipt);
        assert_ne!(malformed.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(malformed.identity.diagnostics[0].kind, "malformed_receipt");

        let mut mismatched = valid_receipt();
        mismatched["binary_version"] = serde_json::json!("999.0.0");
        fixture.write(mismatched);
        let mismatched = resolve_receipt_at(&fixture.receipt);
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
                "schema_version",
                serde_json::json!(2),
                "unsupported_receipt_schema",
            ),
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
        ] {
            let fixture = Fixture::new();
            let mut receipt = valid_receipt();
            receipt[field] = value;
            fixture.write(receipt);
            let resolution = resolve_receipt_at(&fixture.receipt);
            assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
            assert_eq!(resolution.identity.diagnostics[0].kind, expected);
        }
    }

    #[test]
    fn managed_release_sources_require_https_except_for_explicit_loopback_fixtures() {
        for source in [
            "https://releases.example.invalid/botster-hub.json",
            "http://127.0.0.1:8123/botster-hub.json",
            "http://[::1]:8123/botster-hub.json",
            "http://localhost:8123/botster-hub.json",
        ] {
            assert_eq!(validate_release_source(source), Ok(()), "source={source}");
        }
        assert_eq!(
            validate_release_source("http://192.0.2.10/botster-hub.json"),
            Err("insecure_release_source")
        );
        assert_eq!(
            validate_release_source("file:///tmp/botster-hub.json"),
            Err("invalid_release_source")
        );

        let fixture = Fixture::new();
        let mut receipt = valid_receipt();
        receipt["source_url"] = serde_json::json!("http://192.0.2.10/botster-hub.json");
        fixture.write(receipt);
        let resolution = resolve_receipt_at(&fixture.receipt);
        assert_ne!(resolution.identity.mode, DaemonInstallationMode::Managed);
        assert_eq!(
            resolution.identity.diagnostics[0].kind,
            "insecure_release_source"
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
        let world_writable = resolve_receipt_at(&fixture.receipt);
        assert_eq!(
            world_writable.identity.diagnostics[0].kind,
            "receipt_world_writable"
        );

        fs::remove_file(&fixture.receipt).expect("remove receipt");
        let target = fixture.root.join("target.json");
        fs::write(&target, serde_json::to_vec(&valid_receipt()).expect("json"))
            .expect("write target");
        symlink(&target, &fixture.receipt).expect("symlink receipt");
        let symlinked = resolve_receipt_at(&fixture.receipt);
        assert_eq!(symlinked.identity.diagnostics[0].kind, "receipt_symlink");
    }

    #[test]
    fn non_regular_permission_denied_and_world_writable_directory_receipts_are_rejected() {
        let fixture = Fixture::new();
        fs::remove_file(&fixture.receipt).ok();
        fs::create_dir(&fixture.receipt).expect("create receipt directory");
        let non_regular = resolve_receipt_at(&fixture.receipt);
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
        let denied = resolve_receipt_at(&fixture.receipt);
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
        let unsafe_directory = resolve_receipt_at(&fixture.receipt);
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
            schema_version: 1,
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
