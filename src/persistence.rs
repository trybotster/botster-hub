//! Profile-owned durable hub state boundary.
//!
//! The hub persists product and policy state here while `botster-core` remains
//! the owner of reusable session, transport, package, and admission mechanics.
//! Version 2 is a single local JSON file intended for the local runtime. It is a
//! single-writer store: atomic rename keeps the previous committed file intact
//! when a write fails before rename, but concurrent hub processes can still
//! produce last-writer-wins updates.

use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use botster_core::{Capability, CapabilitySurface};
use serde::{Deserialize, Serialize};

use crate::config::{
    CoreEngineOptions, HostIdentity, HubConfig, SessionDefaults, TransportBindings,
};
use crate::credentials::{CredentialKeyPurpose, CredentialProviderKind};
use crate::packages::PackageRegistrySnapshot;
use crate::session_templates::PackageSessionTemplate;
use crate::spawn_targets::SpawnTarget;
use crate::worktrees::Worktree;

const HUB_STATE_SCHEMA_VERSION: u16 = 2;
const HUB_STATE_FILE_NAME: &str = "hub-state.json";
const HUB_STATE_TEMP_FILE_NAME: &str = "hub-state.json.tmp";

/// Persistence buckets the host profile must govern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceBucket {
    /// Durable host and admission state.
    HostState,
    /// Installed package metadata, pins, provenance, and enabled state.
    PackageState,
    /// Provider-owned runtime metadata admitted by hub policy.
    ProviderState,
}

/// Versioned durable hub state aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubState {
    /// Version of this JSON schema. Version 2 rejects older schemas without migration.
    pub schema_version: u16,
    /// Host identity metadata resolved from hub config.
    pub host: HostIdentity,
    /// Config/schema metadata needed by future migrations.
    pub schema: SchemaMetadata,
    /// Package/provider registry records, grants, pins, provenance, and enabled state.
    pub package_registry: PackageRegistrySnapshot,
    /// Device-owned session template sources persisted by the hub profile.
    #[serde(default)]
    pub device_session_template_sources: Vec<DeviceSessionTemplateSource>,
    /// Hub-owned spawn targets admitted for client/plugin references.
    #[serde(default, alias = "admitted_session_template_targets")]
    pub spawn_targets: Vec<SpawnTarget>,
    /// Hub-owned worktree records scoped to admitted spawn targets.
    #[serde(default)]
    pub worktrees: Vec<Worktree>,
    /// References to secret material held by the credential provider.
    #[serde(default)]
    pub credential_keys: Vec<CredentialKeyReference>,
    /// Durable trusted browser public identity metadata. No private keys or grant secrets.
    #[serde(default)]
    pub trusted_browser_identities: Vec<TrustedBrowserIdentity>,
    /// Bootstrap grant metadata. Grant secret material, when durable, is provider-owned.
    #[serde(default)]
    pub bootstrap_grants: Vec<BootstrapGrantRecord>,
    /// Audit-friendly capability grant records.
    pub capability_grants: Vec<CapabilityGrantRecord>,
    /// Admission decision history for package/provider policy.
    pub admission_decisions: Vec<PackageAdmissionDecision>,
    /// Local runtime settings derived from current hub config.
    pub runtime_settings: LocalRuntimeSettings,
    /// Append-only operator decision history.
    pub audit_history: Vec<HubAuditEntry>,
}

impl HubState {
    /// Build an empty durable state aggregate from resolved runtime config.
    #[must_use]
    pub fn from_config(config: &HubConfig) -> Self {
        Self {
            schema_version: HUB_STATE_SCHEMA_VERSION,
            host: config.host.clone(),
            schema: SchemaMetadata::v1(),
            package_registry: PackageRegistrySnapshot::empty(),
            device_session_template_sources: Vec::new(),
            spawn_targets: Vec::new(),
            worktrees: Vec::new(),
            credential_keys: Vec::new(),
            trusted_browser_identities: Vec::new(),
            bootstrap_grants: Vec::new(),
            capability_grants: Vec::new(),
            admission_decisions: Vec::new(),
            runtime_settings: LocalRuntimeSettings::from_config(config),
            audit_history: Vec::new(),
        }
    }

    fn validate_version(&self) -> HubStateResult<()> {
        if self.schema_version == HUB_STATE_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(HubStateError::UnsupportedVersion(self.schema_version))
        }
    }
}

/// One durable device-level session template source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSessionTemplateSource {
    /// Root used to resolve relative template command and cwd policy paths.
    pub root: PathBuf,
    /// Device-owned template declarations.
    #[serde(default)]
    pub session_templates: Vec<PackageSessionTemplate>,
}

/// Reference to credential-provider-owned secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialKeyReference {
    /// Stable provider key id. The referenced credential value is never in hub-state.
    pub key_id: String,
    /// Concrete provider expected to hold this credential.
    pub provider: CredentialProviderKind,
    /// Hub-owned purpose for audit and lookup policy.
    pub purpose: CredentialKeyPurpose,
    /// Logical creation time supplied by the caller.
    pub created_at_unix_ms: u64,
    /// Optional rotation time supplied by the caller.
    pub rotated_at_unix_ms: Option<u64>,
}

/// Durable public browser identity trust metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedBrowserIdentity {
    /// Synthetic browser identity id. Do not use hostnames, emails, or local paths.
    pub browser_id: String,
    /// Public verifying key bytes. Private key material is credential-provider-owned.
    pub public_key: Vec<u8>,
    /// Public-key-derived fingerprint.
    pub fingerprint: String,
    /// Optional credential key reference for browser-owned secret/session material.
    pub credential_key_id: Option<String>,
    /// Time this browser identity became trusted.
    pub trusted_at_unix_ms: u64,
    /// Optional trust expiry.
    pub expires_at_unix_ms: Option<u64>,
    /// Revocation time when trust has been revoked.
    pub revoked_at_unix_ms: Option<u64>,
    /// Audit-safe reason. Callers must not put PII or secrets here.
    pub audit_reason: String,
}

/// Durable bootstrap grant metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapGrantRecord {
    /// Synthetic grant id. The raw grant token is never in hub-state.
    pub grant_id: String,
    /// Package or app instance this grant is scoped to.
    pub package_instance_id: String,
    /// Expected local origin label, such as `localhost`.
    pub origin: String,
    /// Expected peer/session route id.
    pub peer_id: String,
    /// Optional credential key reference for sealed grant material.
    pub credential_key_id: Option<String>,
    /// Grant expiry time.
    pub expires_at_unix_ms: u64,
    /// Revocation time when the grant has been revoked.
    pub revoked_at_unix_ms: Option<u64>,
    /// Redemption time when the grant has been consumed.
    pub redeemed_at_unix_ms: Option<u64>,
    /// Audit-safe reason. Callers must not put PII or secrets here.
    pub audit_reason: String,
}

/// Schema and migration metadata recorded inside the state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// Human-readable config schema generation owned by the hub.
    pub hub_config_version: u16,
    /// Future migration hook; v1 has no prior version.
    pub migrated_from: Option<u16>,
}

impl SchemaMetadata {
    /// Current v1 schema metadata.
    #[must_use]
    pub const fn v1() -> Self {
        Self {
            hub_config_version: 1,
            migrated_from: None,
        }
    }
}

/// One hub-owned capability grant snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityGrantRecord {
    /// Package or provider key receiving the grant.
    pub subject: String,
    /// Granted capability.
    pub capability: Capability,
    /// Surface governed by the hub host profile for audit filtering.
    pub governed_surface: CapabilitySurface,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub audit_reason: String,
}

/// One persisted package/provider admission decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageAdmissionDecision {
    /// Package or provider name.
    pub package_name: String,
    /// Decision action such as install, enable, disable, or pin.
    pub action: String,
    /// Decision result such as accepted or denied.
    pub outcome: String,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub audit_reason: String,
}

/// Local runtime settings expected to survive restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalRuntimeSettings {
    /// Resolved data directory used by this state file.
    pub data_directory: PathBuf,
    /// Session defaults resolved for the hub.
    pub session_defaults: SessionDefaults,
    /// Plugin directories resolved from config.
    pub plugin_directories: Vec<PathBuf>,
    /// Provider directories resolved from config.
    pub provider_directories: Vec<PathBuf>,
    /// Transport bindings resolved from config.
    pub transports: TransportBindings,
    /// Hub policy for core-owned engine knobs.
    pub core_engine: CoreEngineOptions,
}

impl LocalRuntimeSettings {
    /// Snapshot local runtime settings from resolved config.
    #[must_use]
    pub fn from_config(config: &HubConfig) -> Self {
        Self {
            data_directory: config.data_directory.clone(),
            session_defaults: config.session_defaults.clone(),
            plugin_directories: config.plugin_directories.clone(),
            provider_directories: config.provider_directories.clone(),
            transports: config.transports.clone(),
            core_engine: config.core_engine.clone(),
        }
    }
}

/// One append-only audit entry in the v1 state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubAuditEntry {
    /// Logical or wall-clock timestamp supplied by the caller.
    pub recorded_at: String,
    /// Actor label supplied by the caller.
    pub actor: String,
    /// Action label supplied by the caller.
    pub action: String,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub reason: String,
}

/// Typed hub state model errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubStateError {
    /// The state file uses a future or unsupported schema version.
    UnsupportedVersion(u16),
}

impl fmt::Display for HubStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported hub state schema version {version}")
            }
        }
    }
}

impl Error for HubStateError {}

/// Hub state model result alias.
pub type HubStateResult<T> = Result<T, HubStateError>;

/// Storage boundary for durable hub state.
pub trait HubStateStore {
    /// Load existing state or create a v2 default from config when no file exists.
    fn load_or_initialize(&self, config: &HubConfig) -> HubStateStoreResult<HubState>;

    /// Save a complete state snapshot.
    fn save(&self, state: &HubState) -> HubStateStoreResult<()>;

    /// Load, mutate, save, and return the committed state.
    fn update(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState> {
        let mut state = self.load_or_initialize(config)?;
        update(&mut state);
        self.save(&state)?;
        Ok(state)
    }
}

/// Local-first file-backed implementation of durable hub state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHubStateStore {
    path: PathBuf,
    temporary_path: PathBuf,
}

impl FileHubStateStore {
    /// Build a store at `<data_directory>/hub-state.json`.
    #[must_use]
    pub fn for_data_directory(data_directory: impl AsRef<Path>) -> Self {
        let data_directory = data_directory.as_ref();
        Self {
            path: data_directory.join(HUB_STATE_FILE_NAME),
            temporary_path: data_directory.join(HUB_STATE_TEMP_FILE_NAME),
        }
    }

    /// Return the JSON state file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_atomically(&self, state: &HubState) -> HubStateStoreResult<()> {
        self.write_temporary_file(state)?;
        fs::rename(&self.temporary_path, &self.path).map_err(HubStateStoreError::Io)?;
        Ok(())
    }

    fn write_temporary_file(&self, state: &HubState) -> HubStateStoreResult<()> {
        let parent = self
            .path
            .parent()
            .ok_or(HubStateStoreError::MissingParent)?;
        fs::create_dir_all(parent).map_err(HubStateStoreError::Io)?;

        let bytes = serde_json::to_vec_pretty(state).map_err(HubStateStoreError::Serialize)?;
        let mut temporary = File::create(&self.temporary_path).map_err(HubStateStoreError::Io)?;
        temporary
            .write_all(&bytes)
            .map_err(HubStateStoreError::Io)?;
        temporary.sync_all().map_err(HubStateStoreError::Io)?;
        Ok(())
    }

    #[cfg(test)]
    fn save_with_injected_failure(&self, state: &HubState) -> HubStateStoreResult<()> {
        self.write_temporary_file(state)?;
        Err(HubStateStoreError::InjectedWriteFailure)
    }
}

impl HubStateStore for FileHubStateStore {
    fn load_or_initialize(&self, config: &HubConfig) -> HubStateStoreResult<HubState> {
        match fs::read(&self.path) {
            Ok(bytes) => {
                let version: HubStateVersion =
                    serde_json::from_slice(&bytes).map_err(HubStateStoreError::Corrupt)?;
                if version.schema_version != HUB_STATE_SCHEMA_VERSION {
                    return Err(HubStateStoreError::State(
                        HubStateError::UnsupportedVersion(version.schema_version),
                    ));
                }
                let state: HubState =
                    serde_json::from_slice(&bytes).map_err(HubStateStoreError::Corrupt)?;
                Ok(state)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let state = HubState::from_config(config);
                self.save(&state)?;
                Ok(state)
            }
            Err(error) => Err(HubStateStoreError::Io(error)),
        }
    }

    fn save(&self, state: &HubState) -> HubStateStoreResult<()> {
        state
            .validate_version()
            .map_err(HubStateStoreError::State)?;
        self.write_atomically(state)
    }
}

#[derive(Debug, Deserialize)]
struct HubStateVersion {
    schema_version: u16,
}

/// Typed storage boundary errors.
#[derive(Debug)]
pub enum HubStateStoreError {
    /// State file path did not have a parent directory.
    MissingParent,
    /// Filesystem error while reading or writing durable state.
    Io(io::Error),
    /// JSON serialization failed before writing state.
    Serialize(serde_json::Error),
    /// JSON parsing failed while loading the committed state file.
    Corrupt(serde_json::Error),
    /// Loaded or saved state failed model validation.
    State(HubStateError),
    /// Test-only injected failure between temp-file flush and rename.
    #[cfg(test)]
    InjectedWriteFailure,
}

impl fmt::Display for HubStateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingParent => write!(formatter, "hub state file has no parent directory"),
            Self::Io(error) => write!(formatter, "hub state filesystem error: {error}"),
            Self::Serialize(error) => write!(formatter, "hub state serialization error: {error}"),
            Self::Corrupt(error) => write!(formatter, "hub state file is corrupt: {error}"),
            Self::State(error) => write!(formatter, "{error}"),
            #[cfg(test)]
            Self::InjectedWriteFailure => {
                write!(formatter, "injected hub state write failure before rename")
            }
        }
    }
}

impl Error for HubStateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialize(error) | Self::Corrupt(error) => Some(error),
            Self::State(error) => Some(error),
            #[cfg(test)]
            Self::MissingParent | Self::InjectedWriteFailure => None,
            #[cfg(not(test))]
            Self::MissingParent => None,
        }
    }
}

/// Hub state storage result alias.
pub type HubStateStoreResult<T> = Result<T, HubStateStoreError>;

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use botster_core::{
        Capability, CapabilitySurface, CredentialRecord, CredentialStore, CredentialStoreError,
        ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, PackageConfigurationField,
        PackageConfigurationFieldType, PackageConfigurationSchema, PackageConfigurationSecretValue,
        PackageConfigurationValue, PackageSource,
    };

    use super::*;
    use crate::credentials::{
        CredentialKeyPurpose, CredentialProviderKind, TestFileCredentialStore, credential_key_id,
        validate_hub_credentials,
    };
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubPackageManifest, HubStartupOptions,
        PackageProvenance, PackageRegistry, PackageRunnableEntrypoint, PackageRunnableProcessState,
        PackageRunnableWorkingDirectory, RuntimeEnvironment,
    };

    fn test_config(name: &str) -> HubConfig {
        HubStartupOptions {
            host: HostIdentityOptions {
                id: "state-test-host".to_string(),
                display_name: "State Test Host".to_string(),
                fingerprint: None,
            },
            data_directory: DataDirectoryOption::Explicit(unique_test_dir(name)),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build test config")
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        PathBuf::from("target")
            .join("botster-hub-test-data")
            .join(name)
            .join(nanos.to_string())
    }

    fn plugin_manifest() -> HubPackageManifest {
        HubPackageManifest {
            name: "workflow.plugin".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Plugin,
            botster: ">=0.1.0".to_string(),
            source: Some(PackageSource::Git {
                repo: "https://example.invalid/botster/workflow-plugin.git".to_string(),
                reference: "v1.0.0".to_string(),
            }),
            capabilities: vec![Capability {
                surface: CapabilitySurface::Surfaces,
                scope: None,
            }],
            entrypoints: vec![ExtensionEntrypoint {
                runtime: ExtensionRuntime::Lua,
                path: "plugin.lua".to_string(),
                bootstrap: false,
            }],
            dependencies: Vec::new(),
            features: Vec::new(),
            configuration: None,
            host_profile: None,
            surfaces: Vec::new(),
            runnable_entrypoints: Vec::new(),
            navigation: Vec::new(),
        }
    }

    fn provenance() -> PackageProvenance {
        PackageProvenance {
            source: "https://example.invalid/botster/package-index".to_string(),
            checksum: Some("sha256:test-checksum".to_string()),
        }
    }

    #[derive(Debug, Default)]
    struct CountingCredentialStore {
        reads: Cell<usize>,
    }

    impl CredentialStore for CountingCredentialStore {
        fn get(&self, _key: &str) -> Result<Option<CredentialRecord>, CredentialStoreError> {
            self.reads.set(self.reads.get() + 1);
            Ok(Some(CredentialRecord::new(vec![41, 43, 47, 53])))
        }

        fn set(
            &mut self,
            _key: &str,
            _record: CredentialRecord,
        ) -> Result<(), CredentialStoreError> {
            Ok(())
        }

        fn delete(&mut self, _key: &str) -> Result<(), CredentialStoreError> {
            Ok(())
        }
    }

    fn configurable_plugin_manifest() -> HubPackageManifest {
        let mut manifest = plugin_manifest();
        manifest.configuration = Some(PackageConfigurationSchema {
            groups: Vec::new(),
            fields: vec![PackageConfigurationField {
                key: "api_token".to_string(),
                field_type: PackageConfigurationFieldType::Secret,
                label: "API token".to_string(),
                description: None,
                required: true,
                default: Some(PackageConfigurationValue::Secret {
                    state: PackageConfigurationSecretValue::Unset,
                }),
                validation: None,
                group: None,
                order: None,
                options: Vec::new(),
            }],
        });
        manifest
    }

    #[test]
    fn file_store_creates_and_loads_default_v2_state() {
        let config = test_config("creates-default");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);

        let state = store
            .load_or_initialize(&config)
            .expect("initialize default state");
        let reopened = store
            .load_or_initialize(&config)
            .expect("load committed state");

        assert_eq!(state.schema_version, 2);
        assert_eq!(reopened, state);
        assert_eq!(reopened.host.id, "state-test-host");
        assert_eq!(
            reopened.runtime_settings.data_directory,
            config.data_directory
        );
    }

    #[test]
    fn file_store_loads_v2_state_without_session_template_source_fields() {
        let config = test_config("loads-v2-without-session-template-sources");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state = HubState::from_config(&config);
        let mut value = serde_json::to_value(&state).expect("serialize state value");
        let object = value.as_object_mut().expect("state serializes as object");
        object.remove("device_session_template_sources");
        object.insert(
            "spawn_targets".to_string(),
            serde_json::json!([{
                "target_id": "legacy-target",
                "root": "."
            }]),
        );
        object.insert(
            "worktrees".to_string(),
            serde_json::json!([{
                "worktree_id": "legacy-worktree",
                "target_id": "legacy-target",
                "path": "."
            }]),
        );
        object.remove("credential_keys");
        object.remove("trusted_browser_identities");
        object.remove("bootstrap_grants");
        fs::create_dir_all(&config.data_directory).expect("create data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&value).expect("serialize legacy-shaped state"),
        )
        .expect("write legacy-shaped state");

        let reopened = store
            .load_or_initialize(&config)
            .expect("load legacy-shaped v2 state");

        assert!(reopened.device_session_template_sources.is_empty());
        assert_eq!(reopened.spawn_targets.len(), 1);
        assert_eq!(reopened.spawn_targets[0].kind, "directory");
        assert_eq!(reopened.spawn_targets[0].base_ref, None);
        assert_eq!(reopened.worktrees.len(), 1);
        assert_eq!(reopened.worktrees[0].management, "registered");
        assert!(reopened.credential_keys.is_empty());
        assert!(reopened.trusted_browser_identities.is_empty());
        assert!(reopened.bootstrap_grants.is_empty());
        assert_eq!(reopened.schema_version, 2);
    }

    #[test]
    fn file_store_rejects_v1_before_deserializing_legacy_core_engine_options() {
        let config = test_config("rejects-v1-core-engine-options");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let mut value =
            serde_json::to_value(HubState::from_config(&config)).expect("serialize current state");
        let object = value.as_object_mut().expect("state object");
        object.insert("schema_version".to_string(), serde_json::json!(1));
        let core_engine = object
            .get_mut("runtime_settings")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|runtime_settings| runtime_settings.get_mut("core_engine"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("core engine object");
        let queue_capacity = core_engine
            .remove("plugin_worker_queue_capacity")
            .expect("current queue capacity");
        core_engine.remove("plugin_worker_executor_concurrency");
        core_engine.insert("plugin_worker_capacity".to_string(), queue_capacity);
        fs::create_dir_all(&config.data_directory).expect("create data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&value).expect("serialize v1 state"),
        )
        .expect("write v1 state");

        assert!(matches!(
            store.load_or_initialize(&config),
            Err(HubStateStoreError::State(
                HubStateError::UnsupportedVersion(1)
            ))
        ));
    }

    #[test]
    fn browser_trust_metadata_survives_restart_without_raw_secret_material() {
        let config = test_config("browser-trust-metadata");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let public_key = b"synthetic browser public key".to_vec();
        let key_id = credential_key_id(
            &config.host.id,
            CredentialKeyPurpose::BrowserIdentity,
            "browser-a",
        );

        store
            .update(&config, |state| {
                state.credential_keys.push(CredentialKeyReference {
                    key_id: key_id.clone(),
                    provider: CredentialProviderKind::TestFile,
                    purpose: CredentialKeyPurpose::BrowserIdentity,
                    created_at_unix_ms: 10,
                    rotated_at_unix_ms: None,
                });
                let mut browser = TrustedBrowserIdentity::trusted(
                    "browser-a",
                    public_key.clone(),
                    11,
                    "trust synthetic browser",
                );
                browser.credential_key_id = Some(key_id.clone());
                state.trusted_browser_identities.push(browser);
                state.bootstrap_grants.push(BootstrapGrantRecord {
                    grant_id: "grant-a".to_string(),
                    package_instance_id: "package-instance-a".to_string(),
                    origin: "localhost".to_string(),
                    peer_id: "peer-a".to_string(),
                    credential_key_id: Some(key_id.clone()),
                    expires_at_unix_ms: 100,
                    revoked_at_unix_ms: None,
                    redeemed_at_unix_ms: None,
                    audit_reason: "issue synthetic bootstrap grant".to_string(),
                });
            })
            .expect("persist browser trust state");

        let mut credential_store =
            TestFileCredentialStore::new(config.data_directory.join("test-credentials.json"));
        credential_store
            .set(&key_id, CredentialRecord::new(vec![7, 11, 13, 17]))
            .expect("persist test credential outside hub-state");

        let raw_state = fs::read_to_string(store.path()).expect("read hub state");
        assert!(raw_state.contains("browser-a"));
        assert!(raw_state.contains(&key_id));
        assert!(!raw_state.contains("[7,11,13,17]"));
        assert!(!raw_state.contains("[7, 11, 13, 17]"));
        assert!(!raw_state.contains("grant-token"));
        assert!(!raw_state.contains("private key"));
        assert!(!raw_state.contains("write_only"));
        assert!(!raw_state.contains(concat!("/", "Users", "/")));
        assert!(!raw_state.contains("@example.com"));

        let reopened = FileHubStateStore::for_data_directory(&config.data_directory)
            .load_or_initialize(&config)
            .expect("load browser trust metadata");
        assert_eq!(reopened.credential_keys.len(), 1);
        assert_eq!(reopened.trusted_browser_identities.len(), 1);
        assert_eq!(reopened.bootstrap_grants.len(), 1);
        assert!(reopened.trusted_browser_identities[0].is_trusted_at(50));
        assert!(reopened.bootstrap_grants[0].is_redeemable_at(50));
        validate_hub_credentials(
            &reopened,
            CredentialProviderKind::TestFile,
            &credential_store,
        )
        .expect("credential references resolve through explicit test store");
    }

    #[test]
    fn revoked_or_expired_browser_identities_and_grants_are_denied() {
        let public_key = b"revoked browser public key".to_vec();
        let mut revoked = TrustedBrowserIdentity::trusted(
            "browser-revoked",
            public_key.clone(),
            10,
            "trust synthetic browser",
        );
        revoked.revoked_at_unix_ms = Some(20);
        assert!(!revoked.is_trusted_at(30));

        let mut expired = TrustedBrowserIdentity::trusted(
            "browser-expired",
            public_key,
            10,
            "trust synthetic browser",
        );
        expired.expires_at_unix_ms = Some(20);
        assert!(!expired.is_trusted_at(20));
        assert!(!expired.is_trusted_at(21));

        let valid = BootstrapGrantRecord {
            grant_id: "grant-valid".to_string(),
            package_instance_id: "package-instance".to_string(),
            origin: "localhost".to_string(),
            peer_id: "peer".to_string(),
            credential_key_id: None,
            expires_at_unix_ms: 30,
            revoked_at_unix_ms: None,
            redeemed_at_unix_ms: None,
            audit_reason: "grant synthetic local bootstrap".to_string(),
        };
        assert!(valid.is_redeemable_at(29));

        let mut redeemed = valid.clone();
        redeemed.redeemed_at_unix_ms = Some(25);
        assert!(!redeemed.is_redeemable_at(26));

        let mut revoked_grant = valid.clone();
        revoked_grant.revoked_at_unix_ms = Some(25);
        assert!(!revoked_grant.is_redeemable_at(26));
        assert!(!valid.is_redeemable_at(30));
    }

    #[test]
    fn browser_and_grant_references_do_not_re_read_validated_credential_keys() {
        let config = test_config("browser-grant-no-duplicate-reads");
        let key_id = credential_key_id(
            &config.host.id,
            CredentialKeyPurpose::BrowserIdentity,
            "browser-a",
        );
        let public_key = b"browser reference public key".to_vec();
        let mut state = HubState::from_config(&config);
        state.credential_keys.push(CredentialKeyReference {
            key_id: key_id.clone(),
            provider: CredentialProviderKind::TestFile,
            purpose: CredentialKeyPurpose::BrowserIdentity,
            created_at_unix_ms: 10,
            rotated_at_unix_ms: None,
        });
        let mut browser =
            TrustedBrowserIdentity::trusted("browser-a", public_key, 10, "trust synthetic browser");
        browser.credential_key_id = Some(key_id.clone());
        state.trusted_browser_identities.push(browser);
        state.bootstrap_grants.push(BootstrapGrantRecord {
            grant_id: "grant-a".to_string(),
            package_instance_id: "package-instance-a".to_string(),
            origin: "localhost".to_string(),
            peer_id: "peer-a".to_string(),
            credential_key_id: Some(key_id),
            expires_at_unix_ms: 100,
            revoked_at_unix_ms: None,
            redeemed_at_unix_ms: None,
            audit_reason: "issue synthetic bootstrap grant".to_string(),
        });
        let credential_store = CountingCredentialStore::default();

        validate_hub_credentials(&state, CredentialProviderKind::TestFile, &credential_store)
            .expect("state credential references should validate");

        assert_eq!(
            credential_store.reads.get(),
            1,
            "one credential key should be read once even when browser and grant reference it"
        );
    }

    #[test]
    fn file_store_persists_package_registry_and_capability_grants_across_reopen() {
        let config = test_config("registry-grants");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant.clone()].into_iter().collect());
        let mut manifest = plugin_manifest();
        manifest.surfaces = vec![botster_ui_contract::PackageSurfaceDescriptor {
            id: "workflow.home".to_string(),
            kind: botster_ui_contract::PackageSurfaceKind::App,
            title: "Workflow".to_string(),
            description: None,
            icon: None,
            order: None,
            category: None,
            supports: vec![botster_ui_contract::PackageSurfaceOperation::Render],
        }];
        manifest.navigation = vec![botster_ui_contract::PackageNavigationEntry {
            id: "workflow.home".to_string(),
            label: "Workflow".to_string(),
            icon: None,
            description: None,
            target: botster_ui_contract::PackageNavigationTarget::Surface {
                surface_id: "workflow.home".to_string(),
            },
        }];
        registry
            .install(manifest, provenance(), "install synthetic package")
            .expect("install package");
        registry
            .enable("workflow.plugin", "enable synthetic package")
            .expect("enable package");

        store
            .update(&config, |state| {
                state.package_registry = registry.snapshot();
                state.capability_grants.push(CapabilityGrantRecord {
                    subject: "workflow.plugin".to_string(),
                    capability: grant.clone(),
                    governed_surface: CapabilitySurface::Surfaces,
                    audit_reason: "grant synthetic surface capability".to_string(),
                });
            })
            .expect("persist registry state");

        let reopened_store = FileHubStateStore::for_data_directory(&config.data_directory);
        let reopened = reopened_store
            .load_or_initialize(&config)
            .expect("load registry state");

        assert_eq!(reopened.package_registry.records.len(), 1);
        assert_eq!(reopened.schema_version, HUB_STATE_SCHEMA_VERSION);
        assert!(reopened.package_registry.records[0].is_enabled());
        assert_eq!(
            reopened.package_registry.records[0].manifest.surfaces[0].id,
            "workflow.home"
        );
        assert_eq!(
            reopened.package_registry.records[0].manifest.navigation[0].id,
            "workflow.home"
        );
        assert_eq!(reopened.capability_grants.len(), 1);
        assert_eq!(
            reopened.package_registry.granted_capabilities,
            vec![Capability {
                surface: CapabilitySurface::Surfaces,
                scope: None,
            }]
        );
    }

    #[test]
    fn package_configuration_redacted_secret_marker_persists_without_raw_secret() {
        let config = test_config("package-configuration-redaction");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant].into_iter().collect());
        registry
            .install(
                configurable_plugin_manifest(),
                provenance(),
                "install configurable package",
            )
            .expect("install package");
        registry
            .set_configuration(
                "workflow.plugin",
                BTreeMap::from([(
                    "api_token".to_string(),
                    PackageConfigurationValue::Secret {
                        state: PackageConfigurationSecretValue::WriteOnly,
                    },
                )]),
                "set secret",
            )
            .expect("set package configuration");

        store
            .update(&config, |state| {
                state.package_registry = registry.snapshot();
            })
            .expect("persist state");

        let raw_state = fs::read_to_string(store.path()).expect("read hub state");
        assert!(raw_state.contains("\"state\": \"redacted\""));
        assert!(!raw_state.contains("write_only"));
        assert!(!raw_state.contains("super-secret-token"));

        let reopened = store.load_or_initialize(&config).expect("reopen state");
        let restored =
            PackageRegistry::from_snapshot(reopened.package_registry).expect("restore registry");
        let view = restored
            .package("workflow.plugin")
            .expect("restored package")
            .configuration_view();
        assert!(matches!(
            view.effective_values.get("api_token"),
            Some(PackageConfigurationValue::Secret {
                state: PackageConfigurationSecretValue::Redacted
            })
        ));
    }

    #[test]
    fn file_store_persists_runnable_entrypoints_in_package_registry() {
        let config = test_config("registry-runnable-entrypoints");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant].into_iter().collect());
        registry
            .install(plugin_manifest(), provenance(), "install synthetic package")
            .expect("install package");
        let mut snapshot = registry.snapshot();
        snapshot.records[0].runnable_entrypoints = vec![PackageRunnableEntrypoint {
            id: "web".to_string(),
            kind: botster_core::RunnableEntrypointKind::WebApp,
            launch_mode: botster_core::RunnableEntrypointLaunchMode::Background,
            command: "bin/botster-web".to_string(),
            args: vec!["--host".to_string(), "127.0.0.1".to_string()],
            working_directory: PackageRunnableWorkingDirectory::PackageRoot,
            injections: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            readiness: None,
            may_supervise: true,
            process: Default::default(),
        }];

        store
            .update(&config, |state| {
                state.package_registry = snapshot;
            })
            .expect("persist runnable entrypoint state");

        let reopened = FileHubStateStore::for_data_directory(&config.data_directory)
            .load_or_initialize(&config)
            .expect("load runnable entrypoint state");
        let entrypoint = &reopened.package_registry.records[0].runnable_entrypoints[0];

        assert_eq!(entrypoint.id, "web");
        assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
        assert!(entrypoint.may_supervise);
        assert_eq!(
            entrypoint.process.state,
            PackageRunnableProcessState::NotStarted
        );
    }

    #[test]
    fn file_store_updates_state_atomically() {
        let config = test_config("atomic");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let original = store
            .load_or_initialize(&config)
            .expect("initialize original state");
        let mut next = original.clone();
        next.audit_history.push(HubAuditEntry {
            recorded_at: "2026-01-01T00:00:00Z".to_string(),
            actor: "test-operator".to_string(),
            action: "mutate".to_string(),
            reason: "prove interrupted write preserves old state".to_string(),
        });

        let error = store
            .save_with_injected_failure(&next)
            .expect_err("injected failure should stop before rename");
        assert!(matches!(error, HubStateStoreError::InjectedWriteFailure));

        let reopened = store
            .load_or_initialize(&config)
            .expect("old state still loads after interrupted write");
        assert_eq!(reopened, original);
    }

    #[test]
    fn file_store_rejects_corrupt_state_file() {
        let config = test_config("corrupt");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        fs::create_dir_all(&config.data_directory).expect("create test data dir");
        fs::write(store.path(), b"{not json").expect("write corrupt state");

        let error = store
            .load_or_initialize(&config)
            .expect_err("corrupt state should fail");

        assert!(matches!(error, HubStateStoreError::Corrupt(_)));
    }

    #[test]
    fn file_store_rejects_unknown_schema_version() {
        let config = test_config("unknown-version");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let mut state = HubState::from_config(&config);
        state.schema_version = 99;
        fs::create_dir_all(&config.data_directory).expect("create test data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&state).expect("serialize unsupported state"),
        )
        .expect("write unsupported state");

        let error = store
            .load_or_initialize(&config)
            .expect_err("unsupported version should fail");

        assert!(matches!(
            error,
            HubStateStoreError::State(HubStateError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn docs_and_fixture_state_do_not_contain_pii_markers() {
        let config = test_config("pii");
        let state = HubState::from_config(&config);
        let json = serde_json::to_string(&state).expect("serialize state");

        assert!(!json.contains(concat!("/", "Users", "/")));
        assert!(!json.contains("@example.com"));
        assert!(!json.contains("/home/"));
    }
}
