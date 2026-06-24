//! Profile-owned durable hub state boundary.
//!
//! The hub persists product and policy state here while `botster-core` remains
//! the owner of reusable session, transport, package, and admission mechanics.
//! Version 1 is a single local JSON file intended for local dogfood. It is a
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
use crate::packages::PackageRegistrySnapshot;

const HUB_STATE_SCHEMA_VERSION: u16 = 1;
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
    /// Version of this JSON schema. Version 1 has no older migrations.
    pub schema_version: u16,
    /// Host identity metadata resolved from hub config.
    pub host: HostIdentity,
    /// Config/schema metadata needed by future migrations.
    pub schema: SchemaMetadata,
    /// Package/provider registry records, grants, pins, provenance, and enabled state.
    pub package_registry: PackageRegistrySnapshot,
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
    /// Load existing state or create a v1 default from config when no file exists.
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
                let state: HubState =
                    serde_json::from_slice(&bytes).map_err(HubStateStoreError::Corrupt)?;
                state
                    .validate_version()
                    .map_err(HubStateStoreError::State)?;
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use botster_core::{
        Capability, CapabilitySurface, ExtensionEntrypoint, ExtensionKind, ExtensionRuntime,
        PackageManifest, PackageSource,
    };

    use super::*;
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubStartupOptions, PackageProvenance,
        PackageRegistry, PackageRunnableEntrypoint, PackageRunnableEntrypointKind,
        PackageRunnableMode, PackageRunnableProcessState, PackageRunnableWorkingDirectory,
        RuntimeEnvironment,
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
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
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

    fn plugin_manifest() -> PackageManifest {
        PackageManifest {
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
            host_profile: None,
            configuration: None,
            surfaces: Vec::new(),
        }
    }

    fn provenance() -> PackageProvenance {
        PackageProvenance {
            source: "https://example.invalid/botster/package-index".to_string(),
            checksum: Some("sha256:test-checksum".to_string()),
        }
    }

    #[test]
    fn file_store_creates_and_loads_default_v1_state() {
        let config = test_config("creates-default");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);

        let state = store
            .load_or_initialize(&config)
            .expect("initialize default state");
        let reopened = store
            .load_or_initialize(&config)
            .expect("load committed state");

        assert_eq!(state.schema_version, 1);
        assert_eq!(reopened, state);
        assert_eq!(reopened.host.id, "state-test-host");
        assert_eq!(
            reopened.runtime_settings.data_directory,
            config.data_directory
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
        registry
            .install(plugin_manifest(), provenance(), "install synthetic package")
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
        assert!(reopened.package_registry.records[0].is_enabled());
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
            kind: PackageRunnableEntrypointKind::Web,
            command: "bin/botster-web".to_string(),
            args: vec!["--host".to_string(), "127.0.0.1".to_string()],
            working_directory: PackageRunnableWorkingDirectory::PackageRoot,
            environment: Vec::new(),
            mode: PackageRunnableMode::Dev,
            capabilities: Vec::new(),
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
