//! Deterministic local hub daemon lifecycle over the durable state boundary.
//!
//! The daemon owns startup ordering for the first-party host profile: explicit
//! config, durable state load, package/provider policy restoration, core
//! runtime initialization, status, and clean stop. It does not own terminal I/O,
//! transports, provider execution, signal handling, sockets, or supervisors.

use std::error::Error;
use std::fmt;

use botster_core::SessionId;

use crate::config::HubConfig;
use crate::packages::{
    PackageClassification, PackageRegistry, PackageRegistrySnapshotError, PackageState,
};
use crate::persistence::{FileHubStateStore, HubState, HubStateStoreError};
use crate::project_pipelines::runtime_bundle_for_prepared_package;
use crate::runtime::{HubRuntime, HubRuntimeError};

/// Local daemon lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubDaemonState {
    /// The lifecycle object has not initialized core runtime yet.
    Created,
    /// Durable state is loaded and core runtime is available.
    Running,
    /// Runtime ownership has been released through `stop`.
    Stopped,
}

/// Whether startup loaded an existing state file or initialized a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubStateLoadSource {
    /// The state file existed before this daemon start.
    Loaded,
    /// The state file was absent and v1 state was initialized from config.
    Initialized,
}

/// Deterministic daemon status used by tests, CLI output, and future transports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubDaemonStatus {
    /// Current lifecycle state.
    pub lifecycle_state: HubDaemonState,
    /// Host identifier from resolved hub config.
    pub host_id: String,
    /// Host display name from resolved hub config.
    pub host_display_name: String,
    /// Durable hub state schema version.
    pub schema_version: u16,
    /// Startup used an explicit configured data directory.
    pub data_dir_configured: bool,
    /// Whether core runtime ownership is live.
    pub core_initialized: bool,
    /// Whether startup loaded or initialized the state file.
    pub state_source: HubStateLoadSource,
    /// Count of restored package policy records.
    pub package_count: usize,
    /// Count of restored enabled package policy records.
    pub enabled_package_count: usize,
    /// Count of restored provider policy records.
    pub provider_count: usize,
    /// Count of restored enabled provider policy records.
    pub enabled_provider_count: usize,
    /// Worker-backed sessions adopted during startup reconciliation.
    pub recovered_sessions: Vec<SessionId>,
    /// Registry sessions marked stale during startup reconciliation.
    pub stale_sessions: Vec<SessionId>,
}

/// Local daemon lifecycle around `HubRuntime` and durable hub state.
pub struct HubDaemon {
    config: HubConfig,
    state: HubState,
    state_source: HubStateLoadSource,
    package_registry: PackageRegistry,
    runtime: Option<HubRuntime>,
    lifecycle_state: HubDaemonState,
}

impl HubDaemon {
    /// Start the local daemon from explicit, already-validated hub config.
    pub fn start(config: HubConfig) -> HubDaemonResult<Self> {
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state_source = if store.path().exists() {
            HubStateLoadSource::Loaded
        } else {
            HubStateLoadSource::Initialized
        };
        let mut runtime = HubRuntime::load_from_store(config.clone(), &store)?;
        let state = runtime.state().clone();
        let package_registry = PackageRegistry::from_snapshot(state.package_registry.clone())?;
        load_enabled_local_plugins(&mut runtime, &package_registry, &config)?;

        Ok(Self {
            config,
            state,
            state_source,
            package_registry,
            runtime: Some(runtime),
            lifecycle_state: HubDaemonState::Running,
        })
    }

    /// Return the runtime while the daemon is running.
    #[must_use]
    pub fn runtime(&self) -> Option<&HubRuntime> {
        self.runtime.as_ref()
    }

    /// Return a mutable runtime while the daemon is running.
    #[must_use]
    pub fn runtime_mut(&mut self) -> Option<&mut HubRuntime> {
        self.runtime.as_mut()
    }

    /// Return the package registry restored for this daemon lifecycle.
    #[must_use]
    pub const fn package_registry(&self) -> &PackageRegistry {
        &self.package_registry
    }

    /// Return the mutable package registry restored for this daemon lifecycle.
    pub const fn package_registry_mut(&mut self) -> &mut PackageRegistry {
        &mut self.package_registry
    }

    /// Return deterministic lifecycle status without exposing local paths.
    #[must_use]
    pub fn status(&self) -> HubDaemonStatus {
        let packages = self.package_registry.packages();
        let provider_count = packages
            .iter()
            .filter(|record| matches!(record.classification, PackageClassification::Provider))
            .count();
        let enabled_provider_count = packages
            .iter()
            .filter(|record| {
                matches!(record.classification, PackageClassification::Provider)
                    && matches!(record.state, PackageState::Enabled)
            })
            .count();
        let enabled_package_count = packages.iter().filter(|record| record.is_enabled()).count();
        let (recovered_sessions, stale_sessions) = self
            .runtime
            .as_ref()
            .map(|runtime| {
                (
                    runtime.reconciliation().recovered_sessions.clone(),
                    runtime.reconciliation().stale_sessions.clone(),
                )
            })
            .unwrap_or_default();

        HubDaemonStatus {
            lifecycle_state: self.lifecycle_state,
            host_id: self.config.host.id.clone(),
            host_display_name: self.config.host.display_name.clone(),
            schema_version: self.state.schema_version,
            data_dir_configured: true,
            core_initialized: self.runtime.is_some(),
            state_source: self.state_source,
            package_count: packages.len(),
            enabled_package_count,
            provider_count,
            enabled_provider_count,
            recovered_sessions,
            stale_sessions,
        }
    }

    /// Stop the daemon lifecycle. This is idempotent.
    pub fn stop(&mut self) -> HubDaemonStatus {
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.release_for_restart();
        }
        self.runtime = None;
        self.lifecycle_state = HubDaemonState::Stopped;
        self.status()
    }
}

pub(crate) fn load_enabled_local_plugins(
    runtime: &mut HubRuntime,
    package_registry: &PackageRegistry,
    config: &HubConfig,
) -> HubDaemonResult<()> {
    let prepared = package_registry
        .prepare_enabled_local_packages("daemon startup load enabled local plugin packages")?;
    for package in prepared {
        if let Some(bundle) = runtime_bundle_for_prepared_package(&package, &config.data_directory)
        {
            runtime.load_plugin_package(package_registry, &package.package_name, bundle)?;
        }
    }
    Ok(())
}

/// Typed daemon startup errors.
#[derive(Debug)]
pub enum HubDaemonError {
    /// Durable state failed to load or initialize.
    State(HubStateStoreError),
    /// Runtime failed to initialize or reconcile daemon-backed sessions.
    Runtime(HubRuntimeError),
    /// Persisted package/provider policy records could not be restored.
    PackageRegistry(PackageRegistrySnapshotError),
    /// Package policy rejected a local package while loading enabled plugins.
    Package(crate::PackageRegistryError),
    /// Plugin lifecycle rejected a prepared package load.
    Lifecycle(crate::HubLifecycleError),
}

impl fmt::Display for HubDaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::State(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error}"),
            Self::PackageRegistry(error) => {
                write!(formatter, "hub package registry restore error: {error:?}")
            }
            Self::Package(error) => write!(formatter, "hub package policy error: {error:?}"),
            Self::Lifecycle(error) => write!(formatter, "hub plugin lifecycle error: {error:?}"),
        }
    }
}

impl Error for HubDaemonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::State(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::PackageRegistry(_) => None,
            Self::Package(_) | Self::Lifecycle(_) => None,
        }
    }
}

impl From<HubStateStoreError> for HubDaemonError {
    fn from(error: HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<HubRuntimeError> for HubDaemonError {
    fn from(error: HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PackageRegistrySnapshotError> for HubDaemonError {
    fn from(error: PackageRegistrySnapshotError) -> Self {
        Self::PackageRegistry(error)
    }
}

impl From<crate::PackageRegistryError> for HubDaemonError {
    fn from(error: crate::PackageRegistryError) -> Self {
        Self::Package(error)
    }
}

impl From<crate::HubLifecycleError> for HubDaemonError {
    fn from(error: crate::HubLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Daemon lifecycle result alias.
pub type HubDaemonResult<T> = Result<T, HubDaemonError>;
