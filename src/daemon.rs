//! Deterministic local hub daemon lifecycle over the durable state boundary.
//!
//! The daemon owns startup ordering for the first-party host profile: explicit
//! config, durable state load, package/provider policy restoration, core
//! runtime initialization, status, and clean stop. It does not own terminal I/O,
//! transports, provider execution, signal handling, or sockets.

pub(crate) mod client_events;
pub(crate) mod control;
pub(crate) mod error;
pub(crate) mod event_owner;
pub(crate) mod owner_budget;
pub(crate) mod owner_loop;
pub(crate) mod owner_schedule;
pub(crate) mod owner_turn;
pub(crate) mod publication_owner;
pub mod readiness;
pub(crate) mod shutdown;

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use botster_core::SessionId;

use crate::HubLuaPluginLoadError;
use crate::config::HubConfig;
use crate::packages::{
    PackageClassification, PackageRegistry, PackageRegistrySnapshotError, PackageState,
};
use crate::persistence::{
    FileCommitError, FileCommitOutcome, FileHubStateStore, HubState, HubStateStoreError,
};
use crate::runtime::{HubRuntime, HubRuntimeError, SharedHubState};
use crate::shared_view::SharedView;
use crate::transport::webrtc::LocalWebrtcTransport;

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
    state: SharedHubState,
    state_source: HubStateLoadSource,
    package_registry: SharedView<PackageRegistry>,
    local_webrtc: LocalWebrtcTransport,
    runtime: Option<HubRuntime>,
    lifecycle_state: HubDaemonState,
    installation_home: Option<std::path::PathBuf>,
}

impl HubDaemon {
    /// Start the local daemon from explicit, already-validated hub config.
    pub fn start(config: HubConfig) -> HubDaemonResult<Self> {
        let installation_home = std::env::var_os("HOME").map(std::path::PathBuf::from);
        let maximum_completion_bytes = config
            .plugin_worker_config()
            .completion_reservation_byte_capacity;
        validate_plugin_result_capacity(maximum_completion_bytes)?;
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state_source = if store.path().exists() {
            HubStateLoadSource::Loaded
        } else {
            HubStateLoadSource::Initialized
        };
        let mut runtime = HubRuntime::load_from_store(config.clone(), &store)?;
        let mut state = runtime.state();
        let package_registry = PackageRegistry::from_snapshot(state.package_registry.clone())?;
        let (package_registry, decisions) = package_registry
            .refreshed_local_packages("daemon startup refresh local package registrations")?;
        let package_registry =
            reserve_package_registry(&runtime.shared_view_budget(), package_registry)?;
        if !decisions.is_empty() {
            let snapshot = package_registry.snapshot();
            let authority = runtime
                .state_authority()
                .ok_or(HubDaemonError::State(HubStateStoreError::AuthorityRequired))?;
            let (revision, prior) = runtime.state_publication().snapshot();
            state = match store.update_shared(
                &authority,
                revision,
                prior,
                &runtime.shared_view_budget(),
                |state| state.package_registry = snapshot,
            ) {
                Ok(FileCommitOutcome::Synced { state, .. }) => state,
                Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                    return Err(HubDaemonError::State(
                        HubStateStoreError::PublishedUncertain(write),
                    ));
                }
                Err(FileCommitError::Preparation(error))
                | Err(FileCommitError::BeforePublication { error, .. }) => {
                    return Err(HubDaemonError::State(error));
                }
                Err(FileCommitError::Stale(_)) => {
                    return Err(HubDaemonError::State(HubStateStoreError::StaleRevision));
                }
                Err(FileCommitError::RevisionExhausted(_)) => {
                    return Err(HubDaemonError::State(HubStateStoreError::RevisionExhausted));
                }
            };
            runtime.publish_state_view(state.clone());
        }
        runtime.publish_package_registry_view(package_registry.clone());
        load_enabled_local_plugins(&mut runtime, &package_registry)?;

        let state = runtime.state_publication();
        Ok(Self {
            config,
            state,
            state_source,
            package_registry,
            local_webrtc: LocalWebrtcTransport::default(),
            runtime: Some(runtime),
            lifecycle_state: HubDaemonState::Running,
            installation_home,
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

    /// Publish durable hub state after an owner-thread mutation.
    /// Publish one shared state allocation to the daemon and runtime views.
    pub(crate) fn publish_state(&mut self, state: SharedView<HubState>) {
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.publish_state_view(state);
        } else {
            self.state.publish(state);
        }
    }

    /// Return the current shared state allocation and its owner revision.
    pub(crate) fn state_view(&self) -> (u64, SharedView<HubState>) {
        self.state.snapshot()
    }

    /// Return the package registry restored for this daemon lifecycle.
    #[must_use]
    pub fn package_registry(&self) -> &PackageRegistry {
        self.package_registry.as_ref()
    }

    /// Replace the in-memory package registry after reserving its shared-view charge.
    pub fn replace_package_registry(
        &mut self,
        package_registry: PackageRegistry,
    ) -> Result<(), HubStateStoreError> {
        let package_registry = self.prepare_package_registry(package_registry)?;
        self.publish_package_registry_view(package_registry);
        Ok(())
    }

    /// Publish a package view that was reserved before its durable commit.
    pub(crate) fn publish_package_registry_view(
        &mut self,
        package_registry: SharedView<PackageRegistry>,
    ) {
        // Lua plugin reads and spawn resolution see the committed registry.
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.publish_package_registry_view(package_registry.clone());
        }
        self.package_registry = package_registry;
    }

    pub(crate) fn prepare_package_registry(
        &self,
        package_registry: PackageRegistry,
    ) -> Result<SharedView<PackageRegistry>, HubStateStoreError> {
        reserve_package_registry(&self.state.budget(), package_registry)
    }

    /// Return one shared package-registry input for off-owner reads.
    pub(crate) fn package_registry_view(&self) -> SharedView<PackageRegistry> {
        self.package_registry.clone()
    }

    /// Return the ephemeral local WebRTC signaling/admission registry.
    pub const fn local_webrtc(&mut self) -> &mut LocalWebrtcTransport {
        &mut self.local_webrtc
    }

    /// Status uses the installation home captured at daemon startup.
    pub(crate) fn installation_home(&self) -> Option<&std::path::Path> {
        self.installation_home.as_deref()
    }

    pub(crate) fn installation_home_bytes(&self, limit: usize) -> Option<usize> {
        let bytes = std::mem::size_of::<Option<std::path::PathBuf>>().checked_add(
            self.installation_home()
                .map_or(0, |home| home.as_os_str().len()),
        )?;
        (bytes <= limit).then_some(bytes)
    }

    pub(crate) fn bounded_installation_home(
        &self,
        limit: usize,
    ) -> Option<(Option<std::path::PathBuf>, usize)> {
        let bytes = self.installation_home_bytes(limit)?;
        Some((self.installation_home.clone(), bytes))
    }

    /// Count the snapshot's logical storage before copying source fields.
    pub(crate) fn status_bytes(&self, limit: usize) -> Option<usize> {
        let (recovered, stale) = self.runtime.as_ref().map_or((&[][..], &[][..]), |runtime| {
            let reconciliation = runtime.reconciliation();
            (
                reconciliation.recovered_sessions.as_slice(),
                reconciliation.stale_sessions.as_slice(),
            )
        });
        status_snapshot_bytes(
            &self.config.host.id,
            &self.config.host.display_name,
            recovered,
            stale,
            limit,
        )
    }

    pub(crate) fn bounded_status(&self, limit: usize) -> Option<(HubDaemonStatus, usize)> {
        let bytes = self.status_bytes(limit)?;
        Some((self.status(), bytes))
    }

    /// Return deterministic lifecycle status without exposing local paths.
    #[must_use]
    pub fn status(&self) -> HubDaemonStatus {
        let packages = self.package_registry.package_records();
        let package_count = packages.len();
        let mut provider_count = 0;
        let mut enabled_provider_count = 0;
        let mut enabled_package_count = 0;
        for record in packages {
            if matches!(record.classification, PackageClassification::Provider) {
                provider_count += 1;
                if matches!(record.state, PackageState::Enabled) {
                    enabled_provider_count += 1;
                }
            }
            if record.is_enabled() {
                enabled_package_count += 1;
            }
        }
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
            schema_version: self.state.snapshot().1.schema_version,
            data_dir_configured: true,
            core_initialized: self.runtime.is_some(),
            state_source: self.state_source,
            package_count,
            enabled_package_count,
            provider_count,
            enabled_provider_count,
            recovered_sessions,
            stale_sessions,
        }
    }

    /// Stop the daemon lifecycle. This is idempotent.
    pub fn stop(&mut self) -> HubDaemonStatus {
        self.local_webrtc.stop_all();
        if let Some(runtime) = self.runtime.as_mut() {
            runtime.release_for_restart();
        }
        self.runtime = None;
        self.lifecycle_state = HubDaemonState::Stopped;
        self.status()
    }
}

fn status_snapshot_bytes(
    host_id: &str,
    display_name: &str,
    recovered: &[SessionId],
    stale: &[SessionId],
    limit: usize,
) -> Option<usize> {
    let mut bytes = std::mem::size_of::<HubDaemonStatus>();
    for length in [host_id.len(), display_name.len()] {
        bytes = bytes.checked_add(length)?;
        if bytes > limit {
            return None;
        }
    }
    for rows in [recovered, stale] {
        bytes = bytes.checked_add(rows.len().checked_mul(std::mem::size_of::<SessionId>())?)?;
        if bytes > limit {
            return None;
        }
        for row in rows {
            bytes = bytes.checked_add(row.0.len())?;
            if bytes > limit {
                return None;
            }
        }
    }
    Some(bytes)
}

fn validate_plugin_result_capacity(maximum_completion_bytes: usize) -> HubDaemonResult<()> {
    let retained_result_capacity =
        crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY;
    if maximum_completion_bytes > retained_result_capacity {
        return Err(HubDaemonError::PluginResultCapacity {
            maximum_completion_bytes,
            retained_result_capacity,
        });
    }
    Ok(())
}

pub(crate) fn load_enabled_local_plugins(
    runtime: &mut HubRuntime,
    package_registry: &PackageRegistry,
) -> HubDaemonResult<()> {
    let prepared = package_registry
        .prepare_enabled_local_packages("daemon startup load enabled local plugin packages")?;
    for package in prepared {
        if package.selected_lua_entrypoint().is_some()
            && let Err(error) =
                runtime.load_lua_plugin_package(package_registry, &package.package_name)
        {
            if !error.is_package_scoped_startup_failure() {
                return Err(error.into());
            }
            // The failed package stays unloaded. Startup records the exact
            // package failure and continues with healthy siblings.
            runtime.record_startup_plugin_load_failure(&package.package_name, &error);
        }
    }
    Ok(())
}

pub(crate) fn reserve_package_registry(
    budget: &Arc<crate::shared_view::SharedViewBudget>,
    package_registry: PackageRegistry,
) -> Result<SharedView<PackageRegistry>, HubStateStoreError> {
    let logical_bytes = serde_json::to_vec_pretty(&package_registry.snapshot())
        .map_err(HubStateStoreError::Serialize)?
        .len();
    SharedView::try_new(budget, package_registry, logical_bytes).map_err(|error| {
        HubStateStoreError::ViewCapacity {
            requested: error.requested,
            available: error.available,
        }
    })
}

/// Typed daemon startup errors.
#[derive(Debug)]
pub enum HubDaemonError {
    /// Core can create one completion that does not fit Hub's retained-result budget.
    PluginResultCapacity {
        maximum_completion_bytes: usize,
        retained_result_capacity: usize,
    },
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
    /// Enabled local Lua plugin failed to load during daemon startup.
    LuaPlugin(HubLuaPluginLoadError),
}

impl fmt::Display for HubDaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PluginResultCapacity {
                maximum_completion_bytes,
                retained_result_capacity,
            } => write!(
                formatter,
                "plugin completion maximum {maximum_completion_bytes} exceeds retained-result capacity {retained_result_capacity}"
            ),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error}"),
            Self::PackageRegistry(error) => {
                write!(formatter, "hub package registry restore error: {error}")
            }
            Self::Package(error) => write!(formatter, "hub package policy error: {error:?}"),
            Self::Lifecycle(error) => write!(formatter, "hub plugin lifecycle error: {error:?}"),
            Self::LuaPlugin(error) => write!(formatter, "hub lua plugin load error: {error}"),
        }
    }
}

impl Error for HubDaemonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PluginResultCapacity { .. } => None,
            Self::State(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::PackageRegistry(_) => None,
            Self::Package(_) | Self::Lifecycle(_) => None,
            Self::LuaPlugin(error) => Some(error),
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

impl From<HubLuaPluginLoadError> for HubDaemonError {
    fn from(error: HubLuaPluginLoadError) -> Self {
        Self::LuaPlugin(error)
    }
}

/// Daemon lifecycle result alias.
pub type HubDaemonResult<T> = Result<T, HubDaemonError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_snapshot_preflight_counts_both_reconciliation_vectors_before_copy() {
        let recovered = vec![SessionId("recovered".into()), SessionId("second".into())];
        let mut stale = vec![SessionId("stale".into())];
        let bytes = std::mem::size_of::<HubDaemonStatus>()
            + "hostdisplay".len()
            + 3 * std::mem::size_of::<SessionId>()
            + "recoveredsecondstale".len();
        assert_eq!(
            status_snapshot_bytes("host", "display", &recovered, &stale, bytes),
            Some(bytes)
        );
        assert_eq!(
            status_snapshot_bytes("host", "display", &recovered, &stale, bytes - 1),
            None
        );
        stale[0].0.push('x');
        assert_eq!(
            status_snapshot_bytes("host", "display", &recovered, &stale, bytes),
            None
        );
        assert_eq!(
            status_snapshot_bytes("", "", &[], &[], std::mem::size_of::<HubDaemonStatus>() - 1),
            None
        );
    }

    #[test]
    fn status_installation_home_preflight_preserves_missing_empty_and_native_paths() {
        use std::os::unix::ffi::OsStringExt;
        let config = shared_state_config();
        let root = config.data_directory.clone();
        let mut daemon = HubDaemon::start(config).unwrap();
        for home in [
            None,
            Some(std::path::PathBuf::new()),
            Some(std::path::PathBuf::from(std::ffi::OsString::from_vec(
                vec![b'/', 0xff, b'x'],
            ))),
        ] {
            daemon.installation_home = home.clone();
            let bytes = daemon.installation_home_bytes(usize::MAX).unwrap();
            assert!(daemon.bounded_installation_home(bytes - 1).is_none());
            let (copy, charged) = daemon.bounded_installation_home(bytes).unwrap();
            assert_eq!(copy, home);
            assert_eq!(charged, bytes);
            assert_eq!(daemon.installation_home(), home.as_deref());
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    fn shared_state_config() -> HubConfig {
        let data_directory = std::path::PathBuf::from("target")
            .join("botster-hub-test-data")
            .join("shared-state-publication")
            .join(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time after epoch")
                    .as_nanos()
                    .to_string(),
            );
        crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "shared-state-test".to_string(),
                display_name: "Shared State Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build shared state config")
    }

    #[test]
    fn plugin_result_capacity_accepts_exact_limit_and_rejects_larger_completion() {
        let capacity = crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY;
        validate_plugin_result_capacity(capacity).expect("exact capacity fits");
        assert!(matches!(
            validate_plugin_result_capacity(capacity + 1),
            Err(HubDaemonError::PluginResultCapacity {
                maximum_completion_bytes,
                retained_result_capacity,
            }) if maximum_completion_bytes == capacity + 1
                && retained_result_capacity == capacity
        ));
    }

    #[test]
    fn startup_isolates_one_package_load_failure_and_loads_a_healthy_sibling() {
        let config = shared_state_config();
        let broken_dir = config.data_directory.join("broken.plugin");
        std::fs::create_dir_all(&broken_dir).expect("create broken package directory");
        std::fs::write(
            broken_dir.join("botster-package.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "broken.plugin",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": "." },
                "capabilities": [{ "surface": "mcp" }],
                "entrypoints": [
                    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
                ]
            }))
            .expect("serialize broken package manifest"),
        )
        .expect("write broken package manifest");
        std::fs::write(
            broken_dir.join("plugin.lua"),
            "error('deterministic startup load failure')\n",
        )
        .expect("write broken Lua plugin");

        let mut daemon = HubDaemon::start(config.clone()).expect("initialize daemon state");
        let mut registry = daemon.package_registry().clone();
        registry
            .install_local_path(&broken_dir, "install broken startup package")
            .expect("install broken startup package");
        registry
            .enable("broken.plugin", "enable broken startup package")
            .expect("enable broken startup package");
        registry
            .install_local_path(
                std::path::Path::new("examples/synthetic-plugin"),
                "install healthy startup package",
            )
            .expect("install healthy startup package");
        registry
            .enable("runtime.synthetic-plugin", "enable healthy startup package")
            .expect("enable healthy startup package");
        daemon
            .replace_package_registry(registry)
            .expect("publish startup package registry");
        let snapshot = daemon.package_registry().snapshot();
        let runtime = daemon.runtime().expect("initial runtime");
        let authority = runtime.state_authority().expect("File authority");
        let (revision, prior) = runtime.state_publication().snapshot();
        let outcome = FileHubStateStore::for_data_directory(&config.data_directory)
            .update_shared(
                &authority,
                revision,
                prior,
                &runtime.shared_view_budget(),
                |state| state.package_registry = snapshot,
            )
            .expect("persist startup package registry");
        assert!(matches!(outcome, FileCommitOutcome::Synced { .. }));
        drop(authority);
        daemon.stop();

        let mut restarted = HubDaemon::start(config).expect("start with isolated package failure");
        let packages = restarted.package_registry().clone();
        let api = crate::HubClientApi::local_operator("startup-package-isolation");
        let response = api
            .handle_request(
                restarted.runtime_mut().expect("restarted runtime"),
                &packages,
                crate::HubClientRequest::PluginLifecycleStatus {
                    request_id: botster_core::RequestId(
                        "startup-package-isolation-status".to_string(),
                    ),
                },
            )
            .wait(restarted.runtime().expect("restarted runtime"))
            .expect("read startup package failures");
        let crate::HubClientResponseBody::PluginLifecycle(report) = response.body else {
            panic!("plugin lifecycle response expected");
        };
        let broken = report
            .lifecycle
            .iter()
            .find(|row| row.package_name == "broken.plugin")
            .expect("broken package lifecycle row");
        assert!(!broken.loaded);
        let failure = broken
            .load_failure
            .as_ref()
            .expect("broken package has a typed load failure");
        assert_eq!(failure.code, "lua_load_failed");
        assert!(
            failure
                .message
                .contains("deterministic startup load failure")
        );
        let healthy = report
            .lifecycle
            .iter()
            .find(|row| row.package_name == "runtime.synthetic-plugin")
            .expect("healthy package lifecycle row");
        assert!(healthy.loaded);
        assert!(healthy.load_failure.is_none());

        let runtime = restarted.runtime().expect("restarted runtime");
        let healthy_result = runtime
            .call_plugin_mcp_tool(crate::McpCallRequest {
                name: "runtime.synthetic.echo".to_string(),
                arguments: serde_json::json!({ "message": "healthy" }),
            })
            .expect("healthy sibling tool remains functional");
        assert_eq!(healthy_result["message"], "healthy");
        let denied = runtime
            .call_plugin_mcp_tool(crate::McpCallRequest {
                name: "broken.never".to_string(),
                arguments: serde_json::json!({}),
            })
            .expect_err("the isolated package cannot invoke a capability-gated tool");
        assert_eq!(denied.code, "unknown_tool");
        restarted.stop();
    }

    #[test]
    fn state_publication_shares_one_allocation_and_advances_the_owner_revision() {
        let mut daemon = HubDaemon::start(shared_state_config()).expect("start daemon");
        let (initial_revision, initial) = daemon.state_view();
        let runtime_initial = daemon.runtime().expect("runtime").state();
        assert_eq!(initial_revision, 0);
        assert!(SharedView::ptr_eq(&initial, &runtime_initial));

        let mut next = (*initial).clone();
        next.session_type_generation = 1;
        let next = daemon
            .runtime()
            .expect("runtime")
            .prepare_state(next)
            .expect("next view fits");
        daemon.publish_state(next.clone());

        let (published_revision, published) = daemon.state_view();
        let runtime = daemon.runtime().expect("runtime");
        let runtime_published = runtime.state();
        let spawn_targets = runtime.spawn_targets();
        let worktrees = runtime.worktrees();
        let plugin_published = spawn_targets.snapshot().1;
        assert_eq!(published_revision, 1);
        assert!(SharedView::ptr_eq(&next, &published));
        assert!(SharedView::ptr_eq(&published, &runtime_published));
        assert!(SharedView::ptr_eq(&published, &plugin_published));
        assert!(Arc::ptr_eq(&spawn_targets, &worktrees));
        assert!(!SharedView::ptr_eq(&initial, &published));

        let mut final_state = (*published).clone();
        final_state.session_type_generation = 2;
        let final_state = daemon
            .runtime()
            .expect("runtime")
            .prepare_state(final_state)
            .expect("final view fits");
        daemon.publish_state(final_state);
        let (final_revision, _) = daemon.state_view();
        assert_eq!(final_revision, 2);
    }

    #[test]
    fn state_and_package_registry_use_separate_charges_in_one_pool() {
        let daemon = HubDaemon::start(shared_state_config()).expect("start daemon");
        let runtime = daemon.runtime().expect("runtime");
        let state = runtime.state();
        let state_bytes = serde_json::to_vec_pretty(&*state)
            .expect("serialize state")
            .len();
        let package_bytes = serde_json::to_vec_pretty(&daemon.package_registry().snapshot())
            .expect("serialize package registry")
            .len();
        let budget = runtime.shared_view_budget();

        assert_eq!(budget.used(), state_bytes + package_bytes);
        let package_lease = daemon.package_registry_view();
        assert_eq!(budget.used(), state_bytes + package_bytes);
        drop(package_lease);
        assert_eq!(budget.used(), state_bytes + package_bytes);
    }

    #[test]
    fn admitted_mutation_updates_the_shared_state_and_revision() {
        let mut daemon = HubDaemon::start(shared_state_config()).expect("start daemon");
        let (_, before) = daemon.state_view();
        let definition = crate::PackageSessionType {
            id: "shared-publication".to_string(),
            label: "Shared publication".to_string(),
            description: None,
            icon: None,
            role: "botster.agent".to_string(),
            interaction: "interactive".to_string(),
            traits: vec!["terminal".to_string()],
            lifecycle: "task".to_string(),
            execution: crate::PackageSessionTypeExecution::RelativeExecutable,
            command: "bin/agent".to_string(),
            args: Vec::new(),
            working_directory: crate::PackageSessionTypeWorkingDirectory::PackageRoot,
            environment: std::collections::BTreeMap::new(),
            allowed_environment_overrides: Vec::new(),
            context: Vec::new(),
            target_id: None,
        };
        let request = botster_hub_client::DaemonRequest::CreateSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            definition: crate::client_api_dto::session::daemon_session_type_definition_from_client(
                definition,
            ),
        };
        let mut state = crate::daemon::owner_loop::DaemonControlState::default();
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("transport runtime");
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(8);
        let (reply_tx, mut reply_rx) = crate::daemon::control::message::control_reply_channel();
        assert!(!crate::daemon::control::request::handle(
            &mut daemon,
            &mut state,
            transport.handle(),
            control_tx,
            crate::daemon::control::message::ControlMessage::Request {
                request: Box::new(request),
                transport_request_id: None,
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: None,
                enqueued_at: std::time::Instant::now(),
            },
        ));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let response = loop {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            match reply_rx.try_recv() {
                Ok(reply) => break reply.into_parts().0.expect("mutation response"),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    panic!("mutation reply closed")
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the owner must complete the mutation"
                    );
                    std::thread::yield_now();
                }
            }
        };
        assert!(
            response.error.is_none(),
            "mutation must succeed: {:?}",
            response.error
        );
        let (revision, after) = daemon.state_view();
        assert_eq!(revision, 1);
        assert_eq!(after.session_type_generation, 1);
        assert!(!SharedView::ptr_eq(&before, &after));
        assert!(SharedView::ptr_eq(
            &after,
            &daemon.runtime().expect("runtime").state()
        ));
        daemon.stop();
    }
}
