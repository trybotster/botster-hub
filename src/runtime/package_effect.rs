//! Runtime handles used by typed package effects on host workers.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use super::{HubLuaPluginLoadError, PendingEventPlaneReplace};
use crate::lifecycle::{HubLifecycleResult, HubPluginLifecycle, HubPluginRuntimeBundle};
use crate::lua_runtime::{LuaPluginHostApi, LuaPluginRuntime, SharedHubCapabilityRuntime};
use crate::package_event_router::{
    ActivationError, EventPlaneStatus, EventSubscription, PackageEventRouter, StageError,
    StagedGeneration,
};
use crate::packages::PackageRegistry;
use botster_core::{PluginCapabilityRuntime, PluginCleanupResult, PluginKey, RequestId};

#[derive(Default)]
pub(crate) struct HostPackageCleanup {
    pub(crate) family_epoch: Option<u64>,
    pub(crate) family_cursor: super::family_cleanup::FamilyCleanupCursor,
    pub(crate) last_capability_cleanup: Option<PluginCleanupResult>,
    pub(crate) unloaded_families: Vec<(String, BTreeSet<String>)>,
    pub(crate) event_plane_unloads: VecDeque<crate::package_event_router::OwnerOp>,
    pub(crate) event_plane_faults: Vec<(
        Result<u64, EventPlaneStatus>,
        crate::package_event_router::EventOwnerWorkError,
    )>,
    /// A generation staged but never activated, because the effect failed or
    /// unwound between stage and activation. The attempt's restore aborts it.
    pub(crate) staged: Option<StagedGeneration>,
}

/// Funding for staging one package event generation: a handle that keeps the
/// attempt's prepared-byte reservation alive, and that reservation's size.
/// The router holds the handle with the pending entry, so the charge lives
/// exactly as long as the entry.
pub(crate) struct StagingFunding {
    pub(crate) retained: Box<dyn Send>,
    pub(crate) reserved_bytes: usize,
}

impl StagingFunding {
    /// Funding for a load outside any Host attempt: daemon startup, before the
    /// owner loop serves, and direct test loads. The pending entry lives only
    /// inside that one synchronous load, and the Host prepared-byte capacity
    /// bounds it as it bounds every attempt.
    pub(crate) fn outside_attempt() -> Self {
        Self {
            retained: Box::new(()),
            reserved_bytes: crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
        }
    }
}

pub(crate) struct HostPackageRuntime {
    plugin_lifecycle: HubPluginLifecycle,
    host_api: LuaPluginHostApi,
    capability_runtime: SharedHubCapabilityRuntime,
    package_event_router: Arc<PackageEventRouter>,
    last_capability_cleanup: Option<PluginCleanupResult>,
    unloaded_families: Vec<(String, BTreeSet<String>)>,
    event_plane_unloads: VecDeque<crate::package_event_router::OwnerOp>,
    event_plane_faults: Vec<(
        Result<u64, EventPlaneStatus>,
        crate::package_event_router::EventOwnerWorkError,
    )>,
    staging_funding: Option<StagingFunding>,
    /// Held here, outside the effect's `catch_unwind`, so an unwind between
    /// stage and activation still hands it to the restore.
    staged: Option<StagedGeneration>,
}

impl HostPackageRuntime {
    /// Fund the next event generation this runtime stages.
    pub(crate) fn fund_staging(&mut self, funding: StagingFunding) {
        self.staging_funding = Some(funding);
    }

    /// A generation this runtime staged but did not activate.
    pub(crate) fn take_staged(&mut self) -> Option<StagedGeneration> {
        self.staged.take()
    }

    /// Count a quarantined package's queued events as stranded before its
    /// unload retires them. Host workers only; the lock is taken blocking.
    pub(crate) fn record_package_stranded(&self, package_name: &str) {
        let queued = self
            .package_event_router
            .queued_copies_blocking(package_name);
        self.package_event_router
            .counters()
            .record_events_stranded(queued);
    }

    /// Discard a generation a failed attempt staged but never activated.
    /// Runs on a Host worker; the router lock is taken blocking.
    pub(crate) fn abort_staged(&mut self, staged: StagedGeneration) {
        self.package_event_router.abort_staged_generation(staged);
    }

    pub(crate) fn new(plugin_lifecycle: HubPluginLifecycle, host_api: LuaPluginHostApi) -> Self {
        Self {
            plugin_lifecycle,
            capability_runtime: host_api.capabilities.clone(),
            package_event_router: host_api.package_event_router.clone(),
            host_api,
            last_capability_cleanup: None,
            unloaded_families: Vec::new(),
            event_plane_unloads: VecDeque::new(),
            event_plane_faults: Vec::new(),
            staging_funding: None,
            staged: None,
        }
    }

    pub(crate) fn into_cleanup(self) -> HostPackageCleanup {
        HostPackageCleanup {
            family_epoch: None,
            family_cursor: super::family_cleanup::FamilyCleanupCursor::default(),
            last_capability_cleanup: self.last_capability_cleanup,
            unloaded_families: self.unloaded_families,
            event_plane_unloads: self.event_plane_unloads,
            event_plane_faults: self.event_plane_faults,
            staged: self.staged,
        }
    }

    pub(crate) fn record_event_plane_unload(&mut self, package_name: &str) {
        self.event_plane_unloads
            .push_back(crate::package_event_router::OwnerOp {
                kind: crate::package_event_router::OwnerOpKind::Unload,
                owner: package_name.to_string(),
                generation: self
                    .package_event_router
                    .current_package_generation(package_name)
                    .unwrap_or(0),
            });
    }

    pub fn load_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginKey> {
        self.plugin_lifecycle
            .load_package(registry, package_name, bundle)
    }

    pub fn load_lua_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginKey, HubLuaPluginLoadError> {
        let prepared = registry
            .prepare_local_package(package_name, "load local lua plugin package")
            .map_err(HubLuaPluginLoadError::Package)?;
        let configuration = registry
            .package(package_name)
            .map(|record| record.configuration_view())
            .expect("prepared local package must have a registry record");
        let bundle = LuaPluginRuntime::load_prepared_bounded(
            &prepared,
            configuration,
            self.host_api.clone(),
        )
        .map_err(HubLuaPluginLoadError::Lua)?;
        let event_handlers = bundle.event_handlers.clone();
        // Every fallible step runs before anything changes: prepare the
        // plugin, then stage its event generation. The install cannot fail,
        // and activation publishes the subscriptions only after it.
        let mut plugin = self
            .plugin_lifecycle
            .prepare_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)?;
        let staged_plane = self
            .staged_package_event_plane(package_name, registry, &event_handlers)
            .map_err(HubLuaPluginLoadError::EventPlane)?;
        let generation = self.stage_event_generation(package_name, staged_plane)?;
        plugin.set_event_generation(generation);
        let (key, _) = self
            .plugin_lifecycle
            .commit_package(RequestId("hub-load-registration".into()), plugin);
        self.activate_event_generation()?;
        Ok(key)
    }

    /// Stage this package's next event generation, funded by the attempt.
    /// A refusal leaves the router unchanged.
    fn stage_event_generation(
        &mut self,
        package_name: &str,
        staged_plane: PendingEventPlaneReplace,
    ) -> Result<u64, HubLuaPluginLoadError> {
        let funding = self
            .staging_funding
            .take()
            .unwrap_or_else(StagingFunding::outside_attempt);
        let staged = self
            .package_event_router
            .stage_package_generation(
                package_name,
                staged_plane.contracts,
                staged_plane.subscriptions,
                funding.retained,
                funding.reserved_bytes,
            )
            .map_err(|error| match error {
                StageError::Rejected(status) => HubLuaPluginLoadError::EventPlane(status),
                StageError::AlreadyStaged => HubLuaPluginLoadError::EventPlaneStageOverlap,
                StageError::Unfunded { required, reserved } => {
                    HubLuaPluginLoadError::EventPlaneUnfunded { required, reserved }
                }
            })?;
        let generation = staged.generation();
        self.staged = Some(staged);
        Ok(generation)
    }

    /// Publish the staged generation after the plugin that serves it is
    /// installed. Only an invariant break fails here, after the swap.
    fn activate_event_generation(&mut self) -> Result<(), HubLuaPluginLoadError> {
        let staged = self
            .staged
            .take()
            .expect("activation follows a successful stage");
        match self.package_event_router.activate_staged_generation(staged) {
            Ok(_) => Ok(()),
            Err(ActivationError::Faulted(staged)) => {
                self.staged = Some(staged);
                Err(HubLuaPluginLoadError::EventPlaneActivationFaulted)
            }
            Err(ActivationError::NotStaged) => {
                Err(HubLuaPluginLoadError::EventPlaneActivationFaulted)
            }
            Err(ActivationError::Replace(error)) => match error.into_parts() {
                (Err(status), cleanup) => {
                    if let Some(cleanup) = cleanup {
                        self.event_plane_faults.push((Err(status), cleanup));
                    }
                    Err(HubLuaPluginLoadError::EventPlaneStranded(status))
                }
                (Ok(generation), Some(cleanup)) => {
                    // Committed; only the previous generation's cleanup
                    // failed. One version serves; retain the cleanup fault.
                    self.event_plane_faults.push((Ok(generation), cleanup));
                    Ok(())
                }
                (Ok(_), None) => unreachable!("an activation error carries a failure"),
            },
        }
    }

    pub fn reload_lua_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginCleanupResult, HubLuaPluginLoadError> {
        let prepared = registry
            .prepare_local_package(package_name, "reload local lua plugin package")
            .map_err(HubLuaPluginLoadError::Package)?;
        let configuration = registry
            .package(package_name)
            .map(|record| record.configuration_view())
            .expect("prepared local package must have a registry record");
        let bundle = LuaPluginRuntime::load_prepared_bounded(
            &prepared,
            configuration,
            self.host_api.clone(),
        )
        .map_err(HubLuaPluginLoadError::Lua)?;
        let event_handlers = bundle.event_handlers.clone();
        let staged_plane = self
            .staged_package_event_plane(package_name, registry, &event_handlers)
            .map_err(HubLuaPluginLoadError::EventPlane)?;
        // Every fallible step runs before anything changes. The previous
        // generation stays live until activation, which follows the install.
        let mut plugin = self
            .plugin_lifecycle
            .prepare_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)?;
        let generation = self.stage_event_generation(package_name, staged_plane)?;
        plugin.set_event_generation(generation);
        let cleanup = self.commit_reloaded_plugin(request_id, plugin);
        self.activate_event_generation()?;
        Ok(cleanup)
    }

    /// Swap in a prepared plugin and release the previous one's capabilities.
    fn commit_reloaded_plugin(
        &mut self,
        request_id: RequestId,
        plugin: crate::lifecycle::PreparedPluginLoad,
    ) -> PluginCleanupResult {
        let plugin_key = PluginKey(plugin.package_name().to_string());
        let capability_cleanup = self.cleanup_plugin_capabilities(&plugin_key).ok();
        let (_, mut lifecycle_cleanup) = self.plugin_lifecycle.commit_package(request_id, plugin);
        if let Some(cleanup) = capability_cleanup {
            lifecycle_cleanup
                .removed_resources
                .extend(cleanup.removed_resources.clone());
            self.last_capability_cleanup = Some(cleanup);
        }
        lifecycle_cleanup
    }

    fn staged_package_event_plane(
        &self,
        package_name: &str,
        registry: &PackageRegistry,
        event_handlers: &[crate::lifecycle::HubPluginEventHandler],
    ) -> Result<PendingEventPlaneReplace, EventPlaneStatus> {
        let contracts = match registry.package(package_name) {
            Some(record) => record
                .manifest
                .compiled_event_contracts()
                .map_err(|_| EventPlaneStatus::RejectedInvalid)?,
            None => Vec::new(),
        };
        let subscriptions = event_handlers
            .iter()
            .filter(|handler| {
                !(handler.event_owner == crate::package_event_router::HUB_EVENT_OWNER
                    && handler.event_name == "session_family")
            })
            .map(|handler| EventSubscription {
                plugin_key: package_name.to_string(),
                owner: handler.event_owner.clone(),
                name: handler.event_name.clone(),
                handler_id: handler.handler.handler_id.clone(),
                generation: self.package_event_router.next_holder_generation(),
                ..EventSubscription::default()
            })
            .collect();
        Ok(PendingEventPlaneReplace {
            contracts,
            subscriptions,
        })
    }

    pub fn reload_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginCleanupResult> {
        let plugin_key = PluginKey(package_name.to_string());
        let capability_cleanup = self.cleanup_plugin_capabilities(&plugin_key).ok();
        let mut lifecycle_cleanup =
            self.plugin_lifecycle
                .reload_package(request_id, registry, package_name, bundle)?;
        if let Some(cleanup) = capability_cleanup {
            lifecycle_cleanup
                .removed_resources
                .extend(cleanup.removed_resources.clone());
            self.last_capability_cleanup = Some(cleanup);
        }
        Ok(lifecycle_cleanup)
    }

    pub fn unload_plugin_package(
        &mut self,
        request_id: RequestId,
        package_name: &str,
    ) -> PluginCleanupResult {
        // Capture family identities before the lifecycle removes their descriptors.
        self.unloaded_families.push((
            package_name.to_string(),
            self.plugin_lifecycle
                .entity_provider_families_for(package_name),
        ));
        let plugin_key = PluginKey(package_name.to_string());
        let capability_cleanup = self.cleanup_plugin_capabilities(&plugin_key).ok();
        let mut lifecycle_cleanup = self
            .plugin_lifecycle
            .unload_package(request_id, package_name);
        if let Some(cleanup) = capability_cleanup {
            lifecycle_cleanup
                .removed_resources
                .extend(cleanup.removed_resources.clone());
            self.last_capability_cleanup = Some(cleanup);
        }
        lifecycle_cleanup
    }

    pub fn cleanup_plugin_capabilities(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<PluginCleanupResult, botster_core::CapabilityRuntimeError> {
        let cleanup = self
            .capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .cleanup_plugin(plugin_key)?;
        self.last_capability_cleanup = Some(cleanup.clone());
        Ok(cleanup)
    }
}
