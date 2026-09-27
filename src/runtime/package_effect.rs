//! Runtime handles used by typed package effects on host workers.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use super::{HubLuaPluginLoadError, PendingEventPlaneReplace};
use crate::lifecycle::{HubLifecycleResult, HubPluginLifecycle, HubPluginRuntimeBundle};
use crate::lua_runtime::{LuaPluginHostApi, LuaPluginRuntime, SharedHubCapabilityRuntime};
use crate::package_event_router::{EventPlaneStatus, EventSubscription, PackageEventRouter};
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
}

impl HostPackageRuntime {
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
        // Every fallible step runs before the plugin changes. A failed event
        // plane commit restores its own admission state, so each error below
        // leaves any previously loaded version serving.
        let plugin = self
            .plugin_lifecycle
            .prepare_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)?;
        self.commit_loaded_package_event_plane(package_name, registry, &event_handlers)
            .map_err(HubLuaPluginLoadError::EventPlane)?;
        let (key, _) = self
            .plugin_lifecycle
            .commit_package(RequestId("hub-load-registration".into()), plugin);
        Ok(key)
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
        let staged = self
            .staged_package_event_plane(package_name, registry, &event_handlers)
            .map_err(HubLuaPluginLoadError::EventPlane)?;
        // Prepare the plugin before the event plane changes, so every
        // rejection leaves the previous version serving.
        let plugin = self
            .plugin_lifecycle
            .prepare_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)?;
        if let Err(error) = self.package_event_router.try_replace_package_generation(
            package_name,
            staged.contracts,
            staged.subscriptions,
        ) {
            let unloaded = error.unloaded();
            let (result, cleanup) = error.into_parts();
            match (result, cleanup) {
                (Err(status), None) if !unloaded => {
                    return Err(HubLuaPluginLoadError::EventPlane(status));
                }
                (Err(status), cleanup) => {
                    // The previous generation is unloaded and the new one is
                    // not committed; the previous plugin still runs.
                    if let Some(cleanup) = cleanup {
                        self.event_plane_faults.push((Err(status), cleanup));
                    }
                    return Err(HubLuaPluginLoadError::EventPlaneStranded(status));
                }
                (Ok(generation), Some(cleanup)) => {
                    // The new generation is committed; only the previous
                    // generation's cleanup failed. Finish the swap so one
                    // version serves, and retain the cleanup fault.
                    self.event_plane_faults.push((Ok(generation), cleanup));
                }
                (Ok(_), None) => unreachable!("a replacement error carries a failure"),
            }
        }
        Ok(self.commit_reloaded_plugin(request_id, plugin))
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

    fn commit_loaded_package_event_plane(
        &self,
        package_name: &str,
        registry: &PackageRegistry,
        event_handlers: &[crate::lifecycle::HubPluginEventHandler],
    ) -> Result<u64, EventPlaneStatus> {
        let staged = self.staged_package_event_plane(package_name, registry, event_handlers)?;
        self.package_event_router.try_commit_package_generation(
            package_name,
            staged.contracts,
            staged.subscriptions,
        )
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
