//! Hub-owned plugin lifecycle adapter over `botster-core` workers.
//!
//! Package grant and admission policy remains in [`crate::packages`]. This
//! adapter only refuses packages that are not currently enabled, then delegates
//! load, invoke, reload, unload, and cleanup mechanics to `botster-core`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use botster_core::{
    BoundaryJson, PluginCleanupResult, PluginCleanupScope, PluginDescriptorKind,
    PluginHandlerRegistration, PluginInvocationOutcome, PluginInvocationRequest, PluginKey,
    PluginLoadSpec, PluginOwnedDescriptor, PluginReloadSpec, PluginResourceRef, PluginRuntime,
    PluginUnloadSpec, PluginWorkerDebugSnapshot, PluginWorkerEngine, PluginWorkerEngineConfig,
    PluginWorkerRegistration, RequestId,
};

use crate::packages::{PackageClassification, PackageRecord, PackageRegistry, PackageState};

/// Hub-owned lifecycle adapter around core plugin worker mechanics.
#[derive(Clone)]
pub struct HubPluginLifecycle {
    engine: PluginWorkerEngine,
    loaded: Arc<Mutex<BTreeSet<String>>>,
    descriptors: Arc<Mutex<BTreeMap<String, Vec<PluginOwnedDescriptor>>>>,
    event_handlers: Arc<Mutex<BTreeMap<String, Vec<HubPluginEventHandler>>>>,
}

impl HubPluginLifecycle {
    /// Build a lifecycle adapter with explicit core plugin worker config.
    #[must_use]
    pub fn with_config(config: PluginWorkerEngineConfig) -> Self {
        Self {
            engine: PluginWorkerEngine::with_config(config),
            loaded: Arc::new(Mutex::new(BTreeSet::new())),
            descriptors: Arc::new(Mutex::new(BTreeMap::new())),
            event_handlers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Return Core's authoritative read-only plugin worker snapshot.
    #[must_use]
    pub fn debug_snapshot(&self) -> PluginWorkerDebugSnapshot {
        self.engine.debug_snapshot()
    }

    /// Load an installed-and-enabled package through core worker registration.
    pub fn load_package(
        &self,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginKey> {
        let record = enabled_record(registry, package_name)?;
        let plugin_key = plugin_key_for(record);
        let descriptors = bundle.descriptors.clone();
        let event_handlers = bundle.event_handlers.clone();
        let registration = registration_for(record, plugin_key.clone(), bundle)?;

        self.engine.load_plugin(registration);
        self.loaded
            .lock()
            .expect("hub plugin lifecycle loaded set lock")
            .insert(plugin_key.0.clone());
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .insert(plugin_key.0.clone(), descriptors);
        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .insert(plugin_key.0.clone(), event_handlers);

        Ok(plugin_key)
    }

    /// Invoke a plugin handler through core worker dispatch and capability checks.
    #[must_use]
    pub fn invoke(&self, request: PluginInvocationRequest) -> PluginInvocationOutcome {
        self.engine.invoke(request)
    }

    /// Reload an enabled package through core worker reload cleanup and replacement.
    pub fn reload_package(
        &self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginCleanupResult> {
        let record = enabled_record(registry, package_name)?;
        let plugin_key = plugin_key_for(record);
        let descriptors = bundle.descriptors.clone();
        let event_handlers = bundle.event_handlers.clone();
        let registration = registration_for(record, plugin_key.clone(), bundle)?;
        let cleanup = self.engine.reload_plugin(
            PluginReloadSpec {
                request_id,
                plugin_key: plugin_key.clone(),
                load: registration.load.clone(),
                cleanup: PluginCleanupScope::DescriptorsAndResources,
            },
            registration,
        );
        self.loaded
            .lock()
            .expect("hub plugin lifecycle loaded set lock")
            .insert(plugin_key.0.clone());
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .insert(plugin_key.0.clone(), descriptors);
        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .insert(plugin_key.0.clone(), event_handlers);

        Ok(cleanup)
    }

    /// Unload a plugin worker through core cleanup mechanics.
    #[must_use]
    pub fn unload_package(&self, request_id: RequestId, package_name: &str) -> PluginCleanupResult {
        let plugin_key = PluginKey(package_name.to_string());
        let cleanup = self.engine.unload_plugin(PluginUnloadSpec {
            request_id,
            plugin_key,
            cleanup: PluginCleanupScope::DescriptorsAndResources,
        });
        self.loaded
            .lock()
            .expect("hub plugin lifecycle loaded set lock")
            .remove(package_name);
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .remove(package_name);
        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .remove(package_name);
        cleanup
    }

    /// Return plugin-owned MCP tool descriptors with handler refs for daemon-backed MCP routing.
    #[must_use]
    pub fn mcp_tool_descriptors(&self) -> Vec<PluginOwnedDescriptor> {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .values()
            .flat_map(|descriptors| descriptors.iter().cloned())
            .filter(|descriptor| descriptor.descriptor.kind == PluginDescriptorKind::McpTool)
            .collect()
    }

    /// Return plugin-owned surface route descriptors with handler refs.
    #[must_use]
    pub fn surface_route_descriptors(&self) -> Vec<PluginOwnedDescriptor> {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .values()
            .flat_map(|descriptors| descriptors.iter().cloned())
            .filter(|descriptor| descriptor.descriptor.kind == PluginDescriptorKind::SurfaceRoute)
            .collect()
    }

    /// Return plugin-owned UI action descriptors with handler refs.
    #[must_use]
    pub fn ui_action_descriptors(&self) -> Vec<PluginOwnedDescriptor> {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .values()
            .flat_map(|descriptors| descriptors.iter().cloned())
            .filter(|descriptor| descriptor.descriptor.kind == PluginDescriptorKind::UiAction)
            .collect()
    }

    /// Return Event-kind plugin handlers subscribed to one exact event name.
    #[must_use]
    pub fn event_handlers_for(&self, event_name: &str) -> Vec<HubPluginEventHandler> {
        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .values()
            .flat_map(|handlers| handlers.iter().cloned())
            .filter(|handler| handler.event_name == event_name)
            .collect()
    }

    /// Return package-level lifecycle status without exposing core worker internals.
    #[must_use]
    pub fn status(&self, registry: &PackageRegistry) -> Vec<HubPluginLifecycleStatus> {
        let loaded = self
            .loaded
            .lock()
            .expect("hub plugin lifecycle loaded set lock")
            .clone();

        registry
            .packages()
            .into_iter()
            .filter(|record| record.classification == PackageClassification::Plugin)
            .map(|record| HubPluginLifecycleStatus {
                package_name: record.manifest.name.clone(),
                state: record.state,
                loaded: loaded.contains(&record.manifest.name),
            })
            .collect()
    }
}

/// Read-only plugin package lifecycle status visible to local clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubPluginLifecycleStatus {
    /// Package name from the hub package registry.
    pub package_name: String,
    /// Hub package policy state.
    pub state: PackageState,
    /// Whether this lifecycle adapter has loaded the package into a core worker.
    pub loaded: bool,
}

/// Host-supplied executable runtime state for one package load.
#[derive(Clone)]
pub struct HubPluginRuntimeBundle {
    /// Runtime implementation supplied by the hub host.
    pub runtime: Arc<dyn PluginRuntime>,
    /// Stable handlers exposed by this package.
    pub handlers: Vec<PluginHandlerRegistration>,
    /// Event-kind handlers and the exact event names they subscribe to.
    pub event_handlers: Vec<HubPluginEventHandler>,
    /// Plugin-owned descriptors exposed to the hub.
    pub descriptors: Vec<PluginOwnedDescriptor>,
    /// Runtime resources owned by this package.
    pub resources: Vec<PluginResourceRef>,
    /// Optional explicit entrypoint. Defaults to the first manifest entrypoint.
    pub entrypoint: Option<String>,
    /// Optional plugin-owned load metadata.
    pub metadata: Option<BoundaryJson>,
}

/// Hub-owned event subscription metadata for one plugin handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubPluginEventHandler {
    /// Exact event name passed to `events.on(...)`.
    pub event_name: String,
    /// Stable handler address invoked through the core plugin worker.
    pub handler: botster_core::PluginHandlerRef,
}

/// Typed hub lifecycle denial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubLifecycleError {
    /// The requested package is not installed in the registry.
    PackageNotInstalled { package_name: String },
    /// The requested package exists but is not enabled by hub policy.
    PackageNotEnabled { package_name: String },
    /// The enabled package has no manifest entrypoint and no host override.
    MissingEntrypoint { package_name: String },
}

/// Result alias for hub lifecycle operations that can fail before core load.
pub type HubLifecycleResult<T> = Result<T, HubLifecycleError>;

fn enabled_record<'a>(
    registry: &'a PackageRegistry,
    package_name: &str,
) -> HubLifecycleResult<&'a PackageRecord> {
    let record =
        registry
            .package(package_name)
            .ok_or_else(|| HubLifecycleError::PackageNotInstalled {
                package_name: package_name.to_string(),
            })?;

    if !record.is_enabled() {
        return Err(HubLifecycleError::PackageNotEnabled {
            package_name: package_name.to_string(),
        });
    }

    Ok(record)
}

fn registration_for(
    record: &PackageRecord,
    plugin_key: PluginKey,
    bundle: HubPluginRuntimeBundle,
) -> HubLifecycleResult<PluginWorkerRegistration> {
    let entrypoint = bundle.entrypoint.or_else(|| {
        record
            .manifest
            .entrypoints
            .first()
            .map(|entrypoint| entrypoint.path.clone())
    });
    let Some(entrypoint) = entrypoint else {
        return Err(HubLifecycleError::MissingEntrypoint {
            package_name: record.manifest.name.clone(),
        });
    };

    Ok(PluginWorkerRegistration {
        load: PluginLoadSpec {
            plugin_key,
            package: record.manifest.name.clone(),
            entrypoint,
            descriptors: bundle.descriptors,
            metadata: bundle.metadata,
        },
        manifest: record.manifest.clone(),
        runtime: bundle.runtime,
        handlers: bundle.handlers,
        resources: bundle.resources,
    })
}

fn plugin_key_for(record: &PackageRecord) -> PluginKey {
    PluginKey(record.manifest.name.clone())
}
