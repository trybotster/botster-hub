//! Hub-owned plugin lifecycle adapter over `botster-core` workers.
//!
//! Package grant and admission policy remains in [`crate::packages`]. This
//! adapter only refuses packages that are not currently enabled, then delegates
//! load, invoke, reload, unload, and cleanup mechanics to `botster-core`.

use std::sync::Arc;

use botster_core::{
    BoundaryJson, PluginCleanupResult, PluginCleanupScope, PluginHandlerRegistration,
    PluginInvocationOutcome, PluginInvocationRequest, PluginKey, PluginLoadSpec,
    PluginOwnedDescriptor, PluginReloadSpec, PluginResourceRef, PluginRuntime, PluginUnloadSpec,
    PluginWorkerEngine, PluginWorkerRegistration, RequestId,
};

use crate::packages::{PackageRecord, PackageRegistry};

/// Hub-owned lifecycle adapter around core plugin worker mechanics.
#[derive(Clone, Default)]
pub struct HubPluginLifecycle {
    engine: PluginWorkerEngine,
}

impl HubPluginLifecycle {
    /// Build a lifecycle adapter with core's default plugin worker config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: PluginWorkerEngine::new(),
        }
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
        let registration = registration_for(record, plugin_key.clone(), bundle)?;

        self.engine.load_plugin(registration);

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
        let registration = registration_for(record, plugin_key.clone(), bundle)?;
        let cleanup = self.engine.reload_plugin(
            PluginReloadSpec {
                request_id,
                plugin_key,
                load: registration.load.clone(),
                cleanup: PluginCleanupScope::DescriptorsAndResources,
            },
            registration,
        );

        Ok(cleanup)
    }

    /// Unload a plugin worker through core cleanup mechanics.
    #[must_use]
    pub fn unload_package(&self, request_id: RequestId, package_name: &str) -> PluginCleanupResult {
        let plugin_key = PluginKey(package_name.to_string());
        self.engine.unload_plugin(PluginUnloadSpec {
            request_id,
            plugin_key,
            cleanup: PluginCleanupScope::DescriptorsAndResources,
        })
    }
}

/// Host-supplied executable runtime state for one package load.
#[derive(Clone)]
pub struct HubPluginRuntimeBundle {
    /// Runtime implementation supplied by the hub host.
    pub runtime: Arc<dyn PluginRuntime>,
    /// Stable handlers exposed by this package.
    pub handlers: Vec<PluginHandlerRegistration>,
    /// Plugin-owned descriptors exposed to the hub.
    pub descriptors: Vec<PluginOwnedDescriptor>,
    /// Runtime resources owned by this package.
    pub resources: Vec<PluginResourceRef>,
    /// Optional explicit entrypoint. Defaults to the first manifest entrypoint.
    pub entrypoint: Option<String>,
    /// Optional plugin-owned load metadata.
    pub metadata: Option<BoundaryJson>,
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
