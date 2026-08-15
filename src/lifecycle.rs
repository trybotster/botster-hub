//! Hub-owned plugin lifecycle adapter over `botster-core` workers.
//!
//! Package grant and admission policy remains in [`crate::packages`]. This
//! adapter only refuses packages that are not currently enabled, then delegates
//! load, invoke, reload, unload, and cleanup mechanics to `botster-core`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use botster_core::{
    BoundaryJson, PluginAdmissionResult, PluginCleanupResult, PluginCleanupScope,
    PluginCompletionDrain, PluginDescriptorKind, PluginHandlerRegistration, PluginInvocationClass,
    PluginInvocationOutcome, PluginInvocationRequest, PluginKey, PluginLoadSpec,
    PluginOwnedDescriptor, PluginReloadSpec, PluginResourceRef, PluginRuntime, PluginUnloadSpec,
    PluginWorkerDebugSnapshot, PluginWorkerEngine, PluginWorkerEngineConfig,
    PluginWorkerRegistration, RequestId,
};

use crate::packages::{PackageClassification, PackageRecord, PackageRegistry, PackageState};

const PACKAGE_ENTITY_NAMESPACE_V1_MARKER: &str = "bns1_";

/// Map an exact package id to its canonical single-segment entity owner token.
#[must_use]
pub fn package_entity_owner_token(package_id: &str) -> String {
    if !package_id.is_empty()
        && !package_id.contains('.')
        && !package_id.starts_with(PACKAGE_ENTITY_NAMESPACE_V1_MARKER)
    {
        return package_id.to_string();
    }

    let mut token =
        String::with_capacity(PACKAGE_ENTITY_NAMESPACE_V1_MARKER.len() + package_id.len() * 2);
    token.push_str(PACKAGE_ENTITY_NAMESPACE_V1_MARKER);
    for byte in package_id.as_bytes() {
        write!(&mut token, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    token
}

/// Decode a canonical v1 entity owner token for round-trip contract tests.
#[cfg(test)]
fn package_id_from_entity_owner_token(token: &str) -> Result<String, String> {
    let Some(encoded) = token.strip_prefix(PACKAGE_ENTITY_NAMESPACE_V1_MARKER) else {
        if token.is_empty() || token.contains('.') {
            return Err(
                "identity entity owner token must be non-empty and single-segment".to_string(),
            );
        }
        return Ok(token.to_string());
    };
    if encoded.len() % 2 != 0
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(
            "encoded entity owner token requires canonical even-length lowercase hex".to_string(),
        );
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => unreachable!("hex was validated"),
            };
            digit(pair[0]) * 16 + digit(pair[1])
        })
        .collect::<Vec<_>>();
    let package_id = String::from_utf8(bytes)
        .map_err(|_| "encoded entity owner token must decode to valid UTF-8".to_string())?;
    if package_entity_owner_token(&package_id) != token {
        return Err("entity owner token is not the canonical v1 encoding".to_string());
    }
    Ok(package_id)
}

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

    /// Admit one invocation without waiting for execution or completion.
    #[must_use]
    pub fn try_admit(
        &self,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> PluginAdmissionResult {
        self.engine.try_admit(class, request)
    }

    /// Drain previously published async completions without waiting.
    #[must_use]
    pub fn drain_completions(&self, max_items: usize, max_bytes: usize) -> PluginCompletionDrain {
        self.engine.drain_completions(max_items, max_bytes)
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

    /// Return exact entity families provided by one loaded package.
    #[must_use]
    pub fn entity_provider_families_for(&self, package_name: &str) -> BTreeSet<String> {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .get(package_name)
            .into_iter()
            .flatten()
            .filter(|descriptor| descriptor.descriptor.kind == PluginDescriptorKind::EntityProvider)
            .map(|descriptor| descriptor.descriptor.descriptor_id.clone())
            .collect()
    }

    /// Return whether an exact entity family has a loaded provider.
    #[must_use]
    pub fn has_entity_provider_family(&self, entity_type: &str) -> bool {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .values()
            .flatten()
            .any(|descriptor| {
                descriptor.descriptor.kind == PluginDescriptorKind::EntityProvider
                    && descriptor.descriptor.descriptor_id == entity_type
            })
    }

    /// Return the one loaded provider descriptor for an exact entity family.
    #[must_use]
    pub fn entity_provider_descriptor(&self, entity_type: &str) -> Option<PluginOwnedDescriptor> {
        self.descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock")
            .values()
            .flatten()
            .find(|descriptor| {
                descriptor.descriptor.kind == PluginDescriptorKind::EntityProvider
                    && descriptor.descriptor.descriptor_id == entity_type
            })
            .cloned()
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
        manifest: record.manifest.core_execution_manifest(),
        runtime: bundle.runtime,
        handlers: bundle.handlers,
        resources: bundle.resources,
    })
}

fn plugin_key_for(record: &PackageRecord) -> PluginKey {
    PluginKey(record.manifest.name.clone())
}

#[cfg(test)]
mod namespace_tests {
    use super::{package_entity_owner_token, package_id_from_entity_owner_token};
    use std::collections::BTreeSet;

    #[test]
    fn package_entity_namespace_v1_is_canonical_reversible_and_collision_free() {
        let dotted_fixture = "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978";
        assert_eq!(
            package_entity_owner_token("project-pipelines"),
            "project-pipelines"
        );
        assert_eq!(
            package_entity_owner_token("botster.plugin-contract-matrix"),
            dotted_fixture
        );
        assert_eq!(package_entity_owner_token(""), "bns1_");
        assert_ne!(
            package_entity_owner_token("a.b"),
            package_entity_owner_token("a_b")
        );

        let values = [
            "project-pipelines",
            "botster.plugin-contract-matrix",
            "a.b",
            "a_b",
            "bns1_612e62",
            "会話.插件",
            "é.x",
            "e\u{301}.x",
            "",
        ];
        let tokens = values
            .iter()
            .map(|value| package_entity_owner_token(value))
            .collect::<Vec<_>>();
        assert_eq!(tokens.iter().collect::<BTreeSet<_>>().len(), values.len());
        for (value, token) in values.iter().zip(tokens) {
            assert!(!token.contains('.'));
            assert_eq!(
                package_id_from_entity_owner_token(&token).expect("canonical token decodes"),
                *value
            );
        }
    }

    #[test]
    fn package_entity_namespace_v1_rejects_noncanonical_marked_tokens() {
        for token in ["", "a.b", "bns1_0", "bns1_0A", "bns1_zz", "bns1_ff"] {
            assert!(
                package_id_from_entity_owner_token(token).is_err(),
                "{token}"
            );
        }
    }
}
