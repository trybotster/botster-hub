//! Hub-owned plugin lifecycle adapter over `botster-core` workers.
//!
//! Package grant and admission policy remains in [`crate::packages`]. This
//! adapter only refuses packages that are not currently enabled, then delegates
//! load, invoke, reload, unload, and cleanup mechanics to `botster-core`.

mod entity_providers;
pub(crate) use entity_providers::{EntityProviderRegistration, EntityProviderRegistrations};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use botster_core::{
    BoundaryJson, PluginAdmissionResult, PluginCleanupResult, PluginCleanupScope,
    PluginCompletionDrain, PluginCompletionNotifier, PluginDescriptorKind,
    PluginHandlerRegistration, PluginInvocationClass, PluginInvocationOutcome,
    PluginInvocationRequest, PluginKey, PluginLoadSpec, PluginOwnedDescriptor, PluginResourceRef,
    PluginRuntime, PluginUnloadSpec, PluginWorkerDebugSnapshot, PluginWorkerEngine,
    PluginWorkerEngineConfig, PluginWorkerRegistration, RequestId,
};

use crate::packages::{PackageClassification, PackageRecord, PackageRegistry, PackageState};

pub(crate) const PACKAGE_EVENT_INVOCATION_ORIGIN: &str = "package-event";
pub(crate) const SESSION_FAMILY_INVOCATION_ORIGIN: &str = "session-family";

pub(crate) const REQUEST_RESPONSE_COMPLETION_ALLOWANCE: usize = 1024 * 1024;
pub(crate) const BACKGROUND_COMPLETION_ALLOWANCE: usize = 4 * 1024;

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
    load_failures: Arc<Mutex<BTreeMap<String, HubPluginLoadFailure>>>,
    descriptors: Arc<Mutex<BTreeMap<String, Vec<PluginOwnedDescriptor>>>>,
    entity_providers: EntityProviderRegistrations,
    event_handlers: Arc<Mutex<BTreeMap<String, Vec<HubPluginEventHandler>>>>,
}

impl HubPluginLifecycle {
    /// Build a lifecycle adapter with explicit core plugin worker config.
    #[must_use]
    pub fn with_config(config: PluginWorkerEngineConfig) -> Self {
        Self {
            engine: PluginWorkerEngine::with_config(config),
            loaded: Arc::new(Mutex::new(BTreeSet::new())),
            load_failures: Arc::new(Mutex::new(BTreeMap::new())),
            descriptors: Arc::new(Mutex::new(BTreeMap::new())),
            entity_providers: EntityProviderRegistrations::default(),
            event_handlers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn entity_provider_registrations(&self) -> EntityProviderRegistrations {
        self.entity_providers.clone()
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
        let entity_providers = self.entity_providers.prepare(&plugin_key.0, &descriptors);
        let registration = registration_for(record, plugin_key.clone(), bundle)?;

        // Core must stop the previous worker before its registrations change.
        // The registration index remains unlocked while Core joins workers.
        self.engine.unload_plugin(PluginUnloadSpec {
            request_id: RequestId("hub-load-registration".into()),
            plugin_key: plugin_key.clone(),
            cleanup: PluginCleanupScope::DescriptorsAndResources,
        });
        self.entity_providers.replace(entity_providers);
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
        self.load_failures
            .lock()
            .expect("hub plugin lifecycle load failures lock")
            .remove(&plugin_key.0);

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
        let allowance = match class {
            PluginInvocationClass::RequestResponse => REQUEST_RESPONSE_COMPLETION_ALLOWANCE,
            PluginInvocationClass::Background => BACKGROUND_COMPLETION_ALLOWANCE,
            _ => {
                return PluginAdmissionResult::RejectedBudget {
                    request_id: request.request_id,
                    class,
                    queue_bytes: None,
                    reason: "Hub does not support this invocation class".to_string(),
                };
            }
        };
        self.engine.try_admit(class, request, allowance)
    }

    /// Drain previously published async completions without waiting.
    #[must_use]
    pub fn drain_completions(&self, max_items: usize, max_bytes: usize) -> PluginCompletionDrain {
        self.engine.drain_completions(max_items, max_bytes)
    }

    /// Install Hub's owner-loop notification for published completions.
    pub fn install_completion_notifier(&self, notifier: PluginCompletionNotifier) {
        self.engine.install_completion_notifier(notifier);
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
        let entity_providers = self.entity_providers.prepare(&plugin_key.0, &descriptors);
        let registration = registration_for(record, plugin_key.clone(), bundle)?;
        let cleanup = self.engine.unload_plugin(PluginUnloadSpec {
            request_id,
            plugin_key: plugin_key.clone(),
            cleanup: PluginCleanupScope::DescriptorsAndResources,
        });
        self.entity_providers.replace(entity_providers);
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
        self.load_failures
            .lock()
            .expect("hub plugin lifecycle load failures lock")
            .remove(&plugin_key.0);

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
        self.entity_providers.retire(package_name);
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
        self.load_failures
            .lock()
            .expect("hub plugin lifecycle load failures lock")
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

    /// Read one exact provider without cloning its descriptor body.
    pub(crate) fn with_entity_provider_descriptor<T>(
        &self,
        entity_type: &str,
        read: impl FnOnce(Option<&PluginOwnedDescriptor>) -> T,
    ) -> T {
        let descriptors = self
            .descriptors
            .lock()
            .expect("hub plugin lifecycle descriptors lock");
        read(descriptors.values().flatten().find(|descriptor| {
            descriptor.descriptor.kind == PluginDescriptorKind::EntityProvider
                && descriptor.descriptor.descriptor_id == entity_type
        }))
    }

    /// Return Event-kind plugin handlers subscribed to one exact event name.
    #[must_use]
    pub fn event_handlers_for(&self, event_name: &str) -> Vec<HubPluginEventHandler> {
        self.event_handlers_for_page(event_name, None, usize::MAX).0
    }

    /// Return the handler for one exact owner+name subscription on a plugin.
    #[must_use]
    pub fn event_handler_for(
        &self,
        plugin_key: &str,
        owner: &str,
        event_name: &str,
        handler_id: &str,
    ) -> Option<HubPluginEventHandler> {
        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .get(plugin_key)
            .into_iter()
            .flatten()
            .find(|handler| {
                handler.event_owner == owner
                    && handler.event_name == event_name
                    && handler.handler.handler_id == handler_id
            })
            .cloned()
    }

    /// Return one bounded page of Event-kind handlers for an exact event name.
    ///
    /// `max_items` counts visited plugin keys, including keys that do not
    /// match `event_name`. The second return value is the last visited key.
    #[must_use]
    pub fn event_handlers_for_page(
        &self,
        event_name: &str,
        after_plugin_key: Option<&str>,
        max_items: usize,
    ) -> (Vec<HubPluginEventHandler>, Option<String>, usize, bool) {
        let lock = self
            .event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock");
        let start = match after_plugin_key {
            Some(key) => std::ops::Bound::Excluded(key),
            None => std::ops::Bound::Unbounded,
        };
        let mut page = Vec::new();
        let mut last_key = after_plugin_key.map(str::to_string);
        let mut visited = 0;
        let mut more = false;
        for (key, handlers) in lock.range::<str, _>((start, std::ops::Bound::Unbounded)) {
            if visited >= max_items {
                more = true;
                break;
            }
            last_key = Some(key.clone());
            visited += 1;
            page.extend(
                handlers
                    .iter()
                    .filter(|handler| handler.event_name == event_name)
                    .cloned(),
            );
        }
        (page, last_key, visited, more)
    }

    #[cfg(test)]
    pub fn insert_test_event_handler(&self, plugin_key: &str, event_name: &str) {
        use botster_core::{PluginHandlerKind, PluginHandlerRef, PluginKey};

        self.event_handlers
            .lock()
            .expect("hub plugin lifecycle event handlers lock")
            .entry(plugin_key.to_string())
            .or_default()
            .push(HubPluginEventHandler {
                event_owner: "hub".to_string(),
                event_name: event_name.to_string(),
                handler: PluginHandlerRef {
                    plugin_key: PluginKey(plugin_key.to_string()),
                    kind: PluginHandlerKind::Event,
                    handler_id: event_name.to_string(),
                },
            });
    }

    /// Return package-level lifecycle status without exposing core worker internals.
    #[must_use]
    pub fn status(&self, registry: &PackageRegistry) -> Vec<HubPluginLifecycleStatus> {
        let loaded = self
            .loaded
            .lock()
            .expect("hub plugin lifecycle loaded set lock")
            .clone();
        let load_failures = self
            .load_failures
            .lock()
            .expect("hub plugin lifecycle load failures lock")
            .clone();

        registry
            .packages()
            .into_iter()
            .filter(|record| record.classification == PackageClassification::Plugin)
            .map(|record| HubPluginLifecycleStatus {
                package_name: record.manifest.name.clone(),
                state: record.state,
                loaded: loaded.contains(&record.manifest.name),
                load_failure: load_failures.get(&record.manifest.name).cloned(),
            })
            .collect()
    }

    /// Record one package-scoped startup load failure for operator inspection.
    pub fn record_load_failure(&self, package_name: &str, failure: HubPluginLoadFailure) {
        self.load_failures
            .lock()
            .expect("hub plugin lifecycle load failures lock")
            .insert(package_name.to_string(), failure);
    }
}

/// One package-scoped plugin load failure that startup isolated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubPluginLoadFailure {
    pub code: String,
    pub message: String,
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
    /// Package-scoped startup failure when the enabled package stayed unloaded.
    pub load_failure: Option<HubPluginLoadFailure>,
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
    /// Exact owner string passed to `events.on(owner, name, ...)`.
    pub event_owner: String,
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

#[cfg(test)]
mod completion_tests {
    use super::*;
    use botster_core::{
        PluginCancellationToken, PluginHandlerKind, PluginHandlerRef, PluginInvocationContext,
        PluginInvocationFailureKind, PluginInvocationResult, PluginInvocationSuccess,
    };
    use std::time::{Duration, Instant};

    struct ResultRuntime(usize);

    impl PluginRuntime for ResultRuntime {
        fn invoke(
            &self,
            request: PluginInvocationRequest,
            _cancellation: PluginCancellationToken,
        ) -> PluginInvocationResult {
            PluginInvocationResult::Completed(PluginInvocationSuccess {
                request_id: request.request_id,
                handler: request.handler,
                payload: Some(BoundaryJson(serde_json::json!("x".repeat(self.0)))),
            })
        }
    }

    fn lifecycle(result_bytes: usize) -> HubPluginLifecycle {
        let lifecycle = HubPluginLifecycle::with_config(
            crate::config::CoreEngineOptions::default().plugin_worker_config(),
        );
        for name in ["first", "second"] {
            let key = PluginKey(name.to_string());
            lifecycle.engine.load_plugin(PluginWorkerRegistration {
                load: PluginLoadSpec {
                    plugin_key: key.clone(),
                    package: name.to_string(),
                    entrypoint: "plugin.lua".to_string(),
                    descriptors: Vec::new(),
                    metadata: None,
                },
                manifest: botster_core::PackageManifest {
                    name: name.to_string(),
                    version: "0.1.0".to_string(),
                    kind: botster_core::ExtensionKind::Plugin,
                    botster: ">=0.1.0".to_string(),
                    source: None,
                    capabilities: Vec::new(),
                    entrypoints: Vec::new(),
                    dependencies: Vec::new(),
                    features: Vec::new(),
                    host_profile: None,
                    configuration: None,
                    runnable_entrypoints: Vec::new(),
                },
                runtime: Arc::new(ResultRuntime(result_bytes)),
                handlers: vec![PluginHandlerRegistration {
                    handler: handler(name, "run"),
                    required_capability: None,
                }],
                resources: Vec::new(),
            });
        }
        lifecycle
    }

    fn handler(plugin: &str, name: &str) -> PluginHandlerRef {
        PluginHandlerRef {
            plugin_key: PluginKey(plugin.to_string()),
            kind: PluginHandlerKind::Command,
            handler_id: name.to_string(),
        }
    }

    fn request(id: &str, handler: PluginHandlerRef, bytes: usize) -> PluginInvocationRequest {
        PluginInvocationRequest {
            request_id: RequestId(id.to_string()),
            handler,
            timeout_ms: 5_000,
            context: PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: None,
                metadata: None,
            },
            payload: BoundaryJson(serde_json::json!("x".repeat(bytes))),
        }
    }

    fn admit(
        lifecycle: &HubPluginLifecycle,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> PluginAdmissionResult {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let result = lifecycle.try_admit(class, request.clone());
            if matches!(&result, PluginAdmissionResult::Backpressured { reason, .. }
                if reason == "admission lock busy")
            {
                assert!(Instant::now() < deadline, "admission lock remained busy");
                std::thread::yield_now();
            } else {
                if let PluginAdmissionResult::Queued {
                    reservation_bytes, ..
                } = &result
                {
                    let payload_bytes = reservation_bytes
                        .checked_sub(PluginWorkerEngine::completion_reservation_metadata_bytes())
                        .expect("Core charges metadata in every reservation");
                    let config = crate::config::CoreEngineOptions::default().plugin_worker_config();
                    assert!(payload_bytes <= config.completion_reservation_byte_capacity);
                    assert!(
                        payload_bytes
                            <= crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY
                    );
                    assert!(*reservation_bytes <= config.completion_queue_byte_capacity);
                }
                return result;
            }
        }
    }

    fn drain(lifecycle: &HubPluginLifecycle) -> botster_core::PluginCompletion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut drained = lifecycle.drain_completions(
                1,
                crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY,
            );
            if let Some(item) = drained.completions.pop() {
                assert!(
                    item.encoded_len
                        <= crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY
                );
                return item.completion;
            }
            assert!(
                Instant::now() < deadline,
                "completion did not fit Hub drain"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn completion_allowance_accepts_large_result_from_small_request() {
        let lifecycle = lifecycle(900 * 1024);
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::RequestResponse,
                request("large-result", handler("first", "run"), 0)
            ),
            PluginAdmissionResult::Queued {
                reservation_bytes,
                ..
            } if reservation_bytes == REQUEST_RESPONSE_COMPLETION_ALLOWANCE
                + PluginWorkerEngine::completion_reservation_metadata_bytes()
        ));
        let PluginInvocationResult::Completed(result) = drain(&lifecycle).result else {
            panic!("valid large result failed");
        };
        assert_eq!(
            result.payload.unwrap().0.as_str().unwrap().len(),
            900 * 1024
        );
    }

    #[test]
    fn completion_allowance_replaces_oversized_result() {
        for (class, bytes) in [
            (
                PluginInvocationClass::RequestResponse,
                REQUEST_RESPONSE_COMPLETION_ALLOWANCE,
            ),
            (
                PluginInvocationClass::Background,
                BACKGROUND_COMPLETION_ALLOWANCE,
            ),
        ] {
            let lifecycle = lifecycle(bytes);
            assert!(matches!(
                admit(
                    &lifecycle,
                    class,
                    request("oversized", handler("first", "run"), 0)
                ),
                PluginAdmissionResult::Queued { .. }
            ));
            assert!(
                matches!(drain(&lifecycle).result, PluginInvocationResult::Failed(failure)
                if failure.kind == PluginInvocationFailureKind::CompletionTooLarge)
            );
        }
    }

    #[test]
    fn completion_allowance_accounts_for_large_background_input() {
        let lifecycle = lifecycle(512 * 1024);
        let input = request("large-input", handler("first", "run"), 900 * 1024);
        let input_bytes = serde_json::to_vec(&input).unwrap().len();
        assert!(
            matches!(admit(&lifecycle, PluginInvocationClass::Background, input),
            PluginAdmissionResult::Queued { reservation_bytes, .. }
                if reservation_bytes.checked_sub(PluginWorkerEngine::completion_reservation_metadata_bytes())
                    .expect("metadata charged") >= input_bytes)
        );
        assert!(matches!(
            drain(&lifecycle).result,
            PluginInvocationResult::Completed(_)
        ));
    }

    #[test]
    fn completion_allowance_shares_pool_across_plugins_until_drain() {
        let lifecycle = lifecycle(0);
        let config = crate::config::CoreEngineOptions::default().plugin_worker_config();
        let reservation = REQUEST_RESPONSE_COMPLETION_ALLOWANCE
            + PluginWorkerEngine::completion_reservation_metadata_bytes();
        let admitted = config.completion_queue_byte_capacity / reservation;
        assert!(admitted >= 2, "the fixture exercises both plugins");
        assert!(
            admitted < config.completion_queue_capacity,
            "bytes limit this fixture"
        );
        for index in 0..admitted {
            let plugin = if index % 2 == 0 { "first" } else { "second" };
            assert!(matches!(
                admit(
                    &lifecycle,
                    PluginInvocationClass::RequestResponse,
                    request(&index.to_string(), handler(plugin, "missing"), 0)
                ),
                PluginAdmissionResult::Queued { reservation_bytes, .. }
                    if reservation_bytes == reservation
            ));
        }
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::RequestResponse,
                request("saturated", handler("first", "missing"), 0)
            ),
            PluginAdmissionResult::Backpressured { reason, .. }
                if reason == "plugin completion reservation pool is at capacity"
        ));
        let snapshot = lifecycle.debug_snapshot();
        assert_eq!(snapshot.reserved_completion_bytes, admitted * reservation);
        assert_eq!(snapshot.reserved_completion_count, admitted);
        assert!(
            config.completion_queue_byte_capacity - snapshot.reserved_completion_bytes
                < reservation
        );
        assert!(snapshot.reserved_completion_count < snapshot.configured_completion_queue_capacity);
        drain(&lifecycle);
        let released = lifecycle.debug_snapshot();
        assert_eq!(released.reserved_completion_count, admitted - 1);
        assert_eq!(
            released.reserved_completion_bytes,
            (admitted - 1) * reservation
        );
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::RequestResponse,
                request("after-drain", handler("second", "missing"), 0)
            ),
            PluginAdmissionResult::Queued { reservation_bytes, .. }
                if reservation_bytes == reservation
        ));
        let restored = lifecycle.debug_snapshot();
        assert_eq!(restored.reserved_completion_count, admitted);
        assert_eq!(restored.reserved_completion_bytes, admitted * reservation);
    }

    #[test]
    fn completion_allowance_bounds_background_count_independently_of_bytes() {
        let lifecycle = lifecycle(0);
        let reservation = BACKGROUND_COMPLETION_ALLOWANCE
            + PluginWorkerEngine::completion_reservation_metadata_bytes();
        for index in 0..256 {
            let plugin = if index % 2 == 0 { "first" } else { "second" };
            assert!(matches!(
                admit(
                    &lifecycle,
                    PluginInvocationClass::Background,
                    request(&index.to_string(), handler(plugin, "missing"), 0)
                ),
                PluginAdmissionResult::Queued {
                    reservation_bytes,
                    ..
                } if reservation_bytes == reservation
            ));
        }
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::Background,
                request("count-full", handler("first", "missing"), 0)
            ),
            PluginAdmissionResult::Backpressured { reason, .. }
                if reason == "plugin completion reservation pool is at capacity"
        ));
        let snapshot = lifecycle.debug_snapshot();
        assert_eq!(snapshot.reserved_completion_count, 256);
        assert_eq!(snapshot.reserved_completion_bytes, 256 * reservation);
        assert!(
            snapshot.configured_completion_queue_byte_capacity - snapshot.reserved_completion_bytes
                >= reservation,
            "another whole reservation fits in bytes; count caused refusal"
        );
        drain(&lifecycle);
        let released = lifecycle.debug_snapshot();
        assert_eq!(released.reserved_completion_count, 255);
        assert_eq!(released.reserved_completion_bytes, 255 * reservation);
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::Background,
                request("after-drain", handler("second", "missing"), 0)
            ),
            PluginAdmissionResult::Queued { reservation_bytes, .. }
                if reservation_bytes == reservation
        ));
        let restored = lifecycle.debug_snapshot();
        assert_eq!(restored.reserved_completion_count, 256);
        assert_eq!(restored.reserved_completion_bytes, 256 * reservation);
    }

    #[test]
    fn completion_allowance_includes_fallback_bytes_for_large_identity() {
        let lifecycle = lifecycle(0);
        let capacity = crate::config::CoreEngineOptions::default()
            .plugin_worker_config()
            .request_response_queue_byte_capacity;
        let mut input = request("", handler("first", "missing"), 0);
        let overhead = serde_json::to_vec(&input).unwrap().len();
        input.request_id = RequestId("x".repeat(capacity - overhead));
        assert_eq!(serde_json::to_vec(&input).unwrap().len(), capacity);
        let PluginAdmissionResult::Queued {
            queue_bytes,
            reservation_bytes,
            ..
        } = admit(&lifecycle, PluginInvocationClass::RequestResponse, input)
        else {
            panic!("request at the class limit must be admitted");
        };
        // This proof requires the class limit and allowance to coincide.
        // Redesign this test if either policy changes independently.
        assert_eq!(queue_bytes, capacity);
        assert_eq!(queue_bytes, REQUEST_RESPONSE_COMPLETION_ALLOWANCE);
        // Core's current oversize error envelope adds seven bytes to this request.
        let payload_bytes = reservation_bytes
            .checked_sub(PluginWorkerEngine::completion_reservation_metadata_bytes())
            .expect("metadata charged");
        assert!(payload_bytes > queue_bytes);
        assert!(
            matches!(drain(&lifecycle).result, PluginInvocationResult::Failed(failure)
            if failure.kind == PluginInvocationFailureKind::HandlerFailed)
        );
    }

    #[test]
    fn completion_allowance_rejects_unknown_handler_input_above_class_capacity() {
        let lifecycle = lifecycle(0);
        assert!(matches!(
            admit(
                &lifecycle,
                PluginInvocationClass::RequestResponse,
                request(
                    "large-unknown",
                    handler("first", "missing"),
                    9 * 1024 * 1024
                )
            ),
            PluginAdmissionResult::RejectedBudget { reason, .. }
                if reason == "plugin invocation exceeds class byte capacity"
        ));
        assert_eq!(lifecycle.debug_snapshot().undrained_completions, 0);
    }
}
