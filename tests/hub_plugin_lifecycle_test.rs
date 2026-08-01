use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use botster_core::{
    BoundaryJson, Capability, CapabilityOperation, CapabilityOperationId, CapabilityRuntimeEvent,
    CapabilityRuntimeRequest, CapabilitySurface, ExtensionEntrypoint, ExtensionKind,
    ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection, PackageSource,
    PluginCancellationToken, PluginDescriptorKind, PluginDescriptorRef, PluginHandlerKind,
    PluginHandlerRef, PluginHandlerRegistration, PluginInvocationContext, PluginInvocationFailure,
    PluginInvocationFailureKind, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor, PluginResourceKind,
    PluginResourceRef, PluginRuntime, RequestId, TimerCapabilityRequest,
};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions,
    HubLifecycleError, HubPackageManifest, HubPluginRuntimeBundle, HubRuntime, HubStartupOptions,
    HubStateStore, LOCAL_PACKAGE_MANIFEST_FILE, PackageProvenance, PackageRegistry,
    RuntimeEnvironment, SessionDefaults, TransportBindings,
};

#[derive(Clone)]
struct FakeRuntime {
    value: &'static str,
    invocations: Arc<Mutex<Vec<PluginInvocationRequest>>>,
    stopped: Arc<Mutex<Vec<PluginKey>>>,
}

impl FakeRuntime {
    fn new(value: &'static str) -> Self {
        Self {
            value,
            invocations: Arc::new(Mutex::new(Vec::new())),
            stopped: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn invocations(&self) -> Vec<PluginInvocationRequest> {
        self.invocations
            .lock()
            .expect("fake runtime invocations lock")
            .clone()
    }

    fn stopped(&self) -> Vec<PluginKey> {
        self.stopped
            .lock()
            .expect("fake runtime stopped lock")
            .clone()
    }
}

impl PluginRuntime for FakeRuntime {
    fn invoke(
        &self,
        request: PluginInvocationRequest,
        _cancellation: PluginCancellationToken,
    ) -> PluginInvocationResult {
        self.invocations
            .lock()
            .expect("fake runtime invocations lock")
            .push(request.clone());

        PluginInvocationResult::Completed(PluginInvocationSuccess {
            request_id: request.request_id,
            handler: request.handler,
            payload: Some(BoundaryJson(serde_json::json!({ "value": self.value }))),
        })
    }

    fn stop(&self, plugin_key: &PluginKey) {
        self.stopped
            .lock()
            .expect("fake runtime stopped lock")
            .push(plugin_key.clone());
    }
}

fn explicit_runtime() -> HubRuntime {
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-plugin-lifecycle-test".to_string(),
            display_name: "Hub Plugin Lifecycle Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            format!(
                "target/botster-hub-test-data/plugin-lifecycle-{}",
                std::process::id()
            )
            .into(),
        ),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn registry_with_grants(capabilities: Vec<Capability>) -> PackageRegistry {
    PackageRegistry::new(capabilities.into_iter().collect())
}

fn provenance() -> PackageProvenance {
    PackageProvenance {
        source: "https://example.invalid/botster/packages/lifecycle".to_string(),
        checksum: Some("sha256:lifecycle".to_string()),
    }
}

fn plugin_manifest(name: &str, capabilities: Vec<Capability>) -> HubPackageManifest {
    HubPackageManifest {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Plugin,
        botster: ">=0.1.0".to_string(),
        source: Some(PackageSource::Git {
            repo: "https://example.invalid/botster/plugin.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities,
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

fn local_plugin_manifest(name: &str, capabilities: Vec<Capability>) -> HubPackageManifest {
    let mut manifest = plugin_manifest(name, capabilities);
    manifest.source = None;
    manifest
}

fn provider_manifest(name: &str, capabilities: Vec<Capability>) -> HubPackageManifest {
    HubPackageManifest {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Provider,
        botster: ">=0.1.0".to_string(),
        source: Some(PackageSource::Git {
            repo: "https://example.invalid/botster/provider.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities: capabilities.clone(),
        entrypoints: vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Process,
            path: "bin/provider".to_string(),
            bootstrap: true,
        }],
        dependencies: Vec::new(),
        features: Vec::new(),
        host_profile: Some(HostProfileMetadata {
            profile_id: "test-provider".to_string(),
            compatibility: ">=0.1.0".to_string(),
            precedence: 10,
            required_providers: Vec::new(),
            required_capabilities: capabilities,
            policy_sections: vec![HostProfilePolicySection::Providers],
        }),
        configuration: None,
        surfaces: Vec::new(),
        runnable_entrypoints: Vec::new(),
        navigation: Vec::new(),
    }
}

fn plugin_key(package_name: &str) -> PluginKey {
    PluginKey(package_name.to_string())
}

fn handler(package_name: &str, handler_id: &str) -> PluginHandlerRef {
    PluginHandlerRef {
        plugin_key: plugin_key(package_name),
        kind: PluginHandlerKind::Command,
        handler_id: handler_id.to_string(),
    }
}

fn descriptor(
    package_name: &str,
    descriptor_id: &str,
    handler: PluginHandlerRef,
) -> PluginOwnedDescriptor {
    PluginOwnedDescriptor {
        descriptor: PluginDescriptorRef {
            plugin_key: plugin_key(package_name),
            kind: PluginDescriptorKind::Command,
            descriptor_id: descriptor_id.to_string(),
        },
        handler: Some(handler),
        body: BoundaryJson(serde_json::json!({ "id": descriptor_id })),
    }
}

fn resource(package_name: &str, resource_id: &str) -> PluginResourceRef {
    PluginResourceRef {
        plugin_key: plugin_key(package_name),
        kind: PluginResourceKind::McpRegistration,
        resource_id: resource_id.to_string(),
    }
}

fn bundle(
    package_name: &str,
    runtime: FakeRuntime,
    handler: PluginHandlerRef,
    required_capability: Option<Capability>,
    descriptor_id: &str,
    resource_id: &str,
) -> HubPluginRuntimeBundle {
    HubPluginRuntimeBundle {
        runtime: Arc::new(runtime),
        handlers: vec![PluginHandlerRegistration {
            handler: handler.clone(),
            required_capability,
        }],
        event_handlers: Vec::new(),
        descriptors: vec![descriptor(package_name, descriptor_id, handler)],
        resources: vec![resource(package_name, resource_id)],
        entrypoint: None,
        metadata: None,
    }
}

fn invocation(request_id: &str, handler: PluginHandlerRef) -> PluginInvocationRequest {
    PluginInvocationRequest {
        request_id: RequestId(request_id.to_string()),
        handler,
        timeout_ms: 1_000,
        context: PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: Some("hub-plugin-lifecycle-test".to_string()),
            metadata: None,
        },
        payload: BoundaryJson(serde_json::json!({ "request": request_id })),
    }
}

fn test_root(name: &str) -> PathBuf {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join(name);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create integration test root");
    root.canonicalize()
        .expect("canonical integration test root")
}

fn write_local_manifest(package_root: &Path, manifest: &HubPackageManifest) -> PathBuf {
    let manifest_path = package_root.join(LOCAL_PACKAGE_MANIFEST_FILE);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(manifest).expect("serialize local manifest"),
    )
    .expect("write local manifest");
    manifest_path
}

#[test]
fn hub_runtime_loads_and_invokes_enabled_plugin_package_through_core_worker() {
    let package_name = "workflow.plugin";
    let surface = capability(CapabilitySurface::Surfaces, None);
    let mut registry = registry_with_grants(vec![surface.clone()]);
    registry
        .install(
            plugin_manifest(package_name, vec![surface.clone()]),
            provenance(),
            "install plugin",
        )
        .expect("install plugin");
    registry
        .enable(package_name, "enable plugin")
        .expect("enable plugin");
    let command = handler(package_name, "advance");
    let runtime = FakeRuntime::new("ok");
    let mut hub = explicit_runtime();

    let loaded = hub
        .load_plugin_package(
            &registry,
            package_name,
            bundle(
                package_name,
                runtime.clone(),
                command.clone(),
                Some(surface),
                "advance",
                "mcp-tool",
            ),
        )
        .expect("load through hub runtime");

    assert_eq!(loaded, plugin_key(package_name));
    let status = hub.plugin_lifecycle_status(&registry);
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].package_name, package_name);
    assert!(status[0].loaded);

    let outcome = hub.invoke_plugin(invocation("invoke-plugin", command.clone()));
    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Completed(PluginInvocationSuccess { handler, .. }) if handler == command
    ));
    assert_eq!(runtime.invocations().len(), 1);
}

#[test]
fn hub_runtime_passes_split_plugin_worker_config_to_core_engine() {
    let package_name = "configured.workflow.plugin";
    let surface = capability(CapabilitySurface::Surfaces, None);
    let mut registry = registry_with_grants(vec![surface.clone()]);
    registry
        .install(
            plugin_manifest(package_name, vec![surface.clone()]),
            provenance(),
            "install configured plugin",
        )
        .expect("install configured plugin");
    registry
        .enable(package_name, "enable configured plugin")
        .expect("enable configured plugin");
    let config = HubStartupOptions {
        core_engine: CoreEngineOptions {
            plugin_worker_queue_capacity: 7,
            plugin_worker_executor_concurrency: 3,
            ..CoreEngineOptions::default()
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(
        Some(test_root("configured-worker-engine")),
        None,
    ))
    .expect("configured runtime");
    let mut hub = HubRuntime::new(config);
    let baseline = hub.plugin_worker_debug_snapshot();
    assert_eq!(baseline.live_plugin_executors, 0);
    assert_eq!(baseline.live_executor_workers, 0);

    hub.load_plugin_package(
        &registry,
        package_name,
        bundle(
            package_name,
            FakeRuntime::new("configured"),
            handler(package_name, "configured"),
            Some(surface),
            "configured",
            "configured-resource",
        ),
    )
    .expect("load configured plugin");

    let snapshot = hub.plugin_worker_debug_snapshot();
    assert_eq!(snapshot.configured_queue_capacity, 7);
    assert_eq!(snapshot.configured_executor_concurrency, 3);
    assert_eq!(snapshot.live_plugin_executors, 1);
    assert_eq!(snapshot.live_executor_workers, 3);

    let _cleanup = hub.unload_plugin_package(
        RequestId("unload-configured-plugin".to_string()),
        package_name,
    );
    let retired = hub.plugin_worker_debug_snapshot();
    assert_eq!(
        retired.live_plugin_executors,
        baseline.live_plugin_executors
    );
    assert_eq!(
        retired.live_executor_workers,
        baseline.live_executor_workers
    );
}

#[test]
fn local_package_install_persist_enable_prepare_and_load_crosses_core_worker() {
    let package_name = "local.workflow.plugin";
    let surface = capability(CapabilitySurface::Surfaces, None);
    let root = test_root("local-package-runtime-path");
    let data_root = root.join("data");
    let package_root = root.join("package");
    fs::create_dir_all(&package_root).expect("create package root");
    fs::write(package_root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
    write_local_manifest(
        &package_root,
        &local_plugin_manifest(package_name, vec![surface.clone()]),
    );
    let mut installed_registry = registry_with_grants(vec![surface.clone()]);
    installed_registry
        .install_local_path(&package_root, "install local package")
        .expect("install local package");
    let config = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(data_root),
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit state config should build");
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store
        .update(&config, |state| {
            state.package_registry = installed_registry.snapshot();
        })
        .expect("save local package registry through hub state");
    let mut registry = PackageRegistry::from_snapshot(state.package_registry)
        .expect("load local package registry from hub state");
    registry
        .enable(package_name, "enable local package")
        .expect("enable local package");
    let prepared = registry
        .prepare_local_package(package_name, "prepare local package")
        .expect("prepare local package");
    assert_eq!(prepared.package_name, package_name);
    assert_eq!(
        prepared
            .selected_entrypoint
            .as_ref()
            .expect("prepared code-load entrypoint")
            .path,
        "plugin.lua"
    );
    let selected_entrypoint_path = prepared
        .selected_entrypoint_path
        .as_ref()
        .expect("prepared code-load entrypoint path");
    assert_eq!(
        selected_entrypoint_path.as_path(),
        package_root
            .join("plugin.lua")
            .canonicalize()
            .expect("canonical plugin")
            .as_path()
    );
    let command = handler(package_name, "advance");
    let runtime = FakeRuntime::new("local-ok");
    let mut hub = explicit_runtime();

    let loaded = hub
        .load_plugin_package(
            &registry,
            package_name,
            HubPluginRuntimeBundle {
                entrypoint: Some(selected_entrypoint_path.to_string_lossy().into_owned()),
                ..bundle(
                    package_name,
                    runtime.clone(),
                    command.clone(),
                    Some(surface),
                    "advance",
                    "mcp-tool",
                )
            },
        )
        .expect("load prepared local package through hub runtime");

    assert_eq!(loaded, plugin_key(package_name));
    let outcome = hub.invoke_plugin(invocation("invoke-local-plugin", command.clone()));
    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Completed(PluginInvocationSuccess { handler, .. }) if handler == command
    ));
    assert_eq!(runtime.invocations().len(), 1);
}

#[test]
fn enabled_provider_package_loads_through_same_core_worker_path() {
    let package_name = "relay.provider";
    let signaling = capability(CapabilitySurface::SignalingRelay, Some("local"));
    let mut registry = registry_with_grants(vec![signaling.clone()]);
    registry
        .install(
            provider_manifest(package_name, vec![signaling.clone()]),
            provenance(),
            "install provider",
        )
        .expect("install provider");
    let decision = registry
        .enable(package_name, "enable provider")
        .expect("enable provider");
    assert!(decision.admitted_host_profile.is_some());
    let command = handler(package_name, "relay");
    let runtime = FakeRuntime::new("provider-ok");
    let mut hub = explicit_runtime();

    hub.load_plugin_package(
        &registry,
        package_name,
        bundle(
            package_name,
            runtime.clone(),
            command.clone(),
            Some(signaling),
            "relay",
            "provider-resource",
        ),
    )
    .expect("load provider through hub runtime");

    let outcome = hub.invoke_plugin(invocation("invoke-provider", command.clone()));
    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Completed(PluginInvocationSuccess { handler, .. }) if handler == command
    ));
    assert_eq!(runtime.invocations().len(), 1);
}

#[test]
fn disabled_not_installed_and_ungranted_packages_are_refused_before_core_load() {
    let package_name = "disabled.plugin";
    let surface = capability(CapabilitySurface::Surfaces, None);
    let mut registry = registry_with_grants(vec![surface.clone()]);
    registry
        .install(
            plugin_manifest(package_name, vec![surface.clone()]),
            provenance(),
            "install plugin",
        )
        .expect("install plugin");
    let command = handler(package_name, "run");
    let runtime = FakeRuntime::new("should-not-run");
    let mut hub = explicit_runtime();

    let disabled = hub
        .load_plugin_package(
            &registry,
            package_name,
            bundle(
                package_name,
                runtime.clone(),
                command.clone(),
                Some(surface.clone()),
                "run",
                "disabled-resource",
            ),
        )
        .expect_err("disabled package should not load");
    assert_eq!(
        disabled,
        HubLifecycleError::PackageNotEnabled {
            package_name: package_name.to_string()
        }
    );

    let not_installed = hub
        .load_plugin_package(
            &registry,
            "missing.plugin",
            bundle(
                "missing.plugin",
                runtime.clone(),
                handler("missing.plugin", "run"),
                None,
                "run",
                "missing-resource",
            ),
        )
        .expect_err("missing package should not load");
    assert_eq!(
        not_installed,
        HubLifecycleError::PackageNotInstalled {
            package_name: "missing.plugin".to_string()
        }
    );

    let mut ungranted_registry = registry_with_grants(Vec::new());
    ungranted_registry
        .install(
            plugin_manifest("ungranted.plugin", vec![surface]),
            provenance(),
            "install ungranted plugin",
        )
        .expect("install ungranted package");
    assert!(
        ungranted_registry
            .enable("ungranted.plugin", "enable ungranted plugin")
            .is_err()
    );
    let ungranted = hub
        .load_plugin_package(
            &ungranted_registry,
            "ungranted.plugin",
            bundle(
                "ungranted.plugin",
                runtime.clone(),
                handler("ungranted.plugin", "run"),
                None,
                "run",
                "ungranted-resource",
            ),
        )
        .expect_err("ungranted package remains not enabled");
    assert_eq!(
        ungranted,
        HubLifecycleError::PackageNotEnabled {
            package_name: "ungranted.plugin".to_string()
        }
    );
    assert!(runtime.invocations().is_empty());
}

#[test]
fn core_capability_enforcement_denies_missing_handler_capability_without_runtime_call() {
    let package_name = "capability.plugin";
    let granted = capability(CapabilitySurface::Surfaces, None);
    let required_but_missing = capability(CapabilitySurface::Network, Some("api"));
    let mut registry = registry_with_grants(vec![granted.clone()]);
    registry
        .install(
            plugin_manifest(package_name, vec![granted]),
            provenance(),
            "install plugin",
        )
        .expect("install plugin");
    registry
        .enable(package_name, "enable plugin")
        .expect("enable plugin");
    let command = handler(package_name, "network");
    let runtime = FakeRuntime::new("should-not-run");
    let mut hub = explicit_runtime();

    hub.load_plugin_package(
        &registry,
        package_name,
        bundle(
            package_name,
            runtime.clone(),
            command.clone(),
            Some(required_but_missing),
            "network",
            "network-resource",
        ),
    )
    .expect("load package before core handler capability check");
    let outcome = hub.invoke_plugin(invocation("invoke-denied", command));

    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Failed(PluginInvocationFailure {
            kind: PluginInvocationFailureKind::HandlerFailed,
            ..
        })
    ));
    assert!(runtime.invocations().is_empty());
}

#[test]
fn reload_and_unload_return_core_cleanup_and_stop_runtimes() {
    let package_name = "reloadable.plugin";
    let surface = capability(CapabilitySurface::Surfaces, None);
    let mut registry = registry_with_grants(vec![surface.clone()]);
    registry
        .install(
            plugin_manifest(package_name, vec![surface.clone()]),
            provenance(),
            "install plugin",
        )
        .expect("install plugin");
    registry
        .enable(package_name, "enable plugin")
        .expect("enable plugin");
    let old_handler = handler(package_name, "old");
    let new_handler = handler(package_name, "new");
    let old_runtime = FakeRuntime::new("old");
    let new_runtime = FakeRuntime::new("new");
    let mut hub = explicit_runtime();

    hub.load_plugin_package(
        &registry,
        package_name,
        bundle(
            package_name,
            old_runtime.clone(),
            old_handler,
            Some(surface.clone()),
            "old-descriptor",
            "old-resource",
        ),
    )
    .expect("load old plugin");
    hub.submit_capability_request(CapabilityRuntimeRequest {
        plugin_key: plugin_key(package_name),
        operation_id: CapabilityOperationId("reload-timer".to_string()),
        operation: CapabilityOperation::Timer(TimerCapabilityRequest::Interval { interval_ms: 5 }),
        timeout_ms: 1_000,
        callback: None,
    })
    .expect("register timer owned by old plugin generation");
    assert_eq!(hub.active_plugin_timer_resources(), 1);
    let reload_cleanup = hub
        .reload_plugin_package(
            RequestId("reload-plugin".to_string()),
            &registry,
            package_name,
            bundle(
                package_name,
                new_runtime.clone(),
                new_handler.clone(),
                Some(surface),
                "new-descriptor",
                "new-resource",
            ),
        )
        .expect("reload through core worker");

    assert_eq!(old_runtime.stopped(), vec![plugin_key(package_name)]);
    assert_eq!(reload_cleanup.removed_descriptors.len(), 1);
    assert_eq!(reload_cleanup.removed_resources.len(), 2);
    assert!(reload_cleanup.removed_resources.iter().any(|resource| {
        resource.kind == PluginResourceKind::Timer
            && resource.plugin_key == plugin_key(package_name)
    }));
    assert_eq!(hub.active_plugin_timer_resources(), 0);
    let after_reload = hub
        .drain_capability_events_at(&plugin_key(package_name), 10)
        .expect("drain after reload cleanup");
    assert!(
        after_reload
            .iter()
            .all(|event| !matches!(event, CapabilityRuntimeEvent::TimerFired(_)))
    );
    assert!(matches!(
        hub.invoke_plugin(invocation("invoke-new", new_handler))
            .result,
        PluginInvocationResult::Completed(_)
    ));
    assert_eq!(new_runtime.invocations().len(), 1);

    let unload_cleanup =
        hub.unload_plugin_package(RequestId("unload-plugin".to_string()), package_name);
    assert_eq!(new_runtime.stopped(), vec![plugin_key(package_name)]);
    assert_eq!(unload_cleanup.removed_descriptors.len(), 1);
    assert_eq!(unload_cleanup.removed_resources.len(), 1);
    assert!(matches!(
        hub.invoke_plugin(invocation(
            "invoke-after-unload",
            handler(package_name, "new")
        ))
        .result,
        PluginInvocationResult::Failed(PluginInvocationFailure {
            kind: PluginInvocationFailureKind::WorkerStopped,
            ..
        })
    ));
}
