#![cfg(unix)]

use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    BoundaryJson, Capability, CapabilitySurface, PluginCancellationToken, PluginDescriptorKind,
    PluginDescriptorRef, PluginHandlerKind, PluginHandlerRef, PluginHandlerRegistration,
    PluginInvocationContext, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor, PluginResourceKind,
    PluginResourceRef, PluginRuntime, RequestId, SessionId, SessionLifecycleState, SubscriptionId,
};
use botster_hub::{
    DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi, HubClientEvent,
    HubClientPackageClassification, HubClientPackageState, HubClientRequest, HubClientResponseBody,
    HubDaemon, HubPluginRuntimeBundle, HubStartupOptions, HubStateLoadSource, HubStateStore,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, TransportBindings,
};

mod support;
use support::ensure_session_worker_binary;

const DOGFOOD_PACKAGE: &str = "dogfood.synthetic-plugin";
const DOGFOOD_SESSION: &str = "dogfood-local-session";
const DOGFOOD_SUBSCRIPTION: &str = "dogfood-local-subscription";
const INPUT_MARKER: &[u8] = b"dogfood:from-input";

#[derive(Clone)]
struct FakeDogfoodRuntime {
    invocations: Arc<Mutex<Vec<PluginInvocationRequest>>>,
}

impl FakeDogfoodRuntime {
    fn new() -> Self {
        Self {
            invocations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn invocations(&self) -> Vec<PluginInvocationRequest> {
        self.invocations
            .lock()
            .expect("fake dogfood runtime invocations lock")
            .clone()
    }
}

impl PluginRuntime for FakeDogfoodRuntime {
    fn invoke(
        &self,
        request: PluginInvocationRequest,
        _cancellation: PluginCancellationToken,
    ) -> PluginInvocationResult {
        self.invocations
            .lock()
            .expect("fake dogfood runtime invocations lock")
            .push(request.clone());

        PluginInvocationResult::Completed(PluginInvocationSuccess {
            request_id: request.request_id,
            handler: request.handler,
            payload: Some(BoundaryJson(serde_json::json!({ "dogfood": true }))),
        })
    }

    fn stop(&self, _plugin_key: &PluginKey) {}
}

#[test]
fn local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown() {
    run_local_dogfood();
}

fn run_local_dogfood() {
    ensure_session_worker_binary();
    let data_dir = unique_test_dir("dogfood");
    let config = explicit_config(&data_dir);
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let package_dir = PathBuf::from("examples").join("synthetic-plugin");
    assert!(
        package_dir.join("botster-package.json").exists(),
        "documented synthetic package fixture must exist"
    );

    let mut daemon = HubDaemon::start(config.clone()).expect("start dogfood daemon");
    let startup_status = daemon.status();
    assert_eq!(startup_status.state_source, HubStateLoadSource::Initialized);
    assert!(startup_status.core_initialized);
    assert!(store.path().exists());

    let installed_name = {
        let record = daemon
            .package_registry_mut()
            .install_local_path(&package_dir, "dogfood install local synthetic package")
            .expect("install synthetic local package");
        record.manifest.name.clone()
    };
    assert_eq!(installed_name, DOGFOOD_PACKAGE);
    daemon
        .package_registry_mut()
        .enable(DOGFOOD_PACKAGE, "dogfood enable synthetic package")
        .expect("enable synthetic local package");
    persist_package_registry(&daemon);

    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("dogfood-local-client");
    assert_status_and_packages(
        &api,
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        1,
    );

    daemon.stop();
    let mut reloaded = HubDaemon::start(config).expect("reload dogfood daemon");
    let reloaded_status = reloaded.status();
    assert_eq!(reloaded_status.state_source, HubStateLoadSource::Loaded);
    assert_eq!(reloaded_status.package_count, 1);
    assert_eq!(reloaded_status.enabled_package_count, 1);

    let prepared = reloaded
        .package_registry()
        .prepare_local_package(DOGFOOD_PACKAGE, "dogfood prepare local package")
        .expect("prepare enabled local package");
    assert_eq!(prepared.package_name, DOGFOOD_PACKAGE);
    assert!(
        prepared.selected_entrypoint_path.ends_with("plugin.lua"),
        "prepared entrypoint should resolve to plugin.lua"
    );

    let runtime = FakeDogfoodRuntime::new();
    let command = handler("dogfood.invoke");
    let loaded_registry = reloaded.package_registry().clone();
    let loaded = reloaded
        .runtime_mut()
        .expect("runtime initialized after reload")
        .load_plugin_package(
            &loaded_registry,
            DOGFOOD_PACKAGE,
            HubPluginRuntimeBundle {
                runtime: Arc::new(runtime.clone()),
                handlers: vec![PluginHandlerRegistration {
                    handler: command.clone(),
                    required_capability: Some(capability(CapabilitySurface::Surfaces, None)),
                }],
                descriptors: vec![descriptor("dogfood-command", command.clone())],
                resources: vec![resource("dogfood-resource")],
                entrypoint: Some(
                    prepared
                        .selected_entrypoint_path
                        .to_string_lossy()
                        .into_owned(),
                ),
                metadata: None,
            },
        )
        .expect("load synthetic plugin through hub lifecycle");
    assert_eq!(loaded, PluginKey(DOGFOOD_PACKAGE.to_string()));

    let lifecycle_registry = reloaded.package_registry().clone();
    let lifecycle = api
        .handle_request(
            reloaded.runtime_mut().expect("runtime initialized"),
            &lifecycle_registry,
            HubClientRequest::PluginLifecycleStatus {
                request_id: request_id("dogfood-plugin-lifecycle"),
            },
        )
        .expect("pull plugin lifecycle through client api");
    let HubClientResponseBody::PluginLifecycle(records) = lifecycle.body else {
        panic!("plugin lifecycle response expected");
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].package_name, DOGFOOD_PACKAGE);
    assert!(records[0].loaded);

    let outcome = reloaded
        .runtime_mut()
        .expect("runtime initialized")
        .invoke_plugin(invocation(command.clone()));
    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Completed(PluginInvocationSuccess { handler, .. }) if handler == command
    ));
    assert_eq!(runtime.invocations().len(), 1);

    let mut session_started = false;
    let session_id = SessionId(DOGFOOD_SESSION.to_string());
    let subscription_id = SubscriptionId(DOGFOOD_SUBSCRIPTION.to_string());
    let mut logical_clock = 10;
    let flow_registry = reloaded.package_registry().clone();
    let flow = panic::catch_unwind(AssertUnwindSafe(|| {
        session_started = true;
        spawn_attach_input_and_drain(
            &api,
            reloaded.runtime_mut().expect("runtime initialized"),
            &flow_registry,
            session_id.clone(),
            subscription_id.clone(),
            &mut logical_clock,
        );
    }));

    if flow.is_err() && session_started {
        let cleanup_registry = reloaded.package_registry().clone();
        let _ = api.handle_request(
            reloaded
                .runtime_mut()
                .expect("runtime initialized for cleanup"),
            &cleanup_registry,
            HubClientRequest::Shutdown {
                request_id: request_id("dogfood-cleanup-shutdown"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        );
    }
    if let Err(payload) = flow {
        panic::resume_unwind(payload);
    }

    let shutdown_registry = reloaded.package_registry().clone();
    let shutdown = api
        .handle_request(
            reloaded.runtime_mut().expect("runtime initialized"),
            &shutdown_registry,
            HubClientRequest::Shutdown {
                request_id: request_id("dogfood-shutdown"),
                session_id,
                now_seconds: logical_clock,
            },
        )
        .expect("shutdown through client api");
    let HubClientResponseBody::Events(events) = shutdown.body else {
        panic!("shutdown should return events");
    };
    assert!(events.is_empty());

    reloaded.stop();
}

fn assert_status_and_packages(
    api: &HubClientApi,
    runtime: &mut botster_hub::HubRuntime,
    packages: &PackageRegistry,
    expected_package_count: usize,
) {
    let status = api
        .handle_request(
            runtime,
            packages,
            HubClientRequest::Status {
                request_id: request_id("dogfood-status"),
            },
        )
        .expect("status through client api");
    let HubClientResponseBody::Status(status) = status.body else {
        panic!("status response expected");
    };
    assert_eq!(status.package_count, expected_package_count);

    let response = api
        .handle_request(
            runtime,
            packages,
            HubClientRequest::ListPackages {
                request_id: request_id("dogfood-list-packages"),
            },
        )
        .expect("packages through client api");
    let HubClientResponseBody::Packages(records) = response.body else {
        panic!("package response expected");
    };
    assert_eq!(records.len(), expected_package_count);
    assert_eq!(records[0].package_name, DOGFOOD_PACKAGE);
    assert_eq!(
        records[0].classification,
        HubClientPackageClassification::Plugin
    );
    assert_eq!(records[0].state, HubClientPackageState::Enabled);
    assert!(
        !format!("{records:?}").contains(concat!("/", "Users", "/")),
        "client package response must not expose host paths"
    );
}

fn spawn_attach_input_and_drain(
    api: &HubClientApi,
    runtime: &mut botster_hub::HubRuntime,
    packages: &PackageRegistry,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    logical_clock: &mut u64,
) {
    let spawn = api
        .handle_request(
            runtime,
            packages,
            HubClientRequest::Spawn {
                request_id: request_id("dogfood-spawn"),
                session_id: session_id.clone(),
                command: "printf 'dogfood:ready\\n'; while IFS= read -r line; do printf 'dogfood:%s\\n' \"$line\"; done".to_string(),
                now_seconds: *logical_clock,
            },
        )
        .expect("spawn through client api");
    *logical_clock += 1;
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawn response expected");
    };
    assert_eq!(spawned.session.lifecycle, SessionLifecycleState::Running);

    api.handle_request(
        runtime,
        packages,
        HubClientRequest::Attach {
            request_id: request_id("dogfood-attach"),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: *logical_clock,
        },
    )
    .expect("attach through client api");
    *logical_clock += 1;
    drain_until(
        runtime,
        api,
        packages,
        &session_id,
        b"dogfood:ready",
        logical_clock,
    );

    api.handle_request(
        runtime,
        packages,
        HubClientRequest::Input {
            request_id: request_id("dogfood-input"),
            session_id: session_id.clone(),
            data: b"from-input\n".to_vec(),
            now_seconds: *logical_clock,
        },
    )
    .expect("send input through client api");
    *logical_clock += 1;

    let observed = drain_until(
        runtime,
        api,
        packages,
        &session_id,
        INPUT_MARKER,
        logical_clock,
    );
    assert!(
        observed
            .windows(INPUT_MARKER.len())
            .any(|window| window == INPUT_MARKER),
        "drain should include input marker"
    );
}

fn drain_until(
    runtime: &mut botster_hub::HubRuntime,
    api: &HubClientApi,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("dogfood-drain"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        for event in events {
            if let HubClientEvent::TerminalOutput { data, .. } = event {
                observed.extend(data);
            }
        }

        if observed
            .windows(needle.len())
            .any(|window| window == needle)
        {
            return observed;
        }

        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} in {:?}",
        String::from_utf8_lossy(needle),
        String::from_utf8_lossy(&observed)
    );
}

fn explicit_config(data_directory: &Path) -> botster_hub::HubConfig {
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "local-dogfood-test".to_string(),
            display_name: "Local Dogfood Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory.to_path_buf()),
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit dogfood config should build")
}

fn persist_package_registry(daemon: &HubDaemon) {
    let runtime = daemon.runtime().expect("daemon runtime initialized");
    let config = runtime.config().clone();
    let snapshot = daemon.package_registry().snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.package_registry = snapshot;
        })
        .expect("persist dogfood package registry");
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn handler(handler_id: &str) -> PluginHandlerRef {
    PluginHandlerRef {
        plugin_key: PluginKey(DOGFOOD_PACKAGE.to_string()),
        kind: PluginHandlerKind::Command,
        handler_id: handler_id.to_string(),
    }
}

fn descriptor(descriptor_id: &str, handler: PluginHandlerRef) -> PluginOwnedDescriptor {
    PluginOwnedDescriptor {
        descriptor: PluginDescriptorRef {
            plugin_key: PluginKey(DOGFOOD_PACKAGE.to_string()),
            kind: PluginDescriptorKind::Command,
            descriptor_id: descriptor_id.to_string(),
        },
        handler: Some(handler),
        body: BoundaryJson(serde_json::json!({ "id": descriptor_id })),
    }
}

fn resource(resource_id: &str) -> PluginResourceRef {
    PluginResourceRef {
        plugin_key: PluginKey(DOGFOOD_PACKAGE.to_string()),
        kind: PluginResourceKind::McpRegistration,
        resource_id: resource_id.to_string(),
    }
}

fn invocation(handler: PluginHandlerRef) -> PluginInvocationRequest {
    PluginInvocationRequest {
        request_id: request_id("dogfood-invoke"),
        handler,
        timeout_ms: 1_000,
        context: PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: Some("hub-local-dogfood-test".to_string()),
            metadata: None,
        },
        payload: BoundaryJson(serde_json::json!({ "dogfood": "invoke" })),
    }
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("local-dogfood")
        .join(name)
        .join(nanos.to_string());
    fs::create_dir_all(&root).expect("create dogfood data directory");
    root
}
