#![cfg(unix)]

use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{RequestId, SessionId, SessionLifecycleState, SubscriptionId};
use botster_hub::test_internals::TestHubStateStoreExt;
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi,
    HubClientPackageClassification, HubClientPackageState, HubClientRequest, HubClientResponseBody,
    HubConfig, HubDaemon, HubStartupOptions, HubStateLoadSource, PackageRegistry,
    PackageRegistrySnapshot, RuntimeEnvironment, SessionDefaults, TransportBindings,
};
use botster_terminal_protocol_client::TerminalInputCommand;

mod support;
use support::{
    bind_shared_terminal_adapter, candidate_session_worker_binary_path, inject_terminal_command,
};

const RUNTIME_PACKAGE: &str = "runtime.synthetic-plugin";
const RUNTIME_SESSION: &str = "runtime-local-session";
const RUNTIME_SUBSCRIPTION: &str = "runtime-local-subscription";
const INPUT_MARKER: &[u8] = b"runtime:from-input";

#[test]
fn local_runtime_runs_daemon_package_lifecycle_session_and_clean_shutdown() {
    run_local_runtime();
}

fn run_local_runtime() {
    let data_dir = unique_test_dir("runtime");
    let config = explicit_config(&data_dir);
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let package_dir = PathBuf::from("examples").join("synthetic-plugin");
    assert!(
        package_dir.join("botster-package.json").exists(),
        "documented synthetic package fixture must exist"
    );

    let mut daemon = HubDaemon::start(config.clone()).expect("start runtime daemon");
    let startup_status = daemon.status();
    assert_eq!(startup_status.state_source, HubStateLoadSource::Initialized);
    assert!(startup_status.core_initialized);
    assert!(store.path().exists());

    let mut package_registry = daemon.package_registry().clone();
    let installed_name = {
        let record = package_registry
            .install_local_path(&package_dir, "runtime install local synthetic package")
            .expect("install synthetic local package");
        record.manifest.name.clone()
    };
    assert_eq!(installed_name, RUNTIME_PACKAGE);
    package_registry
        .enable(RUNTIME_PACKAGE, "runtime enable synthetic package")
        .expect("enable synthetic local package");
    daemon
        .replace_package_registry(package_registry)
        .expect("replacement package registry fits");

    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("runtime-local-client");
    assert_status_and_packages(
        &api,
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        1,
    );

    let snapshot = daemon.package_registry().snapshot();
    daemon.stop();
    // The running daemon owns the state directory. Seed the registry only
    // after stop releases that ownership.
    persist_package_registry(&config, snapshot);
    let mut reloaded = HubDaemon::start(config).expect("reload runtime daemon");
    let reloaded_status = reloaded.status();
    assert_eq!(reloaded_status.state_source, HubStateLoadSource::Loaded);
    assert_eq!(reloaded_status.package_count, 1);
    assert_eq!(reloaded_status.enabled_package_count, 1);

    let prepared = reloaded
        .package_registry()
        .prepare_local_package(RUNTIME_PACKAGE, "runtime prepare local package")
        .expect("prepare enabled local package");
    assert_eq!(prepared.package_name, RUNTIME_PACKAGE);
    assert!(
        prepared
            .selected_entrypoint_path
            .as_ref()
            .expect("prepared code-load entrypoint path")
            .ends_with("plugin.lua"),
        "prepared entrypoint should resolve to plugin.lua"
    );

    let lifecycle_registry = reloaded.package_registry().clone();
    let lifecycle = api
        .handle_request(
            reloaded.runtime_mut().expect("runtime initialized"),
            &lifecycle_registry,
            HubClientRequest::PluginLifecycleStatus {
                request_id: request_id("runtime-plugin-lifecycle"),
            },
        )
        .wait(reloaded.runtime().expect("runtime initialized"))
        .expect("pull plugin lifecycle through client api");
    let HubClientResponseBody::PluginLifecycle(records) = lifecycle.body else {
        panic!("plugin lifecycle response expected");
    };
    assert_eq!(records.lifecycle.len(), 1);
    assert_eq!(records.lifecycle[0].package_name, RUNTIME_PACKAGE);
    assert!(records.lifecycle[0].loaded);
    let expected = CoreEngineOptions::default();
    assert_eq!(
        records.worker_counters.configured_queue_capacity,
        expected.plugin_worker_queue_capacity
    );
    assert_eq!(
        records.worker_counters.configured_executor_concurrency,
        expected.plugin_worker_executor_concurrency
    );
    assert_eq!(records.worker_counters.live_plugin_executors, 1);

    let tools = reloaded
        .runtime()
        .expect("runtime initialized")
        .list_plugin_mcp_tools();
    assert!(
        tools
            .iter()
            .any(|tool| tool.name == "runtime.synthetic.echo")
    );
    let result = reloaded
        .runtime_mut()
        .expect("runtime initialized")
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "runtime.synthetic.echo".to_string(),
            arguments: serde_json::json!({ "message": "runtime" }),
        })
        .expect("call real synthetic Lua MCP tool");
    assert_eq!(result["message"], "runtime");
    assert_eq!(result["ambient"]["os"], true);

    let mut session_started = false;
    let session_id = SessionId(RUNTIME_SESSION.to_string());
    let subscription_id = SubscriptionId(RUNTIME_SUBSCRIPTION.to_string());
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
        let _ = api
            .handle_request(
                reloaded
                    .runtime_mut()
                    .expect("runtime initialized for cleanup"),
                &cleanup_registry,
                HubClientRequest::Shutdown {
                    request_id: request_id("runtime-cleanup-shutdown"),
                    session_id: session_id.clone(),
                    now_seconds: logical_clock,
                },
            )
            .wait(reloaded.runtime().expect("runtime initialized for cleanup"));
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
                request_id: request_id("runtime-shutdown"),
                session_id,
                now_seconds: logical_clock,
            },
        )
        .wait(reloaded.runtime().expect("runtime initialized"))
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
                request_id: request_id("runtime-status"),
            },
        )
        .wait(runtime)
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
                request_id: request_id("runtime-list-packages"),
            },
        )
        .wait(runtime)
        .expect("packages through client api");
    let HubClientResponseBody::Packages(records) = response.body else {
        panic!("package response expected");
    };
    assert_eq!(records.len(), expected_package_count);
    assert_eq!(records[0].package_name, RUNTIME_PACKAGE);
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
                request_id: request_id("runtime-spawn"),
                session_id: session_id.clone(),
                command: "printf 'runtime:ready\\n'; while IFS= read -r line; do printf 'runtime:%s\\n' \"$line\"; done".to_string(),
                now_seconds: *logical_clock,
            },
        ).wait(runtime)
        .expect("spawn through client api");
    *logical_clock += 1;
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawn response expected");
    };
    assert_eq!(spawned.session.lifecycle, SessionLifecycleState::Running);

    // Attach and bind run as one Core operation inside the shared helper.
    let terminal_adapter = bind_shared_terminal_adapter(
        runtime,
        api.identity().client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
    );
    *logical_clock += 1;
    read_screen_until(
        runtime,
        api,
        packages,
        &session_id,
        "runtime:ready",
        logical_clock,
    );

    inject_terminal_command(
        &terminal_adapter,
        &TerminalInputCommand::RawBytes {
            operation_id: 1,
            data: b"from-input\n".to_vec(),
        },
    );
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

fn read_screen_until(
    runtime: &mut botster_hub::HubRuntime,
    api: &HubClientApi,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &str,
    logical_clock: &mut u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let _ = runtime
            .observe_lifecycle_slice(
                *logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(20),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge");
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("runtime-read-screen"),
                    session_id: session_id.clone(),
                    now_seconds: *logical_clock,
                },
            )
            .wait(runtime)
            .expect("read screen through client api");
        *logical_clock += 1;
        let HubClientResponseBody::ReadScreen(screen) = response.body else {
            panic!("read screen response expected");
        };
        if screen.text.contains(needle) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {needle:?} in ReadScreen");
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

    let needle_text = String::from_utf8_lossy(needle);
    while Instant::now() < deadline {
        let _ = runtime
            .observe_lifecycle_slice(
                *logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(20),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge");
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("runtime-read-after-drain"),
                    session_id: session_id.clone(),
                    now_seconds: *logical_clock,
                },
            )
            .wait(runtime)
            .expect("read screen");
        *logical_clock += 1;
        let HubClientResponseBody::ReadScreen(screen) = response.body else {
            panic!("read screen response expected");
        };
        observed = screen.text.into_bytes();
        if observed
            .windows(needle.len())
            .any(|window| window == needle)
            || String::from_utf8_lossy(&observed).contains(needle_text.as_ref())
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
    let session_worker_path = candidate_session_worker_binary_path().to_path_buf();
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "local-runtime-test".to_string(),
            display_name: "Local Runtime Test".to_string(),
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
        core_engine: CoreEngineOptions {
            session_worker_path: Some(session_worker_path),
            ..CoreEngineOptions::default()
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit runtime config should build")
}

fn persist_package_registry(config: &HubConfig, snapshot: PackageRegistrySnapshot) {
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update_test_fixture(config, |state| {
            state.package_registry = snapshot;
        })
        .expect("persist runtime package registry");
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
        .join("local-runtime")
        .join(name)
        .join(nanos.to_string());
    fs::create_dir_all(&root).expect("create runtime data directory");
    root
}
