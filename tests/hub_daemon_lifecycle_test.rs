#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use botster_core::{
    Capability, CapabilitySurface, CoreSessionMetadata, ExtensionEntrypoint, ExtensionKind,
    ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection, PackageManifest,
    PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi, HubClientEvent,
    HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState, HubStartupOptions,
    HubStateLoadSource, HubStateStore, PackageAdmissionPolicy, PackageProvenance, PackageRegistry,
    RuntimeEnvironment, SessionDefaults, TransportBindings,
};

mod support;
use support::ensure_session_worker_binary;

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("daemon")
        .join(name)
        .join(nanos.to_string())
}

fn explicit_config(data_directory: impl Into<PathBuf>) -> botster_hub::HubConfig {
    ensure_session_worker_binary();
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-daemon-test".to_string(),
            display_name: "Hub Daemon Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory.into()),
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
    .expect("explicit daemon config should build")
}

fn empty_registry() -> PackageRegistry {
    PackageRegistry::new(Vec::<Capability>::new().into_iter().collect())
}

fn spawn_request(config: &botster_hub::HubConfig) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id: RequestId("hub-daemon-spawn".to_string()),
        session_id: SessionId("hub-daemon-session".to_string()),
        executable: config.session_defaults.shell.clone(),
        arguments: vec![
            "-c".to_string(),
            "printf 'daemon-ready\\n'; sleep 1".to_string(),
        ],
        working_directory: SpawnWorkingDirectory {
            path: config
                .session_defaults
                .working_directory
                .as_deref()
                .expect("test config has explicit working directory")
                .display()
                .to_string(),
        },
        environment: SpawnEnvironment::default(),
        initial_pty_size: Some(ResizePayload {
            rows: config.session_defaults.initial_rows,
            cols: config.session_defaults.initial_cols,
        }),
    }
}

fn drain_until_client_output(
    api: &HubClientApi,
    runtime: &mut botster_hub::HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<HubClientEvent> {
    let mut observed = Vec::new();
    for _ in 0..100 {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: RequestId("hub-daemon-drain".to_string()),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through hub client api");
        *logical_clock += 1;
        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        if observed.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput { data, .. }
                    if data.windows(needle.len()).any(|window| window == needle)
            )
        }) {
            return observed;
        }

        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} in client output",
        String::from_utf8_lossy(needle)
    );
}

fn package_provenance() -> PackageProvenance {
    PackageProvenance {
        source: "https://example.invalid/botster/packages/provider".to_string(),
        checksum: Some("sha256:daemon-test".to_string()),
    }
}

fn provider_manifest() -> PackageManifest {
    let capabilities = vec![Capability {
        surface: CapabilitySurface::Surfaces,
        scope: None,
    }];

    PackageManifest {
        name: "daemon.provider".to_string(),
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
        host_profile: Some(HostProfileMetadata {
            profile_id: "daemon-provider".to_string(),
            compatibility: ">=0.1.0".to_string(),
            precedence: 10,
            required_providers: Vec::new(),
            required_capabilities: capabilities,
            policy_sections: vec![HostProfilePolicySection::Providers],
        }),
    }
}

fn write_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create local package root");
    fs::write(
        root.join("plugin.lua"),
        "-- synthetic local dogfood plugin\n",
    )
    .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "dogfood.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write local package manifest");
}

#[test]
fn daemon_starts_empty_state_reports_status_uses_core_and_stops_idempotently() {
    let config = explicit_config(unique_test_dir("empty"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from empty state");

    let status = daemon.status();
    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Initialized);
    assert_eq!(status.host_id, "hub-daemon-test");
    assert_eq!(status.host_display_name, "Hub Daemon Test");
    assert_eq!(status.schema_version, 1);
    assert!(status.data_dir_configured);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 0);
    assert_eq!(status.provider_count, 0);
    assert!(store.path().exists());

    let runtime = daemon.runtime_mut().expect("runtime initialized");
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    runtime
        .spawn_session(request, CoreSessionMetadata::new(), 1)
        .expect("spawn through core daemon runtime");
    assert_eq!(runtime.list_sessions().expect("daemon list").len(), 1);
    runtime
        .shutdown_session(session_id, 2)
        .expect("shutdown through core daemon runtime");

    let stopped = daemon.stop();
    assert_eq!(stopped.lifecycle_state, HubDaemonState::Stopped);
    assert!(!stopped.core_initialized);
    let stopped_again = daemon.stop();
    assert_eq!(stopped_again, stopped);

    let reopened = store
        .load_or_initialize(&config)
        .expect("reload committed daemon state");
    assert_eq!(reopened.schema_version, 1);
    assert_eq!(reopened.host.id, "hub-daemon-test");
}

#[test]
fn daemon_restart_reconnects_worker_backed_session_through_client_api() {
    let config = explicit_config(unique_test_dir("restart-reconnect"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-restart-client");
    let session_id = SessionId("hub-daemon-restart-session".to_string());
    let subscription_id = SubscriptionId("hub-daemon-restart-subscription".to_string());
    let mut logical_clock = 10;

    let mut daemon = HubDaemon::start(config.clone()).expect("start first hub daemon");
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-restart-spawn".to_string()),
            session_id: session_id.clone(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn through hub client api");
    logical_clock += 1;
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Attach {
            request_id: RequestId("hub-daemon-restart-attach".to_string()),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("attach before restart through client api");
    logical_clock += 1;
    drain_until_client_output(
        &api,
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        &session_id,
        b"restart-ready",
        &mut logical_clock,
    );
    daemon.stop();

    let mut restarted = HubDaemon::start(config).expect("restart hub daemon");
    assert!(
        restarted
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&session_id),
        "restart should recover the live worker-backed session"
    );
    let listed = api
        .handle_request(
            restarted.runtime_mut().expect("runtime initialized"),
            &packages,
            HubClientRequest::ListSessions {
                request_id: RequestId("hub-daemon-restart-list".to_string()),
            },
        )
        .expect("list after restart through client api");
    assert!(
        matches!(listed.body, HubClientResponseBody::Sessions(sessions) if sessions.iter().any(|session| session.session_id == session_id))
    );

    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Attach {
            request_id: RequestId("hub-daemon-restart-reattach".to_string()),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("reattach after restart through client api");
    logical_clock += 1;
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Input {
            request_id: RequestId("hub-daemon-restart-input".to_string()),
            session_id: session_id.clone(),
            data: b"after-restart\n".to_vec(),
            now_seconds: logical_clock,
        },
    )
    .expect("input after restart through client api");
    logical_clock += 1;
    drain_until_client_output(
        &api,
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        &session_id,
        b"echo:after-restart",
        &mut logical_clock,
    );
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Shutdown {
            request_id: RequestId("hub-daemon-restart-shutdown".to_string()),
            session_id,
            now_seconds: logical_clock,
        },
    )
    .expect("shutdown after restart through client api");
}

#[test]
fn daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions() {
    let stale_config = explicit_config(unique_test_dir("stale-reconcile"));
    let stale_session_id = SessionId("hub-daemon-stale-session".to_string());
    let registry = SessionRegistry::new(stale_config.data_directory.clone());
    let mut stale_record = RegistryRecord::running(
        stale_session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    stale_record.observe_restart_contract(serde_json::json!({"session": "hub-daemon-stale"}), 2);
    registry
        .save(&stale_record)
        .expect("stale registry fixture should save");

    let stale_daemon = HubDaemon::start(stale_config).expect("start daemon with stale registry");
    assert!(
        stale_daemon
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .stale_sessions
            .contains(&stale_session_id),
        "registry record without a live worker should become stale deterministically"
    );

    let recovered_config = explicit_config(unique_test_dir("recovered-reconcile"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-recovered-client");
    let recovered_session_id = SessionId("hub-daemon-recovered-session".to_string());
    let mut first = HubDaemon::start(recovered_config.clone()).expect("start first daemon");
    api.handle_request(
        first.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-recovered-spawn".to_string()),
            session_id: recovered_session_id.clone(),
            command: "printf 'recovered-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: 1,
        },
    )
    .expect("spawn recovered session through client api");
    first.stop();

    let recovered =
        HubDaemon::start(recovered_config).expect("restart daemon with live core registry record");
    assert!(
        recovered
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&recovered_session_id),
        "core-live worker-backed session absent from hub state should be recovered"
    );
}

#[test]
fn daemon_restores_existing_provider_policy_records_through_snapshot_admission() {
    let config = explicit_config(unique_test_dir("existing"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut policy = PackageAdmissionPolicy::from_host_profile();
    policy
        .install(
            provider_manifest(),
            package_provenance(),
            "install provider policy record",
        )
        .expect("install provider");
    policy
        .enable("daemon.provider", "enable provider policy record")
        .expect("enable provider through admission");

    store
        .update(&config, |state| {
            state.package_registry = policy.registry().snapshot();
        })
        .expect("seed existing state through store");

    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from existing state");
    let status = daemon.status();

    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Loaded);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 1);
    assert_eq!(status.enabled_package_count, 1);
    assert_eq!(status.provider_count, 1);
    assert_eq!(status.enabled_provider_count, 1);
    assert_eq!(status.schema_version, 1);

    daemon.stop();
    let reopened = store
        .load_or_initialize(&config)
        .expect("reload existing state after stop");
    assert_eq!(reopened.package_registry.records.len(), 1);
    assert!(reopened.package_registry.records[0].is_enabled());
}

#[test]
fn cli_start_requires_explicit_data_dir_and_prints_scrubbed_lifecycle_status() {
    let data_dir = unique_test_dir("cli-start");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub start");

    assert!(
        output.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("event=started"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(stdout.contains("event=stopped"));
    assert!(stdout.contains("lifecycle_state=stopped"));
    assert!(stdout.contains("schema_version=1"));
    assert!(stdout.contains("core_initialized=true"));
    assert!(stdout.contains("state_source=initialized"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(concat!("/", "Users", "/")));
    assert!(!stdout.contains("/home/"));
    assert!(data_dir.join("hub-state.json").exists());
}

#[test]
fn cli_status_uses_daemon_status_path_without_local_paths() {
    let data_dir = unique_test_dir("cli-status");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status");

    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(stdout.contains("schema_version=1"));
    assert!(stdout.contains("core_initialized=true"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_sessions_spawn_and_list_route_through_client_api() {
    let data_dir = unique_test_dir("cli-sessions");
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("dogfood-session")
        .arg("--")
        .arg("printf 'dogfood-ok\\n'; sleep 1")
        .output()
        .expect("run botster-hub sessions spawn");

    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );
    let stdout = String::from_utf8(spawn.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=spawned"));
    assert!(stdout.contains("session_id=dogfood-session"));
    assert!(stdout.contains("lifecycle=running"));
    assert!(stdout.contains("event_count=0"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub sessions list");

    assert!(
        list.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=sessions"));
    assert!(stdout.contains("session_count=1"));
    assert!(stdout.contains("session id=dogfood-session lifecycle=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_inspect_reports_not_found_for_fresh_in_process_daemon() {
    let data_dir = unique_test_dir("cli-inspect");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("inspect")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub inspect");

    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("inspect=session"));
    assert!(stdout.contains("session_id=dogfood-session"));
    assert!(stdout.contains("found=false"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_packages_enable_local_path_persists_and_lists_through_client_api() {
    let data_dir = unique_test_dir("cli-packages");
    let package_dir = unique_test_dir("local-package");
    write_local_plugin_package(&package_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable");

    assert!(
        enable.status.success(),
        "enable failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let stdout = String::from_utf8(enable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("decision=package"));
    assert!(stdout.contains("package_name=dogfood.plugin"));
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list");

    assert!(
        list.status.success(),
        "packages list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
}

#[test]
fn no_arg_boot_summary_does_not_create_home_or_xdg_state_file() {
    let home = unique_test_dir("home");
    let xdg = unique_test_dir("xdg");
    fs::create_dir_all(&home).expect("create home");
    fs::create_dir_all(&xdg).expect("create xdg");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .output()
        .expect("run botster-hub summary");

    assert!(
        output.status.success(),
        "summary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_state_file_under(&home);
    assert_no_state_file_under(&xdg);
}

fn assert_no_state_file_under(root: &Path) {
    let direct = root.join("hub-state.json");
    let botster = root.join("botster").join("hub-state.json");
    let botster_hub = root.join("botster-hub").join("hub-state.json");

    assert!(!direct.exists(), "unexpected state file at {direct:?}");
    assert!(!botster.exists(), "unexpected state file at {botster:?}");
    assert!(
        !botster_hub.exists(),
        "unexpected state file at {botster_hub:?}"
    );
}
