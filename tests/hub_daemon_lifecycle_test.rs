#![cfg(unix)]

use std::fs;
use std::io::Read;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::{
    Capability, CapabilitySurface, CoreSessionMetadata, ExtensionEntrypoint, ExtensionKind,
    ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection, PackageManifest,
    PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, UiActionResultState,
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

static REAL_DAEMON_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
            ..TransportBindings::default()
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
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
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

fn write_local_process_plugin_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create process package root");
    fs::write(root.join("bin").join("plugin"), "#!/bin/sh\n").expect("write process entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "dogfood.process-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "process", "path": "bin/plugin", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write local process package manifest");
}

fn daemon_test_lock() -> &'static Mutex<()> {
    REAL_DAEMON_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn start_cli_daemon(data_dir: &Path) -> Child {
    ensure_session_worker_binary();
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn wait_for_status(data_dir: &Path, child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check daemon child") {
            let mut stdout = String::new();
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stdout.take() {
                let _ = pipe.read_to_string(&mut stdout);
            }
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("daemon exited before ready with {status}: stdout={stdout:?} stderr={stderr:?}");
        }
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status");
        if output.status.success() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("daemon did not become ready");
}

fn shutdown_cli_daemon(data_dir: &Path, child: Child) -> Output {
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run botster-hub shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn has_diagnostic_kind(
    diagnostics: &[botster_hub_client::DaemonDiagnostic],
    kind: botster_hub_client::DaemonDiagnosticKind,
) -> bool {
    diagnostics.iter().any(|diagnostic| diagnostic.kind == kind)
}

fn has_failure_diagnostic(diagnostics: &[botster_hub_client::DaemonDiagnostic]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.kind,
            botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
                | botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
                | botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
                | botster_hub_client::DaemonDiagnosticKind::ActionFailure
                | botster_hub_client::DaemonDiagnosticKind::DaemonStartupFailure
        )
    })
}

fn session_worker_binary_path() -> PathBuf {
    ensure_session_worker_binary();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("botster-session-worker")
}

fn run_command_with_timeout(mut command: Command, timeout: Duration) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn timed command");
    let deadline = std::time::Instant::now() + timeout;

    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("poll timed command").is_some() {
            return child.wait_with_output().expect("collect timed command");
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("collect timed out command");
    panic!(
        "command timed out after {timeout:?}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
fn daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues() {
    let config = explicit_config(unique_test_dir("stale-adoption-socket"));
    let session_id = SessionId("hub-daemon-stale-adoption-socket".to_string());
    let stale_socket = PathBuf::from(format!(
        "/tmp/bh-stale-{}.sock",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let registry = SessionRegistry::new(config.data_directory.clone());
    let mut record = RegistryRecord::running(
        session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-adoption-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    record.observe_restart_contract(
        serde_json::json!({
            "worker_control_socket": stale_socket,
            "mode": "worker_process"
        }),
        2,
    );
    registry
        .save(&record)
        .expect("stale adoption registry fixture should save");

    let mut daemon =
        HubDaemon::start(config).expect("start daemon with stale worker control socket");
    let status = daemon.status();
    assert!(
        status.stale_sessions.contains(&session_id),
        "stale worker control socket should be surfaced in daemon status"
    );

    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-stale-adoption-client");
    let fresh_session_id = SessionId("hub-daemon-fresh-after-stale".to_string());
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-fresh-after-stale-spawn".to_string()),
            session_id: fresh_session_id.clone(),
            command: "printf 'fresh-after-stale-ready\\n'; sleep 1".to_string(),
            now_seconds: 3,
        },
    )
    .expect("fresh session should spawn after stale adoption reconciliation");
    assert!(
        daemon
            .runtime()
            .expect("runtime initialized")
            .list_sessions()
            .expect("list sessions after fresh spawn")
            .iter()
            .any(|session| session.session_id == fresh_session_id),
        "fresh session should be visible after stale adoption reconciliation"
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-start");
    let child = start_cli_daemon(&data_dir);
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
    assert!(stdout.contains("state_source=initialized"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(concat!("/", "Users", "/")));
    assert!(!stdout.contains("/home/"));
    assert!(data_dir.join("hub-state.json").exists());

    let output = shutdown_cli_daemon(&data_dir, child);
    let stdout = String::from_utf8(output.stdout).expect("daemon stdout is utf8");
    assert!(stdout.contains("event=stopped"));
    assert!(stdout.contains("lifecycle_state=stopped"));
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
        !output.status.success(),
        "status unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_sessions_spawn_and_list_route_through_client_api() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-sessions");
    let child = start_cli_daemon(&data_dir);
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("dogfood-session")
        .arg("--")
        .arg("printf 'dogfood-ok\\n'; IFS= read -r line; printf 'dogfood:%s\\n' \"$line\"")
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

    let resize = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("resize")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .arg("30")
        .arg("100")
        .output()
        .expect("run botster-hub sessions resize");
    assert!(
        resize.status.success(),
        "resize failed: {}",
        String::from_utf8_lossy(&resize.stderr)
    );

    let attach = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::Attach {
            session_id: "dogfood-session".to_string(),
            subscription_id: "botster-hub-cli-subscription".to_string(),
        },
    )
    .expect("attach before explicit detach");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    let detach = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("detach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub sessions detach");
    assert!(
        detach.status.success(),
        "detach failed: {}",
        String::from_utf8_lossy(&detach.stderr)
    );

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .arg("--")
        .arg("from-cli\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        send.status.success(),
        "send-input failed: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let attach = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub sessions attach");
    assert!(
        attach.status.success(),
        "attach failed: {}",
        String::from_utf8_lossy(&attach.stderr)
    );
    let stdout = String::from_utf8(attach.stdout).expect("attach stdout is utf8");
    assert!(stdout.contains("dogfood-ok"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_short_lived_session_shutdown_returns_structured_cleanup() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-short-lived-shutdown");
    let child = start_cli_daemon(&data_dir);

    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("dogfood-session")
        .arg("--")
        .arg("printf 'dogfood-ok\\n'; IFS= read -r line; printf 'dogfood:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");
    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );

    let attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run botster-hub sessions attach");

    thread::sleep(Duration::from_millis(150));
    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .arg("--")
        .arg("done\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        send.status.success(),
        "send-input failed: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let attach = attach_child
        .wait_with_output()
        .expect("wait for attach child");
    assert!(
        attach.status.success(),
        "attach failed: {}",
        String::from_utf8_lossy(&attach.stderr)
    );
    let attach_stdout = String::from_utf8(attach.stdout).expect("attach stdout is utf8");
    assert!(attach_stdout.contains("dogfood-ok"));
    assert!(attach_stdout.contains("dogfood:done"));

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub sessions shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let stdout = String::from_utf8(shutdown.stdout).expect("shutdown stdout is utf8");
    let stderr = String::from_utf8(shutdown.stderr).expect("shutdown stderr is utf8");
    assert!(stdout.contains("response=session_cleanup"));
    assert!(stdout.contains("session_id=dogfood-session"));
    assert!(stdout.contains("outcome=already_exited"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-operator-error");
    let child = start_cli_daemon(&data_dir);

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("missing-session")
        .arg("--")
        .arg("input\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        !send.status.success(),
        "missing-session send-input should fail with operator frame"
    );
    let stdout = String::from_utf8(send.stdout).expect("send stdout is utf8");
    let stderr = String::from_utf8(send.stderr).expect("send stderr is utf8");
    assert!(stdout.contains("response=operator_error"));
    assert!(stdout.contains("error_code=unknown_session"));
    assert!(stdout.contains("operation=input"));
    assert!(stderr.contains("operator error: unknown_session"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after operator error");
    assert!(
        status.status.success(),
        "status failed after operator error: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("status stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_daemon_restart_recovers_worker_backed_session_through_transport() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-restart-recover");
    let config = explicit_config(&data_dir);
    let session_id = "cli-restart-session";

    let child = start_cli_daemon(&data_dir);
    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
    )
    .expect("spawn restart recovery session through daemon transport");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);
    assert!(spawn
        .sessions
        .iter()
        .any(|session| session.session_id == session_id && session.lifecycle == "running"));

    shutdown_cli_daemon(&data_dir, child);
    let restarted_child = start_cli_daemon(&data_dir);

    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status)
        .expect("status after daemon restart");
    let status = status.status.expect("status response body");
    assert_eq!(status.lifecycle_state, "running");
    assert!(status.core_initialized);
    assert!(
        status
            .recovered_sessions
            .iter()
            .any(|recovered| recovered == session_id),
        "restarted daemon should report startup recovery for the live worker-backed session"
    );
    assert!(
        !status
            .stale_sessions
            .iter()
            .any(|stale| stale == session_id),
        "worker-backed session with protocol evidence should not be marked stale"
    );

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
            .expect("list recovered session through daemon transport");
    assert!(list
        .sessions
        .iter()
        .any(|session| session.session_id == session_id && session.lifecycle == "running"));

    let resize = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Resize {
            session_id: session_id.to_string(),
            rows: 30,
            cols: 100,
        },
    )
    .expect("resize after daemon restart");
    assert_eq!(resize.kind, botster_hub::DaemonResponseKind::Events);
    let attach_config = config.clone();
    let attach_session_id = SessionId(session_id.to_string());
    let attach_handle = thread::spawn(move || {
        let mut output = Vec::new();
        botster_hub::stream_attach(
            &attach_config,
            attach_session_id,
            SubscriptionId("cli-restart-subscription-after".to_string()),
            &mut output,
        )
        .expect("stream attach after daemon restart");
        output
    });
    thread::sleep(Duration::from_millis(100));
    let send = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "after-restart\n".to_string(),
        },
    )
    .expect("send input after daemon restart");
    assert_eq!(send.kind, botster_hub::DaemonResponseKind::Events);
    let attached_output = attach_handle
        .join()
        .expect("stream attach thread should complete");
    let attached_output = String::from_utf8_lossy(&attached_output);
    assert!(
        attached_output.contains("echo:after-restart"),
        "stream attach should observe post-restart echo, got {attached_output:?}"
    );

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown recovered session through daemon transport");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn external_hub_client_crate_drives_real_daemon_socket_protocol() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("external-hub-client");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    assert_eq!(
        status
            .status
            .as_ref()
            .expect("status response body")
            .lifecycle_state,
        "running"
    );

    let list =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("external client list sessions request");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "external-client-session".to_string(),
            command:
                "printf 'external-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("external client spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(spawn
        .sessions
        .iter()
        .any(|session| session.session_id == "external-client-session"
            && session.lifecycle == "running"));

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external attach request");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let resize = connection
        .request(&botster_hub_client::DaemonRequest::Resize {
            session_id: "external-client-session".to_string(),
            rows: 31,
            cols: 101,
        })
        .expect("external resize request");
    assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "external-client-session".to_string(),
            data: "external-input\n".to_string(),
        })
        .expect("external send input request");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "external-client-session".to_string(),
            })
            .expect("external drain request");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("echo:external-input") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("echo:external-input"),
        "external client should drain terminal output through the hub protocol, got {observed:?}"
    );

    let detach = connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external detach request");
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);

    let terminal_unavailable = connection
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing terminal drain returns operator response");
    assert_eq!(
        terminal_unavailable.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert!(terminal_unavailable.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
            && diagnostic.operation.as_deref() == Some("drain_runtime")
            && diagnostic.feature.as_deref() == Some(botster_hub_client::FEATURE_TERMINAL_STREAMING)
    }));
    assert!(!has_diagnostic_kind(
        &terminal_unavailable.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    let terminal_debug = format!("{:?}", terminal_unavailable.diagnostics);
    assert!(!terminal_debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!terminal_debug.contains(concat!("/", "Users", "/")));
    assert!(!terminal_debug.contains("/home/"));

    let reconnect =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external reconnect");
    drop(reconnect);

    let shutdown_session = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "external-client-session".to_string(),
        },
    )
    .expect("external shutdown session request");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_reports_compatibility_descriptor_and_mismatch_diagnostics() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("compat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path.clone());
    let child = start_cli_daemon(&data_dir);

    let mut stream = UnixStream::connect(&socket_path).expect("connect raw compatibility socket");
    botster_hub_client::write_frame(
        &mut stream,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
        },
    )
    .expect("write hello");
    let ack: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut stream).expect("read hello ack");
    assert_eq!(ack.protocol, botster_hub_client::PROTOCOL);
    assert!(ack.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("hello")
    }));
    assert!(!has_failure_diagnostic(&ack.diagnostics));
    assert_eq!(ack.compatibility.protocol, botster_hub_client::PROTOCOL);
    assert_eq!(
        ack.compatibility.protocol_version,
        botster_hub_client::PROTOCOL_VERSION
    );
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_SESSIONS));
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_TERMINAL_STREAMING));
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_RESIZE));
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER));
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION));
    assert_eq!(
        ack.compatibility.conformance_fixture_revision,
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION
    );

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    let status = status.status.expect("status response body");
    assert_eq!(status.compatibility, ack.compatibility);
    assert!(status.diagnostics.is_empty());

    let mut version_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    version_requirement.client_name = "future-version-client".to_string();
    version_requirement.minimum_protocol_version = botster_hub_client::PROTOCOL_VERSION + 1;
    let version_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &version_requirement)
            .expect_err("future protocol version should fail compatibility");
    let version_message = version_error.to_string();
    assert!(version_message.contains("future-version-client"));
    assert!(version_message.contains("unsupported protocol version"));
    assert!(!version_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(version_error) = version_error
    else {
        panic!("version mismatch should be a compatibility error");
    };
    assert!(version_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
            && diagnostic
                .message
                .as_deref()
                .is_some_and(|message| message.contains("unsupported protocol version"))
    }));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    let mut feature_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    feature_requirement.client_name = "future-feature-client".to_string();
    feature_requirement
        .required_features
        .push("future_feature".to_string());
    let feature_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &feature_requirement)
            .expect_err("future feature should fail compatibility");
    let feature_message = feature_error.to_string();
    assert!(feature_message.contains("future-feature-client"));
    assert!(feature_message.contains("missing required feature(s): future_feature"));
    assert!(!feature_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(feature_error) = feature_error
    else {
        panic!("feature mismatch should be a compatibility error");
    };
    assert!(feature_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.feature.as_deref() == Some("future_feature")
    }));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_test_support_drives_isolated_daemon_socket_protocol() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let first = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("downstream-shape")
        .start()
        .expect("start isolated hub through public test-support harness");
    assert!(first.data_dir().starts_with("/tmp/bh-test-support"));
    assert!(first.endpoint().socket_path.starts_with(first.data_dir()));
    let support_matrix = botster_hub_test_support::first_party_client_support_matrix();
    let first_report =
        botster_hub_test_support::run_client_conformance(&first).expect("run client conformance");
    assert_eq!(first_report.lifecycle_state, "running");
    assert_eq!(first_report.initial_session_count, 0);
    assert_eq!(first_report.spawned_lifecycle, "running");
    assert_eq!(
        support_matrix.session_actions,
        vec![
            "status",
            "list_sessions",
            "spawn",
            "attach",
            "drain",
            "send_input",
            "resize",
            "shutdown_session",
        ]
    );
    assert!(first_report.stream_contains_ready);
    assert!(first_report.stream_contains_echo);
    assert!(first_report.stream_contains_resize);
    assert_eq!(first_report.compatibility_protocol, support_matrix.protocol);
    assert_eq!(
        first_report.compatibility_protocol_version,
        support_matrix.protocol_version
    );
    assert_eq!(
        first_report.compatibility_features,
        support_matrix.supported_features
    );
    assert_eq!(
        first_report.compatibility_conformance_fixture_revision,
        support_matrix.conformance_fixture_revision
    );
    assert_eq!(first_report.connected_diagnostic_operation, "status");
    assert_eq!(first_report.validation_error_operation, "drain_runtime");
    assert_eq!(
        first_report.validation_diagnostic_kind,
        support_matrix
            .terminal_streaming
            .missing_session_diagnostic_kind
    );
    assert!(support_matrix.terminal_streaming.supported);
    assert!(support_matrix.terminal_streaming.held_open_stream);
    assert_eq!(
        support_matrix.terminal_streaming.conformance_ready_output,
        "conformance-ready"
    );
    assert_eq!(
        support_matrix.terminal_streaming.conformance_echo_output,
        "echo:from-conformance"
    );
    assert!(support_matrix.resize.supported);
    assert_eq!(support_matrix.resize.action, "resize");
    assert_eq!(support_matrix.resize.conformance_output_prefix, "winsize:");

    let plugin_report = botster_hub_test_support::run_project_pipelines_conformance(
        &first,
        PathBuf::from("examples/project-pipelines"),
    )
    .expect("run project pipelines conformance");
    assert_eq!(plugin_report.package_state, "enabled");
    assert!(support_matrix.plugin_surfaces.render_supported);
    assert!(support_matrix.plugin_surfaces.action_supported);
    assert_eq!(
        plugin_report.surface_kind,
        support_matrix.plugin_surfaces.rendered_surface_kind
    );
    assert_eq!(
        plugin_report.surface_id,
        support_matrix.plugin_surfaces.rendered_surface_node_id
    );
    assert_eq!(plugin_report.invalid_action_status, "failure");
    assert_eq!(
        plugin_report.invalid_action_diagnostic_kind,
        support_matrix
            .plugin_surfaces
            .invalid_action_diagnostic_kind
    );
    assert_eq!(plugin_report.invalid_title_error, "Title is required");
    first.shutdown().expect("shutdown first isolated hub");

    let second = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("downstream-shape-determinism")
        .start()
        .expect("start second isolated hub through public test-support harness");
    let second_report =
        botster_hub_test_support::run_client_conformance(&second).expect("rerun conformance");
    assert_eq!(second_report, first_report);
    second.shutdown().expect("shutdown second isolated hub");
}

#[test]
fn daemon_detaches_subscription_when_attach_connection_drops() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-attach-eof");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "eof-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn eof test session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let attach = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Attach {
            session_id: "eof-session".to_string(),
            subscription_id: "dropped-subscription".to_string(),
        },
    )
    .expect("attach dropped subscription");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    thread::sleep(Duration::from_millis(150));

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SendInput {
            session_id: "eof-session".to_string(),
            data: "after-eof\r".to_string(),
        },
    )
    .expect("send input after dropped attach");

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut observed_events = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::Drain {
                session_id: "eof-session".to_string(),
            },
        )
        .expect("drain after dropped attach");
        observed_events.extend(drain.events);
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        observed_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "dropped-subscription" && data.contains("after-eof")
            )
        }),
        "dropped attach subscription received later terminal output: {observed_events:?}"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_notify_session_defers_without_observed_readiness_over_socket() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("daemon-notify-session");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "notify-socket-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn guarded socket session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect TUI-grade socket");
    connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "notify-socket-session".to_string(),
            subscription_id: "notify-socket-subscription".to_string(),
        })
        .expect("attach persistent socket subscription");

    let write = connection
        .request(&botster_hub::DaemonRequest::NotifySession {
            session_id: "notify-socket-session".to_string(),
            data: "notify-socket\n".to_string(),
        })
        .expect("notify session over daemon socket");
    assert_eq!(write.kind, botster_hub::DaemonResponseKind::SessionNotified);
    let notify = write
        .coordination
        .and_then(|coordination| coordination.notify)
        .expect("notify response body");
    assert!(notify.decision.starts_with("Defer"));
    assert_eq!(notify.states, vec!["accepted", "deferred"]);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "notify-socket-session".to_string(),
            })
            .expect("drain guarded socket session");
        for event in drain.events {
            if let botster_hub::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("echo:notify-socket") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        !observed.contains("echo:notify-socket"),
        "notify session without observed readiness should not reach PTY input path, got {observed:?}"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_tui_project_pipelines_surface_action_round_trip_uses_plugin_result() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("daemon-tui-project-pipelines");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: PathBuf::from("examples/project-pipelines"),
        },
    )
    .expect("enable project pipelines plugin over daemon socket");
    assert_eq!(
        enabled.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let mut driver =
        botster_hub::tui::ScriptedTuiDriver::connect(config.clone()).expect("connect scripted TUI");
    driver.set_project_pipelines_form("   ", "local_pipeline");
    let invalid_results = driver.submit_project_pipelines_form();
    let invalid = invalid_results.last().expect("invalid action result");
    assert_eq!(invalid.state, UiActionResultState::Rejected);
    assert_eq!(invalid.surface_id.0, "project-pipelines.create-ticket");
    assert_eq!(
        invalid
            .field_errors
            .get("project-pipelines-create-title")
            .and_then(|errors| errors.first())
            .map(String::as_str),
        Some("Title is required")
    );
    assert_eq!(invalid.form_errors, vec!["Title is required".to_string()]);

    driver.set_project_pipelines_form("  TUI dogfood ticket  ", "local.pipeline");
    let valid_results = driver.submit_project_pipelines_form();
    let valid = valid_results.last().expect("valid action result");
    assert_eq!(valid.state, UiActionResultState::Accepted);
    assert_eq!(valid.surface_id.0, "project-pipelines.create-ticket");
    assert_eq!(
        valid.normalized_values.as_ref().unwrap().0["title"],
        "TUI dogfood ticket"
    );

    let context = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "project_pipelines.current_context".to_string(),
            arguments: serde_json::json!({}),
        },
    )
    .expect("read project pipelines current context over daemon socket");
    assert_eq!(
        context.plugin_tool_result["tickets"][0]["title"],
        "TUI dogfood ticket"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn scripted_tui_uses_daemon_socket_for_attach_input_doorbell_resize_and_restart_reconnect() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("scripted-tui");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "scripted-tui-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do if [ \"$line\" = size-check ]; then printf 'winsize:%s\\n' \"$(stty size)\"; else printf 'echo:%s\\n' \"$line\"; fi; done"
                    .to_string(),
        },
    )
    .expect("spawn scripted TUI session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let proof = botster_hub::run_scripted_probe(config.clone(), "scripted-tui-session")
        .expect("scripted TUI probe should complete core workflow");
    assert!(proof
        .rendered_sessions
        .contains(&"scripted-tui-session".to_string()));
    assert!(proof.ui_regions.contains(&"sessions-panel".to_string()));
    assert!(proof.ui_regions.contains(&"activity-panel".to_string()));
    assert!(proof.ui_regions.contains(&"attached-terminal".to_string()));
    assert!(proof.observed_output.contains("echo:from-tui"));
    assert!(proof.observed_output.contains("winsize:31 101"));
    assert!(!proof.observed_output.contains("echo:doorbell-from-tui"));
    assert!(proof.observed_output.contains("echo:after-reattach"));
    assert!(proof.guarded_decision.starts_with("Defer"));
    assert_eq!(proof.guarded_states, vec!["accepted", "deferred"]);
    assert_eq!(proof.resize_sent, Some((31, 101)));
    assert_ne!(
        proof.first_subscription_id, proof.second_subscription_id,
        "TUI reattach should allocate a fresh subscription id"
    );

    let mut driver = botster_hub::ScriptedTuiDriver::connect(config.clone())
        .expect("connect scripted TUI driver before restart");
    driver
        .select_session("scripted-tui-session")
        .expect("select recovered session before restart");
    let before_restart_subscription = driver
        .attach_selected()
        .expect("attach before daemon restart");
    driver.send_input("before-restart\n");
    driver
        .drain_until("echo:before-restart", Duration::from_secs(5))
        .expect("observe pre-restart output");

    shutdown_cli_daemon(&data_dir, child);
    let restarted_child = start_cli_daemon(&data_dir);
    driver
        .reconnect()
        .expect("scripted TUI should reconnect after daemon restart");
    let after_restart_subscription = driver
        .subscription_id()
        .expect("reconnect should reattach recovered session");
    assert_ne!(
        before_restart_subscription, after_restart_subscription,
        "daemon restart reconnect must discard stale subscription id"
    );
    driver.send_input("after-restart\n");
    driver
        .drain_until("echo:after-restart", Duration::from_secs(5))
        .expect("TUI should observe output after daemon restart reconnect");
    assert!(driver.output().contains("echo:after-restart"));

    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn scripted_tui_surfaces_session_lost_when_restart_does_not_recover_attached_session() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("scripted-tui-session-lost");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "scripted-lost-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn scripted lost-session fixture");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let mut driver = botster_hub::ScriptedTuiDriver::connect(config.clone())
        .expect("connect scripted TUI driver before lost-session restart");
    driver
        .select_session("scripted-lost-session")
        .expect("select session before loss");
    driver
        .attach_selected()
        .expect("attach before intentional session loss");
    driver.send_input("before-loss\n");
    driver
        .drain_until("echo:before-loss", Duration::from_secs(5))
        .expect("observe pre-loss output");

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: "scripted-lost-session".to_string(),
        },
    )
    .expect("shut down session before daemon restart");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted_child = start_cli_daemon(&data_dir);
    driver
        .reconnect()
        .expect("TUI reconnect should keep operator in status/session view");
    assert!(
        driver.subscription_id().is_none(),
        "unrecovered session should not keep or recreate a subscription"
    );
    assert!(
        driver
            .errors()
            .iter()
            .any(|error| error.contains("attached session was not recovered")),
        "TUI should surface visible session-lost error, got {:?}",
        driver.errors()
    );

    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn scripted_tui_detaches_and_refreshes_when_drain_reports_unknown_session() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("tui-drain-loss");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "scripted-drain-loss-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn scripted drain-loss fixture");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let mut driver = botster_hub::ScriptedTuiDriver::connect(config.clone())
        .expect("connect scripted TUI driver before drain loss");
    driver
        .select_session("scripted-drain-loss-session")
        .expect("select session before drain loss");
    driver.attach_selected().expect("attach before drain loss");
    driver.send_input("before-drain-loss\n");
    driver
        .drain_until("echo:before-drain-loss", Duration::from_secs(5))
        .expect("observe pre-loss output");

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: "scripted-drain-loss-session".to_string(),
        },
    )
    .expect("shut down attached session before drain");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );

    for _ in 0..3 {
        driver
            .drain_once()
            .expect("drain after attached session disappeared");
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        driver.active_session_id().is_none(),
        "drain-time UnknownSession should clear the active session"
    );
    assert!(
        driver.subscription_id().is_none(),
        "drain-time UnknownSession should clear the stale subscription"
    );
    let errors = driver.errors();
    let session_lost_rows = errors
        .iter()
        .filter(|error| error.contains("attached session disappeared"))
        .count();
    assert_eq!(
        session_lost_rows, 1,
        "TUI should surface exactly one actionable session-loss row, got {errors:?}"
    );
    assert!(
        !errors
            .iter()
            .any(|error| error.contains("unknown_session: runtime failed")),
        "TUI should suppress the generic repeated drain error, got {errors:?}"
    );

    let replacement = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "drain-replacement".to_string(),
            command:
                "printf 'replacement-ready\\n'; while IFS= read -r line; do printf 'replacement:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn replacement session after drain loss");
    assert_eq!(
        replacement.kind,
        botster_hub::DaemonResponseKind::Spawned,
        "replacement spawn failed with {:?}",
        replacement.error
    );
    driver
        .select_session("drain-replacement")
        .expect("select replacement session after drain loss");
    assert!(
        driver
            .session_ids()
            .contains(&"drain-replacement".to_string()),
        "TUI should refresh sessions after drain loss"
    );
    driver
        .attach_selected()
        .expect("attach replacement session after drain loss");
    driver.send_input("after-drain-loss\n");
    driver
        .drain_until("replacement:after-drain-loss", Duration::from_secs(5))
        .expect("TUI should remain usable after drain loss");

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn stalled_attach_stdout_does_not_block_other_daemon_commands() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-stalled-attach");
    let child = start_cli_daemon(&data_dir);

    let mut spawn_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    spawn_command
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("slow-consumer")
        .arg("--")
        .arg(
            "i=0; while [ \"$i\" -lt 50000 ]; do printf 'flood-line-%05d\\n' \"$i\"; i=$((i + 1)); done; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        );
    let spawn = run_command_with_timeout(spawn_command, Duration::from_secs(3));
    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );

    let mut attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn stalled attach");
    thread::sleep(Duration::from_millis(500));
    assert!(
        attach_child
            .try_wait()
            .expect("poll stalled attach")
            .is_none(),
        "attach exited before the slow-consumer check"
    );

    let mut list_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    list_command
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir);
    let list = run_command_with_timeout(list_command, Duration::from_secs(2));
    assert!(
        list.status.success(),
        "list failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&list.stderr)
    );

    let mut send_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    send_command
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .arg("--")
        .arg("still-responsive\r");
    let send = run_command_with_timeout(send_command, Duration::from_secs(2));
    assert!(
        send.status.success(),
        "send-input failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let mut resize_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    resize_command
        .arg("sessions")
        .arg("resize")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .arg("32")
        .arg("120");
    let resize = run_command_with_timeout(resize_command, Duration::from_secs(2));
    assert!(
        resize.status.success(),
        "resize failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&resize.stderr)
    );

    let mut shutdown_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    shutdown_command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir);
    let shutdown = run_command_with_timeout(shutdown_command, Duration::from_secs(2));
    assert!(
        shutdown.status.success(),
        "shutdown failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );

    let _ = attach_child.kill();
    let _ = attach_child.wait_with_output();
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
fn cli_packages_enable_local_path_routes_through_running_daemon_and_persists() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-packages");
    let package_dir = unique_test_dir("local-package");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

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

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after package enable");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("enabled_package_count=1"));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert_eq!(
        lifecycle.kind,
        botster_hub::DaemonResponseKind::PluginLifecycle
    );
    assert!(
        lifecycle.lifecycle.iter().any(|plugin| {
            plugin.package_name == "dogfood.plugin" && plugin.state == "enabled" && plugin.loaded
        }),
        "enabled package should load into daemon lifecycle without restart"
    );

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

    let providers = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("providers")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub providers list");
    assert!(
        providers.status.success(),
        "providers list failed: {}",
        String::from_utf8_lossy(&providers.stderr)
    );
    let stdout = String::from_utf8(providers.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=providers"));
    assert!(stdout.contains("package_count=0"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let list_after_restart = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after restart");
    assert!(
        list_after_restart.status.success(),
        "packages list after restart failed: {}",
        String::from_utf8_lossy(&list_after_restart.stderr)
    );
    let stdout = String::from_utf8(list_after_restart.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=enabled"));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn cli_packages_enable_local_process_package_does_not_attempt_lua_load() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-process-package");
    let package_dir = unique_test_dir("local-process-package");
    write_local_process_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable process package");

    assert!(
        enable.status.success(),
        "enable process package failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "dogfood.process-plugin"
            && plugin.state == "enabled"
            && !plugin.loaded
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_without_running_daemon_does_not_mutate_hub_state() {
    let data_dir = unique_test_dir("cli-packages-offline");
    let package_dir = unique_test_dir("local-package-offline");
    write_local_plugin_package(&package_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable without daemon");

    assert!(
        !enable.status.success(),
        "offline enable unexpectedly succeeded: {}",
        String::from_utf8_lossy(&enable.stdout)
    );
    let stderr = String::from_utf8(enable.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(
        !data_dir.join("hub-state.json").exists(),
        "offline package mutation should not create durable state"
    );
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
