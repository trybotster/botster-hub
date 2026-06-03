#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use botster_core::{
    Capability, CapabilitySurface, CoreSessionMetadata, ExtensionEntrypoint, ExtensionKind,
    ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection, PackageManifest,
    PackageSource, RequestId, ResizePayload, SessionId, SessionSpawnRequest, SpawnEnvironment,
    SpawnWorkingDirectory,
};
use botster_hub::{
    DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubDaemon, HubDaemonState,
    HubStartupOptions, HubStateLoadSource, HubStateStore, PackageAdmissionPolicy,
    PackageProvenance, RuntimeEnvironment, SessionDefaults, TransportBindings,
};

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
        .spawn_session(request, CoreSessionMetadata::new())
        .expect("spawn through embedded core runtime");
    assert_eq!(runtime.list_sessions().len(), 1);
    runtime
        .shutdown_session(session_id, "daemon test complete", 1)
        .expect("shutdown through embedded core runtime");

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
    assert!(!stdout.contains("/Users/"));
    assert!(!stdout.contains("/home/"));
    assert!(data_dir.join("hub-state.json").exists());
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
