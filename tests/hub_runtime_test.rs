#![cfg(unix)]

use std::fs;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    ClientId, CoreSessionMetadata, CredentialRecord, CredentialStore, CredentialStoreError,
    ModeFlags, RequestId, ResizePayload, SessionId, SessionLifecycleState, SessionSpawnRequest,
    SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, TransportEgress,
};
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, GuardedWriteRequest, ReadinessEvidence,
    RegistrySessionState, SessionAdoptionState,
};
use botster_hub::{
    CredentialKeyPurpose, CredentialKeyReference, CredentialProviderKind, DataDirectoryOption,
    FileHubStateStore, HostIdentityOptions, HubRuntime, HubRuntimeError, HubStartupOptions,
    HubStateStore, RuntimeEnvironment, SessionDefaults, TestFileCredentialStore, TransportBindings,
    TrustedBrowserIdentity, credential_key_id,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_config() -> botster_hub::HubConfig {
    explicit_config_with_data_dir("target/botster-hub-test-data/runtime")
}

fn explicit_config_with_data_dir(
    data_directory: impl Into<std::path::PathBuf>,
) -> botster_hub::HubConfig {
    ensure_session_worker_binary();
    let data_directory = data_directory.into();
    let _ = fs::remove_dir_all(&data_directory);
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-runtime-test".to_string(),
            display_name: "Hub Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory),
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
    .expect("explicit runtime config should build")
}

fn spawn_request(config: &botster_hub::HubConfig) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id: RequestId("hub-runtime-spawn".to_string()),
        session_id: SessionId("hub-runtime-session".to_string()),
        executable: config.session_defaults.shell.clone(),
        arguments: vec![
            "-c".to_string(),
            "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                .to_string(),
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

#[derive(Debug, Clone)]
struct UnavailableCredentialStore;

impl CredentialStore for UnavailableCredentialStore {
    fn get(&self, _key: &str) -> Result<Option<CredentialRecord>, CredentialStoreError> {
        Err(CredentialStoreError::Rejected(
            "credential provider unavailable".to_string(),
        ))
    }

    fn set(&mut self, _key: &str, _record: CredentialRecord) -> Result<(), CredentialStoreError> {
        Err(CredentialStoreError::Rejected(
            "credential provider unavailable".to_string(),
        ))
    }

    fn delete(&mut self, _key: &str) -> Result<(), CredentialStoreError> {
        Err(CredentialStoreError::Rejected(
            "credential provider unavailable".to_string(),
        ))
    }
}

fn drain_until(
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let output = runtime
            .drain_runtime_once(session_id, *logical_clock)
            .expect("drain runtime through core daemon");
        *logical_clock += 1;

        for (_, frame) in output.client_egress {
            if let TransportEgress::TerminalOutput { data, .. } = frame {
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

#[test]
fn hub_runtime_routes_production_session_verbs_through_core_daemon() {
    let config = explicit_config();
    let mut runtime = HubRuntime::new(config);
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    let client_id = ClientId("fake-client".to_string());
    let subscription_id = SubscriptionId("fake-subscription".to_string());
    let mut logical_clock = 20;

    let spawn = runtime
        .spawn_session(request, CoreSessionMetadata::new(), logical_clock)
        .expect("spawn local command through core daemon");
    logical_clock += 1;
    assert_eq!(spawn.session_id, session_id);
    assert_eq!(spawn.lifecycle, SessionLifecycleState::Running);
    assert!(
        runtime
            .session(&session_id)
            .expect("daemon session lookup")
            .is_some()
    );
    assert_eq!(
        runtime.list_sessions().expect("daemon list").len(),
        1,
        "hub visibility should come from core daemon registry"
    );

    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach fake client through core daemon");
    logical_clock += 1;

    drain_until(&mut runtime, &session_id, b"ready", &mut logical_clock);

    runtime
        .resize(
            client_id.clone(),
            session_id.clone(),
            30,
            100,
            logical_clock,
        )
        .expect("resize through core daemon");
    logical_clock += 1;
    let listed = runtime.list_sessions().expect("daemon list after resize");
    assert_eq!(listed[0].size.rows, 30);
    assert_eq!(listed[0].size.cols, 100);

    runtime
        .write_bytes(
            client_id.clone(),
            session_id.clone(),
            b"ping-hub\n".to_vec(),
            logical_clock,
        )
        .expect("write input through core daemon");
    logical_clock += 1;
    drain_until(
        &mut runtime,
        &session_id,
        b"echo:ping-hub",
        &mut logical_clock,
    );

    runtime
        .shutdown_session(session_id.clone(), logical_clock)
        .expect("shutdown through core daemon");
    let listed = runtime.list_sessions().expect("daemon list after shutdown");
    assert_eq!(listed[0].registry_state, RegistrySessionState::Exited);
}

#[test]
fn hub_runtime_starts_with_available_empty_credential_store() {
    let config = explicit_config_with_data_dir("target/botster-hub-test-data/runtime-empty-creds");
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let credential_store = TestFileCredentialStore::new(
        config
            .data_directory
            .join("test-credentials")
            .join("credentials.json"),
    );

    let runtime = HubRuntime::load_from_store_with_credentials(
        config,
        &store,
        CredentialProviderKind::TestFile,
        &credential_store,
    )
    .expect("empty credential store is valid first boot state");

    assert!(runtime.state().credential_keys.is_empty());
    assert!(runtime.state().trusted_browser_identities.is_empty());
}

#[test]
fn hub_runtime_reloads_trusted_browser_identity_when_credential_reference_resolves() {
    let config =
        explicit_config_with_data_dir("target/botster-hub-test-data/runtime-trusted-browser");
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let key_id = credential_key_id(
        &config.host.id,
        CredentialKeyPurpose::BrowserIdentity,
        "browser-a",
    );
    let public_key = b"runtime browser public key".to_vec();
    let mut credential_store = TestFileCredentialStore::new(
        config
            .data_directory
            .join("test-credentials")
            .join("credentials.json"),
    );
    credential_store
        .set(&key_id, CredentialRecord::new(vec![19, 23, 29, 31]))
        .expect("write explicit test credential");

    store
        .update(&config, |state| {
            state.credential_keys.push(CredentialKeyReference {
                key_id: key_id.clone(),
                provider: CredentialProviderKind::TestFile,
                purpose: CredentialKeyPurpose::BrowserIdentity,
                created_at_unix_ms: 10,
                rotated_at_unix_ms: None,
            });
            let mut browser = TrustedBrowserIdentity::trusted(
                "browser-a",
                public_key.clone(),
                10,
                "trust runtime browser fixture",
            );
            browser.credential_key_id = Some(key_id.clone());
            state.trusted_browser_identities.push(browser);
        })
        .expect("persist trusted browser metadata");

    let runtime = HubRuntime::load_from_store_with_credentials(
        config,
        &store,
        CredentialProviderKind::TestFile,
        &credential_store,
    )
    .expect("trusted browser metadata should validate against credential store");

    assert_eq!(runtime.state().credential_keys.len(), 1);
    assert_eq!(runtime.state().trusted_browser_identities.len(), 1);
    assert!(runtime.state().trusted_browser_identities[0].is_trusted_at(20));
}

#[test]
fn hub_runtime_fails_closed_when_required_credential_store_is_unavailable() {
    let config =
        explicit_config_with_data_dir("target/botster-hub-test-data/runtime-unavailable-creds");
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let key_id = credential_key_id(
        &config.host.id,
        CredentialKeyPurpose::BrowserIdentity,
        "browser-a",
    );

    store
        .update(&config, |state| {
            state.credential_keys.push(CredentialKeyReference {
                key_id: key_id.clone(),
                provider: CredentialProviderKind::TestFile,
                purpose: CredentialKeyPurpose::BrowserIdentity,
                created_at_unix_ms: 10,
                rotated_at_unix_ms: None,
            });
        })
        .expect("persist credential reference");

    let error = match HubRuntime::load_from_store_with_credentials(
        config,
        &store,
        CredentialProviderKind::TestFile,
        &UnavailableCredentialStore,
    ) {
        Ok(_) => panic!("required credential provider failure must fail closed"),
        Err(error) => error,
    };

    assert!(matches!(error, HubRuntimeError::Credentials(_)));
    assert!(error.to_string().contains("credential provider rejected"));
}

#[test]
fn hub_runtime_uses_worker_backed_sessions_and_adopts_after_daemon_restart() {
    let config = explicit_config_with_data_dir("target/botster-hub-test-data/runtime-adoption");
    let session_id = SessionId("hub-runtime-adoption-session".to_string());
    let client_id = ClientId("adoption-client".to_string());
    let subscription_id = SubscriptionId("adoption-subscription".to_string());
    let mut logical_clock = 200;

    {
        let mut runtime = HubRuntime::new(config.clone());
        let mut request = spawn_request(runtime.config());
        request.session_id = session_id.clone();
        runtime
            .spawn_session(request, CoreSessionMetadata::new(), logical_clock)
            .expect("hub runtime should spawn through worker-backed core daemon");
        logical_clock += 1;

        let listed = runtime.list_sessions().expect("daemon list");
        assert_eq!(listed[0].session_id, session_id);
        assert!(
            listed[0]
                .process
                .as_ref()
                .and_then(|process| process.pid)
                .is_some(),
            "worker-backed spawn should persist a child process identity"
        );

        let reports = runtime
            .adoption_scan()
            .expect("worker-backed hub runtime should scan adoption evidence");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, SessionAdoptionState::Adoptable);
        assert!(
            reports[0]
                .record
                .recovery_identity
                .as_ref()
                .and_then(|identity| identity.get("worker_control_socket"))
                .is_some(),
            "hub-created session should carry worker control socket evidence"
        );
        runtime.release_sessions_for_restart();
    }

    let mut restarted = HubRuntime::new(config);
    let reports = restarted
        .adoption_scan()
        .expect("fresh hub runtime should classify released worker");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].state, SessionAdoptionState::Adoptable);
    restarted
        .adopt_session(&session_id, logical_clock)
        .expect("fresh hub runtime should adopt live worker");
    logical_clock += 1;

    restarted
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach through adopted worker");
    logical_clock += 1;
    restarted
        .write_bytes(
            client_id.clone(),
            session_id.clone(),
            b"after-adopt\n".to_vec(),
            logical_clock,
        )
        .expect("input through adopted worker");
    logical_clock += 1;
    drain_until(
        &mut restarted,
        &session_id,
        b"echo:after-adopt",
        &mut logical_clock,
    );
    restarted
        .shutdown_session(session_id.clone(), logical_clock)
        .expect("shutdown adopted worker through hub runtime");
    let listed = restarted
        .list_sessions()
        .expect("registry should list adopted shutdown");
    assert_eq!(listed[0].registry_state, RegistrySessionState::Exited);
}

#[test]
fn hub_runtime_guarded_write_delegates_readiness_and_delivery_state_to_core_daemon() {
    let config = explicit_config_with_data_dir("target/botster-hub-test-data/runtime-guarded");
    let mut runtime = HubRuntime::new(config);
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    let client_id = ClientId("guarded-client".to_string());
    let subscription_id = SubscriptionId("guarded-subscription".to_string());
    let mut logical_clock = 100;

    runtime
        .spawn_session(request, CoreSessionMetadata::new(), logical_clock)
        .expect("spawn for guarded write");
    logical_clock += 1;
    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach for guarded write");
    logical_clock += 1;

    let mode_flags = ModeFlags {
        cursor_visible: true,
        ..ModeFlags::default()
    };
    let written = runtime
        .guarded_write(GuardedWriteRequest {
            session_id: session_id.clone(),
            client_id: client_id.clone(),
            data: b"guarded\n".to_vec(),
            readiness: ReadinessEvidence::ready(mode_flags),
            now_seconds: logical_clock,
        })
        .expect("ready guarded write should cross core daemon");
    logical_clock += 1;
    assert!(matches!(written.decision, GuardedWriteDecision::Write));
    assert_eq!(
        written.states,
        vec![
            GuardedWriteDeliveryState::Accepted,
            GuardedWriteDeliveryState::Written
        ],
        "hub must not fabricate delivered or acknowledged states"
    );
    drain_until(
        &mut runtime,
        &session_id,
        b"echo:guarded",
        &mut logical_clock,
    );

    let deferred = runtime
        .guarded_write(GuardedWriteRequest {
            session_id: session_id.clone(),
            client_id: client_id.clone(),
            data: b"deferred\n".to_vec(),
            readiness: ReadinessEvidence::default(),
            now_seconds: logical_clock,
        })
        .expect("absent readiness evidence should be core-deferred");
    assert!(matches!(
        deferred.decision,
        GuardedWriteDecision::Defer { .. }
    ));
    assert_eq!(
        deferred.states,
        vec![
            GuardedWriteDeliveryState::Accepted,
            GuardedWriteDeliveryState::Deferred
        ]
    );
}

#[test]
fn runtime_boot_loads_hub_state_from_configured_data_directory() {
    let config = explicit_config_with_data_dir("target/botster-hub-test-data/runtime-load");
    let store = FileHubStateStore::for_data_directory(&config.data_directory);

    let runtime = HubRuntime::load(config.clone()).expect("load runtime through state store");

    assert_eq!(runtime.state().host, config.host);
    assert_eq!(
        runtime.state().runtime_settings.data_directory,
        config.data_directory
    );
    assert!(store.path().exists());
}
