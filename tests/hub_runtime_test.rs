#![cfg(unix)]

use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    BotsterEngineObservation, ClientId, CoreSessionMetadata, MailboxSendFailureReason,
    PreparedSnapshotRequest, QueueSource, RequestId, ResizePayload, SessionActivityStatus,
    SessionId, SessionLifecycleState, SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory,
    SubscriptionId, TransportEgress,
};
use botster_hub::{
    DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubRuntime, HubRuntimeError,
    HubRuntimeOutput, HubStartupOptions, RuntimeEnvironment, SessionDefaults, TransportBindings,
};

fn explicit_config() -> botster_hub::HubConfig {
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-runtime-test".to_string(),
            display_name: "Hub Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            "target/botster-hub-test-data/runtime".into(),
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit runtime config should build")
}

fn explicit_config_with_data_dir(
    data_directory: impl Into<std::path::PathBuf>,
) -> botster_hub::HubConfig {
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-runtime-test".to_string(),
            display_name: "Hub Runtime Test".to_string(),
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
            .expect("drain runtime through core");
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
fn hub_runtime_spawns_attaches_writes_reads_classifies_and_shuts_down_through_core() {
    let config = explicit_config();
    let mut runtime = HubRuntime::new(config);
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    let client_id = ClientId("fake-client".to_string());
    let subscription_id = SubscriptionId("fake-subscription".to_string());
    let mut logical_clock = 20;

    let spawn = runtime
        .spawn_session(request, CoreSessionMetadata::new())
        .expect("spawn local command through core");
    assert_eq!(spawn.handle.session_id, session_id);
    assert_eq!(spawn.session.lifecycle, SessionLifecycleState::Running);
    assert!(runtime.session(&session_id).is_some());
    assert_eq!(runtime.list_sessions().len(), 1);

    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            logical_clock,
        )
        .expect("attach fake client through core");
    logical_clock += 1;

    let pressure = runtime
        .report_backpressure(
            client_id.clone(),
            session_id.clone(),
            QueueSource::ClientWorker,
            16,
            12,
        )
        .expect("report pressure evidence through core");
    assert!(!pressure.observations.is_empty());

    let ready = drain_until(&mut runtime, &session_id, b"ready", &mut logical_clock);
    assert!(
        ready
            .windows(b"ready".len())
            .any(|window| window == b"ready"),
        "runtime should fan out local command startup output through core"
    );

    runtime
        .write_bytes(
            client_id.clone(),
            session_id.clone(),
            b"ping-hub\n".to_vec(),
            logical_clock,
        )
        .expect("write input through core");
    logical_clock += 1;

    drain_until(
        &mut runtime,
        &session_id,
        b"echo:ping-hub",
        &mut logical_clock,
    );

    assert_eq!(
        runtime
            .classify_activity(&session_id, logical_clock, 5)
            .expect("classify activity through core"),
        SessionActivityStatus::Active
    );
    assert_eq!(
        runtime
            .inspect_session(&session_id, logical_clock, 5)
            .expect("inspect session through core")
            .activity_status,
        SessionActivityStatus::Active
    );
    runtime
        .drain_runtime_all_once(logical_clock)
        .expect("drain all sessions through core");

    let shutdown = runtime
        .shutdown_session(session_id.clone(), "test complete", logical_clock)
        .expect("shutdown through core");
    assert!(shutdown.observations.iter().any(|observation| {
        observation
            == &BotsterEngineObservation::SessionLifecycle {
                session_id: session_id.clone(),
                state: SessionLifecycleState::Stopping,
            }
    }));
    assert!(matches!(
        runtime
            .session(&session_id)
            .map(|session| &session.lifecycle),
        Some(SessionLifecycleState::Stopping)
    ));
}

#[test]
fn hub_runtime_public_facade_includes_audited_core_visibility_and_reporting_methods() {
    type InspectSession = fn(
        &HubRuntime,
        &SessionId,
        u64,
        u64,
    ) -> Result<botster_core::EngineSessionInspection, HubRuntimeError>;
    type ReadScreen =
        fn(&mut HubRuntime, RequestId, SessionId, u64) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type ReplaySnapshot = fn(
        &mut HubRuntime,
        PreparedSnapshotRequest,
        u64,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type ReportBackpressure = fn(
        &mut HubRuntime,
        ClientId,
        SessionId,
        QueueSource,
        usize,
        usize,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type ReportDeliveryLag = fn(
        &mut HubRuntime,
        ClientId,
        SessionId,
        SubscriptionId,
        QueueSource,
        usize,
        usize,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type ReportDeliveryFailure = fn(
        &mut HubRuntime,
        ClientId,
        SessionId,
        SubscriptionId,
        QueueSource,
        MailboxSendFailureReason,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type DetachClient = fn(
        &mut HubRuntime,
        ClientId,
        SessionId,
        SubscriptionId,
        u64,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;
    type Resize = fn(
        &mut HubRuntime,
        ClientId,
        SessionId,
        u16,
        u16,
        u64,
    ) -> Result<HubRuntimeOutput, HubRuntimeError>;

    let _list_sessions: fn(&HubRuntime) -> Vec<botster_core::CoreSession> =
        HubRuntime::list_sessions;
    let _detach_client: DetachClient = HubRuntime::detach_client;
    let _resize: Resize = HubRuntime::resize;
    let _inspect_session: InspectSession = HubRuntime::inspect_session;
    let _read_screen: ReadScreen = HubRuntime::read_screen;
    let _capture_snapshot: ReadScreen = HubRuntime::capture_snapshot;
    let _replay_snapshot: ReplaySnapshot = HubRuntime::replay_snapshot;
    let _drain_all: fn(&mut HubRuntime, u64) -> Result<HubRuntimeOutput, HubRuntimeError> =
        HubRuntime::drain_runtime_all_once;
    let _report_backpressure: ReportBackpressure = HubRuntime::report_backpressure;
    let _report_delivery_lag: ReportDeliveryLag = HubRuntime::report_delivery_lag;
    let _report_delivery_failure: ReportDeliveryFailure = HubRuntime::report_delivery_failure;
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
