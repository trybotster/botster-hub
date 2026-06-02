#![cfg(unix)]

use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    BotsterEngineObservation, ClientId, CoreSessionMetadata, RequestId, ResizePayload,
    SessionActivityStatus, SessionId, SessionLifecycleState, SessionSpawnRequest, SpawnEnvironment,
    SpawnWorkingDirectory, SubscriptionId, TransportEgress,
};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubRuntime, HubStartupOptions, RuntimeEnvironment,
    SessionDefaults, TransportBindings,
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

    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            logical_clock,
        )
        .expect("attach fake client through core");
    logical_clock += 1;

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
