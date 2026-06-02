//! Boundary between `botster-hub` product policy and `botster-core` mechanisms.
//!
//! `botster-core` remains the reusable local engine layer. The hub composes
//! core mechanics and owns policy around where they run, which clients may
//! attach, and which providers are admitted.

use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    ClientId, CoreSessionMetadata, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, TransportEgress,
};

use crate::config::HubConfig;
use crate::runtime::HubRuntime;

/// Hub-facing role for the embedded core dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedCoreRole {
    /// Session spawning, PTY/process mechanics, lifecycle, and activity.
    LocalEngineMechanics,
    /// Transport-neutral primitives the hub adapts for clients and providers.
    PrimitiveContracts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOneSmokeRequest {
    pub working_directory: PathBuf,
    pub executable: String,
    pub arguments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOneSmokeReport {
    pub spawned: bool,
    pub attached: bool,
    pub drained_bytes: usize,
    pub shutdown: bool,
}

pub fn run_one_smoke(
    config: HubConfig,
    request: RunOneSmokeRequest,
) -> Result<RunOneSmokeReport, String> {
    let mut runtime = HubRuntime::new(config);
    let spawn_request = build_run_one_spawn_request(request);
    let session_id = spawn_request.session_id.clone();
    let client_id = ClientId("hub-cli-smoke-client".to_string());
    let subscription_id = SubscriptionId("hub-cli-smoke-subscription".to_string());
    let mut logical_clock = 1;

    runtime
        .spawn_session(spawn_request, CoreSessionMetadata::new())
        .map_err(|error| error.to_string())?;
    runtime
        .attach_client(
            client_id,
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .map_err(|error| {
            let _ =
                runtime.shutdown_session(session_id.clone(), "attach failed", logical_clock + 1);
            error.to_string()
        })?;
    logical_clock += 1;

    let drained_bytes = drain_available_bytes(&mut runtime, &session_id, &mut logical_clock)?;

    let shutdown = runtime
        .shutdown_session(session_id, "run-one smoke complete", logical_clock)
        .map(|_| true)
        .or_else(|error| {
            let message = error.to_string();
            if is_session_not_found(&message) {
                Ok(true)
            } else {
                Err(message)
            }
        })?;

    Ok(RunOneSmokeReport {
        spawned: true,
        attached: true,
        drained_bytes,
        shutdown,
    })
}

fn build_run_one_spawn_request(request: RunOneSmokeRequest) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id: RequestId("hub-cli-smoke-spawn".to_string()),
        session_id: SessionId("hub-cli-smoke-session".to_string()),
        executable: request.executable,
        arguments: request.arguments,
        working_directory: SpawnWorkingDirectory {
            path: request.working_directory.display().to_string(),
        },
        environment: SpawnEnvironment::default(),
        initial_pty_size: Some(ResizePayload { rows: 24, cols: 80 }),
    }
}

fn drain_available_bytes(
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    logical_clock: &mut u64,
) -> Result<usize, String> {
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut drained_bytes = 0;

    while Instant::now() < deadline {
        let outcome = runtime
            .drain_runtime_once(session_id, *logical_clock)
            .or_else(|error| {
                if is_session_not_found(&error.to_string()) {
                    Ok(Default::default())
                } else {
                    Err(error.to_string())
                }
            })?;
        *logical_clock += 1;

        let iteration_bytes = outcome
            .client_egress
            .iter()
            .map(|(_, frame)| match frame {
                TransportEgress::TerminalOutput { data, .. }
                | TransportEgress::Snapshot { data, .. }
                | TransportEgress::Scrollback { data, .. }
                | TransportEgress::Binary { data } => data.len(),
                _ => 0,
            })
            .sum::<usize>();
        drained_bytes += iteration_bytes;

        if iteration_bytes == 0 && drained_bytes > 0 {
            break;
        }

        thread::sleep(Duration::from_millis(20));
    }

    Ok(drained_bytes)
}

fn is_session_not_found(message: &str) -> bool {
    message.contains("session not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};

    fn explicit_test_config() -> HubConfig {
        HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(
                "target/botster-hub-test-data/core".into(),
            ),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
        .expect("explicit config")
    }

    #[test]
    fn run_one_spawn_environment_is_empty_default() {
        let request = build_run_one_spawn_request(RunOneSmokeRequest {
            working_directory: PathBuf::from("/tmp"),
            executable: "sh".to_string(),
            arguments: vec!["-c".to_string(), "printf ok".to_string()],
        });

        assert!(request.environment.variables.is_empty());
        assert_eq!(request.executable, "sh");
        assert_eq!(request.working_directory.path, "/tmp");
    }

    #[test]
    fn run_one_uses_real_core_engine() {
        let report = run_one_smoke(
            explicit_test_config(),
            RunOneSmokeRequest {
                working_directory: PathBuf::from("."),
                executable: "sh".to_string(),
                arguments: vec!["-c".to_string(), "printf hub-smoke; sleep 1".to_string()],
            },
        )
        .expect("run real core smoke");

        assert!(report.spawned);
        assert!(report.attached);
        assert!(report.drained_bytes > 0);
        assert!(report.shutdown);
    }
}
