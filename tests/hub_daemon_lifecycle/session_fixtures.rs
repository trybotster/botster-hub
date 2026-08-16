#![allow(dead_code, unused_imports)]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, ClientId, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, TransportEgress,
    decrypt_aes_gcm, encrypt_aes_gcm,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi,
    HubClientEvent, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState,
    HubPackageManifest, HubStartupOptions, HubStateLoadSource, HubStateStore,
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, PackageAdmissionPolicy, PackageProvenance,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings,
};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, block_on, channel, default_runtime, sleep,
    timeout,
};

use crate::support::{
    ensure_session_worker_binary, recovering_mutex_guard, validate_cli_daemon_shutdown,
    wait_for_cli_daemon_shutdown,
};

use super::*;

pub(crate) fn live_output_utf8(
    payload: impl std::borrow::Borrow<botster_hub_client::DaemonLiveOutputPayload>,
) -> String {
    String::from_utf8_lossy(&payload.borrow().decoded_bytes().unwrap_or_default()).into_owned()
}

pub(crate) fn live_output_contains(
    payload: impl std::borrow::Borrow<botster_hub_client::DaemonLiveOutputPayload>,
    needle: &str,
) -> bool {
    payload
        .borrow()
        .decoded_bytes()
        .map(|bytes| {
            bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes())
        })
        .unwrap_or(false)
}

pub(crate) const STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES: usize = 8 * 1024;
pub(crate) const STALLED_ATTACH_STABLE_SAMPLES: usize = 5;
pub(crate) fn spawn_request(config: &botster_hub::HubConfig) -> SessionSpawnRequest {
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

pub(crate) struct SessionCleanupGuard {
    pub(crate) data_dir: PathBuf,
    pub(crate) session_id: &'static str,
    pub(crate) armed: bool,
}

impl SessionCleanupGuard {
    pub(crate) fn new(data_dir: &Path, session_id: &'static str) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            session_id,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SessionCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
                .arg("sessions")
                .arg("shutdown")
                .arg("--data-dir")
                .arg(&self.data_dir)
                .arg(self.session_id)
                .output();
        }
    }
}

pub(crate) const GHOSTSNP_MAGIC: &[u8] = b"GHOSTSNP";

pub(crate) fn wait_for_mode_flags<F>(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: &str,
    mut predicate: F,
) -> botster_hub_client::DaemonModeFlags
where
    F: FnMut(&botster_hub_client::DaemonModeFlags) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let _ = connection.request(&botster_hub_client::DaemonRequest::drain_subscription(
            session_id,
            subscription_id,
        ));
        let _ = connection.request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.to_string(),
        });
        let response = connection
            .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
                session_id: session_id.to_string(),
            })
            .expect("read_mode_flags");
        if response.kind != botster_hub_client::DaemonResponseKind::ReadModeFlags {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for mode flags on {session_id}: {:?} error={:?}",
                response.kind,
                response.error
            );
            thread::sleep(Duration::from_millis(20));
            continue;
        }
        let mode_flags = response.mode_flags.expect("mode flags body");
        if predicate(&mode_flags) {
            return mode_flags;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for mode flags on {session_id}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) fn collect_attach_events(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: &str,
    until_live_marker: Option<&str>,
) -> Vec<botster_hub_client::DaemonEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::drain_subscription(
                session_id,
                subscription_id,
            ))
            .expect("drain");
        assert!(
            drain.events.iter().all(|event| !matches!(
                event,
                botster_hub_client::DaemonEvent::AttachState { .. }
                    | botster_hub_client::DaemonEvent::Snapshot { .. }
                    | botster_hub_client::DaemonEvent::Scrollback { .. }
                    | botster_hub_client::DaemonEvent::TerminalOutput { .. }
                    | botster_hub_client::DaemonEvent::ProcessExit { .. }
            )),
            "host Drain must not return terminal bodies: {:?}",
            drain.events
        );
        events.extend(connection.take_skipped_events());
        for envelope in connection.take_skipped_terminal() {
            if let Ok(bytes) = envelope.payload_bytes() {
                if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                {
                    events.push(event);
                } else {
                    events.push(botster_hub_client::DaemonEvent::TerminalOutput {
                        session_id: session_id.to_string(),
                        subscription_id: subscription_id.to_string(),
                        payload: botster_hub_client::DaemonLiveOutputPayload::from_bytes(&bytes),
                    });
                }
            }
        }
        let saw_live = until_live_marker.is_none_or(|marker| {
            events.iter().any(|event| {
                matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalOutput {
                        payload,
                        ..
                    } if live_output_contains(payload, marker)
                )
            }) || connection
                .request(&botster_hub_client::DaemonRequest::ReadScreen {
                    session_id: session_id.to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .is_some_and(|screen| screen.text.contains(marker))
        });
        if saw_live {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    events
}

pub(crate) fn first_snapshot_payload(
    events: &[botster_hub_client::DaemonEvent],
    subscription_id: &str,
) -> Vec<u8> {
    for event in events {
        if let botster_hub_client::DaemonEvent::Snapshot {
            subscription_id: sub,
            history,
            ..
        } = event
            && sub == subscription_id
        {
            return history
                .decoded_bytes()
                .expect("snapshot payload decodes")
                .to_vec();
        }
    }
    panic!("expected Snapshot event for {subscription_id}, got {events:?}");
}

pub(crate) fn install_incremental_attach_snapshots(
    events: &[botster_hub_client::DaemonEvent],
    subscription_id: &str,
    rows: u16,
    cols: u16,
) -> botster_terminal_ghostty::GhosttyClientProjection {
    let mut projection = botster_terminal_ghostty::GhosttyClientProjection::new(
        botster_core::TerminalScreenSize::new(rows, cols),
    )
    .expect("create incremental client projection");
    let mut saw_ready = false;
    for event in events {
        let botster_hub_client::DaemonEvent::Snapshot {
            subscription_id: event_subscription,
            history,
            ..
        } = event
        else {
            continue;
        };
        if event_subscription != subscription_id {
            continue;
        }
        let bytes = history
            .decoded_bytes()
            .expect("snapshot payload decodes")
            .to_vec();
        if !saw_ready {
            assert_eq!(
                projection
                    .install_ghostsnp_ready(bytes)
                    .expect("READY snapshot"),
                botster_terminal_ghostty::GhosttySnapshotDecodeProgress::Ready
            );
            saw_ready = true;
        } else {
            let _ = projection
                .apply_ghostsnp_history(bytes)
                .expect("PAGE or FINISH snapshot");
        }
    }
    assert!(
        saw_ready,
        "expected a READY Snapshot for {subscription_id}, got {events:?}"
    );
    projection
}

/// Production path: shut down the durable worker session, then remove it from the registry.
pub(crate) fn production_shutdown_and_remove_session(
    endpoint: &botster_hub_client::DaemonEndpoint,
    session_id: &str,
) {
    let shutdown = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("production ShutdownSession");
    assert_eq!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "ShutdownSession should return events for {session_id}"
    );
    let remove = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("production RemoveSession");
    assert_eq!(
        remove.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved,
        "RemoveSession should remove {session_id}"
    );
}

pub(crate) fn session_ids_from_list(endpoint: &botster_hub_client::DaemonEndpoint) -> Vec<String> {
    let list =
        botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list sessions");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);
    list.sessions
        .into_iter()
        .map(|session| session.session_id)
        .collect()
}

pub(crate) fn start_isolated_live_output_hub(name: &str) -> botster_hub_test_support::IsolatedHub {
    start_isolated_live_output_hub_with_env(name, &[])
}

pub(crate) fn start_isolated_live_output_hub_with_env(
    name: &str,
    extra_env: &[(&str, &str)],
) -> botster_hub_test_support::IsolatedHub {
    let mut builder = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(unique_short_test_dir(name))
        .name(name);
    for (key, value) in extra_env {
        builder = builder.env(*key, *value);
    }
    builder
        .start()
        .expect("start isolated hub with explicit worker path")
}

pub(crate) fn live_output_decoded_bytes(
    payload: impl std::borrow::Borrow<botster_hub_client::DaemonLiveOutputPayload>,
) -> Vec<u8> {
    payload
        .borrow()
        .decoded_bytes()
        .expect("validated live payload decodes")
}

pub(crate) fn event_is_exact_live_payload(
    event: &botster_hub_client::DaemonEvent,
    subscription_id: &str,
    expected: &[u8],
) -> bool {
    match event {
        botster_hub_client::DaemonEvent::TerminalOutput {
            subscription_id: event_subscription_id,
            payload,
            ..
        } if event_subscription_id == subscription_id => {
            live_output_decoded_bytes(payload) == expected
        }
        _ => false,
    }
}

pub(crate) fn python_bytes_literal(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| byte.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn python_script_command(script_path: &Path) -> String {
    format!("python3 -u {}", script_path.display())
}

pub(crate) fn drain_until(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    predicate: impl FnMut(&botster_hub_client::DaemonEvent) -> bool,
) -> Vec<botster_hub_client::DaemonEvent> {
    drain_until_subscription(connection, session_id, None, predicate)
}

pub(crate) fn drain_until_subscription(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: Option<&str>,
    predicate: impl FnMut(&botster_hub_client::DaemonEvent) -> bool,
) -> Vec<botster_hub_client::DaemonEvent> {
    drain_until_subscription_deadline(
        connection,
        session_id,
        subscription_id,
        Duration::from_secs(5),
        predicate,
    )
}

pub(crate) fn drain_until_subscription_deadline(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: Option<&str>,
    timeout: Duration,
    mut predicate: impl FnMut(&botster_hub_client::DaemonEvent) -> bool,
) -> Vec<botster_hub_client::DaemonEvent> {
    let deadline = Instant::now() + timeout;
    let mut events = Vec::new();
    while Instant::now() < deadline {
        let drain = connection
            .request(&match subscription_id {
                Some(subscription_id) => botster_hub_client::DaemonRequest::drain_subscription(
                    session_id,
                    subscription_id,
                ),
                None => botster_hub_client::DaemonRequest::drain_session(session_id),
            })
            .expect("drain live output");
        events.extend(drain.events);
        events.extend(connection.take_skipped_events());
        for envelope in connection.take_skipped_terminal() {
            if let Ok(bytes) = envelope.payload_bytes() {
                if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                {
                    events.push(event);
                } else {
                    events.push(botster_hub_client::DaemonEvent::TerminalOutput {
                        session_id: session_id.to_string(),
                        subscription_id: subscription_id.unwrap_or("").to_string(),
                        payload: botster_hub_client::DaemonLiveOutputPayload::from_bytes(&bytes),
                    });
                }
            }
        }
        if events.iter().any(&mut predicate) {
            return events;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for live output predicate, events={events:?}");
}

pub(crate) fn wait_for_session_type_metadata(
    subscription: &mut botster_hub_client::DaemonEntitySubscription,
    session_id: &str,
    session_type_id: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let entity = loop {
        assert!(
            Instant::now() < deadline,
            "session metadata entity timed out"
        );
        let frame = subscription
            .next_frame()
            .expect("read session metadata frame");
        let candidate = match frame {
            botster_hub_client::DaemonEntityFrame::Snapshot { items, .. } => items
                .into_iter()
                .filter_map(|item| {
                    serde_json::from_value::<botster_hub_client::DaemonSessionEntity>(item).ok()
                })
                .find(|entity| entity.session_uuid == session_id),
            botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. }
                if id == session_id =>
            {
                Some(
                    serde_json::from_value::<botster_hub_client::DaemonSessionEntity>(entity)
                        .expect("deserialize session entity upsert"),
                )
            }
            _ => None,
        };
        if let Some(entity) = candidate {
            break entity;
        }
    };
    assert_eq!(entity.session_type_id.as_deref(), Some(session_type_id));
    assert_eq!(entity.session_type_source.as_deref(), Some("repo"));
    assert_eq!(entity.role.as_deref(), Some("botster.agent"));
    assert_eq!(entity.interaction.as_deref(), Some("interactive"));
    assert_eq!(entity.session_type_lifecycle.as_deref(), Some("task"));
    assert_eq!(entity.traits, vec!["test"]);
}
