#![allow(dead_code, unused_imports)]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
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
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, decrypt_aes_gcm,
    encrypt_aes_gcm,
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

pub(crate) fn start_cli_daemon(data_dir: &Path) -> PanicSafeCliDaemon {
    start_cli_daemon_with_worker_egress_capacity(data_dir, None)
}

pub(crate) fn start_cli_daemon_with_worker_egress_capacity(
    data_dir: &Path,
    egress_capacity: Option<usize>,
) -> PanicSafeCliDaemon {
    check_harness_taint();
    ensure_session_worker_binary();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(capacity) = egress_capacity {
        command.env(
            "BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY",
            capacity.to_string(),
        );
    }
    configure_test_process_group(&mut command);
    let child = command.spawn().expect("spawn botster-hub start");
    let mut daemon = PanicSafeCliDaemon::from_child(data_dir, child, "lifecycle daemon");
    wait_for_status(data_dir, daemon.child_mut());
    daemon
}

pub(crate) fn start_cli_daemon_with_snapshot_history_failure(
    data_dir: &Path,
) -> PanicSafeCliDaemon {
    check_harness_taint();
    ensure_session_worker_binary();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .env("BOTSTER_HUB_TEST_FAIL_SNAPSHOT_HISTORY_AFTER_READY", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command
        .spawn()
        .expect("spawn botster-hub start with snapshot history failure");
    let mut daemon =
        PanicSafeCliDaemon::from_child(data_dir, child, "snapshot-history-failure daemon");
    wait_for_status(data_dir, daemon.child_mut());
    daemon
}

pub(crate) fn start_cli_daemon_with_home(data_dir: &Path, home: &Path) -> PanicSafeCliDaemon {
    check_harness_taint();
    ensure_session_worker_binary();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .env("HOME", home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command.spawn().expect("spawn botster-hub start");
    let mut daemon = PanicSafeCliDaemon::from_child(data_dir, child, "home-scoped daemon");
    wait_for_status(data_dir, daemon.child_mut());
    daemon
}

/// A schema-2 managed installation receipt for real-daemon fixtures.
///
/// Schema 1 is cold-turkey rejected, so every fixture carries the full schema-2
/// shape. The daemon under test is a development build with no embedded build
/// revision, so `build_revision` agreement is skipped rather than failed: a
/// value cannot disagree with the absence of one.
pub(crate) fn managed_receipt(source_url: &str) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 2,
        "product_id": "botster-hub",
        "binary_version": env!("CARGO_PKG_VERSION"),
        "installation_mode": "managed",
        "release_channel": "stable",
        "provider": "http_json",
        "source_url": source_url,
        "build_revision": "release1",
        "artifacts": [
            {"name": "botster-hub", "sha256": "a".repeat(64), "size": 1024},
            {"name": "botster-session-worker", "sha256": "b".repeat(64), "size": 2048}
        ],
        "source_revisions": {
            "botster_hub": "0".repeat(40),
            "botster_core": "1".repeat(40)
        },
        "signature": {
            "algorithm": "ed25519",
            "key_id": "test-only-do-not-trust",
            "signed_manifest_sha256": "c".repeat(64)
        },
        "installer": {"id": "botster-hub-installer", "version": "0.1.0"}
    })
}

pub(crate) fn spawn_release_metadata_fixture(
    metadata: serde_json::Value,
    request_count: usize,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind release metadata fixture");
    let address = listener.local_addr().expect("release fixture address");
    let body = serde_json::to_vec(&metadata).expect("serialize release metadata");
    let handle = thread::spawn(move || {
        for _ in 0..request_count {
            let (mut stream, _) = listener.accept().expect("accept release metadata request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set release fixture read timeout");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .expect("write release response headers");
            stream
                .write_all(&body)
                .expect("write release response body");
        }
    });
    (format!("http://{address}/botster-hub.json"), handle)
}

pub(crate) fn spawn_release_metadata_sequence_fixture(
    metadata: Vec<serde_json::Value>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind release metadata sequence");
    let address = listener.local_addr().expect("release sequence address");
    let handle = thread::spawn(move || {
        for metadata in metadata {
            let body = serde_json::to_vec(&metadata).expect("serialize release metadata");
            let (mut stream, _) = listener.accept().expect("accept release metadata request");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .expect("write release response headers");
            stream
                .write_all(&body)
                .expect("write release response body");
        }
    });
    (format!("http://{address}/botster-hub.json"), handle)
}

pub(crate) fn spawn_stalled_release_metadata_fixture(
    metadata: serde_json::Value,
) -> (
    String,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled release fixture");
    let address = listener.local_addr().expect("stalled fixture address");
    let body = serde_json::to_vec(&metadata).expect("serialize stalled release metadata");
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept stalled release request");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set stalled fixture read timeout");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        accepted_tx.send(()).expect("report stalled request");
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("release stalled request");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .expect("write stalled response headers");
        stream
            .write_all(&body)
            .expect("write stalled response body");
    });
    (
        format!("http://{address}/botster-hub.json"),
        accepted_rx,
        release_tx,
        handle,
    )
}

pub(crate) fn spawn_timeout_release_metadata_fixture()
-> (String, mpsc::Receiver<()>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind timeout release fixture");
    let address = listener.local_addr().expect("timeout fixture address");
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept timeout release request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        accepted_tx.send(()).expect("report timeout request");
        thread::sleep(Duration::from_secs(4));
    });
    (
        format!("http://{address}/botster-hub.json"),
        accepted_rx,
        handle,
    )
}

pub(crate) fn start_owned_incompatible_local_runtime_daemon(data_dir: &Path) -> PanicSafeCliDaemon {
    check_harness_taint();
    ensure_session_worker_binary();
    fs::create_dir_all(data_dir).expect("create data dir");
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .env("BOTSTER_HUB_TEST_INCOMPATIBLE_DAEMON", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command
        .spawn()
        .expect("spawn incompatible botster-hub start");
    let mut daemon =
        PanicSafeCliDaemon::from_child(data_dir, child, "incompatible local-runtime daemon");
    wait_for_incompatible_status(data_dir, daemon.child_mut());
    write_local_runtime_daemon_metadata(data_dir, daemon.id());
    daemon
}

pub(crate) fn stable_path_string(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

pub(crate) fn wait_for_status(data_dir: &Path, child: &mut Child) {
    wait_for_status_with_budget(data_dir, child, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET)
        .unwrap_or_else(|error| panic!("{error}"));
}

pub(crate) fn wait_for_status_with_budget(
    data_dir: &Path,
    child: &mut Child,
    readiness_budget: Duration,
) -> Result<(), String> {
    let mut last_status = "status probe not attempted".to_string();
    wait_for_child_condition_with_budget(
        child,
        "waiting for typed daemon status readiness",
        readiness_budget,
        || {
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status");
        last_status = command_output_text(&output);
            output.status.success()
        },
    )
    .map_err(|error| {
        format!(
            "daemon did not become ready (readiness budget {readiness_budget:?}); last status output={last_status:?}; {error}"
        )
    })
}

pub(crate) fn shutdown_cli_daemon(data_dir: &Path, daemon: PanicSafeCliDaemon) -> Output {
    daemon.shutdown_at(data_dir)
}

pub(crate) fn request_cli_daemon_shutdown(data_dir: &Path) -> io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
}

pub(crate) struct PanicSafeCliDaemon {
    pub(crate) data_dir: PathBuf,
    pub(crate) child: Option<Child>,
    pub(crate) panic_context: &'static str,
    pub(crate) inspect_local_webrtc_sender: bool,
    pub(crate) mode: GuardCleanupMode,
    pub(crate) test_hooks: GuardTestHooks,
    pub(crate) shutdown_records: Vec<SessionShutdownRecord>,
    pub(crate) known_worker_pids: Vec<u32>,
}

impl PanicSafeCliDaemon {
    pub(crate) fn from_child(data_dir: &Path, child: Child, panic_context: &'static str) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            child: Some(child),
            panic_context,
            inspect_local_webrtc_sender: false,
            mode: GuardCleanupMode::Full,
            test_hooks: GuardTestHooks::default(),
            shutdown_records: Vec::new(),
            known_worker_pids: Vec::new(),
        }
    }

    pub(crate) fn start(data_dir: &Path, panic_context: &'static str) -> Self {
        let mut daemon = start_cli_daemon(data_dir);
        daemon.panic_context = panic_context;
        daemon
    }

    pub(crate) fn start_with_local_webrtc_diagnostics(data_dir: &Path) -> Self {
        let mut daemon = start_cli_daemon(data_dir);
        daemon.panic_context = "local WebRTC target sender evidence";
        daemon.inspect_local_webrtc_sender = true;
        daemon
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("lifecycle daemon child")
    }

    pub(crate) fn transfer_sessions(mut self) -> Self {
        self.mode = GuardCleanupMode::TransferSessions;
        self
    }

    pub(crate) fn disarm(mut self) -> Child {
        self.mode = GuardCleanupMode::Disarmed;
        self.child.take().expect("lifecycle daemon child")
    }

    pub(crate) fn wait_with_output(mut self) -> io::Result<Output> {
        self.mode = GuardCleanupMode::Disarmed;
        self.child
            .take()
            .expect("lifecycle daemon child")
            .wait_with_output()
    }

    pub(crate) fn shutdown(self) {
        let data_dir = self.data_dir.clone();
        let _ = self.shutdown_at(&data_dir);
    }

    pub(crate) fn shutdown_at(mut self, data_dir: &Path) -> Output {
        let transfer = matches!(self.mode, GuardCleanupMode::TransferSessions);
        let _ = self.cleanup_owned_resources(CleanupTrigger::Explicit);
        let child = self.child.take().expect("lifecycle daemon child");
        let shutdown = request_cli_daemon_shutdown(data_dir).unwrap_or_else(|error| {
            panic!("run botster-hub shutdown: {error}");
        });
        let output = wait_for_cli_daemon_shutdown(&shutdown, child);
        let proof = if transfer {
            prove_hub_and_socket_absent(data_dir, None)
        } else {
            prove_owned_children_absent(data_dir, None, &self.known_worker_pids)
        };
        if let Err(error) = proof {
            record_harness_taint(format!("{}: {error}", self.panic_context));
            panic!("{error}");
        }
        output
    }

    pub(crate) fn cleanup_owned_resources(
        &mut self,
        trigger: CleanupTrigger,
    ) -> Result<Vec<SessionShutdownRecord>, String> {
        if matches!(self.mode, GuardCleanupMode::Disarmed) {
            return Ok(Vec::new());
        }
        let skip_sessions = matches!(self.mode, GuardCleanupMode::TransferSessions);
        let socket_present = daemon_socket_path(&self.data_dir).exists();

        if !skip_sessions {
            self.known_worker_pids
                .extend(collect_owned_session_process_pids(&self.data_dir).unwrap_or_default());
            if socket_present {
                match shutdown_owned_sessions(&self.data_dir) {
                    Ok(records) => {
                        self.shutdown_records = records;
                    }
                    Err(error) => {
                        eprintln!(
                            "{}: production session cleanup failed ({trigger:?}): {error}",
                            self.panic_context
                        );
                        match reap_registry_backed_workers(&self.data_dir) {
                            Ok(pids) => self.known_worker_pids.extend(pids),
                            Err(backstop) => eprintln!(
                                "{}: registry-backed backstop failed: {backstop}",
                                self.panic_context
                            ),
                        }
                    }
                }
            } else {
                match reap_registry_backed_workers(&self.data_dir) {
                    Ok(pids) => self.known_worker_pids.extend(pids),
                    Err(error) => eprintln!(
                        "{}: dead-daemon backstop failed: {error}",
                        self.panic_context
                    ),
                }
            }
        }

        if self.test_hooks.force_absence_unproven {
            record_harness_taint(format!(
                "{}: injected prove-absence failure",
                self.panic_context
            ));
            return Err("injected prove-absence failure".to_string());
        }
        if matches!(trigger, CleanupTrigger::Explicit) {
            return Ok(self.shutdown_records.clone());
        }

        let hub_pid = self.child.as_ref().map(Child::id);
        let _ = request_cli_daemon_shutdown(&self.data_dir);
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
            && let Err(error) = try_terminate_and_reap_child(child)
        {
            record_harness_taint(format!(
                "{}: Hub child absence unproven: {error}",
                self.panic_context
            ));
            if !std::thread::panicking() {
                return Err(error);
            }
            eprintln!("{}: {error}", self.panic_context);
        }

        let proof = if self.test_hooks.force_absence_unproven {
            Err("injected prove-absence failure".to_string())
        } else {
            prove_owned_children_absent(&self.data_dir, hub_pid, &self.known_worker_pids)
        };
        if let Err(error) = proof {
            record_harness_taint(format!("{}: {error}", self.panic_context));
            if !matches!(trigger, CleanupTrigger::Panic) && !std::thread::panicking() {
                return Err(error);
            }
            eprintln!("{}: {error}", self.panic_context);
        }
        Ok(self.shutdown_records.clone())
    }
}

impl Deref for PanicSafeCliDaemon {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.child.as_ref().expect("lifecycle daemon child")
    }
}

impl DerefMut for PanicSafeCliDaemon {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.as_mut().expect("lifecycle daemon child")
    }
}

impl Drop for PanicSafeCliDaemon {
    fn drop(&mut self) {
        if self.child.is_none() || matches!(self.mode, GuardCleanupMode::Disarmed) {
            return;
        }
        let trigger = if std::thread::panicking() {
            CleanupTrigger::Panic
        } else {
            CleanupTrigger::Drop
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.cleanup_owned_resources(trigger);
        }))
        .is_err()
        {
            eprintln!(
                "{}: cleanup itself panicked; continuing Hub reap",
                self.panic_context
            );
        }
        let Some(mut child) = self.child.take() else {
            return;
        };
        let shutdown = request_cli_daemon_shutdown(&self.data_dir);
        let shutdown_failed = shutdown.as_ref().map_or(true, |output| {
            !output.status.success()
                && String::from_utf8_lossy(&output.stderr).trim()
                    != "botster-hub shutdown error: client disconnected"
        });
        if shutdown_failed && child.try_wait().ok().flatten().is_none() {
            if let Err(error) = try_terminate_and_reap_child(&mut child) {
                record_harness_taint(format!(
                    "{}: drop-time Hub reap failed: {error}",
                    self.panic_context
                ));
                eprintln!("{}: {error}", self.panic_context);
            }
        } else if let Err(error) = child.wait() {
            eprintln!(
                "{}: unavailable; daemon_status=unavailable; daemon_wait_error_kind={:?}",
                self.panic_context,
                error.kind()
            );
        }
    }
}

pub(crate) fn shell_output(script: &str) -> Output {
    Command::new("sh")
        .arg("-c")
        .arg(script)
        .output()
        .expect("run shell fixture")
}

pub(crate) fn run_local_runtime_up(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
) -> Output {
    ensure_runtime_packages(data_dir, web_package_path, tui_package_path);
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run botster-hub up")
}

pub(crate) fn run_local_runtime_smoke(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
) -> Output {
    run_local_runtime_smoke_with_fault(
        data_dir,
        _project_pipelines_package_path,
        web_package_path,
        tui_package_path,
        _workspaces_package_path,
        _web_port,
        None,
    )
}

pub(crate) fn run_local_runtime_smoke_with_fault(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
    close_operation: Option<&str>,
) -> Output {
    ensure_runtime_packages(data_dir, web_package_path, tui_package_path);
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("smoke")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    if let Some(operation) = close_operation {
        command.env(TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV, operation);
    }
    command.output().expect("run botster-hub smoke")
}

pub(crate) fn ensure_runtime_packages(
    data_dir: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
) {
    ensure_session_worker_binary();
    let config = explicit_config(data_dir);
    let mut setup_daemon =
        match botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status) {
            Ok(_) => None,
            Err(_) => Some(start_cli_daemon_with_session_worker(
                data_dir,
                &session_worker_binary_path(),
            )),
        };
    let installed =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list installed runtime packages")
            .packages;
    for (name, path) in [
        ("botster-web", web_package_path),
        ("botster-tui", tui_package_path),
    ] {
        if !installed.iter().any(|package| package.package_name == name) {
            let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
                .arg("packages")
                .arg("install")
                .arg("--data-dir")
                .arg(data_dir)
                .arg("--path")
                .arg(path)
                .output()
                .expect("install runtime package");
            assert!(
                install.status.success(),
                "install {name} failed: {}",
                command_output_text(&install)
            );
        }
        let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("packages")
            .arg("enable")
            .arg("--data-dir")
            .arg(data_dir)
            .arg(name)
            .output()
            .expect("enable runtime package");
        assert!(
            enable.status.success(),
            "enable {name} failed: {}",
            command_output_text(&enable)
        );
    }
    if let Some(child) = setup_daemon.as_mut() {
        shutdown_local_runtime_daemon(data_dir);
        let status = child.wait().expect("wait for setup daemon");
        assert!(status.success(), "setup daemon exited with {status}");
    }
}

pub(crate) fn assert_smoke_owned_daemon_gone(data_dir: &Path) {
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run botster-hub status after smoke-owned daemon cleanup");
    assert!(
        !status.status.success(),
        "smoke should stop the daemon it started: {}",
        command_output_text(&status)
    );
}

pub(crate) fn has_diagnostic_kind(
    diagnostics: &[botster_hub_client::DaemonDiagnostic],
    kind: botster_hub_client::DaemonDiagnosticKind,
) -> bool {
    diagnostics.iter().any(|diagnostic| diagnostic.kind == kind)
}

pub(crate) fn has_failure_diagnostic(diagnostics: &[botster_hub_client::DaemonDiagnostic]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.kind,
            botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
                | botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
                | botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
                | botster_hub_client::DaemonDiagnosticKind::WorkerCompatibility
                | botster_hub_client::DaemonDiagnosticKind::ActionFailure
                | botster_hub_client::DaemonDiagnosticKind::DaemonStartupFailure
        )
    })
}

pub(crate) struct TimedCommandOutput {
    pub(crate) output: Output,
    pub(crate) elapsed: Duration,
}

impl TimedCommandOutput {
    pub(crate) fn diagnostics(&self) -> String {
        format!(
            "elapsed={:?} status={} stdout={:?} stderr={:?}",
            self.elapsed,
            self.output.status,
            String::from_utf8_lossy(&self.output.stdout),
            String::from_utf8_lossy(&self.output.stderr),
        )
    }
}

pub(crate) fn child_state_diagnostics(child: &mut Child) -> String {
    match child.try_wait().expect("poll child for diagnostics") {
        None => "running".to_string(),
        Some(status) => {
            let (stdout, stderr) = collect_child_output(child);
            format!("exited status={status} stdout={stdout:?} stderr={stderr:?}")
        }
    }
}

pub(crate) fn run_command_with_timeout_diagnostics(
    stage: &str,
    mut command: Command,
    timeout: Duration,
) -> TimedCommandOutput {
    let started_at = std::time::Instant::now();
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn timed command");
    let deadline = started_at + timeout;

    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("poll timed command").is_some() {
            return TimedCommandOutput {
                output: child.wait_with_output().expect("collect timed command"),
                elapsed: started_at.elapsed(),
            };
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("collect timed out command");
    panic!(
        "{stage} timed out after {:?} (budget {timeout:?}): status={} stdout={:?} stderr={:?}",
        started_at.elapsed(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[derive(Debug)]
pub(crate) struct BufferedStdoutObservation {
    pub(crate) available_bytes: usize,
    pub(crate) elapsed: Duration,
    pub(crate) recent_samples: VecDeque<(Duration, usize)>,
}
