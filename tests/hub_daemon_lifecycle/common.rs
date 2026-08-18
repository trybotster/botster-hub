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

pub(crate) static REAL_DAEMON_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) const BOTSTER_WEB_READINESS_LIVENESS_BACKSTOP: Duration = Duration::from_secs(60);
pub(crate) const BOTSTER_WEB_READINESS_STARTUP_DELAY_MS: u64 = 3_000;
pub(crate) const TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV: &str =
    "BOTSTER_HUB_TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS";

pub(crate) fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let mixed = nanos ^ (u128::from(std::process::id()) << 48);
    PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("daemon")
        .join(name)
        .join(mixed.to_string())
}

pub(crate) fn unique_short_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("/tmp").join(format!("bh-{name}-{}-{nanos}", std::process::id()))
}

pub(crate) fn explicit_config(data_directory: impl Into<PathBuf>) -> botster_hub::HubConfig {
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit daemon config should build")
}

pub(crate) fn empty_registry() -> PackageRegistry {
    PackageRegistry::new(Vec::<Capability>::new().into_iter().collect())
}

pub(crate) fn generated_typescript_interface(artifact: &str, name: &str) -> String {
    let start = artifact
        .find(&format!("export interface {name} {{"))
        .unwrap_or_else(|| panic!("generated daemon protocol should export {name}"));
    let rest = &artifact[start..];
    let end = rest
        .find("\n}\n")
        .unwrap_or_else(|| panic!("generated daemon protocol should close {name}"));
    rest[..end + 3].to_string()
}

pub(crate) fn assert_no_raw_html_ui_fields(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for forbidden in ["html", "raw_html", "inner_html", "srcdoc"] {
                assert!(
                    !object.contains_key(forbidden),
                    "iframe render must expose an asset URL reference instead of raw HTML field {forbidden}"
                );
            }
            for child in object.values() {
                assert_no_raw_html_ui_fields(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                assert_no_raw_html_ui_fields(item);
            }
        }
        _ => {}
    }
}

pub(crate) fn decode_hex_bytes(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

pub(crate) fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn wait_for_app_local_url(
    data_dir: &Path,
    entrypoint_id: &str,
    expected_url: &str,
) -> botster_hub::DaemonResponse {
    let mut last_response = None;
    for _ in 0..50 {
        let response = botster_hub::daemon_transport_request(
            &explicit_config(data_dir),
            botster_hub::DaemonRequest::ListApps,
        )
        .expect("list apps while waiting for local url");
        if app_row(&response, entrypoint_id)
            .launch_target
            .local_url
            .as_deref()
            == Some(expected_url)
        {
            return response;
        }
        last_response = Some(response);
        thread::sleep(Duration::from_millis(20));
    }
    let response = last_response.expect("at least one list apps response");
    panic!(
        "expected app {entrypoint_id} local_url {expected_url}, got {:?}",
        app_row(&response, entrypoint_id).launch_target.local_url
    );
}

pub(crate) fn read_json_health(url: &str) -> serde_json::Value {
    let (_, body) = read_http_path(url, "/health");
    serde_json::from_str(body.trim()).expect("health JSON")
}

pub(crate) fn read_http_path(url: &str, path: &str) -> (String, String) {
    let port = url
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP endpoint");
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write HTTP request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");
    let (headers, body) = response.split_once("\r\n\r\n").expect("HTTP response body");
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked_http_body(body)
    } else {
        body.to_string()
    };
    (headers.to_string(), body)
}

pub(crate) fn unused_loopback_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind unused loopback port")
        .local_addr()
        .expect("local addr")
        .port()
}

pub(crate) fn decode_chunked_http_body(body: &str) -> String {
    let mut rest = body;
    let mut decoded = String::new();
    loop {
        let (size_line, after_size) = rest.split_once("\r\n").expect("chunk size");
        let size = usize::from_str_radix(size_line.trim(), 16).expect("hex chunk size");
        if size == 0 {
            return decoded;
        }
        decoded.push_str(&after_size[..size]);
        rest = &after_size[size + 2..];
    }
}

pub(crate) fn command_output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

impl TestChildControl for Child {
    fn try_wait_status(&mut self) -> io::Result<Option<String>> {
        self.try_wait()
            .map(|status| status.map(|status| status.to_string()))
    }

    fn terminate_and_reap(&mut self) -> String {
        terminate_and_reap_child(self)
    }

    fn captured_output(&mut self) -> String {
        let (stdout, stderr) = collect_child_output(self);
        format!("stdout={stdout:?} stderr={stderr:?}")
    }
}

impl TestChildControl for Box<dyn portable_pty::Child + Send + Sync> {
    fn try_wait_status(&mut self) -> io::Result<Option<String>> {
        self.try_wait()
            .map(|status| status.map(|status| format!("{status:?}")))
    }

    fn terminate_and_reap(&mut self) -> String {
        terminate_and_reap_pty_child(self.as_mut())
    }

    fn captured_output(&mut self) -> String {
        String::new()
    }
}

pub(crate) fn wait_for_child_condition_with_budget(
    child: &mut impl TestChildControl,
    description: &str,
    budget: Duration,
    mut condition_met: impl FnMut() -> bool,
) -> Result<(), String> {
    let started_at = Instant::now();
    let deadline = started_at + budget;
    while Instant::now() < deadline {
        if condition_met() {
            return Ok(());
        }
        match child.try_wait_status() {
            Ok(Some(status)) => {
                let output = child.captured_output();
                if condition_met() {
                    return Ok(());
                }
                return Err(format!(
                    "{description}: child exited before condition after {:?} (backstop {budget:?}); child_status={status}; {output}",
                    started_at.elapsed()
                ));
            }
            Ok(None) => {}
            Err(error) => {
                let child_status = child.terminate_and_reap();
                let output = child.captured_output();
                return Err(format!(
                    "{description}: failed to poll child after {:?} (backstop {budget:?}): {error}; child_status={child_status}; {output}",
                    started_at.elapsed()
                ));
            }
        }
        thread::sleep(Duration::from_millis(20));
    }

    let child_status = child.terminate_and_reap();
    let output = child.captured_output();
    let (resource, probe) = classify_budget_expiry("child_condition", None, Some(&output));
    Err(format!(
        "{description}: condition not met after {:?} (backstop {budget:?}); child_status={child_status}; {output}; {}",
        started_at.elapsed(),
        format_harness_budget_expired("child_condition", budget, resource, probe, &output)
    ))
}

pub(crate) fn terminate_and_reap_pty_child(child: &mut dyn portable_pty::Child) -> String {
    match child.try_wait() {
        Ok(Some(status)) => return format!("{status:?}"),
        Ok(None) => {}
        Err(error) => return format!("poll_error={error}"),
    }

    if let Some(pid) = child.process_id()
        && pid > 1
        && pid != std::process::id()
    {
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGTERM);
        }
        for _ in 0..25 {
            match child.try_wait() {
                Ok(Some(status)) => return format!("{status:?}"),
                Ok(None) => thread::sleep(Duration::from_millis(20)),
                Err(error) => return format!("poll_error={error}"),
            }
        }
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
    let _ = child.kill();
    child
        .wait()
        .map(|status| format!("{status:?}"))
        .unwrap_or_else(|error| format!("wait_error={error}"))
}

pub(crate) fn wait_for_owned_pid_exit(pid: u32, budget: Duration) {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline && process_exists(pid) {
        thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) fn assert_detached_daemon_stdin(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let stdin = fs::read_link(format!("/proc/{pid}/fd/0"))
            .unwrap_or_else(|error| panic!("read detached daemon pid {pid} stdin: {error}"));
        assert_eq!(
            stdin,
            Path::new("/dev/null"),
            "detached daemon pid {pid} retained operator-console stdin"
        );
    }

    #[cfg(target_os = "macos")]
    {
        let output = Command::new("lsof")
            .args(["-a", "-p", &pid.to_string(), "-d", "0", "-Fn"])
            .output()
            .unwrap_or_else(|error| panic!("inspect detached daemon pid {pid} stdin: {error}"));
        assert!(
            output.status.success(),
            "inspect detached daemon pid {pid} stdin: {}",
            command_output_text(&output)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == "n/dev/null"),
            "detached daemon pid {pid} retained operator-console stdin: {}",
            command_output_text(&output)
        );
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!("detached daemon stdin assertion is unsupported on this Unix target for pid {pid}");
}

pub(crate) fn daemon_test_lock() -> &'static Mutex<()> {
    REAL_DAEMON_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn daemon_test_guard() -> std::sync::MutexGuard<'static, ()> {
    check_harness_taint();
    recovering_mutex_guard(daemon_test_lock())
}

pub(crate) fn wait_for_incompatible_status(data_dir: &Path, child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last_output = String::new();
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check incompatible daemon child") {
            let mut stdout = String::new();
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stdout.take() {
                let _ = pipe.read_to_string(&mut stdout);
            }
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!(
                "incompatible daemon exited before ready with {status}: stdout={stdout:?} stderr={stderr:?}"
            );
        }
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status against incompatible daemon");
        last_output = command_output_text(&output);
        if !output.status.success()
            && (last_output.contains("running daemon is incompatible or stale")
                || last_output.contains("hub predates compatibility handshake"))
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("incompatible daemon did not become ready; last status output: {last_output}");
}

pub(crate) fn terminate_and_reap_child(child: &mut Child) -> String {
    try_terminate_and_reap_child(child).unwrap_or_else(|error| panic!("{error}"))
}

pub(crate) fn signal_test_group_or_child(pid: u32, signal: libc::c_int) -> io::Result<()> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(group_error);
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(child_error)
    }
}

pub(crate) fn collect_child_output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

pub(crate) fn shutdown_local_runtime_daemon(data_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run local-runtime shutdown");
    assert!(
        output.status.success(),
        "local-runtime shutdown failed: {}",
        command_output_text(&output)
    );
}

pub(crate) fn wait_for_buffered_child_stdout(
    child: &mut Child,
    minimum_bytes: usize,
    stable_samples_required: usize,
    timeout: Duration,
) -> Result<BufferedStdoutObservation, String> {
    let started_at = std::time::Instant::now();
    let deadline = started_at + timeout;
    let mut previous_bytes = None;
    let mut stable_samples = 0;
    let mut last_available_bytes = 0;
    let mut recent_samples = VecDeque::with_capacity(25);

    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll buffered stdout child") {
            let (stdout, stderr) = collect_child_output(child);
            return Err(format!(
                "child exited with {status} before stdout backpressure after {:?}: available_bytes={last_available_bytes} recent_samples={recent_samples:?} stdout={stdout:?} stderr={stderr:?}",
                started_at.elapsed()
            ));
        }

        let stdout = child.stdout.as_ref().expect("buffered stdout child pipe");
        last_available_bytes = pipe_bytes_available(stdout)
            .map_err(|error| format!("inspect buffered child stdout: {error}"))?;
        if recent_samples.len() == recent_samples.capacity() {
            recent_samples.pop_front();
        }
        recent_samples.push_back((started_at.elapsed(), last_available_bytes));
        if last_available_bytes >= minimum_bytes {
            if previous_bytes == Some(last_available_bytes) {
                stable_samples += 1;
            } else {
                stable_samples = 0;
            }
            if stable_samples >= stable_samples_required {
                return Ok(BufferedStdoutObservation {
                    available_bytes: last_available_bytes,
                    elapsed: started_at.elapsed(),
                    recent_samples,
                });
            }
        } else {
            stable_samples = 0;
        }
        previous_bytes = Some(last_available_bytes);
        thread::sleep(Duration::from_millis(20));
    }

    let child_status = terminate_and_reap_child(child);
    let (stdout, stderr) = collect_child_output(child);
    Err(format!(
        "stdout did not reach stable backpressure within {timeout:?}: minimum_bytes={minimum_bytes} stable_samples_required={stable_samples_required} last_available_bytes={last_available_bytes} recent_samples={recent_samples:?} child_status={child_status} stdout={stdout:?} stderr={stderr:?}"
    ))
}

pub(crate) fn pipe_bytes_available(pipe: &impl AsRawFd) -> io::Result<usize> {
    let mut available: libc::c_int = 0;
    let result = unsafe { libc::ioctl(pipe.as_raw_fd(), libc::FIONREAD, &mut available) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(available.max(0) as usize)
    }
}

pub(crate) fn payload_has_utf8_replacement(bytes: &[u8]) -> bool {
    bytes.windows(3).any(|window| window == [0xEF, 0xBF, 0xBD])
}

pub(crate) fn shutdown_short_lived_session(
    endpoint: &botster_hub_client::DaemonEndpoint,
    session_id: &str,
) {
    let shutdown = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown short-lived session");
    assert!(
        matches!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
                | botster_hub_client::DaemonResponseKind::SessionCleanup
        ),
        "shutdown should complete {session_id}, got {:?} error={:?}",
        shutdown.kind,
        shutdown.error
    );
}

pub(crate) fn daemon_endpoint(
    config: &botster_hub::HubConfig,
) -> botster_hub_client::DaemonEndpoint {
    botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    )
}

pub(crate) fn wait_for_managed_git_session_exit(data_dir: &Path, session_id: &str) {
    let started_at = Instant::now();
    let mut ever_observed = false;
    let mut last_listing = "<no ListSessions response>".to_string();
    let mut last_drain = "<no Drain response>".to_string();
    let mut drained_events = Vec::new();

    loop {
        let elapsed = started_at.elapsed();
        assert!(
            elapsed < LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
            "managed Git session {session_id} did not emit SessionLifecycle exited and remain \
             retained at lifecycle exited within {:?}; elapsed={elapsed:?} \
             ever_observed={ever_observed} last_listing={last_listing} \
             last_drain={last_drain} drained_events={drained_events:?}",
            LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
        );

        let drain = botster_hub::daemon_transport_request(
            &explicit_config(data_dir),
            botster_hub::DaemonRequest::Drain {
                session_id: session_id.to_string(),
                subscription_id: None,
            },
        )
        .unwrap_or_else(|error| {
            panic!(
                "Drain failed while waiting for managed Git session {session_id} to exit; \
                 elapsed={elapsed:?} ever_observed={ever_observed} \
                 last_listing={last_listing}; last_drain={last_drain}; \
                 drained_events={drained_events:?}; error={error}"
            )
        });
        assert_eq!(
            drain.kind,
            botster_hub::DaemonResponseKind::Events,
            "unexpected Drain response while waiting for managed Git session {session_id} to \
             exit; elapsed={elapsed:?} response={drain:?}"
        );
        last_drain = format!("{drain:?}");
        drained_events.extend(drain.events.iter().map(|event| format!("{event:?}")));

        let mut observed_exited_lifecycle = false;
        for event in &drain.events {
            match event {
                botster_hub::DaemonEvent::SessionLifecycle {
                    session_id: event_session_id,
                    state,
                } if event_session_id == session_id => match state.as_str() {
                    "starting" | "running" | "stopping" => {}
                    "exited" => observed_exited_lifecycle = true,
                    "failed" => panic!(
                        "managed Git session {session_id} emitted lifecycle failed while \
                         draining; elapsed={elapsed:?} last_drain={last_drain} \
                         drained_events={drained_events:?}"
                    ),
                    lifecycle => panic!(
                        "managed Git session {session_id} emitted unexpected lifecycle \
                         {lifecycle:?}; elapsed={elapsed:?} last_drain={last_drain} \
                         drained_events={drained_events:?}"
                    ),
                },
                _ => {}
            }
        }

        let response = botster_hub::daemon_transport_request(
            &explicit_config(data_dir),
            botster_hub::DaemonRequest::ListSessions,
        )
        .unwrap_or_else(|error| {
            panic!(
                "ListSessions failed while waiting for managed Git session {session_id} to exit; \
                 elapsed={elapsed:?} ever_observed={ever_observed} \
                 last_listing={last_listing}; error={error}"
            )
        });
        assert_eq!(
            response.kind,
            botster_hub::DaemonResponseKind::Sessions,
            "unexpected response while waiting for managed Git session {session_id} to exit; \
             elapsed={elapsed:?} response={response:?}"
        );
        last_listing = format!("{:?}", response.sessions);

        if let Some(session) = response
            .sessions
            .iter()
            .find(|session| session.session_id == session_id)
        {
            ever_observed = true;
            match session.lifecycle.as_str() {
                "exited" => {
                    println!(
                        "managed_git_session_ready session_id={session_id} \
                         drain_lifecycle=exited retained_lifecycle=exited elapsed={:?} \
                         listing={last_listing} \
                         drained_events={drained_events:?}",
                        started_at.elapsed()
                    );
                    return;
                }
                lifecycle if observed_exited_lifecycle => panic!(
                    "managed Git session {session_id} emitted SessionLifecycle exited but was \
                     retained with lifecycle {lifecycle:?}; elapsed={elapsed:?} \
                     full_listing={last_listing} last_drain={last_drain} \
                     drained_events={drained_events:?}"
                ),
                "running" | "stopping" => {}
                "failed" => panic!(
                    "managed Git session {session_id} reached lifecycle failed; ListSessions maps \
                     a stale daemon registry row to failed; elapsed={elapsed:?} \
                     full_listing={last_listing} last_drain={last_drain} \
                     drained_events={drained_events:?}"
                ),
                lifecycle => panic!(
                    "managed Git session {session_id} reached unexpected lifecycle {lifecycle:?}; \
                     elapsed={elapsed:?} full_listing={last_listing} last_drain={last_drain} \
                     drained_events={drained_events:?}"
                ),
            }
        } else if ever_observed {
            panic!(
                "managed Git session {session_id} disappeared after first observation; \
                 elapsed={elapsed:?} full_listing={last_listing} last_drain={last_drain} \
                 drained_events={drained_events:?}"
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn path_str(path: &Path) -> &str {
    path.to_str().expect("test path is UTF-8")
}

pub(crate) fn run_fixture_git(root: Option<&Path>, args: &[&str]) {
    let mut command = Command::new("git");
    if let Some(root) = root {
        command.arg("-C").arg(root);
    }
    assert!(
        command
            .args(args)
            .status()
            .expect("run fixture Git")
            .success(),
        "fixture Git command failed: {args:?}"
    );
}

pub(crate) fn assert_no_state_file_under(root: &Path) {
    let direct = root.join("hub-state.json");
    let botster = root.join("botster").join("hub-state.json");
    let botster_hub = root.join("botster-hub").join("hub-state.json");
    let canonical_hub = root.join(".botster").join("hub").join("hub-state.json");

    assert!(!direct.exists(), "unexpected state file at {direct:?}");
    assert!(!botster.exists(), "unexpected state file at {botster:?}");
    assert!(
        !botster_hub.exists(),
        "unexpected state file at {botster_hub:?}"
    );
    assert!(
        !canonical_hub.exists(),
        "unexpected state file at {canonical_hub:?}"
    );
}

// ---------------------------------------------------------------------------
// Managed distribution: the production path, not scaffolding.
//
// The installer's own integration suite proves install *mechanics* with
// synthetic artifacts. What only this file can prove is the other half:
// receipt/binary agreement against a **real** Hub, and the installation lease
// enforced by real daemons launched through the installed entrypoint. Both
// halves are needed; neither alone is sufficient.
// ---------------------------------------------------------------------------

/// Build the actual Hub with an embedded build revision, plus the locked-Core
/// worker.
///
/// The Hub is built into its own target directory: embedding a revision changes
/// `build.rs` output, and sharing `target/debug` would rewrite the very
/// `CARGO_BIN_EXE_botster-hub` other tests in this file are executing.
pub(crate) fn build_real_release() -> &'static RealRelease {
    static RELEASE: OnceLock<RealRelease> = OnceLock::new();
    RELEASE.get_or_init(|| {
        ensure_session_worker_binary();
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let hub_revision = String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(manifest_dir)
                .output()
                .expect("read the Hub checkout revision")
                .stdout,
        )
        .expect("Hub revision is UTF-8")
        .trim()
        .to_string();
        assert_eq!(hub_revision.len(), 40, "git HEAD is a canonical object id");

        // The Core revision is whatever Cargo.lock pins for the worker. Reading
        // the pin keeps the coupling real instead of asserted, and keeps the two
        // provenance identities distinct.
        let lock = fs::read_to_string(manifest_dir.join("Cargo.lock")).expect("read Cargo.lock");
        let core_revision = lock
            .split("[[package]]")
            .find(|block| block.contains("name = \"botster-core\"\n"))
            .and_then(|block| block.lines().find(|line| line.starts_with("source = ")))
            .and_then(|line| line.rsplit_once('#'))
            .map(|(_, revision)| revision.trim_end_matches('"').to_string())
            .expect("Cargo.lock pins a botster-core revision");
        assert_eq!(
            core_revision.len(),
            40,
            "the locked Core revision is a canonical object id"
        );
        assert_ne!(
            hub_revision, core_revision,
            "Hub and locked-Core provenance are distinct identities"
        );

        let target = manifest_dir.join("target").join("managed-install-proof");
        let status = Command::new("cargo")
            .args(["build", "--locked", "--bin", "botster-hub"])
            .current_dir(manifest_dir)
            .env("CARGO_TARGET_DIR", &target)
            .env("BOTSTER_BUILD_REVISION", &hub_revision)
            .status()
            .expect("build the revisioned Hub binary");
        assert!(status.success(), "the revisioned Hub binary should build");

        let status = Command::new("cargo")
            .args([
                "build",
                "--locked",
                "-p",
                "botster-hub-installer",
                "--bin",
                "botster-hub-installer",
                "--bin",
                "botster-hub-release-tool",
            ])
            .current_dir(manifest_dir)
            .status()
            .expect("build the installer binaries");
        assert!(status.success(), "the installer binaries should build");

        RealRelease {
            hub_binary: target.join("debug").join("botster-hub"),
            worker_binary: Path::new(env!("CARGO_BIN_EXE_botster-hub"))
                .parent()
                .expect("hub binary directory")
                .join("botster-session-worker"),
            hub_revision,
            core_revision,
        }
    })
}

pub(crate) fn installer_binary() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_botster-hub"))
        .parent()
        .expect("hub binary directory")
        .join("botster-hub-installer")
}

pub(crate) fn start_installed_daemon(
    prefix: &Path,
    data_dir: &Path,
    entrypoint: &Path,
) -> PanicSafeCliDaemon {
    check_harness_taint();
    let mut command = Command::new(entrypoint);
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .env("HOME", prefix)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command.spawn().expect("spawn the installed Hub");
    let mut daemon = PanicSafeCliDaemon::from_child(data_dir, child, "installed daemon");
    wait_for_status(data_dir, daemon.child_mut());
    daemon
}

pub(crate) fn wait_for_entity_frame<F>(
    subscription: &mut botster_hub_client::DaemonEntitySubscription,
    deadline: Duration,
    mut predicate: F,
) -> botster_hub_client::DaemonEntityFrame
where
    F: FnMut(&botster_hub_client::DaemonEntityFrame) -> bool,
{
    let started = Instant::now();
    loop {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            let (resource, probe) = classify_budget_expiry(
                "entity_frame",
                None,
                Some("timed out waiting for entity frame"),
            );
            panic!(
                "{}",
                format_harness_budget_expired(
                    "entity_frame",
                    deadline,
                    resource,
                    probe,
                    "timed out waiting for entity frame"
                )
            );
        }
        subscription
            .set_read_timeout(Some(remaining.min(Duration::from_millis(200))))
            .expect("set timeout");
        match subscription.next_frame() {
            Ok(frame) if predicate(&frame) => return frame,
            Ok(_) => continue,
            Err(error) => {
                let message = error.to_string();
                if message.contains("timed out")
                    || message.contains("WouldBlock")
                    || message.contains("Resource temporarily unavailable")
                    || message.contains("os error 35")
                    || message.contains("os error 11")
                {
                    continue;
                }
                panic!("entity frame error: {error}");
            }
        }
    }
}
