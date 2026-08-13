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

pub(crate) const OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP: Duration = Duration::from_secs(60);
pub(crate) const OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP: Duration = Duration::from_secs(2);
pub(crate) const OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP: Duration = LOCAL_RUNTIME_DAEMON_READINESS_BUDGET;
pub(crate) const DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT: &str = "trap '' INT; node -e 'process.on(\"SIGINT\", () => process.exit(130)); console.log(\"foreground-forward-ready\"); setInterval(() => {}, 1000)' & child=$!; wait \"$child\"";
pub(crate) struct OperatorConsolePty {
    pub(crate) child: Box<dyn portable_pty::Child + Send + Sync>,
    pub(crate) master: Box<dyn MasterPty + Send>,
    pub(crate) writer: Option<Box<dyn Write + Send>>,
    pub(crate) output: Arc<Mutex<Vec<u8>>>,
    pub(crate) reader: Option<thread::JoinHandle<()>>,
    pub(crate) reader_done: Arc<AtomicBool>,
}

pub(crate) trait TestChildControl {
    fn try_wait_status(&mut self) -> io::Result<Option<String>>;
    fn terminate_and_reap(&mut self) -> String;
    fn captured_output(&mut self) -> String;
}

pub(crate) struct OwnedOperatorConsoleDaemon {
    pub(crate) data_dir: PathBuf,
    pub(crate) owned_pids: Vec<u32>,
    pub(crate) armed: bool,
}

impl OwnedOperatorConsoleDaemon {
    pub(crate) fn new(data_dir: &Path) -> Self {
        let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("probe operator console daemon before spawn");
        assert!(
            !status.status.success(),
            "operator console test data directory already has a running daemon: {}",
            command_output_text(&status)
        );
        Self {
            data_dir: data_dir.to_path_buf(),
            owned_pids: Vec::new(),
            armed: true,
        }
    }

    pub(crate) fn wait_until_daemon_ready(&mut self, console: &mut OperatorConsolePty) {
        self.try_wait_until_daemon_ready(console)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub(crate) fn try_wait_until_daemon_ready(
        &mut self,
        console: &mut OperatorConsolePty,
    ) -> Result<(), String> {
        self.try_wait_until_daemon_ready_with_backstop(
            console,
            OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP,
        )
    }

    pub(crate) fn try_wait_until_daemon_ready_with_backstop(
        &mut self,
        console: &mut OperatorConsolePty,
        liveness_backstop: Duration,
    ) -> Result<(), String> {
        let config = explicit_config(&self.data_dir);
        let output = Arc::clone(&console.output);
        let mut last_status = "status probe not attempted".to_string();
        let result = wait_for_child_condition_with_budget(
            &mut console.child,
            "waiting for typed operator-console daemon readiness",
            liveness_backstop,
            || {
                self.capture_owned_pid();
                match botster_hub::daemon_transport_request(
                    &config,
                    botster_hub::DaemonRequest::Status,
                ) {
                    Ok(_) => true,
                    Err(error) => {
                        last_status = error.to_string();
                        false
                    }
                }
            },
        );
        self.capture_owned_pid();
        if let Err(error) = result {
            let reader_status = console
                .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
                .map_or_else(|error| error, |()| "reader reached EOF".to_string());
            self.capture_owned_pid();
            let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
            let socket_path = config
                .transports
                .local_socket
                .as_ref()
                .expect("operator console local socket binding")
                .path
                .clone();
            return Err(format!(
                "{error}; last_status={last_status:?}; owned_daemon_pids={:?}; metadata_exists={}; socket_exists={}; reader_status={reader_status:?}; operator_console_output={}",
                self.owned_pids,
                metadata_path.exists(),
                socket_path.exists(),
                String::from_utf8_lossy(
                    &output
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                )
            ));
        }
        Ok(())
    }

    pub(crate) fn capture_owned_pid(&mut self) {
        let Some(pid) = self.validated_metadata_pid() else {
            return;
        };
        if !self.owned_pids.contains(&pid) {
            self.owned_pids.push(pid);
        }
    }

    pub(crate) fn validated_metadata_pid(&self) -> Option<u32> {
        let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
        let bytes = fs::read(metadata_path).ok()?;
        let metadata = serde_json::from_slice::<serde_json::Value>(&bytes).ok()?;
        let pid = metadata["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())?;
        let expected_socket = explicit_config(&self.data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .display()
            .to_string();
        let metadata_matches = metadata["data_directory"].as_str()
            == Some(stable_path_string(&self.data_dir).as_str())
            && metadata["socket_path"].as_str() == Some(expected_socket.as_str())
            && metadata["hub_bin"]
                .as_str()
                .is_some_and(|path| Path::new(path).file_name() == Some("botster-hub".as_ref()));
        metadata_matches.then_some(pid)
    }

    pub(crate) fn assert_cleaned(&mut self) {
        self.cleanup()
            .unwrap_or_else(|error| panic!("operator console daemon cleanup failed: {error}"));
        self.armed = false;
    }

    pub(crate) fn owned_pids(&self) -> &[u32] {
        &self.owned_pids
    }

    pub(crate) fn record_owned_pid(&mut self, pid: u32) {
        if !self.owned_pids.contains(&pid) {
            self.owned_pids.push(pid);
        }
    }

    pub(crate) fn cleanup(&mut self) -> Result<(), String> {
        self.capture_owned_pid();
        let _ = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("shutdown")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .output();

        for pid in self.owned_pids.iter().copied() {
            wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            if process_exists(pid) && self.process_identity_matches(pid) {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
                wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            }
            if process_exists(pid) && self.process_identity_matches(pid) {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGKILL);
                }
                wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            }
        }

        let alive = self
            .owned_pids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid) && self.process_identity_matches(*pid))
            .collect::<Vec<_>>();
        if !alive.is_empty() {
            return Err(format!("owned daemon pids still alive: {alive:?}"));
        }

        let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
        let socket_path = explicit_config(&self.data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .clone();
        let socket_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < socket_deadline && socket_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }
        let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .output()
            .map_err(|error| format!("probe stopped daemon: {error}"))?;
        if status.status.success() {
            return Err(format!(
                "typed daemon status still reports running: {}",
                command_output_text(&status)
            ));
        }
        if socket_path.exists() {
            return Err(format!(
                "owned daemon socket remains after stopped status: {}",
                socket_path.display()
            ));
        }
        if metadata_path.exists() {
            let Some(metadata_pid) = self.validated_metadata_pid() else {
                return Err(format!(
                    "unverified runtime metadata remains: {}",
                    metadata_path.display()
                ));
            };
            if !self.owned_pids.contains(&metadata_pid) {
                return Err(format!(
                    "runtime metadata pid {metadata_pid} was not captured as owned"
                ));
            }
            fs::remove_file(&metadata_path)
                .map_err(|error| format!("remove verified owned runtime metadata: {error}"))?;
        }
        Ok(())
    }

    pub(crate) fn process_identity_matches(&self, pid: u32) -> bool {
        let output = Command::new("ps")
            .arg("-p")
            .arg(pid.to_string())
            .arg("-o")
            .arg("command=")
            .output();
        let Ok(output) = output else {
            return false;
        };
        let command = String::from_utf8_lossy(&output.stdout);
        command.contains("botster-hub")
            && command.contains(" start ")
            && command.contains("--data-dir")
            && (command.contains(&self.data_dir.display().to_string())
                || command.contains(&stable_path_string(&self.data_dir)))
    }
}

impl Drop for OwnedOperatorConsoleDaemon {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

impl OperatorConsolePty {
    pub(crate) fn spawn(data_dir: &Path) -> Self {
        Self::spawn_binary(Path::new(env!("CARGO_BIN_EXE_botster-hub")), data_dir)
    }

    pub(crate) fn spawn_binary(binary: &Path, data_dir: &Path) -> Self {
        Self::spawn_binary_with_env(binary, data_dir, &[])
    }

    pub(crate) fn spawn_with_env(data_dir: &Path, environment: &[(&str, &str)]) -> Self {
        Self::spawn_binary_with_env(
            Path::new(env!("CARGO_BIN_EXE_botster-hub")),
            data_dir,
            environment,
        )
    }

    pub(crate) fn spawn_binary_with_env(binary: &Path, data_dir: &Path, environment: &[(&str, &str)]) -> Self {
        let pty = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open operator console PTY");
        let mut command = CommandBuilder::new(binary);
        command.env("BOTSTER_HUB_DATA_DIR", data_dir);
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = pty
            .slave
            .spawn_command(command)
            .expect("spawn operator console");
        let mut reader = pty
            .master
            .try_clone_reader()
            .expect("clone operator console PTY reader");
        let writer = pty
            .master
            .take_writer()
            .expect("take operator console PTY writer");
        drop(pty.slave);
        let output = Arc::new(Mutex::new(Vec::new()));
        let reader_output = Arc::clone(&output);
        let reader_done = Arc::new(AtomicBool::new(false));
        let reader_done_signal = Arc::clone(&reader_done);
        let reader = thread::spawn(move || {
            let mut buffer = [0_u8; 1024];
            while let Ok(count) = reader.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                reader_output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend_from_slice(&buffer[..count]);
            }
            reader_done_signal.store(true, Ordering::Release);
        });
        Self {
            child,
            master: pty.master,
            writer: Some(writer),
            output,
            reader: Some(reader),
            reader_done,
        }
    }

    pub(crate) fn send(&mut self, bytes: &[u8]) {
        self.writer
            .as_mut()
            .expect("operator console writer is open")
            .write_all(bytes)
            .expect("write operator console input");
        self.writer
            .as_mut()
            .expect("operator console writer is open")
            .flush()
            .expect("flush operator console input");
    }

    pub(crate) fn send_and_wait_for_prompt(&mut self, bytes: &[u8]) {
        let expected = self.prompt_count() + 1;
        self.send(bytes);
        self.wait_for_occurrences("botster-hub> ", expected);
    }

    pub(crate) fn prompt_count(&self) -> usize {
        self.text().matches("botster-hub> ").count()
    }

    pub(crate) fn wait_for(&mut self, needle: &str) {
        self.wait_for_occurrences(needle, 1);
    }

    pub(crate) fn wait_for_occurrences(&mut self, needle: &str, expected: usize) {
        self.try_wait_for_occurrences(needle, expected)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub(crate) fn output_checkpoint(&self) -> usize {
        self.output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub(crate) fn wait_for_output_after(&mut self, checkpoint: usize, needle: &str) {
        self.try_wait_for_output_after(
            checkpoint,
            needle,
            OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP,
        )
        .unwrap_or_else(|error| panic!("{error}"));
    }

    pub(crate) fn try_wait_for_output_after(
        &mut self,
        checkpoint: usize,
        needle: &str,
        budget: Duration,
    ) -> Result<(), String> {
        let started_at = Instant::now();
        let deadline = started_at + budget;
        while Instant::now() < deadline {
            if self.output_contains_after(checkpoint, needle.as_bytes()) {
                return Ok(());
            }
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)?;
                    if self.output_contains_after(checkpoint, needle.as_bytes()) {
                        return Ok(());
                    }
                    return Err(format!(
                        "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: console exited after {:?}; console_status={status:?}; {}",
                        started_at.elapsed(),
                        self.output_progress_context(checkpoint)
                    ));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(format!(
                        "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: failed to poll console after {:?}: {error}; {}",
                        started_at.elapsed(),
                        self.output_progress_context(checkpoint)
                    ));
                }
            }
            thread::sleep(Duration::from_millis(20));
        }

        let diagnostics = self.foreground_diagnostics();
        self.writer.take();
        let console_status = terminate_and_reap_pty_child(self.child.as_mut());
        let reader_status = self
            .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
            .err()
            .unwrap_or_else(|| "reader reached EOF".to_string());
        Err(format!(
            "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: no post-action progress after {:?} (backstop {budget:?}); console_status={console_status}; reader_status={reader_status:?}; {diagnostics}; {}",
            started_at.elapsed(),
            self.output_progress_context(checkpoint)
        ))
    }

    pub(crate) fn output_contains_after(&self, checkpoint: usize, needle: &[u8]) -> bool {
        let output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        output
            .get(checkpoint..)
            .is_some_and(|suffix| suffix.windows(needle.len()).any(|window| window == needle))
    }

    pub(crate) fn output_progress_context(&self, checkpoint: usize) -> String {
        let output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let suffix = output.get(checkpoint..).unwrap_or_default();
        format!(
            "post_checkpoint_output={:?}; operator_console_output={:?}",
            String::from_utf8_lossy(suffix),
            String::from_utf8_lossy(&output)
        )
    }

    pub(crate) fn foreground_diagnostics(&mut self) -> String {
        let console_status = match self.child.try_wait() {
            Ok(Some(status)) => format!("{status:?}"),
            Ok(None) => "running".to_string(),
            Err(error) => format!("inspection_error={error}"),
        };
        let raw_fd = self.master.as_raw_fd();
        let foreground_pgid = self.master.process_group_leader();
        let termios = raw_fd.map(|fd| {
            let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
            let result = unsafe { libc::tcgetattr(fd, attributes.as_mut_ptr()) };
            if result == 0 {
                let attributes = unsafe { attributes.assume_init() };
                format!(
                    "isig={}",
                    attributes.c_lflag & libc::ISIG as libc::tcflag_t != 0
                )
            } else {
                format!("inspection_error={}", io::Error::last_os_error())
            }
        });
        let group_probe = foreground_pgid.map(process_group_probe);
        let leader_probe = foreground_pgid.map(process_probe);
        let census = foreground_pgid.map(process_group_census);
        let inspection_consistency = match (&group_probe, &census) {
            (Some(Ok(true)), Some(Ok(rows))) if rows.is_empty() => {
                "inspection_error=live killpg probe disagrees with empty ps census".to_string()
            }
            (Some(Ok(false)), Some(Ok(rows))) if !rows.is_empty() => {
                "inspection_error=dead killpg probe disagrees with nonempty ps census".to_string()
            }
            (Some(Err(error)), _) => format!("inspection_error={error}"),
            (_, Some(Err(error))) => format!("inspection_error={error}"),
            _ => "inspection_consistent=true".to_string(),
        };
        format!(
            "foreground_diagnostics console_status={console_status}; raw_fd={raw_fd:?}; termios={termios:?}; foreground_pgid={foreground_pgid:?}; killpg_probe={group_probe:?}; leader_pid_probe={leader_probe:?}; group_census={census:?}; {inspection_consistency}"
        )
    }

    pub(crate) fn try_wait_for_occurrences(&mut self, needle: &str, expected: usize) -> Result<(), String> {
        let output = Arc::clone(&self.output);
        let result = wait_for_child_condition_with_budget(
            &mut self.child,
            &format!("waiting for {expected} occurrences of operator console output {needle:?}"),
            LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
            || {
                String::from_utf8_lossy(
                    &output
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                )
                .matches(needle)
                .count()
                    >= expected
            },
        );
        if let Err(error) = result {
            if error.contains("child exited before condition") {
                self.finish_reader_after_exit(LOCAL_RUNTIME_DAEMON_READINESS_BUDGET)?;
                if self.text().matches(needle).count() >= expected {
                    return Ok(());
                }
            }
            return Err(format!("{error}; operator_console_output={}", self.text()));
        }
        Ok(())
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(
            &self
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_owned()
    }

    pub(crate) fn wait_for_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self
                .child
                .try_wait()
                .expect("poll operator console")
                .is_some()
            {
                self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
                    .unwrap_or_else(|error| panic!("{error}"));
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        self.writer.take();
        let child_status = terminate_and_reap_pty_child(self.child.as_mut());
        let reader_status = self
            .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
            .err()
            .unwrap_or_else(|| "reader reached EOF".to_string());
        panic!(
            "operator console did not exit; child_status={child_status}; reader_status={reader_status}; output={}",
            self.text()
        );
    }

    pub(crate) fn finish_reader_after_exit(&mut self, budget: Duration) -> Result<(), String> {
        self.writer.take();
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline && !self.reader_done.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(20));
        }
        if !self.reader_done.load(Ordering::Acquire) {
            return Err(format!(
                "operator console PTY reader did not reach EOF within {budget:?}; output={}",
                self.text()
            ));
        }
        if let Some(reader) = self.reader.take() {
            reader
                .join()
                .map_err(|_| "operator console PTY reader panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for OperatorConsolePty {
    fn drop(&mut self) {
        self.writer.take();
        let _ = terminate_and_reap_pty_child(self.child.as_mut());
        let _ = self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP);
    }
}

