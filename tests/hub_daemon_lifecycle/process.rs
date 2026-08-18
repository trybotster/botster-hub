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

pub(crate) fn process_exists(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[derive(Debug)]
pub(crate) struct ProcessSnapshot {
    pub(crate) pid: u32,
    pub(crate) ppid: u32,
    pub(crate) pgid: u32,
    pub(crate) sid: String,
    pub(crate) stat: String,
    pub(crate) command: String,
}

pub(crate) fn process_snapshot(pid: u32) -> Option<ProcessSnapshot> {
    let output = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("pid=")
        .arg("-o")
        .arg("ppid=")
        .arg("-o")
        .arg("pgid=")
        .arg("-o")
        .arg("sess=")
        .arg("-o")
        .arg("stat=")
        .arg("-o")
        .arg("command=")
        .output()
        .expect("inspect process snapshot");
    if !output.status.success() {
        return None;
    }
    let row = String::from_utf8_lossy(&output.stdout);
    let mut fields = row.split_whitespace();
    Some(ProcessSnapshot {
        pid: fields.next()?.parse().ok()?,
        ppid: fields.next()?.parse().ok()?,
        pgid: fields.next()?.parse().ok()?,
        sid: fields.next()?.to_string(),
        stat: fields.next()?.to_string(),
        command: fields.collect::<Vec<_>>().join(" "),
    })
}

pub(crate) fn wait_for_process_snapshot(
    pid: u32,
    stage: &str,
    predicate: impl Fn(&ProcessSnapshot) -> bool,
) -> ProcessSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_snapshot = None;
    while Instant::now() < deadline {
        if let Some(snapshot) = process_snapshot(pid) {
            if predicate(&snapshot) {
                return snapshot;
            }
            last_snapshot = Some(snapshot);
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("process {pid} did not reach {stage} within 5s; last_snapshot={last_snapshot:?}");
}

pub(crate) struct ReapingChild {
    pub(crate) child: Option<Child>,
}

impl ReapingChild {
    pub(crate) fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("owned child")
    }

    pub(crate) fn wait_with_output(mut self) -> Output {
        self.child
            .take()
            .expect("owned child")
            .wait_with_output()
            .expect("wait for owned child output")
    }
}

impl Drop for ReapingChild {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            if let Err(error) = try_terminate_and_reap_child(child) {
                record_harness_taint(format!(
                    "ReapingChild drop could not prove absence: {error}"
                ));
                eprintln!("ReapingChild drop: {error}");
            }
        }
    }
}

pub(crate) struct ChildCleanup {
    pub(crate) child: Child,
}

impl ChildCleanup {
    pub(crate) fn spawn_non_botster_decoy() -> Self {
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn non-botster decoy process");
        Self { child }
    }

    pub(crate) fn id(&self) -> u32 {
        self.child.id()
    }

    pub(crate) fn assert_alive(&mut self) {
        assert!(
            self.child
                .try_wait()
                .expect("poll non-botster decoy")
                .is_none(),
            "non-botster decoy process should remain alive"
        );
        assert!(
            process_exists(self.id()),
            "non-botster decoy pid should still exist"
        );
    }
}

impl Drop for ChildCleanup {
    fn drop(&mut self) {
        let pid = self.child.id();
        if self.child.try_wait().ok().flatten().is_none() {
            if let Err(error) = signal_test_group_or_child(pid, libc::SIGKILL) {
                let _ = self.child.kill();
                eprintln!("ChildCleanup group kill failed: {error}");
            }
        }
        if let Err(error) = self.child.wait() {
            record_harness_taint(format!("ChildCleanup wait failed for pid {pid}: {error}"));
        }
    }
}

pub(crate) fn wait_for_process_exit(pid: u32) {
    for _ in 0..100 {
        if !process_exists(pid) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("process {pid} still exists");
}

pub(crate) fn write_local_process_plugin_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create process package root");
    fs::write(root.join("bin").join("plugin"), "#!/bin/sh\n").expect("write process entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.process-plugin",
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

pub(crate) fn process_probe(pid: libc::pid_t) -> Result<bool, String> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else if error.raw_os_error() == Some(libc::EPERM) {
        Ok(true)
    } else {
        Err(format!("kill({pid}, 0) failed: {error}"))
    }
}

/// Fail closed: census/kill-probe errors must not become "absent".
pub(crate) fn process_is_alive_u32(pid: u32) -> Result<bool, String> {
    process_probe(pid as libc::pid_t)
}

pub(crate) fn process_must_be_alive(pid: u32, label: &str) -> Result<(), String> {
    match process_is_alive_u32(pid)? {
        true => Ok(()),
        false => Err(format!("{label} pid {pid} is not live")),
    }
}

pub(crate) fn process_must_be_absent(pid: u32, label: &str) -> Result<(), String> {
    match process_is_alive_u32(pid)? {
        false => Ok(()),
        true => Err(format!("{label} pid {pid} is still live")),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionWorkerProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) command: String,
    /// Exact non-root shell/descendant PIDs that must be observed before cleanup.
    pub(crate) shell_descendant_pids: Vec<u32>,
}

/// Portable census of true `botster-session-worker` binaries (not hub argv mentions).
pub(crate) fn session_worker_process_identities()
-> Result<Vec<SessionWorkerProcessIdentity>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .map_err(|error| format!("ps worker census failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps worker census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut workers = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        let mut parts = line.split_whitespace();
        let Some(pid_token) = parts.next() else {
            continue;
        };
        let Ok(pid) = pid_token.parse::<u32>() else {
            continue;
        };
        let Some(argv0) = parts.next() else {
            continue;
        };
        let is_worker = Path::new(argv0)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "botster-session-worker");
        if !is_worker {
            continue;
        }
        let rest = parts.collect::<Vec<_>>().join(" ");
        let command = format!("{argv0} {rest}");
        let descendant_pids = worker_owned_descendant_pids(pid)?;
        let shell_descendant_pids: Vec<u32> = descendant_pids
            .iter()
            .copied()
            .filter(|candidate| *candidate != pid)
            .collect();
        workers.push(SessionWorkerProcessIdentity {
            pid,
            command,
            shell_descendant_pids,
        });
    }
    Ok(workers)
}

pub(crate) fn worker_owned_descendant_pids(root_pid: u32) -> Result<Vec<u32>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid="])
        .output()
        .map_err(|error| format!("ps parent census failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps parent census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut parts = line.split_whitespace();
        let Some(pid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        edges.push((pid, ppid));
    }
    let mut owned = vec![root_pid];
    let mut changed = true;
    while changed {
        changed = false;
        for &(pid, ppid) in &edges {
            if !owned.contains(&ppid) || owned.contains(&pid) {
                continue;
            }
            if process_is_alive_u32(pid)? {
                owned.push(pid);
                changed = true;
            }
        }
    }
    Ok(owned)
}

pub(crate) fn worker_belongs_to_data_dir(
    worker: &SessionWorkerProcessIdentity,
    data_dir: &Path,
) -> bool {
    let dir = data_dir.to_string_lossy();
    if dir.is_empty() {
        return false;
    }
    if worker.command.contains(dir.as_ref()) {
        return true;
    }
    data_dir
        .canonicalize()
        .ok()
        .is_some_and(|canon| worker.command.contains(canon.to_string_lossy().as_ref()))
}

pub(crate) fn worker_executable_from_this_worktree(worker: &SessionWorkerProcessIdentity) -> bool {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let argv0 = worker.command.split_whitespace().next().unwrap_or("");
    if argv0.is_empty() {
        return false;
    }
    let exe = Path::new(argv0);
    if exe.starts_with(root) {
        return true;
    }
    match (exe.canonicalize(), root.canonicalize()) {
        (Ok(exe), Ok(root)) => exe.starts_with(root),
        _ => worker.command.contains(&root.display().to_string()),
    }
}

pub(crate) fn capture_new_session_workers_for_data_dir(
    data_dir: &Path,
    before_pids: &std::collections::BTreeSet<u32>,
) -> Result<Vec<SessionWorkerProcessIdentity>, String> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        // Never adopt host-global "any new pid" — require this worktree's worker
        // binary, then prefer data-dir attribution when present.
        let live_ours: Vec<SessionWorkerProcessIdentity> = session_worker_process_identities()?
            .into_iter()
            .filter(|worker| {
                !before_pids.contains(&worker.pid) && worker_belongs_to_data_dir(worker, data_dir)
            })
            .collect();
        // Fail closed on kill-probe errors for candidate workers.
        let mut live = Vec::new();
        for worker in live_ours {
            if process_is_alive_u32(worker.pid)? {
                live.push(worker);
            }
        }
        if !live.is_empty() {
            // Refresh descendant trees after a short settle so the shell child is included.
            thread::sleep(Duration::from_millis(80));
            let mut refreshed = Vec::new();
            for worker in live {
                if !process_is_alive_u32(worker.pid)? {
                    continue;
                }
                let descendant_pids = worker_owned_descendant_pids(worker.pid)?;
                let shell_descendant_pids: Vec<u32> = descendant_pids
                    .iter()
                    .copied()
                    .filter(|candidate| *candidate != worker.pid)
                    .collect();
                // Require at least one live non-root descendant (sh -c shell).
                let mut live_shells = Vec::new();
                for pid in shell_descendant_pids {
                    if process_is_alive_u32(pid)? {
                        live_shells.push(pid);
                    }
                }
                if live_shells.is_empty() {
                    // Keep polling until shell child appears; fixture is sh -c.
                    continue;
                }
                refreshed.push(SessionWorkerProcessIdentity {
                    pid: worker.pid,
                    command: worker.command,
                    shell_descendant_pids: live_shells,
                });
            }
            if !refreshed.is_empty() {
                return Ok(refreshed);
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for botster-session-worker + live shell descendant owned by data dir {}; worktree-wide adoption is disabled",
                data_dir.display()
            ));
        }
        thread::sleep(Duration::from_millis(30));
    }
}

pub(crate) fn process_group_probe(pgid: libc::pid_t) -> Result<bool, String> {
    if unsafe { libc::killpg(pgid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else if error.raw_os_error() == Some(libc::EPERM) {
        Ok(true)
    } else {
        Err(format!("killpg({pgid}, 0) failed: {error}"))
    }
}

pub(crate) fn process_group_census(pgid: libc::pid_t) -> Result<Vec<String>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,pgid=,stat=,command="])
        .output()
        .map_err(|error| format!("run portable process census: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "portable process census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let rows = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .nth(2)
                .and_then(|value| value.parse::<libc::pid_t>().ok())
                == Some(pgid)
        })
        .map(str::trim)
        .map(str::to_string)
        .collect();
    Ok(rows)
}

#[cfg(target_os = "linux")]
pub(crate) fn linux_process_cpu_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_command = stat.rsplit_once(") ")?.1;
    let fields = after_command.split_whitespace().collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

pub(crate) fn start_cli_daemon_with_session_worker(
    data_dir: &Path,
    session_worker_bin: &Path,
) -> PanicSafeCliDaemon {
    check_harness_taint();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command.spawn().expect("spawn botster-hub start");
    let mut daemon = PanicSafeCliDaemon::from_child(data_dir, child, "session-worker daemon");
    wait_for_status(data_dir, daemon.child_mut());
    daemon
}

pub(crate) fn configure_test_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

pub(crate) fn session_worker_binary_path() -> PathBuf {
    ensure_session_worker_binary();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("botster-session-worker")
}

pub(crate) fn process_thread_count(pid: u32) -> Option<usize> {
    for field in ["thcount=", "nlwp="] {
        let output = Command::new("ps")
            .args(["-o", field, "-p", &pid.to_string()])
            .output()
            .ok()?;
        if output.status.success()
            && let Ok(count) = String::from_utf8_lossy(&output.stdout).trim().parse()
        {
            return Some(count);
        }
    }
    #[cfg(target_os = "linux")]
    if let Ok(entries) = fs::read_dir(format!("/proc/{pid}/task")) {
        return Some(entries.filter_map(Result::ok).count());
    }
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("ps")
            .args(["-M", "-p", &pid.to_string(), "-o", "pid="])
            .output()
            .ok()?;
        if output.status.success() {
            return Some(
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .skip(1)
                    .count(),
            );
        }
    }
    None
}
