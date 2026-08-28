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
use std::process::{Child, ChildStdout, Command, Output, Stdio};
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
use webrtc::runtime::{Receiver as AsyncReceiver, Sender as AsyncSender, channel, default_runtime};

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
        if let Some(child) = self.child.as_mut()
            && let Err(error) = try_terminate_and_reap_child(child)
        {
            record_harness_taint(format!(
                "ReapingChild drop could not prove absence: {error}"
            ));
            eprintln!("ReapingChild drop: {error}");
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
        if self.child.try_wait().ok().flatten().is_none()
            && let Err(error) = signal_test_group_or_child(pid, libc::SIGKILL)
        {
            let _ = self.child.kill();
            eprintln!("ChildCleanup group kill failed: {error}");
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
    #[cfg(target_os = "linux")]
    if let Ok(Some(workers)) = linux_proc_session_worker_identities() {
        return Ok(workers);
    }
    ps_axww_session_worker_identities()
}

#[cfg(target_os = "linux")]
fn linux_proc_session_worker_identities()
-> Result<Option<Vec<SessionWorkerProcessIdentity>>, String> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Ok(None);
    };
    let mut workers = Vec::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let bytes = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
        let mut args: Vec<String> = bytes
            .split(|byte| *byte == 0)
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect();
        if args.is_empty() {
            if let Ok(exe) = fs::read_link(format!("/proc/{pid}/exe")) {
                args.push(exe.to_string_lossy().into_owned());
            } else {
                continue;
            }
        }
        let Some(argv0) = args.first() else {
            continue;
        };
        let is_worker = Path::new(argv0)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name == "botster-session-worker" || name.starts_with("botster-session-worker")
            });
        if !is_worker {
            continue;
        }
        let command = args.join(" ");
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
    Ok(Some(workers))
}

fn ps_axww_session_worker_identities() -> Result<Vec<SessionWorkerProcessIdentity>, String> {
    let output = Command::new("ps")
        .env("COLUMNS", "65535")
        .args(["-axww", "-o", "pid=,command="])
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
    if data_dir
        .canonicalize()
        .ok()
        .is_some_and(|canon| worker.command.contains(canon.to_string_lossy().as_ref()))
    {
        return true;
    }
    linux_proc_matches_data_dir(worker.pid, data_dir)
}

fn linux_proc_matches_data_dir(pid: u32, data_dir: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(cwd) = fs::read_link(format!("/proc/{pid}/cwd"))
            && (cwd == data_dir
                || data_dir
                    .canonicalize()
                    .ok()
                    .is_some_and(|canon| cwd == canon))
        {
            return true;
        }
        if let Ok(bytes) = fs::read(format!("/proc/{pid}/environ")) {
            return bytes.split(|byte| *byte == 0).any(|chunk| {
                let Ok(entry) = std::str::from_utf8(chunk) else {
                    return false;
                };
                let Some((_, value)) = entry.split_once('=') else {
                    return false;
                };
                Path::new(value) == data_dir
                    || data_dir
                        .canonicalize()
                        .ok()
                        .zip(fs::canonicalize(value).ok())
                        .is_some_and(|(left, right)| left == right)
            });
        }
    }
    let _ = pid;
    let _ = data_dir;
    false
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
        let mut live: Vec<SessionWorkerProcessIdentity> = Vec::new();
        for identity in registry_backed_worker_identities(data_dir)? {
            let Some(command_pid) = identity.pid else {
                continue;
            };
            let Some(worker_pid) = worktree_session_worker_ancestor(command_pid) else {
                continue;
            };
            if before_pids.contains(&worker_pid) {
                continue;
            }
            if live.iter().any(|worker| worker.pid == worker_pid) {
                continue;
            }
            if !process_is_alive_u32(worker_pid)? {
                continue;
            }
            let command = process_snapshot(worker_pid)
                .map(|snapshot| snapshot.command)
                .unwrap_or_default();
            live.push(SessionWorkerProcessIdentity {
                pid: worker_pid,
                command,
                shell_descendant_pids: Vec::new(),
            });
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
                "timed out waiting for registry-backed botster-session-worker + live shell descendant under {}; argv data-dir matching is not an ownership oracle",
                data_dir.display()
            ));
        }
        thread::sleep(Duration::from_millis(30));
    }
}

/// Wait for a new worktree `botster-session-worker` for this data directory.
///
/// Do not require a PPID-visible shell descendant. A marked wrapper spawned
/// through portable-pty calls `setsid()`, so the real PTY child can leave the
/// worker PPID tree while remaining alive in another Unix session.
pub(crate) fn capture_new_session_workers_for_marked_pty(
    data_dir: &Path,
    before_pids: &std::collections::BTreeSet<u32>,
    marker: &str,
) -> Result<Vec<SessionWorkerProcessIdentity>, String> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let live_ours: Vec<SessionWorkerProcessIdentity> = session_worker_process_identities()?
            .into_iter()
            .filter(|worker| {
                !before_pids.contains(&worker.pid) && worker_executable_from_this_worktree(worker)
            })
            .collect();
        let mut live_alive = Vec::new();
        for worker in live_ours {
            if process_is_alive_u32(worker.pid)? {
                live_alive.push(worker);
            }
        }
        let owned_by_dir: Vec<SessionWorkerProcessIdentity> = live_alive
            .iter()
            .filter(|worker| worker_belongs_to_data_dir(worker, data_dir))
            .cloned()
            .collect();
        let live = if !owned_by_dir.is_empty() {
            owned_by_dir
        } else {
            live_alive
        };
        let marked = unix_processes_matching_marker(marker)?;
        if !live.is_empty() && !marked.is_empty() {
            return Ok(live);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for botster-session-worker plus all-session PTY marker; \
                 data_dir={} marker={marker} workers={live:?} marked={marked:?}",
                data_dir.display()
            ));
        }
        thread::sleep(Duration::from_millis(30));
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HubProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) command: String,
}

pub(crate) fn hub_process_identities() -> Result<Vec<HubProcessIdentity>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .map_err(|error| format!("ps hub census failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps hub census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut hubs = Vec::new();
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
        let is_hub = Path::new(argv0)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "botster-hub");
        if !is_hub {
            continue;
        }
        let rest = parts.collect::<Vec<_>>().join(" ");
        hubs.push(HubProcessIdentity {
            pid,
            command: format!("{argv0} {rest}"),
        });
    }
    Ok(hubs)
}

pub(crate) fn live_hub_processes_for_data_dir(
    data_dir: &Path,
) -> Result<Vec<HubProcessIdentity>, String> {
    let dir = data_dir.to_string_lossy();
    let canon = data_dir.canonicalize().ok();
    Ok(hub_process_identities()?
        .into_iter()
        .filter(|hub| {
            if !dir.is_empty() && hub.command.contains(dir.as_ref()) {
                return true;
            }
            canon
                .as_ref()
                .is_some_and(|path| hub.command.contains(path.to_string_lossy().as_ref()))
        })
        .collect())
}

#[derive(Debug, Clone)]
pub(crate) struct PtyChildIdentity {
    pub(crate) pid: u32,
    pub(crate) pgid: libc::pid_t,
    pub(crate) sid: libc::pid_t,
    pub(crate) command: String,
}

pub(crate) fn write_marked_sleep_wrapper(dir: &Path, marker: &str) -> PathBuf {
    write_marked_wrapper(dir, marker, "#!/bin/sh\nsleep 3600\n")
}

pub(crate) fn write_marked_echo_wrapper(dir: &Path, marker: &str) -> PathBuf {
    write_marked_wrapper(
        dir,
        marker,
        "#!/bin/sh\nprintf ready; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done\n",
    )
}

fn write_marked_wrapper(dir: &Path, marker: &str, body: &str) -> PathBuf {
    fs::create_dir_all(dir).expect("create marked wrapper directory");
    let path = dir.join(marker);
    fs::write(&path, body).expect("write marked wrapper");
    let mut permissions = fs::metadata(&path)
        .expect("read marked wrapper metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod marked wrapper");
    path
}

fn setsid_marked_command(wrapper: &Path) -> Command {
    let mut command = Command::new(wrapper);
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
}

/// Owns a `setsid` fixture and its detached process group.
///
/// Drop kills the exact child and stored PGID. It does not run a process
/// census before it sends those signals. Census helpers stay post-cleanup
/// oracles.
pub(crate) struct PanicSafeSetsidChild {
    child: Option<Child>,
    stdout: Option<ChildStdout>,
    identity: PtyChildIdentity,
}

impl PanicSafeSetsidChild {
    pub(crate) fn spawn(marker: &str, wrapper: &Path) -> Self {
        let mut child = setsid_marked_command(wrapper)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn setsid child for {marker}: {error}"));
        let stdout = child.stdout.take();
        let identity = identity_from_spawned_setsid_child(&child, marker);
        Self {
            child: Some(child),
            stdout,
            identity,
        }
    }

    pub(crate) fn identity(&self) -> &PtyChildIdentity {
        &self.identity
    }

    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.stdout.take()
    }

    pub(crate) fn reap(mut self) {
        self.terminate_owned();
    }

    fn terminate_owned(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let pid = child.id() as libc::pid_t;
        let pgid = if self.identity.pgid > 1 {
            self.identity.pgid
        } else {
            pid
        };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        if pgid > 1 {
            let _ = unsafe { libc::killpg(pgid, libc::SIGKILL) };
        }
        let _ = child.kill();
        let _ = child.wait();
        if pgid > 1 {
            let _ = unsafe { libc::killpg(pgid, libc::SIGKILL) };
        }
        self.stdout.take();
    }
}

impl Drop for PanicSafeSetsidChild {
    fn drop(&mut self) {
        self.terminate_owned();
    }
}

fn identity_from_spawned_setsid_child(child: &Child, command: &str) -> PtyChildIdentity {
    let pid = child.id();
    match pty_identity_for_pid(pid, command) {
        Ok(identity) => identity,
        Err(_) => PtyChildIdentity {
            pid,
            pgid: pid as libc::pid_t,
            sid: pid as libc::pid_t,
            command: command.to_string(),
        },
    }
}

pub(crate) fn stdout_pipe_is_closed(stdout: &impl AsRawFd) -> bool {
    let mut fds = [libc::pollfd {
        fd: stdout.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }];
    let n = unsafe { libc::poll(fds.as_mut_ptr(), 1, 500) };
    if n <= 0 {
        return false;
    }
    if fds[0].revents & libc::POLLHUP != 0 {
        return true;
    }
    if fds[0].revents & libc::POLLIN != 0 {
        let mut buf = [0u8; 1];
        let read = unsafe { libc::read(stdout.as_raw_fd(), buf.as_mut_ptr().cast(), 1) };
        return read == 0;
    }
    fds[0].revents & libc::POLLERR != 0
}

pub(crate) fn unix_processes_matching_marker(
    marker: &str,
) -> Result<Vec<PtyChildIdentity>, String> {
    if marker.is_empty() {
        return Err("PTY survivor marker must be non-empty".to_string());
    }
    let self_pid = std::process::id();
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .map_err(|error| format!("ps all-session census failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps all-session census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut matches = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        let mut parts = line.splitn(2, char::is_whitespace);
        let Some(pid_token) = parts.next() else {
            continue;
        };
        let Ok(pid) = pid_token.parse::<u32>() else {
            continue;
        };
        if pid == self_pid {
            continue;
        }
        let command = parts.next().unwrap_or("").trim();
        if !command.contains(marker) {
            continue;
        }
        matches.push(pty_identity_for_pid(pid, command)?);
    }
    Ok(matches)
}

fn pty_identity_for_pid(pid: u32, command: &str) -> Result<PtyChildIdentity, String> {
    let raw_pid = pid as libc::pid_t;
    let pgid = unsafe { libc::getpgid(raw_pid) };
    if pgid == -1 {
        return Err(format!(
            "getpgid({pid}) failed: {}",
            io::Error::last_os_error()
        ));
    }
    let sid = unsafe { libc::getsid(raw_pid) };
    if sid == -1 {
        return Err(format!(
            "getsid({pid}) failed: {}",
            io::Error::last_os_error()
        ));
    }
    Ok(PtyChildIdentity {
        pid,
        pgid,
        sid,
        command: command.to_string(),
    })
}

/// Reap captured PTY identities by exact pid and process group.
///
/// A marked wrapper that does not `exec` can leave a marker-less `sleep`
/// child in the same independently sessioned PGID. Marker census cannot see
/// that child after the wrapper is already dead.
pub(crate) fn reap_captured_pty_children(children: &[PtyChildIdentity]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let signal = if Instant::now() + Duration::from_millis(400) >= deadline {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        signal_captured_pty_children(children, signal);
        match live_captured_pty_rows(children) {
            Ok(rows) if rows.is_empty() => return Ok(()),
            Ok(rows) if Instant::now() >= deadline => {
                return Err(format!(
                    "leftover captured PTY process groups; rows={rows:?}"
                ));
            }
            Ok(_) => {}
            Err(error) if Instant::now() >= deadline => {
                return Err(format!(
                    "captured PTY reap census failed after signaling known groups: {error}"
                ));
            }
            Err(_) => {}
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn signal_captured_pty_children(children: &[PtyChildIdentity], signal: libc::c_int) {
    let mut pgids = std::collections::BTreeSet::new();
    for child in children {
        pgids.insert(child.pgid);
        let _ = unsafe { libc::kill(child.pid as libc::pid_t, signal) };
    }
    for pgid in pgids {
        if pgid > 1 {
            let _ = unsafe { libc::killpg(pgid, signal) };
        }
    }
}

pub(crate) fn live_captured_pty_rows(children: &[PtyChildIdentity]) -> Result<Vec<String>, String> {
    let mut rows = Vec::new();
    let mut seen_pgids = std::collections::BTreeSet::new();
    for child in children {
        if process_is_alive_u32(child.pid).unwrap_or(true) {
            rows.push(format!(
                "pid={} pgid={} sid={} command={}",
                child.pid, child.pgid, child.sid, child.command
            ));
        }
        if child.pgid > 1 && seen_pgids.insert(child.pgid) {
            for row in process_group_census(child.pgid)? {
                rows.push(row);
            }
        }
    }
    rows.sort();
    rows.dedup();
    Ok(rows)
}

pub(crate) fn reap_processes_matching_marker(marker: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let live = unix_processes_matching_marker(marker)?;
        if live.is_empty() {
            return Ok(());
        }
        let signal = if Instant::now() + Duration::from_millis(400) >= deadline {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        let mut pgids = std::collections::BTreeSet::new();
        for child in &live {
            pgids.insert(child.pgid);
            let _ = unsafe { libc::kill(child.pid as libc::pid_t, signal) };
        }
        for pgid in pgids {
            if pgid > 1 {
                let _ = unsafe { libc::killpg(pgid, signal) };
            }
        }
        if Instant::now() >= deadline {
            let still = unix_processes_matching_marker(marker)?;
            if still.is_empty() {
                return Ok(());
            }
            return Err(format!(
                "leftover marked Unix-session processes; marker={marker} rows={still:?}"
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub(crate) fn assert_cli_fixture_absent(
    data_dir: &Path,
    hub_pid: u32,
    socket_path: &Path,
    marker: &str,
    pty_children: &[PtyChildIdentity],
    session_ids: &[&str],
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let hub_alive = process_is_alive_u32(hub_pid).unwrap_or(true);
        let hubs = live_hub_processes_for_data_dir(data_dir).expect("hub census");
        let workers = live_session_workers_for_data_dir(data_dir).expect("worker census");
        let group_live = process_group_probe(hub_pid as libc::pid_t).unwrap_or(true);
        let group_rows = process_group_census(hub_pid as libc::pid_t).unwrap_or_default();
        let socket_live = socket_path.exists();
        let marked = unix_processes_matching_marker(marker).expect("all-session marker census");
        let live_pty_pids: Vec<u32> = pty_children
            .iter()
            .filter(|child| process_is_alive_u32(child.pid).unwrap_or(true))
            .map(|child| child.pid)
            .collect();
        let live_pty_groups: Vec<libc::pid_t> = pty_children
            .iter()
            .filter(|child| process_group_probe(child.pgid).unwrap_or(true))
            .map(|child| child.pgid)
            .collect();
        let listed_sessions = listed_running_sessions(socket_path, session_ids);
        if !hub_alive
            && hubs.is_empty()
            && workers.is_empty()
            && !group_live
            && group_rows.is_empty()
            && !socket_live
            && marked.is_empty()
            && live_pty_pids.is_empty()
            && live_pty_groups.is_empty()
            && listed_sessions.is_empty()
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "owned CLI fixture survivors remain data_dir={} hub_pid={hub_pid} \
                 hub_alive={hub_alive} hubs={hubs:?} workers={:?} group_live={group_live} \
                 group_rows={group_rows:?} socket_live={socket_live} socket={} marker={marker} \
                 marked={marked:?} live_pty_pids={live_pty_pids:?} live_pty_groups={live_pty_groups:?} \
                 listed_sessions={listed_sessions:?}",
                data_dir.display(),
                workers.iter().map(|worker| worker.pid).collect::<Vec<_>>(),
                socket_path.display()
            );
        }
        thread::sleep(Duration::from_millis(40));
    }
}

fn listed_running_sessions(socket_path: &Path, session_ids: &[&str]) -> Vec<String> {
    if !socket_path.exists() || session_ids.is_empty() {
        return Vec::new();
    }
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    match botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions) {
        Ok(response) => response
            .sessions
            .into_iter()
            .filter(|session| {
                session_ids.contains(&session.session_id.as_str()) && session.lifecycle == "running"
            })
            .map(|session| session.session_id)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Session workers that still name this data directory after the hub child exits.
pub(crate) fn live_session_workers_for_data_dir(
    data_dir: &Path,
) -> Result<Vec<SessionWorkerProcessIdentity>, String> {
    Ok(session_worker_process_identities()?
        .into_iter()
        .filter(|worker| {
            worker_executable_from_this_worktree(worker)
                && worker_belongs_to_data_dir(worker, data_dir)
        })
        .collect())
}

/// Hub process shutdown keeps durable workers. Tests that SIGKILL a victim
/// must reap leftovers for that data directory before the next IsolatedHub.
pub(crate) fn reap_session_workers_for_data_dir(data_dir: &Path) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let live = live_session_workers_for_data_dir(data_dir)?;
        if live.is_empty() {
            return Ok(());
        }
        let signal = if Instant::now() + Duration::from_millis(400) >= deadline {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        for worker in &live {
            let _ = unsafe { libc::kill(worker.pid as libc::pid_t, signal) };
            for pid in &worker.shell_descendant_pids {
                let _ = unsafe { libc::kill(*pid as libc::pid_t, signal) };
            }
        }
        if Instant::now() >= deadline {
            let still = live_session_workers_for_data_dir(data_dir)?;
            if still.is_empty() {
                return Ok(());
            }
            return Err(format!(
                "leftover session workers after daemon shutdown; data_dir={} pids={:?}",
                data_dir.display(),
                still.iter().map(|worker| worker.pid).collect::<Vec<_>>()
            ));
        }
        thread::sleep(Duration::from_millis(50));
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
    let _guard = daemon_test_guard();
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
