#![allow(dead_code, unused_imports)]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use botster_core_daemon::{RegistryRecord, RegistrySessionState, SessionRegistry};

use super::*;

static HARNESS_TAINT: OnceLock<Mutex<Option<String>>> = OnceLock::new();
const FD_LIMIT_PROBE_MARGIN: u64 = 8;
const HUB_STOP_TERM_GRACE: Duration = Duration::from_millis(500);
const HUB_STOP_KILL_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GuardCleanupMode {
    Full,
    TransferSessions,
    Disarmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupTrigger {
    Explicit,
    Drop,
    Panic,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShutdownSessionClass {
    Found,
    Absent,
    Err(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionShutdownRecord {
    pub(crate) session_id: String,
    pub(crate) classification: ShutdownSessionClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegistryWorkerIdentity {
    pub(crate) session_id: String,
    pub(crate) pid: Option<u32>,
    pub(crate) runtime_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostResourceClass {
    None,
    Emfile,
    Enfile,
    PtyAllocation,
    AmbiguousSocket,
    AmbiguousReadiness,
}

impl HostResourceClass {
    pub(crate) fn marker_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Emfile => "EMFILE",
            Self::Enfile => "ENFILE",
            Self::PtyAllocation => "PTY",
            Self::AmbiguousSocket => "EAGAIN",
            Self::AmbiguousReadiness => "ETIMEDOUT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceProbe {
    Confirmed,
    Unconfirmed,
    NotApplicable,
}

impl ResourceProbe {
    pub(crate) fn marker_name(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Unconfirmed => "unconfirmed",
            Self::NotApplicable => "n/a",
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct GuardTestHooks {
    pub(crate) force_absence_unproven: bool,
}

pub(crate) fn harness_taint_cell() -> &'static Mutex<Option<String>> {
    HARNESS_TAINT.get_or_init(|| Mutex::new(None))
}

pub(crate) fn harness_taint() -> Option<String> {
    harness_taint_cell()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
}

pub(crate) fn record_harness_taint(evidence: impl Into<String>) {
    let mut slot = harness_taint_cell()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if slot.is_none() {
        *slot = Some(evidence.into());
    }
}

pub(crate) fn check_harness_taint() {
    if let Some(evidence) = harness_taint() {
        panic!("environment_tainted: {evidence}");
    }
}

pub(crate) fn reset_harness_taint_after_proof() {
    *harness_taint_cell()
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = None;
}

pub(crate) fn format_harness_budget_expired(
    kind: &str,
    budget: Duration,
    resource: HostResourceClass,
    probe: ResourceProbe,
    evidence: &str,
) -> String {
    format!(
        "harness_budget_expired kind={kind} budget_ms={} resource={} probe={} {evidence}",
        budget.as_millis(),
        resource.marker_name(),
        probe.marker_name()
    )
}

pub(crate) fn classify_os_resource(error: &io::Error) -> HostResourceClass {
    match error.raw_os_error() {
        Some(code) if code == libc::EMFILE => HostResourceClass::Emfile,
        Some(code) if code == libc::ENFILE => HostResourceClass::Enfile,
        Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK => {
            HostResourceClass::AmbiguousSocket
        }
        Some(code) if code == libc::ETIMEDOUT => HostResourceClass::AmbiguousReadiness,
        _ => HostResourceClass::None,
    }
}

/// PTY-allocation errnos frozen from Core `TerminalBackendConstruction` source
/// wrapping `posix_openpt`/`openpty` failures (EMFILE, ENFILE, and
/// EAGAIN-from-the-PTY-allocation-call). Socket EAGAIN is not in this set.
pub(crate) fn pty_allocation_errnos() -> &'static [i32] {
    &[libc::EMFILE, libc::ENFILE, libc::EAGAIN]
}

pub(crate) fn classify_pty_allocation_source(source: &str) -> HostResourceClass {
    let lowered = source.to_ascii_lowercase();
    if lowered.contains("emfile") || lowered.contains("too many open files") {
        return HostResourceClass::Emfile;
    }
    if lowered.contains("enfile") {
        return HostResourceClass::Enfile;
    }
    if pty_allocation_errnos().contains(&libc::EAGAIN)
        && (lowered.contains("eagain")
            || lowered.contains("resource temporarily unavailable")
            || lowered.contains("posix_openpt")
            || lowered.contains("openpty"))
    {
        return HostResourceClass::PtyAllocation;
    }
    HostResourceClass::None
}

pub(crate) fn probe_fd_limit() -> ResourceProbe {
    let Some(open) = open_fd_count() else {
        return ResourceProbe::Unconfirmed;
    };
    let Some(limit) = nofile_limit() else {
        return ResourceProbe::Unconfirmed;
    };
    if u64::try_from(open).unwrap_or(u64::MAX) + FD_LIMIT_PROBE_MARGIN >= limit {
        ResourceProbe::Confirmed
    } else {
        ResourceProbe::Unconfirmed
    }
}

pub(crate) fn probe_pty_allocation() -> ResourceProbe {
    let result = Command::new("python3")
        .args([
            "-c",
            "import os, sys\ntry:\n    fd = os.openpt(os.O_RDWR | os.O_NOCTTY)\n    os.close(fd)\n    sys.exit(0)\nexcept OSError as error:\n    sys.exit(error.errno)",
        ])
        .status();
    match result {
        Ok(status) if status.success() => ResourceProbe::Unconfirmed,
        Ok(status) => {
            let code = status.code().unwrap_or_default();
            if pty_allocation_errnos().contains(&code) {
                ResourceProbe::Confirmed
            } else {
                ResourceProbe::Unconfirmed
            }
        }
        Err(_) => ResourceProbe::Unconfirmed,
    }
}

pub(crate) fn classify_budget_expiry(
    kind: &str,
    error: Option<&io::Error>,
    source_text: Option<&str>,
) -> (HostResourceClass, ResourceProbe) {
    if let Some(error) = error {
        let class = classify_os_resource(error);
        return upgrade_ambiguous(kind, class);
    }
    if let Some(source) = source_text {
        let class = classify_pty_allocation_source(source);
        if !matches!(class, HostResourceClass::None) {
            return (class, ResourceProbe::NotApplicable);
        }
        let lowered = source.to_ascii_lowercase();
        if lowered.contains("wouldblock")
            || lowered.contains("os error 35")
            || lowered.contains("os error 11")
            || lowered.contains("resource temporarily unavailable")
        {
            return upgrade_ambiguous(kind, HostResourceClass::AmbiguousSocket);
        }
        if lowered.contains("timed out") || lowered.contains("etimedout") {
            return upgrade_ambiguous(kind, HostResourceClass::AmbiguousReadiness);
        }
    }
    (HostResourceClass::None, ResourceProbe::NotApplicable)
}

fn upgrade_ambiguous(kind: &str, class: HostResourceClass) -> (HostResourceClass, ResourceProbe) {
    match class {
        HostResourceClass::Emfile | HostResourceClass::Enfile | HostResourceClass::PtyAllocation => {
            (class, ResourceProbe::NotApplicable)
        }
        HostResourceClass::AmbiguousSocket if kind.contains("pty") => {
            (class, probe_pty_allocation())
        }
        HostResourceClass::AmbiguousSocket => (class, probe_fd_limit()),
        HostResourceClass::AmbiguousReadiness => (class, probe_fd_limit()),
        HostResourceClass::None => (class, ResourceProbe::NotApplicable),
    }
}

fn open_fd_count() -> Option<usize> {
    fs::read_dir("/dev/fd").ok().map(|entries| entries.count())
}

fn nofile_limit() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
        Some(limit.rlim_cur)
    } else {
        None
    }
}

pub(crate) fn try_terminate_and_reap_child(child: &mut Child) -> Result<String, String> {
    match child.try_wait() {
        Ok(Some(status)) => return Ok(status.to_string()),
        Ok(None) => {}
        Err(error) => return Err(format!("poll daemon child before cleanup: {error}")),
    }
    let pid = child.id();
    signal_test_group_or_child(pid, libc::SIGTERM)
        .map_err(|error| format!("signal daemon group after readiness failure: {error}"))?;
    let deadline = Instant::now() + HUB_STOP_TERM_GRACE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status.to_string()),
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => return Err(format!("poll daemon child during cleanup: {error}")),
        }
    }
    signal_test_group_or_child(pid, libc::SIGKILL)
        .map_err(|error| format!("kill daemon group after readiness failure: {error}"))?;
    let deadline = Instant::now() + HUB_STOP_KILL_GRACE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status.to_string()),
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => return Err(format!("poll killed daemon child: {error}")),
        }
    }
    Err(format!(
        "daemon child {pid} did not exit within bounded cleanup"
    ))
}

pub(crate) fn registry_backed_worker_identities(
    data_dir: &Path,
) -> Result<Vec<RegistryWorkerIdentity>, String> {
    let registry = SessionRegistry::new(data_dir);
    let records = registry
        .load_all()
        .map_err(|error| format!("load session registry under {}: {error}", data_dir.display()))?;
    Ok(records
        .into_iter()
        .filter(|record| !matches!(record.state, RegistrySessionState::Exited))
        .map(identity_from_record)
        .collect())
}

fn identity_from_record(record: RegistryRecord) -> RegistryWorkerIdentity {
    RegistryWorkerIdentity {
        session_id: record.session_id.0,
        pid: record.process.as_ref().and_then(|process| process.pid),
        runtime_id: record
            .process
            .as_ref()
            .and_then(|process| process.runtime_id.clone()),
    }
}

pub(crate) fn worktree_session_worker_ancestor(pid: u32) -> Option<u32> {
    let mut current = pid;
    for _ in 0..8 {
        if worker_pid_matches_worktree_session_worker(current) {
            return Some(current);
        }
        let snapshot = process_snapshot(current)?;
        if snapshot.ppid <= 1 {
            return None;
        }
        current = snapshot.ppid;
    }
    None
}

pub(crate) fn worker_pid_matches_worktree_session_worker(pid: u32) -> bool {
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
    let argv0 = command.split_whitespace().next().unwrap_or("");
    Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        == Some("botster-session-worker")
        && worker_executable_from_this_worktree(&SessionWorkerProcessIdentity {
            pid,
            command: command.trim().to_string(),
            shell_descendant_pids: Vec::new(),
        })
}

pub(crate) fn daemon_socket_path(data_dir: &Path) -> PathBuf {
    explicit_config(data_dir)
        .transports
        .local_socket
        .map(|binding| binding.path)
        .unwrap_or_else(|| data_dir.join("botster-hub.sock"))
}

pub(crate) fn classify_shutdown_session_response(
    session_id: &str,
    response: Result<botster_hub::DaemonResponse, botster_hub::DaemonTransportError>,
) -> SessionShutdownRecord {
    match response {
        Ok(response)
            if response.kind == botster_hub::DaemonResponseKind::SessionCleanup
                || response.kind == botster_hub::DaemonResponseKind::Sessions =>
        {
            SessionShutdownRecord {
                session_id: session_id.to_string(),
                classification: ShutdownSessionClass::Found,
            }
        }
        Ok(response) if response.kind == botster_hub::DaemonResponseKind::OperatorError => {
            let error = response.error;
            let body = format!(
                "code={} operation={} message={}",
                error
                    .as_ref()
                    .map(|error| error.code.as_str())
                    .unwrap_or("missing"),
                error
                    .as_ref()
                    .map(|error| error.operation.as_str())
                    .unwrap_or("missing"),
                error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("missing")
            );
            let classification = if body.contains("unknown_session") || body.contains("not found") {
                ShutdownSessionClass::Absent
            } else {
                ShutdownSessionClass::Err(body)
            };
            SessionShutdownRecord {
                session_id: session_id.to_string(),
                classification,
            }
        }
        Ok(response) => SessionShutdownRecord {
            session_id: session_id.to_string(),
            classification: ShutdownSessionClass::Err(format!(
                "unexpected ShutdownSession kind={:?} error={:?}",
                response.kind, response.error
            )),
        },
        Err(error) => SessionShutdownRecord {
            session_id: session_id.to_string(),
            classification: ShutdownSessionClass::Err(error.to_string()),
        },
    }
}

pub(crate) fn shutdown_owned_sessions(
    data_dir: &Path,
) -> Result<Vec<SessionShutdownRecord>, String> {
    let config = explicit_config(data_dir);
    let sessions = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessions,
    )
    .map_err(|error| format!("ListSessions failed: {error}"))?;
    if sessions.kind != botster_hub::DaemonResponseKind::Sessions {
        return Err(format!(
            "ListSessions unexpected kind={:?} error={:?}",
            sessions.kind, sessions.error
        ));
    }
    let mut records = Vec::new();
    for session in sessions
        .sessions
        .iter()
        .filter(|session| session.lifecycle != "exited")
    {
        let response = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::ShutdownSession {
                session_id: session.session_id.clone(),
            },
        );
        records.push(classify_shutdown_session_response(
            &session.session_id,
            response,
        ));
    }
    Ok(records)
}

pub(crate) fn reap_registry_backed_workers(data_dir: &Path) -> Result<Vec<u32>, String> {
    let identities = registry_backed_worker_identities(data_dir)?;
    let mut reaped = Vec::new();
    for identity in identities {
        let Some(pid) = identity.pid else {
            for worker in live_session_workers_for_data_dir(data_dir)? {
                if worker_pid_matches_worktree_session_worker(worker.pid) {
                    signal_worker_group(worker.pid)?;
                    reaped.push(worker.pid);
                }
            }
            continue;
        };
        let worker_pid = worktree_session_worker_ancestor(pid).unwrap_or(pid);
        if !worker_pid_matches_worktree_session_worker(worker_pid) && worker_pid == pid {
            continue;
        }
        signal_worker_group(worker_pid)?;
        if worker_pid != pid {
            signal_worker_group(pid)?;
        }
        reaped.push(worker_pid);
    }
    Ok(reaped)
}

fn signal_worker_group(pid: u32) -> Result<(), String> {
    let pgid = process_snapshot(pid)
        .map(|snapshot| snapshot.pgid)
        .unwrap_or(pid);
    signal_test_group_or_child(pgid, libc::SIGTERM)
        .or_else(|_| signal_test_group_or_child(pid, libc::SIGTERM))
        .map_err(|error| format!("TERM worker {pid}: {error}"))?;
    let deadline = Instant::now() + HUB_STOP_TERM_GRACE;
    while Instant::now() < deadline {
        if !process_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_test_group_or_child(pgid, libc::SIGKILL)
        .or_else(|_| signal_test_group_or_child(pid, libc::SIGKILL))
        .map_err(|error| format!("KILL worker {pid}: {error}"))?;
    let deadline = Instant::now() + HUB_STOP_KILL_GRACE;
    while Instant::now() < deadline {
        if !process_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(format!("worker {pid} survived bounded TERM/KILL"))
}

pub(crate) fn unlink_stale_daemon_socket(data_dir: &Path, hub_pid: Option<u32>) {
    if hub_pid.is_some_and(process_exists) {
        return;
    }
    let socket = daemon_socket_path(data_dir);
    if socket.exists() {
        let _ = fs::remove_file(socket);
    }
}

pub(crate) fn prove_owned_children_absent(
    data_dir: &Path,
    hub_pid: Option<u32>,
    known_worker_pids: &[u32],
) -> Result<(), String> {
    unlink_stale_daemon_socket(data_dir, hub_pid);
    let socket = daemon_socket_path(data_dir);
    if socket.exists() {
        return Err(format!(
            "daemon socket still present: {}",
            socket.display()
        ));
    }
    if let Some(pid) = hub_pid
        && process_exists(pid)
    {
        return Err(format!("Hub child pid {pid} still live"));
    }
    for pid in known_worker_pids {
        if process_exists(*pid) {
            return Err(format!("owned worker pid {pid} still live"));
        }
        if let Some(snapshot) = process_snapshot(*pid)
            && snapshot.stat.contains('Z')
        {
            return Err(format!(
                "owned worker pid {pid} is a zombie: {}",
                snapshot.command
            ));
        }
    }
    let leftover = session_worker_process_identities()?
        .into_iter()
        .filter(|worker| worker_belongs_to_data_dir(worker, data_dir))
        .collect::<Vec<_>>();
    if !leftover.is_empty() {
        return Err(format!(
            "data-dir session workers still live: {leftover:?}"
        ));
    }
    Ok(())
}

pub(crate) fn live_session_workers_for_data_dir(
    data_dir: &Path,
) -> Result<Vec<SessionWorkerProcessIdentity>, String> {
    Ok(session_worker_process_identities()?
        .into_iter()
        .filter(|worker| worker_belongs_to_data_dir(worker, data_dir))
        .collect())
}
