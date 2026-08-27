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
    pub(crate) force_identity_capture_failure: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OwnedSessionProcesses {
    pub(crate) pids: Vec<u32>,
    pub(crate) pgids: Vec<u32>,
}

impl OwnedSessionProcesses {
    pub(crate) fn from_pids(pids: impl IntoIterator<Item = u32>) -> Self {
        let mut set = Self::default();
        for pid in pids {
            set.push_pid(pid);
        }
        set
    }

    pub(crate) fn push_pid(&mut self, pid: u32) {
        if pid > 1 && !self.pids.contains(&pid) {
            self.pids.push(pid);
        }
    }

    pub(crate) fn push_pgid(&mut self, pgid: u32) {
        if pgid > 1 && !self.pgids.contains(&pgid) {
            self.pgids.push(pgid);
        }
    }

    pub(crate) fn extend(&mut self, other: Self) {
        for pid in other.pids {
            self.push_pid(pid);
        }
        for pgid in other.pgids {
            self.push_pgid(pgid);
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct IdentityCapture {
    pub(crate) owned: OwnedSessionProcesses,
    pub(crate) errors: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct WorkerReapOutcome {
    pub(crate) reaped: Vec<u32>,
    pub(crate) retained: Vec<u32>,
    pub(crate) errors: Vec<String>,
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

/// Clears injected taint on drop while the caller still holds `daemon_test_guard`.
pub(crate) struct ScopedHarnessTaint;

impl ScopedHarnessTaint {
    pub(crate) fn inject(evidence: impl Into<String>) -> Self {
        record_harness_taint(evidence);
        Self
    }
}

impl Drop for ScopedHarnessTaint {
    fn drop(&mut self) {
        reset_harness_taint_after_proof();
    }
}

/// Clears harness taint on drop so a failed proof cannot poison later tests.
pub(crate) struct ResetHarnessTaintOnDrop;

impl Drop for ResetHarnessTaintOnDrop {
    fn drop(&mut self) {
        reset_harness_taint_after_proof();
    }
}

pub(crate) fn format_harness_budget_expired(
    kind: &str,
    budget: Duration,
    resource: HostResourceClass,
    probe: ResourceProbe,
    evidence: &str,
) -> String {
    let thread = std::thread::current();
    let test = thread
        .name()
        .unwrap_or("unknown")
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
        .to_string();
    format!(
        "harness_budget_expired test={test} kind={kind} budget_ms={} resource={} probe={} {evidence}",
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
    probe_pty_allocation_from_open_result(posix_openpt_result())
}

fn posix_openpt_result() -> Result<(), i32> {
    let fd = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
    if fd >= 0 {
        unsafe { libc::close(fd) };
        Ok(())
    } else {
        Err(io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    }
}

pub(crate) fn probe_pty_allocation_from_open_result(result: Result<(), i32>) -> ResourceProbe {
    match result {
        Ok(()) => ResourceProbe::Unconfirmed,
        Err(code) if pty_allocation_errnos().contains(&code) => ResourceProbe::Confirmed,
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
        HostResourceClass::Emfile
        | HostResourceClass::Enfile
        | HostResourceClass::PtyAllocation => (class, ResourceProbe::NotApplicable),
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

pub(crate) fn collect_owned_session_processes(data_dir: &Path) -> Result<IdentityCapture, String> {
    let registry = SessionRegistry::new(data_dir);
    let records = registry.load_all().map_err(|error| {
        format!(
            "load session registry under {}: {error}",
            data_dir.display()
        )
    })?;
    let mut capture = IdentityCapture::default();
    for record in records {
        collect_registry_record(&registry, record, &mut capture);
    }
    Ok(capture)
}

fn collect_registry_record(
    registry: &SessionRegistry,
    record: RegistryRecord,
    capture: &mut IdentityCapture,
) {
    if matches!(record.state, RegistrySessionState::Exited) {
        return;
    }
    let session_id = record.session_id.0.clone();
    let Some(command_pid) = record.process.as_ref().and_then(|process| process.pid) else {
        capture
            .errors
            .push(format!("registry session {session_id} has no process pid"));
        return;
    };
    if let Some(snapshot) = process_snapshot(command_pid) {
        capture.owned.push_pid(command_pid);
        capture.owned.push_pgid(snapshot.pgid);
        match worktree_session_worker_ancestor(command_pid) {
            Some(worker_pid) => retain_verified_worker(capture, &session_id, worker_pid),
            None => capture.errors.push(format!(
                "unresolved worktree session-worker ancestor for command {command_pid} session {session_id}"
            )),
        }
        return;
    }
    match reread_until_exited_or_bound(registry, &record.session_id) {
        Ok(None) => {}
        Ok(Some(latest)) if matches!(latest.state, RegistrySessionState::Exited) => {}
        Ok(Some(latest)) => {
            if let Some(worker_pid) = recovery_worker_pid(&latest) {
                retain_recovery_worker(capture, &session_id, command_pid, worker_pid);
            } else {
                capture.owned.push_pid(command_pid);
                capture.errors.push(format!(
                    "nonterminal session {session_id} has dead command {command_pid} and no recovery worker pid"
                ));
            }
        }
        Err(error) => capture.errors.push(error),
    }
}

fn recovery_worker_is_live(pid: u32) -> bool {
    process_exists(pid)
        && !process_snapshot(pid).is_some_and(|snapshot| snapshot.stat.contains('Z'))
}

fn retain_recovery_worker(
    capture: &mut IdentityCapture,
    session_id: &str,
    command_pid: u32,
    worker_pid: u32,
) {
    capture.owned.push_pid(command_pid);
    capture.owned.push_pid(worker_pid);
    if !recovery_worker_is_live(worker_pid) {
        return;
    }
    if !worker_pid_matches_worktree_session_worker(worker_pid) {
        capture.errors.push(format!(
            "resolved worker {worker_pid} is live but unverifiable for session {session_id}"
        ));
        return;
    }
    retain_verified_worker(capture, session_id, worker_pid);
}

fn retain_verified_worker(capture: &mut IdentityCapture, session_id: &str, worker_pid: u32) {
    capture.owned.push_pid(worker_pid);
    if let Some(snapshot) = process_snapshot(worker_pid) {
        capture.owned.push_pgid(snapshot.pgid);
    } else if process_exists(worker_pid) {
        capture.errors.push(format!(
            "process snapshot missing for worker {worker_pid} session {session_id}"
        ));
    }
    match worker_owned_descendant_pids(worker_pid) {
        Ok(descendants) => {
            for pid in descendants {
                capture.owned.push_pid(pid);
                if let Some(snapshot) = process_snapshot(pid) {
                    capture.owned.push_pgid(snapshot.pgid);
                }
            }
        }
        Err(error) => capture.errors.push(format!(
            "descendant census failed for worker {worker_pid} session {session_id}: {error}"
        )),
    }
}

fn recovery_worker_pid(record: &RegistryRecord) -> Option<u32> {
    record
        .recovery_identity
        .as_ref()?
        .get("worker_pid")?
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid > 1)
}

fn reread_until_exited_or_bound(
    registry: &SessionRegistry,
    session_id: &botster_core::SessionId,
) -> Result<Option<RegistryRecord>, String> {
    const ATTEMPTS: u32 = 8;
    let mut last = None;
    for attempt in 0..ATTEMPTS {
        let record = registry
            .load(session_id)
            .map_err(|error| format!("reload session registry record {}: {error}", session_id.0))?;
        match &record {
            None => return Ok(None),
            Some(current) if matches!(current.state, RegistrySessionState::Exited) => {
                return Ok(record);
            }
            Some(_) => last = record,
        }
        if attempt + 1 < ATTEMPTS {
            thread::sleep(Duration::from_millis(25));
        }
    }
    Ok(last)
}

pub(crate) fn registry_backed_worker_identities(
    data_dir: &Path,
) -> Result<Vec<RegistryWorkerIdentity>, String> {
    let registry = SessionRegistry::new(data_dir);
    let records = registry.load_all().map_err(|error| {
        format!(
            "load session registry under {}: {error}",
            data_dir.display()
        )
    })?;
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
    Path::new(argv0).file_name().and_then(|name| name.to_str()) == Some("botster-session-worker")
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
    let sessions =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
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

pub(crate) fn reap_registry_backed_workers(data_dir: &Path) -> Result<WorkerReapOutcome, String> {
    let identities = registry_backed_worker_identities(data_dir)?;
    let mut outcome = WorkerReapOutcome::default();
    for identity in identities {
        let Some(pid) = identity.pid else {
            match live_session_workers_for_data_dir(data_dir) {
                Ok(workers) => {
                    let mut matched = false;
                    for worker in workers {
                        if worker_pid_matches_worktree_session_worker(worker.pid) {
                            match signal_worker_group(worker.pid) {
                                Ok(()) => outcome.reaped.push(worker.pid),
                                Err(error) => outcome.errors.push(error),
                            }
                            matched = true;
                        }
                    }
                    if !matched {
                        outcome.errors.push(format!(
                            "registry session {} has no process pid and no worktree session-worker",
                            identity.session_id
                        ));
                    }
                }
                Err(error) => outcome.errors.push(error),
            }
            continue;
        };
        match worktree_session_worker_ancestor(pid) {
            Some(worker_pid) => {
                match signal_worker_group(worker_pid) {
                    Ok(()) => outcome.reaped.push(worker_pid),
                    Err(error) => outcome.errors.push(error),
                }
                if worker_pid != pid {
                    match signal_worker_group(pid) {
                        Ok(()) => outcome.reaped.push(pid),
                        Err(error) => outcome.errors.push(error),
                    }
                }
            }
            None if process_exists(pid) => {
                outcome.errors.push(format!(
                    "unresolved worktree session-worker ancestor for command {pid} session {}",
                    identity.session_id
                ));
                outcome.retained.push(pid);
            }
            None => match reread_until_exited_or_bound(
                &SessionRegistry::new(data_dir),
                &botster_core::SessionId(identity.session_id.clone()),
            ) {
                Ok(None) => {}
                Ok(Some(latest)) if matches!(latest.state, RegistrySessionState::Exited) => {}
                Ok(Some(latest)) => match recovery_worker_pid(&latest) {
                    Some(worker_pid)
                        if recovery_worker_is_live(worker_pid)
                            && worker_pid_matches_worktree_session_worker(worker_pid) =>
                    {
                        match signal_worker_group(worker_pid) {
                            Ok(()) => outcome.reaped.push(worker_pid),
                            Err(error) => outcome.errors.push(error),
                        }
                    }
                    Some(worker_pid) if recovery_worker_is_live(worker_pid) => {
                        outcome.errors.push(format!(
                            "resolved worker {worker_pid} is live but unverifiable for session {}",
                            identity.session_id
                        ));
                        outcome.retained.push(pid);
                        outcome.retained.push(worker_pid);
                    }
                    Some(worker_pid) => {
                        outcome.retained.push(pid);
                        outcome.retained.push(worker_pid);
                    }
                    None => {
                        outcome.errors.push(format!(
                            "nonterminal session {} has dead command {pid} and no recovery worker pid",
                            identity.session_id
                        ));
                        outcome.retained.push(pid);
                    }
                },
                Err(error) => outcome.errors.push(error),
            },
        }
    }
    Ok(outcome)
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
    owned: &OwnedSessionProcesses,
) -> Result<(), String> {
    prove_owned_absence(data_dir, hub_pid, owned, true)
}

pub(crate) fn prove_hub_and_socket_absent(
    data_dir: &Path,
    hub_pid: Option<u32>,
) -> Result<(), String> {
    prove_owned_absence(data_dir, hub_pid, &OwnedSessionProcesses::default(), false)
}

fn prove_owned_absence(
    data_dir: &Path,
    hub_pid: Option<u32>,
    owned: &OwnedSessionProcesses,
    expect_workers_gone: bool,
) -> Result<(), String> {
    unlink_stale_daemon_socket(data_dir, hub_pid);
    let socket = daemon_socket_path(data_dir);
    if socket.exists() {
        return Err(format!("daemon socket still present: {}", socket.display()));
    }
    if let Some(pid) = hub_pid
        && process_exists(pid)
    {
        return Err(format!("Hub child pid {pid} still live"));
    }
    if !expect_workers_gone {
        return Ok(());
    }
    for pid in &owned.pids {
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
    for pgid in &owned.pgids {
        match process_group_probe(*pgid as libc::pid_t) {
            Ok(true) => {
                return Err(format!(
                    "owned process group {pgid} still has members: {:?}",
                    process_group_census(*pgid as libc::pid_t).unwrap_or_default()
                ));
            }
            Ok(false) => {}
            Err(error) => return Err(error),
        }
    }
    let leftover = session_worker_process_identities()?
        .into_iter()
        .filter(|worker| worker_belongs_to_data_dir(worker, data_dir))
        .collect::<Vec<_>>();
    if !leftover.is_empty() {
        return Err(format!("data-dir session workers still live: {leftover:?}"));
    }
    Ok(())
}

#[test]
fn pty_probe_succeeds_with_libc_posix_openpt() {
    assert_eq!(
        probe_pty_allocation(),
        ResourceProbe::Unconfirmed,
        "a free PTY must not be classified as exhaustion"
    );
}

#[test]
fn pty_probe_confirms_typed_allocation_errnos() {
    for code in pty_allocation_errnos() {
        assert_eq!(
            probe_pty_allocation_from_open_result(Err(*code)),
            ResourceProbe::Confirmed,
            "errno {code} must confirm PTY exhaustion"
        );
    }
    assert_eq!(
        probe_pty_allocation_from_open_result(Err(libc::EINVAL)),
        ResourceProbe::Unconfirmed
    );
    assert_eq!(
        probe_pty_allocation_from_open_result(Ok(())),
        ResourceProbe::Unconfirmed
    );
}
