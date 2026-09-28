//! Test-owned process sweep, shared by every test target that starts
//! daemons or other long-lived processes.
//!
//! A test registers each unique directory it creates. When the test's
//! outermost guard drops (last in the test body, and also while unwinding), the
//! sweep stops every process whose command line names one of those
//! directories, with its descendants and the process groups it leads. It
//! signals only those recorded identities, never by name, and waits on
//! process-exit events (`botster_hub::process_exit`), never a poll.
#![allow(dead_code)]

use std::cell::Cell;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// What a sweep found.
#[derive(Debug)]
pub enum SweepOutcome {
    /// No process named a test-owned directory.
    Clean,
    /// Leaked processes were found and stopped; the report names them.
    Stopped(String),
    /// Absence is unproven: a census failed or a process survived SIGKILL.
    Unproven(String),
}

thread_local! {
    /// Unique directory tokens this test created. libtest runs each test on
    /// its own thread, so the registry is per test.
    static TEST_OWNED_DIR_TOKENS: std::cell::RefCell<Vec<String>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Test seam: the census program. Only the sweep's own proofs change it.
    static TEST_OWNED_CENSUS_PROGRAM: Cell<&'static str> = const { Cell::new("ps") };
}

/// How long a process that names a test-owned directory may take to exit on
/// its own after the test body ends, before the sweep treats it as leaked.
const TEST_OWNED_PROCESS_SETTLE: Duration = Duration::from_secs(2);
const TEST_OWNED_PROCESS_TERM_GRACE: Duration = Duration::from_secs(2);

pub fn register_test_owned_dir(path: &Path) {
    // The final component is unique per call, and it survives both a relative
    // `--data-dir` argument and /tmp → /private/tmp canonicalization.
    if let Some(token) = path.file_name().and_then(|name| name.to_str()) {
        TEST_OWNED_DIR_TOKENS.with(|tokens| tokens.borrow_mut().push(token.to_string()));
    }
}

pub fn set_test_owned_census_program(program: &'static str) {
    TEST_OWNED_CENSUS_PROGRAM.with(|slot| slot.set(program));
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestOwnedProcess {
    pub pid: u32,
    pub ppid: u32,
    pub pgid: u32,
    pub command: String,
}

/// Every live (non-zombie) process except this one. A census that cannot run
/// or exits nonzero is an error, never an empty host.
fn test_owned_census() -> Result<Vec<TestOwnedProcess>, String> {
    let program = TEST_OWNED_CENSUS_PROGRAM.with(Cell::get);
    let output = Command::new(program)
        .args(["-axo", "pid=,ppid=,pgid=,stat=,command="])
        .output()
        .map_err(|error| format!("test-owned process census `{program}` did not run: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "test-owned process census `{program}` exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let own_pid = std::process::id();
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid: u32 = parts.next()?.parse().ok()?;
            let ppid: u32 = parts.next()?.parse().ok()?;
            let pgid: u32 = parts.next()?.parse().ok()?;
            let stat = parts.next()?;
            let command = parts.collect::<Vec<_>>().join(" ");
            (pid != own_pid && !stat.contains('Z')).then_some(TestOwnedProcess {
                pid,
                ppid,
                pgid,
                command,
            })
        })
        .collect())
}

/// Processes whose command line names a directory this test created.
pub fn test_owned_process_rows(tokens: &[String]) -> Result<Vec<TestOwnedProcess>, String> {
    Ok(test_owned_census()?
        .into_iter()
        .filter(|row| {
            tokens
                .iter()
                .any(|token| row.command.contains(token.as_str()))
        })
        .collect())
}

/// Processes that name a test-owned directory, plus all their descendants.
/// A session worker's own command line does not name the data directory, so
/// descent from the daemon that does is what attributes it.
pub fn test_owned_process_tree(tokens: &[String]) -> Result<Vec<TestOwnedProcess>, String> {
    TestOwnedIdentities::new(tokens.to_vec()).census()
}

/// The identities the sweep owns once it has found a leak: the matched PIDs,
/// every descendant of them, and every process group led by one of them
/// (never the harness's own group). Ownership is retained, not rediscovered from argv, so
/// a descendant without the token still counts after its parent exits.
struct TestOwnedIdentities {
    tokens: Vec<String>,
    pids: std::collections::BTreeSet<u32>,
    pgids: std::collections::BTreeSet<u32>,
    own_pgid: u32,
}

impl TestOwnedIdentities {
    fn new(tokens: Vec<String>) -> Self {
        Self {
            tokens,
            pids: Default::default(),
            pgids: Default::default(),
            own_pgid: unsafe { libc::getpgrp() } as u32,
        }
    }

    /// Adds token matches, descendants and started groups from one census,
    /// then returns the live owned rows.
    fn census(&mut self) -> Result<Vec<TestOwnedProcess>, String> {
        let rows = test_owned_census()?;
        loop {
            let before = (self.pids.len(), self.pgids.len());
            for row in &rows {
                let owned = self
                    .tokens
                    .iter()
                    .any(|token| row.command.contains(token.as_str()))
                    || self.pids.contains(&row.pid)
                    || self.pids.contains(&row.ppid)
                    || self.pgids.contains(&row.pgid);
                if owned {
                    self.pids.insert(row.pid);
                    // Claim a group only when an owned process leads it, which
                    // means the test's own process tree created it. A matched
                    // process inside someone else's group owns only itself
                    // and its descendants, never its siblings.
                    if row.pid == row.pgid && row.pgid != self.own_pgid && row.pgid > 1 {
                        self.pgids.insert(row.pgid);
                    }
                }
            }
            if (self.pids.len(), self.pgids.len()) == before {
                break;
            }
        }
        Ok(rows
            .into_iter()
            .filter(|row| self.pids.contains(&row.pid) || self.pgids.contains(&row.pgid))
            .collect())
    }

    fn signal(&self, live: &[TestOwnedProcess], signal: libc::c_int) {
        for pgid in &self.pgids {
            unsafe { libc::killpg(*pgid as libc::pid_t, signal) };
        }
        for row in live.iter().filter(|row| !self.pgids.contains(&row.pgid)) {
            unsafe { libc::kill(row.pid as libc::pid_t, signal) };
        }
    }

    /// Waits until no owned process lives, or `grace` passes. Each round takes
    /// one census, then blocks on exit events for everything it found: the
    /// exit of each owned process group (which also covers members that fork
    /// meanwhile) and of each live process outside those groups. The next
    /// census runs only after those events, and finds descendants born before
    /// their parents exited. The deadline is the give-up, never the progress.
    fn wait_absent(&mut self, grace: Duration) -> Result<Vec<TestOwnedProcess>, String> {
        // timer: deadline — the settle or signal grace; exit events end each round.
        let deadline = Instant::now() + grace;
        loop {
            let live = self.census()?;
            if live.is_empty() || Instant::now() >= deadline {
                return Ok(live);
            }
            let pgids: Vec<u32> = self.pgids.iter().copied().collect();
            for pgid in &pgids {
                let exited =
                    botster_hub::process_exit::wait_for_process_group_exit(*pgid, deadline)
                        .map_err(|error| format!("wait for owned group {pgid} to exit: {error}"))?;
                if !exited {
                    return self.census();
                }
            }
            for row in live.iter().filter(|row| !pgids.contains(&row.pgid)) {
                let exited = botster_hub::process_exit::wait_for_pid_exit(row.pid, deadline)
                    .map_err(|error| format!("wait for owned pid {} to exit: {error}", row.pid))?;
                if !exited {
                    return self.census();
                }
            }
        }
    }
}

/// Stops every process left behind that names a directory this test created,
/// with its descendants and the groups they started. It signals only those
/// recorded identities, never by name. On the success path a leftover process
/// or a failed census fails the test; while unwinding, the original panic
/// stays the reported failure and the sweep only reports. A survivor or a
/// failed census also taints the harness, because absence is unproven.
pub fn sweep_test_owned_processes() -> SweepOutcome {
    let tokens = TEST_OWNED_DIR_TOKENS.with(|tokens| std::mem::take(&mut *tokens.borrow_mut()));
    if tokens.is_empty() {
        return SweepOutcome::Clean;
    }
    let mut owned = TestOwnedIdentities::new(tokens);
    let outcome = (|| -> Result<Option<String>, String> {
        // A fixture may still be exiting on its own. Each census retains the
        // descendants and groups it sees, so a child that outlives its
        // token-named parent during the settle is still owned.
        let leaked = owned.wait_absent(TEST_OWNED_PROCESS_SETTLE)?;
        if leaked.is_empty() {
            return Ok(None);
        }
        let mut live = leaked.clone();
        for signal in [libc::SIGTERM, libc::SIGKILL] {
            owned.signal(&live, signal);
            live = owned.wait_absent(TEST_OWNED_PROCESS_TERM_GRACE)?;
            if live.is_empty() {
                break;
            }
        }
        if live.is_empty() {
            Ok(Some(format!(
                "test-owned processes outlived the test: leaked={leaked:?}"
            )))
        } else {
            Err(format!(
                "test-owned processes outlived the test and survived SIGKILL: leaked={leaked:?} survivors={live:?}"
            ))
        }
    })();
    match outcome {
        Ok(None) => SweepOutcome::Clean,
        Ok(Some(report)) => SweepOutcome::Stopped(report),
        Err(error) => SweepOutcome::Unproven(error),
    }
}

/// Reports a sweep outcome. A leak fails a passing test; while unwinding, the
/// original panic stays the reported failure and the sweep only reports.
/// `on_unproven` runs first when absence could not be proven (a census failed
/// or a process survived SIGKILL), for a target that keeps harness taint.
pub fn report_sweep(outcome: SweepOutcome, on_unproven: impl FnOnce(&str)) {
    let report = match outcome {
        SweepOutcome::Clean => return,
        SweepOutcome::Stopped(report) => report,
        SweepOutcome::Unproven(error) => {
            on_unproven(&error);
            error
        }
    };
    if std::thread::panicking() {
        eprintln!("{report}");
    } else {
        panic!("{report}");
    }
}
