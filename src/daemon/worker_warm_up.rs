//! First-exec warm-up of the session worker before the daemon reports ready.
//!
//! On macOS the first exec of a newly written executable can take several
//! seconds, while Core's worker-readiness deadline is 2 s. The daemon execs the
//! worker once with `--probe` before it reports ready, so a newly installed or
//! updated worker pays that cost here instead of failing the first spawn.
//! A failed warm-up is logged and counted; it never blocks readiness, because
//! the first spawn may still succeed.

use std::io::Read;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::process_exit::PidExitWatch;

/// Bound on the wait for the probe's exit after `spawn` returns: a third of
/// the daemon readiness budget (user decision, 2026-09-27). `spawn` itself is
/// not bounded; the log records it separately (`spawn_ms`), which shows
/// whether a first exec's cost falls inside `spawn` or after it.
pub(crate) const WORKER_WARM_UP_DEADLINE: Duration =
    Duration::from_millis(crate::LOCAL_RUNTIME_DAEMON_READINESS_BUDGET.as_millis() as u64 / 3);

/// Bytes read from the probe's stdout. Its identity line is far shorter.
const PROBE_OUTPUT_CAPACITY: u64 = 256;

#[derive(Debug)]
pub(crate) enum WarmUpOutcome {
    /// The worker printed its identity line and exited 0.
    Ready {
        identity: String,
    },
    Failed(WarmUpFailure),
}

#[derive(Debug)]
pub(crate) enum WarmUpFailure {
    Spawn(std::io::Error),
    Wait(std::io::Error),
    /// The probe outlived the deadline and was killed.
    DeadlineExpired,
    Exit(ExitStatus),
    UnexpectedOutput(String),
}

impl std::fmt::Display for WarmUpFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(error) => write!(formatter, "spawn: {error}"),
            Self::Wait(error) => write!(formatter, "wait: {error}"),
            Self::DeadlineExpired => formatter.write_str("deadline expired"),
            Self::Exit(status) => write!(formatter, "exit: {status}"),
            Self::UnexpectedOutput(output) => write!(formatter, "unexpected output: {output:?}"),
        }
    }
}

/// Exec `worker --probe` and wait for its exit event, bounded by `deadline`
/// from when `spawn` returns. Also returns the time spent inside `spawn`.
pub(crate) fn warm_session_worker(worker: &Path, deadline: Duration) -> (WarmUpOutcome, Duration) {
    let spawning = Instant::now();
    let spawned = Command::new(worker)
        .arg("--probe")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn();
    let spawn_elapsed = spawning.elapsed();
    let mut child = match spawned {
        Ok(child) => child,
        Err(error) => {
            return (
                WarmUpOutcome::Failed(WarmUpFailure::Spawn(error)),
                spawn_elapsed,
            );
        }
    };
    // timer: deadline — bounds only a worker that never exits; the wait
    // itself ends on the child's exit event.
    let expires = Instant::now() + deadline;
    let exited = match PidExitWatch::register(child.id()) {
        Ok(Some(mut watch)) => watch.wait(expires),
        // The probe already exited before the watch was registered.
        Ok(None) => Ok(true),
        Err(error) => Err(error),
    };
    match exited {
        Ok(true) => {}
        Ok(false) => {
            // Our own child, by its handle: kill it, then reap it.
            let _ = child.kill();
            let _ = child.wait();
            return (
                WarmUpOutcome::Failed(WarmUpFailure::DeadlineExpired),
                spawn_elapsed,
            );
        }
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return (
                WarmUpOutcome::Failed(WarmUpFailure::Wait(error)),
                spawn_elapsed,
            );
        }
    }
    let status = match child.wait() {
        Ok(status) => status,
        Err(error) => {
            return (
                WarmUpOutcome::Failed(WarmUpFailure::Wait(error)),
                spawn_elapsed,
            );
        }
    };
    let mut output = String::new();
    if let Some(stdout) = child.stdout.take() {
        let _ = stdout
            .take(PROBE_OUTPUT_CAPACITY)
            .read_to_string(&mut output);
    }
    if !status.success() {
        return (
            WarmUpOutcome::Failed(WarmUpFailure::Exit(status)),
            spawn_elapsed,
        );
    }
    let outcome = match probe_identity(&output) {
        Some(identity) => WarmUpOutcome::Ready {
            identity: identity.to_string(),
        },
        None => WarmUpOutcome::Failed(WarmUpFailure::UnexpectedOutput(output)),
    };
    (outcome, spawn_elapsed)
}

/// The probe prints exactly one line: `botster-session-worker <version> protocol <n>`.
fn probe_identity(output: &str) -> Option<&str> {
    let line = output.strip_suffix('\n')?;
    if line.contains('\n') {
        return None;
    }
    let mut words = line.split(' ');
    let valid = words.next() == Some("botster-session-worker")
        && words.next().is_some_and(|version| !version.is_empty())
        && words.next() == Some("protocol")
        && words
            .next()
            .is_some_and(|protocol| protocol.parse::<u32>().is_ok())
        && words.next().is_none();
    valid.then_some(line)
}

/// Warm the worker, log the outcome, and report whether it failed.
pub(crate) fn warm_up_and_log(worker: &Path) -> bool {
    let started = Instant::now();
    let (outcome, spawn_elapsed) = warm_session_worker(worker, WORKER_WARM_UP_DEADLINE);
    let elapsed_ms = started.elapsed().as_millis();
    let spawn_ms = spawn_elapsed.as_millis();
    match &outcome {
        WarmUpOutcome::Ready { identity } => crate::hub_log::hub_log!(
            "worker_warm_up outcome=ready elapsed_ms={elapsed_ms} spawn_ms={spawn_ms} path={} identity={identity:?}",
            worker.display()
        ),
        WarmUpOutcome::Failed(failure) => crate::hub_log::hub_log!(
            "worker_warm_up outcome=failed elapsed_ms={elapsed_ms} spawn_ms={spawn_ms} path={} failure={failure}",
            worker.display()
        ),
    }
    matches!(outcome, WarmUpOutcome::Failed(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    fn fake_worker(name: &str, body: &str) -> (PathBuf, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "bh-warm-up-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("botster-session-worker");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        (directory, path)
    }

    #[test]
    fn the_deadline_is_a_third_of_the_readiness_budget() {
        assert_eq!(
            WORKER_WARM_UP_DEADLINE * 3,
            crate::LOCAL_RUNTIME_DAEMON_READINESS_BUDGET
        );
    }

    #[test]
    fn a_probe_identity_line_is_ready() {
        let (directory, worker) = fake_worker(
            "ready",
            r#"[ "$1" = "--probe" ] || exit 9; echo "botster-session-worker 0.1.0 protocol 3""#,
        );
        match warm_session_worker(&worker, WORKER_WARM_UP_DEADLINE).0 {
            WarmUpOutcome::Ready { identity } => {
                assert_eq!(identity, "botster-session-worker 0.1.0 protocol 3");
            }
            other => panic!("expected ready, got {other:?}"),
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_failing_or_malformed_probe_is_a_typed_failure() {
        let (directory, exits) = fake_worker("exit", "echo 'unknown worker argument' >&2; exit 1");
        assert!(matches!(
            warm_session_worker(&exits, WORKER_WARM_UP_DEADLINE).0,
            WarmUpOutcome::Failed(WarmUpFailure::Exit(status)) if status.code() == Some(1)
        ));
        std::fs::remove_dir_all(directory).unwrap();
        let (directory, wrong) = fake_worker("wrong", "echo 'botster-session-worker 0.1.0'");
        assert!(matches!(
            warm_session_worker(&wrong, WORKER_WARM_UP_DEADLINE).0,
            WarmUpOutcome::Failed(WarmUpFailure::UnexpectedOutput(_))
        ));
        std::fs::remove_dir_all(directory).unwrap();
        assert!(matches!(
            warm_session_worker(
                Path::new("/nonexistent/botster-session-worker"),
                WORKER_WARM_UP_DEADLINE
            )
            .0,
            WarmUpOutcome::Failed(WarmUpFailure::Spawn(_))
        ));
    }

    #[test]
    fn a_probe_that_never_exits_is_killed_at_the_deadline() {
        // The fake blocks opening a FIFO nobody writes: no timer in the fake.
        let (directory, worker) = fake_worker("hang", "exec 3< \"$(dirname \"$0\")/never\"");
        let fifo = directory.join("never");
        let status = Command::new("mkfifo").arg(&fifo).status().unwrap();
        assert!(status.success());
        let started = Instant::now();
        let outcome = warm_session_worker(&worker, Duration::from_millis(300)).0;
        assert!(matches!(
            outcome,
            WarmUpOutcome::Failed(WarmUpFailure::DeadlineExpired)
        ));
        assert!(started.elapsed() >= Duration::from_millis(300));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn the_identity_line_must_be_exact() {
        assert!(probe_identity("botster-session-worker 0.1.0 protocol 3\n").is_some());
        for bad in [
            "botster-session-worker 0.1.0 protocol 3",
            "botster-session-worker 0.1.0 protocol x\n",
            "botster-session-worker  protocol 3\n",
            "botster-session-worker 0.1.0 protocol 3 extra\n",
            "other 0.1.0 protocol 3\n",
            "botster-session-worker 0.1.0 protocol 3\nsecond\n",
        ] {
            assert!(probe_identity(bad).is_none(), "{bad:?}");
        }
    }

    /// A short socket root: a Unix socket path must stay under ~104 bytes.
    fn short_root(name: &str) -> PathBuf {
        PathBuf::from("/private/tmp").join(format!("bh-wu-{name}-{}", std::process::id()))
    }

    /// A daemon serving with `worker` as its session worker and a readiness
    /// pipe whose read end the test owns.
    struct ServedDaemon {
        endpoint: botster_hub_client::DaemonEndpoint,
        ready: std::fs::File,
        owner: std::thread::JoinHandle<()>,
        root: PathBuf,
    }

    fn serve_with_worker(root: PathBuf, worker: &Path) -> ServedDaemon {
        use std::os::fd::{FromRawFd, OwnedFd};
        std::fs::create_dir_all(&root).unwrap();
        let mut config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(root.join("data")),
            core_engine: crate::CoreEngineOptions {
                session_worker_path: Some(worker.to_path_buf()),
                ..crate::CoreEngineOptions::default()
            },
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let socket = root.join("hub.sock");
        config.transports.local_socket = Some(crate::config::LocalSocketBinding {
            path: socket.clone(),
        });
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        let readiness = crate::daemon::readiness::DaemonReadiness::new(write).unwrap();
        let owner = std::thread::spawn(move || {
            crate::daemon::owner_loop::serve_daemon(config, Some(readiness)).unwrap();
        });
        ServedDaemon {
            endpoint: botster_hub_client::DaemonEndpoint::new(socket),
            ready: std::fs::File::from(read),
            owner,
            root,
        }
    }

    impl ServedDaemon {
        /// Block for the ready line (the daemon closes the pipe after it).
        fn wait_ready(&mut self) -> crate::daemon::readiness::ReadyLine {
            let mut ready = self.ready.try_clone().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut line = String::new();
                let _ = ready.read_to_string(&mut line);
                let _ = tx.send(line);
            });
            // timer: deadline — the ready line or the pipe's EOF ends the wait.
            let line = rx
                .recv_timeout(crate::LOCAL_RUNTIME_DAEMON_READINESS_BUDGET)
                .expect("the daemon reports ready within its budget");
            crate::daemon::readiness::ReadyLine::parse(&line).expect("a ready line")
        }

        fn warm_up_failures(&self) -> u64 {
            let status = botster_hub_client::request(
                &self.endpoint,
                botster_hub_client::DaemonRequest::Status,
            )
            .unwrap();
            status
                .status
                .expect("status body")
                .lifecycle_counters
                .worker_warm_up_failures
        }

        fn stop(self) {
            let _ = botster_hub_client::request(
                &self.endpoint,
                botster_hub_client::DaemonRequest::DaemonShutdown,
            );
            self.owner.join().unwrap();
            std::fs::remove_dir_all(self.root).unwrap();
        }
    }

    /// A probe that signals `started`, then blocks until the test writes
    /// `release`. Its first act publishes its pid and then checks `abort`,
    /// the probe's half of the handshake with [`HeldProbe`].
    const HELD_PROBE: &str = r#"dir="$(dirname "$0")"
[ "$1" = "--probe" ] || exit 9
echo $$ > "$dir/pid.tmp" && mv "$dir/pid.tmp" "$dir/pid"
[ -e "$dir/abort" ] && exit 0
echo started > "$dir/started"
read _ < "$dir/release"
echo "botster-session-worker 0.1.0 protocol 3""#;

    fn held_probe(name: &str) -> (PathBuf, PathBuf, HeldProbe) {
        let (directory, worker) = fake_worker(name, HELD_PROBE);
        for fifo in ["started", "release"] {
            assert!(
                Command::new("mkfifo")
                    .arg(directory.join(fifo))
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let guard = HeldProbe {
            directory: directory.clone(),
            worker: worker.clone(),
            armed: true,
        };
        (directory, worker, guard)
    }

    /// Ends a held probe when its test fails before releasing it, so the probe
    /// never outlives the test process blocked on a FIFO.
    ///
    /// The guard creates `abort`, then reads `pid`; the probe writes `pid`,
    /// then checks `abort`. Either the probe published its pid before the
    /// guard read it (the guard kills it, whatever FIFO it is blocked on or
    /// between), or it publishes it after, and its check then sees `abort`
    /// and it exits before touching a FIFO.
    struct HeldProbe {
        directory: PathBuf,
        worker: PathBuf,
        armed: bool,
    }

    impl HeldProbe {
        /// The probe was released and has exited; its pid may be reused.
        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for HeldProbe {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            let _ = std::fs::write(self.directory.join("abort"), b"");
            let Some(pid) = std::fs::read_to_string(self.directory.join("pid"))
                .ok()
                .and_then(|pid| pid.trim().parse::<i32>().ok())
            else {
                return;
            };
            // Kill only our own child running this probe script.
            let owned = Command::new("ps")
                .args(["-o", "ppid=,command=", "-p", &pid.to_string()])
                .output()
                .ok()
                .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
                .is_some_and(|row| {
                    let mut fields = row.trim().splitn(2, ' ');
                    fields
                        .next()
                        .and_then(|ppid| ppid.trim().parse::<u32>().ok())
                        == Some(std::process::id())
                        && fields.next().is_some_and(|command| {
                            command.contains(&*self.worker.to_string_lossy())
                        })
                });
            if owned {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
    }

    fn spawn_probe(worker: &Path) -> std::process::Child {
        Command::new(worker)
            .arg("--probe")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    fn assert_probe_exits(child: &mut std::process::Child) -> ExitStatus {
        // timer: deadline — bounds a probe the guard failed to end.
        let exited = crate::process_exit::wait_for_pid_exit(
            child.id(),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
        if !exited {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the held probe outlived its guard");
        }
        child.wait().unwrap()
    }

    #[test]
    fn a_held_probe_that_starts_after_the_guard_exits_before_any_fifo() {
        let (directory, worker, guard) = held_probe("late");
        drop(guard);
        let mut child = spawn_probe(&worker);
        assert!(assert_probe_exits(&mut child).success());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_held_probe_past_its_started_signal_is_killed_by_the_guard() {
        use std::os::unix::process::ExitStatusExt;
        let (directory, worker, guard) = held_probe("window");
        let mut child = spawn_probe(&worker);
        // EOF on `started`: the probe is between its two FIFO commands or
        // blocked opening `release`, the window a FIFO write cannot reach.
        let mut started = String::new();
        std::fs::File::open(directory.join("started"))
            .unwrap()
            .read_to_string(&mut started)
            .unwrap();
        assert_eq!(started, "started\n");
        drop(guard);
        assert_eq!(assert_probe_exits(&mut child).signal(), Some(libc::SIGKILL));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_held_probe_blocked_before_its_started_signal_is_ended_by_the_guard() {
        let (directory, worker, guard) = held_probe("unread");
        let mut child = spawn_probe(&worker);
        // Nobody opens `started`: whether the probe published its pid yet or
        // not, the guard ends it (a kill, or the abort check).
        drop(guard);
        assert_probe_exits(&mut child);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_running_warm_up_holds_readiness_and_a_failed_one_is_counted() {
        // The probe signals it started, then blocks until the test releases it.
        let (directory, worker, mut held) = held_probe("held");
        let mut daemon = serve_with_worker(short_root("held"), &worker);
        // Opening the FIFO for reading blocks until the probe opens it: the
        // probe is running inside the warm-up.
        let mut started = String::new();
        std::fs::File::open(directory.join("started"))
            .unwrap()
            .read_to_string(&mut started)
            .unwrap();
        assert_eq!(started, "started\n");
        let flags = unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&daemon.ready),
                libc::F_GETFL,
            )
        };
        unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&daemon.ready),
                libc::F_SETFL,
                flags | libc::O_NONBLOCK,
            )
        };
        let mut early = [0u8; 1];
        let unready = std::io::Read::read(&mut daemon.ready, &mut early);
        assert_eq!(
            unready.map_err(|error| error.kind()),
            Err(std::io::ErrorKind::WouldBlock),
            "no ready line while the warm-up probe is running"
        );
        unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&daemon.ready),
                libc::F_SETFL,
                flags,
            )
        };
        std::fs::write(directory.join("release"), "go\n").unwrap();
        daemon.wait_ready();
        held.disarm();
        assert_eq!(daemon.warm_up_failures(), 0);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();

        let (directory, worker) = fake_worker("failed", "exit 1");
        let mut daemon = serve_with_worker(short_root("failed"), &worker);
        daemon.wait_ready();
        assert_eq!(daemon.warm_up_failures(), 1, "a failed warm-up is counted");
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
