//! Bounded execution of a staged binary.
//!
//! A bare timeout does not bound a process *tree*. A descendant can outlive the
//! direct child, retain the stdout write end, and keep the drains from ever
//! reaching EOF — so the runner would hang on a "bounded" wait. This reuses the
//! pattern already proven in the Hub's `src/entrypoint_supervisor.rs` rather
//! than inventing a second one: own process group via `setpgid(0, 0)`,
//! concurrent bounded drains started before the wait, `killpg` with a
//! `kill`-on-`ESRCH` fallback, TERM → bounded grace → KILL escalation, and
//! reaping of the group leader.
//!
//! The staged binary is executed only *after* its checksum and signature
//! verify, so this is never execution of unvalidated bytes.

use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{InstallerError, InstallerResult};

/// Wall-clock bound on a staged-binary identity probe.
pub const RUN_DEADLINE: Duration = Duration::from_secs(10);
/// Bounded grace between TERM and KILL for the process group.
const TERM_GRACE: Duration = Duration::from_millis(500);
/// Bound on captured output. Anything larger is rejected, not truncated: a
/// binary that floods stdout is not one whose identity we should trust.
pub const OUTPUT_LIMIT_BYTES: usize = 8 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Captured, bounded output of a completed child.
#[derive(Debug)]
pub struct BoundedOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Run `program` with `args` under a deadline, owning its process group.
pub fn run_bounded(
    program: &Path,
    args: &[&str],
    deadline: Duration,
) -> InstallerResult<BoundedOutput> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: `setpgid` is async-signal-safe and touches no allocator state.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().map_err(|error| {
        InstallerError::new(
            "staged_binary_unlaunchable",
            format!("{} could not be launched: {error}", program.display()),
        )
    })?;

    // Drains start before the wait so a full pipe can never deadlock the child.
    let stdout = spawn_drain(child.stdout.take());
    let stderr = spawn_drain(child.stderr.take());

    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {}
            Err(error) => {
                return Err(InstallerError::new(
                    "staged_binary_unwaitable",
                    format!("{} could not be waited on: {error}", program.display()),
                ));
            }
        }
        if started.elapsed() >= deadline {
            break None;
        }
        thread::sleep(POLL_INTERVAL);
    };

    let timed_out = status.is_none();
    if timed_out {
        terminate_group(&mut child);
    }

    let stdout = collect(stdout);
    let stderr = collect(stderr);

    if timed_out {
        return Err(InstallerError::new(
            "staged_binary_timeout",
            format!(
                "{} did not exit within {:?}; its process group was terminated",
                program.display(),
                deadline
            ),
        ));
    }
    // Output size is checked *before* exit status on purpose. Once a drain
    // stops reading at the bound the child takes `EPIPE` and dies non-zero, so
    // checking status first would report the symptom and hide the cause.
    if stdout.len() > OUTPUT_LIMIT_BYTES || stderr.len() > OUTPUT_LIMIT_BYTES {
        return Err(InstallerError::new(
            "staged_binary_output_too_large",
            format!(
                "{} produced more output than the bound allows",
                program.display()
            ),
        ));
    }
    let status = status.expect("checked above");
    if !status.success() {
        return Err(InstallerError::new(
            "staged_binary_failed",
            format!(
                "{} exited with {status}: {}",
                program.display(),
                String::from_utf8_lossy(&stderr)
            ),
        ));
    }
    Ok(BoundedOutput { stdout, stderr })
}

/// Parse `key=value` operator output into pairs, rejecting anything else.
pub fn parse_key_value(output: &[u8]) -> InstallerResult<Vec<(String, String)>> {
    let text = std::str::from_utf8(output).map_err(|_| {
        InstallerError::new(
            "staged_binary_output_malformed",
            "staged binary output is not valid UTF-8",
        )
    })?;
    let mut pairs = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(InstallerError::new(
                "staged_binary_output_malformed",
                format!("staged binary emitted a non key=value line: {line:?}"),
            ));
        };
        pairs.push((key.trim().to_string(), value.trim().to_string()));
    }
    if pairs.is_empty() {
        return Err(InstallerError::new(
            "staged_binary_output_malformed",
            "staged binary emitted no identity lines",
        ));
    }
    Ok(pairs)
}

fn spawn_drain(pipe: Option<impl Read + Send + 'static>) -> Option<mpsc::Receiver<Vec<u8>>> {
    let pipe = pipe?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buffer = Vec::new();
        // Read one byte past the bound so an oversized stream is *detected*
        // rather than silently truncated to a passing size.
        let _ = pipe
            .take(OUTPUT_LIMIT_BYTES as u64 + 1)
            .read_to_end(&mut buffer);
        let _ = tx.send(buffer);
    });
    Some(rx)
}

fn collect(drain: Option<mpsc::Receiver<Vec<u8>>>) -> Vec<u8> {
    drain
        .and_then(|rx| rx.recv_timeout(Duration::from_secs(2)).ok())
        .unwrap_or_default()
}

/// TERM the group, wait a bounded grace, then KILL, then reap the leader.
fn terminate_group(child: &mut Child) {
    let pid = child.id();
    let _ = signal_process_group_or_child(pid, libc::SIGTERM);
    let deadline = Instant::now() + TERM_GRACE;
    while Instant::now() < deadline {
        if matches!(child.try_wait(), Ok(Some(_))) && !process_group_exists(pid) {
            return;
        }
        thread::sleep(POLL_INTERVAL);
    }
    let _ = signal_process_group_or_child(pid, libc::SIGKILL);
    // Reap the leader so the installer leaves no zombie behind.
    let _ = child.wait();
}

fn signal_process_group_or_child(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = std::io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(group_error);
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = std::io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(child_error)
    }
}

fn process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_value_output_is_parsed_and_anything_else_is_rejected() {
        assert_eq!(
            parse_key_value(b"product_id=botster-hub\nversion=0.1.0\n").expect("parse"),
            vec![
                ("product_id".to_string(), "botster-hub".to_string()),
                ("version".to_string(), "0.1.0".to_string()),
            ]
        );
        assert_eq!(
            parse_key_value(b"this is not key=value output\nno-equals-here\n")
                .expect_err("malformed")
                .kind(),
            "staged_binary_output_malformed"
        );
        assert_eq!(
            parse_key_value(b"").expect_err("empty").kind(),
            "staged_binary_output_malformed"
        );
    }

    #[test]
    fn a_nonzero_exit_is_rejected() {
        let error = run_bounded(Path::new("/bin/sh"), &["-c", "exit 3"], RUN_DEADLINE)
            .expect_err("a nonzero exit must be rejected");
        assert_eq!(error.kind(), "staged_binary_failed");
    }

    #[test]
    fn oversized_output_is_rejected_rather_than_truncated() {
        let error = run_bounded(
            Path::new("/bin/sh"),
            &[
                "-c",
                "i=0; while [ $i -lt 400 ]; do printf 'k=%040d\\n' $i; i=$((i+1)); done",
            ],
            RUN_DEADLINE,
        )
        .expect_err("oversized output must be rejected");
        assert_eq!(error.kind(), "staged_binary_output_too_large");
    }

    /// The descendant-survival test. Killing only the direct child would leave
    /// the grandchild holding the stdout write end; the group kill is what makes
    /// the bound real.
    #[test]
    fn a_hanging_child_and_its_descendants_are_gone_after_the_deadline() {
        let pidfile = std::env::temp_dir().join(format!(
            "botster-hub-installer-descendant-{}.pid",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&pidfile);
        let script = format!("( sleep 60 & echo $! > {} ; wait ) ", pidfile.display());
        let error = run_bounded(
            Path::new("/bin/sh"),
            &["-c", &script],
            Duration::from_millis(400),
        )
        .expect_err("a hanging child must hit the deadline");
        assert_eq!(error.kind(), "staged_binary_timeout");

        let descendant: i32 = std::fs::read_to_string(&pidfile)
            .expect("descendant recorded its pid")
            .trim()
            .parse()
            .expect("descendant pid parses");
        let _ = std::fs::remove_file(&pidfile);

        let mut remaining = 100;
        while remaining > 0 && unsafe { libc::kill(descendant, 0) } == 0 {
            thread::sleep(POLL_INTERVAL);
            remaining -= 1;
        }
        assert_ne!(
            unsafe { libc::kill(descendant, 0) },
            0,
            "the descendant captured before the timeout must be gone afterwards"
        );

        // Teardown left nothing wedged: the same code path still works.
        let output = run_bounded(Path::new("/bin/sh"), &["-c", "echo ok=1"], RUN_DEADLINE)
            .expect("a second invocation through the same path still succeeds");
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok=1");
    }
}
