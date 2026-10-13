//! `botster-hub up --ready-fd <N>` (HC HS-0, CLI DEP-2).

use botster_hub::daemon::{Daemon, DaemonConfig};
use botster_hub::failure::{FailCode, StartFailure};
use botster_hub::roots::{self, Env};
use std::fs::File;
use std::io::Write;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::process::ExitCode;

const USAGE: &str = "usage: botster-hub up [--ready-fd <N>]";

/// Takes ownership of the inherited ready descriptor `n` (HC HS-0 `--ready-fd`).
///
/// The one `unsafe_code` exception of this package (lead ruling 2026-10-12; listed in the gate-kit Policy exceptions).
/// SAFETY: `n` is the descriptor that the parent passed for HS-0's one ready line. `main` calls this exactly once, before any
/// thread starts and before anything else opens a descriptor, and nothing else in the process refers to `n`. The `F_GETFD`
/// check first proves that `n` is open; a closed or invalid `n` is refused, typed, and never owned.
#[allow(unsafe_code)]
fn take_ready_fd(n: RawFd) -> Result<OwnedFd, StartFailure> {
    let invalid = |why: String| {
        StartFailure::new(
            FailCode::Internal,
            format!("the ready descriptor {n} {why}"),
        )
    };
    if n < 0 {
        return Err(invalid("is negative".into()));
    }
    // SAFETY: the borrow lives only for the `fcntl` call, and `n` is not closed meanwhile (one thread, see above).
    let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(n) };
    rustix::io::fcntl_getfd(borrowed).map_err(|errno| invalid(format!("is not open: {errno}")))?;
    // SAFETY: `n` is open, and this process has no other owner of it (see above).
    Ok(unsafe { OwnedFd::from_raw_fd(n) })
}

/// Writes the one ready line and closes the descriptor (HC HS-0). With no descriptor, the line goes to stdout.
fn ready_line(ready: Option<OwnedFd>, line: &str) {
    match ready {
        Some(fd) => {
            let mut file = File::from(fd);
            let _ = file.write_all(line.as_bytes());
            // Dropping the file closes the descriptor: the parent sees the line, then end of file.
        }
        None => {
            let _ = std::io::stdout().write_all(line.as_bytes());
        }
    }
}

fn parse(args: &[String]) -> Result<Option<RawFd>, String> {
    match args {
        [up] if up == "up" => Ok(None),
        [up, flag, n] if up == "up" && flag == "--ready-fd" => n
            .parse::<RawFd>()
            .map(Some)
            .map_err(|_| format!("--ready-fd takes a descriptor number, not {n:?}")),
        _ => Err(USAGE.into()),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let ready_fd = match parse(&args) {
        Ok(fd) => fd,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    // The one call of `take_ready_fd`, before any thread starts.
    let ready = match ready_fd.map(take_ready_fd).transpose() {
        Ok(ready) => ready,
        Err(failure) => {
            eprint!("{}", failure.ready_line());
            return ExitCode::FAILURE;
        }
    };
    // HC RT-2 and TB-L1: every file and directory the Hub creates is private to its uid.
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
    let env = Env::from_process();
    let roots = roots::derive(&env);
    // HC RT-3: the worker is a sibling of this binary; there is no `PATH` fallback.
    let worker = match std::env::current_exe() {
        Ok(exe) => exe.with_file_name("botster-worker"),
        Err(error) => {
            let failure =
                StartFailure::new(FailCode::Internal, format!("the executable path: {error}"));
            ready_line(ready, &failure.ready_line());
            return ExitCode::FAILURE;
        }
    };
    let daemon = match Daemon::start(&roots, env.uid, DaemonConfig::production(worker)) {
        Ok(daemon) => daemon,
        Err(failure) => {
            ready_line(ready, &failure.ready_line());
            return ExitCode::FAILURE;
        }
    };
    ready_line(ready, &format!("ready {}\n", daemon.socket.display()));
    ExitCode::from(daemon.wait().code())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args() {
        let args = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(parse(&args(&["up"])), Ok(None));
        assert_eq!(parse(&args(&["up", "--ready-fd", "3"])), Ok(Some(3)));
        assert!(parse(&args(&["up", "--ready-fd", "x"])).is_err());
        assert!(parse(&args(&[])).is_err());
        assert!(parse(&args(&["down"])).is_err());
    }
}
