//! Start readiness notification for `botster-hub start --ready-fd <n>`.
//!
//! The launcher passes the write end of a pipe. The daemon writes one line,
//! `ready <protocol_version> <build_revision>`, after the control socket is
//! bound and the owner loop accepts requests, then closes the pipe. EOF
//! without that line means the start failed; the launcher reads the exit
//! status. The launcher waits on the pipe instead of polling Status.

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use botster_hub_client::DaemonCompatibility;

use crate::maintenance::embedded_build_revision;

/// The write end of the launcher's readiness pipe.
#[derive(Debug)]
pub struct DaemonReadiness {
    pipe: File,
}

impl DaemonReadiness {
    /// Wraps the write end of the launcher's pipe and makes it close-on-exec,
    /// so session workers and other children never hold it open and the
    /// launcher sees EOF when the daemon exits.
    ///
    /// # Errors
    /// Returns an error when the descriptor flags cannot be changed.
    pub fn new(pipe: OwnedFd) -> io::Result<Self> {
        let fd = pipe.as_raw_fd();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pipe: File::from(pipe),
        })
    }

    /// Adopts descriptor `fd`, which the launcher passed with `--ready-fd`.
    ///
    /// # Errors
    /// Returns an error when `fd` is a standard stream or not open.
    ///
    /// # Safety
    /// `fd` must be open and owned by nothing else in this process, and the
    /// caller must adopt it only once. The CLI adopts the descriptor that it
    /// inherited for this purpose, once, while starting the daemon.
    pub unsafe fn adopt_inherited_fd(fd: RawFd) -> io::Result<Self> {
        if fd <= libc::STDERR_FILENO {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the readiness descriptor must not be a standard stream",
            ));
        }
        if unsafe { libc::fcntl(fd, libc::F_GETFD) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the caller guarantees exclusive ownership of the open `fd`.
        Self::new(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Writes the ready line and closes the pipe. A launcher that has already
    /// gone away is not an error for the daemon.
    pub(crate) fn notify(mut self) {
        let _ = self.pipe.write_all(ready_line().as_bytes());
    }
}

fn ready_line() -> String {
    format!(
        "ready {} {}\n",
        DaemonCompatibility::current().protocol_version,
        embedded_build_revision().unwrap_or("unknown"),
    )
}

/// One parsed `ready <protocol_version> <build_revision>` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyLine {
    pub protocol_version: u16,
    pub build_revision: String,
}

impl ReadyLine {
    /// Parses one complete ready record, including its trailing newline.
    /// Anything else, such as a partial line, extra fields, or a non-numeric
    /// version, returns `None`.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let record = line.strip_suffix('\n')?;
        let mut fields = record.split(' ');
        if fields.next()? != "ready" {
            return None;
        }
        let protocol_version = fields.next()?.parse().ok()?;
        let build_revision = fields.next()?;
        if build_revision.is_empty()
            || !build_revision.bytes().all(|byte| byte.is_ascii_graphic())
            || fields.next().is_some()
        {
            return None;
        }
        Some(Self {
            protocol_version,
            build_revision: build_revision.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn notify_writes_one_ready_line_then_closes_the_pipe() {
        let (mut reader, writer) = io::pipe().unwrap();
        let readiness =
            DaemonReadiness::new(OwnedFd::from(writer)).expect("wrap the pipe write end");

        readiness.notify();

        let mut line = String::new();
        reader.read_to_string(&mut line).unwrap();
        assert_eq!(line, ready_line());
        let parsed = ReadyLine::parse(&line).expect("the daemon writes a valid record");
        assert_eq!(
            parsed.protocol_version,
            DaemonCompatibility::current().protocol_version
        );
    }

    #[test]
    fn ready_line_parser_accepts_only_complete_records() {
        assert_eq!(
            ReadyLine::parse("ready 10 abc123\n"),
            Some(ReadyLine {
                protocol_version: 10,
                build_revision: "abc123".to_string()
            })
        );
        for malformed in [
            "ready 10 abc123",
            "ready 10\n",
            "ready 10 \n",
            "ready ten abc\n",
            "ready 10 abc extra\n",
            "ready  10 abc\n",
            "READY 10 abc\n",
            "ready 70000 abc\n",
            "",
        ] {
            assert_eq!(ReadyLine::parse(malformed), None, "{malformed:?}");
        }
    }

    #[test]
    fn standard_streams_are_rejected() {
        for fd in 0..=2 {
            assert_eq!(
                // SAFETY: a standard stream is rejected before any ownership is taken.
                unsafe { DaemonReadiness::adopt_inherited_fd(fd) }
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
