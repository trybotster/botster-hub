//! The installation-scoped lease that keeps upgrades offline.
//!
//! A socket probe cannot enforce this. The Hub accepts an arbitrary data
//! directory, so a daemon launched from the same installation under a different
//! data directory stays invisible to a probe — and the installer would then
//! switch generations underneath a live Hub, reaching by another route the exact
//! mixed-pair state the generation design exists to prevent.
//!
//! Every managed Hub daemon takes `LOCK_SH|LOCK_NB` at startup and holds it for
//! its lifetime; the installer takes `LOCK_EX|LOCK_NB` and holds *the same open
//! descriptor* across its whole mutation transaction. Both are non-blocking, so
//! a contended party fails fast with a diagnostic rather than hanging: a
//! blocking installer would add a new indefinite-hang mode for no benefit, since
//! the operator can simply re-run.
//!
//! `flock` releases on process death, including `SIGKILL` and power loss. That
//! is the reason to prefer it over a pidfile — a crashed daemon must never
//! leave an installation permanently unupgradeable. It is advisory and
//! unreliable over NFS; the install prefix is local, so that is acceptable, and
//! recorded here rather than left as an unexamined assumption.

use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

use crate::layout::DAEMON_LOCK_FILE;
use crate::safety::{DirectoryHandle, InstallationProblem};

const LOCK_FILE_MODE: libc::mode_t = 0o600;

/// Which side of the lease is being taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMode {
    /// A managed Hub daemon: any number may hold this concurrently.
    Shared,
    /// The installer: excludes every daemon and every other installer.
    Exclusive,
}

impl LeaseMode {
    const fn flock_operation(self) -> libc::c_int {
        match self {
            Self::Shared => libc::LOCK_SH | libc::LOCK_NB,
            Self::Exclusive => libc::LOCK_EX | libc::LOCK_NB,
        }
    }
}

/// A held installation lease. Dropping it releases the lock.
#[derive(Debug)]
pub struct InstallationLease {
    #[expect(
        dead_code,
        reason = "the lease is the open descriptor; holding it is the point"
    )]
    descriptor: OwnedFd,
    mode: LeaseMode,
}

impl InstallationLease {
    /// Which side of the lease this handle holds.
    #[must_use]
    pub const fn mode(&self) -> LeaseMode {
        self.mode
    }
}

/// Outcome of a non-blocking acquisition attempt.
#[derive(Debug)]
pub enum LeaseOutcome {
    Acquired(InstallationLease),
    /// Someone else holds an incompatible lock right now.
    Contended,
}

/// Take the installation lease at `<prefix>/daemon.lock`.
///
/// The lock file is created with the same discipline the receipt requires:
/// user-owned, non-world-writable, `O_NOFOLLOW`, never through a symlink. A
/// world-writable lock would be a denial-of-upgrade vector.
pub fn acquire(prefix: &Path, mode: LeaseMode) -> Result<LeaseOutcome, InstallationProblem> {
    let prefix = DirectoryHandle::open_root(prefix, "prefix")
        .map_err(|problem| problem.retag("unsafe_installation_prefix"))?;
    let file = prefix
        .open_or_create_file(DAEMON_LOCK_FILE, LOCK_FILE_MODE, "lease file")
        .map_err(|problem| problem.retag("unsafe_installation_lock"))?;
    let descriptor = file.into_owned_fd();
    if unsafe { libc::flock(descriptor.as_raw_fd(), mode.flock_operation()) } == 0 {
        return Ok(LeaseOutcome::Acquired(InstallationLease {
            descriptor,
            mode,
        }));
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(LeaseOutcome::Contended),
        _ => Err(InstallationProblem::new(
            "installation_lock_failed",
            format!("installation lease could not be taken: {error}"),
        )),
    }
}
