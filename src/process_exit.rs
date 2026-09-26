//! Process exit events.
//!
//! Waits end on an OS exit event, never on a poll: kqueue `EVFILT_PROC`
//! `NOTE_EXIT` on macOS and a pidfd on Linux. The caller's deadline is the only
//! timer, and its expiry is the exceptional outcome.
//!
//! A process that has exited but is not yet reaped (a zombie) counts as exited
//! for a single pid. Process groups differ per platform, because each platform
//! offers a different atomic fact; see [`wait_for_process_group_exit`].

use std::io;
use std::time::Instant;

/// Waits until `pid` exits or `deadline` passes. Returns `Ok(false)` when the
/// deadline passes first. A pid that no longer runs returns `Ok(true)` at once.
pub fn wait_for_pid_exit(pid: u32, deadline: Instant) -> io::Result<bool> {
    match PidExitWatch::register(pid)? {
        Some(mut watch) => watch.wait(deadline),
        None => Ok(true),
    }
}

/// Waits until process group `pgid` is empty or `deadline` passes. Returns
/// `Ok(false)` when the deadline passes first.
///
/// macOS: the group is empty when no member runs. `proc_listpgrppids` is an
///   atomic snapshot. Each round watches every member of a snapshot, then takes
///   a second snapshot. A member that forked before its watch was registered
///   has its child in the second snapshot, which extends the round. The group
///   is empty when a round ends with no new pid and no running member. Zombie
///   members count as exited: macOS does report a reap (`NOTE_REAP`, see
///   [`ReapWatch`]), but kqueue refuses to register on a zombie, so a zombie
///   found by enumeration cannot be watched.
///
/// Linux: the group is empty when `killpg(pgid, 0)` reports `ESRCH`, the one
///   atomic group fact, so zombie members must be reaped first. `/proc` is not an
///   atomic snapshot, so each round only chooses what to watch: running members
///   through a readable pidfd (exit) and zombie members through `POLLHUP`
///   (reap). Every round starts on one of those events.
pub fn wait_for_process_group_exit(pgid: u32, deadline: Instant) -> io::Result<bool> {
    loop {
        match watch_process_group(pgid, deadline, &mut process_group_members)? {
            GroupWatch::Empty => return Ok(true),
            GroupWatch::DeadlineExpired => return Ok(false),
            GroupWatch::Watching(mut watch) => {
                if !watch.wait(deadline)? {
                    return Ok(false);
                }
            }
        }
    }
}

/// Reports whether process group `pgid` is not yet empty, by the same rule as
/// [`wait_for_process_group_exit`]. It does not wait for an exit. A group that
/// keeps forking faster than one enumeration round counts as running.
pub fn process_group_running(pgid: u32) -> io::Result<bool> {
    Ok(!matches!(
        watch_process_group(pgid, Instant::now(), &mut process_group_members)?,
        GroupWatch::Empty
    ))
}

enum GroupWatch {
    Empty,
    Watching(ExitWatch),
    /// New members kept appearing until the caller's deadline passed.
    DeadlineExpired,
}

/// Registers watches on the members of `pgid`. The first round always runs;
/// later rounds stop at `deadline`. `members` enumerates the group; tests
/// replace it to stage an enumeration that misses a member.
type Members<'a> = &'a mut dyn FnMut(u32) -> io::Result<Vec<u32>>;

#[cfg(target_os = "macos")]
fn watch_process_group(
    pgid: u32,
    deadline: Instant,
    members: Members<'_>,
) -> io::Result<GroupWatch> {
    let mut watch = ExitWatch::new()?;
    let mut considered = std::collections::BTreeSet::new();
    let mut running = 0usize;
    let mut snapshot = members(pgid)?;
    loop {
        for pid in snapshot {
            if considered.insert(pid) && watch.watch_exit(pid)? {
                running += 1;
            }
        }
        // Every watch above was registered before this snapshot. A member that
        // was already gone at registration had exited before this snapshot, so
        // any process it forked and that still runs is in this snapshot.
        snapshot = members(pgid)?;
        if snapshot.iter().all(|pid| considered.contains(pid)) {
            return Ok(if running == 0 {
                GroupWatch::Empty
            } else {
                GroupWatch::Watching(watch)
            });
        }
        if Instant::now() >= deadline {
            return Ok(GroupWatch::DeadlineExpired);
        }
    }
}

#[cfg(target_os = "linux")]
fn watch_process_group(
    pgid: u32,
    deadline: Instant,
    members: Members<'_>,
) -> io::Result<GroupWatch> {
    let mut unlisted_rounds = 0;
    loop {
        if unsafe { libc::killpg(pgid as libc::pid_t, 0) } != 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(GroupWatch::Empty)
            } else {
                Err(error)
            };
        }
        let mut watch = ExitWatch::new()?;
        for pid in members(pgid)? {
            watch.watch_exit_or_reap(pid)?;
        }
        if !watch.is_empty() {
            return Ok(GroupWatch::Watching(watch));
        }
        // A member that appeared while /proc was read is listed by the next
        // read, because it exists for that whole read. A second empty read
        // means /proc hides the member (another pid namespace or hidepid).
        unlisted_rounds += 1;
        if unlisted_rounds == 2 {
            return Err(io::Error::other(format!(
                "process group {pgid} has members that /proc does not list"
            )));
        }
        if Instant::now() >= deadline {
            return Ok(GroupWatch::DeadlineExpired);
        }
    }
}

/// An exit watch registered now and waited on later, possibly on another
/// thread. Registration pins the process, so a later reap and pid reuse
/// cannot redirect the watch.
pub struct PidExitWatch {
    watch: ExitWatch,
}

impl PidExitWatch {
    /// Registers an exit watch on `pid`. Returns `Ok(None)` when the process
    /// already exited.
    ///
    /// # Errors
    /// Returns an OS error other than "no such process".
    pub fn register(pid: u32) -> io::Result<Option<Self>> {
        let mut watch = ExitWatch::new()?;
        Ok(watch.watch_exit(pid)?.then_some(Self { watch }))
    }

    /// Blocks until the process exits. Returns `Ok(false)` when the deadline
    /// passes first.
    ///
    /// # Errors
    /// Returns an OS error from the wait.
    pub fn wait(&mut self, deadline: Instant) -> io::Result<bool> {
        self.watch.wait(deadline)
    }
}

/// A watch that ends when a process is reaped, meaning it has left the
/// process table, not just exited. Its parent does the reaping, so a
/// non-parent can use this to wait for that step.
///
/// macOS reports the reap with `NOTE_REAP`. The SDK header marks the flag
/// deprecated, but xnu still delivers it, including to a non-parent; kqueue
/// refuses a zombie, so the watch must be registered while the process runs.
/// Linux reports it as `POLLHUP` on a pidfd, which also works for a zombie.
pub struct ReapWatch {
    watch: ExitWatch,
}

/// The outcome of [`ReapWatch::register`].
pub enum ReapRegistration {
    Watching(ReapWatch),
    /// The pid no longer names any process.
    AlreadyReaped,
    /// macOS only: the process already exited and awaits its parent's reap,
    /// and kqueue cannot watch a zombie.
    ExitedBeforeRegistration,
}

impl ReapWatch {
    /// Registers a reap watch on `pid`.
    ///
    /// # Errors
    /// Returns an OS error other than "no such process".
    #[cfg(target_os = "macos")]
    pub fn register(pid: u32) -> io::Result<ReapRegistration> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
        let change = libc::kevent {
            ident: pid as libc::uintptr_t,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_REAP,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let result = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result == 0 {
            return Ok(ReapRegistration::Watching(Self {
                watch: ExitWatch { queue },
            }));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
        // ESRCH covers a zombie and a missing pid; signal 0 separates them.
        if unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
            Ok(ReapRegistration::ExitedBeforeRegistration)
        } else {
            Ok(ReapRegistration::AlreadyReaped)
        }
    }

    /// Registers a reap watch on `pid`.
    ///
    /// # Errors
    /// Returns an OS error other than "no such process".
    #[cfg(target_os = "linux")]
    pub fn register(pid: u32) -> io::Result<ReapRegistration> {
        let Some(pidfd) = open_pidfd(pid)? else {
            return Ok(ReapRegistration::AlreadyReaped);
        };
        let mut watch = ExitWatch::new()?;
        // POLLHUP is reported without being requested; POLLIN (exit) is not
        // requested, so a zombie does not end the wait.
        watch.push(pidfd, 0);
        Ok(ReapRegistration::Watching(Self { watch }))
    }

    /// Blocks until the process is reaped. Returns `Ok(false)` when the
    /// deadline passes first.
    ///
    /// # Errors
    /// Returns an OS error from the wait.
    pub fn wait(&mut self, deadline: Instant) -> io::Result<bool> {
        self.watch.wait(deadline)
    }
}

/// Waits for an owned child's exit event, reaps it, then waits until the
/// process group it leads has no running member. Returns `Ok(None)` when the
/// deadline passes first. A child that does not lead a group still works: its
/// group has no members once it exits.
pub fn wait_for_child_group_exit(
    child: &mut std::process::Child,
    deadline: Instant,
) -> io::Result<Option<std::process::ExitStatus>> {
    // A reaped child's pid may already belong to another process, so watch the
    // pid only while the child is unreaped. `Child` keeps the reaped status.
    let status = match child.try_wait()? {
        Some(status) => status,
        None => {
            if !wait_for_pid_exit(child.id(), deadline)? {
                return Ok(None);
            }
            child.wait()?
        }
    };
    if !wait_for_process_group_exit(child.id(), deadline)? {
        return Ok(None);
    }
    Ok(Some(status))
}

#[cfg(target_os = "macos")]
struct ExitWatch {
    queue: std::os::fd::OwnedFd,
}

#[cfg(target_os = "macos")]
impl ExitWatch {
    fn new() -> io::Result<Self> {
        use std::os::fd::FromRawFd;

        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            queue: unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) },
        })
    }

    /// Returns `Ok(false)` when `pid` no longer runs. The kernel refuses an
    /// `EVFILT_PROC` registration with `ESRCH` for a zombie or a missing pid.
    fn watch_exit(&mut self, pid: u32) -> io::Result<bool> {
        use std::os::fd::AsRawFd;

        let change = libc::kevent {
            ident: pid as libc::uintptr_t,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let result = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    /// Test-only: reports whether a registered event is pending now, without
    /// waiting. A zero timeout makes the kernel answer at once. A reported
    /// event is consumed.
    #[cfg(test)]
    fn ready_now(&mut self) -> io::Result<bool> {
        use std::os::fd::AsRawFd;

        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let mut event = std::mem::MaybeUninit::<libc::kevent>::uninit();
        let result = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                std::ptr::null(),
                0,
                event.as_mut_ptr(),
                1,
                &zero,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result == 1)
    }

    /// Blocks until one watched process exits. Returns `Ok(false)` when the
    /// deadline passes first.
    fn wait(&mut self, deadline: Instant) -> io::Result<bool> {
        use std::os::fd::AsRawFd;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            let timeout = libc::timespec {
                tv_sec: remaining.as_secs() as libc::time_t,
                tv_nsec: remaining.subsec_nanos() as libc::c_long,
            };
            let mut event = std::mem::MaybeUninit::<libc::kevent>::uninit();
            // timer: deadline — the caller's bounded give-up; NOTE_EXIT ends the normal wait and expiry returns false.
            let result = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    &timeout,
                )
            };
            match result {
                0 => return Ok(false),
                1 => {
                    let event = unsafe { event.assume_init() };
                    if event.flags & libc::EV_ERROR != 0 {
                        return Err(io::Error::from_raw_os_error(event.data as i32));
                    }
                    return Ok(true);
                }
                _ => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
struct ExitWatch {
    pidfds: Vec<std::os::fd::OwnedFd>,
    polls: Vec<libc::pollfd>,
}

#[cfg(target_os = "linux")]
impl ExitWatch {
    fn new() -> io::Result<Self> {
        Ok(Self {
            pidfds: Vec::new(),
            polls: Vec::new(),
        })
    }

    fn is_empty(&self) -> bool {
        self.polls.is_empty()
    }

    /// Returns `Ok(false)` when `pid` no longer exists. The pidfd turns
    /// readable at exit, so a zombie ends the next wait at once.
    fn watch_exit(&mut self, pid: u32) -> io::Result<bool> {
        let Some(fd) = open_pidfd(pid)? else {
            return Ok(false);
        };
        self.push(fd, libc::POLLIN);
        Ok(true)
    }

    /// Watches a running member for its exit and a zombie member for its reap.
    /// `pidfd_open(2)`: the pidfd reports `POLLIN` at exit and `POLLHUP` at
    /// reap; `poll` reports `POLLHUP` even when it was not requested.
    fn watch_exit_or_reap(&mut self, pid: u32) -> io::Result<()> {
        let Some(fd) = open_pidfd(pid)? else {
            return Ok(());
        };
        // Read the state after the pidfd pins the process. A member that exits
        // after this read wakes the wait through POLLIN.
        let events = if linux_process_state(pid) == Some('Z') {
            0
        } else {
            libc::POLLIN
        };
        self.push(fd, events);
        Ok(())
    }

    fn push(&mut self, fd: std::os::fd::OwnedFd, events: libc::c_short) {
        use std::os::fd::AsRawFd;

        self.polls.push(libc::pollfd {
            fd: fd.as_raw_fd(),
            events,
            revents: 0,
        });
        self.pidfds.push(fd);
    }

    /// Test-only: reports whether a watched event is pending now, without
    /// waiting. A zero timeout makes the kernel answer at once.
    #[cfg(test)]
    fn ready_now(&mut self) -> io::Result<bool> {
        let result =
            unsafe { libc::poll(self.polls.as_mut_ptr(), self.polls.len() as libc::nfds_t, 0) };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result > 0)
    }

    /// Blocks until one watched process exits or is reaped. Returns
    /// `Ok(false)` when the deadline passes first.
    fn wait(&mut self, deadline: Instant) -> io::Result<bool> {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            // Round up so a sub-millisecond remainder still blocks.
            let millis = remaining
                .as_nanos()
                .div_ceil(1_000_000)
                .min(i32::MAX as u128) as i32;
            // timer: deadline — the caller's bounded give-up; a pidfd event ends the normal wait and expiry returns false.
            let result = unsafe {
                libc::poll(
                    self.polls.as_mut_ptr(),
                    self.polls.len() as libc::nfds_t,
                    millis,
                )
            };
            match result {
                0 => return Ok(false),
                count if count > 0 => return Ok(true),
                _ => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: u32) -> io::Result<Option<std::os::fd::OwnedFd>> {
    use std::os::fd::FromRawFd;

    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if fd >= 0 {
        return Ok(Some(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(fd as libc::c_int)
        }));
    }
    let error = io::Error::last_os_error();
    // A reaped process whose pid still names a process group has no task
    // behind that pid, and pidfd_open reports EINVAL instead of ESRCH.
    if matches!(error.raw_os_error(), Some(libc::ESRCH | libc::EINVAL)) {
        Ok(None)
    } else {
        Err(error)
    }
}

/// The state and group fields of `/proc/<pid>/stat`, or `None` once the
/// process is gone.
#[cfg(target_os = "linux")]
fn linux_process_stat(pid: u32) -> Option<(char, u32)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized command name: state ppid pgrp ...
    let (_, fields) = stat.rsplit_once(')')?;
    let mut fields = fields.split_ascii_whitespace();
    let state = fields.next()?.chars().next()?;
    let group = fields.nth(1)?.parse().ok()?;
    Some((state, group))
}

#[cfg(target_os = "linux")]
fn linux_process_state(pid: u32) -> Option<char> {
    linux_process_stat(pid).map(|(state, _)| state)
}

#[cfg(target_os = "macos")]
fn process_group_members(pgid: u32) -> io::Result<Vec<u32>> {
    let mut capacity = 64usize;
    loop {
        let mut pids = vec![0 as libc::pid_t; capacity];
        let buffer_bytes = (capacity * std::mem::size_of::<libc::pid_t>()) as libc::c_int;
        // Returns the number of pids written, not bytes.
        let count = unsafe {
            libc::proc_listpgrppids(pgid as libc::pid_t, pids.as_mut_ptr().cast(), buffer_bytes)
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = count as usize;
        // The capacity doubles and a group can never exceed kern.maxproc
        // members, so this loop ends after a bounded number of doublings.
        if count < capacity {
            pids.truncate(count);
            return Ok(pids.into_iter().map(|pid| pid as u32).collect());
        }
        capacity *= 2;
    }
}

#[cfg(target_os = "linux")]
fn process_group_members(pgid: u32) -> io::Result<Vec<u32>> {
    let mut members = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let Some(pid) = entry?
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        // A process can exit between read_dir and its stat read.
        if linux_process_stat(pid).is_some_and(|(_, group)| group == pgid) {
            members.push(pid);
        }
    }
    Ok(members)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(10)
    }

    #[test]
    fn missing_pid_reports_exit_at_once() {
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();

        assert!(wait_for_pid_exit(pid, deadline()).unwrap());
    }

    #[test]
    fn unreaped_child_counts_as_exited() {
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        assert!(wait_for_pid_exit(pid, deadline()).unwrap());
        assert!(wait_for_pid_exit(pid, deadline()).unwrap());
        child.wait().unwrap();
    }

    #[test]
    fn running_child_exit_ends_the_wait() {
        let mut child = Command::new("cat").stdin(Stdio::piped()).spawn().unwrap();
        let pid = child.id();
        let past = Instant::now();
        assert!(!wait_for_pid_exit(pid, past).unwrap());

        drop(child.stdin.take());
        assert!(wait_for_pid_exit(pid, deadline()).unwrap());
        child.wait().unwrap();
    }

    /// Starts a group whose leader has exited and been reaped while its
    /// grandchild still runs. The grandchild reads the returned leader's stdin
    /// pipe through an explicit fd 3, so a noninteractive shell cannot give it
    /// /dev/null. It prints "ready", then execs cat with stdout on /dev/null.
    /// Stdout EOF therefore proves that the leader exited and that the
    /// grandchild runs cat, which holds only fd 3. Dropping the returned pipe
    /// ends the grandchild. The pipe is taken out of the leader before it is
    /// reaped, because `Child::wait` closes a piped stdin.
    fn group_with_running_grandchild() -> (std::process::ChildStdin, u32) {
        let mut leader = Command::new("sh")
            .args([
                "-c",
                "exec 3<&0; (echo ready; exec cat <&3 >/dev/null) & exit 0",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = leader.id();
        let mut ready = String::new();
        leader
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut ready)
            .unwrap();
        assert_eq!(ready, "ready\n");
        let stdin = leader.stdin.take().unwrap();
        assert!(leader.wait().unwrap().success());
        (stdin, pgid)
    }

    #[test]
    fn reap_watch_waits_past_exit_until_the_parent_reaps() {
        let mut child = Command::new("cat").stdin(Stdio::piped()).spawn().unwrap();
        let pid = child.id();
        let ReapRegistration::Watching(mut reaped) = ReapWatch::register(pid).unwrap() else {
            panic!("a running child accepts a reap watch");
        };

        drop(child.stdin.take());
        // The exit event fired, so an exit-subscribed watch would be ready now.
        assert!(wait_for_pid_exit(pid, deadline()).unwrap());
        assert!(
            !reaped.watch.ready_now().unwrap(),
            "the reap watch must not fire at exit"
        );

        child.wait().unwrap();
        // ready_now consumes the event on macOS, so it is the last check.
        assert!(reaped.watch.ready_now().unwrap(), "the reap fires at once");
        assert!(matches!(
            ReapWatch::register(pid).unwrap(),
            ReapRegistration::AlreadyReaped
        ));
    }

    #[test]
    fn reap_watch_on_a_zombie_follows_each_platform() {
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        assert!(wait_for_pid_exit(pid, deadline()).unwrap());

        let registration = ReapWatch::register(pid).unwrap();
        #[cfg(target_os = "macos")]
        assert!(matches!(
            registration,
            ReapRegistration::ExitedBeforeRegistration
        ));
        #[cfg(target_os = "linux")]
        let ReapRegistration::Watching(mut reaped) = registration else {
            panic!("a pidfd can watch a zombie");
        };

        child.wait().unwrap();
        #[cfg(target_os = "linux")]
        assert!(reaped.wait(deadline()).unwrap());
    }

    #[test]
    fn group_wait_follows_members_that_outlive_the_leader() {
        let (stdin, pgid) = group_with_running_grandchild();

        assert!(process_group_running(pgid).unwrap());
        assert!(!wait_for_process_group_exit(pgid, Instant::now()).unwrap());

        drop(stdin);
        assert!(wait_for_process_group_exit(pgid, deadline()).unwrap());
        assert!(!process_group_running(pgid).unwrap());
    }

    #[test]
    fn group_watch_finds_a_member_forked_before_a_failed_registration() {
        // Stage the race: the first enumeration lists only the leader, which
        // forked the grandchild and exited before its watch was registered.
        // Later enumerations are real. The group must still count as running.
        let (stdin, pgid) = group_with_running_grandchild();
        let mut first = true;
        let mut members = |pgid| {
            if std::mem::take(&mut first) {
                Ok(vec![pgid])
            } else {
                process_group_members(pgid)
            }
        };

        let watch = watch_process_group(pgid, deadline(), &mut members).unwrap();
        assert!(matches!(watch, GroupWatch::Watching(_)));
        assert!(!first, "the staged enumeration ran");

        drop(stdin);
        assert!(wait_for_process_group_exit(pgid, deadline()).unwrap());
    }
}
