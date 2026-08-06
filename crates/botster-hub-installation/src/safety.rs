//! Descriptor-relative filesystem primitives for managed-installation state.
//!
//! Every safety property here is a property of the *operations*, not of a
//! preceding check. `symlink_metadata` followed by `fs::write` is a check/use
//! race: the path can be substituted between the two. So each component below
//! an already-validated directory descriptor is opened `O_NOFOLLOW` relative to
//! that descriptor, and ownership and permissions are validated with `fstat` on
//! the opened descriptor rather than restated against a path.
//!
//! This module deliberately exposes no path-taking write API. A caller cannot
//! reintroduce the race by reaching for a convenience helper, because none
//! exists.

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

/// A diagnosable installation-state problem.
///
/// `kind` is a stable machine-readable token surfaced through Hub installation
/// diagnostics and installer exit diagnostics; `message` is operator-facing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallationProblem {
    kind: &'static str,
    message: String,
}

impl InstallationProblem {
    #[must_use]
    pub fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Re-label a problem for a subject with its own diagnostic vocabulary,
    /// keeping the message that explains what actually failed.
    #[must_use]
    pub fn retag(self, kind: &'static str) -> Self {
        Self { kind, ..self }
    }
}

impl std::fmt::Display for InstallationProblem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for InstallationProblem {}

/// Result of an open that treats absence as an ordinary outcome.
pub type MaybeOpened<T> = Result<Option<T>, InstallationProblem>;

/// An open directory descriptor whose owner and mode have been validated.
#[derive(Debug)]
pub struct DirectoryHandle {
    fd: OwnedFd,
}

impl AsRawFd for DirectoryHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// An open regular-file descriptor whose owner and mode have been validated.
#[derive(Debug)]
pub struct FileHandle {
    fd: OwnedFd,
}

impl AsRawFd for FileHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl DirectoryHandle {
    /// Open a directory by path, following symlinks on the path itself.
    ///
    /// Used only for roots the caller supplies deliberately — `$HOME` and an
    /// explicit `--prefix`. `$HOME` is legitimately a symlink on many setups, so
    /// constraining it would break working installations; every component
    /// *below* a root is opened `O_NOFOLLOW`.
    pub fn open_root(path: &Path, subject: &'static str) -> Result<Self, InstallationProblem> {
        let raw = cstring(path.as_os_str().as_encoded_bytes(), subject)?;
        let fd = unsafe { libc::open(raw.as_ptr(), libc::O_RDONLY | directory_open_flags()) };
        if fd < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, true));
        }
        let handle = Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        };
        handle.validate_owned_private(subject)?;
        Ok(handle)
    }

    /// Open a child directory relative to this descriptor without following a
    /// symlink. `Ok(None)` means the child does not exist.
    pub fn open_directory(&self, name: &str, subject: &'static str) -> MaybeOpened<Self> {
        let raw = cstring(name.as_bytes(), subject)?;
        let fd = unsafe {
            libc::openat(
                self.as_raw_fd(),
                raw.as_ptr(),
                libc::O_RDONLY | directory_open_flags() | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(open_problem(&error, subject, true));
        }
        let handle = Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        };
        handle.validate_owned_private(subject)?;
        Ok(Some(handle))
    }

    /// Open a child directory, creating it with `mode` when it does not exist.
    pub fn open_or_create_directory(
        &self,
        name: &str,
        mode: libc::mode_t,
        subject: &'static str,
    ) -> Result<Self, InstallationProblem> {
        if let Some(handle) = self.open_directory(name, subject)? {
            return Ok(handle);
        }
        let raw = cstring(name.as_bytes(), subject)?;
        if unsafe { libc::mkdirat(self.as_raw_fd(), raw.as_ptr(), mode) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(open_problem(&error, subject, true));
            }
        }
        self.open_directory(name, subject)?.ok_or_else(|| {
            InstallationProblem::new(
                "receipt_io_error",
                format!("{subject} vanished immediately after creation"),
            )
        })
    }

    /// Open a child regular file for reading without following a symlink.
    /// `Ok(None)` means the file does not exist.
    pub fn open_regular_file(&self, name: &str, subject: &'static str) -> MaybeOpened<FileHandle> {
        let raw = cstring(name.as_bytes(), subject)?;
        let fd = unsafe {
            libc::openat(
                self.as_raw_fd(),
                raw.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(open_problem(&error, subject, false));
        }
        let handle = FileHandle {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        };
        handle.validate_regular(subject)?;
        handle.validate_owned_private(subject)?;
        Ok(Some(handle))
    }

    /// Exclusively create a child regular file. Fails closed when the name is
    /// already taken, so a pre-placed or attacker-controlled file is never
    /// overwritten.
    pub fn create_exclusive_file(
        &self,
        name: &str,
        mode: libc::mode_t,
        subject: &'static str,
    ) -> Result<FileHandle, InstallationProblem> {
        let raw = cstring(name.as_bytes(), subject)?;
        let fd = unsafe {
            libc::openat(
                self.as_raw_fd(),
                raw.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                libc::c_uint::from(mode),
            )
        };
        if fd < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        Ok(FileHandle {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Open a child regular file for reading and writing, creating it when it
    /// does not exist. Never follows a symlink: a pre-placed symlink at the
    /// name fails the open rather than redirecting it.
    pub fn open_or_create_file(
        &self,
        name: &str,
        mode: libc::mode_t,
        subject: &'static str,
    ) -> Result<FileHandle, InstallationProblem> {
        let raw = cstring(name.as_bytes(), subject)?;
        let fd = unsafe {
            libc::openat(
                self.as_raw_fd(),
                raw.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                libc::c_uint::from(mode),
            )
        };
        if fd < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        let handle = FileHandle {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        };
        handle.validate_regular(subject)?;
        handle.validate_owned_private(subject)?;
        Ok(handle)
    }

    /// Create a child directory, failing when the name already exists.
    pub fn create_directory(
        &self,
        name: &str,
        mode: libc::mode_t,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let raw = cstring(name.as_bytes(), subject)?;
        if unsafe { libc::mkdirat(self.as_raw_fd(), raw.as_ptr(), mode) } < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, true));
        }
        Ok(())
    }

    /// `symlinkat` a child pointing at `target`.
    pub fn create_symlink(
        &self,
        name: &str,
        target: &str,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let raw_name = cstring(name.as_bytes(), subject)?;
        let raw_target = cstring(target.as_bytes(), subject)?;
        if unsafe { libc::symlinkat(raw_target.as_ptr(), self.as_raw_fd(), raw_name.as_ptr()) } < 0
        {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        Ok(())
    }

    /// Read a child symlink's target. `Ok(None)` means the child does not exist.
    pub fn read_symlink(&self, name: &str, subject: &'static str) -> MaybeOpened<String> {
        let raw = cstring(name.as_bytes(), subject)?;
        let mut buffer = vec![0_u8; 4096];
        let written = unsafe {
            libc::readlinkat(
                self.as_raw_fd(),
                raw.as_ptr(),
                buffer.as_mut_ptr().cast::<libc::c_char>(),
                buffer.len(),
            )
        };
        if written < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            if error.raw_os_error() == Some(libc::EINVAL) {
                // The entry exists but is not a symlink.
                return Err(InstallationProblem::new(
                    "installation_entry_not_symlink",
                    format!("{subject} exists and is not a symbolic link"),
                ));
            }
            return Err(open_problem(&error, subject, false));
        }
        let written = usize::try_from(written).unwrap_or(0);
        if written >= buffer.len() {
            return Err(InstallationProblem::new(
                "installation_symlink_too_long",
                format!("{subject} target exceeds the supported length"),
            ));
        }
        buffer.truncate(written);
        String::from_utf8(buffer).map(Some).map_err(|_| {
            InstallationProblem::new(
                "installation_symlink_not_utf8",
                format!("{subject} target is not valid UTF-8"),
            )
        })
    }

    /// Rename `from` in this directory to `to` in `destination`.
    pub fn rename_into(
        &self,
        from: &str,
        destination: &Self,
        to: &str,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let raw_from = cstring(from.as_bytes(), subject)?;
        let raw_to = cstring(to.as_bytes(), subject)?;
        if unsafe {
            libc::renameat(
                self.as_raw_fd(),
                raw_from.as_ptr(),
                destination.as_raw_fd(),
                raw_to.as_ptr(),
            )
        } < 0
        {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        Ok(())
    }

    /// Unlink a child file.
    pub fn unlink_file(
        &self,
        name: &str,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let raw = cstring(name.as_bytes(), subject)?;
        if unsafe { libc::unlinkat(self.as_raw_fd(), raw.as_ptr(), 0) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(open_problem(&error, subject, false));
        }
        Ok(())
    }

    /// Unlink a child directory.
    pub fn unlink_directory(
        &self,
        name: &str,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let raw = cstring(name.as_bytes(), subject)?;
        if unsafe { libc::unlinkat(self.as_raw_fd(), raw.as_ptr(), libc::AT_REMOVEDIR) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(open_problem(&error, subject, true));
        }
        Ok(())
    }

    /// List entry names directly under this descriptor.
    pub fn entry_names(&self, subject: &'static str) -> Result<Vec<String>, InstallationProblem> {
        let duplicate = unsafe { libc::dup(self.as_raw_fd()) };
        if duplicate < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, true));
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            let error = io::Error::last_os_error();
            unsafe { libc::close(duplicate) };
            return Err(open_problem(&error, subject, true));
        }
        let mut names = Vec::new();
        loop {
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                break;
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            let Ok(name) = name.to_str() else { continue };
            if name == "." || name == ".." {
                continue;
            }
            names.push(name.to_string());
        }
        unsafe { libc::closedir(stream) };
        Ok(names)
    }

    /// `fsync` this directory, committing its directory entries.
    pub fn sync(&self, subject: &'static str) -> Result<(), InstallationProblem> {
        if unsafe { libc::fsync(self.as_raw_fd()) } < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, true));
        }
        Ok(())
    }

    fn validate_owned_private(&self, subject: &'static str) -> Result<(), InstallationProblem> {
        let status = fstat(self.as_raw_fd(), subject)?;
        if status.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(InstallationProblem::new(
                "unsafe_receipt_directory",
                format!("installation {subject} is not a regular directory"),
            ));
        }
        validate_owned_private_status(&status, subject)
    }
}

impl FileHandle {
    /// Read the whole file, refusing anything larger than `limit`.
    pub fn read_bounded(
        &self,
        limit: u64,
        subject: &'static str,
    ) -> Result<Vec<u8>, InstallationProblem> {
        let status = fstat(self.as_raw_fd(), subject)?;
        let length = u64::try_from(status.st_size).unwrap_or(u64::MAX);
        if length > limit {
            return Err(InstallationProblem::new(
                "receipt_too_large",
                format!("installation {subject} exceeds the size limit"),
            ));
        }
        let mut buffer = vec![0_u8; usize::try_from(length).unwrap_or(0)];
        let mut filled = 0_usize;
        while filled < buffer.len() {
            let read = unsafe {
                libc::read(
                    self.as_raw_fd(),
                    buffer[filled..].as_mut_ptr().cast::<libc::c_void>(),
                    buffer.len() - filled,
                )
            };
            if read < 0 {
                return Err(open_problem(&io::Error::last_os_error(), subject, false));
            }
            if read == 0 {
                break;
            }
            filled += usize::try_from(read).unwrap_or(0);
        }
        buffer.truncate(filled);
        Ok(buffer)
    }

    /// Write the whole buffer to this descriptor.
    pub fn write_all(
        &self,
        bytes: &[u8],
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        let mut written = 0_usize;
        while written < bytes.len() {
            let count = unsafe {
                libc::write(
                    self.as_raw_fd(),
                    bytes[written..].as_ptr().cast::<libc::c_void>(),
                    bytes.len() - written,
                )
            };
            if count < 0 {
                return Err(open_problem(&io::Error::last_os_error(), subject, false));
            }
            written += usize::try_from(count).unwrap_or(0);
        }
        Ok(())
    }

    /// Set this file's permission bits explicitly, so the result does not
    /// depend on the caller's umask.
    pub fn set_mode(
        &self,
        mode: libc::mode_t,
        subject: &'static str,
    ) -> Result<(), InstallationProblem> {
        if unsafe { libc::fchmod(self.as_raw_fd(), mode) } < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        Ok(())
    }

    /// `fsync` this file, committing its contents.
    pub fn sync(&self, subject: &'static str) -> Result<(), InstallationProblem> {
        if unsafe { libc::fsync(self.as_raw_fd()) } < 0 {
            return Err(open_problem(&io::Error::last_os_error(), subject, false));
        }
        Ok(())
    }

    /// Owner uid, size, and permission bits of the opened descriptor.
    pub fn facts(&self, subject: &'static str) -> Result<FileFacts, InstallationProblem> {
        let status = fstat(self.as_raw_fd(), subject)?;
        Ok(FileFacts {
            uid: status.st_uid,
            mode: u32::from(status.st_mode) & 0o7777,
            size: u64::try_from(status.st_size).unwrap_or(0),
        })
    }

    fn validate_regular(&self, subject: &'static str) -> Result<(), InstallationProblem> {
        let status = fstat(self.as_raw_fd(), subject)?;
        if status.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(InstallationProblem::new(
                "receipt_not_regular_file",
                format!("installation {subject} must be a regular file"),
            ));
        }
        Ok(())
    }

    fn validate_owned_private(&self, subject: &'static str) -> Result<(), InstallationProblem> {
        let status = fstat(self.as_raw_fd(), subject)?;
        validate_owned_private_status(&status, subject)
    }

    /// Consume this handle, yielding the raw descriptor for `flock` retention.
    #[must_use]
    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }
}

/// Ownership, permission, and size facts read from an open descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileFacts {
    pub uid: u32,
    pub mode: u32,
    pub size: u64,
}

/// A random, per-attempt temporary-name suffix.
///
/// Unique per attempt rather than fixed: a crash mid-write must leave at most
/// one stale temp, and must never make every later re-run abort on a name that
/// `O_EXCL` refuses to reuse.
#[must_use]
pub fn random_suffix() -> String {
    let mut bytes = [0_u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        // Uniqueness, not unpredictability, is what the suffix is for; pid plus
        // a monotonic counter still gives a distinct name per attempt.
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let fallback = u64::from(std::process::id())
            .rotate_left(32)
            .wrapping_add(SEQUENCE.fetch_add(1, Ordering::Relaxed));
        bytes = fallback.to_le_bytes();
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The effective uid of the calling process.
#[must_use]
pub fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

fn validate_owned_private_status(
    status: &libc::stat,
    subject: &'static str,
) -> Result<(), InstallationProblem> {
    if status.st_uid != effective_uid() {
        return Err(InstallationProblem::new(
            "receipt_wrong_owner",
            format!("installation {subject} is not owned by the current user"),
        ));
    }
    if u32::from(status.st_mode) & 0o002 != 0 {
        return Err(InstallationProblem::new(
            "receipt_world_writable",
            format!("installation {subject} must not be world-writable"),
        ));
    }
    Ok(())
}

fn fstat(fd: RawFd, subject: &'static str) -> Result<libc::stat, InstallationProblem> {
    let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
    if unsafe { libc::fstat(fd, &raw mut status) } < 0 {
        return Err(open_problem(&io::Error::last_os_error(), subject, false));
    }
    Ok(status)
}

const fn directory_open_flags() -> libc::c_int {
    libc::O_DIRECTORY | libc::O_CLOEXEC
}

fn cstring(bytes: &[u8], subject: &'static str) -> Result<CString, InstallationProblem> {
    CString::new(bytes).map_err(|_| {
        InstallationProblem::new(
            "receipt_io_error",
            format!("installation {subject} path contains an interior NUL"),
        )
    })
}

/// Map an `open`-family failure onto a stable diagnostic kind.
///
/// `ELOOP` is the whole point of `O_NOFOLLOW`: it means the name resolved to a
/// symbolic link and the open refused to follow it, rather than a check having
/// noticed one after the fact.
fn open_problem(error: &io::Error, subject: &'static str, directory: bool) -> InstallationProblem {
    match error.raw_os_error() {
        Some(libc::ELOOP) | Some(libc::EMLINK) if directory => InstallationProblem::new(
            "unsafe_receipt_directory",
            format!("installation {subject} must not be a symbolic link"),
        ),
        Some(libc::ELOOP) | Some(libc::EMLINK) => InstallationProblem::new(
            "receipt_symlink",
            format!("installation {subject} must not be a symbolic link"),
        ),
        Some(libc::ENOTDIR) => InstallationProblem::new(
            "unsafe_receipt_directory",
            format!("installation {subject} is not a regular directory"),
        ),
        Some(libc::EISDIR) => InstallationProblem::new(
            "receipt_not_regular_file",
            format!("installation {subject} must be a regular file"),
        ),
        Some(libc::EACCES) | Some(libc::EPERM) => InstallationProblem::new(
            "receipt_permission_denied",
            format!("installation {subject} could not be accessed"),
        ),
        Some(libc::EEXIST) => InstallationProblem::new(
            "installation_entry_exists",
            format!("installation {subject} already exists"),
        ),
        _ => InstallationProblem::new(
            "receipt_io_error",
            format!("installation {subject} could not be accessed: {error}"),
        ),
    }
}
