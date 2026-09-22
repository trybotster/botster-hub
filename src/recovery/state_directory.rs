//! Draft state-directory ownership. Startup and host workers own filesystem calls.
//!
//! The directory itself is the lock object. No lock pathname can be unlinked
//! and recreated while another owner retains the original lock.
//! This module is not connected to persistence or runtime startup yet.

use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Clones authorize work on the same loaded document, not another state snapshot.
#[derive(Debug, Clone)]
pub(crate) struct StateDirectoryOwnership(Arc<DirectoryLock>);

#[derive(Debug)]
struct DirectoryLock {
    directory: PathBuf,
    file: File,
    device: u64,
    inode: u64,
}

#[derive(Debug)]
pub(crate) enum StateDirectoryError {
    Io(io::Error),
    DocumentRead(io::Error),
    Owned(PathBuf),
    Replaced(PathBuf),
    InvalidTemporaryFile,
}

/// Every variant means rename completed. None authorizes a not-committed rollback.
/// Synced means file sync_all and directory fsync succeeded. On macOS, directory
/// fsync does not force the drive cache. Crash durability still requires verification.
#[derive(Debug)]
#[must_use]
pub(crate) enum StateDocumentCommit {
    Synced,
    SyncedDirectoryChanged(StateDirectoryError),
    RenamedSyncFailed(io::Error),
}

#[cfg(test)]
#[derive(Default)]
struct CommitTestFault<'a> {
    after_rename: Option<&'a dyn Fn()>,
    directory_sync_error: bool,
}

impl fmt::Display for StateDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "state directory ownership failed: {error}"),
            Self::DocumentRead(error) => write!(formatter, "state document read failed: {error}"),
            Self::Owned(directory) => write!(
                formatter,
                "state directory {} already has a writer",
                directory.display()
            ),
            Self::Replaced(directory) => write!(
                formatter,
                "state directory {} changed while its writer was active",
                directory.display()
            ),
            Self::InvalidTemporaryFile => formatter
                .write_str("state temporary file is not an exclusively linked regular file"),
        }
    }
}

impl std::error::Error for StateDirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) | Self::DocumentRead(error) => Some(error),
            Self::Owned(_) | Self::Replaced(_) | Self::InvalidTemporaryFile => None,
        }
    }
}

impl StateDirectoryOwnership {
    /// Acquire before loading a writable document. Contention never waits.
    pub(crate) fn acquire(directory: &Path) -> Result<Self, StateDirectoryError> {
        fs::create_dir_all(directory).map_err(StateDirectoryError::Io)?;
        let directory = fs::canonicalize(directory).map_err(StateDirectoryError::Io)?;
        let file = File::open(&directory).map_err(StateDirectoryError::Io)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(StateDirectoryError::Owned(directory)),
            Err(TryLockError::Error(error)) => return Err(StateDirectoryError::Io(error)),
        }
        let metadata = file.metadata().map_err(StateDirectoryError::Io)?;
        if !metadata.is_dir() {
            return Err(StateDirectoryError::Replaced(directory));
        }
        let ownership = Self(Arc::new(DirectoryLock {
            directory,
            file,
            device: metadata.dev(),
            inode: metadata.ino(),
        }));
        ownership.ensure_current()?;
        Ok(ownership)
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.0.directory
    }

    pub(crate) fn identity(&self) -> (u64, u64) {
        (self.0.device, self.0.inode)
    }

    /// Read the document relative to the retained directory descriptor.
    /// The draft requires the separately reviewed rustix dependency handoff.
    pub(crate) fn read_document(&self) -> Result<Vec<u8>, StateDirectoryError> {
        use rustix::fs::{Mode, OFlags, openat};

        self.ensure_current()?;
        let descriptor = openat(
            &self.0.file,
            "hub-state.json",
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|error| StateDirectoryError::DocumentRead(error.into()))?;
        let mut file = File::from(descriptor);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(StateDirectoryError::DocumentRead)?;
        self.ensure_current()?;
        Ok(bytes)
    }

    /// Commit to the locked directory even if its pathname changes during I/O.
    /// Post-rename faults return a distinct published outcome, never a precommit error.
    pub(crate) fn write_document(
        &self,
        bytes: &[u8],
    ) -> Result<StateDocumentCommit, StateDirectoryError> {
        self.write_document_inner(
            bytes,
            #[cfg(test)]
            CommitTestFault::default(),
        )
    }

    fn write_document_inner(
        &self,
        bytes: &[u8],
        #[cfg(test)] fault: CommitTestFault<'_>,
    ) -> Result<StateDocumentCommit, StateDirectoryError> {
        use rustix::fs::{Mode, OFlags, openat, renameat};

        self.ensure_current()?;
        let descriptor = openat(
            &self.0.file,
            "hub-state.json.tmp",
            OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH | Mode::WOTH,
        )
        .map_err(|error| StateDirectoryError::Io(error.into()))?;
        let mut file = File::from(descriptor);
        let metadata = file.metadata().map_err(StateDirectoryError::Io)?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(StateDirectoryError::InvalidTemporaryFile);
        }
        file.set_len(0).map_err(StateDirectoryError::Io)?;
        file.write_all(bytes).map_err(StateDirectoryError::Io)?;
        file.sync_all().map_err(StateDirectoryError::Io)?;
        renameat(
            &self.0.file,
            "hub-state.json.tmp",
            &self.0.file,
            "hub-state.json",
        )
        .map_err(|error| StateDirectoryError::Io(error.into()))?;
        #[cfg(test)]
        if let Some(after_rename) = fault.after_rename {
            after_rename();
        }
        #[cfg(test)]
        if fault.directory_sync_error {
            return Ok(StateDocumentCommit::RenamedSyncFailed(io::Error::other(
                "injected directory sync failure",
            )));
        }
        if let Err(error) = rustix::fs::fsync(&self.0.file) {
            return Ok(StateDocumentCommit::RenamedSyncFailed(error.into()));
        }
        Ok(match self.ensure_current() {
            Ok(()) => StateDocumentCommit::Synced,
            Err(error) => StateDocumentCommit::SyncedDirectoryChanged(error),
        })
    }

    /// Reject replacement detected before a filesystem operation.
    /// This check does not make later path-based operations atomic with rename.
    pub(crate) fn ensure_current(&self) -> Result<(), StateDirectoryError> {
        let retained = self.0.file.metadata().map_err(StateDirectoryError::Io)?;
        let current = fs::metadata(&self.0.directory).map_err(StateDirectoryError::Io)?;
        if !current.is_dir()
            || current.dev() != self.0.device
            || current.ino() != self.0.inode
            || retained.dev() != self.0.device
            || retained.ino() != self.0.inode
        {
            return Err(StateDirectoryError::Replaced(self.0.directory.clone()));
        }
        Ok(())
    }
}

// File closes only after the last clone drops. No explicit unlock, unlink,
// filesystem synchronization, or wait runs in a custom destructor.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "botster-state-owner-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("isolated fixture");
            Self(path)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn aliases_contend_and_the_last_clone_retains_ownership() {
        let fixture = Fixture::new();
        let directory = fixture.0.join("data");
        let owner = StateDirectoryOwnership::acquire(&directory).expect("first owner");
        let alias = fixture.0.join("alias");
        std::os::unix::fs::symlink(&directory, &alias).expect("directory alias");
        let retained = owner.clone();
        drop(owner);
        assert!(matches!(
            StateDirectoryOwnership::acquire(&alias),
            Err(StateDirectoryError::Owned(_))
        ));
        drop(retained);
        let reopened = StateDirectoryOwnership::acquire(&alias).expect("released owner");
        assert_eq!(reopened.directory(), fs::canonicalize(directory).unwrap());
    }

    #[test]
    fn commits_use_the_existing_document_name_and_reload_from_disk() {
        let fixture = Fixture::new();
        let owner = StateDirectoryOwnership::acquire(&fixture.0).expect("owner");
        assert!(matches!(
            owner.write_document(b"first"),
            Ok(StateDocumentCommit::Synced)
        ));
        assert!(matches!(
            owner.write_document(b"second"),
            Ok(StateDocumentCommit::Synced)
        ));
        assert_eq!(owner.read_document().unwrap(), b"second");
        assert!(!fixture.0.join("hub-state.json.tmp").exists());
        drop(owner);
        let reopened = StateDirectoryOwnership::acquire(&fixture.0).expect("new owner");
        assert_eq!(reopened.read_document().unwrap(), b"second");
    }

    #[test]
    fn replaced_directory_never_receives_the_old_owners_write() {
        let fixture = Fixture::new();
        let directory = fixture.0.join("data");
        let retained_path = fixture.0.join("retained");
        let owner = StateDirectoryOwnership::acquire(&directory).expect("owner");
        assert!(matches!(
            owner.write_document(b"original"),
            Ok(StateDocumentCommit::Synced)
        ));
        fs::rename(&directory, &retained_path).expect("move original directory");
        fs::create_dir(&directory).expect("replacement directory");
        fs::write(directory.join("hub-state.json"), b"unrelated").unwrap();
        assert!(matches!(
            owner.write_document(b"stale"),
            Err(StateDirectoryError::Replaced(_))
        ));
        assert_eq!(
            fs::read(directory.join("hub-state.json")).unwrap(),
            b"unrelated"
        );
        assert_eq!(
            fs::read(retained_path.join("hub-state.json")).unwrap(),
            b"original"
        );
        assert!(matches!(
            StateDirectoryOwnership::acquire(&retained_path),
            Err(StateDirectoryError::Owned(_))
        ));
    }

    #[test]
    fn temporary_symlink_does_not_truncate_an_unrelated_file() {
        let fixture = Fixture::new();
        let owner = StateDirectoryOwnership::acquire(&fixture.0.join("data")).unwrap();
        let unrelated = fixture.0.join("unrelated");
        fs::write(&unrelated, b"retained").unwrap();
        std::os::unix::fs::symlink(&unrelated, owner.directory().join("hub-state.json.tmp"))
            .unwrap();
        assert!(owner.write_document(b"replacement").is_err());
        assert_eq!(fs::read(unrelated).unwrap(), b"retained");
    }

    #[test]
    fn temporary_hard_link_does_not_truncate_an_unrelated_file() {
        let fixture = Fixture::new();
        let owner = StateDirectoryOwnership::acquire(&fixture.0.join("data")).unwrap();
        let unrelated = fixture.0.join("unrelated");
        fs::write(&unrelated, b"retained").unwrap();
        fs::hard_link(&unrelated, owner.directory().join("hub-state.json.tmp")).unwrap();
        assert!(matches!(
            owner.write_document(b"replacement"),
            Err(StateDirectoryError::InvalidTemporaryFile)
        ));
        assert_eq!(fs::read(unrelated).unwrap(), b"retained");
    }

    #[test]
    fn replacement_after_rename_reports_a_synced_commit_to_the_retained_directory() {
        let fixture = Fixture::new();
        let directory = fixture.0.join("data");
        let retained_path = fixture.0.join("retained");
        let owner = StateDirectoryOwnership::acquire(&directory).unwrap();
        let replace = || {
            fs::rename(&directory, &retained_path).unwrap();
            fs::create_dir(&directory).unwrap();
            fs::write(directory.join("hub-state.json"), b"unrelated").unwrap();
        };
        let outcome = owner.write_document_inner(
            b"committed",
            CommitTestFault {
                after_rename: Some(&replace),
                directory_sync_error: false,
            },
        );
        assert!(matches!(
            outcome,
            Ok(StateDocumentCommit::SyncedDirectoryChanged(
                StateDirectoryError::Replaced(_)
            ))
        ));
        assert_eq!(
            fs::read(retained_path.join("hub-state.json")).unwrap(),
            b"committed"
        );
        assert_eq!(
            fs::read(directory.join("hub-state.json")).unwrap(),
            b"unrelated"
        );
    }

    #[test]
    fn sync_failure_after_rename_never_reports_a_precommit_error() {
        let fixture = Fixture::new();
        let owner = StateDirectoryOwnership::acquire(&fixture.0).unwrap();
        let outcome = owner.write_document_inner(
            b"published",
            CommitTestFault {
                after_rename: None,
                directory_sync_error: true,
            },
        );
        assert!(matches!(
            outcome,
            Ok(StateDocumentCommit::RenamedSyncFailed(_))
        ));
        assert_eq!(
            fs::read(fixture.0.join("hub-state.json")).unwrap(),
            b"published"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_directory_fsync_completes_after_document_rename() {
        let fixture = Fixture::new();
        let owner = StateDirectoryOwnership::acquire(&fixture.0).unwrap();
        assert!(matches!(
            owner.write_document(b"synced"),
            Ok(StateDocumentCommit::Synced)
        ));
    }

    #[test]
    fn missing_document_is_distinct_from_a_missing_directory() {
        let fixture = Fixture::new();
        let directory = fixture.0.join("data");
        let owner = StateDirectoryOwnership::acquire(&directory).unwrap();
        assert!(matches!(
            owner.read_document(),
            Err(StateDirectoryError::DocumentRead(error)) if error.kind() == io::ErrorKind::NotFound
        ));
        fs::rename(&directory, fixture.0.join("moved")).unwrap();
        assert!(matches!(
            owner.read_document(),
            Err(StateDirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
    }
}
