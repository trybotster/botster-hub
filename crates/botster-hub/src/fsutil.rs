//! Small file helpers with the modes of HC RT-2 and the durable write of EN-7 ("committed": `fsync` before the answer).

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

/// Creates `path` with mode 0700 if it is missing. Its parent must exist. An existing entry is left as it is.
///
/// # Errors
/// The error of `mkdir`, except "already exists".
pub fn ensure_dir(path: &Path) -> io::Result<()> {
    match DirBuilder::new().mode(0o700).create(path) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        other => other,
    }
}

/// Syncs a directory, so that its entries are durable.
///
/// # Errors
/// The error of `open` or `fsync`.
pub fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Writes `bytes` to `path` atomically and durably: a temporary file `tmp` in the same directory, `fsync`, `rename` over
/// `path`, then `fsync` of the directory. The file has mode 0600.
///
/// # Errors
/// [`WriteError::NotWritten`] when the step before the `rename` failed: `path` did not change, and `tmp` is removed.
/// [`WriteError::Unknown`] when the `rename` or the directory `fsync` failed: `path` holds the old or the new content.
pub fn write_atomic(path: &Path, tmp: &Path, bytes: &[u8]) -> Result<(), WriteError> {
    let staged = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(tmp)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    if let Err(error) = staged {
        let _ = fs::remove_file(tmp);
        return Err(WriteError::NotWritten(error));
    }
    fs::rename(tmp, path).map_err(WriteError::Unknown)?;
    let dir = path.parent().unwrap_or(Path::new("."));
    sync_dir(dir).map_err(WriteError::Unknown)
}

/// How a durable write failed.
#[derive(Debug)]
pub enum WriteError {
    /// The target did not change.
    NotWritten(io::Error),
    /// The target holds the old or the new content; the caller cannot tell which.
    Unknown(io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn ensure_dir_creates_0700_and_keeps_an_existing_one() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("d");
        ensure_dir(&dir).unwrap();
        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o750)).unwrap();
        ensure_dir(&dir).unwrap();
        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o750
        );
        assert!(ensure_dir(&tmp.path().join("missing/d")).is_err());
    }

    #[test]
    fn write_atomic_replaces_with_mode_0600() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("f");
        let staging = tmp.path().join(".tmp-f");
        write_atomic(&path, &staging, b"one").unwrap();
        write_atomic(&path, &staging, b"two").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(!staging.exists());
    }

    /// A failure before the `rename` leaves the target unchanged and removes nothing else.
    #[test]
    fn write_atomic_not_written_keeps_the_target() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("f");
        write_atomic(&path, &tmp.path().join(".tmp-f"), b"one").unwrap();
        // The temporary file's directory does not exist, so the open fails.
        let result = write_atomic(&path, &tmp.path().join("missing/.tmp-f"), b"two");
        assert!(matches!(result, Err(WriteError::NotWritten(_))));
        assert_eq!(fs::read(&path).unwrap(), b"one");
    }

    /// A failure at the `rename` is `Unknown`.
    #[test]
    fn write_atomic_rename_failure_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        // The target is a non-empty directory, so the rename of a file over it fails.
        let path = tmp.path().join("t");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("x"), b"").unwrap();
        let staging = tmp.path().join(".tmp-t");
        let result = write_atomic(&path, &staging, b"two");
        assert!(matches!(result, Err(WriteError::Unknown(_))));
    }
}
