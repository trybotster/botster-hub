//! The client's socket-owner check (HC RT-5): before it connects, a client checks that the socket and its directory are
//! owned by its own uid, and refuses otherwise. So another local user cannot serve a fake Hub.
//!
//! Each path is read with `lstat`, so a symbolic link is judged as itself, never as its target.

use std::fmt;
use std::fs::Metadata;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

/// Why a socket path is not this user's.
#[derive(Debug)]
pub enum OwnerError {
    /// `lstat` failed (for example, no Hub is running and the socket is missing).
    Unreadable { path: PathBuf, error: io::Error },
    /// The path is not of the kind it must be: a socket, or a directory.
    WrongKind {
        path: PathBuf,
        expected: &'static str,
    },
    /// Another uid owns the path.
    ForeignOwner { path: PathBuf, owner: u32, uid: u32 },
    /// The socket path has no parent directory.
    NoDirectory { path: PathBuf },
}

impl fmt::Display for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OwnerError::Unreadable { path, error } => {
                write!(f, "cannot read {}: {error}", path.display())
            }
            OwnerError::WrongKind { path, expected } => {
                write!(f, "{} is not a {expected}", path.display())
            }
            OwnerError::ForeignOwner { path, owner, uid } => write!(
                f,
                "{} belongs to uid {owner}, not to this user (uid {uid}); refusing to connect",
                path.display()
            ),
            OwnerError::NoDirectory { path } => write!(f, "{} has no directory", path.display()),
        }
    }
}

impl std::error::Error for OwnerError {}

fn lstat(path: &Path) -> Result<Metadata, OwnerError> {
    std::fs::symlink_metadata(path).map_err(|error| OwnerError::Unreadable {
        path: path.to_path_buf(),
        error,
    })
}

fn owned(path: &Path, metadata: &Metadata, uid: u32) -> Result<(), OwnerError> {
    if metadata.uid() == uid {
        Ok(())
    } else {
        Err(OwnerError::ForeignOwner {
            path: path.to_path_buf(),
            owner: metadata.uid(),
            uid,
        })
    }
}

/// Checks that `socket` is a socket and its directory a directory, both owned by `uid`.
pub fn check_owner(socket: &Path, uid: u32) -> Result<(), OwnerError> {
    let directory = socket
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .ok_or_else(|| OwnerError::NoDirectory {
            path: socket.to_path_buf(),
        })?;
    let dir = lstat(directory)?;
    if !dir.file_type().is_dir() {
        return Err(OwnerError::WrongKind {
            path: directory.to_path_buf(),
            expected: "directory",
        });
    }
    owned(directory, &dir, uid)?;
    let sock = lstat(socket)?;
    if !sock.file_type().is_socket() {
        return Err(OwnerError::WrongKind {
            path: socket.to_path_buf(),
            expected: "socket",
        });
    }
    owned(socket, &sock, uid)
}

/// [`check_owner`] for this process's effective uid.
pub fn check_own_socket(socket: &Path) -> Result<(), OwnerError> {
    check_owner(socket, rustix::process::geteuid().as_raw())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn me() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    #[test]
    fn an_own_socket_in_an_own_directory_passes() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        check_owner(&socket, me()).unwrap();
    }

    #[test]
    fn another_uid_is_refused_for_the_socket_or_its_directory() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        // The test cannot chown to another uid, so it asks as another uid: the owner is then foreign.
        let other = me().wrapping_add(1);
        match check_owner(&socket, other) {
            Err(OwnerError::ForeignOwner { path, owner, uid }) => {
                assert_eq!(path, dir.path(), "the directory is checked first");
                assert_eq!((owner, uid), (me(), other));
            }
            other => panic!("expected ForeignOwner, got {other:?}"),
        }
    }

    #[test]
    fn a_link_or_a_plain_file_is_not_a_socket() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.sock");
        let _listener = UnixListener::bind(&real).unwrap();
        let link = dir.path().join("hub.sock");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            check_owner(&link, me()),
            Err(OwnerError::WrongKind {
                expected: "socket",
                ..
            })
        ));
        let file = dir.path().join("file.sock");
        std::fs::write(&file, b"").unwrap();
        assert!(matches!(
            check_owner(&file, me()),
            Err(OwnerError::WrongKind {
                expected: "socket",
                ..
            })
        ));
    }

    #[test]
    fn a_linked_directory_is_not_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let _listener = UnixListener::bind(real.join("hub.sock")).unwrap();
        let link = dir.path().join("linked");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            check_owner(&link.join("hub.sock"), me()),
            Err(OwnerError::WrongKind {
                expected: "directory",
                ..
            })
        ));
    }

    #[test]
    fn a_missing_socket_is_unreadable_and_a_bare_name_has_no_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            check_owner(&dir.path().join("hub.sock"), me()),
            Err(OwnerError::Unreadable { .. })
        ));
        assert!(matches!(
            check_owner(Path::new("hub.sock"), me()),
            Err(OwnerError::NoDirectory { .. })
        ));
    }
}
