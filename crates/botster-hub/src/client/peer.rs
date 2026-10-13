//! The peer uid of an accepted connection (HC TB-L3): `SO_PEERCRED` on Linux, `getpeereid` on macOS.
//!
//! The Hub reads it before any frame. A uid other than its own is closed with `auth_failed`.

use std::io;
use std::os::fd::AsFd;

/// The effective uid of the process at the other end of a connected Unix socket.
#[cfg(target_os = "linux")]
pub fn peer_uid(socket: impl AsFd) -> io::Result<u32> {
    Ok(rustix::net::sockopt::socket_peercred(socket)?.uid.as_raw())
}

/// The effective uid of the process at the other end of a connected Unix socket.
#[cfg(target_os = "macos")]
pub fn peer_uid(socket: impl AsFd) -> io::Result<u32> {
    let (uid, _gid) = nix::unistd::getpeereid(socket)?;
    Ok(uid.as_raw())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::{UnixListener, UnixStream};

    fn me() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    #[test]
    fn an_accepted_connection_reports_the_connecting_uid() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let _client = UnixStream::connect(&socket).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        assert_eq!(peer_uid(&accepted).unwrap(), me());
    }

    #[test]
    fn a_socket_that_is_not_connected_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(dir.path().join("hub.sock")).unwrap();
        assert!(peer_uid(&listener).is_err());
    }
}
