//! Socket discovery (HC TB-L1, with HC Amendment 2's `<app>` = `botster`; CLI-RT3).
//!
//! In order: `BOTSTER_HUB_SOCKET`; `$BOTSTER_CONFIG_DIR/hub.sock`; `<runtime dir>/botster/hub.sock`, where the runtime
//! directory is `$XDG_RUNTIME_DIR`, or `/tmp/botster-<uid>` when it is unset. A path over 103 bytes is refused with a
//! message that names the limit; it is never shortened.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;

/// The portable limit of a Unix socket address, in bytes (TB-L1).
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

/// The `<app>` directory under the runtime directory (HC Amendment 2, TB-L1 and RT-1).
pub const APP: &str = "botster";

/// The socket file name.
pub const SOCKET_NAME: &str = "hub.sock";

/// The inputs of discovery. An empty variable counts as unset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Env {
    pub hub_socket: Option<OsString>,
    pub config_dir: Option<OsString>,
    pub xdg_runtime_dir: Option<OsString>,
    /// The effective uid, for `/tmp/botster-<uid>`.
    pub uid: u32,
}

impl Env {
    /// The process's own environment and effective uid.
    pub fn from_process() -> Env {
        Env {
            hub_socket: std::env::var_os("BOTSTER_HUB_SOCKET"),
            config_dir: std::env::var_os("BOTSTER_CONFIG_DIR"),
            xdg_runtime_dir: std::env::var_os("XDG_RUNTIME_DIR"),
            uid: rustix::process::geteuid().as_raw(),
        }
    }
}

/// Discovery found a path that no socket address can hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTooLong {
    pub path: PathBuf,
}

impl fmt::Display for PathTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the Hub socket path {} is {} bytes; the limit is {MAX_SOCKET_PATH_BYTES} bytes",
            self.path.display(),
            self.path.as_os_str().len()
        )
    }
}

impl std::error::Error for PathTooLong {}

fn set(value: &Option<OsString>) -> Option<&OsStr> {
    value.as_deref().filter(|v| !v.is_empty())
}

/// The Hub socket path, by the TB-L1 order.
pub fn socket_path(env: &Env) -> Result<PathBuf, PathTooLong> {
    let path = if let Some(socket) = set(&env.hub_socket) {
        PathBuf::from(socket)
    } else if let Some(dir) = set(&env.config_dir) {
        PathBuf::from(dir).join(SOCKET_NAME)
    } else {
        let runtime = match set(&env.xdg_runtime_dir) {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(format!("/tmp/botster-{}", env.uid)),
        };
        runtime.join(APP).join(SOCKET_NAME)
    };
    if path.as_os_str().len() > MAX_SOCKET_PATH_BYTES {
        return Err(PathTooLong { path });
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> Env {
        Env {
            hub_socket: Some("/s/hub-env.sock".into()),
            config_dir: Some("/c".into()),
            xdg_runtime_dir: Some("/run/user/7".into()),
            uid: 7,
        }
    }

    #[test]
    fn discovery_takes_the_first_set_source_in_tb_l1_order() {
        let mut e = env();
        assert_eq!(socket_path(&e).unwrap(), PathBuf::from("/s/hub-env.sock"));
        e.hub_socket = None;
        assert_eq!(socket_path(&e).unwrap(), PathBuf::from("/c/hub.sock"));
        e.config_dir = None;
        assert_eq!(
            socket_path(&e).unwrap(),
            PathBuf::from("/run/user/7/botster/hub.sock")
        );
        e.xdg_runtime_dir = None;
        assert_eq!(
            socket_path(&e).unwrap(),
            PathBuf::from("/tmp/botster-7/botster/hub.sock")
        );
    }

    #[test]
    fn an_empty_variable_counts_as_unset() {
        let e = Env {
            hub_socket: Some("".into()),
            config_dir: Some("".into()),
            xdg_runtime_dir: Some("".into()),
            uid: 42,
        };
        assert_eq!(
            socket_path(&e).unwrap(),
            PathBuf::from("/tmp/botster-42/botster/hub.sock")
        );
    }

    #[test]
    fn a_path_over_103_bytes_is_refused_and_names_the_limit() {
        let at_limit = format!("/{}", "a".repeat(MAX_SOCKET_PATH_BYTES - 1));
        let over = format!("/{}", "a".repeat(MAX_SOCKET_PATH_BYTES));
        let mut e = Env {
            hub_socket: Some(at_limit.clone().into()),
            ..Env::default()
        };
        assert_eq!(socket_path(&e).unwrap(), PathBuf::from(at_limit));
        e.hub_socket = Some(over.clone().into());
        let error = socket_path(&e).unwrap_err();
        assert_eq!(error.path, PathBuf::from(over));
        assert!(
            error.to_string().contains("the limit is 103 bytes"),
            "{error}"
        );
    }
}
