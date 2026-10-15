//! The roots of a Hub (HC RT-1), its socket path (HC TB-L1) and the socket length checks (design section 5a).
//!
//! Everything here is a pure function of [`Env`], so a test sets the environment as a value, not through the process.

use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

/// The app name in every build (HC RT-1).
pub const APP: &str = "botster";

/// The portable socket address limit in path bytes (HC TB-L1).
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

/// The longest data root that the Hub accepts (design section 5a). Core `v1` binds `<data_dir>/w/<InstanceId>`, and the
/// longest `InstanceId` is `"<u64::MAX>-<u64::MAX>"` (41 bytes). With `data_dir = <data root>/core`, the longest socket
/// path is the data root plus `/core/w/` (8 bytes) plus 41 bytes, so the data root holds at most 103 - 49 = 54 bytes.
/// The test `data_root_bound_matches_core` pins the number to Core.
pub const MAX_DATA_ROOT_BYTES: usize = MAX_SOCKET_PATH_BYTES - "/core/w/".len() - 41;

/// The name of Core's data directory under the data root (HC A11 c1).
pub const CORE_DIR: &str = "core";

/// The operating system, for the platform configuration directory (HC RT-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    MacOs,
    Linux,
}

impl Platform {
    /// The platform that this binary was built for.
    pub fn current() -> Platform {
        if cfg!(target_os = "macos") {
            Platform::MacOs
        } else {
            Platform::Linux
        }
    }
}

/// The inputs of the root rules. Each `Option` is an environment variable; `None` means unset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Env {
    pub botster_config_dir: Option<OsString>,
    pub botster_data_dir: Option<OsString>,
    pub botster_hub_socket: Option<OsString>,
    pub xdg_config_home: Option<OsString>,
    pub xdg_runtime_dir: Option<OsString>,
    pub home: PathBuf,
    pub uid: u32,
    pub platform: Platform,
}

impl Env {
    /// Reads the environment of this process. An empty variable counts as unset.
    pub fn from_process() -> Env {
        fn var(name: &str) -> Option<OsString> {
            std::env::var_os(name).filter(|value| !value.is_empty())
        }
        Env {
            botster_config_dir: var("BOTSTER_CONFIG_DIR"),
            botster_data_dir: var("BOTSTER_DATA_DIR"),
            botster_hub_socket: var("BOTSTER_HUB_SOCKET"),
            xdg_config_home: var("XDG_CONFIG_HOME"),
            xdg_runtime_dir: var("XDG_RUNTIME_DIR"),
            home: var("HOME").map(PathBuf::from).unwrap_or_default(),
            uid: rustix::process::getuid().as_raw(),
            platform: Platform::current(),
        }
    }
}

/// The paths that the Hub derives at start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roots {
    pub config_root: PathBuf,
    pub data_root: PathBuf,
    /// The directory that holds the socket: the runtime directory chain (RT-5) when `runtime` is `Some`.
    pub socket: PathBuf,
    /// The runtime directories that the Hub creates and checks (RT-5), outermost first. Empty when the socket path comes
    /// from `BOTSTER_HUB_SOCKET` or `BOTSTER_CONFIG_DIR`.
    pub runtime_dirs: Vec<PathBuf>,
}

impl Roots {
    /// Core's data directory (HC A11 c1).
    pub fn core_dir(&self) -> PathBuf {
        self.data_root.join(CORE_DIR)
    }

    /// The single-instance lock (HC RT-4).
    pub fn lock_path(&self) -> PathBuf {
        self.data_root.join("hub.lock")
    }

    /// The configuration file (HC CF-1).
    pub fn config_file(&self) -> PathBuf {
        self.config_root.join("hub.toml")
    }
}

/// Derives the roots (HC RT-1) and the socket path (HC TB-L1).
pub fn derive(env: &Env) -> Roots {
    let config_root = match &env.botster_config_dir {
        Some(dir) => PathBuf::from(dir),
        None => match env.platform {
            Platform::MacOs => env.home.join("Library/Application Support").join(APP),
            Platform::Linux => match &env.xdg_config_home {
                Some(dir) => PathBuf::from(dir).join(APP),
                None => env.home.join(".config").join(APP),
            },
        },
    };
    let data_root = match (&env.botster_data_dir, &env.botster_config_dir) {
        (Some(dir), _) | (None, Some(dir)) => PathBuf::from(dir),
        (None, None) => env.home.join(".botster"),
    };
    let (socket, runtime_dirs) = match (&env.botster_hub_socket, &env.botster_config_dir) {
        (Some(path), _) => (PathBuf::from(path), Vec::new()),
        (None, Some(dir)) => (PathBuf::from(dir).join("hub.sock"), Vec::new()),
        (None, None) => {
            let mut dirs = Vec::new();
            let runtime = match &env.xdg_runtime_dir {
                Some(dir) => PathBuf::from(dir),
                None => {
                    let dir = PathBuf::from(format!("/tmp/{APP}-{}", env.uid));
                    dirs.push(dir.clone());
                    dir
                }
            };
            let app = runtime.join(APP);
            dirs.push(app.clone());
            (app.join("hub.sock"), dirs)
        }
    };
    Roots {
        config_root,
        data_root,
        socket,
        runtime_dirs,
    }
}

/// The length of a path in bytes.
pub fn path_bytes(path: &Path) -> usize {
    path.as_os_str().as_bytes().len()
}

/// Checks the data root against [`MAX_DATA_ROOT_BYTES`] (design section 5a).
///
/// # Errors
/// The length of the data root when it is over the bound.
pub fn check_data_root(data_root: &Path) -> Result<(), usize> {
    match path_bytes(data_root) {
        len if len > MAX_DATA_ROOT_BYTES => Err(len),
        _ => Ok(()),
    }
}

/// Checks the Hub's socket path against [`MAX_SOCKET_PATH_BYTES`] (HC TB-L1).
///
/// # Errors
/// The length of the path when it is over the limit.
pub fn check_socket(socket: &Path) -> Result<(), usize> {
    match path_bytes(socket) {
        len if len > MAX_SOCKET_PATH_BYTES => Err(len),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(platform: Platform) -> Env {
        Env {
            botster_config_dir: None,
            botster_data_dir: None,
            botster_hub_socket: None,
            xdg_config_home: None,
            xdg_runtime_dir: None,
            home: PathBuf::from("/home/u"),
            uid: 501,
            platform,
        }
    }

    /// HC RT-1: with no variable set, the platform defaults.
    #[test]
    fn rt_1_defaults_per_platform() {
        let mac = derive(&env(Platform::MacOs));
        assert_eq!(
            mac.config_root,
            PathBuf::from("/home/u/Library/Application Support/botster")
        );
        assert_eq!(mac.data_root, PathBuf::from("/home/u/.botster"));
        let linux = derive(&env(Platform::Linux));
        assert_eq!(linux.config_root, PathBuf::from("/home/u/.config/botster"));
        assert_eq!(linux.data_root, PathBuf::from("/home/u/.botster"));
    }

    /// HC RT-1: `XDG_CONFIG_HOME` on Linux only.
    #[test]
    fn rt_1_xdg_config_home_on_linux_only() {
        let mut linux = env(Platform::Linux);
        linux.xdg_config_home = Some("/x".into());
        assert_eq!(derive(&linux).config_root, PathBuf::from("/x/botster"));
        let mut mac = env(Platform::MacOs);
        mac.xdg_config_home = Some("/x".into());
        assert_eq!(
            derive(&mac).config_root,
            PathBuf::from("/home/u/Library/Application Support/botster")
        );
    }

    /// HC RT-1: the data root is `BOTSTER_DATA_DIR`, else `BOTSTER_CONFIG_DIR`, else `~/.botster`; the config root is
    /// `BOTSTER_CONFIG_DIR` when set.
    #[test]
    fn rt_1_roots_precedence() {
        let mut e = env(Platform::Linux);
        e.botster_config_dir = Some("/c".into());
        let roots = derive(&e);
        assert_eq!(roots.config_root, PathBuf::from("/c"));
        assert_eq!(roots.data_root, PathBuf::from("/c"));
        e.botster_data_dir = Some("/d".into());
        let roots = derive(&e);
        assert_eq!(roots.config_root, PathBuf::from("/c"));
        assert_eq!(roots.data_root, PathBuf::from("/d"));
        e.botster_config_dir = None;
        assert_eq!(derive(&e).data_root, PathBuf::from("/d"));
    }

    /// HC TB-L1: `BOTSTER_HUB_SOCKET`, then `$BOTSTER_CONFIG_DIR/hub.sock`, then `<runtime dir>/botster/hub.sock`.
    #[test]
    fn tb_l1_discovery_order() {
        let mut e = env(Platform::Linux);
        let roots = derive(&e);
        assert_eq!(
            roots.socket,
            PathBuf::from("/tmp/botster-501/botster/hub.sock")
        );
        assert_eq!(
            roots.runtime_dirs,
            vec![
                PathBuf::from("/tmp/botster-501"),
                PathBuf::from("/tmp/botster-501/botster")
            ]
        );
        e.xdg_runtime_dir = Some("/run/user/501".into());
        let roots = derive(&e);
        assert_eq!(
            roots.socket,
            PathBuf::from("/run/user/501/botster/hub.sock")
        );
        assert_eq!(
            roots.runtime_dirs,
            vec![PathBuf::from("/run/user/501/botster")]
        );
        e.botster_config_dir = Some("/c".into());
        let roots = derive(&e);
        assert_eq!(roots.socket, PathBuf::from("/c/hub.sock"));
        assert!(roots.runtime_dirs.is_empty());
        e.botster_hub_socket = Some("/s/h.sock".into());
        let roots = derive(&e);
        assert_eq!(roots.socket, PathBuf::from("/s/h.sock"));
        assert!(roots.runtime_dirs.is_empty());
    }

    /// Design section 5a: 54 bytes pass, 55 fail; HC TB-L1: 103 bytes pass, 104 fail.
    #[test]
    fn length_bounds() {
        assert_eq!(MAX_DATA_ROOT_BYTES, 54);
        assert_eq!(check_data_root(Path::new(&"/".repeat(54))), Ok(()));
        assert_eq!(check_data_root(Path::new(&"/".repeat(55))), Err(55));
        assert_eq!(check_socket(Path::new(&"/".repeat(103))), Ok(()));
        assert_eq!(check_socket(Path::new(&"/".repeat(104))), Err(104));
    }

    /// The other paths hang off the roots.
    #[test]
    fn derived_paths() {
        let mut e = env(Platform::Linux);
        e.botster_config_dir = Some("/c".into());
        e.botster_data_dir = Some("/d".into());
        let roots = derive(&e);
        assert_eq!(roots.core_dir(), PathBuf::from("/d/core"));
        assert_eq!(roots.lock_path(), PathBuf::from("/d/hub.lock"));
        assert_eq!(roots.config_file(), PathBuf::from("/c/hub.toml"));
    }
}
