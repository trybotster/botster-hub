//! Start steps 1 to 5 of design section 5: the data root and the lock, the identity, `hub.toml`, the runtime directory,
//! and Core's data directory. Steps 6 to 8 (the owner, adoption, the socket and the ready line) are in `daemon`.

use crate::failure::{FailCode, StartFailure};
use crate::fsutil::{self, ensure_dir, WriteError};
use crate::roots::{self, Roots};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// The software name that a running Hub writes into its lock file, for the `lock_held` message (HC HS-0).
pub const SOFTWARE: &str = concat!("botster-hub ", env!("CARGO_PKG_VERSION"));

/// The held single-instance lock (HC RT-4). Dropping it releases the lock.
#[derive(Debug)]
pub struct HubLock {
    file: File,
}

impl HubLock {
    /// Releases the lock. The stop calls it after the socket is removed (RT-4).
    pub fn release(self) {
        drop(self.file);
    }
}

/// Step 1: checks the length of the data root and the socket path before anything is created, then creates the data root
/// (0700) and `hub.lock` (0600). Step 2: takes `flock(LOCK_EX | LOCK_NB)`.
///
/// # Errors
/// `socket_path_too_long`, `data_root_unusable`, or `lock_held` naming the running Hub.
pub fn take_lock(roots: &Roots) -> Result<HubLock, StartFailure> {
    if let Err(len) = roots::check_data_root(&roots.data_root) {
        return Err(StartFailure::new(
            FailCode::SocketPathTooLong,
            format!(
                "the data root {} is {len} bytes; the limit is {} bytes, so that every socket path stays within {} bytes",
                roots.data_root.display(),
                roots::MAX_DATA_ROOT_BYTES,
                roots::MAX_SOCKET_PATH_BYTES
            ),
        ));
    }
    if let Err(len) = roots::check_socket(&roots.socket) {
        return Err(StartFailure::new(
            FailCode::SocketPathTooLong,
            format!(
                "the socket path {} is {len} bytes; the limit is {} bytes",
                roots.socket.display(),
                roots::MAX_SOCKET_PATH_BYTES
            ),
        ));
    }
    ensure_durable_dir(&roots.data_root, fsutil::sync_dir)
        .map_err(|e| unusable(&roots.data_root, &e))?;
    let lock_path = roots.lock_path();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .map_err(|e| unusable(&lock_path, &e))?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(rustix::io::Errno::WOULDBLOCK) => {
            let mut holder = String::new();
            let _ = file.read_to_string(&mut holder);
            let holder = holder.trim();
            let holder = if holder.is_empty() {
                "an unknown Hub"
            } else {
                holder
            };
            return Err(StartFailure::new(
                FailCode::LockHeld,
                format!("{} is held by {holder}", lock_path.display()),
            ));
        }
        Err(errno) => return Err(unusable(&lock_path, &io::Error::from(errno))),
    }
    // The holder's name is for the message of a second instance only; the lock itself is the `flock` (never a PID check).
    let holder = format!("{SOFTWARE} (pid {})", std::process::id());
    let named = file
        .set_len(0)
        .and_then(|()| file.rewind())
        .and_then(|()| file.write_all(holder.as_bytes()));
    named.map_err(|e| unusable(&lock_path, &e))?;
    Ok(HubLock { file })
}

/// Creates `dir` (0700) if it is missing, then syncs its parent through `sync`, so that the entry of the data root itself is
/// durable before the Hub reports ready: Core and the Hub sync everything below the root, and neither syncs its ancestors. It
/// runs at every start, so a retry after a crash between the creation and the sync also syncs. A failed sync fails the start.
fn ensure_durable_dir(dir: &Path, sync: impl FnOnce(&Path) -> io::Result<()>) -> io::Result<()> {
    ensure_dir(dir)?;
    match dir.parent() {
        Some(parent) => sync(parent),
        None => Ok(()),
    }
}

fn unusable(path: &Path, error: &io::Error) -> StartFailure {
    StartFailure::new(
        FailCode::DataRootUnusable,
        format!("{}: {error}", path.display()),
    )
}

/// The identity of this Hub process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Durable across restarts (HP A2-38).
    pub hub_id: String,
    /// New at each process start (HC HS-5).
    pub incarnation: String,
}

/// 128 random bits from the OS CSPRNG, as lowercase base32 with no padding (26 characters).
///
/// # Errors
/// The OS random source failed.
pub fn random_id() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| io::Error::other(e.to_string()))?;
    Ok(data_encoding::BASE32_NOPAD
        .encode(&bytes)
        .to_ascii_lowercase())
}

/// Step 3, first part: reads or creates `store/hub_id`, and draws a new incarnation. Only the lock holder calls it.
///
/// # Errors
/// `data_root_unusable` when `store/` or `hub_id` cannot be read or written, or `hub_id` is not one line of base32.
pub fn identity(roots: &Roots, _lock: &HubLock) -> Result<Identity, StartFailure> {
    let store = roots.data_root.join("store");
    ensure_dir(&store).map_err(|e| unusable(&store, &e))?;
    let path = store.join("hub_id");
    let hub_id = match fs::read_to_string(&path) {
        Ok(text) => {
            let id = text.trim_end_matches('\n');
            if id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
            {
                return Err(StartFailure::new(
                    FailCode::DataRootUnusable,
                    format!("{}: not a hub id", path.display()),
                ));
            }
            id.to_owned()
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let id = random_id().map_err(|e| unusable(&path, &e))?;
            let tmp = store.join(".tmp-hub_id");
            fsutil::write_atomic(&path, &tmp, format!("{id}\n").as_bytes()).map_err(
                |e| match e {
                    WriteError::NotWritten(e) | WriteError::Unknown(e) => unusable(&path, &e),
                },
            )?;
            id
        }
        Err(error) => return Err(unusable(&path, &error)),
    };
    let incarnation = random_id().map_err(|e| unusable(&path, &e))?;
    Ok(Identity {
        hub_id,
        incarnation,
    })
}

/// Step 3, second part: reads `<config root>/hub.toml` if it exists (HC CF-1). Stage 2 defines no key (lead ruling Q3).
///
/// # Errors
/// `config_unsafe <path>` when another user can write the file (R-118), `config_invalid` when it is not TOML, and
/// `config_unknown_key <key>` for the first key.
pub fn check_config(path: &Path, uid: u32) -> Result<(), StartFailure> {
    let meta = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(StartFailure::new(
                FailCode::ConfigInvalid,
                format!("{}: {error}", path.display()),
            ))
        }
    };
    if meta.uid() != uid || meta.mode() & 0o022 != 0 {
        return Err(StartFailure::new(
            FailCode::ConfigUnsafe,
            path.display().to_string(),
        ));
    }
    let text = fs::read_to_string(path).map_err(|error| {
        StartFailure::new(
            FailCode::ConfigInvalid,
            format!("{}: {error}", path.display()),
        )
    })?;
    let table: toml::Table = text.parse().map_err(|error: toml::de::Error| {
        StartFailure::new(
            FailCode::ConfigInvalid,
            format!("{}: {}", path.display(), error.message()),
        )
    })?;
    match first_key(&table, "") {
        Some(key) => Err(StartFailure::new(FailCode::ConfigUnknownKey, key)),
        None => Ok(()),
    }
}

/// The first key of the table in name order, dotted. An empty table (`[hub]` with no key) names no key.
fn first_key(table: &toml::Table, prefix: &str) -> Option<String> {
    table.iter().find_map(|(key, value)| {
        let name = format!("{prefix}{key}");
        match value {
            toml::Value::Table(inner) => first_key(inner, &format!("{name}.")),
            _ => Some(name),
        }
    })
}

/// Step 4: creates and checks the runtime directories (HC RT-5): each exists as a directory owned by `uid` with no group or
/// other bits, or is created so.
///
/// # Errors
/// `socket_unavailable` naming the directory.
pub fn check_runtime_dirs(dirs: &[PathBuf], uid: u32) -> Result<(), StartFailure> {
    for dir in dirs {
        let refuse = |why: String| {
            StartFailure::new(
                FailCode::SocketUnavailable,
                format!("{}: {why}", dir.display()),
            )
        };
        ensure_dir(dir).map_err(|e| refuse(e.to_string()))?;
        let meta = fs::symlink_metadata(dir).map_err(|e| refuse(e.to_string()))?;
        if !meta.is_dir() {
            return Err(refuse("not a directory".into()));
        }
        if meta.uid() != uid {
            return Err(refuse(format!("owned by uid {}, not {uid}", meta.uid())));
        }
        if meta.mode() & 0o077 != 0 {
            return Err(refuse(format!(
                "mode {:o} is wider than 0700",
                meta.mode() & 0o777
            )));
        }
    }
    Ok(())
}

/// Step 5, first part: creates `<data root>/core/` (0700; HC A11 c1). The caller passes it to `Core::open`.
///
/// # Errors
/// `data_root_unusable`.
pub fn core_dir(roots: &Roots, _lock: &HubLock) -> Result<PathBuf, StartFailure> {
    let dir = roots.core_dir();
    ensure_dir(&dir).map_err(|e| unusable(&dir, &e))?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roots::{Env, Platform};
    use std::os::unix::fs::PermissionsExt;

    /// The parent of a new data root is synced; so is the parent of an existing one (a retry); a sync error fails.
    #[test]
    fn the_data_root_parent_is_synced_and_a_sync_error_fails() {
        let tmp = short_root();
        let dir = tmp.path().join("d");
        let synced = std::cell::RefCell::new(Vec::new());
        let record = |parent: &Path| {
            synced.borrow_mut().push(parent.to_path_buf());
            Ok(())
        };
        ensure_durable_dir(&dir, record).unwrap();
        ensure_durable_dir(&dir, record).unwrap();
        assert_eq!(
            *synced.borrow(),
            vec![tmp.path().to_path_buf(), tmp.path().to_path_buf()]
        );
        let failing = |_: &Path| Err(io::Error::other("injected"));
        let error = ensure_durable_dir(&dir, failing).unwrap_err();
        assert_eq!(error.to_string(), "injected");
    }

    /// A short test root under `/tmp` (design section 5a: the macOS `TMPDIR` is too long for a data root).
    pub(crate) fn short_root() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    fn roots_in(dir: &Path) -> Roots {
        roots::derive(&Env {
            botster_config_dir: Some(dir.join("c").into()),
            botster_data_dir: Some(dir.join("d").into()),
            botster_hub_socket: None,
            xdg_config_home: None,
            xdg_runtime_dir: None,
            home: PathBuf::from("/nonexistent"),
            uid: rustix::process::getuid().as_raw(),
            platform: Platform::current(),
        })
    }

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// HC RT-4: a second lock on the same data root is refused `lock_held`, naming the running Hub.
    #[test]
    fn rt_4_second_hub_same_root_refused() {
        let tmp = short_root();
        let roots = roots_in(tmp.path());
        let first = take_lock(&roots).unwrap();
        assert_eq!(mode(&roots.data_root), 0o700);
        assert_eq!(mode(&roots.lock_path()), 0o600);
        let second = take_lock(&roots).unwrap_err();
        assert_eq!(second.code, FailCode::LockHeld);
        assert!(second.message.contains(SOFTWARE), "{}", second.message);
        first.release();
        take_lock(&roots).unwrap();
    }

    /// HC RT-4: the lock's scope is the data root; another root locks on its own.
    #[test]
    fn rt_4_other_root_is_independent() {
        let a = short_root();
        let b = short_root();
        let _first = take_lock(&roots_in(a.path())).unwrap();
        take_lock(&roots_in(b.path())).unwrap();
    }

    /// Design section 5a: a long data root fails `socket_path_too_long` before anything is created.
    #[test]
    fn long_data_root_creates_nothing() {
        let tmp = short_root();
        let mut roots = roots_in(tmp.path());
        let pad = roots::MAX_DATA_ROOT_BYTES + 1 - roots::path_bytes(tmp.path()) - 1;
        roots.data_root = tmp.path().join("x".repeat(pad));
        assert_eq!(
            roots::path_bytes(&roots.data_root),
            roots::MAX_DATA_ROOT_BYTES + 1
        );
        let failure = take_lock(&roots).unwrap_err();
        assert_eq!(failure.code, FailCode::SocketPathTooLong);
        assert!(!roots.data_root.exists());
    }

    /// HC TB-L1: a socket path over 103 bytes fails before anything is created.
    #[test]
    fn long_socket_path_creates_nothing() {
        let tmp = short_root();
        let mut roots = roots_in(tmp.path());
        roots.socket = PathBuf::from(format!("/{}", "s".repeat(103)));
        let failure = take_lock(&roots).unwrap_err();
        assert_eq!(failure.code, FailCode::SocketPathTooLong);
        assert!(!roots.data_root.exists());
    }

    /// A missing parent of the data root is `data_root_unusable`: the Hub creates only the data root itself.
    #[test]
    fn missing_parent_is_unusable() {
        let tmp = short_root();
        let mut roots = roots_in(tmp.path());
        roots.data_root = tmp.path().join("p/d");
        assert_eq!(
            take_lock(&roots).unwrap_err().code,
            FailCode::DataRootUnusable
        );
    }

    /// HP A2-38, HC HS-5: the hub id survives a restart, the incarnation does not.
    #[test]
    fn hub_id_is_durable_incarnation_is_new() {
        let tmp = short_root();
        let roots = roots_in(tmp.path());
        let lock = take_lock(&roots).unwrap();
        let one = identity(&roots, &lock).unwrap();
        let two = identity(&roots, &lock).unwrap();
        assert_eq!(one.hub_id, two.hub_id);
        assert_ne!(one.incarnation, two.incarnation);
        assert_eq!(one.hub_id.len(), 26);
        assert_eq!(mode(&roots.data_root.join("store")), 0o700);
        assert_eq!(mode(&roots.data_root.join("store/hub_id")), 0o600);
        fs::write(roots.data_root.join("store/hub_id"), "NOT-AN-ID\n").unwrap();
        assert_eq!(
            identity(&roots, &lock).unwrap_err().code,
            FailCode::DataRootUnusable
        );
    }

    fn config(tmp: &Path, text: &str, mode: u32) -> PathBuf {
        let path = tmp.join("hub.toml");
        fs::write(&path, text).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    /// HC CF-1 with lead ruling Q3: no file and an empty file pass; any key is `config_unknown_key <key>`.
    #[test]
    fn cf_1_unknown_key_refuses_start() {
        let tmp = short_root();
        let uid = rustix::process::getuid().as_raw();
        assert_eq!(check_config(&tmp.path().join("none.toml"), uid), Ok(()));
        assert_eq!(check_config(&config(tmp.path(), "", 0o600), uid), Ok(()));
        assert_eq!(
            check_config(&config(tmp.path(), "[hub]\n", 0o600), uid),
            Ok(())
        );
        let failure =
            check_config(&config(tmp.path(), "[hub]\noffline = true\n", 0o600), uid).unwrap_err();
        assert_eq!(failure.code, FailCode::ConfigUnknownKey);
        assert_eq!(failure.message, "hub.offline");
        let failure = check_config(&config(tmp.path(), "x = 1\n", 0o600), uid).unwrap_err();
        assert_eq!(failure.message, "x");
        let failure = check_config(&config(tmp.path(), "x = \n", 0o600), uid).unwrap_err();
        assert_eq!(failure.code, FailCode::ConfigInvalid);
    }

    /// HC CF-1 with R-118: a file that another user can write is `config_unsafe <path>`.
    #[test]
    fn cf_1_world_writable_file_refused() {
        let tmp = short_root();
        let uid = rustix::process::getuid().as_raw();
        for mode in [0o620, 0o602, 0o666] {
            let path = config(tmp.path(), "", mode);
            let failure = check_config(&path, uid).unwrap_err();
            assert_eq!(failure.code, FailCode::ConfigUnsafe);
            assert_eq!(failure.message, path.display().to_string());
        }
        // Another owner can write it whatever the mode.
        let path = config(tmp.path(), "", 0o600);
        assert_eq!(
            check_config(&path, uid + 1).unwrap_err().code,
            FailCode::ConfigUnsafe
        );
    }

    /// HC RT-5: the runtime directories are created 0700; a wider mode, another owner or a non-directory is refused.
    #[test]
    fn rt_5_foreign_runtime_dir_refused() {
        let tmp = short_root();
        let uid = rustix::process::getuid().as_raw();
        let outer = tmp.path().join("r");
        let inner = outer.join("botster");
        let dirs = vec![outer.clone(), inner.clone()];
        check_runtime_dirs(&dirs, uid).unwrap();
        assert_eq!(mode(&outer), 0o700);
        assert_eq!(mode(&inner), 0o700);
        fs::set_permissions(&inner, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            check_runtime_dirs(&dirs, uid).unwrap_err().code,
            FailCode::SocketUnavailable
        );
        fs::set_permissions(&inner, fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            check_runtime_dirs(&dirs, uid + 1).unwrap_err().code,
            FailCode::SocketUnavailable
        );
        let file = tmp.path().join("f");
        fs::write(&file, b"").unwrap();
        assert_eq!(
            check_runtime_dirs(&[file], uid).unwrap_err().code,
            FailCode::SocketUnavailable
        );
    }

    /// HC A11 c1: `core/` is created with mode 0700.
    #[test]
    fn a11_1_core_dir_created_with_mode() {
        let tmp = short_root();
        let roots = roots_in(tmp.path());
        let lock = take_lock(&roots).unwrap();
        let dir = core_dir(&roots, &lock).unwrap();
        assert_eq!(dir, roots.data_root.join("core"));
        assert_eq!(mode(&dir), 0o700);
    }
}
