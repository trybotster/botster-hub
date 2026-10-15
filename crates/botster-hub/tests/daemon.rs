//! The daemon on the real Core (lead condition: the bridge is proven against the real crate): start, adoption, the socket,
//! the lock, and the stop order through `Shutdown`.

use botster_hub::bridge::{Answer, OutItem, Outbox, OutboxCaps, Request};
use botster_hub::daemon::{Daemon, DaemonConfig};
use botster_hub::failure::FailCode;
use botster_hub::phase::{Ended, StopCause, Wake};
use botster_hub::roots::{self, Env, Platform};
use std::fs;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A short test root under `/tmp` (design section 5a).
fn short_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("bh-")
        .tempdir_in("/tmp")
        .unwrap()
}

fn uid() -> u32 {
    rustix::process::getuid().as_raw()
}

fn roots_in(dir: &Path) -> roots::Roots {
    roots::derive(&Env {
        botster_config_dir: Some(dir.join("c").into()),
        botster_data_dir: Some(dir.join("d").into()),
        botster_hub_socket: None,
        xdg_config_home: None,
        xdg_runtime_dir: None,
        home: PathBuf::from("/nonexistent"),
        uid: uid(),
        platform: Platform::current(),
    })
}

fn config() -> DaemonConfig {
    let mut config = DaemonConfig::production(PathBuf::from("/bin/true"));
    config.handle_signals = false;
    config
}

struct NoWake;

impl Wake for NoWake {
    fn wake(&self) {}
}

fn outbox() -> Arc<Outbox> {
    let caps = OutboxCaps {
        replies: 4,
        frames: 4,
        watch_queue: 4,
    };
    Outbox::new(
        caps,
        botster_hub::phase::SharedPhase::new(),
        Arc::new(NoWake),
        Arc::new(NoWake),
    )
}

/// Submits `request` and waits for its answer.
fn ask(daemon: &Daemon, outbox: &Arc<Outbox>, id: u64, request: Request) -> Answer {
    daemon
        .bridge()
        .submit(request, outbox.reserve(id).unwrap())
        .unwrap();
    for _ in 0..2000 {
        if let Some(OutItem::Reply { request_id, answer }) = outbox.take(1).pop() {
            assert_eq!(request_id, id);
            return answer;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("no answer to request {id}");
}

/// HC HS-0, RT-2, RT-4, TB-L1 and A11 c1 on the real Core: the Hub adopts, then listens on a 0600 socket while it holds the
/// lock; a `Shutdown` runs the stop order to the end (socket removed, then the lock released) and exits 0.
#[test]
fn start_adopt_listen_and_stop() {
    let tmp = short_root();
    let roots = roots_in(tmp.path());
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    assert!(
        daemon.bridge().is_ready(),
        "adoption finished before start returned"
    );
    let socket = fs::symlink_metadata(&roots.socket).unwrap();
    assert!(socket.file_type().is_socket());
    assert_eq!(socket.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        fs::metadata(roots.core_dir()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    // A second Hub on the same root is refused while this one runs (RT-4).
    let second = Daemon::start(&roots, uid(), config()).err().unwrap();
    assert_eq!(second.code, FailCode::LockHeld);
    // The bridge reaches the real Core.
    let replies = outbox();
    assert!(
        matches!(ask(&daemon, &replies, 1, Request::List), Answer::Sessions(rows) if rows.is_empty())
    );
    assert_eq!(
        ask(&daemon, &replies, 2, Request::Shutdown),
        Answer::Stopping
    );
    let exit = daemon.wait();
    assert_eq!(exit.cause, Some(StopCause::Requested));
    assert_eq!((exit.owner, exit.io), (Ended::Clean, Ended::Clean));
    assert_eq!(exit.code(), 0);
    assert!(
        !roots.socket.exists(),
        "the socket is removed before the lock is released"
    );
    // The lock is released: a new Hub starts on the same root, and keeps the hub id.
    let again = Daemon::start(&roots, uid(), config()).unwrap();
    let replies = outbox();
    assert_eq!(
        ask(&again, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(again.wait().code(), 0);
}

/// HC A11 c1: `hub.lock` is taken before `Core::open`, so a Core error comes after the lock and leaves the lock free again.
#[test]
fn core_open_error_is_data_root_unusable() {
    let tmp = short_root();
    let roots = roots_in(tmp.path());
    let mut bad = config();
    bad.core_limits.pump_events = 0;
    let failure = Daemon::start(&roots, uid(), bad).err().unwrap();
    assert_eq!(failure.code, FailCode::DataRootUnusable);
    assert!(roots.lock_path().exists());
    assert!(!roots.socket.exists());
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    let replies = outbox();
    assert_eq!(
        ask(&daemon, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
}

/// Design section 5a: the 54-byte bound is pinned to Core. A 54-byte data root opens Core (Core's own socket check passes),
/// and a 55-byte one fails the Hub's check before anything is created.
#[test]
fn data_root_bound_matches_core() {
    let tmp = short_root();
    let mut roots = roots_in(tmp.path());
    let base = roots::path_bytes(tmp.path());
    let pad = |len: usize| tmp.path().join("x".repeat(len - base - 1));
    roots.data_root = pad(roots::MAX_DATA_ROOT_BYTES);
    assert_eq!(roots::path_bytes(&roots.data_root), 54);
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    let replies = outbox();
    assert_eq!(
        ask(&daemon, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
    roots.data_root = pad(roots::MAX_DATA_ROOT_BYTES + 1);
    let failure = Daemon::start(&roots, uid(), config()).err().unwrap();
    assert_eq!(failure.code, FailCode::SocketPathTooLong);
    assert!(!roots.data_root.exists());
}

/// The sweep follows nothing: a symlinked sessions root, and a symlink or a file inside it, leave outside data intact.
#[test]
fn the_orphan_sweep_follows_no_symlink_and_keeps_non_directories() {
    let tmp = short_root();
    let outside = tmp.path().join("outside");
    fs::create_dir_all(outside.join("keep")).unwrap();
    fs::write(outside.join("keep/data"), "x").unwrap();
    let roots = roots_in(tmp.path());
    fs::create_dir_all(&roots.data_root).unwrap();
    std::os::unix::fs::symlink(&outside, roots.data_root.join("sessions")).unwrap();
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    assert!(outside.join("keep/data").exists());
    let outbox = crate::outbox();
    assert_eq!(
        ask(&daemon, &outbox, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);

    let tmp = short_root();
    let roots = roots_in(tmp.path());
    let sessions = roots.data_root.join("sessions");
    fs::create_dir_all(sessions.join("gone")).unwrap();
    let outside = tmp.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, sessions.join("link")).unwrap();
    fs::write(sessions.join("file"), "x").unwrap();
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    assert!(!sessions.join("gone").exists());
    assert!(fs::symlink_metadata(sessions.join("link")).is_ok());
    assert!(outside.exists());
    assert!(sessions.join("file").exists());
    let outbox = crate::outbox();
    assert_eq!(
        ask(&daemon, &outbox, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
}

/// HC RT-2: a session directory with no row after adoption is deleted.
#[test]
fn rt_2_orphan_session_dir_deleted() {
    let tmp = short_root();
    let roots = roots_in(tmp.path());
    fs::create_dir_all(roots.data_root.join("sessions/gone/uploads")).unwrap();
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    assert!(!roots.data_root.join("sessions/gone").exists());
    let replies = outbox();
    assert_eq!(
        ask(&daemon, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
}

/// Design section 4a, "The session directory", and step 6 (Sol S2-HD-13): the orphan sweep deletes a crash-left
/// directory (no row, no record), keeps the directory of a `starting` record (the index is loaded before the sweep), and
/// never follows or deletes a symlink.
#[test]
fn the_orphan_sweep_keeps_starting_records_and_symlinks() {
    let tmp = short_root();
    let roots = roots_in(tmp.path());
    let sessions = roots.data_root.join("sessions");
    fs::create_dir_all(sessions.join("crashed/uploads")).unwrap();
    fs::create_dir_all(sessions.join("pending/uploads")).unwrap();
    let target = tmp.path().join("target");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("keep"), b"x").unwrap();
    std::os::unix::fs::symlink(&target, sessions.join("link")).unwrap();
    let ops = roots.data_root.join("store/ops");
    fs::create_dir_all(&ops).unwrap();
    let key = "b".repeat(26);
    let record = serde_json::json!({
        "version": 1,
        "record_key": key,
        "operation_id": "c".repeat(26),
        "session_id": "pending",
        "state": "starting",
        "admitted_at_ms": 1,
    });
    fs::write(ops.join(format!("{key}.json")), record.to_string()).unwrap();
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    assert!(!sessions.join("crashed").exists());
    assert!(sessions.join("pending/uploads").is_dir());
    assert!(fs::symlink_metadata(sessions.join("link")).is_ok());
    assert!(target.join("keep").exists());
    let replies = outbox();
    assert_eq!(
        ask(&daemon, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
}

/// Design section 4a, "Load at start": a record that cannot be resolved fails the start `data_root_unusable` before the
/// socket is bound; after the operator removes it, the Hub starts.
#[test]
fn undecodable_record_fails_start_before_bind() {
    let tmp = short_root();
    let roots = roots_in(tmp.path());
    let ops = roots.data_root.join("store/ops");
    fs::create_dir_all(&ops).unwrap();
    let bad = ops.join(format!("{}.json", "a".repeat(26)));
    fs::write(&bad, b"{").unwrap();
    let failure = Daemon::start(&roots, uid(), config()).err().unwrap();
    assert_eq!(failure.code, FailCode::DataRootUnusable);
    assert!(
        failure.message.contains("store/ops/"),
        "{}",
        failure.message
    );
    assert!(!roots.socket.exists());
    fs::remove_file(&bad).unwrap();
    let daemon = Daemon::start(&roots, uid(), config()).unwrap();
    let replies = outbox();
    assert_eq!(
        ask(&daemon, &replies, 1, Request::Shutdown),
        Answer::Stopping
    );
    assert_eq!(daemon.wait().code(), 0);
}
