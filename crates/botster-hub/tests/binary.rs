//! `botster-hub up --ready-fd <N>` as a process (HC HS-0): the ready line, a typed refusal of a bad descriptor (the lead's
//! condition on the `take_ready_fd` exception), and the stop on SIGTERM.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};

fn short_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("bh-")
        .tempdir_in("/tmp")
        .unwrap()
}

fn hub(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .env_clear()
        .env("HOME", "/nonexistent")
        .env("BOTSTER_CONFIG_DIR", root.join("c"))
        .env("BOTSTER_DATA_DIR", root.join("d"));
    command
}

/// A closed or invalid descriptor fails the start with a typed line on stderr, and the Hub creates nothing.
#[test]
fn bad_ready_fd_is_refused_typed() {
    let tmp = short_root();
    for fd in ["57", "-1"] {
        let out = hub(tmp.path())
            .args(["up", "--ready-fd", fd])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.starts_with(&format!("failed internal the ready descriptor {fd} ")),
            "{stderr}"
        );
        assert!(!tmp.path().join("d").exists());
    }
}

/// HC HS-0: the ready line on N after listen and adoption, then end of file on N; SIGTERM runs the stop order and the
/// Hub exits 0 with the socket removed.
#[test]
fn ready_line_then_sigterm_stop() {
    let tmp = short_root();
    // Descriptor 1 is the ready descriptor: the test reads the child's stdout.
    let mut child = hub(tmp.path())
        .args(["up", "--ready-fd", "1"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut ready)
        .unwrap();
    let socket = tmp.path().join("c/hub.sock");
    assert_eq!(
        ready,
        format!("ready {}\n", socket.display()),
        "one line, then end of file"
    );
    assert!(socket.exists());
    let pid = rustix::process::Pid::from_child(&child);
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(0));
    assert!(!socket.exists());
}

/// HC HS-0: a start failure writes `failed <code> <message>` on N.
#[test]
fn failed_line_on_the_ready_fd() {
    let tmp = short_root();
    std::fs::create_dir_all(tmp.path().join("c")).unwrap();
    std::fs::write(tmp.path().join("c/hub.toml"), "x = 1\n").unwrap();
    let out = hub(tmp.path())
        .args(["up", "--ready-fd", "1"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "failed config_unknown_key x\n"
    );
}
