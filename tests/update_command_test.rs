#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

mod support;
use support::ensure_session_worker_binary;

#[test]
fn update_requires_an_explicit_scope() {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("update")
        .env("BOTSTER_ENV", "test")
        .output()
        .expect("run update without a scope");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("usage: botster-hub update <core|all> [--data-dir <path>]")
    );
}

#[test]
fn update_rejects_a_dirty_source_repository_through_the_production_cli() {
    let root = unique_test_dir("dirty-source");
    let data_dir = root.join("data");
    let source = root.join("source");
    fs::create_dir_all(&source).expect("create source fixture");
    git(&source, &["init"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(source.join("tracked"), "clean\n").expect("write tracked fixture");
    git(&source, &["add", "tracked"]);
    git(&source, &["commit", "-m", "fixture"]);
    fs::write(source.join("operator-change"), "preserve\n").expect("write dirty fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .args(["update", "core", "--data-dir"])
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT", &source)
        .output()
        .expect("run dirty update");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("repository is dirty"), "{stderr}");
    assert_eq!(
        fs::read_to_string(source.join("operator-change")).unwrap(),
        "preserve\n"
    );
}

#[test]
fn update_build_failure_leaves_the_running_daemon_unchanged() {
    ensure_session_worker_binary();
    let root = unique_test_dir("build-failure");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, false);
    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .unwrap();
    let worker_bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/debug/botster-session-worker")
        .canonicalize()
        .unwrap();
    let mut daemon = Command::new(&hub_bin)
        .args(["start", "--data-dir"])
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(&worker_bin)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start fixture daemon");
    wait_for_status(&hub_bin, &data_dir);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    let socket_path = data_dir.join("botster-hub.sock");
    let metadata = serde_json::json!({
        "pid": daemon.id(),
        "data_directory": data_dir.to_string_lossy(),
        "data_directory_arg": data_directory_arg.to_string_lossy(),
        "socket_path": socket_path.to_string_lossy(),
        "hub_bin": hub_bin.to_string_lossy(),
        "session_worker_bin": worker_bin.to_string_lossy()
    });
    fs::write(
        data_dir.join(".botster-hub-runtime-daemon.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();

    let fake_bin = source.join("fake-bin");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(&hub_bin)
        .args(["update", "core", "--data-dir"])
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT", &source)
        .env("PATH", path)
        .output()
        .expect("run build-failing update");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("build Hub failed"), "{stderr}");
    assert!(
        daemon.try_wait().unwrap().is_none(),
        "old daemon stopped after a pre-stop build failure"
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(persisted["pid"].as_u64(), Some(daemon.id() as u64));
    wait_for_status(&hub_bin, &data_dir);

    fs::remove_file(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap();
    let shutdown = Command::new(&hub_bin)
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .output()
        .unwrap();
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    assert!(daemon.wait().unwrap().success());
}

#[test]
fn update_replaces_the_daemon_before_a_verification_failure() {
    ensure_session_worker_binary();
    let root = unique_test_dir("replace-verification");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, true);
    let source_target = source.join("target/debug");
    fs::create_dir_all(&source_target).unwrap();
    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .unwrap();
    let worker_bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/debug/botster-session-worker")
        .canonicalize()
        .unwrap();
    fs::copy(&hub_bin, source_target.join("botster-hub")).unwrap();
    fs::copy(&worker_bin, source_target.join("botster-session-worker")).unwrap();

    let old_pid = start_detached_daemon(&hub_bin, &worker_bin, &data_dir, &root);
    wait_for_status(&hub_bin, &data_dir);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    write_runtime_metadata(
        &data_dir,
        &data_directory_arg,
        &hub_bin,
        &worker_bin,
        old_pid,
    );

    let path = format!(
        "{}:{}",
        source.join("fake-bin").display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(&hub_bin)
        .args(["update", "core", "--data-dir"])
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT", &source)
        .env("PATH", path)
        .output()
        .expect("run verification-failing update");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("updated Hub revision mismatch"), "{stderr}");
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap(),
    )
    .unwrap();
    let new_pid = metadata["pid"].as_u64().unwrap() as u32;
    assert_ne!(new_pid, old_pid, "update silently reused the old daemon");
    wait_for_status(&source_target.join("botster-hub"), &data_dir);

    let shutdown = Command::new(source_target.join("botster-hub"))
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .output()
        .unwrap();
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
}

#[test]
fn update_all_missing_package_contract_leaves_the_running_daemon_unchanged() {
    ensure_session_worker_binary();
    let root = unique_test_dir("all-missing-contract");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, true);
    let package = create_direct_local_package(&root);
    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .unwrap();
    let worker_bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/debug/botster-session-worker")
        .canonicalize()
        .unwrap();
    let mut daemon = Command::new(&hub_bin)
        .args(["start", "--data-dir"])
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(&worker_bin)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_status(&hub_bin, &data_dir);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    write_runtime_metadata(
        &data_dir,
        &data_directory_arg,
        &hub_bin,
        &worker_bin,
        daemon.id(),
    );
    for args in [
        vec![
            "packages",
            "install",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--path",
            package.to_str().unwrap(),
        ],
        vec![
            "packages",
            "enable",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "runtime.synthetic-plugin",
        ],
    ] {
        let output = Command::new(&hub_bin).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let path = format!(
        "{}:{}",
        source.join("fake-bin").display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(&hub_bin)
        .args(["update", "all", "--data-dir"])
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT", &source)
        .env("PATH", path)
        .output()
        .expect("run update all without package contract");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("requires") && stderr.contains("botster-update.json"),
        "{stderr}"
    );
    assert!(daemon.try_wait().unwrap().is_none());
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["pid"].as_u64(), Some(daemon.id() as u64));

    fs::remove_file(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap();
    let shutdown = Command::new(&hub_bin)
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    assert!(daemon.wait().unwrap().success());
}

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(root)
        .status()
        .expect("run fixture git command");
    assert!(status.success(), "git {}", args.join(" "));
}

fn create_clean_update_source(root: &Path, builds_succeed: bool) -> PathBuf {
    let remote = root.join("remote.git");
    let status = Command::new("git")
        .args(["init", "--bare"])
        .arg(&remote)
        .status()
        .unwrap();
    assert!(status.success());
    let source = root.join("source");
    fs::create_dir_all(source.join("fake-bin")).unwrap();
    git(&source, &["init"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(
        source.join("Cargo.lock"),
        r#"[[package]]
name = "botster-core"
source = "git+https://example.invalid/core#abc123"

[[package]]
name = "botster-core-daemon"
source = "git+https://example.invalid/core#abc123"
"#,
    )
    .unwrap();
    fs::write(source.join(".gitignore"), "target/\n").unwrap();
    let cargo = source.join("fake-bin/cargo");
    let build_status = if builds_succeed { 0 } else { 23 };
    fs::write(
        &cargo,
        format!("#!/bin/sh\nif [ \"$1\" = update ]; then exit 0; fi\nexit {build_status}\n"),
    )
    .unwrap();
    fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755)).unwrap();
    git(
        &source,
        &["add", ".gitignore", "Cargo.lock", "fake-bin/cargo"],
    );
    git(&source, &["commit", "-m", "fixture"]);
    let remote_text = remote.to_string_lossy().into_owned();
    git(&source, &["remote", "add", "origin", &remote_text]);
    git(&source, &["push", "-u", "origin", "main"]);
    source
}

fn create_direct_local_package(root: &Path) -> PathBuf {
    let remote = root.join("package-remote.git");
    assert!(
        Command::new("git")
            .args(["init", "--bare"])
            .arg(&remote)
            .status()
            .unwrap()
            .success()
    );
    let package = root.join("package");
    fs::create_dir_all(&package).unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/synthetic-plugin/botster-package.json"),
        package.join("botster-package.json"),
    )
    .unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/synthetic-plugin/plugin.lua"),
        package.join("plugin.lua"),
    )
    .unwrap();
    git(&package, &["init"]);
    git(
        &package,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&package, &["config", "user.name", "Update Test"]);
    git(&package, &["add", "botster-package.json", "plugin.lua"]);
    git(&package, &["commit", "-m", "fixture"]);
    let remote_text = remote.to_string_lossy().into_owned();
    git(&package, &["remote", "add", "origin", &remote_text]);
    git(&package, &["push", "-u", "origin", "main"]);
    package
}

fn start_detached_daemon(hub_bin: &Path, worker_bin: &Path, data_dir: &Path, root: &Path) -> u32 {
    let pid_file = root.join("daemon.pid");
    let command = format!(
        "{} start --data-dir {} --session-worker-bin {} >/dev/null 2>&1 & echo $! > {}",
        hub_bin.display(),
        data_dir.display(),
        worker_bin.display(),
        pid_file.display()
    );
    let status = Command::new("/bin/sh")
        .args(["-c", &command])
        .status()
        .unwrap();
    assert!(status.success());
    fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn write_runtime_metadata(
    data_dir: &Path,
    data_directory_arg: &Path,
    hub_bin: &Path,
    worker_bin: &Path,
    pid: u32,
) {
    let metadata = serde_json::json!({
        "pid": pid,
        "data_directory": data_dir.to_string_lossy(),
        "data_directory_arg": data_directory_arg.to_string_lossy(),
        "socket_path": data_dir.join("botster-hub.sock").to_string_lossy(),
        "hub_bin": hub_bin.to_string_lossy(),
        "session_worker_bin": worker_bin.to_string_lossy()
    });
    fs::write(
        data_dir.join(".botster-hub-runtime-daemon.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
}

fn wait_for_status(hub_bin: &Path, data_dir: &Path) {
    for _ in 0..200 {
        let status = Command::new(hub_bin)
            .args(["status", "--data-dir"])
            .arg(data_dir)
            .output()
            .unwrap();
        if status.status.success() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("daemon did not become ready");
}

fn unique_test_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let path = PathBuf::from("/tmp").join(format!("bhu-{label}-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).expect("create unique test directory");
    path
}
