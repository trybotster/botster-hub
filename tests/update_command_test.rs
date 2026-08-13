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
fn daemon_api_starts_and_reports_a_failed_source_update() {
    ensure_session_worker_binary();
    let root = unique_test_dir("daemon-api-update");
    let data_dir = root.join("data");
    let source = root.join("source");
    fs::create_dir_all(&source).unwrap();
    git(&source, &["init"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(source.join("tracked"), "clean\n").unwrap();
    git(&source, &["add", "tracked"]);
    git(&source, &["commit", "-m", "fixture"]);
    fs::write(source.join("operator-change"), "preserve\n").unwrap();

    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .unwrap();
    let worker_bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/debug/botster-session-worker")
        .canonicalize()
        .unwrap();
    let daemon_pid =
        start_detached_daemon_with_update_source(&hub_bin, &worker_bin, &data_dir, &source, &root);
    wait_for_status(&hub_bin, &data_dir);
    write_runtime_metadata(&data_dir, &data_dir, &hub_bin, &worker_bin, daemon_pid);
    let daemon_command = Command::new("ps")
        .args(["-p", &daemon_pid.to_string(), "-o", "command="])
        .output()
        .unwrap();
    assert!(daemon_command.status.success());
    assert!(
        String::from_utf8_lossy(&daemon_command.stdout).contains(" start "),
        "{}",
        String::from_utf8_lossy(&daemon_command.stdout)
    );
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));

    let accepted = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartHubUpdate {
            scope: botster_hub_client::DaemonHubUpdateScope::Core,
        },
    )
    .unwrap();
    assert_eq!(
        accepted.kind,
        botster_hub_client::DaemonResponseKind::HubUpdateExecution
    );
    let accepted = accepted.hub_update_execution.unwrap();
    assert_eq!(
        accepted.state,
        botster_hub_client::DaemonHubUpdateExecutionState::Started
    );
    assert!(accepted.updater_pid > 0);

    let failed = wait_for_update_execution(
        &endpoint,
        botster_hub_client::DaemonHubUpdateExecutionState::Failed,
    );
    assert_eq!(failed.update_id, accepted.update_id);
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("repository is dirty")),
        "{failed:?}"
    );
    assert_eq!(
        fs::read_to_string(source.join("operator-change")).unwrap(),
        "preserve\n"
    );
    let status =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status).unwrap();
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    let shutdown = Command::new(&hub_bin)
        .args(["down", "--data-dir"])
        .arg(&data_dir)
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    wait_for_process_exit(daemon_pid);
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
    let head_before = git_output(&source, &["rev-parse", "HEAD"]);
    let lock_before = fs::read(source.join("Cargo.lock")).unwrap();
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
        .env("BOTSTER_UPDATE_TEST_MARKER", root.join("cargo-was-run"))
        .env("PATH", path)
        .output()
        .expect("run update all without package contract");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("requires") && stderr.contains("botster-update.json"),
        "{stderr}"
    );
    assert!(
        !root.join("cargo-was-run").exists(),
        "package contract preflight ran after Cargo changed source inputs"
    );
    assert_eq!(git_output(&source, &["rev-parse", "HEAD"]), head_before);
    assert_eq!(fs::read(source.join("Cargo.lock")).unwrap(), lock_before);
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

#[test]
#[ignore = "run through script/test-update-preupdate-worker"]
fn update_all_replaces_an_incompatible_preupdate_worker_and_proves_attach_order() {
    let preupdate_worker = PathBuf::from(
        std::env::var_os("BOTSTER_PREUPDATE_WORKER_BIN")
            .expect("script must supply the real pre-update worker"),
    )
    .canonicalize()
    .expect("resolve pre-update worker");
    let root = unique_test_dir("preupdate-worker");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_real_build_update_source(&root);
    let source_target = source.join("target/debug");
    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .unwrap();
    let old_pid = start_detached_daemon_with_update_source(
        &hub_bin,
        &preupdate_worker,
        &data_dir,
        &source,
        &root,
    );
    wait_for_status(&hub_bin, &data_dir);
    let data_directory_arg = data_dir.clone();
    write_runtime_metadata(
        &data_directory_arg,
        &data_directory_arg,
        &hub_bin,
        &preupdate_worker,
        old_pid,
    );
    let data_dir = data_dir.canonicalize().unwrap();
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let old_session = "preupdate-incompatible-session";
    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: old_session.to_string(),
            command: "sleep 120".to_string(),
        },
    )
    .expect("spawn through the pre-update worker");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    let old_identity = read_worker_identity(&data_dir, old_session);

    let old_probe = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: old_session.to_string(),
        },
    )
    .expect("probe the pre-update worker");
    let old_is_incompatible = old_probe.kind
        != botster_hub_client::DaemonResponseKind::ReadModeFlags
        || old_probe.mode_flags.as_ref().is_none_or(|flags| {
            flags.mode_generation == 0 || flags.mode_generation > ((1_u64 << 53) - 1)
        });
    assert!(
        old_is_incompatible,
        "fixed pre-update worker must reproduce the incompatibility: {old_probe:?}"
    );

    let accepted = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartHubUpdate {
            scope: botster_hub_client::DaemonHubUpdateScope::All,
        },
    )
    .expect("start production update through the client contract")
    .hub_update_execution
    .expect("accepted Hub update execution");
    assert_eq!(
        accepted.state,
        botster_hub_client::DaemonHubUpdateExecutionState::Started
    );
    let completed = wait_for_update_execution(
        &endpoint,
        botster_hub_client::DaemonHubUpdateExecutionState::Complete,
    );
    assert_eq!(completed.update_id, accepted.update_id);
    let update_log = fs::read_to_string(
        data_dir.join(format!(".botster-hub-update-{}.log", accepted.update_id)),
    )
    .expect("read detached updater log");
    assert!(
        update_log.contains("\"code\":\"unsafe_mode_generation\"")
            || update_log.contains("\"code\":\"read_mode_flags_rejected\"")
    );
    wait_for_process_exit(old_identity.0);
    assert!(
        !old_identity.1.exists(),
        "old worker socket survived update"
    );

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status from updated daemon")
        .status
        .expect("status body");
    assert!(!status.recovered_sessions.contains(&old_session.to_string()));

    let new_session = "postupdate-compatible-session";
    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: new_session.to_string(),
            command: "sleep 120".to_string(),
        },
    )
    .expect("spawn through updated worker");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect updated daemon");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: new_session.to_string(),
            subscription_id: "postupdate-attach".to_string(),
        })
        .expect("attach updated session");
    let events = collect_attach_events(&mut connection, new_session, "postupdate-attach");
    let attaching = event_position(&events, "postupdate-attach", "attaching");
    let snapshot = events
        .iter()
        .position(|event| {
            matches!(event,
            botster_hub_client::DaemonEvent::Snapshot { subscription_id, .. }
                if subscription_id == "postupdate-attach")
        })
        .expect("production attach Snapshot");
    let attached = event_position(&events, "postupdate-attach", "attached");
    assert!(attaching < snapshot && snapshot < attached, "{events:?}");
    let payload = events
        .iter()
        .find_map(|event| match event {
            botster_hub_client::DaemonEvent::Snapshot {
                subscription_id,
                history,
                ..
            } if subscription_id == "postupdate-attach" => {
                Some(history.decoded_bytes().expect("decode GHOSTSNP").to_vec())
            }
            _ => None,
        })
        .expect("Snapshot payload");
    let mut projection = botster_terminal_ghostty::GhosttyClientProjection::new(
        botster_core::TerminalScreenSize::new(24, 80),
    )
    .expect("create client projection");
    projection
        .install_ghostsnp(&payload)
        .expect("install production GHOSTSNP before mode read");
    let mode_flags = connection
        .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: new_session.to_string(),
        })
        .expect("read modes after GHOSTSNP install");
    assert_eq!(
        mode_flags.kind,
        botster_hub_client::DaemonResponseKind::ReadModeFlags
    );
    let generation = mode_flags
        .mode_flags
        .expect("mode flags body")
        .mode_generation;
    assert!((1..=((1_u64 << 53) - 1)).contains(&generation));

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: new_session.to_string(),
        },
    )
    .expect("shutdown post-update session");
    let shutdown = Command::new(source_target.join("botster-hub"))
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .output()
        .expect("shutdown updated daemon");
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
}

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(root)
        .status()
        .expect("run fixture git command");
    assert!(status.success(), "git {}", args.join(" "));
}

fn git_output(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .expect("run fixture git command");
    assert!(output.status.success(), "git {}", args.join(" "));
    String::from_utf8(output.stdout).unwrap().trim().to_string()
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
        format!(
            "#!/bin/sh\nif [ -n \"$BOTSTER_UPDATE_TEST_MARKER\" ]; then touch \"$BOTSTER_UPDATE_TEST_MARKER\"; fi\nif [ \"$1\" = update ]; then exit 0; fi\nexit {build_status}\n"
        ),
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

fn create_real_build_update_source(root: &Path) -> PathBuf {
    let remote = root.join("real-build-remote.git");
    assert!(
        Command::new("git")
            .args(["init", "--bare"])
            .arg(&remote)
            .status()
            .unwrap()
            .success()
    );
    let source = root.join("real-build-source");
    fs::create_dir_all(source.join("fake-bin")).unwrap();
    git(&source, &["init"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
        source.join("Cargo.lock"),
    )
    .unwrap();
    fs::write(source.join(".gitignore"), "target/\n").unwrap();
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let cargo_script = source.join("fake-bin/cargo");
    fs::write(
        &cargo_script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = update ]; then exit 0; fi\nexec '{}' \"$@\" --manifest-path '{}' --target-dir '{}'\n",
            cargo,
            manifest.display(),
            source.join("target").display()
        ),
    )
    .unwrap();
    fs::set_permissions(&cargo_script, fs::Permissions::from_mode(0o755)).unwrap();
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

fn read_worker_identity(data_dir: &Path, session_id: &str) -> (u32, PathBuf) {
    let record: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join("sessions").join(format!("{session_id}.json"))).unwrap(),
    )
    .unwrap();
    let recovery = &record["recovery_identity"];
    (
        recovery["worker_pid"].as_u64().unwrap() as u32,
        PathBuf::from(recovery["worker_control_socket"].as_str().unwrap()),
    )
}

fn wait_for_process_exit(pid: u32) {
    for _ in 0..400 {
        let exists = unsafe { libc::kill(pid as libc::pid_t, 0) } == 0;
        if !exists {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("process {pid} did not exit");
}

fn collect_attach_events(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: &str,
) -> Vec<botster_hub_client::DaemonEvent> {
    let mut events = Vec::new();
    for _ in 0..200 {
        let response = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: session_id.to_string(),
            })
            .expect("drain attach events");
        events.extend(response.events);
        if events.iter().any(|event| {
            matches!(event,
            botster_hub_client::DaemonEvent::AttachState { subscription_id: id, state, .. }
                if id == subscription_id && state == "attached")
        }) {
            return events;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("attach did not complete: {events:?}");
}

fn event_position(
    events: &[botster_hub_client::DaemonEvent],
    subscription_id: &str,
    expected_state: &str,
) -> usize {
    events
        .iter()
        .position(|event| {
            matches!(event,
            botster_hub_client::DaemonEvent::AttachState { subscription_id: id, state, .. }
                if id == subscription_id && state == expected_state)
        })
        .unwrap_or_else(|| panic!("missing {expected_state} event: {events:?}"))
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

fn start_detached_daemon_with_update_source(
    hub_bin: &Path,
    worker_bin: &Path,
    data_dir: &Path,
    source: &Path,
    root: &Path,
) -> u32 {
    let pid_file = root.join("daemon-api.pid");
    let path_prefix = source.join("fake-bin");
    let path_assignment = if path_prefix.is_dir() {
        format!("PATH={}:$PATH ", path_prefix.display())
    } else {
        String::new()
    };
    let command = format!(
        "BOTSTER_ENV=test BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT={} {path_assignment}{} start --data-dir {} --session-worker-bin {} >/dev/null 2>&1 & echo $! > {}",
        source.display(),
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
    let stable_data_directory = data_dir
        .canonicalize()
        .unwrap_or_else(|_| data_dir.to_path_buf());
    let metadata = serde_json::json!({
        "pid": pid,
        "data_directory": stable_data_directory.to_string_lossy(),
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

fn wait_for_update_execution(
    endpoint: &botster_hub_client::DaemonEndpoint,
    expected: botster_hub_client::DaemonHubUpdateExecutionState,
) -> botster_hub_client::DaemonHubUpdateExecution {
    for _ in 0..8_000 {
        let Ok(response) = botster_hub_client::request(
            endpoint,
            botster_hub_client::DaemonRequest::GetHubUpdateExecution,
        ) else {
            thread::sleep(Duration::from_millis(25));
            continue;
        };
        let execution = response
            .hub_update_execution
            .expect("Hub update execution body");
        if execution.state == expected {
            return execution;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("Hub update execution did not reach {expected:?}");
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
