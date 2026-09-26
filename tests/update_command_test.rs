#![cfg(unix)]

use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod support;
use botster_terminal_protocol::TerminalKind;
use support::{candidate_hub_binary_path, candidate_session_worker_binary_path};

/// Bound for a fixture daemon to report ready or to exit.
const DAEMON_BUDGET: Duration = Duration::from_secs(30);
/// Bound for a detached updater to finish a fixture update.
const UPDATE_BUDGET: Duration = Duration::from_secs(600);

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
            .contains("usage: botster-hub update <core|all> [--source <path>] [--data-dir <path>]")
    );
}

#[test]
fn update_rejects_a_dirty_source_repository_through_the_production_cli() {
    let root = unique_test_dir("dirty-source");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    let source = create_plain_source(&root);
    fs::write(source.join("operator-change"), "preserve\n").expect("write dirty fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .args(["update", "core", "--source"])
        .arg(&source)
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
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
    let root = unique_test_dir("daemon-api-update");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    let source = create_plain_source(&root);
    fs::write(source.join("operator-change"), "preserve\n").unwrap();

    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let worker_bin = candidate_session_worker_binary_path()
        .canonicalize()
        .unwrap();
    // The local user fixes the source root at daemon start; no request names it.
    let daemon = FixtureDaemon::start(
        &hub_bin,
        &worker_bin,
        &data_dir,
        &home,
        &[OsStr::new("--update-source-root"), source.as_os_str()],
        None,
    );
    write_runtime_metadata(&data_dir, &data_dir, &hub_bin, &worker_bin, daemon.pid);
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

    wait_for_updater(accepted.updater_pid);
    let failed = read_update_execution(&endpoint);
    assert_eq!(failed.update_id, accepted.update_id);
    assert_eq!(
        failed.state,
        botster_hub_client::DaemonHubUpdateExecutionState::Failed
    );
    // A lost root fails `source_root_required` under BOTSTER_ENV=test; this
    // failure comes from the fixture checkout.
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
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    assert!(daemon.wait_exit().success());
}

#[test]
fn update_build_failure_leaves_the_running_daemon_unchanged() {
    let root = unique_test_dir("build-failure");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, false);
    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let worker_bin = candidate_session_worker_binary_path()
        .canonicalize()
        .unwrap();
    let daemon = FixtureDaemon::start(&hub_bin, &worker_bin, &data_dir, &home, &[], None);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    write_runtime_metadata(
        &data_dir,
        &data_directory_arg,
        &hub_bin,
        &worker_bin,
        daemon.pid,
    );

    let output = Command::new(&hub_bin)
        .args(["update", "core", "--source"])
        .arg(&source)
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .env("PATH", fake_bin_path(&source))
        .output()
        .expect("run build-failing update");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("build Hub failed"), "{stderr}");
    assert!(
        daemon.running(),
        "old daemon stopped after a pre-stop build failure"
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(persisted["pid"].as_u64(), Some(u64::from(daemon.pid)));
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let status =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status).unwrap();
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    fs::remove_file(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap();
    let shutdown = Command::new(&hub_bin)
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    assert!(daemon.wait_exit().success());
}

#[test]
fn update_replaces_the_daemon_and_the_replacement_keeps_the_selected_source() {
    let root = unique_test_dir("replace-verification");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, true);
    let source_target = source.join("target/debug");
    fs::create_dir_all(&source_target).unwrap();
    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let worker_bin = candidate_session_worker_binary_path()
        .canonicalize()
        .unwrap();
    fs::copy(&hub_bin, source_target.join("botster-hub")).unwrap();
    fs::copy(&worker_bin, source_target.join("botster-session-worker")).unwrap();

    let old_daemon = FixtureDaemon::start(&hub_bin, &worker_bin, &data_dir, &home, &[], None);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    write_runtime_metadata(
        &data_dir,
        &data_directory_arg,
        &hub_bin,
        &worker_bin,
        old_daemon.pid,
    );

    // Installed before the update runs, so any later panic still stops the
    // daemon the update starts.
    let _replacement = EndpointShutdownGuard {
        hub_bin: hub_bin.clone(),
        data_dir: data_dir.clone(),
        home: home.clone(),
    };
    let output = Command::new(&hub_bin)
        .args(["update", "core", "--source"])
        .arg(&source)
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .env("PATH", fake_bin_path(&source))
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
    assert_ne!(
        new_pid, old_daemon.pid,
        "update silently reused the old daemon"
    );
    assert!(old_daemon.wait_exit().success());
    // The update waited for the replacement's readiness before it returned.
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let status =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status).unwrap();
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    // The replacement daemon keeps the checkout the operator selected.
    let command = Command::new("ps")
        .args(["-p", &new_pid.to_string(), "-o", "command="])
        .output()
        .unwrap();
    let command = String::from_utf8_lossy(&command.stdout);
    let canonical_source = source.canonicalize().unwrap();
    assert!(
        command.contains(&format!(
            "--update-source-root {}",
            canonical_source.display()
        )),
        "{command}"
    );
    // A second update through the replacement reaches that same checkout: a
    // lost root would fail `source_root_required` under BOTSTER_ENV=test.
    fs::write(source.join("second-update-marker"), "dirty\n").unwrap();
    let accepted = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartHubUpdate {
            scope: botster_hub_client::DaemonHubUpdateScope::Core,
        },
    )
    .unwrap()
    .hub_update_execution
    .expect("accepted second update");
    wait_for_updater(accepted.updater_pid);
    let second = read_update_execution(&endpoint);
    assert_eq!(second.update_id, accepted.update_id);
    assert!(
        second
            .error
            .as_deref()
            .is_some_and(|error| error.contains("repository is dirty")),
        "{second:?}"
    );

    let shutdown = Command::new(source_target.join("botster-hub"))
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    wait_for_pid_exit(new_pid);
}

#[test]
fn update_all_missing_package_contract_leaves_the_running_daemon_unchanged() {
    let root = unique_test_dir("all-missing-contract");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_clean_update_source(&root, true);
    let package = create_direct_local_package(&root);
    let head_before = git_output(&source, &["rev-parse", "HEAD"]);
    let lock_before = fs::read(source.join("Cargo.lock")).unwrap();
    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let worker_bin = candidate_session_worker_binary_path()
        .canonicalize()
        .unwrap();
    let daemon = FixtureDaemon::start(&hub_bin, &worker_bin, &data_dir, &home, &[], None);
    let data_directory_arg = data_dir.clone();
    let data_dir = data_dir.canonicalize().unwrap();
    write_runtime_metadata(
        &data_dir,
        &data_directory_arg,
        &hub_bin,
        &worker_bin,
        daemon.pid,
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
        let output = Command::new(&hub_bin)
            .args(args)
            .env("HOME", &home)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let output = Command::new(&hub_bin)
        .args(["update", "all", "--source"])
        .arg(&source)
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .env("BOTSTER_UPDATE_TEST_MARKER", root.join("cargo-was-run"))
        .env("PATH", fake_bin_path(&source))
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
    assert!(daemon.running());
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["pid"].as_u64(), Some(u64::from(daemon.pid)));

    fs::remove_file(data_dir.join(".botster-hub-runtime-daemon.json")).unwrap();
    let shutdown = Command::new(&hub_bin)
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    assert!(daemon.wait_exit().success());
}

/// A managed installation refuses the source update at every entry point
/// before it takes a lock or runs a command. The unmanaged classification is
/// the release-build fallback, which no test binary is; its refusal is proven
/// at unit level only.
#[test]
fn managed_installation_refuses_the_source_update_at_every_entry_point() {
    let root = unique_test_dir("managed-refusal");
    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let worker_bin = candidate_session_worker_binary_path()
        .canonicalize()
        .unwrap();
    let home = fixture_home(&root);
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().unwrap()).unwrap();
    fs::write(
        &receipt,
        serde_json::to_vec(&managed_receipt(&hub_bin)).unwrap(),
    )
    .unwrap();
    let source = create_plain_source(&root);
    let source_lock = source.join(".git/.botster-update.lock");
    let refused = |text: &str| {
        text.contains("reason=managed_installation") && text.contains("action=managed_release")
    };

    // CLI update.
    let data_dir = root.join("cli-data");
    let output = Command::new(&hub_bin)
        .args(["update", "core", "--source"])
        .arg(&source)
        .arg("--data-dir")
        .arg(&data_dir)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(refused(&stderr), "{stderr}");
    assert!(!source_lock.exists(), "no source lock before the refusal");

    // Daemon start with a source root. A start that does not refuse would
    // serve until stopped, so its exit is awaited under a deadline.
    let mut start = Command::new(&hub_bin)
        .args(["start", "--data-dir"])
        .arg(root.join("start-data"))
        .arg("--session-worker-bin")
        .arg(&worker_bin)
        .arg("--update-source-root")
        .arg(&source)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let exited =
        botster_hub::process_exit::wait_for_pid_exit(start.id(), Instant::now() + DAEMON_BUDGET)
            .expect("watch start exit");
    if !exited {
        let _ = start.kill();
    }
    let output = start.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(exited, "start with a source root did not refuse: {stderr}");
    assert!(!output.status.success());
    assert!(refused(&stderr), "{stderr}");

    // Daemon StartHubUpdate.
    let data_dir = root.join("daemon-data");
    let daemon = FixtureDaemon::start(&hub_bin, &worker_bin, &data_dir, &home, &[], None);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let response = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartHubUpdate {
            scope: botster_hub_client::DaemonHubUpdateScope::Core,
        },
    )
    .unwrap();
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = response.error.expect("typed refusal");
    assert_eq!(error.code, "hub_update_unavailable");
    assert!(refused(&error.message), "{error:?}");
    assert!(
        response.hub_update_execution.is_none(),
        "no updater was started"
    );
    assert!(
        !data_dir.join(".botster-hub-update-execution.json").exists(),
        "no update execution was recorded"
    );
    let shutdown = Command::new(&hub_bin)
        .args(["shutdown", "--data-dir"])
        .arg(&data_dir)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    assert!(daemon.wait_exit().success());

    // The detached updater.
    let data_dir = root.join("handoff-data");
    fs::create_dir_all(&data_dir).unwrap();
    let execution = botster_hub_client::DaemonHubUpdateExecution {
        update_id: "managed-handoff".to_string(),
        scope: botster_hub_client::DaemonHubUpdateScope::Core,
        state: botster_hub_client::DaemonHubUpdateExecutionState::Started,
        updater_pid: std::process::id(),
        error: None,
    };
    fs::write(
        data_dir.join(".botster-hub-update-execution.json"),
        serde_json::to_vec(&execution).unwrap(),
    )
    .unwrap();
    let mut handoff = Command::new(&hub_bin)
        .args(["__update-handoff", "core", "--data-dir"])
        .arg(&data_dir)
        .args(["--update-id", "managed-handoff", "--source"])
        .arg(&source)
        .env("BOTSTER_ENV", "test")
        .env("HOME", &home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    handoff
        .stdin
        .take()
        .unwrap()
        .write_all(b"g")
        .expect("release the handoff gate");
    let output = handoff.wait_with_output().unwrap();
    assert!(!output.status.success());
    let recorded: botster_hub_client::DaemonHubUpdateExecution = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-update-execution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        recorded.state,
        botster_hub_client::DaemonHubUpdateExecutionState::Failed
    );
    assert!(
        recorded.error.as_deref().is_some_and(refused),
        "{recorded:?}"
    );
    assert!(!source_lock.exists(), "no source lock before the refusal");
}

#[test]
#[ignore = "needs BOTSTER_PREUPDATE_WORKER_BIN from script/test-update-preupdate-worker, which must not run in a main checkout; do not run"]
fn update_all_replaces_an_incompatible_preupdate_worker_and_proves_attach_order() {
    let preupdate_worker = PathBuf::from(
        std::env::var_os("BOTSTER_PREUPDATE_WORKER_BIN")
            .expect("script must supply the real pre-update worker"),
    )
    .canonicalize()
    .expect("resolve pre-update worker");
    let root = unique_test_dir("preupdate-worker");
    let home = fixture_home(&root);
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let source = create_real_build_update_source(&root);
    let source_target = source.join("target/debug");
    let hub_bin = candidate_hub_binary_path().canonicalize().unwrap();
    let old_daemon = FixtureDaemon::start(
        &hub_bin,
        &preupdate_worker,
        &data_dir,
        &home,
        &[OsStr::new("--update-source-root"), source.as_os_str()],
        Some(&source.join("fake-bin")),
    );
    let data_directory_arg = data_dir.clone();
    write_runtime_metadata(
        &data_directory_arg,
        &data_directory_arg,
        &hub_bin,
        &preupdate_worker,
        old_daemon.pid,
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
        || old_probe
            .mode_flags
            .as_ref()
            .is_none_or(|flags| flags.unavailable.is_some());
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
    wait_for_updater(accepted.updater_pid);
    let completed = read_update_execution(&endpoint);
    assert_eq!(completed.update_id, accepted.update_id);
    assert_eq!(
        completed.state,
        botster_hub_client::DaemonHubUpdateExecutionState::Complete,
        "{completed:?}"
    );
    let update_log = fs::read_to_string(
        data_dir.join(format!(".botster-hub-update-{}.log", accepted.update_id)),
    )
    .expect("read detached updater log");
    // The updater reports live workers and preserves them; it never
    // terminates an old worker or removes its socket.
    assert!(
        update_log.contains("\"action\":\"report\"") && update_log.contains(old_session),
        "updater must report the live pre-update worker: {update_log}"
    );
    assert!(
        unsafe { libc::kill(old_identity.0 as libc::pid_t, 0) } == 0,
        "pre-update worker {} must be preserved across the update",
        old_identity.0
    );
    assert!(
        old_identity.1.exists(),
        "pre-update worker socket must be preserved across the update"
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
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: new_session.to_string(),
            subscription_id: "postupdate-attach".to_string(),
        })
        .expect("attach updated session");
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    let frames = collect_attach_frames(&mut connection, new_session, "postupdate-attach");
    let kinds: Vec<TerminalKind> = frames.iter().map(|frame| frame.kind()).collect();
    let attached = kinds
        .iter()
        .position(|kind| *kind == TerminalKind::AttachState)
        .expect("production attach state frame");
    let ready = kinds
        .iter()
        .position(|kind| *kind == TerminalKind::SnapshotReady)
        .expect("production SNAPSHOT_READY");
    let finish = kinds
        .iter()
        .position(|kind| *kind == TerminalKind::SnapshotFinish)
        .expect("production SNAPSHOT_FINISH");
    assert!(attached < ready && ready < finish, "{kinds:?}");
    let payload: Vec<u8> = frames
        .iter()
        .filter(|frame| frame.kind() == TerminalKind::SnapshotHistory)
        .flat_map(|frame| frame.body().to_vec())
        .collect();
    assert!(
        !payload.is_empty(),
        "production attach carries GHOSTSNP history"
    );
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
    assert!(
        mode_flags
            .mode_flags
            .expect("mode flags body")
            .unavailable
            .is_none(),
        "updated worker reads modes after the GHOSTSNP install"
    );

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
        .env("HOME", &home)
        .output()
        .expect("shutdown updated daemon");
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
}

/// A `botster-hub start` child that reported ready on its `--ready-fd` pipe.
/// A thread reaps it as soon as it exits, so an updater that waits for the
/// old daemon's reap never waits on the test. Dropping it while the daemon
/// still runs kills and reaps it through its own `Child`, so a failed test
/// never leaves it behind and never signals a reused pid.
struct FixtureDaemon {
    pid: u32,
    child: std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
    exited: mpsc::Receiver<ExitStatus>,
}

impl FixtureDaemon {
    fn start(
        hub_bin: &Path,
        worker_bin: &Path,
        data_dir: &Path,
        home: &Path,
        extra_args: &[&OsStr],
        path_prefix: Option<&Path>,
    ) -> Self {
        const READY_FD: libc::c_int = 3;
        let mut command = Command::new(hub_bin);
        command
            .arg("start")
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--session-worker-bin")
            .arg(worker_bin)
            .args(extra_args)
            .arg("--ready-fd")
            .arg(READY_FD.to_string())
            .env("BOTSTER_ENV", "test")
            .env("HOME", home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                fs::File::create(daemon_stderr_path(data_dir)).expect("daemon stderr log"),
            ));
        if let Some(prefix) = path_prefix {
            command.env(
                "PATH",
                format!(
                    "{}:{}",
                    prefix.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            );
        }
        let (reader, writer) = io::pipe().expect("create readiness pipe");
        let writer_fd = writer.as_raw_fd();
        unsafe {
            // SAFETY: fcntl and dup2 are async-signal-safe; the write end
            // lands at READY_FD without close-on-exec.
            command.pre_exec(move || {
                let placed = if writer_fd == READY_FD {
                    libc::fcntl(READY_FD, libc::F_SETFD, 0)
                } else {
                    libc::dup2(writer_fd, READY_FD)
                };
                if placed == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().expect("spawn fixture daemon");
        // Only the child may hold the write end, so its exit closes the pipe.
        drop(writer);
        let (line_tx, line_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let _ = io::BufReader::new(reader).read_line(&mut line);
            let _ = line_tx.send(line);
        });
        // timer: deadline — the ready line or the pipe's EOF ends the wait.
        let line = line_rx.recv_timeout(DAEMON_BUDGET);
        if !matches!(&line, Ok(line) if !line.is_empty()) {
            let _ = child.kill();
            let status = child.wait().expect("reap failed daemon");
            panic!(
                "fixture daemon did not report ready ({line:?}): {status} {}",
                fs::read_to_string(daemon_stderr_path(data_dir)).unwrap_or_default()
            );
        }
        let pid = child.id();
        let child = std::sync::Arc::new(std::sync::Mutex::new(Some(child)));
        let (exit_tx, exited) = mpsc::channel();
        thread::spawn({
            let child = std::sync::Arc::clone(&child);
            move || {
                // The exit event does not reap; the child stays ours until
                // the wait below, so Drop's kill can never reach another
                // process. The long bound only caps a thread nobody waits on.
                // Reap only on a confirmed exit. On a watch error or expiry
                // the Child stays for Drop to kill and reap.
                if !matches!(
                    botster_hub::process_exit::wait_for_pid_exit(
                        pid,
                        Instant::now() + Duration::from_secs(24 * 60 * 60),
                    ),
                    Ok(true)
                ) {
                    return;
                }
                let reaped = child
                    .lock()
                    .expect("fixture child")
                    .take()
                    .and_then(|mut child| child.wait().ok());
                if let Some(status) = reaped {
                    let _ = exit_tx.send(status);
                }
            }
        });
        Self { pid, child, exited }
    }

    /// Whether the daemon has not exited.
    fn running(&self) -> bool {
        matches!(self.exited.try_recv(), Err(mpsc::TryRecvError::Empty))
    }

    fn wait_exit(self) -> ExitStatus {
        // timer: deadline — the reaper's exit report ends the wait.
        self.exited
            .recv_timeout(DAEMON_BUDGET)
            .expect("fixture daemon exits")
    }
}

impl Drop for FixtureDaemon {
    fn drop(&mut self) {
        let Ok(mut child) = self.child.lock() else {
            return;
        };
        if let Some(mut running) = child.take()
            && matches!(running.try_wait(), Ok(None))
        {
            let _ = running.kill();
            let _ = running.wait();
        }
    }
}

/// Stops whatever daemon serves this test's private data directory, through
/// its socket, never by pid: the daemon an update starts is not this test's
/// child. A shutdown with no daemon listening is a harmless failed request.
struct EndpointShutdownGuard {
    hub_bin: PathBuf,
    data_dir: PathBuf,
    home: PathBuf,
}

impl Drop for EndpointShutdownGuard {
    fn drop(&mut self) {
        let _ = Command::new(&self.hub_bin)
            .args(["shutdown", "--data-dir"])
            .arg(&self.data_dir)
            .env("HOME", &self.home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn daemon_stderr_path(data_dir: &Path) -> PathBuf {
    let parent = data_dir.parent().unwrap_or(data_dir);
    parent.join(format!(
        "{}-daemon-stderr.log",
        data_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    ))
}

/// Waits on the updater's exit event; it records its outcome before exiting.
fn wait_for_updater(pid: u32) {
    assert!(
        botster_hub::process_exit::wait_for_pid_exit(pid, Instant::now() + UPDATE_BUDGET)
            .expect("watch updater exit"),
        "updater {pid} did not exit"
    );
}

fn wait_for_pid_exit(pid: u32) {
    assert!(
        botster_hub::process_exit::wait_for_pid_exit(pid, Instant::now() + DAEMON_BUDGET)
            .expect("watch daemon exit"),
        "process {pid} did not exit"
    );
}

fn read_update_execution(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> botster_hub_client::DaemonHubUpdateExecution {
    botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::GetHubUpdateExecution,
    )
    .expect("read Hub update execution")
    .hub_update_execution
    .expect("Hub update execution body")
}

/// An empty HOME, so the installation classification never depends on the
/// host: no receipt means a development build.
fn fixture_home(root: &Path) -> PathBuf {
    let home = root.join("home");
    fs::create_dir_all(&home).expect("create fixture home");
    home
}

/// A schema-2 managed receipt the candidate binary accepts.
fn managed_receipt(hub_bin: &Path) -> serde_json::Value {
    let version = Command::new(hub_bin)
        .arg("version")
        .output()
        .expect("candidate Hub version");
    assert!(version.status.success());
    let version = String::from_utf8(version.stdout).expect("version is UTF-8");
    let embedded = version
        .lines()
        .find_map(|line| line.strip_prefix("build_revision="))
        .expect("candidate reports its build revision");
    let build_revision = if embedded == "unknown" {
        "release1"
    } else {
        embedded
    };
    serde_json::json!({
        "schema_version": 2,
        "product_id": "botster-hub",
        "binary_version": env!("CARGO_PKG_VERSION"),
        "installation_mode": "managed",
        "release_channel": "stable",
        "provider": "http_json",
        "source_url": "http://127.0.0.1:9/botster-hub.json",
        "build_revision": build_revision,
        "artifacts": [
            {"name": "botster-hub", "sha256": "a".repeat(64), "size": 1024},
            {"name": "botster-session-worker", "sha256": "b".repeat(64), "size": 2048}
        ],
        "source_revisions": {
            "botster_hub": "0".repeat(40),
            "botster_core": "1".repeat(40)
        },
        "signature": {
            "algorithm": "ed25519",
            "key_id": "test-only-do-not-trust",
            "signed_manifest_sha256": "c".repeat(64)
        },
        "installer": {"id": "botster-hub-installer", "version": "0.1.0"}
    })
}

fn fake_bin_path(source: &Path) -> String {
    format!(
        "{}:{}",
        source.join("fake-bin").display(),
        std::env::var("PATH").unwrap_or_default()
    )
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

/// The manifest a source-update checkout must carry.
const HUB_MANIFEST: &str = "[package]\nname = \"botster-hub\"\nversion = \"0.1.0\"\n";

/// A committed `botster-hub` checkout with no remote.
fn create_plain_source(root: &Path) -> PathBuf {
    let source = root.join("source");
    fs::create_dir_all(&source).expect("create source fixture");
    git(&source, &["init", "-q", "-b", "main"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(source.join("Cargo.toml"), HUB_MANIFEST).unwrap();
    fs::write(source.join("tracked"), "clean\n").expect("write tracked fixture");
    git(&source, &["add", "Cargo.toml", "tracked"]);
    git(&source, &["commit", "-q", "-m", "fixture"]);
    source
}

fn create_clean_update_source(root: &Path, builds_succeed: bool) -> PathBuf {
    let remote = root.join("remote.git");
    let status = Command::new("git")
        .args(["init", "-q", "--bare"])
        .arg(&remote)
        .status()
        .unwrap();
    assert!(status.success());
    let source = root.join("source");
    fs::create_dir_all(source.join("fake-bin")).unwrap();
    git(&source, &["init", "-q", "-b", "main"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(source.join("Cargo.toml"), HUB_MANIFEST).unwrap();
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
        &[
            "add",
            ".gitignore",
            "Cargo.toml",
            "Cargo.lock",
            "fake-bin/cargo",
        ],
    );
    git(&source, &["commit", "-q", "-m", "fixture"]);
    let remote_text = remote.to_string_lossy().into_owned();
    git(&source, &["remote", "add", "origin", &remote_text]);
    git(&source, &["push", "-q", "-u", "origin", "main"]);
    source
}

fn create_real_build_update_source(root: &Path) -> PathBuf {
    let remote = root.join("real-build-remote.git");
    assert!(
        Command::new("git")
            .args(["init", "-q", "--bare"])
            .arg(&remote)
            .status()
            .unwrap()
            .success()
    );
    let source = root.join("real-build-source");
    fs::create_dir_all(source.join("fake-bin")).unwrap();
    git(&source, &["init", "-q", "-b", "main"]);
    git(
        &source,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&source, &["config", "user.name", "Update Test"]);
    fs::write(source.join("Cargo.toml"), HUB_MANIFEST).unwrap();
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
        &[
            "add",
            ".gitignore",
            "Cargo.toml",
            "Cargo.lock",
            "fake-bin/cargo",
        ],
    );
    git(&source, &["commit", "-q", "-m", "fixture"]);
    let remote_text = remote.to_string_lossy().into_owned();
    git(&source, &["remote", "add", "origin", &remote_text]);
    git(&source, &["push", "-q", "-u", "origin", "main"]);
    source
}

fn read_worker_identity(data_dir: &Path, session_id: &str) -> (u32, PathBuf) {
    // Core owns the registry filename encoding; read the record through its identity-checked
    // loader rather than a privately constructed path.
    let record = botster_core_daemon::SessionRegistry::new(data_dir)
        .load(&botster_core::SessionId(session_id.to_string()))
        .unwrap()
        .unwrap();
    let recovery = record.recovery_identity.unwrap();
    (
        recovery["worker_pid"].as_u64().unwrap() as u32,
        PathBuf::from(recovery["worker_control_socket"].as_str().unwrap()),
    )
}

/// Read the attach stream on one route until SNAPSHOT_FINISH. Each read
/// blocks until a frame arrives or the one deadline passes.
fn collect_attach_frames(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: &str,
) -> Vec<botster_terminal_protocol::TerminalFrame> {
    let mut frames = Vec::new();
    // timer: deadline — each frame arrival ends a read.
    let deadline = Instant::now() + Duration::from_secs(5);
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        let Ok(Some(frame)) = connection.poll_terminal(remaining) else {
            break;
        };
        if frame.route != subscription_id {
            continue;
        }
        if let Ok(decoded) = botster_terminal_protocol::TerminalFrame::from_bytes(&frame.body) {
            let finished = decoded.kind() == TerminalKind::SnapshotFinish;
            frames.push(decoded);
            if finished {
                return frames;
            }
        }
    }
    panic!(
        "attach history did not finish for {session_id}: {:?}",
        frames.iter().map(|frame| frame.kind()).collect::<Vec<_>>()
    );
}

fn create_direct_local_package(root: &Path) -> PathBuf {
    let remote = root.join("package-remote.git");
    assert!(
        Command::new("git")
            .args(["init", "-q", "--bare"])
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
    git(&package, &["init", "-q", "-b", "main"]);
    git(
        &package,
        &["config", "user.email", "update-test@example.invalid"],
    );
    git(&package, &["config", "user.name", "Update Test"]);
    git(&package, &["add", "botster-package.json", "plugin.lua"]);
    git(&package, &["commit", "-q", "-m", "fixture"]);
    let remote_text = remote.to_string_lossy().into_owned();
    git(&package, &["remote", "add", "origin", &remote_text]);
    git(&package, &["push", "-q", "-u", "origin", "main"]);
    package
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

fn unique_test_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let path = PathBuf::from("/tmp").join(format!("bhu-{label}-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).expect("create unique test directory");
    path
}
