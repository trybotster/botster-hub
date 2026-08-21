fn wait_for_registry_worker(data_dir: &Path) -> RegistryWorkerIdentity {
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = registry_backed_worker_identities(data_dir).unwrap_or_default();
        if let Some(identity) = last.iter().find(|identity| identity.pid.is_some()) {
            return identity.clone();
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("registry did not list a worker pid under {}: {last:?}", data_dir.display());
}

#[test]
fn unique_test_dirs_include_process_id() {
    let first = unique_test_dir("pid-unique");
    let second = unique_test_dir("pid-unique");
    assert_ne!(first, second, "unique_test_dir must not collide");
    let short = unique_short_test_dir("pid-unique");
    assert!(
        short
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(&std::process::id().to_string())),
        "unique_short_test_dir must include pid: {}",
        short.display()
    );
}

#[test]
fn harness_budget_marker_classifies_emfile_as_host_exhaustion() {
    let error = std::io::Error::from_raw_os_error(libc::EMFILE);
    let (resource, probe) = classify_budget_expiry("child_condition", Some(&error), None);
    assert_eq!(resource, HostResourceClass::Emfile);
    assert_eq!(probe, ResourceProbe::NotApplicable);
    let marker = format_harness_budget_expired(
        "child_condition",
        Duration::from_millis(10),
        resource,
        probe,
        "unit",
    );
    assert!(marker.contains("resource=EMFILE"));
    assert!(marker.contains("probe=n/a"));
    assert!(marker.contains("test=harness_budget_marker_classifies_emfile_as_host_exhaustion"));
}

#[test]
fn real_emfile_child_classifies_as_host_exhaustion() {
    let output = Command::new("ruby")
        .arg("-e")
        .arg(
            r#"
Process.setrlimit(Process::RLIMIT_NOFILE, 16, 16)
fds = []
begin
  loop { fds << File.open("/dev/null") }
rescue SystemCallError => error
  STDOUT.write(error.errno.to_s)
end
"#,
        )
        .output()
        .expect("run lowered-RLIMIT_NOFILE child");
    assert!(
        output.status.success(),
        "EMFILE child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let errno: i32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("EMFILE child printed non-errno: {:?}", output.stdout));
    assert_eq!(errno, libc::EMFILE, "lowered RLIMIT_NOFILE child must hit EMFILE");
    let error = std::io::Error::from_raw_os_error(errno);
    let (resource, probe) = classify_budget_expiry("child_condition", Some(&error), None);
    assert_eq!(resource, HostResourceClass::Emfile);
    let marker = format_harness_budget_expired(
        "child_condition",
        Duration::from_millis(10),
        resource,
        probe,
        "real child open",
    );
    assert!(marker.contains("resource=EMFILE"));
    assert!(marker.contains("harness_budget_expired"));
}

#[test]
fn harness_budget_marker_keeps_socket_eagain_as_product_failure() {
    let error = std::io::Error::from_raw_os_error(libc::EAGAIN);
    let (resource, probe) = classify_budget_expiry("entity_frame", Some(&error), None);
    assert_eq!(resource, HostResourceClass::AmbiguousSocket);
    assert_ne!(probe, ResourceProbe::Confirmed);
    let marker = format_harness_budget_expired(
        "entity_frame",
        Duration::from_secs(5),
        resource,
        probe,
        "socket stall",
    );
    assert!(marker.contains("resource=EAGAIN"));
    assert!(!marker.contains("probe=confirmed"));
}

#[test]
fn harness_budget_marker_keeps_readiness_etimedout_as_product_failure() {
    let error = std::io::Error::from_raw_os_error(libc::ETIMEDOUT);
    let (resource, probe) = classify_budget_expiry("child_condition", Some(&error), None);
    assert_eq!(resource, HostResourceClass::AmbiguousReadiness);
    assert_ne!(probe, ResourceProbe::Confirmed);
}

#[test]
fn pty_allocation_errno_set_is_frozen_per_platform() {
    let errnos = pty_allocation_errnos();
    assert!(errnos.contains(&libc::EMFILE));
    assert!(errnos.contains(&libc::ENFILE));
    assert!(errnos.contains(&libc::EAGAIN));
    assert_eq!(
        classify_pty_allocation_source("posix_openpt: Too many open files (os error 24)"),
        HostResourceClass::Emfile
    );
}

#[test]
fn guard_cleanup_after_panic_reaps_worker_socket_and_hub() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gpr");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let endpoint = botster_hub_client::DaemonEndpoint::new(daemon_socket_path(&data_dir));
    let daemon = start_cli_daemon(&data_dir);
    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "guard-panic-session".to_string(),
            command: "sh -c 'sleep 30'".to_string(),
        },
    )
    .expect("spawn worker-backed session");
    assert_eq!(
        spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "spawn worker-backed session: {}",
        typed_operator_error_body(&spawn)
    );
    let identity = wait_for_registry_worker(&data_dir);
    let command_pid = identity.pid.expect("panic-test command pid");
    let worker_pid = worktree_session_worker_ancestor(command_pid).unwrap_or(command_pid);
    let command_pgid = process_snapshot(command_pid)
        .map(|snapshot| snapshot.pgid)
        .unwrap_or(command_pid);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _daemon = daemon;
        panic!("injected cleanup panic");
    }));
    assert!(result.is_err(), "injected panic must unwind");
    assert!(
        !daemon_socket_path(&data_dir).exists(),
        "panic cleanup must remove the daemon socket"
    );
    assert!(
        !process_exists(worker_pid),
        "panic cleanup must reap the session worker pid {worker_pid}"
    );
    assert!(
        !process_exists(command_pid),
        "panic cleanup must reap the PTY command pid {command_pid}"
    );
    assert!(
        !process_group_probe(command_pgid as libc::pid_t).expect("PTY group probe"),
        "panic cleanup must reap the PTY process group {command_pgid}"
    );
}

#[test]
fn guard_timeout_path_emits_budget_marker_and_reaps() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gtr");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let mut daemon = start_cli_daemon(&data_dir);
    let timed_out = wait_for_child_condition_with_budget(
        daemon.child_mut(),
        "injected timeout",
        Duration::from_millis(40),
        || false,
    );
    let error = timed_out.expect_err("timeout path");
    assert!(
        error.contains("harness_budget_expired"),
        "timeout must emit structured marker: {error}"
    );
    assert!(
        error.contains("test=guard_timeout_path_emits_budget_marker_and_reaps"),
        "timeout marker must name this test: {error}"
    );
}

#[test]
fn dead_daemon_backstop_reaps_registry_worker_without_adopting_foreign() {
    let _lock = daemon_test_guard();
    let data_dir_a = unique_short_test_dir("gba");
    let data_dir_b = unique_short_test_dir("gbb");
    fs::create_dir_all(&data_dir_a).expect("create A");
    fs::create_dir_all(&data_dir_b).expect("create B");
    let daemon_a = start_cli_daemon(&data_dir_a);
    let daemon_b = start_cli_daemon(&data_dir_b);
    let endpoint_a = botster_hub_client::DaemonEndpoint::new(daemon_socket_path(&data_dir_a));
    let endpoint_b = botster_hub_client::DaemonEndpoint::new(daemon_socket_path(&data_dir_b));
    botster_hub_client::request(
        &endpoint_a,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "backstop-a".to_string(),
                command: "sh -c 'sleep 30'".to_string(),
        },
    )
    .expect("spawn A");
    botster_hub_client::request(
        &endpoint_b,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "backstop-b".to_string(),
                command: "sh -c 'sleep 30'".to_string(),
        },
    )
    .expect("spawn B");
    let worker_a = wait_for_registry_worker(&data_dir_a);
    let worker_b = wait_for_registry_worker(&data_dir_b);
    let worker_a_pid = worker_a.pid.expect("A worker pid");
    wait_for_process_snapshot(worker_a_pid, "own setsid", |snapshot| {
        snapshot.pgid != daemon_a.id() && snapshot.sid != daemon_a.id().to_string()
    });
    let mut child_a = daemon_a.disarm();
    unsafe {
        libc::kill(child_a.id() as libc::pid_t, libc::SIGKILL);
    }
    let _ = child_a.wait();
    assert!(
        process_exists(worker_a_pid),
        "durable worker A must survive Hub SIGKILL before backstop"
    );
    let reaped = reap_registry_backed_workers(&data_dir_a).expect("A backstop");
    assert!(
        reaped.errors.is_empty(),
        "A backstop must verify worker ancestry: {:?}",
        reaped.errors
    );
    assert!(!reaped.reaped.is_empty(), "A backstop must reap its worker");
    let leftover_a = live_session_workers_for_data_dir(&data_dir_a).expect("A leftover");
    assert!(leftover_a.is_empty(), "A workers must be gone: {leftover_a:?}");
    let leftover_b = live_session_workers_for_data_dir(&data_dir_b).expect("B leftover");
    assert!(
        worker_b
            .pid
            .is_some_and(|pid| leftover_b.iter().any(|worker| worker.pid == pid)
                || process_exists(pid)),
        "foreign-worker adoption must not reap B: before={worker_b:?} leftover={leftover_b:?}"
    );
    drop(daemon_b);
}

#[test]
fn untokened_start_boundary_notify_is_ignored() {
    let _lock = daemon_test_guard();
    let token = next_real_daemon_start_token();
    let boundary = arm_real_daemon_start_boundary(token);
    notify_real_daemon_start_boundary();
    assert!(
        boundary.matched.try_recv().is_err(),
        "a thread without a start token must not match the armed boundary"
    );
    assert!(
        boundary.foreign.try_recv().is_err(),
        "a thread without a start token must not count as a foreign start"
    );
    set_real_daemon_start_token(token);
    notify_real_daemon_start_boundary();
    boundary
        .matched
        .recv_timeout(Duration::from_secs(1))
        .expect("tokened notify matches the armed boundary");
    clear_real_daemon_start_token();
}

fn taint_latch_refuses_next_daemon_start_without_spawning() {
    let _lock = daemon_test_guard();
    record_harness_taint("injected prove-absence failure");
    let data_dir = unique_short_test_dir("tnt");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let before = session_worker_process_identities().unwrap_or_default().len();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = start_cli_daemon(&data_dir);
    }));
    assert!(panicked.is_err(), "tainted start must panic");
    assert!(
        !daemon_socket_path(&data_dir).exists(),
        "tainted start must not create a daemon socket"
    );
    let after = session_worker_process_identities().unwrap_or_default().len();
    assert_eq!(before, after, "tainted start must not spawn workers");
    reset_harness_taint_after_proof();
}

#[test]
fn injected_taint_cannot_race_an_unguarded_real_daemon_start() {
    let lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let data_dir = unique_short_test_dir("gxt");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let token = next_real_daemon_start_token();
    let handle = {
        let _taint = ScopedHarnessTaint::inject("injected race taint");
        let boundary = arm_real_daemon_start_boundary(token);
        let start_dir = data_dir.clone();
        let handle = thread::spawn(move || {
            set_real_daemon_start_token(token);
            let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                start_cli_daemon(&start_dir)
            }));
            clear_real_daemon_start_token();
            started
        });
        boundary
            .matched
            .recv_timeout(Duration::from_secs(5))
            .expect("intended child reached the real-daemon start boundary");
        assert!(
            boundary.foreign.try_recv().is_err(),
            "intended start must not also count as a foreign start"
        );
        assert!(
            harness_taint().is_some_and(|evidence| evidence.contains("injected race taint")),
            "parent must still hold the injected taint at the start boundary: {:?}",
            harness_taint()
        );
        assert!(
            !daemon_socket_path(&data_dir).exists(),
            "concurrent start must wait at the daemon guard, not create a socket under taint"
        );
        handle
    };
    drop(lock);
    let started = handle.join().expect("start thread");
    disarm_real_daemon_start_boundary();
    let daemon = started.unwrap_or_else(|panic| {
        panic!("concurrent real-daemon start raced the injected taint: {panic:?}")
    });
    daemon.shutdown();
}

#[test]
fn injected_taint_race_fails_when_start_guard_is_bypassed() {
    let lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let _taint = ScopedHarnessTaint::inject("injected race taint");
    let data_dir = unique_short_test_dir("gxb");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let token = next_real_daemon_start_token();
    let boundary = arm_real_daemon_start_boundary(token);
    let start_dir = data_dir.clone();
    let handle = thread::spawn(move || {
        set_real_daemon_start_token(token);
        bypass_real_daemon_start_guard(true);
        let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            start_cli_daemon(&start_dir)
        }));
        bypass_real_daemon_start_guard(false);
        clear_real_daemon_start_token();
        started
    });
    boundary
        .matched
        .recv_timeout(Duration::from_secs(5))
        .expect("intended child reached the real-daemon start boundary");
    assert!(
        harness_taint().is_some_and(|evidence| evidence.contains("injected race taint")),
        "ablation must observe taint at the start boundary: {:?}",
        harness_taint()
    );
    let started = handle.join().expect("bypassed start thread");
    disarm_real_daemon_start_boundary();
    assert!(
        started.is_err(),
        "bypassing the start-path guard must panic on the injected taint"
    );
    assert!(
        !daemon_socket_path(&data_dir).exists(),
        "bypassed tainted start must not create a daemon socket"
    );
    drop(_taint);
    drop(lock);
}

#[test]
fn sibling_real_daemon_start_cannot_satisfy_intended_boundary_hook() {
    let lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let intended_dir = unique_short_test_dir("gxi");
    let sibling_dir = unique_short_test_dir("gxs");
    fs::create_dir_all(&intended_dir).expect("create intended data dir");
    fs::create_dir_all(&sibling_dir).expect("create sibling data dir");
    let intended_token = next_real_daemon_start_token();
    let sibling_token = next_real_daemon_start_token();
    let (sibling, intended) = {
        let _taint = ScopedHarnessTaint::inject("injected race taint");
        let boundary = arm_real_daemon_start_boundary(intended_token);
        let sibling_start = sibling_dir.clone();
        let sibling = thread::spawn(move || {
            set_real_daemon_start_token(sibling_token);
            let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                start_cli_daemon(&sibling_start)
            }));
            clear_real_daemon_start_token();
            started
        });
        boundary
            .foreign
            .recv_timeout(Duration::from_secs(5))
            .expect("sibling start reached the boundary without the intended token");
        assert!(
            boundary.matched.try_recv().is_err(),
            "a sibling first-acquire must not satisfy the intended start-boundary hook"
        );
        let intended_start = intended_dir.clone();
        let intended = thread::spawn(move || {
            set_real_daemon_start_token(intended_token);
            let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                start_cli_daemon(&intended_start)
            }));
            clear_real_daemon_start_token();
            started
        });
        boundary
            .matched
            .recv_timeout(Duration::from_secs(5))
            .expect("intended child reached the real-daemon start boundary after the sibling");
        (sibling, intended)
    };
    drop(lock);
    let sibling_started = sibling.join().expect("sibling start thread");
    let intended_started = intended.join().expect("intended start thread");
    disarm_real_daemon_start_boundary();
    let sibling_daemon = sibling_started.unwrap_or_else(|panic| {
        panic!("sibling start raced the injected taint: {panic:?}")
    });
    let intended_daemon = intended_started.unwrap_or_else(|panic| {
        panic!("intended start raced the injected taint: {panic:?}")
    });
    sibling_daemon.shutdown();
    intended_daemon.shutdown();
}

#[test]
fn transfer_mode_keeps_worker_until_successor_cleans() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gtf");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let first = start_cli_daemon(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(daemon_socket_path(&data_dir));
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "transfer-session".to_string(),
            command: "sh -c 'sleep 30'".to_string(),
        },
    )
    .expect("spawn transfer session");
    let identity = wait_for_registry_worker(&data_dir);
    let command_pid = identity.pid.expect("transfer command pid");
    let worker_pid = worktree_session_worker_ancestor(command_pid).unwrap_or(command_pid);
    wait_for_process_snapshot(command_pid, "own setsid", |snapshot| {
        snapshot.pgid != first.id() && snapshot.sid != first.id().to_string()
    });
    let mut first_child = first.transfer_sessions().disarm();
    request_cli_daemon_shutdown(&data_dir).ok();
    let _ = first_child.wait();
    assert!(
        process_exists(command_pid) || process_exists(worker_pid),
        "transfer mode must not reap durable workers on Hub stop command={command_pid} worker={worker_pid}"
    );
    let successor = start_cli_daemon(&data_dir);
    successor.shutdown();
    assert!(
        !process_exists(command_pid) && !process_exists(worker_pid),
        "successor guard must clean transferred workers command={command_pid} worker={worker_pid}"
    );
}

#[test]
fn guard_proof_requires_worker_pid_when_argv_omits_data_dir() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gwa");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let daemon = start_cli_daemon(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(daemon_socket_path(&data_dir));
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "argv-omit-session".to_string(),
            command: "sh -c 'sleep 30'".to_string(),
        },
    )
    .expect("spawn argv-omit session");
    let identity = wait_for_registry_worker(&data_dir);
    let command_pid = identity.pid.expect("argv-omit command pid");
    let worker_pid = worktree_session_worker_ancestor(command_pid).expect("worktree worker ancestor");
    wait_for_process_snapshot(command_pid, "own setsid", |snapshot| {
        snapshot.pgid != daemon.id() && snapshot.sid != daemon.id().to_string()
    });
    assert!(
        live_session_workers_for_data_dir(&data_dir)
            .expect("data-dir argv census")
            .is_empty(),
        "real worker argv must omit the Hub data directory so the argv census is not the oracle"
    );
    let mut child = daemon.transfer_sessions().disarm();
    request_cli_daemon_shutdown(&data_dir).ok();
    let _ = child.wait();
    assert!(
        prove_owned_children_absent(&data_dir, None, &OwnedSessionProcesses::default()).is_ok(),
        "empty known-pid proof must stay green after Hub stop while the worker is live"
    );
    assert!(
        prove_owned_children_absent(
            &data_dir,
            None,
            &OwnedSessionProcesses::from_pids([worker_pid])
        )
        .is_err(),
        "retained worker pid {worker_pid} must fail absence proof while the worker is live"
    );
    let successor = start_cli_daemon(&data_dir);
    successor.shutdown();
    assert!(
        !process_exists(worker_pid) && !process_exists(command_pid),
        "successor cleanup must reap worker {worker_pid} and command {command_pid}"
    );
}

#[test]
fn identity_capture_error_taints_and_blocks_next_start() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gic");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let mut daemon = start_cli_daemon(&data_dir);
    daemon.test_hooks.force_identity_capture_failure = true;
    drop(daemon);
    assert!(
        harness_taint().is_some_and(|evidence| evidence.contains("identity capture failed")),
        "identity capture error must set the taint latch"
    );
    let next_dir = unique_short_test_dir("gin");
    fs::create_dir_all(&next_dir).expect("create next data dir");
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = start_cli_daemon(&next_dir);
    }));
    assert!(
        panicked.is_err(),
        "next daemon start must fail after identity capture taint"
    );
    reset_harness_taint_after_proof();
}

#[test]
fn unresolved_worker_ancestor_taints_and_retains_command_pid() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gua");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let mut decoy = ChildCleanup::spawn_non_botster_decoy();
    let command_pid = decoy.id();
    let daemon = start_cli_daemon(&data_dir);
    let registry = SessionRegistry::new(data_dir.clone());
    let record = RegistryRecord::running(
        SessionId("unresolved-ancestor".to_string()),
        Some(ProcessIdentity {
            pid: Some(command_pid),
            runtime_id: Some("unresolved-ancestor-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sleep".to_string(),
        1,
    );
    registry
        .save(&record)
        .expect("forged registry fixture should save");
    let capture = collect_owned_session_processes(&data_dir).expect("partial capture");
    assert!(
        capture.owned.pids.contains(&command_pid),
        "command pid {command_pid} must be retained: {:?}",
        capture.owned.pids
    );
    assert!(
        capture.errors.iter().any(|error| {
            error.contains("unresolved worktree session-worker ancestor")
                && error.contains(&command_pid.to_string())
        }),
        "missing verified worker ancestor must be a capture error: {:?}",
        capture.errors
    );
    drop(daemon);
    assert!(
        harness_taint().is_some_and(|evidence| {
            evidence.contains("unresolved worktree session-worker ancestor")
        }),
        "unresolved worker ancestor must set the taint latch: {:?}",
        harness_taint()
    );
    decoy.assert_alive();
    let reap = reap_registry_backed_workers(&data_dir).expect("partial reap");
    assert!(
        reap.errors.iter().any(|error| {
            error.contains("unresolved worktree session-worker ancestor")
                && error.contains(&command_pid.to_string())
        }),
        "missing verified worker ancestor must fail closed on reap: {:?}",
        reap.errors
    );
    assert!(
        reap.retained.contains(&command_pid),
        "unverified command pid {command_pid} must stay retained: {:?}",
        reap.retained
    );
    assert!(
        !reap.reaped.contains(&command_pid),
        "unverified command pid {command_pid} must not be signaled: {:?}",
        reap.reaped
    );
    decoy.assert_alive();
    reset_harness_taint_after_proof();
}

fn spawn_and_reap_sleep() -> u32 {
    let mut child = Command::new("sleep")
        .arg("30")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn short-lived sleep");
    let pid = child.id();
    let _ = child.kill();
    let _ = child.wait();
    wait_for_process_exit(pid);
    pid
}

fn save_running_recovery_record(data_dir: &Path, session_id: &str, command_pid: u32, worker_pid: u32) {
    let mut record = RegistryRecord::running(
        SessionId(session_id.to_string()),
        Some(ProcessIdentity {
            pid: Some(command_pid),
            runtime_id: Some(format!("{session_id}-runtime")),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sleep".to_string(),
        1,
    );
    record.observe_restart_contract(
        serde_json::json!({
            "worker_pid": worker_pid,
            "worker_control_socket": format!("/tmp/bh-recovery-{session_id}.sock"),
        }),
        2,
    );
    SessionRegistry::new(data_dir.to_path_buf())
        .save(&record)
        .expect("save recovery registry fixture");
}

#[test]
fn dead_command_and_dead_recovery_worker_do_not_taint() {
    let _lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let data_dir = unique_short_test_dir("gxd");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let command_pid = spawn_and_reap_sleep();
    let worker_pid = spawn_and_reap_sleep();
    save_running_recovery_record(&data_dir, "dead-recovery", command_pid, worker_pid);
    let capture = collect_owned_session_processes(&data_dir).expect("exited-in-transition capture");
    assert!(
        capture.errors.is_empty(),
        "dead command and dead recovery worker must not taint: {:?}",
        capture.errors
    );
    assert!(
        harness_taint().is_none(),
        "benign command-exit race must leave the latch clear: {:?}",
        harness_taint()
    );
    reset_harness_taint_after_proof();
}

#[test]
fn dead_command_with_live_unverified_recovery_worker_taints() {
    let _lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let data_dir = unique_short_test_dir("gxu");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let command_pid = spawn_and_reap_sleep();
    let mut decoy = ChildCleanup::spawn_non_botster_decoy();
    let worker_pid = decoy.id();
    save_running_recovery_record(&data_dir, "live-unverified", command_pid, worker_pid);
    let capture = collect_owned_session_processes(&data_dir).expect("unverified recovery capture");
    assert!(
        capture.owned.pids.contains(&worker_pid),
        "live unverified recovery worker must be retained: {:?}",
        capture.owned.pids
    );
    assert!(
        capture.errors.iter().any(|error| {
            error.contains("live but unverifiable") && error.contains(&worker_pid.to_string())
        }),
        "live unverified recovery worker must be a capture error: {:?}",
        capture.errors
    );
    record_harness_taint(format!(
        "identity capture incomplete: {}",
        capture.errors.join("; ")
    ));
    assert!(
        harness_taint().is_some_and(|evidence| evidence.contains("live but unverifiable")),
        "live unverified recovery worker must set the taint latch: {:?}",
        harness_taint()
    );
    decoy.assert_alive();
    reset_harness_taint_after_proof();
}

#[test]
fn dead_command_with_zombie_recovery_worker_does_not_taint() {
    let _lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let data_dir = unique_short_test_dir("gxz");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let command_pid = spawn_and_reap_sleep();
    let mut zombie = Command::new("sleep")
        .arg("30")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zombie recovery worker");
    let worker_pid = zombie.id();
    let _ = zombie.kill();
    wait_for_process_snapshot(worker_pid, "zombie recovery worker", |snapshot| {
        snapshot.stat.contains('Z')
    });
    save_running_recovery_record(&data_dir, "zombie-recovery", command_pid, worker_pid);
    let capture = collect_owned_session_processes(&data_dir).expect("zombie recovery capture");
    assert!(
        capture.owned.pids.contains(&worker_pid) && capture.owned.pids.contains(&command_pid),
        "zombie recovery worker and dead command must stay in the owned set: {:?}",
        capture.owned.pids
    );
    assert!(
        capture.errors.is_empty(),
        "zombie recovery worker is dead evidence and must not taint: {:?}",
        capture.errors
    );
    assert!(
        harness_taint().is_none(),
        "zombie recovery worker must leave the latch clear: {:?}",
        harness_taint()
    );
    let reap = reap_registry_backed_workers(&data_dir).expect("zombie recovery reap");
    assert!(
        reap.errors.is_empty(),
        "zombie recovery worker must not fail reap: {:?}",
        reap.errors
    );
    assert!(
        reap.retained.contains(&worker_pid) && reap.retained.contains(&command_pid),
        "zombie recovery worker must stay retained, not signaled: {:?}",
        reap.retained
    );
    assert!(
        !reap.reaped.contains(&worker_pid) && !reap.reaped.contains(&command_pid),
        "zombie recovery worker must not be signaled: {:?}",
        reap.reaped
    );
    let _ = zombie.wait();
    reset_harness_taint_after_proof();
}

#[test]
fn dead_command_without_recovery_identity_taints_and_does_not_signal() {
    let _lock = daemon_test_guard();
    reset_harness_taint_after_proof();
    let data_dir = unique_short_test_dir("gxr");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let command_pid = spawn_and_reap_sleep();
    let mut decoy = ChildCleanup::spawn_non_botster_decoy();
    let record = RegistryRecord::running(
        SessionId("missing-recovery".to_string()),
        Some(ProcessIdentity {
            pid: Some(command_pid),
            runtime_id: Some("missing-recovery-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sleep".to_string(),
        1,
    );
    SessionRegistry::new(data_dir.clone())
        .save(&record)
        .expect("save missing-recovery registry fixture");
    let capture = collect_owned_session_processes(&data_dir).expect("missing-recovery capture");
    assert!(
        capture.owned.pids.contains(&command_pid),
        "dead command pid {command_pid} must be retained: {:?}",
        capture.owned.pids
    );
    assert!(
        capture.errors.iter().any(|error| {
            error.contains("no recovery worker pid") && error.contains(&command_pid.to_string())
        }),
        "missing recovery identity must be a capture error: {:?}",
        capture.errors
    );
    record_harness_taint(format!(
        "identity capture incomplete: {}",
        capture.errors.join("; ")
    ));
    assert!(
        harness_taint().is_some_and(|evidence| evidence.contains("no recovery worker pid")),
        "missing recovery identity must set the taint latch: {:?}",
        harness_taint()
    );
    let reap = reap_registry_backed_workers(&data_dir).expect("missing-recovery reap");
    assert!(
        reap.errors.iter().any(|error| {
            error.contains("no recovery worker pid") && error.contains(&command_pid.to_string())
        }),
        "missing recovery identity must fail closed on reap: {:?}",
        reap.errors
    );
    assert!(
        reap.retained.contains(&command_pid),
        "dead command pid {command_pid} must stay retained on reap: {:?}",
        reap.retained
    );
    assert!(
        !reap.reaped.contains(&command_pid) && !reap.reaped.contains(&decoy.id()),
        "missing recovery identity must not signal unverified pids: {:?}",
        reap.reaped
    );
    decoy.assert_alive();
    reset_harness_taint_after_proof();
}

#[test]
fn process_group_proof_detects_member_after_leader_exit() {
    let data_dir = unique_short_test_dir("gpg");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let mut leader = Command::new("sleep");
    leader.arg("30").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    unsafe {
        leader.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    let mut leader = leader.spawn().expect("spawn group leader");
    let pgid = leader.id();
    let mut member = Command::new("sleep");
    member.arg("30").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    let leader_pgid = pgid as libc::pid_t;
    unsafe {
        member.pre_exec(move || {
            libc::setpgid(0, leader_pgid);
            Ok(())
        });
    }
    let mut member = member.spawn().expect("spawn group member");
    wait_for_process_snapshot(member.id(), "join leader group", |snapshot| {
        snapshot.pgid == pgid
    });
    unsafe {
        libc::kill(leader.id() as libc::pid_t, libc::SIGKILL);
    }
    let _ = leader.wait();
    assert!(
        !process_exists(pgid),
        "group leader pid must be gone so a pid-only oracle would go green"
    );
    assert!(
        process_group_probe(pgid as libc::pid_t).expect("group probe"),
        "process-group probe must see the remaining member"
    );
    assert!(
        prove_owned_children_absent(&data_dir, None, &OwnedSessionProcesses::from_pids([pgid]))
            .is_ok(),
        "pid-only proof is the false-clean hole after the leader exits"
    );
    let mut owned = OwnedSessionProcesses::default();
    owned.push_pgid(pgid);
    assert!(
        prove_owned_children_absent(&data_dir, None, &owned).is_err(),
        "typed process-group proof must fail while a group member remains"
    );
    unsafe {
        libc::kill(member.id() as libc::pid_t, libc::SIGKILL);
    }
    let _ = member.wait();
}

#[test]
fn disarmed_guard_leaves_survivor_for_red_on_revert() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gdr");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let daemon = start_cli_daemon(&data_dir);
    let pid = daemon.id();
    let mut child = daemon.disarm();
    assert!(
        process_exists(pid),
        "disarmed guard must leave the Hub child live for the red-on-revert census"
    );
    let _ = try_terminate_and_reap_child(&mut child);
}

#[test]
fn guard_cleanup_after_panic_reaps_supervised_entrypoint() {
    let _lock = daemon_test_guard();
    let data_dir = unique_short_test_dir("gse");
    let package_dir = unique_short_test_dir("gsp");
    fs::create_dir_all(&data_dir).expect("create data dir");
    write_supervised_package(
        &package_dir,
        "runtime.guard",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let daemon = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);
    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.guard".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start supervised entrypoint");
    let pid = package_entrypoint(&start, "runtime.guard")
        .process
        .pid
        .expect("supervised pid");
    assert!(process_exists(pid), "supervised entrypoint must be live");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _daemon = daemon;
        panic!("injected supervised-entrypoint cleanup panic");
    }));
    assert!(result.is_err(), "injected panic must unwind");
    wait_for_process_exit(pid);
    assert!(
        !process_exists(pid),
        "panic cleanup must reap the supervised entrypoint pid {pid}"
    );
    assert!(
        !daemon_socket_path(&data_dir).exists(),
        "panic cleanup must remove the daemon socket"
    );
}
