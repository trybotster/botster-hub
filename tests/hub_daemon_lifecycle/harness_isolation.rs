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
    let mut daemon_a = start_cli_daemon(&data_dir_a);
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
    unsafe {
        libc::kill(daemon_a.id() as libc::pid_t, libc::SIGKILL);
    }
    let _ = daemon_a.child_mut().wait();
    assert!(
        process_exists(worker_a_pid),
        "durable worker A must survive Hub SIGKILL before backstop"
    );
    let reaped = reap_registry_backed_workers(&data_dir_a).expect("A backstop");
    assert!(!reaped.is_empty(), "A backstop must reap its worker");
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
