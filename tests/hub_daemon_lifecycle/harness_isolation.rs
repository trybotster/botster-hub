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
    wait_for_registry_worker(&data_dir);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _daemon = daemon;
        panic!("injected cleanup panic");
    }));
    assert!(result.is_err(), "injected panic must unwind");
    assert!(
        !daemon_socket_path(&data_dir).exists(),
        "panic cleanup must remove the daemon socket"
    );
    let leftover = live_session_workers_for_data_dir(&data_dir).expect("post-panic census");
    assert!(
        leftover.is_empty(),
        "panic cleanup must reap owned workers: {leftover:?}"
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
    wait_for_registry_worker(&data_dir);
    let mut first_child = first.transfer_sessions().disarm();
    request_cli_daemon_shutdown(&data_dir).ok();
    let _ = first_child.wait();
    assert!(
        !live_session_workers_for_data_dir(&data_dir)
            .expect("workers after hub stop")
            .is_empty(),
        "transfer mode must not reap durable workers on Hub stop"
    );
    let successor = start_cli_daemon(&data_dir);
    successor.shutdown();
    assert!(
        live_session_workers_for_data_dir(&data_dir)
            .expect("workers after successor")
            .is_empty(),
        "successor guard must clean transferred workers"
    );
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
