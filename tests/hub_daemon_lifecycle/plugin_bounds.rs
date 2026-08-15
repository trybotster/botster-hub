#[test]
fn daemon_restart_preserves_split_plugin_worker_configuration() {
    let mut config = explicit_config(unique_test_dir("plugin-worker-config-restart"));
    config.core_engine.plugin_worker_queue_capacity = 9;
    config.core_engine.plugin_worker_executor_concurrency = 3;
    config.plugin_worker_class.reserved_request_response_executors = 1;
    config.plugin_worker_class.background_queue_capacity = 6;
    config.plugin_worker_class.completion_queue_capacity = 5;

    let mut daemon = HubDaemon::start(config.clone()).expect("start configured daemon");
    let initial = daemon
        .runtime()
        .expect("runtime initialized")
        .plugin_worker_debug_snapshot();
    assert_eq!(initial.configured_queue_capacity, 9);
    assert_eq!(initial.configured_executor_concurrency, 3);
    assert_eq!(initial.configured_reserved_request_response_executors, 1);
    assert_eq!(initial.configured_background_queue_capacity, 6);
    assert_eq!(initial.configured_completion_queue_capacity, 5);
    daemon.stop();

    let mut restarted = HubDaemon::start(config).expect("restart configured daemon");
    let reopened = restarted
        .runtime()
        .expect("runtime initialized")
        .plugin_worker_debug_snapshot();
    assert_eq!(reopened.configured_queue_capacity, 9);
    assert_eq!(reopened.configured_executor_concurrency, 3);
    assert_eq!(reopened.configured_reserved_request_response_executors, 1);
    assert_eq!(reopened.configured_background_queue_capacity, 6);
    assert_eq!(reopened.configured_completion_queue_capacity, 5);
    restarted.stop();
}

#[test]
fn focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload() {
    const PACKAGE_NAMES: [&str; 4] = [
        "bounds.package-one",
        "bounds.package-two",
        "bounds.package-three",
        "bounds.package-four",
    ];
    const EXPECTED_QUEUE_CAPACITY: usize = 256;
    const EXPECTED_EXECUTOR_CONCURRENCY: usize = 2;
    const EXPECTED_EXECUTOR_WORKERS: usize = 8;
    const MAX_HUB_THREADS: usize = 64;

    let _guard = daemon_test_guard();
    let root = unique_test_dir("plugin-bounds");
    let data_dir = root.join("data");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("resource-bound config has local socket")
            .path
            .clone(),
    );
    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin resource bounds daemon evidence");
    let hub_pid = daemon.child.as_ref().expect("panic-safe daemon child").id();

    for package_name in PACKAGE_NAMES {
        let package_dir = root.join(package_name);
        write_resource_bound_plugin_package(&package_dir, package_name);
        let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .args(["packages", "install", "--data-dir"])
            .arg(&data_dir)
            .arg("--path")
            .arg(&package_dir)
            .output()
            .expect("install resource-bound package");
        assert!(
            install.status.success(),
            "{}",
            command_output_text(&install)
        );
        let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .args(["packages", "enable", "--data-dir"])
            .arg(&data_dir)
            .arg(package_name)
            .output()
            .expect("enable resource-bound package");
        assert!(enable.status.success(), "{}", command_output_text(&enable));
    }

    for _ in 0..12 {
        let status =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("reconnect status request");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    }

    let loaded = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("read loaded plugin resources");
    let loaded_workers = loaded
        .plugin_worker_counters
        .expect("worker counters are projected");
    let loaded_resources = loaded
        .plugin_resource_counters
        .expect("resource counters are projected");
    assert_eq!(
        loaded_workers.configured_queue_capacity,
        EXPECTED_QUEUE_CAPACITY
    );
    assert_eq!(
        loaded_workers.configured_executor_concurrency,
        EXPECTED_EXECUTOR_CONCURRENCY
    );
    assert_eq!(loaded_workers.live_plugin_executors, PACKAGE_NAMES.len());
    assert_eq!(
        loaded_workers.live_executor_workers,
        EXPECTED_EXECUTOR_WORKERS
    );
    assert_eq!(loaded_workers.queued_jobs, 0);
    assert_eq!(loaded_workers.in_flight_jobs, 0);
    assert_eq!(loaded_resources.active_timer_resources, 0);
    assert!(
        loaded_workers.live_executor_workers
            <= loaded_workers.live_plugin_executors
                * loaded_workers.configured_executor_concurrency
    );
    let observed_threads = process_thread_count(hub_pid).expect("count Hub OS threads");
    assert!(
        observed_threads <= MAX_HUB_THREADS,
        "Hub thread bound exceeded: observed={observed_threads} maximum={MAX_HUB_THREADS}"
    );
    let probe =
        Command::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("script/probe-hub-resources"))
            .arg("--socket")
            .arg(&endpoint.socket_path)
            .arg("--hub-pid")
            .arg(hub_pid.to_string())
            .args([
                "--phase",
                "focused-loaded",
                "--expected-owners",
                "4",
                "--exercise-entity-reconnects",
                "4",
            ])
            .output()
            .expect("run public-protocol resource probe");
    assert!(probe.status.success(), "{}", command_output_text(&probe));
    let probe_evidence: serde_json::Value =
        serde_json::from_slice(&probe.stdout).expect("resource probe emits bounded JSON");
    assert_eq!(probe_evidence["checks"]["hub_threads"], true);
    assert_eq!(probe_evidence["checks"]["active_timer_resources"], true);
    assert_eq!(probe_evidence["exercised_entity_reconnects"], 4);

    let non_converging_probe =
        Command::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("script/probe-hub-resources"))
            .arg("--socket")
            .arg(&endpoint.socket_path)
            .arg("--hub-pid")
            .arg(hub_pid.to_string())
            .args([
                "--phase",
                "focused-non-converging-control",
                "--expected-owners",
                "3",
                "--timeout-seconds",
                "1",
            ])
            .output()
            .expect("run non-converging resource probe control");
    assert!(
        !non_converging_probe.status.success(),
        "non-converging resource probe passed unexpectedly"
    );
    let non_converging_evidence: serde_json::Value =
        serde_json::from_slice(&non_converging_probe.stdout)
            .expect("non-converging probe emits its last snapshot");
    assert_eq!(non_converging_evidence["convergence"], "baseline_timeout");
    assert_eq!(
        non_converging_evidence["last_observed"]["workers"]["live_plugin_executors"],
        4
    );

    for package_name in PACKAGE_NAMES {
        let reload = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .args(["packages", "reload", "--data-dir"])
            .arg(&data_dir)
            .arg(package_name)
            .output()
            .expect("reload resource-bound package");
        assert!(reload.status.success(), "{}", command_output_text(&reload));
    }
    let reloaded = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("read resources after reload");
    let reloaded_workers = reloaded
        .plugin_worker_counters
        .expect("worker counters after reload");
    assert_eq!(reloaded_workers.live_plugin_executors, PACKAGE_NAMES.len());
    assert_eq!(
        reloaded_workers.live_executor_workers,
        EXPECTED_EXECUTOR_WORKERS
    );
    assert_eq!(reloaded_workers.queued_jobs, 0);
    assert_eq!(reloaded_workers.in_flight_jobs, 0);
    assert_eq!(
        reloaded
            .plugin_resource_counters
            .expect("resource counters after reload")
            .active_timer_resources,
        0
    );

    #[cfg(target_os = "linux")]
    {
        let start_ticks = linux_process_cpu_ticks(hub_pid).expect("read initial Hub CPU ticks");
        thread::sleep(Duration::from_secs(5));
        let end_ticks = linux_process_cpu_ticks(hub_pid).expect("read final Hub CPU ticks");
        let ticks_per_second = Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .expect("resolve Linux clock ticks per second");
        let cpu_delta_ticks = end_ticks.saturating_sub(start_ticks);
        eprintln!("idle_cpu_delta_ticks={cpu_delta_ticks} ticks_per_second={ticks_per_second}");
        if std::env::var_os("BOTSTER_ASSERT_IDLE_CPU_BOUND").is_some() {
            assert!(
                cpu_delta_ticks.saturating_mul(4) <= ticks_per_second,
                "idle Hub CPU exceeded 250 ms: delta_ticks={cpu_delta_ticks} ticks_per_second={ticks_per_second}"
            );
        } else {
            eprintln!(
                "idle_cpu_bound=observed_not_asserted reason=BOTSTER_ASSERT_IDLE_CPU_BOUND_unset"
            );
        }
    }

    for (index, package_name) in PACKAGE_NAMES.iter().enumerate() {
        let disable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .args(["packages", "disable", "--data-dir"])
            .arg(&data_dir)
            .arg(package_name)
            .output()
            .expect("disable resource-bound package");
        assert!(
            disable.status.success(),
            "{}",
            command_output_text(&disable)
        );
        let retired = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::PluginLifecycleStatus,
        )
        .expect("read stepwise retired resources");
        let counters = retired
            .plugin_worker_counters
            .expect("worker counters after disable");
        let remaining = PACKAGE_NAMES.len() - index - 1;
        assert_eq!(counters.live_plugin_executors, remaining);
        assert_eq!(
            counters.live_executor_workers,
            remaining * EXPECTED_EXECUTOR_CONCURRENCY
        );
        assert_eq!(counters.queued_jobs, 0);
        assert_eq!(counters.in_flight_jobs, 0);
        assert_eq!(
            retired
                .plugin_resource_counters
                .expect("resource counters after disable")
                .active_timer_resources,
            0
        );
    }

    daemon.shutdown();
}

