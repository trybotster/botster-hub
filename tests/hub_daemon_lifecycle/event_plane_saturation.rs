// Event-plane saturation campaign and its always-on architectural gates.
//
// The ignored campaign is selected by `script/run-loaded-daemon-lifecycle`
// `--test-target event-plane-saturation`. Default lifecycle-suite runs only
// the source-guard and isolated client-event tests below.

use botster_hub::{
    EventPlaneStatus, MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS, PackageEventPlaneOptions,
    PackageEventPlanePolicy, PackageEventRouter,
};
use botster_hub_test_support::{
    copy_plugin_contract_matrix_fixture, monotonic_now_ns, run_client_event_conformance,
};

const EVENT_PLANE_FLEET_N: usize = 300;
const EVENT_PLANE_SPAWN_WAVE: usize = 30;
const EVENT_PLANE_WAVE_GAP: Duration = Duration::from_millis(200);
const EVENT_PLANE_MEASUREMENT_WINDOW: Duration = Duration::from_secs(600);
const EVENT_PLANE_WARMUP: Duration = Duration::from_secs(30);
const EVENT_PLANE_MIN_SAMPLES: u64 = 200;
const EVENT_PLANE_DRIVER_CONCURRENCY: usize = 4;
const EVENT_PLANE_BURST_COUNT: u64 = 25;
const EVENT_PLANE_BURSTS_PER_SEC: u64 = 6;
const EVENT_PLANE_RATIO_R: f64 = 1.25;
const EVENT_PLANE_SLACK_MS: f64 = 8.0;
const EVENT_PLANE_THROUGHPUT_T: f64 = 0.80;
const EVENT_PLANE_STRESS_PROFILE: &str = "none";
const EVENT_PLANE_RUNNER: &str = "ubuntu-24.04";
const EVENT_PLANE_NOISY_SESSION: &str = "event-plane-noisy";
const EVENT_PLANE_NOISY_SUB: &str = "event-plane-noisy-sub";
const EVENT_PLANE_OPERATIONS: [&str; 9] = [
    "spawn",
    "attach",
    "drain",
    "input",
    "resize",
    "mcp",
    "ui",
    "entity",
    "shutdown",
];
const EVENT_PLANE_GATED_OPERATIONS: [&str; 11] = [
    "spawn",
    "attach",
    "drain",
    "input",
    "resize",
    "mcp",
    "ui",
    "entity",
    "shutdown",
    "terminal_input",
    "terminal_output",
];
const EVENT_PLANE_QUEUE_AGE_US: u64 = 1_000 * 1_000;
const EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES: u64 = 0;
const EVENT_PLANE_OUTPUT_RECORD_BYTES: usize = 4096;
const EVENT_PLANE_OUTPUT_HEADER_BYTES: usize = 30;
const EVENT_PLANE_NOISY_COMMAND: &str = concat!(
    "python3 -u -c \"",
    "import sys,time,threading\n",
    "sys.stdout.buffer.write(b'ns-ready\\n')\n",
    "sys.stdout.buffer.write(bytes([0x80,0xff,10]))\n",
    "sys.stdout.buffer.flush()\n",
    "def echo():\n",
    "    while True:\n",
    "        line=sys.stdin.buffer.readline()\n",
    "        if not line: break\n",
    "        sys.stdout.buffer.write(b'ns-echo:'+line)\n",
    "        sys.stdout.buffer.flush()\n",
    "threading.Thread(target=echo,daemon=True).start()\n",
    "pad=b'x'*4065\n",
    "i=0\n",
    "while True:\n",
    "    sys.stdout.buffer.write(('N%08dT%020d'%(i,time.monotonic_ns())).encode()+pad+b'\\n')\n",
    "    sys.stdout.buffer.flush()\n",
    "    i+=1\n",
    "    time.sleep(0.1)\"",
);

#[test]
fn event_plane_saturation_source_guards_hold() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let lua = fs::read_to_string(root.join("src/lua_runtime.rs")).expect("lua runtime");
    let production_lua = lua.split("#[cfg(test)]").next().unwrap_or(&lua);
    let emit_fn = production_lua
        .split("\"emit\"")
        .nth(1)
        .and_then(|rest| rest.split("globals.set(\"events\"").next())
        .expect("events.emit installation");
    assert!(
        emit_fn.contains("try_ingress") && emit_fn.matches("try_ingress").count() == 1,
        "events.emit must use one try_ingress attempt"
    );
    assert!(
        !emit_fn.contains("thread::sleep") && !emit_fn.contains("recv("),
        "events.emit must not wait on the owner loop: {emit_fn}"
    );

    for relative in [
        "src/daemon_transport.rs",
        "src/daemon_maintenance.rs",
        "src/daemon_entity_subscriptions.rs",
        "src/session_projection.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source");
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(
            !production.contains("package_event_router().try_ingress")
                || relative == "src/daemon_transport.rs",
            "{relative} operation handlers must not wait on router ingress"
        );
    }

    let defaults = PackageEventPlaneOptions::default();
    assert_eq!(defaults.producer_queue_max_events, 256);
    assert_eq!(defaults.producer_queue_max_bytes, 512 * 1024);
    assert_eq!(defaults.consumer_queue_max_events, 128);
    assert_eq!(defaults.consumer_queue_max_bytes, 2 * 1024 * 1024);
    assert_eq!(defaults.global_in_flight_bytes, 16 * 1024 * 1024);
    assert_eq!(defaults.package_rate_per_sec, 100);
    assert_eq!(defaults.queue_age_ms, 1_000);
    assert_eq!(MAX_OWNER_TURN_MS, 25);
    assert_eq!(MAX_READY_OPERATION_WAIT_MS, 50);
    let maintenance =
        fs::read_to_string(root.join("src/daemon_maintenance.rs")).expect("maintenance");
    assert!(maintenance.contains("max_sessions: 8"));
    assert!(maintenance.contains("max_rows: 16"));
    assert!(
        maintenance.contains("journal_pull_held"),
        "cursor-expiry hold must skip journal pull"
    );
    assert_eq!(EVENT_PLANE_RATIO_R, 1.25);
    assert_eq!(EVENT_PLANE_SLACK_MS, 8.0);
    assert_eq!(EVENT_PLANE_THROUGHPUT_T, 0.80);
    assert_eq!(EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES, 0);
    assert_eq!(EVENT_PLANE_OUTPUT_RECORD_BYTES, 4096);
    assert_eq!(
        EVENT_PLANE_OUTPUT_HEADER_BYTES + 4065 + 1,
        EVENT_PLANE_OUTPUT_RECORD_BYTES
    );
    assert!(EVENT_PLANE_NOISY_COMMAND.contains("time.monotonic_ns"));
    assert!(EVENT_PLANE_NOISY_COMMAND.contains("pad=b'x'*4065"));
    assert!(
        !EVENT_PLANE_NOISY_COMMAND.contains("python3 -c 'import time"),
        "noisy producer must be one process, not one Python process per line"
    );

    let unix = fs::read_to_string(root.join("src/unix_terminal_adapter.rs")).expect("unix adapter");
    let webrtc =
        fs::read_to_string(root.join("src/webrtc_terminal_adapter.rs")).expect("webrtc adapter");
    let attach = fs::read_to_string(root.join("src/daemon_attach_stream.rs")).expect("attach");
    for (name, source) in [
        ("unix", unix.as_str()),
        ("webrtc", webrtc.as_str()),
        ("attach", attach.as_str()),
    ] {
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);
        assert!(
            !production.contains("package_event_router"),
            "{name} terminal adapter must stay content-blind of the event plane"
        );
    }

    let router = fs::read_to_string(root.join("src/package_event_router.rs")).expect("router");
    assert!(
        router.contains("held_lock_try_ingress_returns_shed_busy_without_blocking"),
        "ShedBusy remains a focused in-process lane"
    );
    let webrtc_src = fs::read_to_string(root.join("src/local_webrtc.rs")).expect("webrtc");
    assert!(
        webrtc_src.contains("timeout fail-closed must sacrifice sibling peers"),
        "fail-closed blast-radius oracle must stay in the production test body"
    );
    assert!(
        webrtc_src.contains("BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD")
            && webrtc_src.contains(
                "local_webrtc_close_hang_fail_closed_returns_handler_within_deadline"
            ),
        "IsolatedHub cannot observe dedicated_runtime_worker_threads(); the hang-close child remains the live blast-radius oracle"
    );
    let harness = fs::read_to_string(root.join("tests/hub_daemon_lifecycle/harness.rs"))
        .expect("harness");
    assert!(
        harness.contains("libc::posix_openpt"),
        "PTY probe must call libc posix_openpt on the reference runner"
    );
    assert!(!harness.contains("os.posix_openpt") && !harness.contains("os.openpt("));
    assert_eq!(EVENT_PLANE_STRESS_PROFILE, "none");
    assert_eq!(EVENT_PLANE_RUNNER, "ubuntu-24.04");
    let campaign = fs::read_to_string(
        root.join("tests/hub_daemon_lifecycle/event_plane_saturation.rs"),
    )
    .expect("campaign source");
    let five_ms_clock = format!("from_millis({})", 5);
    assert!(
        !campaign.contains(&five_ms_clock),
        "ShedBusy must not use a wall-clock bound"
    );
    assert!(
        campaign.contains("fn probe_scheduler_lag")
            && campaign.contains("disabled_arm_meets_published_budgets"),
        "host-validity oracles must stay in the campaign"
    );
    let proof = fs::read_to_string(root.join("docs/event-plane-load-proof.md")).expect("proof");
    assert!(
        proof.contains("| Stress profile | `none`"),
        "published machine profile must name stress_profile none"
    );
    assert!(
        proof.contains("-f stress_profile=none"),
        "How to run must dispatch the none profile"
    );
    assert!(
        !proof.contains("stress_profile=residual-tail"),
        "event-plane How to run must not dispatch residual-tail"
    );
    let plan = fs::read_to_string(
        root.join("docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md"),
    )
    .expect("plan");
    assert!(
        plan.contains("| Stress profile | `none`"),
        "plan A.2 must name none as the fixed reference profile"
    );
}

#[test]
fn event_plane_saturation_shed_busy_is_non_blocking() {
    prove_shed_busy_non_blocking();
}

fn prove_shed_busy_non_blocking() {
    let router = Arc::new(PackageEventRouter::new(PackageEventPlanePolicy::default()));
    let status = router.test_with_inner_held(|| {
        let router = Arc::clone(&router);
        thread::spawn(move || {
            router.try_ingress(
                "hub",
                "worktree_created",
                &serde_json::json!({ "event": "worktree_created" }),
                Instant::now(),
            )
        })
        .join()
        .expect("join held ingress")
    });
    assert_eq!(status, EventPlaneStatus::ShedBusy);
}


#[test]
#[allow(clippy::field_reassign_with_default)]
fn event_plane_saturation_host_validity_scheduler_lag_is_host_exhaustion() {
    let lag = SchedulerLagProbe {
        requested_busy_us: MAX_OWNER_TURN_MS * 1000,
        observed_elapsed_us: 1_800_000,
        max_gap_us: 1_775_000,
        lag_us: 1_775_000,
        budget_us: MAX_READY_OPERATION_WAIT_MS * 1000,
    };
    let validity = classify_host_validity(
        Some(&botster_hub_client::DaemonObservabilityCounters::default()),
        &lag,
        ResourceProbe::Unconfirmed,
        ResourceProbe::Unconfirmed,
        HostDiagnostics {
            loadavg_1: Some(0.1),
            ..HostDiagnostics::default()
        },
    );
    assert_eq!(validity.verdict, HostValidityVerdict::Exhausted);
    assert!(validity.scheduler_lag.exceeds_budget());
    assert!(
        validity
            .reasons
            .iter()
            .any(|reason| reason.contains("scheduler_lag"))
    );
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn event_plane_saturation_host_validity_disabled_arm_over_budget_is_host_exhaustion() {
    let mut decoupled = botster_hub_client::DaemonObservabilityCounters::default();
    decoupled.max_owner_turn_us = 219_723;
    decoupled.max_ready_operation_wait_us = 1_845_228;
    let validity = classify_host_validity(
        Some(&decoupled),
        &idle_scheduler_lag(),
        ResourceProbe::Unconfirmed,
        ResourceProbe::Unconfirmed,
        HostDiagnostics::default(),
    );
    assert_eq!(validity.verdict, HostValidityVerdict::Exhausted);
    assert_eq!(validity.disabled_arm_meets_published_budgets, Some(false));
    assert!(!validity.scheduler_lag.exceeds_budget());
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn event_plane_saturation_valid_disabled_arm_makes_enabled_breach_product_failure() {
    let mut decoupled = botster_hub_client::DaemonObservabilityCounters::default();
    decoupled.max_owner_turn_us = 8_000;
    decoupled.max_ready_operation_wait_us = 12_000;
    let validity = classify_host_validity(
        Some(&decoupled),
        &idle_scheduler_lag(),
        ResourceProbe::Unconfirmed,
        ResourceProbe::Unconfirmed,
        HostDiagnostics::default(),
    );
    assert_eq!(validity.verdict, HostValidityVerdict::Valid);
    assert_eq!(validity.disabled_arm_meets_published_budgets, Some(true));
    let mut enabled = botster_hub_client::DaemonObservabilityCounters::default();
    enabled.max_owner_turn_us = 219_723;
    assert_eq!(
        classify_enabled_budget_failure(&validity, &enabled),
        "product_failure"
    );
}

#[test]
fn event_plane_saturation_host_validity_ignores_load_average() {
    let validity = classify_host_validity(
        Some(&botster_hub_client::DaemonObservabilityCounters::default()),
        &idle_scheduler_lag(),
        ResourceProbe::Unconfirmed,
        ResourceProbe::Unconfirmed,
        HostDiagnostics {
            loadavg_1: Some(48.0),
            loadavg_5: Some(47.0),
            loadavg_15: Some(46.0),
            runnable: Some(400),
            total_threads: Some(500),
            cpu_steal: Some(12.0),
        },
    );
    assert_eq!(validity.verdict, HostValidityVerdict::Valid);
    assert_eq!(validity.disabled_arm_meets_published_budgets, Some(true));
}

#[test]
fn event_plane_saturation_scheduler_lag_probe_records_monotonic_elapsed() {
    let probe = probe_scheduler_lag();
    assert_eq!(probe.requested_busy_us, MAX_OWNER_TURN_MS * 1000);
    assert_eq!(probe.budget_us, MAX_READY_OPERATION_WAIT_MS * 1000);
    assert!(probe.observed_elapsed_us >= probe.requested_busy_us);
    assert!(probe.lag_us >= probe.observed_elapsed_us.saturating_sub(probe.requested_busy_us));
}

#[test]
fn event_plane_saturation_percentile_and_budget_formulas() {
    let samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    assert_eq!(nearest_rank(&samples, 0.50), 50);
    assert_eq!(nearest_rank(&samples, 0.95), 100);
    assert_eq!(nearest_rank(&samples, 0.99), 100);
    assert_eq!(sample_max(&samples), 100);
    let enabled = OpMetrics {
        attempts: 200,
        successes: 200,
        failures: 0,
        p50: 40,
        p95: 80,
        p99: 90,
        max: 100,
        throughput: 10.0,
    };
    let thresholds = derive_thresholds(&enabled);
    assert_eq!(thresholds.abs50, ((40.0_f64 * 1.20) + 8.0).ceil() as u64);
    assert_eq!(thresholds.abs95, ((80.0_f64 * 1.20) + 8.0).ceil() as u64);
    assert_eq!(thresholds.abs99, ((90.0_f64 * 1.20) + 8.0).ceil() as u64);
    assert_eq!(thresholds.absmax, ((90.0_f64 * 3.00) + 8.0).ceil() as u64);
    assert_eq!(thresholds.thrmin, (10.0_f64 * 0.80).floor() as u64);
    let decoupled = OpMetrics {
        attempts: 200,
        successes: 200,
        failures: 0,
        p50: 40,
        p95: 80,
        p99: 90,
        max: 110,
        throughput: 10.0,
    };
    gate_relative(&enabled, &decoupled, &thresholds).expect("relative gates pass on equal arms");
    assert_eq!(floor_int((200.0_f64 / 600.0) * 0.80), 0);
    assert_eq!(floor_int(10.0_f64 * 0.80), 8);
}

#[test]
fn event_plane_saturation_queue_bounds_reject_indeterminate_rows() {
    let defaults = PackageEventPlaneOptions::default();
    let mut observability = botster_hub_client::DaemonObservabilityCounters::default();
    observability.queue_ages.push(botster_hub_client::DaemonQueueAgeObservation::new(
        botster_hub_client::DaemonQueueKind::Producer,
        "event-plane-producer",
        Some(1),
        botster_hub_client::DaemonQueueAgeState::Indeterminate,
        None,
        None,
    ));
    let violations = queue_bound_violations(&observability, &defaults);
    assert!(
        violations.iter().any(|row| row.contains("indeterminate")),
        "active indeterminate rows must fail: {violations:?}"
    );

    observability.event_age_sample_failures = 1;
    observability.queue_ages.clear();
    let failures = queue_bound_violations(&observability, &defaults);
    assert!(
        failures
            .iter()
            .any(|row| row.contains("event_age_sample_failures")),
        "sample-failure budget must fail: {failures:?}"
    );
}

#[test]
fn event_plane_saturation_output_records_carry_emission_time() {
    let mut line = b"N00000007T00000001234567890123".to_vec();
    line.extend(std::iter::repeat_n(
        b'x',
        EVENT_PLANE_OUTPUT_RECORD_BYTES - EVENT_PLANE_OUTPUT_HEADER_BYTES - 1,
    ));
    line.push(b'\n');
    assert_eq!(line.len(), EVENT_PLANE_OUTPUT_RECORD_BYTES);
    let payload = botster_hub_client::DaemonLiveOutputPayload::from_bytes(&line);
    let events = [botster_hub_client::DaemonEvent::TerminalOutput {
        session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
        subscription_id: EVENT_PLANE_NOISY_SUB.to_string(),
        payload,
    }];
    let records = output_records(&events);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 7);
    assert_eq!(records[0].emit_ns, 1_234_567_890_123);
    assert_eq!(records[0].bytes, EVENT_PLANE_OUTPUT_RECORD_BYTES);
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn event_plane_saturation_dataset_records_fault_resync() {
    let metrics = dummy_op_metrics();
    let mut operations = BTreeMap::new();
    let mut thresholds = BTreeMap::new();
    for operation in EVENT_PLANE_GATED_OPERATIONS {
        operations.insert(operation.to_string(), metrics.clone());
        thresholds.insert(operation.to_string(), derive_thresholds(&metrics));
    }
    let mut lifecycle = botster_hub_client::DaemonLifecycleCounters::default();
    lifecycle.lifecycle_baseline_reads = 4;
    lifecycle.lifecycle_resync_reads = 2;
    let faults = FaultReport {
        observability: botster_hub_client::DaemonObservabilityCounters::default(),
        lifecycle,
    };
    let body = phase_dataset_body(
        CampaignPhase::Calibration,
        &operations,
        &operations,
        &thresholds,
        &botster_hub_client::DaemonObservabilityCounters::default(),
        &botster_hub_client::DaemonObservabilityCounters::default(),
        &faults,
        Some(serde_json::json!({
            "inbound_frames": {
                "count": 0,
                "bytes": 0,
                "high_water_count": 2,
                "high_water_bytes": 64,
                "oldest_age_us": null,
                "overflow": 0,
                "max_count": WEBRTC_INBOUND_MAX_FRAMES,
                "max_bytes": WEBRTC_INBOUND_MAX_BYTES
            },
            "pending_host_events": {
                "count": 0,
                "bytes": 0,
                "high_water_count": 0,
                "high_water_bytes": 0,
                "oldest_age_us": null,
                "overflow": 0,
                "max_count": WEBRTC_PENDING_HOST_EVENTS_MAX,
                "max_bytes": WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES
            }
        })),
        None,
        &dummy_host_validity(),
    );
    assert_eq!(body["profile"]["stress_profile"], EVENT_PLANE_STRESS_PROFILE);
    assert_eq!(body["host_validity"]["verdict"], "valid");
    assert_eq!(
        body["lifecycle"]["faults"]["lifecycle_resync_reads"],
        2,
        "calibration and acceptance artifacts must record fault resync counts"
    );
    assert_eq!(body["lifecycle"]["faults"]["lifecycle_baseline_reads"], 4);
    assert!(
        body["lifecycle"]["faults"]["lifecycle_resync_reads"]
            .as_u64()
            .is_some(),
        "resync evidence must not be absent"
    );
    assert_eq!(
        body["client_fixture_queues"]["webrtc"]["inbound_frames"]["max_count"],
        WEBRTC_INBOUND_MAX_FRAMES as u64
    );
    assert_eq!(
        body["client_fixture_queues"]["webrtc"]["inbound_frames"]["overflow"],
        0
    );
    assert_eq!(
        body["client_fixture_queues"]["webrtc"]["pending_host_events"]["max_bytes"],
        WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES as u64
    );
    assert!(
        body["client_fixture_queues"]["webrtc"]["pending_host_events"]["oldest_age_us"].is_null()
    );
}

#[test]
fn event_plane_saturation_throughput_keeps_fractional_ops_per_second() {
    let stats = OperationStats {
        attempts: 200,
        successes: 200,
        failures: 0,
        samples_ms: vec![10; 200],
    };
    let metrics = metrics_from_stats(&stats, 600);
    assert!(
        (metrics.throughput - (200.0_f64 / 600.0)).abs() < 1e-12,
        "throughput must not truncate to integer ops/s, got {}",
        metrics.throughput
    );
    assert!(metrics.throughput > 0.0);
}

#[test]
fn event_plane_saturation_isolated_client_event_conformance() {
    let _guard = daemon_test_guard();
    let stall_path = PathBuf::from(format!(
        "/tmp/bh-event-conformance-stall-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = fs::remove_file(&stall_path);
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-event-conformance"))
        .name("event-conformance")
        .env("BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX", "1")
        .env(
            "BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH",
            stall_path.to_str().expect("utf8 stall path"),
        )
        .start()
        .expect("start isolated hub");
    let producer = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
    let report = run_client_event_conformance(&hub, &producer, Some(stall_path.as_path()))
        .expect("client event conformance");
    assert!(report.negotiated_package_event_subscriptions);
    assert!(report.exact_subscribe);
    assert!(report.event_received);
    assert!(report.subject_filter_dropped_non_matching);
    assert!(report.reconnect_without_replay);
    assert!(report.unsubscribed);
    assert!(report.control_progressed_during_events);
    assert!(report.event_gap);
    let _ = fs::remove_file(&stall_path);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
#[ignore = "loaded-runner event-plane-saturation campaign"]
fn event_plane_saturation_campaign() {
    let _guard = daemon_test_guard();
    let fd = probe_fd_limit();
    let pty = probe_pty_allocation();
    let lag = probe_scheduler_lag();
    eprintln!(
        "event-plane saturation host probe fd={:?} pty={:?} scheduler_lag_us={} budget_us={}",
        fd.marker_name(),
        pty.marker_name(),
        lag.lag_us,
        lag.budget_us
    );
    let pre = classify_host_validity(None, &lag, fd, pty, host_load_diagnostics());
    if pre.verdict == HostValidityVerdict::Exhausted {
        panic!(
            "host_exhaustion pre-measurement scheduler or resource probe: {}",
            pre.summary()
        );
    }
    let phase = campaign_phase();
    let decoupled = run_saturation_arm(SaturationArm::PlaneDecoupled);
    let lag = probe_scheduler_lag();
    let fd = probe_fd_limit();
    let pty = probe_pty_allocation();
    let validity = classify_host_validity(
        Some(&decoupled.observability),
        &lag,
        fd,
        pty,
        host_load_diagnostics(),
    );
    if validity.verdict == HostValidityVerdict::Exhausted {
        panic!(
            "host_exhaustion disabled control arm cannot meet published budgets or scheduler lag exceeds budget: {}",
            validity.summary()
        );
    }
    let enabled = run_saturation_arm(SaturationArm::PlaneEnabled);
    let enabled_metrics = metrics_for_arm(&enabled);
    let decoupled_metrics = metrics_for_arm(&decoupled);
    let class = classify_enabled_budget_failure(&validity, &enabled.observability);
    if class != "clean" {
        panic!(
            "{class} enabled arm breached published budgets: {:?}; host_validity={}",
            published_budget_breaches(&enabled.observability),
            validity.summary()
        );
    }
    let (thresholds, source_revision) = match phase {
        CampaignPhase::Calibration => (
            derive_all_thresholds(&enabled_metrics),
            std::env::var("SUBJECT_SHA").ok(),
        ),
        CampaignPhase::Acceptance => {
            let (thresholds, source_revision) = read_committed_thresholds();
            gate_all(&enabled_metrics, &decoupled_metrics, &thresholds);
            (thresholds, source_revision)
        }
    };
    let faults = run_fault_campaign();
    write_phase_dataset(
        phase,
        &enabled_metrics,
        &decoupled_metrics,
        &thresholds,
        &enabled.observability,
        &decoupled.observability,
        &faults,
        enabled.webrtc_queues.clone(),
        source_revision,
        &validity,
    );
}

#[derive(Clone, Copy)]
enum SaturationArm {
    PlaneEnabled,
    PlaneDecoupled,
}

#[derive(Clone, Copy)]
enum CampaignPhase {
    Calibration,
    Acceptance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostValidityVerdict {
    Valid,
    Exhausted,
}

#[derive(Clone, Debug)]
struct SchedulerLagProbe {
    requested_busy_us: u64,
    observed_elapsed_us: u64,
    max_gap_us: u64,
    lag_us: u64,
    budget_us: u64,
}

impl SchedulerLagProbe {
    fn exceeds_budget(&self) -> bool {
        self.lag_us >= self.budget_us
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "requested_busy_us": self.requested_busy_us,
            "observed_elapsed_us": self.observed_elapsed_us,
            "max_gap_us": self.max_gap_us,
            "lag_us": self.lag_us,
            "budget_us": self.budget_us,
            "exceeds_budget": self.exceeds_budget()
        })
    }
}

#[derive(Clone, Debug, Default)]
struct HostDiagnostics {
    loadavg_1: Option<f64>,
    loadavg_5: Option<f64>,
    loadavg_15: Option<f64>,
    runnable: Option<u64>,
    total_threads: Option<u64>,
    cpu_steal: Option<f64>,
}

impl HostDiagnostics {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "loadavg_1": self.loadavg_1,
            "loadavg_5": self.loadavg_5,
            "loadavg_15": self.loadavg_15,
            "runnable": self.runnable,
            "total_threads": self.total_threads,
            "cpu_steal": self.cpu_steal,
            "note": "load average, runnable count, steal, and raw scheduler lag are diagnostics; they never select the verdict"
        })
    }
}

#[derive(Clone, Debug)]
struct HostValidity {
    verdict: HostValidityVerdict,
    disabled_arm_meets_published_budgets: Option<bool>,
    scheduler_lag: SchedulerLagProbe,
    fd_probe: ResourceProbe,
    pty_probe: ResourceProbe,
    reasons: Vec<String>,
    diagnostics: HostDiagnostics,
}

impl HostValidity {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "verdict": match self.verdict {
                HostValidityVerdict::Valid => "valid",
                HostValidityVerdict::Exhausted => "host_exhaustion",
            },
            "disabled_arm_meets_published_budgets": self.disabled_arm_meets_published_budgets,
            "scheduler_lag": self.scheduler_lag.json(),
            "fd_probe": self.fd_probe.marker_name(),
            "pty_probe": self.pty_probe.marker_name(),
            "reasons": self.reasons,
            "diagnostics": self.diagnostics.json()
        })
    }

    fn summary(&self) -> String {
        format!(
            "verdict={:?} disabled_arm={:?} scheduler_lag_us={} budget_us={} fd={} pty={} reasons={:?}",
            self.verdict,
            self.disabled_arm_meets_published_budgets,
            self.scheduler_lag.lag_us,
            self.scheduler_lag.budget_us,
            self.fd_probe.marker_name(),
            self.pty_probe.marker_name(),
            self.reasons
        )
    }
}

fn idle_scheduler_lag() -> SchedulerLagProbe {
    SchedulerLagProbe {
        requested_busy_us: MAX_OWNER_TURN_MS * 1000,
        observed_elapsed_us: MAX_OWNER_TURN_MS * 1000,
        max_gap_us: 0,
        lag_us: 0,
        budget_us: MAX_READY_OPERATION_WAIT_MS * 1000,
    }
}

fn dummy_host_validity() -> HostValidity {
    classify_host_validity(
        Some(&botster_hub_client::DaemonObservabilityCounters::default()),
        &idle_scheduler_lag(),
        ResourceProbe::Unconfirmed,
        ResourceProbe::Unconfirmed,
        HostDiagnostics::default(),
    )
}

fn probe_scheduler_lag() -> SchedulerLagProbe {
    let requested = Duration::from_millis(MAX_OWNER_TURN_MS);
    let budget_us = MAX_READY_OPERATION_WAIT_MS * 1000;
    let start = Instant::now();
    let mut last = start;
    let mut max_gap = Duration::ZERO;
    while start.elapsed() < requested {
        let now = Instant::now();
        max_gap = max_gap.max(now.saturating_duration_since(last));
        last = now;
        std::hint::spin_loop();
    }
    let elapsed = start.elapsed();
    let lag = elapsed.saturating_sub(requested).max(max_gap);
    SchedulerLagProbe {
        requested_busy_us: MAX_OWNER_TURN_MS * 1000,
        observed_elapsed_us: u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        max_gap_us: u64::try_from(max_gap.as_micros()).unwrap_or(u64::MAX),
        lag_us: u64::try_from(lag.as_micros()).unwrap_or(u64::MAX),
        budget_us,
    }
}

fn published_budget_breaches(
    observability: &botster_hub_client::DaemonObservabilityCounters,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if observability.max_owner_turn_us > MAX_OWNER_TURN_MS * 1000 {
        reasons.push(format!(
            "owner-turn {}us exceeds MAX_OWNER_TURN_MS={}ms",
            observability.max_owner_turn_us, MAX_OWNER_TURN_MS
        ));
    }
    if observability.max_ready_operation_wait_us > MAX_READY_OPERATION_WAIT_MS * 1000 {
        reasons.push(format!(
            "ready-wait {}us exceeds MAX_READY_OPERATION_WAIT_MS={}ms",
            observability.max_ready_operation_wait_us, MAX_READY_OPERATION_WAIT_MS
        ));
    }
    reasons.extend(queue_bound_violations(
        observability,
        &PackageEventPlaneOptions::default(),
    ));
    reasons
}

fn classify_host_validity(
    decoupled: Option<&botster_hub_client::DaemonObservabilityCounters>,
    lag: &SchedulerLagProbe,
    fd: ResourceProbe,
    pty: ResourceProbe,
    diagnostics: HostDiagnostics,
) -> HostValidity {
    let mut reasons = Vec::new();
    let disabled_arm_meets_published_budgets = decoupled.map(|obs| {
        let breaches = published_budget_breaches(obs);
        if !breaches.is_empty() {
            reasons.extend(
                breaches
                    .into_iter()
                    .map(|row| format!("disabled_arm:{row}")),
            );
            false
        } else {
            true
        }
    });
    if lag.exceeds_budget() {
        reasons.push(format!(
            "scheduler_lag {}us exceeds ready-operation budget {}us",
            lag.lag_us, lag.budget_us
        ));
    }
    if fd == ResourceProbe::Confirmed {
        reasons.push("fd_probe confirmed".to_string());
    }
    if pty == ResourceProbe::Confirmed {
        reasons.push("pty_probe confirmed".to_string());
    }
    let exhausted = lag.exceeds_budget()
        || fd == ResourceProbe::Confirmed
        || pty == ResourceProbe::Confirmed
        || disabled_arm_meets_published_budgets == Some(false);
    HostValidity {
        verdict: if exhausted {
            HostValidityVerdict::Exhausted
        } else {
            HostValidityVerdict::Valid
        },
        disabled_arm_meets_published_budgets,
        scheduler_lag: lag.clone(),
        fd_probe: fd,
        pty_probe: pty,
        reasons,
        diagnostics,
    }
}

fn classify_enabled_budget_failure(
    validity: &HostValidity,
    enabled: &botster_hub_client::DaemonObservabilityCounters,
) -> &'static str {
    if published_budget_breaches(enabled).is_empty() {
        return "clean";
    }
    if validity.verdict == HostValidityVerdict::Valid {
        "product_failure"
    } else {
        "host_exhaustion"
    }
}

fn host_load_diagnostics() -> HostDiagnostics {
    #[cfg(target_os = "linux")]
    {
        let Ok(text) = fs::read_to_string("/proc/loadavg") else {
            return HostDiagnostics::default();
        };
        let parts: Vec<&str> = text.split_whitespace().collect();
        if parts.len() < 4 {
            return HostDiagnostics::default();
        }
        let (runnable, total_threads) = parts[3]
            .split_once('/')
            .map(|(runnable, total)| (runnable.parse().ok(), total.parse().ok()))
            .unwrap_or((None, None));
        HostDiagnostics {
            loadavg_1: parts[0].parse().ok(),
            loadavg_5: parts[1].parse().ok(),
            loadavg_15: parts[2].parse().ok(),
            runnable,
            total_threads,
            cpu_steal: cpu_steal_from_proc_stat(),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        HostDiagnostics::default()
    }
}

#[cfg(target_os = "linux")]
fn cpu_steal_from_proc_stat() -> Option<f64> {
    let text = fs::read_to_string("/proc/stat").ok()?;
    let cpu = text.lines().next()?;
    let fields: Vec<&str> = cpu.split_whitespace().collect();
    if fields.len() < 9 || fields[0] != "cpu" {
        return None;
    }
    fields[8].parse().ok()
}



struct ArmProfile {
    n: usize,
    window_secs: u64,
}

struct OperationStats {
    attempts: u64,
    successes: u64,
    failures: u64,
    samples_ms: Vec<u64>,
}

#[derive(Clone, Debug)]
struct OpMetrics {
    attempts: u64,
    successes: u64,
    failures: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    throughput: f64,
}

struct DerivedThresholds {
    abs50: u64,
    abs95: u64,
    abs99: u64,
    absmax: u64,
    thrmin: u64,
}

struct ArmReport {
    profile: ArmProfile,
    operations: BTreeMap<String, OperationStats>,
    observability: botster_hub_client::DaemonObservabilityCounters,
    webrtc_queues: Option<serde_json::Value>,
}

struct FaultReport {
    observability: botster_hub_client::DaemonObservabilityCounters,
    lifecycle: botster_hub_client::DaemonLifecycleCounters,
}

fn campaign_phase() -> CampaignPhase {
    match std::env::var("BOTSTER_EVENT_PLANE_PHASE").as_deref() {
        Ok("calibration") => CampaignPhase::Calibration,
        Ok("acceptance") => CampaignPhase::Acceptance,
        Ok(other) => panic!("BOTSTER_EVENT_PLANE_PHASE must be calibration or acceptance, got {other}"),
        Err(_) => panic!("BOTSTER_EVENT_PLANE_PHASE is required for event_plane_saturation_campaign"),
    }
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn floor_int(value: f64) -> u64 {
    value.floor() as u64
}

fn nearest_rank(samples: &[u64], p: f64) -> u64 {
    assert!(!samples.is_empty(), "percentile needs samples");
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let n = ordered.len();
    let index = ((p * n as f64).ceil() as usize).max(1).min(n);
    ordered[index - 1]
}

fn sample_max(samples: &[u64]) -> u64 {
    *samples.iter().max().expect("max needs samples")
}

fn derive_thresholds(enabled: &OpMetrics) -> DerivedThresholds {
    DerivedThresholds {
        abs50: ((enabled.p50 as f64) * 1.20 + EVENT_PLANE_SLACK_MS).ceil() as u64,
        abs95: ((enabled.p95 as f64) * 1.20 + EVENT_PLANE_SLACK_MS).ceil() as u64,
        abs99: ((enabled.p99 as f64) * 1.20 + EVENT_PLANE_SLACK_MS).ceil() as u64,
        absmax: ((enabled.p99 as f64) * 3.00 + EVENT_PLANE_SLACK_MS).ceil() as u64,
        thrmin: floor_int(enabled.throughput * EVENT_PLANE_THROUGHPUT_T),
    }
}

fn derive_all_thresholds(enabled: &BTreeMap<String, OpMetrics>) -> BTreeMap<String, DerivedThresholds> {
    enabled
        .iter()
        .map(|(name, metrics)| (name.clone(), derive_thresholds(metrics)))
        .collect()
}

fn gate_relative(
    enabled: &OpMetrics,
    decoupled: &OpMetrics,
    thresholds: &DerivedThresholds,
) -> Result<(), String> {
    let r = EVENT_PLANE_RATIO_R;
    let s = EVENT_PLANE_SLACK_MS;
    let t = EVENT_PLANE_THROUGHPUT_T;
    if enabled.p50 as f64 > (decoupled.p50 as f64) * r + s {
        return Err("p50 relative".into());
    }
    if enabled.p95 as f64 > (decoupled.p95 as f64) * r + s {
        return Err("p95 relative".into());
    }
    if enabled.p99 as f64 > (decoupled.p99 as f64) * r + s {
        return Err("p99 relative".into());
    }
    if enabled.max as f64 > (decoupled.max as f64) * 3.00 + s {
        return Err("max relative".into());
    }
    if round3(enabled.throughput) < round3(decoupled.throughput * t) {
        return Err("throughput relative".into());
    }
    if enabled.p50 > thresholds.abs50
        || enabled.p95 > thresholds.abs95
        || enabled.p99 > thresholds.abs99
        || enabled.max > thresholds.absmax
        || round3(enabled.throughput) < thresholds.thrmin as f64
    {
        return Err("absolute".into());
    }
    Ok(())
}

fn metrics_from_stats(stats: &OperationStats, window_secs: u64) -> OpMetrics {
    assert_eq!(stats.failures, 0, "measurement arm failure is product_failure");
    assert!(
        stats.successes >= EVENT_PLANE_MIN_SAMPLES,
        "need {} samples, got {}",
        EVENT_PLANE_MIN_SAMPLES,
        stats.successes
    );
    OpMetrics {
        attempts: stats.attempts,
        successes: stats.successes,
        failures: stats.failures,
        p50: nearest_rank(&stats.samples_ms, 0.50),
        p95: nearest_rank(&stats.samples_ms, 0.95),
        p99: nearest_rank(&stats.samples_ms, 0.99),
        max: sample_max(&stats.samples_ms),
        throughput: stats.successes as f64 / window_secs.max(1) as f64,
    }
}

fn metrics_for_arm(arm: &ArmReport) -> BTreeMap<String, OpMetrics> {
    assert_eq!(arm.profile.n, EVENT_PLANE_FLEET_N);
    assert_eq!(arm.profile.window_secs, 600);
    arm.operations
        .iter()
        .map(|(name, stats)| {
            (
                name.clone(),
                metrics_from_stats(stats, arm.profile.window_secs),
            )
        })
        .collect()
}

fn gate_all(
    enabled: &BTreeMap<String, OpMetrics>,
    decoupled: &BTreeMap<String, OpMetrics>,
    thresholds: &BTreeMap<String, DerivedThresholds>,
) {
    for operation in EVENT_PLANE_GATED_OPERATIONS {
        let e = enabled.get(operation).expect("enabled metrics");
        let d = decoupled.get(operation).expect("decoupled metrics");
        let t = thresholds.get(operation).expect("thresholds");
        gate_relative(e, d, t).unwrap_or_else(|reason| {
            panic!("{operation} {reason} gate failed enabled={e:?} decoupled={d:?}")
        });
    }
}

fn calibration_path() -> PathBuf {
    if let Ok(path) = std::env::var("BOTSTER_EVENT_PLANE_CALIBRATION_OUT") {
        return PathBuf::from(path);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json",
    )
}

fn artifact_dir() -> Option<PathBuf> {
    std::env::var("BOTSTER_EVENT_PLANE_ARTIFACT_DIR")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn may_commit_calibration() -> bool {
    cfg!(target_os = "linux")
        && std::env::var("BOTSTER_EVENT_PLANE_COMMIT_CALIBRATION").as_deref() == Ok("1")
}

fn operation_row(
    enabled: &OpMetrics,
    decoupled: &OpMetrics,
    thresholds: &DerivedThresholds,
) -> serde_json::Value {
    serde_json::json!({
        "enabled": {
            "attempts": enabled.attempts,
            "successes": enabled.successes,
            "failures": enabled.failures,
            "p50": enabled.p50,
            "p95": enabled.p95,
            "p99": enabled.p99,
            "max": enabled.max,
            "throughput": enabled.throughput
        },
        "decoupled": {
            "attempts": decoupled.attempts,
            "successes": decoupled.successes,
            "failures": decoupled.failures,
            "p50": decoupled.p50,
            "p95": decoupled.p95,
            "p99": decoupled.p99,
            "max": decoupled.max,
            "throughput": decoupled.throughput
        },
        "thresholds": {
            "ABS50": thresholds.abs50,
            "ABS95": thresholds.abs95,
            "ABS99": thresholds.abs99,
            "ABSMAX": thresholds.absmax,
            "THRMIN": thresholds.thrmin
        }
    })
}

fn campaign_literals() -> serde_json::Value {
    serde_json::json!({
        "R": EVENT_PLANE_RATIO_R,
        "S_ms": EVENT_PLANE_SLACK_MS,
        "T": EVENT_PLANE_THROUGHPUT_T,
        "N": EVENT_PLANE_FLEET_N,
        "window_seconds": EVENT_PLANE_MEASUREMENT_WINDOW.as_secs(),
        "warmup_seconds": EVENT_PLANE_WARMUP.as_secs(),
        "minimum_samples": EVENT_PLANE_MIN_SAMPLES,
        "driver_concurrency": EVENT_PLANE_DRIVER_CONCURRENCY,
        "events_per_second": 150,
        "burst_count": EVENT_PLANE_BURST_COUNT,
        "payload_bytes": 4096,
        "terminal_output_period_ms": 100,
        "terminal_input_period_ms": 500
    })
}

fn write_json_destinations(body: &serde_json::Value, phase: &str) {
    let encoded = serde_json::to_string_pretty(body).expect("encode dataset");
    if let Some(dir) = artifact_dir() {
        let artifact = dir.join(format!("event-plane-saturation-{phase}.json"));
        fs::create_dir_all(&dir).expect("create artifact dir");
        fs::write(&artifact, &encoded).expect("write artifact dataset");
        eprintln!("event-plane {phase} dataset written to {}", artifact.display());
    }
    if phase == "calibration" && may_commit_calibration() {
        let dest = calibration_path();
        fs::write(&dest, &encoded).expect("write committed calibration");
        eprintln!("event-plane calibration committed to {}", dest.display());
    } else if phase == "calibration" && artifact_dir().is_none() {
        let dest = std::env::temp_dir().join("event-plane-saturation-calibration.json");
        fs::write(&dest, encoded).expect("write temp calibration");
        eprintln!("event-plane calibration dataset written to {}", dest.display());
    }
}

fn latency_json(histogram: &botster_hub_client::DaemonLatencyHistogram) -> serde_json::Value {
    serde_json::json!({
        "buckets": histogram.buckets,
        "count": histogram.count,
        "sum_us": histogram.sum_us,
        "max_us": histogram.max_us
    })
}

fn dummy_op_metrics() -> OpMetrics {
    OpMetrics {
        attempts: 200,
        successes: 200,
        failures: 0,
        p50: 40,
        p95: 80,
        p99: 90,
        max: 100,
        throughput: 10.0,
    }
}

fn observability_json(
    observability: &botster_hub_client::DaemonObservabilityCounters,
) -> serde_json::Value {
    serde_json::json!({
        "event_admission_attempts": observability.event_admission_attempts,
        "event_delivery_attempts": observability.event_delivery_attempts,
        "event_admission_latency": latency_json(&observability.event_admission_latency),
        "event_delivery_latency": latency_json(&observability.event_delivery_latency),
        "event_shed_by_reason": observability.event_shed_by_reason,
        "event_gaps": observability.event_gaps,
        "event_mailbox_overflow_gaps": observability.event_mailbox_overflow_gaps,
        "event_handler_timed_out": observability.event_handler_timed_out,
        "event_handler_backpressured": observability.event_handler_backpressured,
        "event_handler_worker_stopped": observability.event_handler_worker_stopped,
        "event_handler_failed": observability.event_handler_failed,
        "event_router_queue_age_expiries": observability.event_router_queue_age_expiries,
        "event_mailbox_queue_age_expiries": observability.event_mailbox_queue_age_expiries,
        "stalled_write_timeouts": observability.stalled_write_timeouts,
        "max_owner_turn_us": observability.max_owner_turn_us,
        "max_ready_operation_wait_us": observability.max_ready_operation_wait_us,
        "global_in_flight_bytes": observability.global_in_flight_bytes,
        "queue_ages": observability.queue_ages.iter().map(|age| serde_json::json!({
            "kind": age.kind.as_str(),
            "identity": age.identity,
            "queue_count": age.queue_count,
            "queue_bytes": age.queue_bytes,
            "oldest_age_us": age.oldest_age_us
        })).collect::<Vec<_>>()
    })
}

fn lifecycle_json(
    lifecycle: &botster_hub_client::DaemonLifecycleCounters,
) -> serde_json::Value {
    serde_json::json!({
        "lifecycle_change_reads": lifecycle.lifecycle_change_reads,
        "lifecycle_baseline_reads": lifecycle.lifecycle_baseline_reads,
        "lifecycle_resync_reads": lifecycle.lifecycle_resync_reads
    })
}

#[allow(clippy::too_many_arguments)]
fn phase_dataset_body(
    phase: CampaignPhase,
    enabled: &BTreeMap<String, OpMetrics>,
    decoupled: &BTreeMap<String, OpMetrics>,
    thresholds: &BTreeMap<String, DerivedThresholds>,
    enabled_obs: &botster_hub_client::DaemonObservabilityCounters,
    decoupled_obs: &botster_hub_client::DaemonObservabilityCounters,
    faults: &FaultReport,
    webrtc_queues: Option<serde_json::Value>,
    source_revision: Option<String>,
    host_validity: &HostValidity,
) -> serde_json::Value {
    let mut operations = serde_json::Map::new();
    for operation in EVENT_PLANE_GATED_OPERATIONS {
        operations.insert(
            operation.to_string(),
            operation_row(&enabled[operation], &decoupled[operation], &thresholds[operation]),
        );
    }
    let status = match phase {
        CampaignPhase::Calibration => "calibrated",
        CampaignPhase::Acceptance => "accepted",
    };
    serde_json::json!({
        "campaign": "event-plane-saturation",
        "status": status,
        "literals": campaign_literals(),
        "formulas": {
            "ABS50": "ceil_ms(P50cal_e * 1.20 + S)",
            "ABS95": "ceil_ms(P95cal_e * 1.20 + S)",
            "ABS99": "ceil_ms(P99cal_e * 1.20 + S)",
            "ABSMAX": "ceil_ms(P99cal_e * 3.00 + S)",
            "THRMIN": "floor_int(THRcal_e * T)",
            "percentile": "nearest_rank ceil(p * n)"
        },
        "profile": {
            "runner": EVENT_PLANE_RUNNER,
            "stress_profile": EVENT_PLANE_STRESS_PROFILE
        },
        "host_validity": host_validity.json(),
        "revisions": {
            "source_revision": source_revision,
            "botster_core": "7eafa470a18025895995bbedc20d34b58106a03b",
            "calibration_dataset_commit": git_path_commit(&calibration_path()),
            "acceptance_revision": match phase {
                CampaignPhase::Acceptance => std::env::var("SUBJECT_SHA").ok(),
                CampaignPhase::Calibration => None,
            }
        },
        "observability": {
            "enabled": observability_json(enabled_obs),
            "decoupled": observability_json(decoupled_obs),
            "faults": observability_json(&faults.observability)
        },
        "lifecycle": {
            "faults": lifecycle_json(&faults.lifecycle)
        },
        "client_fixture_queues": {
            "webrtc": webrtc_queues
        },
        "gated_against": match phase {
            CampaignPhase::Acceptance => Some(calibration_path().display().to_string()),
            CampaignPhase::Calibration => None,
        },
        "thresholds": operations,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_phase_dataset(
    phase: CampaignPhase,
    enabled: &BTreeMap<String, OpMetrics>,
    decoupled: &BTreeMap<String, OpMetrics>,
    thresholds: &BTreeMap<String, DerivedThresholds>,
    enabled_obs: &botster_hub_client::DaemonObservabilityCounters,
    decoupled_obs: &botster_hub_client::DaemonObservabilityCounters,
    faults: &FaultReport,
    webrtc_queues: Option<serde_json::Value>,
    source_revision: Option<String>,
    host_validity: &HostValidity,
) {
    let body = phase_dataset_body(
        phase,
        enabled,
        decoupled,
        thresholds,
        enabled_obs,
        decoupled_obs,
        faults,
        webrtc_queues,
        source_revision,
        host_validity,
    );
    let phase_name = match phase {
        CampaignPhase::Calibration => "calibration",
        CampaignPhase::Acceptance => "acceptance",
    };
    write_json_destinations(&body, phase_name);
}

fn git_path_commit(path: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["log", "-1", "--format=%H", "--", &path.display().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (sha.len() == 40).then_some(sha)
}

fn read_committed_thresholds() -> (BTreeMap<String, DerivedThresholds>, Option<String>) {
    let path = calibration_path();
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).expect("read calibration"))
            .expect("parse calibration");
    assert_eq!(value["status"], "calibrated", "acceptance needs committed thresholds");
    assert_eq!(value["literals"]["R"].as_f64(), Some(EVENT_PLANE_RATIO_R));
    assert_eq!(value["literals"]["S_ms"].as_f64(), Some(EVENT_PLANE_SLACK_MS));
    assert_eq!(value["literals"]["T"].as_f64(), Some(EVENT_PLANE_THROUGHPUT_T));
    assert_eq!(
        value["literals"]["N"].as_u64(),
        Some(EVENT_PLANE_FLEET_N as u64)
    );
    assert_eq!(
        value["literals"]["window_seconds"].as_u64(),
        Some(EVENT_PLANE_MEASUREMENT_WINDOW.as_secs())
    );
    assert_eq!(
        value["literals"]["minimum_samples"].as_u64(),
        Some(EVENT_PLANE_MIN_SAMPLES)
    );
    assert_eq!(value["profile"]["runner"], EVENT_PLANE_RUNNER);
    assert_eq!(value["profile"]["stress_profile"], EVENT_PLANE_STRESS_PROFILE);
    assert_eq!(
        value["formulas"]["THRMIN"],
        "floor_int(THRcal_e * T)",
        "acceptance refuses a calibration record with a different throughput formula"
    );
    let source = value["revisions"]["source_revision"]
        .as_str()
        .or_else(|| value["executed_revisions"]["botster-hub"].as_str());
    if let Some(source) = source {
        assert_eq!(
            source.len(),
            40,
            "calibration source_revision must be a 40-character SHA"
        );
    }
    let dataset_commit = git_path_commit(&path).expect("calibration dataset must be in git history");
    let expected = std::env::var("BOTSTER_EVENT_PLANE_CALIBRATION_COMMIT")
        .expect("acceptance requires BOTSTER_EVENT_PLANE_CALIBRATION_COMMIT");
    assert_eq!(
        expected.len(),
        40,
        "BOTSTER_EVENT_PLANE_CALIBRATION_COMMIT must be a 40-character SHA"
    );
    assert_eq!(
        dataset_commit, expected,
        "acceptance checkout must be the selected calibration dataset commit"
    );
    let mut out = BTreeMap::new();
    for operation in EVENT_PLANE_GATED_OPERATIONS {
        let row = &value["thresholds"][operation];
        out.insert(
            operation.to_string(),
            DerivedThresholds {
                abs50: row["thresholds"]["ABS50"].as_u64().expect("ABS50"),
                abs95: row["thresholds"]["ABS95"].as_u64().expect("ABS95"),
                abs99: row["thresholds"]["ABS99"].as_u64().expect("ABS99"),
                absmax: row["thresholds"]["ABSMAX"].as_u64().expect("ABSMAX"),
                thrmin: row["thresholds"]["THRMIN"]
                    .as_u64()
                    .or_else(|| row["thresholds"]["THRMIN"].as_f64().map(floor_int))
                    .expect("THRMIN"),
            },
        );
    }
    (out, source.map(str::to_string))
}

fn run_saturation_arm(arm: SaturationArm) -> ArmReport {
    let label = match arm {
        SaturationArm::PlaneEnabled => "enabled",
        SaturationArm::PlaneDecoupled => "decoupled",
    };
    let hub = start_campaign_hub(label, &[]);
    let endpoint = hub.endpoint().clone();
    let stop = Arc::new(AtomicBool::new(false));
    let mut unix = None;
    let mut webrtc = None;
    let mut emitter = None;
    if matches!(arm, SaturationArm::PlaneEnabled) {
        enable_saturation_packages(&endpoint, hub.data_dir());
        unix = Some(subscribe_unix_events(&endpoint));
        webrtc = Some(subscribe_webrtc_events(&endpoint, hub.data_dir()));
        emitter = Some(spawn_event_emitter(&endpoint, stop.clone()));
    }
    spawn_quiet_fleet(&endpoint);
    let mut noisy = spawn_noisy_session(&endpoint);
    let (operations, worker_errors) = run_measurement_workers(
        &endpoint,
        hub.data_dir(),
        &mut unix,
        &mut webrtc,
        &mut noisy,
    );
    assert!(
        worker_errors.is_empty(),
        "measurement worker failed: {worker_errors:?}"
    );
    if let Some(connection) = unix.as_mut() {
        prove_client_contract_under_saturation(connection, &endpoint);
    }
    prove_north_star(&endpoint, &mut noisy);
    let observability = snapshot_observability(&endpoint);
    if matches!(arm, SaturationArm::PlaneEnabled) {
        assert_required_signals(&observability, true);
        run_late_event_holder_matrix(&endpoint);
        late_webrtc_event_orders(&endpoint, hub.data_dir());
    }
    stop.store(true, Ordering::SeqCst);
    if let Some(join) = emitter {
        join.join().expect("join emitter");
    }
    let webrtc_queues = webrtc.as_ref().map(|(_, peer, _)| {
        let snap = peer.fixture_queue_snapshot();
        assert_webrtc_queue_snapshot_within_budget(&snap);
        snap
    });
    if let Some((_, peer, key)) = webrtc {
        prove_webrtc_close_unix_survives(&endpoint, peer, &key);
    }
    shutdown_owned_sessions(&endpoint);
    assert_no_live_sessions(&endpoint);
    hub.shutdown().expect("shutdown saturation hub");
    ArmReport {
        profile: ArmProfile {
            n: EVENT_PLANE_FLEET_N,
            window_secs: EVENT_PLANE_MEASUREMENT_WINDOW.as_secs(),
        },
        operations,
        observability,
        webrtc_queues,
    }
}

fn start_campaign_hub(
    name: &str,
    extra_env: &[(&str, &str)],
) -> botster_hub_test_support::IsolatedHub {
    let mut builder = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from(format!("/tmp/bh-event-sat-{name}")))
        .name(format!("event-sat-{name}"));
    for (key, value) in extra_env {
        builder = builder.env(*key, *value);
    }
    match builder.start() {
        Ok(hub) => hub,
        Err(error) => {
            let io_kind = io::Error::other(error.to_string());
            eprintln!(
                "{}",
                format_harness_budget_expired(
                    "start",
                    Duration::from_secs(5),
                    classify_os_resource(&io_kind),
                    probe_fd_limit(),
                    &format!("isolated hub start failed: {io_kind}")
                )
            );
            panic!("isolated hub start failed: {io_kind}");
        }
    }
}

fn enable_saturation_packages(endpoint: &botster_hub_client::DaemonEndpoint, data_dir: &Path) {
    let producer_src =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
    let consumer_src =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-consumer");
    let producer_dir = unique_test_dir("event-plane-producer-sat");
    let consumer_dir = unique_test_dir("event-plane-consumer-sat");
    copy_dir_all(&producer_src, &producer_dir);
    copy_dir_all(&consumer_src, &consumer_dir);
    rewrite_package_source_path(&producer_dir);
    rewrite_package_source_path(&consumer_dir);
    for path in [&producer_dir, &consumer_dir] {
        let enabled = botster_hub_client::request(
            endpoint,
            botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: path.clone() },
        )
        .expect("enable saturation package");
        assert_eq!(
            enabled.kind,
            botster_hub_client::DaemonResponseKind::PackageDecision
        );
    }
    let matrix_dir = copy_plugin_contract_matrix_fixture(data_dir.join("matrix"))
        .expect("materialize plugin-contract-matrix");
    let enabled = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: matrix_dir },
    )
    .expect("enable contract matrix");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
}

fn subscribe_unix_events(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> botster_hub_client::DaemonConnection {
    subscribe_unix_events_filtered(endpoint, "sub-sat-unix", Vec::new())
}

fn subscribe_unix_events_filtered(
    endpoint: &botster_hub_client::DaemonEndpoint,
    subscription_id: &str,
    subjects: Vec<String>,
) -> botster_hub_client::DaemonConnection {
    let mut connection =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("unix events");
    let subscribed = connection
        .subscribe_events(
            subscription_id,
            "event-plane-producer",
            "sample.ready",
            subjects,
        )
        .expect("subscribe unix");
    assert_eq!(
        subscribed.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    connection
}

fn unique_event_marker(kind: &str) -> String {
    format!("{kind}-{}", unix_now_ns())
}

fn emit_unique_event(endpoint: &botster_hub_client::DaemonEndpoint, token: &str) {
    let emitted = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::PluginMcpCallTool {
            name: "event_plane.emit_ready".to_string(),
            arguments: serde_json::json!({ "token": token, "subject": token }),
        },
    )
    .expect("emit unique event");
    assert_ne!(
        emitted.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "unique event emit failed: {emitted:?}"
    );
}

fn package_event_has_token(event: &botster_hub_client::DaemonEvent, token: &str) -> bool {
    matches!(
        event,
        botster_hub_client::DaemonEvent::PackageEvent { payload, .. }
            if payload.get("token").and_then(|value| value.as_str()) == Some(token)
    )
}

fn event_gap_for_subscription(event: &botster_hub_client::DaemonEvent, subscription_id: &str) -> bool {
    matches!(
        event,
        botster_hub_client::DaemonEvent::EventGap { subscription_id: sid, .. }
            if sid == subscription_id
    )
}

fn wait_unix_marker_or_gap(
    connection: &mut botster_hub_client::DaemonConnection,
    token: &str,
    subscription_id: &str,
) {
    connection
        .set_read_timeout(Some(Duration::from_millis(250)))
        .ok();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match connection.next_event() {
            Ok(event) if package_event_has_token(&event, token) => {
                connection.set_read_timeout(None).ok();
                return;
            }
            Ok(event) if event_gap_for_subscription(&event, subscription_id) => {
                connection.set_read_timeout(None).ok();
                return;
            }
            Ok(_) | Err(_) => {}
        }
    }
    connection.set_read_timeout(None).ok();
    panic!(
        "subscribed client must receive unique marker {token} or EventGap for {subscription_id}"
    );
}

fn assert_webrtc_queue_snapshot_within_budget(snap: &serde_json::Value) {
    if snap["inbound_frames"]["overflow"].as_u64().unwrap_or(0) > 0
        || snap["pending_host_events"]["overflow"].as_u64().unwrap_or(0) > 0
    {
        panic!("product_failure webrtc fixture overflow: {snap}");
    }
    if snap["inbound_frames"]["oldest_age_us"]
        .as_u64()
        .is_some_and(|age| age > EVENT_PLANE_QUEUE_AGE_US)
    {
        panic!("product_failure webrtc inbound oldest_age_us exceeds queue_age: {snap}");
    }
    if snap["pending_host_events"]["oldest_age_us"]
        .as_u64()
        .is_some_and(|age| age > EVENT_PLANE_QUEUE_AGE_US)
    {
        panic!("product_failure webrtc pending_host_events oldest_age_us exceeds queue_age: {snap}");
    }
    let inbound_bytes = snap["inbound_frames"]["bytes"].as_u64().unwrap_or(0);
    let inbound_max = snap["inbound_frames"]["max_bytes"].as_u64().unwrap_or(0);
    if inbound_max > 0 && inbound_bytes > inbound_max {
        panic!("product_failure webrtc inbound bytes exceed max_bytes: {snap}");
    }
    let pending_bytes = snap["pending_host_events"]["bytes"].as_u64().unwrap_or(0);
    let pending_max = snap["pending_host_events"]["max_bytes"].as_u64().unwrap_or(0);
    if pending_max == 0 {
        panic!("product_failure webrtc pending_host_events max_bytes missing: {snap}");
    }
    if pending_bytes > pending_max {
        panic!("product_failure webrtc pending_host_events bytes exceed max_bytes: {snap}");
    }
}

fn drain_webrtc_host_events(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    saw_event: &mut bool,
    saw_gap: &mut bool,
) {
    let snap = peer.fixture_queue_snapshot();
    assert_webrtc_queue_snapshot_within_budget(&snap);
    let slice_end = Instant::now() + Duration::from_millis(100);
    loop {
        match block_on(async { timeout(Duration::from_millis(10), peer.next_host_event(key)).await })
        {
            Ok(Ok(botster_hub_client::DaemonEvent::PackageEvent { .. })) => *saw_event = true,
            Ok(Ok(botster_hub_client::DaemonEvent::EventGap { .. })) => *saw_gap = true,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("product_failure webrtc host event: {error}"),
            Err(_) => break,
        }
        if Instant::now() >= slice_end {
            break;
        }
    }
}

fn fail_webrtc_saturation(
    endpoint: &botster_hub_client::DaemonEndpoint,
    data_dir: &Path,
    peer: &LocalWebrtcOfferPeer,
    error: &str,
) -> ! {
    let record_path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    let terminal = fs::read_to_string(&record_path).unwrap_or_else(|_| "missing".to_string());
    let observability = snapshot_observability(endpoint);
    let pty = probe_pty_allocation();
    let fd = probe_fd_limit();
    let lag = probe_scheduler_lag();
    let validity = classify_host_validity(None, &lag, fd, pty, host_load_diagnostics());
    let class = if validity.verdict == HostValidityVerdict::Exhausted {
        "host_exhaustion"
    } else {
        "product_failure"
    };
    panic!(
        "{class} webrtc control failed: {error}; overflow={}; pty={:?} fd={:?} scheduler_lag_us={} budget_us={}; terminal={terminal}; observability={observability:?}",
        peer.inbound_overflow(),
        pty.marker_name(),
        fd.marker_name(),
        lag.lag_us,
        lag.budget_us
    );
}

fn wait_webrtc_marker_or_gap(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    token: &str,
    subscription_id: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match block_on(async { timeout(Duration::from_millis(250), peer.next_host_event(key)).await })
        {
            Ok(Ok(event)) if package_event_has_token(&event, token) => return,
            Ok(Ok(event)) if event_gap_for_subscription(&event, subscription_id) => return,
            _ => {}
        }
    }
    panic!(
        "replacement WebRTC peer must receive unique marker {token} or EventGap for {subscription_id}"
    );
}

fn subscribe_webrtc_events(
    endpoint: &botster_hub_client::DaemonEndpoint,
    data_dir: &Path,
) -> (
    botster_hub_client::DaemonLocalWebrtcBootstrap,
    LocalWebrtcOfferPeer,
    botster_core::AesGcmKey,
) {
    let package_dir = unique_test_dir("event-sat-web");
    write_botster_web_package(&package_dir);
    enable_supervised_package(data_dir, &package_dir);
    let (_origin, bootstrap) = start_botster_web_and_issue_bootstrap(endpoint);
    let (peer, key) = block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(endpoint, &bootstrap).await;
        peer.enable_host_events();
        let ack = peer
            .encrypted_hello(&key, &webrtc_package_event_hello())
            .await
            .expect("package-event hello");
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS)
        );
        let subscribed = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEvents {
                    subscription_id: "sub-sat-webrtc".to_string(),
                    owner: "event-plane-producer".to_string(),
                    name: "sample.ready".to_string(),
                    subjects: Vec::new(),
                },
            )
            .await
            .expect("subscribe webrtc");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        (peer, key)
    });
    (bootstrap, peer, key)
}

fn spawn_event_emitter(
    endpoint: &botster_hub_client::DaemonEndpoint,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let endpoint = endpoint.clone();
    thread::spawn(move || {
        let interval = Duration::from_millis(1000 / EVENT_PLANE_BURSTS_PER_SEC);
        let mut burst: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            burst += 1;
            let started = Instant::now();
            let _ = botster_hub_client::request(
                &endpoint,
                botster_hub_client::DaemonRequest::PluginMcpCallTool {
                    name: "event_plane.emit_burst".to_string(),
                    arguments: serde_json::json!({
                        "count": EVENT_PLANE_BURST_COUNT,
                        "prefix": format!("sat-{burst}")
                    }),
                },
            );
            let elapsed = started.elapsed();
            if elapsed < interval {
                thread::sleep(interval - elapsed);
            }
        }
    })
}

fn spawn_quiet_fleet(endpoint: &botster_hub_client::DaemonEndpoint) {
    for wave in 0..(EVENT_PLANE_FLEET_N / EVENT_PLANE_SPAWN_WAVE) {
        for index in 0..EVENT_PLANE_SPAWN_WAVE {
            let session_id = format!("quiet-{wave}-{index}");
            match botster_hub_client::request(
                endpoint,
                botster_hub_client::DaemonRequest::Spawn {
                    session_id,
                    command: "exec sleep 7200".to_string(),
                },
            ) {
                Ok(response)
                    if response.kind == botster_hub_client::DaemonResponseKind::Spawned => {}
                Ok(response) => {
                    if let Some(error) = &response.error {
                        fail_spawn_resource(error);
                    }
                    panic!("quiet spawn failed: {response:?}");
                }
                Err(error) => panic!("quiet spawn transport failed: {error}"),
            }
        }
        thread::sleep(EVENT_PLANE_WAVE_GAP);
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = botster_hub_client::request(
            endpoint,
            botster_hub_client::DaemonRequest::ListSessions,
        )
        .expect("list quiet sessions");
        let running = listed
            .sessions
            .iter()
            .filter(|session| {
                session.session_id.starts_with("quiet-") && session.lifecycle == "running"
            })
            .count();
        if running >= EVENT_PLANE_FLEET_N {
            break;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "{}",
                format_harness_budget_expired(
                    "fleet",
                    Duration::from_secs(30),
                    HostResourceClass::PtyAllocation,
                    probe_pty_allocation(),
                    &format!("quiet running={running} want={EVENT_PLANE_FLEET_N}")
                )
            );
            panic!("quiet fleet did not reach {EVENT_PLANE_FLEET_N}: running={running}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

struct NoisySession {
    connection: botster_hub_client::DaemonConnection,
}

fn spawn_noisy_session(endpoint: &botster_hub_client::DaemonEndpoint) -> NoisySession {
    let spawned = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            command: EVENT_PLANE_NOISY_COMMAND.to_string(),
        },
    )
    .expect("spawn noisy");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let mut connection =
        botster_hub_client::DaemonConnection::connect(endpoint).expect("noisy connection");
    let attached = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            subscription_id: EVENT_PLANE_NOISY_SUB.to_string(),
        })
        .expect("attach noisy");
    assert_eq!(
        attached.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    NoisySession { connection }
}

fn run_measurement_workers(
    endpoint: &botster_hub_client::DaemonEndpoint,
    data_dir: &Path,
    unix: &mut Option<botster_hub_client::DaemonConnection>,
    webrtc: &mut Option<(
        botster_hub_client::DaemonLocalWebrtcBootstrap,
        LocalWebrtcOfferPeer,
        botster_core::AesGcmKey,
    )>,
    noisy: &mut NoisySession,
) -> (BTreeMap<String, OperationStats>, Vec<String>) {
    let start_at = Instant::now() + EVENT_PLANE_WARMUP;
    let end_at = start_at + EVENT_PLANE_MEASUREMENT_WINDOW;
    let (tx, rx) = mpsc::channel();
    let mut joins = Vec::new();
    for worker in 0..EVENT_PLANE_DRIVER_CONCURRENCY {
        let tx = tx.clone();
        let endpoint = endpoint.clone();
        joins.push(thread::spawn(move || {
            let mut cycle: u64 = 0;
            while Instant::now() < end_at {
                cycle += 1;
                let session_id = format!("churn-{worker}-{cycle}");
                let sub_id = format!("churn-sub-{worker}-{cycle}");
                for operation in EVENT_PLANE_OPERATIONS {
                    let op_start = Instant::now();
                    let in_window = op_start >= start_at && Instant::now() < end_at;
                    let result =
                        perform_cycle_operation(&endpoint, operation, &session_id, &sub_id);
                    let elapsed = op_start.elapsed();
                    let finished_in_window = Instant::now() <= end_at;
                    if in_window && finished_in_window {
                        tx.send((operation.to_string(), result.is_ok(), elapsed.as_millis() as u64))
                            .expect("send sample");
                    }
                    if let Err(error) = result {
                        return Err(format!("{operation}: {error}"));
                    }
                }
            }
            Ok(())
        }));
    }
    let mut saw_unix_event = false;
    let mut saw_unix_gap = false;
    let mut saw_webrtc_event = false;
    let mut saw_webrtc_gap = false;
    let mut next_input_at = start_at;
    let mut input_seq: u64 = 0;
    let mut next_output_seq: Option<u64> = None;
    while Instant::now() < end_at {
        if let Some(connection) = unix.as_mut() {
            let status = connection
                .request(&botster_hub_client::DaemonRequest::Status)
                .expect("unix status during saturation");
            assert_eq!(
                status.kind,
                botster_hub_client::DaemonResponseKind::Status,
                "control must progress on the subscribed Unix connection under saturation"
            );
            let _ = connection.take_skipped_events();
            connection
                .set_read_timeout(Some(Duration::from_millis(20)))
                .ok();
            match connection.next_event() {
                Ok(botster_hub_client::DaemonEvent::PackageEvent { .. }) => {
                    saw_unix_event = true;
                }
                Ok(botster_hub_client::DaemonEvent::EventGap { .. }) => {
                    saw_unix_gap = true;
                }
                Ok(_) | Err(_) => {}
            }
        }
        if let Some((_, peer, key)) = webrtc.as_mut() {
            let response = block_on(async {
                peer.encrypted_request(key, &botster_hub_client::DaemonRequest::Status)
                    .await
            });
            let response = match response {
                Ok(response) => response,
                Err(error) => fail_webrtc_saturation(endpoint, data_dir, peer, &error.to_string()),
            };
            if response.kind != botster_hub_client::DaemonResponseKind::Status {
                fail_webrtc_saturation(
                    endpoint,
                    data_dir,
                    peer,
                    &format!("unexpected status kind {response:?}"),
                );
            }
            drain_webrtc_host_events(peer, key, &mut saw_webrtc_event, &mut saw_webrtc_gap);
        }
        let now = Instant::now();
        if now >= next_input_at && now < end_at {
            input_seq += 1;
            let token = format!("ti-{input_seq}");
            let payload = format!("{token}{}\r", "x".repeat(64_usize.saturating_sub(token.len())));
            let input_start = Instant::now();
            let in_window = input_start >= start_at;
            let sent = noisy
                .connection
                .request(&botster_hub_client::DaemonRequest::SendInput {
                    session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
                    data: payload,
                });
            let echo_marker = format!("ns-echo:{token}");
            let echoed = collect_attach_events(
                &mut noisy.connection,
                EVENT_PLANE_NOISY_SESSION,
                EVENT_PLANE_NOISY_SUB,
                Some(&echo_marker),
            );
            let finished = Instant::now();
            if in_window && finished <= end_at {
                tx.send((
                    "terminal_input".to_string(),
                    sent.is_ok()
                        && echoed.iter().any(|event| {
                            matches!(
                                event,
                                botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                                    if live_output_contains(payload, &echo_marker)
                            )
                        }),
                    input_start.elapsed().as_millis() as u64,
                ))
                .expect("send terminal input sample");
            }
            next_input_at += Duration::from_millis(500);
        }
        let output_start = Instant::now();
        let drain = noisy
            .connection
            .request(&botster_hub_client::DaemonRequest::drain_subscription(
                EVENT_PLANE_NOISY_SESSION,
                EVENT_PLANE_NOISY_SUB,
            ));
        let in_window = output_start >= start_at && Instant::now() <= end_at;
        if drain.is_err() && in_window {
            tx.send((
                "terminal_output".to_string(),
                false,
                output_start.elapsed().as_millis() as u64,
            ))
            .expect("send terminal output failure");
        } else if drain.is_ok() {
            let drained = collect_attach_events(
                &mut noisy.connection,
                EVENT_PLANE_NOISY_SESSION,
                EVENT_PLANE_NOISY_SUB,
                None,
            );
            let records = output_records(&drained);
            let receipt_ns = monotonic_now_ns();
            if in_window {
                for record in records {
                    if next_output_seq.is_some_and(|expected| record.seq != expected) {
                        tx.send((
                            "terminal_output".to_string(),
                            false,
                            output_start.elapsed().as_millis() as u64,
                        ))
                        .expect("send terminal output sequence failure");
                        break;
                    }
                    next_output_seq = Some(record.seq.saturating_add(1));
                    let latency_ms = receipt_ns.saturating_sub(record.emit_ns) / 1_000_000;
                    tx.send(("terminal_output".to_string(), true, latency_ms))
                        .expect("send terminal output sample");
                }
            } else if let Some(record) = records.last() {
                next_output_seq = Some(record.seq.saturating_add(1));
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    if let Some(connection) = unix.as_mut() {
        connection.set_read_timeout(None).ok();
        assert!(
            saw_unix_event || saw_unix_gap,
            "Unix event subscription must observe PackageEvent or EventGap during the measurement window"
        );
    }
    if webrtc.is_some() {
        assert!(
            saw_webrtc_event || saw_webrtc_gap,
            "WebRTC event subscription must observe PackageEvent or EventGap during the measurement window"
        );
    }
    drop(tx);
    let mut operations = BTreeMap::new();
    for name in EVENT_PLANE_GATED_OPERATIONS {
        operations.insert(
            name.to_string(),
            OperationStats {
                attempts: 0,
                successes: 0,
                failures: 0,
                samples_ms: Vec::new(),
            },
        );
    }
    while let Ok((operation, ok, ms)) = rx.recv() {
        let stats = operations.get_mut(&operation).expect("known operation");
        stats.attempts += 1;
        if ok {
            stats.successes += 1;
            stats.samples_ms.push(ms);
        } else {
            stats.failures += 1;
        }
    }
    let mut errors = Vec::new();
    for join in joins {
        match join.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(error),
            Err(_) => errors.push("measurement worker panicked".to_string()),
        }
    }
    (operations, errors)
}

fn perform_cycle_operation(
    endpoint: &botster_hub_client::DaemonEndpoint,
    operation: &str,
    session_id: &str,
    sub_id: &str,
) -> Result<(), String> {
    match operation {
        "spawn" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "printf 'churn-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            },
            botster_hub_client::DaemonResponseKind::Spawned,
        ),
        "attach" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: sub_id.to_string(),
            },
            botster_hub_client::DaemonResponseKind::Events,
        ),
        "drain" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::drain_subscription(session_id, sub_id),
            botster_hub_client::DaemonResponseKind::Events,
        ),
        "input" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::SendInput {
                session_id: session_id.to_string(),
                data: format!("{}\r", "i".repeat(64)),
            },
            botster_hub_client::DaemonResponseKind::Events,
        ),
        "resize" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::Resize {
                session_id: session_id.to_string(),
                rows: 24,
                cols: 80,
            },
            botster_hub_client::DaemonResponseKind::Events,
        ),
        "mcp" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::PluginMcpListTools,
            botster_hub_client::DaemonResponseKind::PluginMcpTools,
        ),
        "ui" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::ListApps,
            botster_hub_client::DaemonResponseKind::Apps,
        ),
        "entity" => expect_kind(
            endpoint,
            botster_hub_client::DaemonRequest::ListSessions,
            botster_hub_client::DaemonResponseKind::Sessions,
        ),
        "shutdown" => {
            let response = botster_hub_client::request(
                endpoint,
                botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: session_id.to_string(),
                },
            )
            .map_err(|error| error.to_string())?;
            if response.kind == botster_hub_client::DaemonResponseKind::OperatorError {
                return Err(format!("shutdown operator error: {response:?}"));
            }
            Ok(())
        }
        other => Err(format!("unknown operation {other}")),
    }
}

fn expect_kind(
    endpoint: &botster_hub_client::DaemonEndpoint,
    request: botster_hub_client::DaemonRequest,
    kind: botster_hub_client::DaemonResponseKind,
) -> Result<(), String> {
    let response =
        botster_hub_client::request(endpoint, request).map_err(|error| error.to_string())?;
    if response.kind != kind {
        return Err(format!("expected {kind:?}, got {:?}", response.kind));
    }
    Ok(())
}

struct OutputRecord {
    seq: u64,
    emit_ns: u64,
    bytes: usize,
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64
}

fn output_records(events: &[botster_hub_client::DaemonEvent]) -> Vec<OutputRecord> {
    let mut records = Vec::new();
    for event in events {
        let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event else {
            continue;
        };
        let Ok(bytes) = payload.decoded_bytes() else {
            continue;
        };
        let mut index = 0;
        while index + EVENT_PLANE_OUTPUT_RECORD_BYTES <= bytes.len() {
            let seq_ok = bytes[index] == b'N'
                && bytes[index + 1..index + 9]
                    .iter()
                    .all(|byte| byte.is_ascii_digit());
            let time_ok = bytes[index + 9] == b'T'
                && bytes[index + 10..index + EVENT_PLANE_OUTPUT_HEADER_BYTES]
                    .iter()
                    .all(|byte| byte.is_ascii_digit());
            let record_ok = seq_ok
                && time_ok
                && bytes[index + EVENT_PLANE_OUTPUT_RECORD_BYTES - 1] == b'\n';
            if record_ok {
                let seq = std::str::from_utf8(&bytes[index + 1..index + 9])
                    .expect("seq digits")
                    .parse::<u64>()
                    .expect("seq");
                let emit_ns = std::str::from_utf8(
                    &bytes[index + 10..index + EVENT_PLANE_OUTPUT_HEADER_BYTES],
                )
                .expect("emit digits")
                .parse::<u64>()
                .expect("emit_ns");
                records.push(OutputRecord {
                    seq,
                    emit_ns,
                    bytes: EVENT_PLANE_OUTPUT_RECORD_BYTES,
                });
                index += EVENT_PLANE_OUTPUT_RECORD_BYTES;
            } else {
                index += 1;
            }
        }
    }
    records
}

fn snapshot_observability(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> botster_hub_client::DaemonObservabilityCounters {
    let status = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status snapshot");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    status.status.expect("status body").observability
}

fn assert_required_signals(
    observability: &botster_hub_client::DaemonObservabilityCounters,
    events_expected: bool,
) {
    let defaults = PackageEventPlaneOptions::default();
    if events_expected {
        assert!(
            observability.event_admission_attempts > 0,
            "admission attempts: {observability:?}"
        );
        assert!(
            observability.event_delivery_attempts > 0,
            "delivery attempts: {observability:?}"
        );
        assert!(
            observability.event_admission_latency.count > 0,
            "admission latency: {observability:?}"
        );
        assert!(
            observability.event_delivery_latency.count > 0
                || observability
                    .event_shed_by_reason
                    .values()
                    .any(|count| *count > 0),
            "delivery latency or shed: {observability:?}"
        );
        assert!(
            !observability.queue_ages.is_empty(),
            "queue age rows: {observability:?}"
        );
        assert_queue_bounds(observability, &defaults);
    }
    assert_ne!(
        observability.event_handler_timed_out,
        u64::MAX,
        "T1 handler timeout counter must be present"
    );
    assert_ne!(
        observability.event_router_queue_age_expiries,
        u64::MAX,
        "T2 router expiry counter must be present"
    );
    assert_ne!(
        observability.event_mailbox_queue_age_expiries,
        u64::MAX,
        "T3 mailbox expiry counter must be present"
    );
    assert_ne!(
        observability.stalled_write_timeouts,
        u64::MAX,
        "T4 stalled-write timeout counter must be present"
    );
}

fn assert_queue_bounds(
    observability: &botster_hub_client::DaemonObservabilityCounters,
    defaults: &PackageEventPlaneOptions,
) {
    let violations = queue_bound_violations(observability, defaults);
    assert!(
        violations.is_empty(),
        "queue bound violations: {violations:?}"
    );
}

fn queue_bound_violations(
    observability: &botster_hub_client::DaemonObservabilityCounters,
    defaults: &PackageEventPlaneOptions,
) -> Vec<String> {
    let mut violations = Vec::new();
    for age in &observability.queue_ages {
        match age.state {
            botster_hub_client::DaemonQueueAgeState::Empty => {
                if age.queue_count != Some(0) || age.queue_bytes != Some(0) {
                    violations.push(format!(
                        "retired queue {:?} identity={} must prove count=0 bytes=0, got count={:?} bytes={:?}",
                        age.kind, age.identity, age.queue_count, age.queue_bytes
                    ));
                }
            }
            botster_hub_client::DaemonQueueAgeState::Usable => {
                let Some(count) = age.queue_count else {
                    violations.push(format!(
                        "usable queue {:?} identity={} missing queue_count",
                        age.kind, age.identity
                    ));
                    continue;
                };
                let Some(bytes) = age.queue_bytes else {
                    violations.push(format!(
                        "usable queue {:?} identity={} missing queue_bytes",
                        age.kind, age.identity
                    ));
                    continue;
                };
                let max = match age.kind {
                    botster_hub_client::DaemonQueueKind::Producer => {
                        defaults.producer_queue_max_events as u64
                    }
                    botster_hub_client::DaemonQueueKind::Consumer
                    | botster_hub_client::DaemonQueueKind::ClientMailbox => {
                        defaults.consumer_queue_max_events as u64
                    }
                    _ => u64::MAX,
                };
                if count > max {
                    violations.push(format!(
                        "queue {:?} identity={} count={count} exceeds bound {max}",
                        age.kind, age.identity
                    ));
                }
                if let Some(age_us) = age.oldest_age_us
                    && age_us > EVENT_PLANE_QUEUE_AGE_US
                {
                    violations.push(format!(
                        "queue {:?} identity={} oldest_age_us={age_us} exceeds queue_age 1000ms",
                        age.kind, age.identity
                    ));
                }
                let max_bytes = match age.kind {
                    botster_hub_client::DaemonQueueKind::Producer => {
                        defaults.producer_queue_max_bytes as u64
                    }
                    botster_hub_client::DaemonQueueKind::Consumer
                    | botster_hub_client::DaemonQueueKind::ClientMailbox => {
                        defaults.consumer_queue_max_bytes as u64
                    }
                    _ => u64::MAX,
                };
                if bytes > max_bytes {
                    violations.push(format!(
                        "queue {:?} identity={} queue_bytes={bytes} exceeds bound {max_bytes}",
                        age.kind, age.identity
                    ));
                }
            }
            _ => {
                violations.push(format!(
                    "active queue {:?} identity={} is indeterminate without count and byte bounds",
                    age.kind, age.identity
                ));
            }
        }
    }
    if observability.global_in_flight_bytes > defaults.global_in_flight_bytes as u64 {
        violations.push(format!(
            "global_in_flight_bytes {} exceeds {}",
            observability.global_in_flight_bytes, defaults.global_in_flight_bytes
        ));
    }
    if observability.event_age_sample_failures > EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES {
        violations.push(format!(
            "event_age_sample_failures {} exceeds EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES={EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES}",
            observability.event_age_sample_failures
        ));
    }
    violations
}

fn shutdown_owned_sessions(endpoint: &botster_hub_client::DaemonEndpoint) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list sessions for teardown");
        let live: Vec<String> = listed
            .sessions
            .iter()
            .filter(|session| session.lifecycle != "exited")
            .map(|session| session.session_id.clone())
            .collect();
        if live.is_empty() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("sessions still live after ShutdownSession: {live:?}");
        }
        for session_id in live {
            let _ = botster_hub_client::request(
                endpoint,
                botster_hub_client::DaemonRequest::ShutdownSession { session_id },
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn assert_no_live_sessions(endpoint: &botster_hub_client::DaemonEndpoint) {
    let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
        .expect("no-survivor list");
    let live: Vec<_> = listed
        .sessions
        .iter()
        .filter(|session| session.lifecycle != "exited")
        .map(|session| (session.session_id.clone(), session.lifecycle.clone()))
        .collect();
    assert!(
        live.is_empty(),
        "no-survivor oracle failed: {live:?}"
    );
}

fn prove_client_contract_under_saturation(
    connection: &mut botster_hub_client::DaemonConnection,
    endpoint: &botster_hub_client::DaemonEndpoint,
) {
    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status under saturation");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let skipped = connection.take_skipped_events();
    let _ = skipped;
    connection
        .set_read_timeout(Some(Duration::from_millis(50)))
        .ok();
    let mut saw_event = false;
    for _ in 0..8 {
        match connection.next_event() {
            Ok(botster_hub_client::DaemonEvent::PackageEvent {
                owner,
                name,
                ..
            }) => {
                assert_eq!(owner, "event-plane-producer");
                assert_eq!(name, "sample.ready");
                saw_event = true;
                break;
            }
            Ok(botster_hub_client::DaemonEvent::EventGap { .. }) => {
                saw_event = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    connection.set_read_timeout(None).ok();
    assert!(
        saw_event,
        "subscribed Unix client must receive a package event or gap under saturation"
    );
    let filtered = connection
        .subscribe_events(
            "sub-sat-unix-filter",
            "event-plane-producer",
            "sample.ready",
            vec!["never-match".to_string()],
        )
        .expect("subject filter subscribe");
    assert_eq!(
        filtered.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    let unsubscribed = connection
        .unsubscribe_events("sub-sat-unix")
        .expect("unsubscribe under saturation");
    assert_eq!(
        unsubscribed.kind,
        botster_hub_client::DaemonResponseKind::EventUnsubscribed
    );
    drop(std::mem::replace(
        connection,
        botster_hub_client::connect_for_package_event_subscriptions(endpoint)
            .expect("reconnect under saturation"),
    ));
    let resubscribed = connection
        .subscribe_events(
            "sub-sat-unix-re",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("resubscribe");
    assert_eq!(
        resubscribed.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    let after = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after reconnect");
    assert_eq!(after.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(
        connection.take_skipped_events().is_empty(),
        "reconnect must not replay"
    );
}

fn prove_north_star(endpoint: &botster_hub_client::DaemonEndpoint, noisy: &mut NoisySession) {
    let identity = collect_attach_events(
        &mut noisy.connection,
        EVENT_PLANE_NOISY_SESSION,
        EVENT_PLANE_NOISY_SUB,
        Some("ns-ready"),
    );
    assert!(
        identity.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(payload, "ns-ready")
        )),
        "noisy session identity marker must appear: {identity:?}"
    );
    assert!(
        identity.iter().any(|event| match event {
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => payload
                .decoded_bytes()
                .ok()
                .is_some_and(|bytes| bytes.windows(2).any(|window| window == [0x80, 0xff])),
            _ => false,
        }),
        "noisy session must preserve exact non-UTF-8 bytes"
    );
    let input = noisy
        .connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            data: "ns-probe\r".to_string(),
        })
        .expect("input noisy");
    assert_eq!(input.kind, botster_hub_client::DaemonResponseKind::Events);
    let echoed = collect_attach_events(
        &mut noisy.connection,
        EVENT_PLANE_NOISY_SESSION,
        EVENT_PLANE_NOISY_SUB,
        Some("ns-echo:ns-probe"),
    );
    let ready_at = identity.iter().position(|event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(payload, "ns-ready")
        )
    });
    let echo_at = echoed.iter().position(|event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(payload, "ns-echo:ns-probe")
        )
    });
    assert!(ready_at.is_some() && echo_at.is_some(), "ordering oracles");
    let resize = noisy
        .connection
        .request(&botster_hub_client::DaemonRequest::Resize {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            rows: 30,
            cols: 100,
        })
        .expect("resize noisy");
    assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);
    let detached = noisy
        .connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            subscription_id: EVENT_PLANE_NOISY_SUB.to_string(),
        })
        .expect("detach noisy");
    assert_ne!(
        detached.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    drop(std::mem::replace(
        &mut noisy.connection,
        botster_hub_client::DaemonConnection::connect(endpoint).expect("reconnect noisy"),
    ));
    let reattached = noisy
        .connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            subscription_id: "event-plane-noisy-re".to_string(),
        })
        .expect("reattach noisy");
    assert_eq!(
        reattached.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    let late_events = collect_attach_events(
        &mut noisy.connection,
        EVENT_PLANE_NOISY_SESSION,
        "event-plane-noisy-re",
        Some("ns-ready"),
    );
    assert!(
        late_events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
                | botster_hub_client::DaemonEvent::Scrollback { .. }
        )),
        "late attach must deliver history: {late_events:?}"
    );
    let shutdown = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
        },
    )
    .expect("shutdown noisy");
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_exit = false;
    while Instant::now() < deadline {
        let events = collect_attach_events(
            &mut noisy.connection,
            EVENT_PLANE_NOISY_SESSION,
            "event-plane-noisy-re",
            None,
        );
        if events
            .iter()
            .any(|event| matches!(event, botster_hub_client::DaemonEvent::ProcessExit { .. }))
        {
            saw_exit = true;
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(saw_exit, "shutdown must surface ProcessExit");
}

fn prove_webrtc_close_unix_survives(
    endpoint: &botster_hub_client::DaemonEndpoint,
    peer: LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
) {
    block_on(async {
        let mut peer = peer;
        let _ = peer
            .encrypted_request(key, &botster_hub_client::DaemonRequest::Status)
            .await;
        drop(peer);
    });
    let unix_status =
        botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("unix status after webrtc close");
    assert_eq!(
        unix_status.kind,
        botster_hub_client::DaemonResponseKind::Status
    );
    let listed =
        botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("unix list after webrtc close");
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id.starts_with("quiet-")),
        "Unix fleet must survive successful WebRTC close"
    );
}

fn run_late_event_holder_matrix(endpoint: &botster_hub_client::DaemonEndpoint) {
    late_subscribe_events_closed_first(endpoint);
    late_subscribe_events_message_first(endpoint);
    late_spawn_closed_first(endpoint);
    late_spawn_message_first(endpoint);
    late_attach_closed_first(endpoint);
    late_attach_message_first(endpoint);
    late_entities_closed_first_and_message_first(endpoint);
    late_unsubscribe_events_does_not_drop_sibling(endpoint);
    late_admitted_holder_survives_reload(endpoint);
}

fn late_subscribe_events_closed_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut first =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("late first");
    first
        .subscribe_events(
            "sub-late-reuse",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("subscribe late first");
    drop(first);
    let mut second =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("late second");
    let reused = second
        .subscribe_events(
            "sub-late-reuse",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("reuse after close");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    second
        .unsubscribe_events("sub-late-reuse")
        .expect("unsubscribe reused");
}

fn late_subscribe_events_message_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut live =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("live");
    live.subscribe_events(
        "sub-late-both",
        "event-plane-producer",
        "sample.ready",
        Vec::new(),
    )
    .expect("live subscribe");
    let mut sibling =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("sibling");
    let sibling_sub = sibling
        .subscribe_events(
            "sub-late-both",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("connection-scoped sibling");
    assert_eq!(
        sibling_sub.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    drop(live);
    let status = sibling
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("sibling status after first drop");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    sibling
        .unsubscribe_events("sub-late-both")
        .expect("unsubscribe sibling");
}

fn late_spawn_closed_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let session_id = "late-spawn-closed";
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn late closed-first");
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    );
    let reused = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
    )
    .expect("reuse spawn id after shutdown");
    assert_ne!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "closed-first spawn reuse must not poison the control plane: {reused:?}"
    );
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    );
}

fn late_spawn_message_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let session_id = "late-spawn-live";
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn live");
    let duplicate = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
    )
    .expect("duplicate spawn while live");
    assert_eq!(
        duplicate.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "message-first duplicate spawn must be a typed error, not a hang"
    );
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    );
}

fn late_attach_closed_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let session_id = "late-attach-closed";
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn for late attach");
    let mut first = botster_hub_client::DaemonConnection::connect(endpoint).expect("attach first");
    first
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: "late-attach-reuse".to_string(),
        })
        .expect("first attach");
    drop(first);
    let mut second = botster_hub_client::DaemonConnection::connect(endpoint).expect("attach second");
    let reused = second
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: "late-attach-reuse".to_string(),
        })
        .expect("reuse attach id");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    drop(second);
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    );
}

fn late_attach_message_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let session_id = "late-attach-live";
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "exec sleep 30".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn for live attach");
    let mut live = botster_hub_client::DaemonConnection::connect(endpoint).expect("live attach");
    live.request(&botster_hub_client::DaemonRequest::Attach {
        session_id: session_id.to_string(),
        subscription_id: "late-attach-both".to_string(),
    })
    .expect("live attach");
    let mut sibling = botster_hub_client::DaemonConnection::connect(endpoint).expect("sibling attach");
    let sibling_attach = sibling
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: "late-attach-both".to_string(),
        })
        .expect("message-first attach");
    assert_ne!(
        sibling_attach.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "message-first attach reuse must not hang: {sibling_attach:?}"
    );
    drop(live);
    let status = sibling
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("sibling after live drop");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    drop(sibling);
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    );
}

fn late_entities_closed_first_and_message_first(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut first = botster_hub_client::DaemonConnection::connect(endpoint).expect("entity first");
    first
        .request(&botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "late-entity-reuse".to_string(),
        })
        .expect("subscribe entities");
    drop(first);
    let mut second = botster_hub_client::DaemonConnection::connect(endpoint).expect("entity second");
    let reused = second
        .request(&botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "late-entity-reuse".to_string(),
        })
        .expect("reuse entity id");
    assert_ne!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let mut live = botster_hub_client::DaemonConnection::connect(endpoint).expect("entity live");
    live.request(&botster_hub_client::DaemonRequest::SubscribeEntities {
        entity_type: "session".to_string(),
        subscription_id: "late-entity-both".to_string(),
    })
    .expect("live entity");
    let mut sibling = botster_hub_client::DaemonConnection::connect(endpoint).expect("entity sibling");
    sibling
        .request(&botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "late-entity-both".to_string(),
        })
        .expect("sibling entity");
    drop(live);
    let unsubscribed = sibling
        .request(&botster_hub_client::DaemonRequest::UnsubscribeEntities {
            subscription_id: "late-entity-both".to_string(),
        })
        .expect("unsubscribe sibling entity");
    assert_ne!(
        unsubscribed.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let status = second
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("entity replacement still live");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
}

fn late_unsubscribe_events_does_not_drop_sibling(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut first =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("unsub first");
    first
        .subscribe_events(
            "sub-unsub-a",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("subscribe a");
    let mut second =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("unsub second");
    second
        .subscribe_events(
            "sub-unsub-b",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("subscribe b");
    first
        .unsubscribe_events("sub-unsub-a")
        .expect("unsubscribe a");
    drop(first);
    let status = second
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("sibling after unsubscribe");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    second
        .unsubscribe_events("sub-unsub-b")
        .expect("unsubscribe b");
}

fn late_webrtc_event_orders(endpoint: &botster_hub_client::DaemonEndpoint, data_dir: &Path) {
    late_webrtc_closed_first(endpoint, data_dir);
    late_webrtc_message_first(endpoint, data_dir);
}

fn webrtc_subscribe(
    endpoint: &botster_hub_client::DaemonEndpoint,
    data_dir: &Path,
    subscription_id: &str,
    subjects: Vec<String>,
) -> (
    botster_hub_client::DaemonLocalWebrtcBootstrap,
    LocalWebrtcOfferPeer,
    botster_core::AesGcmKey,
) {
    let package_dir = unique_test_dir(&format!("event-sat-web-{subscription_id}"));
    write_botster_web_package(&package_dir);
    enable_supervised_package(data_dir, &package_dir);
    let (_origin, bootstrap) = start_botster_web_and_issue_bootstrap(endpoint);
    let (peer, key) = block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(endpoint, &bootstrap).await;
        peer.enable_host_events();
        let ack = peer
            .encrypted_hello(&key, &webrtc_package_event_hello())
            .await
            .expect("package-event hello");
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS)
        );
        let subscribed = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEvents {
                    subscription_id: subscription_id.to_string(),
                    owner: "event-plane-producer".to_string(),
                    name: "sample.ready".to_string(),
                    subjects,
                },
            )
            .await
            .expect("subscribe webrtc");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        (peer, key)
    });
    (bootstrap, peer, key)
}

fn webrtc_status(peer: &mut LocalWebrtcOfferPeer, key: &botster_core::AesGcmKey) {
    let response = block_on(async {
        peer.encrypted_request(key, &botster_hub_client::DaemonRequest::Status)
            .await
    })
    .expect("webrtc holder status");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::Status
    );
}

fn late_webrtc_closed_first(endpoint: &botster_hub_client::DaemonEndpoint, data_dir: &Path) {
    let (_bootstrap, first, _first_key) =
        webrtc_subscribe(endpoint, data_dir, "sub-late-webrtc-closed", Vec::new());
    drop(first);
    let (_bootstrap, mut replacement, replacement_key) =
        webrtc_subscribe(endpoint, data_dir, "sub-late-webrtc-closed", Vec::new());
    webrtc_status(&mut replacement, &replacement_key);
    assert_quiet_fleet_survives(endpoint);
    drop(replacement);
}

fn late_webrtc_message_first(endpoint: &botster_hub_client::DaemonEndpoint, data_dir: &Path) {
    let (_a, mut live, live_key) =
        webrtc_subscribe(endpoint, data_dir, "sub-late-webrtc-both", Vec::new());
    let (_b, mut sibling, sibling_key) =
        webrtc_subscribe(endpoint, data_dir, "sub-late-webrtc-both", Vec::new());
    webrtc_status(&mut live, &live_key);
    webrtc_status(&mut sibling, &sibling_key);
    drop(live);
    webrtc_status(&mut sibling, &sibling_key);
    assert_quiet_fleet_survives(endpoint);
    drop(sibling);
}

fn late_admitted_holder_survives_reload(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut holder =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("holder");
    holder
        .subscribe_events(
            "sub-admitted-holder",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("holder subscribe");
    let reload = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "event-plane-producer".to_string(),
        },
    )
    .expect("reload while holder admitted");
    assert_ne!(
        reload.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let status = holder
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("holder after producer reload");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    holder
        .unsubscribe_events("sub-admitted-holder")
        .expect("unsubscribe holder");
}

fn run_fault_campaign() -> FaultReport {
    prove_shed_busy_non_blocking();
    let stall_path = PathBuf::from(format!(
        "/tmp/bh-event-sat-fault-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let hold_path = PathBuf::from(format!(
        "/tmp/bh-event-sat-hold-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let hub = start_campaign_hub(
        "faults",
        &[
            ("BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX", "1"),
            (
                "BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH",
                stall_path.to_str().expect("utf8"),
            ),
            (
                "BOTSTER_HUB_TEST_HOLD_JOURNAL_PULL",
                hold_path.to_str().expect("utf8 hold"),
            ),
            ("BOTSTER_HUB_TEST_DROP_JOURNAL_WAKES", "1"),
            ("BOTSTER_HUB_TEST_LIFECYCLE_JOURNAL_CAPACITY", "16"),
            ("BOTSTER_HUB_TEST_EVENT_INVOCATION_TIMEOUT_MS", "50"),
            ("BOTSTER_HUB_TEST_EVENT_HANDLER_HOLD_MS", "200"),
            ("BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION", "status"),
        ],
    );
    let endpoint = hub.endpoint().clone();
    enable_saturation_packages(&endpoint, hub.data_dir());
    spawn_quiet_fleet(&endpoint);
    let mut unix = subscribe_unix_events(&endpoint);
    let stop = Arc::new(AtomicBool::new(false));
    let emitter = spawn_event_emitter(&endpoint, stop.clone());
    fault_shed_full_or_over_rate(&endpoint);
    fault_plugin_mailbox_pressure(&endpoint);
    fault_client_mailbox_gap(&endpoint, &mut unix, &stall_path);
    fault_dropped_lifecycle_wake(&endpoint, &mut unix);
    fault_lifecycle_cursor_expiry(&endpoint, &hold_path);
    fault_handler_timeout(&endpoint);
    fault_plugin_worker_restart(&endpoint, &mut unix);
    unix = fault_unix_reconnect(&endpoint, unix);
    fault_webrtc_reconnect_unix_survives(&endpoint, hub.data_dir());
    assert_quiet_fleet_survives(&endpoint);
    run_late_event_holder_matrix(&endpoint);
    stop.store(true, Ordering::SeqCst);
    emitter.join().expect("join fault emitter");
    let observability = snapshot_observability(&endpoint);
    let lifecycle = snapshot_lifecycle(&endpoint);
    drop(unix);
    shutdown_owned_sessions(&endpoint);
    assert_no_live_sessions(&endpoint);
    hub.shutdown().expect("shutdown fault hub");
    FaultReport {
        observability,
        lifecycle,
    }
}

fn fault_shed_full_or_over_rate(endpoint: &botster_hub_client::DaemonEndpoint) {
    thread::sleep(Duration::from_millis(400));
    let snap = snapshot_observability(endpoint);
    let shed_full = snap.event_shed_by_reason.get("shed_full").copied().unwrap_or(0);
    let over_rate = snap
        .event_shed_by_reason
        .get("rejected_over_rate")
        .copied()
        .unwrap_or(0);
    assert!(
        snap.event_admission_attempts > 0 && shed_full + over_rate > 0,
        "full ingress must shed_full or rejected_over_rate: {snap:?}"
    );
    assert_queue_bounds(&snap, &PackageEventPlaneOptions::default());
}

fn fault_plugin_mailbox_pressure(endpoint: &botster_hub_client::DaemonEndpoint) {
    let snap = snapshot_observability(endpoint);
    let consumer_occupied = snap.queue_ages.iter().any(|age| {
        matches!(age.kind, botster_hub_client::DaemonQueueKind::Consumer)
            && age.queue_count.unwrap_or(0) > 0
    });
    assert!(
        consumer_occupied || snap.event_handler_backpressured > 0,
        "plugin mailbox lane must show consumer occupancy or handler backpressure: {snap:?}"
    );
}

fn fault_client_mailbox_gap(
    endpoint: &botster_hub_client::DaemonEndpoint,
    unix: &mut botster_hub_client::DaemonConnection,
    stall_path: &Path,
) {
    fs::write(stall_path, b"stall").expect("stall");
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::PluginMcpCallTool {
            name: "event_plane.emit_burst".to_string(),
            arguments: serde_json::json!({ "count": 25, "prefix": "gap" }),
        },
    );
    let _ = unix.request(&botster_hub_client::DaemonRequest::Status);
    let _ = fs::remove_file(stall_path);
    unix.set_read_timeout(Some(Duration::from_secs(3))).ok();
    let mut saw_gap = false;
    for _ in 0..8 {
        match unix.next_event() {
            Ok(botster_hub_client::DaemonEvent::EventGap { .. }) => {
                saw_gap = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    unix.set_read_timeout(None).ok();
    let snap = snapshot_observability(endpoint);
    assert!(
        saw_gap || snap.event_gaps > 0 || snap.event_mailbox_overflow_gaps > 0,
        "slow-consumer gap must appear: {snap:?}"
    );
    let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
        .expect("list after gap");
    assert!(
        listed
            .sessions
            .iter()
            .filter(|session| session.session_id.starts_with("quiet-")
                && session.lifecycle == "running")
            .count()
            >= EVENT_PLANE_FLEET_N,
        "after EventGap, quiet fleet must still be complete from baseline resync"
    );
}

fn fault_dropped_lifecycle_wake(
    endpoint: &botster_hub_client::DaemonEndpoint,
    unix: &mut botster_hub_client::DaemonConnection,
) {
    let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
        .expect("list with dropped journal wakes");
    assert_eq!(listed.kind, botster_hub_client::DaemonResponseKind::Sessions);
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "wake-probe".to_string(),
            command: "exec sleep 5".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn wake probe");
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "wake-probe".to_string(),
        },
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("poll wake probe");
        let gone = listed
            .sessions
            .iter()
            .find(|session| session.session_id == "wake-probe")
            .is_none_or(|session| session.lifecycle == "exited");
        if gone {
            break;
        }
        if Instant::now() >= deadline {
            panic!("dropped journal wakes must still converge ShutdownSession");
        }
        thread::sleep(Duration::from_millis(50));
    }
    let status = unix
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status with dropped journal wakes");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let counters = status.status.expect("status body").lifecycle_counters;
    assert!(
        counters.lifecycle_baseline_reads + counters.lifecycle_resync_reads > 0,
        "gap/wake faults must still drive baseline or resync reads: {counters:?}"
    );
}

fn snapshot_lifecycle(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> botster_hub_client::DaemonLifecycleCounters {
    let status = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status lifecycle snapshot");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    status.status.expect("status body").lifecycle_counters
}

fn fault_lifecycle_cursor_expiry(
    endpoint: &botster_hub_client::DaemonEndpoint,
    hold_path: &Path,
) {
    let before = snapshot_lifecycle(endpoint).lifecycle_resync_reads;
    fs::write(hold_path, b"hold").expect("hold journal pull");
    thread::sleep(Duration::from_millis(50));
    expect_kind(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "cursor-changed".to_string(),
            command: "exec sleep 7200".to_string(),
        },
        botster_hub_client::DaemonResponseKind::Spawned,
    )
    .expect("spawn cursor-changed during hold");
    for index in 0..20 {
        let session_id = format!("cursor-{index}");
        let _ = botster_hub_client::request(
            endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.clone(),
                command: "exec sleep 1".to_string(),
            },
        );
        let _ = botster_hub_client::request(
            endpoint,
            botster_hub_client::DaemonRequest::ShutdownSession { session_id },
        );
    }
    fs::remove_file(hold_path).expect("release journal pull");
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let listed =
            botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
                .expect("list after journal wrap");
        let after = snapshot_lifecycle(endpoint);
        let changed_running = listed.sessions.iter().any(|session| {
            session.session_id == "cursor-changed" && session.lifecycle == "running"
        });
        let quiet = listed
            .sessions
            .iter()
            .filter(|session| {
                session.session_id.starts_with("quiet-") && session.lifecycle == "running"
            })
            .count();
        if after.lifecycle_resync_reads > before && changed_running && quiet >= EVENT_PLANE_FLEET_N {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "cursor expiry must increment lifecycle_resync_reads (before={before}, after={}) and reconstruct cursor-changed spawned during the hold: {after:?} changed_running={changed_running} quiet={quiet}",
                after.lifecycle_resync_reads
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn fault_handler_timeout(endpoint: &botster_hub_client::DaemonEndpoint) {
    let snap = snapshot_observability(endpoint);
    assert!(
        snap.event_handler_timed_out > 0,
        "hold 200ms vs timeout 50ms must increment event_handler_timed_out: {snap:?}"
    );
}

fn fault_plugin_worker_restart(
    endpoint: &botster_hub_client::DaemonEndpoint,
    unix: &mut botster_hub_client::DaemonConnection,
) {
    let reload = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "event-plane-producer".to_string(),
        },
    )
    .expect("reload producer");
    assert_ne!(
        reload.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let status = unix
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after reload");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let token = unique_event_marker("reload");
    let subscription_id = "sub-sat-reload";
    let mut holder =
        subscribe_unix_events_filtered(endpoint, subscription_id, vec![token.clone()]);
    emit_unique_event(endpoint, &token);
    wait_unix_marker_or_gap(&mut holder, &token, subscription_id);
}

fn fault_unix_reconnect(
    endpoint: &botster_hub_client::DaemonEndpoint,
    unix: botster_hub_client::DaemonConnection,
) -> botster_hub_client::DaemonConnection {
    drop(unix);
    let mut unix = subscribe_unix_events(endpoint);
    let status = unix
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after unix reconnect");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    unix.set_read_timeout(Some(Duration::from_secs(3))).ok();
    let mut delivered = false;
    for _ in 0..8 {
        match unix.next_event() {
            Ok(botster_hub_client::DaemonEvent::PackageEvent { .. })
            | Ok(botster_hub_client::DaemonEvent::EventGap { .. }) => {
                delivered = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    unix.set_read_timeout(None).ok();
    assert!(
        delivered,
        "Unix reconnect must observe PackageEvent or EventGap"
    );
    unix
}

fn fault_webrtc_reconnect_unix_survives(
    endpoint: &botster_hub_client::DaemonEndpoint,
    data_dir: &Path,
) {
    let (_bootstrap, first, first_key) = subscribe_webrtc_events(endpoint, data_dir);
    let first_status = block_on(async {
        let mut peer = first;
        let response = peer
            .encrypted_request(&first_key, &botster_hub_client::DaemonRequest::Status)
            .await;
        drop(peer);
        response
    })
    .expect("initial subscribed WebRTC peer status");
    assert_eq!(
        first_status.kind,
        botster_hub_client::DaemonResponseKind::Status
    );
    let token = unique_event_marker("webrtc-re");
    let subscription_id = "sub-sat-webrtc-re";
    let (_bootstrap, mut replacement, replacement_key) =
        webrtc_subscribe(endpoint, data_dir, subscription_id, vec![token.clone()]);
    emit_unique_event(endpoint, &token);
    wait_webrtc_marker_or_gap(
        &mut replacement,
        &replacement_key,
        &token,
        subscription_id,
    );
    let unix_status =
        botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("unix status after webrtc reconnect");
    assert_eq!(
        unix_status.kind,
        botster_hub_client::DaemonResponseKind::Status
    );
    drop(replacement);
    assert_quiet_fleet_survives(endpoint);
}

fn assert_quiet_fleet_survives(endpoint: &botster_hub_client::DaemonEndpoint) {
    let listed = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions)
        .expect("list after faults");
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id.starts_with("quiet-")),
        "quiet fleet must survive non-fatal faults"
    );
}

fn fail_spawn_resource(error: &botster_hub_client::DaemonOperatorError) {
    let class = classify_pty_allocation_source(&error.message);
    if class != HostResourceClass::None {
        eprintln!(
            "{}",
            format_harness_budget_expired(
                "spawn",
                Duration::from_secs(1),
                class,
                probe_pty_allocation(),
                &error.message
            )
        );
    }
}
