// Event-plane saturation campaign and its always-on architectural gates.
//
// The ignored campaign is selected by `script/run-loaded-daemon-lifecycle`
// `--test-target event-plane-saturation`. Default lifecycle-suite runs only
// the source-guard and isolated client-event tests below.

use botster_hub::{MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS, PackageEventPlaneOptions};
use botster_hub_test_support::{
    copy_plugin_contract_matrix_fixture, run_client_event_conformance,
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
    assert_eq!(EVENT_PLANE_RATIO_R, 1.25);
    assert_eq!(EVENT_PLANE_SLACK_MS, 8.0);
    assert_eq!(EVENT_PLANE_THROUGHPUT_T, 0.80);

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
    eprintln!(
        "event-plane saturation host probe fd={:?} pty={:?}",
        fd.marker_name(),
        pty.marker_name()
    );

    let enabled = run_saturation_arm(SaturationArm::PlaneEnabled);
    let decoupled = run_saturation_arm(SaturationArm::PlaneDecoupled);
    assert_eq!(enabled.profile.n, EVENT_PLANE_FLEET_N);
    assert_eq!(decoupled.profile.n, EVENT_PLANE_FLEET_N);
    assert_eq!(enabled.profile.window_secs, 600);
    assert_eq!(decoupled.profile.window_secs, 600);
    for operation in EVENT_PLANE_OPERATIONS {
        let enabled_op = enabled
            .operations
            .get(operation)
            .unwrap_or_else(|| panic!("missing enabled samples for {operation}"));
        let decoupled_op = decoupled
            .operations
            .get(operation)
            .unwrap_or_else(|| panic!("missing decoupled samples for {operation}"));
        assert_eq!(
            enabled_op.failures, 0,
            "{operation} failed in the enabled arm"
        );
        assert_eq!(
            decoupled_op.failures, 0,
            "{operation} failed in the decoupled arm"
        );
        assert!(
            enabled_op.successes >= EVENT_PLANE_MIN_SAMPLES,
            "{operation} enabled samples {} below {}",
            enabled_op.successes,
            EVENT_PLANE_MIN_SAMPLES
        );
        assert!(
            decoupled_op.successes >= EVENT_PLANE_MIN_SAMPLES,
            "{operation} decoupled samples {} below {}",
            decoupled_op.successes,
            EVENT_PLANE_MIN_SAMPLES
        );
    }
}

#[derive(Clone, Copy)]
enum SaturationArm {
    PlaneEnabled,
    PlaneDecoupled,
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

struct ArmReport {
    profile: ArmProfile,
    operations: BTreeMap<String, OperationStats>,
}

fn run_saturation_arm(arm: SaturationArm) -> ArmReport {
    let label = match arm {
        SaturationArm::PlaneEnabled => "enabled",
        SaturationArm::PlaneDecoupled => "decoupled",
    };
    let stall_path = PathBuf::from(format!(
        "/tmp/bh-event-sat-stall-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let mut builder = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from(format!("/tmp/bh-event-sat-{label}")))
        .name(format!("event-sat-{label}"));
    if matches!(arm, SaturationArm::PlaneEnabled) {
        builder = builder
            .env("BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX", "1")
            .env(
                "BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH",
                stall_path.to_str().expect("utf8 stall path"),
            );
    }
    let hub = match builder.start() {
        Ok(hub) => hub,
        Err(error) => {
            let io_kind = io::Error::other(error.to_string());
            let class = classify_os_resource(&io_kind);
            eprintln!(
                "{}",
                format_harness_budget_expired(
                    "start",
                    Duration::from_secs(5),
                    class,
                    probe_fd_limit(),
                    &format!("isolated hub start failed: {io_kind}")
                )
            );
            panic!("isolated hub start failed: {io_kind}");
        }
    };
    let endpoint = hub.endpoint().clone();

    if matches!(arm, SaturationArm::PlaneEnabled) {
        enable_saturation_packages(&endpoint, hub.data_dir());
        let producer = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
        let report = run_client_event_conformance(&hub, &producer, Some(stall_path.as_path()))
            .expect("generic client event conformance under the enabled arm");
        assert!(report.event_received);
        subscribe_unix_events(&endpoint);
        subscribe_webrtc_events(&endpoint, hub.data_dir());
        spawn_event_emitter(&endpoint);
    }

    spawn_quiet_fleet(&endpoint);
    spawn_noisy_session(&endpoint);
    let operations = run_measurement_workers(&endpoint);
    if matches!(arm, SaturationArm::PlaneEnabled) {
        run_fault_lanes(&endpoint);
        run_late_event_holder_matrix(&endpoint);
    }
    hub.shutdown().expect("shutdown saturation hub");
    let _ = fs::remove_file(&stall_path);
    ArmReport {
        profile: ArmProfile {
            n: EVENT_PLANE_FLEET_N,
            window_secs: EVENT_PLANE_MEASUREMENT_WINDOW.as_secs(),
        },
        operations,
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

fn subscribe_webrtc_events(endpoint: &botster_hub_client::DaemonEndpoint, data_dir: &Path) {
    let package_dir = unique_test_dir("event-sat-web");
    write_botster_web_package(&package_dir);
    enable_supervised_package(data_dir, &package_dir);
    let (_origin, bootstrap) = start_botster_web_and_issue_bootstrap(endpoint);
    block_on(async {
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
        std::mem::forget(peer);
    });
}

fn subscribe_unix_events(endpoint: &botster_hub_client::DaemonEndpoint) {
    let mut connection =
        botster_hub_client::connect_for_package_event_subscriptions(endpoint).expect("unix events");
    let subscribed = connection
        .subscribe_events(
            "sub-sat-unix",
            "event-plane-producer",
            "sample.ready",
            Vec::new(),
        )
        .expect("subscribe unix");
    assert_eq!(
        subscribed.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    std::mem::forget(connection);
}

fn spawn_event_emitter(endpoint: &botster_hub_client::DaemonEndpoint) {
    let endpoint = endpoint.clone();
    thread::spawn(move || {
        let interval = Duration::from_millis(1000 / EVENT_PLANE_BURSTS_PER_SEC);
        loop {
            let started = Instant::now();
            let _ = botster_hub_client::request(
                &endpoint,
                botster_hub_client::DaemonRequest::PluginMcpCallTool {
                    name: "event_plane.emit_burst".to_string(),
                    arguments: serde_json::json!({
                        "count": EVENT_PLANE_BURST_COUNT,
                        "prefix": format!("sat-{}", started.elapsed().as_millis())
                    }),
                },
            );
            let elapsed = started.elapsed();
            if elapsed < interval {
                thread::sleep(interval - elapsed);
            }
        }
    });
}

fn spawn_quiet_fleet(endpoint: &botster_hub_client::DaemonEndpoint) {
    for wave in 0..(EVENT_PLANE_FLEET_N / EVENT_PLANE_SPAWN_WAVE) {
        for index in 0..EVENT_PLANE_SPAWN_WAVE {
            let session_id = format!("quiet-{}-{}", wave, index);
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
            .filter(|session| session.session_id.starts_with("quiet-") && session.lifecycle == "running")
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

fn spawn_noisy_session(endpoint: &botster_hub_client::DaemonEndpoint) {
    let spawned = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            command: "while true; do printf '%.4096d\\n' 0; sleep 0.1; done".to_string(),
        },
    )
    .expect("spawn noisy");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let attached = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::Attach {
            session_id: EVENT_PLANE_NOISY_SESSION.to_string(),
            subscription_id: EVENT_PLANE_NOISY_SUB.to_string(),
        },
    )
    .expect("attach noisy");
    assert_eq!(
        attached.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
}

fn run_measurement_workers(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> BTreeMap<String, OperationStats> {
    let start_at = Instant::now() + EVENT_PLANE_WARMUP;
    let end_at = start_at + EVENT_PLANE_MEASUREMENT_WINDOW;
    let (tx, rx) = mpsc::channel();
    for worker in 0..EVENT_PLANE_DRIVER_CONCURRENCY {
        let tx = tx.clone();
        let endpoint = endpoint.clone();
        thread::spawn(move || {
            let mut cycle: u64 = 0;
            while Instant::now() < end_at {
                cycle += 1;
                let session_id = format!("churn-{worker}-{cycle}");
                let sub_id = format!("churn-sub-{worker}-{cycle}");
                for operation in EVENT_PLANE_OPERATIONS {
                    let op_start = Instant::now();
                    let in_window = op_start >= start_at && Instant::now() < end_at;
                    let result = perform_cycle_operation(&endpoint, operation, &session_id, &sub_id);
                    let elapsed = op_start.elapsed();
                    let finished_in_window = Instant::now() <= end_at;
                    if in_window && finished_in_window {
                        tx.send((
                            operation.to_string(),
                            result.is_ok(),
                            elapsed.as_millis() as u64,
                        ))
                        .expect("send sample");
                    }
                    if let Err(error) = result {
                        panic!("measurement {operation} failed: {error}");
                    }
                }
            }
        });
    }
    drop(tx);
    let mut operations = BTreeMap::new();
    for name in EVENT_PLANE_OPERATIONS {
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
    operations
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
                data: "x\r".to_string(),
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
    let response = botster_hub_client::request(endpoint, request).map_err(|error| error.to_string())?;
    if response.kind != kind {
        return Err(format!("expected {kind:?}, got {:?}", response.kind));
    }
    Ok(())
}

fn run_fault_lanes(endpoint: &botster_hub_client::DaemonEndpoint) {
    let status = botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status during faults");
    assert_eq!(
        status.kind,
        botster_hub_client::DaemonResponseKind::Status
    );
    let observability = &status
        .status
        .as_ref()
        .expect("status body")
        .observability;
    assert!(
        observability.event_admission_attempts > 0
            || observability.event_shed_by_reason.values().any(|count| *count > 0),
        "enabled arm must observe admission or shed: {observability:?}"
    );
    let listed = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ListSessions,
    )
    .expect("list after faults");
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == EVENT_PLANE_NOISY_SESSION),
        "noisy session must survive non-fatal faults"
    );
}

fn run_late_event_holder_matrix(endpoint: &botster_hub_client::DaemonEndpoint) {
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
        .expect("reuse subscription id on a new connection");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    let unsubscribed = second
        .unsubscribe_events("sub-late-reuse")
        .expect("unsubscribe late second");
    assert_eq!(
        unsubscribed.kind,
        botster_hub_client::DaemonResponseKind::EventUnsubscribed
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
