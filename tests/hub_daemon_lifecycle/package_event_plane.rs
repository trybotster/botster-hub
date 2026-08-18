#[test]
fn isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree() {
    let _guard = daemon_test_guard();
    let producer_src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/event-plane-producer");
    let consumer_src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/event-plane-consumer");
    let producer_dir = unique_test_dir("event-plane-producer");
    let consumer_dir = unique_test_dir("event-plane-consumer");
    copy_dir_all(&producer_src, &producer_dir);
    copy_dir_all(&consumer_src, &consumer_dir);
    rewrite_package_source_path(&producer_dir);
    rewrite_package_source_path(&consumer_dir);

    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-event-plane"))
        .name("package-event-plane")
        .start()
        .expect("start isolated hub");

    let hub_bin = PathBuf::from(env!("CARGO_BIN_EXE_botster-hub"))
        .canonicalize()
        .expect("hub realpath");
    let worker_bin = session_worker_binary_path()
        .canonicalize()
        .expect("worker realpath");
    assert!(
        hub_bin.starts_with(Path::new(env!("CARGO_MANIFEST_DIR")).join("target")),
        "hub binary must live under this checkout: {}",
        hub_bin.display()
    );
    assert!(
        worker_bin.starts_with(Path::new(env!("CARGO_MANIFEST_DIR")).join("target")),
        "session worker must live under this checkout: {}",
        worker_bin.display()
    );
    let hub_sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_default();
    let lock_core_sha = lockfile_core_revision();
    assert_eq!(
        lock_core_sha, "d981bb03f91e2d13428000ac989c50d794f659b2",
        "live proof must use the pinned Core revision"
    );
    eprintln!(
        "event-plane live proof hub_sha={} core_sha={} hub_bin={} worker_bin={}",
        hub_sha.trim(),
        lock_core_sha,
        hub_bin.display(),
        worker_bin.display()
    );

    let enable_producer = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: producer_dir.clone(),
        },
    )
    .expect("enable producer");
    assert_eq!(
        enable_producer.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enable_consumer = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: consumer_dir.clone(),
        },
    )
    .expect("enable consumer");
    assert_eq!(
        enable_consumer.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );

    let emitted = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::PluginMcpCallTool {
            name: "event_plane.emit_ready".to_string(),
            arguments: serde_json::json!({ "token": "live" }),
        },
    )
    .expect("emit ready");
    assert_eq!(
        emitted.kind,
        botster_hub_client::DaemonResponseKind::PluginMcpToolResult
    );
    let status = emitted
        .plugin_tool_result
        .get("status")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert_eq!(status, "accepted");

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut last = None;
    while Instant::now() < deadline {
        let received = botster_hub_client::request(
            hub.endpoint(),
            botster_hub_client::DaemonRequest::PluginMcpCallTool {
                name: "event_plane.last_received".to_string(),
                arguments: serde_json::json!({}),
            },
        )
        .expect("read received");
        let count = received.plugin_tool_result["count"]
            .as_u64()
            .or_else(|| received.plugin_tool_result["result"]["count"].as_u64())
            .unwrap_or(0);
        if count >= 1 {
            last = Some(received);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let received = last.expect("consumer received the exact event");
    let last_event = received
        .plugin_tool_result
        .get("last")
        .cloned()
        .unwrap_or_else(|| received.plugin_tool_result["result"]["last"].clone());
    assert_eq!(last_event["ok"], true);
    assert_eq!(last_event["token"], "live");

    let target_root = unique_test_dir("event-plane-target");
    let worktree_path = target_root.join("plain");
    fs::create_dir_all(&worktree_path).expect("create worktree path");
    let target_root = fs::canonicalize(&target_root).expect("canonicalize target");
    let worktree_path = fs::canonicalize(&worktree_path).expect("canonicalize worktree");
    let created_target = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_event_plane".to_string()),
            label: Some("Event Plane".to_string()),
            root: target_root,
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create spawn target");
    assert_eq!(
        created_target.kind,
        botster_hub_client::DaemonResponseKind::SpawnTargets
    );
    let started = Instant::now();
    let created = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::CreateWorktree {
            target_id: "tgt_event_plane".to_string(),
            worktree_id: Some("wt_event_plane".to_string()),
            label: Some("event".to_string()),
            path: worktree_path,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create worktree");
    let elapsed = started.elapsed();
    if created.kind != botster_hub_client::DaemonResponseKind::Worktrees {
        panic!(
            "worktree create failed: kind={:?} error={:?} diagnostics={:?}",
            created.kind, created.error, created.diagnostics
        );
    }
    assert!(
        created
            .events
            .iter()
            .any(|event| matches!(event, botster_hub_client::DaemonEvent::WorktreeLifecycle { .. })),
        "mutating response still carries WorktreeLifecycle"
    );
    eprintln!("worktree create duration observation (package event plane): {elapsed:?}");

    let _ = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "event-plane-producer".to_string(),
        },
    );
    let _ = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "event-plane-consumer".to_string(),
        },
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn isolated_hub_event_to_entity_provider_emit_stays_rejected_causal_scope() {
    let _guard = daemon_test_guard();
    let producer_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
    let cycle_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-cycle");
    let producer_dir = unique_test_dir("event-plane-producer-cycle");
    let cycle_dir = unique_test_dir("event-plane-cycle");
    copy_dir_all(&producer_src, &producer_dir);
    copy_dir_all(&cycle_src, &cycle_dir);
    rewrite_package_source_path(&producer_dir);
    rewrite_package_source_path(&cycle_dir);

    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-event-plane-cycle"))
        .name("package-event-cycle")
        .start()
        .expect("start isolated hub");

    for path in [producer_dir, cycle_dir] {
        let enabled = botster_hub_client::request(
            hub.endpoint(),
            botster_hub_client::DaemonRequest::EnablePackageLocalPath { path },
        )
        .expect("enable package");
        assert_eq!(
            enabled.kind,
            botster_hub_client::DaemonResponseKind::PackageDecision
        );
    }

    let emitted = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::PluginMcpCallTool {
            name: "event_plane.emit_ready".to_string(),
            arguments: serde_json::json!({ "token": "cycle" }),
        },
    )
    .expect("emit ready");
    assert_eq!(emitted.plugin_tool_result["status"], "accepted");

    let deadline = Instant::now() + Duration::from_secs(4);
    let mut last = None;
    while Instant::now() < deadline {
        let status = botster_hub_client::request(
            hub.endpoint(),
            botster_hub_client::DaemonRequest::PluginMcpCallTool {
                name: "event_plane.cycle_status".to_string(),
                arguments: serde_json::json!({}),
            },
        )
        .expect("cycle status");
        let handler = status.plugin_tool_result["handler_status"]
            .as_str()
            .unwrap_or("none");
        let provider = status.plugin_tool_result["provider_status"]
            .as_str()
            .unwrap_or("none");
        if handler == "rejected_causal_scope" && provider == "rejected_causal_scope" {
            last = Some(status);
            break;
        }
        thread::sleep(Duration::from_millis(40));
    }
    let status = last.expect("event handler and later provider emit stayed rejected_causal_scope");
    assert_eq!(
        status.plugin_tool_result["handler_status"],
        "rejected_causal_scope"
    );
    assert_eq!(
        status.plugin_tool_result["provider_status"],
        "rejected_causal_scope"
    );
    hub.shutdown().expect("shutdown isolated hub");
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create dest");
    for entry in fs::read_dir(from).expect("read src") {
        let entry = entry.expect("entry");
        let dest = to.join(entry.file_name());
        if entry.file_type().expect("ty").is_dir() {
            copy_dir_all(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).expect("copy file");
        }
    }
}

fn rewrite_package_source_path(package_dir: &Path) {
    let manifest_path = package_dir.join("botster-package.json");
    let mut value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    value["source"]["path"] = serde_json::json!(package_dir.display().to_string());
    fs::write(
        manifest_path,
        serde_json::to_string_pretty(&value).expect("serialize"),
    )
    .expect("write manifest");
}

fn lockfile_core_revision() -> String {
    let lock = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"))
        .expect("read Cargo.lock");
    let mut saw_core = false;
    for line in lock.lines() {
        if line == "name = \"botster-core\"" {
            saw_core = true;
            continue;
        }
        if saw_core
            && let Some(source) = line.strip_prefix("source = \"")
            && let Some(rev) = source.split('#').nth(1)
        {
            return rev.trim_end_matches('"').to_string();
        }
        if saw_core && line.starts_with('[') {
            saw_core = false;
        }
    }
    String::new()
}
