#[test]
fn generated_daemon_protocol_mirrors_core_aes_gcm_envelope_fields() {
    let envelope = AesGcmEnvelope {
        nonce: "base64-nonce".to_string(),
        ciphertext: "base64-ciphertext".to_string(),
        version: 1,
    };
    let value = serde_json::to_value(envelope).expect("core AES-GCM envelope serializes");
    let fields = value
        .as_object()
        .expect("core AES-GCM envelope serializes as object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(fields, vec!["ciphertext", "nonce", "version"]);

    let artifact = fs::read_to_string("crates/botster-hub-client/generated/daemon-protocol.ts")
        .expect("generated daemon protocol artifact is readable");
    let interface = generated_typescript_interface(&artifact, "AesGcmEnvelope");
    assert!(interface.contains("  nonce: string;"));
    assert!(interface.contains("  ciphertext: string;"));
    assert!(interface.contains("  version: number;"));
}

#[test]
fn daemon_test_guard_recovers_poison_without_losing_mutual_exclusion() {
    static PROBE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let lock = PROBE_LOCK.get_or_init(|| Mutex::new(()));
    let poisoner = thread::spawn(move || {
        let _guard = recovering_mutex_guard(lock);
        panic!("poison daemon test lock intentionally");
    });
    assert!(poisoner.join().is_err());

    let guard = recovering_mutex_guard(lock);
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _guard = recovering_mutex_guard(lock);
        acquired_tx.send(()).expect("report lock acquisition");
    });

    assert!(
        acquired_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    drop(guard);
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiting thread acquires recovered lock after guard drops");
    waiter.join().expect("lock waiter exits cleanly");
}

#[test]
fn real_daemon_status_and_hub_update_use_managed_receipt_authority() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("managed-hub-maintenance");
    let home = unique_test_dir("managed-hub-maintenance-home");
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create receipt parent");
    let (source_url, release_fixture) = spawn_release_metadata_fixture(
        serde_json::json!({
            "schema_version": 2,
            "product_id": "botster-hub",
            "release_channel": "stable",
            "version": "99.0.0",
            "build_revision": "release99"
        }),
        2,
    );
    fs::write(
        &receipt,
        serde_json::to_vec(&managed_receipt(&source_url)).expect("serialize receipt"),
    )
    .expect("write receipt");

    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("read authoritative daemon status")
        .status
        .expect("status payload");
    assert_eq!(status.software.product_id, "botster-hub");
    assert_eq!(status.software.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        status.installation.mode,
        botster_hub_client::DaemonInstallationMode::Managed
    );
    assert_eq!(
        status.installation.release_channel.as_deref(),
        Some("stable")
    );
    let serialized = serde_json::to_string(&status).expect("serialize status");
    assert!(!serialized.contains(&home.display().to_string()));
    // Schema 2 added checksum, signature, provenance, and installer facts to the
    // receipt, so the leak assertion covers every one of them: none may reach a
    // client through the status DTO.
    for private in [
        "source_url",
        "signed_manifest_sha256",
        "sha256",
        "key_id",
        "installer",
        "source_revisions",
    ] {
        assert!(!serialized.contains(private), "status leaked {private}");
    }

    let update =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::CheckHubUpdate)
            .expect("check managed Hub update")
            .hub_update
            .expect("Hub update payload");
    assert_eq!(
        update.state,
        botster_hub_client::DaemonHubUpdateState::Available
    );
    assert_eq!(update.available_version.as_deref(), Some("99.0.0"));
    assert_eq!(update.action.as_deref(), Some("run_managed_installer"));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("check-update")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run Hub check-update CLI");
    assert!(cli.status.success(), "{}", command_output_text(&cli));
    assert!(String::from_utf8_lossy(&cli.stdout).contains("state=available"));
    assert!(String::from_utf8_lossy(&cli.stdout).contains("action=run_managed_installer"));

    release_fixture.join().expect("release fixture exits");
    shutdown_cli_daemon(&data_dir, child);

    let restarted_child = start_cli_daemon_with_home(&data_dir, &home);
    let restarted_status =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("read authoritative status after restart")
            .status
            .expect("restarted status payload");
    assert_eq!(restarted_status.software, status.software);
    assert_eq!(restarted_status.installation, status.installation);
    shutdown_cli_daemon(&data_dir, restarted_child);

    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn real_daemon_missing_receipt_reports_development_manual_update() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("development-hub-maintenance");
    let home = unique_test_dir("development-hub-maintenance-home");
    fs::create_dir_all(&home).expect("create empty home");
    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("read development daemon status")
        .status
        .expect("status payload");
    assert_eq!(
        status.installation.mode,
        botster_hub_client::DaemonInstallationMode::Development
    );
    assert_eq!(status.installation.provenance, "development_build");

    let update =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::CheckHubUpdate)
            .expect("check development Hub update")
            .hub_update
            .expect("Hub update payload");
    assert_eq!(
        update.state,
        botster_hub_client::DaemonHubUpdateState::Unavailable
    );
    assert_eq!(update.reason.as_deref(), Some("development_checkout"));
    assert_eq!(update.action.as_deref(), Some("manual"));

    shutdown_cli_daemon(&data_dir, child);
    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn real_daemon_invalid_receipt_reports_diagnostic_and_manual_update() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("invalid-hub-maintenance");
    let home = unique_test_dir("invalid-hub-maintenance-home");
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create receipt parent");
    fs::write(&receipt, b"{").expect("write malformed receipt");
    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("read invalid-receipt daemon status")
        .status
        .expect("status payload");
    assert_eq!(status.installation.diagnostics.len(), 1);
    assert_eq!(status.installation.diagnostics[0].kind, "malformed_receipt");

    let update =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::CheckHubUpdate)
            .expect("check invalid-receipt Hub update")
            .hub_update
            .expect("Hub update payload");
    assert_eq!(
        update.state,
        botster_hub_client::DaemonHubUpdateState::Unavailable
    );
    assert_eq!(
        update.reason.as_deref(),
        Some("invalid_installation_receipt")
    );
    assert_eq!(update.action.as_deref(), Some("manual"));

    shutdown_cli_daemon(&data_dir, child);
    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn real_provider_reports_current_newer_source_behind_and_unavailable_states() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("managed-hub-release-states");
    let home = unique_test_dir("managed-hub-release-states-home");
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create receipt parent");
    let release = |version: &str| {
        serde_json::json!({
            "schema_version": 2,
            "product_id": "botster-hub",
            "release_channel": "stable",
            "version": version
        })
    };
    let (source_url, release_fixture) = spawn_release_metadata_sequence_fixture(vec![
        release(env!("CARGO_PKG_VERSION")),
        release("99.0.0"),
        release("0.0.1"),
        serde_json::json!({
            "schema_version": 2,
            "product_id": "other-product",
            "release_channel": "stable",
            "version": env!("CARGO_PKG_VERSION")
        }),
    ]);
    fs::write(
        &receipt,
        serde_json::to_vec(&managed_receipt(&source_url)).expect("serialize receipt"),
    )
    .expect("write receipt");

    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    for (state, reason, action) in [
        (
            botster_hub_client::DaemonHubUpdateState::Current,
            "up_to_date",
            None,
        ),
        (
            botster_hub_client::DaemonHubUpdateState::Available,
            "newer_release_available",
            Some("run_managed_installer"),
        ),
        (
            botster_hub_client::DaemonHubUpdateState::Current,
            "source_behind",
            Some("no_downgrade"),
        ),
        (
            botster_hub_client::DaemonHubUpdateState::Unavailable,
            "invalid_release_metadata",
            Some("contact_provider"),
        ),
    ] {
        let update = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::CheckHubUpdate,
        )
        .expect("check managed Hub release state")
        .hub_update
        .expect("Hub update payload");
        assert_eq!(update.state, state);
        assert_eq!(update.reason.as_deref(), Some(reason));
        assert_eq!(update.action.as_deref(), action);
    }

    release_fixture.join().expect("release fixture exits");
    shutdown_cli_daemon(&data_dir, child);
    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn stalled_hub_update_check_keeps_status_responsive_and_second_check_is_busy() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("stalled-hub-maintenance");
    let home = unique_test_dir("stalled-hub-maintenance-home");
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create receipt parent");
    let (source_url, accepted_rx, release_tx, release_fixture) =
        spawn_stalled_release_metadata_fixture(serde_json::json!({
            "schema_version": 2,
            "product_id": "botster-hub",
            "release_channel": "stable",
            "version": env!("CARGO_PKG_VERSION")
        }));
    fs::write(
        &receipt,
        serde_json::to_vec(&managed_receipt(&source_url)).expect("serialize receipt"),
    )
    .expect("write receipt");
    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let first_endpoint = endpoint.clone();
    let first = thread::spawn(move || {
        botster_hub_client::request(
            &first_endpoint,
            botster_hub_client::DaemonRequest::CheckHubUpdate,
        )
        .expect("first stalled update response")
    });
    accepted_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("provider observes first update check");

    let started = Instant::now();
    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status stays responsive during provider stall");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(status.status.is_some());

    let busy =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::CheckHubUpdate)
            .expect("second update check returns typed busy")
            .hub_update
            .expect("busy update payload");
    assert_eq!(
        busy.state,
        botster_hub_client::DaemonHubUpdateState::Unavailable
    );
    assert_eq!(busy.reason.as_deref(), Some("busy"));
    assert_eq!(busy.action.as_deref(), Some("retry"));

    release_tx.send(()).expect("release provider response");
    let first = first.join().expect("first update thread exits");
    assert_eq!(
        first.hub_update.expect("first update payload").state,
        botster_hub_client::DaemonHubUpdateState::Current
    );
    release_fixture
        .join()
        .expect("stalled release fixture exits");
    shutdown_cli_daemon(&data_dir, child);
    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn daemon_shutdown_during_hub_update_check_is_bounded_and_leak_free() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("shutdown-hub-maintenance");
    let home = unique_test_dir("shutdown-hub-maintenance-home");
    let receipt = home.join(".botster/installations/botster-hub.json");
    fs::create_dir_all(receipt.parent().expect("receipt parent")).expect("create receipt parent");
    let (source_url, accepted_rx, release_fixture) = spawn_timeout_release_metadata_fixture();
    fs::write(
        &receipt,
        serde_json::to_vec(&managed_receipt(&source_url)).expect("serialize receipt"),
    )
    .expect("write receipt");
    let child = start_cli_daemon_with_home(&data_dir, &home);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let update_endpoint = endpoint.clone();
    let update = thread::spawn(move || {
        botster_hub_client::request(
            &update_endpoint,
            botster_hub_client::DaemonRequest::CheckHubUpdate,
        )
    });
    accepted_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("provider observes in-flight update check");

    let started = Instant::now();
    let shutdown = request_cli_daemon_shutdown(&data_dir).expect("request daemon shutdown");
    let daemon = wait_for_cli_daemon_shutdown(&shutdown, child.disarm());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown exceeded bounded provider timeout: {:?}",
        started.elapsed()
    );
    assert!(daemon.status.success());
    let update = update
        .join()
        .expect("update request thread exits")
        .expect("in-flight caller receives typed shutdown outcome")
        .hub_update
        .expect("shutdown update payload");
    assert_eq!(
        update.state,
        botster_hub_client::DaemonHubUpdateState::Unavailable
    );
    assert_eq!(update.reason.as_deref(), Some("daemon_shutdown"));
    assert_eq!(update.action.as_deref(), Some("retry"));
    release_fixture
        .join()
        .expect("timeout release fixture exits");
    assert!(!data_dir.join("botster-hub.sock").exists());
    fs::remove_dir_all(&data_dir).expect("remove maintenance data dir");
    fs::remove_dir_all(&home).expect("remove maintenance home");
}

#[test]
fn process_ownership_wait_for_status_timeout_reports_diagnostics_and_reaps_owned_child() {
    let data_dir = unique_test_dir("wait-for-status-timeout");
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(
            "printf 'daemon stdout marker\\n'; printf 'daemon stderr marker\\n' >&2; exec sleep 60",
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn never-ready daemon fixture");

    let error = wait_for_status_with_budget(&data_dir, &mut child, Duration::from_millis(100))
        .expect_err("never-ready child should time out");

    assert!(error.contains("readiness budget 100ms"), "{error}");
    assert!(error.contains("last status output="), "{error}");
    assert!(error.contains("daemon stdout marker"), "{error}");
    assert!(error.contains("daemon stderr marker"), "{error}");
    assert!(error.contains("child_status="), "{error}");
    assert!(
        child
            .try_wait()
            .expect("confirm child was reaped")
            .is_some(),
        "owned child should be reaped after readiness timeout"
    );

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe after readiness timeout");
    assert!(
        !status.status.success(),
        "timed-out fixture must not answer status: {}",
        command_output_text(&status)
    );
}

#[test]
fn wait_for_child_condition_rechecks_after_exit_drain() {
    struct ExitDrainChild {
        drained: Arc<AtomicBool>,
    }

    impl TestChildControl for ExitDrainChild {
        fn try_wait_status(&mut self) -> io::Result<Option<String>> {
            Ok(Some("exit 0".to_string()))
        }

        fn terminate_and_reap(&mut self) -> String {
            "exit 0".to_string()
        }

        fn captured_output(&mut self) -> String {
            self.drained.store(true, Ordering::Release);
            "stdout=\"final output\" stderr=\"\"".to_string()
        }
    }

    let drained = Arc::new(AtomicBool::new(false));
    let mut child = ExitDrainChild {
        drained: Arc::clone(&drained),
    };
    wait_for_child_condition_with_budget(
        &mut child,
        "waiting for final drained output",
        Duration::from_secs(1),
        || drained.load(Ordering::Acquire),
    )
    .expect("condition should be rechecked after the exited child's output drain");
}

#[test]
fn cli_daemon_shutdown_rejects_exact_disconnect_after_clean_exit() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: client disconnected\\n' >&2; exit 1");
    let daemon = shell_output("exit 0");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("shutdown disconnect must remain visible after a clean daemon exit");

    assert!(error.contains("shutdown failed"));
    assert!(error.contains("client disconnected"));
}

#[test]
fn cli_daemon_shutdown_rejects_unrelated_command_error_after_clean_exit() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: permission denied\\n' >&2; exit 1");
    let daemon = shell_output("exit 0");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("unrelated shutdown error must be rejected");

    assert!(error.contains("shutdown failed"));
    assert!(error.contains("permission denied"));
}

#[test]
fn cli_daemon_shutdown_rejects_unclean_exit_with_disconnect_diagnostics() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: client disconnected\\n' >&2; exit 1");
    let daemon = shell_output("printf 'daemon crash\\n' >&2; exit 42");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("unclean daemon exit must be rejected");

    assert!(error.contains("daemon failed"));
    assert!(error.contains("daemon crash"));
    assert!(error.contains("client disconnected"));
}

#[test]
fn buffered_child_stdout_wait_observes_backpressure_condition() {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("exec yes buffered-output")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn buffered stdout fixture");

    let observation = wait_for_buffered_child_stdout(
        &mut child,
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        STALLED_ATTACH_STABLE_SAMPLES,
        Duration::from_secs(5),
    )
    .expect("observe child stdout backpressure");

    terminate_and_reap_child(&mut child);
    let _ = collect_child_output(&mut child);
    assert!(
        observation.available_bytes >= STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        "stdout backpressure should retain at least {} bytes, got {} after {:?}; recent_samples={:?}",
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        observation.available_bytes,
        observation.elapsed,
        observation.recent_samples,
    );
}

#[test]
fn cli_doctor_reports_stopped_runtime_with_remediation() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-stopped");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against stopped runtime");
    assert!(
        !output.status.success(),
        "doctor should fail for stopped runtime: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("doctor=local_runtime"));
    assert!(text.contains("check name=daemon_running status=fail"));
    assert!(text.contains(&format!(
        "remediation=botster-hub up --data-dir {}",
        data_dir.display()
    )));
}

#[test]
fn cli_local_runtime_up_starts_reuses_and_down_stops_runtime() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-local-runtime-up");
    let project_pipelines_package_dir = unique_test_dir("cli-up-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-web");
    let tui_package_dir = unique_test_dir("cli-up-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let web_listener_port = 0;
    let first = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        first.status.success(),
        "first up failed: {}",
        command_output_text(&first)
    );
    let first_text = command_output_text(&first);
    assert!(first_text.contains("runtime=ready"));
    assert!(first_text.contains("daemon=started"));
    assert!(first_text.contains("protocol=botster-hub-daemon-v1"));
    assert!(first_text.contains(&format!(
        "protocol_version={}",
        botster_hub_client::PROTOCOL_VERSION
    )));
    assert!(first_text.contains("conformance_fixture_revision="));
    assert!(first_text.contains("package_count=2"));
    assert!(first_text.contains("enabled_package_count=2"));
    assert!(first_text.contains("app_count="));
    assert!(first_text.contains("app package=botster-web app_id=web-client"));
    assert!(first_text.contains("web=http://127.0.0.1:"));
    assert!(!first_text.contains('?'));
    assert!(first_text.contains(&format!(
        "down=botster-hub down --data-dir {}",
        data_dir.display()
    )));
    for package_dir in [
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
    ] {
        assert!(
            !first_text.contains(package_dir.to_string_lossy().as_ref()),
            "up output should not leak package source path {package_dir:?}: {first_text}"
        );
    }

    let unchanged = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        unchanged.status.success(),
        "unchanged up failed: {}",
        command_output_text(&unchanged)
    );
    let unchanged_text = command_output_text(&unchanged);
    assert!(unchanged_text.contains("daemon=reused"));
    let first_web_url = first_text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("first up output includes Web URL");
    let unchanged_web_url = unchanged_text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("unchanged up output includes Web URL");
    assert_eq!(
        unchanged_web_url, first_web_url,
        "unchanged up should preserve the running Web entrypoint and structured URL"
    );

    rewrite_botster_web_entrypoint(
        &web_package_dir,
        "1.1.0",
        "local-package-server.mjs",
        "reused-up.marker",
    );
    let second = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        second.status.success(),
        "second up failed: {}",
        command_output_text(&second)
    );
    assert!(command_output_text(&second).contains("daemon=reused"));
    assert!(
        web_package_dir.join("reused-up.marker").is_file(),
        "reused-daemon up should launch the refreshed package entrypoint"
    );
    let config = explicit_config(&data_dir);
    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after reused up refresh");
    assert_eq!(
        packages
            .packages
            .iter()
            .find(|package| package.package_name == "botster-web")
            .expect("botster-web package after reused up")
            .version,
        "1.1.0"
    );

    let live_idle_connection =
        botster_hub_client::DaemonConnection::connect(&botster_hub_client::DaemonEndpoint::new(
            config
                .transports
                .local_socket
                .as_ref()
                .expect("local runtime socket binding")
                .path
                .clone(),
        ))
        .expect("hold idle connection across down");
    let mut live_entity_subscription = botster_hub_client::subscribe_session_entities(
        &botster_hub_client::DaemonEndpoint::new(
            config
                .transports
                .local_socket
                .as_ref()
                .expect("local runtime socket binding")
                .path
                .clone(),
        ),
        "down-live-entity",
    )
    .expect("hold entity subscription across down");
    assert!(matches!(
        live_entity_subscription
            .next_frame()
            .expect("live entity initial snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));
    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down");
    assert!(
        down.status.success(),
        "down failed: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("response=shutdown"));
    drop(live_entity_subscription);
    drop(live_idle_connection);

    rewrite_botster_web_entrypoint(
        &web_package_dir,
        "1.2.0",
        "startup-local-package-server.mjs",
        "startup-up.marker",
    );
    let restarted = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        0,
    );
    assert!(
        restarted.status.success(),
        "immediate up after down failed: {}",
        command_output_text(&restarted)
    );
    let restarted_text = command_output_text(&restarted);
    assert!(restarted_text.contains("runtime=ready"));
    assert!(restarted_text.contains("daemon=started"));
    assert!(
        web_package_dir.join("startup-up.marker").is_file(),
        "fresh-daemon up should launch the refreshed package entrypoint"
    );
    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after startup up refresh");
    assert_eq!(
        packages
            .packages
            .iter()
            .find(|package| package.package_name == "botster-web")
            .expect("botster-web package after startup up")
            .version,
        "1.2.0"
    );

    shutdown_local_runtime_daemon(&data_dir);

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after daemon shutdown");
    assert!(
        !status.status.success(),
        "status should fail after daemon shutdown: {}",
        command_output_text(&status)
    );
}

#[test]
fn cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("cli-shutdown-owned-runtime");
    let web_package_dir = unique_test_dir("cli-shutdown-owned-web");
    let tui_package_dir = unique_test_dir("cli-shutdown-owned-tui");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let up = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("start metadata-owned runtime daemon");
    assert!(
        up.status.success(),
        "metadata-owned runtime startup failed: {}",
        command_output_text(&up)
    );

    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(&metadata_path).expect("read metadata-owned runtime daemon metadata"),
    )
    .expect("parse metadata-owned runtime daemon metadata");
    let daemon_pid = metadata["pid"].as_u64().expect("metadata-owned daemon pid") as u32;
    let socket_path = PathBuf::from(
        metadata["socket_path"]
            .as_str()
            .expect("metadata-owned daemon socket path"),
    );
    let daemon_before_shutdown =
        process_snapshot(daemon_pid).expect("metadata-owned daemon process snapshot");

    let shutdown_started_at = Instant::now();
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("shutdown metadata-owned runtime daemon");
    assert!(
        shutdown.status.success(),
        "metadata-owned runtime shutdown failed: {}",
        command_output_text(&shutdown)
    );
    let daemon_after_shutdown = process_snapshot(daemon_pid);
    assert!(
        daemon_after_shutdown.is_none() && !process_exists(daemon_pid),
        "shutdown returned before metadata-owned daemon pid {daemon_pid} disappeared: \
         before={daemon_before_shutdown:?} after={daemon_after_shutdown:?} \
         shutdown_elapsed={:?} metadata_exists={} socket_exists={} output={}",
        shutdown_started_at.elapsed(),
        metadata_path.exists(),
        socket_path.exists(),
        command_output_text(&shutdown),
    );
    assert!(
        !metadata_path.exists(),
        "shutdown returned before owned runtime metadata was removed"
    );
    assert!(
        !socket_path.exists(),
        "shutdown returned before owned runtime socket was removed"
    );
    eprintln!(
        "metadata_owned_shutdown_production_topology pid={daemon_pid} \
         expected_reaper_pid={} before={daemon_before_shutdown:?} after=absent \
         shutdown_elapsed={:?}",
        daemon_before_shutdown.ppid,
        shutdown_started_at.elapsed(),
    );
}

#[test]
fn cli_shutdown_waits_until_metadata_owned_daemon_is_reaped() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("cli-shutdown-reaped-runtime");
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .expect("local socket binding")
        .path;
    let mut daemon = start_cli_daemon_with_session_worker(
        &data_dir,
        &session_worker_binary_path(),
    );
    let daemon_pid = daemon.child_mut().id();
    write_local_runtime_daemon_metadata(&data_dir, daemon_pid);
    let before_shutdown = process_snapshot(daemon_pid).expect("ready daemon process snapshot");

    let shutdown_started_at = Instant::now();
    let mut shutdown_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    shutdown_command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut shutdown = ReapingChild::new(shutdown_command.spawn().expect("spawn shutdown command"));

    let zombie_observed_at = Instant::now();
    let zombie = wait_for_process_snapshot(daemon_pid, "zombie state", |snapshot| {
        snapshot.stat.starts_with('Z')
    });
    assert_eq!(zombie.pid, daemon_pid, "zombie snapshot pid changed");
    assert_eq!(
        zombie.ppid,
        std::process::id(),
        "integration test should remain the daemon's expected reaper"
    );
    assert_eq!(
        zombie.pgid, before_shutdown.pgid,
        "daemon process group changed before exit"
    );
    assert_eq!(
        zombie.sid, before_shutdown.sid,
        "daemon process session changed before exit"
    );
    assert!(
        before_shutdown.command.contains("botster-hub"),
        "ready daemon command identity was unexpected: {before_shutdown:?}"
    );
    let pending_observation_deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < pending_observation_deadline {
        if let Some(status) = shutdown
            .child_mut()
            .try_wait()
            .expect("poll shutdown while daemon is unreaped")
        {
            let (stdout, stderr) = collect_child_output(shutdown.child_mut());
            panic!(
                "shutdown returned before metadata-owned daemon was reaped: status={status} \
                 daemon_before={before_shutdown:?} daemon_zombie={zombie:?} \
                 expected_reaper_pid={} shutdown_elapsed={:?} stdout={stdout:?} stderr={stderr:?} \
                 metadata_exists={} socket_exists={}",
                std::process::id(),
                shutdown_started_at.elapsed(),
                metadata_path.exists(),
                socket_path.exists(),
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    let daemon_output = daemon.wait_with_output().expect("wait for metadata-owned daemon");
    assert!(
        daemon_output.status.success(),
        "daemon did not exit cleanly before reap: status={} stdout={:?} stderr={:?}",
        daemon_output.status,
        String::from_utf8_lossy(&daemon_output.stdout),
        String::from_utf8_lossy(&daemon_output.stderr),
    );
    let reaped_at = Instant::now();
    let shutdown_output = shutdown.wait_with_output();
    assert!(
        shutdown_output.status.success(),
        "shutdown failed after daemon reap: status={} stdout={:?} stderr={:?}",
        shutdown_output.status,
        String::from_utf8_lossy(&shutdown_output.stdout),
        String::from_utf8_lossy(&shutdown_output.stderr),
    );
    assert!(
        !process_exists(daemon_pid),
        "shutdown completed while daemon pid {daemon_pid} still existed"
    );
    assert!(
        !metadata_path.exists(),
        "shutdown returned before owned runtime metadata was removed"
    );
    assert!(
        !socket_path.exists(),
        "shutdown returned before owned runtime socket was removed"
    );
    eprintln!(
        "metadata_owned_shutdown_characterization pid={daemon_pid} \
         expected_reaper_pid={} before={before_shutdown:?} zombie={zombie:?} \
         exit_to_reap={:?} shutdown_elapsed={:?}",
        std::process::id(),
        reaped_at.duration_since(zombie_observed_at),
        shutdown_started_at.elapsed(),
    );
}

#[test]
fn cli_local_runtime_up_reports_missing_installed_checkout_before_launch() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-missing-checkout");
    let project_pipelines_package_dir = unique_test_dir("cli-up-missing-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-missing-web");
    let tui_package_dir = unique_test_dir("cli-up-missing-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-missing-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let first = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        0,
    );
    assert!(
        first.status.success(),
        "initial up failed: {}",
        command_output_text(&first)
    );
    shutdown_local_runtime_daemon(&data_dir);
    let socket_path = data_dir.join("botster-hub.sock");
    for _ in 0..100 {
        if !socket_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !socket_path.exists(),
        "initial daemon socket should be gone before failed-start cleanup proof"
    );
    let failed_data_dir = unique_short_test_dir("cli-up-failed-cleanup");
    fs::create_dir_all(&failed_data_dir).expect("create failed-start data directory");
    fs::copy(
        data_dir.join("hub-state.json"),
        failed_data_dir.join("hub-state.json"),
    )
    .expect("copy installed package state into fresh failed-start directory");
    fs::remove_dir_all(&web_package_dir).expect("remove installed web checkout");

    let failed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&failed_data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run up with missing installed checkout");
    assert!(
        !failed.status.success(),
        "up should fail for missing installed checkout"
    );
    let text = command_output_text(&failed);
    assert!(text.contains("botster-web"), "{text}");
    assert!(
        text.contains(web_package_dir.to_string_lossy().as_ref()),
        "{text}"
    );
    let config = explicit_config(failed_data_dir.clone());
    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status);
    assert!(
        matches!(
            status,
            Err(botster_hub::DaemonTransportError::NotRunning)
                | Err(botster_hub::DaemonTransportError::ClientDisconnected)
        ),
        "failed startup should stop the daemon it started: {status:?}"
    );
    let failed_socket_path = failed_data_dir.join("botster-hub.sock");
    for _ in 0..100 {
        if !failed_socket_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !failed_socket_path.exists(),
        "failed startup left its owned socket: {text}"
    );
}

#[test]
fn process_ownership_cli_local_runtime_up_failure_stops_started_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-post-ready-cleanup");
    let web_package_dir = unique_test_dir("cli-up-post-ready-web");
    let tui_package_dir = unique_test_dir("cli-up-post-ready-tui");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    let web_manifest_path = web_package_dir.join("botster-package.json");
    let mut web_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&web_manifest_path).expect("read Web manifest"))
            .expect("parse Web manifest");
    web_manifest["runnable_entrypoints"][0]["environment"][0]["default"] =
        serde_json::Value::String("not-a-port".to_string());
    fs::write(
        &web_manifest_path,
        serde_json::to_string_pretty(&web_manifest).expect("serialize invalid-port Web manifest"),
    )
    .expect("write invalid-port Web manifest");
    ensure_session_worker_binary();
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let mut up = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn up with invalid Web package port");

    for _ in 0..500 {
        if metadata_path.exists() {
            break;
        }
        assert!(
            up.try_wait().expect("poll invalid-port up").is_none(),
            "invalid-port up exited before publishing owned daemon metadata"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        metadata_path.exists(),
        "invalid-port up should publish owned daemon metadata before Web launch fails"
    );
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(&metadata_path).expect("read invalid-port daemon metadata"),
    )
    .expect("parse invalid-port daemon metadata");
    let daemon_pid = metadata["pid"].as_u64().expect("metadata pid") as u32;
    let socket_path = PathBuf::from(
        metadata["socket_path"]
            .as_str()
            .expect("metadata socket path"),
    );

    let failed = up.wait_with_output().expect("wait for invalid-port up");
    assert!(
        !failed.status.success(),
        "up should fail for invalid Web package port"
    );
    let text = command_output_text(&failed);
    assert!(text.contains("botster-web"), "{text}");
    wait_for_process_exit(daemon_pid);
    assert!(
        !socket_path.exists(),
        "failed up left its configured owned socket: {socket_path:?}"
    );
    assert!(
        !metadata_path.exists(),
        "failed up left its owned daemon metadata"
    );
}

#[test]
fn process_ownership_metadata_write_failure_reaps_started_daemon_group() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("cli-up-metadata-write-failure");
    fs::create_dir_all(&data_dir).expect("create metadata failure data directory");
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    fs::create_dir(&metadata_path).expect("block metadata file creation with a directory");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .expect("local socket binding")
        .path;

    let failed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run up with blocked metadata path");

    assert!(
        !failed.status.success(),
        "metadata write failure must fail local runtime startup"
    );
    let text = command_output_text(&failed);
    assert!(
        text.contains("write local runtime daemon metadata"),
        "{text}"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    let data_dir_text = data_dir.to_string_lossy();
    loop {
        let process_rows = Command::new("ps")
            .args(["-axo", "command="])
            .output()
            .expect("inspect metadata-failure process rows");
        let process_rows_text = String::from_utf8_lossy(&process_rows.stdout);
        let attributable_rows = process_rows_text
            .lines()
            .filter(|row| row.contains(data_dir_text.as_ref()) && row.contains("botster-hub"))
            .collect::<Vec<_>>();
        if attributable_rows.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "metadata write failure left attributable daemon rows: {attributable_rows:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !socket_path.exists(),
        "metadata write failure left the owned daemon socket"
    );
    fs::remove_dir(&metadata_path).expect("remove metadata blocker");
    fs::remove_dir_all(&data_dir).expect("remove metadata failure data directory");
}

#[test]
fn cli_daily_commands_share_canonical_default_data_directory() {
    let _guard = daemon_test_guard();
    let checkout = unique_short_test_dir("daily");
    let other_checkout = unique_short_test_dir("daily-other-cwd");
    let home = unique_short_test_dir("daily-home");
    let xdg = unique_short_test_dir("daily-xdg");
    let data_dir = home.join(".botster/hub");
    let web_package_dir = unique_short_test_dir("cli-daily-default-web");
    let tui_package_dir = unique_short_test_dir("cli-daily-default-tui");
    fs::create_dir_all(&checkout).expect("create daily command checkout");
    fs::create_dir_all(&other_checkout).expect("create second daily command cwd");
    fs::create_dir_all(&home).expect("create daily command home");
    fs::create_dir_all(&xdg).expect("create ignored XDG root");
    for sibling in [
        "plugins",
        "agents",
        "lua",
        "profiles",
        "shared",
        "workspaces",
    ] {
        let sibling = home.join(".botster").join(sibling);
        fs::create_dir_all(&sibling).expect("create protected Botster sibling");
        fs::write(
            sibling.join("sentinel"),
            sibling.to_string_lossy().as_bytes(),
        )
        .expect("write protected Botster sibling sentinel");
    }
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    ensure_session_worker_binary();
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let mut up_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    up_command
        .current_dir(&checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("up")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    let up = up_command.output().expect("run default botster-hub up");
    assert!(
        up.status.success(),
        "default up failed: {}",
        command_output_text(&up)
    );
    assert!(
        command_output_text(&up).contains(&format!("data_dir=resolved:{}", data_dir.display()))
    );

    let run_daily = |command: &str, args: &[&str]| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
        process
            .current_dir(&other_checkout)
            .env("HOME", &home)
            .env("XDG_DATA_HOME", &xdg)
            .env_remove("BOTSTER_HUB_DATA_DIR")
            .arg(command)
            .args(args);
        process.output().expect("run daily command")
    };

    let status = run_daily("status", &[]);
    assert!(
        status.status.success(),
        "default status failed: {}",
        command_output_text(&status)
    );
    assert!(command_output_text(&status).contains("lifecycle_state=running"));

    for (command, args, marker) in [
        ("packages", &["list"][..], "response=packages"),
        ("apps", &["list"][..], "response=apps"),
        ("sessions", &["list"][..], "response=sessions"),
        ("session-types", &["list"][..], "response=session_types"),
        ("spawn-targets", &["list"][..], "response=spawn_targets"),
    ] {
        let output = run_daily(command, args);
        assert!(
            output.status.success(),
            "{command} without --data-dir failed: {}",
            command_output_text(&output)
        );
        assert!(
            command_output_text(&output).contains(marker),
            "{command} did not reach the shared daemon: {}",
            command_output_text(&output)
        );
    }
    for (command, args, usage) in [
        (
            "packages",
            &["list", "--registry", "/tmp/ignored"][..],
            "packages list",
        ),
        ("providers", &["list", "extra"][..], "providers list"),
        ("apps", &["list", "extra"][..], "apps list"),
        ("sessions", &["list", "extra"][..], "sessions list"),
        (
            "session-types",
            &["list", "extra"][..],
            "session-types list",
        ),
        (
            "session-types",
            &["definition"][..],
            "session-types definition",
        ),
        (
            "spawn-targets",
            &["list", "extra"][..],
            "spawn-targets list",
        ),
        ("shutdown", &["extra"][..], "shutdown"),
        ("mcp-serve", &["extra"][..], "mcp-serve"),
    ] {
        let output = run_daily(command, args);
        assert!(
            !output.status.success(),
            "{command} silently accepted extra operands: {}",
            command_output_text(&output)
        );
        assert!(
            command_output_text(&output).contains(&format!("usage: botster-hub {usage}")),
            "{command} did not report its usage: {}",
            command_output_text(&output)
        );
    }
    let mut mcp_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .current_dir(&other_checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("mcp-serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn default mcp-serve");
    mcp_child
        .stdin
        .as_mut()
        .expect("mcp stdin")
        .write_all(
            br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
        )
        .expect("write MCP initialize");
    mcp_child
        .stdin
        .take()
        .expect("close mcp stdin after initialize");
    let mcp = mcp_child.wait_with_output().expect("wait for mcp-serve");
    assert!(
        mcp.status.success(),
        "mcp-serve without --data-dir failed: {}",
        command_output_text(&mcp)
    );
    let mcp_stdout = String::from_utf8(mcp.stdout).expect("MCP output is UTF-8");
    assert!(
        mcp_stdout.contains(r#""protocolVersion":"2025-06-18""#),
        "mcp-serve did not answer initialize through the shared daemon root: {mcp_stdout}"
    );

    let doctor = run_daily("doctor", &[]);
    assert!(
        doctor.status.success(),
        "default doctor failed: {}",
        command_output_text(&doctor)
    );
    let doctor_text = command_output_text(&doctor);
    assert!(doctor_text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(doctor_text.contains("check name=daemon_running status=pass"));

    let open_web = run_daily("open", &["web"]);
    assert!(
        open_web.status.success(),
        "default open web failed: {}",
        command_output_text(&open_web)
    );
    assert!(command_output_text(&open_web).contains("app_url=http://127.0.0.1:"));

    let open_tui = run_daily("open", &["tui"]);
    assert!(
        open_tui.status.success(),
        "default open tui failed: {}",
        command_output_text(&open_tui)
    );
    assert!(command_output_text(&open_tui).contains("botster-tui-fixture"));

    let mut smoke_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    smoke_command
        .current_dir(&checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("smoke")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    let smoke = smoke_command
        .output()
        .expect("run default botster-hub smoke");
    if !smoke.status.success() {
        panic!("{}", local_webrtc_smoke_failure_evidence(&smoke, &data_dir));
    }
    let smoke_text = command_output_text(&smoke);
    assert!(smoke_text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(
        smoke_text.contains("check name=daemon status=pass message=daemon reused"),
        "smoke must reuse the daemon started by up: {smoke_text}"
    );
    assert!(smoke_text.contains("smoke_result=pass"));

    let status_after_smoke = run_daily("status", &[]);
    assert!(
        status_after_smoke.status.success(),
        "reused smoke must leave the default daemon running: {}",
        command_output_text(&status_after_smoke)
    );

    let down = run_daily("down", &[]);
    assert!(
        down.status.success(),
        "default down failed: {}",
        command_output_text(&down)
    );
    assert!(command_output_text(&down).contains("response=shutdown"));

    let stopped = run_daily("status", &[]);
    assert!(
        !stopped.status.success(),
        "default status should fail after down: {}",
        command_output_text(&stopped)
    );
    assert!(
        !xdg.join("botster-hub").exists(),
        "XDG_DATA_HOME must not select or create Hub state"
    );
    assert!(
        !checkout.join("target/botster-hub-runtime-data").exists(),
        "cwd-relative legacy default must not be recreated"
    );
    assert!(
        !other_checkout
            .join("target/botster-hub-runtime-data")
            .exists(),
        "second cwd must not receive legacy runtime state"
    );
    for sibling in [
        "plugins",
        "agents",
        "lua",
        "profiles",
        "shared",
        "workspaces",
    ] {
        assert!(
            home.join(".botster")
                .join(sibling)
                .join("sentinel")
                .exists(),
            "protected Botster sibling {sibling} was mutated"
        );
    }
}

#[test]
fn cli_doctor_reports_healthy_runtime_checks() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-healthy");
    let project_pipelines_package_dir = unique_test_dir("cli-doctor-project-pipelines");
    let web_package_dir = unique_test_dir("cli-doctor-web");
    let tui_package_dir = unique_test_dir("cli-doctor-tui");
    let workspaces_package_dir = unique_test_dir("cli-doctor-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let up = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        0,
    );
    assert!(
        up.status.success(),
        "up failed: {}",
        command_output_text(&up)
    );

    let doctor = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against healthy runtime");
    assert!(
        doctor.status.success(),
        "doctor failed: {}",
        command_output_text(&doctor)
    );
    let text = command_output_text(&doctor);
    assert!(text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(text.contains("check name=daemon_running status=pass"));
    assert!(text.contains("check name=daemon_compatible status=pass"));
    assert!(text.contains("conformance_fixture_revision="));
    assert!(text.contains("check name=core_initialized status=pass"));
    assert!(text.contains("check name=package_registry status=pass"));
    assert!(text.contains("check name=botster_web_app status=pass"));
    for package_dir in [
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
    ] {
        assert!(
            !text.contains(package_dir.to_string_lossy().as_ref()),
            "doctor output should not leak package source path {package_dir:?}: {text}"
        );
    }

    shutdown_local_runtime_daemon(&data_dir);
}

#[test]
fn cli_home_runtime_up_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-owned-incompat");
    let data_dir = home.join(".botster/hub");
    let project_pipelines_package_dir = unique_test_dir("cli-up-owned-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-owned-web");
    let tui_package_dir = unique_test_dir("cli-up-owned-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-owned-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);
    let mut stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let stale_pid = stale_child.id();

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("up")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run bare up after incompatible daemon");
    assert!(
        output.status.success(),
        "up failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("runtime=ready"));
    assert!(text.contains("daemon=started"));
    let web_origin = text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("runtime output includes web URL")
        .trim_end_matches('/')
        .to_string();
    let health = read_json_health(&web_origin);
    assert_eq!(
        health["ok"], true,
        "replacement Web package server health: {health}"
    );
    assert_eq!(health["daemonReady"], true);
    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::Status,
    )
    .expect("replacement daemon answers status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert_eq!(
        status.status.expect("runtime status body").lifecycle_state,
        "running"
    );
    let _ = stale_child.wait().expect("reap stale daemon");
    assert!(
        !process_exists(stale_pid),
        "stale incompatible daemon should be stopped"
    );
    assert!(
        explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("replacement socket binding")
            .path
            .exists(),
        "replacement socket should remain after stale child exit"
    );

    shutdown_local_runtime_daemon(&data_dir);
}

#[test]
fn cli_home_runtime_start_does_not_reuse_dead_pid_metadata_and_rebinds_leftover_socket() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-dead-metadata");
    let data_dir = home.join(".botster/hub");
    fs::create_dir_all(&data_dir).expect("create home runtime data directory");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("home runtime socket binding")
        .path
        .clone();
    let stale_listener = UnixListener::bind(&socket_path).expect("bind leftover socket fixture");
    drop(stale_listener);

    let mut exited = Command::new("true")
        .spawn()
        .expect("spawn dead pid fixture");
    let dead_pid = exited.id();
    assert!(exited.wait().expect("wait for dead pid fixture").success());
    assert!(!process_exists(dead_pid), "fixture pid must be dead");
    write_local_runtime_daemon_metadata(&data_dir, dead_pid);

    ensure_session_worker_binary();
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("start")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start home runtime with stale dead-pid metadata");
    wait_for_status(&data_dir, &mut daemon);

    let stale_metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json"))
            .expect("read stale daemon metadata"),
    )
    .expect("parse stale daemon metadata");
    assert_eq!(stale_metadata["pid"].as_u64(), Some(dead_pid as u64));
    assert_ne!(daemon.id(), dead_pid, "start must not reuse the dead pid");
    assert!(
        process_exists(daemon.id()),
        "replacement daemon must remain alive after readiness"
    );
    assert!(
        socket_path.exists(),
        "replacement daemon must own the canonical home socket"
    );

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("shutdown")
        .output()
        .expect("shut down replacement home runtime");
    assert!(
        shutdown.status.success(),
        "replacement shutdown failed: {}",
        command_output_text(&shutdown)
    );
    assert!(
        daemon.wait().expect("reap replacement daemon").success(),
        "replacement daemon should exit cleanly"
    );
}

#[test]
fn cli_local_runtime_down_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-down-owned-incompat");
    let stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let stale_pid = stale_child.id();
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down against owned incompatible daemon");
    assert!(
        output.status.success(),
        "down failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    assert!(command_output_text(&output).contains("daemon=recovered_stale"));
    let _ = stale_child.wait_with_output().expect("reap stale daemon");
    assert!(
        !process_exists(stale_pid),
        "stale incompatible daemon should be stopped"
    );
    assert!(
        !socket_path.exists(),
        "down recovery should remove the selected data dir socket"
    );
}

#[test]
fn cli_local_runtime_recovery_removes_only_selected_data_dir_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-scoped-owned-incompat");
    let other_data_dir = unique_short_test_dir("cli-scoped-other-incompat");
    let stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let selected_socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("selected local socket binding")
        .path
        .clone();
    let other_socket_path = explicit_config(&other_data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("other local socket binding")
        .path
        .clone();
    fs::create_dir_all(other_socket_path.parent().expect("other socket parent"))
        .expect("create other socket parent");
    let _other_listener = UnixListener::bind(&other_socket_path).expect("bind other socket");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down for selected data dir");
    assert!(
        output.status.success(),
        "down failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    let _ = stale_child.wait_with_output().expect("reap stale daemon");
    assert!(
        !selected_socket_path.exists(),
        "selected data dir socket should be removed"
    );
    assert!(
        other_socket_path.exists(),
        "recovery must not remove sockets for other data dirs"
    );
    let _ = fs::remove_file(other_socket_path);
}

#[test]
fn cli_local_runtime_up_refuses_unowned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-incompat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        for _ in 0..2 {
            let Ok((mut stream, _addr)) = listener.accept() else {
                break;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
            let mut hello = String::new();
            let _ = reader.read_line(&mut hello);
            let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
        }
    });
    ready_rx.recv().expect("fake listener ready");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub up against incompatible daemon");
    assert!(
        !output.status.success(),
        "up unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("botster-hub down"));
    assert!(text.contains("may fail against this daemon"));
    assert!(text.contains("Stop the running botster-hub process directly"));
    assert!(text.contains("remove the stale local socket"));
    assert!(text.contains("botster-hub up [--data-dir <path>]"));
    assert!(
        socket_path.exists(),
        "up must not delete a connectable socket on compatibility failure"
    );

    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down against incompatible daemon");
    assert!(
        !down.status.success(),
        "down unexpectedly succeeded: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("running daemon is incompatible or stale"));
    assert!(down_text.contains("Stop the running botster-hub process directly"));
    assert!(down_text.contains("remove the stale local socket"));

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn cli_local_runtime_refuses_forged_metadata_for_live_non_botster_pid() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-forged-pid-incompat");
    let data_dir = home.join(".botster/hub");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        for _ in 0..2 {
            let Ok((mut stream, _addr)) = listener.accept() else {
                break;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
            let mut hello = String::new();
            let _ = reader.read_line(&mut hello);
            let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
        }
    });
    ready_rx.recv().expect("fake listener ready");

    let mut decoy = ChildCleanup::spawn_non_botster_decoy();
    write_local_runtime_daemon_metadata(&data_dir, decoy.id());

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("up")
        .output()
        .expect("run botster-hub up against forged daemon metadata");
    assert!(
        !output.status.success(),
        "up unexpectedly recovered forged metadata: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("Stop the running botster-hub process directly"));
    decoy.assert_alive();
    assert!(
        socket_path.exists(),
        "up must not delete a connectable socket when metadata pid is not botster-owned"
    );

    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("down")
        .output()
        .expect("run botster-hub down against forged daemon metadata");
    assert!(
        !down.status.success(),
        "down unexpectedly recovered forged metadata: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("running daemon is incompatible or stale"));
    assert!(down_text.contains("Stop the running botster-hub process directly"));
    decoy.assert_alive();
    assert!(
        socket_path.exists(),
        "down must not delete a connectable socket when metadata pid is not botster-owned"
    );

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn cli_doctor_reports_incompatible_stale_daemon_without_deleting_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-incompat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        let Ok((mut stream, _addr)) = listener.accept() else {
            return;
        };
        let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
        let mut hello = String::new();
        let _ = reader.read_line(&mut hello);
        let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
    });
    ready_rx.recv().expect("fake listener ready");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against incompatible daemon");
    assert!(
        !output.status.success(),
        "doctor unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("check name=daemon_compatible status=fail"));
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("stop the stale botster-hub process"));
    assert!(
        socket_path.exists(),
        "doctor must not delete a connectable socket on compatibility failure"
    );

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn daemon_starts_empty_state_reports_status_uses_core_and_stops_idempotently() {
    let config = explicit_config(unique_test_dir("empty"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from empty state");

    let status = daemon.status();
    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Initialized);
    assert_eq!(status.host_id, "hub-daemon-test");
    assert_eq!(status.host_display_name, "Hub Daemon Test");
    assert_eq!(status.schema_version, 3);
    assert!(status.data_dir_configured);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 0);
    assert_eq!(status.provider_count, 0);
    assert!(store.path().exists());

    let runtime = daemon.runtime_mut().expect("runtime initialized");
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    runtime
        .spawn_session(request, CoreSessionMetadata::new(), 1)
        .expect("spawn through core daemon runtime");
    assert_eq!(runtime.list_sessions().expect("daemon list").len(), 1);
    runtime
        .shutdown_session(session_id, 2)
        .expect("shutdown through core daemon runtime");

    let stopped = daemon.stop();
    assert_eq!(stopped.lifecycle_state, HubDaemonState::Stopped);
    assert!(!stopped.core_initialized);
    let stopped_again = daemon.stop();
    assert_eq!(stopped_again, stopped);

    let reopened = store
        .load_or_initialize(&config)
        .expect("reload committed daemon state");
    assert_eq!(reopened.schema_version, 3);
    assert_eq!(reopened.host.id, "hub-daemon-test");
}

#[test]
fn daemon_restart_reconnects_worker_backed_session_through_client_api() {
    let config = explicit_config(unique_test_dir("restart-reconnect"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-restart-client");
    let session_id = SessionId("hub-daemon-restart-session".to_string());
    let subscription_id = SubscriptionId("hub-daemon-restart-subscription".to_string());
    let mut logical_clock = 10;

    let mut daemon = HubDaemon::start(config.clone()).expect("start first hub daemon");
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-restart-spawn".to_string()),
            session_id: session_id.clone(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn through hub client api");
    logical_clock += 1;
    let runtime = daemon.runtime_mut().expect("runtime initialized");
    runtime
        .attach_client(
            api.identity().client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            logical_clock,
        )
        .expect("attach before restart");
    logical_clock += 1;
    let generation = runtime
        .list_terminal_subscriptions()
        .into_iter()
        .find(|row| row.session_id == session_id && row.subscription_id == subscription_id)
        .map(|row| row.generation)
        .expect("live generation");
    runtime
        .bind_terminal_adapter(
            api.identity().client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            generation,
            botster_core::TerminalCapabilitySet::from_tokens(["terminal_streaming", "resize"])
                .expect("tokens"),
            Box::new(botster_core_test_support::terminal_adapter::FakeTerminalAdapter::default()),
        )
        .expect("bind before restart");
    logical_clock += 1;
    daemon.stop();

    let mut restarted = HubDaemon::start(config).expect("restart hub daemon");
    assert!(
        restarted
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&session_id),
        "restart should recover the live worker-backed session"
    );
    let listed = api
        .handle_request(
            restarted.runtime_mut().expect("runtime initialized"),
            &packages,
            HubClientRequest::ListSessions {
                request_id: RequestId("hub-daemon-restart-list".to_string()),
            },
        )
        .expect("list after restart through client api");
    assert!(
        matches!(listed.body, HubClientResponseBody::Sessions(sessions) if sessions.iter().any(|session| session.session_id == session_id))
    );

    let runtime = restarted.runtime_mut().expect("runtime initialized");
    runtime
        .attach_client(
            api.identity().client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            logical_clock,
        )
        .expect("reattach after restart through runtime attach");
    logical_clock += 1;
    let generation = runtime
        .list_terminal_subscriptions()
        .into_iter()
        .find(|row| row.session_id == session_id && row.subscription_id == subscription_id)
        .map(|row| row.generation)
        .expect("live generation after restart");
    runtime
        .bind_terminal_adapter(
            api.identity().client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            generation,
            botster_core::TerminalCapabilitySet::from_tokens(["terminal_streaming", "resize"])
                .expect("tokens"),
            Box::new(botster_core_test_support::terminal_adapter::FakeTerminalAdapter::default()),
        )
        .expect("bind after restart");
    logical_clock += 1;
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Input {
            request_id: RequestId("hub-daemon-restart-input".to_string()),
            session_id: session_id.clone(),
            data: b"after-restart\r".to_vec(),
            now_seconds: logical_clock,
        },
    )
    .expect("input after restart through client api");
    logical_clock += 1;
    let mut screen = String::new();
    for _ in 0..100 {
        let _ = restarted
            .runtime_mut()
            .expect("runtime initialized")
            .observe_lifecycle_slice(
                logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: std::time::Duration::from_millis(25),
                },
            );
        logical_clock += 1;
        let response = api
            .handle_request(
                restarted.runtime_mut().expect("runtime initialized"),
                &packages,
                HubClientRequest::ReadScreen {
                    request_id: RequestId("hub-daemon-restart-screen".to_string()),
                    session_id: session_id.clone(),
                    now_seconds: logical_clock,
                },
            )
            .expect("read screen after restart");
        if let HubClientResponseBody::ReadScreen(body) = response.body {
            screen = body.text;
            if screen.contains("echo:after-restart") {
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        screen.contains("echo:after-restart"),
        "restarted worker must echo after bind+observe: {screen:?}"
    );
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Shutdown {
            request_id: RequestId("hub-daemon-restart-shutdown".to_string()),
            session_id,
            now_seconds: logical_clock,
        },
    )
    .expect("shutdown after restart through client api");
}

#[test]
fn daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions() {
    let stale_config = explicit_config(unique_test_dir("stale-reconcile"));
    let stale_session_id = SessionId("hub-daemon-stale-session".to_string());
    let registry = SessionRegistry::new(stale_config.data_directory.clone());
    let mut stale_record = RegistryRecord::running(
        stale_session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    stale_record.observe_restart_contract(serde_json::json!({"session": "hub-daemon-stale"}), 2);
    registry
        .save(&stale_record)
        .expect("stale registry fixture should save");

    let stale_daemon = HubDaemon::start(stale_config).expect("start daemon with stale registry");
    assert!(
        stale_daemon
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .stale_sessions
            .contains(&stale_session_id),
        "registry record without a live worker should become stale deterministically"
    );

    let recovered_config = explicit_config(unique_test_dir("recovered-reconcile"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-recovered-client");
    let recovered_session_id = SessionId("hub-daemon-recovered-session".to_string());
    let mut first = HubDaemon::start(recovered_config.clone()).expect("start first daemon");
    api.handle_request(
        first.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-recovered-spawn".to_string()),
            session_id: recovered_session_id.clone(),
            command: "printf 'recovered-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: 1,
        },
    )
    .expect("spawn recovered session through client api");
    first.stop();

    let recovered =
        HubDaemon::start(recovered_config).expect("restart daemon with live core registry record");
    assert!(
        recovered
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&recovered_session_id),
        "core-live worker-backed session absent from hub state should be recovered"
    );
}

#[test]
fn daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues() {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let config = explicit_config(unique_test_dir("stale-adoption-socket"));
            let session_id = SessionId("hub-daemon-stale-adoption-socket".to_string());
            let stale_socket = PathBuf::from(format!(
                "/tmp/bh-stale-{}.sock",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time after epoch")
                    .as_nanos()
            ));
            let registry = SessionRegistry::new(config.data_directory.clone());
            let mut record = RegistryRecord::running(
                session_id.clone(),
                Some(ProcessIdentity {
                    pid: Some(42),
                    runtime_id: Some("stale-adoption-runtime".to_string()),
                }),
                ResizePayload { rows: 24, cols: 80 },
                "sh".to_string(),
                1,
            );
            record.observe_restart_contract(
                serde_json::json!({
                    "worker_control_socket": stale_socket,
                    "mode": "worker_process"
                }),
                2,
            );
            registry
                .save(&record)
                .expect("stale adoption registry fixture should save");

            let mut daemon =
                HubDaemon::start(config).expect("start daemon with stale worker control socket");
            let status = daemon.status();
            assert!(
                status.stale_sessions.contains(&session_id),
                "stale worker control socket should be surfaced in daemon status"
            );

            let packages = empty_registry();
            let api = HubClientApi::local_operator("hub-daemon-stale-adoption-client");
            let fresh_session_id = SessionId("hub-daemon-fresh-after-stale".to_string());
            api.handle_request(
                daemon.runtime_mut().expect("runtime initialized"),
                &packages,
                HubClientRequest::Spawn {
                    request_id: RequestId("hub-daemon-fresh-after-stale-spawn".to_string()),
                    session_id: fresh_session_id.clone(),
                    command: "printf 'fresh-after-stale-ready\\n'; sleep 1".to_string(),
                    now_seconds: 3,
                },
            )
            .expect("fresh session should spawn after stale adoption reconciliation");
            assert!(
                daemon
                    .runtime()
                    .expect("runtime initialized")
                    .list_sessions()
                    .expect("list sessions after fresh spawn")
                    .iter()
                    .any(|session| session.session_id == fresh_session_id),
                "fresh session should be visible after stale adoption reconciliation"
            );
        });
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("stale adoption socket startup reconciliation deadlocked")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("stale adoption socket startup reconciliation worker exited unexpectedly")
        }
    }
}

#[test]
fn daemon_restores_existing_provider_policy_records_through_snapshot_admission() {
    let config = explicit_config(unique_test_dir("existing"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut policy = PackageAdmissionPolicy::from_host_profile();
    policy
        .install(
            provider_manifest(),
            package_provenance(),
            "install provider policy record",
        )
        .expect("install provider");
    policy
        .enable("daemon.provider", "enable provider policy record")
        .expect("enable provider through admission");

    store
        .update(&config, |state| {
            state.package_registry = policy.registry().snapshot();
        })
        .expect("seed existing state through store");

    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from existing state");
    let status = daemon.status();

    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Loaded);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 1);
    assert_eq!(status.enabled_package_count, 1);
    assert_eq!(status.provider_count, 1);
    assert_eq!(status.enabled_provider_count, 1);
    assert_eq!(status.schema_version, 3);

    daemon.stop();
    let reopened = store
        .load_or_initialize(&config)
        .expect("reload existing state after stop");
    assert_eq!(reopened.package_registry.records.len(), 1);
    assert!(reopened.package_registry.records[0].is_enabled());
}

#[test]
fn cli_start_and_status_print_scrubbed_lifecycle_status() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-start");
    let child = start_cli_daemon(&data_dir);
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status");

    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(stdout.contains("schema_version=3"));
    assert!(stdout.contains("core_initialized=true"));
    assert!(stdout.contains("state_source=initialized"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(concat!("/", "Users", "/")));
    assert!(!stdout.contains("/home/"));
    assert!(data_dir.join("hub-state.json").exists());

    let output = shutdown_cli_daemon(&data_dir, child);
    let stdout = String::from_utf8(output.stdout).expect("daemon stdout is utf8");
    assert!(stdout.contains("event=stopped"));
    assert!(stdout.contains("lifecycle_state=stopped"));
}

#[test]
fn cli_status_uses_daemon_status_path_without_local_paths() {
    let data_dir = unique_test_dir("cli-status");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status");

    assert!(
        !output.status.success(),
        "status unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn process_ownership_daemon_restart_adopts_then_shuts_down_worker_session() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-restart-recover");
    let config = explicit_config(&data_dir);
    let session_id = format!("cli-restart-session-{}", std::process::id());

    let child = start_cli_daemon(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; if [ \"$line\" = after-restart ]; then exit 0; fi; done".to_string(),
        },
    )
    .expect("spawn restart recovery session through daemon transport");
    assert_eq!(
        spawn.kind,
        botster_hub::DaemonResponseKind::Spawned,
        "spawn failed: {:?}",
        spawn.error
    );
    assert!(
        spawn
            .sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "running")
    );

    let mut pre_restart = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect before daemon restart");
    pre_restart
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: "cli-restart-subscription-before".to_string(),
        })
        .expect("attach before daemon restart");
    let before = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut text = String::new();
        while Instant::now() < deadline {
            let _ = pre_restart.request(&botster_hub_client::DaemonRequest::drain_subscription(
                session_id.as_str(),
                "cli-restart-subscription-before",
            ));
            text = pre_restart
                .request(&botster_hub_client::DaemonRequest::ReadScreen {
                    session_id: session_id.to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .map(|screen| screen.text)
                .unwrap_or_default();
            if text.contains("restart-ready") {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
        text
    };
    assert!(
        before.contains("restart-ready"),
        "session must be readable before restart, got {before:?}"
    );
    drop(pre_restart);

    shutdown_cli_daemon(&data_dir, child.transfer_sessions());
    let restarted_child = start_cli_daemon(&data_dir);

    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status)
        .expect("status after daemon restart");
    let status = status.status.expect("status response body");
    assert_eq!(status.lifecycle_state, "running");
    assert!(status.core_initialized);
    assert!(
        status
            .recovered_sessions
            .iter()
            .any(|recovered| recovered == &session_id),
        "restarted daemon should report startup recovery for the live worker-backed session"
    );
    assert!(
        !status
            .stale_sessions
            .iter()
            .any(|stale| stale == &session_id),
        "worker-backed session with protocol evidence should not be marked stale"
    );

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
            .expect("list recovered session through daemon transport");
    assert!(
        list.sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "running")
    );

    let resize = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Resize {
            session_id: session_id.to_string(),
            rows: 30,
            cols: 100,
        },
    )
    .expect("resize after daemon restart");
    assert_eq!(resize.kind, botster_hub::DaemonResponseKind::Events);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect after daemon restart");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: "cli-restart-subscription-after".to_string(),
        })
        .expect("attach after daemon restart");
    let ready_deadline = Instant::now() + Duration::from_secs(8);
    let mut attached = false;
    while Instant::now() < ready_deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::drain_subscription(
                session_id.as_str(),
                "cli-restart-subscription-after",
            ))
            .ok();
        attached |= drain.is_some_and(|response| {
            response.events.iter().any(|event| {
                matches!(
                    event,
                    botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
                )
            })
        });
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .ok()
            .and_then(|response| response.read_screen)
            .map(|screen| screen.text)
            .unwrap_or_default();
        if attached && (screen.contains("restart-ready") || !screen.trim().is_empty()) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "after-restart\r".to_string(),
        })
        .expect("send input after daemon restart");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut screen_text = String::new();
    while Instant::now() < deadline {
        let _ = connection.request(&botster_hub_client::DaemonRequest::drain_subscription(
            session_id.as_str(),
            "cli-restart-subscription-after",
        ));
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .expect("read screen after restart");
        screen_text = screen
            .read_screen
            .as_ref()
            .map(|screen| screen.text.clone())
            .unwrap_or_default();
        if screen_text.contains("echo:after-restart") || screen_text.contains("restart-ready") {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        screen_text.contains("restart-ready") || screen_text.contains("echo:after-restart"),
        "recovered session should remain readable after restart, got {screen_text:?}"
    );
    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn daemon_resolves_terminal_app_foreground_launch_contract() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("resolve-terminal-app");
    let package_dir = unique_test_dir("resolve-terminal-app-package");
    write_botster_tui_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let response = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolveAppLaunch {
            package_name: "botster-tui".to_string(),
            entrypoint_id: "botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app launch");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::ResolvedAppLaunch
    );
    let launch = response
        .resolved_app_launch
        .expect("resolved foreground launch");
    assert_eq!(launch.package_name, "botster-tui");
    assert_eq!(launch.kind, "terminal_app");
    assert_eq!(launch.launch_mode, "foreground_stdio");
    assert_eq!(launch.command, "sh");
    let connection: serde_json::Value = serde_json::from_str(
        launch
            .environment
            .get("BOTSTER_HUB_CONNECTION")
            .expect("Hub connection injection"),
    )
    .expect("decode Hub connection injection");
    assert_eq!(
        connection["transport"]["type"],
        serde_json::Value::String("unix_socket".to_string())
    );
    assert!(
        connection["transport"]["path"]
            .as_str()
            .expect("Hub connection path")
            .starts_with('/')
    );
    assert!(launch.environment.contains_key("BOTSTER_HUB_DATA_DIR"));
    assert_eq!(
        launch
            .environment
            .get("BOTSTER_TUI_MODE")
            .map(String::as_str),
        Some("headless")
    );

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after daemon restart");
    let app = app_row(&apps, "botster-tui");
    assert_eq!(app.package_name, "botster-tui");
    assert_eq!(app.entrypoint_id, "botster-tui");
    assert_eq!(app.kind, "terminal_app");
    let app_route = app.route.as_ref().expect("app route descriptor");
    assert_eq!(app_route.route_id, "app:botster-tui");
    assert_eq!(
        app_route.route_path,
        "/packages/botster-tui/apps/botster-tui"
    );
    assert_eq!(app_route.target.kind, "app_entrypoint");
    assert_eq!(
        app_route.target.entrypoint_id.as_deref(),
        Some("botster-tui")
    );
    assert_eq!(app_route.layout_mode, "app_entrypoint");
    assert!(app_route.enabled);
    assert!(!app_route.blocked);

    let reloaded = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolveAppLaunch {
            package_name: "botster-tui".to_string(),
            entrypoint_id: "botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app launch after daemon restart");
    assert_eq!(
        reloaded.kind,
        botster_hub::DaemonResponseKind::ResolvedAppLaunch
    );
    assert_eq!(
        reloaded
            .resolved_app_launch
            .expect("resolved foreground launch after restart")
            .command,
        "sh"
    );
    let resolved_route = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "botster-tui".to_string(),
            route_id: "app:botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app route after daemon restart");
    assert_eq!(
        resolved_route.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_route
            .resolved_package_route
            .expect("resolved app route")
            .route_path,
        "/packages/botster-tui/apps/botster-tui"
    );

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn cli_no_arg_non_tty_rejects_before_creating_runtime_state() {
    let data_dir = unique_short_test_dir("no-tty");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("BOTSTER_HUB_DATA_DIR", &data_dir)
        .output()
        .expect("run no-arg hub without a TTY");
    assert!(
        !output.status.success(),
        "no-arg non-TTY invocation should fail: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("requires terminal stdin and stdout"));
    assert!(text.contains("scripts must use an explicit subcommand"));
    assert!(
        !data_dir.exists(),
        "non-TTY invocation created runtime state"
    );
}

#[test]
fn cli_help_like_args_print_command_guidance_without_daemon() {
    for arg in ["help", "--help"] {
        let help = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg(arg)
            .output()
            .expect("run help-like hub command");
        assert!(
            help.status.success(),
            "help command failed: {}",
            command_output_text(&help)
        );
        let text = command_output_text(&help);
        assert!(text.contains("Daily runtime commands:"));
        assert!(text.contains("botster-hub up [--data-dir <path>]"));
        assert!(text.contains("botster-hub down [--data-dir <path>]"));
        assert!(text.contains("botster-hub status [--data-dir <path>]"));
        assert!(text.contains("botster-hub doctor [--data-dir <path>]"));
        assert!(text.contains("botster-hub smoke [--data-dir <path>]"));
        assert!(text.contains("botster-hub open web [--data-dir <path>]"));
        assert!(text.contains("botster-hub open tui [--data-dir <path>]"));
        assert!(text.contains("botster-hub mcp-serve [--data-dir <path>]"));
        assert!(text.contains("botster-hub apps open [--data-dir <path>] <app|package/app>"));
        assert!(text.contains(
            "botster-hub packages config set [--data-dir <path>] <name> '<json-object>'"
        ));
        assert!(text.contains(
            "botster-hub packages apply-update [--data-dir <path>] <name> --revision <revision>"
        ));
        assert!(!text.contains("first-party host profile ready"));
        assert!(!text.contains("unknown command"));
    }
}

#[test]
fn daemon_batch_local_refresh_rejects_mixed_registration_set_on_validation_failure() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("batch-local-refresh-atomic");
    let alpha_dir = unique_test_dir("batch-local-refresh-alpha");
    let beta_dir = unique_test_dir("batch-local-refresh-beta");
    write_reloadable_app_package_named(
        &alpha_dir,
        "refresh.alpha",
        "1.0.0",
        "http://127.0.0.1:49164",
    );
    write_reloadable_app_package_named(
        &beta_dir,
        "refresh.beta",
        "1.0.0",
        "http://127.0.0.1:49165",
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    for package_dir in [&alpha_dir, &beta_dir] {
        botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install local package");
    }

    write_reloadable_app_package_named(
        &alpha_dir,
        "refresh.alpha",
        "2.0.0",
        "http://127.0.0.1:49166",
    );
    fs::remove_file(beta_dir.join("plugin.lua")).expect("remove beta entrypoint");

    let refresh = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::RefreshLocalPackages,
    )
    .expect("failed refresh should return an operator frame");
    assert_eq!(refresh.kind, botster_hub::DaemonResponseKind::OperatorError);
    let error = refresh.error.expect("refresh operator error");
    assert!(error.message.contains("refresh.beta"));
    assert!(error.message.contains(beta_dir.to_string_lossy().as_ref()));

    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after failed refresh");
    for package_name in ["refresh.alpha", "refresh.beta"] {
        assert_eq!(
            packages
                .packages
                .iter()
                .find(|package| package.package_name == package_name)
                .expect("installed package")
                .version,
            "1.0.0"
        );
    }
    let state_json =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read durable hub state");
    assert!(!state_json.contains("\"version\": \"2.0.0\""));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn no_arg_non_tty_does_not_create_home_or_xdg_state_file() {
    let home = unique_test_dir("home");
    let xdg = unique_test_dir("xdg");
    fs::create_dir_all(&home).expect("create home");
    fs::create_dir_all(&xdg).expect("create xdg");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .output()
        .expect("run botster-hub without a TTY");

    assert!(
        !output.status.success(),
        "no-arg non-TTY unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    assert!(
        command_output_text(&output).contains("scripts must use an explicit subcommand"),
        "{}",
        command_output_text(&output)
    );
    assert_no_state_file_under(&home);
    assert_no_state_file_under(&xdg);
}

/// The production path: the actually-installed Hub, launched through the
/// installed entrypoint, reporting a managed installation.
#[test]
fn a_real_managed_installation_reports_managed_status_and_distinct_provenance() {
    let _guard = daemon_test_guard();
    let origin = ManagedReleaseOrigin::start();
    let (prefix, release) = install_real_release("managed-install", &origin);
    let entrypoint = prefix.join("bin/botster-hub");
    let generation = format!("{}-{}", release.hub_revision, release.core_revision);

    // The generation directory holds the revision-coupled pair, and both
    // binaries resolve under the prefix rather than the development checkout.
    let generation_dir = prefix.join("generations").join(&generation);
    let installed_hub = fs::canonicalize(&entrypoint).expect("resolve the installed Hub realpath");
    assert_eq!(
        installed_hub,
        fs::canonicalize(generation_dir.join("botster-hub")).expect("generation Hub realpath"),
        "bin/botster-hub resolves through current into its own generation"
    );
    assert!(
        generation_dir.join("botster-session-worker").is_file(),
        "the locked-Core worker is installed beside the Hub in the same generation"
    );

    // `version` needs no data directory and no daemon: it is how the installer
    // verified a staged binary that had never been started.
    let version = Command::new(&entrypoint)
        .arg("version")
        .env("HOME", &prefix)
        .output()
        .expect("run the installed Hub version subcommand");
    assert!(
        version.status.success(),
        "{}",
        command_output_text(&version)
    );
    let version_text = String::from_utf8_lossy(&version.stdout);
    assert!(
        version_text.contains("product_id=botster-hub"),
        "{version_text}"
    );
    assert!(
        version_text.contains(&format!("version={}", env!("CARGO_PKG_VERSION"))),
        "{version_text}"
    );
    assert!(
        version_text.contains(&format!("build_revision={}", release.hub_revision)),
        "{version_text}"
    );

    // The receipt records both source identities separately.
    let receipt: serde_json::Value = serde_json::from_slice(
        &fs::read(prefix.join(".botster/installations/botster-hub.json")).expect("read receipt"),
    )
    .expect("parse receipt");
    assert_eq!(receipt["schema_version"], 2);
    assert_eq!(
        receipt["source_revisions"]["botster_hub"],
        release.hub_revision
    );
    assert_eq!(
        receipt["source_revisions"]["botster_core"],
        release.core_revision
    );
    assert_ne!(
        receipt["source_revisions"]["botster_hub"], receipt["source_revisions"]["botster_core"],
        "filesystem colocation does not collapse Hub and locked-Core provenance"
    );

    let data_dir = unique_short_test_dir("managed-install-data");
    let child = start_installed_daemon(&prefix, &data_dir, &entrypoint);
    let endpoint = botster_hub_client::DaemonEndpoint::new(data_dir.join("botster-hub.sock"));
    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("read installed daemon status")
        .status
        .expect("status payload");
    assert_eq!(
        status.installation.mode,
        botster_hub_client::DaemonInstallationMode::Managed
    );
    assert_eq!(status.installation.provenance, "managed_receipt");
    assert_eq!(
        status.installation.release_channel.as_deref(),
        Some("stable")
    );
    assert_eq!(status.installation.provider.as_deref(), Some("http_json"));
    assert!(status.installation.diagnostics.is_empty());
    assert_eq!(status.software.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        status.software.build_revision.as_deref(),
        Some(release.hub_revision.as_str()),
        "software identity comes from the binary, not from the receipt"
    );

    // Receipt-private data must not leak through the status DTO.
    let serialized = serde_json::to_string(&status).expect("serialize status");
    for leak in [
        "source_url",
        "signed_manifest_sha256",
        "sha256",
        "installer",
        "key_id",
        "source_revisions",
    ] {
        assert!(
            !serialized.contains(leak),
            "status leaked {leak}: {serialized}"
        );
    }
    assert!(!serialized.contains(&prefix.display().to_string()));

    // check-update against the managed source: schema 2, then a schema-3
    // document carrying unknown future fields. Both must answer the same way.
    for schema in [2, 3] {
        origin.serve(
            "/botster-hub.json",
            serde_json::to_vec(&serde_json::json!({
                "schema_version": schema,
                "product_id": "botster-hub",
                "release_channel": "stable",
                "version": "99.0.0",
                "build_revision": "0123456789abcdef0123456789abcdef01234567",
                "install_manifest": "e30=",
                "signature": {"algorithm": "ed25519", "key_id": "k", "value": "sig"},
                "delta_updates": {"from": ["0.1.0"]},
                "platform_matrix": ["aarch64-apple-darwin"]
            }))
            .expect("serialize forward-compatible document"),
        );
        let update = Command::new(&entrypoint)
            .arg("check-update")
            .arg("--data-dir")
            .arg(&data_dir)
            .env("HOME", &prefix)
            .output()
            .expect("run check-update through the installed Hub");
        assert!(update.status.success(), "{}", command_output_text(&update));
        let text = String::from_utf8_lossy(&update.stdout);
        assert!(text.contains("state=available"), "schema={schema}: {text}");
        assert!(
            text.contains("action=run_managed_installer"),
            "schema={schema}: {text}"
        );
    }

    shutdown_cli_daemon(&data_dir, child);

    // Restart preserves identical software and installation identity.
    let restarted = start_installed_daemon(&prefix, &data_dir, &entrypoint);
    let restarted_status =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("read restarted installed daemon status")
            .status
            .expect("status payload");
    assert_eq!(restarted_status.software, status.software);
    assert_eq!(restarted_status.installation, status.installation);
    shutdown_cli_daemon(&data_dir, restarted);

    let _ = fs::remove_dir_all(&data_dir);
    let _ = fs::remove_dir_all(&prefix);
}

/// Offline enforcement, proven with real daemons.
///
/// A socket probe cannot deliver this: the Hub accepts an arbitrary data
/// directory, so a daemon launched from the same installation under a *different*
/// data directory would stay invisible to a probe. The lease is data-directory
/// independent by construction, and this is where that is tested directly.
#[test]
fn real_daemons_on_custom_data_directories_hold_the_installation_lease() {
    let _guard = daemon_test_guard();
    let origin = ManagedReleaseOrigin::start();
    let (prefix, release) = install_real_release("managed-lease", &origin);
    let entrypoint = prefix.join("bin/botster-hub");
    let generation_hub = prefix
        .join("generations")
        .join(format!(
            "{}-{}",
            release.hub_revision, release.core_revision
        ))
        .join("botster-hub");

    let reinstall = || {
        Command::new(installer_binary())
            .arg("install")
            .arg("--prefix")
            .arg(&prefix)
            .arg("--source")
            .arg(origin.url("/botster-hub.json"))
            .arg("--trust-anchor")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "fixtures/release-signing/UNTRUSTED-TEST-ONLY-botster-hub-release-signing.pub",
            ))
            .env("HOME", &prefix)
            .output()
            .expect("run the managed installer")
    };

    // Two daemons, two *different* custom data directories, launched through
    // two different paths — `bin/botster-hub` and the generation path directly.
    // Prefix derivation matches layout shape rather than counting levels, so
    // both resolve the same prefix and contend for the same lease.
    let first_dir = unique_short_test_dir("managed-lease-a");
    let second_dir = unique_short_test_dir("managed-lease-b");
    let first = start_installed_daemon(&prefix, &first_dir, &entrypoint);
    let second = start_installed_daemon(&prefix, &second_dir, &generation_hub);

    let refused = reinstall();
    assert!(
        !refused.status.success(),
        "{}",
        command_output_text(&refused)
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("installation_busy"),
        "{}",
        command_output_text(&refused)
    );

    shutdown_cli_daemon(&first_dir, first);
    let still_refused = reinstall();
    assert!(
        !still_refused.status.success(),
        "the installer stays refused until every daemon exits: {}",
        command_output_text(&still_refused)
    );

    // `flock` releases on process death, including SIGKILL. A crashed daemon
    // must never leave an installation permanently unupgradeable.
    let killed_pid = second.id();
    signal_test_group_or_child(killed_pid, libc::SIGKILL).expect("kill the second daemon");
    let mut second = second;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if second.try_wait().expect("poll killed daemon").is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        second.try_wait().expect("poll killed daemon").is_some(),
        "the SIGKILLed daemon must be reaped"
    );

    let allowed = reinstall();
    assert!(
        allowed.status.success(),
        "a crashed daemon must not leave the installation unupgradeable: {}",
        command_output_text(&allowed)
    );

    // A daemon that fails to start because the lease is held says so rather
    // than hanging: acquisition is LOCK_SH|LOCK_NB.
    let installer_lease = botster_hub_installation::lease::acquire(
        &prefix,
        botster_hub_installation::LeaseMode::Exclusive,
    )
    .expect("take an installer-shaped exclusive lease");
    assert!(matches!(
        installer_lease,
        botster_hub_installation::LeaseOutcome::Acquired(_)
    ));
    let blocked_dir = unique_short_test_dir("managed-lease-blocked");
    let blocked = Command::new(&entrypoint)
        .arg("start")
        .arg("--data-dir")
        .arg(&blocked_dir)
        .env("HOME", &prefix)
        .output()
        .expect("attempt a daemon start while the installer holds the lease");
    assert!(
        !blocked.status.success(),
        "{}",
        command_output_text(&blocked)
    );
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("being upgraded"),
        "{}",
        command_output_text(&blocked)
    );

    for directory in [&first_dir, &second_dir, &blocked_dir] {
        let _ = fs::remove_dir_all(directory);
    }
    let _ = fs::remove_dir_all(&prefix);
}

#[test]
fn isolated_hub_shutdown_reaps_live_session_workers() {
    let _guard = daemon_test_guard();
    botster_hub_test_support::clear_isolated_hub_taint();
    let hub = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(unique_short_test_dir("ih-reap"))
            .name("owned-worker-reap"),
    );
    let spawn = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "isolated-reap-session".to_string(),
            command: "sleep 120".to_string(),
        },
    )
    .expect("spawn durable session through IsolatedHub");
    assert_eq!(
        spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "spawn must succeed: {spawn:?}"
    );
    let started = Instant::now();
    while Instant::now() < started + Duration::from_secs(5)
        && hub.owned_session_worker_pids().is_empty()
    {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !hub.owned_session_worker_pids().is_empty(),
        "positive control: IsolatedHub census must observe the live session worker before shutdown"
    );
    let hub_pid = hub.hub_child_pid();
    hub.shutdown()
        .expect("IsolatedHub shutdown after spawning a durable session");
    let leftover: Vec<u32> = Command::new("ps")
        .args(["-axo", "pid=,pgid=,command="])
        .output()
        .expect("census after IsolatedHub shutdown")
        .stdout
        .split(|&byte| byte == b'\n')
        .filter_map(|line| {
            let line = String::from_utf8_lossy(line);
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse().ok()?;
            let pgid: u32 = parts.next()?.parse().ok()?;
            let command = parts.collect::<Vec<_>>().join(" ");
            let basename = Path::new(command.split_whitespace().next()?)
                .file_name()
                .and_then(|name| name.to_str())?;
            (pgid == hub_pid && basename == "botster-session-worker").then_some(pid)
        })
        .collect();
    assert!(
        leftover.is_empty(),
        "IsolatedHub shutdown must reap owned session workers, leftover={leftover:?}"
    );
}

