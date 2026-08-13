#[test]
fn operator_console_output_wait_reports_early_child_exit() {
    let fixture_dir = unique_short_test_dir("console-child-exit");
    fs::create_dir_all(&fixture_dir).expect("create early-exit console fixture directory");
    let fixture = fixture_dir.join("early-exit-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'console-started\\n'\nexit 23\n",
    )
    .expect("write early-exit console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read early-exit console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions).expect("make early-exit console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("console-started");
    let error = console
        .try_wait_for_occurrences("output-that-will-never-arrive", 1)
        .expect_err("exited console should fail an unrelated output wait");
    assert!(error.contains("child exited before condition"), "{error}");
    assert!(error.contains("code: 23"), "{error}");
    assert!(
        !error.contains("condition not met after"),
        "child-exit detection must precede the hang backstop: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir).expect("remove early-exit console fixture directory");
}

#[test]
fn operator_console_output_checkpoint_reports_early_child_exit() {
    let fixture_dir = unique_short_test_dir("console-checkpoint-child-exit");
    fs::create_dir_all(&fixture_dir)
        .expect("create checkpoint early-exit console fixture directory");
    let fixture = fixture_dir.join("checkpoint-early-exit-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'console-started\\n'\nexit 23\n",
    )
    .expect("write checkpoint early-exit console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read checkpoint early-exit console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions)
        .expect("make checkpoint early-exit console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("console-started");
    let checkpoint = console.output_checkpoint();
    let error = console
        .try_wait_for_output_after(
            checkpoint,
            "output-that-will-never-arrive",
            OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP,
        )
        .expect_err("exited console should fail a post-checkpoint output wait");
    assert!(error.contains("console exited after"), "{error}");
    assert!(error.contains("code: 23"), "{error}");
    assert!(
        !error.contains("no post-action progress"),
        "child-exit detection must precede the post-action backstop: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir)
        .expect("remove checkpoint early-exit console fixture directory");
}

#[test]
fn operator_console_output_checkpoint_rejects_stale_identical_output() {
    let fixture_dir = unique_short_test_dir("console-output-checkpoint");
    fs::create_dir_all(&fixture_dir).expect("create output-checkpoint fixture directory");
    let fixture = fixture_dir.join("checkpoint-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'repeated-output\\n'; sleep 60\n",
    )
    .expect("write output-checkpoint console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read output-checkpoint console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions)
        .expect("make output-checkpoint console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("repeated-output");
    let checkpoint = console.output_checkpoint();
    assert!(
        !console.output_contains_after(checkpoint, b"repeated-output"),
        "output from before the checkpoint satisfied a post-action observation"
    );
    let error = console
        .try_wait_for_output_after(checkpoint, "repeated-output", Duration::from_millis(100))
        .expect_err("stale identical output must not satisfy a post-checkpoint wait");
    assert!(error.contains("no post-action progress"), "{error}");
    assert!(
        !console.output_contains_after(checkpoint, b"repeated-output"),
        "output from before the checkpoint appeared in the post-checkpoint suffix: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir).expect("remove output-checkpoint fixture directory");
}

#[test]
fn owned_operator_console_cleanup_checks_pid_identity_and_runtime_artifacts() {
    let reused_pid_data_dir = unique_short_test_dir("console-reused-pid");
    let mut reused_pid_cleanup = OwnedOperatorConsoleDaemon::new(&reused_pid_data_dir);
    reused_pid_cleanup.record_owned_pid(std::process::id());
    reused_pid_cleanup.assert_cleaned();

    let stale_artifact_data_dir = unique_short_test_dir("console-stale-artifact");
    let mut stale_artifact_cleanup = OwnedOperatorConsoleDaemon::new(&stale_artifact_data_dir);
    fs::create_dir_all(&stale_artifact_data_dir).expect("create stale-artifact data directory");
    let metadata_path = stale_artifact_data_dir.join(".botster-hub-runtime-daemon.json");
    fs::write(&metadata_path, b"not owned daemon metadata")
        .expect("write unverified daemon metadata");
    let error = stale_artifact_cleanup
        .cleanup()
        .expect_err("cleanup must not unlink unverified runtime artifacts");
    assert!(
        error.contains("unverified runtime metadata remains"),
        "{error}"
    );
    assert!(
        metadata_path.exists(),
        "cleanup oracle removed the artifact it was supposed to verify"
    );
    stale_artifact_cleanup.armed = false;
    fs::remove_dir_all(&stale_artifact_data_dir).expect("remove stale-artifact data directory");
}

#[test]
fn operator_console_readiness_backstop_outlives_policy_and_reports_context() {
    assert!(
        OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP > LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
        "the harness liveness backstop must not preempt production readiness policy"
    );

    let fixture_dir = unique_short_test_dir("console-readiness-backstop");
    fs::create_dir_all(&fixture_dir).expect("create readiness-backstop fixture directory");
    let fixture = fixture_dir.join("wedged-console");
    fs::write(&fixture, "#!/bin/sh\nexec sleep 60\n").expect("write readiness-backstop fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read readiness-backstop fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions).expect("make readiness-backstop fixture executable");

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&fixture_dir);
    let diagnostic_pid = std::process::id();
    daemon_cleanup.record_owned_pid(diagnostic_pid);
    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    let error = daemon_cleanup
        .try_wait_until_daemon_ready_with_backstop(&mut console, Duration::from_millis(100))
        .expect_err("wedged console should hit the harness liveness backstop");
    assert!(error.contains("condition not met after"), "{error}");
    assert!(error.contains("last_status="), "{error}");
    assert!(
        error.contains(&format!("owned_daemon_pids=[{diagnostic_pid}]")),
        "{error}"
    );
    assert!(error.contains("metadata_exists=false"), "{error}");
    assert!(error.contains("socket_exists=false"), "{error}");
    assert!(
        error.contains("reader_status=\"reader reached EOF\""),
        "{error}"
    );
    console.wait_for_exit();
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&fixture_dir).expect("remove readiness-backstop fixture directory");
}

#[test]
fn operator_console_detach_releases_reader_while_daemon_stays_running() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-detach-reader");
    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut console);
    let daemon_pid = *daemon_cleanup
        .owned_pids()
        .first()
        .expect("capture detached daemon pid");
    assert_detached_daemon_stdin(daemon_pid);
    console.wait_for("botster-hub> ");
    console.send(&[4]);
    console.wait_for("detached=daemon_running");
    console.wait_for_exit();
    assert!(
        console.reader.is_none(),
        "console exit did not join its PTY reader"
    );

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe daemon after detached console reader EOF");
    assert!(
        status.status.success(),
        "daemon did not remain running after console reader EOF: {}",
        command_output_text(&status)
    );
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove detached-reader console data directory");
}

#[test]
fn operator_console_ctrl_c_reaches_foreground_app_process_group_and_returns_prompt() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-foreground-interrupt");
    let package_dir =
        unique_short_test_dir("console-foreground-interrupt-package").join("package with spaces");
    write_botster_tui_package_with_script(&package_dir, DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT);

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut console);
    console.wait_for("botster-hub> ");
    console.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    console.send_and_wait_for_prompt(b"packages enable botster-tui\n");

    let prompt_after_interrupt = console.prompt_count() + 1;
    console.send(b"apps open botster-tui\n");
    console.wait_for("foreground-forward-ready");
    let foreground_interrupt_checkpoint = console.output_checkpoint();
    console.send(&[3]);
    console.wait_for_output_after(
        foreground_interrupt_checkpoint,
        "foreground app exited with code 130",
    );
    console.wait_for_output_after(foreground_interrupt_checkpoint, "botster-hub> ");
    assert_eq!(
        console.prompt_count(),
        prompt_after_interrupt,
        "foreground interrupt printed an unexpected number of prompts: {}",
        console.text()
    );
    assert!(
        !console
            .text()
            .contains("interrupt requested; finishing safely"),
        "foreground Ctrl-C was handled as inline console work: {}",
        console.text()
    );

    console.send(b"shutdown\n");
    console.wait_for_exit();
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove foreground-interrupt data directory");
    fs::remove_dir_all(
        package_dir
            .parent()
            .expect("foreground-interrupt package parent"),
    )
    .expect("remove foreground-interrupt package directory");
}

#[test]
fn process_ownership_operator_console_readiness_failure_reaps_console_and_owned_daemon() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-readiness-failure");
    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn_with_env(
        &data_dir,
        &[(TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV, "1")],
    );
    let error = console
        .try_wait_for_occurrences("botster-hub> ", 1)
        .expect_err("injected daemon readiness failure should stop console startup");
    console.wait_for_exit();
    let output = console.text();
    let daemon_pid = output
        .split("terminated owned child_pid=")
        .nth(1)
        .and_then(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u32>()
                .ok()
        })
        .expect("production diagnostic includes the terminated owned daemon pid");
    daemon_cleanup.record_owned_pid(daemon_pid);

    assert!(error.contains("child exited before condition"), "{error}");
    assert!(
        output.contains("timed out waiting for local runtime daemon readiness"),
        "{output}"
    );
    assert!(
        output.contains("(budget 1ms)"),
        "the injected production readiness budget was not observed: {output}"
    );
    assert!(
        output.contains("terminated owned child_pid="),
        "production failure diagnostic omitted terminated daemon evidence: {output}"
    );
    daemon_cleanup.assert_cleaned();
    assert!(
        !process_exists(daemon_pid),
        "induced readiness failure left exact daemon pid {daemon_pid} alive"
    );
    fs::remove_dir_all(&data_dir).expect("remove readiness-failure console data directory");
}

#[test]
fn operator_console_panic_reaps_console_and_owned_daemon() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-panic-cleanup");
    let observed_pids = Arc::new(Mutex::new((None, None)));
    let unwind_pids = Arc::clone(&observed_pids);
    let unwind_data_dir = data_dir.clone();

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&unwind_data_dir);
        let mut console = OperatorConsolePty::spawn(&unwind_data_dir);
        daemon_cleanup.wait_until_daemon_ready(&mut console);
        let console_pid = console
            .child
            .process_id()
            .expect("operator console fixture exposes a process id");
        let daemon_pid = *daemon_cleanup
            .owned_pids()
            .first()
            .expect("capture panic-test owned daemon pid");
        *unwind_pids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            (Some(console_pid), Some(daemon_pid));
        panic!("induced operator console panic");
    }));
    assert!(unwind.is_err(), "panic fixture should unwind");

    let (console_pid, daemon_pid) = *observed_pids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let console_pid = console_pid.expect("recorded panic-test console pid");
    let daemon_pid = daemon_pid.expect("recorded panic-test daemon pid");
    wait_for_owned_pid_exit(console_pid, Duration::from_secs(2));
    wait_for_owned_pid_exit(daemon_pid, Duration::from_secs(2));
    assert!(
        !process_exists(console_pid),
        "panic left operator console pid {console_pid} alive"
    );
    assert!(
        !process_exists(daemon_pid),
        "panic left owned daemon pid {daemon_pid} alive"
    );
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe panic-cleaned daemon");
    assert!(
        !status.status.success(),
        "panic cleanup left typed daemon status running: {}",
        command_output_text(&status)
    );
    assert!(
        !data_dir.join(".botster-hub-runtime-daemon.json").exists(),
        "panic cleanup left daemon metadata"
    );
    assert!(
        !explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("panic-test local socket binding")
            .path
            .exists(),
        "panic cleanup left daemon socket"
    );
    fs::remove_dir_all(&data_dir).expect("remove panic-cleanup console data directory");
}

#[test]
fn cli_shutdown_reaps_metadata_owned_daemon_started_by_live_operator_console() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("external-shutdown-live-console");
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .expect("local socket binding")
        .path;
    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut console);
    console.wait_for("daemon=started");
    console.wait_for("botster-hub> ");
    let daemon_pid = *daemon_cleanup
        .owned_pids()
        .last()
        .expect("operator console started daemon pid");

    let shutdown_started_at = Instant::now();
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run external shutdown while starting console remains live");
    assert!(
        shutdown.status.success(),
        "external shutdown failed while daemon parent console remained live after {:?}: {}",
        shutdown_started_at.elapsed(),
        command_output_text(&shutdown)
    );
    assert!(
        shutdown_started_at.elapsed() < Duration::from_secs(5),
        "external shutdown approached the ten-second timeout while daemon parent console remained live: {:?}",
        shutdown_started_at.elapsed()
    );
    assert!(
        console
            .child
            .try_wait()
            .expect("poll starting operator console after external shutdown")
            .is_none(),
        "starting operator console exited instead of remaining available to reap its daemon: {}",
        console.text()
    );
    assert!(
        !process_exists(daemon_pid),
        "external shutdown returned before console-reaped daemon pid {daemon_pid} disappeared"
    );
    assert!(
        !metadata_path.exists(),
        "external shutdown left owned runtime metadata"
    );
    assert!(
        !socket_path.exists(),
        "external shutdown left owned runtime socket"
    );

    console.send(&[4]);
    console.wait_for_exit();
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove external-shutdown console data directory");
}

#[test]
fn cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console");
    let package_dir = unique_short_test_dir("console-package").join("package with spaces");
    let web_package_dir = unique_short_test_dir("console-web-package").join("web package");
    write_botster_tui_package_with_script(
        &package_dir,
        "stty raw -echo; printf 'console-terminal-failure\\r\\n'; exit 7",
    );
    write_botster_web_package(&web_package_dir);
    let web_manifest_path = web_package_dir.join("botster-package.json");
    let mut web_manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&web_manifest_path).expect("read console botster-web manifest"),
    )
    .expect("parse console botster-web manifest");
    let delay = web_manifest["runnable_entrypoints"][0]["environment"]
        .as_array_mut()
        .expect("botster-web environment array")
        .iter_mut()
        .find(|value| {
            value.get("name").and_then(serde_json::Value::as_str)
                == Some("BOTSTER_WEB_TEST_STARTUP_DELAY_MS")
        })
        .expect("botster-web startup delay environment declaration");
    delay["default"] = serde_json::Value::String("1500".to_string());
    fs::write(
        &web_manifest_path,
        serde_json::to_vec_pretty(&web_manifest).expect("serialize delayed botster-web manifest"),
    )
    .expect("write delayed botster-web manifest");

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut first = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut first);
    first.wait_for("daemon=started");
    first.wait_for("prerequisite botster-web=missing");
    first.wait_for("botster-hub> ");
    first.send_and_wait_for_prompt(b"open tui\n");
    first.wait_for("botster-hub open error: app botster-tui is not installed or enabled");
    first.send_and_wait_for_prompt(b"open web\n");
    first
        .wait_for("botster-hub open error: app botster-web/web-client is not installed or enabled");
    first.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    first.wait_for("decision=package");
    first.send_and_wait_for_prompt(b"packages enable botster-tui\n");
    first.wait_for("state=enabled");
    first.send_and_wait_for_prompt(b"packages list\n");
    first.wait_for("response=packages");
    first.send_and_wait_for_prompt(b"packages show botster-tui\n");
    first.wait_for("package_name=botster-tui");
    first.send_and_wait_for_prompt(b"sessions spawn --session-id console-sentinel -- sleep 300\n");
    first.wait_for("session_id=console-sentinel");
    let mut sentinel_cleanup = SessionCleanupGuard::new(&data_dir, "console-sentinel");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(b"apps list\n");
    first.wait_for("response=apps");
    first.wait_for("kind=terminal_app");
    first.send_and_wait_for_prompt(b"open tui\n");
    first.wait_for("console-terminal-failure");
    first.wait_for("foreground app exited with code 7");
    first.send_and_wait_for_prompt(b"status\r");
    first.wait_for("event=status");
    let explicit_open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("run explicit foreground app after console handoff");
    assert_eq!(
        explicit_open.status.code(),
        Some(7),
        "explicit CLI did not preserve foreground app exit code: {}",
        command_output_text(&explicit_open)
    );
    write_botster_tui_package_with_script(
        &package_dir,
        "stty raw -echo; printf 'foreground-clean\\r\\n'; exit 0",
    );
    first.send_and_wait_for_prompt(b"packages reload botster-tui\n");
    first.wait_for("action=reload");
    first.send_and_wait_for_prompt(b"apps open botster-tui\n");
    first.wait_for("foreground-clean");
    first.send_and_wait_for_prompt(b"status\r");
    first.wait_for("event=status");
    write_botster_tui_package_with_script(&package_dir, DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT);
    first.send_and_wait_for_prompt(b"packages reload botster-tui\n");
    first.wait_for("action=reload");
    let prompt_after_foreground_interrupt = first.prompt_count() + 1;
    first.send(b"apps open botster-tui\n");
    first.wait_for("foreground-forward-ready");
    let foreground_interrupt_checkpoint = first.output_checkpoint();
    first.send(&[3]);
    first.wait_for_output_after(
        foreground_interrupt_checkpoint,
        "foreground app exited with code 130",
    );
    first.wait_for_output_after(foreground_interrupt_checkpoint, "botster-hub> ");
    assert_eq!(
        first.prompt_count(),
        prompt_after_foreground_interrupt,
        "foreground interrupt printed an unexpected number of prompts: {}",
        first.text()
    );
    assert!(
        !first
            .text()
            .contains("interrupt requested; finishing safely"),
        "foreground Ctrl-C was handled as inline console work: {}",
        first.text()
    );
    first.send_and_wait_for_prompt(b"sessions list\r");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&web_package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    first.wait_for_occurrences("package_name=botster-web", 1);
    first.send_and_wait_for_prompt(b"packages enable botster-web\n");
    first.wait_for_occurrences("package_name=botster-web", 2);
    let prompt_after_inline_interrupt = first.prompt_count() + 1;
    first.send(b"up\n");
    thread::sleep(Duration::from_millis(100));
    first.send(&[3]);
    first.wait_for("interrupt requested; finishing safely");
    first.wait_for("runtime=ready");
    first.wait_for_occurrences("botster-hub> ", prompt_after_inline_interrupt);
    first.send_and_wait_for_prompt(b"open web\n");
    first.wait_for("app_url=http://");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    let prompt_after_idle_interrupt = first.prompt_count() + 1;
    first.send(b"partial input");
    first.send(&[3]);
    first.wait_for("^C");
    first.wait_for_occurrences("botster-hub> ", prompt_after_idle_interrupt);
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(b"botster-hub status\n");
    first.wait_for("omit the repeated `botster-hub` prefix");
    first.send_and_wait_for_prompt(b"packages list \"unterminated\n");
    first.wait_for("console parse error");
    first.send_and_wait_for_prompt(b"status --data-dir /tmp/not-this-console\n");
    first.wait_for("this console is pinned to");
    first.send_and_wait_for_prompt(b"not-a-command\n");
    first.wait_for(
        format!(
            "run `botster-hub not-a-command --data-dir {}` outside the console",
            data_dir.display()
        )
        .as_str(),
    );
    first.send_and_wait_for_prompt(b"status\n");
    first.wait_for("event=status");
    first.send_and_wait_for_prompt(b"sessions shutdown console-sentinel\n");
    first.wait_for("response=events");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=exited");
    sentinel_cleanup.disarm();
    first.send(&[4]);
    first.wait_for("detached=daemon_running");
    first.wait_for_exit();

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console detach");
    assert!(
        status.status.success(),
        "detached console stopped daemon: {}",
        command_output_text(&status)
    );

    let mut exit_console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut exit_console);
    exit_console.wait_for("daemon=reused");
    exit_console.wait_for("botster-hub> ");
    exit_console.send(b"exit\n");
    exit_console.wait_for("detached=daemon_running");
    exit_console.wait_for_exit();

    let mut second = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut second);
    let shutdown_daemon_pid = *daemon_cleanup
        .owned_pids()
        .last()
        .expect("capture daemon generation before console shutdown");
    second.wait_for("daemon=reused");
    second.wait_for("botster-hub> ");
    second.send(b"shutdown\n");
    second.wait_for("response=shutdown");
    second.wait_for_exit();
    assert!(
        !process_exists(shutdown_daemon_pid),
        "console shutdown returned before owned daemon pid {shutdown_daemon_pid} exited"
    );
    assert!(
        !data_dir.join(".botster-hub-runtime-daemon.json").exists(),
        "console shutdown left owned daemon metadata"
    );
    assert!(
        !explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .exists(),
        "console shutdown left the owned daemon socket"
    );

    let stopped = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console down");
    assert!(
        !stopped.status.success(),
        "console shutdown left daemon running: {}",
        command_output_text(&stopped)
    );

    let mut third = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut third);
    third.wait_for("daemon=started");
    third.wait_for("botster-hub> ");
    third.send(b"down\n");
    third.wait_for("response=shutdown");
    third.wait_for_exit();
    let stopped = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console down");
    assert!(
        !stopped.status.success(),
        "console down left daemon running: {}",
        command_output_text(&stopped)
    );
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove isolated operator console data directory");
    fs::remove_dir_all(
        package_dir
            .parent()
            .expect("operator console package has a parent"),
    )
    .expect("remove isolated operator console package directory");
    fs::remove_dir_all(
        web_package_dir
            .parent()
            .expect("operator console web package has a parent"),
    )
    .expect("remove isolated operator console web package directory");
}

#[test]
fn cli_operator_console_reuses_before_worker_lookup_and_reports_missing_worker() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-worker-reuse");
    let child = start_cli_daemon(&data_dir);
    let isolated_bin_dir = unique_short_test_dir("console-bin");
    fs::create_dir_all(&isolated_bin_dir).expect("create isolated console binary directory");
    let isolated_hub = isolated_bin_dir.join("botster-hub");
    fs::copy(env!("CARGO_BIN_EXE_botster-hub"), &isolated_hub)
        .expect("copy hub without its worker sibling");

    let mut reused = OperatorConsolePty::spawn_binary(&isolated_hub, &data_dir);
    reused.wait_for("daemon=reused");
    assert!(
        !reused.text().contains("missing botster-session-worker"),
        "reused daemon unexpectedly required a local worker: {}",
        reused.text()
    );
    reused.send(b"exit\n");
    reused.wait_for("detached=daemon_running");
    reused.wait_for_exit();
    shutdown_cli_daemon(&data_dir, child);

    let fresh_data_dir = unique_short_test_dir("console-worker-missing");
    let mut missing = OperatorConsolePty::spawn_binary(&isolated_hub, &fresh_data_dir);
    missing.wait_for("missing botster-session-worker binary");
    missing.wait_for("Install the complete Botster distribution");
    missing.wait_for("cargo build --locked -p botster-core-daemon --bin botster-session-worker");
    missing.wait_for_exit();
    assert!(
        !fresh_data_dir
            .join(".botster-hub-runtime-daemon.json")
            .exists(),
        "missing-worker startup wrote runtime metadata"
    );
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&fresh_data_dir)
        .output()
        .expect("probe missing-worker console runtime");
    assert!(
        !status.status.success(),
        "missing-worker console started a daemon: {}",
        command_output_text(&status)
    );

    fs::remove_dir_all(&data_dir).expect("remove reused console runtime directory");
    fs::remove_dir_all(&fresh_data_dir).expect("remove missing-worker console runtime directory");
    fs::remove_dir_all(&isolated_bin_dir).expect("remove isolated console binary directory");
}

