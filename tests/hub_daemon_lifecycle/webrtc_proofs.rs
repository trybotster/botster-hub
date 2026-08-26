#[test]
fn botster_web_health_rejects_stale_daemon_socket_file() {
    let data_dir = unique_short_test_dir("web-health-stale-socket");
    let package_dir = unique_test_dir("web-health-stale-socket-package");
    fs::create_dir_all(&data_dir).expect("create stale socket data directory");
    write_botster_web_package(&package_dir);
    let socket_path = data_dir.join("hub.sock");
    let stale_listener = UnixListener::bind(&socket_path).expect("bind stale daemon socket");
    drop(stale_listener);
    assert!(
        socket_path.exists(),
        "stale daemon socket file should remain"
    );

    let connection = serde_json::json!({
        "transport": {
            "type": "unix_socket",
            "path": socket_path
        }
    });
    let mut command = Command::new("node");
    command
        .arg("scripts/local-package-server.mjs")
        .current_dir(&package_dir)
        .env("BOTSTER_HUB_CONNECTION", connection.to_string())
        .env("BOTSTER_HUB_DATA_DIR", &data_dir)
        .env("BOTSTER_WEB_PORT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let child = command
        .spawn()
        .expect("spawn botster-web package server against stale socket");
    let mut child = ChildCleanup { child };
    let mut listening = String::new();
    BufReader::new(
        child
            .child
            .stdout
            .take()
            .expect("botster-web package server stdout"),
    )
    .read_line(&mut listening)
    .expect("read botster-web listening marker");
    let origin = listening
        .trim()
        .strip_prefix("web_listening=")
        .expect("botster-web listening marker");
    assert!(
        origin.starts_with("http://127.0.0.1:"),
        "unexpected listening marker: {listening}"
    );

    let health = read_json_health(origin);
    assert_eq!(health["ok"], false, "stale socket health: {health}");
    assert_eq!(health["socketExists"], true);
    assert_eq!(health["daemonReady"], false);
    assert!(
        health["error"]
            .as_str()
            .is_some_and(|error| error.contains("ECONNREFUSED")),
        "stale socket health should report protocol failure: {health}"
    );
}

#[test]
fn local_webrtc_sender_terminal_record_rejects_stale_malformed_and_oversized_evidence() {
    let data_dir = unique_test_dir("local-webrtc-terminal-record-validation");
    fs::create_dir_all(&data_dir).expect("create terminal record validation directory");
    let path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    let valid_record = serde_json::json!({
        "schema_version": 1,
        "grant_id": "grant-current",
        "request_operation": "status",
        "message_id": null,
        "next_chunk_index": 0,
        "last_sent_chunk_index": null,
        "total_chunks": 0,
        "pressured": false,
        "peer_connection_state": "closed",
        "channel_terminal_signal": "on_close",
        "cause": "channel_closed",
        "cleanup_disposition": "newly_sent",
    });

    fs::write(
        &path,
        serde_json::to_vec(&valid_record).expect("serialize validation fixture"),
    )
    .expect("write stale validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-other")
        })
        .is_err(),
        "a record for another grant must not satisfy the evidence gate"
    );

    fs::write(&path, b"{\"schema_version\":1").expect("write truncated validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-current")
        })
        .is_err(),
        "a truncated record must not satisfy the evidence gate"
    );

    fs::write(
        &path,
        vec![b'x'; LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES + 1],
    )
    .expect("write oversized validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-current")
        })
        .is_err(),
        "an oversized record must not satisfy the evidence gate"
    );
}

#[test]
fn local_webrtc_diagnostic_stderr_tail_is_bounded_and_redacts_paths() {
    let data_dir = std::env::temp_dir().join("local-webrtc-diagnostic-data");
    let mut lines = (0..25)
        .map(|index| format!("diagnostic line {index}"))
        .collect::<Vec<_>>();
    lines[23] = "x".repeat(600);
    lines[24] = format!(
        "data={} workspace={} home={} temp={}",
        data_dir.display(),
        env!("CARGO_MANIFEST_DIR"),
        std::env::var("HOME").unwrap_or_default(),
        std::env::temp_dir().display()
    );

    let tail = local_webrtc_bounded_stderr_tail(lines.join("\n").as_bytes(), &data_dir);

    assert!(!tail.contains("diagnostic line 4"));
    assert!(tail.contains("diagnostic line 5"));
    assert!(tail.contains("<truncated>"));
    assert!(tail.contains("<data-dir>"));
    assert!(tail.contains("<workspace>"));
    assert!(tail.contains("<home>"));
    assert!(tail.contains("<temp>"));
    assert!(!tail.contains(&data_dir.display().to_string()));
    assert!(!tail.contains(env!("CARGO_MANIFEST_DIR")));
}

#[test]
fn cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-success");
    let project_pipelines_package_dir = unique_test_dir("cli-smoke-project-pipelines");
    let web_package_dir = unique_test_dir("cli-smoke-web");
    let tui_package_dir = unique_test_dir("cli-smoke-tui");
    let workspaces_package_dir = unique_test_dir("cli-smoke-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let output = run_local_runtime_smoke(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        0,
    );
    let text = command_output_text(&output);
    assert_smoke_owned_daemon_gone(&data_dir);
    if !output.status.success() {
        panic!(
            "{}",
            local_webrtc_smoke_failure_evidence(&output, &data_dir)
        );
    }
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(text.contains("check name=daemon status=pass"));
    assert!(text.contains("check name=core status=pass"));
    assert!(text.contains("check name=packages status=pass"));
    assert!(text.contains("check name=apps status=pass"));
    assert!(text.contains("check name=session_terminal status=pass"));
    assert!(text.contains("check name=webrtc status=pass"));
    assert!(text.contains("smoke_result=pass"));
}

#[test]
fn cli_smoke_persists_matching_sender_record_when_webrtc_response_closes() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-webrtc-close");
    let project_pipelines_package_dir = unique_test_dir("cli-smoke-close-project-pipelines");
    let web_package_dir = unique_test_dir("cli-smoke-close-web");
    let tui_package_dir = unique_test_dir("cli-smoke-close-tui");
    let workspaces_package_dir = unique_test_dir("cli-smoke-close-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let output = run_local_runtime_smoke_with_fault(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        0,
        Some("status"),
    );
    let text = command_output_text(&output);
    assert!(
        !output.status.success(),
        "faulted smoke unexpectedly passed: {text}"
    );
    assert!(text.contains(
        "local_webrtc=local WebRTC response incomplete: operation=status cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending"
    ));
    let grant_id =
        local_webrtc_grant_id(&output).expect("faulted smoke reached local WebRTC bootstrap");
    let terminal_record = local_webrtc_sender_terminal_record(&data_dir, &grant_id);
    assert_eq!(terminal_record["request_operation"], "status");
    assert_eq!(terminal_record["next_chunk_index"], 0);
    assert_eq!(terminal_record["total_chunks"], 0);
    assert!(
        matches!(
            terminal_record["cause"].as_str(),
            Some(
                "channel_closed"
                    | "poll_ended"
                    | "peer_disconnected"
                    | "peer_failed"
                    | "peer_closed"
            )
        ),
        "faulted smoke must retain a usable sender terminal cause: {terminal_record}"
    );
    assert_smoke_owned_daemon_gone(&data_dir);
}

#[test]
fn cli_smoke_reports_missing_first_party_prerequisites() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-missing");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("smoke")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub smoke without package prerequisites");
    assert!(
        !output.status.success(),
        "smoke unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains("missing_prerequisite=botster-web"));
    let failure = local_webrtc_smoke_failure_evidence(&output, &data_dir);
    assert!(failure.contains("smoke failed before local WebRTC bootstrap"));
    assert!(failure.contains("missing_prerequisite=botster-web"));
}

#[test]
fn external_hub_webrtc_live_output_preserves_exact_bytes() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0xff, 0xc0];
    let hub = start_isolated_live_output_hub("webrtc-exact-bytes");
    let package_dir = unique_test_dir("webrtc-exact-bytes-web");
    write_botster_web_package(&package_dir);
    enable_supervised_package(hub.data_dir(), &package_dir);
    let endpoint = hub.endpoint().clone();
    let (_web_origin, bootstrap) = start_botster_web_and_issue_bootstrap(&endpoint);
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

    let mut session_cleanup = None;
    block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");
        let signal = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )
        .expect("signal local WebRTC offer");
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .expect("signal response includes WebRTC answer")
            .answer
            .clone();
        offer_peer
            .accept_answer(answer)
            .await
            .expect("offer peer accepts answer");
        offer_peer.grant_secret = Some(bootstrap.grant_secret.clone());

        let release_path = unique_short_test_dir("webrtc-exact-release").join("go");
        let script_path = write_python_wait_then_write_script(&release_path, expected);
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: "webrtc-exact-bytes-session".to_string(),
                command: python_script_command(&script_path),
            },
        )
        .expect("spawn write(2) producer");
        session_cleanup = Some(SessionCleanupGuard::new(
            hub.data_dir(),
            "webrtc-exact-bytes-session",
        ));
        offer_peer
            .encrypted_hello(
                &stream_key,
                &botster_hub_client::DaemonHello {
                    protocol: botster_hub_client::PROTOCOL.to_string(),
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
                    terminal_compatibility: None,
                },
            )
            .await
            .expect("webrtc adapter hello");
        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "webrtc-exact-bytes-session".to_string(),
                    subscription_id: "webrtc-exact-bytes-sub".to_string(),
                },
            )
            .await
            .expect("attach over encrypted WebRTC");
        assert!(
            attach.events.is_empty(),
            "WebRTC Attach must not return terminal bodies: {:?}",
            attach.events
        );
        offer_peer
            .bind_reserved_from_attach(&attach)
            .await
            .expect("browser creates the reserved terminal DataChannel");
        fs::create_dir_all(release_path.parent().expect("release parent"))
            .expect("create webrtc release dir");
        fs::write(&release_path, b"go").expect("release webrtc write(2) producer");

        let mut concatenated = Vec::new();
        for _ in 0..120 {
            let _ = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::ReadScreen {
                        session_id: "webrtc-exact-bytes-session".to_string(),
                    },
                )
                .await;
            if let Ok(Ok(bytes)) = timeout(
                Duration::from_millis(250),
                offer_peer.next_terminal_frame(&stream_key),
            )
            .await
            {
                if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                {
                    if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event {
                        let decoded = live_output_decoded_bytes(payload);
                        assert!(
                            !payload_has_utf8_replacement(&decoded),
                            "WebRTC live payload must not contain U+FFFD: {decoded:?}"
                        );
                        concatenated.extend(decoded);
                    }
                } else {
                    assert!(
                        !payload_has_utf8_replacement(&bytes),
                        "WebRTC live payload must not contain U+FFFD: {bytes:?}"
                    );
                    concatenated.extend(bytes);
                }
            }
            if concatenated
                .windows(expected.len())
                .any(|window| window == expected)
            {
                break;
            }
        }
        assert!(
            concatenated
                .windows(expected.len())
                .any(|window| window == expected),
            "encrypted WebRTC adapter frames must preserve exact live bytes, got {concatenated:?}"
        );
        let _ = offer_peer.peer.close().await;
    });

    let mut session_cleanup = session_cleanup.expect("armed after Spawn");
    production_cleanup_after_authoritative_exit(
        &endpoint,
        "webrtc-exact-bytes-session",
        "WebRTC exact-bytes after observed exit",
    );
    session_cleanup.disarm();

    let missing = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "webrtc-exact-bytes-missing".to_string(),
        },
    )
    .expect("shutdown never-spawned session");
    assert_eq!(
        missing.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "unknown session must return OperatorError, got kind={:?} error={:?}",
        missing.kind,
        missing.error
    );
    let error = missing.error.as_ref().expect("unknown_session body");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "shutdown");
    let data_dir = hub.data_dir().clone();
    hub.shutdown().expect("shutdown isolated hub");
    reap_session_workers_for_data_dir(&data_dir)
        .expect("exact-bytes hub shutdown must not leave worktree session workers");
}

#[test]
fn external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0xff, 0xc0];
    let hub = start_isolated_live_output_hub("webrtc-sd-exit");
    let package_dir = unique_test_dir("webrtc-sd-exit-web");
    write_botster_web_package(&package_dir);
    enable_supervised_package(hub.data_dir(), &package_dir);
    let endpoint = hub.endpoint().clone();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                "0".to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_daemon_response_ok(
        &start,
        botster_hub_client::DaemonResponseKind::Packages,
        "start botster-web entrypoint",
    );
    let web_origin = wait_for_published_web_origin(&endpoint);

    for round in 0..5 {
        let session_id = format!("webrtc-sd-exit-{round}");
        let subscription_id = format!("webrtc-sd-exit-sub-{round}");
        let bootstrap = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
                package_name: "botster-web".to_string(),
                entrypoint_id: "web-client".to_string(),
                origin: web_origin.clone(),
            },
        )
        .expect("issue local WebRTC bootstrap")
        .local_webrtc_bootstrap
        .expect("bootstrap response includes local WebRTC bootstrap");
        let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

        let mut session_cleanup = None;
        block_on(async {
            let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
                .await
                .expect("create WebRTC offer peer");
            let signal = botster_hub_client::request(
                &endpoint,
                botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                    grant_id: bootstrap.grant_id.clone(),
                    grant_secret: bootstrap.grant_secret.clone(),
                    origin: bootstrap.expected_origin.clone(),
                    offer,
                },
            )
            .expect("signal local WebRTC offer");
            let answer = signal
                .local_webrtc_answer
                .as_ref()
                .expect("signal response includes WebRTC answer")
                .answer
                .clone();
            offer_peer
                .accept_answer(answer)
                .await
                .expect("offer peer accepts answer");
            offer_peer.grant_secret = Some(bootstrap.grant_secret.clone());

            let release_path = unique_short_test_dir(&format!("webrtc-sd-rel-{round}")).join("go");
            let script_path = write_python_wait_then_write_script(&release_path, expected);
            botster_hub_client::request(
                &endpoint,
                botster_hub_client::DaemonRequest::Spawn {
                    session_id: session_id.clone(),
                    command: python_script_command(&script_path),
                },
            )
            .expect("spawn write(2) producer that exits after output");
            session_cleanup = Some(SessionCleanupGuard::new(hub.data_dir(), session_id.clone()));
            offer_peer
                .encrypted_hello(
                    &stream_key,
                    &botster_hub_client::DaemonHello {
                        protocol: botster_hub_client::PROTOCOL.to_string(),
                        compatibility:
                            botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(
                            ),
                        terminal_compatibility: None,
                    },
                )
                .await
                .expect("webrtc adapter hello");
            let attach = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::Attach {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                    },
                )
                .await
                .expect("attach over encrypted WebRTC");
            assert!(
                attach.events.is_empty(),
                "WebRTC Attach must not return terminal bodies: {:?}",
                attach.events
            );
            offer_peer
                .bind_reserved_from_attach(&attach)
                .await
                .expect("browser creates the reserved terminal DataChannel");
            fs::create_dir_all(release_path.parent().expect("release parent"))
                .expect("create webrtc release dir");
            fs::write(&release_path, b"go").expect("release webrtc write(2) producer");

            let mut concatenated = Vec::new();
            for _ in 0..120 {
                let _ = offer_peer
                    .encrypted_request(
                        &stream_key,
                        &botster_hub_client::DaemonRequest::ReadScreen {
                            session_id: session_id.clone(),
                        },
                    )
                    .await;
                if let Ok(Ok(bytes)) = timeout(
                    Duration::from_millis(250),
                    offer_peer.next_terminal_frame(&stream_key),
                )
                .await
                {
                    if let Ok(event) =
                        serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                    {
                        if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } =
                            event
                        {
                            concatenated.extend(live_output_decoded_bytes(payload));
                        }
                    } else {
                        concatenated.extend(bytes);
                    }
                }
                if concatenated
                    .windows(expected.len())
                    .any(|window| window == expected)
                {
                    break;
                }
            }
            assert!(
                concatenated
                    .windows(expected.len())
                    .any(|window| window == expected),
                "round {round} must observe live bytes before shutdown, got {concatenated:?}"
            );
            let _ = offer_peer.peer.close().await;
        });

        let mut session_cleanup = session_cleanup.expect("armed after Spawn");
        production_cleanup_after_authoritative_exit(
            &endpoint,
            &session_id,
            &format!("round {round} after observed exit"),
        );
        session_cleanup.disarm();
    }

    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn local_webrtc_chunks_oversized_encrypted_daemon_response() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-webrtc");
    let package_dir = unique_test_dir("web-webrtc-package");
    let provider_package_dir = unique_test_dir("web-webrtc-entity-provider-package");
    write_botster_web_package(&package_dir);
    write_entity_provider_plugin_package(&provider_package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = PanicSafeCliDaemon::start_with_local_webrtc_diagnostics(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);
    enable_supervised_package(&data_dir, &provider_package_dir);

    let (web_origin, bootstrap) = start_botster_web_and_issue_bootstrap(&endpoint);
    assert_eq!(bootstrap.package_name, "botster-web");
    assert_eq!(bootstrap.entrypoint_id, "web-client");
    assert_eq!(bootstrap.expected_origin, web_origin);
    assert_eq!(bootstrap.signaling_transport, "daemon_request");
    assert_eq!(bootstrap.data_plane, "webrtc_data_channel");
    assert!(bootstrap.ordered);
    assert_eq!(bootstrap.max_retransmits, None);
    assert_eq!(bootstrap.max_packet_lifetime_ms, None);

    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

    block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");

        let rejected_origin = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: "http://127.0.0.1:1".to_string(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-origin signal returns operator response");
        assert_eq!(
            rejected_origin.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_origin
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_origin_mismatch")
        );

        let rejected_secret = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: "wrong-secret".to_string(),
                origin: bootstrap.expected_origin.clone(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-secret signal returns operator response");
        assert_eq!(
            rejected_secret.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_secret
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_secret_mismatch")
        );

        let signal = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )
        .expect("signal local WebRTC offer");
        assert_eq!(
            signal.kind,
            botster_hub_client::DaemonResponseKind::LocalWebrtcAnswer
        );
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .expect("signal response includes WebRTC answer")
            .answer
            .clone();

        offer_peer
            .accept_answer(answer)
            .await
            .expect("offer peer accepts answer and opens channel");
        offer_peer.grant_secret = Some(bootstrap.grant_secret.clone());
        offer_peer
            .encrypted_hello(
                &stream_key,
                &botster_hub_client::DaemonHello {
                    protocol: botster_hub_client::PROTOCOL.to_string(),
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
                    terminal_compatibility: None,
                },
            )
            .await
            .expect("webrtc adapter hello before host requests");

        let status = offer_peer
            .encrypted_request(&stream_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

        let update = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::CheckHubUpdate,
            )
            .await
            .expect("Hub update check over encrypted WebRTC data channel");
        assert_eq!(
            update.kind,
            botster_hub_client::DaemonResponseKind::HubUpdate
        );
        assert_eq!(
            update
                .hub_update
                .expect("WebRTC Hub update payload")
                .current_version,
            env!("CARGO_PKG_VERSION")
        );

        let list = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::ListSessions,
            )
            .await
            .expect("list sessions over encrypted WebRTC data channel");
        assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);

        let subscribed = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "local-webrtc-entities".to_string(),
                },
            )
            .await
            .expect("subscribe to session entities over encrypted WebRTC data channel");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            offer_peer
                .next_entity_frame(&stream_key)
                .await
                .expect("initial WebRTC entity snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot {
                ref subscription_id,
                ref items,
                ..
            } if subscription_id == "local-webrtc-entities" && items.is_empty()
        ));

        for (subscription_id, generation) in [
            ("local-webrtc-provider-first", 1_u64),
            ("local-webrtc-provider-reconnect", 2_u64),
        ] {
            let subscribed = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::SubscribeEntities {
                        entity_type:
                            "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run"
                                .to_string(),
                        subscription_id: subscription_id.to_string(),
                    },
                )
                .await
                .expect("subscribe to package entities over WebRTC");
            assert_eq!(
                subscribed.kind,
                botster_hub_client::DaemonResponseKind::EntitySubscribed
            );
            assert!(matches!(
                offer_peer
                    .next_entity_frame(&stream_key)
                    .await
                    .expect("package entity snapshot over WebRTC"),
                botster_hub_client::DaemonEntityFrame::Snapshot {
                    snapshot_seq,
                    ref entity_type,
                    ref items,
                    ..
                } if snapshot_seq == generation
                    && entity_type == "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run"
                    && items.first().and_then(|item| item.get("status")).and_then(serde_json::Value::as_str)
                        == Some(format!("generation-{generation}").as_str())
            ));
            let unsubscribed = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::UnsubscribeEntities {
                        subscription_id: subscription_id.to_string(),
                    },
                )
                .await
                .expect("unsubscribe package entities over WebRTC");
            assert_eq!(
                unsubscribed.kind,
                botster_hub_client::DaemonResponseKind::EntityUnsubscribed
            );
        }

        let spawn = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: "local-webrtc-session".to_string(),
                command: "printf 'local-webrtc-ready\\n'; while IFS= read -r line; do printf 'webrtc:%s\\n' \"$line\"; done".to_string(),
            },
        )
        .expect("external daemon client spawns a session visible over WebRTC");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
        assert!(matches!(
            offer_peer
                .next_entity_frame(&stream_key)
                .await
                .expect("spawn upsert over WebRTC entity delivery"),
            botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
                if id == "local-webrtc-session"
        ));

        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "local-webrtc-session".to_string(),
                    subscription_id: "local-webrtc-subscription".to_string(),
                },
            )
            .await
            .expect("attach over encrypted WebRTC data channel");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        offer_peer
            .bind_reserved_from_attach(&attach)
            .await
            .expect("browser creates the reserved terminal DataChannel");

        let resize = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Resize {
                    session_id: "local-webrtc-session".to_string(),
                    rows: 33,
                    cols: 111,
                },
            )
            .await
            .expect("resize over encrypted WebRTC data channel");
        assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);

        let send = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: "local-webrtc-session".to_string(),
                    data: "from-local-webrtc\n".to_string(),
                },
            )
            .await
            .expect("send input over encrypted WebRTC data channel");
        assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

        let mut observed = String::new();
        for _ in 0..120 {
            let screen = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::ReadScreen {
                        session_id: "local-webrtc-session".to_string(),
                    },
                )
                .await
                .expect("read screen over encrypted WebRTC data channel");
            if let Some(body) = screen.read_screen {
                observed = body.text;
            }
            while let Some((_, bytes)) = offer_peer.pending_terminal_frames.pop_front() {
                if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                    && let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event
                {
                    observed.push_str(&live_output_utf8(payload));
                }
            }
            if observed.contains("webrtc:from-local-webrtc") {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert!(
            observed.contains("webrtc:from-local-webrtc"),
            "encrypted WebRTC data channel should drain session output, got {observed:?}"
        );

        let created = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::CreateSpawnTarget {
                target_id: Some("local-webrtc-large-target".to_string()),
                label: Some("Local WebRTC oversized response".to_string()),
                root: data_dir.clone(),
                enabled: true,
                kind: Some("directory".to_string()),
                base_ref: None,
                metadata: BTreeMap::from([("synthetic".to_string(), "x".repeat(300_000))]),
            },
        )
        .expect("seed synthetic oversized response through daemon socket");
        assert_eq!(
            created.kind,
            botster_hub_client::DaemonResponseKind::SpawnTargets
        );
        let (large_response, metrics) = offer_peer
            .encrypted_request_with_metrics(
                &stream_key,
                &botster_hub_client::DaemonRequest::ListSpawnTargets,
            )
            .await
            .expect("list oversized spawn-target response over encrypted WebRTC");
        assert_eq!(
            large_response.kind,
            botster_hub_client::DaemonResponseKind::SpawnTargets
        );
        assert_eq!(
            large_response
                .spawn_targets
                .iter()
                .find(|target| target.target_id == "local-webrtc-large-target")
                .and_then(|target| target.metadata.get("synthetic"))
                .map(String::len),
            Some(300_000)
        );
        assert!(metrics.envelope_bytes > 256 * 1024);
        assert!(metrics.chunk_count > 1);
        assert!(metrics.maximum_frame_bytes < botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES);

        let shutdown = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "local-webrtc-session".to_string(),
                },
            )
            .await
            .expect("shutdown over encrypted WebRTC data channel");
        assert_eq!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        loop {
            if matches!(
                offer_peer
                    .next_entity_frame(&stream_key)
                    .await
                    .expect("lifecycle patch over WebRTC entity delivery"),
                botster_hub_client::DaemonEntityFrame::Patch {
                    ref id,
                    ref patch,
                    ..
                } if id == "local-webrtc-session"
                    && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                        == Some("exited")
            ) {
                break;
            }
        }
        let removed = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::RemoveSession {
                    session_id: "local-webrtc-session".to_string(),
                },
            )
            .await
            .expect("remove session while WebRTC entity subscription is active");
        assert_eq!(
            removed.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
        );
        loop {
            if matches!(
                offer_peer
                    .next_entity_frame(&stream_key)
                    .await
                    .expect("remove frame over WebRTC entity delivery"),
                botster_hub_client::DaemonEntityFrame::Remove { ref id, .. }
                    if id == "local-webrtc-session"
            ) {
                break;
            }
        }
        offer_peer
            .data_channel
            .send_text("invalid-encrypted-request")
            .await
            .expect("send terminal invalid request to prove fail-closed cleanup");
        sleep(Duration::from_millis(100)).await;
        let _ = offer_peer.data_channel.close().await;
        offer_peer.peer.close().await.expect("close offer peer");
    });

    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    let cleanup_subscription = loop {
        match botster_hub_client::subscribe_session_entities(&endpoint, "local-webrtc-entities") {
            Ok(subscription) => break subscription,
            Err(error) if Instant::now() < cleanup_deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                panic!("WebRTC peer cleanup did not release entity subscription: {error}")
            }
        }
    };
    cleanup_subscription
        .unsubscribe()
        .expect("cleanup proof subscription unsubscribes");

    let reused = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap.grant_id.clone(),
            grant_secret: bootstrap.grant_secret.clone(),
            origin: bootstrap.expected_origin.clone(),
            offer: serde_json::Value::Null,
        },
    )
    .expect("reused grant returns operator response");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        reused.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_redeemed_grant")
    );

    let persisted_state =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state");
    assert!(!persisted_state.contains(&bootstrap.grant_id));
    assert!(!persisted_state.contains(&bootstrap.grant_secret));
    assert!(!persisted_state.contains("grant_secret"));
    child.shutdown();
}

#[test]
fn botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap() {
    let _guard = daemon_test_guard();
    let test_started = Instant::now();
    let data_dir = unique_short_test_dir("web-webrtc-reload");
    let package_dir = unique_test_dir("web-webrtc-reload-package");
    write_botster_web_package(&package_dir);
    log_botster_web_phase(test_started, "fixture_built");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    log_botster_web_phase(test_started, "daemon_started");
    enable_supervised_package(&data_dir, &package_dir);
    log_botster_web_phase(test_started, "package_enabled");

    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([
                ("BOTSTER_WEB_PORT".to_string(), "0".to_string()),
                (
                    "BOTSTER_WEB_TEST_STARTUP_DELAY_MS".to_string(),
                    BOTSTER_WEB_READINESS_STARTUP_DELAY_MS.to_string(),
                ),
            ]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_daemon_response_ok(
        &start,
        botster_hub_client::DaemonResponseKind::Packages,
        "start botster-web entrypoint",
    );
    log_botster_web_phase(test_started, "entrypoint_start_returned");
    let web_origin = wait_for_published_web_origin(&endpoint);
    let expected_local_url = format!("{web_origin}/");
    let apps =
        wait_for_botster_web_readiness(&endpoint, &web_origin, &expected_local_url, test_started);
    assert_eq!(
        app_row(&apps, "web-client")
            .launch_target
            .local_url
            .as_deref(),
        Some(expected_local_url.as_str())
    );

    let wrong_origin = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: "http://127.0.0.1:1".to_string(),
        },
    )
    .expect("wrong-origin bootstrap issuance returns operator response");
    assert_eq!(
        wrong_origin.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        wrong_origin.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_bootstrap_origin_mismatch")
    );

    let bootstrap_a = botster_web_page_bootstrap(&web_origin);
    let bootstrap_b = botster_web_page_bootstrap(&web_origin);
    let bootstrap_c = botster_web_page_bootstrap(&web_origin);
    assert_eq!(bootstrap_a.package_name, "botster-web");
    assert_eq!(bootstrap_a.entrypoint_id, "web-client");
    assert_eq!(bootstrap_a.expected_origin, web_origin);
    assert_eq!(bootstrap_b.expected_origin, bootstrap_a.expected_origin);
    assert_ne!(bootstrap_a.grant_id, bootstrap_b.grant_id);
    assert_ne!(bootstrap_a.grant_secret, bootstrap_b.grant_secret);
    assert_ne!(bootstrap_b.grant_id, bootstrap_c.grant_id);
    assert_ne!(bootstrap_b.grant_secret, bootstrap_c.grant_secret);

    block_on(async {
        let (mut first_peer, first_key) = open_local_webrtc_peer(&endpoint, &bootstrap_a).await;
        let status = first_peer
            .encrypted_request(&first_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over first encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let subscribed = first_peer
            .encrypted_request(
                &first_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-1".to_string(),
                },
            )
            .await
            .expect("subscribe on first WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            first_peer
                .next_entity_frame(&first_key)
                .await
                .expect("first generation snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.is_empty()
        ));
        let spawn = first_peer
            .encrypted_request(
                &first_key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: "local-webrtc-reload-session".to_string(),
                    command: "printf 'reload-ready\\n'; while IFS= read -r line; do printf 'reload:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await
            .expect("spawn over first encrypted WebRTC data channel");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
        first_peer
            .data_channel
            .close()
            .await
            .expect("close first generation data channel");
        first_peer.peer.close().await.expect("close first peer");

        let rejected_secret = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap_b.grant_id.clone(),
                grant_secret: "wrong-secret".to_string(),
                origin: bootstrap_b.expected_origin.clone(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-secret reload signal returns operator response");
        assert_eq!(
            rejected_secret.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_secret
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_secret_mismatch")
        );

        let (mut reload_peer, reload_key) = open_local_webrtc_peer(&endpoint, &bootstrap_b).await;
        let status = reload_peer
            .encrypted_request(&reload_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over reload encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let subscribed = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-2".to_string(),
                },
            )
            .await
            .expect("subscribe on second WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        loop {
            match reload_peer
                .next_entity_frame(&reload_key)
                .await
                .expect("second generation projection")
            {
                botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                    if items.iter().any(|item| {
                        item.get("session_uuid").and_then(serde_json::Value::as_str)
                            == Some("local-webrtc-reload-session")
                    }) =>
                {
                    break;
                }
                botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
                    if id == "local-webrtc-reload-session" =>
                {
                    break;
                }
                botster_hub_client::DaemonEntityFrame::Snapshot { .. } => {}
                other => panic!("unexpected second-generation frame: {other:?}"),
            }
        }
        let generation_two_shutdown = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "local-webrtc-reload-session".to_string(),
                },
            )
            .await
            .expect("emit a lifecycle delta on the second WebRTC generation");
        assert_eq!(
            generation_two_shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        loop {
            if matches!(
                reload_peer
                    .next_entity_frame(&reload_key)
                    .await
                    .expect("current second-generation lifecycle delta"),
                botster_hub_client::DaemonEntityFrame::Patch {
                    ref subscription_id,
                    ref id,
                    ref patch,
                    ..
                } if subscription_id == "reload-entities-generation-2"
                    && id == "local-webrtc-reload-session"
                    && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                        == Some("exited")
            ) {
                break;
            }
        }
        let sessions = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::ListSessions,
            )
            .await
            .expect("list sessions over reload encrypted WebRTC data channel");
        assert_eq!(
            sessions.kind,
            botster_hub_client::DaemonResponseKind::Sessions
        );
        assert!(
            sessions
                .sessions
                .iter()
                .any(|session| session.session_id == "local-webrtc-reload-session"),
            "reload DataChannel should hydrate existing sessions"
        );
        reload_peer
            .data_channel
            .close()
            .await
            .expect("close second generation data channel");
        reload_peer.peer.close().await.expect("close reload peer");

        let (mut final_peer, final_key) = open_local_webrtc_peer(&endpoint, &bootstrap_c).await;
        let subscribed = final_peer
            .encrypted_request(
                &final_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-3".to_string(),
                },
            )
            .await
            .expect("subscribe on third WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        loop {
            match final_peer
                .next_entity_frame(&final_key)
                .await
                .expect("third generation projection")
            {
                botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                    if items.iter().any(|item| {
                        item.get("session_uuid").and_then(serde_json::Value::as_str)
                            == Some("local-webrtc-reload-session")
                    }) =>
                {
                    break;
                }
                botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
                    if id == "local-webrtc-reload-session" =>
                {
                    break;
                }
                botster_hub_client::DaemonEntityFrame::Snapshot { .. } => {}
                other => panic!("unexpected third-generation frame: {other:?}"),
            }
        }
        let current_generation_remove = final_peer
            .encrypted_request(
                &final_key,
                &botster_hub_client::DaemonRequest::RemoveSession {
                    session_id: "local-webrtc-reload-session".to_string(),
                },
            )
            .await
            .expect("emit a lifecycle delta on the third WebRTC generation");
        assert_eq!(
            current_generation_remove.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
        );
        loop {
            if matches!(
                final_peer
                    .next_entity_frame(&final_key)
                    .await
                    .expect("current third-generation lifecycle delta"),
                botster_hub_client::DaemonEntityFrame::Remove {
                    ref subscription_id,
                    ref id,
                    ..
                } if subscription_id == "reload-entities-generation-3"
                    && id == "local-webrtc-reload-session"
            ) {
                break;
            }
        }
        let status = final_peer
            .encrypted_request(&final_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("ordinary request on third WebRTC generation");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        final_peer
            .data_channel
            .close()
            .await
            .expect("close third generation data channel");
        final_peer.peer.close().await.expect("close final peer");
    });

    let reused = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap_a.grant_id.clone(),
            grant_secret: bootstrap_a.grant_secret.clone(),
            origin: bootstrap_a.expected_origin.clone(),
            offer: serde_json::Value::Null,
        },
    )
    .expect("reused first page-load grant returns operator response");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        reused.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_redeemed_grant")
    );

    let persisted_state =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state");
    for secret in [
        bootstrap_a.grant_id.as_str(),
        bootstrap_a.grant_secret.as_str(),
        bootstrap_b.grant_id.as_str(),
        bootstrap_b.grant_secret.as_str(),
        bootstrap_c.grant_id.as_str(),
        bootstrap_c.grant_secret.as_str(),
    ] {
        assert!(!persisted_state.contains(secret));
    }
    assert!(!persisted_state.contains("grant_secret"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_webrtc_peer_close_detaches_terminal_subscriptions() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-webrtc-close");
    let package_dir = unique_test_dir("web-webrtc-close-package");
    write_botster_web_package(&package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let (_web_origin, bootstrap) = start_botster_web_and_issue_bootstrap(&endpoint);
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

    block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");
        let signal = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )
        .expect("signal local WebRTC offer");
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .expect("signal response includes WebRTC answer")
            .answer
            .clone();
        offer_peer
            .accept_answer(answer)
            .await
            .expect("offer peer accepts answer and opens channel");

        let spawn = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: "local-webrtc-drop-session".to_string(),
                    command: "printf 'local-webrtc-drop-ready\\n'; while IFS= read -r line; do printf 'drop:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await
            .expect("spawn over encrypted WebRTC data channel");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);

        offer_peer
            .encrypted_hello(
                &stream_key,
                &botster_hub_client::DaemonHello {
                    protocol: botster_hub_client::PROTOCOL.to_string(),
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
                    terminal_compatibility: None,
                },
            )
            .await
            .expect("webrtc adapter hello before attach");
        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "local-webrtc-drop-session".to_string(),
                    subscription_id: "local-webrtc-drop-subscription".to_string(),
                },
            )
            .await
            .expect("attach over encrypted WebRTC data channel");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

        offer_peer.peer.close().await.expect("close offer peer");
    });

    thread::sleep(Duration::from_millis(800));

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let socket_attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "local-webrtc-drop-session".to_string(),
            subscription_id: "socket-after-webrtc-close-subscription".to_string(),
        })
        .expect("attach socket client after WebRTC peer close");
    assert_eq!(
        socket_attach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "local-webrtc-drop-session".to_string(),
            data: "after-webrtc-close\n".to_string(),
        })
        .expect("send input after WebRTC peer close");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let observed = wait_for_read_screen_contains(
        &mut connection,
        "local-webrtc-drop-session",
        "drop:after-webrtc-close",
    );
    assert!(
        observed.contains("drop:after-webrtc-close"),
        "socket client should observe output after WebRTC close, got {observed:?}"
    );
    let closed_peer_drain = connection
        .request(&botster_hub_client::DaemonRequest::drain_subscription(
            "local-webrtc-drop-session",
            "local-webrtc-drop-subscription",
        ))
        .expect("drain closed WebRTC subscription");
    let events_after_close = closed_peer_drain.events;
    assert!(
        events_after_close.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput {
                    subscription_id,
                    payload,
                    ..
                } if subscription_id == "local-webrtc-drop-subscription"
                    && live_output_contains(payload, "drop:after-webrtc-close")
            )
        }),
        "closed WebRTC peer subscription must not receive later output: {events_after_close:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "local-webrtc-drop-session".to_string(),
        })
        .expect("shutdown drop test session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_spawns_botster_web_runtime_session_request_shape() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-spawn");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command:
                "printf 'botster-web-runtime-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("botster-web runtime spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(spawn.sessions.iter().any(|session| session.session_id
        == "botster-web-runtime-session"
        && session.lifecycle == "running"));

    let list = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after botster-web runtime spawn");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);
    assert!(list.sessions.iter().any(|session| session.session_id
        == "botster-web-runtime-session"
        && session.lifecycle == "running"));

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages remains observable after botster-web runtime spawn");
    assert_eq!(
        packages.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "botster-web-runtime-session".to_string(),
            subscription_id: "botster-web-runtime-subscription".to_string(),
        })
        .expect("attach botster-web runtime session");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-runtime-session".to_string(),
            data: "from-web-action\n".to_string(),
        })
        .expect("send input to botster-web runtime session");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let observed = wait_for_read_screen_contains(
        &mut connection,
        "botster-web-runtime-session",
        "web:from-web-action",
    );
    assert!(
        observed.contains("web:from-web-action"),
        "botster-web runtime request shape should attach and show output, got {observed:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "botster-web-runtime-session".to_string(),
        })
        .expect("shutdown botster-web runtime session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_duplicate_botster_web_runtime_spawn_is_rejected_without_cleanup() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-duplicate");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let first_spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command:
                "printf 'botster-web-runtime-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("first botster-web runtime spawn request");
    assert_eq!(
        first_spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let duplicate = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command: "printf 'replacement-should-not-start\\n'".to_string(),
        })
        .expect("duplicate botster-web runtime spawn should return operator frame");
    assert_eq!(
        duplicate.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = duplicate.error.as_ref().expect("operator error body");
    assert_eq!(
        error.code, "session_already_exists",
        "unexpected duplicate spawn operator error: {error:?} diagnostics={:?}",
        duplicate.diagnostics
    );
    assert_eq!(error.operation, "spawn");
    assert!(
        duplicate.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
                && diagnostic.operation.as_deref() == Some("spawn")
                && diagnostic
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("already exists"))
        }),
        "duplicate spawn should carry a session_already_exists diagnostic row, got {:?}",
        duplicate.diagnostics
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "botster-web-runtime-session".to_string(),
            subscription_id: "botster-web-runtime-duplicate-subscription".to_string(),
        })
        .expect("attach original botster-web runtime session after duplicate rejection");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-runtime-session".to_string(),
            data: "after-duplicate\n".to_string(),
        })
        .expect("existing session remains writable after duplicate rejection");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let observed = wait_for_read_screen_contains(
        &mut connection,
        "botster-web-runtime-session",
        "web:after-duplicate",
    );
    assert!(
        observed.contains("web:after-duplicate"),
        "duplicate rejection must not clean up or replace the existing session, got {observed:?}"
    );
    assert!(
        !observed.contains("replacement-should-not-start"),
        "duplicate rejected spawn command must not start, got {observed:?}"
    );

    let debug = format!("{error:?} {:?}", duplicate.diagnostics);
    assert!(!debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!debug.contains(concat!("/", "Users", "/")));
    assert!(!debug.contains("/home/"));

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "botster-web-runtime-session".to_string(),
        })
        .expect("shutdown botster-web runtime session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_held_open_fanout_over_local_webrtc() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-webrtc");
    let web_package_dir = unique_test_dir("pkg-entity-webrtc-web");
    let package_dir = unique_test_dir("pkg-entity-webrtc-pkg");
    write_botster_web_package(&web_package_dir);
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &web_package_dir);
    enable_mutation_package(&endpoint, package_dir);

    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                "0".to_string(),
            )]),
        },
    )
    .expect("start botster-web");
    assert_daemon_response_ok(
        &start,
        botster_hub_client::DaemonResponseKind::Packages,
        "start botster-web",
    );
    let web_origin = wait_for_published_web_origin(&endpoint);
    let expected_local_url = format!("{web_origin}/");
    let _apps =
        wait_for_botster_web_readiness(&endpoint, &web_origin, &expected_local_url, Instant::now());

    let bootstrap = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: web_origin.clone(),
        },
    )
    .expect("issue bootstrap")
    .local_webrtc_bootstrap
    .expect("bootstrap body");

    block_on(async {
        let (mut offer_peer, stream_key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        let subscribed = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "project-pipelines.membership".to_string(),
                    subscription_id: "webrtc-held".to_string(),
                },
            )
            .await
            .expect("subscribe over webrtc");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        let snapshot = offer_peer
            .next_entity_frame(&stream_key)
            .await
            .expect("initial snapshot");
        assert!(matches!(
            snapshot,
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. } if items.is_empty()
        ));

        let claim = mutation_action(
            &endpoint,
            "project-pipelines.claim",
            serde_json::json!({ "id": "webrtc-m1" }),
        );
        assert_eq!(
            claim.kind,
            botster_hub_client::DaemonResponseKind::PluginActionResult
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let frame = timeout(
                Duration::from_millis(500),
                offer_peer.next_entity_frame(&stream_key),
            )
            .await;
            match frame {
                Ok(Ok(botster_hub_client::DaemonEntityFrame::Upsert {
                    ref id,
                    snapshot_seq: 1,
                    ..
                })) if id == "webrtc-m1" => break,
                Ok(Ok(_)) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for webrtc upsert"
                    );
                }
                Ok(Err(error)) => panic!("webrtc entity frame error: {error}"),
                Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for webrtc upsert"
                    );
                }
            }
        }
    });

    shutdown_cli_daemon(&data_dir, child);
}
