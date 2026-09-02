use std::collections::BTreeSet;

const LIVE_PASTE_BYTES: usize = 1_048_576;

fn live_paste_payload() -> Vec<u8> {
    (0..LIVE_PASTE_BYTES).map(|index| index as u8).collect()
}

fn paste_sink_command(sink: &Path, ready: &str, done: &str) -> String {
    format!(
        "stty raw -echo; printf '{ready}'; head -c {LIVE_PASTE_BYTES} > {}; printf '{done}'; sleep 30",
        sink.display()
    )
}

fn terminal_frame_contains(bytes: &[u8], marker: &str) -> bool {
    if bytes
        .windows(marker.len())
        .any(|window| window == marker.as_bytes())
    {
        return true;
    }
    serde_json::from_slice::<botster_hub_client::DaemonEvent>(bytes).is_ok_and(|event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(&payload, marker)
        )
    })
}

fn input_result_for_operation(bytes: &[u8], operation_id: u32) -> Option<serde_json::Value> {
    let value = serde_json::from_slice::<serde_json::Value>(bytes).ok()?;
    (value.get("type").and_then(serde_json::Value::as_str) == Some("input_result")
        && value.get("operation_id").and_then(serde_json::Value::as_u64)
            == Some(u64::from(operation_id)))
    .then_some(value)
}

fn unix_paste_results(
    envelopes: &[botster_hub_client::DaemonUnixTerminalEnvelope],
    operation_id: u32,
) -> Vec<serde_json::Value> {
    envelopes
        .iter()
        .filter_map(|envelope| envelope.payload_bytes().ok())
        .filter_map(|bytes| input_result_for_operation(&bytes, operation_id))
        .collect()
}

fn unix_terminal_has_marker(
    envelopes: &[botster_hub_client::DaemonUnixTerminalEnvelope],
    marker: &str,
) -> bool {
    envelopes.iter().any(|envelope| {
        envelope
            .payload_bytes()
            .is_ok_and(|bytes| terminal_frame_contains(&bytes, marker))
    })
}

fn collect_unix_mux_for(
    reader: &mut BufReader<UnixStream>,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
    duration: Duration,
) {
    reader
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("set paste mux timeout");
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        match botster_hub_client::read_unix_mux_frame_from_reader(reader) {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                envelopes.push(envelope)
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Event(event)) => events.push(event),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Response(response)) => {
                panic!("paste mux received an unpaired response: {response:?}")
            }
            Err(_) => {}
        }
    }
    reader
        .get_ref()
        .set_read_timeout(None)
        .expect("clear paste mux timeout");
}

fn collect_unix_paste_completion(
    reader: &mut BufReader<UnixStream>,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
    operation_id: u32,
    done: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        collect_unix_mux_for(reader, envelopes, events, Duration::from_millis(100));
        if unix_paste_results(envelopes, operation_id).len() == 1
            && unix_terminal_has_marker(envelopes, done)
        {
            collect_unix_mux_for(reader, envelopes, events, Duration::from_millis(500));
            return;
        }
    }
    panic!(
        "paste did not complete: results={:?} done={} events={events:?}",
        unix_paste_results(envelopes, operation_id),
        unix_terminal_has_marker(envelopes, done)
    );
}

fn assert_admitted_paste_result(results: &[serde_json::Value], operation_id: u32) {
    assert_eq!(results.len(), 1, "one paste input_result: {results:?}");
    assert_eq!(results[0]["operation_id"], operation_id, "{results:?}");
    assert_eq!(results[0]["admitted"], true, "{results:?}");
    assert_eq!(
        results[0]["bytes_written"],
        LIVE_PASTE_BYTES,
        "{results:?}"
    );
    assert!(
        results[0].get("rejection").is_none(),
        "admitted paste must omit rejection: {:?}",
        results[0]
    );
}

fn assert_no_route_close(
    events: &[botster_hub_client::DaemonEvent],
    session_id: &str,
    subscription_id: &str,
) {
    assert!(
        events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id: closed_session,
                subscription_id: closed_subscription,
                ..
            } if closed_session == session_id && closed_subscription == subscription_id
        )),
        "paste route must stay open: {events:?}"
    );
}

fn wait_for_unix_ready_and_mode(
    stream: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    session_id: &str,
    ready: &str,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) -> botster_hub_client::DaemonModeFlags {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let response = request_collecting_mux(
            stream,
            reader,
            &botster_hub_client::DaemonRequest::ReadModeFlags {
                session_id: session_id.to_string(),
            },
            envelopes,
            events,
        );
        if unix_terminal_has_marker(envelopes, ready)
            && response.kind == botster_hub_client::DaemonResponseKind::ReadModeFlags
            && response
                .mode_flags
                .as_ref()
                .is_some_and(|flags| flags.mode_generation != 0)
        {
            return response.mode_flags.expect("mode flags body");
        }
        assert!(Instant::now() < deadline, "raw paste sink did not become ready");
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_sink_bytes(sink: &Path, payload: &[u8]) {
    let actual = fs::read(sink).expect("read paste sink");
    assert_eq!(actual, payload, "PTY sink must receive byte-exact paste content");
}

#[test]
fn unix_paste_transaction_delivers_one_result_and_byte_exact_pty_content() {
    let _guard = daemon_test_guard();
    let test_dir = unique_short_test_dir("unix-paste");
    fs::create_dir_all(&test_dir).expect("create paste test directory");
    let sink = test_dir.join("paste.bin");
    let hub = start_isolated_live_output_hub("unix-paste");
    let endpoint = hub.endpoint().clone();
    let session_id = "unix-paste-session";
    let subscription_id = "unix-paste-sub";
    let ready = "unix-paste-sink-ready";
    let done = "unix-paste-sink-done";
    let operation_id = 4101;
    let payload = live_paste_payload();
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        session_id,
        subscription_id,
        &paste_sink_command(&sink, ready, done),
        &mut envelopes,
        &mut events,
    );
    let mode = wait_for_unix_ready_and_mode(
        &mut stream,
        &mut reader,
        session_id,
        ready,
        &mut envelopes,
        &mut events,
    );
    envelopes.clear();
    events.clear();

    let frames = terminal_paste_frame_bytes(
        operation_id,
        mode.mode_generation,
        mode.mode_revision,
        &payload,
    );
    assert_eq!(frames.len(), 19);
    for frame in &frames {
        write_unix_terminal_frame(&mut stream, session_id, subscription_id, frame);
    }
    collect_unix_paste_completion(
        &mut reader,
        &mut envelopes,
        &mut events,
        operation_id,
        done,
    );

    assert_admitted_paste_result(&unix_paste_results(&envelopes, operation_id), operation_id);
    assert_no_route_close(&events, session_id, subscription_id);
    assert_sink_bytes(&sink, &payload);
    let status = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert!(occupancy_has_pair(
        &status.status.expect("status body").live_attach_occupancy,
        session_id,
        subscription_id
    ));

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn webrtc_paste_transaction_delivers_one_result_and_byte_exact_pty_content() {
    let _guard = daemon_test_guard();
    let test_dir = unique_short_test_dir("webrtc-paste");
    fs::create_dir_all(&test_dir).expect("create paste test directory");
    let sink = test_dir.join("paste.bin");
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("webrtc-paste");
    let session_id = "webrtc-paste-session";
    let subscription_id = "webrtc-paste-sub";
    let ready = "webrtc-paste-sink-ready";
    let done = "webrtc-paste-sink-done";
    let operation_id = 4201;
    let payload = live_paste_payload();

    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        peer.encrypted_hello(&key, &webrtc_close_event_hello())
            .await
            .expect("close-event hello");
        let spawned = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: session_id.to_string(),
                    command: paste_sink_command(&sink, ready, done),
                },
            )
            .await
            .expect("spawn paste sink");
        assert_eq!(spawned.kind, botster_hub_client::DaemonResponseKind::Spawned);
        let attach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                },
            )
            .await
            .expect("attach paste route");
        let reservation = attach
            .terminal_reservation
            .as_ref()
            .expect("terminal reservation");
        let channel = peer
            .open_reserved_terminal(&key, &reservation.label, &webrtc_terminal_adapter_hello())
            .await
            .expect("open reserved terminal channel");

        let ready_deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Ok(Ok(bytes)) = timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
                && terminal_frame_contains(&bytes, ready)
            {
                break;
            }
            assert!(Instant::now() < ready_deadline, "WebRTC paste sink did not become ready");
        }
        let mode = loop {
            let response = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::ReadModeFlags {
                        session_id: session_id.to_string(),
                    },
                )
                .await
                .expect("read mode flags");
            if let Some(mode) = response.mode_flags
                && mode.mode_generation != 0
            {
                break mode;
            }
            assert!(Instant::now() < ready_deadline, "mode token did not become ready");
        };

        let frames = terminal_paste_frame_bytes(
            operation_id,
            mode.mode_generation,
            mode.mode_revision,
            &payload,
        );
        assert_eq!(frames.len(), 19);
        for frame in &frames {
            LocalWebrtcOfferPeer::send_reserved_terminal_frame(&channel, &key, frame)
                .await
                .expect("send paste frame");
        }

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut delivered = Vec::new();
        let mut completed_at = None;
        while Instant::now() < deadline {
            if let Ok(Ok(bytes)) = timeout(Duration::from_millis(100), peer.next_terminal_frame(&key)).await {
                delivered.push(bytes);
            }
            let result_count = delivered
                .iter()
                .filter(|bytes| input_result_for_operation(bytes, operation_id).is_some())
                .count();
            let saw_done = delivered.iter().any(|bytes| terminal_frame_contains(bytes, done));
            if result_count == 1 && saw_done {
                let first_complete = *completed_at.get_or_insert_with(Instant::now);
                if first_complete.elapsed() >= Duration::from_millis(500) {
                    break;
                }
            }
        }
        let results = delivered
            .iter()
            .filter_map(|bytes| input_result_for_operation(bytes, operation_id))
            .collect::<Vec<_>>();
        assert_admitted_paste_result(&results, operation_id);
        assert!(delivered.iter().any(|bytes| terminal_frame_contains(bytes, done)));
        assert!(peer.pending_host_events().iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id: closed_session,
                subscription_id: closed_subscription,
                ..
            } if closed_session == session_id && closed_subscription == subscription_id
        )));
        assert_eq!(peer.control_terminal_frame_count, 0);
        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status after paste");
        assert!(occupancy_has_pair(
            &status.status.expect("status body").live_attach_occupancy,
            session_id,
            subscription_id
        ));
        peer.peer.close().await.expect("close offer peer");
    });

    assert_sink_bytes(&sink, &payload);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn paused_ingress_holds_nineteen_paste_frames_without_lost() {
    let _guard = daemon_test_guard();
    let test_dir = unique_short_test_dir("paused-paste");
    fs::create_dir_all(&test_dir).expect("create paste test directory");
    let pause = test_dir.join("pause");
    let pause_value = pause.display().to_string();
    let sink = test_dir.join("paste.bin");
    let hub = start_isolated_live_output_hub_with_env(
        "paused-paste",
        &[("BOTSTER_HUB_TEST_PAUSE_DATA_PLANE", pause_value.as_str())],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "paused-paste-session";
    let subscription_id = "paused-paste-sub";
    let ready = "paused-paste-sink-ready";
    let done = "paused-paste-sink-done";
    let operation_id = 4301;
    let payload = live_paste_payload();
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        session_id,
        subscription_id,
        &paste_sink_command(&sink, ready, done),
        &mut envelopes,
        &mut events,
    );
    let mode = wait_for_unix_ready_and_mode(
        &mut stream,
        &mut reader,
        session_id,
        ready,
        &mut envelopes,
        &mut events,
    );
    envelopes.clear();
    events.clear();

    fs::write(&pause, b"pause").expect("arm data-plane pause");
    let entered = pause.with_extension("entered");
    let entered_deadline = Instant::now() + Duration::from_secs(3);
    while !entered.is_file() {
        assert!(Instant::now() < entered_deadline, "data-plane pause was not acknowledged");
        thread::sleep(Duration::from_millis(10));
    }
    let frames = terminal_paste_frame_bytes(
        operation_id,
        mode.mode_generation,
        mode.mode_revision,
        &payload,
    );
    assert_eq!(frames.len(), 19);
    for frame in &frames {
        write_unix_terminal_frame(&mut stream, session_id, subscription_id, frame);
    }
    collect_unix_mux_for(
        &mut reader,
        &mut envelopes,
        &mut events,
        Duration::from_millis(500),
    );
    assert!(unix_paste_results(&envelopes, operation_id).is_empty());
    assert_no_route_close(&events, session_id, subscription_id);

    fs::remove_file(&pause).expect("resume data-plane driver");
    collect_unix_paste_completion(
        &mut reader,
        &mut envelopes,
        &mut events,
        operation_id,
        done,
    );
    assert_admitted_paste_result(&unix_paste_results(&envelopes, operation_id), operation_id);
    assert_no_route_close(&events, session_id, subscription_id);
    assert_sink_bytes(&sink, &payload);

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn paused_ingress_sixty_fifth_frame_latches_lost_and_closes_only_that_route() {
    let _guard = daemon_test_guard();
    let test_dir = unique_short_test_dir("paused-overflow");
    fs::create_dir_all(&test_dir).expect("create overflow test directory");
    let pause = test_dir.join("pause");
    let pause_value = pause.display().to_string();
    let hub = start_isolated_live_output_hub_with_env(
        "paused-overflow",
        &[("BOTSTER_HUB_TEST_PAUSE_DATA_PLANE", pause_value.as_str())],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "paused-overflow-session";
    let primary_id = "paused-overflow-primary";
    let sibling_id = "paused-overflow-sibling";
    let (mut primary, mut primary_reader) = unix_adapter_connection(&endpoint);
    let mut primary_envelopes = Vec::new();
    let mut primary_events = Vec::new();
    spawn_and_bind(
        &mut primary,
        &mut primary_reader,
        session_id,
        primary_id,
        "printf 'overflow-ready'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut primary_envelopes,
        &mut primary_events,
    );
    let _ = wait_for_unix_ready_and_mode(
        &mut primary,
        &mut primary_reader,
        session_id,
        "overflow-ready",
        &mut primary_envelopes,
        &mut primary_events,
    );

    let (mut sibling, mut sibling_reader) = unix_adapter_connection(&endpoint);
    let mut sibling_envelopes = Vec::new();
    let mut sibling_events = Vec::new();
    let attach = request_collecting_mux(
        &mut sibling,
        &mut sibling_reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: sibling_id.to_string(),
        },
        &mut sibling_envelopes,
        &mut sibling_events,
    );
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    let sibling_deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let status = request_collecting_mux(
            &mut sibling,
            &mut sibling_reader,
            &botster_hub_client::DaemonRequest::Status,
            &mut sibling_envelopes,
            &mut sibling_events,
        );
        if occupancy_has_pair(
            &status.status.expect("status body").live_attach_occupancy,
            session_id,
            sibling_id,
        ) {
            break;
        }
        assert!(Instant::now() < sibling_deadline, "sibling route did not attach");
    }
    primary_envelopes.clear();
    primary_events.clear();

    fs::write(&pause, b"pause").expect("arm data-plane pause");
    let entered = pause.with_extension("entered");
    let entered_deadline = Instant::now() + Duration::from_secs(3);
    while !entered.is_file() {
        assert!(Instant::now() < entered_deadline, "data-plane pause was not acknowledged");
        thread::sleep(Duration::from_millis(10));
    }
    for _ in 0..65 {
        write_unix_terminal_frame(
            &mut primary,
            session_id,
            primary_id,
            &terminal_input_frame_bytes(b"x"),
        );
    }
    collect_unix_mux_for(
        &mut primary_reader,
        &mut primary_envelopes,
        &mut primary_events,
        Duration::from_millis(500),
    );
    assert_no_route_close(&primary_events, session_id, primary_id);

    fs::remove_file(&pause).expect("resume data-plane driver");
    assert!(wait_for_subscription_closed(
        &mut primary,
        &mut primary_reader,
        session_id,
        primary_id,
        &mut primary_envelopes,
        &mut primary_events,
    ));
    let closes = primary_events
        .iter()
        .filter(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id: closed_session,
                subscription_id: closed_subscription,
                reason,
                ..
            } if closed_session == session_id
                && closed_subscription == primary_id
                && reason == "core_adapter_closed"
        ))
        .count();
    assert_eq!(closes, 1, "one primary core_adapter_closed event: {primary_events:?}");

    write_unix_terminal_frame(
        &mut sibling,
        session_id,
        sibling_id,
        &terminal_input_frame_bytes(b"sibling-live\r"),
    );
    read_unsolicited_terminal_until(
        &mut sibling_reader,
        &mut sibling_envelopes,
        Instant::now() + Duration::from_secs(8),
        "echo:sibling-live",
    );
    assert!(unix_terminal_has_marker(&sibling_envelopes, "echo:sibling-live"));
    let status = request_collecting_mux(
        &mut sibling,
        &mut sibling_reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut sibling_envelopes,
        &mut sibling_events,
    );
    let occupancy = status.status.expect("status body").live_attach_occupancy;
    assert!(occupancy_has_pair(&occupancy, session_id, sibling_id));

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(test_dir);
}

fn assert_hub_source_paste_blind(root: &Path) -> Result<BTreeSet<PathBuf>, String> {
    const ROOTS: &[&str] = &["transport", "subscription", "data_plane", "admission"];
    const FORBIDDEN: &[&str] = &[
        "KIND_PASTE",
        "PASTE_BEGIN",
        "PASTE_CHUNK",
        "PASTE_COMMIT",
        "PASTE_ABORT",
        "MAX_PASTE",
        "operation_id",
        "encode_paste",
        "botster_terminal_protocol_client",
    ];

    fn scan(
        root: &Path,
        directory: &Path,
        found: &mut BTreeSet<PathBuf>,
    ) -> Result<(), String> {
        for entry in fs::read_dir(directory).map_err(|error| format!("read {directory:?}: {error}"))? {
            let entry = entry.map_err(|error| format!("read entry under {directory:?}: {error}"))?;
            let path = entry.path();
            if entry
                .file_type()
                .map_err(|error| format!("file type {path:?}: {error}"))?
                .is_dir()
            {
                scan(root, &path, found)?;
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| format!("relative path {path:?}: {error}"))?
                    .to_path_buf();
                let bytes = fs::read(&path).map_err(|error| format!("read {path:?}: {error}"))?;
                for token in FORBIDDEN {
                    if bytes.windows(token.len()).any(|window| window == token.as_bytes()) {
                        return Err(format!("{} contains forbidden token {token}", relative.display()));
                    }
                }
                found.insert(relative);
            }
        }
        Ok(())
    }

    let mut found = BTreeSet::new();
    for name in ROOTS {
        let directory = root.join(name);
        if !directory.is_dir() {
            return Err(format!("missing source root {}", directory.display()));
        }
        scan(root, &directory, &mut found)?;
    }
    for required in [
        "transport/shared/ingress.rs",
        "transport/shared/adapter_slot.rs",
        "transport/unix/connection.rs",
        "transport/webrtc/subscription_channel.rs",
    ] {
        if !found.contains(Path::new(required)) {
            return Err(format!("source scan missed required file {required}"));
        }
    }
    let ingress = fs::read(root.join("transport/shared/ingress.rs"))
        .map_err(|error| format!("read ingress anchor: {error}"))?;
    if !ingress.windows(b"push_complete".len()).any(|window| window == b"push_complete") {
        return Err("ingress source scan lost push_complete anchor".to_string());
    }
    Ok(found)
}

#[test]
fn hub_transport_source_stays_paste_blind() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let found = assert_hub_source_paste_blind(&root).expect("Hub source stays paste blind");
    assert!(!found.is_empty());
}

#[test]
fn paste_blind_guard_fails_on_seeded_eof_token() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let scratch = unique_short_test_dir("paste-blind-guard");
    fs::create_dir_all(&scratch).expect("create source guard scratch root");
    for name in ["transport", "subscription", "data_plane", "admission"] {
        copy_dir_all(&manifest.join("src").join(name), &scratch.join(name));
    }
    let ingress = scratch.join("transport/shared/ingress.rs");
    use std::io::Write as _;
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&ingress)
            .expect("open scratch ingress"),
        "// seeded EOF operation_id"
    )
    .expect("seed EOF token");
    let error = assert_hub_source_paste_blind(&scratch).expect_err("seeded EOF token must fail");
    assert!(error.contains("transport/shared/ingress.rs"), "{error}");
    let _ = fs::remove_dir_all(scratch);
}
