use std::collections::BTreeSet;

const LIVE_PASTE_BYTES: usize = 1_048_576;

fn ingress_admission_counts(path: &Path, session_id: &str, subscription_id: &str) -> (usize, usize) {
    let body = fs::read_to_string(path).unwrap_or_default();
    body.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| {
            row.get("session_id").and_then(serde_json::Value::as_str) == Some(session_id)
                && row
                    .get("subscription_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(subscription_id)
        })
        .fold((0, 0), |(stored, lost), row| {
            match row.get("outcome").and_then(serde_json::Value::as_str) {
                Some("stored") => (stored + 1, lost),
                Some("lost") => (stored, lost + 1),
                _ => (stored, lost),
            }
        })
}

fn wait_for_ingress_admissions(
    path: &Path,
    session_id: &str,
    subscription_id: &str,
    expected_stored: usize,
    expected_lost: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counts = ingress_admission_counts(path, session_id, subscription_id);
        assert!(
            counts.0 <= expected_stored && counts.1 <= expected_lost,
            "unexpected ingress admission counts: stored={} lost={}",
            counts.0,
            counts.1
        );
        if counts == (expected_stored, expected_lost) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "ingress admission counts did not reach stored={expected_stored} lost={expected_lost}; observed stored={} lost={}",
            counts.0,
            counts.1
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn live_paste_payload() -> Vec<u8> {
    (0..LIVE_PASTE_BYTES).map(|index| index as u8).collect()
}

/// BEGIN, one CHUNK per protocol-sized slice, COMMIT.
fn expected_paste_frames(payload: &[u8]) -> usize {
    payload.len().div_ceil(botster_terminal_protocol::MAX_PASTE_CHUNK_DATA_BYTES) + 2
}

/// The raw sink has no line editor that can enable bracketed paste wrappers.
fn paste_sink_command(sink: &Path, ready: &str, done: &str) -> String {
    format!(
        "stty raw -echo; printf '{ready}'; head -c {LIVE_PASTE_BYTES} > {}; printf '{done}'; sleep 30",
        sink.display()
    )
}

/// The raw sink, gated on one input byte. A WebRTC route exists only after
/// its channel binds, so the ready marker must follow an input sent through
/// that route to arrive as live output rather than attach history.
fn paste_sink_command_after_go(sink: &Path, ready: &str, done: &str) -> String {
    format!(
        "stty raw -echo; head -c 1 > /dev/null; printf '{ready}'; head -c {LIVE_PASTE_BYTES} > {}; printf '{done}'; sleep 30",
        sink.display()
    )
}

/// One raw terminal frame body carries OUTPUT containing `marker`.
fn terminal_frame_contains(bytes: &[u8], marker: &str) -> bool {
    botster_terminal_protocol::TerminalFrame::from_bytes(bytes).is_ok_and(|frame| {
        frame.kind() == botster_terminal_protocol::TerminalKind::Output
            && bytes_contain(frame.body(), marker.as_bytes())
    })
}

/// The INPUT_RESULT body for `operation_id`, if this frame body carries one.
fn input_result_for_operation(
    bytes: &[u8],
    operation_id: u64,
) -> Option<botster_terminal_protocol::InputResultBody> {
    let frame = botster_terminal_protocol::TerminalFrame::from_bytes(bytes).ok()?;
    let result = botster_terminal_protocol::decode_input_result(&frame).ok()?;
    (result.operation_id == operation_id).then_some(result)
}

fn unix_paste_results(
    frames: &[botster_hub_client::DaemonUnixTerminalFrame],
    operation_id: u64,
) -> Vec<botster_terminal_protocol::InputResultBody> {
    frames
        .iter()
        .filter_map(|frame| input_result_for_operation(&frame.body, operation_id))
        .collect()
}

fn unix_terminal_has_marker(
    frames: &[botster_hub_client::DaemonUnixTerminalFrame],
    marker: &str,
) -> bool {
    frames
        .iter()
        .any(|frame| terminal_frame_contains(&frame.body, marker))
}

/// Read every frame that arrives within `duration`. An unpaired control
/// response is a proof failure.
fn collect_unix_mux_for(
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
    duration: Duration,
) {
    client.set_read_timeout(Some(Duration::from_millis(50)));
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        match client.read_frame() {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(frame)) => frames.push(frame),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Event { event },
            )) => events.push(event),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Response { response, .. },
            )) => panic!("paste mux received an unpaired response: {response:?}"),
            Ok(_) | Err(_) => {}
        }
    }
    client.set_read_timeout(None);
}

fn collect_unix_paste_completion(
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
    operation_id: u64,
    done: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        collect_unix_mux_for(client, frames, events, Duration::from_millis(100));
        if unix_paste_results(frames, operation_id).len() == 1
            && unix_terminal_has_marker(frames, done)
        {
            collect_unix_mux_for(client, frames, events, Duration::from_millis(500));
            return;
        }
    }
    panic!(
        "paste did not complete: results={:?} done={} events={events:?}",
        unix_paste_results(frames, operation_id),
        unix_terminal_has_marker(frames, done)
    );
}

/// One admitted paste result: written, with the whole payload accepted.
fn assert_admitted_paste_result(
    results: &[botster_terminal_protocol::InputResultBody],
    operation_id: u64,
) {
    assert_eq!(results.len(), 1, "one paste input_result: {results:?}");
    assert_eq!(results[0].operation_id, operation_id, "{results:?}");
    assert_eq!(
        results[0].outcome,
        botster_terminal_protocol::InputOutcome::Written,
        "{results:?}"
    );
    assert_eq!(
        results[0].accepted_payload_bytes,
        Some(LIVE_PASTE_BYTES as u64),
        "{results:?}"
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

/// Wait for the sink's ready marker on the route and a readable mode body.
fn wait_for_unix_ready_and_mode(
    client: &mut RawUnixClient,
    session_id: &str,
    ready: &str,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) -> botster_hub_client::DaemonModeFlags {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let response = client.request_collecting(
            &botster_hub_client::DaemonRequest::ReadModeFlags {
                session_id: session_id.to_string(),
            },
            frames,
            events,
        );
        if unix_terminal_has_marker(frames, ready)
            && response.kind == botster_hub_client::DaemonResponseKind::ReadModeFlags
            && response
                .mode_flags
                .as_ref()
                .is_some_and(|flags| flags.unavailable.is_none())
        {
            return response.mode_flags.expect("mode flags body");
        }
        assert!(
            Instant::now() < deadline,
            "raw paste sink did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_sink_bytes(sink: &Path, expected: &[u8]) {
    let actual = fs::read(sink).expect("read paste sink");
    assert_eq!(actual, expected, "PTY sink must receive encoded paste content");
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
    let payload = live_paste_payload();
    let expected_sink =
        botster_core_test_support::fixtures::paste::encode_unbracketed_paste_for_pty(&payload)
            .expect("encode expected unbracketed paste");
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        session_id,
        subscription_id,
        &paste_sink_command(&sink, ready, done),
        &mut envelopes,
        &mut events,
    );
    let mode = wait_for_unix_ready_and_mode(
        &mut stream,
        session_id,
        ready,
        &mut envelopes,
        &mut events,
    );
    assert!(
        !mode.bracketed_paste,
        "raw paste sink must keep bracketed paste disabled"
    );
    envelopes.clear();
    events.clear();

    let frames = terminal_paste_frame_bytes_allowing_unsafe(&payload);
    assert_eq!(frames.len(), expected_paste_frames(&payload));
    let operation_id = stream.send_terminal_input(subscription_id, &frames[0]);
    for frame in &frames[1..] {
        stream.send_terminal_input(subscription_id, frame);
    }
    collect_unix_paste_completion(
        &mut stream,
        &mut envelopes,
        &mut events,
        operation_id,
        done,
    );

    assert_admitted_paste_result(&unix_paste_results(&envelopes, operation_id), operation_id);
    assert_no_route_close(&events, session_id, subscription_id);
    assert_sink_bytes(&sink, &expected_sink);
    let status = stream.request_collecting(
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
    let payload = live_paste_payload();
    let expected_sink =
        botster_core_test_support::fixtures::paste::encode_unbracketed_paste_for_pty(&payload)
            .expect("encode expected unbracketed paste");

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
                    command: paste_sink_command_after_go(&sink, ready, done),
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
        let label = reservation.label.clone();
        let _channel = peer
            .open_reserved_terminal(&key, &label, &webrtc_terminal_adapter_hello())
            .await
            .expect("open reserved terminal channel");
        peer.send_terminal_input(&key, &label, &terminal_input_frame_bytes(b"g"))
            .await
            .expect("release the paste sink through the bound route");

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
                && mode.unavailable.is_none()
            {
                break mode;
            }
            assert!(Instant::now() < ready_deadline, "modes did not become readable");
        };
        assert!(
            !mode.bracketed_paste,
            "raw paste sink must keep bracketed paste disabled"
        );

        let frames = terminal_paste_frame_bytes_allowing_unsafe(&payload);
        assert_eq!(frames.len(), expected_paste_frames(&payload));
        let operation_id = peer
            .send_terminal_input(&key, &label, &frames[0])
            .await
            .expect("send paste begin");
        for frame in &frames[1..] {
            peer.send_terminal_input(&key, &label, frame)
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

    assert_sink_bytes(&sink, &expected_sink);
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
