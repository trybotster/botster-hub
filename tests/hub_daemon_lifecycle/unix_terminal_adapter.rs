fn unix_adapter_connection(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> (
    std::os::unix::net::UnixStream,
    std::io::BufReader<std::os::unix::net::UnixStream>,
) {
    let stream = botster_hub_client::connect_and_hello_with_requirement(
        endpoint,
        &botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
    )
    .expect("unix adapter hello");
    let reader = std::io::BufReader::new(stream.try_clone().expect("clone stream"));
    (stream, reader)
}

fn request_skipping_envelopes(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    request: &botster_hub_client::DaemonRequest,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
) -> botster_hub_client::DaemonResponse {
    let mut events = Vec::new();
    request_collecting_mux(stream, reader, request, envelopes, &mut events)
}

fn request_collecting_mux(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    request: &botster_hub_client::DaemonRequest,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) -> botster_hub_client::DaemonResponse {
    botster_hub_client::write_frame(stream, request).expect("write request");
    loop {
        match botster_hub_client::read_unix_mux_frame_from_reader(reader).expect("read mux") {
            botster_hub_client::DaemonUnixMuxFrame::Response(response) => return *response,
            botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope) => {
                assert!(envelope.is_unix_terminal_plane());
                envelopes.push(envelope);
            }
            botster_hub_client::DaemonUnixMuxFrame::Event(event) => events.push(event),
        }
    }
}

fn unix_envelope_is_process_exit(
    envelope: &botster_hub_client::DaemonUnixTerminalEnvelope,
    session_id: &str,
    subscription_id: &str,
) -> bool {
    if envelope.session_id != session_id || envelope.subscription_id != subscription_id {
        return false;
    }
    let Ok(bytes) = envelope.payload_bytes() else {
        return false;
    };
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some("process_exit")
}

fn unix_envelope_is_attached(
    envelope: &botster_hub_client::DaemonUnixTerminalEnvelope,
    session_id: &str,
    subscription_id: &str,
) -> bool {
    if envelope.session_id != session_id || envelope.subscription_id != subscription_id {
        return false;
    }
    let Ok(bytes) = envelope.payload_bytes() else {
        return false;
    };
    serde_json::from_slice::<serde_json::Value>(&bytes).is_ok_and(|value| {
        value.get("type").and_then(serde_json::Value::as_str) == Some("attach_state")
            && value.get("state").and_then(serde_json::Value::as_str) == Some("attached")
    })
}

fn assert_host_session_retained(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
) {
    let listed = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list");
    match listed
        .sessions
        .iter()
        .find(|session| session.session_id == session_id)
        .map(|session| session.lifecycle.as_str())
    {
        None => panic!(
            "ProcessExited must not shut down the host session: {:?}",
            listed.sessions
        ),
        Some("failed") => panic!(
            "successful printf exit must not classify as failed: {:?}",
            listed.sessions
        ),
        Some("running" | "exited") => {}
        Some(lifecycle) => panic!(
            "host session lifecycle {lifecycle} is not running or exited: {:?}",
            listed.sessions
        ),
    }
}

fn unix_envelope_contains_live_bytes(
    envelopes: &[botster_hub_client::DaemonUnixTerminalEnvelope],
    marker: &str,
) -> bool {
    envelopes.iter().any(|envelope| {
        let Ok(bytes) = envelope.payload_bytes() else {
            return false;
        };
        if bytes
            .windows(marker.len())
            .any(|window| window == marker.as_bytes())
        {
            return true;
        }
        let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes) else {
            return false;
        };
        match event {
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
                live_output_contains(&payload, marker)
            }
            _ => false,
        }
    })
}

fn event_is_terminal_body(event: &botster_hub_client::DaemonEvent) -> bool {
    matches!(
        event,
        botster_hub_client::DaemonEvent::AttachState { .. }
            | botster_hub_client::DaemonEvent::Snapshot { .. }
            | botster_hub_client::DaemonEvent::Scrollback { .. }
            | botster_hub_client::DaemonEvent::TerminalOutput { .. }
            | botster_hub_client::DaemonEvent::ProcessExit { .. }
    )
}

fn opaque_terminal_bytes(
    envelopes: &[botster_hub_client::DaemonUnixTerminalEnvelope],
) -> Vec<u8> {
    let mut output = Vec::new();
    for envelope in envelopes {
        let Ok(bytes) = envelope.payload_bytes() else {
            continue;
        };
        match serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes) {
            Ok(botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }) => {
                if let Ok(decoded) = payload.decoded_bytes() {
                    output.extend_from_slice(&decoded);
                }
            }
            _ => output.extend_from_slice(&bytes),
        }
    }
    output
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn read_unsolicited_terminal_until(
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    deadline: Instant,
    marker: &str,
) {
    reader
        .get_ref()
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("set unsolicited terminal timeout");
    while Instant::now() < deadline && !unix_envelope_contains_live_bytes(envelopes, marker) {
        match botster_hub_client::read_unix_mux_frame_from_reader(reader) {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                envelopes.push(envelope);
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Event(_)) => {}
            Ok(botster_hub_client::DaemonUnixMuxFrame::Response(response)) => {
                panic!("unsolicited terminal wait received a control response: {response:?}")
            }
            Err(_) => {}
        }
    }
}

#[test]
fn unix_adapter_bind_returns_only_attaching_then_opaque_envelopes() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uab");
    let endpoint = hub.endpoint().clone();
    let session_id = "uab-session";
    let subscription_id = "uab-sub";
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();

    let spawned = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'unix-adapter-ready\\n'; sleep 30".to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let attach = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.terminal_reservation.is_none(),
        "Unix Attach must omit terminal_reservation: {:?}",
        attach.terminal_reservation
    );
    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );

    let deadline = Instant::now() + Duration::from_secs(8);
    while envelopes.is_empty() && Instant::now() < deadline {
        let drain = request_skipping_envelopes(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes,
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !event_is_terminal_body(event)),
            "bound drain must not emit terminal bodies: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !envelopes.is_empty(),
        "later frames must arrive as opaque adapter envelopes"
    );
    for envelope in &envelopes {
        assert!(envelope.is_unix_terminal_plane());
        assert!(envelope.payload_bytes().expect("payload decodes").len() > 1);
    }

    let listed = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes,
    );
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "host session stays listed after bind"
    );

    eprintln!(
        "unix adapter bind provenance hub_bin={} session_worker={}",
        env!("CARGO_BIN_EXE_botster-hub"),
        session_worker_binary_path().display()
    );

    let before = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status before bound disconnect")
        .status
        .expect("status body")
        .lifecycle_counters;
    drop(stream);
    let leftover =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list after disconnect");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut counters = before.clone();
    while Instant::now() < deadline {
        let status =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after bound disconnect");
        counters = status.status.expect("status body").lifecycle_counters;
        let bound_closes = counters
            .cleanup_by_reason
            .get("bound_adapter_close")
            .copied()
            .unwrap_or(0);
        let before_closes = before
            .cleanup_by_reason
            .get("bound_adapter_close")
            .copied()
            .unwrap_or(0);
        if bound_closes > before_closes || counters.cleanup_completed > before.cleanup_completed {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let bound_closes = counters
        .cleanup_by_reason
        .get("bound_adapter_close")
        .copied()
        .unwrap_or(0)
        .saturating_sub(
            before
                .cleanup_by_reason
                .get("bound_adapter_close")
                .copied()
                .unwrap_or(0),
        );
    let cleanup_detaches = counters
        .cleanup_by_reason
        .get("cleanup_hub_detach")
        .copied()
        .unwrap_or(0)
        .saturating_sub(
            before
                .cleanup_by_reason
                .get("cleanup_hub_detach")
                .copied()
                .unwrap_or(0),
        );
    let explicit = counters
        .cleanup_by_reason
        .get("explicit_detach")
        .copied()
        .unwrap_or(0)
        .saturating_sub(
            before
                .cleanup_by_reason
                .get("explicit_detach")
                .copied()
                .unwrap_or(0),
        );
    assert!(
        bound_closes >= 1 || cleanup_detaches == 0,
        "bound socket death must close the adapter or at least omit Hub Detach: closes={bound_closes} detaches={cleanup_detaches} before={before:?} after={counters:?}"
    );
    assert_eq!(
        cleanup_detaches, 0,
        "bound socket death must not issue Hub Detach: before={before:?} after={counters:?}"
    );
    assert_eq!(
        explicit, 0,
        "bound socket death must not use the authorized Detach path: before={before:?} after={counters:?}"
    );
    assert!(
        leftover
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "connection death must not shut down the host session"
    );

    let (mut replacement, mut replacement_reader) = unix_adapter_connection(&endpoint);
    let mut replacement_envelopes = Vec::new();
    let reattach = request_skipping_envelopes(
        &mut replacement,
        &mut replacement_reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut replacement_envelopes,
    );
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        reattach.events.is_empty(),
        "adapter close on disconnect is the one Core detach; replacement attach is admitted with empty bodies: {:?}",
        reattach.events
    );
    drop(replacement);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_unbound_scoped_drain_delivers_terminal_output() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uud");
    let endpoint = hub.endpoint().clone();
    let session_id = "uud-session";
    let subscription_id = "uud-sub";
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'unbound-drain-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("default Hello attach");
    assert!(
        attach.events.is_empty(),
        "Attach has no terminal bodies: {:?}",
        attach.events
    );
    let drain = connection
        .request(&botster_hub_client::DaemonRequest::drain_subscription(
            session_id,
            subscription_id,
        ))
        .expect("host drain");
    assert!(
        drain.events.is_empty(),
        "host Drain must not translate Snapshot: {:?}",
        drain.events
    );
    connection
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_input_frame_bytes(b"from-unbound\r"),
        )
        .expect("send");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut text = String::new();
    while Instant::now() < deadline {
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .expect("read screen");
        text = screen
            .read_screen
            .as_ref()
            .map(|screen| screen.text.clone())
            .unwrap_or_default();
        if text.contains("echo:from-unbound") {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        text.contains("echo:from-unbound"),
        "visible echo is on ReadScreen after always-bind: {text:?}"
    );
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn paused_data_plane_keeps_control_requests_from_driving_terminal_progress() {
    let _guard = daemon_test_guard();
    let seam_dir = unique_short_test_dir("data-plane-pause");
    fs::create_dir_all(&seam_dir).expect("create data-plane pause seam directory");
    let pause = seam_dir.join("pause");
    let pause_value = pause.display().to_string();
    let hub = start_isolated_live_output_hub_with_env(
        "data-plane-pause",
        &[("BOTSTER_HUB_TEST_PAUSE_DATA_PLANE", pause_value.as_str())],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "data-plane-pause-session";
    let subscription_id = "data-plane-pause-sub";
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        session_id,
        subscription_id,
        "printf 'pause-baseline-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut envelopes,
        &mut events,
    );
    let baseline_deadline = Instant::now() + Duration::from_secs(5);
    while !unix_envelope_contains_live_bytes(&envelopes, "pause-baseline-ready")
        || !envelopes
            .iter()
            .any(|envelope| unix_envelope_is_attached(envelope, session_id, subscription_id))
    {
        assert!(
            Instant::now() < baseline_deadline,
            "baseline terminal output must arrive before the pause"
        );
        request_collecting_mux(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::Status,
            &mut envelopes,
            &mut events,
        );
    }
    envelopes.clear();
    fs::write(&pause, b"pause").expect("arm data-plane pause");
    let entered = pause.with_extension("entered");
    let entered_deadline = Instant::now() + Duration::from_secs(3);
    while !entered.is_file() {
        assert!(
            Instant::now() < entered_deadline,
            "data-plane driver must acknowledge the pause"
        );
        thread::sleep(Duration::from_millis(10));
    }

    botster_hub_client::write_frame(
        &mut stream,
        &botster_hub_client::DaemonUnixTerminalEnvelope::from_frame_bytes(
            session_id,
            subscription_id,
            &terminal_input_frame_bytes(b"retained-one\rretained-two\r"),
        ),
    )
    .expect("send retained compact terminal input");
    thread::sleep(Duration::from_millis(100));

    let requests = [
        botster_hub_client::DaemonRequest::Status,
        botster_hub_client::DaemonRequest::ListSessions,
        botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.to_string(),
        },
        botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: session_id.to_string(),
        },
        botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: session_id.to_string(),
        },
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "data-plane-pause-missing".to_string(),
        },
    ];
    for request in requests {
        request_collecting_mux(
            &mut stream,
            &mut reader,
            &request,
            &mut envelopes,
            &mut events,
        );
        assert!(
            envelopes.is_empty(),
            "generic control and readback must not drive terminal progress: request={request:?} envelopes={envelopes:?}"
        );
    }

    fs::remove_file(&pause).expect("resume data-plane driver");
    read_unsolicited_terminal_until(
        &mut reader,
        &mut envelopes,
        Instant::now() + Duration::from_secs(5),
        "echo:retained-two",
    );
    let bytes = opaque_terminal_bytes(&envelopes);
    let first = find_bytes(&bytes, b"echo:retained-one").expect("first retained frame delivered");
    let second =
        find_bytes(&bytes, b"echo:retained-two").expect("second retained frame delivered");
    assert!(first < second, "retained terminal input must preserve order");

    drop(stream);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(seam_dir);
}

#[test]
fn unix_writable_wake_resumes_output_before_the_watchdog() {
    let _guard = daemon_test_guard();
    let observation = unique_short_test_dir("writable-wake");
    fs::create_dir_all(&observation).expect("create writable-wake observation directory");
    let observation_value = observation.display().to_string();
    let driver_observation = observation.join("driver.json");
    let driver_observation_value = driver_observation.display().to_string();
    let hub = start_isolated_live_output_hub_with_env(
        "writable-wake",
        &[
            (
                "BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION",
                "writable-wake-session",
            ),
            ("BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_DELAY_MS", "500"),
            (
                "BOTSTER_HUB_TEST_CLEAR_ADAPTER_WOULD_BLOCK_AFTER_REJECTION",
                "1",
            ),
            (
                "BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_OBSERVATION",
                observation_value.as_str(),
            ),
            ("BOTSTER_HUB_TEST_DATA_PLANE_WATCHDOG_MS", "10000"),
            (
                "BOTSTER_HUB_TEST_DATA_PLANE_OBSERVATION",
                driver_observation_value.as_str(),
            ),
        ],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "writable-wake-session";
    let subscription_id = "writable-wake-sub";
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    let started = Instant::now();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        session_id,
        subscription_id,
        "sleep 1; printf 'writable-wake-resumed\\n'; sleep 30",
        &mut envelopes,
        &mut events,
    );
    envelopes.clear();
    read_unsolicited_terminal_until(
        &mut reader,
        &mut envelopes,
        Instant::now() + Duration::from_secs(5),
        "writable-wake-resumed",
    );
    assert!(
        observation.join("would_block").is_file(),
        "the route must enter WouldBlock before delivery resumes"
    );
    assert!(
        observation.join("writable").is_file(),
        "clearing pressure must emit the writable transition"
    );
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "writable-wake-resumed"),
        "the writable wake must resume opaque terminal delivery: driver={:?}",
        fs::read_to_string(&driver_observation)
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "delivery must precede the ten-second data-plane watchdog"
    );

    drop(stream);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_dir_all(observation);
}

#[test]
fn unix_adapter_explicit_detach_is_separate_from_connection_death() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uad");
    let endpoint = hub.endpoint().clone();
    let session_id = "uad-session";
    let subscription_id = "uad-sub";
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();

    request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        },
        &mut envelopes,
    );
    let attach = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );

    let detach = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
    let status = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
    );
    let counters = status.status.expect("status body").lifecycle_counters;
    assert_eq!(
        counters.cleanup_by_reason.get("explicit_detach").copied(),
        Some(1),
        "explicit Detach must use the authorized path: {counters:?}"
    );
    assert_eq!(
        counters
            .cleanup_by_reason
            .get("bound_adapter_close")
            .copied(),
        None,
        "explicit Detach must not use bound socket-death cleanup: {counters:?}"
    );

    let second = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_ne!(
        second.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );

    let listed = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes,
    );
    assert!(listed
        .sessions
        .iter()
        .any(|session| session.session_id == session_id));

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_detach_retires_close_work_to_the_live_route_baseline() {
    let _guard = daemon_test_guard();
    let observation = unique_short_test_dir("close-work-observation");
    let observation_value = observation.display().to_string();
    let hub = start_isolated_live_output_hub_with_env(
        "close-work-retire",
        &[(
            "BOTSTER_HUB_TEST_DATA_PLANE_OBSERVATION",
            observation_value.as_str(),
        )],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "close-work-session";
    let subscription_id = "close-work-sub";
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();

    request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        },
        &mut envelopes,
    );
    request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    wait_for_live_close_routes(&observation, 1);

    request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    wait_for_live_close_routes(&observation, 0);

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
    let _ = fs::remove_file(observation);
}

fn wait_for_live_close_routes(path: &Path, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(body) = fs::read_to_string(path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&body)
            && value["live_close_routes"].as_u64() == Some(expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "data-plane close registry must reach {expected} live routes"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn unix_adapter_stale_disconnect_does_not_cancel_replacement_owner() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uso");
    let endpoint = hub.endpoint().clone();
    let session_id = "uso-session";
    let subscription_id = "uso-sub";

    let (mut owner_a, mut reader_a) = unix_adapter_connection(&endpoint);
    let mut envelopes_a = Vec::new();
    request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
        &mut envelopes_a,
    );
    let attach_a = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(
        attach_a.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        attach_a.events.is_empty(),
        "owner A Attach must not return terminal bodies: {:?}",
        attach_a.events
    );
    let detach_a = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(
        detach_a.kind,
        botster_hub_client::DaemonResponseKind::Events
    );

    let (mut owner_b, mut reader_b) = unix_adapter_connection(&endpoint);
    let mut envelopes_b = Vec::new();
    let attach_b = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_b,
    );
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        attach_b.events.is_empty(),
        "replacement owner B must bind the same key with empty bodies: {:?}",
        attach_b.events
    );

    let before = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes_b,
    )
    .status
    .expect("status body")
    .lifecycle_counters;
    drop(owner_a);
    drop(reader_a);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut after = before.clone();
    while Instant::now() < deadline {
        let status = request_skipping_envelopes(
            &mut owner_b,
            &mut reader_b,
            &botster_hub_client::DaemonRequest::Status,
            &mut envelopes_b,
        );
        after = status.status.expect("status body").lifecycle_counters;
        if after.cleanup_completed > before.cleanup_completed {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        after
            .cleanup_completed
            .saturating_sub(before.cleanup_completed),
        1,
        "A's disconnect must complete Hub cleanup exactly once before sibling-survival checks: before={before:?} after={after:?}"
    );
    let stale_closes = after
        .cleanup_by_reason
        .get("bound_adapter_close")
        .copied()
        .unwrap_or(0)
        .saturating_sub(
            before
                .cleanup_by_reason
                .get("bound_adapter_close")
                .copied()
                .unwrap_or(0),
        );
    assert_eq!(
        stale_closes, 0,
        "A's disconnect must not close B's bound route: before={before:?} after={after:?}"
    );

    write_unix_terminal_frame(
        &mut owner_b,
        session_id,
        subscription_id,
        &terminal_input_frame_bytes(b"after-a-drop\r"),
    );
    let marker = "echo:after-a-drop";
    let output_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < output_deadline
        && !unix_envelope_contains_live_bytes(&envelopes_b, marker)
    {
        let drain = request_skipping_envelopes(
            &mut owner_b,
            &mut reader_b,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes_b,
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !event_is_terminal_body(event)),
            "B's scoped Drain must stay bound after A's disconnect; terminal bodies mean Hub cancelled B: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes_b, marker),
        "B must keep receiving opaque adapter frames after A disconnects: {envelopes_b:?}"
    );
    let confirm = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
        &mut envelopes_b,
    );
    assert!(
        confirm
            .events
            .iter()
            .all(|event| !event_is_terminal_body(event)),
        "B's scoped Drain must stay bound after live echo; terminal bodies mean Hub cancelled B: {:?}",
        confirm.events
    );
    let occupancy = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes_b,
    )
    .status
    .expect("status after replacement-owner cleanup")
    .live_attach_occupancy;
    assert!(
        occupancy
            .iter()
            .any(|row| { row.session_id == session_id && row.subscription_id == subscription_id }),
        "replacement owner occupancy must keep B's pair: {occupancy:?}"
    );

    drop(owner_b);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_unbound_attach_still_drains_snapshot() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uau");
    let endpoint = hub.endpoint().clone();
    let session_id = "uau-session";
    let subscription_id = "uau-sub";
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        })
        .expect("spawn");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("default Hello attach");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.events.is_empty(),
        "default Hello Attach binds without terminal bodies: {:?}",
        attach.events
    );
    let drain = connection
        .request(&botster_hub_client::DaemonRequest::drain_subscription(
            session_id,
            subscription_id,
        ))
        .expect("host drain");
    assert!(
        drain.events.is_empty(),
        "host Drain must not reconstruct Snapshot: {:?}",
        drain.events
    );

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_unbound_printf_stream_attach_completes() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uap");
    let endpoint = hub.endpoint().clone();
    let session_id = "uap-session";
    let subscription_id = "uap-sub";
    let marker = "botster-smoke-terminal-ok";
    let release_path = hub.data_dir().join("uap-release");
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");
    let spawned = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "while [ ! -e '{}' ]; do sleep 0.01; done; printf 'smoke:{marker}\\n'",
                release_path.display()
            ),
        })
        .expect("spawn printf");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), session_id);

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("default hello attach");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.events.is_empty(),
        "default Hello Attach binds without terminal bodies: {:?}",
        attach.events
    );
    fs::write(&release_path, b"go").expect("release unbound printf");
    let needle = format!("smoke:{marker}");
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut text = String::new();
    while Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::drain_subscription(
                session_id,
                subscription_id,
            ))
            .expect("host drain");
        assert!(
            drain.events.is_empty(),
            "host Drain must not return terminal bodies: {:?}",
            drain.events
        );
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .expect("read screen");
        text = screen
            .read_screen
            .as_ref()
            .map(|screen| screen.text.clone())
            .unwrap_or_default();
        if text.contains(&needle) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        text.contains(&needle),
        "visible text is on ReadScreen: {text:?}"
    );
    assert_host_session_retained(&mut connection, session_id);
    let drain = connection
        .request(&botster_hub_client::DaemonRequest::drain_subscription(
            session_id,
            subscription_id,
        ))
        .expect("host drain");
    assert_eq!(
        drain.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "host Drain must stay serviceable after exit: {drain:?}"
    );
    assert!(
        drain.events.is_empty(),
        "host Drain must not return terminal bodies: {:?}",
        drain.events
    );

    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_bound_printf_stream_attach_delivers_process_exit() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uapb");
    let endpoint = hub.endpoint().clone();
    let session_id = "uapb-session";
    let subscription_id = "uapb-sub";
    let marker = "botster-smoke-terminal-ok";
    let release_path = hub.data_dir().join("uapb-release");
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");
    // The release file holds the child until Attach and the first Drain
    // complete. sleep 1 after printf keeps the bound adapter attached while
    // Core emits process_exit; it is not an attach deadline.
    let spawned = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "while [ ! -e '{}' ]; do sleep 0.01; done; printf 'smoke:{marker}\\n'; sleep 1",
                release_path.display()
            ),
        })
        .expect("spawn held printf");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), session_id);

    let (mut term_stream, mut term_reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let term_attach = request_skipping_envelopes(
        &mut term_stream,
        &mut term_reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(
        term_attach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        term_attach.events.is_empty(),
        "unix adapter Attach must bind without terminal bodies: {:?}",
        term_attach.events
    );
    let primed = request_skipping_envelopes(
        &mut term_stream,
        &mut term_reader,
        &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
        &mut envelopes,
    );
    assert!(
        primed.events.is_empty(),
        "host Drain must not return terminal bodies: {:?}",
        primed.events
    );
    fs::write(&release_path, b"go").expect("release held printf");
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < exit_deadline {
        if envelopes
            .iter()
            .any(|envelope| unix_envelope_is_process_exit(envelope, session_id, subscription_id))
        {
            break;
        }
        let drain = request_skipping_envelopes(
            &mut term_stream,
            &mut term_reader,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes,
        );
        assert!(
            drain.events.is_empty(),
            "host Drain must not return terminal bodies: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        envelopes
            .iter()
            .any(|envelope| unix_envelope_is_process_exit(envelope, session_id, subscription_id)),
        "attached terminal subscription must deliver process_exit: {envelopes:?}"
    );
    assert_host_session_retained(&mut connection, session_id);
    let text =
        wait_for_read_screen_contains(&mut connection, session_id, &format!("smoke:{marker}"));
    assert!(
        text.contains(&format!("smoke:{marker}")),
        "visible text is on ReadScreen: {text:?}"
    );
    let drain = request_skipping_envelopes(
        &mut term_stream,
        &mut term_reader,
        &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
        &mut envelopes,
    );
    assert_eq!(
        drain.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "host Drain must stay serviceable after exit: {drain:?}"
    );
    assert!(
        drain.events.is_empty(),
        "host Drain must not return terminal bodies: {:?}",
        drain.events
    );

    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_always_bind_stream_attach_restores_current_screen() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("usa");
    let endpoint = hub.endpoint().clone();
    let session_id = "usa-session";
    let subscription_id = "usa-sub";
    let late = "late-stream-attach";
    let ready_dir = unique_short_test_dir("usa-ready");
    let ready_path = ready_dir.join("ready");
    fs::create_dir_all(&ready_dir).expect("create late-marker ready dir");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "printf 'pre-attach\\n'; printf '{late}\\n'; printf x > {}; sleep 30",
                ready_path.display()
            ),
        },
    )
    .expect("spawn writer that publishes a ready file after the late marker");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !ready_path.exists() {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists(),
        "child must create the ready file after printing {late}"
    );
    let screen_deadline = Instant::now() + Duration::from_secs(5);
    let mut screen_text = String::new();
    while Instant::now() < screen_deadline {
        screen_text = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            },
        )
        .ok()
        .and_then(|response| response.read_screen.map(|screen| screen.text))
        .unwrap_or_default();
        if screen_text.contains(late) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        screen_text.contains(late),
        "host ReadScreen must contain the late marker before stream_attach: {screen_text:?}"
    );
    let mut output = Vec::new();
    botster_hub_client::stream_attach(&endpoint, session_id, subscription_id, &mut output)
        .expect("stream_attach");
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains(late),
        "always-bind stream_attach restores current ReadScreen text: {text:?}"
    );

    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_feature_does_not_raise_default_requirement() {
    let requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    let mut previous = botster_hub_client::DaemonCompatibility::current();
    previous
        .features
        .retain(|feature| feature != botster_hub_client::FEATURE_UNIX_TERMINAL_ADAPTER);
    previous.conformance_fixture_revision =
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::ensure_compatible(&requirement, &previous)
        .expect("default clients still accept a daemon without the unix adapter feature");

    let adapter_requirement =
        botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter();
    botster_hub_client::ensure_compatible(&adapter_requirement, &previous)
        .expect_err("the unix adapter requirement must fail closed without the feature");
    assert_eq!(botster_hub_client::PROTOCOL_VERSION, 8);
}

fn spawn_and_bind(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    session_id: &str,
    subscription_id: &str,
    command: &str,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) {
    let spawned = request_collecting_mux(
        stream,
        reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: command.to_string(),
        },
        envelopes,
        events,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "spawn must succeed for {session_id}: error={:?}",
        spawned.error
    );
    let attach = request_collecting_mux(
        stream,
        reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        envelopes,
        events,
    );
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.terminal_reservation.is_none(),
        "Unix Attach must omit terminal_reservation: {:?}",
        attach.terminal_reservation
    );
    assert!(
        attach.events.is_empty(),
        "bind must return empty Attach bodies: {:?}",
        attach.events
    );
}

fn wait_for_subscription_closed(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    session_id: &str,
    subscription_id: &str,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id: closed_session,
                    subscription_id: closed_subscription,
                    ..
                } if closed_session == session_id && closed_subscription == subscription_id
            )
        }) {
            stream.set_read_timeout(None).expect("clear read timeout");
            return true;
        }
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("read timeout");
        match botster_hub_client::read_unix_mux_frame_from_reader(reader) {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                assert!(envelope.is_unix_terminal_plane());
                envelopes.push(envelope);
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Event(event)) => events.push(event),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Response(_)) => {}
            Err(_) => {
                stream.set_read_timeout(None).expect("clear read timeout");
                let _ = request_collecting_mux(
                    stream,
                    reader,
                    &botster_hub_client::DaemonRequest::Status,
                    envelopes,
                    events,
                );
            }
        }
    }
    false
}

#[test]
fn hello_ack_advertises_independent_terminal_compatibility() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("htc");
    let (stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
        hub.endpoint(),
        &botster_hub_client::DaemonCompatibilityRequirement::current(),
        None,
    )
    .expect("hello");
    drop(stream);
    let terminal = ack
        .terminal_compatibility
        .expect("HelloAck must advertise terminal compatibility");
    assert_eq!(terminal.protocol, botster_terminal_protocol::PROTOCOL);
    assert_eq!(
        terminal.protocol_version,
        botster_terminal_protocol::PROTOCOL_VERSION
    );
    assert_ne!(terminal.protocol, ack.compatibility.protocol);
    assert!(ack
        .compatibility
        .supports_feature(botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED));
    assert!(
        !botster_hub_client::DaemonCompatibilityRequirement::current()
            .required_features
            .iter()
            .any(|feature| feature == botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn tui_shaped_hello_status_succeeds_without_host_terminal_tokens() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("tui-hello");
    let requirement = botster_hub_client::DaemonCompatibilityRequirement {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        protocol_version: botster_hub_client::PROTOCOL_VERSION,
        required_features: vec![
            botster_hub_client::FEATURE_SESSIONS.to_string(),
            botster_hub_client::FEATURE_PACKAGE_NAVIGATION.to_string(),
            botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER.to_string(),
            botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION.to_string(),
            botster_hub_client::FEATURE_TERMINAL_READBACK.to_string(),
            botster_hub_client::FEATURE_SESSION_ENTITY_SUBSCRIPTIONS.to_string(),
            botster_hub_client::FEATURE_UNIX_TERMINAL_ADAPTER.to_string(),
            botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED.to_string(),
        ],
        minimum_conformance_fixture_revision: 40,
        client_name: "botster-tui".to_string(),
    };
    let terminal =
        botster_terminal_protocol::TerminalCompatibilityRequirement::for_ready_then_history_attach(
        );
    let (_stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
        hub.endpoint(),
        &requirement,
        Some(&terminal),
    )
    .expect("TUI-shaped Hello must succeed");
    assert!(
        !requirement.required_features.iter().any(|feature| feature
            == botster_terminal_protocol::FEATURE_TERMINAL_STREAMING
            || feature == botster_terminal_protocol::FEATURE_RESIZE),
        "TUI-shaped host Hello must not require terminal mechanism tokens"
    );
    assert!(ack.terminal_compatibility.is_some());
    let status = botster_hub_client::request_with_requirement(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::Status,
        &requirement,
    )
    .expect("TUI-shaped Status after Hello");
    assert_eq!(
        status.kind,
        botster_hub_client::DaemonResponseKind::Status,
        "TUI-shaped Status must succeed, got {:?}",
        status.error
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn mismatched_terminal_hello_rejects_attach_before_core_ownership() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("htm");
    let endpoint = hub.endpoint().clone();
    let mut terminal = botster_terminal_protocol::TerminalCompatibilityRequirement::current();
    terminal.protocol_version = terminal.protocol_version.saturating_add(1);
    terminal.client_name = "mismatch-client".to_string();
    let (mut stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
        &endpoint,
        &botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
        Some(&terminal),
    )
    .expect("mismatched terminal hello still returns a host connection");
    assert!(
        ack.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
        }),
        "HelloAck must carry a typed terminal diagnostic: {:?}",
        ack.diagnostics
    );
    let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: "htm-session".to_string(),
            command: "sleep 30".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    let attach = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "htm-session".to_string(),
            subscription_id: "htm-sub".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = attach.error.expect("operator error");
    assert_eq!(error.code, "terminal_compatibility");
    assert_eq!(error.operation, "attach");
    assert!(
        !attach
            .events
            .iter()
            .any(|event| matches!(event, botster_hub_client::DaemonEvent::AttachState { .. })),
        "rejected attach must not emit AttachFailed: {:?}",
        attach.events
    );
    let status = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(
        envelopes.is_empty(),
        "rejected attach must not bind: {envelopes:?}"
    );
    drop(stream);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn host_adapter_close_emits_terminal_subscription_closed_for_one_route() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("hac");
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "hac-a",
        "sub-a",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "hac-b",
        "sub-b",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let reattach = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "hac-a".to_string(),
            subscription_id: "sub-a".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        wait_for_subscription_closed(
            &mut stream,
            &mut reader,
            "hac-a",
            "sub-a",
            &mut envelopes,
            &mut events,
        ),
        "host close of generation N must emit TerminalSubscriptionClosed: {events:?}"
    );
    let closed = events
        .iter()
        .find_map(|event| match event {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                generation,
                reason,
            } if session_id == "hac-a" && subscription_id == "sub-a" => {
                Some((*generation, reason.clone()))
            }
            _ => None,
        })
        .expect("closed event");
    assert_eq!(
        closed.1,
        botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
    );
    assert!(closed.0 >= 1);
    let sibling = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(sibling.kind, botster_hub_client::DaemonResponseKind::Status);
    let listed = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes,
        &mut events,
    );
    assert!(listed
        .sessions
        .iter()
        .any(|session| session.session_id == "hac-b"));
    shutdown_short_lived_session(hub.endpoint(), "hac-a");
    shutdown_short_lived_session(hub.endpoint(), "hac-b");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn core_write_budget_hard_stop_emits_core_adapter_closed() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub_with_env(
        "cwb",
        &[
            (
                "BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION",
                "cwb-stall",
            ),
            (
                "BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_DELAY_MS",
                "500",
            ),
        ],
    );
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "cwb-live",
        "sub-live",
        "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut envelopes,
        &mut events,
    );
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "cwb-stall",
        "sub-stall",
        "sleep 3; exec yes write-budget-stall",
        &mut envelopes,
        &mut events,
    );

    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    let mut pre_close_status = None;
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("read timeout");
    while Instant::now() < deadline {
        let stall_closed = events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    ..
                } if session_id == "cwb-stall"
            )
        });
        let pressure_started = envelopes
            .iter()
            .any(|envelope| envelope.session_id == "cwb-stall")
            || started.elapsed() >= Duration::from_millis(200);
        if pre_close_status.is_none() && pressure_started && !stall_closed {
            stream.set_read_timeout(None).expect("clear read timeout");
            let stall_drain = request_collecting_mux(
                &mut stream,
                &mut reader,
                &botster_hub_client::DaemonRequest::drain_subscription("cwb-stall", "sub-stall"),
                &mut envelopes,
                &mut events,
            );
            assert_ne!(
                stall_drain.kind,
                botster_hub_client::DaemonResponseKind::OperatorError,
                "stalled adapter Drain must stay owned before Status: {:?}",
                stall_drain.error
            );
            assert!(
                stall_drain
                    .events
                    .iter()
                    .all(|event| !event_is_terminal_body(event)),
                "content-blind stall Drain must stay bound before Status: {:?}",
                stall_drain.events
            );
            assert!(
                events.iter().all(|event| {
                    !matches!(
                        event,
                        botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                            session_id,
                            ..
                        } if session_id == "cwb-stall"
                    )
                }),
                "owned stall Drain must precede core_adapter_closed: {events:?}"
            );
            let status = request_collecting_mux(
                &mut stream,
                &mut reader,
                &botster_hub_client::DaemonRequest::Status,
                &mut envelopes,
                &mut events,
            );
            assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
            assert!(
                events.iter().all(|event| {
                    !matches!(
                        event,
                        botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                            session_id,
                            ..
                        } if session_id == "cwb-stall"
                    )
                }),
                "pre-close Status must arrive before core_adapter_closed: {events:?}"
            );
            pre_close_status = Some(status);
            stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .expect("read timeout");
        }
        if stall_closed {
            break;
        }
        match botster_hub_client::read_unix_mux_frame_from_reader(&mut reader) {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                assert!(envelope.is_unix_terminal_plane());
                envelopes.push(envelope);
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Event(event)) => events.push(event),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Response(_)) => {}
            Err(_) => {}
        }
    }
    stream.set_read_timeout(None).expect("clear read timeout");

    assert!(
        pre_close_status.is_some(),
        "must send Status after pressure starts and before core_adapter_closed"
    );
    let stall_closes: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                reason,
                ..
            } if session_id == "cwb-stall" && subscription_id == "sub-stall" => {
                Some(reason.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        stall_closes.as_slice(),
        [botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER],
        "exact core_adapter_closed required: {events:?}"
    );
    assert!(
        events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    reason,
                    ..
                } if session_id == "cwb-stall"
                    && reason == botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
            )
        }),
        "host_adapter_closed is not the Core write-budget oracle: {events:?}"
    );

    let status = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let listed = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes,
        &mut events,
    );
    assert!(listed
        .sessions
        .iter()
        .any(|session| session.session_id == "cwb-live" && session.lifecycle == "running"));

    write_unix_terminal_frame(
        &mut stream,
        "cwb-live",
        "sub-live",
        &terminal_input_frame_bytes(b"cwb-sibling-live\r"),
    );
    let sibling_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < sibling_deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "echo:cwb-sibling-live")
    {
        let drain = request_collecting_mux(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription("cwb-live", "sub-live"),
            &mut envelopes,
            &mut events,
        );
        assert_ne!(
            drain.kind,
            botster_hub_client::DaemonResponseKind::OperatorError,
            "sibling scoped Drain must stay owned: {:?}",
            drain.error
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !event_is_terminal_body(event)),
            "content-blind sibling Drain must stay bound: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "echo:cwb-sibling-live"),
        "same-connection sibling must produce a new terminal envelope: {envelopes:?}"
    );

    eprintln!(
        "core_write_budget provenance hub_bin={} session_worker={} hub_sha={} locked_core=786f61c5aeec42b416826af6ca0b4be9f3cc3c0f",
        env!("CARGO_BIN_EXE_botster-hub"),
        session_worker_binary_path().display(),
        option_env!("BOTSTER_HUB_GIT_SHA").unwrap_or("worktree")
    );

    shutdown_short_lived_session(hub.endpoint(), "cwb-stall");
    shutdown_short_lived_session(hub.endpoint(), "cwb-live");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn subscribe_entities_on_bound_unix_mux_returns_operator_error_and_keeps_route() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("sem");
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "sem-live",
        "sub-live",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );

    let subscribe = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "sem-entities".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        subscribe.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "SubscribeEntities on a bound Unix mux must fail closed: {subscribe:?}"
    );
    assert_eq!(
        subscribe.error.as_ref().map(|error| error.code.as_str()),
        Some("unix_mux_owns_connection")
    );

    let status = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let drain = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::drain_subscription("sem-live", "sub-live"),
        &mut envelopes,
        &mut events,
    );
    assert_ne!(
        drain.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "bound Drain must stay owned after rejected SubscribeEntities: {:?}",
        drain.error
    );
    assert!(
        drain
            .events
            .iter()
            .all(|event| !event_is_terminal_body(event)),
        "content-blind Drain must stay bound: {:?}",
        drain.events
    );

    shutdown_short_lived_session(hub.endpoint(), "sem-live");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn failed_remove_session_does_not_suppress_later_core_close() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("frm");
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "frm-stall",
        "sub-stall",
        "yes remove-session-still-live",
        &mut envelopes,
        &mut events,
    );
    let removed = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "frm-stall".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        removed.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        removed.error.as_ref().map(|error| error.code.as_str()),
        Some("session_not_terminal")
    );
    thread::sleep(Duration::from_secs(2));
    assert!(
        wait_for_subscription_closed(
            &mut stream,
            &mut reader,
            "frm-stall",
            "sub-stall",
            &mut envelopes,
            &mut events,
        ),
        "failed RemoveSession must not suppress later Core hard-stop: {events:?}"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
            session_id,
            reason,
            ..
        } if session_id == "frm-stall"
            && reason == botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER
    )));
    shutdown_short_lived_session(hub.endpoint(), "frm-stall");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn connection_death_and_detach_do_not_emit_terminal_subscription_closed() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("cdn");
    let endpoint = hub.endpoint().clone();
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "cdn-session",
        "cdn-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let detach = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: "cdn-session".to_string(),
            subscription_id: "cdn-sub".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { .. }
            )
        }),
        "explicit Detach must not emit TerminalSubscriptionClosed: {events:?}"
    );
    drop(stream);
    drop(reader);
    let (mut replacement, mut replacement_reader) = unix_adapter_connection(&endpoint);
    let mut replacement_events = Vec::new();
    let mut replacement_envelopes = Vec::new();
    spawn_and_bind(
        &mut replacement,
        &mut replacement_reader,
        "cdn-death",
        "cdn-death-sub",
        "sleep 30",
        &mut replacement_envelopes,
        &mut replacement_events,
    );
    drop(replacement);
    thread::sleep(Duration::from_millis(200));
    assert!(
        replacement_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { .. }
            )
        }),
        "connection death must not emit TerminalSubscriptionClosed"
    );
    shutdown_short_lived_session(&endpoint, "cdn-session");
    shutdown_short_lived_session(&endpoint, "cdn-death");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("pex");
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "pex-exit",
        "sub-exit",
        "printf 'done\\n'",
        &mut envelopes,
        &mut events,
    );
    let mut exit_cleanup = SessionCleanupGuard::new(hub.data_dir(), "pex-exit");
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "pex-shutdown",
        "sub-shutdown",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let mut shutdown_cleanup = SessionCleanupGuard::new(hub.data_dir(), "pex-shutdown");
    wait_for_authoritative_session_exit(hub.endpoint(), "pex-exit");
    let before = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    let shutdown_generation = occupancy_generation(
        &before
            .status
            .as_ref()
            .expect("status before Active ShutdownSession")
            .live_attach_occupancy,
        "pex-shutdown",
        "sub-shutdown",
    )
    .expect("Active ShutdownSession victim must have a Core-issued generation");
    let shutdown = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "pex-shutdown".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let listed = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes,
        &mut events,
    );
    assert!(
        listed.sessions.iter().any(|session| {
            session.session_id == "pex-shutdown" && session.lifecycle != "running"
        }),
        "production observe path must advance ShutdownSession off running: {:?}",
        listed.sessions
    );
    let late = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(late.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(
        no_terminal_subscription_closed(
            &events,
            "pex-shutdown",
            Some("sub-shutdown"),
            Some(shutdown_generation)
        ),
        "Active ShutdownSession must not emit TerminalSubscriptionClosed for generation {shutdown_generation}: {events:?}"
    );
    assert!(
        events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { .. }
            )
        }),
        "process exit and ShutdownSession must stay on lifecycle paths: {events:?}"
    );
    exit_cleanup.disarm();
    shutdown_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn shutdown_session_exact_keys_preserve_replacement_owner_and_siblings() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("sgk");
    let endpoint = hub.endpoint().clone();
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();

    let missing = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "sgk-missing".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        missing.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let missing_error = missing.error.as_ref().expect("unknown_session body");
    assert_eq!(missing_error.code, "unknown_session");
    assert_eq!(missing_error.operation, "shutdown");
    assert_eq!(missing_error.message, "unknown session: sgk-missing");

    spawn_and_bind(
        &mut stream,
        &mut reader,
        "sgk-victim",
        "sgk-victim-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "sgk-sibling",
        "sgk-sibling-sub",
        "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut envelopes,
        &mut events,
    );
    let before = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    let occupancy = &before
        .status
        .as_ref()
        .expect("status before victim shutdown")
        .live_attach_occupancy;
    let victim_generation =
        occupancy_generation(occupancy, "sgk-victim", "sgk-victim-sub").expect("victim generation");
    let sibling_generation = occupancy_generation(occupancy, "sgk-sibling", "sgk-sibling-sub")
        .expect("sibling generation");
    assert!(victim_generation >= 1);
    assert!(sibling_generation >= 1);

    write_unix_terminal_frame(
        &mut stream,
        "sgk-sibling",
        "sgk-sibling-sub",
        &terminal_input_frame_bytes(b"before-shutdown\r"),
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "echo:before-shutdown")
    {
        let _ = request_collecting_mux(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(
                "sgk-sibling",
                "sgk-sibling-sub",
            ),
            &mut envelopes,
            &mut events,
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "echo:before-shutdown"),
        "sibling must stream before victim shutdown: {envelopes:?}"
    );

    let shutdown = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "sgk-victim".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "Active ShutdownSession must stay typed, got kind={:?} error={:?}",
        shutdown.kind,
        shutdown.error
    );
    let late = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(late.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(
        no_terminal_subscription_closed(
            &events,
            "sgk-victim",
            Some("sgk-victim-sub"),
            Some(victim_generation)
        ),
        "victim generation {victim_generation} must stay silent: {events:?}"
    );

    write_unix_terminal_frame(
        &mut stream,
        "sgk-sibling",
        "sgk-sibling-sub",
        &terminal_input_frame_bytes(b"after-shutdown\r"),
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "echo:after-shutdown")
    {
        let _ = request_collecting_mux(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(
                "sgk-sibling",
                "sgk-sibling-sub",
            ),
            &mut envelopes,
            &mut events,
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "echo:after-shutdown"),
        "sibling must keep streaming across victim shutdown: {envelopes:?}"
    );

    let remove = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "sgk-victim".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        remove.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved,
        "terminal victim must remove, got kind={:?} error={:?}",
        remove.kind,
        remove.error
    );
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "sgk-victim",
        "sgk-victim-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let replaced = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    let replacement_generation = occupancy_generation(
        &replaced
            .status
            .as_ref()
            .expect("status after replacement spawn")
            .live_attach_occupancy,
        "sgk-victim",
        "sgk-victim-sub",
    )
    .expect("replacement owner Core generation");
    assert_ne!(
        replacement_generation, victim_generation,
        "replacement owner must receive a later Core generation: old={victim_generation} new={replacement_generation}"
    );
    let reattach = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "sgk-victim".to_string(),
            subscription_id: "sgk-victim-sub".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        wait_for_subscription_closed(
            &mut stream,
            &mut reader,
            "sgk-victim",
            "sgk-victim-sub",
            &mut envelopes,
            &mut events,
        ),
        "replacement generation must still emit close events: {events:?}"
    );
    let closed_generation = events.iter().rev().find_map(|event| match event {
        botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
            session_id,
            subscription_id,
            generation,
            ..
        } if session_id == "sgk-victim" && subscription_id == "sgk-victim-sub" => Some(*generation),
        _ => None,
    });
    assert_eq!(closed_generation, Some(replacement_generation));

    spawn_and_bind(
        &mut stream,
        &mut reader,
        "sgk-missing",
        "sgk-missing-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let missing_reattach = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "sgk-missing".to_string(),
            subscription_id: "sgk-missing-sub".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        missing_reattach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        wait_for_subscription_closed(
            &mut stream,
            &mut reader,
            "sgk-missing",
            "sgk-missing-sub",
            &mut envelopes,
            &mut events,
        ),
        "Missing ShutdownSession must not suppress a later attach close: {events:?}"
    );

    shutdown_short_lived_session(&endpoint, "sgk-victim");
    shutdown_short_lived_session(&endpoint, "sgk-sibling");
    shutdown_short_lived_session(&endpoint, "sgk-missing");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn attached_stopping_shutdown_session_suppresses_exact_generation() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub_with_env(
        "stp",
        &[(
            "BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR",
            "stp-session",
        )],
    );
    let endpoint = hub.endpoint().clone();
    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "stp-session",
        "stp-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let before = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    let generation = occupancy_generation(
        &before
            .status
            .as_ref()
            .expect("status before Stopping ShutdownSession")
            .live_attach_occupancy,
        "stp-session",
        "stp-sub",
    )
    .expect("attached Stopping victim must have a Core-issued generation");
    let shutdown = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "stp-session".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_eq!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "forced Stopping classification must take the fall-through Events path, got kind={:?} error={:?} cleanup={:?}",
        shutdown.kind,
        shutdown.error,
        shutdown.cleanup
    );
    let late = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert_eq!(late.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(
        no_terminal_subscription_closed(
            &events,
            "stp-session",
            Some("stp-sub"),
            Some(generation)
        ),
        "attached Stopping ShutdownSession must not emit TerminalSubscriptionClosed for generation {generation}: {events:?}"
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_shutdown_session_from_another_connection_classifies_attached_exit() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("pse");
    let endpoint = hub.endpoint().clone();
    let session_id = "pse-session";
    let subscription_id = "pse-sub";
    let print_release = hub.data_dir().join("pse-print");
    let exit_release = hub.data_dir().join("pse-exit");
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");
    let spawned = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "while [ ! -e '{}' ]; do sleep 0.01; done; printf 'pse-ready\\n'; while [ ! -e '{}' ]; do sleep 0.01; done; exit 0",
                print_release.display(),
                exit_release.display()
            ),
        })
        .expect("spawn held printf");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let (mut stream, mut reader) = unix_adapter_connection(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    let term_attach = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes,
    );
    assert_eq!(
        term_attach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        term_attach.events.is_empty(),
        "unix adapter Attach must bind without terminal bodies: {:?}",
        term_attach.events
    );
    let primed = request_skipping_envelopes(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
        &mut envelopes,
    );
    assert!(
        primed.events.is_empty(),
        "host Drain must not return terminal bodies: {:?}",
        primed.events
    );
    fs::write(&print_release, b"go").expect("release Unix natural-exit printf");
    let print_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < print_deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "pse-ready")
    {
        let drain = request_skipping_envelopes(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes,
        );
        assert!(
            drain.events.is_empty(),
            "host Drain must not return terminal bodies: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "pse-ready"),
        "attached adapter must see live output before process exit: {envelopes:?}"
    );
    fs::write(&exit_release, b"go").expect("release Unix natural-exit process");
    let exit_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < exit_deadline
        && !envelopes
            .iter()
            .any(|envelope| unix_envelope_is_process_exit(envelope, session_id, subscription_id))
    {
        let drain = request_skipping_envelopes(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes,
        );
        assert!(
            drain.events.is_empty(),
            "host Drain must not return terminal bodies: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        envelopes
            .iter()
            .any(|envelope| unix_envelope_is_process_exit(envelope, session_id, subscription_id)),
        "attached adapter must see process_exit before ShutdownSession: {envelopes:?}"
    );

    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown from a separate connection");
    assert_shutdown_strict_natural_exit(
        &shutdown,
        session_id,
        "Unix cross-connection ShutdownSession after attached natural exit",
    );

    let listed =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list after cross-connection shutdown");
    assert!(
        listed.sessions.iter().any(|session| {
            session.session_id == session_id
                && matches!(session.lifecycle.as_str(), "exited" | "stopping" | "failed")
        }) || listed
            .sessions
            .iter()
            .all(|session| session.session_id != session_id),
        "ShutdownSession must leave the host session terminal on the control plane: {:?}",
        listed.sessions
    );
    let drain = request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::Drain {
            session_id: session_id.to_string(),
            subscription_id: None,
        },
        &mut envelopes,
        &mut events,
    );
    assert!(
        drain.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit { .. }
                | botster_hub_client::DaemonEvent::TerminalOutput { .. }
        )),
        "host Drain must not translate ProcessExit after ShutdownSession: {:?}",
        drain.events
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_shutdown_session_stuck_stopping_without_exit_evidence_stays_operator_error() {
    let _guard = daemon_test_guard();
    let session_id = "stk-session";
    let hub = start_isolated_live_output_hub_with_env(
        "stk",
        &[
            ("BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_FOR", session_id),
            (
                "BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_MESSAGE",
                "test-injected observe drain failure: stk-session",
            ),
        ],
    );
    let endpoint = hub.endpoint().clone();
    let data_dir = hub.data_dir().clone();
    let before_pids: std::collections::BTreeSet<u32> = session_worker_process_identities()
        .expect("baseline worker census must succeed")
        .into_iter()
        .map(|worker| worker.pid)
        .collect();

    let spawned = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 3600".to_string(),
        },
    )
    .expect("spawn stuck-Stopping victim");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "stuck-Stopping victim must spawn, got kind={:?} error={:?}",
        spawned.kind,
        spawned.error
    );

    let workers = capture_new_session_workers_for_data_dir(&data_dir, &before_pids)
        .expect("must capture live victim worker after Spawn");
    assert!(
        !workers.is_empty(),
        "must capture live victim worker before SIGKILL"
    );
    for worker in &workers {
        let result = unsafe { libc::kill(worker.pid as libc::pid_t, libc::SIGKILL) };
        assert_eq!(
            result,
            0,
            "SIGKILL victim worker pid={} errno={}",
            worker.pid,
            std::io::Error::last_os_error()
        );
    }

    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown after killed worker and exact-query failure");
    assert_eq!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "stuck session without exact exit evidence must stay OperatorError, got kind={:?} error={:?} cleanup={:?}",
        shutdown.kind,
        shutdown.error,
        shutdown.cleanup
    );
    let error = shutdown.error.as_ref().expect("typed operator error body");
    assert!(
        error.code == "runtime_error" || error.code == "state_error",
        "stuck ShutdownSession must keep runtime_error or state_error, got {error:?}"
    );
    assert_eq!(error.operation, "shutdown");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn stale_generation_close_does_not_sweep_replacement_owner() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("sgo");
    let endpoint = hub.endpoint().clone();
    let (mut owner_a, mut reader_a) = unix_adapter_connection(&endpoint);
    let mut envelopes_a = Vec::new();
    let mut events_a = Vec::new();
    spawn_and_bind(
        &mut owner_a,
        &mut reader_a,
        "sgo-session",
        "sgo-sub",
        "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut envelopes_a,
        &mut events_a,
    );

    let (mut owner_b, mut reader_b) = unix_adapter_connection(&endpoint);
    let mut envelopes_b = Vec::new();
    let mut events_b = Vec::new();
    let attach_b = request_collecting_mux(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "sgo-session".to_string(),
            subscription_id: "sgo-sub".to_string(),
        },
        &mut envelopes_b,
        &mut events_b,
    );
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    assert!(
        !attach_b.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. }
                if state == botster_hub_client::ATTACH_STATE_ATTACH_FAILED
        )),
        "replacement owner B must bind: {:?}",
        attach_b.events
    );
    assert!(
        wait_for_subscription_closed(
            &mut owner_a,
            &mut reader_a,
            "sgo-session",
            "sgo-sub",
            &mut envelopes_a,
            &mut events_a,
        ),
        "A must observe TerminalSubscriptionClosed for generation N: {events_a:?}"
    );
    let closed_generation = events_a.iter().find_map(|event| match event {
        botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
            generation,
            session_id,
            ..
        } if session_id == "sgo-session" => Some(*generation),
        _ => None,
    });
    assert_eq!(closed_generation, Some(1));

    write_unix_terminal_frame(
        &mut owner_b,
        "sgo-session",
        "sgo-sub",
        &terminal_input_frame_bytes(b"after-replace\r"),
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes_b, "echo:after-replace")
    {
        let drain = request_collecting_mux(
            &mut owner_b,
            &mut reader_b,
            &botster_hub_client::DaemonRequest::drain_subscription("sgo-session", "sgo-sub"),
            &mut envelopes_b,
            &mut events_b,
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !event_is_terminal_body(event)),
            "B's scoped Drain must stay bound after A's stale close: {:?}",
            drain.events
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes_b, "echo:after-replace"),
        "generation N+1 must stay owned after N closed: {envelopes_b:?}"
    );
    drop(owner_a);
    shutdown_short_lived_session(&endpoint, "sgo-session");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn terminal_subscription_closed_feature_does_not_raise_default_requirement() {
    let requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    let mut previous = botster_hub_client::DaemonCompatibility::current();
    previous
        .features
        .retain(|feature| feature != botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED);
    previous.conformance_fixture_revision =
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::ensure_compatible(&requirement, &previous)
        .expect("default clients still accept a daemon without terminal_subscription_closed");
    assert_eq!(
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION,
        36
    );
    const _: () = assert!(botster_hub_client::CONFORMANCE_FIXTURE_REVISION >= 45);
}

fn occupancy_has_pair(
    occupancy: &[botster_hub_client::DaemonAttachOccupancy],
    session_id: &str,
    subscription_id: &str,
) -> bool {
    occupancy
        .iter()
        .any(|row| row.session_id == session_id && row.subscription_id == subscription_id)
}

fn occupancy_generation(
    occupancy: &[botster_hub_client::DaemonAttachOccupancy],
    session_id: &str,
    subscription_id: &str,
) -> Option<u64> {
    occupancy.iter().find_map(|row| {
        (row.session_id == session_id && row.subscription_id == subscription_id)
            .then_some(row.generation)
    })
}

fn no_terminal_subscription_closed<'a, I>(
    events: I,
    session_id: &str,
    subscription_id: Option<&str>,
    generation: Option<u64>,
) -> bool
where
    I: IntoIterator<Item = &'a botster_hub_client::DaemonEvent>,
{
    events.into_iter().all(|event| {
        !matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id: closed_session,
                subscription_id: closed_subscription,
                generation: closed_generation,
                ..
            } if closed_session == session_id
                && subscription_id.is_none_or(|expected| closed_subscription == expected)
                && generation.is_none_or(|expected| *closed_generation == expected)
        )
    })
}

fn sibling_status(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
) -> botster_hub_client::DaemonStatus {
    request_skipping_envelopes(
        stream,
        reader,
        &botster_hub_client::DaemonRequest::Status,
        envelopes,
    )
    .status
    .expect("status body")
}

fn wait_for_cleanup_completed(
    stream: &mut std::os::unix::net::UnixStream,
    reader: &mut std::io::BufReader<std::os::unix::net::UnixStream>,
    envelopes: &mut Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    before: &botster_hub_client::DaemonLifecycleCounters,
) -> botster_hub_client::DaemonStatus {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut status = sibling_status(stream, reader, envelopes);
    while Instant::now() < deadline {
        if status.lifecycle_counters.cleanup_completed > before.cleanup_completed {
            return status;
        }
        thread::sleep(Duration::from_millis(20));
        status = sibling_status(stream, reader, envelopes);
    }
    status
}

fn attach_two_unix_clients(
    hub: &botster_hub_test_support::IsolatedHub,
    session_id: &str,
    sub_a: &str,
    sub_b: &str,
) -> (
    std::os::unix::net::UnixStream,
    std::io::BufReader<std::os::unix::net::UnixStream>,
    std::os::unix::net::UnixStream,
    std::io::BufReader<std::os::unix::net::UnixStream>,
    Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
    Vec<botster_hub_client::DaemonUnixTerminalEnvelope>,
) {
    let endpoint = hub.endpoint();
    let (mut owner_a, mut reader_a) = unix_adapter_connection(endpoint);
    let mut envelopes_a = Vec::new();
    let spawned = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let attach_a = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: sub_a.to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(
        attach_a.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    let (mut owner_b, mut reader_b) = unix_adapter_connection(endpoint);
    let mut envelopes_b = Vec::new();
    let attach_b = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: sub_b.to_string(),
        },
        &mut envelopes_b,
    );
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    (
        owner_a,
        reader_a,
        owner_b,
        reader_b,
        envelopes_a,
        envelopes_b,
    )
}

#[test]
fn unix_eof_releases_exact_attach_occupancy_on_sibling_status() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("ueo");
    let session_id = "ueo-session";
    let sub_a = "ueo-sub-a";
    let sub_b = "ueo-sub-b";
    let (owner_a, reader_a, mut owner_b, mut reader_b, _envelopes_a, mut envelopes_b) =
        attach_two_unix_clients(&hub, session_id, sub_a, sub_b);

    let before = sibling_status(&mut owner_b, &mut reader_b, &mut envelopes_b);
    assert!(
        before
            .compatibility
            .features
            .iter()
            .any(|feature| feature == botster_hub_client::FEATURE_ATTACH_OCCUPANCY),
        "sibling Status must advertise attach_occupancy: {:?}",
        before.compatibility.features
    );
    assert!(
        occupancy_has_pair(&before.live_attach_occupancy, session_id, sub_a),
        "both pairs must be occupied before EOF: {:?}",
        before.live_attach_occupancy
    );
    assert!(
        occupancy_has_pair(&before.live_attach_occupancy, session_id, sub_b),
        "both pairs must be occupied before EOF: {:?}",
        before.live_attach_occupancy
    );

    drop(owner_a);
    drop(reader_a);
    let after = wait_for_cleanup_completed(
        &mut owner_b,
        &mut reader_b,
        &mut envelopes_b,
        &before.lifecycle_counters,
    );
    assert!(
        !occupancy_has_pair(&after.live_attach_occupancy, session_id, sub_a),
        "exact-absence: old pair must leave sibling Status occupancy: occupancy={:?} counters={:?}",
        after.live_attach_occupancy,
        after.lifecycle_counters
    );
    assert!(
        occupancy_has_pair(&after.live_attach_occupancy, session_id, sub_b),
        "sibling pair must stay occupied: {:?}",
        after.live_attach_occupancy
    );

    write_unix_terminal_frame(
        &mut owner_b,
        session_id,
        sub_b,
        &terminal_input_frame_bytes(b"after-a-eof\r"),
    );
    let listed = request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut envelopes_b,
    );
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "host session must stay listed after A EOF"
    );
    eprintln!(
        "unix eof occupancy provenance hub_bin={} session_worker={}",
        env!("CARGO_BIN_EXE_botster-hub"),
        session_worker_binary_path().display()
    );

    drop(owner_b);
    shutdown_short_lived_session(hub.endpoint(), session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_eof_leave_route_ablation_keeps_named_pair_on_status() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub_with_env(
        "uel",
        &[("BOTSTER_HUB_UNIX_EOF_ABLATION", "leave_route")],
    );
    let session_id = "uel-session";
    let sub_a = "uel-sub-a";
    let sub_b = "uel-sub-b";
    let (owner_a, reader_a, mut owner_b, mut reader_b, _envelopes_a, mut envelopes_b) =
        attach_two_unix_clients(&hub, session_id, sub_a, sub_b);
    let before = sibling_status(&mut owner_b, &mut reader_b, &mut envelopes_b);
    drop(owner_a);
    drop(reader_a);
    let after = wait_for_cleanup_completed(
        &mut owner_b,
        &mut reader_b,
        &mut envelopes_b,
        &before.lifecycle_counters,
    );
    assert!(
        occupancy_has_pair(&after.live_attach_occupancy, session_id, sub_a),
        "leave-route ablation must redden the exact-absence assertion: {:?}",
        after.live_attach_occupancy
    );
    drop(owner_b);
    shutdown_short_lived_session(hub.endpoint(), session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub_with_env(
        "ues",
        &[("BOTSTER_HUB_UNIX_EOF_ABLATION", "skip_core_detach")],
    );
    let session_id = "ues-session";
    let sub_a = "ues-sub-a";
    let sub_b = "ues-sub-b";
    let (owner_a, reader_a, mut owner_b, mut reader_b, _envelopes_a, mut envelopes_b) =
        attach_two_unix_clients(&hub, session_id, sub_a, sub_b);
    let before = sibling_status(&mut owner_b, &mut reader_b, &mut envelopes_b);
    drop(owner_a);
    drop(reader_a);
    let after = wait_for_cleanup_completed(
        &mut owner_b,
        &mut reader_b,
        &mut envelopes_b,
        &before.lifecycle_counters,
    );
    assert!(
        occupancy_has_pair(&after.live_attach_occupancy, session_id, sub_a),
        "skip-core-detach ablation must redden the exact-absence assertion: {:?}",
        after.live_attach_occupancy
    );
    drop(owner_b);
    shutdown_short_lived_session(hub.endpoint(), session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_eof_pair_only_detach_ablation_drops_replacement_owner_generation() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub_with_env(
        "uep",
        &[("BOTSTER_HUB_UNIX_EOF_ABLATION", "pair_only_detach")],
    );
    let endpoint = hub.endpoint().clone();
    let session_id = "uep-session";
    let subscription_id = "uep-sub";
    let (mut owner_a, mut reader_a) = unix_adapter_connection(&endpoint);
    let mut envelopes_a = Vec::new();
    request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
        &mut envelopes_a,
    );
    request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_a,
    );
    let (mut owner_b, mut reader_b) = unix_adapter_connection(&endpoint);
    let mut envelopes_b = Vec::new();
    request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_b,
    );
    let before = sibling_status(&mut owner_b, &mut reader_b, &mut envelopes_b);
    let before_generation = before
        .live_attach_occupancy
        .iter()
        .find(|row| row.session_id == session_id && row.subscription_id == subscription_id)
        .map(|row| row.generation);
    drop(owner_a);
    drop(reader_a);
    let after = wait_for_cleanup_completed(
        &mut owner_b,
        &mut reader_b,
        &mut envelopes_b,
        &before.lifecycle_counters,
    );
    let after_generation = after
        .live_attach_occupancy
        .iter()
        .find(|row| row.session_id == session_id && row.subscription_id == subscription_id)
        .map(|row| row.generation);
    assert!(
        after_generation != before_generation || after_generation.is_none(),
        "pair-only Detach ablation must redden at B's generation still occupied: before={before_generation:?} after={after:?}"
    );
    drop(owner_b);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_spawn_then_eof_keeps_host_session() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("usp");
    let endpoint = hub.endpoint().clone();
    let session_id = "usp-session";
    let (mut owner_a, mut reader_a) = unix_adapter_connection(&endpoint);
    let mut envelopes_a = Vec::new();
    let spawned = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    drop(owner_a);
    drop(reader_a);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut listed =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list after spawn EOF");
    while Instant::now() < deadline {
        if listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id)
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        listed =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
                .expect("list after spawn EOF");
    }
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "Spawn-then-EOF must keep the host session: {listed:?}"
    );
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}
