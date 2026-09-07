fn unix_envelope_is_process_exit(
    frame: &botster_hub_client::DaemonUnixTerminalFrame,
    _session_id: &str,
    subscription_id: &str,
) -> bool {
    frame.route == subscription_id
        && decode_route_event(frame).is_some_and(|event| event.is_process_exit())
}

fn unix_envelope_is_attached(
    frame: &botster_hub_client::DaemonUnixTerminalFrame,
    _session_id: &str,
    subscription_id: &str,
) -> bool {
    frame.route == subscription_id
        && decode_route_event(frame).is_some_and(|event| event.is_attached())
}

fn assert_host_session_retained(
    connection: &mut UnixRouteClient,
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
    frames: &[botster_hub_client::DaemonUnixTerminalFrame],
    marker: &str,
) -> bool {
    frames_contain_output(frames, marker)
}

fn opaque_terminal_bytes(frames: &[botster_hub_client::DaemonUnixTerminalFrame]) -> Vec<u8> {
    frames_output_bytes(frames)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn read_unsolicited_terminal_until(
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    deadline: Instant,
    marker: &str,
) {
    client.read_terminal_until(frames, deadline, |frames| {
        unix_envelope_contains_live_bytes(frames, marker)
    });
}

fn unix_wake_log_has(path: &std::path::Path, event: &str, byte_len: usize) -> bool {
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    text.lines().any(|line| {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            return false;
        };
        value.get("event").and_then(serde_json::Value::as_str) == Some(event)
            && value.get("byte_len").and_then(serde_json::Value::as_u64) == Some(byte_len as u64)
    })
}

fn unix_process_exit_payload_len(
    frames: &[botster_hub_client::DaemonUnixTerminalFrame],
    session_id: &str,
    subscription_id: &str,
) -> Option<usize> {
    frames
        .iter()
        .find(|frame| unix_envelope_is_process_exit(frame, session_id, subscription_id))
        .map(|frame| frame.body.len())
}

fn read_unsolicited_until_process_exit(
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    session_id: &str,
    subscription_id: &str,
    deadline: Instant,
) {
    client.read_terminal_until(frames, deadline, |frames| {
        frames
            .iter()
            .any(|frame| unix_envelope_is_process_exit(frame, session_id, subscription_id))
    });
}

#[test]
fn unix_adapter_bind_returns_only_attaching_then_opaque_envelopes() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uab");
    let endpoint = hub.endpoint().clone();
    let session_id = "uab-session";
    let subscription_id = "uab-sub";
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();

    let spawned = stream.request_skipping(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'unix-adapter-ready\\n'; sleep 30".to_string(),
        }, &mut envelopes);
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let attach = stream.request_skipping(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes);
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::TerminalAttached);
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
        stream.poll_unsolicited(Duration::from_millis(50), &mut envelopes);
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !envelopes.is_empty(),
        "later frames must arrive as opaque adapter envelopes"
    );
    for envelope in &envelopes {
        assert!(envelope.body.len() > 1);
    }

    let listed = stream.request_skipping(&botster_hub_client::DaemonRequest::ListSessions, &mut envelopes);
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

    let mut replacement = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut replacement_envelopes = Vec::new();
    let reattach = replacement.request_skipping(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut replacement_envelopes);
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
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
fn unix_adapter_unbound_attach_delivers_terminal_output_on_adapter() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uud");
    let endpoint = hub.endpoint().clone();
    let session_id = "uud-session";
    let subscription_id = "uud-sub";
    let mut connection =
        UnixRouteClient::connect(&endpoint).expect("default hello");
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
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status");
    assert!(
        drain.events.is_empty(),
        "host Status must not translate Snapshot: {:?}",
        drain.events
    );
    connection
        .send_terminal_frame(subscription_id, &terminal_input_frame_bytes(b"from-unbound\r"))
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
fn live_generic_core_requests_do_not_drive_idle_terminal_output() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("idle-ctrl");
    let endpoint = hub.endpoint().clone();
    let session_id = "idle-ctrl-session";
    let subscription_id = "idle-ctrl-sub";
    let hold = hub.data_dir().join("idle-ctrl-hold");
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(&mut stream, session_id, subscription_id, &format!(
            "printf 'idle-ctrl-ready\\n'; while [ ! -e '{}' ]; do sleep 0.01; done",
            hold.display()
        ), &mut envelopes, &mut events);
    read_unsolicited_terminal_until(&mut stream, &mut envelopes, Instant::now() + Duration::from_secs(5), "idle-ctrl-ready");
    let attached_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < attached_deadline
        && !envelopes.iter().any(|envelope| {
            unix_envelope_is_attached(envelope, session_id, subscription_id)
        })
    {
        stream.set_read_timeout(Some(Duration::from_millis(200)));
        match stream.read_frame() {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                envelopes.push(envelope);
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Event { .. },
            )) => {}
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Response { response, .. },
            )) => {
                panic!("attached wait received a control response: {response:?}")
                }
                Ok(_) => {}
            Err(_) => {}
        }
    }
    stream.set_read_timeout(None);
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "idle-ctrl-ready"),
        "idle session must deliver ready bytes before control probes: {envelopes:?}"
    );
    assert!(
        envelopes
            .iter()
            .any(|envelope| unix_envelope_is_attached(envelope, session_id, subscription_id)),
        "idle session must attach before control probes: {envelopes:?}"
    );
    stream.set_read_timeout(Some(Duration::from_millis(200)));
    loop {
        match stream.read_frame() {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)) => {
                envelopes.push(envelope);
            }
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Event { .. },
            )) => {}
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Response { response, .. },
            )) => {
                panic!("quiet drain received a control response: {response:?}")
                }
                Ok(_) => {}
            Err(_) => break,
        }
    }
    stream.set_read_timeout(None);
    envelopes.clear();
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
    ];
    for request in requests {
        stream.request_collecting(&request, &mut envelopes, &mut events);
        assert!(
            envelopes.is_empty(),
            "generic Core requests must not drive terminal delivery on an idle bound adapter: request={request:?} envelopes={envelopes:?}"
        );
    }
    fs::write(&hold, b"go").expect("release idle session");
    drop(stream);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_explicit_detach_is_separate_from_connection_death() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("uad");
    let endpoint = hub.endpoint().clone();
    let session_id = "uad-session";
    let subscription_id = "uad-sub";
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();

    stream.request_skipping(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        }, &mut envelopes);
    let attach = stream.request_skipping(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes);
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::TerminalAttached);
    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );

    let detach = stream.request_skipping(&botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes);
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
    let status = stream.request_skipping(&botster_hub_client::DaemonRequest::Status, &mut envelopes);
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

    let second = stream.request_skipping(&botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes);
    assert_ne!(
        second.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );

    let listed = stream.request_skipping(&botster_hub_client::DaemonRequest::ListSessions, &mut envelopes);
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id)
    );

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
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
    let hub = start_isolated_candidate_hub("h-s3");
    let endpoint = hub.endpoint().clone();
    let session_id = "uso-session";
    let subscription_id = "uso-sub";

    let mut owner_a = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes_a = Vec::new();
    owner_a.request_skipping(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        }, &mut envelopes_a);
    let attach_a = owner_a.request_skipping(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes_a);
    assert_eq!(
        attach_a.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        attach_a.events.is_empty(),
        "owner A Attach must not return terminal bodies: {:?}",
        attach_a.events
    );
    let generation_a = attach_a
        .terminal_attach
        .as_ref()
        .expect("owner A attach body")
        .generation;
    let detach_a = owner_a.request_skipping(&botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes_a);
    assert_eq!(
        detach_a.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    let detach_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = owner_a.request_skipping(
            &botster_hub_client::DaemonRequest::Status,
            &mut envelopes_a,
        );
        let status = status.status.expect("status after owner A detach");
        assert!(
            status
                .compatibility
                .features
                .iter()
                .any(|feature| feature == botster_hub_client::FEATURE_ATTACH_OCCUPANCY),
            "H-S3 requires advertised attach occupancy"
        );
        let occupancy = status.live_attach_occupancy;
        if occupancy.iter().all(|row| {
            row.session_id != session_id || row.subscription_id != subscription_id
        }) {
            break;
        }
        assert!(
            Instant::now() < detach_deadline,
            "owner A route remained after detach: generation_a={generation_a} occupancy={occupancy:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let mut owner_b = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes_b = Vec::new();
    let attach_b = owner_b.request_skipping(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        }, &mut envelopes_b);
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        attach_b.events.is_empty(),
        "replacement owner B must bind the same key with empty bodies: {:?}",
        attach_b.events
    );
    let generation_b = attach_b
        .terminal_attach
        .as_ref()
        .expect("owner B attach body")
        .generation;
    assert_ne!(
        generation_b, generation_a,
        "same-id reattach must mint a new generation"
    );

    let reattach_deadline = Instant::now() + Duration::from_secs(5);
    let before = loop {
        let status = owner_b.request_skipping(
            &botster_hub_client::DaemonRequest::Status,
            &mut envelopes_b,
        );
        let status = status.status.expect("status after owner B attach");
        assert!(
            status
                .compatibility
                .features
                .iter()
                .any(|feature| feature == botster_hub_client::FEATURE_ATTACH_OCCUPANCY),
            "H-S3 requires advertised attach occupancy"
        );
        let matching = status
            .live_attach_occupancy
            .iter()
            .filter(|row| {
                row.session_id == session_id && row.subscription_id == subscription_id
            })
            .collect::<Vec<_>>();
        if matches!(matching.as_slice(), [row] if row.generation == generation_b) {
            break status.lifecycle_counters;
        }
        assert!(
            Instant::now() < reattach_deadline,
            "owner B exact generation did not appear: generation_a={generation_a} generation_b={generation_b} occupancy={:?}",
            status.live_attach_occupancy
        );
        thread::sleep(Duration::from_millis(20));
    };
    drop(owner_a);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut after = before.clone();
    while Instant::now() < deadline {
        let status = owner_b.request_skipping(&botster_hub_client::DaemonRequest::Status, &mut envelopes_b);
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

    owner_b.send_terminal_input(subscription_id, &terminal_input_frame_bytes(b"after-a-drop\r"));
    let marker = "echo:after-a-drop";
    let output_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < output_deadline
        && !unix_envelope_contains_live_bytes(&envelopes_b, marker)
    {
        owner_b.poll_unsolicited(Duration::from_millis(50), &mut envelopes_b);
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes_b, marker),
        "B must keep receiving opaque adapter frames after A disconnects: {envelopes_b:?}"
    );
    let occupancy_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let occupancy = owner_b
            .request_skipping(
                &botster_hub_client::DaemonRequest::Status,
                &mut envelopes_b,
            )
            .status
            .expect("status after replacement-owner cleanup")
            .live_attach_occupancy;
        let matching = occupancy
            .iter()
            .filter(|row| {
                row.session_id == session_id && row.subscription_id == subscription_id
            })
            .collect::<Vec<_>>();
        if matches!(matching.as_slice(), [row] if row.generation == generation_b) {
            break;
        }
        assert!(
            Instant::now() < occupancy_deadline,
            "replacement owner occupancy must keep B's exact generation: generation_a={generation_a} generation_b={generation_b} occupancy={occupancy:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

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
        UnixRouteClient::connect(&endpoint).expect("default hello");

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
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::TerminalAttached);
    assert!(
        attach.events.is_empty(),
        "default Hello Attach binds without terminal bodies: {:?}",
        attach.events
    );
    let drain = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status");
    assert!(
        drain.events.is_empty(),
        "host Status must not reconstruct Snapshot: {:?}",
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
        UnixRouteClient::connect(&endpoint).expect("default hello");
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
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::TerminalAttached);
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
    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status after exit");
    assert_eq!(
        status.kind,
        botster_hub_client::DaemonResponseKind::Status,
        "host Status must stay serviceable after exit: {status:?}"
    );
    assert!(
        status.events.is_empty(),
        "host Status must not return terminal bodies: {:?}",
        status.events
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

#[allow(clippy::too_many_arguments)]
fn spawn_and_bind(
    client: &mut RawUnixClient,
    session_id: &str,
    subscription_id: &str,
    command: &str,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    events: &mut Vec<botster_hub_client::DaemonEvent>,
) {
    let spawned = client.request_collecting(
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: command.to_string(),
        },
        frames,
        events,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "spawn must succeed for {session_id}: error={:?}",
        spawned.error
    );
    let attach = client.request_collecting(
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        frames,
        events,
    );
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached,
        "Unix Attach answers with the route generation: error={:?}",
        attach.error
    );
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
    client: &mut RawUnixClient,
    session_id: &str,
    subscription_id: &str,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
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
            client.set_read_timeout(None);
            return true;
        }
        client.set_read_timeout(Some(Duration::from_millis(100)));
        match client.read_frame() {
            Ok(botster_hub_client::DaemonUnixMuxFrame::Terminal(frame)) => frames.push(frame),
            Ok(botster_hub_client::DaemonUnixMuxFrame::Server(
                botster_hub_client::ServerFrame::Event { event },
            )) => events.push(event),
            Ok(_) => {}
            Err(_) => {
                client.set_read_timeout(None);
                let _ = client.request_collecting(
                    &botster_hub_client::DaemonRequest::Status,
                    frames,
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
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
    );
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
    let (stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
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
    let mut stream = RawUnixClient::from_stream(stream);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    stream.request_collecting(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "htm-session".to_string(),
            command: "sleep 30".to_string(),
        }, &mut envelopes, &mut events);
    let attach = stream.request_collecting(&botster_hub_client::DaemonRequest::Attach {
            session_id: "htm-session".to_string(),
            subscription_id: "htm-sub".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = attach.error.expect("operator error");
    assert_eq!(error.code, "terminal_compatibility");
    assert_eq!(error.operation, "attach");
    assert!(
        attach.terminal_attach.is_none(),
        "rejected attach must not mint a route: {:?}",
        attach.terminal_attach
    );
    let status = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(&mut stream, "hac-a", "sub-a", "sleep 30", &mut envelopes, &mut events);
    spawn_and_bind(&mut stream, "hac-b", "sub-b", "sleep 30", &mut envelopes, &mut events);
    let reattach = stream.request_collecting(&botster_hub_client::DaemonRequest::Attach {
            session_id: "hac-a".to_string(),
            subscription_id: "sub-a".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        wait_for_subscription_closed(&mut stream, "hac-a", "sub-a", &mut envelopes, &mut events),
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
    let sibling = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
    assert_eq!(sibling.kind, botster_hub_client::DaemonResponseKind::Status);
    let listed = stream.request_collecting(&botster_hub_client::DaemonRequest::ListSessions, &mut envelopes, &mut events);
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == "hac-b")
    );
    shutdown_short_lived_session(hub.endpoint(), "hac-a");
    shutdown_short_lived_session(hub.endpoint(), "hac-b");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn subscribe_entities_on_bound_unix_mux_returns_operator_error_and_keeps_route() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("sem");
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(&mut stream, "sem-live", "sub-live", "sleep 30", &mut envelopes, &mut events);

    let subscribe = stream.request_collecting(&botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "sem-entities".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        subscribe.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "SubscribeEntities on a bound Unix mux must fail closed: {subscribe:?}"
    );
    assert_eq!(
        subscribe.error.as_ref().map(|error| error.code.as_str()),
        Some("unix_mux_owns_connection")
    );

    let status = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let drain = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
    assert_ne!(
        drain.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "bound adapter must stay owned after rejected SubscribeEntities: {:?}",
        drain.error
    );

    shutdown_short_lived_session(hub.endpoint(), "sem-live");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn failed_remove_session_does_not_suppress_later_core_close() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("frm");
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(&mut stream, "frm-stall", "sub-stall", "yes remove-session-still-live", &mut envelopes, &mut events);
    let removed = stream.request_collecting(&botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "frm-stall".to_string(),
        }, &mut envelopes, &mut events);
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
        wait_for_subscription_closed(&mut stream, "frm-stall", "sub-stall", &mut envelopes, &mut events),
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
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    let producer_dir = unique_short_test_dir("cdn-producers");
    fs::create_dir_all(&producer_dir).expect("create connection-death producer directory");
    let detach_release = producer_dir.join("detach-release");
    let detach_command = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf 'cdn-detach-ready\\n'; sleep 30",
        detach_release.display()
    );
    spawn_and_bind(&mut stream, "cdn-session", "cdn-sub", &detach_command, &mut envelopes, &mut events);
    assert!(
        !detach_release.exists(),
        "the detach producer must stay held until the Unix route is bound"
    );
    fs::write(&detach_release, b"go").expect("release detach producer");
    read_unsolicited_terminal_until(&mut stream, &mut envelopes, Instant::now() + Duration::from_secs(10), "cdn-detach-ready");
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "cdn-detach-ready"),
        "the Unix route must deliver the detach producer marker before Detach"
    );
    let detach = stream.request_collecting(&botster_hub_client::DaemonRequest::Detach {
            session_id: "cdn-session".to_string(),
            subscription_id: "cdn-sub".to_string(),
        }, &mut envelopes, &mut events);
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
    let mut replacement = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut replacement_events = Vec::new();
    let mut replacement_envelopes = Vec::new();
    let death_release = producer_dir.join("death-release");
    let death_command = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf 'cdn-death-ready\\n'; sleep 30",
        death_release.display()
    );
    spawn_and_bind(&mut replacement, "cdn-death", "cdn-death-sub", &death_command, &mut replacement_envelopes, &mut replacement_events);
    assert!(
        !death_release.exists(),
        "the connection-death producer must stay held until the Unix route is bound"
    );
    fs::write(&death_release, b"go").expect("release connection-death producer");
    read_unsolicited_terminal_until(&mut replacement, &mut replacement_envelopes, Instant::now() + Duration::from_secs(10), "cdn-death-ready");
    assert!(
        unix_envelope_contains_live_bytes(&replacement_envelopes, "cdn-death-ready"),
        "the Unix route must deliver the connection-death marker before EOF"
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
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(&mut stream, "pex-exit", "sub-exit", "printf 'done\\n'", &mut envelopes, &mut events);
    let mut exit_cleanup = SessionCleanupGuard::new(hub.data_dir(), "pex-exit");
    spawn_and_bind(&mut stream, "pex-shutdown", "sub-shutdown", "sleep 30", &mut envelopes, &mut events);
    let mut shutdown_cleanup = SessionCleanupGuard::new(hub.data_dir(), "pex-shutdown");
    wait_for_authoritative_session_exit(hub.endpoint(), "pex-exit");
    let before = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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
    let shutdown = stream.request_collecting(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "pex-shutdown".to_string(),
        }, &mut envelopes, &mut events);
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let listed = stream.request_collecting(&botster_hub_client::DaemonRequest::ListSessions, &mut envelopes, &mut events);
    assert!(
        listed.sessions.iter().any(|session| {
            session.session_id == "pex-shutdown" && session.lifecycle != "running"
        }),
        "production observe path must advance ShutdownSession off running: {:?}",
        listed.sessions
    );
    let late = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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
    let mut stream = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();

    let missing = stream.request_collecting(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "sgk-missing".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        missing.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let missing_error = missing.error.as_ref().expect("unknown_session body");
    assert_eq!(missing_error.code, "unknown_session");
    assert_eq!(missing_error.operation, "shutdown");
    assert_eq!(missing_error.message, "unknown session: sgk-missing");

    spawn_and_bind(&mut stream, "sgk-victim", "sgk-victim-sub", "sleep 30", &mut envelopes, &mut events);
    spawn_and_bind(&mut stream, "sgk-sibling", "sgk-sibling-sub", "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done", &mut envelopes, &mut events);
    let before = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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

    stream.send_terminal_input("sgk-sibling-sub", &terminal_input_frame_bytes(b"before-shutdown\r"));
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "echo:before-shutdown")
    {
        let _ = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "echo:before-shutdown"),
        "sibling must stream before victim shutdown: {envelopes:?}"
    );

    let shutdown = stream.request_collecting(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "sgk-victim".to_string(),
        }, &mut envelopes, &mut events);
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "Active ShutdownSession must stay typed, got kind={:?} error={:?}",
        shutdown.kind,
        shutdown.error
    );
    let late = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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

    stream.send_terminal_input("sgk-sibling-sub", &terminal_input_frame_bytes(b"after-shutdown\r"));
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes, "echo:after-shutdown")
    {
        let _ = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        unix_envelope_contains_live_bytes(&envelopes, "echo:after-shutdown"),
        "sibling must keep streaming across victim shutdown: {envelopes:?}"
    );

    let remove = stream.request_collecting(&botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "sgk-victim".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        remove.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved,
        "terminal victim must remove, got kind={:?} error={:?}",
        remove.kind,
        remove.error
    );
    spawn_and_bind(&mut stream, "sgk-victim", "sgk-victim-sub", "sleep 30", &mut envelopes, &mut events);
    let replaced = stream.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes, &mut events);
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
    let reattach = stream.request_collecting(&botster_hub_client::DaemonRequest::Attach {
            session_id: "sgk-victim".to_string(),
            subscription_id: "sgk-victim-sub".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        reattach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        wait_for_subscription_closed(&mut stream, "sgk-victim", "sgk-victim-sub", &mut envelopes, &mut events),
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

    spawn_and_bind(&mut stream, "sgk-missing", "sgk-missing-sub", "sleep 30", &mut envelopes, &mut events);
    let missing_reattach = stream.request_collecting(&botster_hub_client::DaemonRequest::Attach {
            session_id: "sgk-missing".to_string(),
            subscription_id: "sgk-missing-sub".to_string(),
        }, &mut envelopes, &mut events);
    assert_eq!(
        missing_reattach.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        wait_for_subscription_closed(&mut stream, "sgk-missing", "sgk-missing-sub", &mut envelopes, &mut events),
        "Missing ShutdownSession must not suppress a later attach close: {events:?}"
    );

    shutdown_short_lived_session(&endpoint, "sgk-victim");
    shutdown_short_lived_session(&endpoint, "sgk-sibling");
    shutdown_short_lived_session(&endpoint, "sgk-missing");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn stale_generation_close_does_not_sweep_replacement_owner() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("sgo");
    let endpoint = hub.endpoint().clone();
    let mut owner_a = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes_a = Vec::new();
    let mut events_a = Vec::new();
    spawn_and_bind(&mut owner_a, "sgo-session", "sgo-sub", "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done", &mut envelopes_a, &mut events_a);

    let mut owner_b = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes_b = Vec::new();
    let mut events_b = Vec::new();
    let attach_b = owner_b.request_collecting(&botster_hub_client::DaemonRequest::Attach {
            session_id: "sgo-session".to_string(),
            subscription_id: "sgo-sub".to_string(),
        }, &mut envelopes_b, &mut events_b);
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    assert!(
        attach_b.terminal_attach.is_some(),
        "replacement owner B must bind: {:?}",
        attach_b.error
    );
    assert!(
        wait_for_subscription_closed(&mut owner_a, "sgo-session", "sgo-sub", &mut envelopes_a, &mut events_a),
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

    owner_b.send_terminal_input("sgo-sub", &terminal_input_frame_bytes(b"after-replace\r"));
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline
        && !unix_envelope_contains_live_bytes(&envelopes_b, "echo:after-replace")
    {
        let _drain = owner_b.request_collecting(&botster_hub_client::DaemonRequest::Status, &mut envelopes_b, &mut events_b);
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
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
) -> botster_hub_client::DaemonStatus {
    client
        .request_skipping(&botster_hub_client::DaemonRequest::Status, frames)
        .status
        .expect("status body")
}

fn wait_for_cleanup_completed(
    client: &mut RawUnixClient,
    frames: &mut Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    before: &botster_hub_client::DaemonLifecycleCounters,
) -> botster_hub_client::DaemonStatus {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut status = sibling_status(client, frames);
    while Instant::now() < deadline {
        if status.lifecycle_counters.cleanup_completed > before.cleanup_completed {
            return status;
        }
        thread::sleep(Duration::from_millis(20));
        status = sibling_status(client, frames);
    }
    status
}

fn attach_two_unix_clients(
    hub: &botster_hub_test_support::IsolatedHub,
    session_id: &str,
    sub_a: &str,
    sub_b: &str,
) -> (
    RawUnixClient,
    RawUnixClient,
    Vec<botster_hub_client::DaemonUnixTerminalFrame>,
    Vec<botster_hub_client::DaemonUnixTerminalFrame>,
) {
    let endpoint = hub.endpoint();
    let mut owner_a = RawUnixClient::connect_unix_terminal_adapter(endpoint);
    let mut frames_a = Vec::new();
    let spawned = owner_a.request_skipping(
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
        &mut frames_a,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let attach_a = owner_a.request_skipping(
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: sub_a.to_string(),
        },
        &mut frames_a,
    );
    assert_eq!(
        attach_a.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    let mut owner_b = RawUnixClient::connect_unix_terminal_adapter(endpoint);
    let mut frames_b = Vec::new();
    let attach_b = owner_b.request_skipping(
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: sub_b.to_string(),
        },
        &mut frames_b,
    );
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::TerminalAttached
    );
    (owner_a, owner_b, frames_a, frames_b)
}

#[test]
fn unix_eof_releases_exact_attach_occupancy_on_sibling_status() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("ueo");
    let session_id = "ueo-session";
    let sub_a = "ueo-sub-a";
    let sub_b = "ueo-sub-b";
    let (owner_a, mut owner_b, _envelopes_a, mut envelopes_b) = attach_two_unix_clients(&hub, session_id, sub_a, sub_b);

    let before = sibling_status(&mut owner_b, &mut envelopes_b);
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
    let after = wait_for_cleanup_completed(&mut owner_b, &mut envelopes_b, &before.lifecycle_counters);
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

    owner_b.send_terminal_input(sub_b, &terminal_input_frame_bytes(b"after-a-eof\r"));
    let listed = owner_b.request_skipping(&botster_hub_client::DaemonRequest::ListSessions, &mut envelopes_b);
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
fn unix_spawn_then_eof_keeps_host_session() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("usp");
    let endpoint = hub.endpoint().clone();
    let session_id = "usp-session";
    let mut owner_a = RawUnixClient::connect_unix_terminal_adapter(&endpoint);
    let mut envelopes_a = Vec::new();
    let spawned = owner_a.request_skipping(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 30".to_string(),
        }, &mut envelopes_a);
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    drop(owner_a);
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
