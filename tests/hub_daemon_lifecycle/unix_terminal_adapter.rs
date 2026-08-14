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

fn unix_envelope_contains_live_bytes(
    envelopes: &[botster_hub_client::DaemonUnixTerminalEnvelope],
    marker: &str,
) -> bool {
    envelopes.iter().any(|envelope| {
        let Ok(bytes) = envelope.payload_bytes() else {
            return false;
        };
        if bytes.windows(marker.len()).any(|window| window == marker.as_bytes()) {
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
    assert_eq!(spawned.kind, botster_hub_client::DaemonResponseKind::Spawned);

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
    let terminal: Vec<_> = attach
        .events
        .iter()
        .filter(|event| event_is_terminal_body(event))
        .collect();
    assert_eq!(
        terminal.len(),
        1,
        "attach may carry only the initial Attaching frame: {:?}",
        attach.events
    );
    assert!(matches!(
        terminal[0],
        botster_hub_client::DaemonEvent::AttachState {
            state,
            subscription_id: event_subscription,
            ..
        } if state == botster_hub_client::ATTACH_STATE_ATTACHING
            && event_subscription == subscription_id
    ));

    let deadline = Instant::now() + Duration::from_secs(8);
    while envelopes.is_empty() && Instant::now() < deadline {
        let drain = request_skipping_envelopes(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            &mut envelopes,
        );
        assert!(
            drain.events.iter().all(|event| !event_is_terminal_body(event)),
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

    let before = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Status,
    )
    .expect("status before bound disconnect")
    .status
    .expect("status body")
    .lifecycle_counters;
    drop(stream);
    let leftover = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ListSessions,
    )
    .expect("list after disconnect");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut counters = before.clone();
    while Instant::now() < deadline {
        let status = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Status,
        )
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
    assert_eq!(reattach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        reattach.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState {
                state,
                ..
            } if state == botster_hub_client::ATTACH_STATE_ATTACHING
        )),
        "adapter close on disconnect is the one Core detach; replacement attach is admitted: {:?}",
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
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("unbound attach");
    let attached = drain_until_subscription(&mut connection, session_id, Some(subscription_id), |event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
        )
    });
    assert!(
        attached.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
        )),
        "unbound scoped Drain must still translate Snapshot: {attached:?}"
    );
    connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "from-unbound\r".to_string(),
        })
        .expect("send");
    let echoed = drain_until_subscription(&mut connection, session_id, Some(subscription_id), |event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(payload, "echo:from-unbound")
        )
    });
    assert!(
        echoed.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                if live_output_contains(payload, "echo:from-unbound")
        )),
        "unbound scoped Drain must keep translating later TerminalOutput: {echoed:?}"
    );
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
    assert!(
        attach.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState {
                state,
                ..
            } if state == botster_hub_client::ATTACH_STATE_ATTACHING
        ))
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
        counters.cleanup_by_reason.get("bound_adapter_close").copied(),
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
    assert_ne!(second.kind, botster_hub_client::DaemonResponseKind::OperatorError);

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
            .any(|session| session.session_id == session_id)
    );

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
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
    assert!(attach_a.events.iter().any(|event| matches!(
        event,
        botster_hub_client::DaemonEvent::AttachState { state, .. }
            if state == botster_hub_client::ATTACH_STATE_ATTACHING
    )));
    let detach_a = request_skipping_envelopes(
        &mut owner_a,
        &mut reader_a,
        &botster_hub_client::DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        &mut envelopes_a,
    );
    assert_eq!(detach_a.kind, botster_hub_client::DaemonResponseKind::Events);

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
    assert!(
        attach_b.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. }
                if state == botster_hub_client::ATTACH_STATE_ATTACHING
        )),
        "replacement owner B must bind the same key: {:?}",
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
        after.cleanup_completed.saturating_sub(before.cleanup_completed),
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

    request_skipping_envelopes(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "after-a-drop\r".to_string(),
        },
        &mut envelopes_b,
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
        .expect("unbound attach");
    assert!(matches!(
        &attach.events[0],
        botster_hub_client::DaemonEvent::AttachState {
            state,
            ..
        } if state == "attaching"
    ));
    let events = drain_until_subscription(
        &mut connection,
        session_id,
        Some(subscription_id),
        |event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::AttachState {
                    state,
                    ..
                } if state == "attached"
            )
        },
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
        )),
        "unbound attach still drains Snapshot: {events:?}"
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
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("default hello");
    let spawned = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!("printf 'smoke:{marker}\\n'"),
        })
        .expect("spawn printf");
    assert_eq!(spawned.kind, botster_hub_client::DaemonResponseKind::Spawned);

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("attach");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut attached = attach.events.iter().any(|event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
        )
    });
    while !attached && Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::drain_subscription(
                session_id,
                subscription_id,
            ))
            .expect("drain");
        attached = drain.events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
            )
        });
        thread::sleep(Duration::from_millis(25));
    }
    assert!(attached, "unbound printf attach must reach Attached");
    let screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.to_string(),
        })
        .expect("read screen");
    let text = screen
        .read_screen
        .as_ref()
        .map(|screen| screen.text.as_str())
        .unwrap_or("");
    assert!(
        text.contains(&format!("smoke:{marker}")),
        "unbound printf visible text is on ReadScreen after Core pin: {text:?}"
    );
    let listed = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list");
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "running"),
        "ProcessExited must not shut down the host session: {:?}",
        listed.sessions
    );

    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn unix_adapter_unbound_stream_attach_returns_late_bytes() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("usa");
    let endpoint = hub.endpoint().clone();
    let session_id = "usa-session";
    let subscription_id = "usa-sub";
    let late = "late-stream-attach";
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!("printf 'pre-attach\\n'; sleep 1; printf '{late}\\n'"),
        },
    )
    .expect("spawn exiting writer");

    let (tx, rx) = mpsc::channel();
    let attach_endpoint = endpoint.clone();
    thread::spawn(move || {
        let mut output = Vec::new();
        let result = botster_hub_client::stream_attach(
            &attach_endpoint,
            session_id,
            subscription_id,
            &mut output,
        );
        let _ = tx.send((result, output));
    });
    let (result, output) = rx
        .recv_timeout(Duration::from_secs(8))
        .expect("production stream_attach must complete after the process exits");
    result.expect("stream_attach");
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains(late),
        "stream_attach must return late terminal bytes after Attached: {text:?}"
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
    assert_eq!(botster_hub_client::PROTOCOL_VERSION, 7);
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
    assert_eq!(spawned.kind, botster_hub_client::DaemonResponseKind::Spawned);
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
        attach.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. }
                if state == botster_hub_client::ATTACH_STATE_ATTACHING
        )),
        "bind must return Attaching: {:?}",
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
    let deadline = Instant::now() + Duration::from_secs(8);
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
    assert!(envelopes.is_empty(), "rejected attach must not bind: {envelopes:?}");
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
    assert_eq!(reattach.kind, botster_hub_client::DaemonResponseKind::Events);
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
    assert!(listed.sessions.iter().any(|session| session.session_id == "hac-b"));
    shutdown_short_lived_session(hub.endpoint(), "hac-a");
    shutdown_short_lived_session(hub.endpoint(), "hac-b");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn core_write_budget_hard_stop_emits_core_adapter_closed() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("cwb");
    let (mut stream, mut reader) = unix_adapter_connection(hub.endpoint());
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "cwb-stall",
        "sub-stall",
        "yes write-budget-stall",
        &mut envelopes,
        &mut events,
    );
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "cwb-live",
        "sub-live",
        "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
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
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == "cwb-live" && session.lifecycle == "running")
    );

    request_collecting_mux(
        &mut stream,
        &mut reader,
        &botster_hub_client::DaemonRequest::SendInput {
            session_id: "cwb-live".to_string(),
            data: "cwb-sibling-live\r".to_string(),
        },
        &mut envelopes,
        &mut events,
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
        "core_write_budget provenance hub_bin={} session_worker={} hub_sha={} locked_core=f4f6bf5babe92dfb9241a760c414187f711c2c42",
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
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "pex-shutdown",
        "sub-shutdown",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    thread::sleep(Duration::from_secs(1));
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
    let _ = listed;
    assert!(
        events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { .. }
            )
        }),
        "process exit and ShutdownSession must stay on lifecycle paths: {events:?}"
    );
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
    assert_eq!(attach_b.kind, botster_hub_client::DaemonResponseKind::Events);
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

    request_collecting_mux(
        &mut owner_b,
        &mut reader_b,
        &botster_hub_client::DaemonRequest::SendInput {
            session_id: "sgo-session".to_string(),
            data: "after-replace\r".to_string(),
        },
        &mut envelopes_b,
        &mut events_b,
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
    previous.features.retain(|feature| {
        feature != botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED
    });
    previous.conformance_fixture_revision =
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::ensure_compatible(&requirement, &previous)
        .expect("default clients still accept a daemon without terminal_subscription_closed");
    assert_eq!(botster_hub_client::CONFORMANCE_FIXTURE_REVISION, 40);
    assert_eq!(
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION,
        36
    );
}
