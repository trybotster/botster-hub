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
    botster_hub_client::write_frame(stream, request).expect("write request");
    loop {
        match botster_hub_client::read_unix_mux_frame_from_reader(reader).expect("read mux") {
            botster_hub_client::DaemonUnixMuxFrame::Response(response) => return *response,
            botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope) => {
                assert!(envelope.is_unix_terminal_plane());
                envelopes.push(envelope);
            }
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
