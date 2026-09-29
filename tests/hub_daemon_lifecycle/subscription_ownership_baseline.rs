// Characterization tests from plan §15.
// These pin current Hub behavior so later tickets show an intentional change.
// They must not change transport behavior.

fn webrtc_baseline_hello() -> botster_hub_client::DaemonHello {
    let mut compatibility =
        botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
    compatibility
        .required_features
        .push(botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string());
    compatibility
        .required_features
        .push(botster_hub_client::FEATURE_ATTACH_OCCUPANCY.to_string());
    compatibility.minimum_conformance_fixture_revision =
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::DaemonHello {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        compatibility,
        terminal_compatibility: None,
    }
}

async fn wait_for_webrtc_marker(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    _subscription_id: &str,
    marker: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut retained = std::mem::take(&mut peer.pending_terminal_frames);
    while Instant::now() < deadline && !webrtc_terminal_contains(&retained, marker) {
        if let Ok(Ok(bytes)) =
            timeout(Duration::from_millis(200), peer.next_terminal_frame(key)).await
        {
            retained.push_back((String::new(), bytes));
        }
    }
    peer.pending_terminal_frames = retained;
    let screen_text = if webrtc_terminal_contains(&peer.pending_terminal_frames, marker) {
        String::new()
    } else {
        peer.encrypted_request(
            key,
            &botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            },
        )
        .await
        .ok()
        .and_then(|response| response.read_screen)
        .map(|screen| screen.text)
        .unwrap_or_default()
    };
    assert!(
        webrtc_terminal_contains(&peer.pending_terminal_frames, marker),
        "missing terminal marker {marker:?} with screen {screen_text:?} in {:?}",
        peer.pending_terminal_frames
            .iter()
            .map(|(_, bytes)| String::from_utf8_lossy(bytes).into_owned())
            .collect::<Vec<_>>()
    );
}

fn extra_channel_observation(path: &Path) -> Option<(bool, bool, String)> {
    let raw = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some((
        value.get("lost_claim")?.as_bool()?,
        value.get("close_ok")?.as_bool()?,
        value.get("label")?.as_str()?.to_string(),
    ))
}

fn wait_for_path(path: &Path, bound: Duration) -> bool {
    let deadline = Instant::now() + bound;
    while Instant::now() < deadline && !path.exists() {
        thread::sleep(Duration::from_millis(50));
    }
    path.exists()
}

#[test]
fn webrtc_dedicated_channels_carry_control_entity_event_and_terminal_frames() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("so-4cls");
    enable_event_plane_producer_on_hub(&endpoint, "so-4cls");
    let session_id = "so-4cls-session";
    let subscription_id = "so-4cls-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        peer.encrypted_hello(&key, &webrtc_package_event_hello())
            .await
            .expect("hello");
        // The marker is printed only after input arrives on the bound route,
        // so it is live output on that route whatever the attach timing.
        let (_terminal, terminal_label) = spawn_and_bind_webrtc_channel_with_label(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "IFS= read -r go; printf 'so-4cls-ready\\n'; sleep 30",
        )
        .await;
        peer.send_terminal_input(&key, &terminal_label, &terminal_input_frame_bytes(b"go\r"))
            .await
            .expect("release the marker through the bound terminal route");
        let entities = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "so-4cls-entity".to_string(),
                },
            )
            .await
            .expect("subscribe entities");
        assert_eq!(
            entities.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        let events = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEvents {
                    subscription_id: "so-4cls-events".to_string(),
                    owner: "event-plane-producer".to_string(),
                    name: "sample.ready".to_string(),
                    subjects: Vec::new(),
                },
            )
            .await
            .expect("subscribe events");
        assert_eq!(
            events.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        wait_for_webrtc_marker(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "so-4cls-ready",
        )
        .await;
        emit_sample_ready(&endpoint, "so-4cls");
        let entity_deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < entity_deadline && peer.pending_entity_frames.is_empty() {
            if let Ok(Ok(frame)) =
                timeout(Duration::from_millis(250), peer.next_entity_frame(&key)).await
            {
                peer.pending_entity_frames.push_back(frame);
            }
        }
        let mut saw_host_event = !peer.pending_host_events().is_empty();
        let host_started = Instant::now();
        let host_deadline = host_started + Duration::from_secs(20);
        let mut reemitted = false;
        while Instant::now() < host_deadline && !saw_host_event {
            if !reemitted && host_started.elapsed() >= Duration::from_secs(8) {
                emit_sample_ready(&endpoint, "so-4cls-retry");
                reemitted = true;
            }
            if let Ok(Ok(_)) = timeout(Duration::from_millis(250), peer.next_host_event(&key)).await
            {
                saw_host_event = true;
            }
        }
        assert!(
            !peer.pending_entity_frames.is_empty(),
            "the entity channel must carry entity frames"
        );
        assert!(saw_host_event, "the event channel must carry host events");
        assert!(
            webrtc_terminal_contains(&peer.pending_terminal_frames, "so-4cls-ready"),
            "the terminal channel must carry terminal frames"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn terminal_adapter_contract_is_duplex_at_the_locked_core_pin() {
    struct Duplex;
    impl botster_core::contract::terminal_adapter::TerminalAdapter for Duplex {
        fn try_write(
            &mut self,
            _frame: &botster_terminal_protocol::RoutedTerminalFrame,
        ) -> Result<(), botster_core::contract::terminal_adapter::TerminalAdapterWriteError>
        {
            Ok(())
        }

        fn close(&mut self, _reason: botster_core::contract::terminal_adapter::TerminalRouteCloseReason) {}

        fn pressure(&self) -> botster_core::contract::terminal_adapter::TerminalAdapterPressure {
            botster_core::contract::terminal_adapter::TerminalAdapterPressure::Ready
        }

        fn try_read(&mut self) -> botster_core::contract::terminal_adapter::TerminalIngress {
            botster_core::contract::terminal_adapter::TerminalIngress::Empty
        }
    }
    let mut adapter = Duplex;
    assert_eq!(
        botster_core::contract::terminal_adapter::TerminalAdapter::try_read(&mut adapter),
        botster_core::contract::terminal_adapter::TerminalIngress::Empty
    );
}

/// READY or HISTORY payload of one route frame.
fn unix_envelope_snapshot_bytes(
    frame: &botster_hub_client::DaemonUnixTerminalFrame,
) -> Option<Vec<u8>> {
    decode_route_event(frame).and_then(|event| event.snapshot_bytes().map(<[u8]>::to_vec))
}

fn apply_ready_then_history_progress(
    projection: &mut botster_terminal_ghostty::GhosttyClientProjection,
    bytes: Vec<u8>,
    saw_ready: bool,
) -> botster_terminal_ghostty::GhosttySnapshotDecodeProgress {
    if !saw_ready {
        projection
            .install_ghostsnp_ready(&bytes)
            .expect("READY snapshot")
    } else {
        projection
            .apply_ghostsnp_history(&bytes)
            .expect("PAGE or FINISH snapshot")
    }
}

#[test]
fn attach_ready_precedes_history_finish() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("so-rth");
    let endpoint = hub.endpoint().clone();
    let terminal =
        botster_terminal_protocol::TerminalCompatibilityRequirement::for_ready_then_history_attach(
        );
    let (stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
        &endpoint,
        &botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
        Some(&terminal),
    )
    .expect("ready_then_history hello");
    assert!(ack.terminal_compatibility.is_some());
    let mut stream = RawUnixClient::from_stream(stream);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        "so-rth-session",
        "so-rth-sub",
        "printf 'so-rth-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        &mut envelopes,
        &mut events,
    );
    let mut projection = botster_terminal_ghostty::GhosttyClientProjection::new(
        botster_core::TerminalScreenSize::new(24, 80),
    )
    .expect("client projection");
    let mut saw_ready = false;
    let mut saw_finish = false;
    let mut cursor = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && !saw_finish {
        while cursor < envelopes.len() {
            let Some(bytes) = unix_envelope_snapshot_bytes(&envelopes[cursor]) else {
                cursor += 1;
                continue;
            };
            cursor += 1;
            let progress = apply_ready_then_history_progress(&mut projection, bytes, saw_ready);
            if progress == botster_terminal_ghostty::GhosttySnapshotDecodeProgress::Ready {
                assert!(!saw_finish, "READY must precede FINISH");
                saw_ready = true;
                stream.send_terminal_input(
                    "so-rth-sub",
                    &terminal_input_frame_bytes(b"so-rth-input\r"),
                );
            }
            if progress == botster_terminal_ghostty::GhosttySnapshotDecodeProgress::Finish {
                assert!(saw_ready, "FINISH must follow READY");
                saw_finish = true;
                break;
            }
        }
        if saw_finish {
            break;
        }
        let drain = stream.request_collecting(
            &botster_hub_client::DaemonRequest::Status,
            &mut envelopes,
            &mut events,
        );
        assert!(
            drain.events.is_empty(),
            "bound drain must not carry terminal bodies on the host plane: {:?}",
            drain.events
        );
    }
    assert!(saw_ready, "terminal stream must emit READY");
    assert!(saw_finish, "terminal stream must emit FINISH after READY");
    assert!(
        !format!("{events:?}").contains("FINISH"),
        "Hub must not invent FINISH on the host plane: events={events:?}"
    );
    shutdown_short_lived_session(&endpoint, "so-rth-session");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn shutdown_suppresses_exact_route_generations_before_core_teardown() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("so-sup");
    let endpoint = hub.endpoint().clone();
    let mut requirement =
        botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter();
    requirement
        .required_features
        .push(botster_hub_client::FEATURE_ATTACH_OCCUPANCY.to_string());
    let stream = botster_hub_client::connect_and_hello_with_requirement(&endpoint, &requirement)
        .expect("unix+occupancy hello");
    let mut stream = RawUnixClient::from_stream(stream);
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        "so-sup-session",
        "so-sup-sub",
        "sleep 30",
        &mut envelopes,
        &mut events,
    );
    let before = stream.request_collecting(
                &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    let generation = occupancy_generation(
        &before
            .status
            .as_ref()
            .expect("status before ShutdownSession")
            .live_attach_occupancy,
        "so-sup-session",
        "so-sup-sub",
    )
    .expect("attached route must publish a Core generation");
    let shutdown = stream.request_collecting(
                &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "so-sup-session".to_string(),
        },
        &mut envelopes,
        &mut events,
    );
    assert_ne!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "ShutdownSession must complete: {:?}",
        shutdown.error
    );
    let _after = stream.request_collecting(
                &botster_hub_client::DaemonRequest::Status,
        &mut envelopes,
        &mut events,
    );
    assert!(
        no_terminal_subscription_closed(
            &events,
            "so-sup-session",
            Some("so-sup-sub"),
            Some(generation)
        ),
        "exact generation {generation} must be suppressed before Core teardown: {events:?}"
    );
    hub.shutdown().expect("shutdown isolated hub");
}

const WEBRTC_BYTE_EXACT_BACKSTOP: Duration = Duration::from_secs(30);

fn webrtc_session_has_exited(
    endpoint: &botster_hub_client::DaemonEndpoint,
    session_id: &str,
) -> bool {
    let _ = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.to_string(),
        },
    );
    match botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListSessions) {
        Ok(response) => response
            .sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "exited"),
        Err(_) => false,
    }
}

fn extend_concatenated_from_pending_webrtc_frames(
    peer: &mut LocalWebrtcOfferPeer,
    concatenated: &mut Vec<u8>,
) {
    // The fixture yields plaintext terminal frame bodies; only OUTPUT carries PTY bytes.
    while let Some((_, bytes)) = peer.pending_terminal_frames.pop_front() {
        let frame = botster_terminal_protocol::TerminalFrame::from_bytes(&bytes)
            .expect("reserved channel carries protocol terminal frames");
        if frame.kind() == botster_terminal_protocol::TerminalKind::Output {
            assert!(
                !payload_has_utf8_replacement(frame.body()),
                "live payload must not contain U+FFFD: {:?}",
                frame.body()
            );
            concatenated.extend_from_slice(frame.body());
        }
    }
}

async fn drain_webrtc_live_bytes(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    _subscription_id: &str,
    concatenated: &mut Vec<u8>,
) {
    let _ = peer
        .encrypted_request(
            key,
            &botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            },
        )
        .await;
    await_next_webrtc_terminal_frame(peer, key).await;
    extend_concatenated_from_pending_webrtc_frames(peer, concatenated);
}

async fn await_next_webrtc_terminal_frame(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
) {
    if let Ok(Ok(bytes)) = timeout(Duration::from_millis(200), peer.next_terminal_frame(key)).await
    {
        peer.pending_terminal_frames
            .push_back((String::new(), bytes));
    }
}

fn panic_webrtc_byte_exact_starvation(evidence: &str) -> ! {
    let (resource, probe) = classify_budget_expiry("webrtc_byte_exact", None, Some(evidence));
    panic!(
        "{}",
        format_harness_budget_expired(
            "webrtc_byte_exact",
            WEBRTC_BYTE_EXACT_BACKSTOP,
            resource,
            probe,
            evidence,
        )
    );
}

async fn wait_for_webrtc_producer_ready_frames(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    subscription_id: &str,
    context: &str,
) {
    let mut concatenated = Vec::new();
    let started_at = Instant::now();
    loop {
        drain_webrtc_live_bytes(peer, key, session_id, subscription_id, &mut concatenated).await;
        if String::from_utf8_lossy(&concatenated).contains(PRODUCER_READY_MARKER) {
            return;
        }
        if started_at.elapsed() >= WEBRTC_BYTE_EXACT_BACKSTOP {
            panic_webrtc_byte_exact_starvation(&format!(
                "{context}: timed out waiting for WebRTC producer-ready frames; concatenated={concatenated:?}"
            ));
        }
        await_next_webrtc_terminal_frame(peer, key).await;
    }
}

async fn collect_expected_webrtc_bytes_or_authoritative_exit(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    endpoint: &botster_hub_client::DaemonEndpoint,
    session_id: &str,
    subscription_id: &str,
    expected: &[u8],
    context: &str,
) -> Vec<u8> {
    let mut concatenated = Vec::new();
    let started_at = Instant::now();
    loop {
        drain_webrtc_live_bytes(peer, key, session_id, subscription_id, &mut concatenated).await;
        if concatenated
            .windows(expected.len())
            .any(|window| window == expected)
        {
            return concatenated;
        }
        if started_at.elapsed() >= WEBRTC_BYTE_EXACT_BACKSTOP {
            let session_exited = webrtc_session_has_exited(endpoint, session_id);
            if concatenated.is_empty() {
                panic_webrtc_byte_exact_starvation(&format!(
                    "{context}: timed out waiting for WebRTC adapter frames after producer-ready release; concatenated is empty; session_exited={session_exited}"
                ));
            }
            return concatenated;
        }
        await_next_webrtc_terminal_frame(peer, key).await;
    }
}

#[test]
fn webrtc_terminal_output_is_byte_exact() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0xff, 0xc0];
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("so-bytes");
    let session_id = "so-bytes-session";
    let subscription_id = "so-bytes-sub";
    let start_path = unique_short_test_dir("so-bytes-start").join("go");
    let release_path = unique_short_test_dir("so-bytes-release").join("go");
    let script_path = write_python_start_then_write_script(&start_path, &release_path, expected);
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello");
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            &python_script_command(&script_path),
        )
        .await;
        fs::create_dir_all(start_path.parent().expect("start parent")).expect("create start dir");
        fs::write(&start_path, b"go").expect("start producer");
        wait_for_webrtc_producer_ready_frames(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "subscription-ownership byte-exact",
        )
        .await;
        fs::create_dir_all(release_path.parent().expect("release parent"))
            .expect("create release dir");
        fs::write(&release_path, b"go").expect("release writer");
        let concatenated = collect_expected_webrtc_bytes_or_authoritative_exit(
            &mut peer,
            &key,
            &endpoint,
            session_id,
            subscription_id,
            expected,
            "subscription-ownership byte-exact",
        )
        .await;
        assert!(
            concatenated
                .windows(expected.len())
                .any(|window| window == expected),
            "WebRTC adapter frames must preserve exact bytes, got {concatenated:?}"
        );
        wait_for_authoritative_session_exit(&endpoint, session_id);
        peer.peer.close().await.expect("close offer peer");
    });
    production_cleanup_after_authoritative_exit(
        &endpoint,
        session_id,
        "subscription-ownership byte-exact",
    );
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn peer_close_leaves_sibling_peers_working() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap_a) = start_webrtc_adapter_hub("so-sib");
    let bootstrap_b = issue_second_webrtc_bootstrap(&endpoint, &bootstrap_a);
    let session_a = "so-sib-a";
    let session_b = "so-sib-b";
    let sub_a = "so-sib-sub-a";
    let sub_b = "so-sib-sub-b";
    let gate_dir = unique_test_dir("so-sib-output-gates");
    fs::create_dir_all(&gate_dir).expect("create sibling output gate directory");
    let gate_a = gate_dir.join("a");
    let gate_b = gate_dir.join("b");
    let command_a = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf 'so-sib-a-ready\\n'; sleep 30",
        gate_a.display()
    );
    let command_b = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf 'so-sib-b-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        gate_b.display()
    );
    block_on(async {
        let (mut peer_a, key_a) = open_local_webrtc_peer(&endpoint, &bootstrap_a).await;
        let (mut peer_b, key_b) = open_local_webrtc_peer(&endpoint, &bootstrap_b).await;
        peer_a
            .encrypted_hello(&key_a, &webrtc_baseline_hello())
            .await
            .expect("hello a");
        peer_b
            .encrypted_hello(&key_b, &webrtc_baseline_hello())
            .await
            .expect("hello b");
        spawn_and_bind_webrtc(&mut peer_a, &key_a, session_a, sub_a, &command_a).await;
        fs::write(&gate_a, b"release").expect("release peer A output");
        spawn_and_bind_webrtc(&mut peer_b, &key_b, session_b, sub_b, &command_b).await;
        fs::write(&gate_b, b"release").expect("release peer B output");
        wait_for_webrtc_marker(&mut peer_a, &key_a, session_a, sub_a, "so-sib-a-ready").await;
        wait_for_webrtc_marker(&mut peer_b, &key_b, session_b, sub_b, "so-sib-b-ready").await;
        peer_a.peer.close().await.expect("close peer a");
        wait_for_webrtc_marker(&mut peer_b, &key_b, session_b, sub_b, "so-sib-b-ready").await;
        peer_b.peer.close().await.expect("close peer b");
    });
    shutdown_short_lived_session(&endpoint, session_a);
    shutdown_short_lived_session(&endpoint, session_b);
    hub.shutdown().expect("shutdown isolated hub");
}
