// Characterization tests from plan §15.
// These pin current Hub behavior so later tickets show an intentional change.
// They must not change transport behavior.

const LOCKED_CORE_REV: &str = "9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6";

fn hub_source(relative: &str) -> String {
    std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

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
    subscription_id: &str,
    marker: &str,
) {
    let mut seen = std::collections::VecDeque::new();
    let mut last_error = None;
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline && !webrtc_terminal_contains(&seen, marker) {
        let drain = peer
            .encrypted_request(
                key,
                &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            )
            .await
            .expect("drain");
        assert!(
            drain
                .events
                .iter()
                .all(|event| !webrtc_event_is_terminal_body(event)),
            "content-blind drain must not return terminal bodies: {:?}",
            drain.events
        );
        match peer
            .next_terminal_frame_for(key, session_id, subscription_id)
            .await
        {
            Ok(bytes) => {
                last_error = None;
                seen.push_back((String::new(), bytes));
            }
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    assert!(
        webrtc_terminal_contains(&seen, marker),
        "missing terminal marker {marker:?} in {:?} last_error={last_error:?}",
        seen.iter()
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

fn assert_production_second_channel_reject_source() {
    let on_data_channel = hub_source("src/local_webrtc.rs");
    assert!(
        !on_data_channel.contains("test_extra_label"),
        "extra-channel reject must not use a test-only label override"
    );
    let handler = on_data_channel
        .split("async fn on_data_channel")
        .nth(1)
        .expect("on_data_channel handler");
    let label_at = handler
        .find("data_channel.label()")
        .expect("label read in on_data_channel");
    let control_at = handler
        .find("if label == CONTROL_CHANNEL_LABEL")
        .expect("control label match in on_data_channel");
    let claim_at = handler
        .find("self.peer_state.claim_data_channel()")
        .expect("claim in on_data_channel");
    assert!(
        label_at < control_at && control_at < claim_at,
        "the one-shot claim must run only after the control label matches"
    );
    assert!(
        handler.contains("if !self.peer_state.claim_data_channel()"),
        "a second control DataChannel must reject only after claim_data_channel returns false"
    );
    assert!(
        handler.contains("reject_opened_data_channel"),
        "an unreserved DataChannel must take the opened-channel reject path"
    );
    assert!(
        on_data_channel.contains("let close_ok = matches!(close, Ok(Ok(())));"),
        "close observation must require timeout(local_close) to return Ok(Ok(()))"
    );
    assert!(
        on_data_channel.contains("extra-channel close marker requires lost_claim && close_ok"),
        "close marker must require a lost claim and Ok(Ok(())) from timeout(local_close)"
    );
    assert!(
        !on_data_channel.contains("label == EXTRA_DATA_CHANNEL_LABEL"),
        "close marker must not require botster-extra to lose the claim"
    );
    assert!(
        !on_data_channel.contains("wait_for_prior_claim_in_test"),
        "extra DataChannel must not wait in the Hub handler"
    );
    assert!(
        on_data_channel.contains("local WebRTC rejecting extra DataChannel"),
        "rejected extra DataChannel must take the close path"
    );
}

#[test]
fn webrtc_peer_rejects_a_second_data_channel() {
    let _guard = daemon_test_guard();
    let marker_dir = unique_test_dir("so-2ch-close");
    std::fs::create_dir_all(&marker_dir).expect("create extra-channel close marker dir");
    let marker_dir = marker_dir
        .canonicalize()
        .expect("canonicalize extra-channel close marker dir");
    let close_marker = marker_dir.join("extra-closed");
    let observation = marker_dir.join("extra-observation.json");
    let marker = close_marker.to_string_lossy().into_owned();
    let observation_path = observation.to_string_lossy().into_owned();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub_with_env(
        "so-2ch",
        &[
            ("BOTSTER_HUB_TEST_EXTRA_CHANNEL_CLOSE_MARKER", marker.as_str()),
            (
                "BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION",
                observation_path.as_str(),
            ),
        ],
    );
    let session_id = "so-2ch-session";
    let subscription_id = "so-2ch-sub";
    block_on(async {
        let (mut peer, extra, key) =
            open_local_webrtc_peer_with_extra_channel(&endpoint, &bootstrap).await;
        let mut rejected = peer
            .admit_surviving_dual_channel(extra, &key, &webrtc_terminal_adapter_hello())
            .await
            .expect("exactly one initial DataChannel completes encrypted Hello");
        assert!(
            wait_for_path(&observation, Duration::from_secs(10)),
            "Hub must observe the production extra-channel reject"
        );
        let (lost_claim, close_ok, label) = extra_channel_observation(&observation)
            .expect("extra-channel observation must be valid JSON");
        assert!(
            lost_claim,
            "unreserved second DataChannel must not claim the control channel"
        );
        assert!(
            close_ok,
            "timeout(local_close) must return Ok(Ok(())) for the rejected DataChannel"
        );
        assert!(
            label == "botster-client" || label == "botster-extra",
            "rejected label must be one of the initial-offer channels: {label}"
        );
        assert_eq!(
            label, rejected.label,
            "observation label must match the channel that failed Hello"
        );
        assert_ne!(
            label, peer.control_label,
            "exactly one initial-offer channel survives the one-shot claim"
        );
        assert!(
            close_marker.exists(),
            "Hub must finish local_close on the rejected DataChannel"
        );
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "sleep 1; printf 'so-2ch-ready\\n'; sleep 30",
        )
        .await;
        wait_for_webrtc_marker(&mut peer, &key, session_id, subscription_id, "so-2ch-ready")
            .await;
        assert_eq!(
            rejected
                .count_terminal_frames(Duration::from_millis(400))
                .await,
            0,
            "rejected DataChannel must not receive terminal frames"
        );
        assert_production_second_channel_reject_source();
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_peer_rejects_a_second_data_channel_requires_one_shot_claim() {
    let _guard = daemon_test_guard();
    let marker_dir = unique_test_dir("so-2ch-neg");
    std::fs::create_dir_all(&marker_dir).expect("create extra-channel negative-control dir");
    let marker_dir = marker_dir
        .canonicalize()
        .expect("canonicalize extra-channel negative-control dir");
    let close_marker = marker_dir.join("extra-closed");
    let observation = marker_dir.join("extra-observation.json");
    let marker = close_marker.to_string_lossy().into_owned();
    let observation_path = observation.to_string_lossy().into_owned();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub_with_env(
        "so-2ch-neg",
        &[
            ("BOTSTER_HUB_TEST_DISABLE_ONE_SHOT_CLAIM", "1"),
            ("BOTSTER_HUB_TEST_EXTRA_CHANNEL_CLOSE_MARKER", marker.as_str()),
            (
                "BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION",
                observation_path.as_str(),
            ),
        ],
    );
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello still works when every control channel is admitted");
        let second_control = peer
            .create_labeled_data_channel("botster-client")
            .await
            .expect("second control-labeled channel opens when the one-shot claim is disabled");
        assert_eq!(second_control.label, "botster-client");
        thread::sleep(Duration::from_millis(400));
        assert!(
            extra_channel_observation(&observation).is_none(),
            "disabling the control-channel claim must fail the lost-claim oracle"
        );
        assert!(
            !close_marker.exists(),
            "disabling the control-channel claim must not write the successful-close marker"
        );
        assert_production_second_channel_reject_source();
        peer.peer.close().await.expect("close offer peer");
    });
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_peer_post_handshake_data_channel_reaches_production_reject() {
    let _guard = daemon_test_guard();
    let marker_dir = unique_test_dir("so-post-hs-close");
    std::fs::create_dir_all(&marker_dir).expect("create extra-channel close marker dir");
    let marker_dir = marker_dir
        .canonicalize()
        .expect("canonicalize extra-channel close marker dir");
    let close_marker = marker_dir.join("extra-closed");
    let observation = marker_dir.join("extra-observation.json");
    let marker = close_marker.to_string_lossy().into_owned();
    let observation_path = observation.to_string_lossy().into_owned();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub_with_env(
        "so-post-hs",
        &[
            ("BOTSTER_HUB_TEST_EXTRA_CHANNEL_CLOSE_MARKER", marker.as_str()),
            (
                "BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION",
                observation_path.as_str(),
            ),
        ],
    );
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("encrypted Hello takes the one-shot claim before the late channel");
        let extra = peer
            .create_extra_data_channel()
            .await
            .expect("post-handshake extra DataChannel must open on the offerer");
        assert_eq!(extra.label, "botster-extra");
        assert!(
            wait_for_path(&observation, Duration::from_secs(10)),
            "Hub must observe the production extra-channel reject after handshake"
        );
        let (lost_claim, close_ok, label) = extra_channel_observation(&observation)
            .expect("extra-channel observation must be valid JSON");
        assert!(
            lost_claim,
            "post-handshake DataChannel must lose the production one-shot claim"
        );
        assert!(
            close_ok,
            "timeout(local_close) must return Ok(Ok(())) for the rejected DataChannel"
        );
        assert_eq!(label, "botster-extra");
        assert!(
            wait_for_path(&close_marker, Duration::from_secs(10)),
            "Hub must finish bounded local_close on the post-handshake DataChannel"
        );
        assert_production_second_channel_reject_source();
        peer.peer.close().await.expect("close offer peer");
    });
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_shared_channel_carries_control_entity_event_and_terminal_frames() {
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
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "sleep 1; printf 'so-4cls-ready\\n'; sleep 30",
        )
        .await;
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
            if let Ok(Ok(frame)) = timeout(Duration::from_millis(250), peer.next_entity_frame(&key)).await
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
            if let Ok(Ok(_)) = timeout(Duration::from_millis(250), peer.next_host_event(&key)).await {
                saw_host_event = true;
            }
        }
        assert!(
            !peer.pending_entity_frames.is_empty(),
            "the control channel must still carry entity frames"
        );
        assert!(saw_host_event, "the control channel must still carry host events");
        assert!(
            peer.pending_terminal_frames.is_empty(),
            "the control channel must not carry terminal frames after the dedicated-channel cut"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn terminal_input_travels_as_a_json_control_request() {
    let _guard = daemon_test_guard();
    let transport = hub_source("src/daemon_transport.rs");
    assert!(
        transport.contains("DaemonRequest::SendInput { session_id, data } =>"),
        "SendInput must stay a JSON control request"
    );
    assert!(
        transport.contains("HubClientRequest::Input {"),
        "SendInput must reach HubClientApi as Input"
    );
    let client = hub_source("src/client_api.rs");
    assert!(
        client.contains("runtime\n                    .write_bytes(")
            || client.contains(".write_bytes("),
        "Input must reach HubRuntime::write_bytes"
    );
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("so-json");
    let session_id = "so-json-session";
    let subscription_id = "so-json-sub";
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
            "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        )
        .await;
        wait_for_webrtc_marker(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "\"state\":\"attaching\"",
        )
        .await;
        let sent = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: session_id.to_string(),
                    data: "so-json-input\r".to_string(),
                },
            )
            .await
            .expect("SendInput JSON control request");
        assert_ne!(
            sent.kind,
            botster_hub_client::DaemonResponseKind::OperatorError,
            "SendInput must return a control response: {:?}",
            sent.error
        );
        wait_for_webrtc_marker(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "echo:so-json-input",
        )
        .await;
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn locked_core_rev_is_the_rolled_duplex_pin() {
    assert!(
        hub_source("Cargo.toml").contains(LOCKED_CORE_REV),
        "Hub must stay pinned to Core {LOCKED_CORE_REV}"
    );
    assert!(
        hub_source("Cargo.lock").contains(LOCKED_CORE_REV),
        "Cargo.lock must pin Core {LOCKED_CORE_REV}"
    );
}

#[test]
fn no_lua_dispatch_in_terminal_input_or_output() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut lua_importers = Vec::new();
    for entry in ["src/lib.rs", "src/runtime.rs"] {
        let source = hub_source(entry);
        assert!(
            source.contains("lua_runtime"),
            "{entry} must remain a lua_runtime import site"
        );
        lua_importers.push(entry);
    }
    for entry in [
        "src/local_webrtc.rs",
        "src/webrtc_terminal_adapter.rs",
        "src/unix_terminal_adapter.rs",
        "src/daemon_transport.rs",
        "src/client_api.rs",
    ] {
        let source = hub_source(entry);
        assert!(
            !source.contains("lua_runtime"),
            "{entry} must stay out of Lua dispatch; importers={lua_importers:?}"
        );
    }
    let mut extra = Vec::new();
    let src = root.join("src");
    let entries = std::fs::read_dir(&src).expect("read src");
    for entry in entries {
        let entry = entry.expect("src entry");
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if matches!(name, "lib.rs" | "runtime.rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("read rust file");
        if source.contains("lua_runtime") {
            extra.push(name.to_string());
        }
    }
    assert!(
        extra.is_empty(),
        "unexpected lua_runtime importers in src/: {extra:?}"
    );
}

fn unix_envelope_snapshot_bytes(
    envelope: &botster_hub_client::DaemonUnixTerminalEnvelope,
) -> Option<Vec<u8>> {
    let bytes = envelope.payload_bytes().ok()?;
    if bytes.starts_with(GHOSTSNP_MAGIC) {
        return Some(bytes);
    }
    match serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes) {
        Ok(botster_hub_client::DaemonEvent::Snapshot { history, .. }) => {
            history.decoded_bytes().ok().map(|decoded| decoded.to_vec())
        }
        _ => None,
    }
}

fn apply_ready_then_history_progress(
    projection: &mut botster_terminal_ghostty::GhosttyClientProjection,
    bytes: Vec<u8>,
    saw_ready: bool,
) -> botster_terminal_ghostty::GhosttySnapshotDecodeProgress {
    if !saw_ready {
        projection
            .install_ghostsnp_ready(bytes)
            .expect("READY snapshot")
    } else {
        projection
            .apply_ghostsnp_history(bytes)
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
    let (mut stream, ack) = botster_hub_client::connect_and_hello_with_terminal_requirement(
        &endpoint,
        &botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
        Some(&terminal),
    )
    .expect("ready_then_history hello");
    assert!(ack.terminal_compatibility.is_some());
    let attach_source = hub_source("src/daemon_attach_stream.rs");
    assert!(
        attach_source.contains("for_ready_then_history_attach()"),
        "Hub must advertise the ready_then_history split"
    );
    let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "so-rth-session",
        "so-rth-sub",
        "sleep 1; printf 'so-rth-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
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
                let input = request_collecting_mux(
                    &mut stream,
                    &mut reader,
                    &botster_hub_client::DaemonRequest::SendInput {
                        session_id: "so-rth-session".to_string(),
                        data: "so-rth-input\r".to_string(),
                    },
                    &mut envelopes,
                    &mut events,
                );
                assert_ne!(
                    input.kind,
                    botster_hub_client::DaemonResponseKind::OperatorError,
                    "input must be permitted at READY: {:?}",
                    input.error
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
        let drain = request_collecting_mux(
            &mut stream,
            &mut reader,
            &botster_hub_client::DaemonRequest::drain_subscription("so-rth-session", "so-rth-sub"),
            &mut envelopes,
            &mut events,
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !matches!(event, botster_hub_client::DaemonEvent::Snapshot { .. })),
            "bound drain must not return snapshot bodies on the host plane: {:?}",
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
    let source = hub_source("src/daemon_transport.rs");
    assert!(
        source.contains("fn shutdown_session_arm_installs_exact_suppression_before_core_request"),
        "unit suppression-before-teardown proof must remain"
    );
    let hub = start_isolated_live_output_hub("so-sup");
    let endpoint = hub.endpoint().clone();
    let mut requirement =
        botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter();
    requirement
        .required_features
        .push(botster_hub_client::FEATURE_ATTACH_OCCUPANCY.to_string());
    let stream = botster_hub_client::connect_and_hello_with_requirement(&endpoint, &requirement)
        .expect("unix+occupancy hello");
    let mut reader = std::io::BufReader::new(stream.try_clone().expect("clone"));
    let mut stream = stream;
    let mut envelopes = Vec::new();
    let mut events = Vec::new();
    spawn_and_bind(
        &mut stream,
        &mut reader,
        "so-sup-session",
        "so-sup-sub",
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
            .expect("status before ShutdownSession")
            .live_attach_occupancy,
        "so-sup-session",
        "so-sup-sub",
    )
    .expect("attached route must publish a Core generation");
    let shutdown = request_collecting_mux(
        &mut stream,
        &mut reader,
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
    let _after = request_collecting_mux(
        &mut stream,
        &mut reader,
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

#[test]
fn webrtc_terminal_output_is_byte_exact() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0xff, 0xc0];
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("so-bytes");
    let session_id = "so-bytes-session";
    let subscription_id = "so-bytes-sub";
    let release_path = unique_short_test_dir("so-bytes-release").join("go");
    let script_path = write_python_wait_then_write_script(&release_path, expected);
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
        fs::create_dir_all(release_path.parent().expect("release parent"))
            .expect("create release dir");
        fs::write(&release_path, b"go").expect("release writer");
        let mut concatenated = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let _ = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::drain_subscription(
                        session_id,
                        subscription_id,
                    ),
                )
                .await;
            while let Some((_, bytes)) = peer.pending_terminal_frames.pop_front() {
                if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes)
                {
                    if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event {
                        let decoded = live_output_decoded_bytes(payload);
                        assert!(
                            !payload_has_utf8_replacement(&decoded),
                            "live payload must not contain U+FFFD: {decoded:?}"
                        );
                        concatenated.extend(decoded);
                    }
                } else {
                    assert!(
                        !payload_has_utf8_replacement(&bytes),
                        "live payload must not contain U+FFFD: {bytes:?}"
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
            if let Ok(Ok(bytes)) =
                timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
            {
                peer.pending_terminal_frames
                    .push_back((String::new(), bytes));
            }
        }
        assert!(
            concatenated
                .windows(expected.len())
                .any(|window| window == expected),
            "WebRTC adapter frames must preserve exact bytes, got {concatenated:?}"
        );
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
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("so-sib");
    let session_a = "so-sib-a";
    let session_b = "so-sib-b";
    let sub_a = "so-sib-sub-a";
    let sub_b = "so-sib-sub-b";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_baseline_hello())
            .await
            .expect("hello");
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_a,
            sub_a,
            "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        )
        .await;
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_b,
            sub_b,
            "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        )
        .await;
        wait_for_webrtc_marker(&mut peer, &key, session_a, sub_a, "\"state\":\"attaching\"").await;
        wait_for_webrtc_marker(&mut peer, &key, session_b, sub_b, "\"state\":\"attaching\"").await;
        let detach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Detach {
                    session_id: session_a.to_string(),
                    subscription_id: sub_a.to_string(),
                },
            )
            .await
            .expect("detach sibling channel");
        assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
        let sent = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: session_b.to_string(),
                    data: "so-sib-live\r".to_string(),
                },
            )
            .await
            .expect("sibling input");
        assert_ne!(
            sent.kind,
            botster_hub_client::DaemonResponseKind::OperatorError,
            "sibling control must stay live: {:?}",
            sent.error
        );
        let listed = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::ListSessions)
            .await
            .expect("list after sibling detach");
        assert!(
            listed
                .sessions
                .iter()
                .any(|session| session.session_id == session_b && session.lifecycle == "running"),
            "sibling session must stay running after the other channel detaches: {:?}",
            listed.sessions
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_a);
    shutdown_short_lived_session(&endpoint, session_b);
    hub.shutdown().expect("shutdown isolated hub");
}
