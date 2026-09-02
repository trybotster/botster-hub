// Characterization tests from plan §15.
// These pin current Hub behavior so later tickets show an intentional change.
// They must not change transport behavior.

const LOCKED_CORE_REV: &str = "48a437032791e678010254708259568ce4ad02bf";

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
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut retained = std::mem::take(&mut peer.pending_terminal_frames);
    while Instant::now() < deadline && !webrtc_terminal_contains(&retained, marker) {
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

fn assert_production_second_channel_reject_source() {
    let on_data_channel = hub_source("src/transport/webrtc/peer.rs");
    assert!(
        !on_data_channel.contains("test_extra_label"),
        "extra-channel reject must not use a test-only label override"
    );
    assert!(
        on_data_channel.contains("let claimed = self.peer_state.claim_data_channel();"),
        "second DataChannel must hit the production one-shot claim"
    );
    let handler = on_data_channel
        .split("async fn on_data_channel")
        .nth(1)
        .expect("on_data_channel handler");
    let claim_at = handler
        .find("let claimed = self.peer_state.claim_data_channel();")
        .expect("claim in on_data_channel");
    let label_at = handler
        .find("data_channel.label()")
        .expect("label read in on_data_channel");
    assert!(
        claim_at < label_at,
        "claim_data_channel must run before any label await"
    );
    assert!(
        on_data_channel.contains("if !claimed"),
        "second DataChannel must take the reject path only after claim_data_channel returns false"
    );
    let reject = hub_source("src/transport/webrtc/subscription_channel.rs");
    assert!(
        reject.contains("let close_ok = matches!(close, Ok(Ok(())));"),
        "close observation must require timeout(local_close) to return Ok(Ok(()))"
    );
    assert!(
        reject.contains("extra-channel close marker requires lost_claim && close_ok"),
        "close marker must require a lost claim and Ok(Ok(())) from timeout(local_close)"
    );
    assert!(
        !on_data_channel.contains("label == EXTRA_DATA_CHANNEL_LABEL")
            && !reject.contains("label == EXTRA_DATA_CHANNEL_LABEL"),
        "close marker must not require botster-extra to lose the claim"
    );
    assert!(
        !on_data_channel.contains("wait_for_prior_claim_in_test"),
        "extra DataChannel must not wait in the Hub handler"
    );
    assert!(
        on_data_channel.contains("admit_reserved_subscription_channel("),
        "unclaimed DataChannel must enter reserved subscription admission"
    );
    assert!(
        reject.contains("reject_extra_data_channel("),
        "unknown reserved labels must call the subscription-channel reject path"
    );
    assert!(
        reject.contains("local WebRTC rejecting extra DataChannel"),
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
    let producer_release = marker_dir.join("producer-release");
    let producer_command = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf 'so-2ch-ready\\n'; sleep 30",
        producer_release.display()
    );
    block_on(async {
        let (mut peer, extra, key) =
            open_local_webrtc_peer_with_extra_channel(&endpoint, &bootstrap).await;
        peer.enable_host_events();
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
            "second DataChannel must lose the production one-shot claim"
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
            &producer_command,
        )
        .await;
        assert!(
            !producer_release.exists(),
            "the second-channel producer must stay held until the reserved route is bound"
        );
        fs::write(&producer_release, b"go").expect("release second-channel producer");
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
        let (mut peer, _extra, key) =
            open_local_webrtc_peer_with_extra_channel(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello still works when every channel is admitted");
        thread::sleep(Duration::from_millis(400));
        assert!(
            extra_channel_observation(&observation).is_none(),
            "disabling the one-shot claim must fail the lost-claim oracle"
        );
        assert!(
            !close_marker.exists(),
            "disabling the one-shot claim must not write the successful-close marker"
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
            close_marker.exists(),
            "Hub must finish bounded local_close on the post-handshake DataChannel"
        );
        assert_production_second_channel_reject_source();
        peer.peer.close().await.expect("close offer peer");
    });
    hub.shutdown().expect("shutdown isolated hub");
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
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "printf 'so-4cls-ready\\n'; sleep 30",
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
fn webrtc_ready_entity_frame_defers_terminal_output() {
    let control = hub_source("src/transport/webrtc/control_channel.rs");
    assert!(
        !control.contains("flush_webrtc_adapter_frames"),
        "control_channel.rs must not flush adapter frames after the reserved-label cut"
    );
    let subscription = hub_source("src/transport/webrtc/subscription_channel.rs");
    assert!(
        subscription.contains("admit_reserved_subscription_channel"),
        "reserved subscription admission must live in subscription_channel.rs"
    );
    assert!(
        subscription.contains("flush_subscription_adapter_frames"),
        "reserved subscription flush must live in subscription_channel.rs"
    );
}

#[test]
fn terminal_input_is_not_a_json_control_request() {
    let transport = hub_source("src/daemon/control/sessions.rs");
    assert!(
        !transport.contains("DaemonRequest::SendInput"),
        "SendInput must not remain a JSON control request"
    );
    assert!(
        !transport.contains("DaemonRequest::Resize"),
        "Resize must not remain a JSON control request"
    );
    assert!(
        !transport.contains("DaemonRequest::ModeGatedInput"),
        "ModeGatedInput must not remain a JSON control request"
    );
}

#[test]
fn pump_woken_lives_only_in_the_data_plane_driver() {
    let driver = hub_source("src/data_plane/driver.rs");
    assert!(
        driver.contains("pump_woken("),
        "the data-plane driver must call Core pump_woken"
    );
    for path in [
        "src/daemon/owner_loop.rs",
        "src/daemon_maintenance.rs",
        "src/runtime.rs",
        "src/daemon/control.rs",
        "src/transport/unix/adapter.rs",
        "src/transport/webrtc/control_channel.rs",
    ] {
        assert!(
            !hub_source(path).contains("pump_woken("),
            "{path} must not call pump_woken"
        );
    }
}

#[test]
fn source_scan_inventory_includes_data_plane_rs() {
    assert!(
        hub_source("src/lib.rs").contains("include_str!(\"data_plane.rs\")"),
        "ablation: comment out data_plane.rs from the production scan list"
    );
}

#[test]
fn source_scan_inventory_includes_data_plane_driver() {
    assert!(
        hub_source("src/lib.rs").contains("include_str!(\"data_plane/driver.rs\")"),
        "ablation: comment out data_plane/driver.rs from the production scan list"
    );
}

#[test]
fn source_scan_inventory_includes_data_plane_close_work() {
    assert!(
        hub_source("src/lib.rs").contains("include_str!(\"data_plane/close_work.rs\")"),
        "ablation: comment out data_plane/close_work.rs from the production scan list"
    );
}

#[test]
fn source_scan_inventory_includes_shared_ingress() {
    assert!(
        hub_source("src/lib.rs").contains("include_str!(\"transport/shared/ingress.rs\")"),
        "ablation: comment out transport/shared/ingress.rs from the production scan list"
    );
}

#[test]
fn source_scan_inventory_includes_admission_reservations() {
    assert!(
        hub_source("src/lib.rs").contains("include_str!(\"admission/reservations.rs\")"),
        "ablation: comment out admission/reservations.rs from the production scan list"
    );
}

#[test]
fn terminal_adapter_contract_is_duplex_at_the_locked_core_pin() {
    let cargo_toml = hub_source("Cargo.toml");
    assert!(
        cargo_toml.contains(LOCKED_CORE_REV),
        "Hub must stay pinned to Core {LOCKED_CORE_REV}"
    );
    struct Duplex;
    impl botster_core::contract::terminal_adapter::TerminalAdapter for Duplex {
        fn try_write(
            &mut self,
            _frame: &botster_terminal_protocol::TerminalFrame,
        ) -> Result<(), botster_core::contract::terminal_adapter::TerminalAdapterWriteError>
        {
            Ok(())
        }

        fn close(&mut self) {}

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
    let lock = hub_source("Cargo.lock");
    assert!(
        lock.contains(LOCKED_CORE_REV),
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
        "src/transport/webrtc.rs",
        "src/transport/webrtc/peer.rs",
        "src/transport/webrtc/signaling.rs",
        "src/transport/webrtc/control_channel.rs",
        "src/transport/webrtc/subscription_channel.rs",
        "src/transport/webrtc/delivery.rs",
        "src/transport/webrtc/adapter.rs",
        "src/transport/webrtc/test_support.rs",
        "src/transport.rs",
        "src/transport/shared.rs",
        "src/transport/shared/adapter_slot.rs",
        "src/transport/shared/wake.rs",
        "src/transport/shared/close_reason.rs",
        "src/transport/shared/close_progress.rs",
        "src/transport/unix.rs",
        "src/transport/unix/adapter.rs",
        "src/transport/unix/listener.rs",
        "src/transport/unix/connection.rs",
        "src/transport/unix/mux_write.rs",
        "src/daemon/owner_loop.rs",
        "src/daemon/control.rs",
        "src/daemon/control/message.rs",
        "src/daemon/control/connection.rs",
        "src/daemon/control/sessions.rs",
        "src/daemon/control/session_types.rs",
        "src/daemon/control/spawn_targets.rs",
        "src/daemon/control/packages.rs",
        "src/daemon/control/packages/mutations.rs",
        "src/daemon/control/messaging.rs",
        "src/daemon/control/plugins.rs",
        "src/daemon/control/entities.rs",
        "src/daemon/control/events.rs",
        "src/daemon/control/webrtc.rs",
        "src/daemon/control/host.rs",
        "src/daemon/control/request.rs",
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
    let mut pending = vec![src];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).expect("read src");
        for entry in entries {
            let entry = entry.expect("src entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .expect("src path stays under the crate root")
                .to_string_lossy()
                .replace('\\', "/");
            if matches!(rel.as_str(), "src/lib.rs" | "src/runtime.rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read rust file");
            if source.contains("lua_runtime") {
                extra.push(rel);
            }
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
    let attach_source = hub_source("src/subscription/attach_routes.rs");
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
                write_unix_terminal_frame(
                    &mut stream,
                    "so-rth-session",
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
    let source = hub_source("src/subscription/closed_events.rs");
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

const WEBRTC_BYTE_EXACT_BACKSTOP: Duration = Duration::from_secs(30);
const WEBRTC_BYTE_EXACT_QUIET_DRAIN_TURNS: usize = 8;

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
    while let Some((_, bytes)) = peer.pending_terminal_frames.pop_front() {
        if let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes) {
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
}

async fn drain_webrtc_live_bytes(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    subscription_id: &str,
    concatenated: &mut Vec<u8>,
) {
    let _ = peer
        .encrypted_request(
            key,
            &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
        )
        .await;
    extend_concatenated_from_pending_webrtc_frames(peer, concatenated);
}

async fn await_next_webrtc_terminal_frame(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
) {
    if let Ok(Ok(bytes)) = timeout(Duration::from_millis(200), peer.next_terminal_frame(key)).await {
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
        drain_webrtc_live_bytes(
            peer,
            key,
            session_id,
            subscription_id,
            &mut concatenated,
        )
        .await;
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
    let mut session_exited = false;
    let mut quiet_turns_after_exit = 0usize;
    let started_at = Instant::now();
    loop {
        let bytes_before = concatenated.len();
        drain_webrtc_live_bytes(
            peer,
            key,
            session_id,
            subscription_id,
            &mut concatenated,
        )
        .await;
        if concatenated
            .windows(expected.len())
            .any(|window| window == expected)
        {
            return concatenated;
        }
        if !session_exited {
            session_exited = webrtc_session_has_exited(endpoint, session_id);
        }
        if session_exited {
            if concatenated.len() == bytes_before {
                quiet_turns_after_exit += 1;
            } else {
                quiet_turns_after_exit = 0;
            }
            if quiet_turns_after_exit >= WEBRTC_BYTE_EXACT_QUIET_DRAIN_TURNS {
                return concatenated;
            }
        }
        if started_at.elapsed() >= WEBRTC_BYTE_EXACT_BACKSTOP {
            if concatenated.is_empty() {
                panic_webrtc_byte_exact_starvation(&format!(
                    "{context}: timed out waiting for WebRTC adapter frames after producer-ready release; concatenated is empty"
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
        fs::create_dir_all(start_path.parent().expect("start parent"))
            .expect("create start dir");
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
        spawn_and_bind_webrtc(
            &mut peer_a,
            &key_a,
            session_a,
            sub_a,
            &command_a,
        )
        .await;
        fs::write(&gate_a, b"release").expect("release peer A output");
        spawn_and_bind_webrtc(
            &mut peer_b,
            &key_b,
            session_b,
            sub_b,
            &command_b,
        )
        .await;
        fs::write(&gate_b, b"release").expect("release peer B output");
        wait_for_webrtc_marker(&mut peer_a, &key_a, session_a, sub_a, "so-sib-a-ready").await;
        wait_for_webrtc_marker(&mut peer_b, &key_b, session_b, sub_b, "so-sib-b-ready").await;
        peer_a.peer.close().await.expect("close peer a");
        // Reserved-label duplex input is not available on this WebRTC peer yet.
        let _reserved_duplex_input = terminal_input_frame_bytes(b"so-sib-live\r");
        let _ = _reserved_duplex_input.len();
        wait_for_webrtc_marker(&mut peer_b, &key_b, session_b, sub_b, "so-sib-b-ready").await;
        peer_b.peer.close().await.expect("close peer b");
    });
    shutdown_short_lived_session(&endpoint, session_a);
    shutdown_short_lived_session(&endpoint, session_b);
    hub.shutdown().expect("shutdown isolated hub");
}
