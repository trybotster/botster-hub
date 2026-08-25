fn webrtc_terminal_adapter_hello() -> botster_hub_client::DaemonHello {
    botster_hub_client::DaemonHello {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        compatibility:
            botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
        terminal_compatibility: None,
    }
}

fn webrtc_package_event_hello() -> botster_hub_client::DaemonHello {
    let mut compatibility =
        botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
    compatibility
        .required_features
        .push(botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string());
    compatibility.minimum_conformance_fixture_revision =
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::DaemonHello {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        compatibility,
        terminal_compatibility: None,
    }
}

fn enable_event_plane_producer_on_hub(endpoint: &botster_hub_client::DaemonEndpoint, label: &str) {
    let producer_src =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
    let producer_dir = unique_test_dir(&format!("event-plane-producer-{label}"));
    copy_dir_all(&producer_src, &producer_dir);
    rewrite_package_source_path(&producer_dir);
    let enabled = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: producer_dir,
        },
    )
    .expect("enable producer");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
}

fn webrtc_close_event_hello() -> botster_hub_client::DaemonHello {
    botster_hub_client::DaemonHello {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        compatibility:
            botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed(),
        terminal_compatibility: None,
    }
}

async fn spawn_and_bind_webrtc(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    subscription_id: &str,
    command: &str,
) {
    let spawned = peer
        .encrypted_request(
            key,
            &botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: command.to_string(),
            },
        )
        .await
        .expect("spawn");
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let attach = peer
        .encrypted_request(
            key,
            &botster_hub_client::DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
        )
        .await
        .expect("attach");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
    assert!(
        attach.events.is_empty(),
        "WebRTC reserve must return empty Attach bodies: {:?}",
        attach.events
    );
    let label = attach
        .subscription_channel_label
        .as_deref()
        .expect("Attach returns the reserved subscription channel label");
    assert_eq!(
        attach.subscription_channel_generation,
        Some(
            label
                .rsplit('/')
                .next()
                .and_then(|generation| generation.parse().ok())
                .expect("reserved label carries generation")
        )
    );
    peer.open_reserved_subscription_channel(label)
        .await
        .expect("browser creates the reserved terminal DataChannel");
}

fn webrtc_terminal_contains(
    frames: &std::collections::VecDeque<(String, Vec<u8>)>,
    needle: &str,
) -> bool {
    frames.iter().any(|(_, bytes)| {
        if bytes
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
        {
            return true;
        }
        let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(bytes) else {
            return false;
        };
        match event {
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
                live_output_contains(&payload, needle)
            }
            _ => false,
        }
    })
}

async fn wait_for_webrtc_subscription_closed(
    peer: &mut LocalWebrtcOfferPeer,
    key: &botster_core::AesGcmKey,
    session_id: &str,
    subscription_id: &str,
) -> Option<botster_hub_client::DaemonEvent> {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Some(index) = peer.pending_host_events().iter().position(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id: closed_session,
                    subscription_id: closed_subscription,
                    ..
                } if closed_session == session_id && closed_subscription == subscription_id
            )
        }) {
            return peer.take_pending_host_event_at(index);
        }
        match timeout(Duration::from_millis(200), peer.next_host_event(key)).await {
            Ok(Ok(event)) => {
                if matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                        session_id: ref closed_session,
                        subscription_id: ref closed_subscription,
                        ..
                    } if closed_session == session_id && closed_subscription == subscription_id
                ) {
                    return Some(event);
                }
            }
            Ok(Err(_)) | Err(_) => {
                let _ = timeout(Duration::from_millis(20), peer.next_terminal_frame(key)).await;
            }
        }
    }
    None
}

fn issue_second_webrtc_bootstrap(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
) -> botster_hub_client::DaemonLocalWebrtcBootstrap {
    botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: bootstrap.package_name.clone(),
            entrypoint_id: bootstrap.entrypoint_id.clone(),
            origin: bootstrap.expected_origin.clone(),
        },
    )
    .expect("issue second local WebRTC bootstrap")
    .local_webrtc_bootstrap
    .expect("second bootstrap response includes local WebRTC bootstrap")
}

fn start_webrtc_adapter_hub(
    name: &str,
) -> (
    botster_hub_test_support::IsolatedHub,
    botster_hub_client::DaemonEndpoint,
    botster_hub_client::DaemonLocalWebrtcBootstrap,
) {
    start_webrtc_adapter_hub_with_env(name, &[])
}

fn start_webrtc_adapter_hub_with_env(
    name: &str,
    extra_env: &[(&str, &str)],
) -> (
    botster_hub_test_support::IsolatedHub,
    botster_hub_client::DaemonEndpoint,
    botster_hub_client::DaemonLocalWebrtcBootstrap,
) {
    let hub = start_isolated_live_output_hub_with_env(name, extra_env);
    let package_dir = unique_test_dir(&format!("{name}-web"));
    write_botster_web_package(&package_dir);
    enable_supervised_package(hub.data_dir(), &package_dir);
    let endpoint = hub.endpoint().clone();
    let (_origin, bootstrap) = start_botster_web_and_issue_bootstrap(&endpoint);
    (hub, endpoint, bootstrap)
}

fn webrtc_event_is_terminal_body(event: &botster_hub_client::DaemonEvent) -> bool {
    matches!(
        event,
        botster_hub_client::DaemonEvent::AttachState { .. }
            | botster_hub_client::DaemonEvent::Snapshot { .. }
            | botster_hub_client::DaemonEvent::Scrollback { .. }
            | botster_hub_client::DaemonEvent::TerminalOutput { .. }
            | botster_hub_client::DaemonEvent::ProcessExit { .. }
    )
}

fn cleanup_delta(
    before: &botster_hub_client::DaemonLifecycleCounters,
    after: &botster_hub_client::DaemonLifecycleCounters,
    reason: &str,
) -> u64 {
    after
        .cleanup_by_reason
        .get(reason)
        .copied()
        .unwrap_or(0)
        .saturating_sub(before.cleanup_by_reason.get(reason).copied().unwrap_or(0))
}

#[test]
fn webrtc_terminal_adapter_bind_returns_only_attaching_then_terminal_frames() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wab");
    let session_id = "wab-session";
    let subscription_id = "wab-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        let ack = peer
            .encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("datachannel hello");
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER)
        );

        let spawned = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: session_id.to_string(),
                    command: "printf 'webrtc-adapter-ready\\n'; sleep 30".to_string(),
                },
            )
            .await
            .expect("spawn");
        assert_eq!(
            spawned.kind,
            botster_hub_client::DaemonResponseKind::Spawned
        );

        let attach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                },
            )
            .await
            .expect("attach");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        assert!(
            attach.events.is_empty(),
            "WebRTC Attach must not return terminal bodies: {:?}",
            attach.events
        );
        let label = attach
            .subscription_channel_label
            .as_deref()
            .expect("Attach returns the reserved subscription channel label");
        peer.open_reserved_subscription_channel(label)
            .await
            .expect("browser creates the reserved terminal DataChannel");

        let deadline = Instant::now() + Duration::from_secs(8);
        let mut saw_terminal_frame = false;
        while Instant::now() < deadline && !saw_terminal_frame {
            let drain = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::drain_subscription(
                        session_id,
                        subscription_id,
                    ),
                )
                .await
                .expect("bound drain");
            assert!(
                drain
                    .events
                    .iter()
                    .all(|event| !webrtc_event_is_terminal_body(event)),
                "bound drain must not emit terminal bodies: {:?}",
                drain.events
            );
            if let Ok(bytes) =
                timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
            {
                let bytes = bytes.expect("terminal frame");
                assert!(!bytes.is_empty());
                saw_terminal_frame = true;
            }
        }
        assert!(
            saw_terminal_frame,
            "later frames must arrive as DaemonTerminalFrame chunks"
        );

        let listed = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::ListSessions)
            .await
            .expect("list");
        assert!(
            listed
                .sessions
                .iter()
                .any(|session| session.session_id == session_id)
        );
        peer.peer.close().await.expect("close offer peer");
    });
    eprintln!(
        "webrtc adapter bind provenance hub_bin={} session_worker={}",
        env!("CARGO_BIN_EXE_botster-hub"),
        session_worker_binary_path().display()
    );
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_attach_emits_a_nonempty_frame_without_host_drain() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wac");
    let session_id = "wac-session";
    let subscription_id = "wac-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("datachannel hello");
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            session_id,
            subscription_id,
            "printf 'webrtc-attach-pumped\\n'; sleep 30",
        )
        .await;

        let deadline = Instant::now() + Duration::from_secs(8);
        let mut saw_terminal_frame = false;
        while Instant::now() < deadline && !saw_terminal_frame {
            if let Ok(bytes) =
                timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
            {
                let bytes = bytes.expect("terminal frame");
                assert!(!bytes.is_empty());
                saw_terminal_frame = true;
            }
        }
        assert!(
            saw_terminal_frame,
            "Attach must emit a nonempty adapter frame without a later host Drain or ReadScreen"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("w2c");
    let session_id = "w2c-session";
    let subscription_id = "w2c-sub";
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
            "printf 'webrtc-two-channel-ready\\n'; sleep 30",
        )
        .await;
        let mut extra = peer
            .create_extra_data_channel()
            .await
            .expect("post-handshake extra DataChannel must open before isolation is measured");
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut saw_terminal_frame = false;
        while Instant::now() < deadline && !saw_terminal_frame {
            let drain = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::drain_subscription(
                        session_id,
                        subscription_id,
                    ),
                )
                .await
                .expect("bound drain");
            assert!(
                drain
                    .events
                    .iter()
                    .all(|event| !webrtc_event_is_terminal_body(event)),
                "bound drain must not emit terminal bodies: {:?}",
                drain.events
            );
            if let Ok(bytes) =
                timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
            {
                let bytes = bytes.expect("terminal frame");
                assert!(!bytes.is_empty());
                saw_terminal_frame = true;
            }
        }
        assert!(
            saw_terminal_frame,
            "admitted channel must receive DaemonTerminalFrame chunks"
        );
        let extra_deadline = Instant::now() + Duration::from_millis(400);
        let mut extra_terminal_frames = 0;
        while Instant::now() < extra_deadline {
            match timeout(Duration::from_millis(50), extra.messages.recv()).await {
                Ok(Some(message)) => {
                    if let Ok(chunk) = serde_json::from_str::<
                        botster_hub_client::DaemonLocalWebrtcDeliveryChunk,
                    >(&message)
                        && chunk.delivery_kind
                            == botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame
                    {
                        extra_terminal_frames += 1;
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        assert_eq!(
            extra_terminal_frames, 0,
            "rejected extra DataChannel must not receive terminal frames"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_unbound_attach_still_drains_snapshot_without_terminal_frames() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wau");
    let session_id = "wau-session";
    let subscription_id = "wau-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello");
        let spawned = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: session_id.to_string(),
                    command: "printf 'unbound-webrtc-ready\\n'; sleep 30".to_string(),
                },
            )
            .await
            .expect("spawn");
        assert_eq!(
            spawned.kind,
            botster_hub_client::DaemonResponseKind::Spawned
        );
        let attach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                },
            )
            .await
            .expect("always-bind attach");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        assert!(
            attach.events.is_empty(),
            "WebRTC Attach must not return Snapshot: {:?}",
            attach.events
        );
        let drain = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::drain_subscription(session_id, subscription_id),
            )
            .await
            .expect("host drain");
        assert!(
            drain.events.is_empty(),
            "host Drain must not return Snapshot: {:?}",
            drain.events
        );
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut text = String::new();
        while Instant::now() < deadline {
            let screen = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::ReadScreen {
                        session_id: session_id.to_string(),
                    },
                )
                .await
                .expect("read screen");
            text = screen
                .read_screen
                .as_ref()
                .map(|screen| screen.text.clone())
                .unwrap_or_default();
            if text.contains("unbound-webrtc-ready") {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            text.contains("unbound-webrtc-ready"),
            "visible text is on ReadScreen: {text:?}"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wpl");
    let session_id = "wpl-session";
    let subscription_id = "wpl-sub";
    let before = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("status before")
        .status
        .expect("status body")
        .lifecycle_counters;
    let attached = block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello");
        spawn_and_bind_webrtc(&mut peer, &key, session_id, subscription_id, "sleep 30").await;
        let attached =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after attach")
                .status
                .expect("status body")
                .lifecycle_counters;
        peer.peer.close().await.expect("close offer peer");
        attached
    });
    assert!(
        attached.live_attach_subscriptions >= 1,
        "bind must occupy a live attach route: {attached:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut after = before.clone();
    while Instant::now() < deadline {
        after = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after")
            .status
            .expect("status body")
            .lifecycle_counters;
        if cleanup_delta(&before, &after, "bound_adapter_close") >= 1 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        cleanup_delta(&before, &after, "cleanup_hub_detach"),
        0,
        "bound peer loss must not Hub-Detach: before={before:?} after={after:?}"
    );
    assert!(
        cleanup_delta(&before, &after, "bound_adapter_close") >= 1,
        "bound peer loss must close the adapter: before={before:?} after={after:?}"
    );
    assert!(
        after.live_attach_subscriptions < attached.live_attach_subscriptions,
        "bound occupancy must drop after adapter close: attached={attached:?} after={after:?}"
    );
    let listed =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list");
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "host session stays listed after bound peer loss"
    );
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_explicit_detach_is_separate_from_peer_loss() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wad");
    let session_id = "wad-session";
    let subscription_id = "wad-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello");
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "sleep 30".to_string(),
            },
        )
        .await
        .expect("spawn");
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
        )
        .await
        .expect("attach");
        let detach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Detach {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                },
            )
            .await
            .expect("detach");
        assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status");
        let counters = status.status.expect("status body").lifecycle_counters;
        assert_eq!(
            counters.cleanup_by_reason.get("explicit_detach").copied(),
            Some(1),
            "explicit Detach must use the authorized path: {counters:?}"
        );
        let second = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Detach {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                },
            )
            .await
            .expect("second detach");
        assert_ne!(
            second.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wla");
    let session_id = "wla-session";
    let subscription_id = "wla-sub";
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("hello");
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "sleep 30".to_string(),
            },
        )
        .await
        .expect("spawn");
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
        )
        .await
        .expect("attach");
        peer.peer.close().await.expect("close offer peer");
    });
    thread::sleep(Duration::from_millis(400));
    let late = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    )
    .expect("socket attach after webrtc close is a new owner");
    assert_eq!(late.kind, botster_hub_client::DaemonResponseKind::Events);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_feature_does_not_raise_default_requirement() {
    let requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    let mut previous = botster_hub_client::DaemonCompatibility::current();
    previous
        .features
        .retain(|feature| feature != botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER);
    previous.conformance_fixture_revision =
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::ensure_compatible(&requirement, &previous)
        .expect("default clients still accept a daemon without the webrtc adapter feature");

    let adapter_requirement =
        botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
    botster_hub_client::ensure_compatible(&adapter_requirement, &previous)
        .expect_err("the webrtc adapter requirement must fail closed without the feature");
    assert_eq!(botster_hub_client::PROTOCOL_VERSION, 7);
}

#[test]
fn webrtc_terminal_adapter_source_does_not_name_snapshot_phases() {
    let sources = [
        include_str!("../../src/webrtc_terminal_adapter.rs"),
        include_str!("webrtc_terminal_adapter.rs"),
    ];
    for source in sources {
        let production = source.split("mod tests").next().unwrap_or(source);
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "webrtc adapter proofs must stay content-blind: found {forbidden}"
            );
        }
    }
}

#[test]
fn webrtc_terminal_adapter_host_close_emits_negotiated_terminal_subscription_closed() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("whc");
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        let ack = peer
            .encrypted_hello(&key, &webrtc_close_event_hello())
            .await
            .expect("negotiated hello");
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
        );
        spawn_and_bind_webrtc(&mut peer, &key, "whc-a", "sub-a", "sleep 30").await;
        spawn_and_bind_webrtc(&mut peer, &key, "whc-b", "sub-b", "sleep 30").await;
        let reattach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "whc-a".to_string(),
                    subscription_id: "sub-a".to_string(),
                },
            )
            .await
            .expect("host close via replacement attach");
        assert_eq!(
            reattach.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        let closed = wait_for_webrtc_subscription_closed(&mut peer, &key, "whc-a", "sub-a")
            .await
            .expect("negotiated host close must emit TerminalSubscriptionClosed");
        match closed {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                generation,
                reason,
            } => {
                assert_eq!(session_id, "whc-a");
                assert_eq!(subscription_id, "sub-a");
                assert!(generation >= 1);
                assert_eq!(
                    reason,
                    botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
                );
            }
            other => panic!("unexpected host event: {other:?}"),
        }
        assert_eq!(
            peer.pending_host_events()
                .iter()
                .filter(|event| matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                        session_id,
                        ..
                    } if session_id == "whc-a"
                ))
                .count(),
            0,
            "exactly one close event must be consumed"
        );
        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status after host close");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let listed = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::ListSessions)
            .await
            .expect("list after host close");
        assert!(
            listed
                .sessions
                .iter()
                .any(|session| session.session_id == "whc-b")
        );
        let sibling_deadline = Instant::now() + Duration::from_secs(8);
        let mut saw_sibling = webrtc_terminal_contains(&peer.pending_terminal_frames, "whc-b")
            || !peer.pending_terminal_frames.is_empty();
        while Instant::now() < sibling_deadline && !saw_sibling {
            if timeout(Duration::from_millis(200), peer.next_terminal_frame(&key))
                .await
                .is_ok()
            {
                saw_sibling = true;
            }
        }
        assert!(
            saw_sibling || !peer.pending_terminal_frames.is_empty(),
            "sibling must keep delivering daemon_terminal_frame after host close"
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, "whc-a");
    shutdown_short_lived_session(&endpoint, "whc-b");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wwb");
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        peer.encrypted_hello(&key, &webrtc_close_event_hello())
            .await
            .expect("negotiated hello");
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            "wwb-stall",
            "sub-stall",
            "yes write-budget-stall",
        )
        .await;
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            "wwb-live",
            "sub-live",
            "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        )
        .await;

        let closed = wait_for_webrtc_subscription_closed(&mut peer, &key, "wwb-stall", "sub-stall")
            .await
            .expect("keep-reading observer must see core_adapter_closed");
        match closed {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { reason, .. } => {
                assert_eq!(
                    reason,
                    botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER,
                    "write-budget must keep the Core reason"
                );
            }
            other => panic!("unexpected host event: {other:?}"),
        }
        assert!(
            peer.pending_host_events().iter().all(|event| {
                !matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                        session_id,
                        reason,
                        ..
                    } if session_id == "wwb-stall"
                        && reason == botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
                )
            }),
            "host_adapter_closed is not the Core write-budget oracle: {:?}",
            peer.pending_host_events()
        );

        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status after core close");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let listed = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::ListSessions)
            .await
            .expect("list sessions after core close");
        assert!(
            listed
                .sessions
                .iter()
                .any(|session| session.session_id == "wwb-live" && session.lifecycle == "running"),
            "sibling session must stay running after stall write-budget close: {:?}",
            listed.sessions
        );
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::SendInput {
                session_id: "wwb-live".to_string(),
                data: "wwb-sibling-live\r".to_string(),
            },
        )
        .await
        .expect("sibling input");
        let sibling_deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < sibling_deadline
            && !webrtc_terminal_contains(&peer.pending_terminal_frames, "echo:wwb-sibling-live")
        {
            let drain = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::drain_subscription("wwb-live", "sub-live"),
                )
                .await
                .expect("sibling drain");
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
                    .all(|event| !webrtc_event_is_terminal_body(event)),
                "content-blind sibling Drain must stay bound: {:?}",
                drain.events
            );
            if let Ok(Ok(bytes)) =
                timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await
            {
                peer.pending_terminal_frames
                    .push_back((String::new(), bytes));
            }
        }
        assert!(
            webrtc_terminal_contains(&peer.pending_terminal_frames, "echo:wwb-sibling-live"),
            "sibling daemon_terminal_frame must continue: {:?}",
            peer.pending_terminal_frames
                .iter()
                .map(|(_, bytes)| String::from_utf8_lossy(bytes).into_owned())
                .collect::<Vec<_>>()
        );
        eprintln!(
            "webrtc write-budget provenance hub_bin={} session_worker={} hub_sha={} locked_core=358ef1a6bf0f792f6da10d60890be39cb16779d0",
            env!("CARGO_BIN_EXE_botster-hub"),
            session_worker_binary_path().display(),
            option_env!("BOTSTER_HUB_GIT_SHA").unwrap_or("worktree")
        );
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, "wwb-stall");
    shutdown_short_lived_session(&endpoint, "wwb-live");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_unnegotiated_adapter_never_receives_or_decodes_daemon_event() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wun");
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("unnegotiated adapter hello");
        spawn_and_bind_webrtc(&mut peer, &key, "wun-a", "sub-a", "sleep 30").await;
        spawn_and_bind_webrtc(&mut peer, &key, "wun-b", "sub-b", "sleep 30").await;
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Attach {
                session_id: "wun-a".to_string(),
                subscription_id: "sub-a".to_string(),
            },
        )
        .await
        .expect("host close of unnegotiated generation");
        let deadline = Instant::now() + Duration::from_secs(6);
        let mut saw_sibling = false;
        while Instant::now() < deadline {
            match timeout(Duration::from_millis(200), peer.next_terminal_frame(&key)).await {
                Ok(Ok(bytes)) => {
                    assert!(!bytes.is_empty());
                    saw_sibling = true;
                }
                Ok(Err(error)) => {
                    panic!(
                        "unnegotiated receive path must fail closed without decoding daemon_event: {error}"
                    );
                }
                Err(_) => {}
            }
            let status = peer
                .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
                .await
                .expect("status stays available without daemon_event");
            assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        }
        assert!(
            saw_sibling || !peer.pending_terminal_frames.is_empty(),
            "unnegotiated sibling terminal frames must still arrive"
        );
        assert!(peer.pending_host_events().is_empty());
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, "wun-a");
    shutdown_short_lived_session(&endpoint, "wun-b");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wnx");
    let death_bootstrap = issue_second_webrtc_bootstrap(&endpoint, &bootstrap);
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        peer.encrypted_hello(&key, &webrtc_close_event_hello())
            .await
            .expect("hello");
        spawn_and_bind_webrtc(&mut peer, &key, "wnx-detach", "sub-detach", "sleep 30").await;
        spawn_and_bind_webrtc(&mut peer, &key, "wnx-exit", "sub-exit", "printf 'done\\n'").await;
        spawn_and_bind_webrtc(&mut peer, &key, "wnx-shutdown", "sub-shutdown", "sleep 30").await;
        let before = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status before WebRTC ShutdownSession");
        let shutdown_generation = occupancy_generation(
            &before
                .status
                .as_ref()
                .expect("status body before WebRTC ShutdownSession")
                .live_attach_occupancy,
            "wnx-shutdown",
            "sub-shutdown",
        )
        .expect("Active WebRTC ShutdownSession must have a Core-issued generation");
        let detach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Detach {
                    session_id: "wnx-detach".to_string(),
                    subscription_id: "sub-detach".to_string(),
                },
            )
            .await
            .expect("detach");
        assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);
        let shutdown = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "wnx-shutdown".to_string(),
                },
            )
            .await
            .expect("shutdown");
        assert_ne!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        let late = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("late Status after WebRTC ShutdownSession");
        assert_eq!(late.kind, botster_hub_client::DaemonResponseKind::Status);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let _ = timeout(Duration::from_millis(100), peer.next_host_event(&key)).await;
        }
        assert!(
            no_terminal_subscription_closed(
                peer.pending_host_events().iter(),
                "wnx-shutdown",
                Some("sub-shutdown"),
                Some(shutdown_generation)
            ),
            "WebRTC ShutdownSession must not emit TerminalSubscriptionClosed for generation {shutdown_generation}: {:?}",
            peer.pending_host_events()
        );
        assert!(
            peer.pending_host_events().iter().all(|event| {
                !matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { .. }
                )
            }),
            "Detach, process exit, and ShutdownSession must not emit TerminalSubscriptionClosed: {:?}",
            peer.pending_host_events()
        );

        let (mut death_peer, death_key) = open_local_webrtc_peer(&endpoint, &death_bootstrap).await;
        death_peer.enable_host_events();
        death_peer
            .encrypted_hello(&death_key, &webrtc_close_event_hello())
            .await
            .expect("death peer hello");
        spawn_and_bind_webrtc(
            &mut death_peer,
            &death_key,
            "wnx-death",
            "sub-death",
            "sleep 30",
        )
        .await;
        death_peer.peer.close().await.expect("close death peer");
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, "wnx-detach");
    shutdown_short_lived_session(&endpoint, "wnx-death");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wrm");
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        peer.encrypted_hello(&key, &webrtc_close_event_hello())
            .await
            .expect("hello");
        spawn_and_bind_webrtc(
            &mut peer,
            &key,
            "wrm-stall",
            "sub-stall",
            "yes remove-session-still-live",
        )
        .await;
        let removed = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::RemoveSession {
                    session_id: "wrm-stall".to_string(),
                },
            )
            .await
            .expect("failed remove");
        assert_eq!(
            removed.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            removed.error.as_ref().map(|error| error.code.as_str()),
            Some("session_not_terminal")
        );
        let closed = wait_for_webrtc_subscription_closed(&mut peer, &key, "wrm-stall", "sub-stall")
            .await
            .expect("failed RemoveSession must not suppress later Core close");
        match closed {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { reason, .. } => {
                assert_eq!(
                    reason,
                    botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER
                );
            }
            other => panic!("unexpected host event: {other:?}"),
        }
        peer.peer.close().await.expect("close offer peer");
    });
    shutdown_short_lived_session(&endpoint, "wrm-stall");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap_a) = start_webrtc_adapter_hub("wsg");
    let bootstrap_b = issue_second_webrtc_bootstrap(&endpoint, &bootstrap_a);
    block_on(async {
        let (mut owner_a, key_a) = open_local_webrtc_peer(&endpoint, &bootstrap_a).await;
        owner_a.enable_host_events();
        owner_a
            .encrypted_hello(&key_a, &webrtc_close_event_hello())
            .await
            .expect("owner A hello");
        spawn_and_bind_webrtc(
            &mut owner_a,
            &key_a,
            "wsg-session",
            "wsg-sub",
            "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        )
        .await;
        let occupancy_after_a =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after A")
                .status
                .expect("status body")
                .lifecycle_counters
                .live_attach_subscriptions;

        let (mut owner_b, key_b) = open_local_webrtc_peer(&endpoint, &bootstrap_b).await;
        owner_b.enable_host_events();
        owner_b
            .encrypted_hello(&key_b, &webrtc_close_event_hello())
            .await
            .expect("owner B hello");
        let attach_b = owner_b
            .encrypted_request(
                &key_b,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "wsg-session".to_string(),
                    subscription_id: "wsg-sub".to_string(),
                },
            )
            .await
            .expect("replacement attach");
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
        let closed =
            wait_for_webrtc_subscription_closed(&mut owner_a, &key_a, "wsg-session", "wsg-sub")
                .await
                .expect("A must observe TerminalSubscriptionClosed for generation N");
        match closed {
            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed { generation, .. } => {
                assert_eq!(generation, 1);
            }
            other => panic!("unexpected host event: {other:?}"),
        }
        let occupancy_after_b =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after B")
                .status
                .expect("status body")
                .lifecycle_counters
                .live_attach_subscriptions;
        assert!(
            occupancy_after_b >= 1,
            "Hub-visible occupancy must keep B live: after_a={occupancy_after_a} after_b={occupancy_after_b}"
        );
        let drain = owner_b
            .encrypted_request(
                &key_b,
                &botster_hub_client::DaemonRequest::drain_subscription("wsg-session", "wsg-sub"),
            )
            .await
            .expect("B drain");
        assert_ne!(
            drain.kind,
            botster_hub_client::DaemonResponseKind::OperatorError,
            "B's scoped Drain must stay owned after A's stale close: {:?}",
            drain.error
        );
        assert!(
            drain
                .events
                .iter()
                .all(|event| !webrtc_event_is_terminal_body(event)),
            "content-blind B Drain must stay bound: {:?}",
            drain.events
        );
        owner_b
            .encrypted_request(
                &key_b,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: "wsg-session".to_string(),
                    data: "after-replace\r".to_string(),
                },
            )
            .await
            .expect("B input");
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline
            && !webrtc_terminal_contains(&owner_b.pending_terminal_frames, "echo:after-replace")
        {
            if let Ok(Ok(bytes)) = timeout(
                Duration::from_millis(200),
                owner_b.next_terminal_frame(&key_b),
            )
            .await
            {
                owner_b
                    .pending_terminal_frames
                    .push_back((String::new(), bytes));
            }
            let drain = owner_b
                .encrypted_request(
                    &key_b,
                    &botster_hub_client::DaemonRequest::drain_subscription(
                        "wsg-session",
                        "wsg-sub",
                    ),
                )
                .await
                .expect("B keep-alive drain");
            assert_ne!(
                drain.kind,
                botster_hub_client::DaemonResponseKind::OperatorError,
                "B Drain must stay owned while waiting for live bytes: {:?}",
                drain.error
            );
        }
        assert!(
            webrtc_terminal_contains(&owner_b.pending_terminal_frames, "echo:after-replace"),
            "generation N+1 must keep terminal frames: {:?}",
            owner_b
                .pending_terminal_frames
                .iter()
                .map(|(_, bytes)| String::from_utf8_lossy(bytes).into_owned())
                .collect::<Vec<_>>()
        );
        owner_a.peer.close().await.expect("close A");
        owner_b.peer.close().await.expect("close B");
    });
    shutdown_short_lived_session(&endpoint, "wsg-session");
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn webrtc_terminal_adapter_close_event_feature_stays_optional_on_protocol_7() {
    let requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    let mut previous = botster_hub_client::DaemonCompatibility::current();
    previous
        .features
        .retain(|feature| feature != botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED);
    previous.conformance_fixture_revision =
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
    botster_hub_client::ensure_compatible(&requirement, &previous)
        .expect("default clients still accept a daemon without terminal_subscription_closed");
    let adapter = botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
    assert!(
        !adapter
            .required_features
            .iter()
            .any(|feature| feature == botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
    );
    assert_eq!(botster_hub_client::PROTOCOL_VERSION, 7);
    assert_eq!(
        botster_hub_client::DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION,
        36
    );
    const _: () = assert!(botster_hub_client::CONFORMANCE_FIXTURE_REVISION >= 45);
}

#[test]
fn one_session_unix_and_webrtc_dual_attach_exposes_hub_occupancy() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("nsd");
    let session_id = "nsd-session";
    let unix_sub = "nsd-unix";
    let webrtc_sub = "nsd-webrtc";
    let (mut unix, mut unix_reader) = unix_adapter_connection(&endpoint);
    let mut unix_envelopes = Vec::new();

    let spawned = request_skipping_envelopes(
        &mut unix,
        &mut unix_reader,
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
        &mut unix_envelopes,
    );
    assert_eq!(
        spawned.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );
    let unix_attach = request_skipping_envelopes(
        &mut unix,
        &mut unix_reader,
        &botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: unix_sub.to_string(),
        },
        &mut unix_envelopes,
    );
    assert_eq!(
        unix_attach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );

    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.encrypted_hello(&key, &webrtc_terminal_adapter_hello())
            .await
            .expect("webrtc hello");
        let webrtc_attach = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: session_id.to_string(),
                    subscription_id: webrtc_sub.to_string(),
                },
            )
            .await
            .expect("webrtc attach");
        assert_eq!(
            webrtc_attach.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        assert!(
            webrtc_attach.events.is_empty(),
            "WebRTC Attach must not return terminal bodies: {:?}",
            webrtc_attach.events
        );

        let occupied = sibling_status(&mut unix, &mut unix_reader, &mut unix_envelopes);
        assert!(
            occupied
                .compatibility
                .features
                .iter()
                .any(|feature| feature == botster_hub_client::FEATURE_ATTACH_OCCUPANCY),
            "sibling Status must advertise attach_occupancy: {:?}",
            occupied.compatibility.features
        );
        assert!(
            occupancy_has_pair(&occupied.live_attach_occupancy, session_id, unix_sub),
            "Unix pair must occupy the union oracle: {:?}",
            occupied.live_attach_occupancy
        );
        assert!(
            occupancy_has_pair(&occupied.live_attach_occupancy, session_id, webrtc_sub),
            "WebRTC pair must occupy the union oracle: {:?}",
            occupied.live_attach_occupancy
        );

        peer.peer.close().await.expect("close webrtc peer");
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut after = sibling_status(&mut unix, &mut unix_reader, &mut unix_envelopes);
    while Instant::now() < deadline {
        if !occupancy_has_pair(&after.live_attach_occupancy, session_id, webrtc_sub)
            && occupancy_has_pair(&after.live_attach_occupancy, session_id, unix_sub)
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        after = sibling_status(&mut unix, &mut unix_reader, &mut unix_envelopes);
    }
    assert!(
        !occupancy_has_pair(&after.live_attach_occupancy, session_id, webrtc_sub),
        "WebRTC pair must leave occupancy after peer loss: {:?}",
        after.live_attach_occupancy
    );
    assert!(
        occupancy_has_pair(&after.live_attach_occupancy, session_id, unix_sub),
        "Unix sibling must stay occupied after WebRTC peer loss: {:?}",
        after.live_attach_occupancy
    );

    let input = request_skipping_envelopes(
        &mut unix,
        &mut unix_reader,
        &botster_hub_client::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "after-webrtc-loss\r".to_string(),
        },
        &mut unix_envelopes,
    );
    assert_ne!(
        input.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "Unix SendInput must stay accepted after WebRTC peer loss: {input:?}"
    );
    let listed = request_skipping_envelopes(
        &mut unix,
        &mut unix_reader,
        &botster_hub_client::DaemonRequest::ListSessions,
        &mut unix_envelopes,
    );
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.session_id == session_id),
        "host session must stay listed after WebRTC peer loss"
    );
    eprintln!(
        "unix+webrtc occupancy provenance hub_bin={} session_worker={}",
        env!("CARGO_BIN_EXE_botster-hub"),
        session_worker_binary_path().display()
    );

    drop(unix);
    drop(unix_reader);
    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn isolated_hub_webrtc_client_receives_unsolicited_package_event() {
    let _guard = daemon_test_guard();
    let (hub, endpoint, bootstrap) = start_webrtc_adapter_hub("wev");
    enable_event_plane_producer_on_hub(&endpoint, "wev");
    block_on(async {
        let (mut peer, key) = open_local_webrtc_peer(&endpoint, &bootstrap).await;
        peer.enable_host_events();
        let ack = peer
            .encrypted_hello(&key, &webrtc_package_event_hello())
            .await
            .expect("package-event hello");
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS)
        );
        let subscribed = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEvents {
                    subscription_id: "sub-webrtc-live".to_string(),
                    owner: "event-plane-producer".to_string(),
                    name: "sample.ready".to_string(),
                    subjects: Vec::new(),
                },
            )
            .await
            .expect("subscribe");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        emit_sample_ready(&endpoint, "webrtc-live");
        let event = timeout(Duration::from_secs(5), peer.next_host_event(&key))
            .await
            .expect("package event arrives without later traffic")
            .expect("host event");
        match event {
            botster_hub_client::DaemonEvent::PackageEvent {
                subscription_id,
                owner,
                name,
                payload,
            } => {
                assert_eq!(subscription_id, "sub-webrtc-live");
                assert_eq!(owner, "event-plane-producer");
                assert_eq!(name, "sample.ready");
                assert_eq!(payload["token"], "webrtc-live");
            }
            other => panic!("expected PackageEvent, got {other:?}"),
        }
        let status = peer
            .encrypted_request(&key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status after event");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let entities = peer
            .encrypted_request(
                &key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "entity-under-events".to_string(),
                },
            )
            .await
            .expect("entity subscribe after event");
        assert_eq!(
            entities.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        peer.peer.close().await.expect("close peer");
    });
    hub.shutdown().expect("shutdown isolated hub");
}
