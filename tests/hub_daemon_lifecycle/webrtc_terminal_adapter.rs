fn webrtc_terminal_adapter_hello() -> botster_hub_client::DaemonHello {
    botster_hub_client::DaemonHello {
        protocol: botster_hub_client::PROTOCOL.to_string(),
        compatibility: botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
        terminal_compatibility: None,
    }
}

fn start_webrtc_adapter_hub(name: &str) -> (
    botster_hub_test_support::IsolatedHub,
    botster_hub_client::DaemonEndpoint,
    botster_hub_client::DaemonLocalWebrtcBootstrap,
) {
    let hub = start_isolated_live_output_hub(name);
    let package_dir = unique_test_dir(&format!("{name}-web"));
    write_botster_web_package(&package_dir);
    enable_supervised_package(hub.data_dir(), &package_dir);
    let endpoint = hub.endpoint().clone();
    let web_listener_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                web_listener_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(start.kind, botster_hub_client::DaemonResponseKind::Packages);
    let bootstrap = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: format!("http://127.0.0.1:{web_listener_port}"),
        },
    )
    .expect("issue local WebRTC bootstrap")
    .local_webrtc_bootstrap
    .expect("bootstrap response includes local WebRTC bootstrap");
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
            .expect("attach");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        let terminal: Vec<_> = attach
            .events
            .iter()
            .filter(|event| webrtc_event_is_terminal_body(event))
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
            if let Ok(bytes) = timeout(Duration::from_millis(200), peer.next_terminal_frame(&key))
                .await
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
        peer.encrypted_request(
            &key,
            &botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "printf 'webrtc-two-channel-ready\\n'; sleep 30".to_string(),
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
        let mut extra = peer
            .create_extra_data_channel()
            .await
            .expect("create extra DataChannel");
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
            if let Ok(bytes) = timeout(Duration::from_millis(200), peer.next_terminal_frame(&key))
                .await
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
            .expect("unbound attach");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut saw_snapshot = attach.events.iter().any(|event| {
            matches!(event, botster_hub_client::DaemonEvent::Snapshot { .. })
        });
        while Instant::now() < deadline && !saw_snapshot {
            let drain = peer
                .encrypted_request(
                    &key,
                    &botster_hub_client::DaemonRequest::drain_subscription(
                        session_id,
                        subscription_id,
                    ),
                )
                .await
                .expect("unbound drain");
            saw_snapshot = drain.events.iter().any(|event| {
                matches!(event, botster_hub_client::DaemonEvent::Snapshot { .. })
            });
        }
        assert!(saw_snapshot, "unbound WebRTC attach must still Drain Snapshot");
        assert!(
            peer.pending_terminal_frames.is_empty(),
            "unbound attach must not receive daemon_terminal_frame"
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
        let attached = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Status,
        )
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
    let listed = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
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
