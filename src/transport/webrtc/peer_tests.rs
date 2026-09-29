use super::*;
use crate::admission::budgets::ENTITY_SUBSCRIPTION_QUEUE_CAPACITY;
use crate::admission::unix_hello::WebrtcTerminalAdmission;
use crate::daemon::control::handle_control_message;
use crate::daemon::control::message::{ControlMessage, ControlSender, ReservationInspectReply};
use crate::daemon::owner_loop::DaemonControlState;
use crate::subscription::attach_routes::negotiated_unix_capability_set;
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::adapter::WebRtcConnectionMux;
use crate::transport::webrtc::control_channel::*;
use crate::transport::webrtc::delivery::*;
use crate::transport::webrtc::peer::*;
use crate::transport::webrtc::subscription_channel::*;
use crate::transport::webrtc::test_support::*;
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
use crate::{
    DataDirectoryOption, HostIdentityOptions, HubDaemon, HubStartupOptions,
    PackageEventPlaneOptions, RuntimeEnvironment, SessionDefaults,
};
use async_trait::async_trait;
use botster_core::contract::terminal_adapter::{
    TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
};
use botster_core::{AesGcmKey, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonRequest, DaemonResponse,
    LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
};
use botster_hub_client::{DaemonEvent, PROTOCOL};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::oneshot;
use webrtc::data_channel::RTCDataChannelInit;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelMessage};
use webrtc::peer_connection::PeerConnectionBuilder;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionEventHandler, RTCIceGatheringState, RTCPeerConnectionState,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, channel as webrtc_channel, default_runtime,
    timeout,
};

fn retained_terminal_record(grant_id: impl Into<String>) -> LocalWebrtcSenderTerminalRecord {
    LocalWebrtcSenderTerminalRecord {
        schema_version: 1,
        grant_id: grant_id.into(),
        request_operation: "status".to_string(),
        message_id: Some("response-terminal-record".to_string()),
        next_chunk_index: 1,
        last_sent_chunk_index: Some(0),
        total_chunks: 2,
        pressured: true,
        peer_connection_state: "failed".to_string(),
        channel_terminal_signal: LocalWebrtcChannelTerminalSignal::OnClose,
        cause: LocalWebrtcTerminalCause::PeerFailed,
        cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
    }
}

#[test]
fn status_terminal_records_preflight_rechecks_growth_before_copy() {
    let mut transport = LocalWebrtcTransport::default();
    let empty = std::mem::size_of::<Vec<DaemonLocalWebrtcTerminalRecord>>();
    assert!(transport.bounded_terminal_records(empty - 1).is_none());
    assert_eq!(transport.terminal_records_bytes(empty), Some(empty));
    transport
        .retain_terminal_record(retained_terminal_record("first"))
        .unwrap();
    let bytes = transport.terminal_records_bytes(usize::MAX).unwrap();
    assert!(transport.bounded_terminal_records(bytes - 1).is_none());
    let (records, charged) = transport.bounded_terminal_records(bytes).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(charged, bytes);
    transport
        .retain_terminal_record(retained_terminal_record("second"))
        .unwrap();
    assert!(transport.bounded_terminal_records(bytes).is_none());
    assert_eq!(transport.terminal_records.len(), 2);
}

#[test]
fn terminal_record_retention_is_bounded_correlated_and_oldest_evicted() {
    assert!(
        LOCAL_WEBRTC_TERMINAL_RECORD_MAX_TOTAL_BYTES
            <= botster_hub_client::MAX_CONTROL_RESPONSE_BYTES / 8
    );
    let mut transport = LocalWebrtcTransport::default();
    for serial in 0..=LOCAL_WEBRTC_TERMINAL_RECORD_MAX_ENTRIES {
        transport
            .retain_terminal_record(retained_terminal_record(format!("grant-{serial}")))
            .expect("bounded terminal record");
    }
    assert_eq!(
        transport.terminal_records.len(),
        LOCAL_WEBRTC_TERMINAL_RECORD_MAX_ENTRIES
    );
    assert!(transport.terminal_record_bytes <= LOCAL_WEBRTC_TERMINAL_RECORD_MAX_TOTAL_BYTES);
    assert_eq!(transport.terminal_record_evictions(), 1);
    assert!(transport.terminal_record("grant-0").is_none());
    assert!(transport.terminal_record("grant-other").is_none());
    assert!(
        transport
            .terminal_record(&format!(
                "grant-{}",
                LOCAL_WEBRTC_TERMINAL_RECORD_MAX_ENTRIES
            ))
            .is_some()
    );
}

#[test]
fn terminal_record_retention_rejects_oversized_variable_fields() {
    let mut transport = LocalWebrtcTransport::default();

    let record = retained_terminal_record("x".repeat(LOCAL_WEBRTC_TERMINAL_GRANT_ID_MAX_BYTES + 1));
    assert_eq!(
        transport.retain_terminal_record(record),
        Err("local WebRTC terminal grant id exceeded its byte bound")
    );

    let mut record = retained_terminal_record("grant-current");
    record.request_operation = "x".repeat(LOCAL_WEBRTC_TERMINAL_OPERATION_MAX_BYTES + 1);
    assert_eq!(
        transport.retain_terminal_record(record),
        Err("local WebRTC terminal operation exceeded its byte bound")
    );

    let mut record = retained_terminal_record("grant-current");
    record.message_id = Some("x".repeat(LOCAL_WEBRTC_TERMINAL_MESSAGE_ID_MAX_BYTES + 1));
    assert_eq!(
        transport.retain_terminal_record(record),
        Err("local WebRTC terminal message id exceeded its byte bound")
    );

    let mut record = retained_terminal_record("grant-current");
    record.peer_connection_state = "x".repeat(LOCAL_WEBRTC_TERMINAL_PEER_STATE_MAX_BYTES + 1);
    assert_eq!(
        transport.retain_terminal_record(record),
        Err("local WebRTC terminal peer state exceeded its byte bound")
    );

    assert!(transport.terminal_records().is_empty());
    assert_eq!(transport.terminal_record_evictions(), 0);
}

#[test]
fn peer_admits_only_the_first_data_channel() {
    let peer_state = test_peer_state("grant-one-channel");
    assert!(peer_state.claim_data_channel());
    assert!(!peer_state.claim_data_channel());
}
#[test]
fn hanging_data_channel_local_close_still_runs_cleanup_once_within_bound() {
    let data_channel = FakeDataChannel::default();
    let key = AesGcmKey::from_slice(&[22; 32]).unwrap();
    let mut pending = VecDeque::new();
    pending.push_back(PendingLocalWebrtcRequest::Request {
        request_id: "1".to_string(),
        request: Box::new(DaemonRequest::Status),
    });
    let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
    let peer_state = Arc::new(LocalWebrtcPeerState::new(
        "grant-local-close-hang".to_string(),
        runtime_tx,
    ));
    peer_state
        .force_local_close_hang
        .store(true, Ordering::SeqCst);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    runtime.block_on(close_data_channel(
        &data_channel,
        &key,
        &mut pending,
        peer_state.as_ref(),
        LocalWebrtcTerminalCause::ChannelClosed,
    ));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
        "hang inject must wait for the production bound: {elapsed:?}"
    );
    assert!(
        elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
        "cleanup_once must still run after a hung local_close: {elapsed:?}"
    );
    assert!(pending.is_empty());
    let ControlMessage::LocalWebrtcPeerClosed {
        grant_id,
        terminal_record,
        ..
    } = receive_test_runtime_message(&mut runtime_rx)
    else {
        panic!("hung local_close must still emit LocalWebrtcPeerClosed");
    };
    assert_eq!(grant_id, "grant-local-close-hang");
    assert_eq!(
        terminal_record.cause,
        LocalWebrtcTerminalCause::ChannelClosed
    );
}

#[test]
fn production_on_close_hangs_local_close_and_still_cleans_up() {
    let data_channel = FakeDataChannel::default();
    data_channel
        .events
        .lock()
        .unwrap()
        .push_back(DataChannelEvent::OnClose);
    let key = AesGcmKey::from_slice(&[22; 32]).unwrap();
    let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
    let peer_state = Arc::new(LocalWebrtcPeerState::new(
        "grant-on-close-hang".to_string(),
        runtime_tx.clone(),
    ));
    peer_state
        .force_local_close_hang
        .store(true, Ordering::SeqCst);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    let failure = runtime.block_on(run_data_channel(
        &data_channel,
        &key,
        peer_state.as_ref(),
        &runtime_tx,
    ));
    let elapsed = started.elapsed();
    assert!(failure.is_none());
    assert!(
        elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
        "OnClose must wait for the local_close bound: {elapsed:?}"
    );
    assert!(
        elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
        "OnClose hang must still reach cleanup_once: {elapsed:?}"
    );
    let ControlMessage::LocalWebrtcPeerClosed { grant_id, .. } =
        receive_test_runtime_message(&mut runtime_rx)
    else {
        panic!("OnClose hang must still emit LocalWebrtcPeerClosed");
    };
    assert_eq!(grant_id, "grant-on-close-hang");
}
#[test]
fn hung_send_text_times_out_within_close_bound() {
    let data_channel = FakeDataChannel::default();
    data_channel.send_hangs.store(true, Ordering::Release);
    let key = AesGcmKey::from_slice(&[17; 32]).unwrap();
    let mut pending = VecDeque::new();
    let mut flow_control = LocalWebrtcFlowControl::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let peer_state = test_peer_state("grant-hung-send-timeout");
    let started = Instant::now();
    let failure = runtime
        .block_on(send_response_frames(
            &data_channel,
            &key,
            &["response".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ))
        .expect_err("hung send_text must fail within the close bound");
    let elapsed = started.elapsed();
    assert_eq!(failure.cause, LocalWebrtcTerminalCause::SendText);
    assert!(
        elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
        "hung send must wait the close bound: {elapsed:?}"
    );
    assert!(
        elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
        "hung send must not block cleanup: {elapsed:?}"
    );
}
#[test]
fn local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("h1");
    let origin = "http://127.0.0.1:41791";
    let mut peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let subscription_id = "entity-delivery-h1".to_string();

    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
    assert!(harness.daemon.local_webrtc().has_dedicated_runtime());
    assert!(
        harness
            .daemon
            .local_webrtc()
            .dedicated_runtime_worker_threads()
            >= 1
    );
    assert_eq!(
        harness
            .daemon
            .local_webrtc()
            .close_completion_count_for(&grant_id),
        0
    );

    let subscribe = harness.subscribe_entities(&mut peer, &subscription_id);
    assert_eq!(
        subscribe.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "entity subscription must be registered before peer_failed"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        1
    );

    // Production path: handler on_connection_state_change(Failed) → cleanup_once(PeerFailed)
    // → LocalWebrtcPeerClosed → handle_control_message → remove_peer close+map+runtime drop.
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));

    let terminal = harness
        .daemon
        .local_webrtc()
        .terminal_record(&grant_id)
        .expect("peer close retains grant-correlated terminal evidence");
    assert_eq!(terminal.grant_id, grant_id);
    assert_eq!(terminal.cause, "peer_failed");
    assert_eq!(terminal.peer_connection_state, "failed");

    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
    assert_eq!(
        harness
            .daemon
            .local_webrtc()
            .close_completion_count_for(&grant_id),
        1,
        "production forget must invoke and complete PeerConnection::close"
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "entity subscription must be removed on LocalWebrtcPeerClosed"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        0
    );

    wait_until(
        Instant::now() + Duration::from_secs(2),
        || {
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                == 0
        },
        "dedicated botster-local-webrtc worker threads to join",
    );

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn webrtc_subscribe_events_requires_host_negotiation() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("evt");
    let mut peer = harness.signal_peer("http://127.0.0.1:41901");
    harness.hello_on_peer(
        &mut peer,
        DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            terminal_compatibility: Some(
                botster_terminal_protocol::TerminalCompatibilityRequirement {
                    protocol: "botster-terminal-v1".to_string(),
                    protocol_version: 99,
                    required_features: vec!["missing_terminal_feature".to_string()],
                    minimum_conformance_fixture_revision: 1,
                    client_name: "webrtc-event-reject-terminal".to_string(),
                },
            ),
        },
    );
    let unnegotiated = harness.request_on_peer(
        &mut peer,
        DaemonRequest::SubscribeEvents {
            subscription_id: "sub".to_string(),
            owner: "event-plane-producer".to_string(),
            name: "sample.ready".to_string(),
            subjects: Vec::new(),
        },
        "SubscribeEvents",
    );
    assert_eq!(
        unnegotiated.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        unnegotiated.error.as_ref().map(|error| error.code.as_str()),
        Some("package_event_subscriptions_not_negotiated")
    );
    let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    let mut negotiated = harness.signal_peer("http://127.0.0.1:41902");
    harness.hello_on_peer(
        &mut negotiated,
        DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility:
                botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(
                ),
            terminal_compatibility: Some(
                botster_terminal_protocol::TerminalCompatibilityRequirement {
                    protocol: "botster-terminal-v1".to_string(),
                    protocol_version: 99,
                    required_features: vec!["missing_terminal_feature".to_string()],
                    minimum_conformance_fixture_revision: 1,
                    client_name: "webrtc-event-reject-terminal".to_string(),
                },
            ),
        },
    );
    let subscribed = harness.request_on_peer(
        &mut negotiated,
        DaemonRequest::SubscribeEvents {
            subscription_id: "sub-neg".to_string(),
            owner: "event-plane-producer".to_string(),
            name: "sample.ready".to_string(),
            subjects: Vec::new(),
        },
        "SubscribeEvents",
    );
    assert_eq!(
        subscribed.error.as_ref().map(|error| error.code.as_str()),
        Some("rejected_undeclared")
    );
    let status = harness.request_on_peer(&mut negotiated, DaemonRequest::Status, "Status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    peer.close_offer();
    negotiated.close_offer();
    harness.cleanup();
}

#[test]
fn webrtc_entity_subscription_returns_and_binds_a_dedicated_channel() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("entity-dedicated");
    let mut peer = harness.signal_peer("http://127.0.0.1:41903");
    harness.ensure_webrtc_adapter_hello(&mut peer);

    let subscribed = harness.subscribe_entities(&mut peer, "entity-dedicated-sub");
    assert_eq!(
        subscribed.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    let reservation = subscribed
        .subscription_reservation
        .expect("entity subscribe returns a reserved channel");
    assert_eq!(
        reservation.kind,
        botster_hub_client::DaemonSubscriptionReservationKind::Entity
    );
    assert!(!reservation.label.is_empty());
    assert!(reservation.generation > 0);
    assert_eq!(
        harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&reservation.peer_generation)
            .expect("peer budget")
            .channel_count(),
        2
    );

    harness.bind_reserved_on_peer(&mut peer, &reservation.label);
    harness.wait_until_reservation_bound(&peer.grant_id, &reservation.label);
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key("entity-dedicated-sub")
    );

    let unsubscribed = harness.request_on_peer(
        &mut peer,
        DaemonRequest::UnsubscribeEntities {
            subscription_id: "entity-dedicated-sub".to_string(),
        },
        "UnsubscribeEntities",
    );
    assert_eq!(
        unsubscribed.kind,
        botster_hub_client::DaemonResponseKind::EntityUnsubscribed
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("entity-dedicated-sub")
    );
    assert!(
        harness
            .state
            .pending_runtime
            .admission
            .reservations
            .reservation_for_label(&reservation.label, reservation.peer_generation)
            .is_none()
    );
    assert_eq!(
        harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&reservation.peer_generation)
            .expect("peer budget remains for control")
            .channel_count(),
        1
    );

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn reservation_rejection_states_and_timeout_release_are_distinct() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("reservation-matrix");
    let mut peer_a = harness.signal_peer("http://127.0.0.1:41904");
    let mut peer_b = harness.signal_peer("http://127.0.0.1:41905");
    harness.ensure_webrtc_adapter_hello(&mut peer_a);
    harness.ensure_webrtc_adapter_hello(&mut peer_b);

    let inspect = |harness: &mut PeerHarness, grant_id: &str, label: &str| {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::InspectReservation {
                grant_id: grant_id.to_string(),
                label: label.to_string(),
                reply_tx,
            },
        );
        reply_rx.blocking_recv().expect("reservation inspection")
    };

    let live = harness.subscribe_entities(&mut peer_a, "matrix-live");
    let live_reservation = live.subscription_reservation.expect("live reservation");
    assert!(matches!(
        inspect(&mut harness, &peer_a.grant_id, &live_reservation.label),
        ReservationInspectReply::Live { .. }
    ));
    assert_eq!(
        inspect(&mut harness, &peer_b.grant_id, &live_reservation.label),
        ReservationInspectReply::Stale
    );
    assert_eq!(
        inspect(&mut harness, &peer_a.grant_id, "never-reserved"),
        ReservationInspectReply::Unknown
    );

    harness.bind_reserved_on_peer(&mut peer_a, &live_reservation.label);
    harness.wait_until_reservation_bound(&peer_a.grant_id, &live_reservation.label);
    assert_eq!(
        inspect(&mut harness, &peer_a.grant_id, &live_reservation.label),
        ReservationInspectReply::Bound
    );

    let over_limit = harness.subscribe_entities(&mut peer_a, "matrix-over-limit");
    let over_limit_reservation = over_limit
        .subscription_reservation
        .expect("over-limit backstop reservation");
    harness
        .state
        .pending_runtime
        .admission
        .connection_budgets
        .get_mut(&over_limit_reservation.peer_generation)
        .expect("peer budget")
        .release(&over_limit_reservation.label);
    assert_eq!(
        inspect(
            &mut harness,
            &peer_a.grant_id,
            &over_limit_reservation.label,
        ),
        ReservationInspectReply::OverLimit
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("matrix-over-limit")
    );

    let late = crate::admission::reservations::with_reservation_expiry_for_test(1, || {
        harness.subscribe_entities(&mut peer_a, "matrix-late")
    });
    let late_reservation = late.subscription_reservation.expect("late reservation");
    std::thread::sleep(Duration::from_millis(1_100));
    assert!(matches!(
        inspect(&mut harness, &peer_a.grant_id, &late_reservation.label),
        ReservationInspectReply::Expired { .. }
    ));
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("matrix-late")
    );
    assert_eq!(
        harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&late_reservation.peer_generation)
            .expect("peer budget")
            .channel_count(),
        2,
        "control and the bound live route remain after timeout cleanup"
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

#[test]
fn webrtc_negotiated_peer_receives_package_event_and_gap_without_later_traffic() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new_with_event_queue("evt-live", Some(1));
    harness.enable_event_plane_producer();
    let mut peer = harness.signal_peer("http://127.0.0.1:41911");
    harness.hello_on_peer(
        &mut peer,
        DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility:
                botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(
                ),
            terminal_compatibility: Some(
                botster_terminal_protocol::TerminalCompatibilityRequirement {
                    protocol: "botster-terminal-v1".to_string(),
                    protocol_version: 99,
                    required_features: vec!["missing_terminal_feature".to_string()],
                    minimum_conformance_fixture_revision: 1,
                    client_name: "webrtc-event-live".to_string(),
                },
            ),
        },
    );
    peer.enable_host_events();
    let subscribed = harness.request_on_peer(
        &mut peer,
        DaemonRequest::SubscribeEvents {
            subscription_id: "sub-live".to_string(),
            owner: "event-plane-producer".to_string(),
            name: "sample.ready".to_string(),
            subjects: Vec::new(),
        },
        "SubscribeEvents",
    );
    assert_eq!(
        subscribed.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    let reservation = subscribed
        .subscription_reservation
        .as_ref()
        .expect("event subscribe returns a reserved channel");
    assert_eq!(
        reservation.kind,
        botster_hub_client::DaemonSubscriptionReservationKind::PackageEvent
    );
    assert!(!reservation.label.is_empty());
    assert!(
        !harness
            .state
            .pending_runtime
            .webrtc_is_admitted(&peer.grant_id),
        "package-event Hello must not admit a terminal adapter"
    );
    let mailbox = harness
        .daemon
        .local_webrtc()
        .event_plane()
        .mailbox(&peer.grant_id)
        .expect("subscribed connection has a mailbox");
    let mut saw_full = false;
    for index in 0..8 {
        match mailbox.try_push(
            "sub-live",
            "event-plane-producer",
            "sample.ready",
            serde_json::json!({ "ok": true, "token": format!("fill-{index}") }),
            8,
        ) {
            Ok(()) => {}
            Err(crate::package_event_router::EventPlaneStatus::ShedFull) => {
                saw_full = true;
                break;
            }
            other => panic!("unexpected mailbox fill result: {other:?}"),
        }
    }
    assert!(saw_full, "one-event mailbox must shed and set a gap bit");
    let reservation_label = reservation.label.clone();
    harness.bind_reserved_on_peer(&mut peer, &reservation_label);
    harness.wait_until_reservation_bound(&peer.grant_id, &reservation_label);
    let first = harness.wait_for_reserved_event(&mut peer, &reservation_label);
    match first {
        DaemonEvent::EventGap {
            subscription_id,
            owner,
            name,
        } => {
            assert_eq!(subscription_id, "sub-live");
            assert_eq!(owner, "event-plane-producer");
            assert_eq!(name, "sample.ready");
        }
        other => panic!("full mailbox must emit EventGap first: {other:?}"),
    }
    let queued = harness.wait_for_reserved_event(&mut peer, &reservation_label);
    match queued {
        DaemonEvent::PackageEvent {
            subscription_id, ..
        } => {
            assert_eq!(subscription_id, "sub-live");
        }
        other => panic!("queued event remains after gap: {other:?}"),
    }
    harness.emit_sample_ready("after-drain");
    let live = harness.wait_for_reserved_event(&mut peer, &reservation_label);
    match live {
        DaemonEvent::PackageEvent {
            subscription_id,
            payload,
            ..
        } => {
            assert_eq!(subscription_id, "sub-live");
            assert_eq!(payload["token"], "after-drain");
        }
        other => panic!("live emit after drain must be PackageEvent: {other:?}"),
    }
    let unsubscribed = harness.request_on_peer(
        &mut peer,
        DaemonRequest::UnsubscribeEvents {
            subscription_id: "sub-live".to_string(),
        },
        "UnsubscribeEvents",
    );
    assert_eq!(
        unsubscribed.kind,
        botster_hub_client::DaemonResponseKind::EventUnsubscribed
    );
    harness.wait_for_reserved_close(&mut peer, &reservation_label);
    harness.wait_until_reservation_lookup(
        &reservation_label,
        reservation.peer_generation,
        || u64::MAX,
        crate::admission::reservations::ReservationLookup::Unknown,
    );
    assert!(
        harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&reservation.peer_generation)
            .and_then(|budget| budget.usage(&reservation_label))
            .is_none(),
        "event budget must release after the bound host closes"
    );
    let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let entities = harness.subscribe_entities(&mut peer, "entity-under-event-pressure");
    assert_eq!(
        entities.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .webrtc_is_admitted(&peer.grant_id),
        "event delivery must not create a terminal adapter"
    );
    peer.close_offer();
    harness.cleanup();
}

#[test]
fn webrtc_status_and_entity_progress_under_event_flood() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new_with_event_queue("evt-flood", Some(8));
    harness.enable_event_plane_producer();
    let mut peer = harness.signal_peer("http://127.0.0.1:41912");
    harness.hello_on_peer(
        &mut peer,
        DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility:
                botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(
                ),
            terminal_compatibility: None,
        },
    );
    peer.enable_host_events();
    let subscribed = harness.request_on_peer(
        &mut peer,
        DaemonRequest::SubscribeEvents {
            subscription_id: "sub-flood".to_string(),
            owner: "event-plane-producer".to_string(),
            name: "sample.ready".to_string(),
            subjects: Vec::new(),
        },
        "SubscribeEvents",
    );
    assert_eq!(
        subscribed.kind,
        botster_hub_client::DaemonResponseKind::EventSubscribed
    );
    let reservation_label = subscribed
        .subscription_reservation
        .as_ref()
        .expect("event subscribe returns a reserved channel")
        .label
        .clone();
    harness.bind_reserved_on_peer(&mut peer, &reservation_label);
    harness.wait_until_reservation_bound(&peer.grant_id, &reservation_label);
    let mailbox = harness
        .daemon
        .local_webrtc()
        .event_plane()
        .mailbox(&peer.grant_id)
        .expect("subscribed connection has a mailbox");
    for index in 0..8 {
        'admit: for attempt in 0..1_000 {
            match mailbox.try_push(
                "sub-flood",
                "event-plane-producer",
                "sample.ready",
                serde_json::json!({ "ok": true, "token": format!("flood-{index}") }),
                8,
            ) {
                Ok(()) => break 'admit,
                Err(crate::package_event_router::EventPlaneStatus::ShedBusy) => {
                    assert!(
                        attempt < 999,
                        "mailbox stayed busy while admitting flood event"
                    );
                    std::thread::yield_now();
                }
                Err(status) => panic!("admit flood event: {status:?}"),
            }
        }
    }
    let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let entities = harness.subscribe_entities(&mut peer, "entity-under-event-flood");
    assert_eq!(
        entities.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("h2");
    let origin = "http://127.0.0.1:41792";
    let mut peer_a = harness.signal_peer(origin);
    let mut peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();

    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 2);
    assert!(harness.daemon.local_webrtc().has_dedicated_runtime());

    let subscribe_a = harness.subscribe_entities(&mut peer_a, "entity-a");
    assert_eq!(
        subscribe_a.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    let subscribe_b = harness.subscribe_entities(&mut peer_b, "entity-b");
    assert_eq!(
        subscribe_b.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        2
    );

    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));

    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
    assert!(
        harness.daemon.local_webrtc().has_dedicated_runtime(),
        "sibling peer must keep the dedicated runtime alive"
    );
    assert_eq!(
        harness
            .daemon
            .local_webrtc()
            .close_completion_count_for(&grant_a),
        1
    );
    assert_eq!(
        harness
            .daemon
            .local_webrtc()
            .close_completion_count_for(&grant_b),
        0
    );
    assert!(!harness.state.entity_subscriptions.contains_key("entity-a"));
    assert!(harness.state.entity_subscriptions.contains_key("entity-b"));
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        1
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("h3");
    let origin = "http://127.0.0.1:41793";
    let mut first = harness.signal_peer(origin);
    let first_grant = first.grant_id.clone();
    let subscribe = harness.subscribe_entities(&mut first, "entity-h3");
    assert_eq!(
        subscribe.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );

    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&first_grant, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&first_grant, Instant::now() + Duration::from_secs(10));
    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
    wait_until(
        Instant::now() + Duration::from_secs(2),
        || {
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                == 0
        },
        "first dedicated runtime workers to join",
    );

    let second = harness.signal_peer(origin);
    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
    assert!(
        harness.daemon.local_webrtc().has_dedicated_runtime(),
        "new signal after last-peer park must recreate the dedicated runtime"
    );
    assert!(
        harness
            .daemon
            .local_webrtc()
            .dedicated_runtime_worker_threads()
            >= 1
    );

    first.close_offer();
    second.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state() {
    let mut harness = PeerHarness::new("late-subscribe");
    let origin = "http://127.0.0.1:41794";
    let peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let subscription_id = "late-entity".to_string();

    // Terminal cleanup wins first (adverse queue order vs a still-queued SubscribeEntities).
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: subscription_id.clone(),
            transport_request_id: None,
            client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
            frame_tx: EntityFrameSender::Async(frame_tx),
            frame_rx: None,
            reply_tx,
            grant_id: Some(grant_id.clone()),
        },
    );

    let response = reply_rx
        .blocking_recv()
        .expect("reply channel open")
        .expect("daemon returns operator response");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_peer_gone")
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "late SubscribeEntities must not recreate daemon entity ownership after PeerClosed"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        0
    );
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn webrtc_hello_bind_echoes_capability_set_and_closes_adapter_on_peer_loss() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-hello-bind");
    let origin = "http://127.0.0.1:41821";
    let mut peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let session_id = "webrtc-hello-bind-session";
    let subscription_id = "webrtc-hello-bind-sub";
    let hello = DaemonHello {
        protocol: PROTOCOL.to_string(),
        compatibility:
            botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
        terminal_compatibility: None,
    };
    let ack = harness.hello_on_peer(&mut peer, hello);
    assert_eq!(ack.protocol, PROTOCOL);
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER)
    );
    let before = harness.state.lifecycle_counters.clone();
    harness.spawn_and_attach_on_peer(&mut peer, session_id, subscription_id);
    let inventory = harness
        .daemon
        .runtime()
        .expect("runtime")
        .list_terminal_subscriptions_for_test();
    let bound = inventory
        .iter()
        .find(|row| row.session_id.0 == session_id && row.subscription_id.0 == subscription_id);
    let bound = bound.expect("bound inventory row");
    assert!(bound.adapter_bound);
    let expected = negotiated_unix_capability_set(
        &[botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER.to_string()],
        None,
    )
    .expect("capability set");
    assert_eq!(bound.capabilities.as_ref(), Some(&expected));
    assert!(
        harness
            .state
            .pending_runtime
            .is_adapter_bound(session_id, subscription_id)
    );

    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));
    let after = harness.state.lifecycle_counters.clone();
    let bound_closes = after
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
    let hub_detaches = after
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
    assert!(
        bound_closes >= 1,
        "bound peer loss must close the adapter: before={before:?} after={after:?}"
    );
    assert_eq!(
        hub_detaches, 0,
        "bound peer loss must not Hub-Detach: before={before:?} after={after:?}"
    );
    let inventory = harness
        .daemon
        .runtime_mut()
        .expect("runtime")
        .list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
        .wait(Duration::from_secs(5))
        .expect("inventory")
        .expect("inventory fits the test allowance")
        .records;
    assert!(
        inventory.iter().all(|row| {
            row.session_id.0 != session_id || row.subscription_id.0 != subscription_id
        }),
        "adapter Closed is the one Core detach: {inventory:?}"
    );
    assert!(harness.list_session_lifecycle(session_id).is_some());
    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("close-fail-sibling");
    let origin = "http://127.0.0.1:41795";
    let mut peer_a = harness.signal_peer(origin);
    let mut peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();
    let session_b = "fail-closed-sibling-session";
    let attach_b = "fail-closed-sibling-attach";

    harness.ensure_webrtc_adapter_hello(&mut peer_a);
    harness.ensure_webrtc_adapter_hello(&mut peer_b);
    let _ = harness.subscribe_entities(&mut peer_a, "entity-fail-a");
    let _ = harness.subscribe_entities(&mut peer_b, "entity-fail-b");
    harness.spawn_and_attach_on_peer(&mut peer_b, session_b, attach_b);
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key("entity-fail-b")
    );
    let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;
    assert!(live_attach_before >= 1);
    assert!(
        harness
            .daemon
            .local_webrtc()
            .dedicated_runtime_worker_threads()
            >= 1
    );
    let owned_workers = harness.owned_workers.clone();
    assert!(
        !owned_workers.is_empty(),
        "spawn must capture exact worker pid/pgid/socket identity"
    );
    assert!(
        harness.list_session_lifecycle(session_b).is_some(),
        "spawned session must remain listed after attach readiness"
    );
    assert!(
        owned_workers
            .iter()
            .all(|worker| process_is_alive(worker.pid)),
        "captured session-worker PIDs must still be live after attach readiness"
    );

    harness
        .daemon
        .local_webrtc()
        .force_next_close_error_for_test(&grant_a);
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));

    // Fail-closed: ultimate close failure tears down the dedicated runtime so residual
    // PeerConnectionDriver work cannot continue while a sibling would otherwise keep it alive.
    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_b));
    assert_eq!(harness.daemon.local_webrtc().stale_close_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
    assert_eq!(
        harness.daemon.local_webrtc().peer_state_count(),
        0,
        "fail-closed must remove primary + sibling peer_states, not only the live peer map"
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("entity-fail-a"),
        "primary grant entity ownership must be cleared"
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("entity-fail-b"),
        "fail-closed sibling grant entity ownership must be cleared synchronously"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        0
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .active_subscriptions
            .get(session_b)
            .is_some_and(|subs| subs.contains(attach_b)),
        "fail-closed sibling attach must be detached from runtime active subscriptions"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_attach_subscriptions, 0,
        "live attach counter must reach zero after fail-closed sibling detach"
    );
    assert!(
        harness.state.attach_close.released_attach_generations >= 1,
        "released attach generations must account for sibling detach"
    );
    wait_until(
        Instant::now() + Duration::from_secs(2),
        || {
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                == 0
        },
        "dedicated runtime workers must join after fail-closed teardown",
    );

    let inventory = harness
        .daemon
        .runtime_mut()
        .expect("runtime")
        .list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
        .wait(Duration::from_secs(5))
        .expect("inventory")
        .expect("inventory fits the test allowance")
        .records;
    assert!(
        inventory.is_empty(),
        "fail-closed must leave zero Core inventory rows before session shutdown: {inventory:?}"
    );
    assert!(
        harness.state.pending_runtime.live_attach_routes.is_empty(),
        "fail-closed must leave zero Hub attach routes before session shutdown: {:?}",
        harness.state.pending_runtime.live_attach_routes
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness
        .shutdown_owned_sessions()
        .expect("fail-closed attach session must shut down and remove cleanly");
    assert!(
        harness.list_session_lifecycle(session_b).is_none(),
        "logical session must be absent after validated RemoveSession"
    );
    wait_for_owned_workers_gone(&owned_workers, Instant::now() + Duration::from_secs(5));
    for worker in &owned_workers {
        assert!(
            worker.is_fully_gone(),
            "owned worker must be fully gone after cleanup: {worker:?}"
        );
        assert!(
            !live_pids_in_process_group(worker.pgid).contains(&worker.pid),
            "worker pid must no longer appear in its process group after cleanup: {worker:?}"
        );
    }
    harness.cleanup();
}

#[test]
fn ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners() {
    run_close_hang_fail_closed_body();
    local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads();
    let inventory_source = include_str!("peer_tests.rs");
    assert!(
        inventory_source.contains("timeout fail-closed must sacrifice sibling peers"),
        "ultimate close failure must keep the bound-exceeded sibling-sacrifice oracle"
    );
    assert!(
        inventory_source
            .contains("fail-closed must leave zero Core inventory rows before session shutdown"),
        "ultimate close failure must keep the Core inventory sweep"
    );
}

#[test]
fn local_webrtc_spawned_session_is_cleaned_even_if_attach_proof_panics_after_ready() {
    // Deliberate failure after Spawn readiness must still reap the worker via Drop unwind.
    // Keep the harness live until panic so Drop runs during catch_unwind stack unwind.
    let mut owned_workers = Vec::new();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut harness = PeerHarness::new("attach-panic-cleanup");
        let origin = "http://127.0.0.1:41798";
        let mut peer = harness.signal_peer(origin);
        harness.spawn_and_attach_on_peer(
            &mut peer,
            "panic-cleanup-session",
            "panic-cleanup-attach",
        );
        owned_workers = harness.owned_workers.clone();
        assert!(!owned_workers.is_empty());
        assert!(
            harness
                .list_session_lifecycle("panic-cleanup-session")
                .is_some(),
            "session must exist before deliberate panic"
        );
        peer.close_offer();
        // Do not drop harness here — panic while it is still live so Drop runs on unwind.
        panic!("deliberate failure after spawn readiness");
    }));
    assert!(result.is_err(), "deliberate panic must fire");
    assert!(
        take_last_session_cleanup_error().is_none(),
        "Drop cleanup must succeed during unwind"
    );
    wait_for_owned_workers_gone(&owned_workers, Instant::now() + Duration::from_secs(5));
    for worker in &owned_workers {
        assert!(
            worker.is_fully_gone(),
            "owned worker must be fully gone after Drop unwind cleanup: {worker:?}"
        );
        assert!(
            !live_pids_in_process_group(worker.pgid).contains(&worker.pid),
            "worker pid must no longer appear in its process group after Drop cleanup: {worker:?}"
        );
    }
}

#[test]
fn local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner() {
    // Peer A cleanup_once captures subscription_id S, then unsubscribes and peer B reuses S.
    // Delayed PeerClosed for A must not delete B's row.
    let mut harness = PeerHarness::new("stale-snapshot");
    let origin = "http://127.0.0.1:41797";
    let mut peer_a = harness.signal_peer(origin);
    let mut peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();
    let subscription_id = "reused-entity-id".to_string();

    let _ = harness.subscribe_entities(&mut peer_a, &subscription_id);
    harness.ensure_webrtc_adapter_hello(&mut peer_b);
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_a.as_str())
    );

    // Unsubscribe A and register B with the same subscription_id (replacement owner).
    if harness
        .state
        .entity_subscriptions
        .remove(&subscription_id)
        .is_some()
    {
        harness.state.lifecycle_counters.live_entity_subscriptions = harness
            .state
            .lifecycle_counters
            .live_entity_subscriptions
            .saturating_sub(1);
    }
    let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: subscription_id.clone(),
            transport_request_id: None,
            client_id: Some(format!("botster-hub-webrtc-{grant_b}")),
            frame_tx: EntityFrameSender::Async(frame_tx),
            frame_rx: None,
            reply_tx,
            grant_id: Some(grant_b.clone()),
        },
    );
    assert_eq!(
        reply_rx.blocking_recv().expect("reply").expect("ok").kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_b.as_str())
    );

    let terminal_record = LocalWebrtcSenderTerminalRecord {
        schema_version: 1,
        grant_id: grant_a.clone(),
        request_operation: "entity_delivery".to_string(),
        message_id: None,
        next_chunk_index: 0,
        last_sent_chunk_index: None,
        total_chunks: 0,
        pressured: false,
        peer_connection_state: "failed".to_string(),
        channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
        cause: LocalWebrtcTerminalCause::PeerFailed,
        cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
    };
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id: grant_a,
            attached_subscriptions: Vec::new(),
            // Stale snapshot still names the reused subscription id.
            entity_subscription_ids: vec![subscription_id.clone()],
            terminal_record,
        },
    );

    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "replacement owner B must keep the reused subscription id"
    );
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_b.as_str())
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        1
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_subscribe_before_peer_closed_is_swept_by_owner_grant() {
    // Subscribe-first race: daemon registers the entity subscription while the peer is still
    // live, but PeerClosed's cleanup_once snapshot did not include the id. Sweep by
    // owner_grant_id must still remove the daemon row.
    let mut harness = PeerHarness::new("subscribe-first");
    let origin = "http://127.0.0.1:41796";
    let peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let subscription_id = "subscribe-first-entity".to_string();

    assert!(harness.daemon.local_webrtc().has_live_peer(&grant_id));

    let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: subscription_id.clone(),
            transport_request_id: None,
            client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
            frame_tx: EntityFrameSender::Async(frame_tx),
            frame_rx: None,
            reply_tx,
            grant_id: Some(grant_id.clone()),
        },
    );
    let response = reply_rx
        .blocking_recv()
        .expect("reply channel open")
        .expect("subscribe response");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id)
    );
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_id.as_str())
    );

    // PeerClosed with an empty ownership snapshot (as if cleanup_once raced before add).
    let terminal_record = LocalWebrtcSenderTerminalRecord {
        schema_version: 1,
        grant_id: grant_id.clone(),
        request_operation: "entity_delivery".to_string(),
        message_id: None,
        next_chunk_index: 0,
        last_sent_chunk_index: None,
        total_chunks: 0,
        pressured: false,
        peer_connection_state: "failed".to_string(),
        channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
        cause: LocalWebrtcTerminalCause::PeerFailed,
        cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
    };
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id: grant_id.clone(),
            attached_subscriptions: Vec::new(),
            entity_subscription_ids: Vec::new(),
            terminal_record,
        },
    );

    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "PeerClosed must remove grant-owned subscriptions even when the peer snapshot was empty"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        0
    );
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_late_attach_after_peer_closed_does_not_recreate_state() {
    let mut harness = PeerHarness::new("late-attach");
    let origin = "http://127.0.0.1:41799";
    let peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let session_id = "late-attach-session".to_string();
    let subscription_id = "late-attach-sub".to_string();
    let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;

    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
    assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::Request {
            request: Box::new(DaemonRequest::Attach {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            }),
            transport_request_id: None,
            reply_tx,
            response_delivery_rx: None,
            grant_id: Some(grant_id.clone()),
            client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
            enqueued_at: Instant::now(),
        },
    );

    let response = reply_rx
        .blocking_recv()
        .expect("reply channel open")
        .expect("daemon returns operator response");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_peer_gone")
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .active_subscriptions
            .get(&session_id)
            .is_some_and(|subs| subs.contains(&subscription_id)),
        "late Attach must not create residual active attach ownership after PeerClosed"
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .contains_key(&(session_id, subscription_id)),
        "late Attach must not record attach owner grant after PeerClosed"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_attach_subscriptions, live_attach_before,
        "live attach counter must not increase for rejected late Attach"
    );

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_late_spawn_after_peer_closed_does_not_create_session() {
    let mut harness = PeerHarness::new("late-spawn");
    let origin = "http://127.0.0.1:41800";
    let peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let session_id = "late-spawn-session".to_string();

    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::Request {
            request: Box::new(DaemonRequest::Spawn {
                session_id: session_id.clone(),
                command: "true".to_string(),
            }),
            transport_request_id: None,
            reply_tx,
            response_delivery_rx: None,
            grant_id: Some(grant_id.clone()),
            client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
            enqueued_at: Instant::now(),
        },
    );

    let response = reply_rx
        .blocking_recv()
        .expect("reply channel open")
        .expect("daemon returns operator response");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_peer_gone")
    );
    assert!(
        harness.list_session_lifecycle(&session_id).is_none(),
        "late Spawn must not create durable session ownership after PeerClosed"
    );

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row() {
    // Peer A subscribed with id S, then B reused S after A is not live. Late Unsubscribe
    // from A must preserve B's row and counters (owner-checked cleanup).
    let mut harness = PeerHarness::new("late-unsub-reuse");
    let origin = "http://127.0.0.1:41801";
    let peer_a = harness.signal_peer(origin);
    let peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();
    let subscription_id = "reused-unsub-entity".to_string();

    let (frame_tx_a, _frame_rx_a) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: subscription_id.clone(),
            transport_request_id: None,
            client_id: Some(format!("botster-hub-webrtc-{grant_a}")),
            frame_tx: EntityFrameSender::Async(frame_tx_a),
            frame_rx: None,
            reply_tx,
            grant_id: Some(grant_a.clone()),
        },
    );
    assert_eq!(
        reply_rx.blocking_recv().expect("reply").expect("ok").kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );

    // Close A first so grant A is not live, then hand the same id to live peer B.
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "PeerClosed for A must sweep A's entity row before B reuses the id"
    );

    let (frame_tx_b, _frame_rx_b) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: subscription_id.clone(),
            transport_request_id: None,
            client_id: Some(format!("botster-hub-webrtc-{grant_b}")),
            frame_tx: EntityFrameSender::Async(frame_tx_b),
            frame_rx: None,
            reply_tx,
            grant_id: Some(grant_b.clone()),
        },
    );
    assert_eq!(
        reply_rx.blocking_recv().expect("reply").expect("ok").kind,
        botster_hub_client::DaemonResponseKind::EntitySubscribed
    );
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_b.as_str())
    );
    let live_entity_before = harness.state.lifecycle_counters.live_entity_subscriptions;

    let (reply_tx, reply_rx) = control_reply_channel();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::UnsubscribeEntities {
            subscription_id: subscription_id.clone(),
            reply_tx: Some(reply_tx),
            grant_id: Some(grant_a.clone()),
        },
    );
    let response = reply_rx
        .blocking_recv()
        .expect("reply channel open")
        .expect("idempotent unsubscribed reply");
    assert_eq!(
        response.kind,
        botster_hub_client::DaemonResponseKind::EntityUnsubscribed
    );
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key(&subscription_id),
        "late Unsubscribe from stale grant A must preserve replacement owner B's row"
    );
    assert_eq!(
        harness
            .state
            .entity_subscriptions
            .get(&subscription_id)
            .and_then(|sub| sub.owner_grant_id.as_deref()),
        Some(grant_b.as_str())
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions, live_entity_before,
        "entity counter must not drop when preserving replacement owner"
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_attach_owner_sweep_on_empty_snapshot() {
    let _teardown_guard = teardown_test_lock();
    // Attach succeeds while peer is live; PeerClosed with empty attach snapshot must still
    // detach grant-owned attach via control-plane owner index.
    let mut harness = PeerHarness::new("attach-owner-sweep");
    let origin = "http://127.0.0.1:41802";
    let mut peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();
    let session_id = "attach-sweep-session";
    let subscription_id = "attach-sweep-sub";

    harness.ensure_webrtc_adapter_hello(&mut peer);
    harness.spawn_and_attach_on_peer(&mut peer, session_id, subscription_id);
    assert!(
        harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(String::as_str)
            == Some(grant_id.as_str()),
        "successful WebRTC Attach must record grant ownership"
    );
    assert!(
        harness
            .state
            .pending_runtime
            .active_subscriptions
            .get(session_id)
            .is_some_and(|subs| subs.contains(subscription_id))
    );

    let terminal_record = LocalWebrtcSenderTerminalRecord {
        schema_version: 1,
        grant_id: grant_id.clone(),
        request_operation: "attach".to_string(),
        message_id: None,
        next_chunk_index: 0,
        last_sent_chunk_index: None,
        total_chunks: 0,
        pressured: false,
        peer_connection_state: "failed".to_string(),
        channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
        cause: LocalWebrtcTerminalCause::PeerFailed,
        cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
    };
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id: grant_id.clone(),
            // Empty peer-side attach snapshot (raced before peer recorded attach).
            attached_subscriptions: Vec::new(),
            entity_subscription_ids: Vec::new(),
            terminal_record,
        },
    );

    assert!(
        !harness
            .state
            .pending_runtime
            .active_subscriptions
            .get(session_id)
            .is_some_and(|subs| subs.contains(subscription_id)),
        "PeerClosed must detach grant-owned attach even when peer snapshot was empty"
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .contains_key(&(session_id.to_string(), subscription_id.to_string())),
        "attach owner index must be cleared for removed grant"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_attach_subscriptions,
        0
    );
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner() {
    let _teardown_guard = teardown_test_lock();
    // Peer A attached (session S, sub X), then B reused the same attach ids while A is gone.
    // Delayed PeerClosed for A still carries A's attach snapshot and must not detach B's row.
    let mut harness = PeerHarness::new("stale-attach-snapshot");
    let origin = "http://127.0.0.1:41804";
    let mut peer_a = harness.signal_peer(origin);
    let mut peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();
    let session_id = "reused-attach-session";
    let subscription_id = "reused-attach-sub";

    harness.ensure_webrtc_adapter_hello(&mut peer_a);
    harness.ensure_webrtc_adapter_hello(&mut peer_b);
    harness.spawn_and_attach_on_peer(&mut peer_a, session_id, subscription_id);
    assert_eq!(
        harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(String::as_str),
        Some(grant_a.as_str())
    );

    // Close A without clearing B's future ownership of the reused attach id.
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
    harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));

    // B attaches with the same session/subscription ids (replacement owner).
    // Session may still exist after A's PeerClosed detach; re-attach under B.
    let attach_b = harness.request_on_peer(
        &mut peer_b,
        DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
        "Attach-B-reuse",
    );
    assert_eq!(
        attach_b.kind,
        botster_hub_client::DaemonResponseKind::TerminalReservation,
        "replacement owner B must reserve successfully: {:?}",
        attach_b.error
    );
    assert!(attach_b.terminal_reservation.is_some());
    assert_eq!(
        harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(String::as_str),
        Some(grant_b.as_str())
    );
    let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;
    assert!(live_attach_before >= 1);

    // Delayed PeerClosed for A with a stale attach snapshot naming the reused ids.
    let terminal_record = LocalWebrtcSenderTerminalRecord {
        schema_version: 1,
        grant_id: grant_a.clone(),
        request_operation: "attach".to_string(),
        message_id: None,
        next_chunk_index: 0,
        last_sent_chunk_index: None,
        total_chunks: 0,
        pressured: false,
        peer_connection_state: "failed".to_string(),
        channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
        cause: LocalWebrtcTerminalCause::PeerFailed,
        cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
    };
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id: grant_a.clone(),
            attached_subscriptions: vec![LocalWebrtcAttachedSubscription {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            }],
            entity_subscription_ids: Vec::new(),
            terminal_record,
        },
    );

    assert!(
        harness
            .state
            .pending_runtime
            .active_subscriptions
            .get(session_id)
            .is_some_and(|subs| subs.contains(subscription_id)),
        "delayed PeerClosed for A must not detach B's reused attach"
    );
    assert_eq!(
        harness
            .state
            .pending_runtime
            .attach_owner_grant_ids
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(String::as_str),
        Some(grant_b.as_str()),
        "replacement owner B must remain in attach owner index"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_attach_subscriptions, live_attach_before,
        "live attach counter must not drop when preserving replacement owner"
    );
    assert!(harness.daemon.local_webrtc().has_live_peer(&grant_b));

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

/// Child env for the hang-close subprocess oracle. Parent kills the child when the
/// whole-child deadline is exceeded so ablating the production close timeout yields a
/// finite red result instead of hanging the suite.
const HANG_CLOSE_CHILD_ENV: &str = "BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD";
/// Whole-child budget: signal peers + entity subscribe + close bound + fail-closed + cleanup.
/// Intentionally avoids durable session workers so a parent kill cannot orphan them.
const HANG_CLOSE_CHILD_DEADLINE: Duration = Duration::from_secs(15);

fn run_close_hang_fail_closed_body() {
    let _teardown_guard = teardown_test_lock();
    // Deterministic hang on production remove_peer/close path. Handler must return within
    // HANDLER_JOIN_DEADLINE and take the fail-closed sibling path (timeout ≡ ultimate failure).
    // No Spawn/Attach: durable session workers would be orphaned if the parent hard-kills the
    // child after timeout ablation. Sibling attach fail-closed is covered by the forced-error
    // path (`local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads`).
    let mut harness = PeerHarness::new("close-hang-sibling");
    let origin = "http://127.0.0.1:41803";
    let mut peer_a = harness.signal_peer(origin);
    let mut peer_b = harness.signal_peer(origin);
    let grant_a = peer_a.grant_id.clone();
    let grant_b = peer_b.grant_id.clone();

    let _ = harness.subscribe_entities(&mut peer_a, "entity-hang-a");
    let _ = harness.subscribe_entities(&mut peer_b, "entity-hang-b");
    assert!(
        harness
            .state
            .entity_subscriptions
            .contains_key("entity-hang-b")
    );
    assert!(
        harness.owned_workers.is_empty(),
        "hang hard-stop child must not create durable session workers"
    );
    assert!(
        harness
            .daemon
            .local_webrtc()
            .dedicated_runtime_worker_threads()
            >= 1
    );

    harness
        .daemon
        .local_webrtc()
        .force_next_close_hang_for_test(&grant_a);
    harness
        .daemon
        .local_webrtc()
        .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);

    // Drain until PeerClosed is available, but do not handle it on this thread yet.
    let peer_closed_message = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() >= deadline {
                panic!("timed out waiting for LocalWebrtcPeerClosed for hang test");
            }
            match harness.control_rx.try_recv() {
                Ok(message) => {
                    let is_closed = matches!(
                        &message,
                        ControlMessage::LocalWebrtcPeerClosed { grant_id: closed, .. }
                            if closed == &grant_a
                    );
                    if is_closed {
                        break message;
                    }
                    // Handle non-PeerClosed control traffic so the channel does not stall.
                    handle_control_message(
                        &mut harness.daemon,
                        &mut harness.state,
                        &harness.transport_handle,
                        harness.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed before LocalWebrtcPeerClosed");
                }
            }
        }
    };

    // Production PeerClosed handler under forced hang must return within HANDLER_JOIN_DEADLINE.
    // Hang inject goes through the production timeout wrapper around the close future.
    let handler_started = Instant::now();
    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        peer_closed_message,
    );
    let handler_elapsed = handler_started.elapsed();
    assert!(
        handler_elapsed <= LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
        "production PeerClosed handler elapsed {handler_elapsed:?} must be within HANDLER_JOIN_DEADLINE {:?} under forced close hang",
        LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE
    );

    assert_eq!(
        harness.daemon.local_webrtc().active_peer_count(),
        0,
        "fail-closed hang path must clear live peer map"
    );
    assert!(
        !harness.daemon.local_webrtc().has_dedicated_runtime(),
        "fail-closed hang path must drop dedicated runtime"
    );
    assert_eq!(
        harness.daemon.local_webrtc().peer_state_count(),
        0,
        "fail-closed hang path must clear primary + sibling peer_states"
    );
    assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
    assert!(
        !harness.daemon.local_webrtc().has_live_peer(&grant_b),
        "timeout fail-closed must sacrifice sibling peers"
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("entity-hang-a")
    );
    assert!(
        !harness
            .state
            .entity_subscriptions
            .contains_key("entity-hang-b"),
        "fail-closed hang path must clear sibling entity ownership"
    );
    assert_eq!(
        harness.state.lifecycle_counters.live_entity_subscriptions,
        0
    );
    wait_until(
        Instant::now() + Duration::from_secs(2),
        || {
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                == 0
        },
        "dedicated runtime workers must join after hang fail-closed teardown",
    );
    let inventory = harness
        .daemon
        .runtime_mut()
        .expect("runtime")
        .list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
        .wait(Duration::from_secs(5))
        .expect("inventory")
        .expect("inventory fits the test allowance")
        .records;
    assert!(
        inventory.is_empty(),
        "timeout fail-closed must leave zero Core inventory rows: {inventory:?}"
    );
    assert!(
        harness.state.pending_runtime.live_attach_routes.is_empty(),
        "timeout fail-closed must leave zero Hub attach routes: {:?}",
        harness.state.pending_runtime.live_attach_routes
    );

    peer_a.close_offer();
    peer_b.close_offer();
    harness.cleanup();
}

#[test]
fn local_webrtc_close_hang_fail_closed_returns_handler_within_deadline() {
    // External hard-stop oracle: parent process waits on a child that runs the production
    // hang path. If the production close timeout is ablated, the child never exits and the
    // parent kills it after HANG_CLOSE_CHILD_DEADLINE — finite red, not suite hang.
    if std::env::var_os(HANG_CLOSE_CHILD_ENV).is_some() {
        run_close_hang_fail_closed_body();
        return;
    }

    let exe = std::env::current_exe().expect("test executable path");
    let mut child = std::process::Command::new(&exe)
        .env(HANG_CLOSE_CHILD_ENV, "1")
        .env("RUST_BACKTRACE", "0")
        .args([
            "--exact",
            "transport::webrtc::peer::tests::local_webrtc_close_hang_fail_closed_returns_handler_within_deadline",
            "--nocapture",
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn hang-close oracle child");

    let deadline = Instant::now() + HANG_CLOSE_CHILD_DEADLINE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(
                    status.success(),
                    "hang-close child must exit 0 when production close bound is present; status={status}"
                );
                return;
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "hang-close child exceeded {:?}; production close timeout missing or hang path blocked (red-on-revert hard stop)",
                    HANG_CLOSE_CHILD_DEADLINE
                );
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => panic!("hang-close child wait failed: {error}"),
        }
    }
}

#[test]
fn runtime_spawn_detach_on_drop_runs_to_completion() {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for spawn detach proof");
    tokio_rt.block_on(async {
        let runtime = default_runtime().expect("webrtc default runtime");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        {
            let _handle = runtime.spawn(Box::pin(async move {
                let _ = started_tx.send(());
                tokio::task::yield_now().await;
                let _ = done_tx.send(());
            }));
            started_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("spawned task must start");
        }
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("Runtime::spawn must run to completion after JoinHandle drop");
    });
}

struct LateChannelHandler {
    gather_complete_tx: AsyncSender<()>,
    connected_tx: AsyncSender<()>,
    incoming_tx: AsyncSender<Arc<dyn DataChannel>>,
}

#[async_trait]
impl PeerConnectionEventHandler for LateChannelHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let _ = self.incoming_tx.try_send(data_channel);
    }
}

#[test]
fn post_handshake_data_channel_opens_and_delivers_bytes() {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("botster-webrtc-late-channel")
        .build()
        .expect("late-channel tokio runtime");
    tokio_rt.block_on(async {
        let runtime = default_runtime().expect("webrtc default runtime");
        let (offerer_gather_tx, mut offerer_gather_rx) = webrtc_channel::<()>(1);
        let (answerer_gather_tx, mut answerer_gather_rx) = webrtc_channel::<()>(1);
        let (offerer_connected_tx, mut offerer_connected_rx) = webrtc_channel::<()>(1);
        let (answerer_connected_tx, mut answerer_connected_rx) = webrtc_channel::<()>(1);
        let (incoming_tx, mut incoming_rx) = webrtc_channel::<Arc<dyn DataChannel>>(4);

        let offerer = PeerConnectionBuilder::new()
            .with_handler(Arc::new(LateChannelHandler {
                gather_complete_tx: offerer_gather_tx,
                connected_tx: offerer_connected_tx,
                incoming_tx: incoming_tx.clone(),
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .expect("build offerer");
        let answerer = PeerConnectionBuilder::new()
            .with_handler(Arc::new(LateChannelHandler {
                gather_complete_tx: answerer_gather_tx,
                connected_tx: answerer_connected_tx,
                incoming_tx,
            }))
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .expect("build answerer");

        let setup = offerer
            .create_data_channel(
                "botster-client",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await
            .expect("create pre-handshake setup DataChannel");
        let (setup_open_tx, mut setup_open_rx) = webrtc_channel::<()>(1);
        runtime.spawn(Box::pin({
            let setup = setup.clone();
            async move {
                while let Some(event) = setup.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = setup_open_tx.try_send(());
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }
        }));

        let offer = offerer.create_offer(None).await.expect("create offer");
        offerer
            .set_local_description(offer)
            .await
            .expect("set local offer");
        timeout(
            runtime.as_ref(),
            Duration::from_secs(5),
            offerer_gather_rx.recv(),
        )
        .await
        .expect("offerer ICE gather")
        .expect("offerer gather signal");
        let offer = offerer
            .local_description()
            .await
            .expect("offerer local description");
        answerer
            .set_remote_description(offer)
            .await
            .expect("answerer set remote offer");
        let answer = answerer.create_answer(None).await.expect("create answer");
        answerer
            .set_local_description(answer)
            .await
            .expect("set local answer");
        timeout(
            runtime.as_ref(),
            Duration::from_secs(5),
            answerer_gather_rx.recv(),
        )
        .await
        .expect("answerer ICE gather")
        .expect("answerer gather signal");
        let answer = answerer
            .local_description()
            .await
            .expect("answerer local description");
        offerer
            .set_remote_description(answer)
            .await
            .expect("offerer set remote answer");

        timeout(
            runtime.as_ref(),
            Duration::from_secs(15),
            offerer_connected_rx.recv(),
        )
        .await
        .expect("offerer Connected")
        .expect("offerer connected signal");
        timeout(
            runtime.as_ref(),
            Duration::from_secs(15),
            answerer_connected_rx.recv(),
        )
        .await
        .expect("answerer Connected")
        .expect("answerer connected signal");
        timeout(
            runtime.as_ref(),
            Duration::from_secs(10),
            setup_open_rx.recv(),
        )
        .await
        .expect("pre-handshake setup channel open")
        .expect("setup open signal");
        let setup_remote = timeout(
            runtime.as_ref(),
            Duration::from_secs(10),
            incoming_rx.recv(),
        )
        .await
        .expect("remote setup on_data_channel")
        .expect("remote setup channel");
        assert_eq!(
            setup_remote.label().await.expect("setup remote label"),
            "botster-client"
        );

        let late = offerer
            .create_data_channel(
                "botster-late",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await
            .expect("create post-handshake DataChannel");
        assert!(late.ordered().await.expect("late ordered"));
        assert_eq!(
            late.max_retransmits().await.expect("late retransmits"),
            None
        );
        assert_eq!(
            late.max_packet_life_time().await.expect("late lifetime"),
            None
        );

        let (late_open_tx, mut late_open_rx) = webrtc_channel::<()>(1);
        let (late_message_tx, late_message_rx) = webrtc_channel::<String>(8);
        runtime.spawn(Box::pin({
            let late = late.clone();
            async move {
                while let Some(event) = late.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = late_open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = late_message_tx.try_send(text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }
        }));

        let remote = timeout(runtime.as_ref(), Duration::from_secs(10), async {
            loop {
                let channel = incoming_rx
                    .recv()
                    .await
                    .expect("remote late on_data_channel");
                if channel.label().await.expect("incoming label") == "botster-late" {
                    break channel;
                }
            }
        })
        .await
        .expect("remote late channel by label");
        assert_eq!(remote.label().await.expect("remote label"), "botster-late");

        let (remote_open_tx, mut remote_open_rx) = webrtc_channel::<()>(1);
        let (remote_message_tx, mut remote_message_rx) = webrtc_channel::<String>(8);
        runtime.spawn(Box::pin({
            let remote = remote.clone();
            async move {
                while let Some(event) = remote.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = remote_open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = remote_message_tx.try_send(text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }
        }));

        timeout(
            runtime.as_ref(),
            Duration::from_secs(10),
            late_open_rx.recv(),
        )
        .await
        .expect("creating side OnOpen")
        .expect("late open signal");
        timeout(
            runtime.as_ref(),
            Duration::from_secs(10),
            remote_open_rx.recv(),
        )
        .await
        .expect("remote OnOpen")
        .expect("remote open signal");

        const PAYLOAD: &str = "post-handshake-bytes";
        late.send_text(PAYLOAD)
            .await
            .expect("send on late DataChannel");
        let received = timeout(
            runtime.as_ref(),
            Duration::from_secs(10),
            remote_message_rx.recv(),
        )
        .await
        .expect("remote delivery")
        .expect("remote payload");
        assert_eq!(received, PAYLOAD);
        let _ = late_message_rx;
        let _ = offerer.close().await;
        let _ = answerer.close().await;
    });
}

#[test]
fn peer_closed_removes_webrtc_admission_and_host_compatibility() {
    let mut harness = PeerHarness::new("hello-sweep");
    let origin = "http://127.0.0.1:41821";
    let peer = harness.signal_peer(origin);
    let grant_id = peer.grant_id.clone();

    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::RegisterWebrtcAdmission {
            grant_id: grant_id.clone(),
            admission: WebrtcTerminalAdmission::Rejected {
                code: "test_admission_row",
                diagnostic: DaemonDiagnostic::connected("hello"),
                mux: WebRtcConnectionMux::new(),
                peer_generation: 0,
            },
            host_required_features: vec!["host-feature".to_string()],
        },
    );
    assert!(
        harness
            .state
            .pending_runtime
            .has_webrtc_admission_row(&grant_id),
        "positive control: RegisterWebrtcAdmission must insert the admission row"
    );
    assert!(
        harness
            .state
            .pending_runtime
            .has_host_compatibility_row(&grant_id),
        "positive control: RegisterWebrtcAdmission must insert the host compatibility row"
    );
    assert!(
        !harness.state.pending_runtime.webrtc_is_admitted(&grant_id),
        "Rejected rows must not satisfy webrtc_is_admitted; the sweep uses contains_key"
    );

    handle_control_message(
        &mut harness.daemon,
        &mut harness.state,
        &harness.transport_handle,
        harness.control_tx.clone(),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id: grant_id.clone(),
            attached_subscriptions: Vec::new(),
            entity_subscription_ids: Vec::new(),
            terminal_record: LocalWebrtcSenderTerminalRecord {
                schema_version: 1,
                grant_id: grant_id.clone(),
                request_operation: "hello".to_string(),
                message_id: None,
                next_chunk_index: 0,
                last_sent_chunk_index: None,
                total_chunks: 0,
                pressured: false,
                peer_connection_state: "closed".to_string(),
                channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
                cause: LocalWebrtcTerminalCause::PeerClosed,
                cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
            },
        },
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .has_webrtc_admission_row(&grant_id),
        "PeerClosed must remove the webrtc admission row"
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .has_host_compatibility_row(&grant_id),
        "PeerClosed must remove the host compatibility row"
    );

    peer.close_offer();
    harness.cleanup();
}

#[test]
fn disconnect_is_terminal_for_a_peer_holding_only_an_unbound_reservation() {
    use crate::transport::webrtc::subscription_channel::{
        LocalWebrtcAttachedSubscription, LocalWebrtcAttachedSubscriptionChange,
    };
    let idle = crate::transport::webrtc::test_support::test_peer_state("grant-idle");
    assert_eq!(
        idle.observe_peer_connection_state(RTCPeerConnectionState::Disconnected),
        None,
        "a peer that owns no route may recover from disconnect"
    );
    let reserved = crate::transport::webrtc::test_support::test_peer_state("grant-reserved");
    reserved.apply_subscription_change(Some(LocalWebrtcAttachedSubscriptionChange::Attach(
        LocalWebrtcAttachedSubscription {
            session_id: "reserved-session".to_string(),
            subscription_id: "reserved-subscription".to_string(),
        },
    )));
    assert_eq!(
        reserved.observe_peer_connection_state(RTCPeerConnectionState::Disconnected),
        Some(LocalWebrtcTerminalCause::PeerDisconnected),
        "an unbound reservation holds a Core attach, so disconnect must clean it up"
    );
}

fn admitted_peer_generation(harness: &PeerHarness, grant_id: &str) -> u64 {
    match harness
        .state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(grant_id)
    {
        Some(WebrtcTerminalAdmission::Admitted {
            peer_generation, ..
        }) => *peer_generation,
        other => panic!("peer must be admitted: {other:?}"),
    }
}

fn core_route(
    harness: &PeerHarness,
    session_id: &str,
    subscription_id: &str,
) -> Option<botster_core::TerminalSubscriptionRecord> {
    harness
        .daemon
        .runtime()
        .expect("runtime")
        .list_terminal_subscriptions_for_test()
        .into_iter()
        .find(|row| row.session_id.0 == session_id && row.subscription_id.0 == subscription_id)
}

fn wait_for_observation(harness: &mut PeerHarness, peer: &mut LiveSignaledPeer, kind: &str) {
    let mut seen = Vec::new();
    for _ in 0..32 {
        match harness.wait_for_host_event(peer, kind) {
            botster_hub_client::DaemonEvent::RuntimeObservation { kind: observed }
                if observed == kind =>
            {
                return;
            }
            other => seen.push(other),
        }
    }
    panic!("observation {kind} not delivered; saw {seen:?}");
}

fn webrtc_adapter_hello() -> DaemonHello {
    DaemonHello {
        protocol: PROTOCOL.to_string(),
        compatibility:
            botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
        terminal_compatibility: None,
    }
}

const IDLE_ECHO: &str =
    "printf 'reserve-ready\\n'; while IFS= read -r line; do printf 'r:%s\\n' \"$line\"; done";

/// Protocol 10: Attach reserves a channel and creates no Core route. The
/// channel's Hello attaches and binds in one Core call, and the HelloAck
/// names the Core generation of that bound route.
#[test]
fn webrtc_attach_holds_no_core_route_until_the_channel_binds_it_atomically() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-declare-at-ready");
    let mut peer = harness.signal_peer("http://127.0.0.1:41831");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let session_id = "declare-at-ready-session";
    let subscription_id = "declare-at-ready-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);

    let reservation = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    assert!(
        core_route(&harness, session_id, subscription_id).is_none(),
        "a reservation must not declare a Core route"
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_identity(session_id, subscription_id)
            .is_some(),
        "the Hub attach stream exists before the bind"
    );
    assert!(
        !harness
            .state
            .pending_runtime
            .is_adapter_bound(session_id, subscription_id)
    );

    let generation = harness
        .bind_reserved_on_peer(&mut peer, &reservation.label)
        .expect("terminal HelloAck carries terminal_generation");
    harness.wait_until_adapter_bound(session_id, subscription_id);
    let route =
        core_route(&harness, session_id, subscription_id).expect("the bind created the Core route");
    assert!(
        route.adapter_bound,
        "attach and bind land together: {route:?}"
    );
    assert_eq!(
        route.generation.0, generation,
        "the HelloAck names the bound route's Core generation"
    );
    assert_eq!(
        harness
            .state
            .pending_runtime
            .recorded_generation(session_id, subscription_id)
            .map(|recorded| recorded.0),
        Some(generation)
    );
    peer.close_offer();
    harness.cleanup();
}

/// A Core attach+bind that fails rejects the channel with bind_failed,
/// sends no HelloAck, and leaves no Core route and no Hub stream.
#[test]
fn failed_webrtc_attach_bind_rejects_the_channel_and_leaves_nothing() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-bind-failure");
    let mut peer = harness.signal_peer("http://127.0.0.1:41832");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let session_id = "bind-failure-session";
    let subscription_id = "bind-failure-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let reservation = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    // The session ends before the channel opens, so Core refuses the attach.
    harness
        .shutdown_owned_sessions()
        .expect("the session shuts down and is removed");

    assert!(
        harness.open_reserved_expecting_reject(&mut peer, &reservation.label),
        "Hub must close the channel without a HelloAck"
    );
    wait_for_observation(
        &mut harness,
        &mut peer,
        &format!(
            "subscription_channel_rejected:bind_failed:{}",
            reservation.label
        ),
    );
    assert!(core_route(&harness, session_id, subscription_id).is_none());
    assert!(
        harness
            .state
            .pending_runtime
            .stream_identity(session_id, subscription_id)
            .is_none(),
        "the failed attach releases its Hub stream"
    );
    peer.close_offer();
    harness.cleanup();
}

/// Expiry of an unopened terminal reservation reports it by label and
/// releases only its own stream: a replacement stream on the same route
/// survives. Nothing exists in Core either way.
#[test]
fn terminal_reservation_expiry_reports_by_label_and_spares_a_replacement_stream() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-reservation-expiry");
    let mut peer = harness.signal_peer("http://127.0.0.1:41833");
    let grant_id = peer.grant_id.clone();
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let session_id = "expiry-session";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let peer_generation = admitted_peer_generation(&harness, &grant_id);

    // Plain expiry releases the reserved stream and reports it once.
    let expired = harness.reserve_attach_on_peer(&mut peer, session_id, "expiry-sub");
    crate::daemon::control::connection::emit_reservation_expired(
        &mut harness.daemon,
        &mut harness.state,
        &grant_id,
        peer_generation,
        &expired.label,
        u64::MAX,
        true,
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_identity(session_id, "expiry-sub")
            .is_none()
    );
    let expired_kind = format!(
        "subscription_channel_rejected:reservation_expired:{}",
        expired.label
    );
    let count = |events: &[botster_hub_client::DaemonEvent], kind: &str| {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    botster_hub_client::DaemonEvent::RuntimeObservation { kind: observed }
                        if observed == kind
                )
            })
            .count()
    };
    let events = harness.drain_host_events(&mut peer, Duration::from_millis(500));
    assert_eq!(
        count(&events, &expired_kind),
        1,
        "one signal at expiry: {events:?}"
    );
    assert!(core_route(&harness, session_id, "expiry-sub").is_none());
    // A second expiry pass is not a transition: nothing is repeated.
    crate::daemon::control::connection::emit_reservation_expired(
        &mut harness.daemon,
        &mut harness.state,
        &grant_id,
        peer_generation,
        &expired.label,
        u64::MAX,
        true,
    );
    let events = harness.drain_host_events(&mut peer, Duration::from_millis(500));
    assert_eq!(
        count(&events, &expired_kind),
        0,
        "expiry reports once: {events:?}"
    );
    // A late open is one attempt with one signal.
    assert!(harness.open_reserved_expecting_reject(&mut peer, &expired.label));
    let events = harness.drain_host_events(&mut peer, Duration::from_millis(500));
    assert_eq!(
        count(&events, &expired_kind),
        1,
        "one signal per late open: {events:?}"
    );

    // A late open that is the first to see the expiry reports it once:
    // the transition is silent and the reject is the one signal.
    let unnoticed = harness.reserve_attach_on_peer(&mut peer, session_id, "unnoticed-sub");
    harness
        .state
        .pending_runtime
        .admission
        .reservations
        .make_past_due_for_test(&unnoticed.label);
    assert!(harness.open_reserved_expecting_reject(&mut peer, &unnoticed.label));
    let unnoticed_kind = format!(
        "subscription_channel_rejected:reservation_expired:{}",
        unnoticed.label
    );
    let events = harness.drain_host_events(&mut peer, Duration::from_millis(500));
    assert_eq!(
        count(&events, &unnoticed_kind),
        1,
        "a first-noticed late open reports once: {events:?}"
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_identity(session_id, "unnoticed-sub")
            .is_none(),
        "the first-noticed expiry releases the stream"
    );

    // An entity reservation expires the same way, once.
    let entity = harness.request_on_peer(
        &mut peer,
        DaemonRequest::SubscribeEntities {
            entity_type: "session".to_string(),
            subscription_id: "expiry-entities".to_string(),
        },
        "SubscribeEntities",
    );
    let entity_label = entity
        .subscription_reservation
        .expect("entity reservation")
        .label;
    for _ in 0..2 {
        crate::daemon::control::connection::emit_reservation_expired(
            &mut harness.daemon,
            &mut harness.state,
            &grant_id,
            peer_generation,
            &entity_label,
            u64::MAX,
            true,
        );
    }
    let events = harness.drain_host_events(&mut peer, Duration::from_millis(500));
    assert_eq!(
        count(
            &events,
            &format!("subscription_channel_rejected:reservation_expired:{entity_label}")
        ),
        1,
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                botster_hub_client::DaemonEvent::RuntimeObservation { kind }
                    if kind.starts_with("entity_subscription_closed:expiry-entities:")
            ))
            .count(),
        1,
        "{events:?}"
    );

    // A replacement stream on the same route is not the reservation's.
    let fenced = harness.reserve_attach_on_peer(&mut peer, session_id, "fenced-sub");
    let replacement = harness.state.pending_runtime.start_attach(
        crate::subscription::attach_routes::AttachStreamOwner {
            client_id: "replacement-client".to_string(),
            grant_id: None,
        },
        session_id.to_string(),
        "fenced-sub".to_string(),
    );
    crate::daemon::control::connection::emit_reservation_expired(
        &mut harness.daemon,
        &mut harness.state,
        &grant_id,
        peer_generation,
        &fenced.label,
        u64::MAX,
        true,
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_matches(session_id, "fenced-sub", &replacement),
        "an old reservation must not cancel a replacement stream"
    );
    let _ = harness
        .state
        .pending_runtime
        .cancel_stream_if(session_id, "fenced-sub", &replacement);
    peer.close_offer();
    harness.cleanup();
}

/// A bind whose stream was replaced after the reservation is refused
/// before any Core work: no Core route, and the replacement survives.
#[test]
fn webrtc_bind_for_a_replaced_stream_rejects_before_core_and_spares_the_replacement() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-bind-fence");
    let mut peer = harness.signal_peer("http://127.0.0.1:41834");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let session_id = "bind-fence-session";
    let subscription_id = "bind-fence-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let reservation = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    let replacement = harness.state.pending_runtime.start_attach(
        crate::subscription::attach_routes::AttachStreamOwner {
            client_id: "replacement-client".to_string(),
            grant_id: None,
        },
        session_id.to_string(),
        subscription_id.to_string(),
    );

    assert!(
        harness.open_reserved_expecting_reject(&mut peer, &reservation.label),
        "a bind for a replaced stream must not acknowledge"
    );
    harness.pump_until(
        Instant::now() + Duration::from_secs(10),
        "no Core route for the refused bind",
        |harness| core_route(harness, session_id, subscription_id).is_none(),
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_matches(session_id, subscription_id, &replacement),
        "the replacement stream survives the stale bind"
    );
    let _ =
        harness
            .state
            .pending_runtime
            .cancel_stream_if(session_id, subscription_id, &replacement);
    peer.close_offer();
    harness.cleanup();
}

/// The bind succeeded but the terminal HelloAck could not be sent, so the
/// client never learned the generation. Hub closes the bound adapter,
/// Core ends exactly that route, and the reservation and stream retire.
#[test]
fn terminal_hello_ack_failure_after_bind_ends_the_route_and_retires_the_stream() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-ack-failure");
    let mut peer = harness.signal_peer("http://127.0.0.1:41835");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let grant_id = peer.grant_id.clone();
    let session_id = "ack-failure-session";
    let subscription_id = "ack-failure-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let reservation = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    let peer_generation = admitted_peer_generation(&harness, &grant_id);
    let peer_state = harness
        .daemon
        .local_webrtc()
        .peer_states
        .get(&grant_id)
        .expect("live peer state")
        .clone();

    let channel = Arc::new(FakeDataChannel::default());
    channel.push_event(encrypted_hello_event(
        &peer.stream_key,
        &webrtc_adapter_hello(),
    ));
    channel
        .send_fails
        .store(true, std::sync::atomic::Ordering::Release);
    let host_channel = Arc::clone(&channel);
    let label = reservation.label.clone();
    let key = peer.stream_key.clone();
    let host = harness.transport_handle.spawn(async move {
        crate::transport::webrtc::subscription_channel::admit_reserved_subscription_channel(
            &grant_id,
            &label,
            host_channel.as_ref(),
            &key,
            peer_state.as_ref(),
        )
        .await;
    });
    harness.pump_until(
        Instant::now() + Duration::from_secs(10),
        "the channel host to finish after the failed HelloAck",
        |_| host.is_finished(),
    );
    assert!(
        channel.sent.lock().expect("sent frames").is_empty(),
        "no HelloAck reached the client"
    );
    harness.pump_until(
        Instant::now() + Duration::from_secs(10),
        "Core to end the bound route and Hub to retire it",
        |harness| {
            core_route(harness, session_id, subscription_id).is_none()
                && harness
                    .state
                    .pending_runtime
                    .admission
                    .reservations
                    .reservation_for_label(&reservation.label, peer_generation)
                    .is_none()
                && harness
                    .state
                    .pending_runtime
                    .stream_identity(session_id, subscription_id)
                    .is_none()
        },
    );
    peer.close_offer();
    harness.cleanup();
}

/// Two channels for one label: the first bind claims the reservation
/// before its Core call, so the second is rejected as a duplicate and
/// cannot replace the accepted route.
#[test]
fn overlapping_terminal_binds_for_one_label_accept_exactly_one() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-overlapping-bind");
    let mut peer = harness.signal_peer("http://127.0.0.1:41836");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let grant_id = peer.grant_id.clone();
    let session_id = "overlap-session";
    let subscription_id = "overlap-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let reservation = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    let peer_state = harness
        .daemon
        .local_webrtc()
        .peer_states
        .get(&grant_id)
        .expect("live peer state")
        .clone();
    let mut channels = Vec::new();
    let mut hosts = Vec::new();
    for _ in 0..2 {
        let channel = Arc::new(FakeDataChannel::default());
        channel.push_event(encrypted_hello_event(
            &peer.stream_key,
            &webrtc_adapter_hello(),
        ));
        let host_channel = Arc::clone(&channel);
        let host_peer_state = Arc::clone(&peer_state);
        let grant_id = grant_id.clone();
        let label = reservation.label.clone();
        let key = peer.stream_key.clone();
        hosts.push(harness.transport_handle.spawn(async move {
            crate::transport::webrtc::subscription_channel::admit_reserved_subscription_channel(
                &grant_id,
                &label,
                host_channel.as_ref(),
                &key,
                host_peer_state.as_ref(),
            )
            .await;
        }));
        channels.push(channel);
    }
    // Force the overlap: admit both binds in one owner step, before any
    // Core completion can be applied, then let the owner run.
    let mut held = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while held.len() < 2 {
        assert!(
            Instant::now() < deadline,
            "both channels must request a bind"
        );
        match harness.try_receive_owner_message() {
            Ok(message @ crate::daemon::control::message::ControlMessage::BindReservedSubscription { .. }) => {
                held.push(message);
            }
            Ok(message) => {
                crate::daemon::control::handle_control_message(
                    &mut harness.daemon,
                    &mut harness.state,
                    &harness.transport_handle,
                    harness.control_tx.clone(),
                    message,
                );
            }
            Err(tokio_mpsc::error::TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("control channel failed: {error}"),
        }
    }
    for message in held {
        crate::daemon::control::dispatch_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.transport_handle,
            harness.control_tx.clone(),
            message,
        );
    }
    harness.pump_until(
        Instant::now() + Duration::from_secs(10),
        "one channel host to finish",
        |_| hosts.iter().any(|host| host.is_finished()),
    );
    // Let every Core completion of both binds apply before judging.
    let settle = Instant::now() + Duration::from_secs(1);
    harness.pump_until(settle + Duration::from_secs(1), "the settle window", |_| {
        Instant::now() >= settle
    });
    let acknowledged = channels
        .iter()
        .filter(|channel| !channel.sent.lock().expect("sent frames").is_empty())
        .count();
    let route = core_route(&harness, session_id, subscription_id);
    assert_eq!(
        acknowledged, 1,
        "exactly one channel receives a HelloAck; core_route={route:?}"
    );
    let route = route.expect("the accepted route stays in Core");
    assert!(route.adapter_bound, "{route:?}");
    assert!(
        harness
            .state
            .pending_runtime
            .is_adapter_bound(session_id, subscription_id),
        "the accepted stream stays bound"
    );
    assert_eq!(
        harness
            .state
            .pending_runtime
            .recorded_generation(session_id, subscription_id),
        Some(route.generation)
    );
    for host in hosts {
        host.abort();
    }
    peer.close_offer();
    harness.cleanup();
}

/// An Attach whose Core query outlived its reply timeout completes after
/// another Attach reserved the same route. The late completion must not
/// start a stream (that would cancel the live reservation's stream), and a
/// completion for a peer generation that no longer matches reserves
/// nothing.
#[test]
fn deferred_same_route_attach_completion_keeps_the_live_reservation_stream() {
    let _teardown_guard = teardown_test_lock();
    let mut harness = PeerHarness::new("webrtc-deferred-attach");
    let mut peer = harness.signal_peer("http://127.0.0.1:41837");
    harness.hello_on_peer(&mut peer, webrtc_adapter_hello());
    let grant_id = peer.grant_id.clone();
    let session_id = "deferred-session";
    let subscription_id = "deferred-sub";
    harness.spawn_on_peer(&mut peer, session_id, IDLE_ECHO);
    let peer_generation = admitted_peer_generation(&harness, &grant_id);
    let live = harness.reserve_attach_on_peer(&mut peer, session_id, subscription_id);
    let identity = harness
        .state
        .pending_runtime
        .stream_identity(session_id, subscription_id)
        .expect("the live reservation's stream");
    let owner = crate::subscription::attach_routes::AttachStreamOwner {
        client_id: identity.client_id.clone(),
        grant_id: Some(grant_id.clone()),
    };

    let late = crate::daemon::control::sessions::reserve_webrtc_terminal(
        &mut harness.state,
        &owner,
        session_id,
        subscription_id,
        peer_generation,
    );
    assert_eq!(
        late.error.as_ref().map(|error| error.code.as_str()),
        Some("reservation_label_conflict")
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_matches(session_id, subscription_id, &identity),
        "the live reservation keeps its stream"
    );
    assert!(
        harness
            .state
            .pending_runtime
            .admission
            .reservations
            .reservation_for_label(&live.label, peer_generation)
            .is_some()
    );

    let stale_peer = crate::daemon::control::sessions::reserve_webrtc_terminal(
        &mut harness.state,
        &owner,
        session_id,
        "deferred-other-sub",
        peer_generation + 1,
    );
    assert_eq!(
        stale_peer.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_request")
    );
    assert!(
        harness
            .state
            .pending_runtime
            .stream_identity(session_id, "deferred-other-sub")
            .is_none(),
        "a completion for a replaced peer starts no stream"
    );
    peer.close_offer();
    harness.cleanup();
}
