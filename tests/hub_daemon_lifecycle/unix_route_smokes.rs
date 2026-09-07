mod unix_route_smokes {
    use std::time::{Duration, Instant};

    use botster_hub_client::{
        DaemonAttachOccupancy, DaemonRequest, DaemonResponseKind, FEATURE_ATTACH_OCCUPANCY,
    };
    use botster_terminal_protocol::{InputOutcome, TerminalKind};
    use botster_terminal_protocol_client::TerminalInputCommand;

    use super::{
        UnixRouteClient, daemon_test_guard, encode_input_with_operation_id,
        start_isolated_candidate_hub,
    };

    const ROUTE_DEADLINE: Duration = Duration::from_secs(8);

    fn spawn_echo_session(client: &mut UnixRouteClient, session_id: &str) {
        let response = client
            .request(&DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "printf 'route-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            })
            .expect("spawn echo session");
        assert_eq!(response.kind, DaemonResponseKind::Spawned, "{response:?}");
    }

    fn attach_ready(
        client: &mut UnixRouteClient,
        session_id: &str,
        subscription_id: &str,
    ) -> u64 {
        let response = client
            .request(&DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            })
            .expect("attach terminal route");
        assert_eq!(
            response.kind,
            DaemonResponseKind::TerminalAttached,
            "{response:?}"
        );
        let generation = client.route_generation(subscription_id);
        let deadline = Instant::now() + ROUTE_DEADLINE;
        loop {
            client.poll_route_events(Duration::from_millis(25));
            if client.observer(subscription_id).is_some_and(|observer| {
                observer.state().attached() && observer.state().ready_seen
            }) {
                return generation;
            }
            assert!(
                Instant::now() < deadline,
                "route did not become ready: session_id={session_id} subscription_id={subscription_id} observer={:?} abandoned_input_count={} abandoned_inputs={:?}",
                client.observer(subscription_id),
                client.abandoned_input_count(),
                client.abandoned_inputs().collect::<Vec<_>>()
            );
        }
    }

    fn raw_input(data: &[u8]) -> TerminalInputCommand {
        TerminalInputCommand::RawBytes {
            operation_id: 0,
            data: data.to_vec(),
        }
    }

    fn set_marker(client: &mut UnixRouteClient, subscription_id: &str, marker: &str) {
        client
            .observer_mut(subscription_id)
            .unwrap_or_else(|| panic!("missing observer for {subscription_id}"))
            .set_marker(marker.as_bytes());
    }

    fn wait_for_marker_and_result(
        client: &mut UnixRouteClient,
        subscription_id: &str,
        operation_id: Option<u64>,
    ) {
        let deadline = Instant::now() + ROUTE_DEADLINE;
        loop {
            client.poll_route_events(Duration::from_millis(25));
            let observer = client
                .observer(subscription_id)
                .unwrap_or_else(|| panic!("missing observer for {subscription_id}"));
            if observer.state().marker_seen()
                && operation_id.is_none_or(|id| observer.state().has_result(id))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "route did not receive marker or result: subscription_id={subscription_id} operation_id={operation_id:?} observer={observer:?} abandoned_input_count={} abandoned_inputs={:?}",
                client.abandoned_input_count(),
                client.abandoned_inputs().collect::<Vec<_>>()
            );
        }
        if let Some(operation_id) = operation_id {
            let result = client
                .observer_mut(subscription_id)
                .expect("result observer")
                .take_result(operation_id)
                .expect("input result");
            assert_eq!(result.outcome, InputOutcome::Written, "{result:?}");
        }
    }

    fn occupancy_has(
        rows: &[DaemonAttachOccupancy],
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> bool {
        rows.iter().any(|row| {
            row.session_id == session_id
                && row.subscription_id == subscription_id
                && row.generation == generation
        })
    }

    fn wait_for_occupancy(
        client: &mut UnixRouteClient,
        context: &str,
        predicate: impl Fn(&[DaemonAttachOccupancy]) -> bool,
    ) -> Vec<DaemonAttachOccupancy> {
        let deadline = Instant::now() + ROUTE_DEADLINE;
        loop {
            let response = client
                .request(&DaemonRequest::Status)
                .unwrap_or_else(|error| panic!("{context}: status failed: {error}"));
            assert_eq!(response.kind, DaemonResponseKind::Status, "{response:?}");
            let status = response.status.expect("status response body");
            assert!(
                status
                    .compatibility
                    .features
                    .iter()
                    .any(|feature| feature == FEATURE_ATTACH_OCCUPANCY),
                "{context}: attach occupancy is not advertised"
            );
            let rows = status.live_attach_occupancy;
            if predicate(&rows) {
                return rows;
            }
            assert!(
                Instant::now() < deadline,
                "{context}: occupancy deadline passed; rows={rows:?} abandoned_input_count={} abandoned_inputs={:?}",
                client.abandoned_input_count(),
                client.abandoned_inputs().collect::<Vec<_>>()
            );
            client.poll_route_events(Duration::from_millis(10));
        }
    }

    fn shutdown_session(client: &mut UnixRouteClient, session_id: &str) {
        let response = client
            .request(&DaemonRequest::ShutdownSession {
                session_id: session_id.to_string(),
            })
            .expect("shutdown smoke session");
        assert_ne!(response.kind, DaemonResponseKind::OperatorError, "{response:?}");
    }

    #[test]
    fn h_s1_attach_echo_and_detach_through_unix_route_client() {
        let _guard = daemon_test_guard();
        let hub = start_isolated_candidate_hub("h-s1");
        let session_id = "h-s1-session";
        let subscription_id = "h-s1-subscription";
        let mut client = UnixRouteClient::connect(hub.endpoint()).expect("connect H-S1 client");
        spawn_echo_session(&mut client, session_id);
        let generation = attach_ready(&mut client, session_id, subscription_id);
        wait_for_occupancy(&mut client, "H-S1 attached", |rows| {
            occupancy_has(rows, session_id, subscription_id, generation)
        });

        set_marker(&mut client, subscription_id, "echo:h-s1");
        let operation_id = client
            .send_terminal_frame(subscription_id, &raw_input(b"h-s1\r"))
            .expect("send H-S1 input");
        wait_for_marker_and_result(&mut client, subscription_id, Some(operation_id));

        let detached = client
            .request(&DaemonRequest::Detach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            })
            .expect("detach H-S1 route");
        assert_eq!(detached.kind, DaemonResponseKind::Events, "{detached:?}");
        wait_for_occupancy(&mut client, "H-S1 detached", |rows| {
            !rows.iter().any(|row| {
                row.session_id == session_id && row.subscription_id == subscription_id
            })
        });
        assert_eq!(client.abandoned_input_count(), 0);
        shutdown_session(&mut client, session_id);
        drop(client);
        hub.shutdown().expect("shutdown H-S1 hub");
    }

    #[test]
    fn h_s2_two_connections_receive_echo_and_one_disconnect_keeps_the_sibling_live() {
        let _guard = daemon_test_guard();
        let hub = start_isolated_candidate_hub("h-s2");
        let session_id = "h-s2-session";
        let subscription_a = "h-s2-a";
        let subscription_b = "h-s2-b";
        let mut peer_a = UnixRouteClient::connect(hub.endpoint()).expect("connect H-S2 peer A");
        spawn_echo_session(&mut peer_a, session_id);
        let generation_a = attach_ready(&mut peer_a, session_id, subscription_a);
        let mut peer_b = UnixRouteClient::connect(hub.endpoint()).expect("connect H-S2 peer B");
        let generation_b = attach_ready(&mut peer_b, session_id, subscription_b);
        wait_for_occupancy(&mut peer_b, "H-S2 both attached", |rows| {
            occupancy_has(rows, session_id, subscription_a, generation_a)
                && occupancy_has(rows, session_id, subscription_b, generation_b)
        });

        set_marker(&mut peer_a, subscription_a, "echo:h-s2-first");
        set_marker(&mut peer_b, subscription_b, "echo:h-s2-first");
        let operation_id = peer_b
            .send_terminal_frame(subscription_b, &raw_input(b"h-s2-first\r"))
            .expect("send H-S2 shared input");
        let deadline = Instant::now() + ROUTE_DEADLINE;
        loop {
            peer_a.poll_route_events(Duration::from_millis(20));
            peer_b.poll_route_events(Duration::from_millis(20));
            let a_seen = peer_a
                .observer(subscription_a)
                .is_some_and(|observer| observer.state().marker_seen());
            let b_seen = peer_b.observer(subscription_b).is_some_and(|observer| {
                observer.state().marker_seen() && observer.state().has_result(operation_id)
            });
            if a_seen && b_seen {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "H-S2 peers did not both receive the echo: observer_a={:?} observer_b={:?} abandoned_a_count={} abandoned_a={:?} abandoned_b_count={} abandoned_b={:?}",
                peer_a.observer(subscription_a),
                peer_b.observer(subscription_b),
                peer_a.abandoned_input_count(),
                peer_a.abandoned_inputs().collect::<Vec<_>>(),
                peer_b.abandoned_input_count(),
                peer_b.abandoned_inputs().collect::<Vec<_>>()
            );
        }
        let result = peer_b
            .observer_mut(subscription_b)
            .expect("H-S2 B observer")
            .take_result(operation_id)
            .expect("H-S2 B result");
        assert_eq!(result.outcome, InputOutcome::Written, "{result:?}");

        drop(peer_a);
        wait_for_occupancy(&mut peer_b, "H-S2 peer A disconnected", |rows| {
            !rows.iter().any(|row| {
                row.session_id == session_id && row.subscription_id == subscription_a
            }) && occupancy_has(rows, session_id, subscription_b, generation_b)
        });
        set_marker(&mut peer_b, subscription_b, "echo:h-s2-after-drop");
        let operation_id = peer_b
            .send_terminal_frame(subscription_b, &raw_input(b"h-s2-after-drop\r"))
            .expect("send after H-S2 peer A disconnect");
        wait_for_marker_and_result(&mut peer_b, subscription_b, Some(operation_id));
        shutdown_session(&mut peer_b, session_id);
        drop(peer_b);
        hub.shutdown().expect("shutdown H-S2 hub");
    }

    #[test]
    fn h_s4_one_connection_multiplexes_two_sessions_and_converges_same_session_replacement() {
        let _guard = daemon_test_guard();
        let hub = start_isolated_candidate_hub("h-s4");
        let replacement_session_id = "h-s4-replacement-session";
        let replacement_a = "h-s4-replacement-a";
        let replacement_b = "h-s4-replacement-b";
        let session_a = "h-s4-session-a";
        let session_b = "h-s4-session-b";
        let subscription_a = "h-s4-a";
        let subscription_b = "h-s4-b";
        let mut client = UnixRouteClient::connect(hub.endpoint()).expect("connect H-S4 client");

        spawn_echo_session(&mut client, replacement_session_id);
        let replaced_generation =
            attach_ready(&mut client, replacement_session_id, replacement_a);
        let replacement_generation =
            attach_ready(&mut client, replacement_session_id, replacement_b);
        assert_ne!(replaced_generation, replacement_generation);
        wait_for_occupancy(&mut client, "H-S4 same-session replacement", |rows| {
            rows.len() == 1
                && occupancy_has(
                    rows,
                    replacement_session_id,
                    replacement_b,
                    replacement_generation,
                )
        });
        shutdown_session(&mut client, replacement_session_id);
        wait_for_occupancy(&mut client, "H-S4 replacement session stopped", |rows| {
            rows.is_empty()
        });

        spawn_echo_session(&mut client, session_a);
        spawn_echo_session(&mut client, session_b);
        let generation_a = attach_ready(&mut client, session_a, subscription_a);
        let generation_b = attach_ready(&mut client, session_b, subscription_b);
        wait_for_occupancy(&mut client, "H-S4 both attached", |rows| {
            rows.len() == 2
                && occupancy_has(rows, session_a, subscription_a, generation_a)
                && occupancy_has(rows, session_b, subscription_b, generation_b)
        });

        set_marker(&mut client, subscription_a, "echo:h-s4-a");
        set_marker(&mut client, subscription_b, "echo:h-s4-a");
        let operation_a = client
            .send_terminal_frame(subscription_a, &raw_input(b"h-s4-a\r"))
            .expect("send H-S4 route A input");
        wait_for_marker_and_result(&mut client, subscription_a, Some(operation_a));
        client.poll_route_events(Duration::from_millis(100));
        assert!(
            !client
                .observer(subscription_b)
                .expect("H-S4 B observer after A input")
                .state()
                .marker_seen(),
            "session A output must not appear on session B's route"
        );
        set_marker(&mut client, subscription_a, "echo:h-s4-b");
        set_marker(&mut client, subscription_b, "echo:h-s4-b");
        let operation_b = client
            .send_terminal_frame(subscription_b, &raw_input(b"h-s4-b\r"))
            .expect("send H-S4 route B input");
        wait_for_marker_and_result(&mut client, subscription_b, Some(operation_b));
        client.poll_route_events(Duration::from_millis(100));
        assert!(
            !client
                .observer(subscription_a)
                .expect("H-S4 A observer after B input")
                .state()
                .marker_seen(),
            "session B output must not appear on session A's route"
        );

        let detached = client
            .request(&DaemonRequest::Detach {
                session_id: session_a.to_string(),
                subscription_id: subscription_a.to_string(),
            })
            .expect("detach H-S4 route A");
        assert_eq!(detached.kind, DaemonResponseKind::Events, "{detached:?}");
        wait_for_occupancy(&mut client, "H-S4 route A detached", |rows| {
            rows.len() == 1 && occupancy_has(rows, session_b, subscription_b, generation_b)
        });
        let stale_input =
            encode_input_with_operation_id(&raw_input(b"h-s4-stale-generation\r"), 1);
        client
            .send_terminal_bytes_at_generation(subscription_a, generation_a, &stale_input)
            .expect("send H-S4 stale-generation input");
        set_marker(&mut client, subscription_b, "echo:h-s4-after-detach");
        let operation_id = client
            .send_terminal_frame(subscription_b, &raw_input(b"h-s4-after-detach\r"))
            .expect("send after H-S4 route A detach");
        wait_for_marker_and_result(&mut client, subscription_b, Some(operation_id));
        let screen = client
            .request(&DaemonRequest::ReadScreen {
                session_id: session_a.to_string(),
            })
            .expect("read H-S4 screen after stale-generation input");
        assert_eq!(screen.kind, DaemonResponseKind::ReadScreen, "{screen:?}");
        assert!(
            !screen
                .read_screen
                .expect("H-S4 screen body")
                .text
                .contains("h-s4-stale-generation"),
            "stale-generation input must not reach the PTY"
        );
        assert_eq!(client.abandoned_input_count(), 0);
        shutdown_session(&mut client, session_a);
        shutdown_session(&mut client, session_b);
        drop(client);
        hub.shutdown().expect("shutdown H-S4 hub");
    }

    #[test]
    fn h_s6_natural_exit_reports_exit_and_reaps_the_owned_worker() {
        let _guard = daemon_test_guard();
        let hub = start_isolated_candidate_hub("h-s6");
        let session_id = "h-s6-session";
        let subscription_id = "h-s6-subscription";
        let mut client = UnixRouteClient::connect(hub.endpoint()).expect("connect H-S6 client");
        let spawned = client
            .request(&DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: "printf 'h-s6-ready\\n'; sleep 5; exit 7".to_string(),
            })
            .expect("spawn H-S6 finite session");
        assert_eq!(spawned.kind, DaemonResponseKind::Spawned, "{spawned:?}");
        attach_ready(&mut client, session_id, subscription_id);

        let worker_deadline = Instant::now() + Duration::from_secs(1);
        while hub.owned_session_worker_pids().is_empty() && Instant::now() < worker_deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !hub.owned_session_worker_pids().is_empty(),
            "H-S6 positive control did not observe the owned session worker"
        );

        let exit_deadline = Instant::now() + ROUTE_DEADLINE;
        loop {
            client.poll_route_events(Duration::from_millis(25));
            if client
                .observer(subscription_id)
                .is_some_and(|observer| observer.state().process_exit == Some(Some(7)))
            {
                break;
            }
            assert!(
                Instant::now() < exit_deadline,
                "H-S6 did not receive PROCESS_EXIT: {:?}",
                client.observer(subscription_id)
            );
        }
        client.poll_route_events(Duration::from_millis(100));
        assert_eq!(
            client
                .observer(subscription_id)
                .expect("H-S6 observer after exit")
                .state()
                .last_kinds()
                .last(),
            Some(&TerminalKind::ProcessExit),
            "H-S6 PROCESS_EXIT must remain the last route frame"
        );

        let reap_deadline = Instant::now() + Duration::from_secs(5);
        while !hub.owned_session_worker_pids().is_empty() && Instant::now() < reap_deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            hub.owned_session_worker_pids().is_empty(),
            "H-S6 worker remained after natural exit: {:?}",
            hub.owned_session_worker_pids()
        );
        shutdown_session(&mut client, session_id);
        drop(client);
        hub.shutdown().expect("shutdown H-S6 hub");
    }
}
