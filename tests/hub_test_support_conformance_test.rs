use botster_core::{SessionId, TerminalAttachState, TransportEgress};
use botster_hub_client::DaemonEvent;

#[derive(Debug, PartialEq, Eq)]
enum AttachSequenceEvent {
    Attaching,
    History,
    Attached,
    Live,
}

#[test]
fn hub_late_attach_fixture_matches_core_snapshot_before_live_ordering() {
    let core = botster_core_test_support::fixtures::regression::regression_shapes::snapshot_before_live_output(
        SessionId("fixture-ordering-session".to_string()),
        b"history-before-live\r\n",
        b"live-after-attach\r\n",
    )
    .into_iter()
    .filter_map(|event| match event {
        TransportEgress::AttachState {
            state: TerminalAttachState::Attaching,
            ..
        } => Some(AttachSequenceEvent::Attaching),
        TransportEgress::Snapshot { .. } | TransportEgress::Scrollback { .. } => {
            Some(AttachSequenceEvent::History)
        }
        TransportEgress::AttachState {
            state: TerminalAttachState::Attached,
            ..
        } => Some(AttachSequenceEvent::Attached),
        TransportEgress::TerminalOutput { .. } => Some(AttachSequenceEvent::Live),
        _ => None,
    })
    .collect::<Vec<_>>();

    let hub = botster_hub_test_support::late_attach_history_events()
        .into_iter()
        .filter_map(|event| match event {
            DaemonEvent::AttachState { state, .. } if state == "attaching" => {
                Some(AttachSequenceEvent::Attaching)
            }
            DaemonEvent::Snapshot { .. } | DaemonEvent::Scrollback { .. } => {
                Some(AttachSequenceEvent::History)
            }
            DaemonEvent::AttachState { state, .. } if state == "attached" => {
                Some(AttachSequenceEvent::Attached)
            }
            DaemonEvent::TerminalOutput { .. } => Some(AttachSequenceEvent::Live),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(hub, core);
}

#[test]
fn hub_mode_flags_fixture_matches_public_request_response_contract() {
    let scenario = botster_hub_test_support::mode_flags_conformance_scenario();

    assert_eq!(
        scenario.request,
        botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: scenario.mouse_on.mode_flags.session_id.clone(),
        }
    );
    assert_eq!(scenario.mouse_off.mode_flags.mouse_mode, 0);
    assert_eq!(scenario.mouse_on.mode_flags.mouse_mode, 9);
    assert_eq!(
        scenario.mouse_off.mode_flags.session_id,
        scenario.mouse_on.mode_flags.session_id
    );
    assert!(scenario.unknown_session.mode_flags.is_none());
    assert!(scenario.backend_failure.mode_flags.is_none());
    assert_eq!(
        scenario.unknown_session.response_kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        scenario.backend_failure.response_kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
}
