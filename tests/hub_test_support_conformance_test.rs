use botster_terminal_protocol::TerminalKind;

/// Kind order of one Hub late-attach fixture, as `TerminalKind` names.
fn fixture_kinds(frames: &[botster_hub_test_support::TerminalStreamFixtureFrame]) -> Vec<&str> {
    frames.iter().map(|frame| frame.kind.as_str()).collect()
}

#[test]
fn hub_late_attach_fixtures_follow_the_terminal_stream_attach_order() {
    // Protocol 9 delivers history on the terminal route: the attachment is
    // acknowledged first, MODES and SNAPSHOT_READY follow, live OUTPUT may
    // interleave before the SNAPSHOT_HISTORY pages, and SNAPSHOT_FINISH closes
    // the history before later OUTPUT and PROCESS_EXIT.
    let history = botster_hub_test_support::late_attach_history_frames();
    assert_eq!(
        fixture_kinds(&history),
        vec![
            TerminalKind::AttachState.name(),
            TerminalKind::Modes.name(),
            TerminalKind::SnapshotReady.name(),
            TerminalKind::Output.name(),
            TerminalKind::SnapshotHistory.name(),
            TerminalKind::SnapshotHistory.name(),
            TerminalKind::SnapshotFinish.name(),
            TerminalKind::Output.name(),
            TerminalKind::ProcessExit.name(),
        ]
    );
    assert!(
        history
            .iter()
            .all(|frame| frame.route == history[0].route
                && frame.generation == history[0].generation),
        "one late-attach fixture stays on one route and one generation"
    );

    let no_history = botster_hub_test_support::late_attach_no_history_frames();
    let kinds = fixture_kinds(&no_history);
    assert_eq!(
        kinds.first().copied(),
        Some(TerminalKind::AttachState.name())
    );
    assert_eq!(
        kinds.last().copied(),
        Some(TerminalKind::ProcessExit.name())
    );
    let finish = kinds
        .iter()
        .position(|kind| *kind == TerminalKind::SnapshotFinish.name())
        .expect("empty history still finishes the snapshot");
    let ready = kinds
        .iter()
        .position(|kind| *kind == TerminalKind::SnapshotReady.name())
        .expect("empty history still announces readiness");
    assert!(ready < finish, "SNAPSHOT_READY precedes SNAPSHOT_FINISH");
    assert!(
        kinds[finish + 1..]
            .iter()
            .all(|kind| *kind != TerminalKind::SnapshotHistory.name()),
        "no history page follows SNAPSHOT_FINISH"
    );
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
