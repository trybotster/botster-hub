fn hub_state_json(data_dir: &Path) -> serde_json::Value {
    let bytes = fs::read(data_dir.join("hub-state.json")).expect("read hub-state.json");
    serde_json::from_slice(&bytes).expect("hub-state.json is JSON")
}

fn session_frame_is_ended(frame: &botster_hub_client::DaemonEntityFrame, session_id: &str) -> bool {
    let fields = match frame {
        botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. } if id == session_id => entity,
        botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. } if id == session_id => patch,
        _ => return false,
    };
    fields.get("lifecycle_class").and_then(serde_json::Value::as_str) == Some("ended")
}

fn session_frame_is_restartable(frame: &botster_hub_client::DaemonEntityFrame, session_id: &str) -> bool {
    let fields = match frame {
        botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. } if id == session_id => entity,
        botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. } if id == session_id => patch,
        _ => return false,
    };
    session_frame_is_ended(frame, session_id)
        && fields.get("restartable").and_then(serde_json::Value::as_bool) == Some(true)
}

/// A session-type spawn records how to restart the session before its reply,
/// and removing the ended session deletes the record.
#[test]
fn a_session_type_spawn_records_its_restart_inputs_and_removal_deletes_them() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("restart-record");
    let package_root = unique_test_dir("restart-record-package");
    write_session_type_context_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable session type package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision);
    let mut sessions =
        botster_hub_client::subscribe_entities(&socket_endpoint(&data_dir), "session", "restart-record")
            .expect("subscribe to sessions");

    let session_id = "restart-record-session";
    let spawned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: session_id.to_string(),
            request: botster_hub::DaemonSessionTypeRequest {
                context: botster_hub::DaemonSessionTypeContextInput {
                    prompt: Some("pipeline prompt".to_string()),
                    ticket_id: Some("ticket-123".to_string()),
                    ..botster_hub::DaemonSessionTypeContextInput::default()
                },
                ..botster_hub::DaemonSessionTypeRequest::default()
            },
        },
    )
    .expect("spawn session type");
    assert_eq!(spawned.kind, botster_hub::DaemonResponseKind::Spawned, "{spawned:?}");

    // The record is durable before the Spawned reply.
    let state = hub_state_json(&data_dir);
    assert_eq!(state["schema_version"], 6);
    let record = &state["restart_records"][session_id];
    assert_eq!(record["session_type_id"], "init", "{state}");
    assert_eq!(record["context"]["prompt"], "pipeline prompt");
    assert_eq!(record["context"]["ticket_id"], "ticket-123");
    assert!(record.get("environment_keys").is_none(), "{record}");

    // The fixture script exits after a second; wait for the entity to end.
    // The entity turns restartable as it ends: the record is already durable.
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        session_frame_is_restartable(frame, session_id)
    });
    let removed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::RemoveSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("remove ended session");
    assert_eq!(removed.kind, botster_hub::DaemonResponseKind::SessionRemoved, "{removed:?}");

    let state = hub_state_json(&data_dir);
    assert!(
        state["restart_records"].get(session_id).is_none(),
        "removal deletes the restart record: {state}"
    );
    drop(sessions);
    shutdown_cli_daemon(&data_dir, child);
}

fn restart_refusal_code(config: &botster_hub::HubConfig, session_id: &str) -> String {
    let response = botster_hub::daemon_transport_request(
        config,
        botster_hub::DaemonRequest::RestartSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("restart request reaches the daemon");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::OperatorError,
        "{response:?}"
    );
    response.error.expect("a refusal carries its error").code
}

/// A restart is refused, with a typed code, for a session that is unknown,
/// still running, or has ended without a restart record (a plain Spawn).
#[test]
fn a_restart_of_an_unknown_running_or_unrecorded_session_is_refused_with_its_code() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("restart-refusals");
    let package_root = unique_test_dir("restart-refusals-package");
    write_session_type_context_package(&package_root);
    // A session that stays running until its terminal closes: `cat` blocks on
    // its terminal input, and the test ends it by shutting the daemon down.
    write_warm_executable(&package_root.join("bin/init.sh"), "#!/bin/sh\ncat\n");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable session type package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision);
    let mut sessions =
        botster_hub_client::subscribe_entities(&socket_endpoint(&data_dir), "session", "restart-refusals")
            .expect("subscribe to sessions");

    let running = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: "running-session".to_string(),
            request: botster_hub::DaemonSessionTypeRequest::default(),
        },
    )
    .expect("spawn a running session type session");
    assert_eq!(running.kind, botster_hub::DaemonResponseKind::Spawned, "{running:?}");
    let plain = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "plain-session".to_string(),
            command: "true".to_string(),
        },
    )
    .expect("spawn a plain session");
    assert_eq!(plain.kind, botster_hub::DaemonResponseKind::Spawned, "{plain:?}");
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        session_frame_is_ended(frame, "plain-session")
    });

    assert_eq!(restart_refusal_code(&config, "no-such-session"), "unknown_session");
    assert_eq!(restart_refusal_code(&config, "running-session"), "restart_not_ended");
    assert_eq!(restart_refusal_code(&config, "plain-session"), "restart_record_unavailable");
    drop(sessions);
    shutdown_cli_daemon(&data_dir, child);
}

fn session_frame_is_current(frame: &botster_hub_client::DaemonEntityFrame, session_id: &str) -> bool {
    let fields = match frame {
        botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. } if id == session_id => entity,
        botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. } if id == session_id => patch,
        _ => return false,
    };
    fields.get("lifecycle_class").and_then(serde_json::Value::as_str) == Some("current")
}

/// An ended session-type session restarts under the same id: its entity goes
/// from ended to current with no removal in between, its context equals the
/// original, and the restartable flag turns false again.
#[test]
fn a_restart_runs_the_same_session_id_again_with_its_context_and_no_removal() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("restart-same-id");
    let package_root = unique_test_dir("restart-same-id-package");
    write_session_type_context_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable session type package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision);
    let mut sessions =
        botster_hub_client::subscribe_entities(&socket_endpoint(&data_dir), "session", "restart-same-id")
            .expect("subscribe to sessions");

    let session_id = "restart-same-id-session";
    let spawned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: session_id.to_string(),
            request: botster_hub::DaemonSessionTypeRequest {
                context: botster_hub::DaemonSessionTypeContextInput {
                    prompt: Some("restart me".to_string()),
                    ..botster_hub::DaemonSessionTypeContextInput::default()
                },
                ..botster_hub::DaemonSessionTypeRequest::default()
            },
        },
    )
    .expect("spawn session type");
    assert_eq!(spawned.kind, botster_hub::DaemonResponseKind::Spawned, "{spawned:?}");
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        session_frame_is_restartable(frame, session_id)
    });

    let mut removed_seen = false;
    let restarted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::RestartSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("restart the ended session");
    assert_eq!(restarted.kind, botster_hub::DaemonResponseKind::Spawned, "{restarted:?}");
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        if matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Remove { id, .. } if id == session_id
        ) {
            removed_seen = true;
        }
        session_frame_is_current(frame, session_id)
    });
    assert!(!removed_seen, "a restart never removes the session entity");

    let context = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReadSessionContext {
            session_id: session_id.to_string(),
            context_id: None,
            key: Some("prompt".to_string()),
        },
    )
    .expect("read the restarted session context");
    assert_eq!(context.kind, botster_hub::DaemonResponseKind::SessionContext, "{context:?}");
    assert!(format!("{context:?}").contains("restart me"), "{context:?}");
    // The record is kept, so the session can be restarted again once it ends.
    let state = hub_state_json(&data_dir);
    assert!(state["restart_records"].get(session_id).is_some(), "{state}");
    drop(sessions);
    shutdown_cli_daemon(&data_dir, child);
}

/// A restart whose session type is gone is refused naming the type. The
/// session stays ended with its record, so the same restart succeeds once the
/// type is back.
#[test]
fn a_restart_of_a_session_whose_type_is_gone_is_refused_and_can_be_retried() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("restart-type-gone");
    let package_root = unique_test_dir("restart-type-gone-package");
    write_session_type_context_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let enable = |config: &botster_hub::HubConfig| {
        let enabled = botster_hub::daemon_transport_request(
            config,
            botster_hub::DaemonRequest::EnablePackageLocalPath {
                path: package_root.clone(),
            },
        )
        .expect("enable session type package");
        assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision);
    };
    enable(&config);
    let mut sessions =
        botster_hub_client::subscribe_entities(&socket_endpoint(&data_dir), "session", "restart-type-gone")
            .expect("subscribe to sessions");
    let session_id = "restart-type-gone-session";
    let spawned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: session_id.to_string(),
            request: botster_hub::DaemonSessionTypeRequest::default(),
        },
    )
    .expect("spawn session type");
    assert_eq!(spawned.kind, botster_hub::DaemonResponseKind::Spawned, "{spawned:?}");
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        session_frame_is_restartable(frame, session_id)
    });

    let disabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DisablePackage {
            package_name: "runtime.session-type".to_string(),
        },
    )
    .expect("disable the package");
    assert_ne!(disabled.kind, botster_hub::DaemonResponseKind::OperatorError, "{disabled:?}");
    assert_eq!(restart_refusal_code(&config, session_id), "session_type_unavailable");
    let state = hub_state_json(&data_dir);
    assert!(state["restart_records"].get(session_id).is_some(), "{state}");

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "runtime.session-type".to_string(),
        },
    )
    .expect("enable the package again");
    assert_ne!(enabled.kind, botster_hub::DaemonResponseKind::OperatorError, "{enabled:?}");
    let restarted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::RestartSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("retry the restart");
    assert_eq!(restarted.kind, botster_hub::DaemonResponseKind::Spawned, "{restarted:?}");
    drop(sessions);
    shutdown_cli_daemon(&data_dir, child);
}
