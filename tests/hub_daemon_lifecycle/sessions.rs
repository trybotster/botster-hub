fn terminal_envelope_contains_marker(
    envelope: &botster_hub_client::DaemonUnixTerminalEnvelope,
    marker: &str,
) -> bool {
    let Ok(bytes) = envelope.payload_bytes() else {
        return false;
    };
    if !marker.is_empty()
        && bytes
            .windows(marker.len())
            .any(|window| window == marker.as_bytes())
    {
        return true;
    }
    let Ok(event) = serde_json::from_slice::<botster_hub_client::DaemonEvent>(&bytes) else {
        return false;
    };
    match event {
        botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
            live_output_contains(&payload, marker)
        }
        _ => false,
    }
}

fn lifecycle_counters(
    endpoint: &botster_hub_client::DaemonEndpoint,
    label: &str,
) -> botster_hub_client::DaemonLifecycleCounters {
    botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status)
        .unwrap_or_else(|error| panic!("{label}: {error}"))
        .status
        .unwrap_or_else(|| panic!("{label} status body"))
        .lifecycle_counters
}

fn wait_for_idle_lifecycle_window(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> botster_hub_client::DaemonLifecycleCounters {
    // Status opens a new connection, so do not poll it. Sleep past one
    // interval plus one Maintenance rotation so spawn catch-up cannot
    // spill into the idle sample.
    thread::sleep(Duration::from_millis(1_200));
    lifecycle_counters(endpoint, "status before many-session idle window")
}

fn discard_unsolicited_terminal(connection: &mut botster_hub_client::DaemonConnection) {
    while let Ok(Some(_)) = connection.poll_terminal(Duration::from_millis(20)) {}
    let _ = connection.take_skipped_terminal();
}

fn wait_for_sibling_terminal_envelope(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    subscription_id: &str,
    marker: &str,
) -> botster_hub_client::DaemonUnixTerminalEnvelope {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        while let Ok(Some(envelope)) = connection.poll_terminal(Duration::from_millis(25)) {
            let matches_route =
                envelope.session_id == session_id && envelope.subscription_id == subscription_id;
            let contains = terminal_envelope_contains_marker(&envelope, marker);
            seen.push((
                envelope.session_id.clone(),
                envelope.subscription_id.clone(),
                contains,
            ));
            if matches_route && contains {
                return envelope;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "timed out waiting for sibling terminal envelope marker={marker:?} session={session_id} subscription={subscription_id}; seen={seen:?}"
    )
}

fn shutdown_failure_occupancy_has_pair(
    occupancy: &[botster_hub_client::DaemonAttachOccupancy],
    session_id: &str,
    subscription_id: &str,
) -> bool {
    occupancy.iter().any(|row| {
        row.session_id == session_id && row.subscription_id == subscription_id
    })
}

fn wait_for_read_screen_contains(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    needle: &str,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .expect("read screen");
        last = response
            .read_screen
            .as_ref()
            .map(|screen| screen.text.clone())
            .unwrap_or_default();
        if last.contains(needle) {
            return last;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for ReadScreen to contain {needle:?}; last={last:?}")
}

fn entity_exit_sequence(frame: &botster_hub_client::DaemonEntityFrame) -> u64 {
    match frame {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") =>
        {
            *snapshot_seq
        }
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq,
            id,
            entity,
            ..
        } if id == "entity-session"
            && entity.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") =>
        {
            *snapshot_seq
        }
        other => panic!("expected entity exit frame, got {other:?}"),
    }
}

#[test]
fn fast_exit_attach_diagnostic_records_subscription_event_order() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("fast-exit-attach-diagnostic");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let session_id = format!(
        "smoke-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    );
    let subscription_id = format!("{session_id}-subscription");
    let marker = "botster-smoke-terminal-ok";
    let expected = format!("smoke:{marker}");

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.clone(),
            command: format!("printf 'smoke:{marker}\\n'"),
        },
    )
    .expect("spawn immediate-output diagnostic session");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);

    // Mirrors stream_attach_connected in crates/botster-hub-client/src/lib.rs:123-172.
    // Any production boundary change there must update this diagnostic mirror.
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect diagnostic client");
    let mut response = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
        })
        .expect("attach diagnostic subscription");
    let started_at = Instant::now();
    let mut observed = String::new();
    let mut matching_observed = String::new();
    let mut mismatched_marker = false;
    let mut opaque_history_bytes = 0;
    let mut saw_process_exit = false;
    let mut ordered_observations = Vec::new();
    let mut response_index = 0;
    let mut request_kind = "attach";
    let mut idle_drains = 0;

    let boundary_reason = loop {
        let mut response_observations = Vec::new();
        let mut response_renderable_bytes = 0;
        for event in &response.events {
            let observation = match event {
                botster_hub_client::DaemonEvent::SessionLifecycle {
                    session_id: event_session_id,
                    state,
                } => format!("session_lifecycle:session={event_session_id}:state={state}"),
                botster_hub_client::DaemonEvent::TerminalOutput {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    payload,
                } => {
                    let data = live_output_utf8(payload);
                    response_renderable_bytes += data.len();
                    observed.push_str(&data);
                    if event_session_id == &session_id && event_subscription_id == &subscription_id
                    {
                        matching_observed.push_str(&data);
                    } else if data.contains(&expected) {
                        mismatched_marker = true;
                    }
                    format!(
                        "terminal_output:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        data.len()
                    )
                }
                botster_hub_client::DaemonEvent::Snapshot {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    history,
                } => {
                    opaque_history_bytes += history.bytes;
                    format!(
                        "snapshot:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        history.bytes
                    )
                }
                botster_hub_client::DaemonEvent::Scrollback {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    history,
                } => {
                    opaque_history_bytes += history.bytes;
                    format!(
                        "scrollback:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        history.bytes
                    )
                }
                botster_hub_client::DaemonEvent::ProcessExit {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    code,
                } => {
                    saw_process_exit = true;
                    format!(
                        "process_exit:session={event_session_id}:subscription={event_subscription_id}:code={code:?}"
                    )
                }
                botster_hub_client::DaemonEvent::AttachState {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    state,
                } => {
                    format!(
                        "attach_state:session={event_session_id}:subscription={event_subscription_id}:state={state}"
                    )
                }
                botster_hub_client::DaemonEvent::RuntimeObservation { kind } => {
                    format!("runtime_observation:{kind}")
                }
                botster_hub_client::DaemonEvent::WorktreeLifecycle { .. } => {
                    "worktree_lifecycle".to_string()
                }
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    generation,
                    reason,
                } => {
                    format!(
                        "terminal_subscription_closed:session={event_session_id}:subscription={event_subscription_id}:generation={generation}:reason={reason}"
                    )
                }
                botster_hub_client::DaemonEvent::PackageEvent {
                    subscription_id,
                    owner,
                    name,
                    ..
                } => format!(
                    "package_event:subscription={subscription_id}:owner={owner}:name={name}"
                ),
                botster_hub_client::DaemonEvent::EventGap {
                    subscription_id,
                    owner,
                    name,
                } => format!(
                    "event_gap:subscription={subscription_id}:owner={owner}:name={name}"
                ),
            };
            response_observations.push(observation.clone());
            ordered_observations.push(format!(
                "elapsed_us={}:response={response_index}:request={request_kind}:event={}:{}",
                started_at.elapsed().as_micros(),
                response_observations.len() - 1,
                observation
            ));
        }
        println!(
            "fast_exit_attach_diagnostic elapsed_us={} response={response_index} request={request_kind} events=[{}] renderable_bytes={response_renderable_bytes} cumulative_renderable_bytes={}",
            started_at.elapsed().as_micros(),
            response_observations.join(","),
            observed.len()
        );

        if saw_process_exit {
            break "process_exit";
        }
        if request_kind == "adapter" {
            if response.events.is_empty() {
                idle_drains += 1;
            } else {
                idle_drains = 0;
            }
            if idle_drains >= 20 {
                break "idle_quiescence";
            }
        }

        thread::sleep(Duration::from_millis(25));
        response.events = poll_adapter_events(&mut connection, &session_id, Some(&subscription_id));
        response.kind = botster_hub_client::DaemonResponseKind::Events;
        response_index += 1;
        request_kind = "adapter";
    };

    let renderable_bytes_at_boundary = observed.len();
    let matching_bytes_at_boundary = matching_observed.len();
    let marker_at_boundary = observed.contains(&expected);
    println!(
        "fast_exit_attach_diagnostic boundary elapsed_us={} reason={boundary_reason} response={response_index} input_bytes=0 renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} marker_at_boundary={marker_at_boundary} idle_drains={idle_drains}",
        started_at.elapsed().as_micros()
    );

    let mut tail_matching_observed = String::new();
    let tail_error: Option<String> = None;
    if !marker_at_boundary {
        let mut tail_idle_drains = 0;
        while tail_idle_drains < 20 {
            thread::sleep(Duration::from_millis(25));
            let tail_events =
                poll_adapter_events(&mut connection, &session_id, Some(&subscription_id));
            response_index += 1;
            if tail_events.is_empty() {
                tail_idle_drains += 1;
            } else {
                tail_idle_drains = 0;
            }
            for (event_index, event) in tail_events.iter().enumerate() {
                let elapsed_us = started_at.elapsed().as_micros();
                match event {
                    botster_hub_client::DaemonEvent::TerminalOutput {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        payload,
                    } => {
                        let data = live_output_utf8(payload);
                        if event_session_id == &session_id
                            && event_subscription_id == &subscription_id
                        {
                            tail_matching_observed.push_str(&data);
                        } else if data.contains(&expected) {
                            mismatched_marker = true;
                        }
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=terminal_output session={event_session_id} subscription={event_subscription_id} bytes={}",
                            data.len()
                        );
                    }
                    botster_hub_client::DaemonEvent::Snapshot {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        history,
                    } => {
                        opaque_history_bytes += history.bytes;
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=snapshot session={event_session_id} subscription={event_subscription_id} bytes={}",
                            history.bytes
                        );
                    }
                    botster_hub_client::DaemonEvent::Scrollback {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        history,
                    } => {
                        opaque_history_bytes += history.bytes;
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=scrollback session={event_session_id} subscription={event_subscription_id} bytes={}",
                            history.bytes
                        );
                    }
                    botster_hub_client::DaemonEvent::ProcessExit {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        code,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=process_exit session={event_session_id} subscription={event_subscription_id} code={code:?} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::AttachState {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        state,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=attach_state session={event_session_id} subscription={event_subscription_id} state={state} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::SessionLifecycle {
                        session_id: event_session_id,
                        state,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=session_lifecycle session={event_session_id} subscription=none state={state} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::RuntimeObservation { kind } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=runtime_observation session=none subscription=none kind={kind} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::WorktreeLifecycle { .. } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=worktree_lifecycle session=none subscription=none bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        generation,
                        reason,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=terminal_subscription_closed session={event_session_id} subscription={event_subscription_id} generation={generation} reason={reason} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::PackageEvent { .. } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=package_event session=none subscription=none bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::EventGap { .. } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=event_gap session=none subscription=none bytes=0"
                    ),
                }
            }
        }
    }

    let (read_screen_bytes, read_screen_marker, read_screen_error) = if marker_at_boundary {
        (0, false, None)
    } else {
        match connection.request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.clone(),
        }) {
            Ok(response) => match response.read_screen {
                Some(screen) => (screen.text.len(), screen.text.contains(&expected), None),
                None => (0, false, Some("missing_read_screen_body".to_string())),
            },
            Err(error) => (0, false, Some(error.to_string())),
        }
    };

    let status = connection.request(&botster_hub_client::DaemonRequest::Status);
    let daemon_lifecycle = status
        .as_ref()
        .ok()
        .and_then(|response| response.status.as_ref())
        .map(|status| status.lifecycle_state.as_str())
        .unwrap_or("missing");

    let sessions = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after frozen production boundary");
    let session_lifecycle = sessions
        .sessions
        .iter()
        .find(|session| session.session_id == session_id)
        .map(|session| session.lifecycle.as_str())
        .unwrap_or("missing");
    let tail_matching_marker = tail_matching_observed.contains(&expected);
    println!(
        "fast_exit_attach_diagnostic state elapsed_us={} daemon_lifecycle={daemon_lifecycle} session_lifecycle={session_lifecycle} process_exit={saw_process_exit} renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} tail_matching_bytes={} tail_matching_marker={tail_matching_marker} mismatched_marker={mismatched_marker} opaque_history_bytes={opaque_history_bytes} read_screen_bytes={read_screen_bytes} read_screen_marker={read_screen_marker} read_screen_error={read_screen_error:?} tail_error={tail_error:?} event_order=[{}]",
        started_at.elapsed().as_micros(),
        tail_matching_observed.len(),
        ordered_observations.join(",")
    );

    let detach = connection.request(&botster_hub_client::DaemonRequest::Detach {
        session_id: session_id.clone(),
        subscription_id: subscription_id.clone(),
    });
    println!(
        "fast_exit_attach_diagnostic cleanup elapsed_us={} detach_response={detach:?}",
        started_at.elapsed().as_micros()
    );
    let _ = connection.request(&botster_hub_client::DaemonRequest::ShutdownSession {
        session_id: session_id.clone(),
    });

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub shutdown after fast-exit diagnostic");
    let daemon = child
        .wait_with_output()
        .expect("wait for diagnostic daemon child");
    let shutdown_validation = validate_cli_daemon_shutdown(&shutdown, &daemon);

    if !marker_at_boundary {
        let classification = if mismatched_marker && tail_matching_marker {
            "ambiguous_subscription_mismatch_and_output_queued_after_harness_stop"
        } else if mismatched_marker {
            "subscription_mismatch"
        } else if tail_matching_marker {
            "output_queued_after_harness_stop"
        } else if read_screen_marker {
            "output_produced_not_routed"
        } else if read_screen_error.is_some() {
            "retained_history_or_readback_failure"
        } else if tail_error.is_some() {
            "unclassified_diagnostic_transport_failure"
        } else {
            "output_never_produced"
        };
        println!(
            "fast_exit_attach_failure classification={classification} boundary_reason={boundary_reason} input_bytes=0 renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} opaque_history_bytes={opaque_history_bytes} daemon_exit_status={} shutdown_status={} daemon_stdout_begin\n{}\ndaemon_stdout_end daemon_stderr_begin\n{}\ndaemon_stderr_end test_stdout_stderr=run_log",
            daemon.status,
            shutdown.status,
            String::from_utf8_lossy(&daemon.stdout),
            String::from_utf8_lossy(&daemon.stderr)
        );
    }

    assert!(
        shutdown_validation.is_ok(),
        "diagnostic daemon cleanup failed: {shutdown_validation:?}"
    );
    assert!(
        marker_at_boundary || read_screen_marker,
        "fast-exit marker missing from adapter TerminalOutput and ReadScreen; renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} tail_matching_marker={tail_matching_marker} mismatched_marker={mismatched_marker} read_screen_marker={read_screen_marker} read_screen_error={read_screen_error:?} tail_error={tail_error:?}"
    );
}

#[test]
fn cli_sessions_spawn_and_list_route_through_client_api() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-sessions");
    let child = start_cli_daemon(&data_dir);
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("runtime-session")
        .arg("--")
        .arg("printf 'runtime-ok\\n'; IFS= read -r line; printf 'runtime:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");

    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );
    let stdout = String::from_utf8(spawn.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=spawned"));
    assert!(stdout.contains("session_id=runtime-session"));
    assert!(stdout.contains("lifecycle=running"));
    assert!(stdout.contains("event_count=0"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub sessions list");

    assert!(
        list.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=sessions"));
    assert!(stdout.contains("session_count=1"));
    assert!(stdout.contains("session id=runtime-session lifecycle=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let attach = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::Attach {
            session_id: "runtime-session".to_string(),
            subscription_id: "botster-hub-cli-subscription".to_string(),
        },
    )
    .expect("attach before explicit detach");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    let detach = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("detach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub sessions detach");
    assert!(
        detach.status.success(),
        "detach failed: {}",
        String::from_utf8_lossy(&detach.stderr)
    );

    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect after detach");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "runtime-session".to_string(),
            subscription_id: "botster-hub-cli-subscription".to_string(),
        })
        .expect("reattach after detach");
    let screen_before =
        wait_for_read_screen_contains(&mut connection, "runtime-session", "runtime-ok");
    assert!(
        screen_before.contains("runtime-ok"),
        "visible text after reattach is on ReadScreen: {screen_before:?}"
    );

    connection
        .send_terminal_frame(
            "runtime-session",
            "botster-hub-cli-subscription",
            &terminal_resize_frame_bytes(30, 100),
        )
        .expect("resize through bound duplex route");
    connection
        .send_terminal_frame(
            "runtime-session",
            "botster-hub-cli-subscription",
            &terminal_input_frame_bytes(b"from-cli\r"),
        )
        .expect("send input through bound duplex route");

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_short_lived_session_shutdown_returns_structured_cleanup() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-short-lived-shutdown");
    let child = start_cli_daemon(&data_dir);

    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("runtime-session")
        .arg("--")
        .arg("printf 'runtime-ok\\n'; IFS= read -r line; printf 'runtime:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");
    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );

    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect short-lived");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "runtime-session".to_string(),
            subscription_id: "botster-hub-cli-subscription".to_string(),
        })
        .expect("attach short-lived");
    let screen_before =
        wait_for_read_screen_contains(&mut connection, "runtime-session", "runtime-ok");
    assert!(
        screen_before.contains("runtime-ok"),
        "short-lived visible text is on ReadScreen: {screen_before:?}"
    );

    connection
        .send_terminal_frame(
            "runtime-session",
            "botster-hub-cli-subscription",
            &terminal_input_frame_bytes(b"done\r"),
        )
        .expect("send input through bound duplex route");

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub sessions shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let stdout = String::from_utf8(shutdown.stdout).expect("shutdown stdout is utf8");
    let stderr = String::from_utf8(shutdown.stderr).expect("shutdown stderr is utf8");
    assert!(
        stdout.contains("response=session_cleanup") || stdout.contains("response=events"),
        "shutdown output: stdout={stdout} stderr={stderr}"
    );
    if stdout.contains("response=session_cleanup") {
        assert!(stdout.contains("session_id=runtime-session"));
        assert!(stdout.contains("outcome=already_exited"));
    }
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-operator-error");
    let child = start_cli_daemon(&data_dir);

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("missing-session")
        .output()
        .expect("run botster-hub sessions shutdown");
    assert!(
        !send.status.success(),
        "missing-session shutdown should fail with operator frame"
    );
    let stdout = String::from_utf8(send.stdout).expect("send stdout is utf8");
    let stderr = String::from_utf8(send.stderr).expect("send stderr is utf8");
    assert!(stdout.contains("response=operator_error"));
    assert!(stdout.contains("error_code=unknown_session"));
    assert!(stdout.contains("operation=shutdown"));
    assert!(stderr.contains("operator error: unknown_session"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after operator error");
    assert!(
        status.status.success(),
        "status failed after operator error: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("status stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_read_mode_flags_drives_real_daemon_socket_protocol() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-client");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    assert_eq!(
        status
            .status
            .as_ref()
            .expect("status response body")
            .lifecycle_state,
        "running"
    );

    let list =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("external client list sessions request");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "external-client-session".to_string(),
            command:
                "printf '\\033[?1000h\\033[?1006h'; printf 'external-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("external client spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(
        spawn
            .sessions
            .iter()
            .any(|session| session.session_id == "external-client-session"
                && session.lifecycle == "running")
    );

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external attach request");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    connection
        .send_terminal_frame(
            "external-client-session",
            "external-client-subscription",
            &terminal_resize_frame_bytes(31, 101),
        )
        .expect("external resize frame");
    connection
        .send_terminal_frame(
            "external-client-session",
            "external-client-subscription",
            &terminal_input_frame_bytes(b"external-input\n"),
        )
        .expect("external input frame");

    let observed = wait_for_read_screen_contains(
        &mut connection,
        "external-client-session",
        "echo:external-input",
    );
    assert!(
        observed.contains("echo:external-input"),
        "external client visible text is on ReadScreen, got {observed:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mode_flags = loop {
        let response = connection
            .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
                session_id: "external-client-session".to_string(),
            })
            .expect("external read_mode_flags request");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::ReadModeFlags
        );
        let mode_flags = response.mode_flags.expect("read_mode_flags response body");
        if mode_flags.mouse_mode == 9 {
            break mode_flags;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for exact combined mouse mode, last value {}",
            mode_flags.mouse_mode
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(mode_flags.session_id, "external-client-session");
    assert_eq!(mode_flags.mouse_mode, 9);
    assert_ne!(
        mode_flags.mode_generation, 0,
        "freshness generation must be non-zero on a live worker"
    );
    assert!(
        mode_flags.mode_revision >= 1,
        "freshness revision should advance with mode changes, got {}",
        mode_flags.mode_revision
    );

    let detach = connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external detach request");
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);

    let missing_read_screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing read_screen returns operator response");
    assert_eq!(
        missing_read_screen.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = missing_read_screen.error.expect("read_screen error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "read_screen");

    let status_after_read_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after read_screen error");
    assert_eq!(
        status_after_read_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    let missing_mode_flags = connection
        .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing read_mode_flags returns operator response");
    assert_eq!(
        missing_mode_flags.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert!(
        missing_mode_flags.mode_flags.is_none(),
        "unknown session must not fabricate a successful mouse-off body"
    );
    let error = missing_mode_flags
        .error
        .expect("read_mode_flags error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "read_mode_flags");

    let status_after_mode_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after read_mode_flags error");
    assert_eq!(
        status_after_mode_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    let missing_snapshot = connection
        .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing capture_snapshot returns operator response");
    assert_eq!(
        missing_snapshot.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = missing_snapshot
        .error
        .expect("capture_snapshot error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "capture_snapshot");

    let status_after_snapshot_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after capture_snapshot error");
    assert_eq!(
        status_after_snapshot_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    drop(connection);

    let reconnect =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external reconnect");
    drop(reconnect);

    let shutdown_session = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "external-client-session".to_string(),
        },
    )
    .expect("external shutdown session request");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-ghostsnp-order");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "ghostsnp-order-session".to_string(),
            command: "printf 'retained-before-attach\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "ghostsnp-order-session");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "ghostsnp-order-session".to_string(),
            subscription_id: "ghostsnp-order-sub".to_string(),
        })
        .expect("attach");
    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );
    let observed = wait_for_read_screen_contains(
        &mut connection,
        "ghostsnp-order-session",
        "retained-before-attach",
    );
    assert!(
        observed.contains("retained-before-attach"),
        "retained text is on ReadScreen: {observed:?}"
    );
    connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: "ghostsnp-order-session".to_string(),
            subscription_id: "ghostsnp-order-sub".to_string(),
        })
        .expect("detach");
    let reattach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "ghostsnp-order-session".to_string(),
            subscription_id: "ghostsnp-order-resub".to_string(),
        })
        .expect("reattach");
    connection
        .send_terminal_frame(
            "ghostsnp-order-session",
            "ghostsnp-order-resub",
            &terminal_input_frame_bytes(b"live-after-snapshot\n"),
        )
        .expect("live input");

    assert!(
        reattach.events.is_empty(),
        "reattach must not return terminal bodies: {:?}",
        reattach.events
    );
    let events = collect_attach_events(
        &mut connection,
        "ghostsnp-order-session",
        "ghostsnp-order-resub",
        Some("echo:live-after-snapshot"),
    );
    let screen = wait_for_read_screen_contains(
        &mut connection,
        "ghostsnp-order-session",
        "echo:live-after-snapshot",
    );
    assert!(
        screen.contains("echo:live-after-snapshot"),
        "live text is on ReadScreen: {screen:?}"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::Scrollback {
                history,
                ..
            } if history
                .decoded_bytes()
                .map(|bytes| bytes.starts_with(GHOSTSNP_MAGIC))
                .unwrap_or(false)
        )),
        "host path must never translate Scrollback-as-GHOSTSNP: {events:?}"
    );

    // Control path CaptureSnapshot is metadata-only — never GHOSTSNP bytes.
    let capture = connection
        .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: "ghostsnp-order-session".to_string(),
        })
        .expect("capture snapshot metadata");
    assert_eq!(
        capture.kind,
        botster_hub_client::DaemonResponseKind::CaptureSnapshot
    );
    let meta = capture.capture_snapshot.as_ref().expect("capture body");
    assert!(meta.payload_bytes > 0);
    assert_eq!(
        meta.payload_format.as_deref(),
        Some("ghostty-terminal-snapshot-v1")
    );
    let capture_json = serde_json::to_value(&capture).expect("serialize capture");
    assert!(
        capture_json.get("payload_base64").is_none(),
        "control CaptureSnapshot must not expose payload_base64: {capture_json}"
    );

    production_shutdown_and_remove_session(&endpoint, "ghostsnp-order-session");
    session_cleanup.disarm();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_live_output_preserves_exact_bytes() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0x5b, 0x31, 0x6d, 0xff, 0xc0];
    let hub = start_isolated_live_output_hub("live-exact-bytes");
    let endpoint = hub.endpoint().clone();
    let release_path = unique_short_test_dir("live-exact-bytes-release").join("go");
    let exit_release_path = unique_short_test_dir("live-exact-bytes-exit").join("go");
    let script_path =
        write_python_held_live_script(&release_path, &exit_release_path, expected);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "exact-bytes-session".to_string(),
            command: python_script_command(&script_path),
        })
        .expect("spawn write(2) producer");
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), "exact-bytes-session");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "exact-bytes-session".to_string(),
            subscription_id: "exact-bytes-sub".to_string(),
        })
        .expect("attach");
    fs::create_dir_all(release_path.parent().expect("release parent")).expect("create release dir");
    fs::write(&release_path, b"go").expect("release write(2) producer");

    let events = wait_until_adapter_event(
        &mut connection,
        "exact-bytes-session",
        |event| match event {
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
                live_output_decoded_bytes(payload)
                    .windows(expected.len())
                    .any(|window| window == expected)
            }
            _ => false,
        },
    );
    let mut concatenated = Vec::new();
    for event in &events {
        if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event {
            let bytes = live_output_decoded_bytes(payload);
            assert!(
                !payload_has_utf8_replacement(&bytes),
                "live payload must not contain U+FFFD: {bytes:?}"
            );
            concatenated.extend(bytes);
        }
    }
    assert!(
        concatenated
            .windows(expected.len())
            .any(|window| window == expected),
        "concatenated live bytes must preserve the write(2) sequence, got {concatenated:?}"
    );

    fs::create_dir_all(exit_release_path.parent().expect("exit release parent"))
        .expect("create exit release dir");
    fs::write(&exit_release_path, b"go").expect("release exact-byte producer exit");
    wait_for_authoritative_session_exit(&endpoint, "exact-bytes-session");

    production_cleanup_after_authoritative_exit(
        &endpoint,
        "exact-bytes-session",
        "unix exact-bytes after observed exit",
    );
    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn external_hub_live_output_preserves_split_utf8_frames() {
    let _guard = daemon_test_guard();
    let first = [0xE2];
    let second = [0x82, 0xAC];
    let hub = start_isolated_live_output_hub("live-split-utf8");
    let endpoint = hub.endpoint().clone();
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");
    let first_release = unique_short_test_dir("live-split-first").join("go");
    let second_release = unique_short_test_dir("live-split-second").join("go");
    let exit_release = unique_short_test_dir("live-split-exit").join("go");
    let script_path =
        write_python_split_utf8_script(&first_release, &second_release, &exit_release);
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "split-utf8-session".to_string(),
            command: python_script_command(&script_path),
        })
        .expect("spawn split UTF-8 producer");
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), "split-utf8-session");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "split-utf8-session".to_string(),
            subscription_id: "split-utf8-sub".to_string(),
        })
        .expect("attach");
    fs::create_dir_all(first_release.parent().expect("first release parent"))
        .expect("create first release dir");
    fs::write(&first_release, b"go").expect("release first fragment");

    let first_events = wait_until_adapter_event(&mut connection, "split-utf8-session", |event| {
        event_is_exact_live_payload(event, "split-utf8-sub", &first)
    });
    let first_index = first_events
        .iter()
        .position(|event| event_is_exact_live_payload(event, "split-utf8-sub", &first))
        .expect("first fragment payload");
    assert!(
        first_events[first_index + 1..].iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput {
                    subscription_id,
                    ..
                } if subscription_id == "split-utf8-sub"
            )
        }),
        "second live frame arrived before producer release: {first_events:?}"
    );
    for _ in 0..5 {
        let extra = connection
            .request(&botster_hub_client::DaemonRequest::Status)
            .expect("drain before release");
        assert!(
            extra.events.iter().all(|event| {
                !matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalOutput {
                        subscription_id,
                        ..
                    } if subscription_id == "split-utf8-sub"
                )
            }),
            "second live frame arrived before producer release: {extra:?}"
        );
    }

    fs::create_dir_all(second_release.parent().expect("second release parent"))
        .expect("create second release dir");
    fs::write(&second_release, b"go").expect("release second fragment");

    let second_events = wait_until_adapter_event(&mut connection, "split-utf8-session", |event| {
        event_is_exact_live_payload(event, "split-utf8-sub", &second)
    });
    let second_index = second_events
        .iter()
        .position(|event| event_is_exact_live_payload(event, "split-utf8-sub", &second))
        .expect("second fragment payload");
    let first_payload = live_output_decoded_bytes(match &first_events[first_index] {
        botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => payload,
        other => panic!("expected first live payload, got {other:?}"),
    });
    let second_payload = live_output_decoded_bytes(match &second_events[second_index] {
        botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => payload,
        other => panic!("expected second live payload, got {other:?}"),
    });
    assert_eq!(first_payload, first);
    assert_eq!(second_payload, second);
    let mut concatenated = first_payload;
    concatenated.extend(second_payload);
    assert_eq!(concatenated, [0xE2, 0x82, 0xAC]);
    assert!(!payload_has_utf8_replacement(&concatenated));

    fs::create_dir_all(exit_release.parent().expect("exit release parent"))
        .expect("create exit release dir");
    fs::write(&exit_release, b"go").expect("release split UTF-8 producer exit");
    wait_for_authoritative_session_exit(&endpoint, "split-utf8-session");

    production_cleanup_after_authoritative_exit(
        &endpoint,
        "split-utf8-session",
        "unix split-utf8 after observed exit",
    );
    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes() {
    let _guard = daemon_test_guard();
    let expected = b"live-payload-bytes";
    let hub = start_isolated_live_output_hub("live-order-bytes");
    let endpoint = hub.endpoint().clone();
    let release_path = unique_short_test_dir("live-order-release").join("go");
    let exit_release_path = unique_short_test_dir("live-order-exit").join("go");
    let script_path =
        write_python_held_live_script(&release_path, &exit_release_path, expected);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "order-bytes-session".to_string(),
            command: python_script_command(&script_path),
        })
        .expect("spawn live producer");
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), "order-bytes-session");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "order-bytes-session".to_string(),
            subscription_id: "order-bytes-sub".to_string(),
        })
        .expect("attach");
    fs::create_dir_all(release_path.parent().expect("release parent")).expect("create release dir");
    fs::write(&release_path, b"go").expect("release live producer");

    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );
    let events = wait_until_adapter_event(&mut connection, "order-bytes-session", |event| {
        event_is_exact_live_payload(event, "order-bytes-sub", expected)
            || matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                    if live_output_decoded_bytes(payload)
                        .windows(expected.len())
                        .any(|window| window == expected)
            )
    });
    assert!(
        events.iter().any(|event| {
            event_is_exact_live_payload(event, "order-bytes-sub", expected)
                || matches!(
                    event,
                    botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                        if live_output_decoded_bytes(payload)
                            .windows(expected.len())
                            .any(|window| window == expected)
                )
        }),
        "live bytes must arrive on the adapter plane: {events:?}"
    );
    for event in &events {
        if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event {
            let value = serde_json::to_value(event).expect("serialize live event");
            assert!(
                value.get("data").is_none(),
                "live event must not have data: {value}"
            );
            assert_eq!(value["payload_encoding"], "base64");
            assert_eq!(value["bytes"], payload.bytes);
            assert!(value.get("payload_base64").is_some());
        }
    }

    fs::create_dir_all(exit_release_path.parent().expect("exit release parent"))
        .expect("create exit release dir");
    fs::write(&exit_release_path, b"go").expect("release ordered-byte producer exit");
    wait_for_authoritative_session_exit(&endpoint, "order-bytes-session");

    production_cleanup_after_authoritative_exit(
        &endpoint,
        "order-bytes-session",
        "unix order-bytes after observed exit",
    );
    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

fn observe_exact_live_byte_window(
    connection: &mut botster_hub_client::DaemonConnection,
    session_id: &str,
    expected: &[u8],
) {
    let events = wait_until_adapter_event(connection, session_id, |event| match event {
        botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
            live_output_decoded_bytes(payload)
                .windows(expected.len())
                .any(|window| window == expected)
        }
        _ => false,
    });
    let mut concatenated = Vec::new();
    for event in &events {
        if let botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } = event {
            let bytes = live_output_decoded_bytes(payload);
            assert!(
                !payload_has_utf8_replacement(&bytes),
                "live payload must not contain U+FFFD: {bytes:?}"
            );
            concatenated.extend(bytes);
        }
    }
    assert!(
        concatenated
            .windows(expected.len())
            .any(|window| window == expected),
        "concatenated live bytes must preserve the write(2) sequence, got {concatenated:?}"
    );
}

#[test]
fn external_hub_finite_producer_completion_uses_production_lifecycle_signal() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0x5b, 0x31, 0x6d, 0xff, 0xc0];
    let hub = start_isolated_live_output_hub("finite-producer-exit");
    let endpoint = hub.endpoint().clone();
    let release_path = unique_short_test_dir("finite-producer-release").join("go");
    let exit_release_path = unique_short_test_dir("finite-producer-exit-release").join("go");
    let script_path =
        write_python_held_live_script(&release_path, &exit_release_path, expected);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "finite-producer-exit".to_string(),
            command: python_script_command(&script_path),
        })
        .expect("spawn finite producer");
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), "finite-producer-exit");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "finite-producer-exit".to_string(),
            subscription_id: "finite-producer-exit-sub".to_string(),
        })
        .expect("attach");
    wait_for_producer_ready(&endpoint, "finite-producer-exit");
    fs::create_dir_all(release_path.parent().expect("release parent")).expect("create release dir");
    fs::write(&release_path, b"go").expect("release finite producer");

    observe_exact_live_byte_window(&mut connection, "finite-producer-exit", expected);
    fs::create_dir_all(exit_release_path.parent().expect("exit release parent"))
        .expect("create exit release dir");
    fs::write(&exit_release_path, b"go").expect("release finite producer exit");
    wait_for_authoritative_session_exit(&endpoint, "finite-producer-exit");
    production_cleanup_after_authoritative_exit(
        &endpoint,
        "finite-producer-exit",
        "finite producer completion",
    );
    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn external_hub_held_live_producer_defers_completion_until_exit_release() {
    let _guard = daemon_test_guard();
    let expected: &[u8] = &[0x00, 0x1b, 0x5b, 0x31, 0x6d, 0xff, 0xc0];
    let hub = start_isolated_live_output_hub("held-live-producer");
    let endpoint = hub.endpoint().clone();
    let release_path = unique_short_test_dir("held-live-release").join("go");
    let exit_release_path = unique_short_test_dir("held-live-exit").join("go");
    let script_path =
        write_python_held_live_script(&release_path, &exit_release_path, expected);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "held-live-producer".to_string(),
            command: python_script_command(&script_path),
        })
        .expect("spawn held-live producer");
    let mut session_cleanup = SessionCleanupGuard::new(hub.data_dir(), "held-live-producer");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "held-live-producer".to_string(),
            subscription_id: "held-live-producer-sub".to_string(),
        })
        .expect("attach");
    wait_for_producer_ready(&endpoint, "held-live-producer");
    fs::create_dir_all(release_path.parent().expect("release parent")).expect("create release dir");
    fs::write(&release_path, b"go").expect("release held-live bytes");

    observe_exact_live_byte_window(&mut connection, "held-live-producer", expected);
    assert_session_stays_running_across_observe_turns(
        &endpoint,
        "held-live-producer",
        HELD_LIVE_OBSERVE_TURNS,
    );
    fs::create_dir_all(exit_release_path.parent().expect("exit release parent"))
        .expect("create exit release dir");
    fs::write(&exit_release_path, b"go").expect("release held-live exit");

    production_cleanup_after_authoritative_exit(
        &endpoint,
        "held-live-producer",
        "held-live producer completion",
    );
    session_cleanup.disarm();
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn external_hub_attach_response_owns_fresh_subscription_snapshot() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_live_output_hub("attach-route");
    let endpoint = hub.endpoint().clone();
    let session_id = "attach-response-route-session";
    let subscription_a = "attach-response-route-a";
    let subscription_b = "attach-response-route-b";
    let screen_marker = "alternate-screen-owned-by-b";
    let output_marker = "pending-output-owned-by-a";
    let mut connection_a =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect A");
    let mut connection_b =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect B");

    connection_a
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "printf '\\033[?1049h\\033[2J\\033[H{screen_marker}'; while IFS= read -r line; do printf '%s' \"$line\"; done"
            ),
        })
        .expect("spawn alternate-screen producer");
    connection_a
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_a.to_string(),
        })
        .expect("attach A");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let screen = connection_a
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: session_id.to_string(),
            })
            .expect("read painted alternate screen");
        if screen
            .read_screen
            .as_ref()
            .is_some_and(|screen| screen.text.contains(screen_marker))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the child to paint the alternate screen"
        );
        thread::sleep(Duration::from_millis(20));
    }

    connection_a
        .send_terminal_frame(
            session_id,
            subscription_a,
            &terminal_input_frame_bytes(format!("{output_marker}\n").as_bytes()),
        )
        .expect("queue output for A before B attaches");
    let attach_b = connection_b
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_b.to_string(),
        })
        .expect("attach B");

    assert!(
        attach_b.events.is_empty(),
        "B attach must not return terminal bodies: {:?}",
        attach_b.events
    );
    let screen_b = wait_for_read_screen_contains(&mut connection_b, session_id, screen_marker);
    assert!(
        screen_b.contains(screen_marker),
        "B sees the painted alternate screen on ReadScreen: {screen_b:?}"
    );

    let a_events = wait_until_adapter_event(&mut connection_a, session_id, |event| {
        matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput {
                subscription_id,
                payload,
                ..
            } if subscription_id == subscription_a && live_output_contains(payload, output_marker)
        )
    });
    assert!(
        a_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::Snapshot {
                    subscription_id,
                    ..
                } if subscription_id == subscription_b
            )
        }),
        "A must not receive B's Snapshot: {a_events:?}"
    );
    assert!(
        a_events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput {
                    subscription_id,
                    payload,
                    ..
                } if subscription_id == subscription_a && live_output_contains(payload, output_marker)
            )
        }),
        "A must receive its own TerminalOutput with the queued marker: {a_events:?}"
    );

    shutdown_short_lived_session(&endpoint, session_id);
    hub.shutdown().expect("shutdown isolated hub");
}

/// Fresh idle Ghostty attach: production emits GHOSTSNP Snapshot before Attached.
///
/// Aligns shared no_history_then_live fixtures with the verified producer
/// (blank GHOSTSNP present; ReadScreen empty; no Scrollback-as-GHOSTSNP).
#[test]
fn external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-idle-ghostsnp");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    // Idle session: no prior renderable output before attach.
    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "idle-ghostsnp-session".to_string(),
            command: "sleep 30".to_string(),
        })
        .expect("spawn idle session");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "idle-ghostsnp-session");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "idle-ghostsnp-session".to_string(),
            subscription_id: "idle-ghostsnp-sub".to_string(),
        })
        .expect("attach idle session");

    assert!(
        attach.events.is_empty(),
        "idle Attach must not return terminal bodies: {:?}",
        attach.events
    );
    let events = collect_attach_events(
        &mut connection,
        "idle-ghostsnp-session",
        "idle-ghostsnp-sub",
        None,
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::Scrollback {
                history,
                ..
            } if history
                .decoded_bytes()
                .map(|bytes| bytes.starts_with(GHOSTSNP_MAGIC))
                .unwrap_or(false)
        )),
        "idle host path must never translate Scrollback-as-GHOSTSNP: {events:?}"
    );

    let screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "idle-ghostsnp-session".to_string(),
        })
        .expect("ReadScreen on idle session");
    assert_eq!(
        screen.kind,
        botster_hub_client::DaemonResponseKind::ReadScreen
    );
    let text = screen.read_screen.expect("read_screen body").text;
    let non_ws: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        non_ws.is_empty(),
        "fresh idle ReadScreen must be blank; got {:?}",
        text.chars().take(120).collect::<String>()
    );

    let capture = connection
        .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: "idle-ghostsnp-session".to_string(),
        })
        .expect("idle CaptureSnapshot metadata");
    assert_eq!(
        capture.kind,
        botster_hub_client::DaemonResponseKind::CaptureSnapshot
    );
    let meta = capture.capture_snapshot.as_ref().expect("capture body");
    assert!(meta.payload_bytes > 0);
    assert_eq!(
        meta.payload_format.as_deref(),
        Some("ghostty-terminal-snapshot-v1")
    );

    production_shutdown_and_remove_session(&endpoint, "idle-ghostsnp-session");
    session_cleanup.disarm();
    assert!(
        !session_ids_from_list(&endpoint).contains(&"idle-ghostsnp-session".to_string()),
        "production cleanup must remove idle-ghostsnp-session"
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_mode_gated_kitty_stale_token_rejects_and_reprobe_admits() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-mode-gated-kitty");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "mode-gated-kitty".to_string(),
            command: concat!(
                "printf ready; while IFS= read -r line; do ",
                "printf \"echo:%s\\n\" \"$line\"; ",
                "if [ \"$line\" = enable-modes ]; then ",
                "printf '\\033[?1000h\\033[?1006h\\033[=1;1u'; ",
                "fi; ",
                "done"
            )
            .to_string(),
        })
        .expect("spawn");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "mode-gated-kitty");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "mode-gated-kitty".to_string(),
            subscription_id: "mode-gated-kitty-sub".to_string(),
        })
        .expect("attach");

    let baseline = wait_for_mode_flags(
        &mut connection,
        "mode-gated-kitty",
        "mode-gated-kitty-sub",
        |flags| flags.mode_generation != 0,
    );

    connection
        .send_terminal_frame(
            "mode-gated-kitty",
            "mode-gated-kitty-sub",
            &terminal_mode_gated_frame_bytes(
                b"enable-modes\n",
                baseline.mode_generation,
                baseline.mode_revision,
            ),
        )
        .expect("enable modes");

    let after = wait_for_mode_flags(
        &mut connection,
        "mode-gated-kitty",
        "mode-gated-kitty-sub",
        |flags| flags.kitty_enabled && flags.mouse_mode == 9,
    );
    assert!(after.kitty_enabled);
    assert_eq!(after.mouse_mode, 9);

    connection
        .send_terminal_frame(
            "mode-gated-kitty",
            "mode-gated-kitty-sub",
            &terminal_mode_gated_frame_bytes(
                b"stale-kitty\n",
                baseline.mode_generation,
                baseline.mode_revision,
            ),
        )
        .expect("stale gated input");

    thread::sleep(Duration::from_millis(100));
    let screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "mode-gated-kitty".to_string(),
        })
        .expect("screen after stale");
    let text = screen.read_screen.expect("screen body").text;
    assert!(
        !text.contains("echo:stale-kitty"),
        "stale gated input must write zero PTY bytes; screen={text}"
    );

    connection
        .send_terminal_frame(
            "mode-gated-kitty",
            "mode-gated-kitty-sub",
            &terminal_mode_gated_frame_bytes(
                b"fresh-kitty\n",
                after.mode_generation,
                after.mode_revision,
            ),
        )
        .expect("fresh gated input");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_fresh = false;
    while Instant::now() < deadline {
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "mode-gated-kitty".to_string(),
            })
            .expect("screen after fresh");
        if screen
            .read_screen
            .expect("screen body")
            .text
            .contains("echo:fresh-kitty")
        {
            saw_fresh = true;
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(saw_fresh, "fresh gated input should reach the PTY");

    production_shutdown_and_remove_session(&endpoint, "mode-gated-kitty");
    session_cleanup.disarm();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_mode_gated_mouse_stale_token_rejects_and_reprobe_admits() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-mode-gated-mouse");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "mode-gated-mouse".to_string(),
            command: concat!(
                "printf ready; while IFS= read -r line; do ",
                "printf \"echo:%s\\n\" \"$line\"; ",
                "if [ \"$line\" = enable-mouse ]; then ",
                "printf '\\033[?1000h\\033[?1006h'; ",
                "fi; ",
                "done"
            )
            .to_string(),
        })
        .expect("spawn");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "mode-gated-mouse");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "mode-gated-mouse".to_string(),
            subscription_id: "mode-gated-mouse-sub".to_string(),
        })
        .expect("attach");

    let baseline = wait_for_mode_flags(
        &mut connection,
        "mode-gated-mouse",
        "mode-gated-mouse-sub",
        |flags| flags.mode_generation != 0,
    );
    connection
        .send_terminal_frame(
            "mode-gated-mouse",
            "mode-gated-mouse-sub",
            &terminal_mode_gated_frame_bytes(
                b"enable-mouse\n",
                baseline.mode_generation,
                baseline.mode_revision,
            ),
        )
        .expect("enable mouse");

    let after = wait_for_mode_flags(
        &mut connection,
        "mode-gated-mouse",
        "mode-gated-mouse-sub",
        |flags| flags.mouse_mode == 9,
    );
    assert_eq!(after.mouse_mode, 9);

    connection
        .send_terminal_frame(
            "mode-gated-mouse",
            "mode-gated-mouse-sub",
            &terminal_mode_gated_frame_bytes(
                b"stale-mouse\n",
                baseline.mode_generation,
                baseline.mode_revision,
            ),
        )
        .expect("stale mouse input");
    thread::sleep(Duration::from_millis(100));
    let stale_screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "mode-gated-mouse".to_string(),
        })
        .expect("screen after stale");
    let stale_text = stale_screen.read_screen.expect("screen body").text;
    assert!(
        !stale_text.contains("echo:stale-mouse"),
        "stale gated input must write zero PTY bytes; screen={stale_text}"
    );

    connection
        .send_terminal_frame(
            "mode-gated-mouse",
            "mode-gated-mouse-sub",
            &terminal_mode_gated_frame_bytes(
                b"fresh-mouse\n",
                after.mode_generation,
                after.mode_revision,
            ),
        )
        .expect("fresh mouse input");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_fresh = false;
    while Instant::now() < deadline {
        let screen = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "mode-gated-mouse".to_string(),
            })
            .expect("screen");
        if screen
            .read_screen
            .expect("body")
            .text
            .contains("echo:fresh-mouse")
        {
            saw_fresh = true;
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(saw_fresh, "fresh mouse gated input should reach PTY");

    production_shutdown_and_remove_session(&endpoint, "mode-gated-mouse");
    session_cleanup.disarm();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_ghostty_snapshot_reflects_osc_palette_and_specials() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-osc-snapshot");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "osc-snapshot-session".to_string(),
            command: [
                "printf ready; ",
                "printf '\\033]4;3;rgb:1111/2222/3333\\033\\\\'; ",
                "printf '\\033]10;rgb:aaaa/bbbb/cccc\\033\\\\'; ",
                "printf '\\033]11;rgb:0101/0202/0303\\033\\\\'; ",
                "printf '\\033]12;rgb:fefe/fdfd/fcfc\\033\\\\'; ",
                "printf 'echo:color-mutated\\n'; ",
                "while IFS= read -r line; do printf \"echo:%s\\n\" \"$line\"; done",
            ]
            .concat(),
        })
        .expect("spawn");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "osc-snapshot-session");

    // Wait for mutations to land, then attach for data-plane Snapshot.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let screen = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "osc-snapshot-session".to_string(),
            },
        )
        .expect("read screen");
        if screen
            .read_screen
            .as_ref()
            .map(|body| body.text.contains("echo:color-mutated"))
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting for color mutation"
        );
        thread::sleep(Duration::from_millis(30));
    }

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "osc-snapshot-session".to_string(),
            subscription_id: "osc-snapshot-sub".to_string(),
        })
        .expect("attach");
    assert!(
        attach.events.is_empty(),
        "Attach must not return terminal bodies: {:?}",
        attach.events
    );
    let capture = connection
        .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: "osc-snapshot-session".to_string(),
        })
        .expect("capture snapshot metadata");
    assert_eq!(
        capture.kind,
        botster_hub_client::DaemonResponseKind::CaptureSnapshot
    );
    let meta = capture.capture_snapshot.as_ref().expect("capture body");
    assert!(meta.payload_bytes > GHOSTSNP_MAGIC.len());
    assert_eq!(
        meta.payload_format.as_deref(),
        Some("ghostty-terminal-snapshot-v1")
    );
    // Current colors live in GHOSTSNP only after session start. Hub startup
    // defaults (#FFFFFF/#282C34) must not be treated as current post-install.
    // Hub does not decode GHOSTSNP; Core proves color agreement. Here we prove
    // the authoritative byte path is Snapshot and is non-empty GHOSTSNP.

    production_shutdown_and_remove_session(&endpoint, "osc-snapshot-session");
    session_cleanup.disarm();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_osc_101112_session_side_replies_with_startup_baseline() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-osc-baseline");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let script_path = data_dir.join("osc_query_child.py");
    fs::create_dir_all(&data_dir).expect("data dir");
    fs::write(
        &script_path,
        r#"#!/usr/bin/env python3
import sys
import time
sys.stdout.write("ready\n")
sys.stdout.flush()
sys.stdout.buffer.write(b"\x1b]10;?\x1b\\\x1b]11;?\x1b\\\x1b]12;?\x1b\\")
sys.stdout.flush()
time.sleep(2)
sys.stdout.write("done\n")
sys.stdout.flush()
"#,
    )
    .expect("write script");

    // No client attach: OSC replies must come from session-side Ghostty using the
    // Hub startup baseline profile, not Hub query synthesis.
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "osc-baseline-session".to_string(),
            command: format!("python3 {}", script_path.display()),
        },
    )
    .expect("spawn osc child");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "osc-baseline-session");

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut text = String::new();
    while Instant::now() < deadline {
        let screen = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "osc-baseline-session".to_string(),
            },
        )
        .expect("read screen");
        text = screen.read_screen.expect("screen body").text;
        if text.to_ascii_lowercase().contains("]12;rgb:") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let lowered = text.to_ascii_lowercase();
    // Product defaults: FG #FFFFFF, BG #282C34, cursor #FFFFFF
    for expected in [
        "]10;rgb:ffff/ffff/ffff",
        "]11;rgb:2828/2c2c/3434",
        "]12;rgb:ffff/ffff/ffff",
    ] {
        assert!(
            lowered.contains(expected),
            "expected OSC baseline {expected} in session screen; text={text}"
        );
    }
    let seq10 = lowered.find("]10;rgb:ffff/ffff/ffff").expect("osc10");
    let seq11 = lowered.find("]11;rgb:2828/2c2c/3434").expect("osc11");
    let seq12 = lowered.find("]12;rgb:ffff/ffff/ffff").expect("osc12");
    assert!(
        seq10 < seq11 && seq11 < seq12,
        "OSC reply order broken: {text}"
    );

    production_shutdown_and_remove_session(&endpoint, "osc-baseline-session");
    session_cleanup.disarm();
    shutdown_cli_daemon(&data_dir, child);
}

/// Failure path: an armed SessionCleanupGuard must shut down a durable unbounded
/// worker session when production RemoveSession never ran (panic / early return).
///
/// Proves logical-session exit/removal **and** authoritative absence of the
/// captured session-worker PID and at least one exact non-root shell descendant.
#[test]
fn session_cleanup_guard_failure_path_reaps_durable_unbounded_session() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-cleanup-guard-failure");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path.clone());
    let child = start_cli_daemon(&data_dir);

    let before_pids: std::collections::BTreeSet<u32> = session_worker_process_identities()
        .expect("baseline worker census must succeed")
        .into_iter()
        .map(|worker| worker.pid)
        .collect();

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "cleanup-guard-session".to_string(),
            command:
                "printf ready; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn unbounded session");
    // Arm immediately after Spawn so census/assertion panics still clean up.
    let session_cleanup = SessionCleanupGuard::new(&data_dir, "cleanup-guard-session");
    let list_before =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("list before failure path");
    assert!(
        list_before.sessions.iter().any(|session| {
            session.session_id == "cleanup-guard-session" && session.lifecycle == "running"
        }),
        "spawned unbounded session must be running before failure-path cleanup"
    );

    let owned_workers = capture_new_session_workers_for_data_dir(&data_dir, &before_pids)
        .expect("must capture worker + live shell descendant after Spawn");
    assert!(
        !owned_workers.is_empty(),
        "must capture live botster-session-worker for this data_dir after Spawn"
    );

    let mut tracked_worker_pids = Vec::new();
    let mut tracked_shell_pids = Vec::new();
    for worker in &owned_workers {
        process_must_be_alive(worker.pid, "worker")
            .unwrap_or_else(|error| panic!("{error}; worker={worker:?}"));
        assert!(
            !worker.shell_descendant_pids.is_empty(),
            "fixture starts sh -c; must observe at least one live non-root shell PID before cleanup: {worker:?}"
        );
        for shell_pid in &worker.shell_descendant_pids {
            assert_ne!(
                *shell_pid, worker.pid,
                "shell descendant must differ from worker root: {worker:?}"
            );
            process_must_be_alive(*shell_pid, "shell descendant")
                .unwrap_or_else(|error| panic!("{error}; worker={worker:?}"));
            tracked_shell_pids.push(*shell_pid);
        }
        tracked_worker_pids.push(worker.pid);
    }
    assert!(
        !tracked_shell_pids.is_empty(),
        "must track exact non-root shell PIDs before guard drop"
    );

    // Same guard, simulated unwind before production RemoveSession.
    drop(session_cleanup);

    // Bounded process oracle: exact worker + exact shell descendant PIDs must be absent.
    let tracked_pids: Vec<u32> = tracked_worker_pids
        .iter()
        .chain(tracked_shell_pids.iter())
        .copied()
        .collect();
    let process_deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut all_absent = true;
        let mut survivors = Vec::new();
        for pid in &tracked_pids {
            match process_is_alive_u32(*pid) {
                Ok(true) => {
                    all_absent = false;
                    survivors.push(*pid);
                }
                Ok(false) => {}
                Err(error) => {
                    panic!("process probe failed for pid {pid} after guard drop: {error}");
                }
            }
        }
        if all_absent {
            break;
        }
        assert!(
            Instant::now() < process_deadline,
            "SessionCleanupGuard drop must reap exact worker and shell PIDs; still live={survivors:?} workers={owned_workers:?} shells={tracked_shell_pids:?}"
        );
        thread::sleep(Duration::from_millis(40));
    }
    for pid in &tracked_worker_pids {
        process_must_be_absent(*pid, "worker after guard drop")
            .unwrap_or_else(|error| panic!("{error}"));
    }
    for pid in &tracked_shell_pids {
        process_must_be_absent(*pid, "shell descendant after guard drop")
            .unwrap_or_else(|error| panic!("{error}"));
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let exited = loop {
        let list =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
                .expect("list after failure-path guard drop");
        if let Some(session) = list
            .sessions
            .iter()
            .find(|session| session.session_id == "cleanup-guard-session")
        {
            if session.lifecycle == "exited" {
                break session.lifecycle.clone();
            }
        } else {
            break "absent".to_string();
        }
        assert!(
            Instant::now() < deadline,
            "SessionCleanupGuard drop must shut down durable worker session"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert!(
        exited == "exited" || exited == "absent",
        "failure-path cleanup must leave session exited or removed, got {exited}"
    );

    // Production-style registry removal after guard reaped the worker tree.
    if session_ids_from_list(&endpoint)
        .iter()
        .any(|id| id == "cleanup-guard-session")
    {
        let remove = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::RemoveSession {
                session_id: "cleanup-guard-session".to_string(),
            },
        )
        .expect("remove exited session after failure-path shutdown");
        assert_eq!(
            remove.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
        );
    }
    assert!(
        !session_ids_from_list(&endpoint)
            .iter()
            .any(|id| id == "cleanup-guard-session"),
        "ListSessions must not retain cleanup-guard-session after RemoveSession"
    );
    // Re-check exact tracked PIDs after logical removal (no resurrection).
    for pid in &tracked_worker_pids {
        process_must_be_absent(*pid, "worker after RemoveSession")
            .unwrap_or_else(|error| panic!("{error}"));
    }
    for pid in &tracked_shell_pids {
        process_must_be_absent(*pid, "shell descendant after RemoveSession")
            .unwrap_or_else(|error| panic!("{error}"));
    }
    assert!(
        socket_path.exists(),
        "daemon control socket remains for hub shutdown after session cleanup"
    );

    shutdown_cli_daemon(&data_dir, child);
    assert!(
        !socket_path.exists(),
        "hub shutdown must remove the control socket after durable-session cleanup"
    );
}

#[test]
fn session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect() {
    let _guard = daemon_test_guard();
    let conformance_hub = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(unique_short_test_dir("slc"))
            .name("published-runner"),
    );
    let conformance_report =
        botster_hub_test_support::run_session_lifecycle_subscription_conformance(&conformance_hub)
            .expect("run published session lifecycle conformance against real topology");
    assert!(conformance_report.initial_snapshot_authoritative);
    assert!(conformance_report.concurrent_subscribers_consistent);
    assert!(conformance_report.spawn_upsert_observed);
    assert!(conformance_report.lifecycle_patch_observed);
    assert!(conformance_report.natural_exit_patch_observed);
    assert!(conformance_report.remove_observed);
    assert!(conformance_report.sequences_strictly_increasing);
    assert!(conformance_report.disconnect_cleanup_released_subscription);
    assert!(conformance_report.fresh_subscription_snapshot_authoritative);
    assert_eq!(
        conformance_report.overflow_resync_reason,
        "subscriber_overflow"
    );
    assert!(conformance_report.failed_snapshot_delivery_closes_subscription);
    conformance_hub
        .shutdown()
        .expect("shutdown published session lifecycle conformance hub");

    let data_dir = unique_test_dir("session-entity-subscription");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = PanicSafeCliDaemon::start(&data_dir, "session entity daemon evidence");

    let mut first = botster_hub_client::subscribe_session_entities(&endpoint, "entities-first")
        .expect("subscribe first session entity stream");
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound first entity reads");
    let initial = first.next_frame().expect("initial authoritative snapshot");
    assert!(matches!(
        initial,
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            resync_reason: None,
            ..
        } if items.is_empty()
    ));

    let mut second = botster_hub_client::subscribe_session_entities(&endpoint, "entities-second")
        .expect("subscribe independent session entity stream");
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound second entity reads");
    assert!(matches!(
        second.next_frame().expect("second authoritative snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-session".to_string(),
            command: "printf 'entity-before\\nentity-ready\\n'; IFS= read -r release; \
                      printf 'entity-after:%s\\n' \"$release\""
                .to_string(),
        },
    )
    .expect("spawn entity session");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "entity-session");

    let first_upsert = first.next_frame().expect("first subscriber upsert");
    let second_upsert = second.next_frame().expect("second subscriber upsert");
    let upsert_sequence = match first_upsert {
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ref entity,
            ..
        } if id == "entity-session" => {
            assert_eq!(
                entity
                    .get("lifecycle_class")
                    .and_then(serde_json::Value::as_str),
                Some("current")
            );
            snapshot_seq
        }
        other => panic!("expected first upsert, got {other:?}"),
    };
    assert!(matches!(
        second_upsert,
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-session" && snapshot_seq == upsert_sequence
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-session".to_string(),
            subscription_id: "terminal-alongside-entities".to_string(),
        })
        .expect("attach while entity pump is active");
    let mut terminal_output =
        wait_for_read_screen_contains(&mut terminal, "entity-session", "entity-ready");
    assert!(
        terminal_output.contains("entity-before") && terminal_output.contains("entity-ready"),
        "entity fixture must publish semantic readiness through ReadScreen, \
         got {terminal_output:?}"
    );

    terminal
        .send_terminal_frame(
            "entity-session",
            "terminal-alongside-entities",
            &terminal_resize_frame_bytes(31, 101),
        )
        .expect("resize entity session");
    let first_resize = first
        .next_frame()
        .expect("first subscriber resize transition");
    let second_resize = second
        .next_frame()
        .expect("second subscriber resize transition");
    let resize_sequence = match &first_resize {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
            && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected rows=31/cols=101 as the first post-resize frame for both subscribers; \
             first={first_resize:?} second={second_resize:?}"
        ),
    };
    assert!(resize_sequence > upsert_sequence);
    let second_resize_sequence = match &second_resize {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
            && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected rows=31/cols=101 as the first post-resize frame for both subscribers; \
             first={first_resize:?} second={second_resize:?}"
        ),
    };
    assert_eq!(
        second_resize_sequence, resize_sequence,
        "subscriber resize sequences diverged: first={first_resize:?} second={second_resize:?}"
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join("sessions").join("entity-session.json"))
            .expect("read resized session record"),
    )
    .expect("parse resized session record");
    assert_eq!(persisted.get("rows").and_then(serde_json::Value::as_u64), Some(31));
    assert_eq!(persisted.get("cols").and_then(serde_json::Value::as_u64), Some(101));

    terminal
        .send_terminal_frame(
            "entity-session",
            "terminal-alongside-entities",
            &terminal_input_frame_bytes(b"release\r"),
        )
        .expect("release entity fixture through terminal input");
    terminal_output =
        wait_for_read_screen_contains(&mut terminal, "entity-session", "entity-after:release");
    assert!(
        terminal_output.contains("entity-after:release"),
        "entity lifecycle pumping must retain visible text through ReadScreen, \
         got {terminal_output:?}"
    );
    let _ = terminal.send_terminal_frame(
        "entity-session",
        "terminal-alongside-entities",
        &terminal_resize_frame_bytes(31, 101),
    );

    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bounded first entity reads while draining lifecycle");
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bounded second entity reads while draining lifecycle");
    let exit_deadline = Instant::now() + Duration::from_secs(15);
    let mut first_exit = None;
    let mut second_exit = None;
    while Instant::now() < exit_deadline && (first_exit.is_none() || second_exit.is_none()) {
        if first_exit.is_none()
            && let Ok(frame) = first.next_frame()
            && matches!(
                &frame,
                botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. }
                    if id == "entity-session"
                        && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                            == Some("exited")
            )
        {
            first_exit = Some(frame);
        }
        if second_exit.is_none()
            && let Ok(frame) = second.next_frame()
            && matches!(
                &frame,
                botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. }
                    if id == "entity-session"
                        && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                            == Some("exited")
            )
        {
            second_exit = Some(frame);
        }
    }
    let list_deadline = Instant::now() + Duration::from_secs(30);
    let mut listed_lifecycle = None;
    while Instant::now() < list_deadline {
        let _ = terminal.send_terminal_frame(
            "entity-session",
            "terminal-alongside-entities",
            &terminal_resize_frame_bytes(31, 101),
        );
        listed_lifecycle =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
                .ok()
                .and_then(|response| {
                    response.sessions.iter().find_map(|session| {
                        (session.session_id == "entity-session").then(|| session.lifecycle.clone())
                    })
                });
        if listed_lifecycle.as_deref() == Some("exited") || listed_lifecycle.is_none() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        listed_lifecycle.as_deref() == Some("exited") || listed_lifecycle.is_none(),
        "host ListSessions must observe natural exit without Drain, last={listed_lifecycle:?}"
    );
    let exit_sequence = match (first_exit.as_ref(), second_exit.as_ref()) {
        (Some(first_exit), Some(second_exit)) => {
            let first_seq = entity_exit_sequence(first_exit);
            let second_seq = entity_exit_sequence(second_exit);
            assert_eq!(
                first_seq, second_seq,
                "subscriber exit sequences diverged: first={first_exit:?} second={second_exit:?}"
            );
            assert!(first_seq > resize_sequence);
            first_seq
        }
        _ => resize_sequence,
    };

    let remove_deadline = Instant::now() + Duration::from_secs(10);
    let removed = loop {
        let removed = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::RemoveSession {
                session_id: "entity-session".to_string(),
            },
        )
        .expect("remove terminal entity session");
        if removed.kind == botster_hub_client::DaemonResponseKind::SessionRemoved
            || !session_ids_from_list(&endpoint)
                .iter()
                .any(|id| id == "entity-session")
        {
            break removed;
        }
        assert!(
            Instant::now() < remove_deadline,
            "RemoveSession must succeed after host exit, last={removed:?}"
        );
        thread::sleep(Duration::from_millis(200));
    };
    assert!(
        matches!(
            removed.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
                | botster_hub_client::DaemonResponseKind::OperatorError
        ),
        "RemoveSession should remove or report already-gone, got {:?}",
        removed.kind
    );
    let gone_deadline = Instant::now() + Duration::from_secs(5);
    while session_ids_from_list(&endpoint)
        .iter()
        .any(|id| id == "entity-session")
    {
        assert!(
            Instant::now() < gone_deadline,
            "ListSessions retained entity-session after SessionRemoved"
        );
        thread::sleep(Duration::from_millis(50));
    }
    session_cleanup.disarm();
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound remove-frame read");
    let remove_deadline = Instant::now() + Duration::from_secs(5);
    let removed_frame = loop {
        match first.next_frame() {
            Ok(frame @ botster_hub_client::DaemonEntityFrame::Remove { .. }) => break frame,
            Ok(_) => {}
            Err(error) if Instant::now() < remove_deadline => {
                let _ = error;
            }
            Err(error) => panic!("remove delta: {error}"),
        }
    };
    assert!(matches!(
        removed_frame,
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-session" && snapshot_seq > exit_sequence
    ));

    drop(first);
    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    let cleanup_probe = loop {
        match botster_hub_client::subscribe_session_entities(&endpoint, "entities-first") {
            Ok(subscription) => break subscription,
            Err(_) if Instant::now() < cleanup_deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("socket EOF should release the old subscription: {error}"),
        }
    };
    cleanup_probe
        .unsubscribe()
        .expect("unsubscribe cleanup probe stream");
    let mut reconnected =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-reconnected")
            .expect("fresh reconnect subscription");
    reconnected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound reconnect entity reads");
    assert!(matches!(
        reconnected.next_frame().expect("fresh reconnect snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            ref subscription_id,
            ref items,
            ..
        } if subscription_id == "entities-reconnected" && items.is_empty()
    ));
    reconnected
        .unsubscribe()
        .expect("unsubscribe reconnect stream");
    second.unsubscribe().expect("unsubscribe second stream");
    child.shutdown();
}

#[test]
fn session_entity_subscription_projects_stale_row_as_indeterminate() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-stale");
    let config = explicit_config(&data_dir);
    let session_id = SessionId("session-entity-stale".to_string());
    let registry = SessionRegistry::new(config.data_directory.clone());
    let mut stale_record = RegistryRecord::running(
        session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    stale_record.observe_restart_contract(serde_json::json!({"session": "stale"}), 2);
    registry
        .save(&stale_record)
        .expect("save stale registry fixture");

    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "stale-session-entities")
            .expect("subscribe to stale session projection");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound stale projection read");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_stale = false;
    while Instant::now() < deadline {
        match subscription
            .next_frame()
            .expect("authoritative stale projection")
        {
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.iter().any(|entity| {
                    entity
                        .get("session_uuid")
                        .and_then(serde_json::Value::as_str)
                        == Some(session_id.0.as_str())
                        && entity
                            .get("registry_state")
                            .and_then(serde_json::Value::as_str)
                            == Some("stale")
                        && entity
                            .get("lifecycle_class")
                            .and_then(serde_json::Value::as_str)
                            == Some("indeterminate")
                }) =>
            {
                saw_stale = true;
                break;
            }
            botster_hub_client::DaemonEntityFrame::Upsert {
                ref id, ref entity, ..
            } if id == &session_id.0
                && entity
                    .get("registry_state")
                    .and_then(serde_json::Value::as_str)
                    == Some("stale")
                && entity
                    .get("lifecycle_class")
                    .and_then(serde_json::Value::as_str)
                    == Some("indeterminate") =>
            {
                saw_stale = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_stale,
        "stale projection must arrive as snapshot or upsert"
    );
    subscription
        .unsubscribe()
        .expect("unsubscribe stale projection");
    shutdown_cli_daemon(&data_dir, child);
    assert!(
        harness_taint().is_some_and(|evidence| {
            evidence.contains("session-entity-stale") && evidence.contains("no recovery worker pid")
        }),
        "forged stale command 42 must taint as missing recovery identity: {:?}",
        harness_taint()
    );
    reset_harness_taint_after_proof();
}

#[test]
fn focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("focused-connection-lifecycle");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let daemon = PanicSafeCliDaemon::start(&data_dir, "connection lifecycle daemon evidence");
    let daemon_pid = daemon.child.as_ref().expect("panic-safe daemon child").id();
    let startup_counters =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before first entity subscription")
            .status
            .expect("startup status body")
            .lifecycle_counters;
    assert_eq!(startup_counters.lifecycle_baseline_reads, 1);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "focused-idle")
            .expect("subscribe focused idle entity stream");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound initial snapshot read");
    assert!(matches!(
        subscription.next_frame().expect("initial focused snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));
    let before_idle =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before focused idle window")
            .status
            .expect("focused status body")
            .lifecycle_counters;
    assert_eq!(
        before_idle.lifecycle_baseline_reads, startup_counters.lifecycle_baseline_reads,
        "first live subscriber must consume the startup-seeded baseline without owner-path I/O"
    );
    thread::sleep(Duration::from_millis(1_100));
    let after_idle =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after focused idle window")
            .status
            .expect("focused status body")
            .lifecycle_counters;
    assert_eq!(
        after_idle.lifecycle_baseline_reads, before_idle.lifecycle_baseline_reads,
        "steady-state idle reconciliation must not rescan the session registry"
    );
    assert_eq!(
        after_idle.entity_delivery_attempts, before_idle.entity_delivery_attempts,
        "an idle entity stream must not receive timer-driven frames"
    );
    assert!(
        after_idle
            .lifecycle_change_reads
            .saturating_sub(before_idle.lifecycle_change_reads)
            <= 4,
        "the one shared idle backstop must stay low-frequency"
    );

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "focused-pty-progress".to_string(),
            command: "sh -c 'i=0; while [ \"$i\" -lt 80 ]; do i=$((i+1)); printf x; sleep 0.05; done'".to_string(),
        },
    )
    .expect("spawn PTY progress producer");
    thread::sleep(Duration::from_millis(150));
    let screen_before = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "focused-pty-progress".to_string(),
        },
    )
    .expect("read PTY screen before flood")
    .read_screen
    .map(|screen| screen.text)
    .unwrap_or_default();

    const FLOOD_CONNECTIONS: usize = 32;
    const FLOOD_REQUESTS_PER_CONNECTION: usize = 512;
    let mut flood_writers = Vec::new();
    let mut flood_readers = Vec::new();
    for _ in 0..FLOOD_CONNECTIONS {
        let mut writer =
            UnixStream::connect(&endpoint.socket_path).expect("connect pipelined pressure fixture");
        botster_hub_client::write_frame(
            &mut writer,
            &botster_hub_client::DaemonHello {
                protocol: botster_hub_client::PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write pressure-fixture hello");
        let _: botster_hub_client::DaemonHelloAck =
            botster_hub_client::read_frame(&mut writer).expect("read pressure-fixture hello ack");
        let mut reader = BufReader::new(
            writer
                .try_clone()
                .expect("clone pressure-fixture response reader"),
        );
        flood_readers.push(thread::spawn(move || {
            let mut incomplete = String::new();
            for _ in 0..FLOOD_REQUESTS_PER_CONNECTION {
                let _: botster_hub_client::DaemonResponse =
                    botster_hub_client::read_frame_from_reader(&mut reader, &mut incomplete)
                        .expect("drain pipelined status response");
            }
        }));
        flood_writers.push(writer);
    }
    let flood_before =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before sustained control pressure")
            .status
            .expect("sustained control pressure status body")
            .lifecycle_counters;
    for writer in &mut flood_writers {
        for _ in 0..FLOOD_REQUESTS_PER_CONNECTION {
            botster_hub_client::write_frame(writer, &botster_hub_client::DaemonRequest::Status)
                .expect("pipeline sustained status request");
        }
    }
    thread::sleep(Duration::from_millis(1_100));
    let flood_after =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status during sustained control pressure")
            .status
            .expect("post-pressure status body")
            .lifecycle_counters;
    drop(flood_writers);
    for flood_reader in flood_readers {
        flood_reader
            .join()
            .expect("join pipelined status response drain");
    }
    assert!(
        flood_after.reconciliation_wakes > flood_before.reconciliation_wakes,
        "a continuously busy control queue must not starve shared entity reconciliation"
    );
    let screen_after = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "focused-pty-progress".to_string(),
        },
    )
    .expect("read PTY screen during flood")
    .read_screen
    .map(|screen| screen.text)
    .unwrap_or_default();
    let before_xs = screen_before.matches('x').count();
    let after_xs = screen_after.matches('x').count();
    assert!(
        after_xs > before_xs,
        "PTY output must progress during the Status flood; before={before_xs} after={after_xs} text={screen_after:?}"
    );
    let complete_deadline = Instant::now() + Duration::from_secs(8);
    let mut complete_text = screen_after.clone();
    while complete_text.matches('x').count() < 80 && Instant::now() < complete_deadline {
        complete_text = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "focused-pty-progress".to_string(),
            },
        )
        .expect("read PTY screen for complete output")
        .read_screen
        .map(|screen| screen.text)
        .unwrap_or_default();
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        complete_text.matches('x').count() >= 80,
        "the complete producer output must arrive; text={complete_text:?}"
    );
    let _ = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "focused-pty-progress".to_string(),
        },
    );

    for index in 0..8 {
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: format!("focused-idle-session-{index}"),
                command: "sleep 10".to_string(),
            },
        )
        .expect("spawn focused session-count fixture");
    }
    let mut upserts = BTreeMap::new();
    while upserts.len() < 8 {
        if let botster_hub_client::DaemonEntityFrame::Upsert { id, .. } = subscription
            .next_frame()
            .expect("session-count fixture upsert")
            && id.starts_with("focused-idle-session-")
        {
            upserts.insert(id, ());
        }
    }
    let mut additional_subscriptions = Vec::new();
    for index in 1..8 {
        let mut additional = botster_hub_client::subscribe_session_entities(
            &endpoint,
            format!("focused-idle-{index}"),
        )
        .expect("subscribe additional idle entity stream");
        additional
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound additional snapshot read");
        let mut seen = std::collections::BTreeSet::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while seen.len() < 8 && Instant::now() < deadline {
            match additional.next_frame() {
                Err(botster_hub_client::DaemonTransportError::Io(error))
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(error) => panic!("additional idle frame: {error}"),
                Ok(frame) => match frame {
                    botster_hub_client::DaemonEntityFrame::Snapshot { items, .. } => {
                        for item in items {
                            if let Some(id) =
                                item.get("session_uuid").and_then(serde_json::Value::as_str)
                                && id.starts_with("focused-idle-session-")
                            {
                                seen.insert(id.to_string());
                            }
                        }
                    }
                    botster_hub_client::DaemonEntityFrame::Upsert { id, .. }
                        if id.starts_with("focused-idle-session-") =>
                    {
                        seen.insert(id);
                    }
                    _ => {}
                },
            }
        }
        assert_eq!(seen.len(), 8, "paged subscribe must deliver every live row");
        additional_subscriptions.push(additional);
    }
    let many_before = wait_for_idle_lifecycle_window(&endpoint);
    thread::sleep(Duration::from_millis(1_100));
    let many_after = lifecycle_counters(&endpoint, "status after many-session idle window");
    eprintln!(
        "many-session idle wake_delta={} change_delta={} delivery_delta={} drain_delta={} before_wakes={} after_wakes={}",
        many_after
            .reconciliation_wakes
            .saturating_sub(many_before.reconciliation_wakes),
        many_after
            .lifecycle_change_reads
            .saturating_sub(many_before.lifecycle_change_reads),
        many_after
            .entity_delivery_attempts
            .saturating_sub(many_before.entity_delivery_attempts),
        many_after
            .lifecycle_session_drains
            .saturating_sub(many_before.lifecycle_session_drains),
        many_before.reconciliation_wakes,
        many_after.reconciliation_wakes
    );
    assert_eq!(
        many_after.lifecycle_baseline_reads, many_before.lifecycle_baseline_reads,
        "session count must not restore filesystem-backed baseline polling: before={} after={}",
        many_before.lifecycle_baseline_reads,
        many_after.lifecycle_baseline_reads
    );
    assert_eq!(
        many_after.entity_delivery_attempts, many_before.entity_delivery_attempts,
        "subscriber count must not create timer-driven entity delivery"
    );
    assert!(
        many_after
            .reconciliation_wakes
            .saturating_sub(many_before.reconciliation_wakes)
            <= 4,
        "shared wake count must stay independent of session count: before={} after={}",
        many_before.reconciliation_wakes,
        many_after.reconciliation_wakes
    );
    assert!(
        many_after.reconciliation_wakes > many_before.reconciliation_wakes,
        "idle owner turns continue through observe and journal slices without terminal drains"
    );

    let mut attached = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect persistent attach counter fixture");
    attached
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "focused-idle-session-0".to_string(),
            subscription_id: "focused-attach".to_string(),
        })
        .expect("attach persistent counter fixture");
    let attached_counters =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status with live attach")
            .status
            .expect("live attach status body")
            .lifecycle_counters;
    assert_eq!(attached_counters.live_attach_subscriptions, 1);
    assert!(attached_counters.high_water_attach_subscriptions >= 1);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "focused-idle-session-0".to_string(),
        },
    )
    .expect("shutdown attached cleanup-race fixture");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "focused-idle-session-0".to_string(),
        },
    )
    .expect("remove attached cleanup-race fixture");
    drop(attached);

    let attached_cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status while waiting for idempotent detach cleanup")
                .status
                .expect("idempotent cleanup status body")
                .lifecycle_counters;
        if counters.cleanup_completed > attached_counters.cleanup_completed {
            assert_eq!(counters.live_attach_subscriptions, 0);
            assert_eq!(counters.cleanup_failed, attached_counters.cleanup_failed);
            break;
        }
        assert!(
            Instant::now() < attached_cleanup_deadline,
            "idempotent cleanup did not settle: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    drop(subscription);
    drop(additional_subscriptions);

    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status while waiting for entity cleanup")
                .status
                .expect("cleanup status body")
                .lifecycle_counters;
        if counters.live_entity_subscriptions == 0 {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "dropped entity stream did not release its subscription"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let churn_start =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before rapid reconnect churn")
            .status
            .expect("rapid reconnect start status")
            .lifecycle_counters;
    for index in 0..16 {
        let mut churn = botster_hub_client::subscribe_session_entities(
            &endpoint,
            format!("focused-churn-{index}"),
        )
        .expect("register fresh-id reconnect generation");
        churn
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound churn snapshot read");
        assert!(matches!(
            churn.next_frame().expect("fresh-id churn snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { .. }
        ));
        drop(churn);
        let generation_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let counters =
                botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                    .expect("status while releasing reconnect generation")
                    .status
                    .expect("reconnect generation status body")
                    .lifecycle_counters;
            if counters.live_entity_subscriptions == 0 {
                assert_eq!(counters.high_water_entity_subscriptions, 8);
                break;
            }
            assert!(
                Instant::now() < generation_deadline,
                "reconnect generation {index} did not release: {counters:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
    let churn_end =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after rapid reconnect churn")
            .status
            .expect("rapid reconnect end status")
            .lifecycle_counters;
    assert_eq!(
        churn_end
            .reconnect_registrations
            .saturating_sub(churn_start.reconnect_registrations),
        16,
        "every released generation followed by a fresh subscription id is a reconnect"
    );
    assert!(
        churn_end
            .cleanup_by_reason
            .get("eof")
            .copied()
            .unwrap_or_default()
            .saturating_sub(
                churn_start
                    .cleanup_by_reason
                    .get("eof")
                    .copied()
                    .unwrap_or_default()
            )
            >= 16
    );

    let connection_baseline_threads = process_thread_count(daemon_pid);
    let mut idle_connections = Vec::new();
    for _ in 0..64 {
        idle_connections.push(
            botster_hub_client::DaemonConnection::connect(&endpoint)
                .expect("admit bounded idle connection"),
        );
    }
    let saturated = idle_connections[0]
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("existing client stays responsive at admission bound")
        .status
        .expect("saturated status body")
        .lifecycle_counters;
    assert_eq!(saturated.live_connections, 64);
    assert_eq!(saturated.high_water_live_connections, 64);
    assert!(saturated.accepted_connections >= 64);
    if let (Some(baseline), Some(peak)) = (
        connection_baseline_threads,
        process_thread_count(daemon_pid),
    ) {
        assert!(
            peak.saturating_sub(baseline) <= 8,
            "64 idle connections created too many OS threads: baseline={baseline} peak={peak}"
        );
    }

    let stalled_rejection =
        UnixStream::connect(&endpoint.socket_path).expect("connect stalled over-cap peer");
    let rejection_started = Instant::now();
    let mut rejected = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("over-cap client receives typed admission hello");
    assert!(
        rejection_started.elapsed() < Duration::from_secs(1),
        "one silent over-cap peer must not head-of-line block typed rejection"
    );
    assert!(
        rejected
            .request(&botster_hub_client::DaemonRequest::Status)
            .is_err(),
        "over-cap connection must not enter the runtime request path"
    );
    let rejection_counter_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let rejected_status = idle_connections[0]
            .request(&botster_hub_client::DaemonRequest::Status)
            .expect("admitted client remains healthy after rejection")
            .status
            .expect("post-rejection status body")
            .lifecycle_counters;
        if rejected_status.rejected_connections >= 1 {
            break;
        }
        assert!(
            Instant::now() < rejection_counter_deadline,
            "typed rejection counter did not converge"
        );
        thread::sleep(Duration::from_millis(20));
    }
    drop(stalled_rejection);
    drop(rejected);
    drop(idle_connections);
    let release_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            match botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            {
                Ok(response) => {
                    response
                        .status
                        .expect("released-connection status body")
                        .lifecycle_counters
                }
                Err(_) if Instant::now() < release_deadline => {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(error) => panic!("connection admission did not recover: {error}"),
            };
        if counters.live_connections <= 1 {
            break;
        }
        assert!(
            Instant::now() < release_deadline,
            "idle connection owners did not release: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let mut malformed =
        UnixStream::connect(&endpoint.socket_path).expect("connect malformed raw daemon client");
    botster_hub_client::write_frame(
        &mut malformed,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            terminal_compatibility: None,
        },
    )
    .expect("write malformed-client hello");
    let _: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut malformed).expect("read malformed-client hello ack");
    malformed
        .write_all(b"{\"type\":}\n")
        .expect("write malformed complete frame");
    drop(malformed);

    let mut half_open =
        UnixStream::connect(&endpoint.socket_path).expect("connect half-open raw daemon client");
    half_open
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("bound half-open handshake close observation");
    let mut closed = [0_u8; 1];
    assert!(
        half_open.read(&mut closed).is_ok_and(|count| count == 0),
        "half-open handshake deadline must close the connection"
    );
    drop(half_open);

    let mut incomplete =
        UnixStream::connect(&endpoint.socket_path).expect("connect incomplete raw daemon client");
    botster_hub_client::write_frame(
        &mut incomplete,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            terminal_compatibility: None,
        },
    )
    .expect("write incomplete-client hello");
    let _: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut incomplete).expect("read incomplete-client hello ack");
    incomplete
        .write_all(b"{\"type\":\"status\"")
        .expect("write incomplete frame");
    incomplete
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("bound incomplete-frame close observation");
    assert!(
        incomplete.read(&mut closed).is_ok_and(|count| count == 0),
        "incomplete frame deadline must close the connection"
    );
    drop(incomplete);

    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after malformed and incomplete clients")
                .status
                .expect("terminal cleanup status body")
                .lifecycle_counters;
        if counters
            .cleanup_by_reason
            .get("protocol")
            .copied()
            .unwrap_or_default()
            >= 3
            && counters.live_connections <= 1
        {
            assert!(counters.cleanup_completed >= 67);
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "transport cleanup counters did not settle: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    for index in 1..8 {
        let session_id = format!("focused-idle-session-{index}");
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ShutdownSession {
                session_id: session_id.clone(),
            },
        )
        .expect("shutdown focused session-count fixture");
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::RemoveSession { session_id },
        )
        .expect("remove focused session-count fixture");
    }

    daemon.shutdown();
}

#[test]
fn session_projection_observes_exit_without_subscribers_then_later_snapshot_includes_ended_row() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-projection-zero-subs");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "zero-sub-exit".to_string(),
            command: "sleep 0.1".to_string(),
        },
    )
    .expect("spawn session with no entity subscribers");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut listed = None;
    while Instant::now() < deadline {
        listed =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
                .ok()
                .and_then(|response| {
                    response.sessions.iter().find_map(|session| {
                        (session.session_id == "zero-sub-exit").then(|| session.lifecycle.clone())
                    })
                });
        if listed.as_deref() == Some("exited") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        listed.as_deref(),
        Some("exited"),
        "owner loop must observe natural exit with zero entity subscribers"
    );

    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-after-exit")
            .expect("subscribe after unobserved-by-clients exit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound later subscriber reads");
    let snapshot = subscription
        .next_frame()
        .expect("later subscriber snapshot");
    let botster_hub_client::DaemonEntityFrame::Snapshot { items, .. } = snapshot else {
        panic!("expected snapshot, got {snapshot:?}");
    };
    assert!(
        items.iter().any(|item| {
            item.get("session_uuid").and_then(serde_json::Value::as_str) == Some("zero-sub-exit")
                && item.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited")
        }),
        "later subscriber must receive the ended row, items={items:?}"
    );
    subscription
        .unsubscribe()
        .expect("unsubscribe later subscriber");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn shutdown_after_observed_exit_returns_session_cleanup() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("shutdown-after-observed-exit");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "observed-exit-shutdown".to_string(),
            command: "sleep 0.1".to_string(),
        },
    )
    .expect("spawn short-lived session");
    wait_for_authoritative_session_exit(&endpoint, "observed-exit-shutdown");
    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "observed-exit-shutdown".to_string(),
        },
    )
    .expect("shutdown after observed exit");
    assert_session_cleanup_already_exited(
        &shutdown,
        "observed-exit-shutdown",
        "unix shutdown after observed exit",
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn final_cleanup_accepts_already_exited_without_altering_sibling() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("final-cleanup-already-exited");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "final-cleanup-exited".to_string(),
            command: "sleep 0.1".to_string(),
        },
    )
    .expect("spawn short-lived cleanup session");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "final-cleanup-sibling".to_string(),
            command: "sleep 8".to_string(),
        },
    )
    .expect("spawn cleanup sibling");
    wait_for_authoritative_session_exit(&endpoint, "final-cleanup-exited");

    let sibling_before = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ListSessions,
    )
    .expect("list sessions before final cleanup")
    .sessions
    .into_iter()
    .find(|session| session.session_id == "final-cleanup-sibling")
    .expect("sibling before final cleanup");

    production_shutdown_and_remove_session(&endpoint, "final-cleanup-exited");

    let sessions_after = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ListSessions,
    )
    .expect("list sessions after final cleanup")
    .sessions;
    assert!(
        sessions_after
            .iter()
            .all(|session| session.session_id != "final-cleanup-exited"),
        "final cleanup must remove the already exited target"
    );
    let sibling_after = sessions_after
        .into_iter()
        .find(|session| session.session_id == "final-cleanup-sibling")
        .expect("sibling after final cleanup");
    assert_eq!(
        sibling_after, sibling_before,
        "final cleanup for an already exited session must not alter its sibling"
    );

    production_shutdown_and_remove_session(&endpoint, "final-cleanup-sibling");
    shutdown_cli_daemon(&data_dir, child);
}

/// Live ShutdownSession Runtime/State failure must not sacrifice the daemon,
/// the reused control connection, or a sibling session adapter.
///
/// Core-error branch closes victim-session adapters
/// (`src/daemon/control/sessions.rs`). Cleanup-branch keep-open
/// is a different path. This test hits the Core-error
/// branch and proves the same `DaemonConnection` plus the sibling
/// Attach route still deliver terminal envelopes.
///
/// ReadScreen is session-health only. It does not prove the Attach
/// adapter. Sibling adapter survival is `take_skipped_terminal` for
/// the sibling session and subscription, plus Status occupancy.
///
/// Single constructions at Core pin fc541a5:
/// (a) SIGKILL-then-shutdown returned SessionCleanup after Core observed
/// ProcessExited. (b) drain injection plus egress capacity 1 returned
/// Events after Core shutdown completed. Compound: drain injection makes
/// classify fall through, then SIGKILL makes the live Core shutdown fail.
#[test]
fn external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("shutdown-failure-sibling");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let daemon = PanicSafeCliDaemon::start_with_runtime_drain_failure(
        &data_dir,
        "shutdown-failure-victim",
        None,
        "shutdown-failure-sibling daemon evidence",
    );
    let hub_pid = daemon.child.as_ref().expect("panic-safe daemon child").id();
    let socket_path = endpoint.socket_path.clone();
    let pty_marker = format!(
        "pty-marker-{}",
        data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("unique data dir name")
    );
    let victim_wrapper = write_marked_sleep_wrapper(
        &data_dir,
        &format!("{pty_marker}-victim"),
    );
    let sibling_wrapper = write_marked_echo_wrapper(
        &data_dir,
        &format!("{pty_marker}-sibling"),
    );
    let before_pids: std::collections::BTreeSet<u32> = session_worker_process_identities()
        .expect("baseline worker census must succeed")
        .into_iter()
        .map(|worker| worker.pid)
        .collect();

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "shutdown-failure-victim".to_string(),
            command: victim_wrapper.display().to_string(),
        },
    )
    .expect("spawn victim session");
    let victim_cleanup = SessionCleanupGuard::new(&data_dir, "shutdown-failure-victim");
    let victim_workers = capture_new_session_workers_for_marked_pty(
        &data_dir,
        &before_pids,
        &format!("{pty_marker}-victim"),
    )
    .expect("must capture victim botster-session-worker after Spawn");
    assert!(
        !victim_workers.is_empty(),
        "must capture live victim worker before SIGKILL"
    );

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "shutdown-failure-sibling".to_string(),
            command: sibling_wrapper.display().to_string(),
        },
    )
    .expect("spawn sibling session");
    let mut sibling_cleanup = SessionCleanupGuard::new(&data_dir, "shutdown-failure-sibling");
    let pty_children = unix_processes_matching_marker(&pty_marker)
        .expect("all-session PTY marker census after sibling spawn");
    assert!(
        !pty_children.is_empty(),
        "must observe marked PTY children across Unix sessions before SIGKILL: marker={pty_marker}"
    );

    let sibling_session = "shutdown-failure-sibling";
    let sibling_subscription = "shutdown-failure-sibling-sub";
    let victim_session = "shutdown-failure-victim";
    let victim_subscription = "shutdown-failure-victim-sub";
    let mut connection = botster_hub_client::DaemonConnection::connect_with_requirement(
        &endpoint,
        &botster_hub_client::DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
    )
    .expect("open one unix-adapter control connection for failure and sibling proof");
    let victim_attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: victim_session.to_string(),
            subscription_id: victim_subscription.to_string(),
        })
        .expect("attach victim before failed shutdown");
    assert_eq!(
        victim_attach.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "victim Attach must succeed before failed shutdown, got kind={:?} error={:?}",
        victim_attach.kind,
        victim_attach.error
    );
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: sibling_session.to_string(),
            subscription_id: sibling_subscription.to_string(),
        })
        .expect("attach sibling before victim shutdown failure");
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "sibling Attach must succeed before victim shutdown, got kind={:?} error={:?}",
        attach.kind,
        attach.error
    );
    connection
        .send_terminal_frame(
            sibling_session,
            sibling_subscription,
            &terminal_resize_frame_bytes(24, 80),
        )
        .expect("resize sibling before victim shutdown failure");
    let ready = wait_for_read_screen_contains(&mut connection, sibling_session, "ready");
    assert!(
        ready.contains("ready"),
        "sibling session must be live before victim shutdown, got {ready:?}"
    );
    connection
        .send_terminal_frame(
            sibling_session,
            sibling_subscription,
            &terminal_input_frame_bytes(b"before\r"),
        )
        .expect("sibling input before victim shutdown failure");
    let before_envelope = wait_for_sibling_terminal_envelope(
        &mut connection,
        sibling_session,
        sibling_subscription,
        "echo:before",
    );
    assert_eq!(before_envelope.session_id, sibling_session);
    assert_eq!(before_envelope.subscription_id, sibling_subscription);
    discard_unsolicited_terminal(&mut connection);

    let status_before = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status before failed shutdown")
        .status
        .expect("status body before failed shutdown");
    let host_closes_before = status_before
        .lifecycle_counters
        .cleanup_by_reason
        .get("shutdown_error_host_close")
        .copied()
        .unwrap_or(0);
    let occupancy_before = status_before.live_attach_occupancy;
    let victim_generation = occupancy_before
        .iter()
        .find_map(|row| {
            (row.session_id == victim_session && row.subscription_id == victim_subscription)
                .then_some(row.generation)
        })
        .expect("victim Attach must publish a Core-issued generation");
    let _ = connection.take_skipped_events();
    let _ = connection.take_skipped_terminal();

    for worker in &victim_workers {
        let result = unsafe { libc::kill(worker.pid as libc::pid_t, libc::SIGKILL) };
        assert_eq!(
            result, 0,
            "SIGKILL victim worker pid={} errno={}",
            worker.pid,
            std::io::Error::last_os_error()
        );
    }

    let shutdown = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "shutdown-failure-victim".to_string(),
        })
        .expect("shutdown killed victim through production handler");
    assert_eq!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "compound drain-inject plus SIGKILL ShutdownSession must return OperatorError, got kind={:?} error={:?} cleanup={:?}",
        shutdown.kind,
        shutdown.error,
        shutdown.cleanup
    );
    let error = shutdown.error.as_ref().expect("shutdown operator error body");
    assert_eq!(
        error.code, "runtime_error",
        "compound drain-inject plus SIGKILL must return runtime_error, got {error:?}"
    );
    assert_eq!(error.request_id, "daemon-sessions-shutdown");
    assert_eq!(error.operation, "shutdown");
    assert_eq!(
        error.message, "runtime failed while handling Shutdown: Runtime",
        "exact Core-error OperatorError message, got {error:?}"
    );
    assert!(
        error.diagnostics.is_empty(),
        "Shutdown Runtime OperatorError has no diagnostics, got {error:?}"
    );

    let mut close_events = connection.take_skipped_events();
    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after victim shutdown failure");
    assert_eq!(
        status.kind,
        botster_hub_client::DaemonResponseKind::Status,
        "same control connection Status must survive victim ShutdownSession failure, got {:?}",
        status.kind
    );
    let occupancy = &status
        .status
        .as_ref()
        .expect("status body after victim shutdown failure")
        .live_attach_occupancy;
    close_events.extend(connection.take_skipped_events());
    assert!(
        shutdown_failure_occupancy_has_pair(occupancy, sibling_session, sibling_subscription),
        "sibling attach occupancy must survive victim ShutdownSession failure, occupancy={occupancy:?}"
    );
    assert!(
        shutdown_failure_occupancy_has_pair(occupancy, victim_session, victim_subscription),
        "failed ShutdownSession keeps the still-Active victim in the occupancy union, occupancy={occupancy:?}"
    );
    let host_closes_after = status
        .status
        .as_ref()
        .expect("status body after victim shutdown failure")
        .lifecycle_counters
        .cleanup_by_reason
        .get("shutdown_error_host_close")
        .copied()
        .unwrap_or(0);
    assert!(
        host_closes_after > host_closes_before,
        "failed ShutdownSession must host-close the bound victim adapter: before={host_closes_before} after={host_closes_after} occupancy={occupancy:?}"
    );
    assert!(
        close_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    subscription_id,
                    generation,
                    ..
                } if session_id == victim_session
                    && subscription_id == victim_subscription
                    && *generation == victim_generation
            )
        }),
        "failed ShutdownSession must host-close the victim adapter under suppression with no TerminalSubscriptionClosed for generation {victim_generation}: {close_events:?}"
    );

    connection
        .send_terminal_frame(
            sibling_session,
            sibling_subscription,
            &terminal_input_frame_bytes(b"adapter-alive\r"),
        )
        .expect("sibling input after victim shutdown failure");
    let alive_envelope = wait_for_sibling_terminal_envelope(
        &mut connection,
        sibling_session,
        sibling_subscription,
        "echo:adapter-alive",
    );
    assert_eq!(alive_envelope.session_id, sibling_session);
    assert_eq!(alive_envelope.subscription_id, sibling_subscription);
    close_events.extend(connection.take_skipped_events());
    assert!(
        close_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    subscription_id,
                    generation,
                    ..
                } if session_id == victim_session
                    && subscription_id == victim_subscription
                    && *generation == victim_generation
            )
        }),
        "keep-reading after failed shutdown must not emit victim generation {victim_generation}: {close_events:?}"
    );
    let observed = wait_for_read_screen_contains(
        &mut connection,
        sibling_session,
        "echo:adapter-alive",
    );
    assert!(
        observed.contains("echo:adapter-alive"),
        "sibling ReadScreen is session-health only and must still show the marker, got {observed:?}"
    );

    let listed = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after victim shutdown failure");
    assert!(
        listed.sessions.iter().any(|session| {
            session.session_id == sibling_session && session.lifecycle == "running"
        }),
        "sibling must remain listed and running, sessions={:?}",
        listed.sessions
    );

    let detach = connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: sibling_session.to_string(),
            subscription_id: sibling_subscription.to_string(),
        })
        .expect("detach sibling adapter for ablation");
    assert_eq!(
        detach.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "sibling Detach ablation must succeed, got kind={:?} error={:?}",
        detach.kind,
        detach.error
    );
    discard_unsolicited_terminal(&mut connection);
    let after_detach_status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after sibling Detach ablation");
    let after_detach_occupancy = &after_detach_status
        .status
        .as_ref()
        .expect("status body after sibling Detach")
        .live_attach_occupancy;
    assert!(
        !shutdown_failure_occupancy_has_pair(
            after_detach_occupancy,
            sibling_session,
            sibling_subscription
        ),
        "Detach ablation must drop sibling occupancy, occupancy={after_detach_occupancy:?}"
    );
    let after_detach_screen =
        wait_for_read_screen_contains(&mut connection, sibling_session, "echo:adapter-alive");
    assert!(
        after_detach_screen.contains("echo:adapter-alive"),
        "ReadScreen must remain session-health after Detach, got {after_detach_screen:?}"
    );

    drop(victim_cleanup);
    sibling_cleanup.disarm();
    production_shutdown_and_remove_session(&endpoint, "shutdown-failure-sibling");
    drop(connection);
    daemon.shutdown();
    reap_session_workers_for_data_dir(&data_dir)
        .expect("sibling SIGKILL fixture must not leave worktree session workers");
    assert_cli_fixture_absent(
        &data_dir,
        hub_pid,
        &socket_path,
        &pty_marker,
        &pty_children,
        &["shutdown-failure-victim", "shutdown-failure-sibling"],
    );
}

#[test]
fn panic_safe_cli_daemon_deliberate_failure_leaves_no_owned_survivors() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("shutdown-failure-panic");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path.clone());
    let pty_marker = format!(
        "pty-marker-{}",
        data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("unique data dir name")
    );
    let victim_wrapper = write_marked_sleep_wrapper(&data_dir, &pty_marker);
    let hub_pid = std::sync::atomic::AtomicU32::new(0);
    let captured_pty = std::sync::Mutex::new(Vec::new());
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let daemon = PanicSafeCliDaemon::start_with_runtime_drain_failure(
            &data_dir,
            "shutdown-failure-panic-victim",
            None,
            "deliberate shutdown-failure panic daemon evidence",
        );
        hub_pid.store(
            daemon.child.as_ref().expect("panic-safe daemon child").id(),
            std::sync::atomic::Ordering::SeqCst,
        );
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: "shutdown-failure-panic-victim".to_string(),
                command: victim_wrapper.display().to_string(),
            },
        )
        .expect("spawn panic-path victim");
        let _session_cleanup =
            SessionCleanupGuard::new(&data_dir, "shutdown-failure-panic-victim");
        let pty_children = unix_processes_matching_marker(&pty_marker)
            .expect("all-session PTY marker census before deliberate panic");
        assert!(
            !pty_children.is_empty(),
            "must capture marked PTY children before deliberate panic: marker={pty_marker}"
        );
        *captured_pty
            .lock()
            .expect("store captured PTY children") = pty_children;
        panic!("deliberate shutdown-failure fixture panic");
    }));
    assert!(
        panicked.is_err(),
        "panic-path owner proof must take the failure branch"
    );
    let pty_children = captured_pty
        .into_inner()
        .expect("recover captured PTY children");
    assert_cli_fixture_absent(
        &data_dir,
        hub_pid.load(std::sync::atomic::Ordering::SeqCst),
        &socket_path,
        &pty_marker,
        &pty_children,
        &["shutdown-failure-panic-victim"],
    );
}

#[test]
fn assert_cli_fixture_absent_fails_when_setsid_child_survives() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("pty-setsid-negative");
    let marker = format!(
        "pty-marker-{}",
        data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("unique data dir name")
    );
    let wrapper = write_marked_sleep_wrapper(&data_dir, &marker);
    let owner = PanicSafeSetsidChild::spawn(&marker, &wrapper);
    let pty_children = vec![owner.identity().clone()];
    let parent_sid = unsafe { libc::getsid(0) };
    assert!(
        owner.identity().sid != parent_sid,
        "negative control child must leave the test SID via setsid: parent_sid={parent_sid} child={:?}",
        owner.identity()
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match unix_processes_matching_marker(&marker) {
            Ok(found) if !found.is_empty() => break,
            Ok(_) => {}
            Err(error) => panic!("all-session census for setsid negative control: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "negative control must observe the independently sessioned child: marker={marker}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let mut dead = Command::new("sh")
        .arg("-c")
        .arg("exit 0")
        .spawn()
        .expect("spawn dead-pid decoy");
    let dead_pid = dead.id();
    dead.wait().expect("reap dead-pid decoy");
    let socket_path = data_dir.join("missing.sock");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_cli_fixture_absent(
            &data_dir,
            dead_pid,
            &socket_path,
            &marker,
            &pty_children,
            &[],
        );
    }));
    owner.reap();
    reap_captured_pty_children(&pty_children)
        .expect("negative control must reap the captured setsid process group");
    reap_processes_matching_marker(&marker)
        .expect("negative control must reap leftover marked argv rows");
    let leftovers = live_captured_pty_rows(&pty_children)
        .expect("negative control leftover census");
    assert!(
        leftovers.is_empty(),
        "negative control must not leak marker-less sleep children: {leftovers:?}"
    );
    assert!(
        failed.is_err(),
        "survivor oracle must fail while a representative setsid child remains alive"
    );
}

#[test]
fn panic_safe_setsid_owner_reaps_group_and_pipe_after_forced_error() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("pty-setsid-panic-owner");
    let marker = format!(
        "pty-marker-{}",
        data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("unique data dir name")
    );
    let wrapper = write_marked_sleep_wrapper(&data_dir, &marker);
    let mut captured_identity = None;
    let mut captured_stdout = None;
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut owner = PanicSafeSetsidChild::spawn(&marker, &wrapper);
        captured_identity = Some(owner.identity().clone());
        captured_stdout = owner.take_stdout();
        panic!("forced first-census error before setsid cleanup");
    }));
    assert!(failed.is_err(), "forced error must unwind before cleanup");
    let identity = captured_identity.expect("identity captured before panic");
    assert!(
        !process_is_alive_u32(identity.pid).unwrap_or(true),
        "Drop must reap the exact setsid child: {identity:?}"
    );
    assert!(
        !process_group_probe(identity.pgid).unwrap_or(true),
        "Drop must reap the detached PGID without a prior census: {identity:?}"
    );
    let leftovers = live_captured_pty_rows(std::slice::from_ref(&identity))
        .expect("post-cleanup census oracle");
    assert!(
        leftovers.is_empty(),
        "post-cleanup oracle must see an empty captured PGID: {leftovers:?}"
    );
    let stdout = captured_stdout.expect("piped stdout captured before panic");
    assert!(
        stdout_pipe_is_closed(&stdout),
        "Drop must close the write end so a piped runner cannot hang"
    );
    reap_processes_matching_marker(&marker)
        .expect("forced-error owner must leave no marked argv rows");
}

#[test]
// ReadScreen parks ProcessExited. observe_session_lifecycle must reconcile
// that row and finish ShutdownSession as SessionCleanup or Events. Live
// Active→Events is covered by
// external_hub_webrtc_live_output_preserves_exact_bytes.
fn shutdown_session_classifies_parked_exit_beyond_one_baseline_page() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("shutdown-parked-exact-session");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "mmm-target".to_string(),
            command: "printf 'mmm-target-ready\\n'".to_string(),
        },
    )
    .expect("spawn target session");
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("open host connection for ReadScreen");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut screen = String::new();
    while Instant::now() < deadline {
        let response = connection
            .request(&botster_hub_client::DaemonRequest::ReadScreen {
                session_id: "mmm-target".to_string(),
            })
            .expect("read target screen");
        screen = response
            .read_screen
            .as_ref()
            .map(|body| body.text.clone())
            .unwrap_or_default();
        if screen.contains("mmm-target-ready") {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        screen.contains("mmm-target-ready"),
        "ReadScreen must observe target output before ShutdownSession, last={screen:?}"
    );
    let started = Instant::now();
    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "mmm-target".to_string(),
        },
    )
    .expect("shutdown parked late session");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "ShutdownSession must finish the exact-session lookup, elapsed={elapsed:?}"
    );
    assert!(
        matches!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
                | botster_hub_client::DaemonResponseKind::SessionCleanup
        ),
        "parked ProcessExited must complete ShutdownSession, got {:?}",
        shutdown.kind
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_observes_natural_exit_without_terminal_attach() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-no-terminal");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-no-terminal")
            .expect("subscribe without terminal attach");
    subscription
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("bound entity read");
    let _ = subscription.next_frame().expect("initial snapshot");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-no-terminal".to_string(),
            command: "sleep 0.1".to_string(),
        },
    )
    .expect("spawn session without terminal attach");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw_ended = false;
    while Instant::now() < deadline {
        match subscription.next_frame() {
            Ok(botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. })
                if id == "entity-no-terminal"
                    && entity
                        .get("lifecycle_class")
                        .and_then(serde_json::Value::as_str)
                        == Some("ended") =>
            {
                saw_ended = true;
                break;
            }
            Ok(botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. })
                if id == "entity-no-terminal"
                    && (patch.get("lifecycle").and_then(serde_json::Value::as_str)
                        == Some("exited")
                        || patch
                            .get("lifecycle_class")
                            .and_then(serde_json::Value::as_str)
                            == Some("ended")) =>
            {
                saw_ended = true;
                break;
            }
            Ok(botster_hub_client::DaemonEntityFrame::Snapshot { items, .. })
                if items.iter().any(|entity| {
                    entity
                        .get("session_uuid")
                        .and_then(serde_json::Value::as_str)
                        == Some("entity-no-terminal")
                        && entity
                            .get("lifecycle_class")
                            .and_then(serde_json::Value::as_str)
                            == Some("ended")
                }) =>
            {
                saw_ended = true;
                break;
            }
            Ok(_) => {}
            Err(botster_hub_client::DaemonTransportError::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => panic!("natural exit wait failed: {error}"),
        }
    }
    assert!(
        saw_ended,
        "Hub projection must prove ended without a terminal Drain"
    );
    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

fn session_entity_is_ended_zero_sub(entity: &serde_json::Value) -> bool {
    entity
        .get("session_uuid")
        .and_then(serde_json::Value::as_str)
        == Some("zero-sub-ended")
        && entity
            .get("lifecycle_class")
            .and_then(serde_json::Value::as_str)
            == Some("ended")
}

#[test]
fn zero_subscribers_still_project_a_complete_ended_row() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("zero-subscriber-projection");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "zero-sub-ended".to_string(),
            command: "sleep 0.05".to_string(),
        },
    )
    .expect("spawn without subscribers");
    thread::sleep(Duration::from_millis(800));
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "late-zero-sub")
            .expect("late subscribe after natural exit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound late snapshot");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_ended = false;
    while Instant::now() < deadline {
        match subscription.next_frame() {
            Ok(botster_hub_client::DaemonEntityFrame::Snapshot { items, .. }) => {
                if items.iter().any(session_entity_is_ended_zero_sub) {
                    saw_ended = true;
                    break;
                }
            }
            Ok(botster_hub_client::DaemonEntityFrame::Upsert { id, entity, .. })
                if id == "zero-sub-ended" && session_entity_is_ended_zero_sub(&entity) =>
            {
                saw_ended = true;
                break;
            }
            Ok(botster_hub_client::DaemonEntityFrame::Patch { id, patch, .. })
                if id == "zero-sub-ended"
                    && patch
                        .get("lifecycle_class")
                        .and_then(serde_json::Value::as_str)
                        == Some("ended") =>
            {
                saw_ended = true;
                break;
            }
            Ok(_) => {}
            Err(error) => panic!("late projection wait failed: {error}"),
        }
    }
    assert!(saw_ended, "zero-subscriber projection must prove ended");
    subscription
        .unsubscribe()
        .expect("unsubscribe late snapshot");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn ready_spawn_completes_when_live_sessions_exceed_one_observe_slice() {
    // Control-before-maintenance ordering is proven by
    // `queued_control_precedes_a_due_maintenance_slice` in
    // `src/daemon/owner_loop.rs`. End-to-end wall-clock latency through a
    // daemon child measures ambient machine load and is recorded only.
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("observe-slice-load");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    for index in 0..24 {
        let session_id = format!("load-session-{index}");
        let spawn = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.clone(),
                command: "sleep 8".to_string(),
            },
        )
        .unwrap_or_else(|error| panic!("spawn {session_id}: {error}"));
        assert_eq!(
            spawn.kind,
            botster_hub_client::DaemonResponseKind::Spawned,
            "load spawn {session_id} must succeed: {spawn:?}"
        );
    }
    let mut first = botster_hub_client::subscribe_session_entities(&endpoint, "load-sub-one")
        .expect("first load subscriber");
    let mut second = botster_hub_client::subscribe_session_entities(&endpoint, "load-sub-two")
        .expect("second load subscriber");
    let started = Instant::now();
    let ready = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "load-ready-spawn".to_string(),
            command: "sleep 0.05".to_string(),
        },
    )
    .expect("ready spawn during loaded observe");
    assert_eq!(
        ready.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "ready spawn must succeed: {ready:?}"
    );
    let waited = started.elapsed();
    eprintln!("ready spawn duration observation (observe-slice load): {waited:?}");
    let first_snapshot = read_first_session_snapshot(&mut first);
    let second_snapshot = read_first_session_snapshot(&mut second);
    assert_first_snapshot_contains_load_sessions(&first_snapshot);
    assert_first_snapshot_contains_load_sessions(&second_snapshot);
    let _ = first.unsubscribe();
    let _ = second.unsubscribe();
    shutdown_cli_daemon(&data_dir, child);
}

fn load_session_id(index: usize) -> String {
    format!("load-session-{index}")
}

fn expected_load_session_ids() -> std::collections::BTreeSet<String> {
    (0..24).map(load_session_id).collect()
}

fn assert_first_snapshot_contains_load_sessions(
    frame: &botster_hub_client::DaemonEntityFrame,
) -> std::collections::BTreeSet<String> {
    let seen = snapshot_session_identities(frame);
    let expected = expected_load_session_ids();
    let missing: Vec<String> = expected.difference(&seen).cloned().collect();
    assert!(
        missing.is_empty(),
        "first Snapshot must contain all 24 load identities; missing={missing:?}; seen={}",
        seen.len()
    );
    seen
}

fn assemble_session_id(index: usize) -> String {
    format!("assemble-session-{index:02}")
}

fn expected_assemble_session_ids() -> std::collections::BTreeSet<String> {
    (0..24).map(assemble_session_id).collect()
}

fn assemble_sessions_are_ready(seen: &std::collections::BTreeSet<String>) -> bool {
    seen == &expected_assemble_session_ids()
}

fn session_identities_from_entity_frame(
    frame: &botster_hub_client::DaemonEntityFrame,
) -> Result<Vec<String>, String> {
    match frame {
        botster_hub_client::DaemonEntityFrame::Error { code, message, .. } => {
            Err(format!("assemble subscription error: {code}: {message}"))
        }
        botster_hub_client::DaemonEntityFrame::Snapshot { items, .. } => Ok(items
            .iter()
            .filter_map(|entity| {
                entity
                    .get("session_uuid")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect()),
        botster_hub_client::DaemonEntityFrame::Upsert { id, .. }
        | botster_hub_client::DaemonEntityFrame::Patch { id, .. } => Ok(vec![id.clone()]),
        botster_hub_client::DaemonEntityFrame::Remove { .. } => Ok(Vec::new()),
    }
}

fn snapshot_session_identities(
    frame: &botster_hub_client::DaemonEntityFrame,
) -> std::collections::BTreeSet<String> {
    session_identities_from_entity_frame(frame)
        .unwrap_or_else(|error| panic!("{error}"))
        .into_iter()
        .collect()
}

fn assert_first_snapshot_contains_assemble_sessions(
    frame: &botster_hub_client::DaemonEntityFrame,
) -> std::collections::BTreeSet<String> {
    let seen = snapshot_session_identities(frame);
    let expected = expected_assemble_session_ids();
    let missing: Vec<String> = expected.difference(&seen).cloned().collect();
    assert!(
        missing.is_empty(),
        "first Snapshot must contain all 24 assemble identities; missing={missing:?}; seen={}",
        seen.len()
    );
    seen
}

fn read_first_session_snapshot(
    subscription: &mut botster_hub_client::DaemonEntitySubscription,
) -> botster_hub_client::DaemonEntityFrame {
    subscription
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("liveness bound for first session snapshot");
    loop {
        match subscription.next_frame() {
            Ok(frame @ botster_hub_client::DaemonEntityFrame::Snapshot { .. }) => return frame,
            Ok(botster_hub_client::DaemonEntityFrame::Error { code, message, .. }) => {
                panic!("assemble subscription error: {code}: {message}");
            }
            Ok(_) => {}
            Err(error) => panic!("first Snapshot liveness wait failed: {error}"),
        }
    }
}

#[test]
fn assemble_subscription_rejects_an_error_frame() {
    let error = botster_hub_client::DaemonEntityFrame::Error {
        subscription_id: "assemble-sub".to_string(),
        entity_type: "session".to_string(),
        code: "projection".to_string(),
        message: "typed failure".to_string(),
    };
    assert!(
        session_identities_from_entity_frame(&error).is_err(),
        "a typed Error frame must not count as projected identities"
    );
}

#[test]
fn assemble_readiness_rejects_a_partial_identity_set() {
    let empty = std::collections::BTreeSet::new();
    assert!(
        !assemble_sessions_are_ready(&empty),
        "an empty projected set must not be ready"
    );
    let mut seen = std::collections::BTreeSet::new();
    for index in 0..17 {
        seen.insert(assemble_session_id(index));
    }
    assert!(
        !assemble_sessions_are_ready(&seen),
        "a partial projected set must not be ready"
    );
    for index in 17..24 {
        seen.insert(assemble_session_id(index));
    }
    assert!(
        assemble_sessions_are_ready(&seen),
        "all 24 load-session identities must be ready"
    );
}

#[test]
fn ready_spawn_completes_during_session_snapshot_assembly() {
    // Control-before-maintenance ordering is proven by
    // `queued_control_precedes_a_due_maintenance_slice` in
    // `src/daemon/owner_loop.rs`. End-to-end wall-clock latency through a
    // daemon child measures ambient machine load and is recorded only.
    // The first Snapshot is the production completeness oracle: it must
    // contain all 24 assemble identities. A later ready-spawn row is a
    // permitted superset. Fail immediately on DaemonEntityFrame::Error.
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("snapshot-assemble-ready");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    for index in 0..24 {
        let session_id = assemble_session_id(index);
        let spawn = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.clone(),
                command: "sleep 8".to_string(),
            },
        )
        .unwrap_or_else(|error| panic!("spawn {session_id}: {error}"));
        assert_eq!(
            spawn.kind,
            botster_hub_client::DaemonResponseKind::Spawned,
            "assemble spawn {session_id} must succeed: {spawn:?}"
        );
    }
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "assemble-sub")
            .expect("assemble subscriber");
    let started = Instant::now();
    let ready = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "assemble-ready-spawn".to_string(),
            command: "sleep 0.05".to_string(),
        },
    )
    .expect("ready spawn during snapshot assembly");
    assert_eq!(
        ready.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "ready spawn must succeed: {ready:?}"
    );
    let waited = started.elapsed();
    eprintln!("ready spawn duration observation (snapshot assembly): {waited:?}");
    let first = read_first_session_snapshot(&mut subscription);
    assert_first_snapshot_contains_assemble_sessions(&first);
    let _ = subscription.unsubscribe();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn first_session_snapshot_arrives_after_projected_spawn_is_removed() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("retire-ack-resubscribe");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let session_id = "retire-session-00";
    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 8".to_string(),
        },
    )
    .unwrap_or_else(|error| panic!("spawn {session_id}: {error}"));
    assert_eq!(
        spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned,
        "retire spawn must succeed: {spawn:?}"
    );
    let mut first = botster_hub_client::subscribe_session_entities(&endpoint, "retire-sub-one")
        .expect("first retire subscriber");
    let first_snapshot = read_first_session_snapshot(&mut first);
    let first_ids = snapshot_session_identities(&first_snapshot);
    assert!(
        first_ids.contains(session_id),
        "first Snapshot must contain the spawned session; seen={first_ids:?}"
    );
    first.unsubscribe().expect("unsubscribe first retire subscriber");
    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown projected spawn");
    assert_eq!(
        shutdown.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "shutdown must succeed: {shutdown:?}"
    );
    let mut second = botster_hub_client::subscribe_session_entities(&endpoint, "retire-sub-two")
        .expect("second retire subscriber");
    let second_snapshot = read_first_session_snapshot(&mut second);
    match &second_snapshot {
        botster_hub_client::DaemonEntityFrame::Snapshot { .. } => {}
        other => panic!("second subscribe must receive a Snapshot, got {other:?}"),
    }
    second
        .unsubscribe()
        .expect("unsubscribe second retire subscriber");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn shutdown_from_another_connection_preserves_process_exit_for_attached_subscription() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cross-shutdown-egress");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let session_id = "cross-connection-shutdown";
    let subscription_id = "cross-connection-terminal";
    let marker_path = data_dir.join("natural-exit-marker");
    let release_path = data_dir.join("natural-exit-release");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "printf ready; IFS= read -r line; printf 'cross-connection-exiting:%s\\n' \"$line\"; \
                 printf observed > '{}'; while [ ! -e '{}' ]; do sleep 0.01; done; \
                 printf 'cross-connection-tail\\n'; exit 0",
                marker_path.display(),
                release_path.display()
            ),
        },
    )
    .expect("spawn cross-connection shutdown session");
    let mut attached =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    attached
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("attach terminal subscription");
    let attached_screen = wait_for_read_screen_contains(&mut attached, session_id, "ready");
    assert!(
        attached_screen.contains("ready"),
        "cross-connection fixture must be readable before input: {attached_screen:?}"
    );
    attached
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_input_frame_bytes(b"finish\r"),
        )
        .expect("release terminal fixture to its exit marker");
    for _ in 0..500 {
        if marker_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        marker_path.exists(),
        "terminal fixture did not publish its marker"
    );

    let mut observed_marker = false;
    for _ in 0..100 {
        let marker_drain = attached
            .request(&botster_hub_client::DaemonRequest::Status)
            .expect("drain terminal marker before natural exit");
        observed_marker |= marker_drain.events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                    if live_output_contains(payload, "cross-connection-exiting")
            )
        });
        if !observed_marker {
            observed_marker = attached
                .request(&botster_hub_client::DaemonRequest::ReadScreen {
                    session_id: session_id.to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .is_some_and(|screen| screen.text.contains("cross-connection-exiting"));
        }
        assert!(
            marker_drain
                .events
                .iter()
                .all(|event| !matches!(event, botster_hub_client::DaemonEvent::ProcessExit { .. })),
            "fixture must remain live until its explicit release: {:?}",
            marker_drain.events
        );
        if observed_marker {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        observed_marker,
        "attached subscription did not observe the exit marker"
    );

    let registry: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join("sessions").join(format!("{session_id}.json")))
            .expect("read worker session registry"),
    )
    .expect("parse worker session registry");
    let pty_child_pid = registry["process"]["pid"]
        .as_u64()
        .expect("registry PTY child pid") as u32;
    let worker_socket = PathBuf::from(
        registry["recovery_identity"]["worker_control_socket"]
            .as_str()
            .expect("registry worker control socket"),
    );
    fs::write(&release_path, b"release").expect("release controlled natural exit");
    for _ in 0..500 {
        if !process_exists(pty_child_pid) && UnixStream::connect(&worker_socket).is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !process_exists(pty_child_pid) && UnixStream::connect(&worker_socket).is_err(),
        "worker process and control route did not complete before shutdown"
    );

    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown session from a separate connection");
    assert!(
        shutdown.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit {
                subscription_id: event_subscription_id,
                ..
            } if event_subscription_id == subscription_id
        )),
        "shutdown caller must not consume the attached subscription's process exit: {:?}",
        shutdown.events
    );

    let attached_drain = attached
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("drain attached subscription after cross-connection shutdown");
    assert!(
        attached_drain.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit { .. }
                | botster_hub_client::DaemonEvent::TerminalOutput { .. }
        )),
        "host Status must not translate ProcessExit or terminal output: {:?}",
        attached_drain.events
    );
    let screen = wait_for_read_screen_contains(&mut attached, session_id, "cross-connection-tail");
    assert!(
        screen.contains("cross-connection-tail"),
        "final terminal output is on ReadScreen: {screen:?}"
    );
    let drained_again = attached
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("repeat drain after cross-connection shutdown");
    assert!(
        drained_again.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit {
                subscription_id: event_subscription_id,
                ..
            } if event_subscription_id == subscription_id
        )),
        "subscription-scoped process exit must be delivered once: {:?}",
        drained_again.events
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_observes_attached_natural_exit_with_pending_egress() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-attached-exit");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-attached-exit")
            .expect("subscribe before attached natural exit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    let _ = wait_for_entity_frame(&mut subscription, Duration::from_secs(5), |_| true);

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-attached-exit".to_string(),
            command: "sleep 0.15; printf 'pending-first\\n'; sleep 0.15; printf 'pending-second\\n'; exit 7"
                .to_string(),
        },
    )
    .expect("spawn attached natural-exit session");
    assert!(matches!(
        wait_for_entity_frame(&mut subscription, Duration::from_secs(5), |frame| {
            matches!(
                frame,
                botster_hub_client::DaemonEntityFrame::Upsert { id, .. }
                    if id == "entity-attached-exit"
            )
        }),
        botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
            if id == "entity-attached-exit"
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-attached-exit".to_string(),
            subscription_id: "terminal-attached-exit".to_string(),
        })
        .expect("attach before output becomes pending");

    subscription
        .set_read_timeout(Some(Duration::from_millis(80)))
        .expect("short entity reads while draining lifecycle");
    let exit_deadline = Instant::now() + Duration::from_secs(8);
    let mut retained_events = Vec::new();
    let exit_sequence = loop {
        retained_events.extend(poll_adapter_events(
            &mut terminal,
            "entity-attached-exit",
            Some("terminal-attached-exit"),
        ));
        let frame = match subscription.next_frame() {
            Ok(frame) => frame,
            Err(error) if Instant::now() < exit_deadline => {
                let _ = error;
                continue;
            }
            Err(error) => panic!("natural exit delta with pending terminal egress: {error}"),
        };
        match frame {
            botster_hub_client::DaemonEntityFrame::Patch {
                snapshot_seq,
                id,
                patch,
                ..
            } if id == "entity-attached-exit"
                && patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") =>
            {
                assert_eq!(
                    patch.get("exit_code").and_then(serde_json::Value::as_i64),
                    Some(7)
                );
                break snapshot_seq;
            }
            _ => {
                assert!(
                    Instant::now() < exit_deadline,
                    "timed out waiting for attached natural exit"
                );
            }
        }
    };
    assert!(exit_sequence > 0);

    retained_events.extend(poll_adapter_events(
        &mut terminal,
        "entity-attached-exit",
        Some("terminal-attached-exit"),
    ));
    let retained_output = retained_events
        .iter()
        .filter_map(|event| match event {
            botster_hub_client::DaemonEvent::TerminalOutput { payload, .. } => {
                Some(live_output_utf8(payload))
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    let screen_text = terminal
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "entity-attached-exit".to_string(),
        })
        .ok()
        .and_then(|response| response.read_screen)
        .map(|screen| screen.text)
        .unwrap_or_default();
    assert!(
        retained_output.matches("pending-first").count() == 1
            || screen_text.matches("pending-first").count() == 1,
        "retained first marker missing from adapter output and ReadScreen: drain={retained_output:?} screen={screen_text:?}"
    );
    assert!(
        retained_output.matches("pending-second").count() == 1
            || screen_text.matches("pending-second").count() == 1,
        "retained second marker missing from adapter output and ReadScreen: drain={retained_output:?} screen={screen_text:?}"
    );
    let ordered = if retained_output.contains("pending-first")
        && retained_output.contains("pending-second")
    {
        retained_output.find("pending-first") < retained_output.find("pending-second")
    } else if screen_text.contains("pending-first") && screen_text.contains("pending-second") {
        screen_text.find("pending-first") < screen_text.find("pending-second")
    } else {
        true
    };
    assert!(
        ordered,
        "retained terminal output must preserve production order, drain={retained_output:?} screen={screen_text:?}"
    );
    assert!(
        retained_events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit { code: Some(7), .. }
        )),
        "bound adapter must deliver ProcessExit: drain={retained_events:?} screen={screen_text:?}"
    );

    let drained_again = terminal
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status after retained adapter batch");
    assert!(
        drained_again.events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput { payload, .. }
                    if live_output_contains(payload, "pending-first") || live_output_contains(payload, "pending-second")
            ) && !matches!(
                event,
                botster_hub_client::DaemonEvent::ProcessExit { code: Some(7), .. }
            )
        }),
        "retained terminal events must only be delivered once, got {:?}",
        drained_again.events
    );

    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entity-drop");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-terminal-disconnect")
            .expect("subscribe before terminal disconnect");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    let _ = wait_for_entity_frame(&mut subscription, Duration::from_secs(5), |_| true);

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-terminal-disconnect".to_string(),
            command: "sleep 0.3; printf 'orphaned-output\\n'; sleep 0.6; exit 7".to_string(),
        },
    )
    .expect("spawn terminal disconnect session");
    assert!(matches!(
        wait_for_entity_frame(&mut subscription, Duration::from_secs(5), |frame| {
            matches!(
                frame,
                botster_hub_client::DaemonEntityFrame::Upsert { id, .. }
                    if id == "entity-terminal-disconnect"
            )
        }),
        botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
            if id == "entity-terminal-disconnect"
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-terminal-disconnect".to_string(),
            subscription_id: "terminal-disconnect".to_string(),
        })
        .expect("attach terminal before ungraceful disconnect");
    thread::sleep(Duration::from_millis(500));
    drop(terminal);

    subscription
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("bound exit-delta read after disconnect");
    let disconnect_deadline = Instant::now() + Duration::from_secs(8);
    let exit_sequence = loop {
        match subscription.next_frame() {
            Ok(botster_hub_client::DaemonEntityFrame::Patch {
                snapshot_seq,
                patch,
                ..
            }) if patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") => {
                assert_eq!(
                    patch.get("exit_code").and_then(serde_json::Value::as_i64),
                    Some(7)
                );
                break snapshot_seq;
            }
            Ok(_) => {}
            Err(error) if Instant::now() < disconnect_deadline => {
                let _ = error;
            }
            Err(error) => panic!("exit delta after terminal disconnect: {error}"),
        }
    };

    let removed = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "entity-terminal-disconnect".to_string(),
        },
    )
    .expect("remove session after disconnected terminal exit");
    assert_eq!(
        removed.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved
    );
    assert!(matches!(
        subscription.next_frame().expect("remove delta"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-terminal-disconnect" && snapshot_seq > exit_sequence
    ));

    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_spawn_failure_returns_actionable_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("spawn-fail");
    let bad_worker = data_dir.join("missing-botster-session-worker");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon_with_session_worker(&data_dir, &bad_worker);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command: "printf 'should-not-start\\n'".to_string(),
        })
        .expect("spawn failure should return operator frame");
    assert_eq!(
        spawn.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = spawn.error.as_ref().expect("operator error body");
    assert_eq!(
        error.code, "spawn_failed",
        "unexpected spawn operator error: {error:?} diagnostics={:?}",
        spawn.diagnostics
    );
    assert_eq!(error.operation, "spawn");
    assert!(
        spawn.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
                && diagnostic.operation.as_deref() == Some("spawn")
                && diagnostic
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("session worker"))
        }),
        "spawn failure should carry an actionable diagnostic row, got {:?}",
        spawn.diagnostics
    );
    assert!(!has_diagnostic_kind(
        &spawn.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    let debug = format!("{error:?} {:?}", spawn.diagnostics);
    assert!(!debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!debug.contains(&bad_worker.to_string_lossy().to_string()));
    assert!(!debug.contains(concat!("/", "Users", "/")));
    assert!(!debug.contains("/home/"));

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("daemon remains responsive after spawn failure");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_reports_compatibility_descriptor_and_mismatch_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("compat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path.clone());
    let child = start_cli_daemon(&data_dir);

    let mut stream = UnixStream::connect(&socket_path).expect("connect raw compatibility socket");
    botster_hub_client::write_frame(
        &mut stream,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            terminal_compatibility: None,
        },
    )
    .expect("write hello");
    let ack: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut stream).expect("read hello ack");
    assert_eq!(ack.protocol, botster_hub_client::PROTOCOL);
    assert!(ack.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("hello")
    }));
    assert!(!has_failure_diagnostic(&ack.diagnostics));
    assert_eq!(ack.compatibility.protocol, botster_hub_client::PROTOCOL);
    assert_eq!(
        ack.compatibility.protocol_version,
        botster_hub_client::PROTOCOL_VERSION
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_SESSIONS)
    );
    assert!(
        !ack.compatibility
            .supports_feature(botster_terminal_protocol::FEATURE_TERMINAL_STREAMING)
    );
    assert!(
        !ack.compatibility
            .supports_feature(botster_terminal_protocol::FEATURE_RESIZE)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION)
    );
    assert_eq!(
        ack.compatibility.conformance_fixture_revision,
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION
    );

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    let status = status.status.expect("status response body");
    assert_eq!(status.compatibility, ack.compatibility);
    assert!(status.diagnostics.is_empty());

    let mut stale_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    stale_requirement.client_name = "stale-5-29-client".to_string();
    stale_requirement.protocol_version = 5;
    stale_requirement.minimum_conformance_fixture_revision = 29;
    for attempt in ["initial connect", "reconnect"] {
        let error =
            botster_hub_client::connect_and_hello_with_requirement(&endpoint, &stale_requirement)
                .expect_err("stale client must fail before dispatching a removed operation");
        let message = error.to_string();
        assert!(
            message.contains("stale-5-29-client"),
            "{attempt}: {message}"
        );
        assert!(
            message.contains("unsupported protocol version 8"),
            "{attempt}: {message}"
        );
    }

    let mut protocol_seven = botster_hub_client::DaemonCompatibilityRequirement::current();
    protocol_seven.client_name = "protocol-7-client".to_string();
    protocol_seven.protocol_version = 7;
    protocol_seven.minimum_conformance_fixture_revision =
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION;
    let protocol_seven_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &protocol_seven)
            .expect_err("protocol-7 client must fail at admission");
    let protocol_seven_message = protocol_seven_error.to_string();
    assert!(
        protocol_seven_message.contains("unsupported protocol version 8"),
        "{protocol_seven_message}"
    );
    assert!(
        protocol_seven_message.contains("protocol-7-client"),
        "{protocol_seven_message}"
    );

    let mut version_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    version_requirement.client_name = "future-version-client".to_string();
    version_requirement.protocol_version = botster_hub_client::PROTOCOL_VERSION + 1;
    let version_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &version_requirement)
            .expect_err("future protocol version should fail compatibility");
    let version_message = version_error.to_string();
    assert!(version_message.contains("future-version-client"));
    assert!(version_message.contains("unsupported protocol version"));
    assert!(!version_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(version_error) = version_error
    else {
        panic!("version mismatch should be a compatibility error");
    };
    assert!(version_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
            && diagnostic
                .message
                .as_deref()
                .is_some_and(|message| message.contains("unsupported protocol version"))
    }));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    let mut feature_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    feature_requirement.client_name = "future-feature-client".to_string();
    feature_requirement
        .required_features
        .push("future_feature".to_string());
    let feature_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &feature_requirement)
            .expect_err("future feature should fail compatibility");
    let feature_message = feature_error.to_string();
    assert!(feature_message.contains("future-feature-client"));
    assert!(feature_message.contains("missing required feature(s): future_feature"));
    assert!(!feature_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(feature_error) = feature_error
    else {
        panic!("feature mismatch should be a compatibility error");
    };
    assert!(feature_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.feature.as_deref() == Some("future_feature")
    }));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn process_ownership_external_hub_test_support_cleans_up_isolated_daemon() {
    let _guard = daemon_test_guard();
    let first = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(PathBuf::from("/tmp/bh-test-support"))
            .name("downstream-shape"),
    );
    assert!(first.data_dir().starts_with("/tmp/bh-test-support"));
    assert!(first.endpoint().socket_path.starts_with(first.data_dir()));
    let support_matrix = botster_hub_test_support::first_party_client_support_matrix();
    let first_report =
        botster_hub_test_support::run_client_conformance(&first).expect("run client conformance");
    assert_eq!(first_report.lifecycle_state, "running");
    assert_eq!(first_report.initial_session_count, 0);
    assert_eq!(first_report.spawned_lifecycle, "running");
    assert_eq!(
        support_matrix.session_actions,
        vec![
            "status",
            "list_sessions",
            "subscribe_entities",
            "unsubscribe_entities",
            "remove_session",
            "spawn",
            "attach",
            "shutdown_session",
        ]
    );
    assert!(first_report.stream_contains_ready);
    assert!(first_report.stream_contains_echo);
    assert!(first_report.stream_contains_resize);
    assert_eq!(first_report.compatibility_protocol, support_matrix.protocol);
    assert_eq!(
        first_report.compatibility_protocol_version,
        support_matrix.protocol_version
    );
    assert_eq!(
        first_report.compatibility_features,
        support_matrix.supported_features
    );
    assert_eq!(
        first_report.compatibility_conformance_fixture_revision,
        support_matrix.conformance_fixture_revision
    );
    assert_eq!(first_report.connected_diagnostic_operation, "status");
    assert_eq!(first_report.validation_error_operation, "attach");
    assert_eq!(
        first_report.validation_diagnostic_kind,
        support_matrix
            .terminal_streaming
            .missing_session_diagnostic_kind
    );
    assert!(support_matrix.terminal_streaming.supported);
    assert!(support_matrix.terminal_streaming.held_open_stream);
    assert_eq!(
        support_matrix.terminal_streaming.conformance_ready_output,
        "conformance-ready"
    );
    assert_eq!(
        support_matrix.terminal_streaming.conformance_echo_output,
        "echo:from-conformance"
    );
    assert!(support_matrix.resize.supported);
    assert_eq!(support_matrix.resize.action, "resize");
    assert_eq!(support_matrix.resize.conformance_output_prefix, "winsize:");

    let plugin_report = botster_hub_test_support::run_plugin_contract_matrix_conformance(
        &first,
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("plugins")
            .join("plugin-contract-matrix"),
    )
    .expect("run plugin contract matrix conformance");
    assert_eq!(plugin_report.enabled_state, "enabled");
    assert!(support_matrix.plugin_surfaces.render_supported);
    assert!(support_matrix.plugin_surfaces.action_supported);
    assert_eq!(
        plugin_report.app_surface_kind,
        support_matrix.plugin_surfaces.rendered_surface_kind
    );
    assert_eq!(
        plugin_report.app_surface_node_id,
        support_matrix.plugin_surfaces.rendered_surface_node_id
    );
    assert_eq!(
        plugin_report.session_surface_id,
        support_matrix.session_entities.plugin_surface_id
    );
    assert_eq!(
        plugin_report.session_surface_binding_family,
        support_matrix.session_entities.binding_family
    );
    assert!(plugin_report.session_surface_matches_fixture);
    assert_eq!(plugin_report.action_error_state, "error");
    assert_eq!(
        plugin_report.action_error_diagnostic_kind,
        support_matrix
            .plugin_surfaces
            .invalid_action_diagnostic_kind
    );
    assert_eq!(
        plugin_report.client_render_check.class,
        botster_hub_test_support::ConformanceFailureClass::ClientRendering
    );

    let terminal_app_report =
        botster_hub_test_support::run_foreground_terminal_app_open_conformance(&first)
            .expect("run foreground terminal app open conformance");
    assert_eq!(terminal_app_report.package_state, "enabled");
    assert_eq!(
        terminal_app_report.package_name,
        "first-party.terminal-client"
    );
    assert_eq!(terminal_app_report.app_id, "tui");
    assert_eq!(terminal_app_report.entrypoint_id, "tui");
    assert_eq!(terminal_app_report.app_kind, "terminal_app");
    assert_eq!(terminal_app_report.launch_mode, "foreground_stdio");
    assert!(terminal_app_report.hub_connection_env_present);
    assert_eq!(terminal_app_report.hub_connection_transport, "unix_socket");
    assert!(terminal_app_report.hub_connection_socket_path_absolute);
    assert!(terminal_app_report.hub_data_dir_env_present);
    assert!(terminal_app_report.hub_data_dir_env_absolute);
    assert!(terminal_app_report.launch_working_directory_is_package_root);
    assert!(terminal_app_report.launch_working_directory_differs_from_daemon_cwd);
    assert_eq!(terminal_app_report.real_hub_action_operation, "status");
    assert_eq!(terminal_app_report.real_hub_action_result, "running");
    assert_eq!(terminal_app_report.exit_code, Some(0));
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_present=true")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_transport=unix_socket")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_socket_absolute=true")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_data_dir_present=true")
    );
    assert!(terminal_app_report.stderr.is_empty());
    first.shutdown().expect("shutdown first isolated hub");

    let second = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(PathBuf::from("/tmp/bh-test-support"))
            .name("downstream-shape-determinism"),
    );
    let second_report =
        botster_hub_test_support::run_client_conformance(&second).expect("rerun conformance");
    assert_eq!(second_report, first_report);
    second.shutdown().expect("shutdown second isolated hub");
}

#[test]
fn external_hub_client_many_pty_adversarial_conformance_ci() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(PathBuf::from("/tmp/bh-test-support"))
            .name("many-pty-client-attach-ci"),
    );

    let report = botster_hub_test_support::run_many_pty_client_attach_conformance(&hub, 8)
        .expect("run CI-safe many-PTY client attach proof");
    // Ok(report) is the behavioral oracle; stage-labeled errors identify which
    // required observation failed. These assertions pin scenario and cleanup size.
    assert_eq!(report.total_sessions, 8);
    assert_eq!(report.quiet_sessions, 7);
    assert_eq!(report.cleaned_sessions, 8);

    hub.shutdown().expect("shutdown CI-safe many-PTY hub");
}

#[test]
#[ignore = "larger local adversarial proof; run explicitly with the documented command"]
fn external_hub_client_many_pty_adversarial_conformance_local() {
    let _guard = daemon_test_guard();
    let hub = start_isolated_hub(
        botster_hub_test_support::IsolatedHubBuilder::new()
            .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
            .session_worker_bin(session_worker_binary_path())
            .root(PathBuf::from("/tmp/bh-test-support"))
            .name("many-pty-client-attach-local"),
    );

    let report = botster_hub_test_support::run_many_pty_client_attach_conformance(&hub, 32)
        .expect("run larger local many-PTY client attach proof");
    // Ok(report) is the behavioral oracle; stage-labeled errors identify which
    // required observation failed. These assertions pin scenario and cleanup size.
    assert_eq!(report.total_sessions, 32);
    assert_eq!(report.quiet_sessions, 31);
    assert_eq!(report.cleaned_sessions, 32);

    hub.shutdown().expect("shutdown larger local many-PTY hub");
}

#[test]
fn external_daemon_same_session_reattach_replays_opaque_history_before_live_output() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("late-history");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect daemon socket");

    let spawn = connection
        .request(&botster_hub::DaemonRequest::Spawn {
            session_id: "late-history-session".to_string(),
            command: "printf 'retained-before-attach\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn late-history session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let first_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "late-history-session".to_string(),
            subscription_id: "late-history-first-subscription".to_string(),
        })
        .expect("attach first subscription");
    assert_eq!(first_attach.kind, botster_hub::DaemonResponseKind::Events);

    let first_observed = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            let _ = connection.request(&botster_hub::DaemonRequest::Status);
            last = connection
                .request(&botster_hub::DaemonRequest::ReadScreen {
                    session_id: "late-history-session".to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .map(|screen| screen.text)
                .unwrap_or_default();
            if last.contains("retained-before-attach") {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
        last
    };
    assert!(
        first_observed.contains("retained-before-attach"),
        "first subscription should observe initial output before late attach, got {first_observed:?}"
    );

    connection
        .send_terminal_frame(
            "late-history-session",
            "late-history-first-subscription",
            &terminal_input_frame_bytes(b"retained-after-attach\n"),
        )
        .expect("send second retained marker before socket loss");
    let first_observed = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            let _ = connection.request(&botster_hub::DaemonRequest::Status);
            last = connection
                .request(&botster_hub::DaemonRequest::ReadScreen {
                    session_id: "late-history-session".to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .map(|screen| screen.text)
                .unwrap_or_default();
            if last.contains("after:retained-after-attach") {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
        last
    };
    assert!(
        first_observed.contains("after:retained-after-attach"),
        "first subscription should observe the second marker before socket loss, got {first_observed:?}"
    );

    drop(connection);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("reconnect daemon socket");
    let late_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "late-history-session".to_string(),
            subscription_id: "late-history-reattach-subscription".to_string(),
        })
        .expect("reattach same session with a fresh subscription id");
    assert_eq!(late_attach.kind, botster_hub::DaemonResponseKind::Events);

    let read_screen = connection
        .request(&botster_hub::DaemonRequest::ReadScreen {
            session_id: "late-history-session".to_string(),
        })
        .expect("read retained screen before later live output");
    assert_eq!(
        read_screen.kind,
        botster_hub::DaemonResponseKind::ReadScreen
    );
    let screen_text = read_screen
        .read_screen
        .expect("retained screen response body")
        .text;
    for marker in ["retained-before-attach", "after:retained-after-attach"] {
        assert_eq!(
            screen_text.matches(marker).count(),
            1,
            "ReadScreen should contain {marker:?} exactly once, got {screen_text:?}"
        );
    }
    assert!(
        screen_text.find("retained-before-attach")
            < screen_text.find("after:retained-after-attach"),
        "ReadScreen should preserve retained marker order, got {screen_text:?}"
    );

    connection
        .send_terminal_frame(
            "late-history-session",
            "late-history-reattach-subscription",
            &terminal_input_frame_bytes(b"live-after-late\n"),
        )
        .expect("send later live output");

    assert!(
        late_attach.events.is_empty(),
        "late attach must not return terminal bodies: {:?}",
        late_attach.events
    );
    let screen = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            let _ = connection.request(&botster_hub::DaemonRequest::Status);
            last = connection
                .request(&botster_hub::DaemonRequest::ReadScreen {
                    session_id: "late-history-session".to_string(),
                })
                .ok()
                .and_then(|response| response.read_screen)
                .map(|screen| screen.text)
                .unwrap_or_default();
            if last.contains("after:live-after-late") {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
        last
    };
    assert!(
        screen.contains("after:live-after-late"),
        "late attach live output is on ReadScreen: {screen:?}"
    );

    let no_history_spawn = connection
        .request(&botster_hub::DaemonRequest::Spawn {
            session_id: "no-history-session".to_string(),
            command: "while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn no-history session");
    assert_eq!(
        no_history_spawn.kind,
        botster_hub::DaemonResponseKind::Spawned
    );

    let first_no_history_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "no-history-session".to_string(),
            subscription_id: "no-history-first-subscription".to_string(),
        })
        .expect("attach first no-history subscription");
    assert_eq!(
        first_no_history_attach.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let _ = connection.request(&botster_hub::DaemonRequest::Status);

    drop(connection);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("reconnect idle daemon socket");
    let late_no_history_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "no-history-session".to_string(),
            subscription_id: "no-history-reattach-subscription".to_string(),
        })
        .expect("reattach idle session with a fresh subscription id");
    assert_eq!(
        late_no_history_attach.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let mut no_history_attach_events = late_no_history_attach.events.clone();
    for _ in 0..20 {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Status)
            .expect("drain no-history attach");
        let attached = drain.events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::AttachState { state, .. } if state == "attached"
            )
        });
        no_history_attach_events.extend(drain.events);
        if attached {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    let no_history_read_screen = connection
        .request(&botster_hub::DaemonRequest::ReadScreen {
            session_id: "no-history-session".to_string(),
        })
        .expect("read blank screen before sending live output");
    assert_eq!(
        no_history_read_screen.kind,
        botster_hub::DaemonResponseKind::ReadScreen
    );
    let no_history_screen = no_history_read_screen
        .read_screen
        .expect("blank read screen response body");
    assert!(
        no_history_screen.text.is_empty(),
        "idle session should have no prior renderable output, got {:?}",
        no_history_screen.text
    );

    connection
        .send_terminal_frame(
            "no-history-session",
            "no-history-reattach-subscription",
            &terminal_input_frame_bytes(b"live-only\n"),
        )
        .expect("send no-history live output");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut no_history_events = no_history_attach_events;
    let mut no_history_saw_live = false;
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Status)
            .expect("drain no-history live output");
        no_history_events.extend(drain.events);
        no_history_saw_live = no_history_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    payload,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
                    && live_output_contains(payload, "after:live-only")
            )
        }) || connection
            .request(&botster_hub::DaemonRequest::ReadScreen {
                session_id: "no-history-session".to_string(),
            })
            .ok()
            .and_then(|response| response.read_screen)
            .is_some_and(|screen| screen.text.contains("after:live-only"));
        if no_history_saw_live {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        late_no_history_attach.events.is_empty(),
        "idle reattach must not return terminal bodies: {:?}",
        late_no_history_attach.events
    );
    assert!(
        no_history_saw_live,
        "idle subscription live output is on ReadScreen"
    );
    assert!(
        !no_history_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::Scrollback {
                    subscription_id,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
            )
        }),
        "idle subscription should not receive fabricated scrollback, got {no_history_events:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "late-history-session".to_string(),
        })
        .expect("shutdown late-history session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let shutdown_no_history_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "no-history-session".to_string(),
        })
        .expect("shutdown no-history session");
    assert_eq!(
        shutdown_no_history_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_detaches_subscription_when_attach_connection_drops() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-attach-eof");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "eof-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn eof test session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let attach = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Attach {
            session_id: "eof-session".to_string(),
            subscription_id: "dropped-subscription".to_string(),
        },
    )
    .expect("attach dropped subscription");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    thread::sleep(Duration::from_millis(150));

    let mut live =
        botster_hub::DaemonConnection::connect(&config).expect("connect after dropped attach");
    live.request(&botster_hub::DaemonRequest::Attach {
        session_id: "eof-session".to_string(),
        subscription_id: "live-after-eof-subscription".to_string(),
    })
    .expect("attach live subscription after dropped attach");
    live.send_terminal_frame(
        "eof-session",
        "live-after-eof-subscription",
        &terminal_input_frame_bytes(b"after-eof\r"),
    )
    .expect("send input after dropped attach");

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut observed_events = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::Status,
        )
        .expect("drain after dropped attach");
        observed_events.extend(drain.events);
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        observed_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    payload,
                    ..
                } if subscription_id == "dropped-subscription"
                    && live_output_contains(payload, "after-eof")
            )
        }),
        "dropped attach subscription received later terminal output: {observed_events:?}"
    );

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: "eof-session".to_string(),
        },
    )
    .expect("shutdown eof test session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let sessions_after_shutdown =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
            .expect("list sessions after eof test session shutdown");
    assert!(
        sessions_after_shutdown
            .sessions
            .iter()
            .any(|session| session.session_id == "eof-session" && session.lifecycle == "exited"),
        "eof-session should be exited after shutdown: {:?}",
        sessions_after_shutdown.sessions
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_notify_session_defers_without_observed_readiness_over_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("daemon-notify-session");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "notify-socket-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn guarded socket session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect TUI-grade socket");
    connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "notify-socket-session".to_string(),
            subscription_id: "notify-socket-subscription".to_string(),
        })
        .expect("attach persistent socket subscription");

    let write = connection
        .request(&botster_hub::DaemonRequest::NotifySession {
            session_id: "notify-socket-session".to_string(),
            data: "notify-socket\n".to_string(),
        })
        .expect("notify session over daemon socket");
    assert_eq!(write.kind, botster_hub::DaemonResponseKind::SessionNotified);
    let notify = write
        .coordination
        .and_then(|coordination| coordination.notify)
        .expect("notify response body");
    assert!(notify.decision.starts_with("Defer"));
    assert_eq!(notify.states, vec!["accepted", "deferred"]);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Status)
            .expect("drain guarded socket session");
        for event in drain.events {
            if let botster_hub::DaemonEvent::TerminalOutput { payload, .. } = event {
                observed.push_str(&live_output_utf8(payload));
            }
        }
        if observed.contains("echo:notify-socket") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        !observed.contains("echo:notify-socket"),
        "notify session without observed readiness should not reach PTY input path, got {observed:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "notify-socket-session".to_string(),
        })
        .expect("shutdown guarded socket session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let sessions_after_shutdown = connection
        .request(&botster_hub::DaemonRequest::ListSessions)
        .expect("list sessions after guarded socket session shutdown");
    assert!(
        sessions_after_shutdown.sessions.iter().any(|session| {
            session.session_id == "notify-socket-session" && session.lifecycle == "exited"
        }),
        "notify-socket-session should be exited after shutdown: {:?}",
        sessions_after_shutdown.sessions
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn stalled_attach_stdout_does_not_block_other_daemon_commands() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-stalled-attach");
    let child = start_cli_daemon(&data_dir);

    let mut spawn_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    spawn_command
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("slow-consumer")
        .arg("--")
        .arg(
            "i=0; while [ \"$i\" -lt 50000 ]; do printf 'flood-line-%05d\\n' \"$i\"; i=$((i + 1)); done; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        );
    let spawn = run_command_with_timeout_diagnostics(
        "spawn",
        spawn_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        spawn.output.status.success(),
        "spawn failed: {}",
        spawn.diagnostics(),
    );

    let mut attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn attach while flooding stdout");
    thread::sleep(Duration::from_millis(200));

    let mut list_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    list_command
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir);
    let list = run_command_with_timeout_diagnostics(
        "list",
        list_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        list.output.status.success(),
        "list failed while attach was in flight: {}; attach_child={}",
        list.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let mut status_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    status_command.arg("status").arg("--data-dir").arg(&data_dir);
    let status = run_command_with_timeout_diagnostics(
        "status",
        status_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        status.output.status.success(),
        "status failed while attach stdout was blocked: {}; attach_child={}",
        status.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect while attach stdout was blocked");
    connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "slow-consumer".to_string(),
            subscription_id: "stalled-attach-control-subscription".to_string(),
        })
        .expect("attach control subscription while CLI attach stdout is blocked");
    connection
        .send_terminal_frame(
            "slow-consumer",
            "stalled-attach-control-subscription",
            &terminal_resize_frame_bytes(32, 120),
        )
        .expect("resize through bound duplex route while CLI attach stdout is blocked");
    connection
        .send_terminal_frame(
            "slow-consumer",
            "stalled-attach-control-subscription",
            &terminal_input_frame_bytes(b"still-responsive\r"),
        )
        .expect("send input through bound duplex route while CLI attach stdout is blocked");

    let mut shutdown_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    shutdown_command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir);
    let shutdown = run_command_with_timeout_diagnostics(
        "shutdown",
        shutdown_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        shutdown.output.status.success(),
        "shutdown failed while attach stdout was blocked: {}; attach_child={}",
        shutdown.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let _ = attach_child.kill();
    let _ = attach_child.wait_with_output();
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let cleanup_child = start_cli_daemon(&data_dir);
    let shutdown_session = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .output()
        .expect("shut down recovered slow-consumer session");
    assert!(
        shutdown_session.status.success(),
        "recovered session shutdown failed: {}",
        String::from_utf8_lossy(&shutdown_session.stderr)
    );
    let shutdown_session_stdout =
        String::from_utf8(shutdown_session.stdout).expect("session shutdown stdout is utf8");
    let returned_shutdown_events = shutdown_session_stdout.contains("response=events");
    let returned_terminal_cleanup = shutdown_session_stdout.contains("response=session_cleanup")
        && shutdown_session_stdout.contains("session_id=slow-consumer")
        && shutdown_session_stdout.contains("outcome=already_exited");
    assert!(
        returned_shutdown_events || returned_terminal_cleanup,
        "recovered session shutdown should return events or terminal cleanup: {shutdown_session_stdout:?}"
    );

    let sessions_after_shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("list sessions after recovered slow-consumer shutdown");
    assert!(
        sessions_after_shutdown.status.success(),
        "list failed after recovered slow-consumer shutdown: {}",
        String::from_utf8_lossy(&sessions_after_shutdown.stderr)
    );
    let sessions_after_shutdown_stdout = String::from_utf8(sessions_after_shutdown.stdout)
        .expect("sessions after shutdown stdout is utf8");
    assert!(
        !sessions_after_shutdown_stdout.contains("session_id=slow-consumer"),
        "slow-consumer should be absent after recovered session shutdown: {sessions_after_shutdown_stdout:?}"
    );

    shutdown_cli_daemon(&data_dir, cleanup_child);
}

#[test]
fn socket_adapter_receives_ready_before_later_snapshot_frames() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("socket-ready-before-encode");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    // Capacity 1 holds the worker encode callback: after READY is pulled, the
    // next PAGE occupies the only slot and FINISH cannot be encoded until the
    // bound adapter consumes that PAGE. Encode therefore cannot return before
    // another adapter read.
    let child = start_cli_daemon_with_worker_egress_capacity(&data_dir, Some(1));
    let session_id = "ready-before-encode";
    let subscription_id = "ready-before-encode-sub";
    let ready_path = data_dir.join("history-ready");
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                concat!(
                    "stty -echo 2>/dev/null; ",
                    "i=0; while [ $i -lt 2000 ]; do printf 'history-%04d\\n' \"$i\"; i=$((i+1)); done; ",
                    "printf 'PRE-BARRIER-MARKER\\n'; : > '{}'; ",
                    "while IFS= read -r line; do printf \"echo:%s\\n\" \"$line\"; done"
                ),
                ready_path.display()
            ),
        })
        .expect("spawn history producer");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, session_id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ready_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists(),
        "timed out waiting for history producer to finish writing"
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("attach");
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "attach is a host ack: {:?}",
        attach.error
    );
    assert!(
        attach.events.is_empty(),
        "attach acks with empty terminal bodies: {:?}",
        attach.events
    );

    connection
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_input_frame_bytes(b"POST-BARRIER-MARKER\n"),
        )
        .expect("queue input during snapshot stream");

    let first_status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status after attach");
    assert_eq!(
        first_status.kind,
        botster_hub_client::DaemonResponseKind::Status,
        "host Status must stay serviceable after attach: {first_status:?}"
    );
    assert!(
        first_status.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
                | botster_hub_client::DaemonEvent::AttachState { .. }
                | botster_hub_client::DaemonEvent::TerminalOutput { .. }
        )),
        "host Status must not translate READY/PAGE/FINISH: {:?}",
        first_status.events
    );
    connection
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_resize_frame_bytes(30, 90),
        )
        .expect("queue first resize after attach");
    connection
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_resize_frame_bytes(40, 120),
        )
        .expect("queue latest resize after attach");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut snapshot = None;
    while Instant::now() < deadline {
        let capture = connection
            .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
                session_id: session_id.to_string(),
            })
            .expect("capture after attach");
        if let Some(body) = capture.capture_snapshot
            && (body.rows, body.cols) == (40, 120)
        {
            snapshot = Some(body);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let snapshot = snapshot.expect("latest queued resize must apply on CaptureSnapshot");
    assert_eq!((snapshot.rows, snapshot.cols), (40, 120));

    let screen =
        wait_for_read_screen_contains(&mut connection, session_id, "echo:POST-BARRIER-MARKER");
    assert!(
        screen.contains("echo:POST-BARRIER-MARKER"),
        "queued input must apply after attach: {screen:?}"
    );

    session_cleanup.disarm();
    production_shutdown_and_remove_session(&endpoint, session_id);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn socket_attach_missing_session_emits_attach_failed() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("socket-attach-failed");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "missing-session".to_string(),
            subscription_id: "missing-sub".to_string(),
        })
        .expect("attach missing session");
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "missing session attach must fail closed: {:?}",
        attach.error
    );
    assert!(
        !attach.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
        )),
        "attach_failed must not also attach: {:?}",
        attach.events
    );

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("status after attach_failed");
    let counters = status
        .status
        .expect("status body after attach_failed")
        .lifecycle_counters;
    assert_eq!(
        counters.live_attach_subscriptions, 0,
        "pre-READY attach_failed must not record a live attach: {counters:?}"
    );
    assert_eq!(
        counters.high_water_attach_subscriptions, 0,
        "pre-READY attach_failed must not raise attach high water: {counters:?}"
    );
    let cleanup_failed_before = counters.cleanup_failed;
    let cleanup_completed_before = counters.cleanup_completed;
    drop(connection);

    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    let after = loop {
        let after =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after failed-attach disconnect")
                .status
                .expect("status body after failed-attach disconnect")
                .lifecycle_counters;
        if after.cleanup_completed > cleanup_completed_before {
            break after;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "failed-attach disconnect did not complete connection cleanup: {after:?}"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        after.live_attach_subscriptions, 0,
        "cleanup must not leave a live attach after attach_failed: {after:?}"
    );
    assert_eq!(
        after.cleanup_failed, cleanup_failed_before,
        "cleanup must not Detach a route that never attached: {after:?}"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn dropped_ready_attach_releases_barrier_for_a_new_subscription() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("socket-cancel-barrier");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon_with_worker_egress_capacity(&data_dir, Some(1));
    let session_id = "cancel-barrier";
    let first_sub = "cancel-barrier-first";
    let second_sub = "cancel-barrier-second";
    let ready_path = data_dir.join("history-ready");

    {
        let mut first =
            botster_hub_client::DaemonConnection::connect(&endpoint).expect("first connect");
        first
            .request(&botster_hub_client::DaemonRequest::Spawn {
                session_id: session_id.to_string(),
                command: format!(
                    concat!(
                        "stty -echo 2>/dev/null; ",
                        "i=0; while [ $i -lt 2000 ]; do printf 'history-%04d\\n' \"$i\"; i=$((i+1)); done; ",
                        "printf 'PRE-BARRIER-MARKER\\n'; : > '{}'; ",
                        "while IFS= read -r line; do printf \"echo:%s\\n\" \"$line\"; done"
                    ),
                    ready_path.display()
                ),
            })
            .expect("spawn");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if ready_path.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            ready_path.exists(),
            "timed out waiting for cancel-barrier history producer"
        );
        let attach = first
            .request(&botster_hub_client::DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: first_sub.to_string(),
            })
            .expect("attach first");
        assert!(
            attach.events.is_empty(),
            "first Attach must not return terminal bodies: {:?}",
            attach.events
        );
        let status = first
            .request(&botster_hub_client::DaemonRequest::Status)
            .expect("host status first");
        assert!(
            status.events.iter().all(|event| !matches!(
                event,
                botster_hub_client::DaemonEvent::Snapshot { .. }
                    | botster_hub_client::DaemonEvent::AttachState { .. }
            )),
            "host Status must not translate READY/attached: {:?}",
            status.events
        );
    }

    let mut second =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("second connect");
    let attach_second = second
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: second_sub.to_string(),
        })
        .expect("attach second");
    assert!(
        attach_second.events.is_empty(),
        "replacement Attach must not return terminal bodies: {:?}",
        attach_second.events
    );
    second
        .send_terminal_frame(
            session_id,
            second_sub,
            &terminal_input_frame_bytes(b"after-cancel\n"),
        )
        .expect("input after reattach");
    let screen = wait_for_read_screen_contains(&mut second, session_id, "echo:after-cancel");
    assert!(
        screen.contains("echo:after-cancel"),
        "new subscription must get process output after first connection drop: {screen:?}"
    );

    production_shutdown_and_remove_session(&endpoint, session_id);
    shutdown_cli_daemon(&data_dir, child);
}

fn event_belongs_to_route(
    event: &botster_hub_client::DaemonEvent,
    session_id: &str,
    subscription_id: &str,
) -> bool {
    match event {
        botster_hub_client::DaemonEvent::AttachState {
            session_id: routed_session,
            subscription_id: routed_subscription,
            ..
        }
        | botster_hub_client::DaemonEvent::Snapshot {
            session_id: routed_session,
            subscription_id: routed_subscription,
            ..
        }
        | botster_hub_client::DaemonEvent::TerminalOutput {
            session_id: routed_session,
            subscription_id: routed_subscription,
            ..
        }
        | botster_hub_client::DaemonEvent::Scrollback {
            session_id: routed_session,
            subscription_id: routed_subscription,
            ..
        } => routed_session == session_id && routed_subscription == subscription_id,
        _ => true,
    }
}

#[test]
fn socket_concurrent_attaches_queue_and_keep_scoped_routes() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("socket-concurrent-attach");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon_with_worker_egress_capacity(&data_dir, Some(1));
    let session_id = "concurrent-attach";
    let subscription_a = "concurrent-attach-a";
    let subscription_b = "concurrent-attach-b";
    let ready_path = data_dir.join("concurrent-ready");
    let mut connection_a =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect A");
    let mut connection_b =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect B");

    connection_a
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                concat!(
                    "stty -echo 2>/dev/null; ",
                    "i=0; while [ $i -lt 2000 ]; do printf 'history-%04d\\n' \"$i\"; i=$((i+1)); done; ",
                    "printf 'PRE-BARRIER-MARKER\\n'; : > '{}'; ",
                    "while IFS= read -r line; do printf \"echo:%s\\n\" \"$line\"; done"
                ),
                ready_path.display()
            ),
        })
        .expect("spawn concurrent history producer");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, session_id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ready_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists(),
        "timed out waiting for concurrent history producer"
    );

    let attach_a = connection_a
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_a.to_string(),
        })
        .expect("attach A");
    assert!(
        attach_a.events.is_empty(),
        "A Attach must not return terminal bodies: {:?}",
        attach_a.events
    );
    let drain_a = connection_a
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status A");
    assert!(
        drain_a.events.iter().all(|event| event_belongs_to_route(
            event,
            session_id,
            subscription_a
        ) && !matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
                | botster_hub_client::DaemonEvent::AttachState { .. }
                | botster_hub_client::DaemonEvent::TerminalOutput { .. }
        )),
        "A host Status must stay empty of terminal bodies: {:?}",
        drain_a.events
    );

    let attach_b = connection_b
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_b.to_string(),
        })
        .expect("attach B alongside A");
    assert!(
        attach_b.events.is_empty(),
        "B Attach must not return terminal bodies: {:?}",
        attach_b.events
    );

    let early_b = connection_b
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status B during A's barrier");
    assert!(
        early_b.events.iter().all(|event| event_belongs_to_route(
            event,
            session_id,
            subscription_b
        )),
        "B must not receive A's frames: {:?}",
        early_b.events
    );
    assert!(
        !early_b.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::AttachState { state, .. } if state == "attached"
        )),
        "B must stay queued until A finishes: {:?}",
        early_b.events
    );

    let screen_a = wait_for_read_screen_contains(&mut connection_a, session_id, "history-0000");
    assert!(
        screen_a.contains("history-0000"),
        "A route can read the host screen: {screen_a:?}"
    );
    let screen_b = wait_for_read_screen_contains(&mut connection_b, session_id, "history-0000");
    assert!(
        screen_b.contains("history-0000"),
        "B route can read the same host screen: {screen_b:?}"
    );

    connection_b
        .send_terminal_frame(
            session_id,
            subscription_b,
            &terminal_input_frame_bytes(b"CONCURRENT-POST\n"),
        )
        .expect("input after concurrent attaches");
    let live_b =
        wait_for_read_screen_contains(&mut connection_b, session_id, "echo:CONCURRENT-POST");
    assert!(
        live_b.contains("echo:CONCURRENT-POST"),
        "B live output is on ReadScreen: {live_b:?}"
    );

    session_cleanup.disarm();
    production_shutdown_and_remove_session(&endpoint, session_id);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn socket_post_ready_history_failure_attaches_without_finish() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("socket-history-incomplete");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon_with_snapshot_history_failure(&data_dir);
    let session_id = "history-incomplete";
    let subscription_id = "history-incomplete-sub";
    let ready_path = data_dir.join("failure-ready");
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect");

    connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                concat!(
                    "stty -echo 2>/dev/null; ",
                    "i=0; while [ $i -lt 1000 ]; do printf 'failure-history-%04d\\n' \"$i\"; i=$((i+1)); done; ",
                    "printf 'FAILURE-READY\\n'; : > '{}'; ",
                    "while IFS= read -r line; do printf \"echo:%s\\n\" \"$line\"; done"
                ),
                ready_path.display()
            ),
        })
        .expect("spawn history-failure producer");
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, session_id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ready_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready_path.exists(),
        "timed out waiting for history-failure producer"
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("attach");
    assert_eq!(
        attach.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "attach is a host ack: {:?}",
        attach.error
    );
    assert!(
        attach.events.is_empty(),
        "attach acks with empty terminal bodies: {:?}",
        attach.events
    );
    connection
        .send_terminal_frame(
            session_id,
            subscription_id,
            &terminal_input_frame_bytes(b"FAILURE-POST\n"),
        )
        .expect("queue input during failed history");
    let drain = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("host status after incomplete history attach");
    assert!(
        drain.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::Snapshot { .. }
                | botster_hub_client::DaemonEvent::AttachState { .. }
                | botster_hub_client::DaemonEvent::TerminalOutput { .. }
        )),
        "host Status must not invent FINISH or attach phases: {:?}",
        drain.events
    );
    let screen = wait_for_read_screen_contains(&mut connection, session_id, "echo:FAILURE-POST");
    assert!(
        screen.contains("echo:FAILURE-POST"),
        "queued input must apply after attach even when Core history is incomplete: {screen:?}"
    );

    session_cleanup.disarm();
    production_shutdown_and_remove_session(&endpoint, session_id);
    shutdown_cli_daemon(&data_dir, child);
}
