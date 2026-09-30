/// Start a session running `command`, attach a route to it and return the
/// daemon, its data directory and the route connection.
fn doorbell_session(
    name: &str,
    command: &str,
) -> (std::path::PathBuf, PanicSafeCliDaemon, UnixRouteClient) {
    let data_dir = unique_test_dir(name);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: format!("{name}-session"),
            command: command.to_string(),
        },
    )
    .expect("spawn doorbell session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);
    let mut connection = lifecycle_connection_for(&config).expect("connect socket");
    connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: format!("{name}-session"),
            subscription_id: format!("{name}-sub"),
        })
        .expect("attach route");
    (data_dir, child, connection)
}

/// Ring the session through the native tool and check the queued answer.
fn ring_session(connection: &mut UnixRouteClient, session_id: &str, text: &str) {
    let answer = connection
        .request(&botster_hub::DaemonRequest::NotifySession {
            session_id: session_id.to_string(),
            data: text.to_string(),
        })
        .expect("ring the session");
    assert_eq!(answer.kind, botster_hub::DaemonResponseKind::SessionNotified);
    let notify = answer
        .coordination
        .and_then(|coordination| coordination.notify)
        .expect("ring answer");
    assert_eq!(notify.decision, "queued");
}

/// Everything the route shows until `needle` appears or the deadline passes.
fn observe_until(connection: &mut UnixRouteClient, needle: &str, within: Duration) -> String {
    let deadline = Instant::now() + within;
    let mut observed = String::new();
    while Instant::now() < deadline && !observed.contains(needle) {
        for event in connection.poll_route_events(Duration::from_millis(30)) {
            if let Some(bytes) = event.output() {
                observed.push_str(&live_output_utf8(bytes));
            }
        }
    }
    observed
}

#[test]
fn a_ring_types_its_text_and_leaves_no_probe_behind() {
    let _guard = daemon_test_guard();
    let (data_dir, child, mut connection) = doorbell_session(
        "doorbell-types",
        "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
    );
    // Ring a session that is already reading. Output that lands between the
    // probe's echo and its erase makes the terminal reprint the line, which
    // is the terminal's behaviour and not the doorbell's.
    let started = observe_until(&mut connection, "ready", Duration::from_secs(20));
    assert!(started.contains("ready"), "the program started, got {started:?}");
    ring_session(&mut connection, "doorbell-types-session", "ring-text-one");
    let observed = observe_until(&mut connection, "echo:ring-text-one", Duration::from_secs(20));
    assert!(
        observed.contains("echo:ring-text-one"),
        "the ring text arrives as a line, got {observed:?}"
    );
    assert!(
        !observed.contains("echo:zx"),
        "the probe was erased before the line ended, got {observed:?}"
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn a_ring_waits_while_the_program_hides_the_cursor() {
    let _guard = daemon_test_guard();
    // The program hides the cursor and reads one line. A ring typed now would
    // be that line and would echo nothing. The human line that follows shows
    // the cursor, and then the ring may type.
    let (data_dir, child, mut connection) = doorbell_session(
        "doorbell-hidden",
        "printf '\\033[?25l'; IFS= read -r go; printf '\\033[?25h'; \
         while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
    );
    let hidden = observe_until(&mut connection, "\u{1b}[?25l", Duration::from_secs(20));
    assert!(hidden.contains("\u{1b}[?25l"), "the cursor is hidden, got {hidden:?}");
    ring_session(&mut connection, "doorbell-hidden-session", "ring-text-two");
    connection
        .send_terminal_frame(
            "doorbell-hidden-sub",
            &terminal_input_frame_bytes(b"go\r"),
        )
        .expect("send the human line");
    let observed = observe_until(&mut connection, "echo:ring-text-two", Duration::from_secs(30));
    assert!(
        observed.contains("echo:ring-text-two"),
        "the ring types once the cursor shows, got {observed:?}"
    );
    assert!(
        !observed.contains("echo:go"),
        "the ring was not typed while the cursor was hidden, got {observed:?}"
    );
    shutdown_cli_daemon(&data_dir, child);
}
