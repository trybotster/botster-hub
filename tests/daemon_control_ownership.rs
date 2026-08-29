//! Ownership guards for the daemon control-plane move.

use std::fs;
use std::path::PathBuf;

fn hub_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

fn daemon_sources() -> Vec<(String, String)> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/daemon");
    let mut pending = vec![root.clone()];
    let mut files = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).expect("read src/daemon") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let rel = path
                .strip_prefix(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
                .expect("under crate root")
                .to_string_lossy()
                .replace('\\', "/");
            files.push((rel.clone(), hub_source(&rel)));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

#[test]
fn daemon_modules_reject_unix_transport_mechanism_symbols() {
    for (path, source) in daemon_sources() {
        let production = source.split("mod tests").next().unwrap_or(&source);
        for needle in [
            "async fn accept_connections",
            "async fn handle_connection_async",
            "struct MuxWriteState",
            "async fn read_async_frame",
            "fn prepare_socket_path",
            "fn unix_event_flush_stalled",
        ] {
            assert!(
                !production.contains(needle),
                "{path} must not contain {needle}"
            );
        }
    }
}

#[test]
fn webrtc_liveness_gates_remain_four_distinct_sites() {
    let connection = hub_source("src/daemon/control/connection.rs");
    let control = hub_source("src/daemon/control.rs");
    let entities = hub_source("src/daemon/control/entities.rs");
    assert!(
        connection.contains("has_live_peer(&grant_id)"),
        "RegisterWebrtcAdmission insert gate must stay in connection.rs"
    );
    assert!(
        !connection.contains("local_webrtc_peer_gone_request_error"),
        "connection insert gate drops rather than returning a request error"
    );
    let request_open = format!(
        "ControlMessage::Request {}",
        char::from_u32(0x7b).expect("left brace")
    );
    let request_gate = control
        .split(&request_open)
        .nth(1)
        .expect("Request arm")
        .split("        ControlMessage::HubUpdateCheckCompleted")
        .next()
        .expect("Request arm end");
    assert!(
        request_gate.contains("has_live_peer(grant_id)"),
        "Request pre-dispatch gate must stay in control.rs"
    );
    assert!(
        request_gate.contains("local_webrtc_peer_gone_request_error"),
        "Request gate must use local_webrtc_peer_gone_request_error"
    );
    let sessions_call = request_gate.find("handle_control_request");
    let live_peer = request_gate.find("has_live_peer(grant_id)");
    assert!(
        live_peer.is_some()
            && sessions_call.is_some()
            && live_peer.expect("gate") < sessions_call.expect("delegate"),
        "Request has_live_peer gate must precede family delegation"
    );
    assert!(
        entities.contains("has_live_peer(grant_id)") && entities.contains("local_webrtc_peer_gone"),
        "SubscribeEntities reply gate must stay in entities.rs"
    );
    assert!(
        entities.contains("EntityUnsubscribed") && entities.contains("owner_grant_id"),
        "UnsubscribeEntities owner-checked gate must stay in entities.rs"
    );
}

#[test]
fn daemon_control_does_not_remove_grant_rows() {
    for (path, source) in daemon_sources() {
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(
            !production.contains("prune_expired_grants"),
            "{path} must not prune grant rows"
        );
        assert!(
            !production.contains("GrantRegistry"),
            "{path} must not name GrantRegistry"
        );
    }
}

#[test]
fn runtime_dispatcher_delegates_to_sessions() {
    let dispatcher = hub_source("src/daemon/control.rs");
    let runtime = dispatcher
        .split("pub(crate) fn handle_runtime_control_request(")
        .nth(1)
        .expect("runtime dispatcher");
    assert!(
        runtime.contains("sessions::handle_runtime("),
        "handle_runtime_control_request must delegate"
    );
    assert!(
        !runtime.contains("HubClientApi"),
        "runtime dispatcher must not construct HubClientApi"
    );
}
