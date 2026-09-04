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
    let request = hub_source("src/daemon/control/request.rs");
    let entities = hub_source("src/daemon/control/entities.rs");
    assert!(
        connection.contains("has_live_peer(&grant_id)"),
        "RegisterWebrtcAdmission insert gate must stay in connection.rs"
    );
    assert!(
        !connection.contains("local_webrtc_peer_gone_request_error"),
        "connection insert gate drops rather than returning a request error"
    );
    assert!(
        request.contains("has_live_peer(grant_id)"),
        "Request pre-dispatch gate must stay in request.rs"
    );
    assert!(
        request.contains("local_webrtc_peer_gone_request_error"),
        "Request gate must use local_webrtc_peer_gone_request_error"
    );
    let request_body = request
        .split("pub(crate) fn handle(")
        .nth(1)
        .expect("request owner");
    let sessions_call = request_body.find("handle_control_request");
    let live_peer = request_body.find("has_live_peer(grant_id)");
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

fn request_variant_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let needle = "DaemonRequest::";
    let mut rest = source;
    while let Some(index) = rest.find(needle) {
        rest = &rest[index + needle.len()..];
        let name: String = rest
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

fn control_message_variant_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let needle = "ControlMessage::";
    let mut rest = source;
    while let Some(index) = rest.find(needle) {
        rest = &rest[index + needle.len()..];
        let name: String = rest
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

fn enum_variants(source: &str, enum_name: &str) -> Vec<String> {
    let pub_header = format!("pub enum {enum_name} ");
    let crate_header = format!("pub(crate) enum {enum_name} ");
    let after = source
        .split(&pub_header)
        .nth(1)
        .or_else(|| source.split(&crate_header).nth(1))
        .expect(enum_name);
    let start = after.find('{').expect("enum body");
    let mut depth = 0;
    let mut body = String::new();
    for ch in after[start..].chars() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        if depth >= 1 {
            body.push(ch);
        }
    }
    let mut names = Vec::new();
    let mut depth = 0;
    for line in body.lines() {
        let trimmed = line.trim();
        let line_depth = depth;
        depth += trimmed.matches('{').count() as i32 - trimmed.matches('}').count() as i32;
        if line_depth > 1 {
            continue;
        }
        if trimmed.starts_with('#') || trimmed.starts_with('/') || trimmed.is_empty() {
            continue;
        }
        let ident: String = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect();
        if ident
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase())
        {
            names.push(ident);
        }
    }
    names
}

const FAMILY_OWNERS: &[(&str, &str, &[&str])] = &[
    (
        "src/daemon/control/sessions.rs",
        "sessions",
        &[
            "Status",
            "ListSessions",
            "RemoveSession",
            "Spawn",
            "Attach",
            "Detach",
            "ShutdownSession",
            "ReadScreen",
            "ReadModeFlags",
            "CaptureSnapshot",
            "ReadSessionContext",
        ],
    ),
    (
        "src/daemon/control/session_types.rs",
        "session_types",
        &[
            "ListSessionTypes",
            "ListSessionTypesForTarget",
            "ShowSessionType",
            "ShowSessionTypeDefinition",
            "CreateSessionType",
            "UpdateSessionType",
            "DeleteSessionType",
            "ResolveSessionType",
            "SpawnSessionType",
        ],
    ),
    (
        "src/daemon/control/spawn_targets.rs",
        "spawn_targets",
        &[
            "ListSpawnTargets",
            "ShowSpawnTarget",
            "CreateSpawnTarget",
            "UpdateSpawnTarget",
            "DeleteSpawnTarget",
            "ValidateSpawnTarget",
            "ListWorktrees",
            "ShowWorktree",
            "CreateWorktree",
            "DeleteWorktree",
        ],
    ),
    (
        "src/daemon/control/packages.rs",
        "packages",
        &[
            "ListApps",
            "ResolveAppLaunch",
            "ResolvePackageRoute",
            "ListPackageNavigation",
            "ListPackages",
            "ListAvailablePackages",
            "InspectAvailablePackage",
            "PreviewPackageInstall",
            "InstallPackageRegistryEntry",
            "InstallPackageLocalPath",
            "CheckPackageUpdate",
            "PreviewPackageUpdate",
            "ApplyPackageUpdate",
            "ShowPackage",
            "SetPackageConfiguration",
            "ReloadPackage",
            "RefreshLocalPackages",
            "EnablePackageLocalPath",
            "EnablePackage",
            "DisablePackage",
            "RemovePackage",
            "StartPackageEntrypoint",
            "StopPackageEntrypoint",
            "RestartPackageEntrypoint",
            "PackageEntrypointStatus",
        ],
    ),
    (
        "src/daemon/control/messaging.rs",
        "messaging",
        &[
            "Whoami",
            "PostMessage",
            "ReceiveMessages",
            "AckMessage",
            "NotifySession",
        ],
    ),
    (
        "src/daemon/control/plugins.rs",
        "plugins",
        &[
            "PluginMcpListTools",
            "PluginMcpCallTool",
            "PluginSurfaceRender",
            "PluginSurfaceAction",
            "PluginLifecycleStatus",
        ],
    ),
    (
        "src/daemon/control/entities.rs",
        "entities",
        &["SubscribeEntities", "UnsubscribeEntities"],
    ),
    (
        "src/daemon/control/events.rs",
        "events",
        &["SubscribeEvents", "UnsubscribeEvents"],
    ),
    (
        "src/daemon/control/webrtc.rs",
        "webrtc",
        &["IssueLocalWebrtcBootstrap", "LocalWebrtcSignal"],
    ),
    (
        "src/daemon/control/host.rs",
        "host",
        &[
            "CheckHubUpdate",
            "StartHubUpdate",
            "GetHubUpdateExecution",
            "DaemonShutdown",
        ],
    ),
];

#[test]
fn each_daemon_request_has_exactly_one_family_owner() {
    let declared = enum_variants(
        &hub_source("crates/botster-hub-client/src/lib.rs"),
        "DaemonRequest",
    );
    let mut mapped = Vec::new();
    for (path, _family, variants) in FAMILY_OWNERS {
        mapped.extend(variants.iter().copied().map(str::to_string));
        let named = request_variant_names(&hub_source(path));
        for variant in *variants {
            assert!(
                named.iter().any(|name| name == variant),
                "{path} must own DaemonRequest::{variant}"
            );
        }
    }
    mapped.sort();
    mapped.dedup();
    let mut declared_sorted = declared.clone();
    declared_sorted.sort();
    assert_eq!(
        declared_sorted, mapped,
        "ownership matrix must cover every DaemonRequest variant exactly once"
    );

    let family_paths: Vec<&str> = FAMILY_OWNERS.iter().map(|(path, ..)| *path).collect();
    for (owner_path, _family, variants) in FAMILY_OWNERS {
        for other_path in &family_paths {
            if other_path == owner_path {
                continue;
            }
            let named = request_variant_names(&hub_source(other_path));
            for variant in *variants {
                if *other_path == "src/daemon/control/webrtc.rs" && *variant == "Detach" {
                    continue;
                }
                assert!(
                    !named.iter().any(|name| name == variant),
                    "{other_path} must not own DaemonRequest::{variant}; owner is {owner_path}"
                );
            }
        }
    }

    let webrtc = hub_source("src/daemon/control/webrtc.rs");
    assert!(
        webrtc.contains("DaemonRequest::Detach"),
        "PeerClosed sweep may construct DaemonRequest::Detach"
    );
}

#[test]
fn dispatcher_names_request_variants_only_in_delegating_arms() {
    let dispatcher = hub_source("src/daemon/control.rs");
    for forbidden in [
        "HubClientApi",
        "FileHubStateStore",
        "overlay_live_attach_occupancy",
        "drain_owned_before",
        "record_acknowledged_spawn",
        "events::handle_client_event_request",
        "host::handle_request",
    ] {
        assert!(
            !dispatcher.contains(forbidden),
            "control.rs must not contain request-specific {forbidden}"
        );
    }
    let request_open = format!(
        "ControlMessage::Request {}",
        char::from_u32(0x7b).expect("left brace")
    );
    let request_arm = dispatcher
        .split(&request_open)
        .nth(1)
        .expect("Request arm")
        .split("ControlMessage::HubUpdateCheckCompleted")
        .next()
        .expect("Request arm end");
    assert!(
        request_arm.contains("request::handle("),
        "Request arm must delegate once to request::handle"
    );
    assert_eq!(
        request_arm.matches("::handle(").count(),
        1,
        "Request arm must contain exactly one handle delegation: {request_arm}"
    );
    let runtime = dispatcher
        .split("pub(crate) fn handle_runtime_control_request(")
        .nth(1)
        .expect("runtime dispatcher");
    assert!(
        runtime.contains("sessions::handle_runtime("),
        "session family must be delegated"
    );
    assert!(
        runtime.contains("session_types::handle_runtime("),
        "session-type family must be delegated"
    );
    assert!(
        runtime.contains("messaging::handle_runtime("),
        "messaging family must be delegated"
    );
    assert!(
        runtime.contains("plugins::handle_runtime("),
        "plugin family must be delegated"
    );
    assert!(
        runtime.contains("host::handle_runtime("),
        "host shutdown must be delegated"
    );
    assert!(
        !runtime.contains("HubClientApi"),
        "runtime dispatcher must not construct HubClientApi"
    );
}

const CONTROL_MESSAGE_OWNERS: &[(&str, &[&str])] = &[
    (
        "src/daemon/control/connection.rs",
        &[
            "AcceptedConnection",
            "RejectedConnection",
            "RegisterUnixAdmission",
            "RegisterWebrtcAdmission",
            "InspectReservation",
            "BindReservedSubscription",
            "RetireReservedSubscription",
            "AuthorizeSubscriptionSend",
            "AuthorizeSubscriptionHelloAck",
        ],
    ),
    (
        "src/daemon/control/entities.rs",
        &["SubscribeEntities", "UnsubscribeEntities"],
    ),
    ("src/daemon/control/request.rs", &["Request"]),
    ("src/daemon/control/host.rs", &["HubUpdateCheckCompleted"]),
    ("src/daemon/control/webrtc.rs", &["LocalWebrtcPeerClosed"]),
];

const CONTROL_MESSAGE_DISPATCHER_OWNED: &[&str] = &["DataPlaneProgress", "EgressWriteFailed"];

fn control_handler_modules() -> Vec<String> {
    let mut paths = Vec::new();
    for (path, _) in daemon_sources() {
        if !path.starts_with("src/daemon/control/") {
            continue;
        }
        if path == "src/daemon/control.rs" || path == "src/daemon/control/message.rs" {
            continue;
        }
        paths.push(path);
    }
    paths.sort();
    paths
}

#[test]
fn control_message_variants_have_one_family_or_dispatcher_owner() {
    let declared = enum_variants(
        &hub_source("src/daemon/control/message.rs"),
        "ControlMessage",
    );
    let mut mapped: Vec<String> = CONTROL_MESSAGE_OWNERS
        .iter()
        .flat_map(|(_, variants)| variants.iter().copied().map(str::to_string))
        .chain(
            CONTROL_MESSAGE_DISPATCHER_OWNED
                .iter()
                .copied()
                .map(str::to_string),
        )
        .collect();
    mapped.sort();
    let mut declared_sorted = declared.clone();
    declared_sorted.sort();
    assert_eq!(
        declared_sorted, mapped,
        "ControlMessage matrix must cover every variant exactly once"
    );

    for (path, variants) in CONTROL_MESSAGE_OWNERS {
        let named = control_message_variant_names(&hub_source(path));
        for variant in *variants {
            assert!(
                named.iter().any(|name| name == variant),
                "{path} must own ControlMessage::{variant}"
            );
        }
    }
    let dispatcher = hub_source("src/daemon/control.rs");
    let dispatcher_named = control_message_variant_names(&dispatcher);
    for variant in CONTROL_MESSAGE_DISPATCHER_OWNED {
        assert!(
            dispatcher_named.iter().any(|name| name == *variant),
            "control.rs must own ControlMessage::{variant}"
        );
    }
    assert!(dispatcher.contains("request::handle"));
    assert!(dispatcher.contains("webrtc::handle_peer_closed"));
    assert!(dispatcher.contains("connection::handle"));
    assert!(dispatcher.contains("entities::handle"));
    let request = hub_source("src/daemon/control/request.rs");
    assert!(request.contains("overlay_live_attach_occupancy"));
    assert!(request.contains("has_live_peer(grant_id)"));

    let owner_paths: Vec<&str> = CONTROL_MESSAGE_OWNERS
        .iter()
        .map(|(path, _)| *path)
        .collect();
    for other_path in control_handler_modules() {
        let named = control_message_variant_names(&hub_source(&other_path));
        for (owner_path, variants) in CONTROL_MESSAGE_OWNERS {
            if other_path == *owner_path {
                continue;
            }
            for variant in *variants {
                assert!(
                    !named.iter().any(|name| name == variant),
                    "{other_path} must not own ControlMessage::{variant}; owner is {owner_path}"
                );
            }
        }
        for variant in CONTROL_MESSAGE_DISPATCHER_OWNED {
            assert!(
                !named.iter().any(|name| name == *variant),
                "{other_path} must not own dispatcher ControlMessage::{variant}"
            );
        }
    }
    let _ = owner_paths;
}

#[test]
fn duplicating_a_variant_into_the_wrong_owner_fails_the_matrix() {
    let sessions = hub_source("src/daemon/control/sessions.rs");
    let plugins = hub_source("src/daemon/control/plugins.rs");
    let host = hub_source("src/daemon/control/host.rs");
    assert!(
        sessions.contains("DaemonRequest::Attach") && !plugins.contains("DaemonRequest::Attach"),
        "Attach must not be duplicated into plugins.rs"
    );
    for deleted in [
        "DaemonRequest::SendInput",
        "DaemonRequest::ModeGatedInput",
        "DaemonRequest::Resize",
    ] {
        assert!(
            !sessions.contains(deleted) && !plugins.contains(deleted) && !host.contains(deleted),
            "{deleted} must not remain a JSON control request"
        );
    }
    assert!(
        host.contains("DaemonRequest::CheckHubUpdate")
            && !sessions.contains("DaemonRequest::CheckHubUpdate"),
        "CheckHubUpdate must not be duplicated into sessions.rs"
    );
    assert!(
        plugins.contains("DaemonRequest::PluginMcpListTools")
            && !sessions.contains("DaemonRequest::PluginMcpListTools"),
        "PluginMcpListTools must not remain in sessions.rs"
    );
    let request = hub_source("src/daemon/control/request.rs");
    assert!(
        control_message_variant_names(&request)
            .iter()
            .any(|name| name == "Request")
            && !control_message_variant_names(&plugins)
                .iter()
                .any(|name| name == "Request"),
        "ControlMessage::Request must not be duplicated into plugins.rs"
    );
    let _ = request_variant_names(&sessions);
}

#[test]
fn control_rs_request_arm_rejects_inlined_post_processing() {
    let dispatcher = hub_source("src/daemon/control.rs");
    for needle in [
        "overlay_live_attach_occupancy",
        "drain_owned_before",
        "acknowledged_spawn_ids",
        "shutdown_error_host_close",
        "explicit_detach",
    ] {
        assert!(
            !dispatcher.contains(needle),
            "inserting {needle} into control.rs must fail this single-delegation boundary"
        );
    }
}
