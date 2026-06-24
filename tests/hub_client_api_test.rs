#![cfg(unix)]

use std::time::{Duration, Instant};
use std::{fs, thread};

use botster_core::{
    Capability, CapabilitySurface, ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, ModeFlags,
    RequestId, SessionId, SessionLifecycleState, SubscriptionId,
};
use botster_core_daemon::{GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubClientAdmission, HubClientApi, HubClientError,
    HubClientEvent, HubClientIdentity, HubClientOperation, HubClientPackageClassification,
    HubClientPackageState, HubClientRequest, HubClientResponseBody, HubClientRole, HubRuntime,
    HubStartupOptions, PackageProvenance, PackageRegistry, RuntimeEnvironment, SessionDefaults,
    TransportBindings,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    ensure_session_worker_binary();
    let data_directory = format!("target/botster-hub-test-data/client-api-{name}");
    let _ = fs::remove_dir_all(&data_directory);
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-client-api-test".to_string(),
            display_name: "Hub Client API Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory.into()),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn session_id() -> SessionId {
    SessionId("hub-client-api-session".to_string())
}

fn subscription_id() -> SubscriptionId {
    SubscriptionId("hub-client-api-subscription".to_string())
}

fn empty_registry() -> PackageRegistry {
    PackageRegistry::new(Vec::<Capability>::new().into_iter().collect())
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn plugin_manifest(name: &str, capabilities: Vec<Capability>) -> botster_core::PackageManifest {
    botster_core::PackageManifest {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Plugin,
        botster: ">=0.1.0".to_string(),
        source: Some(botster_core::PackageSource::Git {
            repo: "https://example.invalid/botster/plugin.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities,
        entrypoints: vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Lua,
            path: "plugin.lua".to_string(),
            bootstrap: false,
        }],
        host_profile: None,
        configuration: None,
        surfaces: Vec::new(),
    }
}

fn provenance() -> PackageProvenance {
    PackageProvenance {
        source: "local-private-source".to_string(),
        checksum: Some("sha256:test".to_string()),
    }
}

fn drain_until(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        for event in events {
            if let HubClientEvent::TerminalOutput { data, .. } = event {
                observed.extend(data);
            }
        }

        if observed
            .windows(needle.len())
            .any(|window| window == needle)
        {
            return observed;
        }

        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} in {:?}",
        String::from_utf8_lossy(needle),
        String::from_utf8_lossy(&observed)
    );
}

fn drain_events_until(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    subscription_id: &SubscriptionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<HubClientEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain-events"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        if observed.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id: observed_subscription_id,
                    data,
                    ..
                } if observed_subscription_id == subscription_id
                    && data.windows(needle.len()).any(|window| window == needle)
            )
        }) {
            return observed;
        }

        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} on {:?}",
        String::from_utf8_lossy(needle),
        subscription_id
    );
}

fn history_data(event: &HubClientEvent) -> Option<(&SubscriptionId, &[u8])> {
    match event {
        HubClientEvent::Snapshot {
            subscription_id,
            data,
            ..
        }
        | HubClientEvent::Scrollback {
            subscription_id,
            data,
            ..
        } => Some((subscription_id, data)),
        _ => None,
    }
}

fn drain_events_for(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    logical_clock: &mut u64,
    duration: Duration,
) -> Vec<HubClientEvent> {
    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain-extra"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("extra drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        thread::sleep(Duration::from_millis(20));
    }

    observed
}

#[test]
fn late_attach_receives_prior_terminal_history_before_later_live_output() {
    let first_api = HubClientApi::local_operator("late-history-first-client");
    let late_api = HubClientApi::local_operator("late-history-late-client");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("late-history");
    let session_id = SessionId("late-history-session".to_string());
    let first_subscription = SubscriptionId("late-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("late-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("late-history-spawn"),
                session_id: session_id.clone(),
                command: "printf 'before-late\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn late-history session");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("late-history-first-attach"),
                session_id: session_id.clone(),
                subscription_id: first_subscription,
                now_seconds: logical_clock,
            },
        )
        .expect("attach first subscription");
    logical_clock += 1;
    drain_until(
        &first_api,
        &mut runtime,
        &packages,
        &session_id,
        b"before-late",
        &mut logical_clock,
    );

    late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("late-history-late-attach"),
                session_id: session_id.clone(),
                subscription_id: late_subscription.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach late subscription");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("late-history-input"),
                session_id: session_id.clone(),
                data: b"live-after-late\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("send live output after late attach");
    logical_clock += 1;

    let events = drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-after-late",
        &mut logical_clock,
    );
    let history_index = events
        .iter()
        .position(|event| {
            history_data(event).is_some_and(|(subscription_id, data)| {
                subscription_id == &late_subscription
                    && data
                        .windows(b"before-late".len())
                        .any(|window| window == b"before-late")
            })
        })
        .unwrap_or_else(|| {
            panic!("late subscription should receive prior output as history, got {events:?}")
        });
    let live_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == &late_subscription
                    && data
                        .windows(b"after:live-after-late".len())
                        .any(|window| window == b"after:live-after-late")
            )
        })
        .expect("late subscription should receive later live output");

    assert!(
        history_index < live_index,
        "late history should precede later live output, got {events:?}"
    );
}

#[test]
fn late_attach_without_prior_output_does_not_fabricate_history() {
    let first_api = HubClientApi::local_operator("no-history-first-client");
    let late_api = HubClientApi::local_operator("no-history-late-client");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("no-history");
    let session_id = SessionId("no-history-session".to_string());
    let first_subscription = SubscriptionId("no-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("no-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("no-history-spawn"),
                session_id: session_id.clone(),
                command: "while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done"
                    .to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn no-history session");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("no-history-first-attach"),
                session_id: session_id.clone(),
                subscription_id: first_subscription,
                now_seconds: logical_clock,
            },
        )
        .expect("attach first no-history subscription");
    logical_clock += 1;

    late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("no-history-late-attach"),
                session_id: session_id.clone(),
                subscription_id: late_subscription.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach late no-history subscription");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("no-history-input"),
                session_id: session_id.clone(),
                data: b"live-only\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("send live output after no-history late attach");
    logical_clock += 1;

    let events = drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-only",
        &mut logical_clock,
    );

    assert!(
        !events.iter().any(|event| {
            history_data(event).is_some_and(|(subscription_id, data)| {
                subscription_id == &late_subscription && !data.is_empty()
            })
        }),
        "late no-history subscription should not receive fabricated history, got {events:?}"
    );
}

#[test]
fn local_client_api_exercises_status_spawn_attach_input_resize_detach_shutdown_and_events() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let second_api = HubClientApi::local_operator("local-client-api-test-two");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("session-flow");
    let session_id = session_id();
    let subscription_id = subscription_id();
    let second_subscription_id = SubscriptionId("hub-client-api-subscription-two".to_string());
    let mut logical_clock = 100;

    let status = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Status {
                request_id: request_id("status"),
            },
        )
        .expect("status through client api");
    let HubClientResponseBody::Status(status) = status.body else {
        panic!("status response expected");
    };
    assert_eq!(status.profile_id, "botster-hub");
    assert_eq!(status.session_count, 0);

    let sessions = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessions {
                request_id: request_id("list-empty"),
            },
        )
        .expect("list through client api");
    assert!(
        matches!(sessions.body, HubClientResponseBody::Sessions(sessions) if sessions.is_empty())
    );

    let spawn = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("spawn"),
                session_id: session_id.clone(),
                command: "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn through client api");
    logical_clock += 1;
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawned response expected");
    };
    assert_eq!(spawned.session.session_id, session_id);
    assert_eq!(spawned.session.lifecycle, SessionLifecycleState::Running);
    assert!(spawned.events.is_empty());

    let attach = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("attach"),
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach through client api");
    logical_clock += 1;
    assert!(matches!(attach.body, HubClientResponseBody::Events(_)));
    second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("attach-two"),
                session_id: session_id.clone(),
                subscription_id: second_subscription_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach second client through client api");
    logical_clock += 1;

    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"ready",
        &mut logical_clock,
    );

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Resize {
            request_id: request_id("resize"),
            session_id: session_id.clone(),
            rows: 30,
            cols: 100,
            now_seconds: logical_clock,
        },
    )
    .expect("resize through client api");
    logical_clock += 1;

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Input {
            request_id: request_id("input"),
            session_id: session_id.clone(),
            data: b"ping-hub\n".to_vec(),
            now_seconds: logical_clock,
        },
    )
    .expect("input through client api");
    logical_clock += 1;

    let echo_events = drain_events_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        &subscription_id,
        b"echo:ping-hub",
        &mut logical_clock,
    );
    assert!(echo_events.iter().any(|event| {
        matches!(
            event,
            HubClientEvent::TerminalOutput {
                subscription_id: observed_subscription_id,
                data,
                ..
            } if observed_subscription_id == &subscription_id
                && data
                    .windows(b"echo:ping-hub".len())
                    .any(|window| window == b"echo:ping-hub")
        )
    }));
    assert!(
        echo_events.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id: observed_subscription_id,
                    data,
                    ..
                } if observed_subscription_id == &second_subscription_id
                    && data
                        .windows(b"echo:ping-hub".len())
                        .any(|window| window == b"echo:ping-hub")
            )
        }),
        "both attached subscriptions should receive shared session output"
    );

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Detach {
            request_id: request_id("detach"),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("detach through client api");
    logical_clock += 1;

    second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("input-after-detach"),
                session_id: session_id.clone(),
                data: b"after-detach\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("input from still-attached client through client api");
    logical_clock += 1;

    let after_detach_events = drain_events_until(
        &second_api,
        &mut runtime,
        &packages,
        &session_id,
        &second_subscription_id,
        b"echo:after-detach",
        &mut logical_clock,
    );
    let extra_after_detach_events = drain_events_for(
        &second_api,
        &mut runtime,
        &packages,
        &session_id,
        &mut logical_clock,
        Duration::from_millis(200),
    );
    assert!(
        after_detach_events
            .iter()
            .chain(extra_after_detach_events.iter())
            .all(|event| {
                !matches!(
                    event,
                    HubClientEvent::TerminalOutput {
                        subscription_id: observed_subscription_id,
                        data,
                        ..
                    } if observed_subscription_id == &subscription_id
                        && data
                            .windows(b"echo:after-detach".len())
                            .any(|window| window == b"echo:after-detach")
                )
            }),
        "detached subscription should not receive later output, including after an extra drain window"
    );

    let shutdown = second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Shutdown {
                request_id: request_id("shutdown"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("shutdown through client api");
    let HubClientResponseBody::Events(events) = shutdown.body else {
        panic!("shutdown should return events");
    };
    assert!(events.is_empty());
}

#[test]
fn guarded_notification_write_is_hub_admitted_and_core_delivered() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let mut runtime = explicit_runtime("guarded-write");
    let session_actions = capability(
        CapabilitySurface::SessionActions,
        Some("guarded_session_notification_write"),
    );
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(
        vec![session_actions.clone(), surfaces.clone()]
            .into_iter()
            .collect(),
    );
    packages
        .install(
            plugin_manifest("workflow.plugin", vec![session_actions.clone()]),
            provenance(),
            "install package",
        )
        .expect("install allowed package");
    packages
        .enable("workflow.plugin", "enable package")
        .expect("enable allowed package");
    packages
        .install(
            plugin_manifest("blocked.plugin", vec![surfaces]),
            provenance(),
            "install blocked package",
        )
        .expect("install blocked package");
    packages
        .enable("blocked.plugin", "enable blocked package")
        .expect("enable blocked package");

    let session_id = SessionId("client-guarded".to_string());
    let subscription_id = SubscriptionId("client-guarded-subscription".to_string());
    let mut logical_clock = 200;
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Spawn {
            request_id: request_id("guarded-spawn"),
            session_id: session_id.clone(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn through client api");
    logical_clock += 1;
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Attach {
            request_id: request_id("guarded-attach"),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("attach through client api");
    logical_clock += 1;

    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"ready",
        &mut logical_clock,
    );

    let mode_flags = ModeFlags {
        cursor_visible: true,
        ..ModeFlags::default()
    };
    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::GuardedNotificationWrite {
                request_id: request_id("guarded-write"),
                session_id: session_id.clone(),
                package_name: "workflow.plugin".to_string(),
                data: b"guarded-client\n".to_vec(),
                readiness: ReadinessEvidence::ready(mode_flags.clone()),
                now_seconds: logical_clock,
            },
        )
        .expect("allowed package should write through core daemon");
    logical_clock += 1;
    let HubClientResponseBody::GuardedWrite(result) = response.body else {
        panic!("guarded write response expected");
    };
    assert!(matches!(result.decision, GuardedWriteDecision::Write));
    assert_eq!(
        result.states,
        vec![
            GuardedWriteDeliveryState::Accepted,
            GuardedWriteDeliveryState::Written
        ],
        "core daemon owns guarded-write delivery states"
    );
    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"echo:guarded-client",
        &mut logical_clock,
    );

    let denied = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::GuardedNotificationWrite {
                request_id: request_id("guarded-denied"),
                session_id,
                package_name: "blocked.plugin".to_string(),
                data: b"blocked\n".to_vec(),
                readiness: ReadinessEvidence::ready(mode_flags),
                now_seconds: logical_clock,
            },
        )
        .expect_err("ungranted package should be denied by hub policy");
    assert_eq!(
        denied,
        HubClientError::PackageCapabilityDenied {
            request_id: request_id("guarded-denied"),
            operation: HubClientOperation::GuardedNotificationWrite,
            package_name: "blocked.plugin".to_string(),
        }
    );
}

#[test]
fn read_screen_and_snapshot_return_typed_unsupported_until_daemon_api_exists() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("unsupported-daemon-ops");

    let read_screen = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadScreen {
                request_id: request_id("read-screen"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("daemon-backed read_screen should be typed unsupported");
    assert_eq!(
        read_screen,
        HubClientError::UnsupportedDaemonOperation {
            request_id: request_id("read-screen"),
            operation: HubClientOperation::ReadScreen,
            daemon_operation: "read_screen",
        }
    );

    let capture_snapshot = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CaptureSnapshot {
                request_id: request_id("capture-snapshot"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("daemon-backed capture_snapshot should be typed unsupported");
    assert_eq!(
        capture_snapshot,
        HubClientError::UnsupportedDaemonOperation {
            request_id: request_id("capture-snapshot"),
            operation: HubClientOperation::CaptureSnapshot,
            daemon_operation: "capture_snapshot",
        }
    );
}

#[test]
fn package_and_lifecycle_queries_are_sanitized_and_explicitly_pulled() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let mut runtime = explicit_runtime("packages");
    let surface = capability(CapabilitySurface::Surfaces, None);
    let network = capability(CapabilitySurface::Network, Some("localhost"));
    let package_root = "target/botster-hub-test-data/client-api-package-runnable";
    let _ = fs::remove_dir_all(package_root);
    fs::create_dir_all(format!("{package_root}/web")).expect("create package directories");
    fs::write(format!("{package_root}/plugin.lua"), "-- synthetic plugin").expect("write plugin");
    fs::write(format!("{package_root}/web/dev-server"), "#!/bin/sh\n")
        .expect("write runnable command");
    fs::write(
        format!("{package_root}/botster-package.json"),
        r#"{
  "name": "workflow.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
  "surfaces": [{
    "id": "workflow.home",
    "kind": "app",
    "title": "Workflow Home",
    "description": "Workflow dashboard",
    "icon": "workflow",
    "order": 10,
    "category": "workflows",
    "supports": ["render", "action"]
  }],
  "runnable_entrypoints": [{
    "id": "web",
    "kind": "web",
    "command": "web/dev-server",
    "args": ["--host", "127.0.0.1"],
    "working_directory": { "policy": "relative", "path": "web" },
    "environment": [{ "name": "BOTSTER_WEB_PORT", "required": false, "default": "5173" }],
    "mode": "dev",
    "capabilities": [{ "surface": "network", "scope": "localhost" }],
    "may_supervise": true
  }]
}
"#,
    )
    .expect("write package manifest");
    let mut packages = PackageRegistry::new(vec![surface.clone(), network].into_iter().collect());
    packages
        .install_local_path(package_root, "install package")
        .expect("install package");
    packages
        .enable("workflow.plugin", "enable package")
        .expect("enable package");

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Attach {
            request_id: request_id("attach-missing-session"),
            session_id: session_id(),
            subscription_id: subscription_id(),
            now_seconds: 1,
        },
    )
    .expect_err("attach is a transport handshake and should not hydrate packages");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("packages"),
            },
        )
        .expect("package query through client api");
    let HubClientResponseBody::Packages(records) = response.body else {
        panic!("packages response expected");
    };
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.package_name, "workflow.plugin");
    assert_eq!(
        record.classification,
        HubClientPackageClassification::Plugin
    );
    assert_eq!(record.state, HubClientPackageState::Enabled);
    assert_eq!(record.surfaces.len(), 1);
    let surface = &record.surfaces[0];
    assert_eq!(surface.id, "workflow.home");
    assert_eq!(surface.kind, "app");
    assert_eq!(surface.title, "Workflow Home");
    assert_eq!(surface.description.as_deref(), Some("Workflow dashboard"));
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(10));
    assert_eq!(surface.category.as_deref(), Some("workflows"));
    assert_eq!(surface.supports, ["render", "action"]);
    assert_eq!(record.runnable_entrypoints.len(), 1);
    let entrypoint = &record.runnable_entrypoints[0];
    assert_eq!(entrypoint.id, "web");
    assert_eq!(entrypoint.kind, "web");
    assert_eq!(entrypoint.command, "web/dev-server");
    assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
    assert_eq!(entrypoint.working_directory.policy, "relative");
    assert_eq!(entrypoint.working_directory.path.as_deref(), Some("web"));
    assert_eq!(entrypoint.environment[0].name, "BOTSTER_WEB_PORT");
    assert_eq!(entrypoint.environment[0].default.as_deref(), Some("5173"));
    assert_eq!(entrypoint.mode, "dev");
    assert_eq!(entrypoint.capabilities[0].surface, "Network");
    assert!(entrypoint.may_supervise);
    assert_eq!(entrypoint.process.state, "not_started");
    assert!(
        !format!("{record:?}").contains("local-private-source"),
        "package client response must not expose provenance"
    );
    assert!(
        !format!("{record:?}").contains(package_root),
        "package client response must not expose local package root"
    );

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::PluginLifecycleStatus {
                request_id: request_id("plugin-lifecycle"),
            },
        )
        .expect("plugin lifecycle status through client api");
    let HubClientResponseBody::PluginLifecycle(records) = response.body else {
        panic!("plugin lifecycle response expected");
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].package_name, "workflow.plugin");
    assert_eq!(records[0].state, HubClientPackageState::Enabled);
    assert!(!records[0].loaded);
}

#[test]
fn denied_client_request_returns_typed_admission_error() {
    let api = HubClientApi::new(
        HubClientIdentity {
            client_id: botster_core::ClientId("denied-client".to_string()),
            role: HubClientRole::Unadmitted,
        },
        HubClientAdmission::deny_all(),
    );
    let mut runtime = explicit_runtime("denied");
    let packages = empty_registry();

    let error = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Status {
                request_id: request_id("denied-status"),
            },
        )
        .expect_err("denied client should fail");

    assert_eq!(
        error,
        HubClientError::AdmissionDenied {
            request_id: request_id("denied-status"),
            operation: HubClientOperation::Status,
            role: HubClientRole::Unadmitted,
        }
    );

    let error = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Shutdown {
                request_id: request_id("denied-shutdown"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("denied client should not shut down sessions");

    assert_eq!(
        error,
        HubClientError::AdmissionDenied {
            request_id: request_id("denied-shutdown"),
            operation: HubClientOperation::Shutdown,
            role: HubClientRole::Unadmitted,
        }
    );
}
