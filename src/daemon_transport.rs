//! Same-device daemon socket transport for the thin operator CLI.
//!
//! This module is a framing adapter over `HubClientApi`. The daemon owns one
//! mutable `HubRuntime` on the accept/control thread; socket threads submit discrete
//! requests and never hold runtime access while writing to a client.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use botster_core::{
    ExtensionRuntime, RequestId, SessionId, SessionLifecycleState, SubscriptionId,
    TerminalAttachState,
};
use botster_core_daemon::RegistrySessionState;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    FileHubStateStore, HubClientApi, HubClientEvent, HubClientPackage,
    HubClientPackageClassification, HubClientPluginLifecycle, HubClientRequest,
    HubClientResponseBody, HubClientSession, HubConfig, HubDaemon, HubDaemonStatus,
    HubStateLoadSource, HubStateStore, McpToolDescriptor, PackageAction, PackageDecision,
};

const PROTOCOL: &str = "botster-hub-daemon-v1";
const ATTACH_DRAIN_INTERVAL: Duration = Duration::from_millis(25);

/// Run the local daemon socket until a shutdown request is received.
pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, control_rx) = mpsc::channel();
    let mut daemon = HubDaemon::start(config)?;
    let mut logical_clock = 1;
    let mut drain_cursors = BTreeMap::<String, u64>::new();

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let tx = control_tx.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, tx) {
                        eprintln!("botster-hub daemon connection error: {error}");
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                while let Ok(message) = control_rx.try_recv() {
                    if handle_control_message(
                        &mut daemon,
                        &mut logical_clock,
                        &mut drain_cursors,
                        message,
                    ) {
                        let status = daemon.stop();
                        cleanup_socket_path(&socket_path);
                        return Ok(status);
                    }
                }
                if !socket_path.exists() {
                    rebind_missing_socket_path(&socket_path);
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(DaemonTransportError::Io(error)),
        }
    }
}

/// Connect to a daemon and send one operator request.
pub fn request(
    config: &HubConfig,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let mut stream = connect_and_hello(config)?;
    write_frame(&mut stream, &request)?;
    read_frame(&mut stream)
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    config: &HubConfig,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let mut stream = connect_and_hello(config)?;
    let session_id_value = session_id.0.clone();
    let subscription_id_value = subscription_id.0.clone();
    let result = stream_attach_connected(
        &mut stream,
        &session_id_value,
        &subscription_id_value,
        output,
    );
    detach_stream_subscription(&mut stream, &session_id_value, &subscription_id_value);
    result
}

fn stream_attach_connected(
    stream: &mut UnixStream,
    session_id: &str,
    subscription_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    write_frame(
        stream,
        &DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    )?;
    let response: DaemonResponse = read_frame(stream)?;
    write_terminal_events(&response.events, output)?;
    if response.events.iter().any(DaemonEvent::is_process_exit) {
        return Ok(());
    }
    let mut idle_drains = 0;

    loop {
        thread::sleep(ATTACH_DRAIN_INTERVAL);
        write_frame(
            stream,
            &DaemonRequest::Drain {
                session_id: session_id.to_string(),
            },
        )?;
        let response: DaemonResponse = read_frame(stream)?;
        if response.events.is_empty() {
            idle_drains += 1;
        } else {
            idle_drains = 0;
        }
        write_terminal_events(&response.events, output)?;
        if response.events.iter().any(DaemonEvent::is_process_exit) {
            return Ok(());
        }
        if idle_drains >= 20 {
            write_frame(stream, &DaemonRequest::ListSessions)?;
            let response: DaemonResponse = read_frame(stream)?;
            if response
                .sessions
                .iter()
                .any(|session| session.session_id == session_id && session.lifecycle == "exited")
            {
                return Ok(());
            }
            return Ok(());
        }
    }
}

fn detach_stream_subscription(stream: &mut UnixStream, session_id: &str, subscription_id: &str) {
    if write_frame(
        stream,
        &DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    )
    .is_ok()
    {
        let _ = read_frame::<DaemonResponse>(stream);
    }
}

fn handle_connection(
    mut stream: UnixStream,
    control_tx: Sender<ControlMessage>,
) -> DaemonTransportResult<()> {
    stream
        .set_nonblocking(false)
        .map_err(DaemonTransportError::Io)?;
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    let hello: DaemonHello = read_frame_from_reader(&mut reader)?;
    if hello.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol("unexpected hello protocol"));
    }
    write_frame(
        &mut stream,
        &DaemonHelloAck {
            protocol: PROTOCOL.to_string(),
        },
    )?;
    let mut attached_subscriptions = Vec::<AttachedSubscription>::new();

    loop {
        let request = match read_frame_from_reader::<DaemonRequest>(&mut reader) {
            Ok(request) => request,
            Err(DaemonTransportError::ClientDisconnected) => {
                detach_connection_subscriptions(&control_tx, &attached_subscriptions);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        let close_after_response = matches!(request, DaemonRequest::DaemonShutdown);
        let active_change = AttachedSubscriptionChange::from_request(&request);
        control_tx
            .send(ControlMessage::Request { request, reply_tx })
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        let response = reply_rx
            .recv()
            .map_err(|_| DaemonTransportError::ControlThreadStopped)??;
        apply_attached_subscription_change(&mut attached_subscriptions, active_change);
        if let Err(error) = write_frame(&mut stream, &response) {
            detach_connection_subscriptions(&control_tx, &attached_subscriptions);
            return Err(error);
        }
        if close_after_response {
            detach_connection_subscriptions(&control_tx, &attached_subscriptions);
            return Ok(());
        }
    }
}

fn detach_connection_subscriptions(
    control_tx: &Sender<ControlMessage>,
    attached_subscriptions: &[AttachedSubscription],
) {
    for subscription in attached_subscriptions {
        let (reply_tx, reply_rx) = mpsc::channel();
        if control_tx
            .send(ControlMessage::Request {
                request: DaemonRequest::Detach {
                    session_id: subscription.session_id.clone(),
                    subscription_id: subscription.subscription_id.clone(),
                },
                reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
        }
    }
}

fn apply_attached_subscription_change(
    attached_subscriptions: &mut Vec<AttachedSubscription>,
    active_change: Option<AttachedSubscriptionChange>,
) {
    match active_change {
        Some(AttachedSubscriptionChange::Attach(subscription)) => {
            if !attached_subscriptions.contains(&subscription) {
                attached_subscriptions.push(subscription);
            }
        }
        Some(AttachedSubscriptionChange::Detach(subscription)) => {
            attached_subscriptions.retain(|attached| attached != &subscription);
        }
        None => {}
    }
}

fn handle_control_message(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    message: ControlMessage,
) -> bool {
    let ControlMessage::Request { request, reply_tx } = message;
    let response =
        handle_control_request(daemon, logical_clock, drain_cursors, request).or_else(|error| {
            match error {
                DaemonTransportError::Client(error) => Ok(DaemonResponse::operator_error(error)),
                DaemonTransportError::Package(error) => Ok(DaemonResponse::package_error(error)),
                DaemonTransportError::State(error) => Ok(DaemonResponse::state_error(error)),
                error => Err(error),
            }
        });
    let should_stop = matches!(
        response,
        Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            ..
        })
    );
    let _ = reply_tx.send(response);
    should_stop
}

fn handle_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListPackages => list_packages_response(daemon),
        DaemonRequest::PluginLifecycleStatus => plugin_lifecycle_response(daemon),
        DaemonRequest::EnablePackageLocalPath { path } => {
            let package_name = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "daemon socket enable local package")?;
                record.manifest.name.clone()
            };
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable local package")?;
            let registry = daemon.package_registry().clone();
            let prepared = registry
                .prepare_local_package(&package_name, "daemon socket inspect local package")?;
            if prepared.selected_entrypoint.runtime == ExtensionRuntime::Lua {
                daemon
                    .runtime_mut()
                    .ok_or(DaemonTransportError::DaemonNotRunning)?
                    .load_lua_plugin_package(&registry, &package_name)
                    .map_err(crate::HubDaemonError::from)?;
            }
            persist_package_registry(daemon)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::EnablePackage { package_name } => {
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable package")?;
            persist_package_registry(daemon)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::DisablePackage { package_name } => {
            let decision = daemon
                .package_registry_mut()
                .disable(&package_name, "daemon socket disable package")?;
            persist_package_registry(daemon)?;
            package_decision_response(daemon, decision)
        }
        other => handle_runtime_control_request(daemon, logical_clock, drain_cursors, other),
    }
}

fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::Status => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Status {
                    request_id: request_id("daemon-status"),
                },
            )?;
            let HubClientResponseBody::Status(client_status) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(DaemonResponse::status(status, client_status.session_count))
        }
        DaemonRequest::ListSessions => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessions {
                    request_id: request_id("daemon-sessions-list"),
                },
            )?;
            let HubClientResponseBody::Sessions(sessions) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(DaemonResponse::sessions(sessions))
        }
        DaemonRequest::Spawn {
            session_id,
            command,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Spawn {
                    request_id: request_id("daemon-sessions-spawn"),
                    session_id: SessionId(session_id),
                    command,
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::Spawned(spawned) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            drain_cursors.insert(spawned.session.session_id.0.clone(), *logical_clock);
            Ok(DaemonResponse::spawned(
                spawned.session.into(),
                events_from_client(spawned.events),
            ))
        }
        DaemonRequest::Attach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Attach {
                    request_id: request_id("daemon-sessions-attach"),
                    session_id: SessionId(session_id),
                    subscription_id: SubscriptionId(subscription_id),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Detach {
                    request_id: request_id("daemon-sessions-detach"),
                    session_id: SessionId(session_id),
                    subscription_id: SubscriptionId(subscription_id),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::SendInput { session_id, data } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Input {
                    request_id: request_id("daemon-sessions-send-input"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::Resize {
            session_id,
            rows,
            cols,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Resize {
                    request_id: request_id("daemon-sessions-resize"),
                    session_id: SessionId(session_id),
                    rows,
                    cols,
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::ShutdownSession { session_id } => {
            let now = tick(logical_clock);
            match classify_shutdown_session(runtime, &session_id)? {
                ShutdownSessionClassification::Active => {}
                ShutdownSessionClassification::Cleanup(cleanup) => {
                    return Ok(DaemonResponse::session_cleanup(cleanup));
                }
                ShutdownSessionClassification::Missing => {
                    return Ok(DaemonResponse::unknown_session_cleanup(&session_id));
                }
            }
            let shutdown_session_id = session_id.clone();
            let response = match api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Shutdown {
                    request_id: request_id("daemon-sessions-shutdown"),
                    session_id: SessionId(shutdown_session_id),
                    now_seconds: now,
                },
            ) {
                Ok(response) => response,
                Err(error) => {
                    if shutdown_error_is_unknown_session(&error) {
                        return Ok(DaemonResponse::session_cleanup(DaemonSessionCleanup {
                            session_id: session_id.clone(),
                            outcome: "already_exited".to_string(),
                        }));
                    }
                    return match classify_shutdown_session(runtime, &session_id)? {
                        ShutdownSessionClassification::Cleanup(cleanup) => {
                            Ok(DaemonResponse::session_cleanup(cleanup))
                        }
                        ShutdownSessionClassification::Missing => {
                            Ok(DaemonResponse::unknown_session_cleanup(&session_id))
                        }
                        ShutdownSessionClassification::Active => {
                            Err(DaemonTransportError::Client(error))
                        }
                    };
                }
            };
            events_response(response.body)
        }
        DaemonRequest::Drain { session_id } => {
            let cursor = drain_cursors
                .entry(session_id.clone())
                .or_insert_with(|| tick(logical_clock));
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("daemon-sessions-drain"),
                    session_id: SessionId(session_id),
                    last_output_at: *cursor,
                },
            )?;
            let response = events_response(response.body)?;
            if !response.events.is_empty() {
                *cursor = tick(logical_clock);
            }
            Ok(response)
        }
        DaemonRequest::PluginMcpListTools => Ok(DaemonResponse::plugin_tools(
            runtime.list_plugin_mcp_tools(),
        )),
        DaemonRequest::PluginMcpCallTool { name, arguments } => {
            match runtime.call_plugin_mcp_tool(crate::McpCallRequest { name, arguments }) {
                Ok(result) => Ok(DaemonResponse::plugin_tool_result(result)),
                Err(error) => Ok(DaemonResponse::plugin_tool_error(error)),
            }
        }
        DaemonRequest::DaemonShutdown => Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            status: Some(DaemonStatus::from_status(
                &status,
                runtime
                    .list_sessions()
                    .map_err(crate::HubRuntimeError::from)?
                    .len(),
            )),
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }),
        DaemonRequest::ListPackages
        | DaemonRequest::PluginLifecycleStatus
        | DaemonRequest::EnablePackageLocalPath { .. }
        | DaemonRequest::EnablePackage { .. }
        | DaemonRequest::DisablePackage { .. } => {
            unreachable!("package requests are handled before runtime borrow")
        }
    }
}

fn list_packages_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackages {
            request_id: request_id("daemon-packages-list"),
        },
    )?;
    let HubClientResponseBody::Packages(packages) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(DaemonResponse::packages(packages))
}

fn plugin_lifecycle_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::PluginLifecycleStatus {
            request_id: request_id("daemon-plugin-lifecycle-status"),
        },
    )?;
    let HubClientResponseBody::PluginLifecycle(lifecycle) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(DaemonResponse::plugin_lifecycle(lifecycle))
}

fn package_decision_response(
    daemon: &mut HubDaemon,
    decision: PackageDecision,
) -> DaemonTransportResult<DaemonResponse> {
    let mut response = list_packages_response(daemon)?;
    response.kind = DaemonResponseKind::PackageDecision;
    response.package_decision = Some(DaemonPackageDecision::from(decision));
    Ok(response)
}

fn persist_package_registry(daemon: &HubDaemon) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = daemon.package_registry().snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    Ok(())
}

fn events_response(body: HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> {
    let HubClientResponseBody::Events(events) = body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(DaemonResponse::events(events_from_client(events)))
}

enum ShutdownSessionClassification {
    Active,
    Cleanup(DaemonSessionCleanup),
    Missing,
}

fn classify_shutdown_session(
    runtime: &mut crate::HubRuntime,
    session_id: &str,
) -> Result<ShutdownSessionClassification, crate::HubRuntimeError> {
    let Some(session) = runtime
        .list_sessions()
        .map_err(crate::HubRuntimeError::from)?
        .into_iter()
        .find(|session| session.session_id.0 == session_id)
    else {
        return Ok(ShutdownSessionClassification::Missing);
    };

    match session.registry_state {
        RegistrySessionState::Running => Ok(ShutdownSessionClassification::Active),
        RegistrySessionState::Stopping | RegistrySessionState::Exited => Ok(
            ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            }),
        ),
        RegistrySessionState::Stale => Ok(ShutdownSessionClassification::Cleanup(
            DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "stale_session".to_string(),
            },
        )),
    }
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn tick(logical_clock: &mut u64) -> u64 {
    let current = *logical_clock;
    *logical_clock += 1;
    current
}

fn socket_path(config: &HubConfig) -> DaemonTransportResult<PathBuf> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(DaemonTransportError::MissingSocketBinding)
}

fn prepare_socket_path(path: &PathBuf) -> DaemonTransportResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    }
    match UnixStream::connect(path) {
        Ok(mut stream) => {
            write_frame(
                &mut stream,
                &DaemonHello {
                    protocol: PROTOCOL.to_string(),
                },
            )?;
            let ack = read_frame::<DaemonHelloAck>(&mut stream);
            if ack.is_ok() {
                return Err(DaemonTransportError::AlreadyRunning);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
    if path.exists() {
        fs::remove_file(path).map_err(DaemonTransportError::Io)?;
    }
    Ok(())
}

fn rebind_missing_socket_path(_path: &PathBuf) {
    // The current std-only listener cannot recreate the public pathname without
    // replacing the accept loop. Keep the daemon alive; clients report
    // not-running until a future listener-rebind pass repairs the path.
}

fn cleanup_socket_path(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

fn connect_and_hello(config: &HubConfig) -> DaemonTransportResult<UnixStream> {
    let path = socket_path(config)?;
    let mut stream = UnixStream::connect(path).map_err(|error| {
        if matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ) {
            DaemonTransportError::NotRunning
        } else {
            DaemonTransportError::Io(error)
        }
    })?;
    write_frame(
        &mut stream,
        &DaemonHello {
            protocol: PROTOCOL.to_string(),
        },
    )?;
    let ack: DaemonHelloAck = read_frame(&mut stream)?;
    if ack.protocol == PROTOCOL {
        Ok(stream)
    } else {
        Err(DaemonTransportError::NotRunning)
    }
}

fn write_frame<T: Serialize>(stream: &mut UnixStream, frame: &T) -> DaemonTransportResult<()> {
    let bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    stream.write_all(&bytes).map_err(DaemonTransportError::Io)?;
    stream.write_all(b"\n").map_err(DaemonTransportError::Io)
}

fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut UnixStream) -> DaemonTransportResult<T> {
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    read_frame_from_reader(&mut reader)
}

fn read_frame_from_reader<T: for<'de> Deserialize<'de>>(
    reader: &mut BufReader<UnixStream>,
) -> DaemonTransportResult<T> {
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .map_err(DaemonTransportError::Io)?;
    if bytes == 0 {
        return Err(DaemonTransportError::ClientDisconnected);
    }
    serde_json::from_str(&line).map_err(DaemonTransportError::Json)
}

fn write_terminal_events(
    events: &[DaemonEvent],
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    for event in events {
        if let DaemonEvent::TerminalOutput { data, .. } = event {
            output
                .write_all(data.as_bytes())
                .map_err(DaemonTransportError::Io)?;
            output.flush().map_err(DaemonTransportError::Io)?;
        }
    }
    Ok(())
}

fn events_from_client(events: Vec<HubClientEvent>) -> Vec<DaemonEvent> {
    events.into_iter().map(DaemonEvent::from).collect()
}

#[derive(Debug)]
enum ControlMessage {
    Request {
        request: DaemonRequest,
        reply_tx: Sender<DaemonTransportResult<DaemonResponse>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachedSubscription {
    session_id: String,
    subscription_id: String,
}

enum AttachedSubscriptionChange {
    Attach(AttachedSubscription),
    Detach(AttachedSubscription),
}

impl AttachedSubscriptionChange {
    fn from_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => Some(Self::Attach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => Some(Self::Detach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonHello {
    protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonHelloAck {
    protocol: String,
}

/// Client request variants for the local daemon protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Status,
    ListSessions,
    Spawn {
        session_id: String,
        command: String,
    },
    Attach {
        session_id: String,
        subscription_id: String,
    },
    Detach {
        session_id: String,
        subscription_id: String,
    },
    SendInput {
        session_id: String,
        data: String,
    },
    Resize {
        session_id: String,
        rows: u16,
        cols: u16,
    },
    ShutdownSession {
        session_id: String,
    },
    Drain {
        session_id: String,
    },
    ListPackages,
    EnablePackageLocalPath {
        path: PathBuf,
    },
    EnablePackage {
        package_name: String,
    },
    DisablePackage {
        package_name: String,
    },
    PluginLifecycleStatus,
    PluginMcpListTools,
    PluginMcpCallTool {
        name: String,
        arguments: Value,
    },
    DaemonShutdown,
}

/// Server response variants for one local daemon request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub kind: DaemonResponseKind,
    pub status: Option<DaemonStatus>,
    pub sessions: Vec<DaemonSession>,
    pub packages: Vec<DaemonPackage>,
    pub package_decision: Option<DaemonPackageDecision>,
    pub lifecycle: Vec<DaemonPluginLifecycle>,
    #[serde(default)]
    pub plugin_tools: Vec<McpToolDescriptor>,
    #[serde(default)]
    pub plugin_tool_result: Value,
    pub events: Vec<DaemonEvent>,
    pub cleanup: Option<DaemonSessionCleanup>,
    pub error: Option<DaemonOperatorError>,
}

impl DaemonResponse {
    fn status(status: HubDaemonStatus, session_count: usize) -> Self {
        Self {
            kind: DaemonResponseKind::Status,
            status: Some(DaemonStatus::from_status(&status, session_count)),
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn sessions(sessions: Vec<HubClientSession>) -> Self {
        Self {
            kind: DaemonResponseKind::Sessions,
            status: None,
            sessions: sessions.into_iter().map(Into::into).collect(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn spawned(session: DaemonSession, events: Vec<DaemonEvent>) -> Self {
        Self {
            kind: DaemonResponseKind::Spawned,
            status: None,
            sessions: vec![session],
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events,
            cleanup: None,
            error: None,
        }
    }

    fn events(events: Vec<DaemonEvent>) -> Self {
        Self {
            kind: DaemonResponseKind::Events,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events,
            cleanup: None,
            error: None,
        }
    }

    fn packages(packages: Vec<HubClientPackage>) -> Self {
        Self {
            kind: DaemonResponseKind::Packages,
            status: None,
            sessions: Vec::new(),
            packages: packages.into_iter().map(Into::into).collect(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn plugin_lifecycle(lifecycle: Vec<HubClientPluginLifecycle>) -> Self {
        Self {
            kind: DaemonResponseKind::PluginLifecycle,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: lifecycle.into_iter().map(Into::into).collect(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn session_cleanup(cleanup: DaemonSessionCleanup) -> Self {
        Self {
            kind: DaemonResponseKind::SessionCleanup,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: Some(cleanup),
            error: None,
        }
    }

    fn unknown_session_cleanup(session_id: &str) -> Self {
        Self {
            kind: DaemonResponseKind::OperatorError,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: Some(DaemonOperatorError {
                code: "unknown_session".to_string(),
                request_id: "daemon-sessions-shutdown".to_string(),
                operation: "shutdown".to_string(),
                message: format!("unknown session: {session_id}"),
            }),
        }
    }

    fn operator_error(error: crate::HubClientError) -> Self {
        Self {
            kind: DaemonResponseKind::OperatorError,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: Some(DaemonOperatorError::from_client_error(error)),
        }
    }

    fn package_error(error: crate::PackageRegistryError) -> Self {
        Self {
            kind: DaemonResponseKind::OperatorError,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: Some(DaemonOperatorError::from_package_error(error)),
        }
    }

    fn state_error(error: crate::HubStateStoreError) -> Self {
        Self {
            kind: DaemonResponseKind::OperatorError,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: Some(DaemonOperatorError::from_state_error(error)),
        }
    }

    fn plugin_tools(plugin_tools: Vec<McpToolDescriptor>) -> Self {
        Self {
            kind: DaemonResponseKind::PluginMcpTools,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools,
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn plugin_tool_result(plugin_tool_result: Value) -> Self {
        Self {
            kind: DaemonResponseKind::PluginMcpToolResult,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result,
            events: Vec::new(),
            cleanup: None,
            error: None,
        }
    }

    fn plugin_tool_error(error: crate::McpToolError) -> Self {
        Self {
            kind: DaemonResponseKind::OperatorError,
            status: None,
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            events: Vec::new(),
            cleanup: None,
            error: Some(DaemonOperatorError {
                code: error.code,
                request_id: "daemon-plugin-mcp-call".to_string(),
                operation: "plugin_mcp_call".to_string(),
                message: error.message,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonResponseKind {
    Status,
    Sessions,
    Spawned,
    Events,
    Packages,
    PackageDecision,
    PluginLifecycle,
    PluginMcpTools,
    PluginMcpToolResult,
    SessionCleanup,
    OperatorError,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackage {
    pub package_name: String,
    pub version: String,
    pub classification: String,
    pub state: String,
    pub requested_capabilities: Vec<DaemonCapability>,
    pub provider_profile_admitted: bool,
}

impl From<HubClientPackage> for DaemonPackage {
    fn from(package: HubClientPackage) -> Self {
        Self {
            package_name: package.package_name,
            version: package.version,
            classification: package_classification_label(package.classification).to_string(),
            state: package_state_label(package.state).to_string(),
            requested_capabilities: package
                .requested_capabilities
                .into_iter()
                .map(|capability| DaemonCapability {
                    surface: capability.surface,
                    scope: capability.scope,
                })
                .collect(),
            provider_profile_admitted: package.provider_profile_admitted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCapability {
    pub surface: String,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageDecision {
    pub package_name: String,
    pub action: String,
    pub state: String,
    pub classification: String,
}

impl From<PackageDecision> for DaemonPackageDecision {
    fn from(decision: PackageDecision) -> Self {
        Self {
            package_name: decision.package_name,
            action: package_action_label(decision.action).to_string(),
            state: package_state_label(decision.state.into()).to_string(),
            classification: package_classification_label(decision.classification.into())
                .to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPluginLifecycle {
    pub package_name: String,
    pub state: String,
    pub loaded: bool,
}

impl From<HubClientPluginLifecycle> for DaemonPluginLifecycle {
    fn from(lifecycle: HubClientPluginLifecycle) -> Self {
        Self {
            package_name: lifecycle.package_name,
            state: package_state_label(lifecycle.state).to_string(),
            loaded: lifecycle.loaded,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub lifecycle_state: String,
    pub host_id: String,
    pub host_display_name: String,
    pub schema_version: u16,
    pub data_dir_configured: bool,
    pub core_initialized: bool,
    pub state_source: String,
    pub package_count: usize,
    pub enabled_package_count: usize,
    pub provider_count: usize,
    pub enabled_provider_count: usize,
    pub session_count: usize,
    pub recovered_sessions: Vec<String>,
    pub stale_sessions: Vec<String>,
}

impl DaemonStatus {
    fn from_status(status: &HubDaemonStatus, session_count: usize) -> Self {
        Self {
            lifecycle_state: match status.lifecycle_state {
                crate::HubDaemonState::Created => "created",
                crate::HubDaemonState::Running => "running",
                crate::HubDaemonState::Stopped => "stopped",
            }
            .to_string(),
            host_id: status.host_id.clone(),
            host_display_name: status.host_display_name.clone(),
            schema_version: status.schema_version,
            data_dir_configured: status.data_dir_configured,
            core_initialized: status.core_initialized,
            state_source: match status.state_source {
                HubStateLoadSource::Loaded => "loaded",
                HubStateLoadSource::Initialized => "initialized",
            }
            .to_string(),
            package_count: status.package_count,
            enabled_package_count: status.enabled_package_count,
            provider_count: status.provider_count,
            enabled_provider_count: status.enabled_provider_count,
            session_count,
            recovered_sessions: status
                .recovered_sessions
                .iter()
                .map(|session_id| session_id.0.clone())
                .collect(),
            stale_sessions: status
                .stale_sessions
                .iter()
                .map(|session_id| session_id.0.clone())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSession {
    pub session_id: String,
    pub lifecycle: String,
}

impl From<HubClientSession> for DaemonSession {
    fn from(session: HubClientSession) -> Self {
        Self {
            session_id: session.session_id.0,
            lifecycle: lifecycle_label(&session.lifecycle).to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionCleanup {
    pub session_id: String,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonOperatorError {
    pub code: String,
    pub request_id: String,
    pub operation: String,
    pub message: String,
}

impl DaemonOperatorError {
    fn from_client_error(error: crate::HubClientError) -> Self {
        match error {
            crate::HubClientError::AdmissionDenied {
                request_id,
                operation,
                role,
            } => Self {
                code: "admission_denied".to_string(),
                request_id: request_id.0,
                operation: operation_label(operation).to_string(),
                message: format!("{role:?} is not allowed to run {operation:?}"),
            },
            crate::HubClientError::Runtime {
                request_id,
                operation,
                kind,
            } => Self {
                code: runtime_error_code(kind).to_string(),
                request_id: request_id.0,
                operation: operation_label(operation).to_string(),
                message: format!("runtime failed while handling {operation:?}: {kind:?}"),
            },
            crate::HubClientError::UnsupportedDaemonOperation {
                request_id,
                operation,
                daemon_operation,
            } => Self {
                code: "unsupported_daemon_operation".to_string(),
                request_id: request_id.0,
                operation: operation_label(operation).to_string(),
                message: format!("{daemon_operation} is not supported by the daemon"),
            },
            crate::HubClientError::PackageCapabilityDenied {
                request_id,
                operation,
                package_name,
            } => Self {
                code: "package_capability_denied".to_string(),
                request_id: request_id.0,
                operation: operation_label(operation).to_string(),
                message: format!("{package_name} is not allowed to run {operation:?}"),
            },
        }
    }

    fn from_package_error(error: crate::PackageRegistryError) -> Self {
        Self {
            code: "package_policy_error".to_string(),
            request_id: "daemon-package-mutation".to_string(),
            operation: package_action_label(error.action).to_string(),
            message: format!(
                "package {} denied for {}: {:?}",
                error.package_name,
                package_action_label(error.action),
                error.reason
            ),
        }
    }

    fn from_state_error(error: crate::HubStateStoreError) -> Self {
        Self {
            code: "hub_state_error".to_string(),
            request_id: "daemon-package-mutation".to_string(),
            operation: "persist_package_registry".to_string(),
            message: format!("failed to persist package registry: {error}"),
        }
    }
}

fn shutdown_error_is_unknown_session(error: &crate::HubClientError) -> bool {
    matches!(
        error,
        crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Shutdown,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        }
    )
}

fn runtime_error_code(kind: crate::HubClientRuntimeErrorKind) -> &'static str {
    match kind {
        crate::HubClientRuntimeErrorKind::UnknownSession => "unknown_session",
        crate::HubClientRuntimeErrorKind::Runtime => "runtime_error",
        crate::HubClientRuntimeErrorKind::State => "state_error",
    }
}

fn operation_label(operation: crate::HubClientOperation) -> &'static str {
    match operation {
        crate::HubClientOperation::Status => "status",
        crate::HubClientOperation::ListSessions => "list_sessions",
        crate::HubClientOperation::Spawn => "spawn",
        crate::HubClientOperation::Attach => "attach",
        crate::HubClientOperation::Detach => "detach",
        crate::HubClientOperation::Input => "input",
        crate::HubClientOperation::Resize => "resize",
        crate::HubClientOperation::DrainRuntime => "drain_runtime",
        crate::HubClientOperation::Shutdown => "shutdown",
        crate::HubClientOperation::GuardedNotificationWrite => "guarded_notification_write",
        crate::HubClientOperation::ReadScreen => "read_screen",
        crate::HubClientOperation::CaptureSnapshot => "capture_snapshot",
        crate::HubClientOperation::ListPackages => "list_packages",
        crate::HubClientOperation::PluginLifecycleStatus => "plugin_lifecycle_status",
    }
}

fn package_classification_label(classification: HubClientPackageClassification) -> &'static str {
    match classification {
        HubClientPackageClassification::Plugin => "plugin",
        HubClientPackageClassification::Provider => "provider",
    }
}

fn package_state_label(state: crate::HubClientPackageState) -> &'static str {
    match state {
        crate::HubClientPackageState::Installed => "installed",
        crate::HubClientPackageState::Enabled => "enabled",
        crate::HubClientPackageState::Disabled => "disabled",
    }
}

fn package_action_label(action: PackageAction) -> &'static str {
    match action {
        PackageAction::Install => "install",
        PackageAction::Enable => "enable",
        PackageAction::Disable => "disable",
        PackageAction::Pin => "pin",
        PackageAction::Prepare => "prepare",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEvent {
    SessionLifecycle {
        session_id: String,
        state: String,
    },
    TerminalOutput {
        session_id: String,
        subscription_id: String,
        data: String,
    },
    Snapshot {
        session_id: String,
        subscription_id: String,
        bytes: usize,
    },
    Scrollback {
        session_id: String,
        subscription_id: String,
        bytes: usize,
    },
    ProcessExit {
        session_id: String,
        subscription_id: String,
        code: Option<i32>,
    },
    AttachState {
        session_id: String,
        subscription_id: String,
        state: String,
    },
    RuntimeObservation {
        kind: String,
    },
}

impl DaemonEvent {
    fn is_process_exit(&self) -> bool {
        matches!(self, Self::ProcessExit { .. })
    }
}

impl From<HubClientEvent> for DaemonEvent {
    fn from(event: HubClientEvent) -> Self {
        match event {
            HubClientEvent::SessionLifecycle { session_id, state } => Self::SessionLifecycle {
                session_id: session_id.0,
                state: lifecycle_label(&state).to_string(),
            },
            HubClientEvent::TerminalOutput {
                session_id,
                subscription_id,
                data,
            } => Self::TerminalOutput {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                data: String::from_utf8_lossy(&data).to_string(),
            },
            HubClientEvent::Snapshot {
                session_id,
                subscription_id,
                bytes,
            } => Self::Snapshot {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                bytes,
            },
            HubClientEvent::Scrollback {
                session_id,
                subscription_id,
                bytes,
            } => Self::Scrollback {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                bytes,
            },
            HubClientEvent::ProcessExit {
                session_id,
                subscription_id,
                code,
            } => Self::ProcessExit {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                code,
            },
            HubClientEvent::AttachState {
                session_id,
                subscription_id,
                state,
            } => Self::AttachState {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                state: attach_state_label(&state).to_string(),
            },
            HubClientEvent::RuntimeObservation { kind } => Self::RuntimeObservation {
                kind: match kind {
                    crate::HubClientObservationKind::SessionActivity => "session_activity",
                    crate::HubClientObservationKind::Subscription => "subscription",
                    crate::HubClientObservationKind::Backpressure => "backpressure",
                }
                .to_string(),
            },
        }
    }
}

fn lifecycle_label(state: &SessionLifecycleState) -> &'static str {
    match state {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
    }
}

fn attach_state_label(state: &TerminalAttachState) -> &'static str {
    match state {
        TerminalAttachState::Attaching => "attaching",
        TerminalAttachState::Attached => "attached",
        TerminalAttachState::Detached => "detached",
    }
}

/// Daemon socket transport error.
#[derive(Debug)]
pub enum DaemonTransportError {
    MissingSocketBinding,
    NotRunning,
    AlreadyRunning,
    ClientDisconnected,
    Protocol(&'static str),
    UnexpectedResponse,
    DaemonNotRunning,
    ControlThreadStopped,
    Io(std::io::Error),
    Json(serde_json::Error),
    Daemon(crate::HubDaemonError),
    Client(crate::HubClientError),
    Package(crate::PackageRegistryError),
    State(crate::HubStateStoreError),
    Runtime(crate::HubRuntimeError),
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketBinding => write!(formatter, "local socket transport is disabled"),
            Self::NotRunning => write!(formatter, "daemon not running"),
            Self::AlreadyRunning => write!(formatter, "daemon already running"),
            Self::ClientDisconnected => write!(formatter, "client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::DaemonNotRunning => write!(formatter, "daemon runtime is not running"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "{error:?}"),
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Daemon(error) => Some(error),
            Self::State(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::HubDaemonError> for DaemonTransportError {
    fn from(error: crate::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<crate::HubClientError> for DaemonTransportError {
    fn from(error: crate::HubClientError) -> Self {
        Self::Client(error)
    }
}

impl From<crate::PackageRegistryError> for DaemonTransportError {
    fn from(error: crate::PackageRegistryError) -> Self {
        Self::Package(error)
    }
}

impl From<crate::HubStateStoreError> for DaemonTransportError {
    fn from(error: crate::HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<crate::HubRuntimeError> for DaemonTransportError {
    fn from(error: crate::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

/// Result alias for daemon socket transport operations.
pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;
