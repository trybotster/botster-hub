//! Same-device daemon socket transport for the thin operator CLI.
//!
//! This module is a framing adapter over `HubClientApi`. The daemon owns one
//! mutable `HubRuntime` on the accept/control thread; socket threads submit discrete
//! requests and never hold runtime access while writing to a client.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    EndpointId, EnvelopeCursor, EnvelopeDeliveryState, EnvelopeId, EnvelopeTarget,
    PackageConfigurationValue, PackageSource, RequestId, RoutedEnvelope, RoutedEnvelopePayload,
    RunnableEntrypointKind, RunnableEntrypointLaunchMode, RunnableEntrypointProcessState,
    RunnableEntrypointResultField, SessionId, SessionLifecycleState, SubscriptionId,
    TerminalAttachState,
};
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence, RegistrySessionState,
    SessionLifecycleBaseline, SessionLifecycleChangeKind, SessionLifecycleCursor,
    SessionLifecycleRecord,
};
use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
pub use botster_hub_client::{
    DaemonApp, DaemonAppLaunchTarget, DaemonAvailablePackage, DaemonCapability,
    DaemonCaptureSnapshot, DaemonCompatibility, DaemonConnection as ClientDaemonConnection,
    DaemonCoordination, DaemonDiagnostic, DaemonEndpoint, DaemonEntityFrame, DaemonEnvelope,
    DaemonEnvelopeAck, DaemonEnvelopeDelivery, DaemonEnvelopePublish, DaemonEvent, DaemonHello,
    DaemonHelloAck, DaemonHubUpdate, DaemonHubUpdateState, DaemonIdentity,
    DaemonInstallationDiagnostic, DaemonInstallationIdentity, DaemonInstallationMode,
    DaemonLifecycleCounters, DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap, DaemonModeFlags,
    DaemonNotify, DaemonOperatorError, DaemonPackage, DaemonPackageActionRequest,
    DaemonPackageActionRequiredReference, DaemonPackageActionState, DaemonPackageActionStatus,
    DaemonPackageAvailability, DaemonPackageAvailabilityReason, DaemonPackageAvailabilityState,
    DaemonPackageCompatibility, DaemonPackageConfiguration, DaemonPackageDecision,
    DaemonPackageDependencyAvailability, DaemonPackageDiagnostic,
    DaemonPackageEnvironmentRequirement, DaemonPackageFeatureAvailability,
    DaemonPackageInstallEffect, DaemonPackageInstallPlan, DaemonPackageNavigationEntry,
    DaemonPackageNavigationSource, DaemonPackagePin, DaemonPackageProcess,
    DaemonPackageRouteDescriptor, DaemonPackageRouteTarget, DaemonPackageRunnableEntrypoint,
    DaemonPackageUpdateStatus, DaemonPackageWorkingDirectory, DaemonPluginLifecycle,
    DaemonPluginResourceCounters, DaemonPluginSurface, DaemonPluginWorkerCounters,
    DaemonReadScreen, DaemonRequest, DaemonResolvedAppLaunch, DaemonResolvedSessionType,
    DaemonResponse, DaemonResponseKind, DaemonSession, DaemonSessionCleanup, DaemonSessionContext,
    DaemonSessionEntity, DaemonSessionType, DaemonSessionTypeContextInput,
    DaemonSessionTypeDefinition, DaemonSessionTypeEditableDefinition,
    DaemonSessionTypeMutationSource, DaemonSessionTypeRequest, DaemonSessionTypeWorkingDirectory,
    DaemonSoftwareIdentity, DaemonSpawnTarget, DaemonSpawnTargetValidation, DaemonStatus,
    DaemonUiTreeSnapshot, DaemonWorktree, DaemonWorktreeGitMetadata, DaemonWorktreeLifecycleEvent,
    FEATURE_PLUGIN_SURFACE_ACTION, FEATURE_PLUGIN_SURFACE_RENDER, PROTOCOL, read_frame,
    read_frame_from_reader, write_frame,
};
use botster_ui_contract::{
    PackageSurfaceDescriptor, PackageSurfaceKind, UiActionResult, UiActionResultState,
};
use serde_json::Value;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc as tokio_mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::local_webrtc::{
    LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE, LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES,
    LocalWebrtcAttachedSubscription, LocalWebrtcSenderTerminalRecord, LocalWebrtcSignalRequest,
};
use crate::maintenance::{
    HubUpdateCheckPlan, execute_managed_update_check, installation_identity, plan_hub_update_check,
    software_identity,
};
use crate::packages::{PackageResolvedEntrypointLaunch, resolve_entrypoint_launch_contract};
use crate::{
    AvailablePackage, AvailablePackageState, FileHubStateStore, HubClientApi,
    HubClientCaptureSnapshot, HubClientEvent, HubClientModeFlags, HubClientPackage,
    HubClientPackageAvailabilityReason, HubClientPackageAvailabilityState,
    HubClientPackageClassification, HubClientPackageNavigationEntry,
    HubClientPackageNavigationTarget, HubClientPluginLifecycle, HubClientPluginLifecycleReport,
    HubClientPluginSurface, HubClientPluginWorkerCounters, HubClientReadScreen, HubClientRequest,
    HubClientResponseBody, HubClientSession, HubConfig, HubDaemon, HubDaemonStatus,
    HubStateLoadSource, HubStateStore, McpToolDescriptor, PackageAction, PackageAdmissionReason,
    PackageCompatibilityResult, PackageDecision, PackageInstallPlan, PackagePin, PackageRegistry,
    PackageRegistryEntrySourceKind, PackageRegistryError, PackageSessionType,
    PackageSessionTypeWorkingDirectory, PackageState, PackageUpdatePolicy, ResolvedSessionType,
    SessionTypeContextInput, SessionTypeMutationSource, SessionTypeRequest,
    resolve_foreground_launch_contract,
};
use crate::{EntrypointProcessSnapshot, EntrypointSupervisorError};
use crate::{
    SpawnTarget, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate, SpawnTargetValidation,
};
use crate::{Worktree, WorktreeCreate, WorktreeError};

const MESSAGE_CONTENT_TYPE: &str = "application/vnd.botster.coordination.message+text";
const WEBRTC_SIGNAL_OPERATION: &str = "local_webrtc_signal";
const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const DAEMON_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const DAEMON_INCOMPLETE_FRAME_TIMEOUT: Duration = Duration::from_secs(2);
const DAEMON_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DAEMON_MAX_CONNECTIONS: usize = 64;
const DAEMON_MAX_REJECTION_TASKS: usize = 8;
const DAEMON_CONTROL_QUEUE_CAPACITY: usize = 256;
pub(crate) const ENTITY_SUBSCRIPTION_QUEUE_CAPACITY: usize = 64;
const ENTITY_RECONCILIATION_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) type ControlSender = tokio_mpsc::Sender<ControlMessage>;
type ControlReplySender = oneshot::Sender<DaemonTransportResult<DaemonResponse>>;

enum OwnerEvent {
    Control(Box<Option<ControlMessage>>),
    Reconcile,
}

async fn receive_owner_event(
    control_rx: &mut tokio_mpsc::Receiver<ControlMessage>,
    reconciliation_wait: Duration,
) -> OwnerEvent {
    if reconciliation_wait.is_zero() {
        return OwnerEvent::Reconcile;
    }
    tokio::select! {
        biased;
        _ = tokio::time::sleep(reconciliation_wait) => OwnerEvent::Reconcile,
        message = control_rx.recv() => OwnerEvent::Control(Box::new(message)),
    }
}

/// Run the local daemon socket until a shutdown request is received.
pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    let local_webrtc_terminal_record_path = config
        .data_directory
        .join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
    let (cleanup_tx, cleanup_rx) = mpsc::sync_channel(DAEMON_MAX_CONNECTIONS);
    let (shutdown_tx, _) = watch::channel(false);
    install_signal_forwarder(control_tx.clone())?;
    let mut daemon = HubDaemon::start(config)?;
    let mut control_state = DaemonControlState::default();
    seed_lifecycle_reconciliation(&mut daemon, &mut control_state);
    let transport_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("botster-hub-transport")
        .build()
        .map_err(DaemonTransportError::Io)?;
    let listener = {
        let _runtime = transport_runtime.enter();
        TokioUnixListener::from_std(listener).map_err(DaemonTransportError::Io)?
    };
    let mut connection_tasks = vec![transport_runtime.spawn(accept_connections(
        listener,
        control_tx.clone(),
        shutdown_tx.subscribe(),
        Arc::new(Semaphore::new(DAEMON_MAX_CONNECTIONS)),
    ))];
    loop {
        reap_finished_connection_tasks(&mut connection_tasks);
        while let Ok(cleanup) = cleanup_rx.try_recv() {
            handle_connection_cleanup(&mut daemon, &mut control_state, control_tx.clone(), cleanup);
        }
        let wait = control_state
            .next_reconciliation
            .saturating_duration_since(Instant::now());
        match transport_runtime.block_on(receive_owner_event(&mut control_rx, wait)) {
            OwnerEvent::Control(message) => match *message {
                Some(ControlMessage::AcceptedConnection {
                    stream,
                    admission_permit,
                }) => {
                    control_state.lifecycle_counters.accepted_connections = control_state
                        .lifecycle_counters
                        .accepted_connections
                        .saturating_add(1);
                    let tx = control_tx.clone();
                    let cleanup = cleanup_tx.clone();
                    let shutdown = shutdown_tx.subscribe();
                    control_state.lifecycle_counters.live_connections = control_state
                        .lifecycle_counters
                        .live_connections
                        .saturating_add(1);
                    control_state.lifecycle_counters.high_water_live_connections = control_state
                        .lifecycle_counters
                        .high_water_live_connections
                        .max(control_state.lifecycle_counters.live_connections);
                    connection_tasks.push(transport_runtime.spawn(async move {
                        let _admission_permit = admission_permit;
                        if let Err(error) =
                            handle_connection_async(stream, tx, cleanup, shutdown).await
                        {
                            eprintln!("botster-hub daemon connection error: {error}");
                        }
                    }));
                }
                Some(ControlMessage::RejectedConnection) => {
                    control_state.lifecycle_counters.rejected_connections = control_state
                        .lifecycle_counters
                        .rejected_connections
                        .saturating_add(1);
                }
                Some(message) => {
                    if handle_control_message(
                        &mut daemon,
                        &mut control_state,
                        &local_webrtc_terminal_record_path,
                        transport_runtime.handle(),
                        control_tx.clone(),
                        message,
                    ) {
                        let _ = shutdown_tx.send(true);
                        wait_for_connection_tasks(
                            &transport_runtime,
                            &mut connection_tasks,
                            &cleanup_rx,
                            &mut daemon,
                            &mut control_state,
                            control_tx.clone(),
                        );
                        let status = daemon.stop();
                        cleanup_socket_path(&socket_path);
                        return Ok(status);
                    }
                }
                None => return Err(DaemonTransportError::ControlThreadStopped),
            },
            OwnerEvent::Reconcile => {
                if control_state.next_reconciliation <= Instant::now() {
                    drive_entity_subscriptions(&mut daemon, &mut control_state);
                    control_state.next_reconciliation =
                        Instant::now() + ENTITY_RECONCILIATION_INTERVAL;
                }
                if !socket_path.exists() {
                    rebind_missing_socket_path(&socket_path);
                }
            }
        }
    }
}

/// Connect to a daemon and send one operator request.
pub fn request(
    config: &HubConfig,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::request(&endpoint, request).map_err(DaemonTransportError::from)
}

/// Persistent daemon connection for clients that own attach subscription state.
pub struct DaemonConnection {
    inner: ClientDaemonConnection,
}

impl DaemonConnection {
    /// Connect to the daemon and complete the socket protocol handshake.
    pub fn connect(config: &HubConfig) -> DaemonTransportResult<Self> {
        let endpoint = daemon_endpoint(config)?;
        let inner =
            ClientDaemonConnection::connect(&endpoint).map_err(DaemonTransportError::from)?;
        Ok(Self { inner })
    }

    /// Send one request over this persistent connection.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        self.inner
            .request(request)
            .map_err(DaemonTransportError::from)
    }
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    config: &HubConfig,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::stream_attach(&endpoint, &session_id.0, &subscription_id.0, output)
        .map_err(DaemonTransportError::from)
}

async fn accept_connections(
    listener: TokioUnixListener,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    admission: Arc<Semaphore>,
) {
    let rejection_admission = Arc::new(Semaphore::new(DAEMON_MAX_REJECTION_TASKS));
    let mut rejection_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        match admission.clone().try_acquire_owned() {
                            Ok(admission_permit) => {
                                if control_tx
                                    .send(ControlMessage::AcceptedConnection {
                                        stream,
                                        admission_permit,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(_) => {
                                let permit = tokio::select! {
                                    permit = rejection_admission.clone().acquire_owned() => {
                                        permit.expect("rejection semaphore remains owned by accept loop")
                                    }
                                    changed = shutdown_rx.changed() => {
                                        let _ = changed;
                                        return;
                                    }
                                };
                                let rejection_tx = control_tx.clone();
                                rejection_tasks.spawn(async move {
                                    let _permit = permit;
                                    reject_connection_async(stream).await;
                                    let _ = rejection_tx
                                        .send(ControlMessage::RejectedConnection)
                                        .await;
                                });
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("botster-hub daemon accept error: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            changed = shutdown_rx.changed() => {
                let _ = changed;
                return;
            }
            result = rejection_tasks.join_next(), if !rejection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("botster-hub daemon rejection task error: {error}");
                }
            }
        }
    }
}

async fn reject_connection_async(stream: TokioUnixStream) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    if read_async_frame::<DaemonHello, _>(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT))
        .await
        .is_err()
    {
        return;
    }
    let _ = write_async_frame(
        &mut write_half,
        &DaemonHelloAck {
            protocol: PROTOCOL.to_string(),
            compatibility: DaemonCompatibility::current(),
            diagnostics: vec![DaemonDiagnostic::backpressure(
                "daemon_connection_admission",
                "daemon connection capacity reached",
            )],
        },
    )
    .await;
}

async fn handle_connection_async(
    stream: TokioUnixStream,
    control_tx: ControlSender,
    cleanup_tx: SyncSender<ConnectionCleanup>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> DaemonTransportResult<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    let mut cleanup = ConnectionCleanupGuard::new(cleanup_tx, ConnectionTerminalReason::Protocol);
    let hello: DaemonHello =
        match read_async_frame(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT)).await {
            Ok(hello) => hello,
            Err(ClientDaemonTransportError::ClientDisconnected) => {
                cleanup.set_reason(ConnectionTerminalReason::Eof);
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
    if hello.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol("unexpected hello protocol"));
    }
    if let Err(error) = write_async_frame(
        &mut write_half,
        &DaemonHelloAck {
            protocol: PROTOCOL.to_string(),
            compatibility: DaemonCompatibility::current(),
            diagnostics: vec![DaemonDiagnostic::connected("hello")],
        },
    )
    .await
    {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        return Err(error);
    }
    cleanup.set_reason(ConnectionTerminalReason::Eof);

    loop {
        let request = tokio::select! {
            request = read_async_frame::<DaemonRequest, _>(&mut reader, None) => request,
            changed = shutdown_rx.changed() => {
                let _ = changed;
                cleanup.set_reason(ConnectionTerminalReason::Shutdown);
                return Ok(());
            }
        };
        let request = match request {
            Ok(request) => request,
            Err(ClientDaemonTransportError::ClientDisconnected) => {
                cleanup.set_reason(ConnectionTerminalReason::Eof);
                return Ok(());
            }
            Err(error) => {
                cleanup.set_reason(ConnectionTerminalReason::Protocol);
                return Err(error.into());
            }
        };
        if let DaemonRequest::SubscribeEntities {
            entity_type,
            subscription_id,
        } = request
        {
            return handle_entity_subscription_async(
                write_half,
                reader,
                control_tx,
                entity_type,
                subscription_id,
                cleanup,
                shutdown_rx,
            )
            .await;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let close_after_response = matches!(request, DaemonRequest::DaemonShutdown);
        let (response_delivery_tx, response_delivery_rx) = if close_after_response {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let active_change = AttachedSubscriptionChange::from_request(&request);
        control_tx
            .send(ControlMessage::Request {
                request: Box::new(request),
                reply_tx,
                response_delivery_rx,
                grant_id: None,
            })
            .await
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        let response = receive_control_response(reply_rx).await?;
        if response.kind != DaemonResponseKind::OperatorError {
            cleanup.apply_subscription_change(active_change);
        }
        let write_result = write_async_frame(&mut write_half, &response).await;
        if let Some(response_delivery_tx) = response_delivery_tx {
            let _ = response_delivery_tx.send(());
        }
        if let Err(error) = write_result {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
                delivery_kind: daemon_delivery_kind(&response),
            });
            return Err(error);
        }
        if close_after_response {
            cleanup.set_reason(ConnectionTerminalReason::NormalClose);
            return Ok(());
        }
    }
}

async fn handle_entity_subscription_async(
    mut write_half: tokio::net::unix::OwnedWriteHalf,
    mut reader: AsyncBufReader<tokio::net::unix::OwnedReadHalf>,
    control_tx: ControlSender,
    entity_type: String,
    subscription_id: String,
    mut cleanup: ConnectionCleanupGuard,
    mut shutdown_rx: watch::Receiver<bool>,
) -> DaemonTransportResult<()> {
    let (frame_tx, mut frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id: subscription_id.clone(),
            frame_tx: EntityFrameSender::Async(frame_tx),
            reply_tx,
            grant_id: None,
        })
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
    let response = receive_control_response(reply_rx).await?;
    write_async_frame(&mut write_half, &response).await?;
    if response.kind != DaemonResponseKind::EntitySubscribed {
        cleanup.set_reason(ConnectionTerminalReason::NormalClose);
        return Ok(());
    }
    cleanup.add_entity_subscription(subscription_id.clone());
    let inbound_request = read_async_frame::<DaemonRequest, _>(&mut reader, None);
    tokio::pin!(inbound_request);

    loop {
        tokio::select! {
            frame = frame_rx.recv() => {
                let Some(frame) = frame else {
                    cleanup.set_reason(ConnectionTerminalReason::Cancellation);
                    return Ok(());
                };
                if let Err(error) = write_async_frame(&mut write_half, &frame).await {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
                        delivery_kind: DaemonDeliveryKind::Control,
                    });
                    return Err(error);
                }
            }
            request = &mut inbound_request => {
                let request = match request {
                    Ok(request) => request,
                    Err(ClientDaemonTransportError::ClientDisconnected) => {
                        cleanup.set_reason(ConnectionTerminalReason::Eof);
                        return Ok(());
                    }
                    Err(error) => {
                        cleanup.set_reason(ConnectionTerminalReason::Protocol);
                        return Err(error.into());
                    }
                };
                if !matches!(
                    request,
                    DaemonRequest::UnsubscribeEntities { subscription_id: ref requested }
                        if requested == &subscription_id
                ) {
                    cleanup.set_reason(ConnectionTerminalReason::Protocol);
                    return Err(DaemonTransportError::Protocol(
                        "entity stream accepts only its matching unsubscribe request",
                    ));
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                control_tx
                    .send(ControlMessage::UnsubscribeEntities {
                        subscription_id: subscription_id.clone(),
                        reply_tx: Some(reply_tx),
                        grant_id: None,
                    })
                    .await
                    .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
                let response = receive_control_response(reply_rx).await?;
                write_async_frame(&mut write_half, &response).await?;
                cleanup.remove_entity_subscription(&subscription_id);
                cleanup.set_reason(ConnectionTerminalReason::NormalClose);
                return Ok(());
            }
            changed = shutdown_rx.changed() => {
                let _ = changed;
                cleanup.set_reason(ConnectionTerminalReason::Shutdown);
                return Ok(());
            }
        }
    }
}

async fn read_async_frame<T, R>(
    reader: &mut AsyncBufReader<R>,
    first_byte_timeout: Option<Duration>,
) -> Result<T, ClientDaemonTransportError>
where
    T: for<'de> serde::Deserialize<'de>,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut first = [0_u8; 1];
    let read_first = reader.read(&mut first);
    let count = if let Some(timeout) = first_byte_timeout {
        tokio::time::timeout(timeout, read_first)
            .await
            .map_err(|_| {
                ClientDaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "daemon handshake deadline elapsed",
                ))
            })?
            .map_err(ClientDaemonTransportError::Io)?
    } else {
        read_first.await.map_err(ClientDaemonTransportError::Io)?
    };
    if count == 0 {
        return Err(ClientDaemonTransportError::ClientDisconnected);
    }
    bytes.push(first[0]);
    if first[0] != b'\n' {
        tokio::time::timeout(DAEMON_INCOMPLETE_FRAME_TIMEOUT, async {
            loop {
                let available = reader
                    .fill_buf()
                    .await
                    .map_err(ClientDaemonTransportError::Io)?;
                if available.is_empty() {
                    return Err(ClientDaemonTransportError::Protocol(
                        "daemon frame ended before newline",
                    ));
                }
                let consumed = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |index| index + 1);
                if bytes.len().saturating_add(consumed) > DAEMON_MAX_FRAME_BYTES {
                    return Err(ClientDaemonTransportError::Protocol(
                        "daemon frame exceeded size bound",
                    ));
                }
                bytes.extend_from_slice(&available[..consumed]);
                reader.consume(consumed);
                if bytes.last() == Some(&b'\n') {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| {
            ClientDaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon incomplete frame deadline elapsed",
            ))
        })??;
    }
    if bytes.len() > DAEMON_MAX_FRAME_BYTES {
        return Err(ClientDaemonTransportError::Protocol(
            "daemon frame exceeded size bound",
        ));
    }
    if bytes.last() != Some(&b'\n') {
        return Err(ClientDaemonTransportError::Protocol(
            "daemon frame ended before newline",
        ));
    }
    serde_json::from_slice(&bytes).map_err(ClientDaemonTransportError::Json)
}

async fn write_async_frame<T>(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &T,
) -> DaemonTransportResult<()>
where
    T: serde::Serialize,
{
    let mut bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    bytes.push(b'\n');
    tokio::time::timeout(DAEMON_CLIENT_WRITE_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| {
            DaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon client write deadline elapsed",
            ))
        })?
        .map_err(DaemonTransportError::Io)
}

async fn receive_control_response(
    reply_rx: oneshot::Receiver<DaemonTransportResult<DaemonResponse>>,
) -> DaemonTransportResult<DaemonResponse> {
    reply_rx
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionTerminalReason {
    Eof,
    Protocol,
    WriteFailure,
    Cancellation,
    Shutdown,
    NormalClose,
}

impl ConnectionTerminalReason {
    fn label(self) -> &'static str {
        match self {
            Self::Eof => "eof",
            Self::Protocol => "protocol",
            Self::WriteFailure => "write_failure",
            Self::Cancellation => "cancellation",
            Self::Shutdown => "shutdown",
            Self::NormalClose => "normal_close",
        }
    }
}

#[derive(Debug)]
struct ConnectionCleanup {
    attached_subscriptions: Vec<AttachedSubscription>,
    entity_subscription_ids: BTreeSet<String>,
    reason: ConnectionTerminalReason,
}

struct ConnectionCleanupGuard {
    cleanup_tx: SyncSender<ConnectionCleanup>,
    cleanup: Option<ConnectionCleanup>,
}

impl ConnectionCleanupGuard {
    fn new(cleanup_tx: SyncSender<ConnectionCleanup>, reason: ConnectionTerminalReason) -> Self {
        Self {
            cleanup_tx,
            cleanup: Some(ConnectionCleanup {
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: BTreeSet::new(),
                reason,
            }),
        }
    }

    fn apply_subscription_change(&mut self, change: Option<AttachedSubscriptionChange>) {
        let Some(cleanup) = self.cleanup.as_mut() else {
            return;
        };
        apply_attached_subscription_change(&mut cleanup.attached_subscriptions, change);
    }

    fn add_entity_subscription(&mut self, subscription_id: String) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.entity_subscription_ids.insert(subscription_id);
        }
    }

    fn remove_entity_subscription(&mut self, subscription_id: &str) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.entity_subscription_ids.remove(subscription_id);
        }
    }

    fn set_reason(&mut self, reason: ConnectionTerminalReason) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.reason = reason;
        }
    }
}

impl Drop for ConnectionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take()
            && let Err(error) = self.cleanup_tx.try_send(cleanup)
        {
            eprintln!("botster-hub connection cleanup enqueue failed: {error}");
        }
    }
}

fn reap_finished_connection_tasks(tasks: &mut Vec<JoinHandle<()>>) {
    tasks.retain(|task| !task.is_finished());
}

fn wait_for_connection_tasks(
    runtime: &tokio::runtime::Runtime,
    tasks: &mut Vec<JoinHandle<()>>,
    cleanup_rx: &mpsc::Receiver<ConnectionCleanup>,
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
) {
    let deadline = Instant::now() + DAEMON_CLIENT_WRITE_TIMEOUT;
    while !tasks.iter().all(JoinHandle::is_finished) && Instant::now() < deadline {
        while let Ok(cleanup) = cleanup_rx.try_recv() {
            handle_connection_cleanup(daemon, state, control_tx.clone(), cleanup);
        }
        thread::sleep(Duration::from_millis(10));
    }
    for task in tasks.iter() {
        if !task.is_finished() {
            task.abort();
        }
    }
    runtime.block_on(async {
        for task in tasks.drain(..) {
            let _ = task.await;
        }
    });
    while let Ok(cleanup) = cleanup_rx.try_recv() {
        handle_connection_cleanup(daemon, state, control_tx.clone(), cleanup);
    }
}

fn handle_connection_cleanup(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
    cleanup: ConnectionCleanup,
) {
    state.lifecycle_counters.live_connections =
        state.lifecycle_counters.live_connections.saturating_sub(1);
    *state
        .lifecycle_counters
        .cleanup_by_reason
        .entry(cleanup.reason.label().to_string())
        .or_default() += 1;

    let mut failed = false;
    for subscription_id in cleanup.entity_subscription_ids {
        if state
            .entity_subscriptions
            .remove(&subscription_id)
            .is_some()
        {
            state.lifecycle_counters.live_entity_subscriptions = state
                .lifecycle_counters
                .live_entity_subscriptions
                .saturating_sub(1);
            state.released_entity_generations = state.released_entity_generations.saturating_add(1);
        }
    }
    for subscription in cleanup.attached_subscriptions {
        let result = handle_control_request(
            daemon,
            &mut state.logical_clock,
            &mut state.drain_cursors,
            &mut state.pending_runtime,
            DaemonObservability {
                egress: &state.egress_diagnostics,
                lifecycle: &state.lifecycle_counters,
            },
            control_tx.clone(),
            DaemonRequest::Detach {
                session_id: subscription.session_id,
                subscription_id: subscription.subscription_id,
            },
        );
        state.lifecycle_counters.live_attach_subscriptions = state
            .lifecycle_counters
            .live_attach_subscriptions
            .saturating_sub(1);
        state.released_attach_generations = state.released_attach_generations.saturating_add(1);
        failed |= cleanup_detach_failed(&result);
    }
    if failed {
        // `connection_cleanup_ignores_only_an_already_removed_session` is the
        // designated positive control for predicate-true cleanup failures.
        state.lifecycle_counters.cleanup_failed =
            state.lifecycle_counters.cleanup_failed.saturating_add(1);
    } else {
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
    }
}

fn cleanup_detach_failed(result: &DaemonTransportResult<DaemonResponse>) -> bool {
    match result {
        Err(DaemonTransportError::Client(crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Detach,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        })) => false,
        Ok(response) => response.kind == DaemonResponseKind::OperatorError,
        Err(_) => true,
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

pub(crate) fn handle_control_message(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    local_webrtc_terminal_record_path: &Path,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::AcceptedConnection { .. } | ControlMessage::RejectedConnection => false,
        ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id,
            frame_tx,
            reply_tx,
            grant_id,
        } => {
            // Late WebRTC control messages after PeerClosed must not recreate peer-owned state.
            if let Some(grant_id) = grant_id.as_deref()
                && !daemon.local_webrtc().has_live_peer(grant_id)
            {
                let _ = reply_tx.send(Ok(entity_subscription_error(
                    "local_webrtc_peer_gone",
                    &subscription_id,
                    "local WebRTC peer is no longer live",
                )));
                return false;
            }
            let response = register_entity_subscription(
                daemon,
                state,
                entity_type,
                subscription_id,
                frame_tx,
                grant_id,
            );
            let _ = reply_tx.send(response);
            false
        }
        ControlMessage::UnsubscribeEntities {
            subscription_id,
            reply_tx,
            grant_id,
        } => {
            if let Some(grant_id) = grant_id.as_deref()
                && !daemon.local_webrtc().has_live_peer(grant_id)
            {
                // Peer already gone: owner-checked residual cleanup only. Never delete a row now
                // owned by a different live grant (subscription-id reuse after PeerClosed).
                let should_remove = match state.entity_subscriptions.get(&subscription_id) {
                    None => false,
                    Some(subscription) => match subscription.owner_grant_id.as_deref() {
                        None => true,
                        Some(owner) => owner == grant_id,
                    },
                };
                if should_remove
                    && state
                        .entity_subscriptions
                        .remove(&subscription_id)
                        .is_some()
                {
                    state.lifecycle_counters.live_entity_subscriptions = state
                        .lifecycle_counters
                        .live_entity_subscriptions
                        .saturating_sub(1);
                    state.released_entity_generations =
                        state.released_entity_generations.saturating_add(1);
                }
                if let Some(reply_tx) = reply_tx {
                    // Idempotent unsubscribed reply for the stale client even when the row is
                    // preserved for a replacement owner.
                    let _ = reply_tx.send(Ok(daemon_response_base(
                        DaemonResponseKind::EntityUnsubscribed,
                    )));
                }
                return false;
            }
            if state
                .entity_subscriptions
                .remove(&subscription_id)
                .is_some()
            {
                state.lifecycle_counters.live_entity_subscriptions = state
                    .lifecycle_counters
                    .live_entity_subscriptions
                    .saturating_sub(1);
                state.released_entity_generations =
                    state.released_entity_generations.saturating_add(1);
            }
            if let Some(reply_tx) = reply_tx {
                let _ = reply_tx.send(Ok(daemon_response_base(
                    DaemonResponseKind::EntityUnsubscribed,
                )));
            }
            false
        }
        ControlMessage::Request {
            request,
            reply_tx,
            response_delivery_rx,
            grant_id,
        } => {
            // Late WebRTC Requests after PeerClosed must not create durable ownership or run
            // stale control against a gone peer. Socket path leaves grant_id = None.
            if let Some(grant_id) = grant_id.as_deref()
                && !daemon.local_webrtc().has_live_peer(grant_id)
            {
                let operation = control_request_operation_label(request.as_ref());
                return send_control_response(
                    reply_tx,
                    Ok(local_webrtc_peer_gone_request_error(operation)),
                    response_delivery_rx,
                );
            }
            if matches!(request.as_ref(), DaemonRequest::CheckHubUpdate) {
                return match plan_hub_update_check() {
                    HubUpdateCheckPlan::Immediate(update) => send_control_response(
                        reply_tx,
                        Ok(daemon_hub_update(update)),
                        response_delivery_rx,
                    ),
                    HubUpdateCheckPlan::Managed(_check)
                        if state.pending_hub_update_reply.is_some() =>
                    {
                        send_control_response(
                            reply_tx,
                            Ok(daemon_hub_update(DaemonHubUpdate {
                                state: DaemonHubUpdateState::Unavailable,
                                current_version: software_identity().version,
                                available_version: None,
                                build_revision: None,
                                reason: Some("busy".to_string()),
                                action: Some("retry".to_string()),
                            })),
                            response_delivery_rx,
                        )
                    }
                    HubUpdateCheckPlan::Managed(check) => {
                        state.pending_hub_update_reply = Some(reply_tx);
                        let completion_tx = control_tx.clone();
                        transport_handle.spawn_blocking(move || {
                            let update = execute_managed_update_check(check);
                            let _ = completion_tx
                                .blocking_send(ControlMessage::HubUpdateCheckCompleted { update });
                        });
                        false
                    }
                };
            }
            let attached_change = AttachedSubscriptionChange::from_request(&request);
            let reconcile_after_request = matches!(
                request.as_ref(),
                DaemonRequest::Spawn { .. }
                    | DaemonRequest::Resize { .. }
                    | DaemonRequest::ShutdownSession { .. }
                    | DaemonRequest::RemoveSession { .. }
            );
            let response = handle_control_request(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                },
                control_tx,
                *request,
            )
            .or_else(|error| match error {
                DaemonTransportError::Client(error) => Ok(daemon_operator_error(error)),
                DaemonTransportError::Package(error) => Ok(daemon_package_error(error)),
                DaemonTransportError::SpawnTarget(error) => Ok(daemon_spawn_target_error(error)),
                DaemonTransportError::Worktree(error) => Ok(daemon_worktree_error(error)),
                DaemonTransportError::State(error) => Ok(daemon_state_error(error)),
                DaemonTransportError::Entrypoint(error) => Ok(daemon_entrypoint_error(error)),
                DaemonTransportError::LocalWebrtc(error) => Ok(daemon_local_webrtc_error(error)),
                error => Err(error),
            });
            if response
                .as_ref()
                .is_ok_and(|response| response.kind != DaemonResponseKind::OperatorError)
            {
                record_attached_subscription_change(state, attached_change, grant_id.as_deref());
            }
            if reconcile_after_request && !state.entity_subscriptions.is_empty() {
                drive_entity_subscriptions(daemon, state);
                state.next_reconciliation = Instant::now() + ENTITY_RECONCILIATION_INTERVAL;
            }
            if response
                .as_ref()
                .is_ok_and(|response| response.kind == DaemonResponseKind::Shutdown)
                && let Some(update_reply_tx) = state.pending_hub_update_reply.take()
            {
                let _ = send_control_response(
                    update_reply_tx,
                    Ok(daemon_hub_update(DaemonHubUpdate {
                        state: DaemonHubUpdateState::Unavailable,
                        current_version: software_identity().version,
                        available_version: None,
                        build_revision: None,
                        reason: Some("daemon_shutdown".to_string()),
                        action: Some("retry".to_string()),
                    })),
                    None,
                );
            }
            send_control_response(reply_tx, response, response_delivery_rx)
        }
        ControlMessage::HubUpdateCheckCompleted { update } => state
            .pending_hub_update_reply
            .take()
            .is_some_and(|reply_tx| {
                send_control_response(reply_tx, Ok(daemon_hub_update(update)), None)
            }),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            attached_subscriptions,
            entity_subscription_ids,
            terminal_record,
        } => {
            let cleanup_reason = format!("webrtc_{}", terminal_record.cause);
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry(cleanup_reason)
                .or_default() += 1;
            state.lifecycle_counters.cleanup_completed =
                state.lifecycle_counters.cleanup_completed.saturating_add(1);
            if let Err(error) = persist_local_webrtc_terminal_record(
                local_webrtc_terminal_record_path,
                &terminal_record,
            ) {
                eprintln!(
                    "local WebRTC sender terminal record persistence failed: kind={:?}",
                    error.kind()
                );
            }
            let remove_result = daemon.local_webrtc().remove_peer(&grant_id);
            let mut removed_grants: BTreeSet<String> =
                remove_result.removed_grant_ids.into_iter().collect();
            // Always include the closing grant so entity/attach sweep runs even if the peer
            // map entry was already gone (idempotent PeerClosed).
            removed_grants.insert(grant_id.clone());

            // Snapshot IDs are only removed when the current row is unowned or still owned by a
            // removed grant. A reused subscription_id owned by a different live peer is preserved.
            let mut removed_entity_ids = BTreeSet::new();
            for subscription_id in entity_subscription_ids {
                let should_remove = match state.entity_subscriptions.get(&subscription_id) {
                    None => false,
                    Some(subscription) => match subscription.owner_grant_id.as_deref() {
                        None => true,
                        Some(owner) => removed_grants.contains(owner),
                    },
                };
                if should_remove {
                    removed_entity_ids.insert(subscription_id);
                }
            }
            // Independent of the peer-side snapshot: remove every daemon entity subscription
            // owned by any grant this forget removed (primary + fail-closed siblings).
            for (id, subscription) in &state.entity_subscriptions {
                if let Some(owner) = subscription.owner_grant_id.as_deref()
                    && removed_grants.contains(owner)
                {
                    removed_entity_ids.insert(id.clone());
                }
            }
            for subscription_id in removed_entity_ids {
                if state
                    .entity_subscriptions
                    .remove(&subscription_id)
                    .is_some()
                {
                    state.lifecycle_counters.live_entity_subscriptions = state
                        .lifecycle_counters
                        .live_entity_subscriptions
                        .saturating_sub(1);
                    state.released_entity_generations =
                        state.released_entity_generations.saturating_add(1);
                }
            }

            // Merge attach candidates from the PeerClosed snapshot and any fail-closed siblings.
            // Owner-check every row: a delayed snapshot must not detach an attach that a
            // different live grant now owns after (session_id, subscription_id) reuse.
            let mut detach_candidates = attached_subscriptions;
            for subscription in remove_result.attached_subscriptions {
                if !detach_candidates.iter().any(|existing| {
                    existing.session_id == subscription.session_id
                        && existing.subscription_id == subscription.subscription_id
                }) {
                    detach_candidates.push(subscription);
                }
            }
            // Independent of the peer-side snapshot: include every attach currently owned by a
            // removed grant so residual Attach rows that raced after cleanup_once still get cleaned.
            for ((session_id, subscription_id), owner) in
                &state.pending_runtime.attach_owner_grant_ids
            {
                if removed_grants.contains(owner.as_str())
                    && !detach_candidates.iter().any(|existing| {
                        existing.session_id == *session_id
                            && existing.subscription_id == *subscription_id
                    })
                {
                    detach_candidates.push(LocalWebrtcAttachedSubscription {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                    });
                }
            }
            let detach_list: Vec<LocalWebrtcAttachedSubscription> = detach_candidates
                .into_iter()
                .filter(|subscription| {
                    match state
                        .pending_runtime
                        .attach_owner_grant_ids
                        .get(&(
                            subscription.session_id.clone(),
                            subscription.subscription_id.clone(),
                        ))
                        .map(String::as_str)
                    {
                        // Unowned residual (socket path or missing index): allow cleanup.
                        None => true,
                        // Only detach when the current owner is one of the grants this forget removes.
                        Some(owner) => removed_grants.contains(owner),
                    }
                })
                .collect();
            // Drop owner index rows for removed grants before Detach so counters stay consistent
            // even if Detach is a no-op for an already-gone session. Preserve replacement owners.
            state
                .pending_runtime
                .attach_owner_grant_ids
                .retain(|_, owner| !removed_grants.contains(owner.as_str()));
            state.lifecycle_counters.live_attach_subscriptions = state
                .lifecycle_counters
                .live_attach_subscriptions
                .saturating_sub(detach_list.len() as u64);
            state.released_attach_generations = state
                .released_attach_generations
                .saturating_add(detach_list.len() as u64);
            detach_local_webrtc_subscriptions(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                control_tx,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                },
                detach_list,
            );
            false
        }
        ControlMessage::EgressWriteFailed { delivery_kind } => {
            record_egress_write_failure(
                &mut state.egress_diagnostics,
                &mut state.lifecycle_counters,
                delivery_kind,
            );
            false
        }
    }
}

fn record_egress_write_failure(
    diagnostics: &mut DaemonEgressDiagnostics,
    counters: &mut DaemonLifecycleCounters,
    delivery_kind: DaemonDeliveryKind,
) {
    diagnostics.record_write_failure(delivery_kind);
    counters.stalled_writes = counters.stalled_writes.saturating_add(1);
}

fn send_control_response(
    reply_tx: ControlReplySender,
    response: DaemonTransportResult<DaemonResponse>,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    let should_stop = matches!(
        response,
        Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            ..
        })
    );
    let response_received = reply_tx.send(response).is_ok();
    wait_for_response_delivery(should_stop, response_received, response_delivery_rx);
    should_stop
}

fn wait_for_response_delivery(
    should_stop: bool,
    response_received: bool,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    if should_stop
        && response_received
        && let Some(response_delivery_rx) = response_delivery_rx
    {
        let _ = response_delivery_rx.recv_timeout(DAEMON_CLIENT_WRITE_TIMEOUT);
        return true;
    }
    false
}

fn persist_local_webrtc_terminal_record(
    path: &Path,
    record: &LocalWebrtcSenderTerminalRecord,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if bytes.len() > LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "local WebRTC sender terminal record exceeded size bound",
        ));
    }
    let temporary_path = path.with_extension("json.tmp");
    fs::write(&temporary_path, bytes)?;
    if let Err(error) = fs::rename(&temporary_path, path) {
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }
    Ok(())
}

fn detach_local_webrtc_subscriptions(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    control_tx: ControlSender,
    observability: DaemonObservability<'_>,
    attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
) {
    for subscription in attached_subscriptions {
        let _ = handle_control_request(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability,
            control_tx.clone(),
            DaemonRequest::Detach {
                session_id: subscription.session_id,
                subscription_id: subscription.subscription_id,
            },
        );
    }
}

#[derive(Clone, Copy)]
struct DaemonObservability<'a> {
    egress: &'a DaemonEgressDiagnostics,
    lifecycle: &'a DaemonLifecycleCounters,
}

fn handle_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    observability: DaemonObservability<'_>,
    control_tx: ControlSender,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListApps => list_apps_response(daemon),
        DaemonRequest::ListSpawnTargets => list_spawn_targets_response(daemon),
        DaemonRequest::ShowSpawnTarget { target_id } => {
            show_spawn_target_response(daemon, &target_id)
        }
        DaemonRequest::CreateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            // Only pre-check session-types once the root is known to be a directory.
            // Non-directory roots must fall through to create_spawn_target's
            // root_not_directory rather than a misleading invalid_repo_session_types.
            if enabled && root.is_dir() {
                ensure_repo_session_types_valid_for_enabled_root(&root)?;
            }
            let before_session_types = session_type_definition_map(daemon)?;
            let response = mutate_spawn_targets_response(daemon, |targets| {
                crate::create_spawn_target(
                    targets,
                    SpawnTargetCreate {
                        target_id,
                        label,
                        root,
                        enabled,
                        kind,
                        base_ref,
                        metadata,
                    },
                )
            })?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            Ok(response)
        }
        DaemonRequest::UpdateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            let recovery_disable = enabled == Some(false);
            if !recovery_disable {
                ensure_update_would_not_enable_invalid_repo_session_types(
                    daemon,
                    &target_id,
                    root.as_ref(),
                    enabled,
                )?;
            }
            let before_session_types = match session_type_definition_map(daemon) {
                Ok(before) => Some(before),
                Err(error) if recovery_disable && is_invalid_repo_session_types_error(&error) => {
                    None
                }
                Err(error) => return Err(error),
            };
            let response = mutate_spawn_targets_with_worktrees_response(
                daemon,
                |targets, worktrees| {
                    if kind.as_deref().is_some_and(|kind| kind != "git")
                        && worktrees.iter().any(|worktree| {
                            worktree.target_id == target_id
                                && worktree.management == "hub_managed_git"
                        })
                    {
                        return Err(SpawnTargetError::new(
                            "managed_worktrees_exist",
                            "Git target cannot be reclassified while managed worktrees reference it",
                        ));
                    }
                    crate::update_spawn_target(
                        targets,
                        &target_id,
                        SpawnTargetUpdate {
                            label,
                            root,
                            enabled,
                            kind,
                            base_ref,
                            metadata,
                        },
                    )
                },
            )?;
            match before_session_types {
                Some(before) => {
                    advance_session_type_generation_if_changed(daemon, &before)?;
                }
                None => {
                    force_advance_session_type_generation(daemon)?;
                }
            }
            Ok(response)
        }
        DaemonRequest::DeleteSpawnTarget { target_id } => {
            let before_session_types = match session_type_definition_map(daemon) {
                Ok(before) => Some(before),
                Err(error) if is_invalid_repo_session_types_error(&error) => None,
                Err(error) => return Err(error),
            };
            let response =
                mutate_spawn_targets_with_worktrees_response(daemon, |targets, worktrees| {
                    if worktrees.iter().any(|worktree| {
                        worktree.target_id == target_id && worktree.management == "hub_managed_git"
                    }) {
                        return Err(SpawnTargetError::new(
                            "managed_worktrees_exist",
                            "Git target cannot be deleted while managed worktrees reference it",
                        ));
                    }
                    crate::delete_spawn_target(targets, &target_id)
                })?;
            match before_session_types {
                Some(before) => {
                    advance_session_type_generation_if_changed(daemon, &before)?;
                }
                None => {
                    force_advance_session_type_generation(daemon)?;
                }
            }
            Ok(response)
        }
        DaemonRequest::ValidateSpawnTarget { target_id } => Ok(daemon_spawn_target_validation(
            crate::validate_spawn_target(
                &daemon
                    .runtime()
                    .ok_or(DaemonTransportError::DaemonNotRunning)?
                    .state()
                    .spawn_targets,
                &target_id,
            ),
        )),
        DaemonRequest::ListWorktrees => list_worktrees_response(daemon),
        DaemonRequest::ShowWorktree { worktree_id } => show_worktree_response(daemon, &worktree_id),
        DaemonRequest::CreateWorktree {
            worktree_id,
            target_id,
            label,
            path,
            metadata,
        } => create_worktree_response(
            daemon,
            WorktreeCreate {
                worktree_id,
                target_id,
                label,
                path,
                metadata,
            },
        ),
        DaemonRequest::DeleteWorktree { worktree_id } => {
            delete_worktree_response(daemon, &worktree_id)
        }
        DaemonRequest::ResolveAppLaunch {
            package_name,
            entrypoint_id,
        } => resolve_app_launch_response(daemon, &package_name, &entrypoint_id),
        DaemonRequest::ResolvePackageRoute {
            package_name,
            route_id,
        } => resolve_package_route_response(daemon, &package_name, &route_id),
        DaemonRequest::ListPackageNavigation => list_package_navigation_response(daemon),
        DaemonRequest::ListPackages => list_packages_response(daemon),
        DaemonRequest::ListAvailablePackages { registry_path } => {
            available_packages_response(daemon, registry_path)
        }
        DaemonRequest::InspectAvailablePackage {
            registry_path,
            entry_id,
        } => inspect_available_package_response(daemon, registry_path, &entry_id),
        DaemonRequest::PreviewPackageInstall {
            registry_path,
            entry_id,
        } => preview_package_install_response(daemon, registry_path, &entry_id),
        DaemonRequest::InstallPackageRegistryEntry {
            registry_path,
            entry_id,
        } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let decision = {
                let record = daemon.package_registry_mut().install_registry_entry(
                    registry_path,
                    &entry_id,
                    "daemon socket install registry package",
                )?;
                PackageDecision {
                    package_name: record.manifest.name.clone(),
                    action: PackageAction::Install,
                    state: record.state,
                    classification: record.classification,
                    admitted_host_profile: None,
                    audit_reason: record.last_audit_reason.clone(),
                }
            };
            persist_package_registry(daemon)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::PluginLifecycleStatus => plugin_lifecycle_response(daemon),
        DaemonRequest::InstallPackageLocalPath { path } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let decision = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "daemon socket install local package")?;
                PackageDecision {
                    package_name: record.manifest.name.clone(),
                    action: PackageAction::Install,
                    state: record.state,
                    classification: record.classification,
                    admitted_host_profile: None,
                    audit_reason: record.last_audit_reason.clone(),
                }
            };
            persist_package_registry(daemon)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::CheckPackageUpdate { package_name } => {
            check_package_update_response(daemon, &package_name)
        }
        DaemonRequest::PreviewPackageUpdate { package_name, pin } => {
            preview_package_update_response(daemon, &package_name, pin)
        }
        DaemonRequest::ApplyPackageUpdate { package_name, pin } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let update_status = package_update_status(daemon, &package_name, Some(pin.clone()))?;
            let decision = {
                let pin = package_pin_from_daemon(pin)?;
                let record = daemon.package_registry_mut().pin(
                    &package_name,
                    pin,
                    "daemon socket apply package update",
                )?;
                PackageDecision {
                    package_name: record.manifest.name.clone(),
                    action: PackageAction::ApplyUpdate,
                    state: record.state,
                    classification: record.classification,
                    admitted_host_profile: record.admitted_host_profile.clone(),
                    audit_reason: record.last_audit_reason.clone(),
                }
            };
            persist_package_registry(daemon)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            let mut response = package_decision_response(daemon, decision)?;
            response.update_status = Some(update_status);
            Ok(response)
        }
        DaemonRequest::ShowPackage { package_name } => show_package_response(daemon, &package_name),
        DaemonRequest::SetPackageConfiguration {
            package_name,
            values,
        } => {
            let values = values
                .into_iter()
                .map(|(key, value)| {
                    serde_json::from_value::<PackageConfigurationValue>(value)
                        .map(|value| (key.clone(), value))
                        .map_err(|error| {
                            PackageRegistryError::without_record(
                                package_name.clone(),
                                PackageAction::Configure,
                                PackageAdmissionReason::InvalidConfiguration(vec![
                                    crate::PackageConfigurationDiagnostic {
                                        kind: "value_decode_error".to_string(),
                                        field: Some(key),
                                        message: format!(
                                            "configuration value is not a package configuration value: {error}"
                                        ),
                                    },
                                ]),
                                "daemon socket configure package".to_string(),
                            )
                        })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            daemon.package_registry_mut().set_configuration(
                &package_name,
                values,
                "daemon socket configure package",
            )?;
            persist_package_registry(daemon)?;
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::ReloadPackage { package_name } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let running_entrypoints = daemon
                .entrypoint_supervisor()
                .snapshots()
                .into_iter()
                .filter(|snapshot| {
                    snapshot.package_name == package_name && snapshot.state == "running"
                })
                .map(|snapshot| snapshot.entrypoint_id)
                .collect::<Vec<_>>();
            let (candidate, decision) = daemon
                .package_registry()
                .refreshed_local_package(&package_name, "daemon socket reload local package")?;
            commit_package_registry(daemon, candidate)?;
            if decision.state == PackageState::Enabled {
                reload_package_after_reload(daemon, &package_name)?;
            }
            restart_running_package_entrypoints(daemon, &package_name, &running_entrypoints)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::RefreshLocalPackages => {
            let before_session_types = session_type_definition_map(daemon)?;
            let response = refresh_local_packages_response(daemon)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            Ok(response)
        }
        DaemonRequest::EnablePackageLocalPath { path } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let package_name = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "daemon socket enable local package")?;
                record.manifest.name.clone()
            };
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable local package")?;
            persist_package_registry(daemon)?;
            load_package_after_enable(daemon, &package_name)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::EnablePackage { package_name } => {
            let before_session_types = session_type_definition_map(daemon)?;
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable package")?;
            persist_package_registry(daemon)?;
            load_package_after_enable(daemon, &package_name)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::DisablePackage { package_name } => {
            let before_session_types = session_type_definition_map(daemon)?;
            daemon.entrypoint_supervisor().stop_package(&package_name);
            let decision = daemon
                .package_registry_mut()
                .disable(&package_name, "daemon socket disable package")?;
            persist_package_registry(daemon)?;
            unload_package_after_disable(daemon, &package_name)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::RemovePackage { package_name } => {
            let before_session_types = session_type_definition_map(daemon)?;
            daemon.entrypoint_supervisor().stop_package(&package_name);
            unload_package_after_disable(daemon, &package_name)?;
            let decision = daemon
                .package_registry_mut()
                .remove(&package_name, "daemon socket remove package")?;
            persist_package_registry(daemon)?;
            advance_session_type_generation_if_changed(daemon, &before_session_types)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::StartPackageEntrypoint {
            package_name,
            entrypoint_id,
            environment_overrides,
        } => {
            let config = daemon
                .runtime()
                .ok_or(DaemonTransportError::DaemonNotRunning)?
                .config()
                .clone();
            let packages = daemon.package_registry().clone();
            let launch = supervised_launch_contract(
                &config,
                &packages,
                &package_name,
                &entrypoint_id,
                &environment_overrides,
            )?;
            daemon.entrypoint_supervisor().start(
                &packages,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name,
            entrypoint_id,
            origin,
        } => issue_local_webrtc_bootstrap_response(daemon, &package_name, &entrypoint_id, &origin),
        DaemonRequest::LocalWebrtcSignal {
            grant_id,
            grant_secret,
            origin,
            offer,
        } => {
            let signal = LocalWebrtcSignalRequest {
                grant_id,
                grant_secret,
                origin,
                offer,
            };
            let answer = daemon.local_webrtc().signal(signal, control_tx.clone())?;
            Ok(daemon_local_webrtc_answer(answer))
        }
        DaemonRequest::StopPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .stop(&package_name, &entrypoint_id);
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::RestartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let config = daemon
                .runtime()
                .ok_or(DaemonTransportError::DaemonNotRunning)?
                .config()
                .clone();
            let packages = daemon.package_registry().clone();
            let launch = supervised_launch_contract(
                &config,
                &packages,
                &package_name,
                &entrypoint_id,
                &BTreeMap::new(),
            )?;
            daemon.entrypoint_supervisor().restart(
                &packages,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::PackageEntrypointStatus {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .status(&package_name, &entrypoint_id);
            show_package_response(daemon, &package_name)
        }
        other => handle_runtime_control_request(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability.egress,
            observability.lifecycle,
            other,
        ),
    }
}

fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    egress_diagnostics: &DaemonEgressDiagnostics,
    lifecycle_counters: &DaemonLifecycleCounters,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            Err(DaemonTransportError::Protocol(
                "entity subscriptions require the held-open stream handler",
            ))
        }
        DaemonRequest::RemoveSession { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::RemoveSession {
                    request_id: request_id("daemon-session-remove"),
                    session_id: SessionId(session_id.clone()),
                },
            )?;
            let HubClientResponseBody::SessionRemoved(removed) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            if !removed {
                return Ok(entity_subscription_error(
                    "session_not_terminal",
                    "daemon-session-remove",
                    "session must be terminal before it can be removed",
                ));
            }
            Ok(daemon_response_base(DaemonResponseKind::SessionRemoved))
        }
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
            Ok(daemon_status(
                status,
                client_status.session_count,
                egress_diagnostics.diagnostics(),
                lifecycle_counters.clone(),
            ))
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
            Ok(daemon_sessions(sessions))
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
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
                events_from_client(spawned.events),
            ))
        }
        DaemonRequest::Attach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let tracked_session_id = session_id.clone();
            let tracked_subscription_id = subscription_id.clone();
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
            pending_runtime
                .active_subscriptions
                .entry(tracked_session_id)
                .or_default()
                .insert(tracked_subscription_id);
            events_response(response.body)
        }
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let tracked_session_id = session_id.clone();
            let tracked_subscription_id = subscription_id.clone();
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
            if let Some(subscriptions) = pending_runtime
                .active_subscriptions
                .get_mut(&tracked_session_id)
            {
                subscriptions.remove(&tracked_subscription_id);
                if subscriptions.is_empty() {
                    pending_runtime
                        .active_subscriptions
                        .remove(&tracked_session_id);
                    pending_runtime.events.remove(&tracked_session_id);
                }
            }
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
                    return Ok(daemon_session_cleanup(cleanup));
                }
                ShutdownSessionClassification::Missing => {
                    return Ok(daemon_unknown_session_cleanup(&session_id));
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
                        return Ok(daemon_session_cleanup(DaemonSessionCleanup {
                            session_id: session_id.clone(),
                            outcome: "already_exited".to_string(),
                        }));
                    }
                    return match classify_shutdown_session(runtime, &session_id)? {
                        ShutdownSessionClassification::Cleanup(cleanup) => {
                            Ok(daemon_session_cleanup(cleanup))
                        }
                        ShutdownSessionClassification::Missing => {
                            Ok(daemon_unknown_session_cleanup(&session_id))
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
                    session_id: SessionId(session_id.clone()),
                    last_output_at: *cursor,
                },
            )?;
            let mut response = events_response(response.body)?;
            if let Some(pending) = pending_runtime.events.remove(&session_id) {
                let mut pending = pending
                    .into_iter()
                    .map(daemon_event_from_client)
                    .collect::<Vec<_>>();
                pending.extend(response.events);
                response.events = pending;
            }
            if !response.events.is_empty() {
                *cursor = tick(logical_clock);
            }
            Ok(response)
        }
        DaemonRequest::ReadScreen { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("daemon-sessions-read-screen"),
                    session_id: SessionId(session_id),
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::ReadScreen(screen) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_read_screen(screen))
        }
        DaemonRequest::ReadModeFlags { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadModeFlags {
                    request_id: request_id("daemon-sessions-read-mode-flags"),
                    session_id: SessionId(session_id),
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::ModeFlags(mode_flags) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_mode_flags(mode_flags))
        }
        DaemonRequest::CaptureSnapshot { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::CaptureSnapshot {
                    request_id: request_id("daemon-sessions-capture-snapshot"),
                    session_id: SessionId(session_id),
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::CaptureSnapshot(snapshot) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_capture_snapshot(snapshot))
        }
        DaemonRequest::ListSessionTypes => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessionTypes {
                    request_id: request_id("daemon-session-types-list"),
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ListSessionTypesForTarget { target_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessionTypesForTarget {
                    request_id: request_id("daemon-session-types-list-for-target"),
                    target_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionType { session_type_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ShowSessionType {
                    request_id: request_id("daemon-session-types-show"),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionTypeDefinition { session_type_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ShowSessionTypeDefinition {
                    request_id: request_id("daemon-session-types-definition"),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypeDefinition(definition) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_type_definition(*definition))
        }
        DaemonRequest::CreateSessionType { source, definition } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::CreateSessionType {
                    request_id: request_id("daemon-session-types-create"),
                    source: session_type_mutation_source_from_daemon(source),
                    definition: session_type_definition_from_daemon(definition),
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::UpdateSessionType { source, definition } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::UpdateSessionType {
                    request_id: request_id("daemon-session-types-update"),
                    source: session_type_mutation_source_from_daemon(source),
                    definition: session_type_definition_from_daemon(definition),
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::DeleteSessionType {
            source,
            session_type_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DeleteSessionType {
                    request_id: request_id("daemon-session-types-delete"),
                    source: session_type_mutation_source_from_daemon(source),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::ResolveSessionType {
            session_type_id,
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ResolveSessionType {
                    request_id: request_id("daemon-session-types-resolve"),
                    session_type_id,
                    session_type_request: session_type_request_from_daemon(None, request),
                },
            )?;
            let HubClientResponseBody::ResolvedSessionType(resolved) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_resolved_session_type(*resolved))
        }
        DaemonRequest::SpawnSessionType {
            session_type_id,
            session_id,
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::SpawnSessionType {
                    request_id: request_id("daemon-session-types-spawn"),
                    session_type_id,
                    session_type_request: session_type_request_from_daemon(
                        Some(SessionId(session_id)),
                        request,
                    ),
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::Spawned(spawned) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            drain_cursors.insert(spawned.session.session_id.0.clone(), *logical_clock);
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
                events_from_client(spawned.events),
            ))
        }
        DaemonRequest::ReadSessionContext {
            session_id,
            context_id,
            key,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadSessionContext {
                    request_id: request_id("daemon-session-context-read"),
                    session_id: SessionId(session_id),
                    context_id,
                    key,
                },
            )?;
            let HubClientResponseBody::SessionContext(context) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_context(context))
        }
        DaemonRequest::Whoami { caller_session_id } => Ok(daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "botster-hub-daemon-socket".to_string(),
                role: "local_operator".to_string(),
                identity_source: if caller_session_id.is_some() {
                    "BOTSTER_SESSION_UUID".to_string()
                } else {
                    "local_operator".to_string()
                },
                caller_session_id,
                host_id: status.host_id.clone(),
                host_display_name: status.host_display_name.clone(),
            }),
        )),
        DaemonRequest::PostMessage {
            caller_session_id,
            target_session_id,
            envelope_id,
            body,
        } => {
            let now = tick(logical_clock);
            let envelope = RoutedEnvelope::new(
                EnvelopeId(
                    envelope_id
                        .unwrap_or_else(|| format!("hub-message-{}-{now}", target_session_id)),
                ),
                EndpointId(
                    caller_session_id
                        .map(|session_id| format!("session:{session_id}"))
                        .unwrap_or_else(|| "botster-hub-mcp".to_string()),
                ),
                vec![EnvelopeTarget::Session {
                    session_id: SessionId(target_session_id),
                }],
                RoutedEnvelopePayload {
                    content_type: MESSAGE_CONTENT_TYPE.to_string(),
                    body: body.into_bytes(),
                    extension: None,
                },
                now,
            );
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PublishRoutedEnvelope {
                    request_id: request_id("daemon-mcp-post-message"),
                    envelope,
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopePublish(publish) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessagePosted,
                daemon_coordination_publish(publish.deliveries),
            ))
        }
        DaemonRequest::ReceiveMessages {
            caller_session_id,
            after,
            limit,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DrainRoutedEnvelopes {
                    request_id: request_id("daemon-mcp-receive-messages"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    after: after.map(EnvelopeCursor),
                    limit: limit.clamp(1, 128),
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopeDrain(drain) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::Messages,
                daemon_coordination_messages(drain.envelopes, drain.next_cursor),
            ))
        }
        DaemonRequest::AckMessage {
            caller_session_id,
            envelope_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::AcknowledgeRoutedEnvelope {
                    request_id: request_id("daemon-mcp-ack-message"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    envelope_id: EnvelopeId(envelope_id),
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopeAck(ack) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessageAcked,
                daemon_coordination_ack(ack.state),
            ))
        }
        DaemonRequest::NotifySession { session_id, data } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::NotifySession {
                    request_id: request_id("daemon-mcp-notify-session"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    readiness: ReadinessEvidence::default(),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::GuardedWrite(write) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::SessionNotified,
                daemon_coordination_notify(write.decision, write.states),
            ))
        }
        DaemonRequest::PluginMcpListTools => {
            Ok(daemon_plugin_tools(runtime.list_plugin_mcp_tools()))
        }
        DaemonRequest::PluginMcpCallTool { name, arguments } => {
            match runtime.call_plugin_mcp_tool(crate::McpCallRequest { name, arguments }) {
                Ok(result) => Ok(daemon_plugin_tool_result(result)),
                Err(error) => Ok(daemon_plugin_tool_error(error)),
            }
        }
        DaemonRequest::PluginSurfaceRender {
            package_name,
            surface_id,
            payload,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PluginSurfaceRender {
                    request_id: request_id("daemon-plugin-surface-render"),
                    package_name,
                    surface_id,
                    payload,
                },
            )?;
            let HubClientResponseBody::PluginSurface(surface) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_plugin_surface(surface))
        }
        DaemonRequest::PluginSurfaceAction {
            package_name,
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PluginSurfaceAction {
                    request_id: request_id("daemon-plugin-surface-action"),
                    package_name,
                    action: request,
                },
            )?;
            let HubClientResponseBody::PluginActionResult(result) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_plugin_action_result(result))
        }
        DaemonRequest::DaemonShutdown => Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            status: Some(daemon_status_from_status(
                &status,
                runtime
                    .list_sessions()
                    .map_err(crate::HubRuntimeError::from)?
                    .len(),
                Vec::new(),
                lifecycle_counters.clone(),
            )),
            sessions: Vec::new(),
            session_types: Vec::new(),
            session_type_definition: None,
            resolved_session_type: None,
            session_context: None,
            read_screen: None,
            mode_flags: None,
            capture_snapshot: None,
            spawn_targets: Vec::new(),
            spawn_target_validation: None,
            worktrees: Vec::new(),
            apps: Vec::new(),
            resolved_app_launch: None,
            resolved_package_route: None,
            package_navigation: Vec::new(),
            packages: Vec::new(),
            available_packages: Vec::new(),
            install_plan: None,
            update_status: None,
            hub_update: None,
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_worker_counters: None,
            plugin_resource_counters: None,
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            plugin_surface: None,
            plugin_action_result: None,
            local_webrtc_bootstrap: None,
            local_webrtc_answer: None,
            events: Vec::new(),
            cleanup: None,
            coordination: None,
            error: None,
            diagnostics: vec![DaemonDiagnostic::connected("shutdown")],
        }),
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => Err(DaemonTransportError::UnexpectedResponse),
        DaemonRequest::CheckHubUpdate => {
            unreachable!("Hub update checks are handled before runtime borrow")
        }
        DaemonRequest::ListApps
        | DaemonRequest::ResolveAppLaunch { .. }
        | DaemonRequest::ResolvePackageRoute { .. }
        | DaemonRequest::ListPackageNavigation
        | DaemonRequest::ListPackages
        | DaemonRequest::ListSpawnTargets
        | DaemonRequest::ShowSpawnTarget { .. }
        | DaemonRequest::CreateSpawnTarget { .. }
        | DaemonRequest::UpdateSpawnTarget { .. }
        | DaemonRequest::DeleteSpawnTarget { .. }
        | DaemonRequest::ValidateSpawnTarget { .. }
        | DaemonRequest::ListWorktrees
        | DaemonRequest::ShowWorktree { .. }
        | DaemonRequest::CreateWorktree { .. }
        | DaemonRequest::DeleteWorktree { .. }
        | DaemonRequest::ListAvailablePackages { .. }
        | DaemonRequest::InspectAvailablePackage { .. }
        | DaemonRequest::PreviewPackageInstall { .. }
        | DaemonRequest::InstallPackageRegistryEntry { .. }
        | DaemonRequest::InstallPackageLocalPath { .. }
        | DaemonRequest::CheckPackageUpdate { .. }
        | DaemonRequest::PreviewPackageUpdate { .. }
        | DaemonRequest::ApplyPackageUpdate { .. }
        | DaemonRequest::ShowPackage { .. }
        | DaemonRequest::SetPackageConfiguration { .. }
        | DaemonRequest::ReloadPackage { .. }
        | DaemonRequest::RefreshLocalPackages
        | DaemonRequest::PluginLifecycleStatus
        | DaemonRequest::EnablePackageLocalPath { .. }
        | DaemonRequest::EnablePackage { .. }
        | DaemonRequest::DisablePackage { .. }
        | DaemonRequest::RemovePackage { .. }
        | DaemonRequest::StartPackageEntrypoint { .. }
        | DaemonRequest::StopPackageEntrypoint { .. }
        | DaemonRequest::RestartPackageEntrypoint { .. }
        | DaemonRequest::PackageEntrypointStatus { .. } => {
            unreachable!("package requests are handled before runtime borrow")
        }
    }
}

fn load_package_after_enable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let package_registry = daemon.package_registry().clone();
    let prepared = package_registry.prepare_local_package(
        package_name,
        "daemon socket load enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .load_lua_plugin_package(&package_registry, package_name)
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn reload_package_after_reload(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let package_registry = daemon.package_registry().clone();
    let prepared = package_registry.prepare_local_package(
        package_name,
        "daemon socket reload enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .reload_lua_plugin_package(
                request_id(&format!("daemon-reload-{package_name}")),
                &package_registry,
                package_name,
            )
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn unload_package_after_disable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let _ = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .unload_plugin_package(
            request_id(&format!("daemon-disable-{package_name}")),
            package_name,
        );
    Ok(())
}

fn restart_running_package_entrypoints(
    daemon: &mut HubDaemon,
    package_name: &str,
    entrypoint_ids: &[String],
) -> DaemonTransportResult<()> {
    if entrypoint_ids.is_empty() {
        return Ok(());
    }
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    let packages = daemon.package_registry().clone();
    for entrypoint_id in entrypoint_ids {
        let environment = daemon
            .entrypoint_supervisor()
            .launch_environment(package_name, entrypoint_id);
        let launch = supervised_launch_contract(
            &config,
            &packages,
            package_name,
            entrypoint_id,
            &environment,
        )?;
        daemon.entrypoint_supervisor().restart(
            &packages,
            package_name,
            entrypoint_id,
            &launch.args,
            &launch.environment,
        )?;
    }
    Ok(())
}

fn refresh_local_packages_response(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<DaemonResponse> {
    let previous_packages = daemon.package_registry().clone();
    let running_entrypoints = daemon
        .entrypoint_supervisor()
        .snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.state == "running")
        .fold(
            BTreeMap::<String, Vec<String>>::new(),
            |mut running, snapshot| {
                running
                    .entry(snapshot.package_name)
                    .or_default()
                    .push(snapshot.entrypoint_id);
                running
            },
        );
    let (candidate, decisions) = daemon
        .package_registry()
        .refreshed_local_packages("daemon socket refresh local package registrations")?;
    commit_package_registry(daemon, candidate)?;

    for decision in &decisions {
        if decision.state == PackageState::Enabled {
            reload_package_after_reload(daemon, &decision.package_name)?;
        }
        if let Some(entrypoint_ids) = running_entrypoints.get(&decision.package_name) {
            let changed_entrypoint_ids = entrypoint_ids
                .iter()
                .filter(|entrypoint_id| {
                    runnable_entrypoint_definition_changed(
                        &previous_packages,
                        daemon.package_registry(),
                        &decision.package_name,
                        entrypoint_id,
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            restart_running_package_entrypoints(
                daemon,
                &decision.package_name,
                &changed_entrypoint_ids,
            )?;
        }
    }

    list_packages_response(daemon)
}

fn runnable_entrypoint_definition_changed(
    previous_packages: &PackageRegistry,
    refreshed_packages: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
) -> bool {
    let Some(previous) = previous_packages.package(package_name) else {
        return true;
    };
    let Some(refreshed) = refreshed_packages.package(package_name) else {
        return true;
    };
    let previous_entrypoint = previous
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);
    let refreshed_entrypoint = refreshed
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);

    previous.manifest != refreshed.manifest || previous_entrypoint != refreshed_entrypoint
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
    let HubClientResponseBody::Packages(mut packages) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(&mut packages, snapshots);
    Ok(daemon_packages(packages))
}

fn list_package_navigation_response(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackageNavigation {
            request_id: request_id("daemon-package-navigation-list"),
        },
    )?;
    let HubClientResponseBody::PackageNavigation(navigation) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    let packages = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    Ok(daemon_package_navigation(navigation, &packages))
}

fn list_apps_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry().clone();
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    Ok(daemon_apps(apps_from_registry(&registry, snapshots)))
}

fn resolve_package_route_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    route_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry();
    let Some(record) = registry.package(package_name) else {
        return Ok(daemon_package_route_error(
            package_name,
            route_id,
            "package_not_installed",
            "package is not installed",
        ));
    };
    let package = HubClientPackage::from_record(registry, record);
    let route = package_route_descriptors(&package)
        .into_iter()
        .find(|route| route.route_id == route_id);
    match route {
        Some(route) => Ok(daemon_resolved_package_route(route)),
        None => Ok(daemon_package_route_error(
            package_name,
            route_id,
            "route_not_found",
            "package route is not declared",
        )),
    }
}

fn supervised_launch_contract(
    config: &HubConfig,
    registry: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
    environment_overrides: &BTreeMap<String, String>,
) -> DaemonTransportResult<PackageResolvedEntrypointLaunch> {
    let socket = runtime_path(socket_path(config)?);
    let record = registry.package(package_name).ok_or_else(|| {
        DaemonTransportError::Entrypoint(EntrypointSupervisorError::PackageNotInstalled(
            package_name.to_string(),
        ))
    })?;
    if !matches!(record.state, PackageState::Enabled) {
        return Err(DaemonTransportError::Entrypoint(
            EntrypointSupervisorError::PackageDisabled(package_name.to_string()),
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Err(DaemonTransportError::Entrypoint(
            EntrypointSupervisorError::EntrypointNotFound {
                package_name: package_name.to_string(),
                entrypoint_id: entrypoint_id.to_string(),
            },
        ));
    };

    resolve_entrypoint_launch_contract(
        entrypoint,
        &runtime_path(config.data_directory.clone()),
        &socket,
        environment_overrides,
    )
    .map_err(|details| {
        DaemonTransportError::Entrypoint(EntrypointSupervisorError::LaunchContract {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
            details,
        })
    })
}

fn runtime_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn resolve_app_launch_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    entrypoint_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    let data_directory = runtime_path(config.data_directory.clone());
    let socket = runtime_path(socket_path(&config)?);
    let registry = daemon.package_registry();
    let Some(record) = registry.package(package_name) else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_installed",
            "package is not installed",
        ));
    };
    if !record.is_enabled() {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_enabled",
            "package is not enabled",
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "entrypoint_not_found",
            "entrypoint is not installed for package",
        ));
    };
    if !matches!(entrypoint.kind, RunnableEntrypointKind::TerminalApp) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_app_kind",
            "app is not a terminal_app",
        ));
    }
    if !matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::ForegroundStdio
    ) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_launch_mode",
            "terminal app must use foreground_stdio launch mode",
        ));
    }
    let launch =
        match resolve_foreground_launch_contract(record, entrypoint, &data_directory, &socket) {
            Ok(launch) => launch,
            Err(message) => {
                return Ok(daemon_app_launch_error(
                    package_name,
                    entrypoint_id,
                    "launch_contract_unavailable",
                    message,
                ));
            }
        };

    Ok(daemon_resolved_app_launch(DaemonResolvedAppLaunch {
        package_name: record.manifest.name.clone(),
        app_id: entrypoint.id.clone(),
        entrypoint_id: entrypoint.id.clone(),
        kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
        launch_mode: runnable_launch_mode_label(&entrypoint.launch_mode).to_string(),
        command: launch.command,
        args: launch.args,
        working_directory: launch.working_directory.display().to_string(),
        environment: launch.environment,
    }))
}

fn available_packages_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
) -> DaemonTransportResult<DaemonResponse> {
    let available = daemon
        .package_registry()
        .available_packages(&registry_path)?;
    Ok(daemon_available_packages(available, &registry_path))
}

fn inspect_available_package_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
    entry_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let available = daemon
        .package_registry()
        .inspect_available_package(&registry_path, entry_id)?;
    Ok(daemon_available_packages(vec![available], &registry_path))
}

fn preview_package_install_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
    entry_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let plan = daemon
        .package_registry()
        .preview_registry_install(registry_path, entry_id)?;
    Ok(daemon_package_install_plan(plan))
}

fn show_package_response(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry();
    let mut package = registry
        .package(package_name)
        .map(|record| HubClientPackage::from_record(registry, record))
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::Show,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket show package".to_string(),
            )
        })?;
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(std::slice::from_mut(&mut package), snapshots);
    Ok(daemon_packages(vec![package]))
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
    let HubClientResponseBody::PluginLifecycle(report) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_plugin_lifecycle(report))
}

fn package_decision_response(
    daemon: &mut HubDaemon,
    decision: PackageDecision,
) -> DaemonTransportResult<DaemonResponse> {
    let mut response = list_packages_response(daemon)?;
    response.kind = DaemonResponseKind::PackageDecision;
    response.package_decision = Some(daemon_package_decision_from_policy(decision));
    Ok(response)
}

fn apply_entrypoint_snapshots(
    packages: &mut [HubClientPackage],
    snapshots: Vec<EntrypointProcessSnapshot>,
) {
    for snapshot in snapshots {
        let Some(package) = packages
            .iter_mut()
            .find(|package| package.package_name == snapshot.package_name)
        else {
            continue;
        };
        let Some(entrypoint) = package
            .runnable_entrypoints
            .iter_mut()
            .find(|entrypoint| entrypoint.id == snapshot.entrypoint_id)
        else {
            continue;
        };
        entrypoint.process.state = snapshot.state;
        entrypoint.process.pid = snapshot.pid;
        entrypoint.process.started_at = snapshot.started_at;
        entrypoint.process.exited_at = snapshot.exited_at;
        entrypoint.process.exit_status = snapshot.exit_status;
        entrypoint.process.diagnostics = snapshot
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::HubClientPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect();
    }
}

fn apps_from_registry(
    registry: &PackageRegistry,
    snapshots: Vec<EntrypointProcessSnapshot>,
) -> Vec<DaemonApp> {
    let snapshots = snapshots
        .into_iter()
        .map(|snapshot| {
            (
                (
                    snapshot.package_name.clone(),
                    snapshot.entrypoint_id.clone(),
                ),
                snapshot,
            )
        })
        .collect::<BTreeMap<_, _>>();
    registry
        .packages()
        .into_iter()
        .flat_map(|record| apps_from_record(record, &snapshots))
        .collect()
}

fn apps_from_record(
    record: &crate::PackageRecord,
    snapshots: &BTreeMap<(String, String), EntrypointProcessSnapshot>,
) -> Vec<DaemonApp> {
    let package_state = package_state_label(record.state.into()).to_string();
    record
        .runnable_entrypoints
        .iter()
        .map(|entrypoint| {
            let snapshot = snapshots.get(&(record.manifest.name.clone(), entrypoint.id.clone()));
            let lifecycle_state = snapshot
                .and_then(|snapshot| snapshot.launch_result.as_ref())
                .map(|result| runnable_process_state_label(&result.process_state).to_string())
                .or_else(|| snapshot.map(|snapshot| snapshot.state.clone()))
                .unwrap_or_else(|| "not_started".to_string());
            let diagnostics: Vec<DaemonPackageDiagnostic> = snapshot
                .map(|snapshot| {
                    snapshot
                        .diagnostics
                        .iter()
                        .map(|diagnostic| DaemonPackageDiagnostic {
                            kind: diagnostic.kind.clone(),
                            message: diagnostic.message.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let blocked_reasons = app_blocked_reasons(&package_state, entrypoint);
            let actions = app_entrypoint_actions(
                &record.manifest.name,
                &package_state,
                &entrypoint.id,
                entrypoint,
                &lifecycle_state,
            );
            DaemonApp {
                package_name: record.manifest.name.clone(),
                app_id: entrypoint.id.clone(),
                entrypoint_id: entrypoint.id.clone(),
                kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
                launch_mode: runnable_launch_mode_label(&entrypoint.launch_mode).to_string(),
                lifecycle_state,
                diagnostics: diagnostics.clone(),
                actions,
                blocked_reasons: blocked_reasons.clone(),
                launch_target: DaemonAppLaunchTarget {
                    kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
                    local_url: app_local_url(entrypoint, snapshot),
                },
                route: Some(app_entrypoint_route_descriptor(
                    record,
                    entrypoint,
                    &package_state,
                    blocked_reasons,
                    diagnostics.clone(),
                )),
            }
        })
        .collect()
}

fn app_blocked_reasons(
    package_state: &str,
    entrypoint: &crate::PackageRunnableEntrypoint,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if package_state != "enabled" {
        reasons.push("package_not_enabled".to_string());
    }
    if matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::Background
    ) && !entrypoint.may_supervise
    {
        reasons.push("entrypoint_not_supervisable".to_string());
    }
    let supported = matches!(
        (&entrypoint.kind, &entrypoint.launch_mode),
        (
            RunnableEntrypointKind::WebApp,
            RunnableEntrypointLaunchMode::Background
        ) | (
            RunnableEntrypointKind::TerminalApp,
            RunnableEntrypointLaunchMode::ForegroundStdio
        )
    );
    if !supported {
        reasons.push("unsupported_launch_mode".to_string());
    }
    reasons
}

fn app_entrypoint_actions(
    package_name: &str,
    package_state: &str,
    entrypoint_id: &str,
    entrypoint: &crate::PackageRunnableEntrypoint,
    lifecycle_state: &str,
) -> Vec<DaemonPackageActionState> {
    if !matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::Background
    ) {
        return Vec::new();
    }
    if !entrypoint.may_supervise {
        return vec![
            unavailable_action(
                "start_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
            unavailable_action(
                "stop_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
            unavailable_action(
                "restart_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
        ];
    }
    if package_state != "enabled" {
        return vec![
            blocked_action(
                "start_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "stop_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "restart_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
        ];
    }
    let running = lifecycle_state == "running";
    let mut actions = Vec::new();
    if running {
        actions.push(unavailable_action(
            "start_package_entrypoint",
            "already_running",
            "entrypoint is already running",
        ));
        actions.push(available_package_action(
            "stop_package_entrypoint",
            request_for_entrypoint("stop_package_entrypoint", package_name, entrypoint_id),
        ));
    } else {
        actions.push(available_package_action(
            "start_package_entrypoint",
            request_for_entrypoint("start_package_entrypoint", package_name, entrypoint_id),
        ));
        actions.push(unavailable_action(
            "stop_package_entrypoint",
            "not_running",
            "entrypoint is not running",
        ));
    }
    actions.push(available_package_action(
        "restart_package_entrypoint",
        request_for_entrypoint("restart_package_entrypoint", package_name, entrypoint_id),
    ));
    actions
}

fn app_local_url(
    entrypoint: &crate::PackageRunnableEntrypoint,
    snapshot: Option<&EntrypointProcessSnapshot>,
) -> Option<String> {
    let declares_local_url = entrypoint.readiness.as_ref().is_some_and(|readiness| {
        readiness
            .result_fields
            .iter()
            .any(|field| matches!(field, RunnableEntrypointResultField::LocalUrl))
    });
    if !declares_local_url {
        return None;
    }
    snapshot
        .and_then(|snapshot| snapshot.launch_result.as_ref())
        .and_then(|result| result.local_url.clone())
}

fn package_route_descriptors(package: &HubClientPackage) -> Vec<DaemonPackageRouteDescriptor> {
    let package_state = package_state_label(package.state).to_string();
    let supports_settings = package.configuration.schema.is_some();
    let mut routes = package
        .surfaces
        .iter()
        .map(|surface| {
            plugin_surface_route_descriptor(
                &package.package_name,
                &package_state,
                &package.requested_capabilities,
                surface,
                supports_settings,
            )
        })
        .collect::<Vec<_>>();
    routes.extend(package.runnable_entrypoints.iter().map(|entrypoint| {
        client_entrypoint_route_descriptor(
            &package.package_name,
            &package_state,
            entrypoint,
            supports_settings,
        )
    }));
    if supports_settings {
        routes.push(settings_route_descriptor(
            &package.package_name,
            &package_state,
            &package.configuration,
        ));
    }
    routes
}

fn package_navigation_entries(
    navigation: Vec<HubClientPackageNavigationEntry>,
    packages: &[HubClientPackage],
) -> Vec<DaemonPackageNavigationEntry> {
    navigation
        .into_iter()
        .map(|entry| package_navigation_entry(entry, packages))
        .collect()
}

fn package_navigation_entry(
    entry: HubClientPackageNavigationEntry,
    packages: &[HubClientPackage],
) -> DaemonPackageNavigationEntry {
    let (route_id, source) = match &entry.target {
        HubClientPackageNavigationTarget::Surface { surface_id } => (
            surface_route_id(surface_id),
            DaemonPackageNavigationSource {
                kind: "surface".to_string(),
                surface_id: Some(surface_id.clone()),
                entrypoint_id: None,
            },
        ),
    };
    let route = packages
        .iter()
        .find(|package| package.package_name == entry.package_name)
        .and_then(|package| {
            package_route_descriptors(package)
                .into_iter()
                .find(|route| route.route_id == route_id)
        });

    match route {
        Some(route) => DaemonPackageNavigationEntry {
            package_name: entry.package_name,
            item_id: entry.item_id,
            label: entry.label,
            icon: entry.icon,
            description: entry.description,
            route_id: route.route_id,
            route_path: route.route_path,
            target: route.target,
            source,
            enabled: route.enabled,
            blocked: route.blocked,
            diagnostics: route.diagnostics,
        },
        None => DaemonPackageNavigationEntry {
            package_name: entry.package_name.clone(),
            item_id: entry.item_id,
            label: entry.label,
            icon: entry.icon,
            description: entry.description,
            route_id,
            route_path: String::new(),
            target: match entry.target {
                HubClientPackageNavigationTarget::Surface { surface_id } => {
                    DaemonPackageRouteTarget {
                        kind: "plugin_surface".to_string(),
                        entrypoint_id: None,
                        surface_id: Some(surface_id),
                    }
                }
            },
            source,
            enabled: false,
            blocked: true,
            diagnostics: vec![DaemonPackageDiagnostic {
                kind: "navigation_target_not_found".to_string(),
                message: "navigation target route is not declared".to_string(),
            }],
        },
    }
}

fn plugin_surface_route_descriptor(
    package_name: &str,
    package_state: &str,
    requested_capabilities: &[crate::HubClientCapability],
    surface: &PackageSurfaceDescriptor,
    supports_settings: bool,
) -> DaemonPackageRouteDescriptor {
    let diagnostics = route_state_diagnostics(package_state);
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: surface_route_id(&surface.id),
        route_path: surface_route_path(package_name, &surface.id),
        target: DaemonPackageRouteTarget {
            kind: "plugin_surface".to_string(),
            entrypoint_id: None,
            surface_id: Some(surface.id.clone()),
        },
        title: surface.title.clone(),
        label: surface.title.clone(),
        app_id: (surface.kind == PackageSurfaceKind::App).then(|| surface.id.clone()),
        surface_id: Some(surface.id.clone()),
        icon: surface.icon.clone(),
        category: surface.category.clone(),
        layout_mode: "plugin_surface".to_string(),
        required_capabilities: requested_capabilities
            .iter()
            .filter(|capability| capability.surface.eq_ignore_ascii_case("surfaces"))
            .map(daemon_capability_from_client)
            .collect(),
        enabled: package_state == "enabled",
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings,
    }
}

fn client_entrypoint_route_descriptor(
    package_name: &str,
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
    supports_settings: bool,
) -> DaemonPackageRouteDescriptor {
    let mut diagnostics = route_state_diagnostics(package_state);
    diagnostics.extend(
        client_app_blocked_reasons(package_state, entrypoint)
            .into_iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason,
                message: format!("{package_name}/{} cannot be opened", entrypoint.id),
            }),
    );
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: app_route_id(&entrypoint.id),
        route_path: app_route_path(package_name, &entrypoint.id),
        target: DaemonPackageRouteTarget {
            kind: "app_entrypoint".to_string(),
            entrypoint_id: Some(entrypoint.id.clone()),
            surface_id: None,
        },
        title: entrypoint.id.clone(),
        label: entrypoint.id.clone(),
        app_id: Some(entrypoint.id.clone()),
        surface_id: None,
        icon: None,
        category: Some("apps".to_string()),
        layout_mode: "app_entrypoint".to_string(),
        required_capabilities: entrypoint
            .capabilities
            .iter()
            .map(daemon_capability_from_client)
            .collect(),
        enabled: package_state == "enabled" && diagnostics.is_empty(),
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings,
    }
}

fn client_app_blocked_reasons(
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if package_state != "enabled" {
        reasons.push("package_not_enabled".to_string());
    }
    if entrypoint.launch_mode == "background" && !entrypoint.may_supervise {
        reasons.push("entrypoint_not_supervisable".to_string());
    }
    let supported = (entrypoint.kind == "web_app" && entrypoint.launch_mode == "background")
        || (entrypoint.kind == "terminal_app" && entrypoint.launch_mode == "foreground_stdio");
    if !supported {
        reasons.push("unsupported_launch_mode".to_string());
    }
    reasons
}

fn app_entrypoint_route_descriptor(
    record: &crate::PackageRecord,
    entrypoint: &crate::PackageRunnableEntrypoint,
    package_state: &str,
    blocked_reasons: Vec<String>,
    mut diagnostics: Vec<DaemonPackageDiagnostic>,
) -> DaemonPackageRouteDescriptor {
    diagnostics.extend(
        blocked_reasons
            .iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason.clone(),
                message: format!("{} cannot be opened", entrypoint.id),
            }),
    );
    DaemonPackageRouteDescriptor {
        package_name: record.manifest.name.clone(),
        route_id: app_route_id(&entrypoint.id),
        route_path: app_route_path(&record.manifest.name, &entrypoint.id),
        target: DaemonPackageRouteTarget {
            kind: "app_entrypoint".to_string(),
            entrypoint_id: Some(entrypoint.id.clone()),
            surface_id: None,
        },
        title: entrypoint.id.clone(),
        label: entrypoint.id.clone(),
        app_id: Some(entrypoint.id.clone()),
        surface_id: None,
        icon: None,
        category: Some("apps".to_string()),
        layout_mode: "app_entrypoint".to_string(),
        required_capabilities: entrypoint
            .capabilities
            .iter()
            .map(|capability| DaemonCapability {
                surface: core_capability_surface_label(&capability.surface).to_string(),
                scope: capability.scope.clone(),
            })
            .collect(),
        enabled: package_state == "enabled" && diagnostics.is_empty(),
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings: record.configuration_view().schema.is_some(),
    }
}

fn settings_route_descriptor(
    package_name: &str,
    package_state: &str,
    configuration: &crate::HubClientPackageConfiguration,
) -> DaemonPackageRouteDescriptor {
    let mut diagnostics = route_state_diagnostics(package_state);
    diagnostics.extend(configuration.diagnostics.iter().map(|diagnostic| {
        DaemonPackageDiagnostic {
            kind: diagnostic.kind.clone(),
            message: diagnostic.message.clone(),
        }
    }));
    for key in &configuration.missing_required {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "missing_required_configuration".to_string(),
            message: format!("configuration field {key} is required"),
        });
    }
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: "settings".to_string(),
        route_path: settings_route_path(package_name),
        target: DaemonPackageRouteTarget {
            kind: "package_settings".to_string(),
            entrypoint_id: None,
            surface_id: None,
        },
        title: "Settings".to_string(),
        label: "Settings".to_string(),
        app_id: None,
        surface_id: None,
        icon: Some("settings".to_string()),
        category: Some("settings".to_string()),
        layout_mode: "settings_form".to_string(),
        required_capabilities: Vec::new(),
        enabled: true,
        blocked: false,
        diagnostics,
        supports_settings: true,
    }
}

fn route_state_diagnostics(package_state: &str) -> Vec<DaemonPackageDiagnostic> {
    if package_state == "enabled" {
        Vec::new()
    } else {
        vec![DaemonPackageDiagnostic {
            kind: "package_not_enabled".to_string(),
            message: "package is not enabled".to_string(),
        }]
    }
}

fn daemon_capability_from_client(capability: &crate::HubClientCapability) -> DaemonCapability {
    DaemonCapability {
        surface: capability.surface.clone(),
        scope: capability.scope.clone(),
    }
}

fn core_capability_surface_label(surface: &botster_core::CapabilitySurface) -> &'static str {
    match surface {
        botster_core::CapabilitySurface::ClientAdmission => "ClientAdmission",
        botster_core::CapabilitySurface::PairingInvites => "PairingInvites",
        botster_core::CapabilitySurface::SignalingRelay => "SignalingRelay",
        botster_core::CapabilitySurface::HubPresence => "HubPresence",
        botster_core::CapabilitySurface::BrowserShell => "BrowserShell",
        botster_core::CapabilitySurface::Secrets => "Secrets",
        botster_core::CapabilitySurface::Crypto => "Crypto",
        botster_core::CapabilitySurface::Network => "Network",
        botster_core::CapabilitySurface::Surfaces => "Surfaces",
        botster_core::CapabilitySurface::SessionActions => "SessionActions",
        botster_core::CapabilitySurface::Mcp => "Mcp",
        botster_core::CapabilitySurface::PluginDb => "PluginDb",
        botster_core::CapabilitySurface::Filesystem => "Filesystem",
        botster_core::CapabilitySurface::Timers => "Timers",
    }
}

fn surface_route_id(surface_id: &str) -> String {
    format!("surface:{surface_id}")
}

fn app_route_id(entrypoint_id: &str) -> String {
    format!("app:{entrypoint_id}")
}

fn surface_route_path(package_name: &str, surface_id: &str) -> String {
    format!("/packages/{package_name}/surfaces/{surface_id}")
}

fn app_route_path(package_name: &str, entrypoint_id: &str) -> String {
    format!("/packages/{package_name}/apps/{entrypoint_id}")
}

fn settings_route_path(package_name: &str) -> String {
    format!("/packages/{package_name}/settings")
}

fn issue_local_webrtc_bootstrap_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    entrypoint_id: &str,
    origin: &str,
) -> DaemonTransportResult<DaemonResponse> {
    if package_name != "botster-web" || entrypoint_id != "web-client" {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_unsupported_entrypoint",
            "local WebRTC page-load bootstrap is only supported for botster-web/web-client",
        ));
    }

    let packages = daemon.package_registry().clone();
    let Some(record) = packages.package(package_name) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_package_not_installed",
            format!("package {package_name} is not installed"),
        ));
    };
    if !record.is_enabled() {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_package_disabled",
            format!("package {package_name} is not enabled"),
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_entrypoint_not_found",
            format!("entrypoint {entrypoint_id} was not found for package {package_name}"),
        ));
    };

    let snapshot = daemon
        .entrypoint_supervisor()
        .status(package_name, entrypoint_id);
    if snapshot.state != "running" {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_entrypoint_not_running",
            format!("entrypoint {package_name}/{entrypoint_id} is not running"),
        ));
    }

    let Some(local_url) = app_local_url(entrypoint, Some(&snapshot)) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_local_url_unavailable",
            format!("entrypoint {package_name}/{entrypoint_id} has no structured local_url"),
        ));
    };
    let Some(expected_origin) = origin_from_local_url(&local_url) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_invalid_local_url",
            format!("entrypoint {package_name}/{entrypoint_id} local_url has no origin"),
        ));
    };
    if origin != expected_origin {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_origin_mismatch",
            "requested origin does not match running entrypoint local_url origin",
        ));
    }

    let bootstrap =
        daemon
            .local_webrtc()
            .issue_bootstrap(package_name, entrypoint_id, &expected_origin)?;
    Ok(daemon_local_webrtc_bootstrap(bootstrap))
}

fn origin_from_local_url(local_url: &str) -> Option<String> {
    let scheme_end = local_url.find("://")?;
    let after_scheme = scheme_end + 3;
    let authority_end = local_url[after_scheme..]
        .find(['/', '?', '#'])
        .map(|index| after_scheme + index)
        .unwrap_or(local_url.len());
    if authority_end == after_scheme {
        return None;
    }
    Some(local_url[..authority_end].to_string())
}

fn persist_package_registry(daemon: &mut HubDaemon) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = daemon.package_registry().snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    daemon.replace_state(state);
    Ok(())
}

fn commit_package_registry(
    daemon: &mut HubDaemon,
    package_registry: PackageRegistry,
) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = package_registry.snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    daemon.replace_package_registry(package_registry);
    daemon.replace_state(state);
    Ok(())
}

fn persist_spawn_targets(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<SpawnTarget> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let target = update(&mut state.spawn_targets);
        changed = Some(target);
    })?;
    let target = changed
        .expect("spawn target update closure always runs")
        .map_err(DaemonTransportError::SpawnTarget)?;
    daemon.replace_state(state);
    Ok(target)
}

fn persist_spawn_targets_with_worktrees(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>, &[Worktree]) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<SpawnTarget> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let worktrees = state.worktrees.clone();
        changed = Some(update(&mut state.spawn_targets, &worktrees));
    })?;
    let target = changed
        .expect("spawn target update closure always runs")
        .map_err(DaemonTransportError::SpawnTarget)?;
    daemon.replace_state(state);
    Ok(target)
}

fn persist_worktrees(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<Worktree>, &[SpawnTarget]) -> crate::WorktreeResult<Worktree>,
) -> DaemonTransportResult<Worktree> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let targets = state.spawn_targets.clone();
        let worktree = update(&mut state.worktrees, &targets);
        changed = Some(worktree);
    })?;
    let worktree = changed
        .expect("worktree update closure always runs")
        .map_err(DaemonTransportError::Worktree)?;
    daemon.replace_state(state);
    Ok(worktree)
}

fn events_response(body: HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> {
    let HubClientResponseBody::Events(events) = body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_events(events_from_client(events)))
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

fn daemon_endpoint(config: &HubConfig) -> DaemonTransportResult<DaemonEndpoint> {
    socket_path(config).map(DaemonEndpoint::new)
}

fn install_signal_forwarder(control_tx: ControlSender) -> DaemonTransportResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(DaemonTransportError::Io)?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            let (reply_tx, _reply_rx) = oneshot::channel();
            let _ = control_tx.blocking_send(ControlMessage::Request {
                request: Box::new(DaemonRequest::DaemonShutdown),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
            });
        }
    });
    Ok(())
}

fn prepare_socket_path(path: &PathBuf) -> DaemonTransportResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    }
    match UnixStream::connect(path) {
        Ok(mut stream) => {
            let hello = write_frame(
                &mut stream,
                &DaemonHello {
                    protocol: PROTOCOL.to_string(),
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                },
            );
            match hello {
                Ok(()) => {
                    let ack = read_frame::<DaemonHelloAck>(&mut stream);
                    if ack.is_ok() {
                        return Err(DaemonTransportError::AlreadyRunning);
                    }
                }
                Err(ClientDaemonTransportError::ClientDisconnected) => {}
                Err(error) => return Err(error.into()),
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

fn events_from_client(events: Vec<HubClientEvent>) -> Vec<DaemonEvent> {
    events.into_iter().map(daemon_event_from_client).collect()
}

#[derive(Debug)]
pub(crate) enum ControlMessage {
    AcceptedConnection {
        stream: TokioUnixStream,
        admission_permit: OwnedSemaphorePermit,
    },
    RejectedConnection,
    SubscribeEntities {
        entity_type: String,
        subscription_id: String,
        frame_tx: EntityFrameSender,
        reply_tx: ControlReplySender,
        /// When set, admission requires a still-live local WebRTC peer for this grant.
        /// Socket-path subscriptions leave this `None`.
        grant_id: Option<String>,
    },
    UnsubscribeEntities {
        subscription_id: String,
        reply_tx: Option<ControlReplySender>,
        /// Same live-peer guard as `SubscribeEntities` when the request originated on WebRTC.
        grant_id: Option<String>,
    },
    Request {
        request: Box<DaemonRequest>,
        reply_tx: ControlReplySender,
        response_delivery_rx: Option<mpsc::Receiver<()>>,
        /// When set, admission requires a still-live local WebRTC peer for this grant.
        /// Socket-path and signal-handler requests leave this `None`.
        grant_id: Option<String>,
    },
    HubUpdateCheckCompleted {
        update: DaemonHubUpdate,
    },
    EgressWriteFailed {
        delivery_kind: DaemonDeliveryKind,
    },
    LocalWebrtcPeerClosed {
        grant_id: String,
        attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
        entity_subscription_ids: Vec<String>,
        terminal_record: LocalWebrtcSenderTerminalRecord,
    },
}

#[derive(Debug)]
pub(crate) enum EntityFrameSender {
    #[cfg(test)]
    Blocking(SyncSender<DaemonEntityFrame>),
    Async(tokio::sync::mpsc::Sender<DaemonEntityFrame>),
}

#[derive(Debug)]
pub(crate) enum EntityFrameTrySendError {
    Full,
    Disconnected,
}

impl EntityFrameSender {
    pub(crate) fn try_send(&self, frame: DaemonEntityFrame) -> Result<(), EntityFrameTrySendError> {
        match self {
            #[cfg(test)]
            Self::Blocking(sender) => sender.try_send(frame).map_err(|error| match error {
                mpsc::TrySendError::Full(_) => EntityFrameTrySendError::Full,
                mpsc::TrySendError::Disconnected(_) => EntityFrameTrySendError::Disconnected,
            }),
            Self::Async(sender) => sender.try_send(frame).map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => EntityFrameTrySendError::Full,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    EntityFrameTrySendError::Disconnected
                }
            }),
        }
    }
}

fn entity_frame_exceeds_limit(frame: &DaemonEntityFrame) -> bool {
    serde_json::to_vec(frame)
        .expect("daemon entity frame values always serialize")
        .len()
        > DAEMON_MAX_FRAME_BYTES
}

#[derive(Debug)]
pub(crate) struct EntitySubscriptionState {
    sender: EntityFrameSender,
    entity_type: String,
    cursor: Option<SessionLifecycleCursor>,
    entities: BTreeMap<String, DaemonSessionEntity>,
    definition_generation: u64,
    definition_entities: BTreeMap<String, Value>,
    resync_reason: Option<String>,
    /// Local WebRTC grant that owns this subscription, when registered over DataChannel.
    /// Used so PeerClosed can sweep rows that arrived after cleanup_once's id snapshot.
    pub(crate) owner_grant_id: Option<String>,
}

#[derive(Debug, Default)]
struct EntityReconciliationState {
    cursor: Option<SessionLifecycleCursor>,
    records: BTreeMap<String, SessionLifecycleRecord>,
}

#[derive(Debug, Default)]
pub(crate) struct PendingRuntimeState {
    events: BTreeMap<String, Vec<HubClientEvent>>,
    pub(crate) active_subscriptions: BTreeMap<String, BTreeSet<String>>,
    /// WebRTC grant that owns each attach row created with a tagged Request.
    /// Key is (session_id, subscription_id). Used so PeerClosed can sweep residual
    /// attaches even when the peer-side ownership snapshot was empty.
    pub(crate) attach_owner_grant_ids: BTreeMap<(String, String), String>,
}

#[derive(Debug)]
pub(crate) struct DaemonControlState {
    logical_clock: u64,
    drain_cursors: BTreeMap<String, u64>,
    egress_diagnostics: DaemonEgressDiagnostics,
    pub(crate) entity_subscriptions: BTreeMap<String, EntitySubscriptionState>,
    reconciliation: EntityReconciliationState,
    pub(crate) pending_runtime: PendingRuntimeState,
    pub(crate) lifecycle_counters: DaemonLifecycleCounters,
    next_reconciliation: Instant,
    released_entity_generations: u64,
    pub(crate) released_attach_generations: u64,
    pending_hub_update_reply: Option<ControlReplySender>,
}

impl Default for DaemonControlState {
    fn default() -> Self {
        Self {
            logical_clock: 1,
            drain_cursors: BTreeMap::new(),
            egress_diagnostics: DaemonEgressDiagnostics::default(),
            entity_subscriptions: BTreeMap::new(),
            reconciliation: EntityReconciliationState::default(),
            pending_runtime: PendingRuntimeState::default(),
            lifecycle_counters: DaemonLifecycleCounters::default(),
            next_reconciliation: Instant::now(),
            released_entity_generations: 0,
            released_attach_generations: 0,
            pending_hub_update_reply: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DaemonDeliveryKind {
    Terminal,
    Control,
}

impl DaemonDeliveryKind {
    fn label(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Default)]
struct DaemonEgressDiagnostics {
    terminal_write_failures: u64,
    control_write_failures: u64,
}

impl DaemonEgressDiagnostics {
    fn record_write_failure(&mut self, delivery_kind: DaemonDeliveryKind) {
        match delivery_kind {
            DaemonDeliveryKind::Terminal => {
                self.terminal_write_failures = self.terminal_write_failures.saturating_add(1);
            }
            DaemonDeliveryKind::Control => {
                self.control_write_failures = self.control_write_failures.saturating_add(1);
            }
        }
    }

    fn diagnostics(&self) -> Vec<DaemonDiagnostic> {
        let mut diagnostics = Vec::new();
        if self.terminal_write_failures > 0 {
            diagnostics.push(egress_backpressure_diagnostic(
                DaemonDeliveryKind::Terminal,
                self.terminal_write_failures,
            ));
        }
        if self.control_write_failures > 0 {
            diagnostics.push(egress_backpressure_diagnostic(
                DaemonDeliveryKind::Control,
                self.control_write_failures,
            ));
        }
        diagnostics
    }
}

fn egress_backpressure_diagnostic(
    delivery_kind: DaemonDeliveryKind,
    failures: u64,
) -> DaemonDiagnostic {
    DaemonDiagnostic::backpressure(
        "daemon_client_egress",
        format!(
            "daemon client {} egress observed {failures} bounded write failure(s)",
            delivery_kind.label()
        ),
    )
}

fn daemon_delivery_kind(response: &DaemonResponse) -> DaemonDeliveryKind {
    if response.events.iter().any(|event| {
        matches!(
            event,
            DaemonEvent::TerminalOutput { .. }
                | DaemonEvent::Snapshot { .. }
                | DaemonEvent::Scrollback { .. }
        )
    }) {
        DaemonDeliveryKind::Terminal
    } else {
        DaemonDeliveryKind::Control
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachedSubscription {
    session_id: String,
    subscription_id: String,
}

#[derive(Clone)]
enum AttachedSubscriptionChange {
    Attach(AttachedSubscription),
    Detach(AttachedSubscription),
}

fn record_attached_subscription_change(
    state: &mut DaemonControlState,
    change: Option<AttachedSubscriptionChange>,
    owner_grant_id: Option<&str>,
) {
    let Some(change) = change else {
        return;
    };
    match change {
        AttachedSubscriptionChange::Attach(subscription) => {
            if state.released_attach_generations > 0 {
                state.released_attach_generations -= 1;
                state.lifecycle_counters.reconnect_registrations = state
                    .lifecycle_counters
                    .reconnect_registrations
                    .saturating_add(1);
            }
            state.lifecycle_counters.live_attach_subscriptions = state
                .lifecycle_counters
                .live_attach_subscriptions
                .saturating_add(1);
            state.lifecycle_counters.high_water_attach_subscriptions = state
                .lifecycle_counters
                .high_water_attach_subscriptions
                .max(state.lifecycle_counters.live_attach_subscriptions);
            if let Some(grant_id) = owner_grant_id {
                state.pending_runtime.attach_owner_grant_ids.insert(
                    (
                        subscription.session_id.clone(),
                        subscription.subscription_id.clone(),
                    ),
                    grant_id.to_string(),
                );
            }
        }
        AttachedSubscriptionChange::Detach(subscription) => {
            state.lifecycle_counters.live_attach_subscriptions = state
                .lifecycle_counters
                .live_attach_subscriptions
                .saturating_sub(1);
            state.released_attach_generations = state.released_attach_generations.saturating_add(1);
            state
                .pending_runtime
                .attach_owner_grant_ids
                .remove(&(subscription.session_id, subscription.subscription_id));
        }
    }
}

fn control_request_operation_label(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::ListSessions => "list_sessions",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::Detach { .. } => "detach",
        DaemonRequest::SendInput { .. } => "send_input",
        DaemonRequest::Drain { .. } => "drain",
        DaemonRequest::Resize { .. } => "resize",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        DaemonRequest::RemoveSession { .. } => "remove_session",
        DaemonRequest::DaemonShutdown => "daemon_shutdown",
        DaemonRequest::CheckHubUpdate => "check_hub_update",
        _ => "request",
    }
}

fn local_webrtc_peer_gone_request_error(operation: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "local_webrtc_peer_gone".to_string(),
        request_id: format!("local-webrtc-{operation}"),
        operation: operation.to_string(),
        message: "local WebRTC peer is no longer live".to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            operation,
            "local WebRTC peer is no longer live",
        )],
    });
    response
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

fn register_entity_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entity_type: String,
    subscription_id: String,
    sender: EntityFrameSender,
    owner_grant_id: Option<String>,
) -> DaemonTransportResult<DaemonResponse> {
    if state.entity_subscriptions.contains_key(&subscription_id) {
        return Ok(entity_subscription_error(
            "duplicate_entity_subscription",
            &subscription_id,
            "entity subscription id is already active",
        ));
    }
    if entity_type == "session_type" {
        let (snapshot_seq, entities) = match session_type_entity_snapshot(daemon) {
            Ok(snapshot) => snapshot,
            Err(DaemonTransportError::Client(crate::HubClientError::SessionType {
                kind,
                message,
                ..
            })) => {
                // Keep entity-subscription operator frames on the subscribe_entities
                // convention (request_id = subscription_id), not list_session_types.
                return Ok(entity_subscription_error(kind, &subscription_id, &message));
            }
            Err(error) => return Err(error),
        };
        let snapshot = DaemonEntityFrame::Snapshot {
            subscription_id: subscription_id.clone(),
            entity_type: entity_type.clone(),
            snapshot_seq,
            items: entities.values().cloned().collect(),
            resync_reason: None,
        };
        if entity_frame_exceeds_limit(&snapshot) {
            return Ok(entity_subscription_error(
                "entity_provider_frame_too_large",
                &subscription_id,
                "session type snapshot exceeds daemon frame limit",
            ));
        }
        sender
            .try_send(snapshot)
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: snapshot_seq,
                definition_entities: entities,
                resync_reason: None,
                owner_grant_id,
            },
        );
        state.lifecycle_counters.live_entity_subscriptions =
            state.entity_subscriptions.len() as u64;
        state.lifecycle_counters.high_water_entity_subscriptions = state
            .lifecycle_counters
            .high_water_entity_subscriptions
            .max(state.lifecycle_counters.live_entity_subscriptions);
        return Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed));
    }
    if entity_type != "session" {
        let runtime = daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?;
        let (snapshot_seq, items) =
            match runtime.plugin_entity_snapshot(&entity_type, &subscription_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return Ok(entity_subscription_error(
                        &error.code,
                        &subscription_id,
                        &error.message,
                    ));
                }
            };
        let snapshot = DaemonEntityFrame::Snapshot {
            subscription_id: subscription_id.clone(),
            entity_type: entity_type.clone(),
            snapshot_seq,
            items,
            resync_reason: None,
        };
        if entity_frame_exceeds_limit(&snapshot) {
            return Ok(entity_subscription_error(
                "entity_provider_frame_too_large",
                &subscription_id,
                "entity provider snapshot exceeds daemon frame limit",
            ));
        }
        sender
            .try_send(snapshot)
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 0,
                definition_entities: BTreeMap::new(),
                resync_reason: None,
                owner_grant_id,
            },
        );
        state.lifecycle_counters.live_entity_subscriptions =
            state.entity_subscriptions.len() as u64;
        state.lifecycle_counters.high_water_entity_subscriptions = state
            .lifecycle_counters
            .high_water_entity_subscriptions
            .max(state.lifecycle_counters.live_entity_subscriptions);
        return Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed));
    }
    let baseline = if let Some(cursor) = state.reconciliation.cursor.clone() {
        SessionLifecycleBaseline {
            cursor,
            sessions: state.reconciliation.records.values().cloned().collect(),
        }
    } else {
        let packages = daemon.package_registry().clone();
        let runtime = daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?;
        let api = HubClientApi::local_operator("botster-hub-daemon-entity-stream");
        let response = api.handle_request(
            runtime,
            &packages,
            HubClientRequest::SubscribeEntities {
                request_id: request_id("daemon-entity-subscribe"),
                entity_type,
                subscription_id: subscription_id.clone(),
            },
        );
        let response = match response {
            Ok(response) => response,
            Err(error) => return Ok(daemon_operator_error(error)),
        };
        let HubClientResponseBody::SessionLifecycleBaseline(baseline) = response.body else {
            return Err(DaemonTransportError::UnexpectedResponse);
        };
        state.lifecycle_counters.lifecycle_baseline_reads = state
            .lifecycle_counters
            .lifecycle_baseline_reads
            .saturating_add(1);
        baseline
    };
    if state.reconciliation.cursor.is_none() {
        state.reconciliation.cursor = Some(baseline.cursor.clone());
        state.reconciliation.records = baseline
            .sessions
            .iter()
            .cloned()
            .map(|record| (record.session.session_id.0.clone(), record))
            .collect();
    }
    let cursor = baseline.cursor.clone();
    let (entities, snapshot) = entity_snapshot(&subscription_id, baseline, None);
    sender
        .try_send(snapshot)
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
    state.entity_subscriptions.insert(
        subscription_id.clone(),
        EntitySubscriptionState {
            sender,
            entity_type: "session".to_string(),
            cursor: Some(cursor),
            entities,
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id,
        },
    );
    state.lifecycle_counters.live_entity_subscriptions = state
        .lifecycle_counters
        .live_entity_subscriptions
        .saturating_add(1);
    state.lifecycle_counters.high_water_entity_subscriptions = state
        .lifecycle_counters
        .high_water_entity_subscriptions
        .max(state.lifecycle_counters.live_entity_subscriptions);
    if state.released_entity_generations > 0 {
        state.released_entity_generations -= 1;
        state.lifecycle_counters.reconnect_registrations = state
            .lifecycle_counters
            .reconnect_registrations
            .saturating_add(1);
    }
    state.next_reconciliation = Instant::now();
    Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed))
}

fn seed_lifecycle_reconciliation(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime_mut() else {
        return;
    };
    state.lifecycle_counters.lifecycle_baseline_reads = state
        .lifecycle_counters
        .lifecycle_baseline_reads
        .saturating_add(1);
    let Ok(baseline) = runtime.session_lifecycle_baseline() else {
        return;
    };
    state.reconciliation.cursor = Some(baseline.cursor);
    state.reconciliation.records = baseline
        .sessions
        .into_iter()
        .map(|record| (record.session.session_id.0.clone(), record))
        .collect();
}

fn session_type_entity_snapshot(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<(u64, BTreeMap<String, Value>)> {
    let packages = daemon.package_registry().clone();
    let records = packages.packages();
    let runtime = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let state = runtime.state();
    let generation = state.session_type_generation;
    let session_types =
        crate::session_types::list_session_types(&records, &state).map_err(|error| {
            DaemonTransportError::Client(crate::HubClientError::SessionType {
                request_id: request_id("daemon-session-types-list"),
                operation: crate::HubClientOperation::ListSessionTypes,
                kind: error.kind,
                message: error.message,
            })
        })?;
    let entities = session_types
        .into_iter()
        .map(daemon_session_type_from_client)
        .map(|session_type| {
            let id = session_type.session_type_id.clone();
            serde_json::to_value(session_type)
                .map(|value| (id, value))
                .map_err(DaemonTransportError::Json)
        })
        .collect::<DaemonTransportResult<BTreeMap<_, _>>>()?;
    Ok((generation, entities))
}

fn session_type_definition_map(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<BTreeMap<String, Value>> {
    session_type_entity_snapshot(daemon).map(|(_, entities)| entities)
}

fn is_invalid_repo_session_types_error(error: &DaemonTransportError) -> bool {
    matches!(
        error,
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            kind: "invalid_repo_session_types",
            ..
        })
    )
}

fn ensure_repo_session_types_valid_for_enabled_root(root: &Path) -> DaemonTransportResult<()> {
    crate::session_types::validate_repo_session_types_at(root).map_err(|error| {
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            request_id: request_id("daemon-session-types-list"),
            operation: crate::HubClientOperation::ListSessionTypes,
            kind: error.kind,
            message: error.message,
        })
    })
}

fn ensure_update_would_not_enable_invalid_repo_session_types(
    daemon: &HubDaemon,
    target_id: &str,
    root: Option<&PathBuf>,
    enabled: Option<bool>,
) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let state = runtime.state();
    let Some(target) = state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == target_id)
    else {
        // Let the later update path return not_found.
        return Ok(());
    };
    let resulting_enabled = enabled.unwrap_or(target.enabled);
    if !resulting_enabled {
        return Ok(());
    }
    let resulting_root = root.cloned().unwrap_or_else(|| target.root.clone());
    // Defer non-directory roots to update_spawn_target's root_not_directory.
    if !resulting_root.is_dir() {
        return Ok(());
    }
    ensure_repo_session_types_valid_for_enabled_root(&resulting_root)
}

fn advance_session_type_generation_if_changed(
    daemon: &mut HubDaemon,
    before: &BTreeMap<String, Value>,
) -> DaemonTransportResult<()> {
    if session_type_definition_map(daemon)? == *before {
        return Ok(());
    }
    force_advance_session_type_generation(daemon)
}

fn force_advance_session_type_generation(daemon: &mut HubDaemon) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store.update(&config, |state| {
        state.session_type_generation = state.session_type_generation.saturating_add(1);
    })?;
    daemon.replace_state(state);
    Ok(())
}

fn drive_session_type_subscriptions(
    subscriptions: &mut BTreeMap<String, EntitySubscriptionState>,
    generation: u64,
    entities: &BTreeMap<String, Value>,
) {
    subscriptions.retain(|subscription_id, subscription| {
        if subscription.entity_type != "session_type" {
            return true;
        }

        if let Some(reason) = subscription.resync_reason.clone() {
            let frame = DaemonEntityFrame::Snapshot {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq: generation,
                items: entities.values().cloned().collect(),
                resync_reason: Some(reason),
            };
            if entity_frame_exceeds_limit(&frame) {
                let error = DaemonEntityFrame::Error {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    code: "entity_provider_frame_too_large".to_string(),
                    message: "session type snapshot exceeds daemon frame limit".to_string(),
                };
                return match subscription.sender.try_send(error) {
                    Ok(()) | Err(EntityFrameTrySendError::Disconnected) => false,
                    Err(EntityFrameTrySendError::Full) => true,
                };
            }
            return match subscription.sender.try_send(frame) {
                Ok(()) => {
                    subscription.definition_generation = generation;
                    subscription.definition_entities = entities.clone();
                    subscription.resync_reason = None;
                    true
                }
                Err(EntityFrameTrySendError::Full) => true,
                Err(EntityFrameTrySendError::Disconnected) => false,
            };
        }

        if subscription.definition_generation == generation {
            return true;
        }

        let mut frames = subscription
            .definition_entities
            .keys()
            .filter(|id| !entities.contains_key(*id))
            .map(|id| DaemonEntityFrame::Remove {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq: generation,
                id: id.clone(),
            })
            .collect::<Vec<_>>();
        frames.extend(
            entities
                .iter()
                .filter(|(id, entity)| subscription.definition_entities.get(*id) != Some(*entity))
                .map(|(id, entity)| DaemonEntityFrame::Upsert {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    snapshot_seq: generation,
                    id: id.clone(),
                    entity: entity.clone(),
                }),
        );
        for frame in frames {
            match subscription.sender.try_send(frame) {
                Ok(()) => {}
                Err(EntityFrameTrySendError::Full) => {
                    subscription.resync_reason = Some("subscriber_overflow".to_string());
                    return true;
                }
                Err(EntityFrameTrySendError::Disconnected) => return false,
            }
        }
        subscription.definition_generation = generation;
        subscription.definition_entities = entities.clone();
        true
    });
}

fn drive_entity_subscriptions(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    if state.entity_subscriptions.is_empty() {
        return;
    }
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        state.entity_subscriptions.clear();
        state.lifecycle_counters.live_entity_subscriptions = 0;
        return;
    };
    state.entity_subscriptions.retain(|_, subscription| {
        subscription.entity_type == "session"
            || subscription.entity_type == "session_type"
            || runtime.has_plugin_entity_provider_family(&subscription.entity_type)
    });
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;

    state.lifecycle_counters.reconciliation_wakes = state
        .lifecycle_counters
        .reconciliation_wakes
        .saturating_add(1);

    if state
        .entity_subscriptions
        .values()
        .any(|subscription| subscription.entity_type == "session_type")
    {
        let records = packages.packages();
        let runtime_state = runtime.state();
        let generation = runtime_state.session_type_generation;
        if let Ok(session_types) =
            crate::session_types::list_session_types(&records, &runtime_state)
        {
            let entities = session_types
                .into_iter()
                .map(daemon_session_type_from_client)
                .filter_map(|session_type| {
                    let id = session_type.session_type_id.clone();
                    serde_json::to_value(session_type)
                        .ok()
                        .map(|value| (id, value))
                })
                .collect::<BTreeMap<_, _>>();
            drive_session_type_subscriptions(
                &mut state.entity_subscriptions,
                generation,
                &entities,
            );
        }
    }

    let Some(cursor) = state.reconciliation.cursor.clone() else {
        return;
    };
    if state.entity_subscriptions.values().any(|subscription| {
        subscription.entity_type == "session" && subscription.resync_reason.is_some()
    }) {
        let baseline = SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: state.reconciliation.records.values().cloned().collect(),
        };
        state
            .entity_subscriptions
            .retain(|subscription_id, subscription| {
                if subscription.entity_type != "session" {
                    return true;
                }
                let Some(reason) = subscription.resync_reason.clone() else {
                    return true;
                };
                try_resync_subscription(
                    subscription_id,
                    subscription,
                    baseline.clone(),
                    reason,
                    &mut state.lifecycle_counters,
                )
            });
    }
    state.lifecycle_counters.lifecycle_change_reads = state
        .lifecycle_counters
        .lifecycle_change_reads
        .saturating_add(1);
    let changes = runtime.session_lifecycle_changes(&cursor);
    if let Some(reason) = changes.resync_required {
        state.lifecycle_counters.lifecycle_resync_reads = state
            .lifecycle_counters
            .lifecycle_resync_reads
            .saturating_add(1);
        state.lifecycle_counters.lifecycle_baseline_reads = state
            .lifecycle_counters
            .lifecycle_baseline_reads
            .saturating_add(1);
        let Ok(baseline) = runtime.session_lifecycle_baseline() else {
            return;
        };
        state.reconciliation.cursor = Some(baseline.cursor.clone());
        state.reconciliation.records = baseline
            .sessions
            .iter()
            .cloned()
            .map(|record| (record.session.session_id.0.clone(), record))
            .collect();
        let reason = format!("core_{reason:?}").to_lowercase();
        state
            .entity_subscriptions
            .retain(|subscription_id, subscription| {
                if subscription.entity_type != "session" {
                    return true;
                }
                try_resync_subscription(
                    subscription_id,
                    subscription,
                    baseline.clone(),
                    reason.clone(),
                    &mut state.lifecycle_counters,
                )
            });
    } else if changes.changes.iter().any(|change| {
        !matches!(
            &change.kind,
            SessionLifecycleChangeKind::Upsert { .. } | SessionLifecycleChangeKind::Removed { .. }
        )
    }) {
        state.lifecycle_counters.lifecycle_resync_reads = state
            .lifecycle_counters
            .lifecycle_resync_reads
            .saturating_add(1);
        state.lifecycle_counters.lifecycle_baseline_reads = state
            .lifecycle_counters
            .lifecycle_baseline_reads
            .saturating_add(1);
        let Ok(baseline) = runtime.session_lifecycle_baseline() else {
            return;
        };
        state.reconciliation.cursor = Some(baseline.cursor.clone());
        state.reconciliation.records = baseline
            .sessions
            .iter()
            .cloned()
            .map(|record| (record.session.session_id.0.clone(), record))
            .collect();
        state
            .entity_subscriptions
            .retain(|subscription_id, subscription| {
                try_resync_subscription(
                    subscription_id,
                    subscription,
                    baseline.clone(),
                    "unknown_core_change".to_string(),
                    &mut state.lifecycle_counters,
                )
            });
    } else {
        let mut unsupported_change = false;
        for change in changes.changes {
            match &change.kind {
                SessionLifecycleChangeKind::Upsert { record } => {
                    state
                        .reconciliation
                        .records
                        .insert(record.session.session_id.0.clone(), record.clone());
                }
                SessionLifecycleChangeKind::Removed { session_id } => {
                    state.reconciliation.records.remove(&session_id.0);
                }
                _ => {
                    unsupported_change = true;
                    break;
                }
            }
            state.reconciliation.cursor = Some(change.cursor.clone());
            state
                .entity_subscriptions
                .retain(|subscription_id, subscription| {
                    if subscription.entity_type != "session" {
                        return true;
                    }
                    deliver_lifecycle_change(
                        subscription_id,
                        subscription,
                        &change,
                        &mut state.lifecycle_counters,
                    )
                });
        }
        if unsupported_change {
            state.lifecycle_counters.lifecycle_resync_reads = state
                .lifecycle_counters
                .lifecycle_resync_reads
                .saturating_add(1);
            state.lifecycle_counters.lifecycle_baseline_reads = state
                .lifecycle_counters
                .lifecycle_baseline_reads
                .saturating_add(1);
            let Ok(baseline) = runtime.session_lifecycle_baseline() else {
                return;
            };
            state.reconciliation.cursor = Some(baseline.cursor.clone());
            state.reconciliation.records = baseline
                .sessions
                .iter()
                .cloned()
                .map(|record| (record.session.session_id.0.clone(), record))
                .collect();
            state
                .entity_subscriptions
                .retain(|subscription_id, subscription| {
                    if subscription.entity_type != "session" {
                        return true;
                    }
                    try_resync_subscription(
                        subscription_id,
                        subscription,
                        baseline.clone(),
                        "unknown_core_change".to_string(),
                        &mut state.lifecycle_counters,
                    )
                });
        } else {
            state.reconciliation.cursor = Some(changes.cursor);
        }
    }

    let active_session_ids = state
        .reconciliation
        .records
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    state.pending_runtime.events.retain(|session_id, _| {
        active_session_ids.contains(session_id)
            && state
                .pending_runtime
                .active_subscriptions
                .contains_key(session_id)
    });
    for record in state.reconciliation.records.values() {
        let session_id = record.session.session_id.0.clone();
        if record.lifecycle.as_ref().is_some_and(|lifecycle| {
            matches!(
                lifecycle,
                SessionLifecycleState::Exited { .. } | SessionLifecycleState::Failed { .. }
            )
        }) {
            continue;
        }
        let drain_cursor = state
            .drain_cursors
            .entry(session_id.clone())
            .or_insert_with(|| tick(&mut state.logical_clock));
        state.lifecycle_counters.lifecycle_session_drains = state
            .lifecycle_counters
            .lifecycle_session_drains
            .saturating_add(1);
        if let Ok(output) =
            runtime.drain_runtime_once(&SessionId(session_id.clone()), *drain_cursor)
        {
            let has_client_egress = !output.client_egress.is_empty();
            let events = crate::client_api::events_from_drain(output);
            if has_client_egress && !events.is_empty() {
                *drain_cursor = tick(&mut state.logical_clock);
                state
                    .pending_runtime
                    .events
                    .entry(session_id)
                    .or_default()
                    .extend(events);
            }
        }
    }
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

fn deliver_lifecycle_change(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    change: &botster_core_daemon::SessionLifecycleChange,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    let sequence = change.cursor.sequence;
    let frame = match &change.kind {
        SessionLifecycleChangeKind::Upsert { record } => {
            let entity = project_session_entity(record);
            let id = entity.session_uuid.clone();
            match state.entities.insert(id.clone(), entity.clone()) {
                None => DaemonEntityFrame::Upsert {
                    subscription_id: subscription_id.to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: sequence,
                    id,
                    entity: serde_json::to_value(entity).expect("serialize session entity"),
                },
                Some(previous) => {
                    let patch = session_entity_patch(&previous, &entity);
                    if patch.as_object().is_some_and(serde_json::Map::is_empty) {
                        state.cursor = Some(change.cursor.clone());
                        return true;
                    }
                    DaemonEntityFrame::Patch {
                        subscription_id: subscription_id.to_string(),
                        entity_type: "session".to_string(),
                        snapshot_seq: sequence,
                        id,
                        patch,
                    }
                }
            }
        }
        SessionLifecycleChangeKind::Removed { session_id } => {
            state.entities.remove(&session_id.0);
            DaemonEntityFrame::Remove {
                subscription_id: subscription_id.to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: sequence,
                id: session_id.0.clone(),
            }
        }
        _ => {
            state.resync_reason = Some("unknown_core_change".to_string());
            return true;
        }
    };
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    match state.sender.try_send(frame) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.cursor = Some(change.cursor.clone());
            true
        }
        Err(EntityFrameTrySendError::Full) => {
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            state.resync_reason = Some("subscriber_overflow".to_string());
            true
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            false
        }
    }
}

fn try_resync_subscription(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    baseline: SessionLifecycleBaseline,
    reason: String,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    let cursor = baseline.cursor.clone();
    let (entities, snapshot) = entity_snapshot(subscription_id, baseline, Some(reason));
    match state.sender.try_send(snapshot) {
        Ok(()) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.cursor = Some(cursor);
            state.entities = entities;
            state.resync_reason = None;
            true
        }
        Err(EntityFrameTrySendError::Full) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            true
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            false
        }
    }
}

fn entity_snapshot(
    subscription_id: &str,
    baseline: SessionLifecycleBaseline,
    resync_reason: Option<String>,
) -> (BTreeMap<String, DaemonSessionEntity>, DaemonEntityFrame) {
    let entities = baseline
        .sessions
        .iter()
        .map(project_session_entity)
        .map(|entity| (entity.session_uuid.clone(), entity))
        .collect::<BTreeMap<_, _>>();
    let frame = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: baseline.cursor.sequence,
        items: entities
            .values()
            .map(|entity| serde_json::to_value(entity).expect("serialize session entity"))
            .collect(),
        resync_reason,
    };
    (entities, frame)
}

fn project_session_entity(record: &SessionLifecycleRecord) -> DaemonSessionEntity {
    let (lifecycle, exit_code, failure_reason) = match &record.lifecycle {
        Some(SessionLifecycleState::Starting) => (Some("starting".to_string()), None, None),
        Some(SessionLifecycleState::Running) => (Some("running".to_string()), None, None),
        Some(SessionLifecycleState::Stopping) => (Some("stopping".to_string()), None, None),
        Some(SessionLifecycleState::Exited { code }) => (Some("exited".to_string()), *code, None),
        Some(SessionLifecycleState::Failed { reason }) => {
            (Some("failed".to_string()), None, Some(reason.clone()))
        }
        None => (None, None, None),
    };
    let lifecycle_class =
        session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
    let metadata = &record.metadata.entries;
    let traits = metadata
        .get("botster.session_type.traits")
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default();
    DaemonSessionEntity {
        session_uuid: record.session.session_id.0.clone(),
        registry_state: match record.session.registry_state {
            RegistrySessionState::Running => "running",
            RegistrySessionState::Stopping => "stopping",
            RegistrySessionState::Exited => "exited",
            RegistrySessionState::Stale => "stale",
        }
        .to_string(),
        lifecycle,
        lifecycle_class: lifecycle_class.to_string(),
        rows: record.session.size.rows,
        cols: record.session.size.cols,
        updated_at: record.session.updated_at,
        exit_code,
        failure_reason,
        session_type_id: metadata.get("botster.session_type.id").cloned(),
        session_type_source: metadata.get("botster.session_type.source").cloned(),
        role: metadata.get("botster.session_type.role").cloned(),
        traits,
        interaction: metadata.get("botster.session_type.interaction").cloned(),
        session_type_lifecycle: metadata.get("botster.session_type.lifecycle").cloned(),
    }
}

fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        "indeterminate"
    } else {
        match lifecycle {
            Some(
                SessionLifecycleState::Starting
                | SessionLifecycleState::Running
                | SessionLifecycleState::Stopping,
            ) => "current",
            Some(SessionLifecycleState::Exited { .. } | SessionLifecycleState::Failed { .. }) => {
                "ended"
            }
            None => "indeterminate",
        }
    }
}

fn session_entity_patch(previous: &DaemonSessionEntity, current: &DaemonSessionEntity) -> Value {
    let previous = serde_json::to_value(previous).expect("serialize previous session entity");
    let current = serde_json::to_value(current).expect("serialize current session entity");
    let previous = previous.as_object().expect("session entity object");
    let current = current.as_object().expect("session entity object");
    Value::Object(
        current
            .iter()
            .filter(|(key, value)| previous.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

fn entity_subscription_error(code: &str, subscription_id: &str, message: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: subscription_id.to_string(),
        operation: "subscribe_entities".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "subscribe_entities",
            message,
        )],
    });
    response
}

fn daemon_response_base(kind: DaemonResponseKind) -> DaemonResponse {
    DaemonResponse {
        kind,
        status: None,
        sessions: Vec::new(),
        session_types: Vec::new(),
        session_type_definition: None,
        resolved_session_type: None,
        session_context: None,
        read_screen: None,
        mode_flags: None,
        capture_snapshot: None,
        spawn_targets: Vec::new(),
        spawn_target_validation: None,
        worktrees: Vec::new(),
        apps: Vec::new(),
        resolved_app_launch: None,
        resolved_package_route: None,
        package_navigation: Vec::new(),
        packages: Vec::new(),
        available_packages: Vec::new(),
        install_plan: None,
        update_status: None,
        hub_update: None,
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_worker_counters: None,
        plugin_resource_counters: None,
        plugin_tools: Vec::new(),
        plugin_tool_result: Value::Null,
        plugin_surface: None,
        plugin_action_result: None,
        local_webrtc_bootstrap: None,
        local_webrtc_answer: None,
        events: Vec::new(),
        cleanup: None,
        coordination: None,
        error: None,
        diagnostics: Vec::new(),
    }
}

fn daemon_status(
    status: HubDaemonStatus,
    session_count: usize,
    mut egress_diagnostics: Vec<DaemonDiagnostic>,
    lifecycle_counters: DaemonLifecycleCounters,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Status);
    response.status = Some(daemon_status_from_status(
        &status,
        session_count,
        egress_diagnostics.clone(),
        lifecycle_counters,
    ));
    response.diagnostics = vec![DaemonDiagnostic::connected("status")];
    response.diagnostics.append(&mut egress_diagnostics);
    response
}

fn daemon_hub_update(update: DaemonHubUpdate) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::HubUpdate);
    response.hub_update = Some(update);
    response
}

fn daemon_sessions(sessions: Vec<HubClientSession>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Sessions);
    response.sessions = sessions
        .into_iter()
        .map(daemon_session_from_client)
        .collect();
    response
}

fn daemon_spawned(session: DaemonSession, events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Spawned);
    response.sessions = vec![session];
    response.events = events;
    response
}

fn daemon_events(events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Events);
    response.events = events;
    response
}

fn daemon_read_screen(screen: HubClientReadScreen) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ReadScreen);
    response.read_screen = Some(DaemonReadScreen {
        session_id: screen.session_id.0,
        text: screen.text,
    });
    response
}

fn daemon_mode_flags(mode_flags: HubClientModeFlags) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ReadModeFlags);
    response.mode_flags = Some(DaemonModeFlags {
        session_id: mode_flags.session_id.0,
        mouse_mode: mode_flags.mouse_mode,
    });
    response
}

fn daemon_capture_snapshot(snapshot: HubClientCaptureSnapshot) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::CaptureSnapshot);
    response.capture_snapshot = Some(DaemonCaptureSnapshot {
        session_id: snapshot.session_id.0,
        rows: snapshot.rows,
        cols: snapshot.cols,
        payload_format: snapshot.payload_format,
        payload_bytes: snapshot.payload_bytes,
    });
    response
}

fn daemon_packages(packages: Vec<HubClientPackage>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Packages);
    response.packages = packages
        .into_iter()
        .map(daemon_package_from_client)
        .collect();
    response
}

fn daemon_package_navigation(
    navigation: Vec<HubClientPackageNavigationEntry>,
    packages: &[HubClientPackage],
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageNavigation);
    response.package_navigation = package_navigation_entries(navigation, packages);
    response
}

fn daemon_session_types(templates: Vec<crate::HubSessionType>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionTypes);
    response.session_types = templates
        .into_iter()
        .map(daemon_session_type_from_client)
        .collect();
    response
}

fn daemon_session_type_definition(definition: crate::HubSessionTypeDefinition) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionTypeDefinition);
    response.session_type_definition = Some(DaemonSessionTypeEditableDefinition {
        session_type_id: definition.session_type_id,
        source: daemon_session_type_mutation_source(definition.source),
        definition: daemon_session_type_definition_from_client(definition.definition),
    });
    response
}

fn daemon_resolved_session_type(resolved: ResolvedSessionType) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedSessionType);
    response.resolved_session_type = Some(DaemonResolvedSessionType {
        session_type: daemon_session_type_from_client(resolved.session_type),
        session_id: resolved.session_id.0,
        executable: resolved.executable,
        arguments: resolved.arguments,
        working_directory: resolved.working_directory,
        environment: resolved.environment,
        context_id: resolved.context_id,
        context_keys: resolved.context_keys,
    });
    response
}

fn daemon_session_context(context: crate::HubSessionContext) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionContext);
    response.session_context = Some(DaemonSessionContext {
        context_id: context.context_id,
        session_id: context.session_id.0,
        values: context.values,
    });
    response
}

fn daemon_spawn_targets(targets: Vec<SpawnTarget>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SpawnTargets);
    response.spawn_targets = targets.into_iter().map(daemon_spawn_target).collect();
    response
}

fn daemon_spawn_target_validation(validation: SpawnTargetValidation) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SpawnTargetValidation);
    response.spawn_target_validation = Some(DaemonSpawnTargetValidation {
        target_id: validation.target_id,
        ok: validation.ok,
        status: validation.status,
    });
    response
}

fn daemon_worktrees(worktrees: Vec<Worktree>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Worktrees);
    response.worktrees = worktrees.into_iter().map(daemon_worktree).collect();
    response
}

fn list_spawn_targets_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_spawn_targets(crate::list_spawn_targets(
        &runtime.state().spawn_targets,
    )))
}

fn show_spawn_target_response(
    daemon: &mut HubDaemon,
    target_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_spawn_targets(vec![crate::show_spawn_target(
        &runtime.state().spawn_targets,
        target_id,
    )?]))
}

fn mutate_spawn_targets_response(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<DaemonResponse> {
    let target = persist_spawn_targets(daemon, update)?;
    Ok(daemon_spawn_targets(vec![target]))
}

fn mutate_spawn_targets_with_worktrees_response(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>, &[Worktree]) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<DaemonResponse> {
    let target = persist_spawn_targets_with_worktrees(daemon, update)?;
    Ok(daemon_spawn_targets(vec![target]))
}

fn daemon_spawn_target(target: SpawnTarget) -> DaemonSpawnTarget {
    DaemonSpawnTarget {
        target_id: target.target_id,
        label: target.label,
        root: target.root,
        enabled: target.enabled,
        kind: target.kind,
        base_ref: target.base_ref,
        metadata: target.metadata,
    }
}

fn list_worktrees_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_worktrees(crate::list_worktrees(
        &runtime.state().worktrees,
        &runtime.state().spawn_targets,
    )))
}

fn show_worktree_response(
    daemon: &mut HubDaemon,
    worktree_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_worktrees(vec![crate::show_worktree(
        &runtime.state().worktrees,
        &runtime.state().spawn_targets,
        worktree_id,
    )?]))
}

fn create_worktree_response(
    daemon: &mut HubDaemon,
    request: WorktreeCreate,
) -> DaemonTransportResult<DaemonResponse> {
    let requested_worktree_id = request.worktree_id.clone();
    let requested_target_id = request.target_id.clone();
    match persist_worktrees(daemon, |worktrees, targets| {
        crate::create_worktree(worktrees, targets, request)
    }) {
        Ok(worktree) => {
            let event = worktree_lifecycle_event(
                "worktree_created",
                Some(&worktree),
                &daemon_targets(daemon),
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(DaemonTransportError::Worktree(error)) => {
            let event = worktree_failure_event(
                "worktree_create_failed",
                requested_worktree_id,
                Some(requested_target_id),
                &error,
            );
            let mut response = daemon_worktree_error(error);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

fn delete_worktree_response(
    daemon: &mut HubDaemon,
    worktree_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    match persist_worktrees(daemon, |worktrees, targets| {
        crate::delete_worktree(worktrees, targets, worktree_id)
    }) {
        Ok(worktree) => {
            let event = worktree_lifecycle_event(
                "worktree_deleted",
                Some(&worktree),
                &daemon_targets(daemon),
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(DaemonTransportError::Worktree(error)) => {
            let event = worktree_failure_event(
                "worktree_delete_failed",
                Some(worktree_id.to_string()),
                None,
                &error,
            );
            let mut response = daemon_worktree_error(error);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

fn daemon_targets(daemon: &HubDaemon) -> Vec<SpawnTarget> {
    daemon
        .runtime()
        .map(|runtime| runtime.state().spawn_targets.clone())
        .unwrap_or_default()
}

fn emit_worktree_lifecycle_event(
    daemon: &HubDaemon,
    response: &mut DaemonResponse,
    event: DaemonWorktreeLifecycleEvent,
) {
    if let Some(runtime) = daemon.runtime()
        && let Ok(payload) = serde_json::to_value(&event)
    {
        let _ = runtime.emit_plugin_event(&event.event, payload);
    }
    response
        .events
        .push(DaemonEvent::WorktreeLifecycle { event });
}

fn worktree_lifecycle_event(
    event: &str,
    worktree: Option<&Worktree>,
    targets: &[SpawnTarget],
    failure: Option<(&str, &str)>,
) -> DaemonWorktreeLifecycleEvent {
    DaemonWorktreeLifecycleEvent {
        event: event.to_string(),
        worktree_id: worktree.map(|worktree| worktree.worktree_id.clone()),
        target_id: worktree.map(|worktree| worktree.target_id.clone()),
        status: worktree.map(|worktree| worktree.status.clone()),
        label: worktree.map(|worktree| worktree.label.clone()),
        display_path: worktree
            .and_then(|worktree| sanitized_worktree_display_path(worktree, targets)),
        failure_kind: failure.map(|(kind, _)| kind.to_string()),
        message: failure.map(|(_, message)| message.to_string()),
    }
}

fn worktree_failure_event(
    event: &str,
    worktree_id: Option<String>,
    target_id: Option<String>,
    error: &WorktreeError,
) -> DaemonWorktreeLifecycleEvent {
    DaemonWorktreeLifecycleEvent {
        event: event.to_string(),
        worktree_id,
        target_id,
        status: None,
        label: None,
        display_path: None,
        failure_kind: Some(error.kind.to_string()),
        message: Some(sanitize_worktree_error_message(&error.message)),
    }
}

fn sanitized_worktree_display_path(worktree: &Worktree, targets: &[SpawnTarget]) -> Option<String> {
    let target = targets
        .iter()
        .find(|target| target.target_id == worktree.target_id)?;
    let relative = worktree.path.strip_prefix(&target.root).ok()?;
    if relative.as_os_str().is_empty() {
        None
    } else {
        Some(relative.to_string_lossy().into_owned())
    }
}

fn sanitize_worktree_error_message(message: &str) -> String {
    if message.contains('/') {
        "worktree operation failed".to_string()
    } else {
        message.to_string()
    }
}

fn daemon_worktree(worktree: Worktree) -> DaemonWorktree {
    DaemonWorktree {
        worktree_id: worktree.worktree_id,
        target_id: worktree.target_id,
        label: worktree.label,
        path: worktree.path,
        status: worktree.status,
        management: worktree.management,
        git: worktree.git.map(|git| DaemonWorktreeGitMetadata {
            repository_root: git.repository_root,
            branch: git.branch,
            head: git.head,
        }),
        metadata: worktree.metadata,
    }
}

fn daemon_session_type_from_client(template: crate::HubSessionType) -> DaemonSessionType {
    DaemonSessionType {
        session_type_id: template.session_type_id,
        source_name: template.source_name,
        id: template.id,
        source: template.source,
        editable: template.editable,
        overridden_sources: template
            .overridden_sources
            .into_iter()
            .map(|source| botster_hub_client::DaemonSessionTypeSource {
                kind: source.kind,
                name: source.name,
            })
            .collect(),
        diagnostics: template.diagnostics,
        label: template.label,
        description: template.description,
        icon: template.icon,
        role: template.role,
        interaction: template.interaction,
        traits: template.traits,
        lifecycle: template.lifecycle,
        command: template.command,
        args: template.args,
        working_directory_policy: template.working_directory_policy,
        allowed_environment_overrides: template.allowed_environment_overrides,
        context_keys: template.context_keys,
        target_id: template.target_id,
        available: template.available,
    }
}

fn session_type_request_from_daemon(
    session_id: Option<SessionId>,
    request: DaemonSessionTypeRequest,
) -> SessionTypeRequest {
    SessionTypeRequest {
        target_id: request.target_id,
        session_id,
        cwd: request.cwd,
        environment: request.environment,
        context: session_type_context_from_daemon(request.context),
    }
}

fn session_type_mutation_source_from_daemon(
    source: DaemonSessionTypeMutationSource,
) -> SessionTypeMutationSource {
    match source {
        DaemonSessionTypeMutationSource::Device => SessionTypeMutationSource::Device,
        DaemonSessionTypeMutationSource::Repo { target_id } => {
            SessionTypeMutationSource::Repo { target_id }
        }
        DaemonSessionTypeMutationSource::Package { package_name } => {
            SessionTypeMutationSource::Package { package_name }
        }
    }
}

fn daemon_session_type_mutation_source(
    source: SessionTypeMutationSource,
) -> DaemonSessionTypeMutationSource {
    match source {
        SessionTypeMutationSource::Device => DaemonSessionTypeMutationSource::Device,
        SessionTypeMutationSource::Repo { target_id } => {
            DaemonSessionTypeMutationSource::Repo { target_id }
        }
        SessionTypeMutationSource::Package { package_name } => {
            DaemonSessionTypeMutationSource::Package { package_name }
        }
    }
}

fn daemon_session_type_definition_from_client(
    definition: PackageSessionType,
) -> DaemonSessionTypeDefinition {
    DaemonSessionTypeDefinition {
        id: definition.id,
        label: definition.label,
        description: definition.description,
        icon: definition.icon,
        role: definition.role,
        interaction: definition.interaction,
        traits: definition.traits,
        lifecycle: definition.lifecycle,
        command: definition.command,
        args: definition.args,
        working_directory: match definition.working_directory {
            PackageSessionTypeWorkingDirectory::PackageRoot => {
                DaemonSessionTypeWorkingDirectory::PackageRoot
            }
            PackageSessionTypeWorkingDirectory::Relative { path } => {
                DaemonSessionTypeWorkingDirectory::Relative { path }
            }
        },
        environment: definition.environment,
        allowed_environment_overrides: definition.allowed_environment_overrides,
        context: definition.context,
        target_id: definition.target_id,
    }
}

fn session_type_definition_from_daemon(
    definition: DaemonSessionTypeDefinition,
) -> PackageSessionType {
    PackageSessionType {
        id: definition.id,
        label: definition.label,
        description: definition.description,
        icon: definition.icon,
        role: definition.role,
        interaction: definition.interaction,
        traits: definition.traits,
        lifecycle: definition.lifecycle,
        command: definition.command,
        args: definition.args,
        working_directory: match definition.working_directory {
            DaemonSessionTypeWorkingDirectory::PackageRoot => {
                PackageSessionTypeWorkingDirectory::PackageRoot
            }
            DaemonSessionTypeWorkingDirectory::Relative { path } => {
                PackageSessionTypeWorkingDirectory::Relative { path }
            }
        },
        environment: definition.environment,
        allowed_environment_overrides: definition.allowed_environment_overrides,
        context: definition.context,
        target_id: definition.target_id,
    }
}

fn session_type_context_from_daemon(
    context: DaemonSessionTypeContextInput,
) -> SessionTypeContextInput {
    SessionTypeContextInput {
        worktree_path: context.worktree_path,
        repo_path: context.repo_path,
        branch_name: context.branch_name,
        prompt: context.prompt,
        ticket_id: context.ticket_id,
        workspace_id: context.workspace_id,
        metadata: context.metadata,
    }
}

fn daemon_apps(apps: Vec<DaemonApp>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Apps);
    response.apps = apps;
    response
}

fn daemon_resolved_app_launch(launch: DaemonResolvedAppLaunch) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedAppLaunch);
    response.resolved_app_launch = Some(launch);
    response
}

fn daemon_resolved_package_route(route: DaemonPackageRouteDescriptor) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedPackageRoute);
    response.resolved_package_route = Some(route);
    response
}

fn daemon_available_packages(
    packages: Vec<AvailablePackage>,
    registry_path: &PathBuf,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::AvailablePackages);
    response.available_packages = packages
        .into_iter()
        .map(|package| daemon_available_package_from_policy(package, Some(registry_path)))
        .collect();
    response
}

fn daemon_package_install_plan(plan: PackageInstallPlan) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageInstallPlan);
    response.install_plan = Some(DaemonPackageInstallPlan {
        entry: daemon_available_package_from_policy(plan.entry, None),
        effects: plan
            .effects
            .into_iter()
            .map(|effect| DaemonPackageInstallEffect {
                kind: effect.kind,
                message: effect.message,
            })
            .collect(),
        diagnostics: plan
            .diagnostics
            .into_iter()
            .map(|diagnostic| DaemonPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect(),
        mutates_registry: plan.mutates_registry,
        starts_entrypoints: plan.starts_entrypoints,
    });
    response
}

fn check_package_update_response(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let update_status = package_update_status(daemon, package_name, None)?;
    Ok(daemon_package_update_status(update_status))
}

fn preview_package_update_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    pin: DaemonPackagePin,
) -> DaemonTransportResult<DaemonResponse> {
    let update_status = package_update_status(daemon, package_name, Some(pin.clone()))?;
    let mut response = daemon_package_update_status(update_status);
    response.install_plan = Some(package_update_plan(daemon, package_name, pin)?);
    Ok(response)
}

fn daemon_package_update_status(update_status: DaemonPackageUpdateStatus) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageUpdateStatus);
    response.update_status = Some(update_status);
    response
}

fn daemon_plugin_lifecycle(report: HubClientPluginLifecycleReport) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginLifecycle);
    response.lifecycle = report
        .lifecycle
        .into_iter()
        .map(daemon_plugin_lifecycle_from_client)
        .collect();
    response.plugin_worker_counters = Some(daemon_plugin_worker_counters_from_client(
        report.worker_counters,
    ));
    response.plugin_resource_counters = Some(DaemonPluginResourceCounters {
        active_timer_resources: report.resource_counters.active_timer_resources,
    });
    response
}

fn daemon_session_cleanup(cleanup: DaemonSessionCleanup) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionCleanup);
    response.cleanup = Some(cleanup);
    response
}

fn daemon_unknown_session_cleanup(session_id: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "unknown_session".to_string(),
        request_id: "daemon-sessions-shutdown".to_string(),
        operation: "shutdown".to_string(),
        message: format!("unknown session: {session_id}"),
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_operator_error(error: crate::HubClientError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_client(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_package_error(error: crate::PackageRegistryError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_package(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_spawn_target_error(error: SpawnTargetError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.kind.to_string(),
        request_id: "daemon-spawn-targets".to_string(),
        operation: "spawn_targets".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_worktree_error(error: WorktreeError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.kind.to_string(),
        request_id: "daemon-worktrees".to_string(),
        operation: "worktrees".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_state_error(error: crate::HubStateStoreError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_state(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_entrypoint_error(error: EntrypointSupervisorError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_entrypoint(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_local_webrtc_error(error: crate::LocalWebrtcError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_local_webrtc(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_local_webrtc_answer(answer: DaemonLocalWebrtcAnswer) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::LocalWebrtcAnswer);
    response.diagnostics = answer.diagnostics.clone();
    response.local_webrtc_answer = Some(answer);
    response
}

fn daemon_local_webrtc_bootstrap(bootstrap: DaemonLocalWebrtcBootstrap) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::LocalWebrtcBootstrap);
    response.local_webrtc_bootstrap = Some(bootstrap);
    response
}

fn local_webrtc_bootstrap_issue_error(code: &str, message: impl Into<String>) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("issue_local_webrtc_bootstrap", message.clone());
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "issue-local-webrtc-bootstrap".to_string(),
        operation: "issue_local_webrtc_bootstrap".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

fn daemon_app_launch_error(
    package_name: &str,
    entrypoint_id: &str,
    code: &str,
    message: impl Into<String>,
) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("resolve_app_launch", format!("{code}: {message}"));
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("resolve-app-launch-{package_name}-{entrypoint_id}"),
        operation: "resolve_app_launch".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

fn daemon_package_route_error(
    package_name: &str,
    route_id: &str,
    code: &str,
    message: impl Into<String>,
) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("resolve_package_route", format!("{code}: {message}"));
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("resolve-package-route-{package_name}-{route_id}"),
        operation: "resolve_package_route".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

fn daemon_coordination(
    kind: DaemonResponseKind,
    coordination: DaemonCoordination,
) -> DaemonResponse {
    let mut response = daemon_response_base(kind);
    response.coordination = Some(coordination);
    response
}

fn daemon_plugin_tools(plugin_tools: Vec<McpToolDescriptor>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpTools);
    response.plugin_tools = plugin_tools
        .into_iter()
        .map(|tool| serde_json::to_value(tool).unwrap_or(Value::Null))
        .collect();
    response
}

fn daemon_plugin_tool_result(plugin_tool_result: Value) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpToolResult);
    response.plugin_tool_result = plugin_tool_result;
    response
}

fn daemon_plugin_surface(plugin_surface: HubClientPluginSurface) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginSurface);
    let body = plugin_surface.body;
    response.plugin_surface = Some(DaemonPluginSurface {
        package_name: plugin_surface.package_name.clone(),
        surface_id: plugin_surface.surface_id.clone(),
        body: body.clone(),
        ui_tree_snapshot: Some(DaemonUiTreeSnapshot {
            package_name: plugin_surface.package_name,
            surface_id: plugin_surface.surface_id,
            body,
        }),
    });
    response
}

fn daemon_plugin_action_result(plugin_action_result: UiActionResult) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginActionResult);
    if matches!(
        plugin_action_result.state,
        UiActionResultState::Rejected | UiActionResultState::Error
    ) {
        response.diagnostics = vec![DaemonDiagnostic::action_failure(
            "plugin_surface_action",
            "plugin surface action did not complete successfully",
        )];
    }
    response.plugin_action_result = Some(plugin_action_result);
    response
}

fn daemon_plugin_tool_error(error: crate::McpToolError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.code,
        request_id: "daemon-plugin-mcp-call".to_string(),
        operation: "plugin_mcp_call".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_coordination_identity(identity: DaemonIdentity) -> DaemonCoordination {
    DaemonCoordination {
        identity: Some(identity),
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_publish(deliveries: Vec<EnvelopeDeliveryState>) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: Some(DaemonEnvelopePublish {
            deliveries: deliveries
                .into_iter()
                .map(daemon_envelope_delivery_from_state)
                .collect(),
        }),
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_messages(
    envelopes: Vec<RoutedEnvelope>,
    next_cursor: Option<EnvelopeCursor>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: envelopes
            .into_iter()
            .map(daemon_envelope_from_routed)
            .collect(),
        next_cursor: next_cursor.map(|cursor| cursor.0),
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_ack(state: Option<EnvelopeDeliveryState>) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: Some(daemon_envelope_ack_from_state(state)),
        notify: None,
    }
}

fn daemon_coordination_notify(
    decision: GuardedWriteDecision,
    states: Vec<GuardedWriteDeliveryState>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: Some(DaemonNotify {
            decision: format!("{decision:?}"),
            state_count: states.len(),
            states: states
                .into_iter()
                .map(guarded_write_delivery_state_label)
                .map(ToString::to_string)
                .collect(),
        }),
    }
}

fn daemon_envelope_delivery_from_state(state: EnvelopeDeliveryState) -> DaemonEnvelopeDelivery {
    DaemonEnvelopeDelivery {
        envelope_id: state.envelope_id.0,
        target: envelope_target_label(&state.target),
        cursor: state.cursor.0,
        status: format!("{:?}", state.status).to_ascii_lowercase(),
    }
}

fn daemon_envelope_from_routed(envelope: RoutedEnvelope) -> DaemonEnvelope {
    DaemonEnvelope {
        envelope_id: envelope.id.0,
        source: envelope.source.0,
        content_type: envelope.payload.content_type,
        body: String::from_utf8_lossy(&envelope.payload.body).to_string(),
        created_at: envelope.created_at,
        cursor: envelope.cursor.map(|cursor| cursor.0),
    }
}

fn daemon_envelope_ack_from_state(state: Option<EnvelopeDeliveryState>) -> DaemonEnvelopeAck {
    match state {
        Some(state) => DaemonEnvelopeAck {
            envelope_id: Some(state.envelope_id.0),
            target: Some(envelope_target_label(&state.target)),
            cursor: Some(state.cursor.0),
            status: format!("{:?}", state.status).to_ascii_lowercase(),
        },
        None => DaemonEnvelopeAck {
            envelope_id: None,
            target: None,
            cursor: None,
            status: "unknown".to_string(),
        },
    }
}

fn daemon_package_from_client(package: HubClientPackage) -> DaemonPackage {
    let package_name = package.package_name.clone();
    let package_state = package_state_label(package.state).to_string();
    let package_actions = installed_package_actions(&package);
    let routes = package_route_descriptors(&package);
    DaemonPackage {
        package_name: package.package_name,
        version: package.version,
        classification: package_classification_label(package.classification).to_string(),
        source_kind: package.source_kind,
        state: package_state_label(package.state).to_string(),
        requested_capabilities: package
            .requested_capabilities
            .into_iter()
            .map(|capability| DaemonCapability {
                surface: capability.surface,
                scope: capability.scope,
            })
            .collect(),
        surfaces: package.surfaces,
        routes,
        runnable_entrypoints: package
            .runnable_entrypoints
            .into_iter()
            .map(|entrypoint| {
                let actions = entrypoint_actions(&package_name, &package_state, &entrypoint);
                DaemonPackageRunnableEntrypoint {
                    id: entrypoint.id,
                    kind: entrypoint.kind,
                    launch_mode: entrypoint.launch_mode,
                    command: entrypoint.command,
                    args: entrypoint.args,
                    working_directory: DaemonPackageWorkingDirectory {
                        policy: entrypoint.working_directory.policy,
                        path: entrypoint.working_directory.path,
                    },
                    environment: entrypoint
                        .environment
                        .into_iter()
                        .map(|requirement| DaemonPackageEnvironmentRequirement {
                            name: requirement.name,
                            required: requirement.required,
                            default: requirement.default,
                            description: requirement.description,
                        })
                        .collect(),
                    capabilities: entrypoint
                        .capabilities
                        .into_iter()
                        .map(|capability| DaemonCapability {
                            surface: capability.surface,
                            scope: capability.scope,
                        })
                        .collect(),
                    may_supervise: entrypoint.may_supervise,
                    process: DaemonPackageProcess {
                        state: entrypoint.process.state,
                        pid: entrypoint.process.pid,
                        started_at: entrypoint.process.started_at,
                        exited_at: entrypoint.process.exited_at,
                        exit_status: entrypoint.process.exit_status,
                        diagnostics: entrypoint
                            .process
                            .diagnostics
                            .into_iter()
                            .map(|diagnostic| DaemonPackageDiagnostic {
                                kind: diagnostic.kind,
                                message: diagnostic.message,
                            })
                            .collect(),
                    },
                    actions,
                }
            })
            .collect(),
        configuration: DaemonPackageConfiguration {
            schema: package.configuration.schema,
            effective_values: package.configuration.effective_values,
            missing_required: package.configuration.missing_required,
            diagnostics: package
                .configuration
                .diagnostics
                .into_iter()
                .map(|diagnostic| DaemonPackageDiagnostic {
                    kind: diagnostic.kind,
                    message: diagnostic.message,
                })
                .collect(),
        },
        availability: DaemonPackageAvailability {
            state: daemon_availability_state(package.availability.state),
            reasons: package
                .availability
                .reasons
                .into_iter()
                .map(daemon_availability_reason)
                .collect(),
        },
        dependency_availability: package
            .dependency_availability
            .into_iter()
            .map(|dependency| DaemonPackageDependencyAvailability {
                id: dependency.id,
                package_name: dependency.package_name,
                state: daemon_availability_state(dependency.state),
                reasons: dependency
                    .reasons
                    .into_iter()
                    .map(daemon_availability_reason)
                    .collect(),
            })
            .collect(),
        feature_availability: package
            .feature_availability
            .into_iter()
            .map(|feature| DaemonPackageFeatureAvailability {
                id: feature.id,
                state: daemon_availability_state(feature.state),
                reasons: feature
                    .reasons
                    .into_iter()
                    .map(daemon_availability_reason)
                    .collect(),
            })
            .collect(),
        actions: package_actions,
        provider_profile_admitted: package.provider_profile_admitted,
    }
}

fn daemon_available_package_from_policy(
    package: AvailablePackage,
    registry_path: Option<&PathBuf>,
) -> DaemonAvailablePackage {
    let actions = available_package_actions(&package, registry_path);
    DaemonAvailablePackage {
        entry_id: package.entry_id,
        package_name: package.package_name,
        version: package.version,
        classification: package_classification_label(package.classification.into()).to_string(),
        source_kind: registry_source_kind_label(package.source_kind).to_string(),
        source_label: package.source_label,
        first_party: package.first_party,
        state: available_package_state_label(package.state).to_string(),
        requested_capabilities: package
            .requested_capabilities
            .into_iter()
            .map(|capability| DaemonCapability {
                surface: format!("{:?}", capability.surface),
                scope: capability.scope,
            })
            .collect(),
        compatibility: DaemonPackageCompatibility {
            botster_requirement: package.compatibility.botster_requirement,
            result: package_compatibility_label(package.compatibility.result).to_string(),
            diagnostics: package.compatibility.diagnostics,
        },
        pin: package.pin.map(daemon_package_pin_from_policy),
        actions,
    }
}

fn installed_package_actions(package: &HubClientPackage) -> Vec<DaemonPackageActionState> {
    let package_name = package.package_name.as_str();
    let availability_blocked = matches!(
        package.availability.state,
        HubClientPackageAvailabilityState::Blocked
    );
    let required_references = package_required_references(package);
    let blocked_diagnostics =
        package
            .availability
            .reasons
            .iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason.reason.clone(),
                message: format!("{} is blocked for {}", package.package_name, reason.action),
            })
            .chain(package.configuration.diagnostics.iter().map(|diagnostic| {
                DaemonPackageDiagnostic {
                    kind: diagnostic.kind.clone(),
                    message: diagnostic.message.clone(),
                }
            }))
            .collect::<Vec<_>>();
    let state = package_state_label(package.state);
    let mut actions = Vec::new();

    actions.push(unavailable_action(
        "install_package_registry_entry",
        "already_installed",
        "package is already installed; use update actions for source metadata changes",
    ));

    match state {
        "enabled" => actions.push(unavailable_action(
            "enable_package",
            "already_enabled",
            "package is already enabled",
        )),
        _ if availability_blocked => actions.push(blocked_action(
            "enable_package",
            "package_requirements_blocked",
            blocked_diagnostics.clone(),
            required_references.clone(),
        )),
        _ => actions.push(available_package_action(
            "enable_package",
            request_for_package("enable_package", package_name),
        )),
    }

    if state == "enabled" {
        actions.push(available_package_action(
            "disable_package",
            request_for_package("disable_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "disable_package",
            "not_enabled",
            "package is not enabled",
        ));
    }

    actions.push(available_package_action(
        "remove_package",
        request_for_package("remove_package", package_name),
    ));

    if package.configuration.schema.is_some()
        || !package.configuration.missing_required.is_empty()
        || !package.configuration.diagnostics.is_empty()
    {
        actions.push(available_package_action(
            "set_package_configuration",
            request_for_package("set_package_configuration", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "set_package_configuration",
            "no_configuration_schema",
            "package does not declare configurable fields",
        ));
    }

    actions.push(available_package_action(
        "check_package_update",
        request_for_package("check_package_update", package_name),
    ));
    actions.push(blocked_action(
        "preview_package_update",
        "pin_required",
        vec![DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "preview update requires explicit pinned source metadata".to_string(),
        }],
        vec![DaemonPackageActionRequiredReference {
            kind: "pin".to_string(),
            key: "package_update_pin".to_string(),
        }],
    ));
    actions.push(blocked_action(
        "apply_package_update",
        "pin_required",
        vec![DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "apply update requires explicit pinned source metadata".to_string(),
        }],
        vec![DaemonPackageActionRequiredReference {
            kind: "pin".to_string(),
            key: "package_update_pin".to_string(),
        }],
    ));
    if package.source_kind == "path" {
        actions.push(available_package_action(
            "reload_package",
            request_for_package("reload_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "reload_package",
            "local_path_required",
            "package reload is only available for local path packages",
        ));
    }
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));

    actions
}

fn available_package_actions(
    package: &AvailablePackage,
    registry_path: Option<&PathBuf>,
) -> Vec<DaemonPackageActionState> {
    let mut actions = Vec::new();
    let compatible = matches!(
        package.compatibility.result,
        PackageCompatibilityResult::Compatible
    );
    let install_blocked = !matches!(package.state, AvailablePackageState::Available) || !compatible;
    if install_blocked {
        let reason = if compatible {
            "already_installed"
        } else {
            "botster_compatibility"
        };
        let diagnostics = package
            .compatibility
            .diagnostics
            .iter()
            .map(|message| DaemonPackageDiagnostic {
                kind: "botster_compatibility".to_string(),
                message: message.clone(),
            })
            .collect();
        actions.push(blocked_action(
            "install_package_registry_entry",
            reason,
            diagnostics,
            Vec::new(),
        ));
    } else if let Some(registry_path) = registry_path {
        actions.push(available_package_action(
            "install_package_registry_entry",
            Some(DaemonPackageActionRequest {
                request_type: "install_package_registry_entry".to_string(),
                pin: None,
                package_name: Some(package.package_name.clone()),
                entry_id: Some(package.entry_id.clone()),
                entrypoint_id: None,
                registry_path: Some(registry_path.to_string_lossy().to_string()),
            }),
        ));
    } else {
        actions.push(blocked_action(
            "install_package_registry_entry",
            "registry_path_required",
            vec![DaemonPackageDiagnostic {
                kind: "registry_path_required".to_string(),
                message:
                    "install request mapping requires the registry path used to list the package"
                        .to_string(),
            }],
            vec![DaemonPackageActionRequiredReference {
                kind: "registry".to_string(),
                key: "registry_path".to_string(),
            }],
        ));
    }

    for action_id in [
        "enable_package",
        "disable_package",
        "remove_package",
        "start_package_entrypoint",
        "stop_package_entrypoint",
        "restart_package_entrypoint",
        "check_package_update",
        "preview_package_update",
        "apply_package_update",
        "set_package_configuration",
    ] {
        actions.push(unavailable_action(
            action_id,
            "install_required",
            "install the package before running installed-package lifecycle actions",
        ));
    }
    actions.push(unavailable_action(
        "reload_package",
        "unsupported",
        "package reload is not supported by the hub daemon",
    ));
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));
    actions
}

fn entrypoint_actions(
    package_name: &str,
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
) -> Vec<DaemonPackageActionState> {
    if !entrypoint.may_supervise {
        return vec![
            unavailable_action(
                "start_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
            unavailable_action(
                "stop_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
            unavailable_action(
                "restart_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
        ];
    }

    if package_state != "enabled" {
        return vec![
            blocked_action(
                "start_package_entrypoint",
                "package_not_enabled",
                vec![DaemonPackageDiagnostic {
                    kind: "package_not_enabled".to_string(),
                    message: "enable the package before starting entrypoints".to_string(),
                }],
                Vec::new(),
            ),
            blocked_action(
                "stop_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "restart_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
        ];
    }

    let running = entrypoint.process.state == "running";
    let mut actions = Vec::new();
    if running {
        actions.push(unavailable_action(
            "start_package_entrypoint",
            "already_running",
            "entrypoint is already running",
        ));
        actions.push(available_package_action(
            "stop_package_entrypoint",
            request_for_entrypoint("stop_package_entrypoint", package_name, &entrypoint.id),
        ));
    } else {
        actions.push(available_package_action(
            "start_package_entrypoint",
            request_for_entrypoint("start_package_entrypoint", package_name, &entrypoint.id),
        ));
        actions.push(unavailable_action(
            "stop_package_entrypoint",
            "not_running",
            "entrypoint is not running",
        ));
    }
    actions.push(available_package_action(
        "restart_package_entrypoint",
        request_for_entrypoint("restart_package_entrypoint", package_name, &entrypoint.id),
    ));
    actions
}

fn update_status_actions(
    package_name: &str,
    pin: Option<&DaemonPackagePin>,
    has_pin: bool,
    source_metadata_present: bool,
    local_path_source: bool,
) -> Vec<DaemonPackageActionState> {
    let mut actions = vec![available_package_action(
        "check_package_update",
        request_for_package("check_package_update", package_name),
    )];
    if has_pin && source_metadata_present {
        actions.push(available_package_action(
            "preview_package_update",
            request_for_package_with_pin("preview_package_update", package_name, pin.cloned()),
        ));
        actions.push(available_package_action(
            "apply_package_update",
            request_for_package_with_pin("apply_package_update", package_name, pin.cloned()),
        ));
    } else {
        let reason = if source_metadata_present {
            "pin_required"
        } else {
            "source_metadata_required"
        };
        let references = if has_pin {
            Vec::new()
        } else {
            vec![DaemonPackageActionRequiredReference {
                kind: "pin".to_string(),
                key: "package_update_pin".to_string(),
            }]
        };
        actions.push(blocked_action(
            "preview_package_update",
            reason,
            Vec::new(),
            references.clone(),
        ));
        actions.push(blocked_action(
            "apply_package_update",
            reason,
            Vec::new(),
            references,
        ));
    }
    if local_path_source {
        actions.push(available_package_action(
            "reload_package",
            request_for_package("reload_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "reload_package",
            "local_path_required",
            "package reload is only available for local path packages",
        ));
    }
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));
    actions
}

fn package_required_references(
    package: &HubClientPackage,
) -> Vec<DaemonPackageActionRequiredReference> {
    let mut references = package
        .configuration
        .missing_required
        .iter()
        .map(|key| DaemonPackageActionRequiredReference {
            kind: "config".to_string(),
            key: key.clone(),
        })
        .collect::<Vec<_>>();
    for dependency in &package.dependency_availability {
        if matches!(dependency.state, HubClientPackageAvailabilityState::Blocked) {
            references.push(DaemonPackageActionRequiredReference {
                kind: "dependency".to_string(),
                key: dependency.package_name.clone(),
            });
        }
    }
    references
}

fn available_package_action(
    action_id: &str,
    request: Option<DaemonPackageActionRequest>,
) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Available,
        reason: None,
        diagnostics: Vec::new(),
        required_references: Vec::new(),
        request,
    }
}

fn blocked_action(
    action_id: &str,
    reason: &str,
    diagnostics: Vec<DaemonPackageDiagnostic>,
    required_references: Vec<DaemonPackageActionRequiredReference>,
) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Blocked,
        reason: Some(reason.to_string()),
        diagnostics,
        required_references,
        request: None,
    }
}

fn unavailable_action(action_id: &str, reason: &str, message: &str) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Unavailable,
        reason: Some(reason.to_string()),
        diagnostics: vec![DaemonPackageDiagnostic {
            kind: reason.to_string(),
            message: message.to_string(),
        }],
        required_references: Vec::new(),
        request: None,
    }
}

fn request_for_package(
    request_type: &str,
    package_name: &str,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin: None,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: None,
        registry_path: None,
    })
}

fn request_for_package_with_pin(
    request_type: &str,
    package_name: &str,
    pin: Option<DaemonPackagePin>,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: None,
        registry_path: None,
    })
}

fn request_for_entrypoint(
    request_type: &str,
    package_name: &str,
    entrypoint_id: &str,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin: None,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: Some(entrypoint_id.to_string()),
        registry_path: None,
    })
}

fn daemon_package_pin_from_policy(pin: PackagePin) -> DaemonPackagePin {
    DaemonPackagePin {
        revision: pin.revision,
        branch: pin.branch,
        tag: pin.tag,
        rev: pin.rev,
        checksum: pin.checksum,
        update_policy: package_update_policy_label(pin.update_policy).to_string(),
    }
}

fn daemon_availability_state(
    state: HubClientPackageAvailabilityState,
) -> DaemonPackageAvailabilityState {
    match state {
        HubClientPackageAvailabilityState::Available => DaemonPackageAvailabilityState::Available,
        HubClientPackageAvailabilityState::Blocked => DaemonPackageAvailabilityState::Blocked,
    }
}

fn daemon_availability_reason(
    reason: HubClientPackageAvailabilityReason,
) -> DaemonPackageAvailabilityReason {
    DaemonPackageAvailabilityReason {
        reason: reason.reason,
        action: reason.action,
        package_name: reason.package_name,
        capability: reason.capability.map(|capability| DaemonCapability {
            surface: capability.surface,
            scope: capability.scope,
        }),
        requirement: reason.requirement,
    }
}

fn package_update_status(
    daemon: &mut HubDaemon,
    package_name: &str,
    proposed_pin: Option<DaemonPackagePin>,
) -> DaemonTransportResult<DaemonPackageUpdateStatus> {
    let record = daemon
        .package_registry()
        .package(package_name)
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::CheckUpdate,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket check package update".to_string(),
            )
        })?;
    let source_metadata_present = record.source_metadata.is_some();
    let local_path_source = matches!(record.manifest.source, Some(PackageSource::Path { .. }));
    let existing_pin = record.pin.clone();
    let enabled = package_state_label(record.state.into()) == "enabled";
    let live_entrypoint = daemon
        .entrypoint_supervisor()
        .snapshots()
        .into_iter()
        .any(|snapshot| snapshot.package_name == package_name && snapshot.state == "running");
    let pin = proposed_pin.or_else(|| existing_pin.map(daemon_package_pin_from_policy));
    let mut diagnostics = Vec::new();

    if !source_metadata_present {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "update_unavailable".to_string(),
            message:
                "update resolution is unavailable for packages without registry source metadata"
                    .to_string(),
        });
    }
    if pin.is_none() {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "apply update requires explicit pinned source metadata".to_string(),
        });
    }
    if enabled && !local_path_source {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_unavailable".to_string(),
            message: "enabled package changes require an operator disable/enable cycle".to_string(),
        });
    } else if enabled {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_available".to_string(),
            message: "enabled local path package changes can be reloaded with reload_package"
                .to_string(),
        });
    }
    if live_entrypoint {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "restart_required".to_string(),
            message: "running package entrypoints must be restarted after update metadata changes"
                .to_string(),
        });
    }

    let has_pin = pin.is_some();
    let actions = update_status_actions(
        package_name,
        pin.as_ref(),
        has_pin,
        source_metadata_present,
        local_path_source,
    );
    Ok(DaemonPackageUpdateStatus {
        package_name: package_name.to_string(),
        update_available: has_pin && source_metadata_present,
        reload_required: enabled,
        restart_required: live_entrypoint,
        pin,
        diagnostics,
        actions,
    })
}

fn package_update_plan(
    daemon: &mut HubDaemon,
    package_name: &str,
    pin: DaemonPackagePin,
) -> DaemonTransportResult<DaemonPackageInstallPlan> {
    let diagnostics = package_update_status(daemon, package_name, Some(pin.clone()))?.diagnostics;
    let record = daemon
        .package_registry()
        .package(package_name)
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::PreviewUpdate,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket preview package update".to_string(),
            )
        })?;
    let source = record.source_metadata.as_ref();
    Ok(DaemonPackageInstallPlan {
        entry: DaemonAvailablePackage {
            entry_id: source
                .map(|source| source.entry_id.clone())
                .unwrap_or_else(|| package_name.to_string()),
            package_name: record.manifest.name.clone(),
            version: record.manifest.version.clone(),
            classification: package_classification_label(record.classification.into()).to_string(),
            source_kind: source
                .map(|source| registry_source_kind_label(source.source_kind).to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            source_label: source
                .map(|source| source.source_label.clone())
                .unwrap_or_else(|| "installed package has no registry source metadata".to_string()),
            first_party: record.trust.first_party,
            state: package_state_label(record.state.into()).to_string(),
            requested_capabilities: record
                .manifest
                .capabilities
                .iter()
                .cloned()
                .map(|capability| DaemonCapability {
                    surface: format!("{:?}", capability.surface),
                    scope: capability.scope,
                })
                .collect(),
            compatibility: DaemonPackageCompatibility {
                botster_requirement: record.compatibility.botster_requirement.clone(),
                result: package_compatibility_label(record.compatibility.result).to_string(),
                diagnostics: record.compatibility.diagnostics.clone(),
            },
            pin: Some(pin),
            actions: Vec::new(),
        },
        effects: vec![DaemonPackageInstallEffect {
            kind: "update_pin_metadata".to_string(),
            message: "would update pinned source metadata without fetching, enabling, or starting entrypoints"
                .to_string(),
        }],
        diagnostics,
        mutates_registry: false,
        starts_entrypoints: false,
    })
}

fn package_pin_from_daemon(pin: DaemonPackagePin) -> DaemonTransportResult<PackagePin> {
    let update_policy = match pin.update_policy.as_str() {
        "manual" => PackageUpdatePolicy::Manual,
        "track_source" => PackageUpdatePolicy::TrackSource,
        _ => {
            return Err(PackageRegistryError::without_record(
                "<package-update>",
                PackageAction::ApplyUpdate,
                PackageAdmissionReason::MissingPinRevision,
                "daemon socket apply package update".to_string(),
            )
            .into());
        }
    };
    Ok(PackagePin {
        revision: pin.revision,
        branch: pin.branch,
        tag: pin.tag,
        rev: pin.rev,
        checksum: pin.checksum,
        update_policy,
    })
}

fn daemon_package_decision_from_policy(decision: PackageDecision) -> DaemonPackageDecision {
    DaemonPackageDecision {
        package_name: decision.package_name,
        action: package_action_label(decision.action).to_string(),
        state: package_state_label(decision.state.into()).to_string(),
        classification: package_classification_label(decision.classification.into()).to_string(),
    }
}

fn daemon_plugin_lifecycle_from_client(
    lifecycle: HubClientPluginLifecycle,
) -> DaemonPluginLifecycle {
    DaemonPluginLifecycle {
        package_name: lifecycle.package_name,
        state: package_state_label(lifecycle.state).to_string(),
        loaded: lifecycle.loaded,
    }
}

fn daemon_plugin_worker_counters_from_client(
    counters: HubClientPluginWorkerCounters,
) -> DaemonPluginWorkerCounters {
    DaemonPluginWorkerCounters {
        configured_queue_capacity: counters.configured_queue_capacity,
        configured_executor_concurrency: counters.configured_executor_concurrency,
        live_plugin_executors: counters.live_plugin_executors,
        live_executor_workers: counters.live_executor_workers,
        queued_jobs: counters.queued_jobs,
        in_flight_jobs: counters.in_flight_jobs,
    }
}

fn daemon_status_from_status(
    status: &HubDaemonStatus,
    session_count: usize,
    diagnostics: Vec<DaemonDiagnostic>,
    lifecycle_counters: DaemonLifecycleCounters,
) -> DaemonStatus {
    DaemonStatus {
        lifecycle_state: match status.lifecycle_state {
            crate::HubDaemonState::Created => "created",
            crate::HubDaemonState::Running => "running",
            crate::HubDaemonState::Stopped => "stopped",
        }
        .to_string(),
        compatibility: DaemonCompatibility::current(),
        software: software_identity(),
        installation: installation_identity(),
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
        lifecycle_counters,
        diagnostics,
    }
}

fn daemon_session_from_client(session: HubClientSession) -> DaemonSession {
    DaemonSession {
        session_id: session.session_id.0,
        lifecycle: lifecycle_label(&session.lifecycle).to_string(),
    }
}

fn daemon_operator_error_from_client(error: crate::HubClientError) -> DaemonOperatorError {
    match error {
        crate::HubClientError::InvalidRequest {
            request_id,
            operation,
            message,
        } => DaemonOperatorError {
            code: "invalid_request".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            diagnostics: vec![DaemonDiagnostic::action_failure(
                operation_label(operation),
                &message,
            )],
            message,
        },
        crate::HubClientError::AdmissionDenied {
            request_id,
            operation,
            role,
        } => DaemonOperatorError {
            code: "admission_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{role:?} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Runtime {
            request_id,
            operation,
            kind,
        } => {
            let operation_label = operation_label(operation).to_string();
            let message = runtime_error_message(operation, kind);
            DaemonOperatorError {
                code: runtime_error_code(operation, kind).to_string(),
                request_id: request_id.0,
                diagnostics: runtime_error_diagnostics(operation, kind, &message),
                operation: operation_label,
                message,
            }
        }
        crate::HubClientError::PackageCapabilityDenied {
            request_id,
            operation,
            package_name,
        } => DaemonOperatorError {
            code: "package_capability_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{package_name} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::SessionType {
            request_id,
            operation,
            kind,
            message,
        } => DaemonOperatorError {
            code: kind.to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message,
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Plugin {
            request_id,
            operation,
            code,
            message,
        } => DaemonOperatorError {
            diagnostics: plugin_error_diagnostics(operation, &code, &message),
            code,
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message,
        },
    }
}

fn plugin_error_diagnostics(
    operation: crate::HubClientOperation,
    code: &str,
    message: &str,
) -> Vec<DaemonDiagnostic> {
    if matches!(
        code,
        "undeclared_plugin_surface" | "unsupported_plugin_surface_operation"
    ) {
        let feature = match operation {
            crate::HubClientOperation::PluginSurfaceRender => FEATURE_PLUGIN_SURFACE_RENDER,
            crate::HubClientOperation::PluginSurfaceAction => FEATURE_PLUGIN_SURFACE_ACTION,
            _ => return Vec::new(),
        };
        return vec![DaemonDiagnostic {
            kind: botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature,
            operation: Some(operation_label(operation).to_string()),
            feature: Some(feature.to_string()),
            message: Some(message.to_string()),
        }];
    }
    if operation == crate::HubClientOperation::PluginSurfaceRender && code == "invalid_surface" {
        return vec![DaemonDiagnostic::action_failure(
            operation_label(operation),
            message.to_string(),
        )];
    }

    Vec::new()
}

fn daemon_operator_error_from_package(error: crate::PackageRegistryError) -> DaemonOperatorError {
    let package_name = package_error_display_name(&error);
    let operation = package_action_label(error.action).to_string();
    let diagnostics = package_registry_error_diagnostics(&error, &operation);
    DaemonOperatorError {
        code: "package_policy_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: operation.clone(),
        message: format!(
            "package {} denied for {}: {:?}",
            package_name, operation, error.reason
        ),
        diagnostics,
    }
}

fn package_registry_error_diagnostics(
    error: &crate::PackageRegistryError,
    operation: &str,
) -> Vec<DaemonDiagnostic> {
    match &error.reason {
        PackageAdmissionReason::InvalidConfiguration(diagnostics) => diagnostics
            .iter()
            .map(|diagnostic| DaemonDiagnostic {
                kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
                operation: Some(operation.to_string()),
                feature: Some("package_registry".to_string()),
                message: Some(diagnostic.message.clone()),
            })
            .collect(),
        PackageAdmissionReason::MissingRequiredConfiguration(fields) => fields
            .iter()
            .map(|field| DaemonDiagnostic {
                kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
                operation: Some(operation.to_string()),
                feature: Some("package_registry".to_string()),
                message: Some(format!("required configuration field {field} is missing")),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn package_error_display_name(error: &crate::PackageRegistryError) -> &str {
    if error
        .audit_reason
        .contains("refresh local package registrations")
    {
        return &error.package_name;
    }
    match error.reason {
        PackageAdmissionReason::InvalidLocalManifest(_)
        | PackageAdmissionReason::UnsafeLocalPath(_) => "<local-package>",
        _ => &error.package_name,
    }
}

fn daemon_operator_error_from_state(error: crate::HubStateStoreError) -> DaemonOperatorError {
    DaemonOperatorError {
        code: "hub_state_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: "persist_package_registry".to_string(),
        message: format!("failed to persist package registry: {error}"),
        diagnostics: Vec::new(),
    }
}

fn daemon_operator_error_from_entrypoint(error: EntrypointSupervisorError) -> DaemonOperatorError {
    let (code, message) = match error {
        EntrypointSupervisorError::PackageNotInstalled(package_name) => (
            "package_not_installed",
            format!("package {package_name} is not installed"),
        ),
        EntrypointSupervisorError::PackageDisabled(package_name) => (
            "package_disabled",
            format!("package {package_name} is not enabled"),
        ),
        EntrypointSupervisorError::PackageNotLocal(package_name) => (
            "package_not_local",
            format!("package {package_name} is not a local package"),
        ),
        EntrypointSupervisorError::EntrypointNotFound {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_found",
            format!("package {package_name} has no runnable entrypoint {entrypoint_id}"),
        ),
        EntrypointSupervisorError::EntrypointNotSupervisable {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_supervisable",
            format!("package {package_name} entrypoint {entrypoint_id} is not marked supervisable"),
        ),
        EntrypointSupervisorError::ReadinessFailed {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_readiness_failed",
            format!(
                "package {package_name} entrypoint {entrypoint_id} exited before publishing structured readiness: {details}"
            ),
        ),
        EntrypointSupervisorError::ReadinessTimeout {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_readiness_timeout",
            format!(
                "package {package_name} entrypoint {entrypoint_id} did not publish structured readiness before the liveness deadline: {details}"
            ),
        ),
        EntrypointSupervisorError::LaunchContract {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_launch_contract_error",
            format!(
                "package {package_name} entrypoint {entrypoint_id} launch contract could not be resolved: {details}"
            ),
        ),
        EntrypointSupervisorError::Watch(message) => (
            "entrypoint_readiness_watch_error",
            format!("entrypoint launch-result watch failed: {message}"),
        ),
        EntrypointSupervisorError::Io(error) => (
            "entrypoint_io_error",
            format!("entrypoint process error: {error}"),
        ),
    };
    DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-package-entrypoint".to_string(),
        operation: "package_entrypoint".to_string(),
        message,
        diagnostics: Vec::new(),
    }
}

fn daemon_operator_error_from_local_webrtc(error: crate::LocalWebrtcError) -> DaemonOperatorError {
    let (code, message) = match error {
        crate::LocalWebrtcError::MissingGrant => (
            "local_webrtc_missing_grant",
            "local WebRTC bootstrap grant was not found".to_string(),
        ),
        crate::LocalWebrtcError::ExpiredGrant => (
            "local_webrtc_expired_grant",
            "local WebRTC bootstrap grant expired".to_string(),
        ),
        crate::LocalWebrtcError::RedeemedGrant => (
            "local_webrtc_redeemed_grant",
            "local WebRTC bootstrap grant was already redeemed".to_string(),
        ),
        crate::LocalWebrtcError::SecretMismatch => (
            "local_webrtc_secret_mismatch",
            "local WebRTC bootstrap grant secret mismatch".to_string(),
        ),
        crate::LocalWebrtcError::OriginMismatch => (
            "local_webrtc_origin_mismatch",
            "local WebRTC bootstrap origin mismatch".to_string(),
        ),
        crate::LocalWebrtcError::InvalidOffer(message) => (
            "local_webrtc_invalid_offer",
            format!("invalid local WebRTC offer: {message}"),
        ),
        crate::LocalWebrtcError::Random(message) => (
            "local_webrtc_random_failed",
            format!("local WebRTC random token failed: {message}"),
        ),
        crate::LocalWebrtcError::Webrtc(message) => (
            "local_webrtc_signaling_failed",
            format!("local WebRTC signaling failed: {message}"),
        ),
    };
    let diagnostic = DaemonDiagnostic::action_failure(WEBRTC_SIGNAL_OPERATION, message.clone());
    DaemonOperatorError {
        code: code.to_string(),
        request_id: WEBRTC_SIGNAL_OPERATION.to_string(),
        operation: WEBRTC_SIGNAL_OPERATION.to_string(),
        message,
        diagnostics: vec![diagnostic],
    }
}

fn daemon_event_from_client(event: HubClientEvent) -> DaemonEvent {
    match event {
        HubClientEvent::SessionLifecycle { session_id, state } => DaemonEvent::SessionLifecycle {
            session_id: session_id.0,
            state: lifecycle_label(&state).to_string(),
        },
        HubClientEvent::TerminalOutput {
            session_id,
            subscription_id,
            data,
        } => DaemonEvent::TerminalOutput {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            data: String::from_utf8_lossy(&data).to_string(),
        },
        HubClientEvent::Snapshot {
            session_id,
            subscription_id,
            data,
        } => DaemonEvent::Snapshot {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(&data),
        },
        HubClientEvent::Scrollback {
            session_id,
            subscription_id,
            data,
        } => DaemonEvent::Scrollback {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(&data),
        },
        HubClientEvent::ProcessExit {
            session_id,
            subscription_id,
            code,
        } => DaemonEvent::ProcessExit {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            code,
        },
        HubClientEvent::AttachState {
            session_id,
            subscription_id,
            state,
        } => DaemonEvent::AttachState {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            state: attach_state_label(&state).to_string(),
        },
        HubClientEvent::RuntimeObservation { kind } => DaemonEvent::RuntimeObservation {
            kind: match kind {
                crate::HubClientObservationKind::SessionActivity => "session_activity",
                crate::HubClientObservationKind::Subscription => "subscription",
                crate::HubClientObservationKind::Backpressure => "backpressure",
                crate::HubClientObservationKind::RoutedEnvelope => "routed_envelope",
            }
            .to_string(),
        },
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

fn runtime_error_code(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
) -> &'static str {
    match (operation, kind) {
        (_, crate::HubClientRuntimeErrorKind::UnknownSession) => "unknown_session",
        (_, crate::HubClientRuntimeErrorKind::SessionAlreadyExists) => "session_already_exists",
        (_, crate::HubClientRuntimeErrorKind::SpawnFailed)
        | (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::Runtime) => {
            "spawn_failed"
        }
        (_, crate::HubClientRuntimeErrorKind::Runtime) => "runtime_error",
        (_, crate::HubClientRuntimeErrorKind::State) => "state_error",
    }
}

fn runtime_error_message(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
) -> String {
    match (operation, kind) {
        (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::SessionAlreadyExists) => {
            "spawn rejected because a session with that id already exists".to_string()
        }
        (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::SpawnFailed)
        | (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::Runtime) => {
            "spawn failed before the session started; verify the configured session worker and command"
                .to_string()
        }
        _ => format!("runtime failed while handling {operation:?}: {kind:?}"),
    }
}

fn runtime_error_diagnostics(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
    message: &str,
) -> Vec<DaemonDiagnostic> {
    if matches!(operation, crate::HubClientOperation::Spawn) {
        match kind {
            crate::HubClientRuntimeErrorKind::SessionAlreadyExists => {
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    "spawn rejected because a session with that id already exists",
                )];
            }
            crate::HubClientRuntimeErrorKind::SpawnFailed => {
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    "spawn failed before the session started; verify the configured session worker and command",
                )];
            }
            crate::HubClientRuntimeErrorKind::Runtime => {
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    "spawn failed before the session started; verify the configured session worker and command",
                )];
            }
            _ => {}
        }
    }

    if kind == crate::HubClientRuntimeErrorKind::UnknownSession
        && matches!(
            operation,
            crate::HubClientOperation::Attach | crate::HubClientOperation::DrainRuntime
        )
    {
        return vec![DaemonDiagnostic::terminal_stream_unavailable(
            operation_label(operation),
            message,
        )];
    }

    Vec::new()
}

fn operation_label(operation: crate::HubClientOperation) -> &'static str {
    match operation {
        crate::HubClientOperation::Status => "status",
        crate::HubClientOperation::ListSessions => "list_sessions",
        crate::HubClientOperation::SubscribeEntities => "subscribe_entities",
        crate::HubClientOperation::UnsubscribeEntities => "unsubscribe_entities",
        crate::HubClientOperation::RemoveSession => "remove_session",
        crate::HubClientOperation::Spawn => "spawn",
        crate::HubClientOperation::Attach => "attach",
        crate::HubClientOperation::Detach => "detach",
        crate::HubClientOperation::Input => "input",
        crate::HubClientOperation::Resize => "resize",
        crate::HubClientOperation::DrainRuntime => "drain_runtime",
        crate::HubClientOperation::Shutdown => "shutdown",
        crate::HubClientOperation::GuardedNotificationWrite => "guarded_notification_write",
        crate::HubClientOperation::NotifySession => "notify_session",
        crate::HubClientOperation::PublishRoutedEnvelope => "publish_routed_envelope",
        crate::HubClientOperation::DrainRoutedEnvelopes => "drain_routed_envelopes",
        crate::HubClientOperation::AcknowledgeRoutedEnvelope => "acknowledge_routed_envelope",
        crate::HubClientOperation::ReadScreen => "read_screen",
        crate::HubClientOperation::ReadModeFlags => "read_mode_flags",
        crate::HubClientOperation::CaptureSnapshot => "capture_snapshot",
        crate::HubClientOperation::ListPackages => "list_packages",
        crate::HubClientOperation::ListPackageNavigation => "list_package_navigation",
        crate::HubClientOperation::ListSessionTypes => "list_session_types",
        crate::HubClientOperation::ListSessionTypesForTarget => "list_session_types_for_target",
        crate::HubClientOperation::ShowSessionType => "show_session_type",
        crate::HubClientOperation::ShowSessionTypeDefinition => "show_session_type_definition",
        crate::HubClientOperation::CreateSessionType => "create_session_type",
        crate::HubClientOperation::UpdateSessionType => "update_session_type",
        crate::HubClientOperation::DeleteSessionType => "delete_session_type",
        crate::HubClientOperation::ResolveSessionType => "resolve_session_type",
        crate::HubClientOperation::SpawnSessionType => "spawn_session_type",
        crate::HubClientOperation::ReadSessionContext => "read_session_context",
        crate::HubClientOperation::PluginLifecycleStatus => "plugin_lifecycle_status",
        crate::HubClientOperation::PluginSurfaceRender => "plugin_surface_render",
        crate::HubClientOperation::PluginSurfaceAction => "plugin_surface_action",
    }
}

fn envelope_target_label(target: &EnvelopeTarget) -> String {
    match target {
        EnvelopeTarget::Endpoint { endpoint_id } => format!("endpoint:{}", endpoint_id.0),
        EnvelopeTarget::Client { client_id } => format!("client:{}", client_id.0),
        EnvelopeTarget::Session { session_id } => format!("session:{}", session_id.0),
        EnvelopeTarget::Subscription {
            session_id,
            subscription_id,
        } => format!("subscription:{}:{}", session_id.0, subscription_id.0),
        EnvelopeTarget::Plugin { plugin_key } => format!("plugin:{}", plugin_key.0),
        EnvelopeTarget::Stream { stream } => format!("stream:{stream}"),
        EnvelopeTarget::Topic { topic } => format!("topic:{topic}"),
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

fn runnable_entrypoint_kind_label(kind: &RunnableEntrypointKind) -> &'static str {
    match kind {
        RunnableEntrypointKind::WebApp => "web_app",
        RunnableEntrypointKind::TerminalApp => "terminal_app",
    }
}

fn runnable_launch_mode_label(mode: &RunnableEntrypointLaunchMode) -> &'static str {
    match mode {
        RunnableEntrypointLaunchMode::Background => "background",
        RunnableEntrypointLaunchMode::ForegroundStdio => "foreground_stdio",
    }
}

fn runnable_process_state_label(state: &RunnableEntrypointProcessState) -> &'static str {
    match state {
        RunnableEntrypointProcessState::NotStarted => "not_started",
        RunnableEntrypointProcessState::Running => "running",
        RunnableEntrypointProcessState::Exited => "exited",
        RunnableEntrypointProcessState::Failed => "failed",
    }
}

fn available_package_state_label(state: AvailablePackageState) -> &'static str {
    match state {
        AvailablePackageState::Available => "available",
        AvailablePackageState::Installed => "installed",
        AvailablePackageState::Enabled => "enabled",
        AvailablePackageState::Disabled => "disabled",
    }
}

fn registry_source_kind_label(kind: PackageRegistryEntrySourceKind) -> &'static str {
    match kind {
        PackageRegistryEntrySourceKind::LocalPath => "local_path",
        PackageRegistryEntrySourceKind::Git => "git",
    }
}

fn package_compatibility_label(result: PackageCompatibilityResult) -> &'static str {
    match result {
        PackageCompatibilityResult::Compatible => "compatible",
        PackageCompatibilityResult::Incompatible => "incompatible",
        PackageCompatibilityResult::InvalidRequirement => "invalid_requirement",
    }
}

fn package_update_policy_label(policy: PackageUpdatePolicy) -> &'static str {
    match policy {
        PackageUpdatePolicy::Manual => "manual",
        PackageUpdatePolicy::TrackSource => "track_source",
    }
}

fn guarded_write_delivery_state_label(state: GuardedWriteDeliveryState) -> &'static str {
    match state {
        GuardedWriteDeliveryState::Accepted => "accepted",
        GuardedWriteDeliveryState::Deferred => "deferred",
        GuardedWriteDeliveryState::Rejected => "rejected",
        GuardedWriteDeliveryState::Written => "written",
        GuardedWriteDeliveryState::Delivered => "delivered",
        GuardedWriteDeliveryState::Acknowledged => "acknowledged",
    }
}

fn package_action_label(action: PackageAction) -> &'static str {
    match action {
        PackageAction::Install => "install",
        PackageAction::Show => "show",
        PackageAction::Configure => "configure",
        PackageAction::Reload => "reload",
        PackageAction::Enable => "enable",
        PackageAction::Disable => "disable",
        PackageAction::Remove => "remove",
        PackageAction::CheckUpdate => "check_update",
        PackageAction::PreviewUpdate => "preview_update",
        PackageAction::ApplyUpdate => "apply_update",
        PackageAction::Pin => "pin",
        PackageAction::Prepare => "prepare",
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
    Compatibility(botster_hub_client::DaemonCompatibilityError),
    UnexpectedResponse,
    DaemonNotRunning,
    ControlThreadStopped,
    Io(std::io::Error),
    Json(serde_json::Error),
    Daemon(crate::HubDaemonError),
    Client(crate::HubClientError),
    Package(crate::PackageRegistryError),
    SpawnTarget(SpawnTargetError),
    Worktree(WorktreeError),
    State(crate::HubStateStoreError),
    Entrypoint(EntrypointSupervisorError),
    LocalWebrtc(crate::LocalWebrtcError),
    Runtime(crate::HubRuntimeError),
    Lifecycle(crate::HubLifecycleError),
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketBinding => write!(formatter, "local socket transport is disabled"),
            Self::NotRunning => write!(formatter, "daemon not running"),
            Self::AlreadyRunning => write!(formatter, "daemon already running"),
            Self::ClientDisconnected => write!(formatter, "client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::Compatibility(error) => write!(formatter, "{error}"),
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::DaemonNotRunning => write!(formatter, "daemon runtime is not running"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "{error:?}"),
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::SpawnTarget(error) => write!(formatter, "{error}"),
            Self::Worktree(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Entrypoint(error) => write!(formatter, "{error:?}"),
            Self::LocalWebrtc(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error:?}"),
            Self::Lifecycle(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Compatibility(error) => Some(error),
            Self::Daemon(error) => Some(error),
            Self::LocalWebrtc(error) => Some(error),
            Self::SpawnTarget(error) => Some(error),
            Self::Worktree(error) => Some(error),
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

impl From<ClientDaemonTransportError> for DaemonTransportError {
    fn from(error: ClientDaemonTransportError) -> Self {
        match error {
            ClientDaemonTransportError::Io(error) => Self::Io(error),
            ClientDaemonTransportError::Json(error) => Self::Json(error),
            ClientDaemonTransportError::MissingSocketBinding => Self::MissingSocketBinding,
            ClientDaemonTransportError::AlreadyRunning => Self::AlreadyRunning,
            ClientDaemonTransportError::NotRunning => Self::NotRunning,
            ClientDaemonTransportError::ClientDisconnected => Self::ClientDisconnected,
            ClientDaemonTransportError::Protocol(message) => Self::Protocol(message),
            ClientDaemonTransportError::Compatibility(error) => Self::Compatibility(error),
            ClientDaemonTransportError::ControlThreadStopped => Self::ControlThreadStopped,
        }
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

impl From<SpawnTargetError> for DaemonTransportError {
    fn from(error: SpawnTargetError) -> Self {
        Self::SpawnTarget(error)
    }
}

impl From<WorktreeError> for DaemonTransportError {
    fn from(error: WorktreeError) -> Self {
        Self::Worktree(error)
    }
}

impl From<crate::HubStateStoreError> for DaemonTransportError {
    fn from(error: crate::HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<EntrypointSupervisorError> for DaemonTransportError {
    fn from(error: EntrypointSupervisorError) -> Self {
        Self::Entrypoint(error)
    }
}

impl From<crate::LocalWebrtcError> for DaemonTransportError {
    fn from(error: crate::LocalWebrtcError) -> Self {
        Self::LocalWebrtc(error)
    }
}

impl From<crate::HubRuntimeError> for DaemonTransportError {
    fn from(error: crate::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<crate::HubLifecycleError> for DaemonTransportError {
    fn from(error: crate::HubLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Result alias for daemon socket transport operations.
pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;

#[cfg(test)]
fn receive_test_control_message(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded test receive runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timed out waiting for daemon control message")
            .expect("daemon control sender remains live")
    })
}

#[cfg(test)]
fn receive_test_control_reply(
    receiver: oneshot::Receiver<DaemonTransportResult<DaemonResponse>>,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded test reply runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("timed out waiting for daemon control reply")
            .expect("daemon control reply sender remains live")
    })
}

#[cfg(test)]
fn handle_connection(stream: UnixStream, control_tx: ControlSender) -> DaemonTransportResult<()> {
    stream
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;
    let (cleanup_tx, cleanup_rx) = mpsc::sync_channel(1);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DaemonTransportError::Io)?;
    let stream = {
        let _runtime = runtime.enter();
        TokioUnixStream::from_std(stream).map_err(DaemonTransportError::Io)?
    };
    let result = runtime.block_on(handle_connection_async(
        stream,
        control_tx.clone(),
        cleanup_tx,
        shutdown_rx,
    ));
    if let Ok(cleanup) = cleanup_rx.try_recv() {
        for subscription in cleanup.attached_subscriptions {
            let (reply_tx, reply_rx) = oneshot::channel();
            if control_tx
                .blocking_send(ControlMessage::Request {
                    request: Box::new(DaemonRequest::Detach {
                        session_id: subscription.session_id,
                        subscription_id: subscription.subscription_id,
                    }),
                    reply_tx,
                    response_delivery_rx: None,
                    grant_id: None,
                })
                .is_ok()
            {
                let _ = receive_test_control_reply(reply_rx);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Shutdown;

    #[test]
    fn session_lifecycle_class_is_total_and_stale_first() {
        let concrete = [
            (SessionLifecycleState::Starting, "current"),
            (SessionLifecycleState::Running, "current"),
            (SessionLifecycleState::Stopping, "current"),
            (SessionLifecycleState::Exited { code: Some(0) }, "ended"),
            (
                SessionLifecycleState::Failed {
                    reason: "failed".to_string(),
                },
                "ended",
            ),
        ];
        for (lifecycle, expected) in &concrete {
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Running, Some(lifecycle)),
                *expected
            );
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Stale, Some(lifecycle)),
                "indeterminate"
            );
        }
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Running, None),
            "indeterminate"
        );
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Stale, None),
            "indeterminate"
        );
    }

    #[test]
    fn session_entity_patch_explicitly_updates_required_lifecycle_class() {
        let entity = |registry_state: &str, lifecycle: Option<&str>, lifecycle_class: &str| {
            DaemonSessionEntity {
                session_uuid: "session-1".to_string(),
                registry_state: registry_state.to_string(),
                lifecycle: lifecycle.map(str::to_string),
                lifecycle_class: lifecycle_class.to_string(),
                rows: 24,
                cols: 80,
                updated_at: 1,
                exit_code: None,
                failure_reason: None,
                session_type_id: None,
                session_type_source: None,
                role: None,
                traits: Vec::new(),
                interaction: None,
                session_type_lifecycle: None,
            }
        };
        let current = entity("running", Some("running"), "current");
        let ended = entity("exited", Some("exited"), "ended");
        let no_lifecycle = entity("running", None, "indeterminate");
        let stale = entity("stale", Some("running"), "indeterminate");

        assert_eq!(
            session_entity_patch(&current, &ended)["lifecycle_class"],
            "ended"
        );
        assert_eq!(
            session_entity_patch(&current, &no_lifecycle)["lifecycle_class"],
            "indeterminate"
        );
        assert_eq!(
            session_entity_patch(&current, &stale)["lifecycle_class"],
            "indeterminate"
        );
    }

    #[test]
    fn live_session_entity_subscription_emits_exact_stale_transition_patch() {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-stale-transition-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "stale-transition-test".to_string(),
                display_name: "Stale Transition Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build stale transition config");
        let mut daemon = HubDaemon::start(config).expect("start stale transition daemon");
        let session_id = SessionId("stale-transition-session".to_string());
        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .spawn_session(
                botster_core::SessionSpawnRequest {
                    request_id: RequestId("stale-transition-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sh".to_string(),
                    arguments: vec![
                        "-c".to_string(),
                        "while IFS= read -r line; do printf '%s\\n' \"$line\"; done".to_string(),
                    ],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
                1,
            )
            .expect("spawn worker-backed session");

        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        let (sender, receiver) = mpsc::sync_channel(4);
        let response = register_entity_subscription(
            &mut daemon,
            &mut state,
            "session".to_string(),
            "stale-transition-subscription".to_string(),
            EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register entity subscription");
        assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
        assert!(matches!(
            receiver.recv().expect("initial current snapshot"),
            DaemonEntityFrame::Snapshot { ref items, .. }
                if items.iter().any(|entity| {
                    entity.get("session_uuid").and_then(Value::as_str) == Some(&session_id.0)
                        && entity.get("registry_state").and_then(Value::as_str) == Some("running")
                        && entity.get("lifecycle").and_then(Value::as_str) == Some("running")
                        && entity.get("lifecycle_class").and_then(Value::as_str) == Some("current")
                })
        ));

        daemon
            .runtime()
            .expect("runtime initialized")
            .mark_session_stale(&session_id, 2)
            .expect("mark live session stale through core daemon");
        drive_entity_subscriptions(&mut daemon, &mut state);
        assert!(matches!(
            receiver.recv().expect("stale transition patch"),
            DaemonEntityFrame::Patch {
                ref id,
                ref patch,
                ..
            } if id == &session_id.0
                && patch == &serde_json::json!({
                    "registry_state": "stale",
                    "lifecycle_class": "indeterminate",
                    "updated_at": 2
                })
        ));

        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .shutdown_session(session_id, 3)
            .expect("stop worker-backed test session");
        daemon.stop();
        let _ = fs::remove_dir_all(data_directory);
    }

    #[test]
    fn due_reconciliation_precedes_an_already_ready_control_message() {
        let (control_tx, mut control_rx) = tokio_mpsc::channel(1);
        control_tx
            .try_send(ControlMessage::RejectedConnection)
            .expect("prefill owner control queue");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build owner event test runtime");

        assert!(matches!(
            runtime.block_on(receive_owner_event(&mut control_rx, Duration::ZERO)),
            OwnerEvent::Reconcile
        ));
        let OwnerEvent::Control(message) =
            runtime.block_on(receive_owner_event(&mut control_rx, Duration::from_secs(1)))
        else {
            panic!("ready control message must win before a future reconciliation deadline");
        };
        assert!(matches!(*message, Some(ControlMessage::RejectedConnection)));
    }

    #[test]
    fn read_mode_flags_runtime_failure_projects_operator_error_without_default_body() {
        let response = daemon_operator_error(crate::HubClientError::Runtime {
            request_id: RequestId("mode-flags-backend-failure".to_string()),
            operation: crate::HubClientOperation::ReadModeFlags,
            kind: crate::HubClientRuntimeErrorKind::Runtime,
        });

        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        assert!(response.mode_flags.is_none());
        let error = response.error.expect("operator error body");
        assert_eq!(error.code, "runtime_error");
        assert_eq!(error.operation, "read_mode_flags");
    }

    #[test]
    fn connection_cleanup_ignores_only_an_already_removed_session() {
        let unknown_session = Err(DaemonTransportError::Client(
            crate::HubClientError::Runtime {
                request_id: RequestId("cleanup-detach".to_string()),
                operation: crate::HubClientOperation::Detach,
                kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            },
        ));
        assert!(!cleanup_detach_failed(&unknown_session));

        let unavailable_runtime: DaemonTransportResult<DaemonResponse> =
            Err(DaemonTransportError::DaemonNotRunning);
        assert!(cleanup_detach_failed(&unavailable_runtime));
        assert!(cleanup_detach_failed(&Ok(entity_subscription_error(
            "detach_failed",
            "cleanup-detach",
            "detach failed",
        ))));
    }

    #[test]
    fn client_eof_detaches_connection_subscriptions() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        write_frame(
            &mut client,
            &DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
        )
        .expect("write attach request");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_message(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(daemon_events(Vec::new())))
            .expect("reply to attach request");
        let _: DaemonResponse = read_frame(&mut client).expect("read attach response");

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_message(&mut control_rx)
        else {
            panic!("expected detach control request");
        };
        assert!(matches!(
            *request,
            DaemonRequest::Detach {
                ref session_id,
                ref subscription_id,
            } if session_id == "session" && subscription_id == "subscription"
        ));
        reply_tx
            .send(Ok(daemon_events(Vec::new())))
            .expect("reply to disconnect detach request");

        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
    }

    #[test]
    fn daemon_event_projection_round_trips_opaque_history_bytes_without_loss() {
        let session_id = SessionId("daemon-projection-session".to_string());
        let subscription_id = SubscriptionId("daemon-projection-subscription".to_string());
        let snapshot = daemon_event_from_client(HubClientEvent::Snapshot {
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            data: vec![b's', b'n', b'a', b'p', 0xff],
        });
        let scrollback = daemon_event_from_client(HubClientEvent::Scrollback {
            session_id,
            subscription_id,
            data: b"scrollback".to_vec(),
        });

        assert_eq!(
            snapshot,
            DaemonEvent::Snapshot {
                session_id: "daemon-projection-session".to_string(),
                subscription_id: "daemon-projection-subscription".to_string(),
                history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(&[
                    b's', b'n', b'a', b'p', 0xff,
                ]),
            }
        );
        assert_eq!(
            scrollback,
            DaemonEvent::Scrollback {
                session_id: "daemon-projection-session".to_string(),
                subscription_id: "daemon-projection-subscription".to_string(),
                history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(b"scrollback"),
            }
        );
    }

    #[test]
    fn daemon_egress_diagnostics_classify_terminal_and_control_backpressure() {
        let terminal = daemon_events(vec![DaemonEvent::TerminalOutput {
            session_id: "session-redacted".to_string(),
            subscription_id: "subscription-redacted".to_string(),
            data: "private terminal payload".to_string(),
        }]);
        let control = daemon_response_base(DaemonResponseKind::Sessions);

        assert_eq!(
            daemon_delivery_kind(&terminal),
            DaemonDeliveryKind::Terminal
        );
        assert_eq!(daemon_delivery_kind(&control), DaemonDeliveryKind::Control);

        let mut diagnostics = DaemonEgressDiagnostics::default();
        let mut counters = DaemonLifecycleCounters::default();
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            daemon_delivery_kind(&terminal),
        );
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            daemon_delivery_kind(&control),
        );
        let rows = diagnostics.diagnostics();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Backpressure
                && diagnostic.operation.as_deref() == Some("daemon_client_egress")
        }));
        let debug = format!("{rows:?}");
        assert!(debug.contains("terminal"));
        assert!(debug.contains("control"));
        assert!(!debug.contains("private terminal payload"));
        assert!(!debug.contains("session-redacted"));
        assert!(!debug.contains("subscription-redacted"));
        assert_eq!(counters.stalled_writes, 2);
    }

    #[test]
    fn daemon_shutdown_waits_for_response_delivery_before_stopping() {
        let (completed_delivery_tx, completed_delivery_rx) = mpsc::channel();
        completed_delivery_tx
            .send(())
            .expect("pre-signal completed shutdown response delivery");
        assert!(
            wait_for_response_delivery(true, true, Some(completed_delivery_rx)),
            "shutdown response delivery must pass through the wait enforcement seam"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        let (response_delivery_tx, response_delivery_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();

        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        let response = receive_test_control_reply(reply_rx).expect("shutdown response succeeds");
        assert_eq!(response.kind, DaemonResponseKind::Shutdown);
        assert!(
            stopped_rx.try_recv().is_err(),
            "daemon must remain alive until the transport attempts delivery"
        );

        response_delivery_tx
            .send(())
            .expect("report shutdown response delivery attempt");
        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("daemon stop decision follows delivery attempt")
        );
    }

    #[test]
    fn daemon_shutdown_releases_when_delivery_owner_drops() {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (response_delivery_tx, response_delivery_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();

        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        let _ = receive_test_control_reply(reply_rx);
        drop(response_delivery_tx);

        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("dropped delivery owner releases daemon stop")
        );
    }

    #[test]
    fn daemon_shutdown_releases_when_response_receiver_drops() {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (_response_delivery_tx, response_delivery_rx) = mpsc::channel();
        drop(reply_rx);

        assert!(send_control_response(
            reply_tx,
            Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
            Some(response_delivery_rx),
        ));
    }

    #[test]
    fn daemon_shutdown_write_failure_releases_stop_and_preserves_error() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let server_control = server.try_clone().expect("clone daemon server socket");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");
        write_frame(&mut client, &DaemonRequest::DaemonShutdown)
            .expect("write daemon shutdown request");

        let ControlMessage::Request {
            request,
            reply_tx,
            response_delivery_rx,
            grant_id,
        } = receive_test_control_message(&mut control_rx)
        else {
            panic!("expected shutdown control request");
        };
        assert!(matches!(*request, DaemonRequest::DaemonShutdown));
        assert_eq!(
            grant_id, None,
            "socket path must leave Request grant_id unset"
        );
        let response_delivery_rx = response_delivery_rx.expect("shutdown has delivery receiver");
        server_control
            .shutdown(Shutdown::Write)
            .expect("fail daemon shutdown response write");
        let (stopped_tx, stopped_rx) = mpsc::channel();
        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        assert!(
            connection.join().expect("join daemon connection").is_err(),
            "failed shutdown response write remains a transport error"
        );
        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("failed response delivery releases daemon stop")
        );
    }

    #[test]
    fn entity_overflow_requires_empty_snapshot_resync_and_failed_delivery_disconnects() {
        let fixture =
            botster_hub_test_support::session_lifecycle_subscription_conformance_scenario();
        let overflow_reason = fixture.overflow.resync_reason.clone();
        assert!(fixture.overflow.empty_snapshot_valid);
        assert!(fixture.overflow.snapshot_precedes_later_deltas);
        assert!(
            fixture
                .overflow
                .failed_snapshot_delivery_closes_subscription
        );
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            })
            .expect("fill bounded subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: Some(overflow_reason.clone()),
            owner_grant_id: None,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str())
        );
        let _ = receiver.recv().expect("drain stale queued frame");
        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.recv().expect("receive empty resync snapshot"),
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }

    #[test]
    fn session_type_resync_replaces_oversized_snapshot_with_typed_error() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut subscriptions = BTreeMap::from([(
            "oversized-session-types".to_string(),
            EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session_type".to_string(),
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 1,
                definition_entities: BTreeMap::new(),
                resync_reason: Some("subscriber_overflow".to_string()),
                owner_grant_id: None,
            },
        )]);
        let entities = BTreeMap::from([(
            "device/oversized".to_string(),
            serde_json::json!({ "description": "x".repeat(DAEMON_MAX_FRAME_BYTES) }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 2, &entities);

        assert!(
            subscriptions.is_empty(),
            "typed error closes the subscription"
        );
        assert!(matches!(
            receiver.recv().expect("receive bounded typed error"),
            DaemonEntityFrame::Error {
                ref subscription_id,
                ref entity_type,
                ref code,
                ..
            } if subscription_id == "oversized-session-types"
                && entity_type == "session_type"
                && code == "entity_provider_frame_too_large"
        ));
    }

    #[test]
    fn async_entity_overflow_requires_empty_snapshot_resync_and_closed_delivery_disconnects() {
        let overflow_reason = "subscriber_overflow".to_string();
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "async-subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            })
            .expect("fill bounded async subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Async(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: Some(overflow_reason.clone()),
            owner_grant_id: None,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str()),
            "a full production WebRTC queue must retain its pending resync"
        );
        let _ = receiver.try_recv().expect("drain stale async frame");
        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.try_recv().expect("receive async resync snapshot"),
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }
}
