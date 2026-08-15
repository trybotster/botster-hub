//! Same-device daemon socket transport for the thin operator CLI.
//!
//! This module is a framing adapter over `HubClientApi`. The daemon owns one
//! mutable `HubRuntime` on the accept/control thread; socket threads submit discrete
//! requests and never hold runtime access while writing to a client.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    ClientId, EndpointId, EnvelopeCursor, EnvelopeDeliveryState, EnvelopeId, EnvelopeTarget,
    PackageSource, RequestId, RoutedEnvelope, RoutedEnvelopePayload, RunnableEntrypointKind,
    RunnableEntrypointLaunchMode, SessionId, SessionLifecycleState, SubscriptionId,
    TerminalCapabilitySet,
};
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence, RegistrySessionState,
};
use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
pub use botster_hub_client::{
    DaemonApp, DaemonAppLaunchTarget, DaemonAvailablePackage, DaemonCapability,
    DaemonCaptureSnapshot, DaemonCompatibility, DaemonConnection as ClientDaemonConnection,
    DaemonCoordination, DaemonDiagnostic, DaemonEndpoint, DaemonEntityFrame, DaemonEnvelope,
    DaemonEnvelopeAck, DaemonEnvelopeDelivery, DaemonEnvelopePublish, DaemonEvent, DaemonHello,
    DaemonHelloAck, DaemonHubUpdate, DaemonHubUpdateExecution, DaemonHubUpdateExecutionState,
    DaemonHubUpdateScope, DaemonHubUpdateState, DaemonIdentity, DaemonInstallationDiagnostic,
    DaemonInstallationIdentity, DaemonInstallationMode, DaemonLifecycleCounters,
    DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap, DaemonModeFlags, DaemonNotify,
    DaemonOperatorError, DaemonPackage, DaemonPackageActionRequest,
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
    DaemonSessionTypeDefinition, DaemonSessionTypeEditableDefinition, DaemonSessionTypeExecution,
    DaemonSessionTypeMutationSource, DaemonSessionTypeRequest, DaemonSessionTypeWorkingDirectory,
    DaemonSoftwareIdentity, DaemonSpawnTarget, DaemonSpawnTargetValidation, DaemonStatus,
    DaemonUiTreeSnapshot, DaemonWorktree, DaemonWorktreeGitMetadata, DaemonWorktreeLifecycleEvent,
    FEATURE_PLUGIN_SURFACE_ACTION, FEATURE_PLUGIN_SURFACE_RENDER, PROTOCOL, read_frame,
    read_frame_from_reader, write_frame,
};
use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};
use botster_ui_contract::{UiActionResult, UiActionResultState};
use serde_json::Value;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc as tokio_mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::daemon_projection::{
    app_local_url, apps_from_registry, available_package_action, available_package_actions,
    blocked_action, daemon_operator_error_from_client, daemon_operator_error_from_package,
    daemon_status_from_status, package_action_label, package_navigation_entries,
    package_route_descriptors, package_state_label, request_for_entrypoint, request_for_package,
    request_for_package_with_pin, runnable_entrypoint_kind_label, runnable_launch_mode_label,
    unavailable_action,
};
use crate::local_webrtc::{
    LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE, LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES,
    LocalWebrtcAttachedSubscription, LocalWebrtcSenderTerminalRecord, LocalWebrtcSignalRequest,
};
use crate::maintenance::{
    HubUpdateCheckPlan, execute_managed_update_check, installation_identity, plan_hub_update_check,
    software_identity,
};
use crate::packages::{PackageResolvedEntrypointLaunch, resolve_entrypoint_launch_contract};
use crate::source_update::{current_update_execution, mark_update_failed, start_update_handoff};
use crate::unix_terminal_adapter::{UnixConnectionMux, UnixTerminalAdapterHandle};
use crate::webrtc_terminal_adapter::WebRtcConnectionMux;
use crate::{
    AvailablePackage, AvailablePackageState, FileHubStateStore, HubClientApi,
    HubClientCaptureSnapshot, HubClientEvent, HubClientModeFlags, HubClientPackage,
    HubClientPackageAvailabilityReason, HubClientPackageAvailabilityState,
    HubClientPackageClassification, HubClientPackageNavigationEntry, HubClientPluginLifecycle,
    HubClientPluginLifecycleReport, HubClientPluginSurface, HubClientPluginWorkerCounters,
    HubClientReadScreen, HubClientRequest, HubClientResponseBody, HubClientSession, HubConfig,
    HubDaemon, HubDaemonStatus, HubStateStore, McpToolDescriptor, PackageAction,
    PackageAdmissionReason, PackageCompatibilityResult, PackageDecision, PackageInstallPlan,
    PackagePin, PackageRegistry, PackageRegistryEntrySourceKind, PackageRegistryError,
    PackageSessionType, PackageSessionTypeWorkingDirectory, PackageState, PackageUpdatePolicy,
    ResolvedSessionType, SessionTypeContextInput, SessionTypeMutationSource, SessionTypeRequest,
    resolve_foreground_launch_contract,
};
use crate::{EntrypointProcessSnapshot, EntrypointSupervisorError};
use crate::{
    SpawnTarget, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate, SpawnTargetValidation,
};
use crate::{Worktree, WorktreeCreate, WorktreeError};

#[path = "daemon_attach_stream.rs"]
mod daemon_attach_stream;
use daemon_attach_stream::{
    AttachStreamOwner, AttachStreamRegistry, BoundAdapterHandle, UnixBindRequest,
    WebrtcBindRequest, bind_unix_adapter_after_attaching, bind_webrtc_adapter_after_attaching,
    fail_closed_pre_bind_attach, forward_attach_bootstrap, live_generation_for_route,
};
pub(crate) use daemon_attach_stream::{
    hello_requires_terminal_subscription_closed, negotiated_unix_capability_set,
};

#[path = "daemon_package_control.rs"]
mod daemon_package_control;
use daemon_package_control::{
    apply_package_update, configure_package, disable_package, enable_package,
    enable_package_local_path, install_local_package, install_registry_package,
    refresh_local_packages, reload_package, remove_package,
};

#[path = "daemon_entity_subscriptions.rs"]
mod daemon_entity_subscriptions;
pub(crate) use daemon_entity_subscriptions::{EntityFrameSender, EntitySubscriptionState};
use daemon_entity_subscriptions::{
    EntityReconciliationState, drive_entity_subscriptions, drive_package_entity_fanout,
    drive_package_entity_resync, entity_subscription_error, register_entity_subscription,
    seed_lifecycle_reconciliation,
};

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
static NEXT_SOCKET_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

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
        let event = match control_rx.try_recv() {
            Ok(message) => OwnerEvent::Control(Box::new(Some(message))),
            Err(_) => {
                let wait = control_state
                    .next_reconciliation
                    .saturating_duration_since(Instant::now());
                transport_runtime.block_on(receive_owner_event(&mut control_rx, wait))
            }
        };
        match event {
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
                    pump_bound_unix_routes(&mut daemon, &mut control_state);
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
        &daemon_hello_ack(vec![DaemonDiagnostic::backpressure(
            "daemon_connection_admission",
            "daemon connection capacity reached",
        )]),
    )
    .await;
}

async fn handle_connection_async(
    stream: TokioUnixStream,
    control_tx: ControlSender,
    cleanup_tx: SyncSender<ConnectionCleanup>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> DaemonTransportResult<()> {
    let client_id = format!(
        "botster-hub-daemon-socket-{}",
        NEXT_SOCKET_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
    );
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    let mut cleanup = ConnectionCleanupGuard::new(
        cleanup_tx,
        client_id.clone(),
        ConnectionTerminalReason::Protocol,
    );
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
    let (admission, hello_ack) = unix_hello_admission(&hello);
    if let Err(error) = write_async_frame(&mut write_half, &hello_ack).await {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        return Err(error);
    }
    cleanup.set_reason(ConnectionTerminalReason::Eof);

    let mut mux_write = MuxWriteState::default();
    let mux = match &admission {
        UnixTerminalAdmission::Admitted { mux, .. } => mux.clone(),
        UnixTerminalAdmission::Rejected { .. } => UnixConnectionMux::new(),
    };
    control_tx
        .send(ControlMessage::RegisterUnixAdmission {
            client_id: client_id.clone(),
            admission,
        })
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;

    loop {
        let request = tokio::select! {
            request = read_async_frame::<DaemonRequest, _>(&mut reader, None) => request,
            _ = mux.notify().notified() => {
                mux.clear_deferred_flushes();
                if let Err(error) = flush_unix_mux_writes(&mut write_half, &mux, &mut mux_write).await {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
            _ = tokio::time::sleep(Duration::from_millis(25)), if mux.has_unsent_mux_writes() || mux_write.has_pending() => {
                mux.clear_deferred_flushes();
                if let Err(error) = flush_unix_mux_writes(&mut write_half, &mux, &mut mux_write).await {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
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
            if unix_mux_blocks_entity_subscription(&mux, &mux_write) {
                mux_write.enqueue_response(&entity_subscription_mux_busy_error(), None, false)?;
                if let Err(error) =
                    flush_pending_responses(&mut write_half, &mux, &mut mux_write, Instant::now())
                        .await
                {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
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
        let requires_delivery_ack =
            close_after_response || matches!(request, DaemonRequest::StartHubUpdate { .. });
        let (response_delivery_tx, response_delivery_rx) = if requires_delivery_ack {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let ownership_request = request.clone();
        control_tx
            .send(ControlMessage::Request {
                request: Box::new(request),
                reply_tx,
                response_delivery_rx,
                grant_id: None,
                client_id: Some(client_id.clone()),
            })
            .await
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        let response = receive_control_response(reply_rx).await?;
        cleanup.apply_subscription_change(attached_subscription_change_for_response(
            &ownership_request,
            &response,
        ));
        mux_write.enqueue_response(&response, response_delivery_tx, close_after_response)?;
        debug_assert_eq!(mux_write.pending_response_count(), 1);
        if let Err(error) =
            flush_pending_responses(&mut write_half, &mux, &mut mux_write, Instant::now()).await
        {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
                delivery_kind: daemon_delivery_kind(&response),
            });
            mux.close_all();
            return Err(error);
        }
        mux.clear_deferred_flushes();
        if let Err(error) = flush_unix_mux_writes(&mut write_half, &mux, &mut mux_write).await {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            mux.close_all();
            return Err(error);
        }
        if close_after_response {
            debug_assert!(!mux_write.has_close_after_pending());
            cleanup.set_reason(ConnectionTerminalReason::NormalClose);
            return Ok(());
        }
    }
}

#[derive(Default)]
struct MuxWriteState {
    pending: Option<PendingMuxFrame>,
    queued_responses: VecDeque<PendingMuxFrame>,
}

impl MuxWriteState {
    fn has_pending(&self) -> bool {
        self.pending.is_some() || !self.queued_responses.is_empty()
    }

    fn has_close_after_pending(&self) -> bool {
        self.pending.as_ref().is_some_and(|frame| frame.close_after)
            || self.queued_responses.iter().any(|frame| frame.close_after)
    }

    fn has_pending_response(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|frame| frame.class == PendingMuxClass::Response)
            || !self.queued_responses.is_empty()
    }

    fn pending_response_count(&self) -> usize {
        let pending =
            self.pending
                .as_ref()
                .is_some_and(|frame| frame.class == PendingMuxClass::Response) as usize;
        pending + self.queued_responses.len()
    }

    fn enqueue_response(
        &mut self,
        response: &DaemonResponse,
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
    ) -> DaemonTransportResult<()> {
        self.queued_responses.push_back(serialize_mux_frame(
            response,
            None,
            PendingMuxClass::Response,
            delivery_ack,
            close_after,
        )?);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMuxClass {
    Terminal,
    Event,
    Response,
}

struct PendingMuxFrame {
    bytes: Vec<u8>,
    offset: usize,
    complete_envelope: Option<UnixTerminalAdapterHandle>,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    close_after: bool,
    backpressured: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum MuxWrite {
    Written,
    Pending,
}

fn unix_mux_blocks_entity_subscription(
    mux: &UnixConnectionMux,
    write_state: &MuxWriteState,
) -> bool {
    write_state.has_pending() || mux.has_unsent_mux_writes() || mux.has_bound_routes()
}

fn entity_subscription_mux_busy_error() -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "unix_mux_owns_connection".to_string(),
        request_id: "daemon-subscribe-entities".to_string(),
        operation: "subscribe_entities".to_string(),
        message: "entity subscription cannot start while the Unix mux owns this connection"
            .to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "subscribe_entities",
            "unix mux still owns bound routes or unsent frames",
        )],
    });
    response
}

async fn flush_pending_responses(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    mux: &UnixConnectionMux,
    write_state: &mut MuxWriteState,
    started: Instant,
) -> DaemonTransportResult<()> {
    loop {
        flush_unix_mux_writes(writer, mux, write_state).await?;
        if !write_state.has_pending_response() {
            return Ok(());
        }
        if started.elapsed() >= DAEMON_CLIENT_WRITE_TIMEOUT {
            return Err(DaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon client write deadline elapsed",
            )));
        }
    }
}

async fn flush_unix_mux_writes(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    mux: &UnixConnectionMux,
    write_state: &mut MuxWriteState,
) -> DaemonTransportResult<()> {
    abandon_zero_offset_terminal_for_response(write_state);
    if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
        return Ok(());
    }
    while let Some(frame) = write_state.queued_responses.pop_front() {
        write_state.pending = Some(frame);
        if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
            return Ok(());
        }
    }
    while let Some(event) = mux.pop_pending_event() {
        write_state.pending = Some(serialize_mux_frame(
            &event,
            None,
            PendingMuxClass::Event,
            None,
            false,
        )?);
        if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
            return Ok(());
        }
    }
    for (session_id, subscription_id, handle, bytes) in mux.snapshot_writes() {
        if handle.is_closed() {
            continue;
        }
        let envelope = botster_hub_client::DaemonUnixTerminalEnvelope::from_frame_bytes(
            session_id,
            subscription_id,
            &bytes,
        );
        write_state.pending = Some(serialize_mux_frame(
            &envelope,
            Some(handle),
            PendingMuxClass::Terminal,
            None,
            false,
        )?);
        if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
            return Ok(());
        }
    }
    Ok(())
}

fn serialize_mux_frame<T: serde::Serialize>(
    frame: &T,
    complete_envelope: Option<UnixTerminalAdapterHandle>,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    close_after: bool,
) -> DaemonTransportResult<PendingMuxFrame> {
    let mut bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    bytes.push(b'\n');
    Ok(PendingMuxFrame {
        bytes,
        offset: 0,
        complete_envelope,
        class,
        delivery_ack,
        close_after,
        backpressured: false,
    })
}

fn abandon_zero_offset_terminal_for_response(write_state: &mut MuxWriteState) {
    let should_abandon = write_state.pending.as_ref().is_some_and(|pending| {
        pending.class == PendingMuxClass::Terminal
            && pending.offset == 0
            && !write_state.queued_responses.is_empty()
    });
    if should_abandon {
        abandon_pending_terminal(write_state);
    }
}

fn abandon_pending_terminal(write_state: &mut MuxWriteState) {
    if let Some(pending) = write_state.pending.take()
        && let Some(handle) = pending.complete_envelope
    {
        handle.defer_flush();
    }
}

async fn resume_pending_mux_write(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    write_state: &mut MuxWriteState,
) -> DaemonTransportResult<MuxWrite> {
    let Some(pending) = write_state.pending.as_mut() else {
        return Ok(MuxWrite::Written);
    };
    match write_frame_bytes_resumable(writer, pending).await? {
        MuxWrite::Written => {
            let pending = write_state.pending.take().expect("pending mux frame");
            if let Some(delivery_ack) = pending.delivery_ack {
                let _ = delivery_ack.send(());
            }
            if let Some(handle) = pending.complete_envelope {
                if pending.backpressured {
                    handle.defer_flush();
                }
                if !handle.is_closed() {
                    let _ = handle.complete_active();
                }
            }
            Ok(MuxWrite::Written)
        }
        MuxWrite::Pending => {
            if pending.class == PendingMuxClass::Terminal && pending.offset == 0 {
                abandon_pending_terminal(write_state);
                return Ok(MuxWrite::Written);
            }
            pending.backpressured = true;
            Ok(MuxWrite::Pending)
        }
    }
}

async fn write_frame_bytes_resumable(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    pending: &mut PendingMuxFrame,
) -> DaemonTransportResult<MuxWrite> {
    while pending.offset < pending.bytes.len() {
        match tokio::time::timeout(
            Duration::from_millis(50),
            writer.write(&pending.bytes[pending.offset..]),
        )
        .await
        {
            Ok(Ok(0)) => {
                return Err(DaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "unix mux write returned zero bytes",
                )));
            }
            Ok(Ok(written)) => pending.offset += written,
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(MuxWrite::Pending);
            }
            Ok(Err(error)) => return Err(DaemonTransportError::Io(error)),
            Err(_) => return Ok(MuxWrite::Pending),
        }
    }
    Ok(MuxWrite::Written)
}

#[cfg(test)]
mod mux_write_resume_tests {
    use super::{
        MuxWrite, MuxWriteState, PendingMuxClass, PendingMuxFrame, daemon_response_base,
        entity_subscription_mux_busy_error, flush_pending_responses, flush_unix_mux_writes,
        unix_mux_blocks_entity_subscription, write_frame_bytes_resumable,
    };
    use crate::unix_terminal_adapter::{UnixConnectionMux, UnixTerminalAdapter};
    use botster_core::contract::terminal_adapter::{TerminalAdapter, TerminalAdapterPressure};
    use botster_hub_client::{
        DaemonEvent, DaemonResponseKind, DaemonUnixTerminalEnvelope,
        TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER, parse_unix_mux_value,
    };
    use botster_terminal_protocol::TerminalFrame;
    use std::io;
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};
    use tokio::io::AsyncWrite;

    struct PrefixStallWriter {
        written: Vec<u8>,
        stall_after: usize,
        allow_remainder: bool,
    }

    impl AsyncWrite for PrefixStallWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let room = this.stall_after.saturating_sub(this.written.len());
            if room == 0 && !this.allow_remainder {
                return Poll::Pending;
            }
            let take = if this.allow_remainder {
                buf.len()
            } else {
                room.min(buf.len())
            };
            if take == 0 {
                return Poll::Pending;
            }
            this.written.extend_from_slice(&buf[..take]);
            Poll::Ready(Ok(take))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn closed_event() -> DaemonEvent {
        DaemonEvent::TerminalSubscriptionClosed {
            session_id: "session".to_string(),
            subscription_id: "sub".to_string(),
            generation: 2,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER.to_string(),
        }
    }

    fn frame_bytes(event: &DaemonEvent) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(event).expect("serialize");
        bytes.push(b'\n');
        bytes
    }

    #[tokio::test]
    async fn resumable_mux_write_keeps_offset_and_emits_one_valid_frame() {
        let event = closed_event();
        let expected = frame_bytes(&event);
        let prefix = 8.min(expected.len() - 1);
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: prefix,
            allow_remainder: false,
        };
        let mut pending = PendingMuxFrame {
            bytes: expected.clone(),
            offset: 0,
            complete_envelope: None,
            class: PendingMuxClass::Event,
            delivery_ack: None,
            close_after: false,
            backpressured: false,
        };

        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        assert_eq!(pending.offset, prefix);
        assert_eq!(writer.written, expected[..prefix]);

        writer.allow_remainder = true;
        let second = write_frame_bytes_resumable(&mut writer, &mut pending)
            .await
            .expect("resume write");
        assert!(matches!(second, MuxWrite::Written));
        assert_eq!(writer.written, expected);
        assert_eq!(
            writer.written.iter().filter(|byte| **byte == b'\n').count(),
            1
        );
        let line = std::str::from_utf8(&writer.written)
            .expect("utf8")
            .trim_end();
        let parsed = parse_unix_mux_value(serde_json::from_str(line).expect("json"))
            .expect("classify mux frame");
        match parsed {
            botster_hub_client::DaemonUnixMuxFrame::Event(
                DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    generation,
                    reason,
                    ..
                },
            ) => {
                assert_eq!(session_id, "session");
                assert_eq!(generation, 2);
                assert_eq!(reason, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER);
            }
            other => panic!("expected one close event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resumable_mux_write_does_not_start_a_second_frame_while_first_is_pending() {
        let first_bytes = frame_bytes(&closed_event());
        let second_bytes = frame_bytes(&DaemonEvent::TerminalSubscriptionClosed {
            session_id: "other".to_string(),
            subscription_id: "sub-2".to_string(),
            generation: 3,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER.to_string(),
        });
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 4,
            allow_remainder: false,
        };
        let mut pending = PendingMuxFrame {
            bytes: first_bytes.clone(),
            offset: 0,
            complete_envelope: None,
            class: PendingMuxClass::Event,
            delivery_ack: None,
            close_after: false,
            backpressured: false,
        };
        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        assert_eq!(writer.written, first_bytes[..4]);
        assert_ne!(writer.written, [first_bytes.clone(), second_bytes].concat());
        assert_eq!(pending.bytes, first_bytes);
    }

    fn occupy_route(
        mux: &UnixConnectionMux,
        session_id: &str,
        subscription_id: &str,
        marker: &str,
    ) -> UnixTerminalAdapter {
        let (mut adapter, handle) = mux.create_adapter();
        mux.register(
            session_id.to_string(),
            subscription_id.to_string(),
            1,
            handle,
        );
        let payload = format!(r#"{{"type":"terminal_output","marker":"{marker}"}}"#);
        let frame = TerminalFrame::from_bytes(payload.as_bytes()).expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        adapter
    }

    fn parse_written_mux_lines(written: &[u8]) -> Vec<botster_hub_client::DaemonUnixMuxFrame> {
        written
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                parse_unix_mux_value(serde_json::from_slice(line).expect("json line"))
                    .expect("classify mux frame")
            })
            .collect()
    }

    #[tokio::test]
    async fn abandoned_zero_progress_terminal_retries_the_original_frame() {
        let mux = UnixConnectionMux::new();
        let stall = occupy_route(&mux, "stall", "sub", "flood");
        assert_eq!(mux.snapshot_writes().len(), 1);

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 0,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("zero-progress terminal start is abandoned");
        assert_eq!(
            stall.pressure(),
            TerminalAdapterPressure::Full,
            "abandon must keep the original adapter frame"
        );
        assert!(
            mux.snapshot_writes().is_empty(),
            "deferred flush omits the frame only for this pass"
        );

        mux.clear_deferred_flushes();
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("retry the original deferred frame");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            lines
                .iter()
                .any(|line| matches!(line, botster_hub_client::DaemonUnixMuxFrame::Terminal(_))),
            "the original flood frame must still be delivered, lines={lines:?}"
        );
    }

    #[tokio::test]
    async fn partial_terminal_then_response_parses_two_complete_mux_lines() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let terminal = mux.snapshot_writes();
        let prefix = 8.min(terminal[0].3.len().saturating_add(16));
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: prefix,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("first flush");
        assert!(write_state.pending.is_some());
        assert!(write_state.pending.as_ref().is_some_and(|pending| {
            pending.class == PendingMuxClass::Terminal && pending.offset == prefix
        }));

        let response = daemon_response_base(DaemonResponseKind::Status);
        write_state
            .enqueue_response(&response, None, false)
            .expect("enqueue status");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("resume flush");
        assert!(!write_state.has_pending());
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2, "expected two complete mux lines");
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(DaemonUnixTerminalEnvelope {
                ref session_id,
                ..
            }) if session_id == "stall"
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Status
        ));
    }

    #[tokio::test]
    async fn zero_progress_terminal_start_is_abandoned_without_completing_slot() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 0,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("abandon flush");
        assert!(writer.written.is_empty());
        assert!(write_state.pending.is_none());
        assert!(mux.snapshot_writes().is_empty());
        let _sibling = occupy_route(&mux, "sibling", "sub-live", "live");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("sibling flush");
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(DaemonUnixTerminalEnvelope {
                ref session_id,
                ..
            }) if session_id == "sibling"
        ));
    }

    #[tokio::test]
    async fn host_event_flushes_before_new_terminal_slots() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let (mut closer, close_handle) = mux.create_adapter();
        mux.register(
            "closing".to_string(),
            "sub-close".to_string(),
            1,
            close_handle.clone(),
        );
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"close"}"#)
            .expect("opaque frame");
        assert_eq!(closer.try_write(&frame), Ok(()));
        close_handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("host-first flush");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            matches!(
                lines.first(),
                Some(botster_hub_client::DaemonUnixMuxFrame::Event(
                    DaemonEvent::TerminalSubscriptionClosed { session_id, .. }
                )) if session_id == "closing"
            ),
            "host Event must precede new terminal slots: {lines:?}"
        );
    }

    #[tokio::test]
    async fn partial_terminal_then_shutdown_response_acks_after_written() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("partial terminal");
        let (ack_tx, ack_rx) = mpsc::channel();
        write_state
            .enqueue_response(
                &daemon_response_base(DaemonResponseKind::Shutdown),
                Some(ack_tx),
                true,
            )
            .expect("enqueue shutdown");
        assert!(ack_rx.try_recv().is_err());
        assert!(write_state.has_close_after_pending());
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("cannot finish while stalled");
        assert!(ack_rx.try_recv().is_err());
        assert!(write_state.has_close_after_pending());
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("finish close-after");
        ack_rx.try_recv().expect("ack after complete shutdown line");
        assert!(!write_state.has_close_after_pending());
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(_)
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Shutdown
        ));
    }

    #[tokio::test]
    async fn partial_terminal_then_update_response_acks_after_written() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("partial terminal");
        let (ack_tx, ack_rx) = mpsc::channel();
        write_state
            .enqueue_response(
                &daemon_response_base(DaemonResponseKind::HubUpdate),
                Some(ack_tx),
                false,
            )
            .expect("enqueue update");
        assert!(ack_rx.try_recv().is_err());
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("still pending");
        assert!(ack_rx.try_recv().is_err());
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("finish update");
        ack_rx.try_recv().expect("ack after complete update line");
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::HubUpdate
        ));
    }

    #[tokio::test]
    async fn stalled_response_stays_bounded_and_blocks_entity_subscription() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("partial terminal");
        write_state
            .enqueue_response(
                &daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state)
            .await
            .expect("response remains pending");
        assert!(write_state.has_pending_response());
        assert_eq!(write_state.pending_response_count(), 1);
        assert!(
            write_state.has_pending(),
            "entity subscription must not start while a mux frame is pending"
        );
        let timed_out = flush_pending_responses(
            &mut writer,
            &mux,
            &mut write_state,
            Instant::now() - Duration::from_secs(3),
        )
        .await;
        assert!(
            timed_out.is_err(),
            "a stalled Response must not return until Written or timeout"
        );
        assert_eq!(write_state.pending_response_count(), 1);
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_pending_responses(&mut writer, &mux, &mut write_state, Instant::now())
            .await
            .expect("finish the one pending Response");
        assert!(!write_state.has_pending_response());
        assert!(
            unix_mux_blocks_entity_subscription(&mux, &write_state),
            "a bound Unix route must still block the entity-subscription handoff"
        );
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(_)
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Status
        ));
        assert!(!write_state.has_pending());
    }

    #[tokio::test]
    async fn bound_route_or_queued_event_blocks_entity_subscription_without_closing_routes() {
        let mux = UnixConnectionMux::new();
        let idle = MuxWriteState::default();
        assert!(!unix_mux_blocks_entity_subscription(&mux, &idle));

        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        assert!(mux.has_bound_routes());
        assert!(unix_mux_blocks_entity_subscription(&mux, &idle));
        let handle = mux.snapshot_writes()[0].2.clone();
        assert!(!handle.host_closed());

        handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(mux.has_unsent_mux_writes());
        assert!(unix_mux_blocks_entity_subscription(&mux, &idle));
        assert!(
            !handle.host_closed(),
            "rejecting SubscribeEntities must not host-close the bound route"
        );
        assert!(mux.has_bound_routes());

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        write_state
            .enqueue_response(&entity_subscription_mux_busy_error(), None, false)
            .expect("enqueue reject");
        flush_pending_responses(&mut writer, &mux, &mut write_state, Instant::now())
            .await
            .expect("write reject");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            matches!(
                lines.first(),
                Some(botster_hub_client::DaemonUnixMuxFrame::Response(response))
                    if response.kind == DaemonResponseKind::OperatorError
                        && response.error.as_ref().is_some_and(|error| {
                            error.code == "unix_mux_owns_connection"
                        })
            ),
            "reject must be an OperatorError Response: {lines:?}"
        );
        assert!(
            matches!(
                lines.get(1),
                Some(botster_hub_client::DaemonUnixMuxFrame::Event(
                    DaemonEvent::TerminalSubscriptionClosed { session_id, .. }
                )) if session_id == "stall"
            ),
            "close Event must still flush after the reject: {lines:?}"
        );
        assert!(mux.has_bound_routes());
        assert!(!handle.host_closed());
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
    client_id: String,
    attached_subscriptions: Vec<AttachedSubscription>,
    entity_subscription_ids: BTreeSet<String>,
    reason: ConnectionTerminalReason,
}

struct ConnectionCleanupGuard {
    cleanup_tx: SyncSender<ConnectionCleanup>,
    cleanup: Option<ConnectionCleanup>,
}

impl ConnectionCleanupGuard {
    fn new(
        cleanup_tx: SyncSender<ConnectionCleanup>,
        client_id: String,
        reason: ConnectionTerminalReason,
    ) -> Self {
        Self {
            cleanup_tx,
            cleanup: Some(ConnectionCleanup {
                client_id,
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
    if let Some(UnixTerminalAdmission::Admitted { mux, .. }) = state
        .pending_runtime
        .unix_admissions
        .remove(&cleanup.client_id)
    {
        mux.close_all();
    }
    let bound_claims = state
        .pending_runtime
        .take_connection_bound_routes(&cleanup.client_id);
    state
        .pending_runtime
        .close_adapters_for_client(&cleanup.client_id);
    let mut bound_subscriptions = BTreeSet::new();
    for claim in bound_claims {
        if !state.pending_runtime.connection_bound_route_still_owned(
            &cleanup.client_id,
            &claim.session_id,
            &claim.subscription_id,
            claim.generation,
        ) {
            continue;
        }
        bound_subscriptions.insert((claim.session_id.clone(), claim.subscription_id.clone()));
        state
            .pending_runtime
            .cancel_stream(&claim.session_id, &claim.subscription_id);
        state
            .live_attach_routes
            .remove(&(claim.session_id, claim.subscription_id));
    }
    if !bound_subscriptions.is_empty() {
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry("bound_adapter_close".to_string())
            .or_insert(0) += bound_subscriptions.len() as u64;
    }
    for subscription in cleanup.attached_subscriptions {
        if state
            .pending_runtime
            .stream_owner_client_id(&subscription.session_id, &subscription.subscription_id)
            .is_some_and(|owner| owner != cleanup.client_id)
        {
            continue;
        }
        let bound = bound_subscriptions.contains(&(
            subscription.session_id.clone(),
            subscription.subscription_id.clone(),
        ));
        let result = if bound {
            state
                .pending_runtime
                .close_adapter(&subscription.session_id, &subscription.subscription_id);
            state
                .pending_runtime
                .cancel_stream(&subscription.session_id, &subscription.subscription_id);
            state.live_attach_routes.remove(&(
                subscription.session_id.clone(),
                subscription.subscription_id.clone(),
            ));
            Ok(daemon_events(Vec::new()))
        } else {
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("cleanup_hub_detach".to_string())
                .or_insert(0) += 1;
            handle_control_request(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                    client_id: Some(&cleanup.client_id),
                    grant_id: None,
                },
                control_tx.clone(),
                DaemonRequest::Detach {
                    session_id: subscription.session_id,
                    subscription_id: subscription.subscription_id,
                },
            )
        };
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
        ControlMessage::RegisterUnixAdmission {
            client_id,
            admission,
        } => {
            state
                .pending_runtime
                .unix_admissions
                .insert(client_id, admission);
            false
        }
        ControlMessage::RegisterWebrtcAdmission {
            grant_id,
            admission,
        } => {
            if daemon.local_webrtc().has_live_peer(&grant_id) {
                state
                    .pending_runtime
                    .webrtc_admissions
                    .insert(grant_id, admission);
            }
            false
        }
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
            client_id,
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
            if let DaemonRequest::StartHubUpdate { scope } = request.as_ref() {
                let data_directory = match daemon.runtime() {
                    Some(runtime) => runtime.config().data_directory.clone(),
                    None => {
                        return send_control_response(
                            reply_tx,
                            Ok(hub_update_execution_error(
                                "hub_update_runtime_unavailable",
                                "start_hub_update",
                                "the Hub runtime is not available",
                            )),
                            response_delivery_rx,
                        );
                    }
                };
                return match start_update_handoff(&data_directory, *scope) {
                    Ok((execution, handoff)) => {
                        let update_id = execution.update_id.clone();
                        let response_received = reply_tx
                            .send(Ok(daemon_hub_update_execution(execution)))
                            .is_ok();
                        wait_for_response_delivery(
                            response_received,
                            response_received,
                            response_delivery_rx,
                        );
                        if response_received {
                            if let Err(error) = handoff.release() {
                                let _ = mark_update_failed(&data_directory, &update_id, &error);
                            }
                        } else {
                            handoff.stop();
                            let _ = mark_update_failed(
                                &data_directory,
                                &update_id,
                                "client disconnected before update handoff",
                            );
                        }
                        false
                    }
                    Err(error) => send_control_response(
                        reply_tx,
                        Ok(hub_update_execution_error(
                            if error.contains("already active") {
                                "hub_update_busy"
                            } else {
                                "hub_update_start_failed"
                            },
                            "start_hub_update",
                            &error,
                        )),
                        response_delivery_rx,
                    ),
                };
            }
            if matches!(request.as_ref(), DaemonRequest::GetHubUpdateExecution) {
                let response = match daemon.runtime() {
                    Some(runtime) => {
                        match current_update_execution(&runtime.config().data_directory) {
                            Ok(Some(execution)) => daemon_hub_update_execution(execution),
                            Ok(None) => hub_update_execution_error(
                                "hub_update_execution_not_found",
                                "get_hub_update_execution",
                                "no Hub update execution record exists",
                            ),
                            Err(error) => hub_update_execution_error(
                                "hub_update_execution_read_failed",
                                "get_hub_update_execution",
                                &error,
                            ),
                        }
                    }
                    None => hub_update_execution_error(
                        "hub_update_runtime_unavailable",
                        "get_hub_update_execution",
                        "the Hub runtime is not available",
                    ),
                };
                return send_control_response(reply_tx, Ok(response), response_delivery_rx);
            }
            let request = *request;
            let drain_owned_before = match &request {
                DaemonRequest::Drain {
                    session_id,
                    subscription_id: Some(subscription_id),
                } => state
                    .pending_runtime
                    .stream_owner_client_id(session_id, subscription_id)
                    .is_some(),
                _ => false,
            };
            let reconcile_after_request = matches!(
                request,
                DaemonRequest::Spawn { .. }
                    | DaemonRequest::Attach { .. }
                    | DaemonRequest::Resize { .. }
                    | DaemonRequest::SendInput { .. }
                    | DaemonRequest::ModeGatedInput { .. }
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
                    client_id: client_id.as_deref(),
                    grant_id: grant_id.as_deref(),
                },
                control_tx,
                request.clone(),
            )
            .or_else(|error| match error {
                DaemonTransportError::Client(error) => Ok(daemon_operator_error(error)),
                DaemonTransportError::Package(error) => Ok(daemon_package_error(error)),
                DaemonTransportError::SpawnTarget(error) => Ok(daemon_spawn_target_error(error)),
                DaemonTransportError::Worktree(error) => Ok(daemon_worktree_error(error)),
                DaemonTransportError::State(error) => Ok(daemon_state_error(error)),
                DaemonTransportError::Entrypoint(error) => Ok(daemon_entrypoint_error(error)),
                DaemonTransportError::LocalWebrtc(error) => Ok(daemon_local_webrtc_error(error)),
                error @ DaemonTransportError::PackageCompensation { .. } => {
                    Ok(daemon_package_compensation_error(error))
                }
                error @ DaemonTransportError::SnapshotStreamForbidden { .. } => {
                    Ok(daemon_snapshot_stream_forbidden_error(error))
                }
                error => Err(error),
            });
            if matches!(request, DaemonRequest::Detach { .. })
                && response
                    .as_ref()
                    .is_ok_and(|response| response.kind != DaemonResponseKind::OperatorError)
            {
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("explicit_detach".to_string())
                    .or_insert(0) += 1;
            }
            if let Some(runtime) = daemon.runtime() {
                queue_unix_subscription_closed_events(runtime, &state.pending_runtime);
                queue_webrtc_subscription_closed_events(runtime, &state.pending_runtime);
            }
            if let Ok(response) = response.as_ref() {
                let change = attached_subscription_change_for_response(&request, response);
                let change = match change {
                    Some(AttachedSubscriptionChange::Detach(_))
                        if matches!(request, DaemonRequest::Drain { .. })
                            && !drain_owned_before =>
                    {
                        None
                    }
                    change => change,
                };
                record_attached_subscription_change(state, change, grant_id.as_deref());
            }
            if reconcile_after_request {
                pump_bound_unix_routes(daemon, state);
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
            // Reply first so surface-action publish can return before fanout delivery.
            // Attach writes `attaching` before Core attach work; Drain advances the stream.
            let reply_result = send_control_response(reply_tx, response, response_delivery_rx);
            drive_package_entity_fanout(daemon, state);
            drive_package_entity_resync(daemon, state);
            reply_result
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
            // Occupancy set is the counter source of truth. PeerClosed must release
            // live_attach_routes here so a replacement Attach can become live.
            for subscription in &detach_list {
                record_attached_subscription_change(
                    state,
                    Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                        session_id: subscription.session_id.clone(),
                        subscription_id: subscription.subscription_id.clone(),
                    })),
                    None,
                );
            }
            let mut bound_detach = Vec::new();
            let mut unbound_detach = Vec::new();
            for subscription in detach_list {
                if state
                    .pending_runtime
                    .is_adapter_bound(&subscription.session_id, &subscription.subscription_id)
                {
                    bound_detach.push(subscription);
                } else {
                    unbound_detach.push(subscription);
                }
            }
            if !bound_detach.is_empty() {
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("bound_adapter_close".to_string())
                    .or_insert(0) += bound_detach.len() as u64;
            }
            for grant_id in &removed_grants {
                state.pending_runtime.close_adapters_for_grant(grant_id);
            }
            for subscription in &bound_detach {
                state
                    .pending_runtime
                    .cancel_stream(&subscription.session_id, &subscription.subscription_id);
            }
            for grant_id in &removed_grants {
                state.pending_runtime.webrtc_admissions.remove(grant_id);
            }
            // Residual same-grant index rows can survive a no-op Core Detach. Drop them
            // after occupancy release. Preserve replacement owners.
            state
                .pending_runtime
                .attach_owner_grant_ids
                .retain(|_, owner| !removed_grants.contains(owner.as_str()));
            detach_local_webrtc_subscriptions(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                control_tx,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                    client_id: None,
                    grant_id: None,
                },
                unbound_detach,
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
    client_id: Option<&'a str>,
    grant_id: Option<&'a str>,
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
        } => install_registry_package(daemon, registry_path, entry_id),
        DaemonRequest::PluginLifecycleStatus => plugin_lifecycle_response(daemon),
        DaemonRequest::InstallPackageLocalPath { path } => install_local_package(daemon, path),
        DaemonRequest::CheckPackageUpdate { package_name } => {
            check_package_update_response(daemon, &package_name)
        }
        DaemonRequest::PreviewPackageUpdate { package_name, pin } => {
            preview_package_update_response(daemon, &package_name, pin)
        }
        DaemonRequest::ApplyPackageUpdate { package_name, pin } => {
            apply_package_update(daemon, package_name, pin)
        }
        DaemonRequest::ShowPackage { package_name } => show_package_response(daemon, &package_name),
        DaemonRequest::SetPackageConfiguration {
            package_name,
            values,
        } => configure_package(daemon, package_name, values),
        DaemonRequest::ReloadPackage { package_name } => reload_package(daemon, package_name),
        DaemonRequest::RefreshLocalPackages => refresh_local_packages(daemon),
        DaemonRequest::EnablePackageLocalPath { path } => enable_package_local_path(daemon, path),
        DaemonRequest::EnablePackage { package_name } => enable_package(daemon, package_name),
        DaemonRequest::DisablePackage { package_name } => disable_package(daemon, package_name),
        DaemonRequest::RemovePackage { package_name } => remove_package(daemon, package_name),
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
            observability,
            other,
        ),
    }
}

fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    observability: DaemonObservability<'_>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator(
        observability
            .client_id
            .map(str::to_string)
            .unwrap_or_else(|| runtime_client_id(&request)),
    );
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
            suppress_unix_session_close_events(pending_runtime, &session_id);
            suppress_webrtc_session_close_events(pending_runtime, &session_id);
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
                observability.egress.diagnostics(),
                observability.lifecycle.clone(),
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
            let client_id = observability
                .client_id
                .unwrap_or("botster-hub-daemon-socket")
                .to_string();
            if let Some(UnixTerminalAdmission::Rejected { code, diagnostic }) =
                pending_runtime.unix_admissions.get(&client_id)
            {
                return Ok(terminal_compatibility_attach_error(
                    code,
                    diagnostic.clone(),
                ));
            }
            if let Some(grant_id) = observability.grant_id
                && let Some(WebrtcTerminalAdmission::Rejected { code, diagnostic }) =
                    pending_runtime.webrtc_admissions.get(grant_id)
            {
                return Ok(terminal_compatibility_attach_error(
                    code,
                    diagnostic.clone(),
                ));
            }
            let owner = AttachStreamOwner {
                client_id: client_id.clone(),
                grant_id: observability.grant_id.map(str::to_string),
            };
            let previous_generation = live_generation_for_route(
                &runtime.list_terminal_subscriptions(),
                &client_id,
                &session_id,
                &subscription_id,
            );
            pending_runtime.start_attach(owner, session_id.clone(), subscription_id.clone());
            if let Some(generation) = previous_generation {
                let _ = runtime.detach_terminal_subscription(
                    ClientId(client_id.clone()),
                    SessionId(session_id.clone()),
                    SubscriptionId(subscription_id.clone()),
                    generation,
                    now,
                );
            }
            let bootstrap_egress = match pending_runtime.begin_core_attach(
                runtime,
                &session_id,
                &subscription_id,
                now,
            ) {
                Ok(egress) => egress,
                Err(_) => {
                    pending_runtime.cancel_stream(&session_id, &subscription_id);
                    return Ok(attach_bind_operator_error(
                        "invalid_request",
                        "attach failed before adapter bind",
                    ));
                }
            };
            let unix_admission = pending_runtime.unix_admissions.get(&client_id).cloned();
            let webrtc_admission = observability
                .grant_id
                .and_then(|grant_id| pending_runtime.webrtc_admissions.get(grant_id).cloned());
            if observability.grant_id.is_some() {
                let Some(WebrtcTerminalAdmission::Admitted {
                    required_features,
                    mux,
                    terminal_requirement,
                }) = webrtc_admission.as_ref()
                else {
                    fail_closed_pre_bind_attach(
                        pending_runtime,
                        runtime,
                        &client_id,
                        &session_id,
                        &subscription_id,
                        now,
                        None,
                    );
                    return Ok(attach_bind_operator_error(
                        "invalid_request",
                        "Attach requires an admitted WebRTC adapter",
                    ));
                };
                return match bind_webrtc_adapter_after_attaching(
                    pending_runtime,
                    runtime,
                    WebrtcBindRequest {
                        client_id: &client_id,
                        session_id: &session_id,
                        subscription_id: &subscription_id,
                        required_features,
                        terminal_requirement: terminal_requirement.as_ref(),
                        now_seconds: now,
                        mux: Some(mux),
                    },
                ) {
                    Ok(handle) => {
                        if let Some(handle) = handle {
                            forward_attach_bootstrap(
                                &BoundAdapterHandle::WebRtc(handle),
                                &bootstrap_egress,
                            );
                        }
                        observe_lifecycle_turn(runtime, tick(logical_clock));
                        Ok(daemon_events(Vec::new()))
                    }
                    Err(_) => Ok(attach_bind_operator_error(
                        "invalid_request",
                        "Attach failed to bind a WebRTC adapter",
                    )),
                };
            }
            let Some(UnixTerminalAdmission::Admitted {
                capabilities, mux, ..
            }) = unix_admission.as_ref()
            else {
                fail_closed_pre_bind_attach(
                    pending_runtime,
                    runtime,
                    &client_id,
                    &session_id,
                    &subscription_id,
                    now,
                    None,
                );
                return Ok(attach_bind_operator_error(
                    "invalid_request",
                    "Attach requires an admitted Unix adapter",
                ));
            };
            match bind_unix_adapter_after_attaching(
                pending_runtime,
                runtime,
                UnixBindRequest {
                    client_id: &client_id,
                    session_id: &session_id,
                    subscription_id: &subscription_id,
                    capabilities: capabilities.clone(),
                    now_seconds: now,
                    mux: Some(mux),
                },
            ) {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        forward_attach_bootstrap(
                            &BoundAdapterHandle::Unix(handle),
                            &bootstrap_egress,
                        );
                    }
                    observe_lifecycle_turn(runtime, tick(logical_clock));
                    Ok(daemon_events(Vec::new()))
                }
                Err(_) => Ok(attach_bind_operator_error(
                    "invalid_request",
                    "Attach failed to bind a Unix adapter",
                )),
            }
        }
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let tracked_session_id = session_id.clone();
            let tracked_subscription_id = subscription_id.clone();
            let generation = observability.client_id.and_then(|client_id| {
                live_generation_for_route(
                    &runtime.list_terminal_subscriptions(),
                    client_id,
                    &tracked_session_id,
                    &tracked_subscription_id,
                )
            });
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
            if let Some(client_id) = observability.client_id
                && let Some(UnixTerminalAdmission::Admitted { mux, .. }) =
                    pending_runtime.unix_admissions.get(client_id)
                && let Some(generation) = generation
            {
                mux.suppress_generation(
                    tracked_session_id.clone(),
                    tracked_subscription_id.clone(),
                    generation.0,
                );
            }
            if let Some(grant_id) = observability.grant_id
                && let Some(WebrtcTerminalAdmission::Admitted { mux, .. }) =
                    pending_runtime.webrtc_admissions.get(grant_id)
                && let Some(generation) = generation
            {
                mux.suppress_generation(
                    tracked_session_id.clone(),
                    tracked_subscription_id.clone(),
                    generation.0,
                );
            }
            pending_runtime.close_adapter(&tracked_session_id, &tracked_subscription_id);
            pending_runtime.cancel_stream(&tracked_session_id, &tracked_subscription_id);
            events_response(response.body)
        }
        DaemonRequest::SendInput { session_id, data } => {
            let data = data.into_bytes();
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Input {
                    request_id: request_id("daemon-sessions-send-input"),
                    session_id: SessionId(session_id),
                    data,
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::ModeGatedInput {
            session_id,
            data,
            mode_generation,
            mode_revision,
        } => {
            let data = data.into_bytes();
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ModeGatedInput {
                    request_id: request_id("daemon-sessions-mode-gated-input"),
                    session_id: SessionId(session_id),
                    data,
                    mode_generation,
                    mode_revision,
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::ModeGatedInput(result) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_mode_gated_input(result))
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
            let deadline = Instant::now() + SHUTDOWN_CLASSIFY_BUDGET;
            let mut walk = crate::runtime::SessionLifecycleWalk::default();
            let now = tick(logical_clock);
            observe_lifecycle_turn(runtime, now);
            let mut observed = true;
            match finish_shutdown_classify(deadline, || {
                if !observed {
                    observe_lifecycle_turn(runtime, tick(logical_clock));
                }
                observed = false;
                classify_shutdown_session(runtime, &session_id, deadline, &mut walk)
            })? {
                ShutdownSessionClassification::Active
                | ShutdownSessionClassification::Incomplete => {}
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
                    observed = false;
                    let classification = finish_shutdown_classify(deadline, || {
                        if !observed {
                            observe_lifecycle_turn(runtime, tick(logical_clock));
                        }
                        observed = false;
                        classify_shutdown_session(runtime, &session_id, deadline, &mut walk)
                    })?;
                    let response = shutdown_error_response(classification, error, &session_id)?;
                    suppress_unix_session_close_events(pending_runtime, &session_id);
                    suppress_webrtc_session_close_events(pending_runtime, &session_id);
                    return Ok(response);
                }
            };
            suppress_unix_session_close_events(pending_runtime, &session_id);
            suppress_webrtc_session_close_events(pending_runtime, &session_id);
            events_response(response.body)
        }
        DaemonRequest::Drain {
            session_id,
            subscription_id,
        } => {
            let session_known = runtime.list_sessions().ok().is_some_and(|sessions| {
                sessions
                    .iter()
                    .any(|session| session.session_id.0 == session_id)
            });
            if !session_known {
                return Ok(missing_session_drain_error(&session_id));
            }
            if let Some(subscription_id) = subscription_id {
                pending_runtime.authorize_drain(
                    &session_id,
                    &subscription_id,
                    observability.client_id,
                    observability.grant_id,
                )?;
            }
            Ok(daemon_events(Vec::new()))
        }
        DaemonRequest::ReadScreen { session_id } => {
            let now = tick(logical_clock);
            observe_lifecycle_turn(runtime, now);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("daemon-sessions-read-screen"),
                    session_id: SessionId(session_id),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::ReadScreen(screen) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_read_screen(screen))
        }
        DaemonRequest::ReadModeFlags { session_id } => {
            let now = tick(logical_clock);
            observe_lifecycle_turn(runtime, now);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadModeFlags {
                    request_id: request_id("daemon-sessions-read-mode-flags"),
                    session_id: SessionId(session_id),
                    now_seconds: now,
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
                observability.lifecycle.clone(),
                software_identity(),
                installation_identity(),
            )),
            sessions: Vec::new(),
            session_types: Vec::new(),
            session_type_definition: None,
            resolved_session_type: None,
            session_context: None,
            read_screen: None,
            mode_flags: None,
            mode_gated_input: None,
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
            hub_update_execution: None,
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
        DaemonRequest::CheckHubUpdate
        | DaemonRequest::StartHubUpdate { .. }
        | DaemonRequest::GetHubUpdateExecution => {
            unreachable!("Hub update requests are handled before runtime borrow")
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

fn runtime_client_id(request: &DaemonRequest) -> String {
    match request {
        DaemonRequest::Attach {
            subscription_id, ..
        }
        | DaemonRequest::Detach {
            subscription_id, ..
        } => format!("botster-hub-daemon-subscription-{subscription_id}"),
        _ => "botster-hub-daemon-socket".to_string(),
    }
}

pub(super) fn list_packages_response(
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

pub(super) fn supervised_launch_contract(
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

pub(super) fn show_package_response(
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

pub(super) fn package_decision_response(
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

#[derive(Debug)]
enum ShutdownSessionClassification {
    Active,
    Cleanup(DaemonSessionCleanup),
    Missing,
    Incomplete,
}

const SHUTDOWN_CLASSIFY_BUDGET: Duration = Duration::from_millis(1000);

fn finish_shutdown_classify<F>(
    deadline: Instant,
    mut classify: F,
) -> Result<ShutdownSessionClassification, crate::HubRuntimeError>
where
    F: FnMut() -> Result<ShutdownSessionClassification, crate::HubRuntimeError>,
{
    loop {
        let classification = classify()?;
        if matches!(classification, ShutdownSessionClassification::Incomplete)
            && Instant::now() < deadline
        {
            continue;
        }
        return Ok(classification);
    }
}

fn daemon_hello_ack(diagnostics: Vec<DaemonDiagnostic>) -> DaemonHelloAck {
    DaemonHelloAck {
        protocol: PROTOCOL.to_string(),
        compatibility: DaemonCompatibility::current(),
        terminal_compatibility: Some(TerminalCompatibility::current()),
        diagnostics,
    }
}

fn unix_hello_admission(hello: &DaemonHello) -> (UnixTerminalAdmission, DaemonHelloAck) {
    let mut diagnostics = vec![DaemonDiagnostic::connected("hello")];
    if let Some(requirement) = hello.terminal_compatibility.as_ref()
        && let Err(error) =
            ensure_terminal_compatible(requirement, &TerminalCompatibility::current())
    {
        let diagnostic = DaemonDiagnostic::compatibility_mismatch(error.diagnostic);
        diagnostics.push(diagnostic.clone());
        return (
            UnixTerminalAdmission::Rejected {
                code: "terminal_compatibility",
                diagnostic,
            },
            daemon_hello_ack(diagnostics),
        );
    }
    let capabilities = negotiated_unix_capability_set(
        &hello.compatibility.required_features,
        hello.terminal_compatibility.as_ref(),
    )
    .unwrap_or_else(|_| TerminalCapabilitySet::empty());
    (
        UnixTerminalAdmission::Admitted {
            required_features: hello.compatibility.required_features.clone(),
            capabilities,
            mux: UnixConnectionMux::new(),
        },
        daemon_hello_ack(diagnostics),
    )
}

fn terminal_compatibility_attach_error(
    code: &'static str,
    diagnostic: DaemonDiagnostic,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-attach-terminal-compatibility".to_string(),
        operation: "attach".to_string(),
        message: diagnostic
            .message
            .clone()
            .unwrap_or_else(|| "terminal compatibility mismatch".to_string()),
        diagnostics: vec![diagnostic],
    });
    response
}

fn attach_bind_operator_error(code: &'static str, message: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-attach-bind".to_string(),
        operation: "attach".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure("attach", message)],
    });
    response
}

fn missing_session_drain_error(session_id: &str) -> DaemonResponse {
    let message = format!("unknown session: {session_id}");
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.diagnostics = vec![DaemonDiagnostic::terminal_stream_unavailable(
        "drain_runtime",
        message.clone(),
    )];
    response.error = Some(DaemonOperatorError {
        code: "unknown_session".to_string(),
        request_id: "daemon-sessions-drain".to_string(),
        operation: "drain_runtime".to_string(),
        message,
        diagnostics: response.diagnostics.clone(),
    });
    response
}

fn suppress_unix_session_close_events(pending_runtime: &PendingRuntimeState, session_id: &str) {
    for admission in pending_runtime.unix_admissions.values() {
        if let UnixTerminalAdmission::Admitted { mux, .. } = admission {
            mux.suppress_session(session_id);
        }
    }
}

fn suppress_webrtc_session_close_events(pending_runtime: &PendingRuntimeState, session_id: &str) {
    for admission in pending_runtime.webrtc_admissions.values() {
        if let WebrtcTerminalAdmission::Admitted { mux, .. } = admission {
            mux.suppress_session(session_id);
        }
    }
}

fn session_suppresses_terminal_subscription_closed(
    runtime: &crate::HubRuntime,
    session_id: &str,
) -> bool {
    let Ok(sessions) = runtime.list_sessions() else {
        return true;
    };
    let Some(session) = sessions
        .iter()
        .find(|session| session.session_id.0 == session_id)
    else {
        return true;
    };
    session.registry_state != RegistrySessionState::Running
}

fn queue_unix_subscription_closed_events(
    runtime: &crate::HubRuntime,
    pending_runtime: &PendingRuntimeState,
) {
    for admission in pending_runtime.unix_admissions.values() {
        if let UnixTerminalAdmission::Admitted { mux, .. } = admission {
            mux.queue_closed_subscription_events(|session_id| {
                !session_suppresses_terminal_subscription_closed(runtime, session_id)
            });
        }
    }
}

fn queue_webrtc_subscription_closed_events(
    runtime: &crate::HubRuntime,
    pending_runtime: &PendingRuntimeState,
) {
    for admission in pending_runtime.webrtc_admissions.values() {
        if let WebrtcTerminalAdmission::Admitted { mux, .. } = admission {
            mux.queue_closed_subscription_events(|session_id| {
                !session_suppresses_terminal_subscription_closed(runtime, session_id)
            });
        }
    }
}

fn shutdown_error_is_already_gone(error: &crate::HubClientError) -> bool {
    matches!(
        error,
        crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Shutdown,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        }
    )
}

fn shutdown_error_response(
    classification: ShutdownSessionClassification,
    error: crate::HubClientError,
    session_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    match classification {
        ShutdownSessionClassification::Cleanup(cleanup) => Ok(daemon_session_cleanup(cleanup)),
        ShutdownSessionClassification::Missing => Ok(daemon_unknown_session_cleanup(session_id)),
        ShutdownSessionClassification::Active if shutdown_error_is_already_gone(&error) => {
            Ok(daemon_session_cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            }))
        }
        ShutdownSessionClassification::Active | ShutdownSessionClassification::Incomplete => {
            Err(DaemonTransportError::Client(error))
        }
    }
}

fn classify_shutdown_session(
    runtime: &mut crate::HubRuntime,
    session_id: &str,
    deadline: Instant,
    walk: &mut crate::runtime::SessionLifecycleWalk,
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
        RegistrySessionState::Stopping | RegistrySessionState::Exited => {
            return Ok(ShutdownSessionClassification::Cleanup(
                DaemonSessionCleanup {
                    session_id: session_id.to_string(),
                    outcome: "already_exited".to_string(),
                },
            ));
        }
        RegistrySessionState::Stale => {
            return Ok(ShutdownSessionClassification::Cleanup(
                DaemonSessionCleanup {
                    session_id: session_id.to_string(),
                    outcome: "stale_session".to_string(),
                },
            ));
        }
        RegistrySessionState::Running => {}
    }

    Ok(shutdown_classification_from_engine_lookup(
        session_id,
        runtime.session_runtime_lifecycle(&SessionId(session_id.to_string()), deadline, walk),
    ))
}

fn shutdown_classification_from_engine_lookup(
    session_id: &str,
    lookup: crate::runtime::SessionRuntimeLifecycleLookup,
) -> ShutdownSessionClassification {
    match lookup {
        crate::runtime::SessionRuntimeLifecycleLookup::Found(SessionLifecycleState::Stopping)
        | crate::runtime::SessionRuntimeLifecycleLookup::Found(SessionLifecycleState::Exited {
            ..
        })
        | crate::runtime::SessionRuntimeLifecycleLookup::Found(SessionLifecycleState::Failed {
            ..
        }) => ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
            session_id: session_id.to_string(),
            outcome: "already_exited".to_string(),
        }),
        crate::runtime::SessionRuntimeLifecycleLookup::Found(SessionLifecycleState::Starting)
        | crate::runtime::SessionRuntimeLifecycleLookup::Found(SessionLifecycleState::Running)
        | crate::runtime::SessionRuntimeLifecycleLookup::CompleteAbsent => {
            ShutdownSessionClassification::Active
        }
        crate::runtime::SessionRuntimeLifecycleLookup::Incomplete
        | crate::runtime::SessionRuntimeLifecycleLookup::Error => {
            ShutdownSessionClassification::Incomplete
        }
    }
}

pub(super) fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

pub(super) fn tick(logical_clock: &mut u64) -> u64 {
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
                client_id: None,
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
                    terminal_compatibility: None,
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
        /// Stable Core client identity for one transport connection.
        client_id: Option<String>,
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
    RegisterUnixAdmission {
        client_id: String,
        admission: UnixTerminalAdmission,
    },
    RegisterWebrtcAdmission {
        grant_id: String,
        admission: WebrtcTerminalAdmission,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum UnixTerminalAdmission {
    Admitted {
        #[allow(dead_code)]
        required_features: Vec<String>,
        capabilities: TerminalCapabilitySet,
        mux: UnixConnectionMux,
    },
    Rejected {
        code: &'static str,
        diagnostic: DaemonDiagnostic,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum WebrtcTerminalAdmission {
    Admitted {
        required_features: Vec<String>,
        mux: WebRtcConnectionMux,
        terminal_requirement: Option<botster_terminal_protocol::TerminalCompatibilityRequirement>,
    },
    Rejected {
        code: &'static str,
        diagnostic: DaemonDiagnostic,
    },
}

#[derive(Default)]
pub(crate) struct PendingRuntimeState {
    pub(crate) streams: AttachStreamRegistry,
    unix_admissions: BTreeMap<String, UnixTerminalAdmission>,
    webrtc_admissions: BTreeMap<String, WebrtcTerminalAdmission>,
}

impl fmt::Debug for PendingRuntimeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingRuntimeState")
            .finish_non_exhaustive()
    }
}

impl std::ops::Deref for PendingRuntimeState {
    type Target = AttachStreamRegistry;
    fn deref(&self) -> &Self::Target {
        &self.streams
    }
}

impl std::ops::DerefMut for PendingRuntimeState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.streams
    }
}

impl PendingRuntimeState {
    pub(super) fn retain_active_sessions(&mut self, active_session_ids: &BTreeSet<String>) {
        self.streams.retain_active_sessions(active_session_ids);
    }

    #[cfg(test)]
    pub(crate) fn webrtc_is_admitted(&self, grant_id: &str) -> bool {
        matches!(
            self.webrtc_admissions.get(grant_id),
            Some(WebrtcTerminalAdmission::Admitted { .. })
        )
    }
}

fn observe_lifecycle_turn(runtime: &crate::HubRuntime, now: u64) {
    let _ = runtime.observe_lifecycle_slice(
        now,
        None,
        botster_core_daemon::ObserveLifecycleBudget {
            max_sessions: 32,
            max_encoded_result_bytes: 64 * 1024,
            max_elapsed: Duration::from_millis(25),
        },
    );
}

fn pump_bound_unix_routes(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime_mut() else {
        return;
    };
    let inventory = runtime.list_terminal_subscriptions();
    queue_unix_subscription_closed_events(runtime, &state.pending_runtime);
    queue_webrtc_subscription_closed_events(runtime, &state.pending_runtime);
    state.pending_runtime.reconcile_inventory(&inventory);
    let now = tick(&mut state.logical_clock);
    let resume = state.observe_resume.as_ref();
    let slice = runtime.observe_lifecycle_slice(
        now,
        resume,
        botster_core_daemon::ObserveLifecycleBudget {
            max_sessions: 32,
            max_encoded_result_bytes: 64 * 1024,
            max_elapsed: Duration::from_millis(25),
        },
    );
    if let Ok(slice) = slice {
        state.lifecycle_counters.lifecycle_session_drains = state
            .lifecycle_counters
            .lifecycle_session_drains
            .saturating_add(1);
        state.observe_resume = if slice.complete || slice.resync_required.is_some() {
            None
        } else {
            Some(botster_core_daemon::ObserveLifecycleCursor {
                pass_id: slice.pass_id,
                last_visited: slice.last_visited,
            })
        };
    }
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
    live_attach_routes: BTreeSet<(String, String)>,
    pending_hub_update_reply: Option<ControlReplySender>,
    observe_resume: Option<botster_core_daemon::ObserveLifecycleCursor>,
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
            live_attach_routes: BTreeSet::new(),
            pending_hub_update_reply: None,
            observe_resume: None,
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

fn daemon_delivery_kind(_response: &DaemonResponse) -> DaemonDeliveryKind {
    DaemonDeliveryKind::Control
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
            let route = (
                subscription.session_id.clone(),
                subscription.subscription_id.clone(),
            );
            let inserted = state.live_attach_routes.insert(route.clone());
            if !inserted && state.lifecycle_counters.live_attach_subscriptions > 0 {
                return;
            }
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
                state
                    .pending_runtime
                    .attach_owner_grant_ids
                    .insert(route, grant_id.to_string());
            }
        }
        AttachedSubscriptionChange::Detach(subscription) => {
            let route = (subscription.session_id, subscription.subscription_id);
            if !state.live_attach_routes.remove(&route) {
                return;
            }
            state.lifecycle_counters.live_attach_subscriptions = state
                .lifecycle_counters
                .live_attach_subscriptions
                .saturating_sub(1);
            state.released_attach_generations = state.released_attach_generations.saturating_add(1);
            state.pending_runtime.attach_owner_grant_ids.remove(&route);
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
        DaemonRequest::ModeGatedInput { .. } => "mode_gated_input",
        DaemonRequest::Drain { .. } => "drain",
        DaemonRequest::Resize { .. } => "resize",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        DaemonRequest::RemoveSession { .. } => "remove_session",
        DaemonRequest::DaemonShutdown => "daemon_shutdown",
        DaemonRequest::CheckHubUpdate => "check_hub_update",
        DaemonRequest::StartHubUpdate { .. } => "start_hub_update",
        DaemonRequest::GetHubUpdateExecution => "get_hub_update_execution",
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

pub(crate) fn response_records_attach_ownership(response: &DaemonResponse) -> bool {
    response.kind != DaemonResponseKind::OperatorError
}

fn attached_subscription_change_for_response(
    request: &DaemonRequest,
    response: &DaemonResponse,
) -> Option<AttachedSubscriptionChange> {
    if response.kind == DaemonResponseKind::OperatorError {
        return None;
    }
    AttachedSubscriptionChange::from_request(request)
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

pub(super) fn session_type_definition_map(
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

pub(super) fn advance_session_type_generation_if_changed(
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
        mode_gated_input: None,
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
        hub_update_execution: None,
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
        software_identity(),
        installation_identity(),
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

fn daemon_hub_update_execution(execution: DaemonHubUpdateExecution) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::HubUpdateExecution);
    response.hub_update_execution = Some(execution);
    response
}

fn hub_update_execution_error(code: &str, operation: &str, message: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("daemon-{operation}"),
        operation: operation.to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(operation, message)],
    });
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
    response.mode_flags = Some(DaemonModeFlags::new(
        mode_flags.session_id.0,
        mode_flags.kitty_enabled,
        mode_flags.cursor_visible,
        mode_flags.bracketed_paste,
        mode_flags.mouse_mode,
        mode_flags.alt_screen,
        mode_flags.focus_reporting,
        mode_flags.application_cursor,
        mode_flags.mode_generation,
        mode_flags.mode_revision,
    ));
    response
}

fn daemon_mode_gated_input(result: crate::HubClientModeGatedInputResult) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ModeGatedInput);
    response.mode_gated_input = Some(botster_hub_client::DaemonModeGatedInputResult::new(
        result.session_id.0,
        result.admitted,
        result.bytes_written,
        result.kitty_enabled,
        result.cursor_visible,
        result.bracketed_paste,
        result.mouse_mode,
        result.alt_screen,
        result.focus_reporting,
        result.application_cursor,
        result.mode_generation,
        result.mode_revision,
        result.error_kind,
    ));
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
        execution: match template.execution {
            crate::PackageSessionTypeExecution::RelativeExecutable => {
                botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable
            }
            crate::PackageSessionTypeExecution::ShellCommand => {
                botster_hub_client::DaemonSessionTypeExecution::ShellCommand
            }
        },
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
        execution: match definition.execution {
            crate::PackageSessionTypeExecution::RelativeExecutable => {
                botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable
            }
            crate::PackageSessionTypeExecution::ShellCommand => {
                botster_hub_client::DaemonSessionTypeExecution::ShellCommand
            }
        },
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
        execution: match definition.execution {
            botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable => {
                crate::PackageSessionTypeExecution::RelativeExecutable
            }
            botster_hub_client::DaemonSessionTypeExecution::ShellCommand => {
                crate::PackageSessionTypeExecution::ShellCommand
            }
        },
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

pub(super) fn daemon_operator_error(error: crate::HubClientError) -> DaemonResponse {
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

fn daemon_snapshot_stream_forbidden_error(error: DaemonTransportError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "snapshot_stream_forbidden".to_string(),
        request_id: "daemon-sessions-drain".to_string(),
        operation: "drain".to_string(),
        message: error.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "drain",
            "snapshot stream is owned by another connection",
        )],
    });
    response
}

fn daemon_package_compensation_error(error: DaemonTransportError) -> DaemonResponse {
    const MESSAGE_BOUND: usize = 512;
    let DaemonTransportError::PackageCompensation {
        original,
        rollbacks,
    } = error
    else {
        let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
        response.error = Some(DaemonOperatorError {
            code: "package_compensation_failed".to_string(),
            request_id: "daemon-package-mutation".to_string(),
            operation: "package_mutation_compensation".to_string(),
            message: error.to_string(),
            diagnostics: Vec::new(),
        });
        return response;
    };

    let mut diagnostics = vec![DaemonDiagnostic {
        kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
        operation: Some("original".to_string()),
        feature: None,
        message: Some(bound_compensation_message(
            original.to_string(),
            MESSAGE_BOUND,
        )),
    }];
    for rollback in &rollbacks {
        diagnostics.push(DaemonDiagnostic {
            kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
            operation: Some(rollback.step.to_string()),
            feature: rollback.package_name.clone(),
            message: Some(bound_compensation_message(
                rollback.error.to_string(),
                MESSAGE_BOUND,
            )),
        });
    }

    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "package_compensation_failed".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: "package_mutation_compensation".to_string(),
        message: format!(
            "package mutation failed ({original}); rollback failures: {}",
            rollbacks.len()
        ),
        diagnostics: diagnostics.clone(),
    });
    response.diagnostics = diagnostics;
    response
}

fn bound_compensation_message(message: String, bound: usize) -> String {
    if message.chars().count() <= bound {
        return message;
    }
    message.chars().take(bound).collect()
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

pub(super) fn package_update_status(
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

pub(super) fn package_pin_from_daemon(pin: DaemonPackagePin) -> DaemonTransportResult<PackagePin> {
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

fn daemon_session_from_client(session: HubClientSession) -> DaemonSession {
    DaemonSession {
        session_id: session.session_id.0,
        lifecycle: lifecycle_label(&session.lifecycle).to_string(),
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

pub(super) fn daemon_event_from_client(event: HubClientEvent) -> DaemonEvent {
    match event {
        HubClientEvent::SessionLifecycle { session_id, state } => DaemonEvent::SessionLifecycle {
            session_id: session_id.0,
            state: lifecycle_label(&state).to_string(),
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

fn lifecycle_label(state: &SessionLifecycleState) -> &'static str {
    match state {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
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
    /// A package mutation side effect failed, and one or more rollback steps also failed.
    PackageCompensation {
        original: Box<DaemonTransportError>,
        rollbacks: Vec<PackageRollbackFailure>,
    },
    SnapshotStreamForbidden {
        session_id: String,
        subscription_id: String,
    },
}

/// One failed compensation step after a package mutation side-effect failure.
#[derive(Debug)]
pub struct PackageRollbackFailure {
    pub step: &'static str,
    pub package_name: Option<String>,
    pub error: Box<DaemonTransportError>,
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
            Self::PackageCompensation {
                original,
                rollbacks,
            } => {
                write!(
                    formatter,
                    "package mutation failed ({original}); rollback failures: {}",
                    rollbacks.len()
                )
            }
            Self::SnapshotStreamForbidden {
                session_id,
                subscription_id,
            } => write!(
                formatter,
                "snapshot stream forbidden session={session_id} subscription={subscription_id}"
            ),
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
fn receive_test_control_request(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    loop {
        match receive_test_control_message(receiver) {
            ControlMessage::RegisterUnixAdmission { .. }
            | ControlMessage::RegisterWebrtcAdmission { .. } => {}
            message => return message,
        }
    }
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
                    client_id: Some(cleanup.client_id.clone()),
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
            kind: crate::HubClientRuntimeErrorKind::ModeReadFailed,
        });

        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        assert!(response.mode_flags.is_none());
        let error = response.error.expect("operator error body");
        assert_eq!(error.code, "mode_read_failed");
        assert_eq!(error.operation, "read_mode_flags");
        assert!(error.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::WorkerCompatibility
        }));
    }

    fn shutdown_runtime_error(kind: crate::HubClientRuntimeErrorKind) -> crate::HubClientError {
        crate::HubClientError::Runtime {
            request_id: RequestId("daemon-sessions-shutdown".to_string()),
            operation: crate::HubClientOperation::Shutdown,
            kind,
        }
    }

    #[test]
    fn failed_engine_lifecycle_lookup_is_not_active_or_cleanup() {
        assert!(matches!(
            shutdown_classification_from_engine_lookup(
                "late-session",
                crate::runtime::SessionRuntimeLifecycleLookup::Incomplete,
            ),
            ShutdownSessionClassification::Incomplete
        ));
        assert!(matches!(
            shutdown_classification_from_engine_lookup(
                "error-session",
                crate::runtime::SessionRuntimeLifecycleLookup::Error,
            ),
            ShutdownSessionClassification::Incomplete
        ));
        assert!(matches!(
            shutdown_classification_from_engine_lookup(
                "missing-session",
                crate::runtime::SessionRuntimeLifecycleLookup::CompleteAbsent,
            ),
            ShutdownSessionClassification::Active
        ));
        assert!(matches!(
            shutdown_classification_from_engine_lookup(
                "exited-session",
                crate::runtime::SessionRuntimeLifecycleLookup::Found(
                    SessionLifecycleState::Exited { code: Some(0) }
                ),
            ),
            ShutdownSessionClassification::Cleanup(_)
        ));
    }

    #[test]
    fn incomplete_classify_retries_share_one_deadline() {
        let deadline = Instant::now() + Duration::from_millis(80);
        let mut calls = 0;
        let started = Instant::now();
        let result = finish_shutdown_classify(deadline, || {
            calls += 1;
            thread::sleep(Duration::from_millis(25));
            Ok(ShutdownSessionClassification::Incomplete)
        })
        .expect("forced Incomplete classify");
        let elapsed = started.elapsed();
        assert!(
            matches!(result, ShutdownSessionClassification::Incomplete),
            "forced Incomplete must stay Incomplete, got {result:?}"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "shared deadline must bound retries, elapsed={elapsed:?} calls={calls}"
        );
        assert!(
            (2..=6).contains(&calls),
            "shared 80 ms deadline must retry a few 25 ms walks, calls={calls}"
        );
    }

    #[test]
    fn shutdown_unknown_session_error_while_active_is_already_exited_cleanup() {
        let response = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::UnknownSession),
            "live-session",
        )
        .expect("unknown-session while Active is cleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "live-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn shutdown_exited_classification_returns_cleanup_for_any_shutdown_error() {
        let response = shutdown_error_response(
            ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
                session_id: "exited-session".to_string(),
                outcome: "already_exited".to_string(),
            }),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "exited-session",
        )
        .expect("Cleanup classification stays SessionCleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "exited-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn shutdown_active_runtime_error_remains_operator_error() {
        let error = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "live-session",
        )
        .expect_err("Active plus Runtime stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::Runtime,
                ..
            })
        ));
        let response = daemon_operator_error(match error {
            DaemonTransportError::Client(error) => error,
            other => panic!("expected Client error, got {other:?}"),
        });
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error body");
        assert_eq!(operator.code, "runtime_error");
        assert_eq!(operator.operation, "shutdown");
    }

    #[test]
    fn shutdown_active_state_error_remains_operator_error() {
        let error = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::State),
            "live-session",
        )
        .expect_err("Active plus State stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::State,
                ..
            })
        ));
        let response = daemon_operator_error(match error {
            DaemonTransportError::Client(error) => error,
            other => panic!("expected Client error, got {other:?}"),
        });
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error body");
        assert_eq!(operator.code, "state_error");
        assert_eq!(operator.operation, "shutdown");
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
                terminal_compatibility: None,
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
        } = receive_test_control_request(&mut control_rx)
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
        } = receive_test_control_request(&mut control_rx)
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
    fn attach_operator_error_does_not_detach_on_client_eof() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        write_frame(
            &mut client,
            &DaemonRequest::Attach {
                session_id: "missing-session".to_string(),
                subscription_id: "missing-sub".to_string(),
            },
        )
        .expect("write attach request");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(attach_bind_operator_error(
                "invalid_request",
                "attach failed before adapter bind",
            )))
            .expect("reply with attach operator error");
        let _: DaemonResponse = read_frame(&mut client).expect("read attach operator error");

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
        assert!(
            control_rx.try_recv().is_err(),
            "pre-bind OperatorError must not enqueue Detach cleanup"
        );
    }

    #[test]
    fn drain_does_not_inspect_legacy_attach_state_for_ownership() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
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
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(attach_bind_operator_error(
                "invalid_request",
                "attach failed before adapter bind",
            )))
            .expect("reply with attach operator error");
        let _: DaemonResponse = read_frame(&mut client).expect("read attach operator error");

        write_frame(
            &mut client,
            &DaemonRequest::drain_subscription("session", "subscription"),
        )
        .expect("write scoped drain");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected drain control request");
        };
        assert!(matches!(*request, DaemonRequest::Drain { .. }));
        reply_tx
            .send(Ok(daemon_events(Vec::new())))
            .expect("reply with host drain");
        let _: DaemonResponse = read_frame(&mut client).expect("read host drain");

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
        assert!(
            control_rx.try_recv().is_err(),
            "pre-bind OperatorError plus host Drain must not enqueue Detach cleanup"
        );
    }

    #[test]
    fn drain_does_not_change_attach_occupancy() {
        let mut state = DaemonControlState::default();
        record_attached_subscription_change(
            &mut state,
            Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            })),
            None,
        );
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 1);

        let drain = DaemonRequest::drain_subscription("session", "subscription");
        let drain_ok = daemon_events(Vec::new());
        assert!(attached_subscription_change_for_response(&drain, &drain_ok).is_none());
        record_attached_subscription_change(
            &mut state,
            attached_subscription_change_for_response(&drain, &drain_ok),
            None,
        );
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 1);

        let detach = DaemonRequest::Detach {
            session_id: "session".to_string(),
            subscription_id: "subscription".to_string(),
        };
        let change = attached_subscription_change_for_response(&detach, &drain_ok);
        record_attached_subscription_change(&mut state, change.clone(), None);
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 0);
        record_attached_subscription_change(&mut state, change, None);
        assert_eq!(
            state.lifecycle_counters.live_attach_subscriptions, 0,
            "a second Detach must not decrement another route"
        );
    }

    #[test]
    fn daemon_egress_diagnostics_classify_terminal_and_control_backpressure() {
        let control = daemon_response_base(DaemonResponseKind::Sessions);
        assert_eq!(daemon_delivery_kind(&control), DaemonDeliveryKind::Control);

        let mut diagnostics = DaemonEgressDiagnostics::default();
        let mut counters = DaemonLifecycleCounters::default();
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            DaemonDeliveryKind::Terminal,
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
                terminal_compatibility: None,
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
            ..
        } = receive_test_control_request(&mut control_rx)
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

    fn unique_package_control_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        PathBuf::from("target")
            .join("botster-hub-test-data")
            .join("package-control")
            .join(name)
            .join(nanos.to_string())
    }

    fn package_control_config(data_directory: PathBuf) -> crate::HubConfig {
        crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "package-control-test".to_string(),
                display_name: "Package Control Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build package control config")
    }

    fn write_package_control_manifest(root: &Path, name: &str, extra: serde_json::Value) {
        std::fs::create_dir_all(root).expect("create package root");
        let mut manifest = serde_json::json!({
            "name": name,
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [],
            "entrypoints": []
        });
        if let Some(object) = extra.as_object() {
            for (key, value) in object {
                manifest[key] = value.clone();
            }
        }
        std::fs::write(
            root.join("botster-package.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize package manifest"),
        )
        .expect("write package manifest");
    }

    fn drive_package_request(
        daemon: &mut HubDaemon,
        request: DaemonRequest,
    ) -> DaemonTransportResult<DaemonResponse> {
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let egress = DaemonEgressDiagnostics::default();
        let lifecycle = DaemonLifecycleCounters::default();
        let observability = DaemonObservability {
            egress: &egress,
            lifecycle: &lifecycle,
            client_id: None,
            grant_id: None,
        };
        let mut clock = 1;
        let mut drain_cursors = BTreeMap::new();
        let mut pending = PendingRuntimeState::default();
        handle_control_request(
            daemon,
            &mut clock,
            &mut drain_cursors,
            &mut pending,
            observability,
            control_tx,
            request,
        )
    }

    fn live_and_durable_registries(
        daemon: &HubDaemon,
        config: &crate::HubConfig,
    ) -> (
        crate::PackageRegistrySnapshot,
        crate::PackageRegistrySnapshot,
    ) {
        let live = daemon.package_registry().snapshot();
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let durable = store
            .load_or_initialize(config)
            .expect("load durable hub state")
            .package_registry;
        (live, durable)
    }

    #[test]
    fn package_compensation_projects_every_rollback_to_socket_diagnostics() {
        let error = DaemonTransportError::PackageCompensation {
            original: Box::new(DaemonTransportError::Entrypoint(
                EntrypointSupervisorError::ReadinessFailed {
                    package_name: "reload.plugin".to_string(),
                    entrypoint_id: "sleeper".to_string(),
                    details: "entrypoint state after restart is failed".to_string(),
                },
            )),
            rollbacks: vec![
                PackageRollbackFailure {
                    step: "persist",
                    package_name: None,
                    error: Box::new(DaemonTransportError::State(
                        crate::HubStateStoreError::InjectedWriteFailure,
                    )),
                },
                PackageRollbackFailure {
                    step: "entrypoint",
                    package_name: Some("reload.plugin".to_string()),
                    error: Box::new(DaemonTransportError::Entrypoint(
                        EntrypointSupervisorError::ReadinessFailed {
                            package_name: "reload.plugin".to_string(),
                            entrypoint_id: "sleeper".to_string(),
                            details: "restore spawn failed".to_string(),
                        },
                    )),
                },
            ],
        };

        let response = daemon_package_compensation_error(error);
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error");
        assert_eq!(operator.code, "package_compensation_failed");
        assert_eq!(response.diagnostics, operator.diagnostics);
        assert_eq!(operator.diagnostics.len(), 3);

        let original = &operator.diagnostics[0];
        assert_eq!(
            original.kind,
            botster_hub_client::DaemonDiagnosticKind::ActionFailure
        );
        assert_eq!(original.operation.as_deref(), Some("original"));
        assert!(
            original.message.as_deref().is_some_and(
                |message| message.contains("reload.plugin") && message.contains("failed")
            )
        );

        let persist = operator
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.operation.as_deref() == Some("persist"))
            .expect("persist rollback diagnostic");
        assert_eq!(persist.feature, None);
        assert!(
            persist
                .message
                .as_deref()
                .is_some_and(|message| message.contains("injected"))
        );

        let entrypoint = operator
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.operation.as_deref() == Some("entrypoint"))
            .expect("entrypoint rollback diagnostic");
        assert_eq!(entrypoint.feature.as_deref(), Some("reload.plugin"));
        assert!(
            entrypoint
                .message
                .as_deref()
                .is_some_and(|message| message.contains("restore spawn failed"))
        );
    }

    fn package_state(daemon: &HubDaemon, name: &str) -> PackageState {
        daemon
            .package_registry()
            .package(name)
            .expect("package record")
            .state
    }

    fn plugin_is_loaded(daemon: &HubDaemon, name: &str) -> bool {
        daemon
            .runtime()
            .expect("runtime")
            .plugin_lifecycle_status(daemon.package_registry())
            .into_iter()
            .any(|status| status.package_name == name && status.loaded)
    }

    fn write_sleeper_script(package_dir: &Path) {
        std::fs::create_dir_all(package_dir.join("bin")).expect("create bin");
        std::fs::write(
            package_dir.join("bin/sleeper"),
            "#!/bin/sh\nexec /bin/sleep \"$@\"\n",
        )
        .expect("write sleeper");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(package_dir.join("bin/sleeper"))
                .expect("sleeper metadata")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(package_dir.join("bin/sleeper"), permissions)
                .expect("chmod sleeper");
        }
    }

    fn sleeper_manifest(args: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/sleeper",
                "args": args,
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn lua_and_sleeper_manifest(args: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/sleeper",
                "args": args,
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn write_lua_plugin(package_dir: &Path) {
        std::fs::write(
            package_dir.join("plugin.lua"),
            "return botster.register({})\n",
        )
        .expect("write lua plugin");
    }

    fn write_broken_entrypoint(package_dir: &Path) {
        std::fs::create_dir_all(package_dir.join("bin")).expect("create bin");
        std::fs::write(package_dir.join("bin/gone"), "not-executable\n").expect("write decoy");
    }

    fn broken_sleeper_manifest() -> serde_json::Value {
        serde_json::json!({
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/gone",
                "args": ["30"],
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn entrypoint_is_running(daemon: &mut HubDaemon, package_name: &str) -> bool {
        daemon
            .entrypoint_supervisor()
            .snapshots()
            .iter()
            .any(|snapshot| {
                snapshot.package_name == package_name
                    && snapshot.entrypoint_id == "sleeper"
                    && snapshot.state == "running"
            })
    }

    fn entrypoint_command<'a>(daemon: &'a HubDaemon, package_name: &str) -> &'a str {
        daemon
            .package_registry()
            .package(package_name)
            .expect("package")
            .runnable_entrypoints
            .iter()
            .find(|entrypoint| entrypoint.id == "sleeper")
            .expect("sleeper entrypoint")
            .command
            .as_str()
    }

    #[test]
    fn failed_package_persist_leaves_live_registry_equal_to_durable_snapshot() {
        let root = unique_package_control_dir("persist-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("mutate.plugin");
        write_package_control_manifest(&package_dir, "mutate.plugin", serde_json::json!({}));
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start package control daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install local package");
        assert_eq!(
            package_state(&daemon, "mutate.plugin"),
            PackageState::Installed
        );

        FileHubStateStore::inject_next_save_failure();
        let error = drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "mutate.plugin".to_string(),
            },
        )
        .expect_err("injected persist failure");
        assert!(matches!(
            error,
            DaemonTransportError::State(crate::HubStateStoreError::InjectedWriteFailure)
        ));
        assert_eq!(
            package_state(&daemon, "mutate.plugin"),
            PackageState::Installed
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_disable_commit_does_not_stop_or_unload_running_package() {
        let root = unique_package_control_dir("disable-running");
        let data_directory = root.join("data");
        let package_dir = root.join("running.plugin");
        write_package_control_manifest(
            &package_dir,
            "running.plugin",
            lua_and_sleeper_manifest(&["30"]),
        );
        write_sleeper_script(&package_dir);
        write_lua_plugin(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start disable-running daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install running package");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "running.plugin".to_string(),
            },
        )
        .expect("enable running package");
        assert!(
            plugin_is_loaded(&daemon, "running.plugin"),
            "lua plugin must be loaded before failed disable"
        );
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "running.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start supervised sleeper");
        assert!(
            daemon
                .entrypoint_supervisor()
                .snapshots()
                .iter()
                .any(|snapshot| {
                    snapshot.package_name == "running.plugin"
                        && snapshot.entrypoint_id == "sleeper"
                        && snapshot.state == "running"
                }),
            "sleeper must be running before failed disable"
        );

        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::DisablePackage {
                package_name: "running.plugin".to_string(),
            },
        )
        .expect_err("injected disable persist failure");
        assert_eq!(
            package_state(&daemon, "running.plugin"),
            PackageState::Enabled
        );
        assert!(
            daemon
                .entrypoint_supervisor()
                .snapshots()
                .iter()
                .any(|snapshot| {
                    snapshot.package_name == "running.plugin"
                        && snapshot.entrypoint_id == "sleeper"
                        && snapshot.state == "running"
                }),
            "failed disable must not stop the running entrypoint"
        );
        assert!(
            plugin_is_loaded(&daemon, "running.plugin"),
            "failed disable commit must keep the plugin loaded"
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn enable_load_failure_rolls_back_registry_and_durable_state() {
        let root = unique_package_control_dir("enable-load-rollback");
        let data_directory = root.join("data");
        let package_dir = root.join("broken.plugin");
        write_package_control_manifest(
            &package_dir,
            "broken.plugin",
            serde_json::json!({
                "capabilities": [{ "surface": "surfaces" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(package_dir.join("plugin.lua"), "-- placeholder").expect("write lua");
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start enable-load daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install broken package");
        std::fs::remove_file(package_dir.join("plugin.lua")).expect("remove lua before enable");

        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "broken.plugin".to_string(),
            },
        )
        .expect_err("missing lua must fail enable load");
        assert_eq!(
            package_state(&daemon, "broken.plugin"),
            PackageState::Installed
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_reload_keeps_prior_registry_and_runtime_state() {
        let root = unique_package_control_dir("reload-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", serde_json::json!({}));
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start reload daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install reload package");
        let before = daemon.package_registry().snapshot();
        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("injected reload persist failure");
        assert_eq!(daemon.package_registry().snapshot(), before);
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_reload_after_commit_restores_prior_process() {
        let root = unique_package_control_dir("reload-restart-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start reload runtime daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect("enable");
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "reload.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start");
        assert!(entrypoint_is_running(&mut daemon, "reload.plugin"));

        write_broken_entrypoint(&package_dir);
        write_package_control_manifest(&package_dir, "reload.plugin", broken_sleeper_manifest());
        drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("broken candidate restart must fail");
        assert_eq!(entrypoint_command(&daemon, "reload.plugin"), "bin/sleeper");
        assert!(
            entrypoint_is_running(&mut daemon, "reload.plugin"),
            "compensation must restart the prior sleeper definition"
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_refresh_restores_earlier_package_runtime() {
        let root = unique_package_control_dir("refresh-later-failure");
        let data_directory = root.join("data");
        let alpha_dir = root.join("alpha.plugin");
        let zeta_dir = root.join("zeta.plugin");
        write_package_control_manifest(&alpha_dir, "alpha.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&alpha_dir);
        write_package_control_manifest(&zeta_dir, "zeta.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&zeta_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start refresh daemon");
        for dir in [&alpha_dir, &zeta_dir] {
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: dir.clone() },
            )
            .expect("install");
        }
        for name in ["alpha.plugin", "zeta.plugin"] {
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: name.to_string(),
                },
            )
            .expect("enable");
            drive_package_request(
                &mut daemon,
                DaemonRequest::StartPackageEntrypoint {
                    package_name: name.to_string(),
                    entrypoint_id: "sleeper".to_string(),
                    environment_overrides: BTreeMap::new(),
                },
            )
            .expect("start");
        }
        write_package_control_manifest(&alpha_dir, "alpha.plugin", sleeper_manifest(&["31"]));
        write_broken_entrypoint(&zeta_dir);
        write_package_control_manifest(&zeta_dir, "zeta.plugin", broken_sleeper_manifest());

        drive_package_request(&mut daemon, DaemonRequest::RefreshLocalPackages)
            .expect_err("later zeta restart must fail refresh");
        assert_eq!(entrypoint_command(&daemon, "alpha.plugin"), "bin/sleeper");
        assert_eq!(
            daemon
                .package_registry()
                .package("alpha.plugin")
                .expect("alpha")
                .runnable_entrypoints[0]
                .args,
            ["30"]
        );
        assert!(entrypoint_is_running(&mut daemon, "alpha.plugin"));
        assert_eq!(entrypoint_command(&daemon, "zeta.plugin"), "bin/sleeper");
        assert!(entrypoint_is_running(&mut daemon, "zeta.plugin"));
        daemon.stop();
    }

    #[test]
    fn enable_load_rollback_persist_failure_preserves_original_error() {
        let root = unique_package_control_dir("rollback-persist-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("broken.plugin");
        write_package_control_manifest(
            &package_dir,
            "broken.plugin",
            serde_json::json!({
                "capabilities": [{ "surface": "surfaces" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(package_dir.join("plugin.lua"), "-- placeholder").expect("write lua");
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start rollback persist daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        std::fs::remove_file(package_dir.join("plugin.lua")).expect("remove lua");
        FileHubStateStore::inject_save_failure_after(1);
        let error = drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "broken.plugin".to_string(),
            },
        )
        .expect_err("load failure plus rollback persist failure");
        let DaemonTransportError::PackageCompensation {
            original,
            rollbacks,
        } = error
        else {
            panic!("expected typed compensation error, got {error:?}");
        };
        assert!(matches!(&*original, DaemonTransportError::Package(_)));
        assert!(rollbacks.iter().any(|rollback| rollback.step == "persist"
            && matches!(
                &*rollback.error,
                DaemonTransportError::State(crate::HubStateStoreError::InjectedWriteFailure)
            )));
        daemon.stop();
    }

    #[test]
    fn reload_restore_failure_preserves_original_and_runtime_error() {
        let root = unique_package_control_dir("restore-runtime-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start restore-failure daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect("enable");
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "reload.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start");
        write_broken_entrypoint(&package_dir);
        write_package_control_manifest(&package_dir, "reload.plugin", broken_sleeper_manifest());
        std::fs::remove_file(package_dir.join("bin/sleeper")).expect("delete prior binary");
        let error = drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("restart and restore should both fail");
        let DaemonTransportError::PackageCompensation {
            original,
            rollbacks,
        } = error
        else {
            panic!("expected typed compensation error, got {error:?}");
        };
        assert!(matches!(&*original, DaemonTransportError::Entrypoint(_)));
        assert!(
            rollbacks
                .iter()
                .any(|rollback| rollback.step == "entrypoint"
                    && rollback.package_name.as_deref() == Some("reload.plugin"))
        );
        daemon.stop();
    }

    #[test]
    fn session_type_generation_advances_only_after_successful_commit() {
        let root = unique_package_control_dir("session-type-generation");
        let data_directory = root.join("data");
        let package_dir = root.join("types.plugin");
        write_package_control_manifest(
            &package_dir,
            "types.plugin",
            serde_json::json!({
                "session_types": [{
                    "id": "init",
                    "label": "Mutate agent",
                    "role": "botster.agent",
                    "interaction": "interactive",
                    "traits": ["test"],
                    "lifecycle": "task",
                    "command": "bin/init.sh"
                }]
            }),
        );
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start generation daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install types package");
        let generation_after_install = daemon
            .runtime()
            .expect("runtime")
            .state()
            .session_type_generation;

        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "types.plugin".to_string(),
            },
        )
        .expect_err("injected enable persist failure");
        assert_eq!(
            daemon
                .runtime()
                .expect("runtime")
                .state()
                .session_type_generation,
            generation_after_install
        );

        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "types.plugin".to_string(),
            },
        )
        .expect("enable types package");
        assert!(
            daemon
                .runtime()
                .expect("runtime")
                .state()
                .session_type_generation
                > generation_after_install,
            "successful enable must advance session-type generation after commit"
        );
        daemon.stop();
    }
}
