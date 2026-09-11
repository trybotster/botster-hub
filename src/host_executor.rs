//! Fixed, bounded execution for Hub work that must not run on the owner thread.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread;
use std::time::Instant;

use serde_json::Value;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::session_types::{
    SessionTypeCatalogBuild, bounded_session_type_catalog_entities,
};
use crate::entrypoint_supervisor::EntrypointSupervisor;
use crate::managed_git_worktrees::{
    ManagedGitRequest, ManagedWorktreeDecision, PreparedManagedWorktree,
    create_managed_worktree_effect, finalize_managed_worktree,
};
use crate::owner_identity::OwnerWorkIdentity;
use crate::packages::PackageRegistry;
use crate::persistence::HubState;
use crate::shared_view::SharedView;

pub(crate) const HOST_WORKER_COUNT: usize = 2;
pub(crate) const HOST_OPERATION_CAPACITY: usize = 8;
pub(crate) const HOST_PREPARED_BYTE_CAPACITY: usize = 8 * 1024 * 1024;
pub(crate) const HOST_PREPARED_AGGREGATE_BYTE_CAPACITY: usize =
    HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY;

pub(crate) type HostJobIdentity = OwnerWorkIdentity;

#[derive(Debug, Clone)]
pub(crate) struct HostError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl HostError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

pub(crate) enum HostCommand {
    DeliverCoordinationResponse {
        response: crate::lua_runtime::CoordinationReplySender,
        result: crate::lua_runtime::CoordinationDelivery,
        discard: Option<crate::data_plane::driver::CoreRejectedRequest>,
    },
    ClientEventCleanup {
        router: Arc<crate::package_event_router::PackageEventRouter>,
        work: crate::subscription::package_events::ClientCleanupWork,
    },
    #[cfg(test)]
    DisposalProbe(TestDisposalProbe),
    Dispose(Box<HostCommand>),
    TerminalDispose(crate::host_disposal::Work),
    DiscardCompletion(Box<HostResult>),
    EntityModel(crate::runtime::entity_model::Work),
    ReclaimEntityModel(crate::runtime::entity_model::Work),
    EventOwner {
        router: Arc<crate::package_event_router::PackageEventRouter>,
        work: crate::package_event_router::EventOwnerWork,
    },
    PluginEntity(crate::plugin_entity::Command),
    PreparePluginResponse {
        input: crate::plugin_response::PluginResponseInput,
        reply_tx: crate::daemon::control::message::ControlReplySender,
        reply_live: Arc<AtomicBool>,
    },
    PrepareStatusResponse(crate::status_response::StatusResponseInput),
    StopForStatus(crate::status_response::PreparedStatusResponse),
    DeliverStatusResponse {
        prepared: crate::status_response::PreparedStatusResponse,
        reply_tx: crate::daemon::control::message::ControlReplySender,
    },
    StopEntrypoints,
    BuildSessionTypeCatalog {
        generation: u64,
        packages: SharedView<PackageRegistry>,
        state: SharedView<HubState>,
    },
    Mutation(crate::host_mutations::HostMutationCommand),
    ReclaimSessionTypeCatalog(SessionTypeCatalogReclamation),
    /// Run this external-effect phase before document admission. The owner must
    /// not hold the shared document reservation while Git runs.
    CreateManagedWorktree {
        request: ManagedGitRequest,
    },
    /// Complete the external-effect phase with the same operation permit and a
    /// later phase serial after the owner makes its document decision.
    FinalizeManagedWorktree {
        prepared: PreparedManagedWorktree,
        decision: ManagedWorktreeDecision,
        deadline: Instant,
        discard: Option<Box<crate::host_mutations::PreparedMutation>>,
        suppress_rollback: Arc<Mutex<BTreeSet<String>>>,
        #[cfg(test)]
        rollback_hold: Option<Arc<TestHostGate>>,
    },
    #[cfg(test)]
    Panic {
        generation: u64,
    },
    #[cfg(test)]
    Wait {
        generation: u64,
        gate: Arc<TestHostGate>,
    },
}

impl HostCommand {
    fn generation(&self) -> u64 {
        match self {
            Self::DeliverCoordinationResponse { .. } => 0,
            #[cfg(test)]
            Self::DisposalProbe(_) => 0,
            Self::Dispose(_) | Self::TerminalDispose(_) | Self::DiscardCompletion(_) => 0,
            Self::EntityModel(_) | Self::ReclaimEntityModel(_) => 0,
            Self::EventOwner { .. } | Self::ClientEventCleanup { .. } => 0,
            Self::PluginEntity(_) | Self::PreparePluginResponse { .. } | Self::StopEntrypoints => 0,
            Self::PrepareStatusResponse(_)
            | Self::StopForStatus(_)
            | Self::DeliverStatusResponse { .. } => 0,
            Self::BuildSessionTypeCatalog { generation, .. } => *generation,
            Self::Mutation(_) => 0,
            Self::ReclaimSessionTypeCatalog(_) => 0,
            Self::CreateManagedWorktree { .. } | Self::FinalizeManagedWorktree { .. } => 0,
            #[cfg(test)]
            Self::Panic { generation } => *generation,
            #[cfg(test)]
            Self::Wait { generation, .. } => *generation,
        }
    }
}

impl std::fmt::Debug for HostCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeliverCoordinationResponse { .. } => {
                formatter.write_str("DeliverCoordinationResponse")
            }
            Self::ClientEventCleanup { .. } => formatter.write_str("ClientEventCleanup"),
            #[cfg(test)]
            Self::DisposalProbe(_) => formatter.write_str("DisposalProbe"),
            Self::Dispose(_) => formatter.write_str("Dispose"),
            Self::TerminalDispose(_) => formatter.write_str("TerminalDispose"),
            Self::DiscardCompletion(_) => formatter.write_str("DiscardCompletion"),
            Self::EntityModel(work) => formatter
                .debug_tuple("EntityModel")
                .field(&work.kind())
                .finish(),
            Self::ReclaimEntityModel(_) => formatter.write_str("ReclaimEntityModel"),
            Self::EventOwner { work, .. } => formatter
                .debug_tuple("EventOwner")
                .field(work.identity())
                .finish(),
            Self::PluginEntity(_) => formatter.write_str("PluginEntity"),
            Self::PreparePluginResponse { .. } => formatter.write_str("PreparePluginResponse"),
            Self::PrepareStatusResponse(_) => formatter.write_str("PrepareStatusResponse"),
            Self::StopForStatus(_) => formatter.write_str("StopForStatus"),
            Self::DeliverStatusResponse { .. } => formatter.write_str("DeliverStatusResponse"),
            Self::StopEntrypoints => formatter.write_str("StopEntrypoints"),
            Self::BuildSessionTypeCatalog { generation, .. } => formatter
                .debug_struct("BuildSessionTypeCatalog")
                .field("generation", generation)
                .finish_non_exhaustive(),
            Self::Mutation(command) => formatter.debug_tuple("Mutation").field(command).finish(),
            Self::ReclaimSessionTypeCatalog(_) => formatter.write_str("ReclaimSessionTypeCatalog"),
            Self::CreateManagedWorktree { .. } => formatter
                .debug_struct("CreateManagedWorktree")
                .finish_non_exhaustive(),
            Self::FinalizeManagedWorktree { decision, .. } => formatter
                .debug_struct("FinalizeManagedWorktree")
                .field("decision", decision)
                .finish_non_exhaustive(),
            #[cfg(test)]
            Self::Panic { generation } => formatter
                .debug_struct("Panic")
                .field("generation", generation)
                .finish(),
            #[cfg(test)]
            Self::Wait { generation, .. } => formatter
                .debug_struct("Wait")
                .field("generation", generation)
                .finish_non_exhaustive(),
        }
    }
}

/// One superseded catalog allocation and its retained byte charge.
pub(crate) struct SessionTypeCatalogReclamation {
    pub(crate) entities: BTreeMap<String, Value>,
    pub(crate) prepared_charge: Option<HostPreparedCharge>,
}

#[derive(Debug)]
pub(crate) struct HostJob {
    pub(crate) identity: HostJobIdentity,
    pub(crate) command: HostCommand,
    permit: HostWorkPermit,
}

#[derive(Debug)]
pub(crate) enum HostResult {
    CoordinationResponseDelivered {
        received: bool,
    },
    ClientEventCleanup(
        Result<
            crate::subscription::package_events::ClientCleanupCompletion,
            crate::subscription::package_events::ClientCleanupFailure,
        >,
    ),
    EntityModelComplete(crate::runtime::entity_model::Kind),
    EventOwner(
        Result<
            crate::package_event_router::EventOwnerCompletion,
            crate::package_event_router::EventOwnerWorkError,
        >,
    ),
    PluginEntity(crate::plugin_entity::Completion),
    PluginResponseAbandoned,
    PluginResponseDelivered {
        kind: botster_hub_client::DaemonResponseKind,
    },
    EntrypointsStopped,
    StatusResponsePrepared(crate::status_response::PreparedStatusResponse),
    StatusResponseDelivered {
        shutdown: bool,
        received: bool,
    },
    SessionTypeCatalogReady {
        generation: u64,
        entities: BTreeMap<String, Value>,
        logical_bytes: usize,
    },
    Failed {
        generation: u64,
        error: HostError,
    },
    Mutation(crate::host_mutations::HostMutationResult),
    ManagedWorktreeCreated(PreparedManagedWorktree),
    ManagedWorktreeFailed(crate::managed_git_worktrees::ManagedGitError),
    ManagedWorktreeFinalized,
    ManagedWorktreeRecoveryRequired {
        prepared: PreparedManagedWorktree,
        error: HostError,
    },
}

impl HostResult {
    fn generation(&self) -> u64 {
        match self {
            Self::CoordinationResponseDelivered { .. } => 0,
            Self::EntityModelComplete(_) => 0,
            Self::EventOwner(_) | Self::ClientEventCleanup(_) => 0,
            Self::StatusResponsePrepared(_) | Self::StatusResponseDelivered { .. } => 0,
            Self::PluginEntity(_)
            | Self::PluginResponseAbandoned
            | Self::PluginResponseDelivered { .. }
            | Self::EntrypointsStopped => 0,
            Self::SessionTypeCatalogReady { generation, .. } | Self::Failed { generation, .. } => {
                *generation
            }
            Self::Mutation(_) => 0,
            Self::ManagedWorktreeCreated(_)
            | Self::ManagedWorktreeFailed(_)
            | Self::ManagedWorktreeFinalized
            | Self::ManagedWorktreeRecoveryRequired { .. } => 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct HostCompletion {
    pub(crate) identity: HostJobIdentity,
    pub(crate) result: HostResult,
    permit: HostWorkPermit,
}

impl HostCompletion {
    pub(crate) fn from_parts(
        identity: HostJobIdentity,
        result: HostResult,
        permit: HostWorkPermit,
    ) -> Self {
        Self {
            identity,
            result,
            permit,
        }
    }

    pub(crate) fn release(self) -> (HostResult, HostPreparedCharge) {
        let Self {
            mut result, permit, ..
        } = self;
        normalize_result_size(&mut result);
        let logical_bytes = result_logical_bytes(&result);
        let charge = permit.into_prepared_charge(logical_bytes);
        (result, charge)
    }

    /// Retain this operation slot for an off-owner reclamation job.
    pub(crate) fn release_for_reclamation(
        self,
    ) -> (HostResult, HostPreparedCharge, HostWorkPermit) {
        let Self {
            mut result,
            mut permit,
            ..
        } = self;
        normalize_result_size(&mut result);
        let logical_bytes = result_logical_bytes(&result);
        let charge = permit.take_prepared_charge(logical_bytes);
        (result, charge, permit)
    }

    pub(crate) fn into_parts(self) -> (HostJobIdentity, HostResult, HostWorkPermit) {
        (self.identity, self.result, self.permit)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        identity: HostJobIdentity,
        result: HostResult,
        permit: HostWorkPermit,
    ) -> Self {
        Self {
            identity,
            result,
            permit,
        }
    }
}

fn normalize_result_size(result: &mut HostResult) {
    if result_logical_bytes(result) > HOST_PREPARED_BYTE_CAPACITY {
        let generation = result.generation();
        *result = HostResult::Failed {
            generation,
            error: HostError::new(
                "host_result_too_large",
                "host result exceeds its prepared-byte reservation",
            ),
        };
    }
}

fn result_logical_bytes(result: &HostResult) -> usize {
    match result {
        HostResult::CoordinationResponseDelivered { .. } => 0,
        HostResult::StatusResponsePrepared(prepared) => prepared.logical_bytes(),
        HostResult::StatusResponseDelivered { .. } => 0,
        HostResult::EntityModelComplete(_) => 0,
        // The router retains each allocation charge until worker destruction completes.
        HostResult::EventOwner(_) | HostResult::ClientEventCleanup(_) => 0,
        HostResult::PluginEntity(crate::plugin_entity::Completion::Finished { .. }) => 0,
        HostResult::PluginEntity(_) => HOST_PREPARED_BYTE_CAPACITY,
        HostResult::PluginResponseAbandoned
        | HostResult::PluginResponseDelivered { .. }
        | HostResult::EntrypointsStopped => 0,
        HostResult::SessionTypeCatalogReady { logical_bytes, .. } => *logical_bytes,
        HostResult::Failed { .. } | HostResult::Mutation(_) => 0,
        HostResult::ManagedWorktreeCreated(_)
        | HostResult::ManagedWorktreeFailed(_)
        | HostResult::ManagedWorktreeFinalized
        | HostResult::ManagedWorktreeRecoveryRequired { .. } => 0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostSubmitError {
    Full,
    Stopped,
    PhaseExhausted,
    WrongExecutor,
}

/// Submission failure retains the command and its operation slot.
#[derive(Debug)]
pub(crate) struct HostSubmissionFailure {
    pub(crate) error: HostSubmitError,
    pub(crate) identity: HostJobIdentity,
    pub(crate) command: HostCommand,
    pub(crate) permit: HostWorkPermit,
}

/// A live shutdown caller retains this complete owner when disposal refuses work.
/// Final destruction of this owner requires the terminal integration contract.
#[derive(Debug)]
pub(crate) struct HostCompletionDisposalFailure {
    pub(crate) failed: HostSubmissionFailure,
    remaining: mpsc::Receiver<HostCompletion>,
}

impl HostCompletionDisposalFailure {
    pub(crate) fn retry(self) -> Result<(), Self> {
        dispose_completion_receiver(self.remaining, Some(self.failed))
    }
}

fn dispose_completion_receiver(
    remaining: mpsc::Receiver<HostCompletion>,
    mut failed: Option<HostSubmissionFailure>,
) -> Result<(), HostCompletionDisposalFailure> {
    loop {
        let (identity, command, permit) = match failed.take() {
            Some(failed) => (failed.identity, failed.command, failed.permit),
            None => match remaining.try_recv() {
                Ok(completion) => {
                    let (identity, result, permit) = completion.into_parts();
                    (
                        identity,
                        HostCommand::DiscardCompletion(Box::new(result)),
                        permit,
                    )
                }
                Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => return Ok(()),
            },
        };
        if let Err(failed) = permit.dispose(identity, command) {
            return Err(HostCompletionDisposalFailure { failed, remaining });
        }
    }
}

type HostCompletionMailbox = Mutex<Option<mpsc::Receiver<HostCompletion>>>;

#[derive(Debug)]
pub(crate) enum HostCompletionPoll {
    Ready(HostCompletion),
    Empty,
    Stopped,
}

#[derive(Debug)]
struct HostWake {
    completion_pending: AtomicBool,
    capacity_pending: AtomicBool,
    owner: Mutex<Option<ControlSender>>,
    terminal_owner: Mutex<Option<std::thread::Thread>>,
}

impl HostWake {
    fn new() -> Self {
        Self {
            completion_pending: AtomicBool::new(false),
            capacity_pending: AtomicBool::new(false),
            owner: Mutex::new(None),
            terminal_owner: Mutex::new(None),
        }
    }

    fn bind(&self, sender: ControlSender) {
        // `serve_daemon` binds before the owner can submit a host job. Install
        // the sender first so this method also remains safe if that order changes.
        let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let should_wake = self.completion_pending.load(Ordering::Acquire)
            || self.capacity_pending.load(Ordering::Acquire);
        drop(owner);
        if should_wake {
            let _ = sender.try_send(ControlMessage::HostProgressPublished);
        }
    }

    fn publish_completion(&self) {
        if !self.completion_pending.swap(true, Ordering::AcqRel) {
            self.wake_owner();
        }
    }

    fn publish_capacity(&self) {
        if !self.capacity_pending.swap(true, Ordering::AcqRel) {
            self.wake_owner();
        }
    }

    fn wake_owner(&self) {
        if let Some(owner) = self
            .terminal_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            owner.unpark();
        }
        if let Some(sender) = self
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = sender.try_send(ControlMessage::HostProgressPublished);
        }
    }
}

#[derive(Debug)]
struct HostPermitPool {
    #[cfg(test)]
    status_stops: AtomicUsize,
    #[cfg(test)]
    refuse_status_delivery: AtomicBool,
    outstanding: AtomicUsize,
    wake: Arc<HostWake>,
}

#[derive(Debug)]
struct HostPreparedPool {
    used: AtomicUsize,
    wake: Arc<HostWake>,
}

#[derive(Debug)]
struct HostPreparedReservation {
    pool: Arc<HostPreparedPool>,
    logical_bytes: usize,
}

impl HostPreparedReservation {
    fn into_charge(mut self, logical_bytes: usize) -> HostPreparedCharge {
        debug_assert!(logical_bytes <= self.logical_bytes);
        let released = self.logical_bytes.saturating_sub(logical_bytes);
        if released > 0 {
            self.pool.used.fetch_sub(released, Ordering::AcqRel);
        }
        self.logical_bytes = 0;
        HostPreparedCharge {
            pool: Arc::clone(&self.pool),
            logical_bytes,
            _retained: None,
        }
    }
}

impl Drop for HostPreparedReservation {
    fn drop(&mut self) {
        if self.logical_bytes > 0 {
            self.pool
                .used
                .fetch_sub(self.logical_bytes, Ordering::AcqRel);
            self.pool.wake.publish_capacity();
        }
    }
}

#[derive(Debug)]
pub(crate) struct HostPreparedCharge {
    pool: Arc<HostPreparedPool>,
    logical_bytes: usize,
    _retained: Option<Arc<HostPreparedReservation>>,
}

/// A retained record shares the original reservation until its last handle drops.
#[derive(Debug, Clone)]
pub(crate) struct HostRetainedPrepared {
    _reservation: Arc<HostPreparedReservation>,
}

impl Drop for HostPreparedCharge {
    fn drop(&mut self) {
        if self.logical_bytes > 0 {
            self.pool
                .used
                .fetch_sub(self.logical_bytes, Ordering::AcqRel);
            self.pool.wake.publish_capacity();
        }
    }
}

#[derive(Debug)]
pub(crate) struct HostWorkPermit {
    pool: Arc<HostPermitPool>,
    prepared: Option<Arc<HostPreparedReservation>>,
    disposal: mpsc::SyncSender<HostJob>,
}

impl HostWorkPermit {
    pub(crate) fn disposal_notifier(&self) -> impl FnOnce() + use<> {
        let wake = Arc::clone(&self.pool.wake);
        move || wake.publish_completion()
    }

    /// Use the original slot to discard a command on an existing Host worker.
    /// A refusal returns the complete command and permit to its caller.
    pub(crate) fn dispose(
        self,
        identity: HostJobIdentity,
        command: HostCommand,
    ) -> Result<(), HostSubmissionFailure> {
        let sender = self.disposal.clone();
        let job = HostJob {
            identity,
            command: HostCommand::Dispose(Box::new(command)),
            permit: self,
        };
        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(error) => {
                let (error, job) = match error {
                    mpsc::TrySendError::Full(job) => (HostSubmitError::Full, job),
                    mpsc::TrySendError::Disconnected(job) => (HostSubmitError::Stopped, job),
                };
                let HostCommand::Dispose(command) = job.command else {
                    unreachable!("disposal submission retains its wrapper");
                };
                Err(HostSubmissionFailure {
                    error,
                    identity: job.identity,
                    command: *command,
                    permit: job.permit,
                })
            }
        }
    }

    pub(crate) fn has_retained_prepared_reservation(&self) -> bool {
        self.prepared
            .as_ref()
            .is_some_and(|reservation| Arc::strong_count(reservation) > 1)
    }

    pub(crate) fn retain_prepared_reservation(&self) -> HostRetainedPrepared {
        HostRetainedPrepared {
            _reservation: Arc::clone(
                self.prepared
                    .as_ref()
                    .expect("the phase retains its reservation"),
            ),
        }
    }

    pub(crate) fn reserved_prepared_bytes(&self) -> usize {
        self.prepared
            .as_ref()
            .map_or(0, |reservation| reservation.logical_bytes)
    }

    pub(crate) fn into_prepared_charge(mut self, logical_bytes: usize) -> HostPreparedCharge {
        self.take_prepared_charge(logical_bytes)
    }

    /// Consume the prepared-byte reservation. Call this only in a terminal phase.
    pub(crate) fn take_prepared_charge(&mut self, logical_bytes: usize) -> HostPreparedCharge {
        let reservation = self
            .prepared
            .take()
            .expect("host work permit owns a prepared-byte reservation");
        match Arc::try_unwrap(reservation) {
            Ok(reservation) => reservation.into_charge(logical_bytes),
            Err(reservation) => HostPreparedCharge {
                pool: Arc::clone(&reservation.pool),
                logical_bytes: 0,
                _retained: Some(reservation),
            },
        }
    }
}

impl Drop for HostWorkPermit {
    fn drop(&mut self) {
        let previous = self.pool.outstanding.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "host operation permit underflow");
        self.pool.wake.publish_capacity();
    }
}

pub(crate) struct HostExecutor {
    jobs: Option<mpsc::SyncSender<HostJob>>,
    completions: Arc<HostCompletionMailbox>,
    permits: Arc<HostPermitPool>,
    prepared: Arc<HostPreparedPool>,
    wake: Arc<HostWake>,
    stopping: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for HostExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostExecutor")
            .field("worker_count", &self.workers.len())
            .field(
                "outstanding",
                &self.permits.outstanding.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl HostExecutor {
    pub(crate) fn new() -> Self {
        let (jobs_tx, jobs_rx) = mpsc::sync_channel::<HostJob>(HOST_OPERATION_CAPACITY);
        let (completions_tx, completions_rx) =
            mpsc::sync_channel::<HostCompletion>(HOST_OPERATION_CAPACITY);
        let jobs_rx = Arc::new(Mutex::new(jobs_rx));
        let completions = Arc::new(Mutex::new(Some(completions_rx)));
        let wake = Arc::new(HostWake::new());
        let permits = Arc::new(HostPermitPool {
            #[cfg(test)]
            status_stops: AtomicUsize::new(0),
            #[cfg(test)]
            refuse_status_delivery: AtomicBool::new(false),
            outstanding: AtomicUsize::new(0),
            wake: Arc::clone(&wake),
        });
        let prepared = Arc::new(HostPreparedPool {
            used: AtomicUsize::new(0),
            wake: Arc::clone(&wake),
        });
        let stopping = Arc::new(AtomicBool::new(false));
        let entrypoints = Arc::new(Mutex::new(EntrypointSupervisor::default()));
        let workers = (0..HOST_WORKER_COUNT)
            .map(|index| {
                let jobs = Arc::clone(&jobs_rx);
                let completion_mailbox = Arc::downgrade(&completions);
                let completions = completions_tx.clone();
                let wake = Arc::clone(&wake);
                let stopping = Arc::clone(&stopping);
                let entrypoints = Arc::clone(&entrypoints);
                thread::Builder::new()
                    .name(format!("botster-hub-host-{index}"))
                    .spawn(move || {
                        run_worker(
                            jobs,
                            completions,
                            completion_mailbox,
                            wake,
                            stopping,
                            entrypoints,
                        )
                    })
                    .expect("start bounded Hub host worker")
            })
            .collect();
        Self {
            jobs: Some(jobs_tx),
            completions,
            permits,
            prepared,
            wake,
            stopping,
            workers,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_stop_submissions(&mut self) {
        self.jobs.take();
    }

    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        self.wake.bind(sender);
    }

    /// Terminal shutdown keeps a wake even when the control channel has closed.
    pub(crate) fn bind_terminal_owner(&self) {
        *self
            .wake
            .terminal_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::thread::current());
    }

    pub(crate) fn try_reserve(&self) -> Option<HostWorkPermit> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        let disposal = self.jobs.as_ref()?;
        let mut outstanding = self.permits.outstanding.load(Ordering::Acquire);
        loop {
            if outstanding >= HOST_OPERATION_CAPACITY {
                return None;
            }
            match self.permits.outstanding.compare_exchange_weak(
                outstanding,
                outstanding + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let mut prepared_used = self.prepared.used.load(Ordering::Acquire);
                    loop {
                        let Some(next) = prepared_used.checked_add(HOST_PREPARED_BYTE_CAPACITY)
                        else {
                            self.permits.outstanding.fetch_sub(1, Ordering::AcqRel);
                            return None;
                        };
                        if next > HOST_PREPARED_AGGREGATE_BYTE_CAPACITY {
                            self.permits.outstanding.fetch_sub(1, Ordering::AcqRel);
                            return None;
                        }
                        match self.prepared.used.compare_exchange_weak(
                            prepared_used,
                            next,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(observed) => prepared_used = observed,
                        }
                    }
                    return Some(HostWorkPermit {
                        pool: Arc::clone(&self.permits),
                        disposal: disposal.clone(),
                        prepared: Some(Arc::new(HostPreparedReservation {
                            pool: Arc::clone(&self.prepared),
                            logical_bytes: HOST_PREPARED_BYTE_CAPACITY,
                        })),
                    });
                }
                Err(observed) => outstanding = observed,
            }
        }
    }

    pub(crate) fn submit(
        &self,
        identity: HostJobIdentity,
        command: HostCommand,
        permit: HostWorkPermit,
    ) -> Result<(), HostSubmissionFailure> {
        if !Arc::ptr_eq(&self.permits, &permit.pool)
            || permit
                .prepared
                .as_ref()
                .is_some_and(|prepared| !Arc::ptr_eq(&self.prepared, &prepared.pool))
        {
            return Err(HostSubmissionFailure {
                error: HostSubmitError::WrongExecutor,
                identity,
                command,
                permit,
            });
        }
        if self.stopping.load(Ordering::Acquire) {
            return Err(HostSubmissionFailure {
                error: HostSubmitError::Stopped,
                identity,
                command,
                permit,
            });
        }
        #[cfg(test)]
        if matches!(command, HostCommand::DeliverStatusResponse { .. })
            && self.permits.refuse_status_delivery.load(Ordering::Acquire)
        {
            return Err(HostSubmissionFailure {
                error: HostSubmitError::Stopped,
                identity,
                command,
                permit,
            });
        }
        let job = HostJob {
            identity,
            command,
            permit,
        };
        let failure = match self.jobs.as_ref() {
            Some(jobs) => match jobs.try_send(job) {
                Ok(()) => return Ok(()),
                // Each queued command owns a permit. The queue and permit pool
                // both hold eight operations, so a reserved command has space.
                // Preserve the command if this invariant fails.
                Err(mpsc::TrySendError::Full(job)) => (HostSubmitError::Full, job),
                Err(mpsc::TrySendError::Disconnected(job)) => (HostSubmitError::Stopped, job),
            },
            None => (HostSubmitError::Stopped, job),
        };
        let (
            error,
            HostJob {
                identity,
                command,
                permit,
            },
        ) = failure;
        Err(HostSubmissionFailure {
            error,
            identity,
            command,
            permit,
        })
    }

    pub(crate) fn poll_completion(&self) -> HostCompletionPoll {
        poll_completion_mailbox(&self.completions)
    }

    /// Close publication under its existing mutex, then transfer buffered results to Host.
    /// The caller must retain and retry the complete failure owner on refusal.
    /// A later successful close does not resolve a previously returned failure.
    pub(crate) fn close_and_dispose_completions(
        &self,
    ) -> Result<(), HostCompletionDisposalFailure> {
        let remaining = {
            let mut mailbox = self
                .completions
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.stopping.store(true, Ordering::Release);
            mailbox.take()
        };
        match remaining {
            Some(remaining) => dispose_completion_receiver(remaining, None),
            None => Ok(()),
        }
    }

    pub(crate) fn take_completion_notification(&self) -> bool {
        self.wake.completion_pending.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_capacity_notification(&self) -> bool {
        self.wake.capacity_pending.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn outstanding(&self) -> usize {
        self.permits.outstanding.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_terminal_probe(&self) -> impl Fn() -> (usize, bool, bool) + Send + 'static {
        let permits = Arc::clone(&self.permits);
        let mailbox = Arc::clone(&self.completions);
        let stopping = Arc::clone(&self.stopping);
        move || {
            (
                permits.outstanding.load(Ordering::Acquire),
                mailbox.lock().unwrap().is_some(),
                stopping.load(Ordering::Acquire),
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn status_stop_count(&self) -> usize {
        self.permits.status_stops.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn prepared_bytes(&self) -> usize {
        self.prepared.used.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_status_delivery(&self) {
        self.permits
            .refuse_status_delivery
            .store(true, Ordering::Release);
    }
}

fn poll_completion_mailbox(completions: &HostCompletionMailbox) -> HostCompletionPoll {
    let mailbox = completions
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(receiver) = mailbox.as_ref() else {
        return HostCompletionPoll::Stopped;
    };
    match receiver.try_recv() {
        Ok(completion) => HostCompletionPoll::Ready(completion),
        Err(mpsc::TryRecvError::Empty) => HostCompletionPoll::Empty,
        Err(mpsc::TryRecvError::Disconnected) => HostCompletionPoll::Stopped,
    }
}

impl Drop for HostExecutor {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.jobs.take();
        // The owner never waits for host work. Workers discard queued jobs and
        // finish any job already in execution. Dropped handles detach them.
        self.workers.clear();
    }
}

fn run_worker(
    jobs: Arc<Mutex<mpsc::Receiver<HostJob>>>,
    completions: mpsc::SyncSender<HostCompletion>,
    completion_mailbox: Weak<HostCompletionMailbox>,
    wake: Arc<HostWake>,
    stopping: Arc<AtomicBool>,
    entrypoints: Arc<Mutex<EntrypointSupervisor>>,
) {
    loop {
        let job = jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .recv();
        let Ok(job) = job else {
            return;
        };
        let HostJob {
            identity,
            command,
            mut permit,
        } = job;
        let command = match command {
            HostCommand::DiscardCompletion(result) => {
                drop(result);
                drop(permit);
                continue;
            }
            HostCommand::Dispose(command) => {
                match *command {
                    HostCommand::TerminalDispose(work) => work.run(permit),
                    command => {
                        drop(command);
                        drop(permit);
                    }
                }
                continue;
            }
            HostCommand::TerminalDispose(work) => {
                work.run(permit);
                continue;
            }
            HostCommand::ReclaimSessionTypeCatalog(reclamation) => {
                let SessionTypeCatalogReclamation {
                    entities,
                    prepared_charge,
                } = reclamation;
                drop(entities);
                drop(prepared_charge);
                drop(permit);
                continue;
            }
            command => command,
        };
        if stopping.load(Ordering::Acquire) {
            drop(command);
            drop(permit);
            continue;
        }
        let generation = command.generation();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute(identity, command, &entrypoints, &mut permit)
        }))
        .unwrap_or_else(|_| HostResult::Failed {
            generation,
            error: HostError::new("host_worker_panicked", "host worker execution panicked"),
        });
        let completion = HostCompletion {
            identity,
            result,
            permit,
        };
        if let Err((failure, completion)) =
            publish_completion(completion, &completions, &completion_mailbox, &stopping)
        {
            // An undelivered created worktree stays in its deterministic path.
            // Startup adoption publishes the same preserved external effect.
            eprintln!("Host completion publication failed: {failure:?}");
            drop(completion);
            continue;
        }
        // The completion is in the mailbox before this bit and doorbell publish.
        wake.publish_completion();
    }
}

#[derive(Debug)]
enum HostCompletionPublishFailure {
    Full,
    Closed,
}

fn publish_completion(
    completion: HostCompletion,
    sender: &mpsc::SyncSender<HostCompletion>,
    mailbox: &Weak<HostCompletionMailbox>,
    stopping: &AtomicBool,
) -> Result<(), (HostCompletionPublishFailure, HostCompletion)> {
    // A worker retains this Arc only during publication, never during the next job receive.
    let Some(mailbox) = mailbox.upgrade() else {
        stopping.store(true, Ordering::Release);
        return Err((HostCompletionPublishFailure::Closed, completion));
    };
    let guard = mailbox.lock().unwrap_or_else(|error| error.into_inner());
    let result = if guard.is_none() || stopping.load(Ordering::Acquire) {
        Err((HostCompletionPublishFailure::Closed, completion))
    } else {
        sender.try_send(completion).map_err(|error| match error {
            mpsc::TrySendError::Full(completion) => {
                (HostCompletionPublishFailure::Full, completion)
            }
            mpsc::TrySendError::Disconnected(completion) => {
                (HostCompletionPublishFailure::Closed, completion)
            }
        })
    };
    if result.is_err() {
        stopping.store(true, Ordering::Release);
    }
    drop(guard);
    drop(mailbox);
    result
}

fn execute(
    identity: HostJobIdentity,
    command: HostCommand,
    entrypoints: &Mutex<EntrypointSupervisor>,
    permit: &mut HostWorkPermit,
) -> HostResult {
    match command {
        HostCommand::DeliverCoordinationResponse {
            response,
            result,
            discard,
        } => {
            drop(discard);
            let received = match response.send(result) {
                Ok(()) => true,
                Err(result) => {
                    drop(result);
                    false
                }
            };
            HostResult::CoordinationResponseDelivered { received }
        }
        #[cfg(test)]
        HostCommand::DisposalProbe(probe) => {
            probe.executed.store(true, Ordering::Release);
            HostResult::StatusResponsePrepared(crate::status_response::PreparedStatusResponse {
                kind: botster_hub_client::DaemonResponseKind::Status,
                encoded_frame: None,
                shutdown: false,
                dispose_probe: Some(probe),
            })
        }
        HostCommand::Dispose(_)
        | HostCommand::TerminalDispose(_)
        | HostCommand::DiscardCompletion(_) => {
            unreachable!("worker handles disposal before execution")
        }
        HostCommand::EntityModel(work) => HostResult::EntityModelComplete(work.run(identity)),
        HostCommand::ReclaimEntityModel(work) => {
            HostResult::EntityModelComplete(work.reclaim(identity))
        }

        HostCommand::EventOwner { router, work } => HostResult::EventOwner(work.run(&router)),
        HostCommand::ClientEventCleanup { router, work } => {
            HostResult::ClientEventCleanup(work.run(&router))
        }
        HostCommand::PluginEntity(command) => {
            HostResult::PluginEntity(crate::plugin_entity::execute(command, permit))
        }
        HostCommand::PreparePluginResponse {
            input,
            reply_tx,
            reply_live,
        } => {
            if reply_tx.is_closed() || !reply_live.load(Ordering::Acquire) {
                drop(input);
                return HostResult::PluginResponseAbandoned;
            }
            let prepared = crate::plugin_response::prepare(input);
            let kind = prepared.kind;
            let charge = permit.take_prepared_charge(prepared.logical_bytes);
            // This exchange orders publication against owner cancellation.
            if reply_live.swap(false, Ordering::AcqRel) {
                let reply = crate::daemon::control::reply::ControlReply::prepared(
                    kind,
                    prepared.encoded_frame,
                    charge,
                );
                match reply_tx.send_reply(reply) {
                    Ok(()) => HostResult::PluginResponseDelivered { kind },
                    Err(reply) => {
                        // A closed transport returns the allocation to this worker.
                        drop(reply);
                        HostResult::PluginResponseAbandoned
                    }
                }
            } else {
                drop(prepared);
                drop(charge);
                HostResult::PluginResponseAbandoned
            }
        }

        HostCommand::PrepareStatusResponse(input) => HostResult::StatusResponsePrepared(
            crate::status_response::prepare(input, permit.reserved_prepared_bytes()),
        ),
        HostCommand::DeliverStatusResponse { prepared, reply_tx } => {
            let charge = permit.take_prepared_charge(prepared.logical_bytes());
            let shutdown = prepared.shutdown;
            let Some(encoded_frame) = prepared.encoded_frame else {
                drop(reply_tx);
                drop(charge);
                return HostResult::StatusResponseDelivered {
                    shutdown,
                    received: false,
                };
            };
            let reply = crate::daemon::control::reply::ControlReply::prepared(
                prepared.kind,
                encoded_frame,
                charge,
            );
            let received = match reply_tx.send_reply(reply) {
                Ok(()) => true,
                Err(reply) => {
                    drop(reply);
                    false
                }
            };
            HostResult::StatusResponseDelivered { shutdown, received }
        }
        HostCommand::StopForStatus(prepared) => {
            let mut entrypoints = entrypoints
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            drop(std::mem::take(&mut *entrypoints));
            #[cfg(test)]
            permit.pool.status_stops.fetch_add(1, Ordering::AcqRel);
            HostResult::StatusResponsePrepared(prepared)
        }
        HostCommand::StopEntrypoints => {
            let mut entrypoints = entrypoints
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            drop(std::mem::take(&mut *entrypoints));
            HostResult::EntrypointsStopped
        }
        HostCommand::BuildSessionTypeCatalog {
            generation,
            packages,
            state,
        } => match bounded_session_type_catalog_entities(
            &packages,
            &state,
            HOST_PREPARED_BYTE_CAPACITY,
        ) {
            Ok(SessionTypeCatalogBuild::Ready {
                entities,
                logical_bytes,
            }) => HostResult::SessionTypeCatalogReady {
                generation,
                entities,
                logical_bytes,
            },
            Ok(SessionTypeCatalogBuild::TooLarge) => HostResult::Failed {
                generation,
                error: HostError::new(
                    "host_result_too_large",
                    "session type catalog exceeds the host result limit",
                ),
            },
            Err(crate::daemon::error::DaemonTransportError::Client(
                crate::HubClientError::SessionType { kind, message, .. },
            )) => HostResult::Failed {
                generation,
                error: HostError::new(kind, message),
            },
            Err(error) => HostResult::Failed {
                generation,
                error: HostError::new("session_type_catalog_failed", error.to_string()),
            },
        },
        HostCommand::Mutation(command) => {
            if command.uses_entrypoints() {
                let mut entrypoints = entrypoints
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                HostResult::Mutation(crate::host_mutations::execute(
                    command,
                    Some(&mut entrypoints),
                ))
            } else {
                HostResult::Mutation(crate::host_mutations::execute(command, None))
            }
        }
        HostCommand::ReclaimSessionTypeCatalog(_) => {
            unreachable!("catalog reclamation completes without a result")
        }
        HostCommand::CreateManagedWorktree { request } => {
            match create_managed_worktree_effect(&request) {
                Ok(prepared) => HostResult::ManagedWorktreeCreated(prepared),
                Err(mut error) => match error.take_recovery() {
                    Some(prepared) => HostResult::ManagedWorktreeRecoveryRequired {
                        prepared,
                        error: HostError::new(error.kind, error.message),
                    },
                    None => HostResult::ManagedWorktreeFailed(error),
                },
            }
        }
        HostCommand::FinalizeManagedWorktree {
            prepared,
            decision,
            deadline,
            discard,
            suppress_rollback,
            #[cfg(test)]
            rollback_hold,
        } => {
            let suppressed = matches!(decision, ManagedWorktreeDecision::Rollback)
                && suppress_rollback
                    .lock()
                    .ok()
                    .is_some_and(|held| held.contains(&prepared.worktree_id));
            let result = if suppressed {
                HostResult::ManagedWorktreeFinalized
            } else {
                #[cfg(test)]
                if matches!(decision, ManagedWorktreeDecision::Rollback) {
                    if let Some(gate) = rollback_hold.as_ref() {
                        gate.wait();
                    }
                }
                match finalize_managed_worktree(
                    &prepared,
                    decision,
                    deadline,
                    Some(&suppress_rollback),
                ) {
                    Ok(()) => HostResult::ManagedWorktreeFinalized,
                    Err(error) => HostResult::ManagedWorktreeRecoveryRequired {
                        prepared,
                        error: HostError::new(error.kind, error.message),
                    },
                }
            };
            drop(discard);
            result
        }
        #[cfg(test)]
        HostCommand::Panic { .. } => panic!("host executor panic test"),
        #[cfg(test)]
        HostCommand::Wait { generation, gate } => {
            gate.wait();
            HostResult::SessionTypeCatalogReady {
                generation,
                entities: BTreeMap::new(),
                logical_bytes: 0,
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TestHostGate {
    started: AtomicBool,
    released: Mutex<bool>,
    ready: std::sync::Condvar,
}

#[cfg(test)]
impl TestHostGate {
    pub(crate) fn wait(&self) {
        self.started.store(true, Ordering::Release);
        let mut released = self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !*released {
            released = self
                .ready
                .wait(released)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    pub(crate) fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.ready.notify_all();
    }

    pub(crate) fn has_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestDisposalProbe {
    pub(crate) dropped: mpsc::Sender<String>,
    pub(crate) executed: Arc<AtomicBool>,
}

#[cfg(test)]
impl Drop for TestDisposalProbe {
    fn drop(&mut self) {
        let _ = self
            .dropped
            .send(thread::current().name().unwrap_or("unnamed").to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::managed_git_worktrees::{
        MANAGED_GIT_OPERATION_TIMEOUT, adopt_unrecorded_managed_worktrees, managed_worktree_path,
    };
    use crate::owner_identity::WaiterId;
    use crate::packages::PackageRegistry;
    use crate::persistence::HubState;
    use crate::spawn_targets::SpawnTarget;
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::atomic::AtomicU64;

    static NEXT_MANAGED_REPOSITORY: AtomicU64 = AtomicU64::new(1);

    fn wait_for_worker_exit(workers: Vec<thread::JoinHandle<()>>) {
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while workers.iter().any(|worker| !worker.is_finished()) {
            assert!(
                Instant::now() < deadline,
                "last disposal sender must let workers exit"
            );
            thread::yield_now();
        }
        for worker in workers {
            worker.join().expect("disposal worker exits normally");
        }
    }

    #[test]
    fn terminal_disposal_refusal_keeps_the_original_payload_and_slot_for_retry() {
        let executor = HostExecutor::new();
        let mut permit = executor.try_reserve().unwrap();
        let original_sender = permit.disposal.clone();
        let (blocked_sender, _blocked_receiver) = mpsc::sync_channel(0);
        permit.disposal = blocked_sender;
        let identity = HostJobIdentity::first(WaiterId(89));
        let (dropped, receiver) = mpsc::channel();
        let mut job = crate::host_disposal::Job::new(crate::host_disposal::Parts {
            storage: None,
            identity,
            permit,
            payload: Box::new(TestDisposalProbe {
                dropped,
                executed: Arc::new(AtomicBool::new(false)),
            }),
            model: None,
        });
        assert_eq!(job.refused(), Some(HostSubmitError::Full));
        assert!(matches!(job.poll(), crate::host_disposal::Poll::Pending));
        assert_eq!(job.refused(), Some(HostSubmitError::Full));
        assert_eq!(executor.outstanding(), 1);
        assert_eq!(executor.prepared_bytes(), HOST_PREPARED_BYTE_CAPACITY);
        assert!(receiver.try_recv().is_err());
        let failure = job.test_failure_mut().unwrap();
        assert_eq!(failure.identity, identity);
        failure.permit.disposal = original_sender;
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                drop(permit);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the same disposal record retries its original permit"
            );
            thread::yield_now();
        }
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
                .starts_with("botster-hub-host")
        );
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(executor.prepared_bytes(), 0);
    }

    #[test]
    fn guarded_close_disposes_buffered_completion_on_host() {
        let mut executor = HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let (dropped, receiver) = mpsc::channel();
        let executed = Arc::new(AtomicBool::new(false));
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::DisposalProbe(TestDisposalProbe {
                    dropped,
                    executed: executed.clone(),
                }),
                permit,
            )
            .unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !executor.take_completion_notification() {
            assert!(
                Instant::now() < deadline,
                "completion must reach its mailbox"
            );
            thread::yield_now();
        }
        assert!(executed.load(Ordering::Acquire));
        assert!(receiver.try_recv().is_err());
        executor.close_and_dispose_completions().unwrap();
        assert!(matches!(
            executor.poll_completion(),
            HostCompletionPoll::Stopped
        ));
        let workers = std::mem::take(&mut executor.workers);
        drop(executor);
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host-")
        );
        wait_for_worker_exit(workers);
    }

    #[test]
    fn guarded_close_rejects_publication_from_a_running_job() {
        let mut executor = HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let pool = permit.pool.clone();
        let prepared_pool = executor.prepared.clone();
        let gate = Arc::new(TestHostGate::default());
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::Wait {
                    generation: 1,
                    gate: gate.clone(),
                },
                permit,
            )
            .unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !gate.has_started() {
            assert!(
                Instant::now() < deadline,
                "job must start before publication closes"
            );
            thread::yield_now();
        }
        executor.close_and_dispose_completions().unwrap();
        assert!(matches!(
            executor.poll_completion(),
            HostCompletionPoll::Stopped
        ));
        gate.release();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while pool.outstanding.load(Ordering::Acquire) != 0
            || prepared_pool.used.load(Ordering::Acquire) != 0
        {
            assert!(Instant::now() < deadline, "Host must dispose the result");
            assert!(matches!(
                executor.poll_completion(),
                HostCompletionPoll::Stopped
            ));
            thread::yield_now();
        }
        assert_eq!(pool.outstanding.load(Ordering::Acquire), 0);
        assert_eq!(prepared_pool.used.load(Ordering::Acquire), 0);
        let workers = std::mem::take(&mut executor.workers);
        drop(executor);
        wait_for_worker_exit(workers);
    }

    #[test]
    fn foreign_permit_refusal_preserves_its_original_disposal_owner() {
        let executor = HostExecutor::new();
        let foreign = HostExecutor::new();
        let permit = foreign.try_reserve().unwrap();
        let (dropped, receiver) = mpsc::channel();
        let executed = Arc::new(AtomicBool::new(false));
        let failure = executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::DisposalProbe(TestDisposalProbe {
                    dropped,
                    executed: executed.clone(),
                }),
                permit,
            )
            .expect_err("foreign permit must be refused");
        assert_eq!(failure.error, HostSubmitError::WrongExecutor);
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(foreign.outstanding(), 1);
        assert!(receiver.try_recv().is_err());
        failure
            .permit
            .dispose(failure.identity, failure.command)
            .unwrap();
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host-")
        );
        assert!(!executed.load(Ordering::Acquire));
    }

    #[test]
    fn guarded_close_refusal_retains_failed_and_remaining_completions_for_retry() {
        let executor = HostExecutor::new();
        let mut first = executor.try_reserve().unwrap();
        let second = executor.try_reserve().unwrap();
        let original_sender = first.disposal.clone();
        let (full_sender, _full_receiver) = mpsc::sync_channel(0);
        first.disposal = full_sender;
        let (sender, receiver) = mpsc::sync_channel(HOST_OPERATION_CAPACITY);
        let (dropped, dropped_rx) = mpsc::channel();
        for (index, permit) in [first, second].into_iter().enumerate() {
            let result = HostResult::StatusResponsePrepared(
                crate::status_response::PreparedStatusResponse {
                    kind: botster_hub_client::DaemonResponseKind::Status,
                    encoded_frame: None,
                    shutdown: false,
                    dispose_probe: Some(TestDisposalProbe {
                        dropped: dropped.clone(),
                        executed: Arc::new(AtomicBool::new(false)),
                    }),
                },
            );
            sender
                .send(HostCompletion::from_parts(
                    HostJobIdentity {
                        waiter_id: WaiterId(index as u64 + 1),
                        phase: 1,
                    },
                    result,
                    permit,
                ))
                .unwrap();
        }
        *executor.completions.lock().unwrap() = Some(receiver);
        let mut failure = executor
            .close_and_dispose_completions()
            .expect_err("injected full disposal queue");
        assert_eq!(failure.failed.error, HostSubmitError::Full);
        assert_eq!(executor.outstanding(), 2);
        assert_eq!(executor.prepared_bytes(), 2 * HOST_PREPARED_BYTE_CAPACITY);
        assert!(dropped_rx.try_recv().is_err());
        assert!(matches!(
            failure.failed.command,
            HostCommand::DiscardCompletion(_)
        ));
        executor.close_and_dispose_completions().unwrap();
        assert_eq!(executor.outstanding(), 2);
        assert_eq!(executor.prepared_bytes(), 2 * HOST_PREPARED_BYTE_CAPACITY);
        assert!(dropped_rx.try_recv().is_err());
        failure.failed.permit.disposal = original_sender;
        failure
            .retry()
            .expect("retry drains both retained completions");
        for _ in 0..2 {
            assert!(
                dropped_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
                    .starts_with("botster-hub-host-")
            );
        }
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while executor.outstanding() != 0 || executor.prepared_bytes() != 0 {
            assert!(
                Instant::now() < deadline,
                "retry releases both original reservations"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn shutdown_snapshot_retains_one_slot_through_stop_and_worker_delivery() {
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("reserve shutdown");
        let (dropped, receiver) = mpsc::channel();
        let mut input = crate::status_response::test_input(true);
        input.drop_probe = Some(TestDisposalProbe {
            dropped,
            executed: Arc::new(AtomicBool::new(false)),
        });
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::PrepareStatusResponse(input),
                permit,
            )
            .expect("submit snapshot");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("typed seed drops")
                .starts_with("botster-hub-host-")
        );
        let HostResult::StatusResponsePrepared(prepared) = result else {
            panic!("encoded snapshot");
        };
        let expected = prepared.encoded_frame.clone().expect("encoded response");
        assert_eq!(executor.permits.outstanding.load(Ordering::Acquire), 1);
        assert_eq!(
            executor.prepared.used.load(Ordering::Acquire),
            HOST_PREPARED_BYTE_CAPACITY
        );
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 2,
                },
                HostCommand::StopForStatus(prepared),
                permit,
            )
            .expect("submit stop");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        let HostResult::StatusResponsePrepared(prepared) = result else {
            panic!("stop preserves encoded snapshot");
        };
        assert_eq!(prepared.encoded_frame.as_ref(), Some(&expected));
        assert_eq!(executor.permits.outstanding.load(Ordering::Acquire), 1);
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 3,
                },
                HostCommand::DeliverStatusResponse { prepared, reply_tx },
                permit,
            )
            .expect("submit delivery");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        assert!(matches!(
            result,
            HostResult::StatusResponseDelivered {
                shutdown: true,
                received: true
            }
        ));
        drop(permit);
        assert_eq!(executor.permits.outstanding.load(Ordering::Acquire), 0);
        assert_eq!(
            executor.prepared.used.load(Ordering::Acquire),
            expected.len()
        );
        let reply = reply_rx.blocking_recv().expect("prepared reply");
        assert_eq!(
            reply.kind(),
            Some(botster_hub_client::DaemonResponseKind::Shutdown)
        );
        drop(reply);
        assert_eq!(executor.prepared.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn failed_shutdown_reply_releases_encoded_charge_on_worker() {
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("reserve response");
        let prepared = crate::status_response::prepare(
            crate::status_response::test_input(true),
            HOST_PREPARED_BYTE_CAPACITY,
        );
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        drop(reply_rx);
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 3,
                },
                HostCommand::DeliverStatusResponse { prepared, reply_tx },
                permit,
            )
            .expect("submit failed delivery");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        assert!(matches!(
            result,
            HostResult::StatusResponseDelivered {
                shutdown: true,
                received: false
            }
        ));
        assert_eq!(executor.prepared.used.load(Ordering::Acquire), 0);
        drop(permit);
    }

    #[test]
    fn unencodable_shutdown_fallback_still_stops_and_completes_without_a_frame() {
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("reserve shutdown slot");
        let prepared = crate::status_response::prepare(crate::status_response::test_input(true), 1);
        assert!(prepared.encoded_frame.is_none());
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 2,
                },
                HostCommand::StopForStatus(prepared),
                permit,
            )
            .expect("submit no-frame stop");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        let HostResult::StatusResponsePrepared(prepared) = result else {
            panic!("stop retains no-frame shutdown");
        };
        assert_eq!(executor.status_stop_count(), 1);
        assert_eq!(executor.outstanding(), 1);
        assert_eq!(executor.prepared_bytes(), HOST_PREPARED_BYTE_CAPACITY);
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 3,
                },
                HostCommand::DeliverStatusResponse { prepared, reply_tx },
                permit,
            )
            .expect("submit no-frame completion");
        let (_, result, permit) = receive_host_completion(&executor).into_parts();
        assert!(matches!(
            result,
            HostResult::StatusResponseDelivered {
                shutdown: true,
                received: false
            }
        ));
        assert!(reply_rx.blocking_recv().is_err());
        assert_eq!(executor.prepared_bytes(), 0);
        drop(permit);
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(executor.status_stop_count(), 1);
    }

    #[test]
    fn disposal_after_executor_drop_uses_original_workers_and_releases_last_sender() {
        let mut executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("reserve external work");
        let workers = std::mem::take(&mut executor.workers);
        let (dropped, receiver) = mpsc::channel();
        let executed = Arc::new(AtomicBool::new(false));
        drop(executor);
        permit
            .dispose(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::DisposalProbe(TestDisposalProbe {
                    dropped,
                    executed: executed.clone(),
                }),
            )
            .expect("external permit preserves a disposal consumer");
        let thread = receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("worker drops payload");
        assert!(thread.starts_with("botster-hub-host-"));
        assert!(!executed.load(Ordering::Acquire));
        wait_for_worker_exit(workers);
    }

    #[test]
    fn completion_loss_stops_execution_but_keeps_disposal_consumers_alive() {
        let mut executor = HostExecutor::new();
        let external = executor.try_reserve().expect("reserve external work");
        let trigger = executor.try_reserve().expect("reserve completion trigger");
        let receiver = executor.completions.lock().unwrap().take();
        drop(receiver);
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::StopEntrypoints,
                trigger,
            )
            .expect("submit completion-loss trigger");
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !executor.stopping.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "completion loss must stop ordinary execution"
            );
            thread::yield_now();
        }
        assert!(executor.try_reserve().is_none());
        let workers = std::mem::take(&mut executor.workers);
        drop(executor);
        let (dropped, receiver) = mpsc::channel();
        let executed = Arc::new(AtomicBool::new(false));
        external
            .dispose(
                HostJobIdentity {
                    waiter_id: WaiterId(2),
                    phase: 1,
                },
                HostCommand::DisposalProbe(TestDisposalProbe {
                    dropped,
                    executed: executed.clone(),
                }),
            )
            .expect("completion loss preserves disposal consumers");
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("worker drops payload")
                .starts_with("botster-hub-host-")
        );
        assert!(!executed.load(Ordering::Acquire));
        wait_for_worker_exit(workers);
    }

    fn empty_catalog_inputs() -> (SharedView<PackageRegistry>, SharedView<HubState>) {
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(PathBuf::from("host-executor-test")),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build host executor test config");
        let budget = crate::shared_view::SharedViewBudget::new();
        (
            SharedView::try_new(
                &budget,
                PackageRegistry::new(botster_core::CapabilitySet::new()),
                1,
            )
            .expect("registry view fits"),
            SharedView::try_new(&budget, HubState::from_config(&config), 1)
                .expect("state view fits"),
        )
    }

    struct ManagedGitFixture {
        root: PathBuf,
        repository: PathBuf,
        managed_root: PathBuf,
    }

    impl ManagedGitFixture {
        fn new() -> Self {
            let id = NEXT_MANAGED_REPOSITORY.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "botster-host-managed-git-{}-{id}",
                std::process::id()
            ));
            let repository = root.join("repository");
            let managed_root = root.join("managed");
            fs::create_dir_all(&repository).expect("create managed Git repository");
            run_test_git(
                None,
                &[
                    "init",
                    "-b",
                    "main",
                    repository.to_str().expect("repository path"),
                ],
            );
            run_test_git(
                Some(&repository),
                &["config", "user.email", "botster@example.invalid"],
            );
            run_test_git(Some(&repository), &["config", "user.name", "Botster Test"]);
            fs::write(repository.join("README.md"), "fixture\n")
                .expect("write managed Git fixture");
            run_test_git(Some(&repository), &["add", "README.md"]);
            run_test_git(Some(&repository), &["commit", "-m", "fixture"]);
            Self {
                root,
                repository,
                managed_root,
            }
        }

        fn target(&self) -> SpawnTarget {
            SpawnTarget {
                target_id: "tgt_host_managed".to_string(),
                label: "Managed".to_string(),
                root: self.repository.clone(),
                enabled: true,
                kind: "git".to_string(),
                base_ref: Some("main".to_string()),
                metadata: BTreeMap::new(),
            }
        }

        fn request(&self, branch: &str) -> ManagedGitRequest {
            ManagedGitRequest {
                target: self.target(),
                branch: branch.to_string(),
                managed_root: self.managed_root.clone(),
                persisted_worktree: None,
                accepted_at: Instant::now(),
            }
        }
    }

    impl Drop for ManagedGitFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn run_test_git(root: Option<&std::path::Path>, args: &[&str]) {
        let mut command = Command::new("git");
        if let Some(root) = root {
            command.arg("-C").arg(root);
        }
        assert!(
            command
                .args(args)
                .status()
                .expect("run managed Git fixture command")
                .success(),
            "managed Git fixture command must succeed"
        );
    }

    fn receive_host_completion(executor: &HostExecutor) -> HostCompletion {
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match executor.poll_completion() {
                HostCompletionPoll::Ready(completion) => return completion,
                HostCompletionPoll::Stopped => panic!("host completion mailbox stopped"),
                HostCompletionPoll::Empty => {
                    assert!(Instant::now() < deadline, "host completion must arrive");
                    std::thread::yield_now();
                }
            }
        }
    }

    fn plugin_response_input() -> (
        crate::plugin_response::PluginResponseInput,
        crate::daemon::control::reply::RetainedPluginResultBudget,
    ) {
        use crate::daemon::control::reply::{RetainedPluginResult, RetainedPluginResultBudget};
        let budget = RetainedPluginResultBudget::new();
        let charge = budget
            .try_reserve(128 * 1024)
            .expect("raw result reservation");
        let input = crate::plugin_response::PluginResponseInput {
            kind: crate::plugin_response::PluginResponseKind::McpTool,
            lifecycle: crate::lifecycle::HubPluginLifecycle::with_config(
                botster_core::PluginWorkerEngineConfig::default(),
            ),
            result: RetainedPluginResult::new(Err("x".repeat(128 * 1024)), charge),
            transport_request_id: "42".to_string(),
            inconsistent: false,
        };
        (input, budget)
    }

    #[test]
    fn plugin_response_delivery_keeps_slot_until_owner_consumes_terminal_outcome() {
        let executor = HostExecutor::new();
        let (input, raw_budget) = plugin_response_input();
        let (reply_tx, mut reply_rx) = crate::daemon::control::message::control_reply_channel();
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(81),
                    phase: 1,
                },
                HostCommand::PreparePluginResponse {
                    input,
                    reply_tx,
                    reply_live: Arc::new(AtomicBool::new(true)),
                },
                executor.try_reserve().expect("host response slot"),
            )
            .expect("submit response");
        let completion = receive_host_completion(&executor);
        assert_eq!(raw_budget.retained_bytes(), 0);
        assert_eq!(executor.outstanding(), 1);
        assert!(executor.prepared.used.load(Ordering::Acquire) > 0);
        let reply = reply_rx.try_recv().expect("worker delivered response");
        let (response, charge, encoded) = reply.into_parts();
        let decoded: botster_hub_client::ServerFrame =
            serde_json::from_slice(encoded.as_ref().expect("encoded frame"))
                .expect("complete frame");
        assert!(
            matches!(decoded, botster_hub_client::ServerFrame::Response { request_id, .. } if request_id == "42")
        );
        let (_, result, permit) = completion.into_parts();
        assert!(matches!(result, HostResult::PluginResponseDelivered { .. }));
        drop(permit);
        assert_eq!(executor.outstanding(), 0);
        assert!(executor.prepared.used.load(Ordering::Acquire) > 0);
        drop(response);
        drop(encoded);
        drop(charge);
        assert_eq!(executor.prepared.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cancelled_or_disconnected_plugin_response_reclaims_raw_result_on_worker() {
        for disconnected in [false, true] {
            let executor = HostExecutor::new();
            let (input, raw_budget) = plugin_response_input();
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            let mut retained_receiver = Some(reply_rx);
            if disconnected {
                retained_receiver.take();
            }
            executor
                .submit(
                    HostJobIdentity {
                        waiter_id: WaiterId(82),
                        phase: 1,
                    },
                    HostCommand::PreparePluginResponse {
                        input,
                        reply_tx,
                        reply_live: Arc::new(AtomicBool::new(disconnected)),
                    },
                    executor.try_reserve().expect("host response slot"),
                )
                .expect("submit cancelled response");
            let completion = receive_host_completion(&executor);
            assert_eq!(raw_budget.retained_bytes(), 0);
            assert_eq!(executor.outstanding(), 1);
            assert!(matches!(
                completion.result,
                HostResult::PluginResponseAbandoned
            ));
            drop(completion);
            assert_eq!(executor.outstanding(), 0);
            assert_eq!(executor.prepared.used.load(Ordering::Acquire), 0);
            if let Some(mut receiver) = retained_receiver {
                assert!(matches!(
                    receiver.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed)
                ));
            }
        }
    }

    #[test]
    fn executor_refuses_a_ninth_outstanding_operation() {
        let executor = HostExecutor::new();
        assert_eq!(executor.workers.len(), HOST_WORKER_COUNT);
        let permits = (0..HOST_OPERATION_CAPACITY)
            .map(|_| executor.try_reserve().expect("reserve host operation"))
            .collect::<Vec<_>>();
        assert_eq!(executor.outstanding(), HOST_OPERATION_CAPACITY);
        assert!(executor.try_reserve().is_none());
        drop(permits);
        assert_eq!(executor.outstanding(), 0);
    }

    #[test]
    fn rejected_submission_retains_command_and_both_reservations() {
        for expected in [HostSubmitError::Stopped, HostSubmitError::Full] {
            let mut executor = HostExecutor::new();
            let permit = executor.try_reserve().expect("reserve rejected operation");
            executor.jobs.take();
            // A zero-capacity queue injects the reserved-queue invariant failure.
            let (sender, receiver) = mpsc::sync_channel(0);
            if expected == HostSubmitError::Full {
                executor.jobs = Some(sender);
            }
            let gate = Arc::new(TestHostGate::default());
            let identity = HostJobIdentity {
                waiter_id: WaiterId(91),
                phase: 4,
            };
            let failure = executor
                .submit(
                    identity,
                    HostCommand::Wait {
                        generation: 73,
                        gate: Arc::clone(&gate),
                    },
                    permit,
                )
                .expect_err("submission must reject the injected queue state");
            assert_eq!(failure.error, expected);
            assert_eq!(failure.identity, identity);
            assert!(
                matches!(&failure.command, HostCommand::Wait { generation: 73, gate: retained } if Arc::ptr_eq(retained, &gate))
            );
            assert_eq!(executor.outstanding(), 1);
            assert_eq!(
                executor.prepared.used.load(Ordering::Acquire),
                HOST_PREPARED_BYTE_CAPACITY
            );
            drop(failure);
            assert_eq!(executor.outstanding(), 0);
            assert_eq!(executor.prepared.used.load(Ordering::Acquire), 0);
            drop(receiver);
        }
    }

    #[test]
    fn retained_result_bytes_reduce_new_operation_capacity() {
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("first operation fits");
        let completion = HostCompletion::for_test(
            HostJobIdentity {
                waiter_id: WaiterId(1),
                phase: 1,
            },
            HostResult::SessionTypeCatalogReady {
                generation: 1,
                entities: BTreeMap::new(),
                logical_bytes: 1,
            },
            permit,
        );
        let (_result, retained) = completion.release();
        let permits = (0..7)
            .map(|_| executor.try_reserve().expect("seven operations fit"))
            .collect::<Vec<_>>();
        assert!(executor.try_reserve().is_none());
        drop(retained);
        let final_permit = executor
            .try_reserve()
            .expect("released retained bytes restore capacity");
        drop(final_permit);
        drop(permits);
    }

    #[test]
    fn completion_is_published_before_the_owner_wake_and_idle_clears() {
        let executor = HostExecutor::new();
        let (owner, mut owner_rx) = tokio::sync::mpsc::channel(4);
        executor.bind_owner_wake(owner);
        let permit = executor.try_reserve().expect("reserve catalog build");
        let identity = HostJobIdentity {
            waiter_id: WaiterId(1),
            phase: 1,
        };
        let (packages, state) = empty_catalog_inputs();
        executor
            .submit(
                identity,
                HostCommand::BuildSessionTypeCatalog {
                    generation: 7,
                    packages,
                    state,
                },
                permit,
            )
            .expect("submit catalog build");
        assert!(matches!(
            owner_rx.blocking_recv(),
            Some(ControlMessage::HostProgressPublished)
        ));
        assert!(executor.take_completion_notification());
        assert_eq!(executor.outstanding(), 1);
        let HostCompletionPoll::Ready(completion) = executor.poll_completion() else {
            panic!("published completion must be ready");
        };
        assert_eq!(completion.identity, identity);
        assert!(matches!(
            completion.result,
            HostResult::SessionTypeCatalogReady { generation: 7, .. }
        ));
        let _result = completion.release();
        assert_eq!(executor.outstanding(), 0);
        assert!(executor.take_capacity_notification());
        assert!(!executor.take_completion_notification());
        assert!(!executor.take_capacity_notification());
        while owner_rx.try_recv().is_ok() {}
        assert!(matches!(
            owner_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(!executor.take_completion_notification());
        assert!(!executor.take_capacity_notification());
    }

    #[test]
    fn full_owner_queue_cannot_lose_published_completion_state() {
        let executor = HostExecutor::new();
        let (owner, mut owner_rx) = tokio::sync::mpsc::channel(1);
        owner
            .try_send(ControlMessage::HostProgressPublished)
            .expect("fill owner queue");
        executor.bind_owner_wake(owner);
        let permit = executor.try_reserve().expect("reserve catalog build");
        let identity = HostJobIdentity {
            waiter_id: WaiterId(1),
            phase: 1,
        };
        let (packages, state) = empty_catalog_inputs();
        executor
            .submit(
                identity,
                HostCommand::BuildSessionTypeCatalog {
                    generation: 11,
                    packages,
                    state,
                },
                permit,
            )
            .expect("submit catalog build");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !executor.wake.completion_pending.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "catalog completion must publish"
            );
            std::thread::yield_now();
        }

        assert!(matches!(
            owner_rx.try_recv(),
            Ok(ControlMessage::HostProgressPublished)
        ));
        assert!(matches!(
            owner_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(executor.take_completion_notification());
        let HostCompletionPoll::Ready(completion) = executor.poll_completion() else {
            panic!("published completion must be ready");
        };
        assert_eq!(completion.identity, identity);
        let _result = completion.release();
        assert!(executor.take_capacity_notification());
    }

    #[test]
    fn worker_panic_returns_a_typed_completion_and_releases_on_apply() {
        let executor = HostExecutor::new();
        let (owner, mut owner_rx) = tokio::sync::mpsc::channel(1);
        executor.bind_owner_wake(owner);
        let permit = executor.try_reserve().expect("reserve panic job");
        let identity = HostJobIdentity {
            waiter_id: WaiterId(1),
            phase: 4,
        };
        executor
            .submit(identity, HostCommand::Panic { generation: 12 }, permit)
            .expect("submit panic job");

        assert!(matches!(
            owner_rx.blocking_recv(),
            Some(ControlMessage::HostProgressPublished)
        ));
        let HostCompletionPoll::Ready(completion) = executor.poll_completion() else {
            panic!("panic completion must be ready");
        };
        assert_eq!(completion.identity, identity);
        assert!(matches!(
            completion.result,
            HostResult::Failed {
                generation: 12,
                ref error,
            } if error.code == "host_worker_panicked"
        ));
        assert_eq!(executor.outstanding(), 1);
        let _result = completion.release();
        assert_eq!(executor.outstanding(), 0);
    }

    #[test]
    fn disconnected_completion_mailbox_is_not_reported_as_empty() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);
        assert!(matches!(
            poll_completion_mailbox(&Mutex::new(Some(receiver))),
            HostCompletionPoll::Stopped
        ));
    }

    #[test]
    fn shutdown_detaches_in_flight_work_and_releases_its_resources_later() {
        let executor = HostExecutor::new();
        let permits = Arc::clone(&executor.permits);
        let gate = Arc::new(TestHostGate::default());
        let permit = executor.try_reserve().expect("reserve waiting job");
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::Wait {
                    generation: 1,
                    gate: Arc::clone(&gate),
                },
                permit,
            )
            .expect("submit waiting job");
        let started_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !gate.has_started() {
            assert!(
                std::time::Instant::now() < started_deadline,
                "waiting job must start"
            );
            std::thread::yield_now();
        }

        let (dropped_tx, dropped_rx) = mpsc::sync_channel(1);
        let dropper = std::thread::spawn(move || {
            drop(executor);
            let _ = dropped_tx.send(());
        });
        let drop_result = dropped_rx.recv_timeout(std::time::Duration::from_millis(250));
        assert_eq!(permits.outstanding.load(Ordering::Acquire), 1);
        gate.release();
        dropper.join().expect("join executor drop test");
        assert!(
            drop_result.is_ok(),
            "executor shutdown must not wait for in-flight host work"
        );
        let release_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while permits.outstanding.load(Ordering::Acquire) != 0 {
            assert!(
                std::time::Instant::now() < release_deadline,
                "detached job resources must release"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn managed_create_and_failed_finalize_keep_one_permit_and_recovery_identity() {
        let fixture = ManagedGitFixture::new();
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().expect("reserve managed operation");
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                HostCommand::CreateManagedWorktree {
                    request: fixture.request("feature/recovery"),
                },
                permit,
            )
            .expect("submit managed create phase");
        let (created_identity, created_result, permit) =
            receive_host_completion(&executor).into_parts();
        assert_eq!(created_identity.phase, 1);
        let HostResult::ManagedWorktreeCreated(prepared) = created_result else {
            panic!("managed create phase must return its rollback descriptor");
        };
        fs::write(prepared.path.join("user-change"), "preserve\n")
            .expect("change created worktree");

        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 2,
                },
                HostCommand::FinalizeManagedWorktree {
                    prepared,
                    decision: ManagedWorktreeDecision::Rollback,
                    deadline: Instant::now() + MANAGED_GIT_OPERATION_TIMEOUT,
                    discard: None,
                    suppress_rollback: Arc::new(Mutex::new(BTreeSet::new())),
                    rollback_hold: None,
                },
                permit,
            )
            .expect("submit managed rollback phase");
        let (failed_identity, failed_result, permit) =
            receive_host_completion(&executor).into_parts();
        assert_eq!(failed_identity.phase, 2);
        let HostResult::ManagedWorktreeRecoveryRequired { prepared, error } = failed_result else {
            panic!("failed rollback must retain recovery identity");
        };
        assert_eq!(error.code, "rollback_identity_mismatch");
        assert!(prepared.path.exists());
        assert_eq!(executor.outstanding(), 1, "one permit spans both phases");

        fs::remove_file(prepared.path.join("user-change")).expect("remove test user change");
        executor
            .submit(
                HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 3,
                },
                HostCommand::FinalizeManagedWorktree {
                    prepared,
                    decision: ManagedWorktreeDecision::Rollback,
                    deadline: Instant::now() + MANAGED_GIT_OPERATION_TIMEOUT,
                    discard: None,
                    suppress_rollback: Arc::new(Mutex::new(BTreeSet::new())),
                    rollback_hold: None,
                },
                permit,
            )
            .expect("submit managed recovery phase");
        let (recovered_identity, recovered_result, permit) =
            receive_host_completion(&executor).into_parts();
        assert_eq!(recovered_identity.phase, 3);
        assert!(matches!(
            recovered_result,
            HostResult::ManagedWorktreeFinalized
        ));
        drop(permit);
        assert_eq!(executor.outstanding(), 0);
    }

    #[test]
    fn disconnected_create_delivery_preserves_an_adoptable_external_effect() {
        let fixture = ManagedGitFixture::new();
        let permit_executor = HostExecutor::new();
        let permit = permit_executor
            .try_reserve()
            .expect("reserve disconnected managed operation");
        let (jobs_tx, jobs_rx) = mpsc::sync_channel(1);
        let (completions_tx, completions_rx) = mpsc::sync_channel(1);
        drop(completions_rx);
        let wake = Arc::new(HostWake::new());
        let stopping = Arc::new(AtomicBool::new(false));
        let worker = std::thread::spawn({
            let wake = Arc::clone(&wake);
            let stopping = Arc::clone(&stopping);
            move || {
                run_worker(
                    Arc::new(Mutex::new(jobs_rx)),
                    completions_tx,
                    Weak::new(),
                    wake,
                    stopping,
                    Arc::new(Mutex::new(EntrypointSupervisor::default())),
                )
            }
        });
        let branch = "feature/disconnected";
        let path =
            managed_worktree_path(&fixture.managed_root, &fixture.target().target_id, branch);
        jobs_tx
            .send(HostJob {
                identity: HostJobIdentity {
                    waiter_id: WaiterId(1),
                    phase: 1,
                },
                command: HostCommand::CreateManagedWorktree {
                    request: fixture.request(branch),
                },
                permit,
            })
            .expect("submit disconnected managed create");
        drop(jobs_tx);
        worker.join().expect("join disconnected host worker");
        assert!(path.exists(), "undelivered create must preserve one policy");

        let mut rows = Vec::new();
        assert!(adopt_unrecorded_managed_worktrees(
            &[fixture.target()],
            &mut rows,
            &fixture.managed_root,
        ));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].path,
            path.canonicalize().expect("canonical managed path")
        );
    }
}
