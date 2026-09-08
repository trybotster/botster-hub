//! Fixed, bounded execution for Hub work that must not run on the owner thread.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use serde_json::Value;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::session_types::{
    SessionTypeCatalogBuild, bounded_session_type_catalog_entities,
};
use crate::packages::PackageRegistry;
use crate::persistence::HubState;
use crate::shared_view::SharedView;

pub(crate) const HOST_WORKER_COUNT: usize = 2;
pub(crate) const HOST_OPERATION_CAPACITY: usize = 8;
pub(crate) const HOST_PREPARED_BYTE_CAPACITY: usize = 8 * 1024 * 1024;
pub(crate) const HOST_PREPARED_AGGREGATE_BYTE_CAPACITY: usize =
    HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WaiterId(u64);

#[derive(Debug, Default)]
pub(crate) struct WaiterIdSequence {
    next: u64,
}

impl WaiterIdSequence {
    pub(crate) fn next(&mut self) -> Option<WaiterId> {
        self.next = self.next.checked_add(1)?;
        Some(WaiterId(self.next))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostJobIdentity {
    pub(crate) waiter_id: WaiterId,
    pub(crate) phase: u64,
}

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
    BuildSessionTypeCatalog {
        generation: u64,
        packages: SharedView<PackageRegistry>,
        state: SharedView<HubState>,
    },
    #[cfg(test)]
    Panic { generation: u64 },
    #[cfg(test)]
    Wait {
        generation: u64,
        gate: Arc<TestHostGate>,
    },
}

impl HostCommand {
    fn generation(&self) -> u64 {
        match self {
            Self::BuildSessionTypeCatalog { generation, .. } => *generation,
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
            Self::BuildSessionTypeCatalog { generation, .. } => formatter
                .debug_struct("BuildSessionTypeCatalog")
                .field("generation", generation)
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

#[derive(Debug)]
pub(crate) struct HostJob {
    pub(crate) identity: HostJobIdentity,
    pub(crate) command: HostCommand,
    permit: HostWorkPermit,
}

#[derive(Debug)]
pub(crate) enum HostResult {
    SessionTypeCatalogReady {
        generation: u64,
        entities: BTreeMap<String, Value>,
        logical_bytes: usize,
    },
    Failed {
        generation: u64,
        error: HostError,
    },
}

impl HostResult {
    fn generation(&self) -> u64 {
        match self {
            Self::SessionTypeCatalogReady { generation, .. } | Self::Failed { generation, .. } => {
                *generation
            }
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
    pub(crate) fn release(self) -> (HostResult, HostPreparedCharge) {
        let Self {
            mut result, permit, ..
        } = self;
        let logical_bytes = match &result {
            HostResult::SessionTypeCatalogReady { logical_bytes, .. } => *logical_bytes,
            HostResult::Failed { .. } => 0,
        };
        if logical_bytes > HOST_PREPARED_BYTE_CAPACITY {
            let generation = result.generation();
            result = HostResult::Failed {
                generation,
                error: HostError::new(
                    "host_result_too_large",
                    "host result exceeds its prepared-byte reservation",
                ),
            };
        }
        let logical_bytes = match &result {
            HostResult::SessionTypeCatalogReady { logical_bytes, .. } => *logical_bytes,
            HostResult::Failed { .. } => 0,
        };
        let charge = permit.into_prepared_charge(logical_bytes);
        (result, charge)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostSubmitError {
    Full,
    Stopped,
}

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
}

impl HostWake {
    fn new() -> Self {
        Self {
            completion_pending: AtomicBool::new(false),
            capacity_pending: AtomicBool::new(false),
            owner: Mutex::new(None),
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
    prepared: Option<HostPreparedReservation>,
}

impl HostWorkPermit {
    fn into_prepared_charge(mut self, logical_bytes: usize) -> HostPreparedCharge {
        let reservation = self
            .prepared
            .take()
            .expect("host work permit owns a prepared-byte reservation");
        reservation.into_charge(logical_bytes)
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
    completions: Arc<Mutex<mpsc::Receiver<HostCompletion>>>,
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
        let completions = Arc::new(Mutex::new(completions_rx));
        let wake = Arc::new(HostWake::new());
        let permits = Arc::new(HostPermitPool {
            outstanding: AtomicUsize::new(0),
            wake: Arc::clone(&wake),
        });
        let prepared = Arc::new(HostPreparedPool {
            used: AtomicUsize::new(0),
            wake: Arc::clone(&wake),
        });
        let stopping = Arc::new(AtomicBool::new(false));
        let workers = (0..HOST_WORKER_COUNT)
            .map(|index| {
                let jobs = Arc::clone(&jobs_rx);
                let completions = completions_tx.clone();
                let wake = Arc::clone(&wake);
                let stopping = Arc::clone(&stopping);
                thread::Builder::new()
                    .name(format!("botster-hub-host-{index}"))
                    .spawn(move || run_worker(jobs, completions, wake, stopping))
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

    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        self.wake.bind(sender);
    }

    pub(crate) fn try_reserve(&self) -> Option<HostWorkPermit> {
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
                        prepared: Some(HostPreparedReservation {
                            pool: Arc::clone(&self.prepared),
                            logical_bytes: HOST_PREPARED_BYTE_CAPACITY,
                        }),
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
    ) -> Result<(), HostSubmitError> {
        let job = HostJob {
            identity,
            command,
            permit,
        };
        self.jobs
            .as_ref()
            .ok_or(HostSubmitError::Stopped)?
            .try_send(job)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => HostSubmitError::Full,
                mpsc::TrySendError::Disconnected(_) => HostSubmitError::Stopped,
            })
    }

    pub(crate) fn poll_completion(&self) -> HostCompletionPoll {
        poll_completion_mailbox(&self.completions)
    }

    pub(crate) fn take_completion_notification(&self) -> bool {
        self.wake.completion_pending.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_capacity_notification(&self) -> bool {
        self.wake.capacity_pending.swap(false, Ordering::AcqRel)
    }

    #[cfg(test)]
    fn outstanding(&self) -> usize {
        self.permits.outstanding.load(Ordering::Acquire)
    }
}

fn poll_completion_mailbox(
    completions: &Mutex<mpsc::Receiver<HostCompletion>>,
) -> HostCompletionPoll {
    match completions
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .try_recv()
    {
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
    wake: Arc<HostWake>,
    stopping: Arc<AtomicBool>,
) {
    loop {
        let job = jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .recv();
        let Ok(job) = job else {
            return;
        };
        if stopping.load(Ordering::Acquire) {
            drop(job);
            continue;
        }
        let HostJob {
            identity,
            command,
            permit,
        } = job;
        let generation = command.generation();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(command)))
            .unwrap_or_else(|_| HostResult::Failed {
                generation,
                error: HostError::new("host_worker_panicked", "host worker execution panicked"),
            });
        if completions
            .send(HostCompletion {
                identity,
                result,
                permit,
            })
            .is_err()
        {
            return;
        }
        // The completion is in the mailbox before this bit and doorbell publish.
        wake.publish_completion();
    }
}

fn execute(command: HostCommand) -> HostResult {
    match command {
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
    fn wait(&self) {
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

    fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.ready.notify_all();
    }

    fn has_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::packages::PackageRegistry;
    use crate::persistence::HubState;
    use std::path::PathBuf;

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
            poll_completion_mailbox(&Mutex::new(receiver)),
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
    fn waiter_ids_stop_before_wrap() {
        let mut sequence = WaiterIdSequence { next: u64::MAX - 1 };
        assert_eq!(sequence.next(), Some(WaiterId(u64::MAX)));
        assert_eq!(sequence.next(), None);
        assert_eq!(sequence.next(), None);
    }
}
