//! The owner of the embedded Core (design sections 2 and 4): the only thread that touches `Core`. It owns the waiter map and
//! the session view, admits requests from the bridge, and turns Core with counted work per turn (no wall-clock budget).
//!
//! The owner is generic over `CoreApi`, so its unit tests run on the contracts' fake Core with no I/O, one turn at a time.
//! [`run`] is the thread's loop over `mio`; it runs only on the real Core (the fake's wake has no descriptor).

use crate::bridge::{
    Answer, Bridge, Effect, FailKind, Failure, Item, ReplyTo, Request, SpawnAnswer,
};
use crate::phase::{request_stop, Ended, Lifecycle, SharedPhase, StopCause};
use crate::store::{
    install_on_ready, CreateRefused, FailReason, InstallBlocked, Lookup, SpawnStore, StoreEvent,
    Terminal,
};
use botster_core::contract::prelude::{
    CoreApi, CoreError, ErrorCode, Event, LostReason, Now, Op, OpId, OpResult, SessionId,
    SessionState, SpawnRequest, StartFailReason,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

/// The per-turn counts (section 2; parameters with small test values, HP LM-5 rule). Each is at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnCounts {
    pub control: usize,
    pub request: usize,
    pub bulk: usize,
    pub events: usize,
}

/// A waiter: who gets an op's `Completed` (section 4).
#[derive(Debug)]
enum Waiter {
    /// A client request.
    Reply(ReplyTo),
    /// The owner's own `AdoptAll` (start step 6).
    Adopt,
    /// The spawn flow (design section 4a, "Spawn flow"): the `Create` of an admitted spawn. The request waits for it.
    SpawnCreate {
        operation_id: String,
        session: SessionId,
        reply: ReplyTo,
    },
    /// The `Start` that follows admission. No client waits.
    SpawnStart {
        operation_id: String,
        session: SessionId,
    },
    /// The Hub's own `Remove` of a U3 row (the cleanup obligation).
    SpawnRemove { session: SessionId },
}

/// A spawn whose `starting` write is in flight: `Create` is begun only when it is durable (the order rule).
#[derive(Debug)]
struct Admit {
    session: SessionId,
    request: Box<SpawnRequest>,
    reply: ReplyTo,
}

/// The state of start step 6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Adoption {
    NotStarted,
    Running,
    Done,
    /// `begin(AdoptAll)` was refused or its `Completed` carried an error.
    Failed(String),
}

/// What one turn found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnReport {
    /// Run the next turn at once (section 2, step 5).
    pub runnable: bool,
}

/// The owner state. It lives outside the loop's `catch_unwind` closure, so a panic path can still answer every token.
pub struct Owner<C: CoreApi> {
    core: C,
    bridge: Arc<Bridge>,
    phase: Arc<SharedPhase>,
    lifecycle: SyncSender<Lifecycle>,
    counts: TurnCounts,
    waiters: HashMap<OpId, Waiter>,
    view: BTreeMap<SessionId, SessionState>,
    events_left: bool,
    adoption: Adoption,
    /// `Shutdown` tokens: answered `Stopping` by the stop drain.
    shutdowns: Vec<ReplyTo>,
    /// The spawn operation store (design section 4a), when the daemon has one.
    store: Option<SpawnStore>,
    /// A store fault was seen: the stop closes the store at once instead of writing pending terminal states.
    store_failed: bool,
    /// Spawns that wait for their `starting` write, by operation id.
    admitting: HashMap<String, Admit>,
    /// R-122 U1, U2: the answer waits for the forget's completion.
    forgetting: HashMap<String, (ReplyTo, Answer)>,
    /// The U3 rows that the Hub must still remove (`remove_row: true`): session to operation id.
    cleanup: BTreeMap<SessionId, String>,
    /// The cleanup sessions with a `Remove` in flight: at most one per session.
    removing: BTreeSet<SessionId>,
    /// Hub faults that the spawn flow logged (a sync `Start` refusal, `IdInUse` for a Hub-minted id, ...).
    spawn_faults: usize,
    /// The wall clock of this turn in milliseconds, for the store (design section 4a).
    now_ms: u64,
}

impl<C: CoreApi> Owner<C> {
    pub fn new(
        core: C,
        bridge: Arc<Bridge>,
        phase: Arc<SharedPhase>,
        lifecycle: SyncSender<Lifecycle>,
        counts: TurnCounts,
    ) -> Owner<C> {
        Owner {
            core,
            bridge,
            phase,
            lifecycle,
            counts,
            waiters: HashMap::new(),
            view: BTreeMap::new(),
            events_left: false,
            adoption: Adoption::NotStarted,
            shutdowns: Vec::new(),
            store: None,
            store_failed: false,
            admitting: HashMap::new(),
            forgetting: HashMap::new(),
            cleanup: BTreeMap::new(),
            removing: BTreeSet::new(),
            spawn_faults: 0,
            now_ms: 0,
        }
    }

    /// Gives the owner the spawn operation store. The records with `remove_row: true` give the cleanup obligations.
    pub fn with_store(mut self, store: SpawnStore) -> Owner<C> {
        self.cleanup = store
            .remove_rows()
            .into_iter()
            .map(|(operation_id, session)| (SessionId(session), operation_id))
            .collect();
        self.store = Some(store);
        self
    }

    pub fn store(&self) -> Option<&SpawnStore> {
        self.store.as_ref()
    }

    pub fn store_mut(&mut self) -> Option<&mut SpawnStore> {
        self.store.as_mut()
    }

    /// The Hub faults that the spawn flow logged.
    pub fn spawn_faults(&self) -> usize {
        self.spawn_faults
    }

    /// The sessions whose rows the Hub must still remove (R-122 U3).
    pub fn cleanup(&self) -> Vec<SessionId> {
        self.cleanup.keys().cloned().collect()
    }

    pub fn core(&self) -> &C {
        &self.core
    }

    pub fn adoption(&self) -> &Adoption {
        &self.adoption
    }

    /// The session view: built only from `list()` at start, the `SessionState` events of `AdoptAll`, and the events after
    /// it (Core LC-11). The Hub keeps no second registry of sessions.
    pub fn view(&self) -> &BTreeMap<SessionId, SessionState> {
        &self.view
    }

    /// The number of op waiters (bounded by Core's op limits and `max_outstanding` per connection).
    pub fn waiters(&self) -> usize {
        self.waiters.len()
    }

    /// Start step 6: reads `list()` into the view and begins `AdoptAll`. Every row then gets its `SessionState` (LC-11).
    pub fn begin_adoption(&mut self) {
        self.view = self
            .core
            .list()
            .into_iter()
            .map(|row| (row.id, row.state))
            .collect();
        match self.core.begin(Op::AdoptAll) {
            Ok(op) => {
                self.waiters.insert(op, Waiter::Adopt);
                self.adoption = Adoption::Running;
            }
            Err(error) => self.adoption = Adoption::Failed(error_text(&error)),
        }
    }

    /// One owner turn (section 2): admit, pump once, poll events and apply them, then report whether to run again.
    pub fn turn(&mut self, now: Now) -> TurnReport {
        self.now_ms = now.unix.saturating_mul(1000);
        let items =
            self.bridge
                .take_turn(self.counts.control, self.counts.request, self.counts.bulk);
        for item in items {
            self.dispatch(item);
        }
        let pump = self.core.pump(now);
        let events = self.core.poll_events(self.counts.events.max(1));
        // A non-empty poll sets `events_left`; only an empty poll clears it (Core `poll_events` promises "at most max").
        self.events_left = !events.is_empty();
        for event in events {
            self.apply(event);
        }
        self.store_turn();
        TurnReport {
            runnable: pump.more || self.events_left || !self.bridge.is_empty(),
        }
    }

    /// The store's part of a turn: take the completions and act on them, then queue the pending terminal states, the
    /// forgets and the sweep. A fault starts the stop (cause `store_failed`). The wall clock for retention is `now.unix`
    /// (design section 4a).
    fn store_turn(&mut self) {
        let Some(store) = self.store.as_mut() else {
            return;
        };
        for event in store.poll() {
            self.store_event(event);
        }
        if let Some(store) = self.store.as_mut() {
            store.pump(self.now_ms);
        }
    }

    fn store_event(&mut self, event: StoreEvent) {
        match event {
            StoreEvent::Fault(_) => {
                self.store_failed = true;
                request_stop(&self.phase, &self.lifecycle, StopCause::StoreFailed);
            }
            StoreEvent::CreateDurable { operation_id } => self.begin_create(operation_id),
            StoreEvent::CreateNotWritten { operation_id } => {
                if let Some(admit) = self.admitting.remove(&operation_id) {
                    admit.reply.send(failed(FailKind::Internal, Effect::None));
                }
            }
            StoreEvent::Forgotten { operation_id } => {
                if let Some((reply, answer)) = self.forgetting.remove(&operation_id) {
                    reply.send(answer);
                }
            }
        }
    }

    /// A spawn request: reserve and write the `starting` record (design section 4a, API 1).
    fn admit_spawn(&mut self, session: SessionId, request: Box<SpawnRequest>, mut reply: ReplyTo) {
        let Some(store) = self.store.as_mut() else {
            return reply.send(failed(FailKind::Internal, Effect::None));
        };
        match store.create_spawn(&session.0, self.now_ms) {
            Ok(operation_id) => {
                // The record can be on disk from now on: a drop answers `effect: unknown`.
                reply.mark_dispatched();
                self.admitting.insert(
                    operation_id,
                    Admit {
                        session,
                        request,
                        reply,
                    },
                );
            }
            Err(CreateRefused::Busy) => reply.send(failed(FailKind::Busy, Effect::None)),
            Err(CreateRefused::Closed) => {
                let kind = if self.phase.stopping() {
                    FailKind::HubShutdown
                } else {
                    FailKind::Internal
                };
                reply.send(failed(kind, Effect::None));
            }
        }
    }

    /// Spawn flow step 1: the `starting` record is durable, so `begin(Create)`.
    fn begin_create(&mut self, operation_id: String) {
        let Some(Admit {
            session,
            request,
            reply,
        }) = self.admitting.remove(&operation_id)
        else {
            return;
        };
        let op = Op::Create {
            session: session.clone(),
            request: *request,
        };
        match self.core.begin(op) {
            Ok(id) => {
                self.waiters.insert(
                    id,
                    Waiter::SpawnCreate {
                        operation_id,
                        session,
                        reply,
                    },
                );
            }
            // U1: a sync refusal.
            Err(error) => {
                let answer = self.refused_none(error);
                self.forget(operation_id, reply, answer);
            }
        }
    }

    /// U1, U2: no row exists. The answer is the mapped error, `effect: none`. `IdInUse` cannot happen for a Hub-minted
    /// id: it is logged, and the answer is `internal`.
    fn refused_none(&mut self, error: CoreError) -> Answer {
        if matches!(error.code, ErrorCode::IdInUse) {
            self.spawn_fault(format!(
                "Create of a Hub-minted session id: {}",
                error_text(&error)
            ));
            return failed(FailKind::Internal, Effect::None);
        }
        Answer::Spawn(SpawnAnswer::Refused {
            error,
            effect: Effect::None,
        })
    }

    /// U1, U2: delete the `starting` record durably, then answer.
    fn forget(&mut self, operation_id: String, reply: ReplyTo, answer: Answer) {
        let forgot = match self.store.as_mut() {
            Some(store) => store.forget_spawn(&operation_id),
            None => Err("no store".into()),
        };
        match forgot {
            Ok(()) => {
                self.forgetting.insert(operation_id, (reply, answer));
            }
            Err(error) => {
                self.spawn_fault(error);
                reply.send(failed(FailKind::Internal, Effect::Unknown));
            }
        }
    }

    /// Spawn flow step 2: `Completed{Create}`.
    fn create_completed(
        &mut self,
        operation_id: String,
        session: SessionId,
        reply: ReplyTo,
        result: OpResult,
    ) {
        let error = match result {
            OpResult::Ok(_) => {
                reply.send(Answer::Spawn(SpawnAnswer::Accepted {
                    operation_id: operation_id.clone(),
                }));
                return self.begin_start(operation_id, session);
            }
            OpResult::Err(error) => Some(error),
            // `OpResult` is non-exhaustive: an unknown result proves nothing, so a row may exist (U3).
            _ => None,
        };
        let row = self.core.get(&session);
        if let (Some(error), Err(get)) = (&error, &row) {
            if matches!(get.code, ErrorCode::UnknownSession) {
                // U2: Core proves no row.
                let answer = self.refused_none(error.clone());
                return self.forget(operation_id, reply, answer);
            }
        }
        // U3: a row exists or may exist. The record ends `failed { internal }` with the cleanup obligation.
        self.finish(&operation_id, None);
        reply.send(match error {
            Some(error) => Answer::Spawn(SpawnAnswer::Refused {
                error,
                effect: Effect::Unknown,
            }),
            None => failed(FailKind::Internal, Effect::Unknown),
        });
        self.cleanup.insert(session.clone(), operation_id);
        if matches!(&row, Ok(row) if row.state == SessionState::Created) {
            self.begin_remove(&session);
        }
    }

    /// Spawn flow step 3: `begin(Start)` after admission. A sync refusal is a Hub fault: `failed { internal }`, and the
    /// row is kept (R-122 corrects R-120 item 2).
    fn begin_start(&mut self, operation_id: String, session: SessionId) {
        match self.core.begin(Op::Start {
            id: session.clone(),
        }) {
            Ok(id) => {
                self.waiters.insert(
                    id,
                    Waiter::SpawnStart {
                        operation_id,
                        session,
                    },
                );
            }
            Err(error) => {
                self.spawn_fault(format!(
                    "Start of the admitted session {} refused: {}",
                    session.0,
                    error_text(&error)
                ));
                self.finish(&operation_id, Some(FailReason::Internal));
            }
        }
    }

    /// Spawn flow step 4: `Completed{Start}`.
    fn start_completed(&mut self, operation_id: String, session: SessionId, result: OpResult) {
        match result {
            OpResult::Ok(_) => {
                // `installed` needs the row's `ready_reached` (A26). Marked A26: a typed stub, so the spawn stays
                // `starting` until the A26 pin (lead ruling 2026-10-12).
                if let Some(store) = self.store.as_mut() {
                    match install_on_ready(store, &operation_id) {
                        Ok(()) | Err(InstallBlocked::NeedsA26) => {}
                    }
                }
            }
            OpResult::Err(CoreError {
                code: ErrorCode::StartFailed(reason),
                ..
            }) => self.finish(&operation_id, Some(start_fail_reason(reason))),
            // U4: another error, or an unknown result.
            _ => self.start_undecided(&operation_id, &session),
        }
    }

    /// U4: the R-121 tests on the row as it is then. Tests 1 (`start_failure`) and 3 (`ready_reached`) read A26 fields,
    /// so on Core `v1` they never match. A row that is still `Created` gives `failed { internal }`, and the row is kept.
    fn start_undecided(&mut self, operation_id: &str, session: &SessionId) {
        let reason = match self.core.get(session) {
            Err(error) if matches!(error.code, ErrorCode::UnknownSession) => {
                Some(FailReason::Interrupted)
            }
            Ok(row) => match row.state {
                SessionState::Created => Some(FailReason::Internal),
                SessionState::Lost(LostReason::StartInterrupted) => Some(FailReason::Interrupted),
                _ => None,
            },
            Err(_) => None,
        };
        if let Some(reason) = reason {
            self.finish(operation_id, Some(reason));
        }
    }

    /// Writes `failed { reason }`. `None` is U3: `failed { internal }` with `remove_row: true`.
    fn finish(&mut self, operation_id: &str, reason: Option<FailReason>) {
        let now_ms = self.now_ms;
        let finished = match (self.store.as_mut(), reason) {
            (Some(store), Some(reason)) => {
                store.finish_spawn(operation_id, Terminal::Failed(reason), now_ms)
            }
            (Some(store), None) => store.finish_with_cleanup(operation_id, now_ms),
            (None, _) => Err("no store".into()),
        };
        if let Err(error) = finished {
            self.spawn_fault(error);
        }
    }

    /// The cleanup obligation (Sol S2-HD-11): one `Remove` for each sighting of a U3 row as `Created`. A refusal leaves
    /// the marker for the next sighting.
    fn begin_remove(&mut self, session: &SessionId) {
        if !self.cleanup.contains_key(session) || self.removing.contains(session) {
            return;
        }
        if let Ok(id) = self.core.begin(Op::Remove {
            id: session.clone(),
        }) {
            self.removing.insert(session.clone());
            self.waiters.insert(
                id,
                Waiter::SpawnRemove {
                    session: session.clone(),
                },
            );
        }
    }

    /// The U3 row is proved gone: write the record again with `remove_row: false`.
    fn row_gone(&mut self, session: &SessionId) {
        let Some(operation_id) = self.cleanup.remove(session) else {
            return;
        };
        let cleared = match self.store.as_mut() {
            Some(store) => store.clear_remove_row(&operation_id),
            None => Err("no store".into()),
        };
        if let Err(error) = cleared {
            self.spawn_fault(error);
        }
    }

    /// Adoption is a sighting of every U3 row: no row proves it gone, and a `Created` row gets its `Remove`.
    fn adoption_sighting(&mut self) {
        let sessions: Vec<SessionId> = self.cleanup.keys().cloned().collect();
        for session in sessions {
            match self.view.get(&session) {
                None => self.row_gone(&session),
                Some(SessionState::Created) => self.begin_remove(&session),
                Some(_) => {}
            }
        }
    }

    fn spawn_fault(&mut self, message: String) {
        eprintln!("botster-hub: spawn flow: {message}");
        self.spawn_faults += 1;
    }

    fn dispatch(&mut self, item: Item) {
        let Item { request, mut reply } = item;
        match request {
            Request::Op(op) => {
                reply.mark_dispatched();
                match self.core.begin(*op) {
                    Ok(id) => {
                        self.waiters.insert(id, Waiter::Reply(reply));
                    }
                    Err(error) => reply.send(Answer::CoreRefused(error)),
                }
            }
            Request::Get(id) => match self.core.get(&id) {
                Ok(row) => reply.send(Answer::Session(row)),
                Err(error) => reply.send(Answer::CoreRefused(error)),
            },
            Request::List => reply.send(Answer::Sessions(self.core.list())),
            Request::Cancel(op) => {
                reply.mark_dispatched();
                reply.send(Answer::Cancel(self.core.cancel(op)));
            }
            Request::Shutdown => {
                request_stop(&self.phase, &self.lifecycle, StopCause::Requested);
                self.shutdowns.push(reply);
            }
            Request::Spawn { session, request } => self.admit_spawn(session, request, reply),
            Request::GetSpawn(operation_id) => {
                let lookup = match self.store.as_ref() {
                    Some(store) => store.get_spawn(&operation_id),
                    None => Lookup::NotFound,
                };
                reply.send(Answer::SpawnRecord(lookup));
            }
        }
    }

    fn apply(&mut self, event: Event) {
        match event {
            Event::Completed { op, result } => match self.waiters.remove(&op) {
                Some(Waiter::Reply(reply)) => reply.send(Answer::Op(result)),
                Some(Waiter::Adopt) => match result {
                    OpResult::Ok(_) => {
                        self.adoption = Adoption::Done;
                        self.adoption_sighting();
                        self.bridge.set_ready();
                    }
                    OpResult::Err(error) => self.adoption = Adoption::Failed(error_text(&error)),
                    // `OpResult` is non-exhaustive; an unknown result is not a proof of adoption.
                    _ => self.adoption = Adoption::Failed("unknown adoption result".into()),
                },
                Some(Waiter::SpawnCreate {
                    operation_id,
                    session,
                    reply,
                }) => self.create_completed(operation_id, session, reply, result),
                Some(Waiter::SpawnStart {
                    operation_id,
                    session,
                }) => self.start_completed(operation_id, session, result),
                Some(Waiter::SpawnRemove { session }) => {
                    self.removing.remove(&session);
                    // An error leaves the marker: the next `Created` sighting begins `Remove` again.
                    if matches!(result, OpResult::Ok(_)) {
                        self.row_gone(&session);
                    }
                }
                // A `Completed` after the stop took the waiters, or of an op with no waiter: dropped.
                None => {}
            },
            Event::SessionState { id, state, .. } => {
                if state == SessionState::Released {
                    self.view.remove(&id);
                    self.row_gone(&id);
                } else {
                    // During adoption, the sighting waits for `Completed{AdoptAll}` (`adoption_sighting`).
                    let sighting =
                        state == SessionState::Created && self.adoption == Adoption::Done;
                    self.view.insert(id.clone(), state);
                    if sighting {
                        self.begin_remove(&id);
                    }
                }
            }
            // The fan-out to watches is a later step (stacked PR C). Other events change no owner state.
            _ => {}
        }
    }

    /// Stop step 2 (section 5): every item in the lanes and every waiter is answered `hub_shutdown`; a `Shutdown` request is
    /// answered `Stopping`. A lane item never reached Core (`effect: none`); a waiter's op did (`effect: unknown`).
    /// The caller then drops `Core` on this thread.
    pub fn drain(&mut self) {
        for Item { request, reply } in self.bridge.drain() {
            match request {
                Request::Shutdown => reply.send(Answer::Stopping),
                _ => reply.send(shutdown(Effect::None)),
            }
        }
        for (_, waiter) in self.waiters.drain() {
            match waiter {
                Waiter::Reply(reply) | Waiter::SpawnCreate { reply, .. } => {
                    reply.send(shutdown(Effect::Unknown));
                }
                Waiter::Adopt | Waiter::SpawnStart { .. } | Waiter::SpawnRemove { .. } => {}
            }
        }
        // A spawn whose record write or forget did not complete: the record can be on disk.
        for (_, admit) in self.admitting.drain() {
            admit.reply.send(shutdown(Effect::Unknown));
        }
        for (_, (reply, _)) in self.forgetting.drain() {
            reply.send(shutdown(Effect::Unknown));
        }
        for reply in self.shutdowns.drain(..) {
            reply.send(Answer::Stopping);
        }
        // Quiesce the store (Sol finding 6): after a fault, close at once; else write the pending terminal states first.
        // Either way the store thread has ended when this returns, so no store write follows `OwnerDone`.
        if let Some(store) = self.store.as_mut() {
            let events = if self.store_failed {
                store.close()
            } else {
                store.quiesce()
            };
            if events.iter().any(|e| matches!(e, StoreEvent::Fault(_))) {
                self.store_failed = true;
            }
        }
    }

    /// Whether a store fault was seen.
    pub fn store_failed(&self) -> bool {
        self.store_failed
    }

    /// Ends the owner: returns `Core`, so that the caller drops it on this thread.
    pub fn into_core(self) -> C {
        self.core
    }
}

/// Runs `body` on the owner under `catch_unwind`. The owner state lives outside the closure, so after a panic the caller
/// still drains it: every lane token and waiter is answered (design section 5, "Owner panic").
pub fn guarded<C: CoreApi>(owner: &mut Owner<C>, body: impl FnOnce(&mut Owner<C>)) -> Ended {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(owner))) {
        Ok(()) => Ended::Clean,
        Err(_) => Ended::Panicked,
    }
}

fn shutdown(effect: Effect) -> Answer {
    failed(FailKind::HubShutdown, effect)
}

fn failed(kind: FailKind, effect: Effect) -> Answer {
    Answer::Failed(Failure { kind, effect })
}

/// The `StartFailed` reason to the record's reason (design section 4a). `GuardianFailed` and an unknown reason have no
/// ruled mapping: `internal` until the steward answers S2-HD-12 (open; lead 2026-10-12). Change it only if the ruling
/// differs.
fn start_fail_reason(reason: StartFailReason) -> FailReason {
    match reason {
        StartFailReason::ExecFailed { .. } => FailReason::ExecFailed,
        StartFailReason::CwdMissing => FailReason::CwdMissing,
        StartFailReason::StartupTimeout | StartFailReason::WorkerFailed => FailReason::WorkerFailed,
        _ => FailReason::Internal,
    }
}

fn error_text(error: &CoreError) -> String {
    format!("{:?}: {}", error.code, error.detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::{LaneCaps, OutItem, Outbox};
    use crate::phase::tests::Count;
    use crate::phase::{Phase, LIFECYCLE_CAPACITY};
    use botster_core::contract::prelude::*;
    use botster_fake_core::{FakeCore, FakeStore};
    use std::sync::mpsc::Receiver;
    use std::time::{Duration, Instant};

    const COUNTS: TurnCounts = TurnCounts {
        control: 1,
        request: 2,
        bulk: 1,
        events: 2,
    };

    struct Rig {
        owner: Owner<FakeCore>,
        bridge: Arc<Bridge>,
        phase: Arc<SharedPhase>,
        outbox: Arc<Outbox>,
        lifecycle: Receiver<Lifecycle>,
        clock: Instant,
    }

    fn open(store: &FakeStore) -> FakeCore {
        let config = OpenConfig {
            data_dir: "/fake".into(),
            worker_path: Some("/fake/botster-worker".into()),
            limits: CoreLimits::default(),
        };
        FakeCore::open(store, config, 7).unwrap()
    }

    fn rig(core: FakeCore) -> Rig {
        let phase = SharedPhase::new();
        let bridge = Arc::new(Bridge::new(
            LaneCaps {
                control: 2,
                request: 4,
                bulk: 2,
            },
            phase.clone(),
            Arc::new(Count::default()),
        ));
        let (tx, rx) = std::sync::mpsc::sync_channel(LIFECYCLE_CAPACITY);
        let outbox = Outbox::new(16, phase.clone(), Arc::new(Count::default()));
        Rig {
            owner: Owner::new(core, bridge.clone(), phase.clone(), tx, COUNTS),
            bridge,
            phase,
            outbox,
            lifecycle: rx,
            clock: Instant::now(),
        }
    }

    impl Rig {
        fn now(&mut self) -> Now {
            self.clock += Duration::from_millis(1);
            Now {
                monotonic: self.clock,
                unix: 1_000_000,
            }
        }

        /// Turns until the owner reports no runnable work, at most `limit` turns.
        fn settle(&mut self, limit: usize) -> usize {
            for turn in 1..=limit {
                let now = self.now();
                if !self.owner.turn(now).runnable {
                    return turn;
                }
            }
            panic!("still runnable after {limit} turns");
        }

        fn turn(&mut self) -> TurnReport {
            let now = self.now();
            self.owner.turn(now)
        }

        fn submit(&self, id: u64, request: Request) {
            let reply = self.outbox.reserve(id).unwrap();
            self.bridge.submit(request, reply).unwrap();
        }

        fn answers(&self) -> Vec<(u64, Answer)> {
            self.outbox
                .take(64)
                .into_iter()
                .map(|OutItem::Reply { request_id, answer }| (request_id, answer))
                .collect()
        }

        fn adopt(&mut self) {
            self.owner.begin_adoption();
            self.settle(64);
            assert_eq!(self.owner.adoption(), &Adoption::Done);
            assert!(self.bridge.is_ready());
        }
    }

    fn sid(name: &str) -> SessionId {
        SessionId(name.into())
    }

    fn spawn_request() -> SpawnRequest {
        SpawnRequest {
            argv: vec!["/bin/sh".into()],
            env: Default::default(),
            cwd: "/".into(),
            size: Size {
                rows: 24,
                cols: 80,
                cell_px: None,
            },
            labels: Default::default(),
            color_profile: None,
            notification_policy: None,
            size_policy: None,
        }
    }

    fn create(name: &str) -> Request {
        Request::Op(Box::new(Op::Create {
            session: sid(name),
            request: spawn_request(),
        }))
    }

    fn spawn(name: &str) -> Request {
        Request::Spawn {
            session: sid(name),
            request: Box::new(spawn_request()),
        }
    }

    /// Start step 6: `AdoptAll` completes, the bridge becomes ready, and requests before it were refused `NotReady`.
    #[test]
    fn adoption_makes_the_bridge_ready() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        assert!(!rig.bridge.is_ready());
        rig.adopt();
        assert!(rig.owner.view().is_empty());
        assert_eq!(rig.owner.waiters(), 0);
    }

    /// Core: `AdoptAll` runs once per handle; a second `begin` is refused, and the owner reports a failed adoption.
    #[test]
    fn a_refused_adopt_all_fails_adoption() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.owner.begin_adoption();
        assert!(matches!(rig.owner.adoption(), Adoption::Failed(_)));
    }

    /// A Core op is answered from its `Completed` through the waiter map; a sync read is answered in the same turn; the view
    /// follows the `SessionState` events.
    #[test]
    fn op_answers_from_completed_and_view_follows_events() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        let answers = rig.answers();
        assert_eq!(answers.len(), 1);
        assert!(
            matches!(&answers[0], (1, Answer::Op(OpResult::Ok(_)))),
            "{answers:?}"
        );
        assert_eq!(rig.owner.waiters(), 0);
        assert_eq!(
            rig.owner.view().get(&sid("a")),
            Some(&SessionState::Created)
        );
        rig.submit(2, Request::Get(sid("a")));
        rig.submit(3, Request::Get(sid("none")));
        rig.submit(4, Request::List);
        // The request count is 2 per turn: two turns answer the three reads.
        rig.settle(8);
        let answers = rig.answers();
        assert!(matches!(&answers[0], (2, Answer::Session(row)) if row.id == sid("a")));
        assert!(
            matches!(&answers[1], (3, Answer::CoreRefused(e)) if e.code == ErrorCode::UnknownSession)
        );
        assert!(matches!(&answers[2], (4, Answer::Sessions(rows)) if rows.len() == 1));
        rig.submit(5, Request::Op(Box::new(Op::Remove { id: sid("a") })));
        rig.settle(64);
        assert!(matches!(
            &rig.answers()[0],
            (5, Answer::Op(OpResult::Ok(_)))
        ));
        assert!(
            rig.owner.view().is_empty(),
            "Released removes the row from the view"
        );
    }

    /// A sync `begin` refusal is answered at once; no waiter is kept.
    #[test]
    fn begin_refusal_answers_at_once() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, Request::Op(Box::new(Op::Start { id: sid("none") })));
        rig.turn();
        let answers = rig.answers();
        assert!(
            matches!(&answers[0], (1, Answer::CoreRefused(e)) if e.code == ErrorCode::UnknownSession),
            "{answers:?}"
        );
        assert_eq!(rig.owner.waiters(), 0);
    }

    /// `Cancel` answers its `CancelResult` at once.
    #[test]
    fn cancel_answers_at_once() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, Request::Cancel(OpId(999)));
        rig.turn();
        assert!(matches!(&rig.answers()[0], (1, Answer::Cancel(_))));
    }

    /// Stop step 2: `Shutdown` starts the stop (one `StopRequested`), lane items are answered `hub_shutdown` with
    /// `effect: none`, waiters with `effect: unknown`, and the `Shutdown` itself is answered `Stopping`.
    #[test]
    fn shutdown_drain_answers_every_token_once() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        // One op is admitted and dispatched (a waiter); then Shutdown and one more request are submitted together.
        rig.submit(1, create("a"));
        let items = rig.bridge.take_turn(1, 1, 1);
        for item in items {
            rig.owner.dispatch(item);
        }
        assert_eq!(rig.owner.waiters(), 1);
        rig.submit(2, Request::Shutdown);
        rig.submit(3, Request::List);
        rig.submit(4, Request::List);
        rig.submit(5, Request::List);
        // The turn admits the Shutdown and two requests (count 2); one request stays in the lane.
        rig.turn();
        assert_eq!(rig.phase.get(), Phase::Stopping);
        assert_eq!(
            rig.lifecycle.try_recv(),
            Ok(Lifecycle::StopRequested(StopCause::Requested))
        );
        let before = rig.answers();
        rig.owner.drain();
        let mut answers = before;
        answers.extend(rig.answers());
        answers.sort_by_key(|(id, _)| *id);
        let ids: Vec<u64> = answers.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "every request answered exactly once"
        );
        let shutdown = |effect| {
            Answer::Failed(Failure {
                kind: FailKind::HubShutdown,
                effect,
            })
        };
        // Request 1 either completed in the turn or was answered by the drain with `effect: unknown`.
        assert!(
            matches!(&answers[0].1, Answer::Op(_)) || answers[0].1 == shutdown(Effect::Unknown),
            "{:?}",
            answers[0]
        );
        assert_eq!(answers[1].1, Answer::Stopping);
        assert!(matches!(&answers[2].1, Answer::Sessions(_)));
        assert!(matches!(&answers[3].1, Answer::Sessions(_)));
        assert_eq!(answers[4].1, shutdown(Effect::None));
        assert_eq!(rig.outbox.reserved(), 0);
        assert!(rig.lifecycle.try_recv().is_err());
    }

    /// Owner panic: a panic in the loop body leaves the owner state intact, and the drain still answers every token once.
    #[test]
    fn panic_in_the_loop_still_answers_every_token() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.submit(2, Request::List);
        let ended = guarded(&mut rig.owner, |owner| {
            let item = owner.bridge.take_turn(1, 1, 1).into_iter().next().unwrap();
            owner.dispatch(item);
            panic!("injected");
        });
        assert_eq!(ended, Ended::Panicked);
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.owner.drain();
        let mut answers = rig.answers();
        answers.sort_by_key(|(id, _)| *id);
        let shutdown = |effect| {
            Answer::Failed(Failure {
                kind: FailKind::HubShutdown,
                effect,
            })
        };
        assert_eq!(
            answers,
            vec![(1, shutdown(Effect::Unknown)), (2, shutdown(Effect::None))]
        );
        assert_eq!(guarded(&mut rig.owner, |_| {}), Ended::Clean);
    }

    fn store_config() -> crate::store::StoreConfig {
        crate::store::StoreConfig {
            max_records: 4,
            store_queue: 2,
            retention_ms: 1000,
            max_record_bytes: 4096,
            turn_sweeps: 1,
        }
    }

    fn short_root() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    /// Turns until the store has nothing in flight (the store thread is real).
    fn store_settle(rig: &mut Rig) {
        for _ in 0..5000 {
            rig.turn();
            if rig.owner.store().unwrap().in_flight() == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("the store did not settle");
    }

    /// A rig with a real store in `dir` on the fake Core of `core`.
    fn store_rig(dir: &std::path::Path, core: &FakeStore) -> Rig {
        let store = crate::store::SpawnStore::open(dir, store_config(), Arc::new(Count::default()))
            .unwrap();
        let base = rig(open(core));
        Rig {
            owner: base.owner.with_store(store),
            ..base
        }
    }

    /// Turns until Core, the store and the spawn flow have nothing left (the store thread is real).
    fn flow_settle(rig: &mut Rig) {
        for _ in 0..5000 {
            let report = rig.turn();
            let owner = &rig.owner;
            if !report.runnable
                && owner.store().unwrap().in_flight() == 0
                && owner.admitting.is_empty()
                && owner.forgetting.is_empty()
                && owner.waiters.is_empty()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("the spawn flow did not settle");
    }

    fn record(rig: &Rig, operation_id: &str) -> Option<crate::store::Record> {
        match rig.owner.store().unwrap().get_spawn(operation_id) {
            crate::store::Lookup::Found(record) => Some(record),
            _ => None,
        }
    }

    fn records(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir.join("store/ops")).unwrap().count()
    }

    fn accepted(answer: &Answer) -> String {
        match answer {
            Answer::Spawn(SpawnAnswer::Accepted { operation_id }) => operation_id.clone(),
            other => panic!("not accepted: {other:?}"),
        }
    }

    fn fail_next(core: &FakeCore, target: &str, error: ErrorCode) {
        core.control(
            "fail_next",
            &serde_json::json!({ "target": target, "error": error }),
        )
        .unwrap();
    }

    fn stop(mut rig: Rig) {
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.owner.drain();
        assert!(!rig.owner.store_failed());
    }

    /// R-122: `accepted` goes after `Completed{Create}`, and `Start` follows. `installed` needs `ready_reached` (A26), so the
    /// record stays `starting` (lead ruling 2026-10-12).
    #[test]
    fn spawn_is_accepted_and_stays_starting_until_a26() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        rig.submit(1, spawn("a"));
        flow_settle(&mut rig);
        let answers = rig.answers();
        assert_eq!(answers.len(), 1);
        let id = accepted(&answers[0].1);
        let record = record(&rig, &id).unwrap();
        assert_eq!(record.state, crate::store::SpawnState::Starting);
        assert_eq!(record.session_id, "a");
        assert!(!record.remove_row);
        assert_ne!(
            rig.owner.core().get(&sid("a")).unwrap().state,
            SessionState::Created,
            "Start was begun"
        );
        assert_eq!(rig.owner.spawn_faults(), 0);
        stop(rig);
    }

    /// R-122 U1: a sync `Create` refusal deletes the record durably, then answers the error with `effect: none`.
    #[test]
    fn u1_sync_refusal_forgets_then_answers() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.submit(1, spawn(&"x".repeat(4096)));
        flow_settle(&mut rig);
        let answers = rig.answers();
        assert!(
            matches!(
                &answers[..],
                [(1, Answer::Spawn(SpawnAnswer::Refused { error, effect: Effect::None }))]
                    if error.code == ErrorCode::IdTooLong
            ),
            "{answers:?}"
        );
        assert!(rig.owner.store().unwrap().is_empty());
        assert_eq!(records(tmp.path()), 0);
        stop(rig);
    }

    /// R-122 U1: `IdInUse` cannot happen for a Hub-minted id. It is logged, and the answer is `internal`, `effect: none`.
    #[test]
    fn u1_id_in_use_answers_internal() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        rig.answers();
        rig.submit(2, spawn("a"));
        flow_settle(&mut rig);
        assert_eq!(
            rig.answers(),
            vec![(2, failed(FailKind::Internal, Effect::None))]
        );
        assert_eq!(rig.owner.spawn_faults(), 1);
        assert_eq!(records(tmp.path()), 0);
        stop(rig);
    }

    /// R-122 U2: a `Create` error and Core proves no row: as U1.
    #[test]
    fn u2_create_error_with_no_row_forgets_then_answers() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        fail_next(rig.owner.core(), "Create", ErrorCode::WorkerLinkFailed);
        rig.submit(1, spawn("a"));
        flow_settle(&mut rig);
        let answers = rig.answers();
        assert!(
            matches!(
                &answers[..],
                [(1, Answer::Spawn(SpawnAnswer::Refused { error, effect: Effect::None }))]
                    if error.code == ErrorCode::WorkerLinkFailed
            ),
            "{answers:?}"
        );
        assert_eq!(records(tmp.path()), 0);
        stop(rig);
    }

    /// Spawn flow step 4: `StartFailed` gives `failed` with the mapped reason; the row is kept.
    #[test]
    fn start_failed_maps_the_reason() {
        let cases = [
            (
                StartFailReason::ExecFailed { errno: 2 },
                FailReason::ExecFailed,
            ),
            (StartFailReason::CwdMissing, FailReason::CwdMissing),
            (StartFailReason::StartupTimeout, FailReason::WorkerFailed),
            (StartFailReason::WorkerFailed, FailReason::WorkerFailed),
            (StartFailReason::GuardianFailed, FailReason::Internal),
        ];
        for (reason, mapped) in cases {
            let tmp = short_root();
            let mut rig = store_rig(tmp.path(), &FakeStore::new());
            rig.adopt();
            rig.owner.store_mut().unwrap().end_recovery();
            fail_next(rig.owner.core(), "Start", ErrorCode::StartFailed(reason));
            rig.submit(1, spawn("a"));
            flow_settle(&mut rig);
            let id = accepted(&rig.answers()[0].1);
            let record = record(&rig, &id).unwrap();
            assert_eq!(record.state, crate::store::SpawnState::Failed);
            assert_eq!(record.reason, Some(mapped), "{reason:?}");
            assert!(!record.remove_row);
            assert!(rig.owner.core().get(&sid("a")).is_ok(), "the row is kept");
            stop(rig);
        }
    }

    /// R-122 U4: another `Start` error with the row still `Created` gives `failed { internal }`, and the row is kept.
    #[test]
    fn u4_start_error_with_a_created_row_is_internal() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        fail_next(rig.owner.core(), "Start", ErrorCode::WorkerLinkFailed);
        rig.submit(1, spawn("a"));
        flow_settle(&mut rig);
        let id = accepted(&rig.answers()[0].1);
        let record = record(&rig, &id).unwrap();
        assert_eq!(
            (record.state, record.reason),
            (crate::store::SpawnState::Failed, Some(FailReason::Internal))
        );
        assert_eq!(
            rig.owner.core().get(&sid("a")).unwrap().state,
            SessionState::Created
        );
        assert!(
            rig.owner.cleanup().is_empty(),
            "no Hub Remove after admission"
        );
        stop(rig);
    }

    /// The `starting` record of a spawn whose write is durable, with a `Created` row for `name`. Core cannot fail a
    /// `Create` and keep its row, so the U3 tests call `create_completed` with the error themselves.
    fn u3_setup(rig: &mut Rig, name: &str) -> String {
        rig.submit(9, create(name));
        rig.settle(64);
        rig.answers();
        let id = rig
            .owner
            .store_mut()
            .unwrap()
            .create_spawn(name, 0)
            .unwrap();
        flow_settle(rig);
        id
    }

    /// R-122 U3 and Sol S2-HD-11: a `Create` error with a row gives `failed { internal }` with `remove_row: true` and
    /// `effect: unknown`; the Hub begins `Remove` of the `Created` row, and its `Ok` clears the marker.
    #[test]
    fn u3_removes_the_row_then_clears_the_marker() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        let id = u3_setup(&mut rig, "a");
        let reply = rig.outbox.reserve(1).unwrap();
        let error = CoreError::new(ErrorCode::WorkerLinkFailed, "lost");
        rig.owner
            .create_completed(id.clone(), sid("a"), reply, OpResult::Err(error.clone()));
        assert_eq!(rig.owner.cleanup(), vec![sid("a")]);
        flow_settle(&mut rig);
        assert_eq!(
            rig.answers(),
            vec![(
                1,
                Answer::Spawn(SpawnAnswer::Refused {
                    error,
                    effect: Effect::Unknown
                })
            )]
        );
        assert!(
            rig.owner.core().get(&sid("a")).is_err(),
            "the row is removed"
        );
        assert!(rig.owner.cleanup().is_empty());
        let record = record(&rig, &id).unwrap();
        assert_eq!(
            (record.state, record.reason, record.remove_row),
            (
                crate::store::SpawnState::Failed,
                Some(FailReason::Internal),
                false
            )
        );
        stop(rig);
    }

    /// Sol S2-HD-11: a failed `Remove` keeps the marker, durably; the next sighting (an adoption after a restart) begins
    /// `Remove` again. Retention does not sweep the record while the marker is set.
    #[test]
    fn u3_failed_remove_keeps_the_marker_until_the_next_sighting() {
        let tmp = short_root();
        let core = FakeStore::new();
        let mut rig = store_rig(tmp.path(), &core);
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        let id = u3_setup(&mut rig, "a");
        fail_next(rig.owner.core(), "Remove", ErrorCode::WorkerLinkFailed);
        let reply = rig.outbox.reserve(1).unwrap();
        rig.owner.create_completed(
            id.clone(),
            sid("a"),
            reply,
            OpResult::Err(CoreError::new(ErrorCode::WorkerLinkFailed, "lost")),
        );
        flow_settle(&mut rig);
        assert!(rig.owner.core().get(&sid("a")).is_ok());
        assert_eq!(rig.owner.cleanup(), vec![sid("a")]);
        assert!(record(&rig, &id).unwrap().remove_row);
        // Far past the retention: the marker keeps the record.
        rig.owner.now_ms = u64::MAX / 2;
        rig.owner.store_mut().unwrap().pump(u64::MAX / 2);
        flow_settle(&mut rig);
        assert!(record(&rig, &id).is_some());
        let Rig { mut owner, .. } = rig;
        owner.drain();
        drop(owner.into_core());
        // The next start: the marker is loaded, and adoption sees the `Created` row.
        let mut rig = store_rig(tmp.path(), &core);
        assert_eq!(rig.owner.cleanup(), vec![sid("a")]);
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        flow_settle(&mut rig);
        assert!(rig.owner.core().get(&sid("a")).is_err());
        assert!(rig.owner.cleanup().is_empty());
        assert!(!record(&rig, &id).unwrap().remove_row);
        stop(rig);
    }

    /// Sol S2-HD-11: no row after adoption proves the U3 row gone, with no `Remove`.
    #[test]
    fn u3_no_row_after_adoption_clears_the_marker() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        let id = rig
            .owner
            .store_mut()
            .unwrap()
            .create_spawn("gone", 0)
            .unwrap();
        flow_settle(&mut rig);
        let now_ms = rig.owner.now_ms;
        rig.owner
            .store_mut()
            .unwrap()
            .finish_with_cleanup(&id, now_ms)
            .unwrap();
        flow_settle(&mut rig);
        let Rig { mut owner, .. } = rig;
        owner.drain();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        assert_eq!(rig.owner.cleanup(), vec![sid("gone")]);
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        flow_settle(&mut rig);
        assert!(rig.owner.cleanup().is_empty());
        assert!(!record(&rig, &id).unwrap().remove_row);
        stop(rig);
    }

    /// Stop step 2: a spawn whose `starting` write is in flight is answered `hub_shutdown`, `effect: unknown` (the record
    /// can be on disk).
    #[test]
    fn drain_answers_a_spawn_in_admission() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.submit(1, spawn("a"));
        for item in rig.bridge.take_turn(1, 1, 1) {
            rig.owner.dispatch(item);
        }
        assert_eq!(rig.owner.admitting.len(), 1);
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.owner.drain();
        assert_eq!(rig.answers(), vec![(1, shutdown(Effect::Unknown))]);
    }

    /// Store fault: a write with an unknown result starts the stop (cause `store_failed`), and the drain closes the store at
    /// once.
    #[test]
    fn store_fault_starts_the_stop() {
        let tmp = short_root();
        let store =
            crate::store::SpawnStore::open(tmp.path(), store_config(), Arc::new(Count::default()))
                .unwrap();
        let store_rig = rig(open(&FakeStore::new()));
        let mut rig = Rig {
            owner: store_rig.owner.with_store(store),
            ..store_rig
        };
        let id = rig.owner.store_mut().unwrap().create_spawn("s", 0).unwrap();
        store_settle(&mut rig);
        // A non-empty directory in the record's place makes the next `rename` fail: an unknown result.
        let ops = tmp.path().join("store/ops");
        let file = std::fs::read_dir(&ops)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        std::fs::write(file.join("x"), b"").unwrap();
        rig.owner
            .store_mut()
            .unwrap()
            .finish_spawn(&id, crate::store::Terminal::Installed, 1)
            .unwrap();
        store_settle(&mut rig);
        assert!(rig.owner.store_failed());
        assert_eq!(rig.phase.get(), Phase::Stopping);
        assert_eq!(
            rig.lifecycle.try_recv(),
            Ok(Lifecycle::StopRequested(StopCause::StoreFailed))
        );
        rig.owner.drain();
    }

    /// Stop step 2 with a store: the drain writes the pending terminal states, so the next start reads them.
    #[test]
    fn drain_writes_pending_terminal_states() {
        let tmp = short_root();
        let store =
            crate::store::SpawnStore::open(tmp.path(), store_config(), Arc::new(Count::default()))
                .unwrap();
        let store_rig = rig(open(&FakeStore::new()));
        let mut rig = Rig {
            owner: store_rig.owner.with_store(store),
            ..store_rig
        };
        let id = rig.owner.store_mut().unwrap().create_spawn("s", 0).unwrap();
        store_settle(&mut rig);
        rig.owner
            .store_mut()
            .unwrap()
            .finish_spawn(&id, crate::store::Terminal::Cancelled, 1)
            .unwrap();
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.owner.drain();
        let mut reopened =
            crate::store::SpawnStore::open(tmp.path(), store_config(), Arc::new(Count::default()))
                .unwrap();
        reopened.end_recovery();
        assert!(matches!(
            reopened.get_spawn(&id),
            crate::store::Lookup::Found(r) if r.state == crate::store::SpawnState::Cancelled
        ));
        reopened.close();
    }

    /// A waiter whose `Completed` arrives after the drain finds no waiter: no second answer.
    #[test]
    fn completed_after_drain_is_dropped() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        for item in rig.bridge.take_turn(1, 1, 1) {
            rig.owner.dispatch(item);
        }
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.owner.drain();
        // Core still posts the op's `Completed`.
        rig.settle(64);
        let answers = rig.answers();
        assert_eq!(answers.len(), 1);
        assert_eq!(
            answers[0].1,
            Answer::Failed(Failure {
                kind: FailKind::HubShutdown,
                effect: Effect::Unknown,
            })
        );
    }

    /// Section 2 step 5: the owner runs again while a lane is not empty, and reports no work when idle.
    #[test]
    fn runnable_while_lanes_hold_items() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        for id in 0..4 {
            rig.submit(id, Request::List);
        }
        // Count 2 per turn: two turns admit four; the first reports runnable.
        assert!(rig.turn().runnable);
        let settled = rig.settle(8);
        assert!(settled >= 1);
        assert_eq!(rig.answers().len(), 4);
        assert!(!rig.turn().runnable);
    }
}
