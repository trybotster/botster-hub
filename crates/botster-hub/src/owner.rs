//! The owner of the embedded Core (design sections 2 and 4): the only thread that touches `Core`. It owns the waiter map and
//! the session view, admits requests from the bridge, and turns Core with counted work per turn (no wall-clock budget).
//!
//! The owner is generic over `CoreApi`, so its unit tests run on the contracts' fake Core with no I/O, one turn at a time.
//! [`run`] is the thread's loop over `mio`; it runs only on the real Core (the fake's wake has no descriptor).

use crate::bridge::{
    Answer, Bridge, Effect, FailKind, Failure, Item, ReplyTo, Request, SpawnAnswer,
};
use crate::client::handoff::{
    ReserveRefused, ReturnedStream, RouteConnectAnswer, RouteParams, Routes,
};
use crate::phase::{request_stop, Ended, Lifecycle, SharedPhase, StopCause};
use crate::store::{
    install_on_ready, CreateRefused, FailReason, InstallBlocked, Lookup, SpawnStore, StoreEvent,
    Terminal,
};
use crate::watch::{Activity, Row, WatchFrame, WatchKey, WatchLimits, Watches};
use botster_core::contract::prelude::{
    ActivitySource, CoreApi, CoreError, ErrorCode, Event, LostReason, Now, Op, OpId, OpResult,
    SessionId, SessionState, SpawnRequest, StartFailReason,
};
use botster_hub_contract::prelude::{CloseReason, HubShutdownData};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// HC section 6 `activity_idle_ms`, the Hub's default (design section 4, "The view row").
pub const ACTIVITY_IDLE: Duration = Duration::from_millis(3000);

/// The per-turn counts (section 2; parameters with small test values, HP LM-5 rule). Each is at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnCounts {
    pub control: usize,
    pub request: usize,
    pub bulk: usize,
    pub events: usize,
    /// Watch deliveries per turn (the fan-out continuation).
    pub fanout: usize,
    /// Baseline rows per turn, across all baselines.
    pub baseline_rows: usize,
}

/// The fan-out continuation (design section 4): one poll's events, the next event, and the next watch. While it is
/// pending, the owner polls no new event, so the events wait in Core's own bounded queue and the order per watch holds.
#[derive(Debug)]
struct FanOut {
    batch: Vec<Event>,
    next_event: usize,
    /// The current event was applied to the view; its delta goes to the watches after `next_watch`.
    started: bool,
    next_watch: Option<WatchKey>,
    delta: Option<(SessionId, WatchFrame)>,
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
    /// The label writer's one `UpdateMetadata` of a session, with the full map that it sent.
    SetLabels {
        session: SessionId,
        labels: BTreeMap<String, String>,
        reply: ReplyTo,
    },
}

/// A queued `SetLabels` (design section 4, "The label writer").
#[derive(Debug)]
struct LabelEdit {
    set: BTreeMap<String, String>,
    remove: Vec<String>,
    reply: ReplyTo,
}

/// What an event did to the view: the delta that the watches get.
enum ViewChange {
    Row(SessionId),
    Removed(SessionId),
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
    view: BTreeMap<SessionId, Arc<Row>>,
    watches: Watches,
    fanout: Option<FanOut>,
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
    /// The monotonic clock of this turn, for the reservation deadlines (read once per turn, section 2).
    now: Instant,
    /// The route reservations and the route table (client protocol design, section 2), when the daemon serves clients.
    routes: Option<Routes>,
    /// `activity_idle_ms`: the TM-4 silence threshold of every session (design section 4, "The view row").
    activity_idle: Duration,
    /// The sessions whose threshold this process has set (the threshold is not in Core's registry).
    thresholds: BTreeSet<SessionId>,
    /// The label writer: the queued writes of each session, and the sessions with an `UpdateMetadata` in flight.
    label_queues: BTreeMap<SessionId, VecDeque<LabelEdit>>,
    labels_busy: BTreeSet<SessionId>,
}

impl<C: CoreApi> Owner<C> {
    pub fn new(
        core: C,
        bridge: Arc<Bridge>,
        phase: Arc<SharedPhase>,
        lifecycle: SyncSender<Lifecycle>,
        counts: TurnCounts,
        limits: WatchLimits,
    ) -> Owner<C> {
        Owner {
            core,
            bridge,
            phase,
            lifecycle,
            counts,
            waiters: HashMap::new(),
            view: BTreeMap::new(),
            watches: Watches::new(limits),
            fanout: None,
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
            now: Instant::now(),
            routes: None,
            activity_idle: ACTIVITY_IDLE,
            thresholds: BTreeSet::new(),
            label_queues: BTreeMap::new(),
            labels_busy: BTreeSet::new(),
        }
    }

    /// Sets `activity_idle_ms`. For a live change, the owner sets the threshold again for every row (TM-4: a new
    /// threshold starts a new idle period).
    pub fn set_activity_idle(&mut self, idle: Duration) {
        let changed = self.activity_idle != idle;
        self.activity_idle = idle;
        if changed {
            let sessions: Vec<SessionId> = self.thresholds.iter().cloned().collect();
            for id in sessions {
                self.set_threshold(&id);
            }
        }
    }

    /// The threshold call. An `UnknownSession` error is a row that is gone; its `Released` follows.
    fn set_threshold(&mut self, id: &SessionId) {
        if let Err(error) = self
            .core
            .set_silence_threshold(id, Some(self.activity_idle))
        {
            if !matches!(error.code, ErrorCode::UnknownSession) {
                eprintln!(
                    "botster-hub: the silence threshold of {}: {}",
                    id.0,
                    error_text(&error)
                );
            }
        }
    }

    /// Replaces a view row through `edit`. It returns the delta only when the row changed.
    fn update_row(&mut self, id: &SessionId, edit: impl FnOnce(&mut Row)) -> Option<ViewChange> {
        let current = self.view.get(id)?;
        let mut row = Row::clone(current);
        edit(&mut row);
        if row == **current {
            return None;
        }
        self.view.insert(id.clone(), Arc::new(row));
        Some(ViewChange::Row(id.clone()))
    }

    /// The label writer: begins the next queued `SetLabels` of `session` when none is in flight.
    fn next_labels(&mut self, session: &SessionId) {
        if self.labels_busy.contains(session) {
            return;
        }
        loop {
            let Some(mut edit) = self
                .label_queues
                .get_mut(session)
                .and_then(VecDeque::pop_front)
            else {
                self.label_queues.remove(session);
                return;
            };
            let Some(row) = self.view.get(session) else {
                edit.reply.send(Answer::CoreRefused(CoreError::new(
                    ErrorCode::UnknownSession,
                    "no such session",
                )));
                continue;
            };
            let mut labels = row.labels.clone();
            for key in &edit.remove {
                labels.remove(key);
            }
            labels.extend(edit.set);
            edit.reply.mark_dispatched();
            let op = Op::UpdateMetadata {
                id: session.clone(),
                labels: labels.clone(),
            };
            match self.core.begin(op) {
                Ok(op) => {
                    self.labels_busy.insert(session.clone());
                    self.waiters.insert(
                        op,
                        Waiter::SetLabels {
                            session: session.clone(),
                            labels,
                            reply: edit.reply,
                        },
                    );
                    return;
                }
                Err(error) => edit.reply.send(Answer::CoreRefused(error)),
            }
        }
    }

    /// Gives the owner the route tables (client protocol design, section 2.1).
    pub fn with_routes(mut self, params: RouteParams) -> Owner<C> {
        self.routes = Some(Routes::new(params));
        self
    }

    pub fn routes(&self) -> Option<&Routes> {
        self.routes.as_ref()
    }

    pub fn routes_mut(&mut self) -> Option<&mut Routes> {
        self.routes.as_mut()
    }

    /// The owner's wait timeout: the earlier of Core's next deadline and the earliest reservation deadline.
    pub fn next_deadline(&self) -> Option<Instant> {
        let routes = self.routes.as_ref().and_then(Routes::next_deadline);
        match (self.core.next_deadline(), routes) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Sets `activity_idle_ms` at construction (the first `SessionState` of each session sets the threshold, so nothing is
    /// set yet); a live change after the start is [`Owner::set_activity_idle`].
    pub fn with_activity_idle(mut self, idle: Duration) -> Owner<C> {
        self.activity_idle = idle;
        self
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
    pub fn view(&self) -> &BTreeMap<SessionId, Arc<Row>> {
        &self.view
    }

    pub fn watches(&self) -> &Watches {
        &self.watches
    }

    /// Whether a fan-out continuation is pending.
    pub fn fanout_pending(&self) -> bool {
        self.fanout.is_some()
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
            .map(|record| (record.id.clone(), Arc::new(Row::from_record(record))))
            .collect();
        match self.core.begin(Op::AdoptAll) {
            Ok(op) => {
                self.waiters.insert(op, Waiter::Adopt);
                self.adoption = Adoption::Running;
            }
            Err(error) => self.adoption = Adoption::Failed(error_text(&error)),
        }
    }

    /// One owner turn (section 2): admit; pump once; continue the fan-out; only with no fan-out pending, the baselines
    /// and a new poll; then report whether to run again.
    pub fn turn(&mut self, now: Now) -> TurnReport {
        self.now_ms = now.unix.saturating_mul(1000);
        self.now = now.monotonic;
        // Due reservations end before this turn's connects, so a connect past its window finds no entry.
        if let Some(routes) = self.routes.as_mut() {
            routes.expire(self.now);
        }
        let items =
            self.bridge
                .take_turn(self.counts.control, self.counts.request, self.counts.bulk);
        for item in items {
            self.dispatch(item);
        }
        // Once the stop has begun, Core does no more work: no pump, no event poll (the stop drain answers what is left).
        if self.phase.stopping() {
            return TurnReport { runnable: false };
        }
        let pump = self.core.pump(now);
        self.watches.remove_closed();
        self.fanout_step();
        if self.fanout.is_none() {
            self.watches
                .baseline_turn(self.counts.baseline_rows, &self.view);
            let events = self.core.poll_events(self.counts.events.max(1));
            // A non-empty poll sets `events_left`; only an empty poll clears it (Core `poll_events` promises "at most
            // max").
            self.events_left = !events.is_empty();
            if self.events_left {
                self.fanout = Some(FanOut {
                    batch: events,
                    next_event: 0,
                    started: false,
                    next_watch: None,
                    delta: None,
                });
            }
        }
        self.store_turn();
        TurnReport {
            runnable: pump.more
                || self.events_left
                || self.fanout.is_some()
                || self.watches.runnable()
                || !self.bridge.is_empty(),
        }
    }

    /// Continues the fan-out for at most `fanout` units of work: applying one event to the view is one unit, and each
    /// watch visited is one unit, so a turn's work is bounded whatever the number of watches (Sol finding 5). The view
    /// advances one event at a time, as the event's fan-out starts (Sol finding 3, round 2).
    fn fanout_step(&mut self) {
        let Some(mut fan) = self.fanout.take() else {
            return;
        };
        let mut budget = self.counts.fanout.max(1);
        while budget > 0 {
            if !fan.started {
                let Some(event) = fan.batch.get(fan.next_event).cloned() else {
                    return;
                };
                fan.delta = match self.apply(event) {
                    Some(ViewChange::Removed(id)) => Some((id.clone(), WatchFrame::Removed(id))),
                    Some(ViewChange::Row(id)) => self
                        .view
                        .get(&id)
                        .map(|row| (id, WatchFrame::Delta(row.clone()))),
                    None => None,
                };
                fan.started = true;
                fan.next_watch = None;
                budget -= 1;
                continue;
            }
            let next = match &fan.delta {
                Some(_) => self.watches.next_after(fan.next_watch),
                None => None,
            };
            match (next, &fan.delta) {
                (Some(key), Some((id, frame))) => {
                    self.watches.deliver(key, id, frame);
                    fan.next_watch = Some(key);
                    budget -= 1;
                }
                _ => {
                    fan.next_event += 1;
                    fan.started = false;
                }
            }
        }
        if fan.started || fan.next_event < fan.batch.len() {
            self.fanout = Some(fan);
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
            Err(CreateRefused::BadSession) => {
                self.spawn_fault(format!(
                    "the session id {:?} cannot name a directory",
                    session.0
                ));
                reply.send(failed(FailKind::Internal, Effect::None));
            }
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
            }) => {
                let mapped = start_fail_reason(reason);
                if mapped == FailReason::StartFailed {
                    eprintln!(
                        "botster-hub: spawn flow: an unknown StartFailReason {reason:?} for {}",
                        session.0
                    );
                }
                self.finish(&operation_id, Some(mapped));
            }
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
            match self.view.get(&session).map(|row| row.state) {
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
        // After a `Shutdown` (or any stop cause) no later request of the turn reaches Core: it is answered `hub_shutdown`,
        // effect none. A repeated `Shutdown` is still answered `Stopping`.
        if self.phase.stopping() && !matches!(request, Request::Shutdown) {
            reply.send(match request {
                // HS-9: a route connection still gets its typed close, as in the drain.
                Request::RouteConnect(request) => route_connect_shutdown(*request),
                _ => shutdown(Effect::None),
            });
            return;
        }
        match request {
            // One writer of the labels (design section 4, "The label writer").
            Request::Op(op) if matches!(*op, Op::UpdateMetadata { .. }) => {
                reply.send(failed(FailKind::Internal, Effect::None));
            }
            Request::SetLabels {
                session,
                set,
                remove,
            } => {
                self.label_queues
                    .entry(session.clone())
                    .or_default()
                    .push_back(LabelEdit { set, remove, reply });
                self.next_labels(&session);
            }
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
            Request::Watch {
                key,
                filter,
                outbox,
            } => match self.watches.add(key, filter, outbox) {
                Ok(()) => reply.send(Answer::Watching),
                Err(_) => reply.send(Answer::Failed(Failure {
                    kind: FailKind::TooManyWatches,
                    effect: Effect::None,
                })),
            },
            // The watch, its baseline entry and its queue go in one step; then the reply, so every frame of the watch is
            // ahead of it in the FIFO (HC WT-6).
            Request::Unwatch(key) => {
                self.watches.remove(key);
                reply.send(Answer::Unwatched);
            }
            Request::ReserveRoute(request) => {
                let Some(routes) = self.routes.as_mut() else {
                    reply.send(failed(FailKind::Internal, Effect::None));
                    return;
                };
                let answer = match routes.reserve(&self.core, self.now, *request) {
                    Ok(reservation) => Answer::RouteReserved(reservation),
                    Err(ReserveRefused::Core(error)) => Answer::CoreRefused(error),
                    Err(ReserveRefused::Busy) => failed(FailKind::Busy, Effect::None),
                    Err(ReserveRefused::Internal(message)) => {
                        eprintln!("botster-hub: route reservation: {message}");
                        failed(FailKind::Internal, Effect::None)
                    }
                };
                reply.send(answer);
            }
            Request::RouteConnect(request) => {
                reply.mark_dispatched();
                let answer = match self.routes.as_mut() {
                    Some(routes) => routes.connect(&mut self.core, self.now, *request),
                    // No route table: no token can be valid.
                    None => RouteConnectAnswer::Refused {
                        reason: CloseReason::RouteTokenInvalid,
                        stream: ReturnedStream(Arc::new(request.stream)),
                    },
                };
                reply.send(Answer::RouteConnect(answer));
            }
        }
    }

    /// Applies one Core event to the owner state. It returns the view change, which the fan-out sends to the watches.
    fn apply(&mut self, event: Event) -> Option<ViewChange> {
        match event {
            Event::Completed { op, result } => match self.waiters.remove(&op) {
                Some(Waiter::SetLabels {
                    session,
                    labels,
                    reply,
                }) => {
                    self.labels_busy.remove(&session);
                    // The base of the next write is the map that Core now holds; `MetadataChanged` comes later and
                    // reads the same map. An error leaves the row as it was.
                    let change = if matches!(result, OpResult::Ok(_)) {
                        self.update_row(&session, |row| row.labels = labels)
                    } else {
                        None
                    };
                    reply.send(Answer::Op(result));
                    self.next_labels(&session);
                    change
                }
                Some(Waiter::Reply(reply)) => {
                    reply.send(Answer::Op(result));
                    None
                }
                Some(Waiter::Adopt) => {
                    match result {
                        OpResult::Ok(_) => {
                            self.adoption = Adoption::Done;
                            self.adoption_sighting();
                            self.bridge.set_ready();
                        }
                        OpResult::Err(error) => {
                            self.adoption = Adoption::Failed(error_text(&error))
                        }
                        // `OpResult` is non-exhaustive; an unknown result is not a proof of adoption.
                        _ => self.adoption = Adoption::Failed("unknown adoption result".into()),
                    }
                    None
                }
                Some(Waiter::SpawnCreate {
                    operation_id,
                    session,
                    reply,
                }) => {
                    self.create_completed(operation_id, session, reply, result);
                    None
                }
                Some(Waiter::SpawnStart {
                    operation_id,
                    session,
                }) => {
                    self.start_completed(operation_id, session, result);
                    None
                }
                Some(Waiter::SpawnRemove { session }) => {
                    self.removing.remove(&session);
                    // An error leaves the marker: the next `Created` sighting begins `Remove` again.
                    if matches!(result, OpResult::Ok(_)) {
                        self.row_gone(&session);
                    }
                    None
                }
                // A `Completed` after the stop took the waiters, or of an op with no waiter: dropped.
                None => None,
            },
            Event::SessionState { id, state, .. } => {
                let change = if state == SessionState::Released {
                    self.view.remove(&id);
                    self.row_gone(&id);
                    self.thresholds.remove(&id);
                    // Queued label writes of a gone row get the op's own answer.
                    for edit in self.label_queues.remove(&id).into_iter().flatten() {
                        edit.reply.send(Answer::CoreRefused(CoreError::new(
                            ErrorCode::UnknownSession,
                            "the session is released",
                        )));
                    }
                    // RT-2: the session directory goes with its row (design section 4a, "The session directory").
                    if let Some(store) = self.store.as_mut() {
                        store.release_session(&id.0);
                    }
                    Some(ViewChange::Removed(id.clone()))
                } else {
                    // During adoption, the sighting waits for `Completed{AdoptAll}` (`adoption_sighting`).
                    let sighting =
                        state == SessionState::Created && self.adoption == Adoption::Done;
                    let set_exit = |row: &mut Row| {
                        row.state = state;
                        if let SessionState::Exited(exit) = state {
                            row.exit = Some(exit);
                        }
                    };
                    let change = if self.view.contains_key(&id) {
                        self.update_row(&id, set_exit)
                    } else {
                        // A new row: Core's record gives the other fields. A row that is gone has its `Released` next.
                        self.core.get(&id).ok().map(|record| {
                            let mut row = Row::from_record(record);
                            set_exit(&mut row);
                            self.view.insert(id.clone(), Arc::new(row));
                            ViewChange::Row(id.clone())
                        })
                    };
                    // TM-4 is the activity timer; the threshold is per process, so it is set once per session here.
                    if self.thresholds.insert(id.clone()) {
                        self.set_threshold(&id);
                    }
                    if sighting {
                        self.begin_remove(&id);
                    }
                    change
                };
                // R-124: a worker's formats appear with its state (`Launched`, adoption) and leave with its row.
                if let Some(routes) = self.routes.as_ref() {
                    routes.refresh_formats(&self.core, self.view.keys());
                }
                change
            }
            Event::SizeChanged { id, size, .. } => self.update_row(&id, |row| row.size = size),
            Event::MetadataChanged { id, .. } => match self.core.get(&id) {
                Ok(record) => self.update_row(&id, |row| row.labels = record.labels),
                Err(_) => None,
            },
            Event::Activity {
                id,
                source: ActivitySource::Output,
                ..
            } => self.update_row(&id, |row| row.activity = Activity::Active),
            Event::Silent { id, .. } => self.update_row(&id, |row| row.activity = Activity::Idle),
            Event::RouteClosed { .. } | Event::RouteAdopted { .. } => {
                if let Some(routes) = self.routes.as_mut() {
                    routes.event(&event);
                }
                None
            }
            // Other events change no owner state, and the session watches carry only rows.
            _ => None,
        }
    }

    /// Stop step 2 (section 5): every item in the lanes and every waiter is answered `hub_shutdown`; a `Shutdown` request is
    /// answered `Stopping`. A lane item never reached Core (`effect: none`); a waiter's op did (`effect: unknown`).
    /// The caller then drops `Core` on this thread.
    pub fn drain(&mut self) {
        for Item { request, reply } in self.bridge.drain() {
            match request {
                Request::Shutdown => reply.send(Answer::Stopping),
                // HS-9: a typed close whenever the Hub can still write. The lock is still held here.
                Request::RouteConnect(request) => reply.send(route_connect_shutdown(*request)),
                _ => reply.send(shutdown(Effect::None)),
            }
        }
        for (_, waiter) in self.waiters.drain() {
            match waiter {
                Waiter::Reply(reply)
                | Waiter::SpawnCreate { reply, .. }
                | Waiter::SetLabels { reply, .. } => {
                    reply.send(shutdown(Effect::Unknown));
                }
                Waiter::Adopt | Waiter::SpawnStart { .. } | Waiter::SpawnRemove { .. } => {}
            }
        }
        // Queued label writes never reached Core.
        for edit in std::mem::take(&mut self.label_queues)
            .into_values()
            .flatten()
        {
            edit.reply.send(shutdown(Effect::None));
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

/// The answer to a route connection that arrives once the stop has begun (HS-9): a typed close `hub_shutdown` with
/// `lock_released: false`, on the returned stream, whenever the Hub can still write. The lock is still held.
fn route_connect_shutdown(request: crate::client::handoff::RouteConnectRequest) -> Answer {
    Answer::RouteConnect(RouteConnectAnswer::Refused {
        reason: CloseReason::HubShutdown(HubShutdownData {
            lock_released: false,
        }),
        stream: ReturnedStream(Arc::new(request.stream)),
    })
}

fn shutdown(effect: Effect) -> Answer {
    failed(FailKind::HubShutdown, effect)
}

fn failed(kind: FailKind, effect: Effect) -> Answer {
    Answer::Failed(Failure { kind, effect })
}

/// The `StartFailed` reason to the record's reason (design section 4a; R-121 at contracts main 16721c1c, S2-HD-12). The
/// same mapping serves a recorded `start_failure` (A26).
/// - `GuardianFailed` is `worker_failed`: A26 renames it to `WorkerFailed`; the arm serves a Core built before A26.
/// - An unknown reason (the set is non-exhaustive) is `start_failed`, not `internal` (`internal` names a Hub fault,
///   R-120). The caller logs the raw Core value.
fn start_fail_reason(reason: StartFailReason) -> FailReason {
    match reason {
        StartFailReason::ExecFailed { .. } => FailReason::ExecFailed,
        StartFailReason::CwdMissing => FailReason::CwdMissing,
        StartFailReason::StartupTimeout
        | StartFailReason::WorkerFailed
        | StartFailReason::GuardianFailed => FailReason::WorkerFailed,
        _ => FailReason::StartFailed,
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
        fanout: 3,
        baseline_rows: 2,
    };

    const LIMITS: WatchLimits = WatchLimits {
        max_watches: 2,
        chunk_rows: 1,
    };

    struct Rig<C: CoreApi = FakeCore> {
        owner: Owner<C>,
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

    fn rig<C: CoreApi>(core: C) -> Rig<C> {
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
        let caps = crate::bridge::OutboxCaps {
            replies: 16,
            frames: 4,
            watch_queue: 2,
        };
        let outbox = Outbox::new(
            caps,
            phase.clone(),
            Arc::new(Count::default()),
            Arc::new(Count::default()),
        );
        Rig {
            owner: Owner::new(core, bridge.clone(), phase.clone(), tx, COUNTS, LIMITS),
            bridge,
            phase,
            outbox,
            lifecycle: rx,
            clock: Instant::now(),
        }
    }

    impl<C: CoreApi> Rig<C> {
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
                .filter_map(|item| match item {
                    OutItem::Reply { request_id, answer } => Some((request_id, answer)),
                    OutItem::Frame { .. } => None,
                })
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
        spawn_with(name, spawn_request())
    }

    fn spawn_with(name: &str, request: SpawnRequest) -> Request {
        Request::Spawn {
            session: sid(name),
            request: Box::new(request),
        }
    }

    fn session_dir(root: &std::path::Path, name: &str) -> std::path::PathBuf {
        root.join("sessions").join(name)
    }

    fn set_label(key: &str, value: &str) -> Request {
        Request::SetLabels {
            session: sid("a"),
            set: [(key.to_string(), value.to_string())].into(),
            remove: Vec::new(),
        }
    }

    /// R-125: two edits of different keys, queued together, both survive; the writer sends one `UpdateMetadata` at a time
    /// and the second edit starts from the map that the first one wrote.
    #[test]
    fn two_interleaved_label_edits_both_survive() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        rig.answers();
        rig.submit(2, set_label("h/x", "1"));
        rig.submit(3, set_label("h/y", "2"));
        rig.settle(64);
        let answers = rig.answers();
        assert_eq!(answers.len(), 2, "{answers:?}");
        assert!(answers
            .iter()
            .all(|(_, a)| matches!(a, Answer::Op(OpResult::Ok(_)))));
        let labels = &rig.owner.view()[&sid("a")].labels;
        assert_eq!(labels.get("h/x").map(String::as_str), Some("1"));
        assert_eq!(labels.get("h/y").map(String::as_str), Some("2"));
        assert_eq!(rig.owner.core.get(&sid("a")).unwrap().labels, *labels);
        // A remove applies to the latest map.
        rig.submit(
            4,
            Request::SetLabels {
                session: sid("a"),
                set: Default::default(),
                remove: vec!["h/x".into()],
            },
        );
        rig.settle(64);
        rig.answers();
        let labels = &rig.owner.view()[&sid("a")].labels;
        assert!(!labels.contains_key("h/x") && labels.contains_key("h/y"));
    }

    /// A label edit of a session with no row is refused `UnknownSession`.
    #[test]
    fn a_label_edit_of_an_unknown_session_is_refused() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, set_label("h/x", "1"));
        rig.settle(64);
        let answers = rig.answers();
        assert!(
            matches!(&answers[..], [(1, Answer::CoreRefused(e))] if matches!(e.code, ErrorCode::UnknownSession)),
            "{answers:?}"
        );
    }

    /// R-125: the raw `UpdateMetadata` op is not for clients; the label writer is the only writer.
    #[test]
    fn a_raw_update_metadata_is_refused_internal_with_no_effect() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        rig.answers();
        rig.submit(
            2,
            Request::Op(Box::new(Op::UpdateMetadata {
                id: sid("a"),
                labels: [("k".to_string(), "v".to_string())].into(),
            })),
        );
        rig.settle(64);
        let answers = rig.answers();
        assert!(
            matches!(&answers[..], [(2, Answer::Failed(f))]
                if f.kind == FailKind::Internal && f.effect == Effect::None),
            "{answers:?}"
        );
        assert!(rig.owner.view()[&sid("a")].labels.is_empty());
    }

    /// The row follows `Activity{Output}` (active), `Silent` (idle) and `SizeChanged`; other sources change nothing.
    #[test]
    fn the_row_follows_activity_silence_and_size() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        rig.answers();
        let id = sid("a");
        let instance = InstanceId("i".into());
        assert_eq!(rig.owner.view()[&id].activity, Activity::Idle);
        let change = rig.owner.apply(Event::Activity {
            id: id.clone(),
            instance: instance.clone(),
            source: ActivitySource::Output,
            at: 1,
        });
        assert!(matches!(change, Some(ViewChange::Row(_))));
        assert_eq!(rig.owner.view()[&id].activity, Activity::Active);
        let again = rig.owner.apply(Event::Activity {
            id: id.clone(),
            instance: instance.clone(),
            source: ActivitySource::Output,
            at: 2,
        });
        assert!(again.is_none(), "an unchanged row is no delta");
        let change = rig.owner.apply(Event::Silent {
            id: id.clone(),
            instance,
            since: 3,
        });
        assert!(matches!(change, Some(ViewChange::Row(_))));
        assert_eq!(rig.owner.view()[&id].activity, Activity::Idle);
        let size = Size {
            rows: 30,
            cols: 100,
            cell_px: None,
        };
        // The fake posts `SizeChanged` only for a started session (SZ-3), so the event is applied directly.
        let change = rig.owner.apply(Event::SizeChanged {
            id: id.clone(),
            instance: InstanceId("i".into()),
            size,
        });
        assert!(matches!(change, Some(ViewChange::Row(_))));
        assert_eq!(rig.owner.view()[&id].size, size);
    }

    type ThresholdCalls = Arc<std::sync::Mutex<Vec<(SessionId, Option<Duration>)>>>;

    /// A [`FakeCore`] that records every `set_silence_threshold` call (the pinned fake keeps no threshold).
    struct Recording {
        inner: FakeCore,
        calls: ThresholdCalls,
    }

    impl CoreApi for Recording {
        fn begin(&mut self, op: Op) -> Result<OpId, CoreError> {
            self.inner.begin(op)
        }
        fn pump(&mut self, now: Now) -> PumpReport {
            self.inner.pump(now)
        }
        fn poll_events(&mut self, max: usize) -> Vec<Event> {
            self.inner.poll_events(max)
        }
        fn wake_handle(&self) -> Arc<dyn WakeHandle> {
            self.inner.wake_handle()
        }
        fn next_deadline(&self) -> Option<Instant> {
            self.inner.next_deadline()
        }
        fn cancel(&mut self, op: OpId) -> CancelResult {
            self.inner.cancel(op)
        }
        fn get(&self, id: &SessionId) -> Result<SessionRecord, CoreError> {
            self.inner.get(id)
        }
        fn list(&self) -> Vec<SessionRecord> {
            self.inner.list()
        }
        fn status(&self) -> Status {
            self.inner.status()
        }
        fn diagnostics(&self) -> serde_json::Value {
            self.inner.diagnostics()
        }
        fn terminal_state(&self, id: &SessionId) -> Result<TerminalState, CoreError> {
            self.inner.terminal_state(id)
        }
        fn read_page(&self, capture: CaptureId, page: u32) -> Result<Page, CoreError> {
            self.inner.read_page(capture, page)
        }
        fn release(&mut self, capture: CaptureId) {
            self.inner.release(capture)
        }
        fn release_owner(&mut self, client: &ClientId) {
            self.inner.release_owner(client)
        }
        fn snapshot_formats(&self, session: &SessionId) -> Result<Vec<SnapshotFormat>, CoreError> {
            self.inner.snapshot_formats(session)
        }
        fn shadow_answerable_kinds(&self) -> Vec<botster_core_route_codec::prelude::QueryKind> {
            self.inner.shadow_answerable_kinds()
        }
        fn attach(
            &mut self,
            client: ClientId,
            session: SessionId,
            transport: RouteTransport,
            options: AttachOptions,
        ) -> Result<AttachResult, AttachRefused> {
            self.inner.attach(client, session, transport, options)
        }
        fn tap_read(&mut self, session: &SessionId, max: usize) -> Result<TapChunk, CoreError> {
            self.inner.tap_read(session, max)
        }
        fn set_silence_threshold(
            &mut self,
            session: &SessionId,
            threshold: Option<Duration>,
        ) -> Result<(), CoreError> {
            self.calls
                .lock()
                .unwrap()
                .push((session.clone(), threshold));
            self.inner.set_silence_threshold(session, threshold)
        }
        fn service_send(
            &mut self,
            id: &ServiceId,
            lane: u8,
            frame: &OutboundFrame,
        ) -> Result<(), SendError> {
            self.inner.service_send(id, lane, frame)
        }
        fn service_recv(&mut self, id: &ServiceId, lane: u8) -> Result<Option<Frame>, RecvError> {
            self.inner.service_recv(id, lane)
        }
        fn service_report(&self, id: &ServiceId) -> Result<SpawnReport, CoreError> {
            self.inner.service_report(id)
        }
        fn service_log_tail(&self, id: &ServiceId, max: usize) -> Result<Vec<u8>, CoreError> {
            self.inner.service_log_tail(id, max)
        }
        fn features(&self) -> Features {
            self.inner.features()
        }
        fn limits(&self) -> CoreLimits {
            self.inner.limits()
        }
        fn worker_protocol(&self) -> u8 {
            self.inner.worker_protocol()
        }
        fn adoptable_worker_protocols(&self) -> BTreeSet<u8> {
            self.inner.adoptable_worker_protocols()
        }
        fn worker_protocol_compatibility(&self, protocol: Option<u8>) -> WorkerCompatibility {
            self.inner.worker_protocol_compatibility(protocol)
        }
        fn terminal_identity(&self) -> TerminalIdentity {
            self.inner.terminal_identity()
        }
    }

    /// TM-4: the threshold call carries `activity_idle`, it is made once per session (a second state event makes no
    /// call), a changed value reaches every tracked session, and an adoption sighting sets it for a recovered row.
    #[test]
    fn the_silence_threshold_is_set_once_per_session() {
        let store = FakeStore::new();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        // A session that exists before the owner adopts: the sighting of `AdoptAll`.
        let mut seed = rig(open(&store));
        seed.adopt();
        seed.submit(1, create("old"));
        seed.settle(64);
        seed.answers();
        drop(seed);
        let mut rig = rig(Recording {
            inner: open(&store),
            calls: calls.clone(),
        });
        let seen = |calls: &Arc<std::sync::Mutex<Vec<_>>>| calls.lock().unwrap().clone();
        let idle = Some(ACTIVITY_IDLE);
        rig.adopt();
        let sightings = seen(&calls);
        assert!(sightings.iter().all(|(_, t)| *t == idle), "{sightings:?}");
        // The recovered row `old` got its threshold at the sighting of `AdoptAll`.
        assert_eq!(
            sightings
                .iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>(),
            vec![sid("old")]
        );
        rig.submit(2, create("a"));
        rig.settle(64);
        rig.answers();
        let after_create = seen(&calls);
        let for_a = |calls: &[(SessionId, Option<Duration>)]| {
            calls.iter().filter(|(id, _)| *id == sid("a")).count()
        };
        assert_eq!(for_a(&after_create), 1, "{after_create:?}");
        // Another state event of the same session makes no second call.
        rig.owner.apply(Event::SessionState {
            id: sid("a"),
            instance: InstanceId("i".into()),
            state: SessionState::Starting,
        });
        assert_eq!(for_a(&seen(&calls)), 1);
        // A live change sets the new value for every tracked session, and an unchanged value sets nothing.
        let before = seen(&calls).len();
        let tracked = rig.owner.thresholds.len();
        rig.owner.set_activity_idle(ACTIVITY_IDLE);
        assert_eq!(seen(&calls).len(), before);
        let new = Duration::from_millis(500);
        rig.owner.set_activity_idle(new);
        let all = seen(&calls);
        let fresh = &all[before..];
        assert_eq!(fresh.len(), tracked, "{fresh:?}");
        assert!(fresh.iter().all(|(_, t)| *t == Some(new)));
        assert_eq!(tracked, 2);
        // `Released` forgets the session: a later change does not touch it.
        rig.owner.apply(Event::SessionState {
            id: sid("a"),
            instance: InstanceId("i".into()),
            state: SessionState::Released,
        });
        let before = seen(&calls).len();
        rig.owner.set_activity_idle(Duration::from_millis(900));
        assert!(seen(&calls)[before..].iter().all(|(id, _)| *id != sid("a")));
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
            rig.owner.view().get(&sid("a")).map(|row| row.state),
            Some(SessionState::Created)
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
        // The requests behind the Shutdown never reach Core (even a read): all are answered `hub_shutdown`, effect none.
        for later in &answers[2..] {
            assert_eq!(later.1, shutdown(Effect::None), "{later:?}");
        }
        assert_eq!(rig.outbox.reserved(), 0);
        assert!(rig.lifecycle.try_recv().is_err());
    }

    /// A mutation queued behind the `Shutdown` in the same turn is not admitted to Core: no waiter, no pump, and the answer is
    /// `hub_shutdown` with `effect: none`.
    #[test]
    fn a_mutation_behind_the_shutdown_is_not_admitted() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, Request::Shutdown);
        rig.submit(2, create("a"));
        let report = rig.turn();
        assert_eq!(rig.phase.get(), Phase::Stopping);
        assert_eq!(rig.owner.waiters(), 0, "the mutation reached Core");
        assert!(!report.runnable, "Core is pumped no more after the stop");
        assert!(rig.owner.view().is_empty());
        let answers = rig.answers();
        assert_eq!(
            answers,
            vec![(
                2,
                Answer::Failed(Failure {
                    kind: FailKind::HubShutdown,
                    effect: Effect::None,
                })
            )]
        );
        rig.owner.drain();
        assert_eq!(rig.answers(), vec![(1, Answer::Stopping)]);
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

    use crate::bridge::OutboxCaps;
    use crate::watch::{Row, WatchFilter, WatchFrame, WatchKey};

    /// Creates sessions through the owner, so that the view holds rows.
    fn with_sessions(rig: &mut Rig, names: &[&str]) {
        // One at a time: the rig's request lane holds 4.
        for (n, name) in names.iter().enumerate() {
            rig.submit(1000 + n as u64, create(name));
            rig.settle(256);
        }
        rig.answers();
        assert_eq!(rig.owner.view().len(), names.len());
    }

    fn conn(rig: &Rig, frames: usize, watch_queue: usize) -> (Arc<Outbox>, Arc<Count>) {
        let owner_wakes = Arc::new(Count::default());
        let caps = OutboxCaps {
            replies: 8,
            frames,
            watch_queue,
        };
        let outbox = Outbox::new(
            caps,
            rig.phase.clone(),
            Arc::new(Count::default()),
            owner_wakes.clone(),
        );
        (outbox, owner_wakes)
    }

    fn watch(rig: &Rig, outbox: &Arc<Outbox>, id: u64, key: WatchKey, filter: WatchFilter) {
        let reply = outbox.reserve(id).unwrap();
        rig.bridge
            .submit(
                Request::Watch {
                    key,
                    filter,
                    outbox: outbox.clone(),
                },
                reply,
            )
            .unwrap();
    }

    /// The frames of `watch` in `items`, in order, with the replies kept as `None`.
    fn frames(items: &[OutItem]) -> Vec<Option<WatchFrame>> {
        items
            .iter()
            .map(|item| match item {
                OutItem::Frame { frame, .. } => Some(frame.clone()),
                OutItem::Reply { .. } => None,
            })
            .collect()
    }

    fn ids(rows: &[Arc<Row>]) -> Vec<String> {
        rows.iter().map(|r| r.id.0.clone()).collect()
    }

    const KEY: WatchKey = WatchKey { conn: 1, watch: 7 };

    /// HC WT-1: the reply, then the baseline of the current rows in chunks, then `snapshot_end`; a later change arrives as a
    /// delta after `snapshot_end`.
    #[test]
    fn wt_1_baseline_then_deltas() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        with_sessions(&mut rig, &["a", "b"]);
        let (outbox, _) = conn(&rig, 8, 8);
        watch(&rig, &outbox, 1, KEY, WatchFilter::default());
        rig.settle(64);
        let got = frames(&outbox.take(64));
        assert_eq!(got[0], None, "the reply first");
        let mut rows = Vec::new();
        for frame in &got[1..got.len() - 1] {
            match frame {
                Some(WatchFrame::SnapshotChunk(chunk)) => rows.extend(ids(chunk)),
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(rows, vec!["a", "b"]);
        assert_eq!(got.last().unwrap(), &Some(WatchFrame::SnapshotEnd));
        rig.submit(5, create("c"));
        rig.settle(64);
        let got = frames(&outbox.take(64));
        assert!(
            got.iter()
                .any(|f| matches!(f, Some(WatchFrame::Delta(row)) if row.id == sid("c"))),
            "{got:?}"
        );
    }

    /// A filter sees only its session.
    #[test]
    fn filter_limits_rows_and_deltas() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        with_sessions(&mut rig, &["a", "b"]);
        let (outbox, _) = conn(&rig, 8, 8);
        let filter = WatchFilter {
            session: Some(sid("b")),
        };
        watch(&rig, &outbox, 1, KEY, filter);
        rig.settle(64);
        let got = frames(&outbox.take(64));
        assert_eq!(
            got[1],
            Some(WatchFrame::SnapshotChunk(vec![
                rig.owner.view()[&sid("b")].clone()
            ]))
        );
        rig.submit(5, Request::Op(Box::new(Op::Remove { id: sid("a") })));
        rig.settle(64);
        assert!(frames(&outbox.take(64)).iter().all(Option::is_none));
        rig.submit(6, Request::Op(Box::new(Op::Remove { id: sid("b") })));
        rig.settle(64);
        assert!(frames(&outbox.take(64)).contains(&Some(WatchFrame::Removed(sid("b")))));
    }

    /// The capacity wake: a full outbox blocks the baseline, the owner is not runnable on it (no spin), and the drain by
    /// `io` wakes the owner once; then the baseline finishes.
    #[test]
    fn capacity_wake_resumes_a_blocked_baseline() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        with_sessions(&mut rig, &["a", "b", "c", "d", "e"]);
        // Two frames of room; one row per chunk.
        let (outbox, wakes) = conn(&rig, 2, 8);
        watch(&rig, &outbox, 1, KEY, WatchFilter::default());
        rig.settle(64);
        assert!(outbox.wants_room());
        assert!(
            !rig.turn().runnable,
            "a blocked baseline does not keep the owner running"
        );
        assert_eq!(wakes.get(), 0);
        let mut rows = Vec::new();
        let mut end = false;
        for _ in 0..16 {
            for frame in frames(&outbox.take(64)).into_iter().flatten() {
                match frame {
                    WatchFrame::SnapshotChunk(chunk) => rows.extend(ids(&chunk)),
                    WatchFrame::SnapshotEnd => end = true,
                    other => panic!("{other:?}"),
                }
            }
            if end {
                break;
            }
            rig.settle(64);
        }
        assert!(end);
        assert_eq!(rows, vec!["a", "b", "c", "d", "e"]);
        assert!(wakes.get() >= 1, "each drain below half woke the owner");
    }

    /// The arm-then-recheck under one lock: `io` drains while the owner sees no room; the owner's own recheck then finds
    /// room, so no wake is lost.
    #[test]
    fn capacity_wake_is_not_lost() {
        let rig = rig(open(&FakeStore::new()));
        let (outbox, wakes) = conn(&rig, 4, 8);
        for _ in 0..4 {
            assert!(outbox.push_frame(1, WatchFrame::SnapshotEnd));
        }
        assert!(!outbox.push_frame(1, WatchFrame::SnapshotEnd));
        assert!(outbox.wants_room());
        outbox.take(1);
        assert!(outbox.wants_room(), "3 frames is above half: no wake yet");
        assert_eq!(wakes.get(), 0);
        outbox.take(1);
        assert!(!outbox.wants_room(), "2 frames is half: the wake");
        assert_eq!(wakes.get(), 1);
        outbox.take(1);
        assert_eq!(wakes.get(), 1, "one wake per crossing");
        assert!(outbox.push_frame(1, WatchFrame::SnapshotEnd));
    }

    /// HC WT-4: a watch queue overflow drops the queue, and a `Resync` and a new baseline follow.
    #[test]
    fn wt_4_overflow_resyncs() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        let (outbox, _) = conn(&rig, 8, 1);
        watch(&rig, &outbox, 1, KEY, WatchFilter::default());
        rig.settle(64);
        outbox.take(64);
        // Two sessions change before `io` takes anything: the queue of 1 overflows.
        rig.submit(5, create("x"));
        rig.submit(6, create("y"));
        rig.settle(128);
        let got: Vec<WatchFrame> = frames(&outbox.take(64)).into_iter().flatten().collect();
        let resync = got
            .iter()
            .position(|f| *f == WatchFrame::Resync)
            .expect("a resync");
        let end = got
            .iter()
            .rposition(|f| *f == WatchFrame::SnapshotEnd)
            .unwrap();
        assert!(resync < end);
        let rows: Vec<String> = got[resync..end]
            .iter()
            .filter_map(|f| match f {
                WatchFrame::SnapshotChunk(chunk) => Some(ids(chunk)),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(rows, vec!["x", "y"], "the new baseline holds both rows");
    }

    /// HC WT-6: after `unwatch`, every frame of the watch is ahead of the reply, and no frame follows it.
    #[test]
    fn wt_6_unwatch_reply_is_last() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        with_sessions(&mut rig, &["a", "b", "c"]);
        let (outbox, _) = conn(&rig, 8, 8);
        watch(&rig, &outbox, 1, KEY, WatchFilter::default());
        rig.turn();
        rig.bridge
            .submit(Request::Unwatch(KEY), outbox.reserve(2).unwrap())
            .unwrap();
        rig.submit(9, create("d"));
        rig.settle(64);
        let items = outbox.take(64);
        let unwatched = items
            .iter()
            .position(|i| matches!(i, OutItem::Reply { request_id: 2, .. }))
            .unwrap();
        assert!(items[unwatched + 1..]
            .iter()
            .all(|i| !matches!(i, OutItem::Frame { watch: 7, .. })));
        assert!(rig.owner.watches().is_empty());
    }

    /// HC BD-1: the per-connection watch bound refuses typed, with `effect: none`.
    #[test]
    fn bd_1_watch_bound() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        let (outbox, _) = conn(&rig, 8, 8);
        for w in 0..3 {
            watch(
                &rig,
                &outbox,
                w,
                WatchKey { conn: 1, watch: w },
                WatchFilter::default(),
            );
        }
        rig.settle(64);
        let replies: Vec<Answer> = outbox
            .take(64)
            .into_iter()
            .filter_map(|i| match i {
                OutItem::Reply { answer, .. } => Some(answer),
                OutItem::Frame { .. } => None,
            })
            .collect();
        assert_eq!(
            replies,
            vec![
                Answer::Watching,
                Answer::Watching,
                Answer::Failed(Failure {
                    kind: FailKind::TooManyWatches,
                    effect: Effect::None
                })
            ]
        );
    }

    /// HC HS-9: a closed connection's watches end with it.
    #[test]
    fn hs_9_disconnect_releases_watches() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        let (outbox, _) = conn(&rig, 8, 8);
        watch(&rig, &outbox, 1, KEY, WatchFilter::default());
        rig.settle(64);
        assert_eq!(rig.owner.watches().len(), 1);
        outbox.close();
        rig.turn();
        assert!(rig.owner.watches().is_empty());
    }

    /// Section 2: a turn's fan-out work is bounded by `fanout`, whatever the number of watches; the continuation finishes
    /// the batch over later turns.
    #[test]
    fn fanout_is_counted() {
        let mut rig = rig(open(&FakeStore::new()));
        rig.adopt();
        let (outbox, _) = conn(&rig, 64, 64);
        for w in 0..2 {
            watch(
                &rig,
                &outbox,
                w,
                WatchKey { conn: 1, watch: w },
                WatchFilter::default(),
            );
        }
        let (other, _) = conn(&rig, 64, 64);
        for w in 0..2 {
            watch(
                &rig,
                &other,
                10 + w,
                WatchKey { conn: 2, watch: w },
                WatchFilter::default(),
            );
        }
        rig.settle(64);
        assert_eq!(rig.owner.watches().len(), 4);
        rig.submit(5, create("a"));
        // Turn until the fan-out of the batch with the `SessionState` event starts, then count turns until it ends.
        let mut pending_turns = 0;
        for _ in 0..64 {
            rig.turn();
            if rig.owner.fanout_pending() {
                pending_turns += 1;
            } else if pending_turns > 0 {
                break;
            }
        }
        // 3 units per turn: an event of 4 watches needs 5 units, so the fan-out spans more than one turn.
        assert!(pending_turns >= 2, "{pending_turns}");
        rig.settle(64);
        let deltas = |o: &Arc<Outbox>| {
            frames(&o.take(64))
                .into_iter()
                .flatten()
                .filter(|f| matches!(f, WatchFrame::Delta(_)))
                .count()
        };
        assert!(deltas(&outbox) >= 2);
        assert!(deltas(&other) >= 2);
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
        // The session directory exists before `accepted` (design section 4a, "The session directory").
        for dir in [
            session_dir(tmp.path(), "a"),
            session_dir(tmp.path(), "a").join("uploads"),
        ] {
            let meta = std::fs::symlink_metadata(&dir).unwrap();
            assert!(meta.is_dir(), "{dir:?}");
            assert_eq!(
                std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o777,
                0o700
            );
        }
        stop(rig);
    }

    /// The session directory: a `Released` row's directory is deleted by a store job.
    #[test]
    fn a_released_row_deletes_its_session_directory() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        rig.owner.store_mut().unwrap().end_recovery();
        fail_next(
            rig.owner.core(),
            "Start",
            ErrorCode::StartFailed(StartFailReason::CwdMissing),
        );
        rig.submit(1, spawn("a"));
        flow_settle(&mut rig);
        rig.answers();
        assert!(session_dir(tmp.path(), "a").is_dir());
        rig.submit(2, Request::Op(Box::new(Op::Remove { id: sid("a") })));
        flow_settle(&mut rig);
        assert!(
            matches!(&rig.answers()[..], [(2, Answer::Op(OpResult::Ok(_)))]),
            "Remove completed"
        );
        assert!(!rig.owner.view().contains_key(&sid("a")));
        assert!(!session_dir(tmp.path(), "a").exists());
        assert_eq!(rig.owner.store().unwrap().dir_deletes(), 0);
        stop(rig);
    }

    /// A session id that cannot name a directory is an owner bug: logged, `internal`, `effect: none`, nothing written.
    #[test]
    fn a_session_id_that_is_no_path_component_is_internal() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        for (id, name) in [(1, "../x"), (2, ".."), (3, "")] {
            rig.submit(id, spawn(name));
            flow_settle(&mut rig);
            assert_eq!(
                rig.answers(),
                vec![(id, failed(FailKind::Internal, Effect::None))]
            );
        }
        assert_eq!(rig.owner.spawn_faults(), 3);
        assert_eq!(records(tmp.path()), 0);
        assert!(!tmp.path().join("x").exists());
        stop(rig);
    }

    /// R-122 U1: a sync `Create` refusal deletes the record durably, then answers the error with `effect: none`.
    #[test]
    fn u1_sync_refusal_forgets_then_answers() {
        let tmp = short_root();
        let mut rig = store_rig(tmp.path(), &FakeStore::new());
        rig.adopt();
        let mut request = spawn_request();
        request.argv.clear();
        rig.submit(1, spawn_with("a", request));
        flow_settle(&mut rig);
        let answers = rig.answers();
        assert!(
            matches!(
                &answers[..],
                [(1, Answer::Spawn(SpawnAnswer::Refused { error, effect: Effect::None }))]
                    if matches!(error.code, ErrorCode::InvalidInput { .. })
            ),
            "{answers:?}"
        );
        assert!(rig.owner.store().unwrap().is_empty());
        assert_eq!(records(tmp.path()), 0);
        // The forget deletes the session directory after the record.
        assert!(!session_dir(tmp.path(), "a").exists());
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
            (StartFailReason::GuardianFailed, FailReason::WorkerFailed),
            (StartFailReason::Other, FailReason::StartFailed),
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

    /// A write that fails while a requested stop quiesces the store is a store failure: the flag is set by the drain, which
    /// `run_owner` carries into the owner's end (`daemon::owner_end`).
    #[test]
    fn a_write_failure_during_the_stop_quiesce_sets_store_failed() {
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
        // A requested stop: no fault has been seen yet.
        rig.phase.advance(Phase::Running, Phase::Stopping);
        assert!(!rig.owner.store_failed());
        rig.owner.drain();
        assert!(rig.owner.store_failed());
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

    // The route handoff through the bridge (client protocol design, sections 2.2, 2.3 and 2.5).

    const WINDOW: Duration = Duration::from_secs(10);

    fn route_params() -> RouteParams {
        RouteParams {
            max_reservations: 4,
            connect_window: WINDOW,
            query_deadline: Duration::from_millis(200),
            sessions_root: "/data/sessions".into(),
            pending_ends: 8,
        }
    }

    fn with_routes(rig: Rig) -> Rig {
        Rig {
            owner: rig.owner.with_routes(route_params()),
            ..rig
        }
    }

    fn operator() -> crate::client::handoff::Caller {
        crate::client::handoff::Caller { uid: 501 }
    }

    fn reserve_route(name: &str) -> Request {
        Request::ReserveRoute(Box::new(crate::client::handoff::ReserveRequest {
            caller: operator(),
            session: sid(name),
            terminal_formats: vec!["ghostsnp/1+raw".into()],
            history: None,
            answers_queries: true,
            route_features: vec![],
        }))
    }

    fn route_connect(
        route_id: &str,
        route_token: &str,
    ) -> (Request, std::os::unix::net::UnixStream) {
        let (hub_end, client_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let request =
            Request::RouteConnect(Box::new(crate::client::handoff::RouteConnectRequest {
                route_id: route_id.into(),
                route_token: route_token.into(),
                caller: operator(),
                stream: hub_end,
            }));
        (request, client_end)
    }

    /// A rig with route tables, adopted, with session `a` started.
    fn route_rig() -> Rig {
        let store = FakeStore::new();
        let mut rig = with_routes(rig(open(&store)));
        rig.adopt();
        rig.submit(1, create("a"));
        rig.settle(64);
        rig.submit(2, Request::Op(Box::new(Op::Start { id: sid("a") })));
        rig.settle(64);
        rig.answers();
        rig
    }

    fn reserved(rig: &mut Rig, id: u64) -> crate::client::handoff::Reservation {
        rig.submit(id, reserve_route("a"));
        rig.settle(8);
        match rig.answers().pop() {
            Some((got, Answer::RouteReserved(r))) if got == id => r,
            other => panic!("{other:?}"),
        }
    }

    /// R-124: the owner refreshes the common formats on each session change; a row's formats leave with it. The fake
    /// Core announces `GHOSTSNP/1` for every session it knows, so the starting-session rule (an empty
    /// `snapshot_formats` is left out) is proved in `client::formats` tests, not here.
    #[test]
    fn the_owner_refreshes_the_welcome_format_as_sessions_come_and_go() {
        let wanted = vec!["ghostsnp/2+raw".to_string(), "ghostsnp/1+raw".to_string()];
        let store = FakeStore::new();
        let mut rig = with_routes(rig(open(&store)));
        rig.adopt();
        let formats = rig.owner.routes().unwrap().formats();
        assert_eq!(formats.pick(&wanted), None, "no session, no format");
        rig.submit(1, create("a"));
        rig.settle(64);
        assert_eq!(formats.pick(&wanted), Some("ghostsnp/1+raw".into()));
        rig.submit(2, Request::Op(Box::new(Op::Remove { id: sid("a") })));
        rig.settle(64);
        assert!(rig.owner.view().is_empty());
        assert_eq!(
            formats.pick(&wanted),
            None,
            "the removed row's formats left"
        );
    }

    #[test]
    fn a_route_is_reserved_and_connected_through_the_bridge() {
        let mut rig = route_rig();
        let r = reserved(&mut rig, 3);
        let (request, _client) = route_connect(&r.route_id, &r.route_token);
        rig.submit(4, request);
        rig.settle(8);
        assert_eq!(
            rig.answers(),
            vec![(4, Answer::RouteConnect(RouteConnectAnswer::Handed))]
        );
        let routes = rig.owner.routes().unwrap();
        assert_eq!(routes.tables.route(&r.route_id).unwrap().session, sid("a"));
    }

    #[test]
    fn a_reservation_bounds_the_wait_and_ends_not_connected_at_its_deadline() {
        let mut rig = route_rig();
        let before = rig.clock;
        let r = reserved(&mut rig, 3);
        let deadline = rig.owner.next_deadline().unwrap();
        assert!(
            deadline > before && deadline <= rig.clock + WINDOW,
            "{deadline:?}"
        );
        rig.clock = deadline;
        rig.turn();
        let routes = rig.owner.routes_mut().unwrap();
        let (ends, lost) = routes.take_ends();
        assert_eq!(lost, 0);
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0].route_id, r.route_id);
        assert_eq!(
            ends[0].reason,
            botster_route_codec::prelude::RouteEndReason::NotConnected
        );
        assert_eq!(routes.next_deadline(), None);
    }

    #[test]
    fn the_stop_drain_answers_a_queued_connect_with_a_typed_hub_shutdown_close() {
        let mut rig = route_rig();
        let r = reserved(&mut rig, 3);
        let (request, _client) = route_connect(&r.route_id, &r.route_token);
        rig.submit(4, request);
        rig.owner.drain();
        match rig.answers().as_slice() {
            [(4, Answer::RouteConnect(RouteConnectAnswer::Refused { reason, .. }))] => assert_eq!(
                *reason,
                CloseReason::HubShutdown(HubShutdownData {
                    lock_released: false
                })
            ),
            other => panic!("{other:?}"),
        }
    }

    /// A route connection that is admitted in the same turn as a `Shutdown`, behind it, gets the typed close with
    /// `lock_released: false` and keeps its stream, not a generic failure (HS-9).
    #[test]
    fn a_route_connect_behind_a_shutdown_gets_the_typed_close() {
        let mut rig = route_rig();
        rig.submit(5, Request::Shutdown);
        let (request, _client) = route_connect("r", "t");
        rig.submit(6, request);
        rig.turn();
        assert_eq!(rig.phase.get(), Phase::Stopping);
        let answers = rig.answers();
        assert!(
            matches!(
                answers.as_slice(),
                [(
                    6,
                    Answer::RouteConnect(RouteConnectAnswer::Refused {
                        reason: CloseReason::HubShutdown(HubShutdownData {
                            lock_released: false
                        }),
                        ..
                    })
                )]
            ),
            "{answers:?}"
        );
    }

    /// The stop can begin from another thread (a signal, an io failure) after a request left its lane and before it is
    /// dispatched: the route connection still gets the typed close.
    #[test]
    fn a_route_connect_admitted_before_a_stop_from_elsewhere_gets_the_typed_close() {
        let mut rig = route_rig();
        let (request, _client) = route_connect("r", "t");
        rig.submit(6, request);
        rig.phase.advance(Phase::Running, Phase::Stopping);
        rig.turn();
        let answers = rig.answers();
        assert!(
            matches!(
                answers.as_slice(),
                [(
                    6,
                    Answer::RouteConnect(RouteConnectAnswer::Refused {
                        reason: CloseReason::HubShutdown(HubShutdownData {
                            lock_released: false
                        }),
                        ..
                    })
                )]
            ),
            "{answers:?}"
        );
    }

    #[test]
    fn an_owner_without_route_tables_refuses_routes() {
        let store = FakeStore::new();
        let mut rig = rig(open(&store));
        rig.adopt();
        rig.submit(1, reserve_route("a"));
        rig.settle(8);
        assert_eq!(
            rig.answers(),
            vec![(1, failed(FailKind::Internal, Effect::None))]
        );
        let (request, _client) = route_connect("r", "t");
        rig.submit(2, request);
        rig.settle(8);
        assert!(matches!(
            rig.answers().as_slice(),
            [(
                2,
                Answer::RouteConnect(RouteConnectAnswer::Refused {
                    reason: CloseReason::RouteTokenInvalid,
                    ..
                })
            )]
        ));
    }
}
