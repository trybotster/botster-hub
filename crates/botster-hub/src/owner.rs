//! The owner of the embedded Core (design sections 2 and 4): the only thread that touches `Core`. It owns the waiter map and
//! the session view, admits requests from the bridge, and turns Core with counted work per turn (no wall-clock budget).
//!
//! The owner is generic over `CoreApi`, so its unit tests run on the contracts' fake Core with no I/O, one turn at a time.
//! [`run`] is the thread's loop over `mio`; it runs only on the real Core (the fake's wake has no descriptor).

use crate::bridge::{Answer, Bridge, Effect, FailKind, Failure, Item, ReplyTo, Request};
use crate::phase::{request_stop, Ended, Lifecycle, SharedPhase, StopCause};
use botster_core::contract::prelude::{
    CoreApi, CoreError, Event, Now, Op, OpId, OpResult, SessionId, SessionState,
};
use std::collections::{BTreeMap, HashMap};
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
        }
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
        let events = self.core.poll_events(self.counts.events.max(1));
        // A non-empty poll sets `events_left`; only an empty poll clears it (Core `poll_events` promises "at most max").
        self.events_left = !events.is_empty();
        for event in events {
            self.apply(event);
        }
        TurnReport {
            runnable: pump.more || self.events_left || !self.bridge.is_empty(),
        }
    }

    fn dispatch(&mut self, item: Item) {
        let Item { request, mut reply } = item;
        // After a `Shutdown` (or any stop cause) no later request of the turn reaches Core: it is answered `hub_shutdown`,
        // effect none. A repeated `Shutdown` is still answered `Stopping`.
        if self.phase.stopping() && !matches!(request, Request::Shutdown) {
            reply.send(shutdown(Effect::None));
            return;
        }
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
        }
    }

    fn apply(&mut self, event: Event) {
        match event {
            Event::Completed { op, result } => match self.waiters.remove(&op) {
                Some(Waiter::Reply(reply)) => reply.send(Answer::Op(result)),
                Some(Waiter::Adopt) => match result {
                    OpResult::Ok(_) => {
                        self.adoption = Adoption::Done;
                        self.bridge.set_ready();
                    }
                    OpResult::Err(error) => self.adoption = Adoption::Failed(error_text(&error)),
                    // `OpResult` is non-exhaustive; an unknown result is not a proof of adoption.
                    _ => self.adoption = Adoption::Failed("unknown adoption result".into()),
                },
                // A `Completed` after the stop took the waiters, or of an op with no waiter: dropped.
                None => {}
            },
            Event::SessionState { id, state, .. } => {
                if state == SessionState::Released {
                    self.view.remove(&id);
                } else {
                    self.view.insert(id, state);
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
            if let Waiter::Reply(reply) = waiter {
                reply.send(shutdown(Effect::Unknown));
            }
        }
        for reply in self.shutdowns.drain(..) {
            reply.send(Answer::Stopping);
        }
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
    Answer::Failed(Failure {
        kind: FailKind::HubShutdown,
        effect,
    })
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

    fn create(name: &str) -> Request {
        Request::Op(Box::new(Op::Create {
            session: sid(name),
            request: SpawnRequest {
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
            },
        }))
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
