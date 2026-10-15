//! The bridge from `io` to the owner (design section 3) and the reply token (section 3a).
//!
//! - Bounded lanes: `Control`, `Request`, `Bulk`. A full lane refuses at once (`Backpressured`); nothing blocks.
//! - Only `Bulk` coalesces, by key. No client request is coalesced: every client request gets its own answer (HC EN-3).
//! - `submit` returns admission only. The real answer reaches the [`ReplyTo`] token later, exactly once.
//! - No domain verbs: [`Request`] mirrors Core's ops and reads, plus `Shutdown` and the spawn operation (design section
//!   4a, "Spawn flow").

use crate::phase::{SharedPhase, Wake};
use crate::store::Lookup;
use botster_core::contract::prelude::{
    CancelResult, CoreError, Op, OpId, OpResult, SessionId, SessionRecord, SpawnRequest,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// The lanes of the bridge, in admission order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Control,
    Request,
    Bulk,
}

/// A request: Core's ops and reads, plus `Shutdown` and the spawn operation.
#[derive(Debug)]
pub enum Request {
    /// A spawn operation (R-122): a durable `starting` record, then `Create`, then `Start`. Boxed like `Op`.
    Spawn {
        session: SessionId,
        request: Box<SpawnRequest>,
    },
    /// A read of a spawn operation record (design section 4a, API 4).
    GetSpawn(String),
    /// A Core op (`begin`). Boxed: an `Op` is large, and a request moves through the lanes and back in `Rejected`.
    Op(Box<Op>),
    /// A sync read of one row.
    Get(SessionId),
    /// A sync read of every row.
    List,
    /// `cancel(op)`.
    Cancel(OpId),
    /// Starts the stop. No Core call.
    Shutdown,
}

impl Request {
    /// The lane of a client request. Internal work uses [`Bridge::submit_internal`] and the `Bulk` lane.
    pub fn lane(&self) -> Lane {
        match self {
            Request::Shutdown => Lane::Control,
            _ => Lane::Request,
        }
    }

    /// Whether the request reads or changes sessions, so that it waits for adoption (`NotReady`; HP SS-3).
    fn needs_sessions(&self) -> bool {
        !matches!(self, Request::Shutdown)
    }
}

/// Why `submit` refused a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The lane is full. Retryable (HC `busy`, HP `backpressured`).
    Backpressured { lane: Lane },
    /// Adoption has not completed. Retryable (HP SS-3 `unavailable`).
    NotReady,
    /// The stop has started (HC `hub_shutdown`).
    ShuttingDown,
}

impl Refused {
    /// The one answer that `io` sends for this refusal. A refused request never reached Core: `effect: none`.
    pub fn failure(self) -> Failure {
        let kind = match self {
            Refused::Backpressured { .. } => FailKind::Busy,
            Refused::NotReady => FailKind::Unavailable,
            Refused::ShuttingDown => FailKind::HubShutdown,
        };
        Failure {
            kind,
            effect: Effect::None,
        }
    }
}

/// A refusal hands the request and its token back untouched, so that `io` sends the one refusal (section 3a, step 2).
#[derive(Debug)]
pub struct Rejected {
    pub refused: Refused,
    pub request: Request,
    pub reply: ReplyTo,
}

/// The effect of a failed request (HC section 11.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Proved: the request never reached Core.
    None,
    /// The request may have taken effect.
    Unknown,
}

/// The kind of a failure that the Hub itself answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailKind {
    HubShutdown,
    Internal,
    Busy,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    pub kind: FailKind,
    pub effect: Effect,
}

/// The answer to [`Request::Spawn`] (R-122). A Hub failure goes as [`Answer::Failed`].
#[derive(Debug, Clone, PartialEq)]
pub enum SpawnAnswer {
    /// Admission: `Completed{Create}` was `Ok`. The owner begins `Start` next.
    Accepted { operation_id: String },
    /// A pre-admission Core error, with its effect: `none` when no row exists (U1, U2), `unknown` when one may (U3).
    Refused { error: CoreError, effect: Effect },
}

/// What the owner sends through a token. The protocol layer maps it to the client protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    Spawn(SpawnAnswer),
    /// The answer to [`Request::GetSpawn`]: the durable state.
    SpawnRecord(Lookup),
    /// The `Completed` result of a Core op.
    Op(OpResult),
    /// A sync refusal by Core (`begin` or a read returned an error).
    CoreRefused(CoreError),
    Session(SessionRecord),
    Sessions(Vec<SessionRecord>),
    Cancel(CancelResult),
    /// The answer to `Shutdown`: the stop runs.
    Stopping,
    /// A failure that the Hub answers itself.
    Failed(Failure),
}

/// One entry of a connection's outbox.
#[derive(Debug, Clone, PartialEq)]
pub enum OutItem {
    Reply { request_id: u64, answer: Answer },
}

struct OutboxInner {
    /// Reserved reply slots: tokens that are still held, plus replies in `items` that `io` has not taken.
    reserved: usize,
    items: VecDeque<OutItem>,
    /// The connection ended: a send frees its slot with no write.
    closed: bool,
}

/// The FIFO outbox of one connection. A reply's reservation is a count, not a place; the reply is appended when it is
/// sent (section 4, "Watch frames and `unwatch`"). A send never blocks and never fails.
pub struct Outbox {
    inner: Mutex<OutboxInner>,
    max_replies: usize,
    phase: Arc<SharedPhase>,
    /// Wakes `io` when an item is appended.
    io: Arc<dyn Wake>,
}

impl Outbox {
    /// A new outbox with room for `max_replies` replies in flight (HC EN-4 `max_outstanding`).
    pub fn new(max_replies: usize, phase: Arc<SharedPhase>, io: Arc<dyn Wake>) -> Arc<Outbox> {
        Arc::new(Outbox {
            inner: Mutex::new(OutboxInner {
                reserved: 0,
                items: VecDeque::new(),
                closed: false,
            }),
            max_replies,
            phase,
            io,
        })
    }

    fn lock(&self) -> MutexGuard<'_, OutboxInner> {
        // A panic while the lock is held leaves only counts and a queue; the next user goes on with them.
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Section 3a step 1: reserves one reply slot and makes the token. `None` when the outbox is full: `io` then answers
    /// `too_many_requests` itself (HC EN-4) and calls nothing.
    pub fn reserve(self: &Arc<Self>, request_id: u64) -> Option<ReplyTo> {
        let mut inner = self.lock();
        if inner.closed || inner.reserved >= self.max_replies {
            return None;
        }
        inner.reserved += 1;
        Some(ReplyTo {
            slot: Some(Slot {
                outbox: Arc::clone(self),
                request_id,
            }),
            dispatched: false,
        })
    }

    fn fill(&self, request_id: u64, answer: Answer) {
        let mut inner = self.lock();
        if inner.closed {
            inner.reserved -= 1;
            return;
        }
        inner.items.push_back(OutItem::Reply { request_id, answer });
        drop(inner);
        self.io.wake();
    }

    /// `io` takes at most `max` items in FIFO order. Each taken reply frees its slot.
    pub fn take(&self, max: usize) -> Vec<OutItem> {
        let mut inner = self.lock();
        let n = max.min(inner.items.len());
        let taken: Vec<OutItem> = inner.items.drain(..n).collect();
        inner.reserved -= taken.len();
        taken
    }

    /// The connection ended: unwritten replies are dropped and their slots freed; later sends free their slots with no
    /// write.
    pub fn close(&self) {
        let mut inner = self.lock();
        inner.closed = true;
        let unwritten = inner.items.len();
        inner.items.clear();
        inner.reserved -= unwritten;
    }

    /// Reserved slots (held tokens plus unwritten replies).
    pub fn reserved(&self) -> usize {
        self.lock().reserved
    }

    /// Whether replies wait for `io`.
    pub fn has_items(&self) -> bool {
        !self.lock().items.is_empty()
    }
}

struct Slot {
    outbox: Arc<Outbox>,
    request_id: u64,
}

/// The move-only reply token (section 3a). It is consumed by exactly one [`ReplyTo::send`] or one `Drop`; both fill the
/// reserved slot, so every path releases it.
pub struct ReplyTo {
    /// `None` for internal work, which has no client and no reply.
    slot: Option<Slot>,
    dispatched: bool,
}

impl std::fmt::Debug for ReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyTo")
            .field("request_id", &self.slot.as_ref().map(|s| s.request_id))
            .field("dispatched", &self.dispatched)
            .finish()
    }
}

impl ReplyTo {
    /// The token of internal work (no client, no reply).
    pub fn internal() -> ReplyTo {
        ReplyTo {
            slot: None,
            dispatched: false,
        }
    }

    /// Set just before the owner calls `begin`, `attach` or `cancel`: from now on a drop answers `effect: unknown`.
    pub fn mark_dispatched(&mut self) {
        self.dispatched = true;
    }

    /// Sends the one answer. It never waits and never fails.
    pub fn send(mut self, answer: Answer) {
        if let Some(slot) = self.slot.take() {
            slot.outbox.fill(slot.request_id, answer);
        }
    }
}

impl Drop for ReplyTo {
    /// A drop without a send (a bug or a panic path) fills the slot with a typed failure (section 3a step 4): `hub_shutdown`
    /// while the Hub stops, `internal` otherwise; `effect: unknown` once dispatched, else `none`.
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let kind = if slot.outbox.phase.stopping() {
                FailKind::HubShutdown
            } else {
                FailKind::Internal
            };
            let effect = if self.dispatched {
                Effect::Unknown
            } else {
                Effect::None
            };
            slot.outbox
                .fill(slot.request_id, Answer::Failed(Failure { kind, effect }));
        }
    }
}

/// One admitted request with its token.
#[derive(Debug)]
pub struct Item {
    pub request: Request,
    pub reply: ReplyTo,
}

/// The ring sizes (parameters with small test values; HP LM-5 rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneCaps {
    pub control: usize,
    pub request: usize,
    pub bulk: usize,
}

#[derive(Default)]
struct Lanes {
    control: VecDeque<Item>,
    request: VecDeque<Item>,
    bulk: VecDeque<(String, Item)>,
}

/// The bridge. `io` and internal callers submit; the owner takes.
pub struct Bridge {
    lanes: Mutex<Lanes>,
    caps: LaneCaps,
    ready: AtomicBool,
    phase: Arc<SharedPhase>,
    owner: Arc<dyn Wake>,
}

impl Bridge {
    pub fn new(caps: LaneCaps, phase: Arc<SharedPhase>, owner: Arc<dyn Wake>) -> Bridge {
        Bridge {
            lanes: Mutex::new(Lanes::default()),
            caps,
            ready: AtomicBool::new(false),
            phase,
            owner,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Lanes> {
        self.lanes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// The owner sets it when `Completed{AdoptAll}` is polled.
    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Admits a client request, or hands it back with its token. The phase is checked under the lanes lock, so a request
    /// is either refused `ShuttingDown` or seen by the owner's stop drain.
    ///
    /// # Errors
    /// [`Rejected`] with the request and the token untouched.
    pub fn submit(&self, request: Request, reply: ReplyTo) -> Result<(), Rejected> {
        let refuse = |refused, request, reply| {
            Err(Rejected {
                refused,
                request,
                reply,
            })
        };
        let mut lanes = self.lock();
        if self.phase.stopping() {
            return refuse(Refused::ShuttingDown, request, reply);
        }
        if request.needs_sessions() && !self.is_ready() {
            return refuse(Refused::NotReady, request, reply);
        }
        let lane = request.lane();
        let (ring, cap) = match lane {
            Lane::Control => (&mut lanes.control, self.caps.control),
            Lane::Request | Lane::Bulk => (&mut lanes.request, self.caps.request),
        };
        if ring.len() >= cap {
            return refuse(Refused::Backpressured { lane }, request, reply);
        }
        ring.push_back(Item { request, reply });
        drop(lanes);
        self.owner.wake();
        Ok(())
    }

    /// Admits internal work on the `Bulk` lane. A newer item with the same key replaces the pending one (coalesce before
    /// reject).
    ///
    /// # Errors
    /// `Backpressured { Bulk }` when the lane is full and no item has the key; `ShuttingDown` after the stop started.
    pub fn submit_internal(&self, key: String, request: Request) -> Result<(), Refused> {
        let mut lanes = self.lock();
        if self.phase.stopping() {
            return Err(Refused::ShuttingDown);
        }
        let item = Item {
            request,
            reply: ReplyTo::internal(),
        };
        if let Some(pending) = lanes.bulk.iter_mut().find(|(k, _)| *k == key) {
            pending.1 = item;
        } else if lanes.bulk.len() >= self.caps.bulk {
            return Err(Refused::Backpressured { lane: Lane::Bulk });
        } else {
            lanes.bulk.push_back((key, item));
        }
        drop(lanes);
        self.owner.wake();
        Ok(())
    }

    /// One turn's admission (section 2, step 1): at most `control`, then `request`, then `bulk` items. Each count is at
    /// least 1, so a non-empty lower lane always gets a slot (the starvation bound).
    pub fn take_turn(&self, control: usize, request: usize, bulk: usize) -> Vec<Item> {
        let mut lanes = self.lock();
        let mut out = Vec::new();
        let n = control.max(1).min(lanes.control.len());
        out.extend(lanes.control.drain(..n));
        let n = request.max(1).min(lanes.request.len());
        out.extend(lanes.request.drain(..n));
        let n = bulk.max(1).min(lanes.bulk.len());
        out.extend(lanes.bulk.drain(..n).map(|(_, item)| item));
        out
    }

    /// The stop drain: every item in every lane.
    pub fn drain(&self) -> Vec<Item> {
        let mut lanes = self.lock();
        let mut out: Vec<Item> = lanes.control.drain(..).collect();
        out.extend(lanes.request.drain(..));
        out.extend(lanes.bulk.drain(..).map(|(_, item)| item));
        out
    }

    /// Whether every lane is empty.
    pub fn is_empty(&self) -> bool {
        let lanes = self.lock();
        lanes.control.is_empty() && lanes.request.is_empty() && lanes.bulk.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase::tests::Count;
    use crate::phase::Phase;

    fn sid(name: &str) -> SessionId {
        SessionId(name.into())
    }

    struct Rig {
        phase: Arc<SharedPhase>,
        io: Arc<Count>,
        owner: Arc<Count>,
        outbox: Arc<Outbox>,
        bridge: Bridge,
    }

    fn rig(max_replies: usize, caps: LaneCaps) -> Rig {
        let phase = SharedPhase::new();
        let io = Arc::new(Count::default());
        let owner = Arc::new(Count::default());
        let outbox = Outbox::new(max_replies, phase.clone(), io.clone());
        let bridge = Bridge::new(caps, phase.clone(), owner.clone());
        Rig {
            phase,
            io,
            owner,
            outbox,
            bridge,
        }
    }

    const CAPS: LaneCaps = LaneCaps {
        control: 1,
        request: 2,
        bulk: 2,
    };

    fn reply(rig: &Rig, items: Vec<OutItem>) -> Vec<(u64, Answer)> {
        let _ = rig;
        items
            .into_iter()
            .map(|OutItem::Reply { request_id, answer }| (request_id, answer))
            .collect()
    }

    /// Section 3a step 1: a full outbox makes no token; a taken reply frees its slot.
    #[test]
    fn reserve_bounds_and_take_frees() {
        let rig = rig(2, CAPS);
        let a = rig.outbox.reserve(1).unwrap();
        let _b = rig.outbox.reserve(2).unwrap();
        assert!(rig.outbox.reserve(3).is_none());
        a.send(Answer::Stopping);
        assert_eq!(rig.io.get(), 1);
        assert_eq!(rig.outbox.reserved(), 2);
        assert!(rig.outbox.reserve(3).is_none());
        assert_eq!(reply(&rig, rig.outbox.take(8)), vec![(1, Answer::Stopping)]);
        assert_eq!(rig.outbox.reserved(), 1);
        assert!(rig.outbox.reserve(3).is_some());
    }

    /// Section 3a step 4: a drop without a send answers once; `none` before dispatch, `unknown` after; `internal` while
    /// running, `hub_shutdown` while stopping.
    #[test]
    fn drop_answers_typed_once() {
        let rig = rig(4, CAPS);
        drop(rig.outbox.reserve(1).unwrap());
        let mut dispatched = rig.outbox.reserve(2).unwrap();
        dispatched.mark_dispatched();
        drop(dispatched);
        rig.phase.advance(Phase::Running, Phase::Stopping);
        drop(rig.outbox.reserve(3).unwrap());
        let failed = |kind, effect| Answer::Failed(Failure { kind, effect });
        assert_eq!(
            reply(&rig, rig.outbox.take(8)),
            vec![
                (1, failed(FailKind::Internal, Effect::None)),
                (2, failed(FailKind::Internal, Effect::Unknown)),
                (3, failed(FailKind::HubShutdown, Effect::None)),
            ]
        );
        assert_eq!(rig.outbox.reserved(), 0);
    }

    /// A closed connection frees its slots with no write.
    #[test]
    fn closed_outbox_frees_slots() {
        let rig = rig(4, CAPS);
        let a = rig.outbox.reserve(1).unwrap();
        let b = rig.outbox.reserve(2).unwrap();
        a.send(Answer::Stopping);
        rig.outbox.close();
        assert_eq!(rig.outbox.reserved(), 1);
        drop(b);
        assert_eq!(rig.outbox.reserved(), 0);
        assert!(!rig.outbox.has_items());
        assert!(rig.outbox.reserve(3).is_none());
    }

    /// Section 3: `NotReady` before adoption (control is admitted), `Backpressured` on a full lane, `ShuttingDown` after the
    /// stop; each refusal hands the token back, and its one send is the refusal.
    #[test]
    fn submit_refusals_hand_the_token_back() {
        let rig = rig(8, CAPS);
        let r = rig.outbox.reserve(1).unwrap();
        let rejected = rig.bridge.submit(Request::List, r).unwrap_err();
        assert_eq!(rejected.refused, Refused::NotReady);
        rejected
            .reply
            .send(Answer::Failed(rejected.refused.failure()));
        rig.bridge
            .submit(Request::Shutdown, rig.outbox.reserve(2).unwrap())
            .unwrap();
        let rejected = rig
            .bridge
            .submit(Request::Shutdown, rig.outbox.reserve(3).unwrap())
            .unwrap_err();
        assert_eq!(
            rejected.refused,
            Refused::Backpressured {
                lane: Lane::Control
            }
        );
        rejected
            .reply
            .send(Answer::Failed(rejected.refused.failure()));
        rig.bridge.set_ready();
        rig.bridge
            .submit(Request::Get(sid("a")), rig.outbox.reserve(4).unwrap())
            .unwrap();
        rig.bridge
            .submit(Request::List, rig.outbox.reserve(5).unwrap())
            .unwrap();
        let rejected = rig
            .bridge
            .submit(Request::List, rig.outbox.reserve(6).unwrap())
            .unwrap_err();
        assert_eq!(
            rejected.refused,
            Refused::Backpressured {
                lane: Lane::Request
            }
        );
        rejected
            .reply
            .send(Answer::Failed(rejected.refused.failure()));
        rig.phase.advance(Phase::Running, Phase::Stopping);
        let rejected = rig
            .bridge
            .submit(Request::Shutdown, rig.outbox.reserve(7).unwrap())
            .unwrap_err();
        assert_eq!(rejected.refused, Refused::ShuttingDown);
        rejected
            .reply
            .send(Answer::Failed(rejected.refused.failure()));
        let fail = |kind| {
            Answer::Failed(Failure {
                kind,
                effect: Effect::None,
            })
        };
        assert_eq!(
            reply(&rig, rig.outbox.take(8)),
            vec![
                (1, fail(FailKind::Unavailable)),
                (3, fail(FailKind::Busy)),
                (6, fail(FailKind::Busy)),
                (7, fail(FailKind::HubShutdown)),
            ]
        );
        // Three admissions woke the owner (the phase move has no owner waker registered here).
        assert_eq!(rig.owner.get(), 3);
        // The admitted requests still hold their tokens: three slots.
        assert_eq!(rig.outbox.reserved(), 3);
        drop(rig.bridge.drain());
    }

    /// Section 2 step 1: each lane gets at most its count, and at least one slot when it is not empty.
    #[test]
    fn take_turn_counts_and_starvation_bound() {
        let caps = LaneCaps {
            control: 4,
            request: 4,
            bulk: 4,
        };
        let rig = rig(16, caps);
        rig.bridge.set_ready();
        for id in 0..3 {
            rig.bridge
                .submit(Request::List, rig.outbox.reserve(id).unwrap())
                .unwrap();
        }
        rig.bridge
            .submit(Request::Shutdown, rig.outbox.reserve(9).unwrap())
            .unwrap();
        rig.bridge
            .submit_internal("k".into(), Request::List)
            .unwrap();
        let turn = rig.bridge.take_turn(1, 0, 0);
        let kinds: Vec<bool> = turn
            .iter()
            .map(|i| matches!(i.request, Request::Shutdown))
            .collect();
        // One control item first, then one request (count 0 still gets one), then one bulk item.
        assert_eq!(kinds, vec![true, false, false]);
        assert_eq!(turn.len(), 3);
        let turn = rig.bridge.take_turn(1, 8, 1);
        assert_eq!(turn.len(), 2);
        assert!(rig.bridge.is_empty());
    }

    /// Section 3: only `Bulk` coalesces; a newer item with the same key replaces the pending one.
    #[test]
    fn bulk_coalesces_by_key() {
        let caps = LaneCaps {
            control: 1,
            request: 1,
            bulk: 1,
        };
        let rig = rig(4, caps);
        rig.bridge
            .submit_internal("k".into(), Request::List)
            .unwrap();
        rig.bridge
            .submit_internal("k".into(), Request::Get(sid("new")))
            .unwrap();
        assert_eq!(
            rig.bridge.submit_internal("j".into(), Request::List),
            Err(Refused::Backpressured { lane: Lane::Bulk })
        );
        let turn = rig.bridge.take_turn(1, 1, 1);
        assert_eq!(turn.len(), 1);
        assert!(matches!(&turn[0].request, Request::Get(id) if *id == sid("new")));
        rig.phase.advance(Phase::Running, Phase::Stopping);
        assert_eq!(
            rig.bridge.submit_internal("k".into(), Request::List),
            Err(Refused::ShuttingDown)
        );
    }
}
