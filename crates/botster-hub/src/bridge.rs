//! The bridge from `io` to the owner (design section 3) and the reply token (section 3a).
//!
//! - Bounded lanes: `Control`, `Request`, `Bulk`. A full lane refuses at once (`Backpressured`); nothing blocks.
//! - Only `Bulk` coalesces, by key. No client request is coalesced: every client request gets its own answer (HC EN-3).
//! - `submit` returns admission only. The real answer reaches the [`ReplyTo`] token later, exactly once.
//! - No domain verbs: [`Request`] mirrors Core's ops and reads, plus `Shutdown` and the spawn operation (design section
//!   4a, "Spawn flow").

use crate::phase::{SharedPhase, Wake};
use crate::store::Lookup;
use crate::watch::{WatchFilter, WatchFrame, WatchKey};
use botster_core::contract::prelude::{
    CancelResult, CoreError, Op, OpId, OpResult, SessionId, SessionRecord, SpawnRequest,
};
use std::collections::{BTreeMap, VecDeque};
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
    /// Watches the session view (HC WT-1): a baseline, then deltas, through the connection's outbox.
    Watch {
        key: WatchKey,
        filter: WatchFilter,
        outbox: Arc<Outbox>,
    },
    /// Ends a watch (HC WT-6): every frame of it is ahead of the reply.
    Unwatch(WatchKey),
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
    /// The connection is at its watch bound (HC BD-1).
    TooManyWatches,
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
    /// The watch exists; its baseline follows.
    Watching,
    /// The watch is gone (or never existed).
    Unwatched,
    /// A failure that the Hub answers itself.
    Failed(Failure),
}

/// One entry of a connection's outbox.
#[derive(Debug, Clone, PartialEq)]
pub enum OutItem {
    Reply { request_id: u64, answer: Answer },
    Frame { watch: u64, frame: WatchFrame },
}

/// How a delta push went (design section 4, "Overflow and resync").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaPush {
    Queued,
    /// The watch's queue was full: it is dropped, and the owner puts the watch in the baseline queue again.
    Overflow,
    /// No such watch queue (the watch ended): nothing to do.
    NoWatch,
}

/// The bounded delta queue of one watch. Keyed rows coalesce to the newest value; `live` is false while a baseline is in
/// progress, so `io` writes the deltas only after `snapshot_end` (HC WT-1, WT-4).
#[derive(Debug, Default)]
struct WatchQueue {
    deltas: VecDeque<(SessionId, WatchFrame)>,
    live: bool,
}

struct OutboxInner {
    /// Reserved reply slots: tokens that are still held, plus replies in `items` that `io` has not taken.
    reserved: usize,
    /// Replies and baseline frames, FIFO.
    items: VecDeque<OutItem>,
    /// The baseline frames in `items`.
    frames: usize,
    /// The owner waits for frame room (the capacity wake).
    wants_room: bool,
    queues: BTreeMap<u64, WatchQueue>,
    /// The connection ended: a send frees its slot with no write.
    closed: bool,
}

/// The sizes of an outbox (small test values; HP LM-5 rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxCaps {
    /// Replies in flight (HC EN-4 `max_outstanding`).
    pub replies: usize,
    /// Baseline frames that wait for `io` (`outbox_frames`).
    pub frames: usize,
    /// Deltas per watch queue.
    pub watch_queue: usize,
}

/// The FIFO outbox of one connection (design section 4). A reply's reservation is a count, not a place; the reply is
/// appended when it is sent. Baseline frames need frame room; deltas go to their watch's bounded queue and never wait. A
/// send never blocks and never fails.
///
/// The capacity wake: the owner arms `wants_room` and checks the room again under the same lock, and `io` lowers the frame
/// count and checks the flag under that lock. This is one synchronized level check (Sol finding 1, round 4), so no wake is
/// lost, and a blocked baseline does not spin.
pub struct Outbox {
    inner: Mutex<OutboxInner>,
    caps: OutboxCaps,
    phase: Arc<SharedPhase>,
    /// Wakes `io` when an item is appended.
    io: Arc<dyn Wake>,
    /// Wakes the owner when frame room returns (the capacity wake).
    owner: Arc<dyn Wake>,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox").field("caps", &self.caps).finish()
    }
}

impl Outbox {
    pub fn new(
        caps: OutboxCaps,
        phase: Arc<SharedPhase>,
        io: Arc<dyn Wake>,
        owner: Arc<dyn Wake>,
    ) -> Arc<Outbox> {
        Arc::new(Outbox {
            inner: Mutex::new(OutboxInner {
                reserved: 0,
                items: VecDeque::new(),
                frames: 0,
                wants_room: false,
                queues: BTreeMap::new(),
                closed: false,
            }),
            caps,
            phase,
            io,
            owner,
        })
    }

    fn lock(&self) -> MutexGuard<'_, OutboxInner> {
        // A panic while the lock is held leaves only counts and queues; the next user goes on with them.
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Section 3a step 1: reserves one reply slot and makes the token. `None` when the outbox is full: `io` then answers
    /// `too_many_requests` itself (HC EN-4) and calls nothing.
    pub fn reserve(self: &Arc<Self>, request_id: u64) -> Option<ReplyTo> {
        let mut inner = self.lock();
        if inner.closed || inner.reserved >= self.caps.replies {
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

    /// Appends a baseline frame if the outbox has frame room. Without room, it arms `wants_room` under the same lock and
    /// returns `false`: the owner marks the baseline blocked, and `io` wakes the owner when room returns. A closed outbox
    /// takes the frame and drops it.
    pub fn push_frame(&self, watch: u64, frame: WatchFrame) -> bool {
        let mut inner = self.lock();
        if inner.closed {
            return true;
        }
        if inner.frames >= self.caps.frames {
            inner.wants_room = true;
            return false;
        }
        inner.frames += 1;
        inner.items.push_back(OutItem::Frame { watch, frame });
        drop(inner);
        self.io.wake();
        true
    }

    /// Whether the owner still waits for frame room. `io` clears it with the capacity wake.
    pub fn wants_room(&self) -> bool {
        self.lock().wants_room
    }

    /// Opens the delta queue of a new watch (not live: its baseline comes first).
    pub fn open_watch(&self, watch: u64) {
        self.lock().queues.insert(watch, WatchQueue::default());
    }

    /// The baseline ended (`snapshot_end` is in `items`): `io` may write the watch's deltas after it.
    pub fn set_live(&self, watch: u64) {
        if let Some(queue) = self.lock().queues.get_mut(&watch) {
            queue.live = true;
        }
        self.io.wake();
    }

    /// Drops the watch's deltas and makes it wait for a new baseline (overflow or resync).
    pub fn reset_watch(&self, watch: u64) {
        if let Some(queue) = self.lock().queues.get_mut(&watch) {
            queue.deltas.clear();
            queue.live = false;
        }
    }

    /// `unwatch` and the connection's end: the watch's queue goes in this one step (HC WT-6).
    pub fn remove_watch(&self, watch: u64) {
        self.lock().queues.remove(&watch);
    }

    /// Queues a delta of `key` for the watch. A delta of a key that is already queued replaces it in place (keyed rows
    /// coalesce to the newest value). A full queue overflows: its deltas are dropped.
    pub fn push_delta(&self, watch: u64, key: &SessionId, frame: WatchFrame) -> DeltaPush {
        let mut inner = self.lock();
        let closed = inner.closed;
        let Some(queue) = inner.queues.get_mut(&watch) else {
            return DeltaPush::NoWatch;
        };
        if closed {
            return DeltaPush::Queued;
        }
        if let Some(slot) = queue.deltas.iter_mut().find(|(k, _)| k == key) {
            slot.1 = frame;
        } else if queue.deltas.len() >= self.caps.watch_queue {
            queue.deltas.clear();
            queue.live = false;
            return DeltaPush::Overflow;
        } else {
            queue.deltas.push_back((key.clone(), frame));
        }
        let live = queue.live;
        drop(inner);
        if live {
            self.io.wake();
        }
        DeltaPush::Queued
    }

    /// `io` takes at most `max` items: first `items` in FIFO order (replies and baseline frames), then the deltas of live
    /// watches. Each taken reply frees its slot. When the frame count falls to `frames / 2` or less while the owner waits
    /// for room, `io` clears `wants_room` and wakes the owner once (the capacity wake).
    pub fn take(&self, max: usize) -> Vec<OutItem> {
        let mut inner = self.lock();
        let n = max.min(inner.items.len());
        let mut taken: Vec<OutItem> = inner.items.drain(..n).collect();
        for item in &taken {
            match item {
                OutItem::Reply { .. } => inner.reserved -= 1,
                OutItem::Frame { .. } => inner.frames -= 1,
            }
        }
        for (watch, queue) in inner.queues.iter_mut() {
            while taken.len() < max && queue.live {
                let Some((_, frame)) = queue.deltas.pop_front() else {
                    break;
                };
                taken.push(OutItem::Frame {
                    watch: *watch,
                    frame,
                });
            }
        }
        let wake = inner.wants_room && inner.frames <= self.caps.frames / 2;
        if wake {
            inner.wants_room = false;
        }
        drop(inner);
        if wake {
            self.owner.wake();
        }
        taken
    }

    /// The connection ended: unwritten replies and frames are dropped and their slots freed; later sends free their slots
    /// with no write. The owner drops the connection's watches when it sees `is_closed`.
    pub fn close(&self) {
        let mut inner = self.lock();
        inner.closed = true;
        let replies = inner
            .items
            .iter()
            .filter(|item| matches!(item, OutItem::Reply { .. }))
            .count();
        inner.items.clear();
        inner.frames = 0;
        inner.queues.clear();
        inner.reserved -= replies;
        let waiting = std::mem::take(&mut inner.wants_room);
        drop(inner);
        if waiting {
            self.owner.wake();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Reserved slots (held tokens plus unwritten replies).
    pub fn reserved(&self) -> usize {
        self.lock().reserved
    }

    /// Whether items wait for `io`.
    pub fn has_items(&self) -> bool {
        let inner = self.lock();
        !inner.items.is_empty()
            || inner
                .queues
                .values()
                .any(|q| q.live && !q.deltas.is_empty())
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
        let sizes = OutboxCaps {
            replies: max_replies,
            frames: 4,
            watch_queue: 4,
        };
        let outbox = Outbox::new(sizes, phase.clone(), io.clone(), owner.clone());
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
            .map(|item| match item {
                OutItem::Reply { request_id, answer } => (request_id, answer),
                OutItem::Frame { .. } => panic!("a frame in a reply test"),
            })
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
