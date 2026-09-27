# Readiness and flow control

Status: accepted by review (verdict a27dfa2c), 2026-09-27; pending the orchestrator and the user for scope. The design is accepted; the implementation tests in section 4 remain acceptance criteria. Design only. No production code changes in this phase.
Scope: botster-hub at `1ec61b94`, and botster-core at `origin/main` `19edeb1`. Hub pins Core `549b3f62`. The unlanded roll branch `delivery/core-roll-85b3507-20260927` moves the pin to `85b3507`.

## 0. Summary

One defect class keeps coming back: a lost wake, a spin, or a "work exists" predicate used as a "can progress now" predicate. The cause is not one bad site. The Hub owner already has a correct run queue (`ReadyQueues`, `src/daemon/owner_schedule.rs`). But about ten side channels bypass it:
- a hand-kept wake bit set;
- hand-kept `waiting_for_*` flags;
- a bit plus a doorbell message, one pair per producer;
- refusals that spin, drop, or open a gap.

Each side channel needs every writer to remember to wake the right party. The design removes that duty:
1. **One readiness contract.** Every owner work source answers `readiness() -> Ready | Wait(Wake)`. The function derives the answer from state. `Wait` must name the event that makes the source ready. The type does not allow a wait without a named event.
2. **One wake primitive, with arm-then-check registration.** Every cross-thread producer raises one `OwnerSignal` key epoch. A source reads the epoch before its final attempt, so a release cannot fall between the attempt and the wait. The owner re-evaluates every source predicate after every owner step, so owner state needs no signal at all.
3. **One refusal rule.** Contention (a busy lock) never becomes a refusal, a drop, or a gap. It becomes `Wait` on the release event. Capacity may refuse, and the refusal is typed and names its retry event.
4. **End-to-end terminal backpressure.** This follows the orchestrator ruling of 2026-09-27, and the Core writer is building it. When the slowest progressing bound reader has no egress room, the worker stops draining the PTY. A reader with no progress for `READER_PROGRESS_DEADLINE` (10 s) is ended with `Stalled`. This ends the flood resync storm without a new limit.

The design deletes `MaintenanceWakes`, `mark_all`/`try_wake` (29 + 11 production sites), about nine doorbell message variants, the hand flags, and the causal sweep. It adds one small set of types (`Readiness`, `Outcome`, `OwnerSignal`), one Core notifier edge, and a cause field on Core `Backpressured`.

## 1. Flow-control map

Legend for "at the bound": **BP** = the producer stalls (lossless); **REF** = typed refusal to the caller; **DROP+RS** = drop, then resync from state; **UNB** = no bound.

### 1.1 Terminal output: PTY → client

| Hop | Crossing | Bound | At the bound |
| --- | --- | --- | --- |
| PTY → worker reader thread | — | 8 KiB reads | kernel PTY buffer blocks the child |
| reader → worker main loop | thread (`ReaderFence` + condvar) | 64 chunks (`local_process.rs:33`) | BP |
| main loop (Ghostty apply) → egress writer | thread (`sync_channel`) | 64 (`DEFAULT_WORKER_EGRESS_CAPACITY`) | BP for protected frames; metadata lane DROP (counted) |
| egress writer → parent | **process** (Unix socket) | socket buffer | BP. **Defect:** after a write error, later frames are discarded silently until reconnect (`botster-session-worker.rs:1693-1705`) |
| parent reader thread → data-plane thread | thread (`sync_channel` + wake queue) | 64 events; wake queue 512 | BP when a consumer is attached; DROP with a `Backpressure{SessionIo}` summary when none is attached |
| data plane → route queue | — | 64 frames / 4 MiB per route (`frame.rs:20-22`) | DROP+RS: visual frames dropped, `ROUTE_RESYNC`, new capture; teardown `Overflowed` if still over. **Backpressure ends here.** |
| route → adapter slot | — | 1 frame; 512 refused writes; 10 s reader progress | stall resync, then `Stalled` teardown |
| Unix: slot → connection task → socket | thread (slot + `Notify`) | 1 slot | BP to the route (Writable wake) |
| WebRTC: slot → channel task → rtc driver | thread, and task | 128/64 KiB per channel, 2 MiB/1 MiB per peer | BP by watermark edges. **UNB** below: rtc send buffer (`usize::MAX`, never set) and SCTP pending queue; the watermarks are checked per frame, not per 12 KiB chunk |

### 1.2 Terminal input: client → PTY

| Hop | Bound | At the bound |
| --- | --- | --- |
| Unix read / WebRTC SCTP | frame size; SCTP window | protocol close; real BP to the WebRTC peer |
| transport → adapter ingress | 64 frames (`MIN_ADAPTER_INGRESS_BUFFER_FRAMES`) | **DROP + `Lost` latch; Core ends the route.** The transport keeps reading. **Backpressure ends here.** |
| intake lanes | 32 ops / 2 MiB per session; 128 / 8 MiB per client | REF (`RejectedLaneFull`) |
| paste assembly | `MAX_PASTE_BYTES` 1 MiB per paste; 1 assembling paste per route, and a second is refused; `PASTE_ASSEMBLY_TIMEOUT` 5 s (`input_frame.rs:43-55`; `client_worker.rs:98, 1850-1870`) | REF on size and on a second paste; `timer: deadline` expiry fails the paste typed |
| input rejection results | 16 unreserved rejection results queued per route (`MAX_QUEUED_REJECTIONS_PER_ROUTE`, `client_worker.rs:101, 1295-1314`) | the route closes `Overflowed`. A client that keeps sending into a full lane loses its route. S5 input backpressure makes this rare, because the transport stops reading first. |
| apply → control queue | 32 frames (2 reserved); 30 pending resizes | BP (park for capacity, woken by the writer's `freed_capacity`) |
| control writer → worker | **process**; 2 s write deadline | control plane sealed → `WorkerLinkFailed` |
| worker control reader → main loop | **UNB** `mpsc::channel()` (`botster-session-worker.rs:236`); bounded upstream only by the 32-frame queue | — |
| main loop → PTY write | 32 ops / 2 MiB keyed (duplicates the intake session lane); keyless input (terminal-model replies from `flush_runtime_inputs_for_session`) is bounded at the parent by the control queue, but it is **UNB** in the worker | REF for keyed ops |

Coupling: while the worker main loop is blocked on a full egress lane, it applies no input.

### 1.3 Plugin → Hub → clients

| Path | Bound | At the bound |
| --- | --- | --- |
| host call (coordination) | Lua callback account | REF (`Capacity`); Lua parks `recv_timeout(1 s)` |
| Core plugin class queues | request-response and Background, 256 / 1 MiB each per plugin (`plugin_worker.rs:194-207`) | REF (`Backpressured`, class cause); no retry event until C2 |
| Core completion store | 256 entries; 1 MiB reservations; 1 MiB queue bytes (`plugin_worker.rs:194-207`) | REF (`Backpressured`, completion cause). Reservations return on two paths: the host's drain, and `close_fund` (the unused part of a delivery fund, at worker shutdown or unload; `plugin_completion_store.rs:145-162`, `plugin_worker.rs:2190-2203`). Generation `retire` returns none: it moves published completions to the retired queue, and they return when drained. C2 fires the engine notifier on both paths (2.4.1). |
| Hub retained plugin results | 8 MiB, global (`RETAINED_PLUGIN_RESULT_BYTE_CAPACITY`, `reply.rs:10-18`) | the completion drain stops. The charge drop raises `PluginResultCapacityReleased` (`reply.rs:147-171`). |
| async capabilities (fs, store) | 128 ops / 256 events per plugin | REF (`Backpressured`). Results go to an **UNB** mpsc with **no production consumer** (`drain_capability_events` has test callers only) |
| entity publication | one shared bridge: 256 entries / 8 MiB / 1 MiB each (`entity_publish.rs:16-18`) | REF (`NeverQueued`). One plugin can exhaust it for all (plan §4.5.1 moves this to per-plugin pools) |
| fanout → subscriber | Unix 256, WebRTC 64 per subscriber; 64 MiB `SharedViewBudget` | DROP+RS (`subscriber_overflow`), paced by `PACKAGE_ENTITY_*` |
| Unix entity/event write queue | `MuxWriteState.queued_events` **UNB** (`mux_write.rs:35`). The connection task moves each frame from the 256-deep entity channel into it, and it publishes the capacity wake at once (`connection.rs:264-267`). So the 256 bound does not hold while the socket is slow. | — (S6) |
| plugin logs | none on main (planned ring: 256 records / 512 KiB, pull-only) | planned: evict oldest, counted |

**Process host (Core, built but not wired into Hub).** Every number here is supplied by the Hub; the values are the approved plugin-platform §5.0 numbers.

| Hop | Bound | At the bound | Wake |
| --- | --- | --- | --- |
| child host call → parent ingress | `ingress_bytes` (request-body credit 512 KiB) plus one delivery-pool unit (128 slots / 512 KiB) | the child's sender refuses locally with `backpressured`, or a publication waits for credit (§4.5.4). An overdraw is a protocol violation, and the worker is killed. | `install_ingress_notifier` → `PluginIngress` |
| child reply → parent | `reply_credits` 2 × 1 MiB | the platform path (`HostPort::reply`) **waits** for reply credit. The invocation's cancellation (its deadline or a `Cancel`) ends the wait with a typed failure, so a chain's final result is sent or its invocation fails typed; it is never lost to call backpressure (`host_port.rs:1-16, 431-453`). `try_reply` refuses at once, and the platform does not use it for final results. | `PluginIngress`; the credit returns at `release_reply` |
| child log → parent | `log_credits` (256 records / 512 KiB) | the child drops the line and counts it in `dropped_since_last` | `PluginIngress` |
| parent → child (the writer thread) | 4 lanes: Startup 2, Invoke `max_in_flight_invokes`, Cancel `max_in_flight_invokes`, Shutdown 1 (`outbound.rs:60-76`). Pending credits: one pending id per returned unit or released reply (at most the granted units plus the reply credits); ingress and log credit each coalesce into one pending total (`outbound.rs:8-12`) | `Refused::Full` is a caller bug for these derived lanes; `Closed` means the process is gone | credits are never refused |
| child → parent (the child writer's FIFO) | host calls and logs enter only after their credit is debited; results only for an invocation in flight, at most `max_results` (`host_port.rs:67-99`) | no new bound: the FIFO is bounded by the credits and `max_results` | — |
| result → child | `admit_result` from the pool, never refused for capacity | — | `install_unit_returned` → `PluginCredit` |
| frame size | `max_frame_bytes` | protocol violation, and the worker is killed | — |
| lifecycle | startup, shutdown, and cancel-grace deadlines | `timer: deadline`: the process group is killed | `install_exit_notifier` → `PluginExit` |

### 1.4 Hub events → plugins and clients

| Hop | Bound | At the bound |
| --- | --- | --- |
| `try_ingress` (router) | 64 KiB payload; producer 256 / 512 KiB; 100/s burst 200; global 16 MiB | REF (`EventPlaneStatus`) |
| router → client mailbox | 128 / 2 MiB | DROP + gap bit (client resyncs) |
| router consumer queue → Core Background queue | router 128 / 2 MiB, then Core 256 / 1 MiB per plugin | **Spin:** `Backpressured` requeues and calls `mark_all` (`daemon_maintenance.rs:1453-1461`). `LockBusy` and other results **retire the event (DROP, no resync)** |
| off-owner emit (Lua `botster.events.emit`) | — | sets `delivery_wake` with **no doorbell**; delivery waits for an unrelated wake |

### 1.5 Missing links

1. **Terminal output has no backpressure from the client to the PTY.** The lossless chain stops at the route queue. That queue drops and resyncs instead of stalling its producer. Before Core `2845aec`, the 1 ms reader sleep hid the gap by capping output near 1 MB/s. Without the sleep, the result is the flood storm: 3-15 resyncs in 3 s and about 50 times the byte rate (`botster-evidence/core-residual-waits-20260927/flood-bisect-log.txt`). Overflow also asks for a capture at once, while the queue is still full, so recovery can overflow again. A line-buffered flood (`seq` to a tty) makes 7-byte `Output` frames, so the 64-frame route bound limits it long before the 4 MiB byte bound (step S12).
2. **Terminal input drops at adapter ingress.** A paste burst faster than the data plane ends the route. The transport should stop reading instead. The kernel socket buffer and the SCTP window then carry the backpressure to the client.
3. **Contention becomes loss.** `LockBusy` on a package event drops the event. On a session-family frame it opens a gap and starts a full baseline resync (`daemon_maintenance.rs:1247-1250`). On a plugin control request it refuses the client (`plugins.rs:550-563`). `SubscribeEvents` returns `shed_busy` when the owner loses a try-lock race with the connection's own reader (`package_events.rs:687-707`).
4. **Core plugin `Backpressured` has no retry event.** Class-queue space frees when a worker dequeues, and nothing notifies the host (`contract/actor.rs:1326-1330`).
5. **Unbounded queues:** the WebRTC rtc send buffer and SCTP pending queue, the Unix `queued_events`, the worker control mpsc, keyless input inside the worker, and the async capability result mpsc.
6. **Silent drops:** the worker egress after a socket error, and the event retire on `LockBusy`.
7. **Refused maintenance reads stall (live defect).** When the bounded Core request queue is full (`CoreTicketPoll::Refused`), the Observe, journal-pull, and baseline slices clear their read and return without marking themselves again (`daemon_maintenance.rs:955, 1004, 1111`). A freed queue slot publishes no owner wake. The read then waits for an unrelated wake. This is confirmed by code reading, and no test proves it yet. Step S4 fixes it: the slice becomes `Wait(Signal(DataPlaneCapacity))`, and a test that fills the request queue proves the fix.

### 1.6 Duplicate bounds (recorded; no value changes in this plan)

- Three 64-deep stages on one terminal stream (reader fence, egress lane, parent channel). They form a pipeline, not a conflict. Section 3 removes two of them with their threads.
- The route queue allows 4 MiB, but the WebRTC per-peer aggregate is 2 MiB.
- Keyed input lanes: 32 ops / 2 MiB at Core intake and again in the worker.
- Host executor: 8 permits duplicate the `sync_channel(8)`, and each permit reserves 8 MiB, so the 64 MiB aggregate check can never bind first (`host_executor.rs:860-872`).
- Package events: router consumer queue (128 / 2 MiB) on top of the Core Background queue (256 / 1 MiB).

Follow-up: each duplicate is a user decision about a number, so this plan only lists them.

## 2. Readiness model

### 2.1 The contract

```rust
enum Readiness { Ready, Wait(Wake) }

enum Wake {
    Local(Seen),        // owner state; re-evaluated after every owner step
    Signal(Seen),       // cross-thread; see 2.2
    Deadline(Instant),  // only with an approved `timer:` marker at the source
}

// The counter that ends the wait, and its value read before the final attempt.
// Local counters are plain owner fields; Signal counters are OwnerSignal epochs.
struct Seen { counter: CounterKey, value: u64 }

enum Outcome { Progress, Blocked(Wake) }

trait OwnerSource {
    fn readiness(&self, owner: &OwnerView) -> Readiness;
    fn run(&mut self, owner: &mut OwnerCtx, budget: &mut OwnerTurnBudget) -> Outcome;
}
```

Rules:
1. **Readiness is derived.** `readiness` is a function of state. A source may keep an internal index, for example "subscribers with a pending frame and capacity". Only the mutators of the underlying state change that index, and a debug check recomputes it from scratch after each `run` in tests.
2. **Every owner step is followed by re-evaluation.** "Owner step" means the dispatch of a control message, the run of a ready item, and the processing before a sleep. The scheduler re-evaluates every parked source after each step, and once more immediately before it blocks. This is the only path by which a source becomes ready, so no transition can skip it.
3. **`Ready` from owner state promises progress.** When `readiness` says `Ready`, `run` returns `Progress`, unless it was refused by a resource that the owner does not own: a lock shared with another thread, a Core queue, or Core's completion store. `Outcome` has no "no progress" value. A refused source returns `Blocked(wake)`, and the wake carries the counter value that it read before its final attempt (2.2). A `Local` wake names an owner counter, for example an owner-permit release count. A `Signal` wake names an `OwnerSignal` epoch. A `Blocked` return with no such refusal is a bug. Debug builds assert, and release builds fault the source with a typed error, counted and logged. It does not stay ready, so release builds cannot spin either.
4. **Every wait names its event.** `Wait` cannot be built without a `Wake`, and a `Local` or `Signal` wake cannot be built without its `Seen` counter value. A refusal that crosses a boundary carries its cause, so the caller can pick the right `Wake` (2.3).
5. **Three kinds of wait; each has one mechanism.**

   | Kind | Examples | Mechanism |
   | --- | --- | --- |
   | Source predicate | the maintenance slices; publication and event owners; admission sources | re-evaluated after every owner step (rule 2); about 20 coarse sources, each O(1) |
   | Keyed completion | a Host job by job id; a Core ticket by ticket id; a causal transition waiting for its own receipt | the completion router marks exactly its waiter (today's routing). It is never swept. |
   | FIFO capacity (interchangeable units) | Host permits; owner permits; causal table slots; the exclusive entity model; plugin credit waiters (plugin-platform §4.5.4) | a FIFO per capacity. Its head is a source predicate. Rule 2 then gives, with no extra code: a check at registration, the next head after a head cancels, and continuation while a coalesced release still leaves room. |

   A wait belongs to exactly one kind. A FIFO holds only waiters for the same interchangeable unit, so a blocked head can never hide a waiter that waits for something else. A fault (for example the entity model faulting) makes every waiter of that resource `Ready`, and each one then fails typed.
6. **Cursors do not decide readiness.** A round-robin or paged cursor only chooses the order of work. Readiness comes from a maintained ready set, for example "consumers with a pending frame, not in flight, and with a handler". A pass that reaches the end of a page must wrap within the pass or leave the source `Ready`. A pass that returns "nothing" at a page end while the ready set is not empty is the lost-wake shape of b870bca3. Session-family admission needs such a ready set. The family gap pass and prune already keep flags that suffice.
7. **A refusal that cannot change is a fault, not a retry.** If a unit of work can never fit its bound (for example Observe `BudgetTooSmall` for one row at the constant `OBSERVE_SLICE_BUDGET`, `daemon_maintenance.rs:989`), the source fails that unit with a typed error. It does not stay `Ready`. Today the site calls `mark_all` and resubmits every turn, which is a permanent spin if Core can return that result for a single row.

### 2.2 Why no wake is lost and nothing spins

**One primitive: `OwnerSignal`.** It holds one epoch counter per `SignalKey` and one doorbell, which is a `tokio::sync::Notify`.
- **Producer.** The producer mutates the shared state first. It then calls `raise(key)`, which increments the key's epoch (release ordering) and calls `notify_one()`. `Notify` stores at most one permit, so the doorbell can never be full and never loses a raise.
- **Registration (arm, then check).** A source reads the counter **before** its final attempt on the refused resource. If that attempt is refused, it returns `Blocked(wake)` with that value. The source's readiness stays `Wait` while the counter still equals the value, and becomes `Ready` once the counter moves. `Local` counters work the same way, with no atomics: the owner increments them in its own steps.
- **The owner's sleep.** The owner blocks on its control channel, the doorbell, and the next deadline. It blocks only after the rule-2 re-evaluation finds nothing ready.

Proof sketch:
- **A release after the epoch read** increments the epoch. The source then re-evaluates to `Ready`. If the owner is already asleep, the stored `Notify` permit wakes it.
- **A release before the epoch read** is visible to the final attempt, so that attempt does not refuse for that reason.
- **No spin.** A blocked source retries at most once per epoch increment, and the epoch moves only on a real release. If another thread takes the unit first, one retry is wasted. The number of retries is bounded by the number of releases, not by owner turns.
- **Owner state** cannot change without an owner step, and rule 2 re-evaluates after every step.

**One key per resource, and the refused attempt never raises it.** A key belongs to exactly one resource: one lock, one queue, or one pool. A refusal names the resource that refused, and the source arms that resource's key. The failed attempt never acquired that resource, so nothing the attempt releases can advance the armed counter. This rules out a retry that feeds itself (see the `ShedBusy` rows in 2.3). A poisoned lock is a terminal fault, never a wait.

**The one combined key: `PluginEngine`.** It deliberately covers several Core edges: an armed lock release, a freed class slot, and a returned completion reservation. The same no-self-trigger property holds by a Core rule instead of by resource identity: only releases of held state fire the notifier (a worker's dequeue or lock release, a drain, a fund close). A refused `try_admit` rolls back what it reserved and fires nothing. The Core conformance test (C2) asserts that a refused admission fires no notification of its own. A concurrent legitimate release may still notify.

**Producer duty.** A producer of a cross-thread key must raise on **every** release that can turn a refusal into success. Alternatively, it can follow Core's `try_admit` pattern (`plugin_worker.rs:920-950`): arm inside the refusing call before its last retry, and fire on the next release while armed. Each key names its producer in 2.3. A key with no producer cannot be registered.

**Tests (per key, deterministic).** A test hook runs between the source's epoch read and its final attempt, and another runs between the refusal and the `Blocked` return. At each hook the test performs the release. The source must then run with no other wake. An ablation that removes the producer's raise must fail. The idle and flood tests in section 4 measure cost; these interleaving tests prove correctness.

This is the `Future::poll` contract ("`Pending` must have registered a waker"), carried by a return type and without async. The owner's budgeted, synchronous turn stays as it is.

### 2.3 The refusal rule, per boundary

Contention never becomes a refusal to a client, a drop, or a gap. Capacity may refuse, and that refusal is typed.

| Refusal | Cause | Caller's wait | Producer of the release |
| --- | --- | --- | --- |
| Core `LockBusy` | contention | `Signal(PluginEngine)` | Core fires the notifier on an armed lock release (today) |
| Core `Backpressured`, class queue | capacity | `Signal(PluginEngine)`; a client request instead gets a typed refusal (unchanged) | Core fires on an armed class-slot free (C2, new) |
| Core `Backpressured`, completion reservation | capacity | `Signal(PluginEngine)` | Core fires the notifier, while armed, on every reservation return: the host's `drain_completions` (today it fires nothing there, `plugin_worker.rs:1365-1401`) and `close_fund` (C2, new). `retire` returns no reservation; a retired completion returns its reservation when it is drained. A Hub-side drain counter would miss fund closure at another plugin's unload, when nothing drains. |
| Core `ControlQueueFull` / `PendingLimit` | capacity | park on the session wake (today, correct for input) | the control writer's `freed_capacity` |
| Hub `CoreTicketPoll::Refused` (request channel full) | capacity | `Signal(DataPlaneCapacity)` | the data-plane thread raises after it dequeues requests (`data_plane/driver.rs:1598, 1609`), in S4a |
| Host permit refused | capacity | FIFO (`HostCapacity`) | the permit drop raises `HostCapacity` (today's `capacity_pending`) |
| Retained plugin-result bytes (8 MiB) | capacity | `Signal(PluginResultCapacity)` | the charge drop (today's `PluginResultCapacityReleased`, `reply.rs:147-171`) |
| `ShedBusy`, the client-event connections mutex (`package_events.rs:934-938`) | contention | `Signal(EventConnections)` | the guard drop of that mutex (S3) |
| `ShedBusy`, one connection's slots mutex (`package_events.rs:1028-1030`, `try_slots`) | contention | `Signal(ConnectionSlots(conn))` | the slots guard drop (S3) |
| `ShedBusy`, one connection's pool mutex (`package_events.rs:1043-1047`) | contention | `Signal(ConnectionPool(conn))` | the pool guard drop (S3). The owner's own slots guard drop raises `ConnectionSlots`, not this key, so the failed attempt cannot wake itself. |
| `ShedBusy`, the router inner mutex (`package_event_router.rs:1771-1777`) | contention | `Signal(EventRouter)` | the router guard drop (S3) |
| a poisoned lock at any of these four sites | fault | none: a typed terminal fault. Today the connections, pool, and router sites map poison to `ShedBusy` (the router explicitly, `:1774-1776`); `try_slots` already maps it to `RejectedInvalid` (`package_events.rs:688-705`). | — |
| Subscriber queue full | capacity | DROP+RS (unchanged: entity state resyncs from the model) | — |

C2 makes `Backpressured` carry its cause (class queue or completion reservation), and S3 makes `ShedBusy` carry the lock that refused (connections, slots, pool, or router). Both are typed mechanism changes, not policy. One small type, a mutex whose guard drop raises its own key, serves all four `ShedBusy` locks.

### 2.4 Core contract changes (mechanism only; no policy moves into Core)

1. **One plugin-engine notifier meaning.** "Plugin engine state changed. A completion may be ready, or a refused admission may now succeed." Core fires it on: a completion published (today); an armed lock release (today, `admission_retry_armed`); and, new in C2, **any release that can end an armed `Backpressured`**:
   - a class-queue slot freed, where a worker dequeues a job (`take_dispatchable`) and where a job is unqueued;
   - a completion reservation returned, in `drain_completions` and `close_fund`. `retire` returns none; retired completions return theirs when drained.
   `Backpressured` carries its cause (class queue or completion reservation). Both causes wait on `Signal(PluginEngine)`. A refused `try_admit` fires nothing. A conformance test in Core pins these edges, so a later Core commit cannot move the contract silently (defect: 1c2e526 changed the notifier's meaning).
2. **Source backpressure for terminal output.** This is the orchestrator ruling of 2026-09-27, and the Core writer is building it.
   - **Mechanism, with no new signal.** When the slowest progressing bound reader has no egress room, Core holds the session's runtime output (`ManagedSessionRuntime.held_runtime_output`) and stops calling `drain_output`. The chain then fills and stalls, one existing stage at a time:
     1. the parent's bounded worker channel fills;
     2. the parent reader stalls in `EgressStall`;
     3. the worker's egress writer blocks;
     4. the worker stops reading the PTY;
     5. the program blocks on write.
   - **Resume.** After each pump, Core calls `notify_session` for every session whose held output now fits.
   - A reader with no progress for `READER_PROGRESS_DEADLINE` (10 s, an existing bound) is ended with `Stalled`. A stalled reader therefore cannot hold the session for longer than that.
   - A route at a capture boundary is exempt, so a capture always completes.
   - A Core probe of this fix shows 0 resyncs under flood. Branch: `delivery/core-flood-backpressure-20260927`, not yet pushed.
   - In the readiness terms of this plan, the session's PTY drain is `Wait(route egress room)`. Its negative state names its event, and the deadline is the existing `timer: deadline`.
   - **Overflow is still reachable, so C1 has a second part.** The Core writer names two paths by which a bound, progressing route can still reach `overflow_route`:
     1. **Capture boundary.** Output routed ungated at a new route's capture boundary overflows a saturated existing reader. The Core test "attach while held" shows 1 resync.
     2. **`WRITE_ATTEMPT_BUDGET` stall.** Repeated adapter wakes that the adapter then refuses end in a stall resync.
   - For path 1, the preferred fix is that Core does not poll the snapshot boundary while the session holds output. The boundary result stays intact until the existing routes have room. The fallback, for any overflow that remains, asks for the recovery capture only after the route queue drains, and not while the queue is still full (`client_worker.rs:1354`).
   - **The PTY tail must survive backpressure (macOS).** A macOS PTY discards output that is still queued when the last slave descriptor closes. Under backpressure, a program that writes a lot and then exits therefore loses the end of its output. The worker keeps its own slave descriptor open until it has drained the master. This belongs to C1, because backpressure is what exposes it.
3. **`journal_advanced` is returned, not polled** (`daemon.rs:1004, 4329`). Until S10, the data-plane thread reads `take_journal_advanced_wake()` after every pump and every request batch, as it does today. It is the only thread that can set the bit, so no set is missed. It raises `Signal(JournalAdvanced)` when the bit was set. S4 depends on this and keeps it, and it deletes only the `progressed` wake. S10 moves the bit into Core's pump result, so no host has to remember to read it.
4. **`exit_hold` derives from capture state** (`engine/botster.rs:1934`). Today 9 call sites mirror it by hand with `sync_exit_hold`.
5. **The worker egress reports a lost socket.** It does not discard silently (`botster-session-worker.rs:1693-1705`).
6. **Bound-queue wakes are delivered, not discarded.** `pump_woken` (`daemon.rs:1389`) and the managed `pump_woken_phase_three` discard `take_bound_queue_wake_sessions`. But `start_resync_captures` runs for the resync requests of every session, not only the sessions in the batch. It can queue route frames for a session outside the batch, and the discard then drops that session's queue wake. This is a suspected lost wake. The Core writer is chasing a stall that may be it. The fix calls `notify_session` for each taken session.

### 2.5 Mapping of today's mechanisms

| Mechanism | Fate |
| --- | --- |
| `ReadyQueues` / `ReadyClass` round-robin | **Kept.** It is the run queue. Classes become sources. |
| `MaintenanceWakes` (9 bits), `mark_all` (29 sites), `try_wake` (11 sites), dead `needs_work` | **Deleted.** Each slice becomes an `OwnerSource`. A source that stops at the turn budget returns `Outcome::Progress` and stays `Ready`, which is a legal continuation. |
| Doorbell `ControlMessage` variants: `CoreCompletionPublished`, `HostProgressPublished`, `PluginCompletionPublished`, `PluginResultCapacityReleased`, `CausalProgressPublished`, `EntityPublishProgress`, `CoordinationProgress`, `EntitySubscriptionCapacityReleased`, the data-plane progress wake | **Replaced** by `OwnerSignal` keys, with the same producers. The control channel carries client requests and cleanup only. |
| `DataPlaneProgress.progressed` (an owner wake after every pump batch) | **Deleted.** Terminal output costs no owner wake. `journal_advanced` and `inventory_changed` stay as keys. |
| Owner flags `host_completion_drain_pending`, `host_capacity_wake_pending`; `publication_owner` `waiting_for_host/owner/progress`; `event_owner` equivalents; the event-plane `Cell`; spawner pending bits | **Deleted.** Readiness derives from queue state and the named waits. |
| `publication_owner.recovery` | **Live hang.** It is set on a submit failure and is never cleared outside terminal dispose (`publication_owner.rs:109-117`). Publication then stops for good. S4a makes recovery a source with a defined exit: it retries the failed submission as a keyed completion, or it faults the plugin's publications with a typed error. |
| Package-event `delivery_wake` (atomic, no doorbell) | **Replaced** by `OwnerSignal(PackageEvents)`. This fixes the lost wake after an off-owner emit. |
| Core completion notifier | **Kept** as Core mechanism, with the meaning in 2.4.1. Hub installs `raise(PluginEngine)`. |
| `admission_retry_armed` (Core) | **Kept** inside Core as the arming detail of 2.4.1. Hub never sees it. |
| Capability event notifier and `next_deadline` (Core `2cda9e9`) | **This plan owns the wake plumbing (step S4; orchestrator decision, 2026-09-27).** Hub installs `set_event_notifier` as `raise(CapabilityEvents)`, and it adds `next_deadline` to the owner's `DeadlineIndex`. The orchestrator paused this wiring in plugin-platform slice 3 because this redesign rewrites the plumbing. The plugin platform keeps the consumer: draining the events and resuming handlers (its §4.2). Today the results have no production consumer (`drain_capability_events` has test callers only). |
| Host executor `HostWake` (`completion_pending`, `capacity_pending`) | **Kept** as the producer side. It raises `HostCompletion` and `HostCapacity`. |
| Publication sweep (`publication_owner.ready` every iteration) | **Deleted.** It is a source with derived readiness. |
| Causal sweep (`causal_wake_through` walks every family-cleanup, causal, and model waiter; `owner_loop.rs:875-929`) | **Deleted.** Each waiter moves to its own kind (rule 2.1.5). See the note below the table. |
| Owner-permit `budget.released` harvest | **Kept** as a `Local` wait: the permit table is owner state. |
| Core terminal wake pump (targeted, CAS-coalesced, lossless overflow) | **Kept.** It already meets this contract. |
| Process host (Core, not yet wired into Hub): `install_ingress_notifier`, `install_exit_notifier` (`plugin_process/host.rs:285-300`), `DeliveryPool::install_unit_returned` (`plugin_delivery_pool.rs:319`) | Hub installs `raise(PluginIngress(plugin))`, `raise(PluginExit(plugin))`, and `raise(PluginCredit(plugin))`. `DeliveryPool::close()` clears units without firing (`:245-250`). That is correct only because generation retirement retires every credit waiter of that generation at once (plugin-platform §4.5.4). A test proves it: a waiter parked at unload is answered `cancelled`. |

**Causal sweep replacement (S4b).**
- **Model waiters** wait for the exclusive entity model. They form a FIFO on the model, released when the active `Work` ends. A model fault makes every waiter `Ready`, and each then fails typed.
- **Causal waiters** (`CausalTransitionStatus::Waiting`, `entities/worker.rs:895-960`). S4b types `Waiting` with its cause. A wait for table capacity joins the causal-slot FIFO. A wait for a specific receipt is a keyed completion on that receipt.
- **Family-cleanup waiters** are keyed completions on their cleanup phase.
- **Tests:** completion out of order, where a later receipt completes first and runs with no other wake; cancellation of a FIFO head; a model fault with waiters parked.

## 3. Thread and process boundaries

### 3.1 Terminal output frame (PTY read → client socket write)

These are logical handoffs, where work moves from one thread or task to another. They are not necessarily OS thread switches: two tokio tasks can share a worker thread.

Today (Core main): **Unix 5 handoffs, WebRTC 6.**
1. PTY reader thread → worker main thread.
2. worker main thread → worker egress writer thread.
3. worker process → Hub process (socket).
4. parent reader thread → data-plane thread.
5. data-plane thread → transport task (slot + `Notify`).
6. WebRTC only: channel task → rtc driver task.

The Hub owner is not on the byte path. It is woken once per pump batch, and step S4 removes that wake.

**Remove 1 and 2 (step S9).** The session worker becomes one thread with one `poll` over the PTY fd, the control socket, and the egress socket. This also removes the unbounded control mpsc: reads stop when the main loop cannot accept. It also removes the coupling where blocked output stops input. And it deletes two of the three 64-deep stages.

**Remove 4 later (step S11).** The data-plane thread polls worker sockets directly instead of one reader thread per session. It reads and pumps on the same thread, so handoff 4 disappears. The reactor moves bytes only and carries no Hub policy.

**Keep 3, 5, and 6.** Step 3 is the process boundary. Step 5 keeps socket ownership with the task that multiplexes the connection. Step 6 is inside the rtc library.

After: **Unix 3 (2 after S11), WebRTC 4 (3 after S11).**

### 3.2 Plugin entity publication (Lua call → client socket write)

Scope of the count: one publication, one Unix recipient, and `Advance` running once. `Advance` can repeat, and `Deliver` runs once per recipient. WebRTC adds its channel → rtc handoff (+1). The Host → owner completion after the final `Deliver` is off the path to the socket write.

Today (in-process Lua), under that scope: **11 to 13 serialized handoffs**, not 8. Plan §4.5 counts only to the Lua reply. The path:
- Lua → owner.
- owner ↔ Host for `Admit`, `Advance`, and `Reply`.
- owner ↔ Host for `TakeFanout`, `PrepareMutation`, and `Deliver`.
- Host → connection task.

Retirement adds about 8 wakes off the critical path. Only one fanout runs at a time Hub-wide (`entities.rs:853-860`).

**Fuse the Host round trips (step S8).** One Host job applies a batch of publications and takes, prepares, and delivers their fanout. The owner admits, and the Host does the work. `PrepareMutation` is a trivial wrap (`plugin_entity.rs:454-458`) and does not need its own round trip. Plan §4.5.2 batches the apply but leaves the fanout half as it is. This step completes that work. The plugin platform writer agreed to it on the following conditions, which keep the §4.5 contracts:
- **Credit returns at apply.** `release_call` and the returned pool unit happen when a publication is applied or discarded, never after delivery. A slow client socket must not throttle the plugin's pool.
- **The barrier counts applied.** The job reports "applied through seq N" separately from delivery. A held `Reply` never waits on fanout delivery.
- **Progress and budget.** Each activation applies at least one publication, and a partial batch marks itself ready again (§4.5.2). The job keeps to the owner turn budget. Delivery stays nonblocking per client: a full client queue is a per-client outcome (resync), never a stall for the batch.
- **Failure mapping.** An apply failure maps to its `publication_seq` for a held `Reply`. The job skips the failed publication's fanout, and the rest of the batch continues.

After S8, in-process: **3** on the path (Lua → owner → Host → connection). On the process host: **5** (child Lua → child writer → parent reader → owner → Host → connection, with one of these the process crossing). S11 does not reduce this count: it replaces the parent reader thread with the data-plane reactor, but the reactor → owner handoff remains. This plan claims no further reduction. Credit return (`release_call`) stays off the path.

### 3.3 Other boundaries

- **Owner ↔ data plane** (`CoreRequest` channel of 64 plus the interrupt). This boundary is kept. `CoreDaemon` is `!Send`, and it keeps terminal bytes off the policy thread.
- **Owner ↔ Host executor** (2 threads). This boundary is kept for bounded heavy work. Jobs become coarse (S8).
- **Lua VM threads** (2 executors per plugin). They are kept until the process host replaces them.

## 4. Staged migration

Every step lands alone, keeps the strict gates green (fmt, clippy `-D warnings` at or below main's count, Rust 1.97.0), and keeps the live lanes green:
- TUI T-S10 and T-S12;
- Web flood, worker-lost, and workspaces-lifecycle.

"Ablation" means that the named test fails with the fix reverted.

| Step | Repo | Content | Fixes | Test | Size |
| --- | --- | --- | --- | --- | --- |
| **C1** | Core | 2.4.2 source backpressure, the capture-boundary hold, and the PTY tail hold (the Core writer's step, in progress); 2.4.5 egress reports a lost socket | flood storm; resync at a capture boundary; lost output tail at exit on macOS; silent egress drop | the Core probe (0 resyncs under flood); "attach while held" gives 0 resyncs (today 1); a program that floods and exits delivers its whole tail; a reader with no progress ends `Stalled` at the deadline; flood lane | in progress (+0.25 d for 2.4.5) |
| **C2** | Core | 2.4.1 notifier edges: arm on `Backpressured` as for `LockBusy`, and fire `wake_armed_admission` at a class-slot free (`take_dispatchable`, unqueue) and at every completion-reservation return (`drain_completions`, `close_fund`); `Backpressured` carries its cause, a cold cut whose Hub struct-literal sites are `runtime.rs` `force_plugin_admit_backpressure` and `plugin_entity.rs` `force_backpressure`; the notifier conformance test; 2.4.6 `notify_session` instead of the discard | `Backpressured` has no retry event; completion capacity returned by an unload wakes no one; suspected lost bound-queue wake | an armed class-cause admission is notified when a worker dequeues; plugin A holds unused delivery funds, plugin B parks on the completion cause, A unloads with zero drained completions, and B is notified (ablation: no fire in `close_fund`); a drain notifies an armed completion-cause admission; a retired completion returns its reservation when drained; a refused `try_admit` fires nothing; a resync capture for a session outside the batch is pumped with no other wake (ablation: discard) | 1 d |
| **S0** | Hub | Land the roll branch (b870bca3, stale close, NotSubscribed), rolled to a Core that holds C1 and C2 | cursor lost wake; silent close on Stale | existing branch tests; full lifecycle target | existing work |
| **S1** | Hub | `Readiness`, `Wake`, `Seen`, `Outcome`, and `OwnerSignal` (with its per-key interleaving test harness, 2.2); convert package-event delivery | `mark_all` spin on Backpressured; emit lost wake; `LockBusy` drop | interleaving tests for `PluginEngine` (release before the final attempt, release between refusal and return) and `PackageEvents` (emit from a request-response handler, with no other wake); `Backpressured` parks with zero owner turns until C2's edge fires (ablation: `mark_all`); a `LockBusy` event is delivered, not retired; the completion-store orderings of 2.3, each with no other wake: (a) the store is full, the completion signal is consumed, the admission parks, and the drain frees the store; (b) plugin A holds unused funds, plugin B parks, and A unloads with zero drained completions. In both, the admission then runs. | 1.25 d |
| **S2** | Hub | Session-family admission as a source with a maintained ready set (rule 2.1.6); `LockBusy` waits | gap and baseline on contention; b870bca3 class removed | contention on a session-family frame gives no gap (ablation: old arm); a frame queued before the cursor is admitted with no other wake (the b870bca3 test); the debug recompute of the ready set; Web workspaces-lifecycle lane | 0.5 d |
| **S3** | Hub | `ShedBusy` carries the refusing lock; the four locks raise their own keys on guard drop; a poisoned lock is a terminal fault; `SubscribeEvents` waits on the named key; `bind_reader` stops signalling a bound reader | `shed_busy` on first subscribe; poison read as busy | for each of the connections, slots, pool, and router locks held separately: zero owner retries while it is held, and exactly one successful retry after release with no other wake (ablation: arm the slots key for a pool refusal, and the retry loop appears); a poisoned lock ends typed; the three lifecycle tests named in the triage report | 0.75 d |
| **S4a** | Hub | Convert the lifecycle slices (Observe, journal pull, projection apply, baseline, HostBridge) and the pump and reconcile; add the `DataPlaneCapacity` producer at request dequeue; publication `recovery` with a defined exit; delete `DataPlaneProgress.progressed` (keeping `journal_advanced`, 2.4.3) and the `mark_all` sites of these slices | the `Refused` maintenance-read stall; the `Refused` pump spin; the permanent publication `recovery` hang; the Observe `BudgetTooSmall` spin (rule 2.1.7); one owner wake per pump batch | full Core request queue with no incidental wake: the read runs after one dequeue (ablation: no raise); interleaving tests for `DataPlaneCapacity`; a submit failure followed by recovery (ablation: today's flag); owner-loop idle test with zero turns while idle | 1.25 d |
| **S4b** | Hub | Convert subscriber delivery, the publication and event owners, the causal and model waits (the 2.5 mapping), and the capability-event plumbing; delete `MaintenanceWakes`, the remaining `mark_all` and `try_wake`, the doorbell variants, the hand flags, and both sweeps | causal herd; incidental wakes; capability-event wake (plugin-platform consumer) | out-of-order receipt completion; FIFO head cancellation; model fault with parked waiters; interleaving tests for `HostCapacity`, `PluginResultCapacity`, and `CapabilityEvents`; flood lane with an owner-turn count (`measurement-window`); zero progress faults across the full lifecycle target | 1.25 d |
| **S5** | Hub | Input backpressure: the Unix connection stops reading while adapter ingress is full; the WebRTC channel stops draining, so the SCTP window closes | route ended by a paste burst | paste burst larger than 64 frames on a stalled data plane is delivered whole on both transports (ablation: drop + `Lost`) | 1 d |
| **S6** | Hub | WebRTC: check the watermark before each 12 KiB chunk. This bounds what the Hub submits, so the rtc send buffer and SCTP queue stay at most the high watermark plus one chunk; the rtc types stay unbounded, but the Hub no longer feeds them past that. Unix: take from the entity channel only when `queued_events` is empty, and publish the capacity wake at that dequeue; the 256-frame channel is then the real bound | two unbounded queues | an oversize frame on a stalled peer stays under the aggregate plus one chunk; with the socket stalled and one frame partly written, the test fills the channel to 256 and holds one frame in `queued_events`. The next frame gets the channel's existing outcome (subscriber DROP+RS, `subscriber_overflow`). The test asserts the total: channel plus `queued_events` plus the partly written frame is at most 256 + 1 + 1 | 0.5 d |
| **S8** | Hub | Fused Host job: apply batch, fanout, prepare, deliver | 11-13 → 3 wakes per publication | 512-publication test by gates, not by clock (plan §4.5.8); wake count assertion; a stalled client socket does not delay credit return or a held `Reply` (ablation: release after delivery) | 1 d |
| **S9** | Core | Single-threaded session worker `poll` loop. The Core writer sees no blocker, but it is a rewrite: the reader thread carries the mode barrier and the snapshot-barrier fence (reader pause, residual drain); egress needs nonblocking writes with `POLLOUT`; Ghostty model work shares the thread, which adds latency under a flood | 2 crossings; unbounded control mpsc; input blocked behind output | existing worker process tests; the barrier tests; input applied while egress is full; flood-lane input latency | 2 to 3 d |
| **S10** | Core + Hub | Derive `exit_hold` (2.4.4); `journal_advanced` as a wake (2.4.3) | hand-mirrored flags | post-exit capture tests; journal pull with no host poll | 0.5 d |
| **S11** | Core | Data-plane fd reactor for worker and plugin-process sockets | 1 crossing per session; one reader thread per worker | worker process and plugin process suites | 2 to 3 d |
| **S12** | Core | Coalesce adjacent `Output` frames in route egress | a line-buffered flood (`seq` to a tty) makes 7-byte frames, so the 64-frame route bound limits throughput, not the 4 MiB byte bound | a line-buffered flood fills the route by bytes, not by frame count; frame order and input-result order are kept | 0.5 d |

### 4.1 Cutover split

**User decision (2026-09-27): every step, S6 to S12 included, lands before cutover, without lowering quality.** The split below remains the priority order: the first table fixes live defects.

The total is about 13.75 to 15.75 writer-days of new work.

**Must land before cutover: about 7.25 writer-days of new work.** These steps fix every live defect in the catalogue.

| Step | New work | Why before cutover |
| --- | --- | --- |
| C1 | in progress, plus 0.25 d | flood storm; resync at a capture boundary; lost output tail at exit (macOS); silent egress drop |
| C2 | 1 d | `Backpressured` has no retry event; completion capacity returned at unload wakes no one; suspected lost bound-queue wake (2.4.6) |
| S0 | existing branch | lost cursor wake; silent close on Stale |
| S1 | 1.25 d | event spin; lost emit wake; `LockBusy` drops an event |
| S2 | 0.5 d | contention opens a gap and a full baseline |
| S3 | 0.75 d | `shed_busy` on the first subscribe; a poisoned lock read as busy |
| S4a | 1.25 d | the live `Refused` maintenance-read stall (1.5 item 7); the permanent publication `recovery` hang; the `Refused` pump spin; the owner wake per pump batch |
| S4b | 1.25 d | deletes the incidental wakes that hid the cursor defect; causal herd; capability-event wake |
| S5 | 1 d | a paste burst ends the route |

Every step in this list fixes a live defect. If time forces a cut, only S4b can move after cutover without leaving a known hang: its defects waste CPU (the causal herd and incidental wakes), and the capability-event wake has no production consumer until plugin-platform slice 3. S4a stays before cutover, because it fixes two hangs.

**Second priority: about 6.5 to 8.5 writer-days.**

| Step | Size | Note |
| --- | --- | --- |
| S6 | 0.5 d | two unbounded queues (WebRTC chunks, Unix `queued_events`) |
| S8 | 1 d | aligns with plugin-platform slice 3 |
| S9 | 2 to 3 d | single-threaded session worker; a rewrite (see the table) |
| S10 | 0.5 d | derived `exit_hold`; `journal_advanced` as a wake |
| S11 | 2 to 3 d | follows the process host |
| S12 | 0.5 d | coalesce small `Output` frames |

### 4.2 Order constraints

- **C1 must reach Hub in the same roll as Core `2845aec`.** The roll branch (`85b3507`) already contains `2845aec`. Landing S0 without C1 brings the flood storm into the Hub build. The orchestrator confirms that the roll already waits on C1.
- **S1 lands in two parts.** S1a (the types, `OwnerSignal`, the interleaving harness, and the `PackageEvents` emit key) needs no Core change. S1b (the `PluginEngine` waits for `Backpressured` and `LockBusy`) requires C2 through S0: the incidental completion notification is not a retry edge, so S1b does not land before C2.
- **S3 and S4a require S1a**, the types and the test harness. **S2 requires S1b**, because it uses `LockBusy`. S4b follows S2, S3, and S4a, so that it only deletes.

### 4.3 Execution (orchestrator assignment, 2026-09-27)

Each writer lands their own steps on main once their reviewer accepts and the landing procedure passes. Edits to `src/daemon/owner_loop.rs` are serialized through the owner-loop lead. Other writers branch after the lead's step lands, or they agree the edit with the lead first.

| Step | Owner | Starts after | Reaches Hub by |
| --- | --- | --- | --- |
| C1 | Core writer (006c) | now | S0 |
| C2 | Core writer (006c) | now, in parallel with C1 | S0 |
| S0 | Foundation writer (0058) | C1 and C2 on Core main | Hub main |
| S1a | owner-loop lead (Claude architect, 008c) | now | Hub main |
| S3 | owner-loop lead | S1a | Hub main |
| S4a | owner-loop lead | S3 (serial on `owner_loop.rs`) | Hub main |
| S1b | owner-loop lead | S0 and S4a | Hub main |
| S2 | owner-loop lead | S1b | Hub main |
| S4b | owner-loop lead | S2 | Hub main |
| S5 | Foundation writer | S0 | Hub main |
| S6 | Foundation writer | S5 | Hub main |
| S8 | Plugin platform writer (0079) | S4b | Hub main |
| S9, S12, S10 (Core half) | Core writer | C1 and C2 | a Core pin roll by the Foundation writer |
| S11 | Core writer | S9 | a Core pin roll by the Foundation writer |
| S10 (Hub half) | owner-loop lead | the roll with the S10 Core half | Hub main |

The plugin platform writer also resumes its paused consumer of capability events after S4b, which installs the plumbing. Every Core step reaches Hub through a pin roll that the Foundation writer owns.

## 5. Open questions for review

1. **Client `Backpressured` on plugin control.** This plan keeps the typed refusal for capacity. The alternative is to park the request under its existing deadline.
2. **Source-predicate cost.** Rule 2.1.2 re-evaluates about 20 coarse source predicates after every owner step. A source with many rows (for example per subscriber) must keep an internal ready index (rule 2.1.1). Keyed completions are never swept.
