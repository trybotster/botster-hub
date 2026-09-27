# Readiness and flow control

Status: draft 2, 2026-09-27. Design only. No production code changes in this phase.
Scope: botster-hub at `1ec61b94`, and botster-core at `origin/main` `19edeb1`. Hub pins Core `549b3f62`. The unlanded roll branch `delivery/core-roll-85b3507-20260927` moves the pin to `85b3507`.

## 0. Summary

One defect class keeps coming back: a lost wake, a spin, or a "work exists" predicate used as a "can progress now" predicate. The cause is not one bad site. The Hub owner already has a correct run queue (`ReadyQueues`, `src/daemon/owner_schedule.rs`). But about ten side channels bypass it:
- a hand-kept wake bit set;
- hand-kept `waiting_for_*` flags;
- a bit plus a doorbell message, one pair per producer;
- refusals that spin, drop, or open a gap.

Each side channel needs every writer to remember to wake the right party. The design removes that duty:
1. **One readiness contract.** Every owner work source answers `readiness() -> Ready | Wait(Wake)`. The function derives the answer from state. `Wait` must name the event that makes the source ready. The type does not allow a wait without a named event.
2. **One wake primitive.** Every cross-thread producer uses one `OwnerSignal` type. The owner re-evaluates owner-local waits after every step, so owner-local state needs no signal at all.
3. **One refusal rule.** Contention (a busy lock) never becomes a refusal, a drop, or a gap. It becomes `Wait` on the release event. Capacity may refuse, and the refusal is typed and names its retry event.
4. **End-to-end terminal backpressure.** This follows the orchestrator ruling of 2026-09-27, and the Core writer is building it. When the slowest progressing bound reader has no egress room, the worker stops draining the PTY. A reader with no progress for `READER_PROGRESS_DEADLINE` (10 s) is ended with `Stalled`. This ends the flood resync storm without a new limit.

The design deletes `MaintenanceWakes`, `mark_all`/`try_wake` (29 + 11 production sites), about nine doorbell message variants, the hand flags, and the causal sweep. It adds one small type and one Core notifier edge.

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
| apply → control queue | 32 frames (2 reserved); 30 pending resizes | BP (park for capacity, woken by the writer's `freed_capacity`) |
| control writer → worker | **process**; 2 s write deadline | control plane sealed → `WorkerLinkFailed` |
| worker control reader → main loop | **UNB** `mpsc::channel()` (`botster-session-worker.rs:236`); bounded upstream only by the 32-frame queue | — |
| main loop → PTY write | 32 ops / 2 MiB keyed (duplicates the intake session lane); keyless input and Ghostty query replies **UNB** | REF for keyed ops |

Coupling: while the worker main loop is blocked on a full egress lane, it applies no input.

### 1.3 Plugin → Hub → clients

| Path | Bound | At the bound |
| --- | --- | --- |
| host call (coordination) | Lua callback account | REF (`Capacity`); Lua parks `recv_timeout(1 s)` |
| async capabilities (fs, store) | 128 ops / 256 events per plugin | REF (`Backpressured`). Results go to an **UNB** mpsc with **no production consumer** (`drain_capability_events` has test callers only) |
| entity publication | one shared bridge: 256 entries / 8 MiB / 1 MiB each (`entity_publish.rs:16-18`) | REF (`NeverQueued`). One plugin can exhaust it for all (plan §4.5.1 moves this to per-plugin pools) |
| fanout → subscriber | Unix 256, WebRTC 64 per subscriber; 64 MiB `SharedViewBudget` | DROP+RS (`subscriber_overflow`), paced by `PACKAGE_ENTITY_*` |
| Unix entity/event write queue | `MuxWriteState.queued_events` **UNB** (`mux_write.rs:35`) | — |
| plugin logs | none on main (planned ring: 256 records / 512 KiB, pull-only) | planned: evict oldest, counted |

### 1.4 Hub events → plugins and clients

| Hop | Bound | At the bound |
| --- | --- | --- |
| `try_ingress` (router) | 64 KiB payload; producer 256 / 512 KiB; 100/s burst 200; global 16 MiB | REF (`EventPlaneStatus`) |
| router → client mailbox | 128 / 2 MiB | DROP + gap bit (client resyncs) |
| router consumer queue → Core Background queue | router 128 / 2 MiB, then Core 256 / 1 MiB per plugin | **Spin:** `Backpressured` requeues and calls `mark_all` (`daemon_maintenance.rs:1453-1461`). `LockBusy` and other results **retire the event (DROP, no resync)** |
| off-owner emit (Lua `botster.events.emit`) | — | sets `delivery_wake` with **no doorbell**; delivery waits for an unrelated wake |

### 1.5 Missing links

1. **Terminal output has no backpressure from the client to the PTY.** The lossless chain stops at the route queue. That queue drops and resyncs instead of stalling its producer. Before Core `2845aec`, the 1 ms reader sleep hid the gap by capping output near 1 MB/s. Without the sleep, the result is the flood storm: 3-15 resyncs in 3 s and about 50 times the byte rate (`botster-evidence/core-residual-waits-20260927/flood-bisect-log.txt`). Overflow also asks for a capture at once, while the queue is still full, so recovery can overflow again.
2. **Terminal input drops at adapter ingress.** A paste burst faster than the data plane ends the route. The transport should stop reading instead. The kernel socket buffer and the SCTP window then carry the backpressure to the client.
3. **Contention becomes loss.** `LockBusy` on a package event drops the event. On a session-family frame it opens a gap and starts a full baseline resync (`daemon_maintenance.rs:1247-1250`). On a plugin control request it refuses the client (`plugins.rs:550-563`). `SubscribeEvents` returns `shed_busy` when the owner loses a try-lock race with the connection's own reader (`package_events.rs:687-707`).
4. **Core plugin `Backpressured` has no retry event.** Class-queue space frees when a worker dequeues, and nothing notifies the host (`contract/actor.rs:1326-1330`).
5. **Unbounded queues:** the WebRTC rtc send buffer, the Unix `queued_events`, the worker control mpsc, keyless PTY input, and the async capability result mpsc.
6. **Silent drops:** the worker egress after a socket error, and the event retire on `LockBusy`.

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
    Local,              // owner-local state; re-evaluated after every owner step
    Signal(SignalKey),  // a cross-thread producer raises this key
    Deadline(Instant),  // only with an approved `timer:` marker at the source
}

trait OwnerSource {
    fn readiness(&self, owner: &OwnerView) -> Readiness;
    fn run(&mut self, owner: &mut OwnerCtx, budget: &mut OwnerTurnBudget) -> Progress;
}
```

Rules:
1. `readiness` is a function of state. A source may keep an internal index, for example "subscribers with a pending frame and capacity". Only the mutators of the underlying state may change that index. A debug check recomputes the index from scratch after each `run` in tests.
2. **No spin.** If `readiness` returned `Ready`, `run` must return `Progress::Made`. `Progress::None` after `Ready` is a fault. Debug builds assert it, and a counter records it in release builds. The owner-loop tests assert that the counter is zero. A source that stops at the turn budget has made progress. It returns `Ready` again, which is a legal continuation.
3. **Every wait names its event.** `Wait` cannot be built without a `Wake`. A refusal that crosses a boundary (Core, Host executor, transport) carries the `Wake` that ends it.
4. **Capacity waits use FIFO.** A source that waits for a shared capacity (causal slots, Host permits, plugin credit) is queued per capacity. The release of one unit re-checks the head only. This rule replaces the causal sweep, which walks every waiter.
5. **Cursors do not decide readiness.** A round-robin or paged cursor only chooses the order of work. Readiness comes from a maintained ready set, for example "consumers with a pending frame, not in flight, and with a handler". A pass that reaches the end of a page must wrap within the pass or leave the source `Ready`. A pass that returns "nothing" at a page end while the ready set is not empty is the lost-wake shape of b870bca3. Session-family admission needs such a ready set. The family gap pass and prune already keep flags that suffice.
6. **A refusal that cannot change is a fault, not a retry.** If a unit of work can never fit its bound (for example Observe `BudgetTooSmall` for one row at the constant `OBSERVE_SLICE_BUDGET`, `daemon_maintenance.rs:989`), the source fails that unit with a typed error. It does not stay `Ready`. Today the site calls `mark_all` and resubmits every turn, which is a permanent spin if Core can return that result for a single row.

### 2.2 Why no wake is lost, by construction

- **Owner-local state.** The owner is the only mutator, so the scheduler re-evaluates every `Wake::Local` waiter after every `run`. A mutation cannot happen without a following re-evaluation. The cost is one O(1) predicate per local waiter per step. About 20 sources exist, so the cost is small. The b870bca3 class of bug (a cursor reset with no wake) cannot occur: no wake exists to forget.
- **Cross-thread state.** A producer mutates, then calls `OwnerSignal::raise(key)`. `raise` sets the key's bit and sends at most one doorbell per owner sleep. The owner takes all raised bits before it evaluates, and it blocks only on the doorbell channel, which returns at once if a doorbell is pending. So a raise either comes before the take (the owner sees it) or after it (the doorbell wakes the owner). This is the ordering the Hub bit-plus-doorbell pairs already use. The design makes it one type, with one test, instead of about nine copies.
- **Deadlines** come from the existing `DeadlineIndex`.

The owner's existing invariant stays: it never blocks while a ready row exists. Rule 2 makes that invariant safe, because a ready row always makes progress.

This is the `Future::poll` contract ("`Pending` must have registered a waker"), made explicit in a return type and without async. The owner's budgeted, synchronous turn stays as it is.

### 2.3 The refusal rule, per boundary

| Refusal | Meaning | Caller does |
| --- | --- | --- |
| Core `LockBusy` | contention | `Wait(Signal(PluginEngine))` |
| Core `Backpressured` (class or completion store) | capacity | internal work: `Wait(Signal(PluginEngine))`; client request: typed refusal (unchanged) |
| Core `ControlQueueFull` / `PendingLimit` | capacity | park on the session wake (already correct for input) |
| Core `CoreTicketPoll::Refused` (request channel full) | capacity | `Wait(Signal(DataPlaneCapacity))`. Today pump and reconcile return `Runnable` and spin (`owner_loop.rs:1726, 1812`), and maintenance reads return with no wake (`daemon_maintenance.rs:955, 1004, 1111`) |
| Host permit refused | capacity | `Wait(Signal(HostCapacity))`, FIFO |
| Router `ShedBusy` / slot try-lock lost | contention | `Wait(Signal(ConnectionSlots(conn)))`, raised by the guard drop |
| subscriber queue full | capacity | DROP+RS (unchanged: entity state resyncs from the model) |

### 2.4 Core contract changes (mechanism only; no policy moves into Core)

1. **One plugin-engine notifier meaning.** "Plugin engine state changed. A completion may be ready, or a refused admission may now succeed." Core fires it on: a completion published (today); an armed lock release (today, `admission_retry_armed`); and **a class-queue slot freed while a `Backpressured` admission is armed (new)**. Completion-store space frees only through the host's own drain, so it is owner-local. A conformance test in Core pins these edges, so a later Core commit cannot move the contract silently (defect: 1c2e526 changed the notifier's meaning).
2. **Source backpressure for terminal output.** This is the orchestrator ruling of 2026-09-27, and the Core writer is building it.
   - When the slowest progressing bound reader has no egress room, the worker stops draining the PTY. The kernel PTY buffer then blocks the child.
   - A reader with no progress for `READER_PROGRESS_DEADLINE` (10 s, an existing bound) is ended with `Stalled`. A stalled reader therefore cannot hold the session for longer than that.
   - A route at a capture boundary is exempt, so a capture always completes.
   - A Core probe of this fix shows 0 resyncs under flood.
   - In the readiness terms of this plan, the session's PTY drain is `Wait(route egress room)`. Its negative state names its event, and the deadline is the existing `timer: deadline`.
   - **Possible second part (pending the Core writer):** if a bound, progressing route can still overflow, overflow recovery should ask for its capture only after the route queue drains, and not while the queue is still full (`client_worker.rs:1354`). The step includes this part only if the Core writer confirms that the overflow path is still reachable.
3. **`journal_advanced` becomes a wake, not a polled bit** (`daemon.rs:1004, 4329`). The data plane includes it in its signal. Today the host must remember to poll it.
4. **`exit_hold` derives from capture state** (`engine/botster.rs:1934`). Today 9 call sites mirror it by hand with `sync_exit_hold`.
5. **The worker egress reports a lost socket.** It does not discard silently (`botster-session-worker.rs:1693-1705`).

### 2.5 Mapping of today's mechanisms

| Mechanism | Fate |
| --- | --- |
| `ReadyQueues` / `ReadyClass` round-robin | **Kept.** It is the run queue. Classes become sources. |
| `MaintenanceWakes` (9 bits), `mark_all` (29 sites), `try_wake` (11 sites), dead `needs_work` | **Deleted.** Each slice becomes an `OwnerSource`. Budget continuation is `Ready` after `Progress::Made`. |
| Doorbell `ControlMessage` variants: `CoreCompletionPublished`, `HostProgressPublished`, `PluginCompletionPublished`, `CausalProgressPublished`, `EntityPublishProgress`, `CoordinationProgress`, `EntitySubscriptionCapacityReleased`, the data-plane progress wake | **Replaced** by `OwnerSignal` keys. The control channel carries client requests and cleanup only. |
| `DataPlaneProgress.progressed` (an owner wake after every pump batch) | **Deleted.** Terminal output costs no owner wake. `journal_advanced` and `inventory_changed` stay as keys. |
| Owner flags `host_completion_drain_pending`, `host_capacity_wake_pending`; `publication_owner` `waiting_for_host/owner/progress`; `event_owner` equivalents; the event-plane `Cell`; spawner pending bits | **Deleted.** Readiness derives from queue state and the named waits. `publication_owner.recovery` is never cleared today (publication stops for good after one submit failure); its readiness becomes explicit. |
| Package-event `delivery_wake` (atomic, no doorbell) | **Replaced** by `OwnerSignal(PackageEvents)`. This fixes the lost wake after an off-owner emit. |
| Core completion notifier | **Kept** as Core mechanism, with the meaning in 2.4.1. Hub installs `raise(PluginEngine)`. |
| `admission_retry_armed` (Core) | **Kept** inside Core as the arming detail of 2.4.1. Hub never sees it. |
| Capability event notifier and `next_deadline` (Core `2cda9e9`) | **This plan owns the wake plumbing (step S4).** Hub installs `set_event_notifier` as `raise(CapabilityEvents)`, and it adds `next_deadline` to the owner's `DeadlineIndex`. The orchestrator paused this wiring in plugin-platform slice 3 because this redesign rewrites the plumbing. The plugin platform keeps the consumer: draining the events and resuming handlers (its §4.2). Today the results have no production consumer (`drain_capability_events` has test callers only). |
| Host executor `HostWake` (`completion_pending`, `capacity_pending`) | **Kept** as the producer side. It raises `HostCompletion` and `HostCapacity`. |
| Publication sweep (`publication_owner.ready` every iteration) | **Deleted.** It is a source with derived readiness. |
| Causal sweep (`causal_wake_through` walks every waiter) | **Deleted.** FIFO capacity waits (rule 2.1.4). |
| Owner-permit `budget.released` harvest | **Kept** as a `Local` wait: the permit table is owner state. |
| Core terminal wake pump (targeted, CAS-coalesced, lossless overflow) | **Kept.** It already meets this contract. |

## 3. Thread and process boundaries

### 3.1 Terminal output frame (PTY read → client socket write)

Today (Core main): **Unix 5 crossings, WebRTC 6.**
1. PTY reader thread → worker main thread.
2. worker main thread → worker egress writer thread.
3. worker process → Hub process (socket).
4. parent reader thread → data-plane thread.
5. data-plane thread → transport task (slot + `Notify`).
6. WebRTC only: channel task → rtc driver task.

The Hub owner is not on the byte path. It is woken once per pump batch, and step S4 removes that wake.

**Remove 1 and 2 (step S9).** The session worker becomes one thread with one `poll` over the PTY fd, the control socket, and the egress socket. This also removes the unbounded control mpsc: reads stop when the main loop cannot accept. It also removes the coupling where blocked output stops input. And it deletes two of the three 64-deep stages.

**Remove 4 later (step S11).** The data-plane thread polls worker sockets directly instead of one reader thread per session. Plugin process readers can use the same reactor.

**Keep 3, 5, and 6.** Step 3 is the process boundary. Step 5 keeps socket ownership with the task that multiplexes the connection. Step 6 is inside the rtc library.

After: **Unix 3 (2 after S11), WebRTC 4 (3 after S11).**

### 3.2 Plugin entity publication (Lua call → client socket write)

Today (in-process Lua): **11 to 13 serialized wakes**, not 8. Plan §4.5 counts only to the Lua reply. The path:
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

After, in-process: **3** on the path (Lua → owner → Host → connection). After, on the process host: **5** (child Lua → child writer → parent reader → owner → Host → connection, where the process crossing is one of them), and **4** after S11. Credit return (`release_call`) stays off the path.

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
| **C1** | Core | 2.4.2 source backpressure (the Core writer's step, in progress); 2.4.5 egress reports a lost socket | flood storm; silent egress drop | the Core probe (0 resyncs under flood); a slow progressing reader paces the PTY; a reader with no progress ends `Stalled` at the deadline; flood lane | in progress (+0.25 d for 2.4.5) |
| **C2** | Core | 2.4.1 notifier edge for freed class slots, plus the notifier conformance test | Backpressured with no retry event | armed `Backpressured` admission is notified when a worker dequeues (ablation: no fire) | 0.5 d |
| **S0** | Hub | Land the roll branch (b870bca3, stale close, NotSubscribed), rolled to a Core that holds C1 and C2 | cursor lost wake; silent close on Stale | existing branch tests; full lifecycle target | existing work |
| **S1** | Hub | `Readiness`, `Wake`, `OwnerSignal`, the progress fault counter; convert package-event delivery | `mark_all` spin on Backpressured; emit lost wake; `LockBusy` drop | owner-loop test: Backpressured parks with zero turns until the notifier fires (ablation: `mark_all`); emit from a request handler is delivered; `LockBusy` event is delivered, not retired | 1 d |
| **S2** | Hub | Session-family admission as a source with a maintained ready set (rule 2.1.5); `LockBusy` waits | gap and baseline on contention; b870bca3 class removed | contention on a session-family frame gives no gap (ablation: old arm); a frame queued before the cursor is admitted with no other wake (the b870bca3 test); the debug recompute of the ready set; Web workspaces-lifecycle lane | 0.5 d |
| **S3** | Hub | `SubscribeEvents` waits on `ConnectionSlots`; `bind_reader` stops signalling a bound reader | `shed_busy` on first subscribe | the three lifecycle tests named in the triage report; a forced try-lock loss is served | 0.5 d |
| **S4** | Hub | Convert the other maintenance slices, publication and event owners, and causal waits; delete `MaintenanceWakes`, `mark_all`, `try_wake`, the doorbell variants, the hand flags, the sweeps, and `DataPlaneProgress.progressed`. Install the capability event notifier and `next_deadline` (2.5). Two commits: lifecycle slices, then entity, causal, and capability | `Refused` spin, and the `Refused` maintenance-read stall (confirmed by code reading, `daemon_maintenance.rs:955, 1004, 1111`); the Observe `BudgetTooSmall` spin (rule 2.1.6); permanent `recovery`; causal herd; owner wake per pump batch | owner-loop idle test: zero turns while idle; flood lane with an owner-turn count (`measurement-window`); progress fault counter stays zero across the full lifecycle target | 2 d |
| **S5** | Hub | Input backpressure: the Unix connection stops reading while adapter ingress is full; the WebRTC channel stops draining, so the SCTP window closes | route ended by a paste burst | paste burst larger than 64 frames on a stalled data plane is delivered whole on both transports (ablation: drop + `Lost`) | 1 d |
| **S6** | Hub | WebRTC: check the watermark per chunk; bound `queued_events` with the existing mailbox bound | two unbounded queues | an oversize frame on a stalled peer stays under the aggregate plus one chunk; queued events stop at the mailbox bound | 0.5 d |
| **S7** | Hub | Map `CoreTicketPoll::Refused` and Host permits to FIFO capacity waits (if S4 did not already cover them) | — | covered by S4 tests | 0 to 0.5 d |
| **S8** | Hub | Fused Host job: apply batch, fanout, prepare, deliver | 11-13 → 3 wakes per publication | 512-publication test by gates, not by clock (plan §4.5.8); wake count assertion; a stalled client socket does not delay credit return or a held `Reply` (ablation: release after delivery) | 1 d |
| **S9** | Core | Single-threaded session worker `poll` loop | 2 crossings; unbounded control mpsc; input blocked behind output | existing worker process tests; input applied while egress is full | 1.5 d |
| **S10** | Core + Hub | Derive `exit_hold` (2.4.4); `journal_advanced` as a wake (2.4.3) | hand-mirrored flags | post-exit capture tests; journal pull with no host poll | 0.5 d |
| **S11** | Core | Data-plane fd reactor for worker and plugin-process sockets | 1 crossing per session; one reader thread per worker | worker process and plugin process suites | 2 to 3 d |

### 4.1 Cutover split

The total is about 11 to 13 writer-days of new work. That exceeds the 3-day guide, so the steps split as follows.

**Must land before cutover: about 5.75 writer-days of new work.** These steps fix every live defect in the catalogue.

| Step | New work | Why before cutover |
| --- | --- | --- |
| C1 | in progress, plus 0.25 d | flood storm; silent egress drop |
| C2 | 0.5 d | `Backpressured` has no retry event |
| S0 | existing branch | lost cursor wake; silent close on Stale |
| S1 | 1 d | event spin; lost emit wake; `LockBusy` drops an event |
| S2 | 0.5 d | contention opens a gap and a full baseline |
| S3 | 0.5 d | `shed_busy` on the first subscribe |
| S4 | 2 d | deletes the incidental wakes that hid the cursor defect; fixes the `Refused` spin and lost wake, and the permanent `recovery` flag |
| S5 | 1 d | a paste burst ends the route |

If time forces a cut, S4 is the only step that can move after cutover without leaving a known hang. The spins it fixes waste CPU; they do not hang. The recommendation is to keep S4 before cutover, because it makes the readiness model the only wake path in the Hub owner.

**Can follow cutover: about 5.5 to 7 writer-days.**

| Step | Size | Note |
| --- | --- | --- |
| S6 | 0.5 d | two unbounded queues (WebRTC chunks, Unix `queued_events`) |
| S7 | 0 to 0.5 d | only if S4 left capacity waits uncovered |
| S8 | 1 d | aligns with plugin-platform slice 3 |
| S9 | 1.5 d | single-threaded session worker |
| S10 | 0.5 d | derived `exit_hold`; `journal_advanced` as a wake |
| S11 | 2 to 3 d | follows the process host |

### 4.2 Order constraints

- **C1 must reach Hub in the same roll as Core `2845aec`.** The roll branch (`85b3507`) already contains `2845aec`. Landing S0 without C1 brings the flood storm into the Hub build. The orchestrator confirms that the roll already waits on C1.
- **S1 requires C2** for the `Backpressured` wake. Until C2 lands, S1 can wait on the completion notifier alone. That gives the same behavior as today's incidental wake, but without the spin.
- **S4 requires S1** (the type) and should follow S2 and S3, so that it only deletes.

## 5. Open questions for review

1. **Deferred capture.** Can a bound, progressing route still overflow after C1? If it can, C1 also defers the recovery capture until the queue drains (2.4.2). This question is with the Core writer.
2. **Client `Backpressured` on plugin control.** This plan keeps the typed refusal for capacity. The alternative is to park the request under its existing deadline.
3. **Local-waiter cost.** Re-evaluating every `Local` waiter after every step assumes about 20 sources with O(1) predicates. A source with many rows (for example per subscriber) must keep an internal ready index (rule 2.1.1).
