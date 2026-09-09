# Hub owner scheduling and control execution: consolidated replacement plan

Status: consolidated from two independent drafts, Claude and Codex, after root's
rulings and corrections of 2026-09-07. Baseline `dcfb73b`, parent `08357fd`,
clean tree. Read-only survey. No edits, no builds, no tests run. Every source
reference was checked against `dcfb73b`.

Scope: the Hub owner thread's scheduling, readiness discovery, deadline
handling, durable mutation path, and off-owner execution. Out of scope: the
binary terminal data plane, Core terminal authority, adapter close and barrier
contracts, the plugin completion and charge path, the `BOTSTER_ENV` fault
injection cleanup that belongs to root plan section 4.5, and the `dcfb73b`
diagnostic removal.

Both planners converged. Where the two drafts differed, this document records
the settled position and the reason, so no superseded variant is inherited.

## 1. End state

One owner thread, one loop. One wake source, ready classes, one ordered deadline
index, one shared turn budget.

Owner turn contract, shared across all owner work without exception:

- 2 ms cooperative time, 64 work items, or 256 KiB inspected bytes, whichever
  comes first, then yield.
- The budget covers everything the owner does, including cleanup, deadline
  processing, and completion absorption. There is no oversized-item exception
  and no exempt path.
- A typed value may move opaquely for one item and zero inspected bytes. What
  today's code does around those moves is not free: request cloning, response
  shaping, registry comparison, and destruction of large values are real,
  proportional work. Each is either moved off the owner or split into bounded
  units. Naming a step a move does not make it one.
- A valid 1 MiB request stays accepted. The connection task already decodes the
  frame off the owner (`transport/unix/connection.rs:441`), so the owner
  receives a typed `DaemonRequest`; admission and routing are bounded, and any
  step proportional to the payload runs off the owner.

The owner does five things: accept wakes and route typed messages; drain ready
waiters fairly; fire due deadlines; submit off-owner and Core work; apply
completions.

The owner performs no filesystem call, no process spawn, and no network call. It
never waits on plugin execution, on serialization, on a blocking receive, or on
a thread join. It may take a lock guarding bounded, item-proportional
bookkeeping, which is what Core's drain admission already does.

Readiness is distinct from an unfinished entry. A pending waiter never makes the
owner runnable. A deadline is never a substitute for a correct notification. A
must-finish mutation and a cleanup obligation are never retired because a
deadline passed.

## 2. Current defects, with references

Per-turn full scans. `next_owner_deadline` (`owner_loop.rs:93`) takes a minimum
over three collections through `OwnerBudget::next_obligation_deadline`
(`owner_budget.rs:193`), `next_request_deadline` (`control/pending.rs:140`), and
`PluginEntityState::next_reply_deadline` (`control/entities.rs:160`).
`poll_pending_requests` (`control/pending.rs:195`) and `poll_owner_obligations`
(`owner_budget.rs:205`) poll every entry. `retire_plugin_entity_replies` and
`expire_plugin_entity_resyncs` (`control/entities.rs:711`, `:727`) scan the
pending entity map twice per turn. `TerminalReservations::retire_expired`
(`admission/reservations.rs:342`) scans every reservation, then scans the WebRTC
admission map per expired row (`owner_loop.rs:655-700`).
`owner_maintenance_pending` (`owner_loop.rs:156`) runs five runtime predicates
and is called twice per turn. The loop serves at most one control message per
turn (`classify_owner_poll`, `owner_loop.rs:119`), so serving N queued messages
costs O(N x pending). This is complexity read from code, not a measurement.

Periodic polling. `ENTITY_RECONCILIATION_INTERVAL` is 500 ms
(`owner_loop.rs:55`) and `mark_due_reconciliation` (`owner_loop.rs:170`) marks
Pump on that timer, so an idle Hub runs owner turns twice a second; each Pump
phase submits a Core read (`owner_loop.rs:748`, `:829`) that wakes the
data-plane thread, which wakes the owner again. `socket_path.exists()`
(`owner_loop.rs:497`) is a `stat` syscall in the loop body.

Rotation instead of readiness. Three nested schedulers select work:
`BackgroundClassScheduler` (`daemon_maintenance.rs:267`), `PumpScheduler`
(`:220`), and `MaintenanceScheduler` over nine kinds (`:91-118`) with three
`prefer_*` overrides. One ready item in the ninth kind costs up to eight owner
turns that do nothing, each paying the scan cost above.

Polled pending instead of woken completion. Each Core operation gets its own
`CoreTicket` channel (`data_plane/driver.rs:146`) that the owner polls (`:188`).
There is no index from a finished operation to its waiter, so the owner polls
all of them. The data-plane doorbell carries one coalesced bit, not an identity.

No shared owner turn budget. Individual maintenance slices already carry private
item and byte limits; what is absent is one budget shared across all owner work.
`MAX_OWNER_TURN_MS = 25` (`daemon_maintenance.rs:35`) is applied
at one site, the host-bridge refresh (`:428`). Its test
(`tests/session_projection_owner_loop.rs:224`) is a compile-time constant
assertion and proves nothing about the loop. No shared item or byte budget for
the owner turn exists.

Blocking and proportional work on the owner. `packages::handle_request` and
`spawn_targets::handle_request` run synchronously in the dispatcher
(`daemon/control.rs:211`, `:223`), reaching package files (`src/packages.rs`, 44
filesystem sites) and `git` (`spawn_targets.rs:651`,
`managed_git_worktrees.rs:638`, `:691`, `:1318`). `SessionTypeCatalogCache`
(`subscription/entity.rs:158`) clones its inputs on the owner and then starts a
receiver-polled thread (`:213`). `HubDaemon::replace_state` (`daemon.rs:142`)
deep-clones the whole `HubState` into the runtime on the owner; moving disk work
alone does not remove that cost. `HubRuntime::invoke_plugin` (`runtime.rs:1612`)
spawns a thread and blocks in a `recv_timeout` loop; the owner control path does
not call it today, but `client_api.rs:1024` and `:1047` reach it through
`HubClientApi::handle_request`, which the owner also uses for other families.

## 3. State and ownership model

Owner state:

- `waiters`, one entry per accepted unit of owner work, each owning one
  `OwnerPermit`. Live waiters are bounded by `OWNER_BUDGET_CAPACITY`
  (`owner_budget.rs:35`).
- `ready`, one ordered set per class keyed by `(enqueue_serial, WaiterId)`, with
  repeated readiness marks coalesced so queue storage stays bounded. Section 8.1
  fixes the representation.
- `deadlines`, one `BTreeSet<(Instant, WaiterId)>` with exact-key removal.
- `wakes`, one bit per background class, set by the event that created the work.
- Domain continuation cursors, kept as they are today.

Retained input, prepared results, completion results, and superseded published
views each carry finite item and byte bounds.

### 3.1 One durable artifact, one reservation

`HubState` is a single struct persisted as one document at
`<data_directory>/hub-state.json` (`persistence.rs:31`, `:47-77`, `:332`),
holding the package registry, spawn targets, worktrees, device session-type
sources, the session-type generation, credential keys, trusted browser
identities, and bootstrap grants. `HubStateStore::update` is load, mutate, save
of the whole document (`persistence.rs:310`), and `write_atomically` uses one
shared temporary path (`:346`).

Separate domain keys therefore do not serialize this storage. Two concurrent
"independent" writers would each rewrite the whole document and the second save
would silently drop the first mutation.

Ruling: one reservation covers all of `hub-state.json`. Every runtime writer
takes it, including compensation and managed-worktree completion.

Confirmed live writers, verified by Codex and to be routed through the
reservation: `control/packages/mutations.rs:302`, `control/spawn_targets.rs:96`,
`:117`, `:138`, `control/session_types.rs:135`, and `runtime.rs:526`, `:2188`,
`:2235`.

Not writers, recorded so the audit is not repeated: `update.rs:709` reads
`hub-state.json` and its `FileHubStateStore` reference only constructs the path;
`packages.rs` imports the store inside its test module (`:3445`) and its update
at `:5279` is a test.

`HubDaemon::start` writes at `daemon.rs:110`, before the owner loop. That write
is outside live admission only while startup holds exclusive ownership, and the
plan states that condition rather than assuming it.

## 4. Durable mutation: reservation, prepare, check, commit

Settled model, agreed by both planners and matching the outgoing reviewer's
rule set.

1. At admission the owner reserves execution, storage, and completion capacity,
   and takes immutable versioned inputs. It does not reserve the document here.
2. Preparation runs off the owner, concurrently, from those inputs. A prepared
   mutation carries its base revision. Plan preparation performs no write. Where
   a family creates an external effect before commit, section 4.2 applies; that
   effect is not called preparation.
3. On completion the owner checks the base revision and reserves the document in
   one atomic bounded step. If the revision moved, the mutation is rejected or
   prepared again before any write. If another commit holds the document, the
   prepared work is retained under its bounds and is not marked runnable until
   the reservation is released. A worker never reconstructs a whole-state
   snapshot from stale inputs and never overwrites unrelated fields.
4. The worker commits under the reservation. The reservation is retained until
   the owner publishes the committed view or completes failure recovery.
5. After write admission, cancellation removes reply delivery only. Nothing
   written is rolled back for a departed client.

The document reservation is granted with the revision check, never before
preparation. Granting it at admission would hold the shared document across
`git` preparation and serialize unrelated mutations behind it. A reservation is
ordinary owner state, never a mutex held across disk I/O, and the owner blocks
on nothing to hold it.

Never write and then reject a stale generation.

Capacity for the commit and for any required compensation is reserved at
admission, together with the `OwnerPermit`, in the same shape as the existing
second-permit rule for attach and reserved-bind (`owner_budget.rs:30-36`).
Must-finish work therefore carries its completion and recovery capacity across
the preparation-to-commit transition and never needs a fresh best-effort slot
after a disk effect has occurred.

### 4.1 Failure and compensation contract

- Failure before a disk commit preserves the old published view.
- A successful commit must be published even if the requester disconnected.
- Package runtime effects and compensation are must-finish operations.
- A repository session-type file plus `hub-state.json` is not one atomic write.
  Partial writes need explicit compensation, specified per mutation family.
- If compensation fails, report the actual partial failure and prevent affected
  mutations until recovery establishes the durable state. Do not promise that
  the old in-memory view always equals disk. This is a deliberate narrowing of
  an earlier draft assertion that was too strong.
- Replacing a large published view must not destroy the large old value on the
  owner. Reclamation of the superseded view transfers off the owner. This is
  also why `replace_state`'s deep clone (`daemon.rs:142`) is in scope: moving
  only the disk work would leave the proportional owner cost in place.

### 4.2 External effects created before commit

`prepare_managed_worktree` (`managed_git_worktrees.rs:98`) runs `git worktree
add` (`:239`) before any `hub-state.json` commit. That is a real external effect,
so "preparation performs no write" cannot cover it, and this plan does not
pretend otherwise.

Rules for such a family:

- Creating the external effect is a separate must-finish phase, not preparation.
- Cleanup ownership is held from before creation, not acquired afterwards.
- The effect is never silently dropped because the base revision went stale. A
  stale revision leads to compensation, which removes the created worktree, or
  to re-preparation. It never leads to an abandoned directory.
- The order is fixed, with no implementer choice: creation runs as its own
  must-finish phase outside the `hub-state.json` reservation, and only then does
  the generation check, reservation, and commit follow. Staging creation after
  write admission is rejected, because section 4 retains the reservation until
  publication and that would hold it across the `git` execution.
- Stale state at the check requires revalidation or compensation, using the
  existing rollback semantics.
- This uses the existing managed-worktree contract. It does not introduce a
  generic transaction framework.

## 5. Host execution

One fixed, bounded host executor, extending the existing bounded mechanism
(`ManagedGitCoordinator`, `runtime.rs:4361`) only as far as these paths need.

Rejected, with reasons:

- Three dedicated durable threads. Rejected by Codex and withdrawn by Claude:
  with one shared document there is one durable artifact, so per-domain threads
  serialize nothing extra and idle.
- A `Brief` and `Extended` workload-class split. Both root and Codex rejected
  it and Claude withdrew it. Filesystem reads and saves stall too, so a label
  establishes no time bound, and a reserved spare worker excludes only the named
  saturation case rather than proving isolation. No new priority or
  workload-class scheduler is introduced.
- Consolidating capability execution or plugin execution merely to reduce the
  number of thread mechanisms. Out of scope without a current defect. The
  reviewed plugin completion and charge path is preserved.

Guarantees, stated at the width they can be held:

- The owner and the terminal path remain independent of host job latency.
- Host jobs receive bounded admission and fair dispatch. Accepted does not mean
  starts immediately.
- Admission reserves bounded queue, completion, and retained-byte capacity
  before host work is accepted. A request that cannot get capacity is refused
  with a typed error in the shape of the existing `owner_budget_exhausted`
  refusal (`owner_budget.rs:44`). Nothing queues past the bound.
- A reserved slot prevents capacity refusal after acceptance. Submit can still
  report executor shutdown or worker failure, and that path is typed.
- Capacity release wakes the owner directly. No accepted operation and no
  refusal path depends on incidental terminal traffic to make progress.

Not a framework, as an enforceable ceiling: one bounded queue, a fixed thread
count, one submit function, one typed completion. No work stealing, no
priorities, no dynamic thread growth, no async runtime.

## 6. Readiness, fairness, and deadlines

Readiness. Core completions and host completions carry identities. The owner
maps an identity to its waiter and marks that waiter ready. Only ready waiters
are touched in a turn. Pending ownership never makes a class ready.

Fairness, corrected from an earlier draft claim. Time or byte limits can stop a
turn early, so no design can guarantee every non-empty class is served within
one turn. Instead:

- Simple round robin among non-empty classes, with the next-class cursor
  preserved across turns, so a class skipped by an exhausted budget is first
  next turn.
- A class that exhausts the remaining byte budget gets its chance with a fresh
  budget on the following turn.
- No preference override may reset fairness. The current `prefer_*` calls
  become readiness marks, not cursor resets.
- Deficit counters are not adopted; they need a demonstrated requirement that
  does not exist today.

Deadlines. One `BTreeSet<(Instant, WaiterId)>`. Each armed waiter stores its
armed instant, so removal is by exact key. Arm inserts, re-arm removes the old
key first, retire removes in the same operation that frees the waiter.
`next_deadline` is the first key. Index size equals the number of armed waiters
at every instant, bounded by `OWNER_BUDGET_CAPACITY`, so no stale entry is ever
created and none has to be bounded or compacted.

Expiry has exactly three outcomes: retire a retirable operation; retain and
disarm must-finish work, flagged once as `past_deadline` does today
(`control/pending.rs`); or schedule a legitimate later deadline. A deadline never
ends an execution: the reply retires, the job completes in the executor, and its
completion is drained without delivery. Due work beyond one turn stays runnable
and shares class fairness rather than forcing a turn overrun.

Insertion of an already-due deadline is legal and is not an error. If admission
took long enough that a request's first deadline is already in the past when it
is armed, the entry is inserted and becomes ready work on the next `fire_due`
pass, charged one item like any other. A valid request must never fail because
admission was slow. An earlier draft made arming at an instant not after `now` a
typed error; that rule was too broad and is withdrawn.

The prohibited case is a re-arm that makes no progress: after a waiter's
deadline fires, that waiter may not re-arm at an instant no later than the
deadline that just fired. That is the loop the rule exists to forbid, and it is
the only case the typed error covers.

## 7. Keep, delete, replace

Keep:

- The binary terminal data plane and its causal inventory wake
  (`data_plane/driver.rs`), Core terminal authority, and the adapter close and
  barrier contracts.
- `RetainedPluginResultBudget` and its completion and release notifications
  (`control/reply.rs`).
- `OwnerBudget` permit accounting, explicit retire hooks, and must-finish
  semantics. `OWNER_BUDGET_CAPACITY` is load bearing for the deadline bound.
- `ControlStep::Pending` and `request_must_finish`.
- `note_terminal_inventory_changed` (`owner_loop.rs:82`), the attach-epoch
  protection, and the change-during-read latch.
- Domain continuation cursors.
- Useful security and ownership source guards, including the peer-liveness gates
  (`tests/daemon_control_ownership.rs:58`) and the two variant-to-owner matrices
  (`:333`, `:501`). The behaviour proofs in section 9 do not replace them. Only
  guards naming symbols this plan deletes are removed; guards that parse a
  source file by splitting on a signature are rewritten to be robust rather than
  deleted wholesale.
- Carry three known stale source guards into the scheduler replacement:
  `dispatcher_names_request_variants_only_in_delegating_arms`,
  `pump_phases_do_not_list_subscriptions_or_sessions`, and
  `shutdown_handler_installs_exact_suppression_before_core_request`. Code moves
  caused these guards to inspect old regions. The shutdown guard is corrected
  before this replacement. Each later correction must anchor to the function
  that owns the property. Each guard must require positive presence before it
  checks order or absence.

Replace:

- The `owner_loop.rs` loop body and prologue, and the three nested schedulers in
  `daemon_maintenance.rs`, with one fixed rotation over ready classes under the
  shared turn budget.
- `poll_pending_requests` (`control/pending.rs:195`) and `poll_owner_obligations`
  (`owner_budget.rs:205`) with keyed, completion-driven readiness.
- `CoreTicket` polling for owner consumers with completion identities. Non-owner
  consumers of `CoreTicket::wait` are audited before any API is deleted; they
  exist today in `runtime.rs` startup paths (`:4147`, `:4156`, `:5235`, `:5274`,
  `:5295`).
- The deadline minima and entity scans with the one ordered index.
- Synchronous package, spawn-target, session-type, and managed-worktree control
  execution with the preparation, reservation, and commit path.
- `SessionTypeCatalogCache::refresh` (`subscription/entity.rs:158`) with
  versioned shared inputs and a completion wake, removing the owner-side input
  clone and the receiver-polled thread (`:213`).
- `HubDaemon::replace_state` (`daemon.rs:142`) so the published view is shared
  rather than deep-cloned on the owner, with reclamation transferred off-owner.

Delete:

- `ENTITY_RECONCILIATION_INTERVAL` and `mark_due_reconciliation`
  (`owner_loop.rs:55`, `:170`), only behind the section 8 gate.
- The per-turn `socket_path.exists()` (`owner_loop.rs:497`), only with a
  concrete event-driven replacement that preserves missing-socket rebind
  behaviour.
- `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, their `lib.rs:161`
  exports, and `tests/session_projection_owner_loop.rs:224`.
Audit, then delete only on evidence:

- `HubRuntime::invoke_plugin` (`runtime.rs:1612`) and its blocking callers
  (`:3038`, `:3115`, `:3218`, reached from `client_api.rs:1024` and `:1047`).
  Audit the current consumers. Convert any path that is reachable from a shared
  owner and blocks. Delete a wrapper only after its actual consumers have moved.
  Valid non-owner APIs are preserved until that evidence exists. Preventing a
  hypothetical future caller is not a reason to delete, and this plan is not
  authorization for speculative plugin refactoring.

## 8. Sequencing, writer boundaries, and gates

1. Freeze the contracts first. Everything else depends on them. Section 8.1 is
   that freeze, and it is complete: concrete shapes, ready classes, identity
   lifecycle, and selected ceilings with their source rationale. Implementation
   starts from section 8.1, not from prose elsewhere in this document.
2. Off-owner preparation and commit, and the Core completion bridge, can be
   prepared independently against the frozen contracts.
3. One Hub writer integrates `runtime.rs`, `control/pending.rs`, and
   `owner_loop.rs`. Parallel writers on different functions of the same
   repository are not safe under the one-writer rule, and this plan does not
   claim they are. An earlier draft did; that claim is withdrawn.
4. Deadline conversion follows the common identity contract.
5. The shared turn budget is enforced only after blocking and oversized owner
   work is removed. No temporary exemption is shipped at any point.
6. Timer and socket-stat changes are last and gated.

Gate for the 500 ms timer, retained on root's ruling and not assumed away:
removal requires complete causal inventory wakes, fair bounded owner work, and
passing race tests together. The candidate wake set is attach, detach,
reservation expiry, peer loss, session exit, and Core inventory change.
Completeness is not established by either planner and must be shown, not
assumed.

### 8.1 Frozen contracts

Every value here is selected Hub policy, not a measured optimum. Existing limits
provide comparison points; the host capacities below are selected policy. All
byte accounting is logical encoded bytes, in the manner of the reviewed plugin
result budget, and is explicitly not a physical RSS limit.

Capacity.

- Two fixed host worker threads. `ManagedGitCoordinator` today runs one active
  and one queued job (`runtime.rs:4361`, `:4400`); two workers are the smallest
  concurrent extension of that. This is a capacity choice, not an
  execution-isolation promise.
- Eight admitted host operations in total. Queued, executing, prepared,
  committing, and recovering operations all count against the eight.
- One scheduled job and one completion slot per operation, so the job queue
  holds at most eight and the completion mailbox at most eight.
- An operation keeps its slot across every phase. It never acquires a second
  slot for commit or compensation, which is what makes must-finish work safe
  after a disk effect.
- Workers stay free between phases. No worker waits for an owner decision.
- Eight is selected policy. It is not derived from the 8 MiB result budget,
  which bounds a different quantity, and this plan does not present the shared
  numeral as a derivation. The existing limits serve only as a sanity check that
  eight sits far below them: the Core request queue of 64
  (`data_plane/driver.rs:35`), the owner control queue of 256
  (`admission/budgets.rs:8`), and the 64 x 33 = 2112 owner permits
  (`admission/budgets.rs:6`, `owner_budget.rs:35`).

Request and result bytes.

- The 1 MiB encoded request and response limits are preserved
  (`botster-hub-client/src/lib.rs:72`, `:74`).
- Eight accepted operations therefore reserve 8 MiB of request capacity and
  8 MiB of reply capacity.
- Retained request and result charges travel with their buffers through
  transport delivery.
- Correlation stores a small descriptor. The full `DaemonRequest` is never
  cloned for correlation.

Prepared, recovery, and view bytes.

- 8 MiB logical prepared-and-recovery capacity per host operation, 64 MiB
  aggregate across the eight. This is a selected policy ceiling, not an existing
  host limit and not a derivation from the reviewed plugin result budget
  (`control/reply.rs:12`), which bounds a different quantity.
- The quota bounds retained memory only: in-memory prepared change payloads and
  rollback descriptors. Overflow is rejected before any shared-state write.
- External effects are bounded separately from memory, by their descriptors, not
  by their size. A managed worktree, an installed package tree, or any other
  on-disk artifact is tracked for compensation by a rollback descriptor whose
  own memory cost is charged. The artifact's bytes are not charged, and the
  quota therefore imposes no size limit on worktrees or package filesystems.
- File processing streams wherever it can, so a large artifact is handled
  without an in-memory copy and without a charge proportional to its bytes.
- Existing filesystem behaviour is preserved, including current package and
  worktree size behaviour. This plan introduces no new filesystem quota. One
  would need its own product rationale, which does not exist and is outside this
  replacement.
- Large immutable views are never cloned into these payloads.
- State views carry their own bound: 64 MiB aggregate logical bytes across the
  current, candidate, retained base, and retired views, counted once per shared
  allocation, with no uncharged version chain. Also a new policy ceiling.
- Views are built off the owner under that reservation. Capacity pressure waits
  for a release; it never invalidates a valid 1 MiB request.
- A single in-memory view above the configured ceiling returns a typed
  resource-limit failure, with no truncation and no disk mutation.
- Request-frame validity and resource admission are different things, and the
  typed errors keep them distinct.
- No allocation waits on itself. A capacity wait must not retain inputs whose
  release is required to satisfy that same allocation. A single view that cannot
  coexist with the current committed view inside the 64 MiB pool is rejected
  before any write, with a typed resource-limit failure. Such a request is never
  parked forever.
- A stale prepared job releases its obsolete base before it retries, so the
  retry does not compete with the memory its predecessor still holds.
- Quotas are enforced before oversized artifacts are materialized, through
  bounded input and output structures and off-owner incremental reads and
  builds. The charge is never computed by adding an owner-side serialization
  pass, which would put proportional work back on the owner.
- The host ceiling is independent of the 64-item scheduling limit. The turn
  budget bounds owner work per turn; the host ceiling bounds concurrently
  retained host work. Their rationale is recorded separately and they share no
  value by design.

Ready classes. One fixed enum, covering the existing `MaintenanceSliceKind`
values, the pump phases, and today's prologue work:

`ControlIngress`, `Cleanup`, `CoreCompletion`, `HostCompletion`,
`PluginCompletion`, `Deadline`, `Observe`, `InventoryReconcile`, `JournalPull`,
`ProjectionApply`, `Baseline`, `HostBridge`, `SubscriberDelivery`,
`ProviderResync`, `PackageEventDelivery`.

One round-robin cursor persists across turns. There are no priority rewrites.
Each waiter has one queued-ready flag, and different triggers OR into its pending
reasons instead of enqueueing duplicates.

Each class is an ordered set keyed by `(enqueue_serial, WaiterId)`, with that
exact key stored on the waiter. This gives FIFO service and exact O(log n)
removal on final retirement, with no map-wide scan and no stale-entry
accumulation, using the same standard collection approach as the deadline index.
This representation is selected explicitly because no existing indexed queue is
reused. A plain `VecDeque` is rejected: it cannot remove a retired waiter without
a scan.

Identity lifecycle.

- `WaiterId(u64)` from one checked, monotonically increasing owner counter. Ids
  are never reused during an owner lifetime. On exhaustion the owner refuses new
  admission; it never wraps.
- `OwnerPermit` bounds the number of live entries.
- A Core operation id maps to a `WaiterId` plus a phase serial. A host job and
  its completion carry the `WaiterId` and a checked phase serial.
- A completion is accepted only for the expected phase. A stale or duplicate
  result is dropped with its charges and can never affect a sibling.
- A must-finish operation keeps its retained operation row until completion or
  recovery. That row is the existing waiter entry, not a second tombstone
  mechanism. Transport identity is separate and may retire earlier.
- Final retirement deletes the entry from every ready, deadline, and connection
  index.
- Identity is registered before submission. A completion is published before the
  coalesced wake is signalled.

APIs.

```rust
fn admit_host(correlation: Correlation, owned_request: DaemonRequest, byte_charge: u64)
    -> Result<(WaiterId, HostWorkPermit), AdmissionError>;
fn submit(job: HostJob) -> Result<(), HostStopped>;

struct HostJob { id: WaiterId, phase: u64, command: HostCommand, permit: HostWorkPermit }

enum HostCommand {
    Read { request: DaemonRequest, view: Arc<HubView> },
    Prepare { request: DaemonRequest, view: Arc<HubView> },
    CreateManagedWorktree { request: ManagedGitRequest },
    Commit { prepared: PreparedMutation, reservation: DocumentReservation },
    Recover { recovery: RecoveryState },
}

struct HostCompletion { id: WaiterId, phase: u64, result: HostResult, permit: HostWorkPermit }

enum HostResult {
    ReadReady(HostReply),
    Prepared(PreparedMutation),
    Committed(CommittedView),
    Recovered(RecoveryOutcome),
    Failed(HostError),
}

struct HostReply { response: DaemonResponse, charge: HostReplyCharge }
struct CommittedView { committed_revision: u64, view: Arc<HubView>, reply: HostReply }
struct PreparedMutation { base_revision: u64, change: Change, rollback: Rollback, charge: u64 }

enum ReservedDocumentResult { Granted(DocumentReservation), Busy, Stale }
fn check_and_reserve(id: WaiterId, base_revision: u64) -> ReservedDocumentResult;
```

`Read` and `Prepare` accept only their existing routed request families.
Correlation reuses the existing transport request id and the connection or grant
identity; it never retains a duplicate request. `Change` and `Rollback` are
private, family-specific, owned typed data. They are never closures borrowing the
daemon.

`Busy` parks the operation on reservation-release readiness. `Stale` fails or
re-prepares before any write. Commit completion carries `committed_revision` and
`Arc<HubView>`. A worker failure after external effects retains recovery
ownership and never returns a clean failure. `command` is an enum following the
current request families; it must not accept arbitrary closures borrowing
`HubDaemon`.

Deadlines. An initial deadline at or before `now` is inserted and marks
`Deadline` ready at once. Firing removes the exact index key. A retirable reply
ends; must-finish execution stays and disarms; only a real later deadline may
re-arm. Any due remainder stays runnable under shared fairness. No new
user-visible error appears merely because queue delay made a valid initial
deadline past due.

Evidence categories. Deterministic scheduler and readiness proofs use controlled
time and wakes and assert quiescence; they need no quiet host. CPU and latency
measurement is a separate category and does need one. This plan contains only
the first category.

## 9. Minimal real-path proofs

Through the real control socket and the real owner loop. Reuse existing tests
and helpers. This is not a campaign, and no performance is inferred from
counters.

1. Hold a host job. Control and terminal siblings, and background cleanup, all
   progress. Includes sibling owner control during the held job.
2. A valid 1 MiB request completes with bounded owner inspection and no deep
   owner clone.
3. Flood ready classes. Assert persistent cross-turn fairness through the
   preserved cursor and shared budget accounting.
4. Churn deadlines. Index size equals armed obligations; cancellation and
   must-finish expiry produce no stale-minimum spin.
5. Complete work and release capacity with no terminal traffic. Verify the wakes
   and that the owner returns to idle sleep.
6. Overlap package, spawn-target, session-type, and managed-worktree writes.
   Inject commit and compensation failures. Verify no lost update and the
   correct published or recovered state.
7. Disconnect before and after commit admission. Verify reply retirement,
   durable completion, and permit release.
8. Preserve the inventory-change-during-reconcile and attach-epoch tests.
   Exercise the causal close and expiry paths before removing the timer.

None of these eight proofs needs a quiet host. Each asserts a deterministic
readiness, idle, ordering, or counter fact, and a deterministic assertion is
unaffected by neighbouring load. An earlier draft gated the idle and no-spin
proofs on a quiet host; that gate is withdrawn. Only CPU or latency measurement
would need one, and this plan contains no measurement.

## 10. What this plan does not claim

- No performance number. Section 2's complexity statements are read from code.
- The 2 ms, 64, 256 KiB triple is scheduling policy, not a latency guarantee.
- No execution isolation for host jobs. The guarantee is bounded admission and
  fair dispatch, plus independence of the owner and terminal paths.
- No claim that the old in-memory view always equals disk. Partial writes across
  two artifacts are handled by compensation and reported honestly when
  compensation fails.
- Commit latency is not bounded by excluding `git`. JSON serialization, fsync,
  and rename run off the owner and are not short by construction. An earlier
  draft said otherwise; that is withdrawn.
- The causal wake set for the timer gate is not established.
- The idle wake rate is read from source, not observed. Proof 5 is the check.

## 11. Diagnostic retirement contract, 2026-09-08

Lifecycle retirement removes the exact diagnostic row immediately. Registration does not scan the registry for retired rows.
This decision supersedes deferred admission pruning in the historical observability plan, S1d/AC11.

Retirement resets the cell to zero count and bytes. It also clears the age, prior-generation gate, and invalid sample flag.
Retirement closes writes even when another object retains the cell. An already closed cell must still reach the empty retired state.
Live Empty rows remain registered. An exact generation key protects another generation with the same name.
Cell retirement also checks Arc identity. A delayed old mailbox retirement must not remove or close its replacement.

Mailbox cleanup retires diagnostics while it holds the existing mailbox lock. Refused cleanup retains the mailbox for the existing cleanup retry.
This boundary serializes retirement with metric publication. The final mailbox Drop provides an exclusive fallback.
Router retirement and metric publication share the RouterInner lock. Consumer rebind retires the old Arc, including same-generation replacement.
Consumer rebind retires the old registered cell before it registers the new cell. A diagnostic snapshot can observe that temporary absence.
The registry snapshot does not represent an atomic router lifecycle snapshot.

Required tests cover immediate row removal, retained closed handles, live Empty rows, generation isolation, and delayed old-cell retirement.
This change removes the two global pruning scans. It does not establish diagnostic lock progress or close other owner scheduling findings.

Validation used Rust 1.97.0 with `CARGO_INCREMENTAL=0` and two Cargo jobs.
The `event_plane_counters::tests` suite passed 17 tests. The `package_event_router::tests` suite passed 60 tests.
After caller serialization changed, `subscription::package_events::tests` passed all 21 tests.
The focused same-generation consumer rebind test also passed.

## 12. Intermediate family generation migration, 2026-09-08

Mutation and resync causal identities now include the family generation. New family state captures the current owner-held epoch.
An accepted package cleanup reserves its next epoch before family removal. A retained cleanup reserves that epoch only once.
Epoch exhaustion retains the exact package result, fault, Host permit, and Owner permit through terminal recovery.
That recovery rejects new Host requests before admission. Daemon status and shutdown requests remain available.
Direct Lua load and unload check epoch capacity before host execution. Direct unload now returns a typed cleanup error.
Direct cleanup consumes the checked epoch only when execution records a family unload.

Provider preparation captures the family generation before asynchronous admission. Owner delivery phases compare that generation with live family state.
A late fanout finish releases its old lease without recreating an absent family or marking a newer family for resync.

This is an intermediate migration. Cleanup still reinserts unloading family state into the live map.
Detached retired state, generation-specific fanout membership, bounded cleanup phases, and causal notification progress remain incomplete.
The retained-old-state and new-admission test remains required.

Rust 1.97.0 `check --tests` passed. Nine family-focused library tests passed.
The package recovery test passed both event failure and family epoch exhaustion with two previously accepted requests.
The first lease integration run stopped during setup because the candidate environment was absent. It did not execute the test bodies.
The matched e233753 candidate then passed six lease tests and failed three. That candidate did not pass integration validation.
Two failed tests used event readiness to drain causal cleanup. Their progress checks now use causal readiness and retain eventual-release assertions.
The third fixture now applies its provider snapshot through the public begin/step path before it tests a separate degraded scope.
Reading the snapshot alone does not complete family resync or release its lease.
All nine lease tests passed against the ce96389 matched binaries after this test-only correction.
The direct cleanup regression and repeated recovery request regression also passed.
This evidence does not close the remaining family cleanup work listed above.

The fanout queue now keeps exact membership by family generation and preserves global FIFO delivery.
The admission path checks sequence capacity before it creates or changes family state.
The locked pending count bounds all mutations that admission can release. Unused capacity does not consume sequence values.
The conservative check can refuse admission when fewer mutations would become ready.
The queue module passed 20 tests on Rust 1.97.0.
A runtime test with a loaded Lua provider passed sequence exhaustion before family creation and before pending-gap release.
That test also verified that refusal releases the pending publication lease and preserves family state.
The current cleanup caller uses exact generation buckets but still performs bulk cleanup.
Detached state, retained worker phases, and indexed family readiness remain open.
Review found that cleanup could miss queued mutations after live family state was removed.
The queue now finds old generations through its membership index, without a live-state lookup.
A regression test verifies that cleanup removes only the old queue item and lease when live state is absent.
The newer item and lease use the same family, causal scope, and mutation sequence. Both survive cleanup.
All 26 runtime module tests passed. All 20 queue module tests passed after the indexed lookup change.
The source reviewer closed the queue integration findings. The remaining cleanup work is still open.


## 13. Retained family cleanup, 2026-09-08

The package operation now retains one cleanup cursor and its original Host permit.
The cursor detaches one old family from the live map. Cleanup never reinserts retired state.
The cursor preserves the accepted epoch across every phase and retry.
Live and queued family lookup use ordered indexes. Cleanup also finds old queue entries without a live family or a declared provider.
The queue cursor skips all generations of the previous family before it selects the next family.

Each owner phase selects one payload or one causal release. A Host worker destroys each selected payload.
The owner retains the payload's exact lease until the exact worker phase completes.
The cursor then attempts release admission. A refused release remains in the cursor and prevents the next payload selection.
The package result, Host permit, and Owner permit remain owned while release admission waits.
A stale completion or failed submission enters explicit recovery with the original state and permits.
Recovery also retains an unexpected completion instead of destroying its result on the owner.
Synchronous runtime callers drain the same cursor outside the daemon owner. They retain refused releases in the existing direct-call storage.
Those callers have no admitted Host operation.

An exact family-generation index replaces the scan for releasable resync leases.
Each resync transition updates that index. Each retry selects one exact family and one lease.
The causal table now publishes capacity and mutex-release progress through one retained bit and the existing bounded control channel.
Registration occurs before the admission attempt. A second lock attempt closes the unlock-before-registration race.
The unlock notice publishes only after both causal guards drop. A full queue refusal does not publish its own wake.
The owner wakes one registered cleanup per ready item. New notifications preserve the current waiter cursor before another pass starts.
Maintenance uses causal readiness instead of logical pending work when it decides whether to run again.
Control completion uses causal readiness for its maintenance wake.
Shutdown still uses event cleanup ownership to retain unfinished unloads.

The combined Rust 1.97.0 check with tests passed. Eleven causal tests passed, including eight notifier tests.
Fifteen selected family tests passed. All 30 runtime tests then passed, including the empty poisoned-table readiness test.
The production package test passed four cases with every other Host slot occupied: normal completion, client disconnect, causal contention, and a stale worker phase.
The contention case holds the causal inner mutex and fills the pending queue. Cleanup retains both permits while the owner ready queue becomes empty.
Mutex release resumes cleanup. A stale worker phase instead retains the original release and both permits in recovery.
The worker test records a different thread identifier after payload destruction and confirms that the original Host slot remains occupied.
All 73 router tests and all 20 fanout queue tests passed.
The terminal recovery test also passed with two accepted package requests.
The readiness review found and corrected three maintenance wake decisions that used logical causal ownership.
No production wake decision in the four reviewed predicate pairs still uses the ownership predicate.
The review covered causal table readiness, runtime causal readiness, event owner readiness, and the owner queue readiness split.
The canonical development build produced matched Hub and worker binaries for `a9c56ce`.
All nine lease integration tests passed against that candidate. Active resync and old-family detachment integrations also passed.
All seven plugin lifecycle integration tests passed with serial execution.
The candidate and its manifest are in `/private/tmp/hub-retained-family-candidate-2026-09-08`.
A final test-only extension also verifies that detached cleanup preserves the recreated family's resync lease in the same causal scope.
That focused test passed. Production code remains the tested `a9c56ce` revision.

Poison latches for the table lifetime. The new family path reports explicit recovery; a daemon restart replaces the table.
The broader audit of legacy retry callers and fault visibility remains open.
These tests do not close the remaining owner scheduling, notification, or idle-progress proof requirements outside this family cleanup path.


## 14. Ordered causal ownership and publication service, 2026-09-09

This local implementation replaces the earlier 13-phase cursor and all causal fallback stores.
One owner-only FIFO holds at most 256 operations, including immediate reservations.
A reservation exists before its source changes. Its destructor returns an unused slot, including during unwinding.
The owner applies only the FIFO head. A table wait or fault retains that exact head.
The causal table has no operation queue or admission bypass.

Each daemon dispatch applies one causal operation in the existing `HostBridge` class.
Family resync releases use a separate source step in that class.
A full FIFO retains each source: a publication request, fanout finish, provider invocation, event flight, or cleanup cursor.
Entity completion waits preserve the original Host permit and owner permit.
Admission refusal uses an explicit retirement stage before it sends the refusal.
A required transition that encounters a table fault enters retained recovery.
Cancellation does not release that retained state. Restart remains the table recovery boundary.

Table unlock notifications and FIFO capacity notifications wake indexed domain waiters.
Each wake pass captures an upper bound and retains its cursor across new notifications.
The owner keeps a live waiter indexed if ready admission fails.
Tests pass for reinsertion during a wake pass and retention after ready-queue serial exhaustion.

The publication audit found no daemon consumer for the previous Lua bridge queue.
The new bridge has a daemon ready item and shares its one-request transition with synchronous runtime pumping.
The bridge admits at most 256 queued requests and 8 MiB of logical bytes.
Each request has a 1 MiB limit. The metric counts encoded JSON bytes and both retained plugin-key strings.
The bridge measures bytes without allocating a second encoded frame.
The Lua worker parses and validates each frame before queue admission.
The bridge retains the typed mutation and charges the original encoded frame plus both plugin-key strings.
Preparation removes unknown frame fields before retention.
Malformed frames return `NeverQueued` before causal acquisition.
The mutation size check serializes borrowed fields through a counting writer.
The check uses the shared daemon frame limit without cloning or retaining an encoded frame.
Admission uses the existing subscription placeholder; delivery checks each actual recipient frame separately.
The preparation check passes 16 focused library tests, including actual asynchronous Lua publication.
Two Lua integration tests pass for oversized mutations and nested empty objects.
One removed request can coexist with a refilled queue during owner processing.
These limits do not claim exact allocator memory usage.

Workers use nonblocking enqueue and exact-token retraction.
The owner uses a nonblocking try/arm/retry lock protocol before it reserves causal capacity and acquires the pending lease.
The owner removes the exact head only after those prerequisites succeed.
Capacity waits and table-lock waits have separate wake sources.
A fault latches under the bridge guard. After that boundary, timeout cannot retract the retained source.

Daemon publication admission reserves Host and Owner permits and an exact dispatch identity before it removes the bridge head.
Family admission and validation return each discarded mutation to the caller.
The daemon transfers that mutation to the existing Host disposal command.
One runtime row retains the response, scalar result, and required publication release.
The Host slot bounds the removed payload while the bridge can refill.
This adds at most one removed request to the bridge's logical byte limit.

An out-of-window publication can start resync work while its payload still requires disposal.
Admission first commits a transfer that retains `PendingEntityPublish` and establishes `ProviderResyncNeed`.
Only then does the runtime expose the resync obligation.
The exact Host completion confirms disposal before the owner admits the separate publication release.
Either release order preserves the remaining obligation.
MissingScope never releases an unacquired lease.

FIFO pressure retains the completion, response, release, and both permits.
Submission failure retains the original command and both permits for recovery.
Causal faults retain the row even after the response receiver closes.
Synchronous runtime pumping cannot finish a disposal owned by a Host worker.
Shutdown waits for queued publications and retained disposal.
Bridge retraction wakes the shutdown waiter even when no publication remains ready.
Before that boundary, timeout can retract an unacquired request.

Each pending publication lease includes its unique checked token.
Two publications from the same plugin and scope therefore keep separate pending identities before their transfers apply.
Lease acquisition borrows the request identity and copies it only after it acquires the table lock.
A missing scope produces a terminal rejection without acquiring a lease.

Each event root uses a newly minted scope as its identity.
Provider invocations retain separate checked tokens through retries and retirement.
The scope allocator permits its final token once, then rejects further allocation without replacing existing scopes.

Each family receives a causal token before its first scoped publication changes family state.
Unscoped family state can remain tokenless.
Mutation and resync leases retain the family token through fanout, snapshot processing, and cleanup.
A recreated family receives a new causal token, even if its name and cleanup generation match the retired family.
The cleanup generation still identifies routing work outside the causal table.
Token exhaustion preserves the original publication for disposal and does not prevent retirement of existing leases.

Causal identities contain only fixed-size values.
A transfer has three inline target slots for mutation, resync, and publication disposal obligations.
The causal FIFO therefore retains a fixed payload size per occupied slot.
The queue allocates its buffer during construction.
Its logical occupied-payload limit is `CAUSAL_OWNER_PAYLOAD_BYTES`, equal to 256 times `size_of::<CausalOp>()`.
The buffer capacity can exceed 256; it does not grow while the queue operates within its slot limit.
The causal handler charges the supplied operation metadata before it removes the head.
A budget refusal preserves the head and lease and marks the drain ready for a later turn.
This does not establish a bound for all causal table storage or table update time.

The earlier production regression failed because a retained publication transfer could follow its release.
The new FIFO passes that production admission-and-finish regression.
The earlier event regression lost a release after admission backpressure.
The event path now reserves release capacity before it mints the scope or attempts admission.
When the FIFO is full, the event retains its original source and does not mint a scope.

Current local evidence:

- The first owner migration passed 36 selected library tests and 11 selected integration tests.
- Two worker tests passed with retained completions, Host permits, owner permits, and cancellation after table fault.
- The publication and table selection passed 28 library tests, including limits, unlock races, distinct waits, and fault retention.
- An asynchronous Lua tool published twice through daemon owner work. Its test passed without synchronous runtime pumping.
- All four cases in the production package cleanup test passed with the new FIFO capacity fixture.
- Formatting and the Rust 1.97.0 check with tests passed.
- The actual admission-refusal test passed with a full causal FIFO and no available Host slots.
- Two wake tests passed for a retained scan cursor, a captured upper bound, and a live waiter after ready-queue exhaustion.
- The configured full Lua integration run passed 43 tests and failed five.
- The rerun outside the sandbox passed 46 tests and failed one cross-package managed-spawn test.
- The disposal checkpoint passed 32 focused library tests.
- Those tests cover actual Host execution, independent capacity waits, stopped submission, causal pressure, fault retention, shutdown wake, and both resync release orders.
- The disposal checkpoint also passed 13 integration tests and the asynchronous Lua daemon publication test.
- The integration fixture now applies queued causal work before it checks the final lease set after out-of-window publication.

The first integration run found an oversize error-text mismatch, an obsolete directory-recovery fixture, a spawn timeout, and two socket-permission failures.
The bridge now preserves the existing oversize error code.
The obsolete fixture tested directory transactions, but the baseline already uses `plugin-db.redb`.
The existing database transaction and restart test remains and passes.
The socket-dependent spawn tests pass outside the sandbox.
The remaining cross-package test calls the synchronous runtime helper, while the managed-spawn queue has only a daemon consumer.
That timeout remains open. Source inspection shows the consumer gap predates this publication change; no baseline execution has verified that conclusion.
Two earlier integration attempts stopped before execution because candidate environment paths were incomplete.

The integration runs use the existing `e34f3c0` candidate worker and the current linked Hub library.
They do not establish a matching final Hub binary or complete foundation acceptance.

Family admission now exposes one ready mutation per step.
The publication continuation retains the response until it finishes the consecutive pending run.
Each continuation activation moves at most one mutation.

The family token tests pass for shared scopes, same-generation recreation, allocator exhaustion, and a 300,000-character family suffix.
The long-name test establishes runtime admission behavior. It does not measure owner execution time or exercise bridge byte accounting.
The lifecycle selection passed 119 library tests.
Four additional selected tests initially failed during socket setup in the sandbox; all four passed with local socket access.
The exhaustion test retains the original bridge payload through manual disposal; earlier Host tests cover worker disposal.
Eleven Lua integration tests pass for lease retirement, resync, cleanup, causal capacity, and distinct publications.
Six focused tests pass after the final cleanup fixture and settlement signature changes.
These integration results use the existing candidate worker and the current linked Hub library.
Three queue and daemon tests pass for allocation stability, metadata-budget refusal, and draining under the shared owner budget.

The owner inspection budget remains open.
Causal table updates now compare and copy fixed-size identities.
The handler charges operation metadata, but table lookup, comparison, and allocation work still require separate accounting.
Publication parsing and mutation size validation now run on the Lua worker.
The daemon now disposes of rejected payloads on a Host worker.
The owner still checks current family ownership and clones the provider-family set.
Routing leases still contain family strings outside the causal table.
The next implementation step must account for causal operations and bound the remaining table and routing work.
The broader owner scheduling, memory, final Core pin, and matched client requirements remain open.

The live causal table has a separate memory gap.
A production test invokes one Lua event handler nine times; each invocation publishes an out-of-window sequence and completes.
After each invocation, the owner permits return and the causal FIFO drains, but a distinct resync lease remains.
The final table contains nine scopes with only `ProviderResyncNeed`.
This exceeds the eight Host slots and proves that execution permits do not bound these retained obligations.
The test passes; it does not measure unlimited growth or establish a capacity policy.
Source inspection shows that later publications can rearm resync while provider snapshots remain below the high-water mark.
The next memory decision must cover the lifetime of publication descendants, including resync and provider work.

The implementation now extends the existing publication budget through descendant retirement.
The limits remain 256 publications, 8 MiB in original logical request bytes, and 1 MiB per request.
Bridge removal transfers the admission permit; it does not return capacity.
Mutation, resync, and inherited provider work share one permit from the originating publication.
An existing resync identity retains its existing permit when later publications coalesce into it.
A provider inherits the permit from the exact family obligation that selects its scope.
A publication made by that provider requires its own admission permit.
Unscoped payloads retain their permits through pending state, fanout, and disposal.
Queued retirement and faults retain ownership until the original table identity and domain item retire.
Mandatory retirement does not reserve another publication permit.
The last descendant returns capacity, even if the original event handler remains active.
The permit owns fixed accounting data only; it does not own a payload, scope, table row, or descendant.

Admission remains nonblocking: a full budget rejects before enqueue rather than waiting for dependent work.
This changes when admission capacity returns, not the per-request frame limit.
Existing event and provider-result budgets remain responsible for their own roots and payloads.
Acceptance must cover saturation, coalescing, unscoped work, provider inheritance, queued retirement, unload, and fault retention.
It must also cover a handler that makes more than 256 sequential publications after earlier descendants retire.


The shared permit owns fixed accounting data and a weak reference to the bridge account.
It does not keep the bridge queue alive.
Rust field order keeps the permit live until payload destruction finishes.
Provider invocations and prepared snapshots retain the permit after the provider table identity retires.
Delivery preserves the permit across conversion to and from a protocol frame.
The original request charge remains unchanged when preparation removes unknown fields.

Ten focused tests cover count and byte saturation, reference sharing, bridge destruction, queued retirement, coalescing, provider inheritance, and family cleanup.
A live Lua event completes 512 accepted publications through the daemon owner and returns all publication capacity.
A HostExecutor test covers cancellation, successful delivery, a full queue, disconnection, and reclamation.
The provider snapshot test invokes Lua and directly executes preparation, delivery, and reclamation after table retirement.
The family cleanup test uses the production cleanup cursor and manual payload disposal.
These tests do not establish a complete public unload campaign or final matched artifacts.

The regression selection exposed a missing completion callback in the abandoned-subscription test.
The test now installs the same callback as the production owner loop before subscription admission.
No production callback code changed.
The fixed accounting record still requires allocator work when its last reference drops.
General routing strings, complete owner work accounting, and the broader foundation acceptance remain open.


Rust 1.97.0 `check --tests` passes for the completed change.
The combined regression selection passed 93 library tests.
After the final saturation fixture extension, all ten lifetime tests passed again.
The saturated Lua provider rejects its new publication and returns its snapshot without another publication permit.
Ten Lua integration tests pass for fanout, distinct publications, cleanup, refusal, and causal contention.
Those integration tests use the existing candidate worker and the current linked Hub library.
Formatting and the patch whitespace check pass.

### Provider registration before publication admission

The Lua worker now selects an exact package and family registration before enqueue.
The Lua worker also validates the entity namespace before it reserves publication capacity.
The queued request retains one fixed registration record instead of the package string.
Owner admission checks that record before it routes the mutation.
The owner no longer clones the provider-family set or constructs the namespace token for publication admission.
The original logical request charge remains unchanged.

The lifecycle keeps only current registrations in its index.
Each queued request retains its selected record with an `Arc`.
Replacement and unload permanently invalidate old records and remove them from the index.
No numeric identity, pointer-derived identity, or registration history map exists.

Registration selection can wait for the index lock on the Lua worker.
It releases that lock before enqueue or response waiting.
The lifecycle holds no registration lock while Core stops and joins old workers.
It stops the old worker, replaces the registrations, and then starts the new worker.
This uses Core's existing unload and load operations, which compose its reload operation at revision `ca24328`.
Initial load follows the same order because Core also permits replacement through load.
The lifecycle preserves the production requirement that package effects execute serially.
The index does not independently serialize concurrent lifecycle calls.

These changes remove publication ownership validation from the owner path.
Family strings still participate in owner routing, cloning, and other indices.
Fixed record destruction still requires allocator work.
Complete owner work accounting, the memory inventory, final Core pins, and matched client verification remain open.

Registration retirement occurs at the record swap, before the lifecycle call returns.
An old queued publication can still pass admission while unload remains in progress before that swap.
Core worker shutdown and registration retirement are not one atomic operation.
Core load inserts its new worker before it stops the previous worker.
The explicit unload places that stop before registration replacement.

The selected registration and publication checks pass 37 library tests.
The runtime test queues publications before real Lua reload and unload, then checks refusal, scope retirement, and returned publication capacity.
It also checks acceptance after the same package and family load again.
Four selected Lua integration checks passed with the existing candidate worker and the current linked library.
The reserved-release integration fixture initially failed because its unowned family now fails before enqueue.
The corrected fixture queues a valid publication, unloads its package, and then admits the stale record with one causal slot available.
That check passes through public unload and the runtime publication path.
The existing lifecycle integration check also passes for returned Core cleanup and stopped runtimes.
Rust 1.97.0 `check --tests` passes.
These checks do not establish final matched binaries or complete foundation acceptance.
The final focused contract test passes for both a different owner namespace and the invalid `p:item` format.
Both cases fail before queue admission and retain no publication credit.
Formatting and the patch whitespace check pass.

### Entity delivery and Host capacity

The next provider audit found a capacity cycle before the proposed Host admission change.
Eight provider snapshots can hold all Host permits while they prepare their payloads.
Fanout previously claimed exclusive delivery before it reserved a Host permit.
The snapshots then waited for fanout delivery, while fanout waited for a Host permit from those snapshots.
Cancellation, connection close, shutdown, or the 30-second deadline can break that cycle.
Normal delivery cannot break it.

A regression uses eight real Lua provider completions and their actual Host preparation permits.
It starts fanout before the owner collects those preparation results, then resumes the production owner dispatcher.
The baseline fails its two-second progress check with nine pending operations, one capacity waiter, and eight delivery waiters.
This result proves the capacity cycle for the tested scheduling order.

The implementation now tracks the pending fanout separately from exclusive delivery ownership.
Both snapshots and fanout reserve Host capacity before they claim delivery.
A delivery owner can therefore complete its remaining phases with its retained permit.
The exact fanout waiter prevents duplicate fanout work while it waits for capacity.
The final retirement clears that identity, including cancellation and an empty queue.
The existing capacity and delivery notifications continue to wake their own waiters.
Fanout still removes mutations in global FIFO order.
A prepared snapshot can pass fanout that has not obtained Host capacity.
Verification must check subscriber sequence order after that case.

The corrected capacity test passes with all eight subscriber sequence checks.
Each subscriber receives snapshot 1, skips the superseded sequence-1 mutation, and receives mutation 2 through bridge admission and owner dispatch.
The test returns all publication credits and Host permits.
The related library selection passed 34 tests.
An earlier selection had 33 passes and a fixture setup failure before the cycle assertion.
The fixture now retires and retries only explicit Core admission backpressure before it forms the eight-provider test state.
The final focused test passes after that adjustment.
Four Lua integration tests pass for lease completion, detached cleanup, and retained fanout transitions.
They use the existing candidate worker and the current linked library, not final matched artifacts.
The two owner checks pass for refused admission retirement and shutdown with retained entity payloads.
Formatting and the patch whitespace check pass.
Provider preparation, Core admission encoding, family-string routing, and complete memory/work accounting remain open.

### Entity model execution boundary: source map under review

This section records the next replacement boundary at `f188592`.
It does not claim implementation or acceptance.
The owner still performs family string comparisons, allocations, and destruction in the paths below.
Moving only publication validation does not meet the shared owner budget.

The candidate boundary moves the existing family model through finite Host phases.
The model contains the family map, fanout queue, resync release index, epoch, and family token counter.
The owner retains causal table authority and the shared scheduler.
The existing Host executor supplies execution and completion capacity.
No additional executor, scheduler, family history map, or numeric limit is proposed.

| Existing operation | Host phase | Owner result and ordering |
| --- | --- | --- |
| `admit_package_entity_publish_inner` | Check the retained registration, admit one mutation, and update the fanout and resync indices. | Retain the publication response and original credit. Apply at most one causal transition before another model phase. |
| `advance_entity_publish` | Move one consecutive pending mutation into global fanout. | Retain the same publication continuation. Complete its response only at the existing retirement boundary. |
| `take_one_package_entity_fanout` | Remove one global FIFO entry. | Retain its finish record and publication credit through delivery and reclamation. |
| `prepare_finish_op` | Update resync state and construct the corresponding transition together. | Apply one transition after the existing payload reclamation boundary. |
| `begin_package_entity_provider_snapshot` | Advance the provider floor and update the resync release index. | Apply scalar progress before subscriber delivery. Keep exclusive delivery ownership separate from model ownership. |
| `step_package_entity_provider_snapshot` | Remove one payload, remove one resync obligation, or return completion. | Retain the removed payload through delivery or reclamation. Apply any release before restoring model availability. |
| `mark_package_entity_resync_needed` and `rearm_package_entity_resync` | Update one family and its resync release index. | Publish the existing resync change notification after the model returns. |
| `record_package_entity_resync_attempt` | Update one attempt and inspect degradation. | Retain the existing retry policy. Separate any obligation release into its own causal result. |
| `retry_family_resync_release` and `release_one_degraded_package_entity_resync_lease` | Remove one exact releasable obligation and update its index. | Apply one release before restoring model availability. |
| `step_host_package_entity_cleanup` | Advance the existing cleanup cursor by one item. | Retain each release through payload reclamation. A detached family belongs to the cursor and does not reserve the live model. |
| Family generation, progress, and resync deadline readers | Read the model within a Host phase. | Consume scalar results under the same ordering as mutations. Revalidate the family when a later phase starts. |
| `next_package_entity_resync_family` | Advance the existing family cursor. | Retain the current change and deadline rules. Move family names opaquely through the owner. |
| `prepare_plugin_entity_snapshot` | Select the provider and its exact family obligation. Prepare the invocation off the owner. | Acquire `ProviderInFlight` on the owner before Core admission. Retain the selected publication credit until acquisition or retirement. |

The daemon callers include `publication_owner`, the entity delivery continuation, package cleanup, and `subscription::entity_resync`.
The entity delivery continuation currently reads family generation on every activation, including before it consumes a Host completion.
That reader must change with the mutation boundary.
The request completion path also calls `package_entity_work_pending`, which scans family resync state.
That readiness query must use a published scalar result or the existing resync notification.
Synchronous runtime helpers must call the same model operations outside daemon execution.
They must not create a second implementation.

The selected ownership boundary keeps one shared `PackageEntities` model.
It consolidates the existing family and fanout locks with the related model indices.
Host workers lock this model during finite phases.
The production owner does not lock the model.
An owner-held reservation orders each phase through causal table application.
The owner reserves Host capacity before it grants the model reservation.
The owner releases the model reservation only after the causal result reaches the table.
Existing queued causal work must precede that result.
FIFO admission alone does not order the later direct provider acquisition.
Causal draining and its table wake must not require the model reservation.
The owner retains the fixed causal result independently and applies it without another Host phase.

Moving the sole model through Host commands was considered and rejected.
Generic panic handling, result normalization, and unmatched completion handling can destroy a command or result.
Shared ownership preserves the model across those paths and avoids a separate protocol for returning the sole model.
Partial mutation still requires a faulted reservation and the original ownership records.

Model ownership must not span Core execution or subscriber delivery.
Each later model phase revalidates the family generation or token.
Reload preserves existing family state, including state for a removed descriptor family.
Unload cleanup retains its existing epoch and namespace rules.
This replacement must not add family cleanup to reload.

Two premises remain unresolved before Host execution:

1. Partial mutation must remain faulted with its original ownership records.
   Submission refusal, phase exhaustion, and mismatched completion must retain the exact command or result and model reservation.
   Cancellation can suppress the response but must preserve the exact job identity and completion route.
   Result normalization must not discard a pending causal result.
   Queued-job destruction and completion disconnection require an explicit terminal shutdown boundary.
2. Provider preparation needs a retained byte charge after its finite Host phase ends.
   Core admission encodes the invocation on its caller, so that call must also move off the owner.
   Holding all eight Host permits during Lua execution can prevent provider publications from advancing.
   Keeping eight full prepared-byte charges can cause the same dependency after execution slots return.
   The replacement needs an existing, sufficient metadata budget before it releases those reservations.

The next decisive check is the complete ownership protocol for these two conditions.
The first production proof must combine publication, provider snapshots, and cleanup through the actual owner dispatcher.
It must include full Host capacity, causal contention, cancellation, stale completion, and retained fault ownership.
Family model relocation alone will not close subscription string accounting, causal allocator work, or the complete memory inventory.

The first extraction now places family state, fanout, the resync release index, epoch, and family token counter in `PackageEntities`.
One shared lock replaces the separate family and fanout locks.
The model returns publication and fanout causal transitions to the runtime.
The runtime keeps its existing causal reservations and table authority.
The existing cleanup cursor accesses the related indices through one model guard.
These calls still execute synchronously; Host execution and the model reservation remain unimplemented.

The selected regression group passes 67 library tests, including the actual full-Host-capacity test and resync deadline tests.
Review found an extra resync notification for stale fanout finishes.
The model now returns a change flag that preserves the previous notification condition.
The extended stale-family test passes and checks that notification directly.
The initial check failed on a test guard lifetime after the lock consolidation.
The corrected test uses its existing model guard and releases that guard before runtime destruction.
Four Lua integration checks pass for lease completion, detached resync cleanup, and retained fanout transitions.
They use the existing candidate worker and the current linked Hub library.
These results do not establish final matched artifacts or owner work-budget closure.
Rust 1.97.0 `check --tests`, formatting, and the patch whitespace check pass.
Independent review found no remaining extraction defect after the notification correction.

### Finite provider admission and retained expectations

Provider request construction and Core admission encoding now execute on Host workers.
The daemon retains separate preparation, causal acquisition, and admission phases.
The owner installs the selected expectation and causal token before Host submits the request to Core.
Causal acquisition uses the existing nonblocking table operation and retains the selected token across contention.
Synchronous callers use the same preparation algorithm outside the daemon path.

A `SharedView` charge covers the retained expectation before Host copies its fields.
The charge includes the expectation record, entity family, plugin key, and handler identifier.
It counts logical bytes; it does not measure allocator overhead or string capacity.
The request stays under the existing Host preparation reservation through Core admission.
Preparation checks the request with the largest scalar scope metadata before admission.
No new numeric limit or Core contract was added.

Confirmed admission releases the Host slot and its full prepared-byte reservation.
The expectation keeps its separate shared-view charge while Core executes Lua.
A later result preparation reserves Host capacity again and preserves the next phase identity.
Known refusal retains its original Host slot through causal retirement and error preparation.
Host destroys retained request metadata and refusal strings during disposal.

The owner checks each completion variant against the retained stage before consuming the completion.
An unexpected result retains the exact completion and permit in a faulted phase.
An early Core result remains retained until Host confirms admission.
Cancellation does not release a possibly admitted invocation after an ambiguous Host failure.
These fault checks retain ownership; they do not add recovery from partial Core admission.

The focused regression run passed 57 library tests.
Two additional boundary tests passed after independent review corrected the exact-lease assertions.
The early-result test then passed its expanded preparation, acquisition-contention, admitted-cancellation, delivery, and fault cases.
Rust 1.97.0 `check --tests`, formatting, and the patch whitespace check pass.
The checks establish:

- Eight admitted providers retain their expectations while all eight Host slots remain available.
  Their real Lua publications and snapshots then complete through the owner dispatcher.
- An actual Core result can arrive before Hub consumes the Host admission completion.
  The owner delays delivery and preserves the accepted scope and token.
- An injected generic Host failure retains the exact lease, metadata charge, Core result bytes, and Host and owner permits after cancellation.
  This injects the failure result; it does not cause a panic inside Core admission.
- Metadata capacity refusal precedes provider selection.
  Actual Host disposal returns the charge, and a later preparation reuses the capacity.
- Cancellation before admission and during causal acquisition contention disposes of metadata on Host.
  Cancellation after admission retires the exact acquired lease.
- The updated causal saturation fixture retains its Host slot through refusal retirement.
  The full-capacity snapshot and fanout fixture preserves its original delivery assertions.

The first regression run exposed three fixtures that assumed synchronous admission.
The revised fixtures drive Host admission and preserve their causal, cancellation, and capacity checks.
Review also found wrong-stage completion acceptance and owner destruction of retained refusal strings.
Both defects were corrected before the final boundary checks.

Family model execution, provider family selection, descriptor readiness checks, and variable-length completion routing still touch the owner.
The next phase must move those operations under the shared model reservation described above.
Complete owner work and memory accounting, final Core allowance integration, canonical pins, and matched client evidence remain open.
