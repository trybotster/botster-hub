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

Required tests cover immediate row removal, retained closed handles, live Empty rows, generation isolation, and delayed old-cell retirement.
This change removes the two global pruning scans. It does not establish diagnostic lock progress or close other owner scheduling findings.

Validation used Rust 1.97.0 with `CARGO_INCREMENTAL=0` and two Cargo jobs.
The `event_plane_counters::tests` suite passed 17 tests. The `package_event_router::tests` suite passed 60 tests.
