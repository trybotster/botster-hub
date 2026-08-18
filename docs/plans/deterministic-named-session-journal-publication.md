# Deterministic named-session journal publication for entity projection catch-up

Ticket: `ticket_1787007684_566852`
Run: `run_1787007689_823119`
Target repository: `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`)
Pipeline step: Plan (`botster_stack_plan`)

## Required guarantee (from the ticket)

After a client issues a production operation for a named `session_id`, Hub must publish that session into the lifecycle journal and into the session entity projection through a deterministic state transition. A later Subscribe `JournalPull` plus `ProjectionApply` turn must be able to deliver that identity on the entity stream. The transition must name the exact session. A bounded global Observe slice of eight sessions is not sufficient.

## Context loaded

Playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-hub-playbook]] (repository charter).

Targeted atomic notes:
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] (test-oracle discipline)
- [[Hub background fairness must stay policy-neutral]]

Runtime-teardown class: does **not** apply. This ticket changes lifecycle journal and projection catch-up on the control plane. It does not touch WebRTC or peer teardown, SessionIo/ClientWorker teardown, multi-peer ownership, resource spin, or terminal-state divergence. [[botster runtime teardown lenses]] is therefore not loaded, per the planner overlay rule.

## Base-code findings (this worktree, branch head 66ca79c)

1. `DaemonRequest::ReadScreen` (`src/daemon_transport.rs:3468`) runs only `observe_lifecycle_turn` — a bounded **global** observe slice (32 sessions, 25 ms, `resume: None`; `src/daemon_transport.rs:4952`). It has no exact-session observe and no journal-to-projection catch-up.
2. `HubRuntime::observe_session_lifecycle` (`src/runtime.rs:3539`) already exposes the exact-session Core query. Only ShutdownSession classification uses it today (`src/daemon_transport.rs:4681`).
3. Pinned Core `fc541a5` (Cargo.lock): `spawn` appends a lifecycle journal upsert for the new session (Core `daemon.rs:540`). Observation journals a row only when the registry state **changes** (`reconcile_lifecycle_observations`). Journal capacity is 1024. `observe_session_lifecycle` exact-observes the named session, then returns a typed `Found` / `Absent` / error lookup.
4. Hub projection catch-up: `run_journal_pull_slice` returns with **zero progress** while baseline recovery is in flight (`src/daemon_maintenance.rs:596`). Subscribe entity turns run exactly one `JournalPull` plus one `ProjectionApply` (`src/daemon_entity_subscriptions.rs:278-289`), so a Subscribe catch-up turn during baseline recovery makes no progress. `Baseline` slices run only on owner-loop idle round-robin turns, and every authoritative mutation resets the round-robin pointer to `JournalPull` (`prefer_journal_pull`), which can starve `Baseline` under a spawn burst. Snapshot assembly gates on `baseline_complete && !gap`.
5. The parent 24-identity oracle is in-tree: `ready_spawn_stays_within_budget_during_session_snapshot_assembly` asserts a complete first snapshot with at least 24 items (`tests/hub_daemon_lifecycle/sessions.rs:3596-3644`).

Interpretation of the ticket evidence against this base: the journal already holds all spawned identities (Core journals at spawn). The nondeterminism lives in Hub catch-up — zero-progress Subscribe turns during baseline recovery, baseline starvation under mutation bursts, and reliance on a bounded global observe slice that can cut before it reaches a named session.

## Scope

Smallest change that satisfies the guarantee, in three parts:

### 1. Named-session publication turn (new helper)

Add a helper beside `observe_lifecycle_turn` in `src/daemon_transport.rs` (logic can live in `src/daemon_maintenance.rs`):

- Call `runtime.observe_session_lifecycle(&session_id, now)`. This is the exact-session deterministic transition. It journals any pending lifecycle change (for example exit) for exactly this session before the operation returns.
- On `Found(record)`: publish the record into the shared `state.maintenance.projection` through a new exact-session upsert API on `SessionProjection`. The upsert must **not** advance the projection cursor and must **not** change `baseline_complete` or `gap`. It must mark `projection_dirty`, queue the session family delta (same path `run_projection_apply_slice` uses), and wake the scheduler.
- If `runtime.take_journal_advanced_wake()`: run one bounded `JournalPull` then one `ProjectionApply` via `run_maintenance_kind` (the existing pattern at `src/daemon_transport.rs:4996-5008`).

### 2. Wire the helper into the named-session production operation

Wire the helper into `DaemonRequest::ReadScreen` after the existing bounded global slice. ReadScreen is the production operation the parent proof issues. The helper stays reusable for other named-session operations, but no other operation is wired in this ticket.

### 3. No zero-progress Subscribe catch-up turns

While baseline recovery is in flight, `run_journal_pull_slice` currently returns without progress. Change it to run one bounded `Baseline` page in that case (one bounded slice per turn — either a journal page or a baseline page, never both). This makes every Subscribe catch-up turn advance recovery deterministically, so a bounded number of turns — derived from work size, not wall clock — completes the baseline and delivers the named identity.

## Non-scope

- No change to `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, or `BASELINE_PAGE_BUDGET` values.
- No wall-clock bounds. No turn-count inflation as proof. No weakening of the 24-identity oracle on `ticket_1786938984_190098`.
- No Core changes. The required Core primitives (exact-session observe, spawn journal upsert, bounded pages) already exist at the pinned revision `fc541a59`.
- No wiring of ReadModeFlags, SendInput, Resize, or Attach. Flag for review only if the guarantee class demands them.
- No absorption of `ticket_1786938984_190098`, `ticket_1786977409_499180`, `ticket_1786937228_425608`, or `ticket_1786913892_208903`.

## Ownership boundaries and cross-repo dependencies

- Hub owns the named-session publication turn, projection catch-up policy, and Subscribe delivery — all control plane. This matches [[botster hub is a first party host profile over core]].
- Core owns journal mechanics, the exact-session query, and bounded page APIs. All required primitives exist at the pinned revision, so **no cross-repository dependency is registered**. If Plan Review reads the ticket as requiring an unconditional journal append per named operation (a Core journal-semantics change), that becomes a `botster-core` dependency ticket — see Unknowns U1.
- Sibling seam: `ticket_1786977409_499180` touches its own ReadScreen exact-session observe path for the ShutdownSession OperatorError repair. Same handler, different guarantee. Merge-order conflict risk only; whichever lands second rebases.

## Assumptions and unknowns

- A1: Exact-session observe of a running, unchanged session appends no new journal row. The "publish into the lifecycle journal" half of the guarantee is satisfied by the spawn upsert already in the journal plus exact-session observation of any pending transition. The projection half is satisfied by the direct named upsert plus deterministic catch-up.
- A2: The direct projection upsert is safe for delivery ordering when it does not advance the cursor. Implement must verify `change_seq` and delivery-phase interaction with unit tests before wiring.
- A3: The parent failure shape at this base reproduces through the zero-progress / baseline-starvation path. Implement must first reproduce with focused repetition of the in-tree oracle and confirm the mechanism before changing code.
- U1: If review determines the ticket requires an unconditional journal row per named operation, that is Core-owned journal semantics and needs a dependency ticket against `botster-core`. Ask before broadening; do not silently absorb.

## Affected surfaces and files

- `src/daemon_transport.rs` — ReadScreen handler, new helper call site.
- `src/daemon_maintenance.rs` — named-session publication logic, baseline-progress change in `run_journal_pull_slice`, unit tests.
- `src/session_projection.rs` — exact-session upsert API that preserves cursor and baseline flags.
- `src/daemon_entity_subscriptions.rs` — only if the Subscribe turn needs explicit wiring beyond part 3.
- `tests/hub_daemon_lifecycle/sessions.rs` — new deterministic named-session catch-up test.
- `docs/plans/deterministic-named-session-journal-publication.md` — this plan.

## Risks

1. Cursor-contract corruption from the direct projection upsert (row `change_seq` versus snapshot assembly phases). Mitigation: unit tests for cursor stability, flag stability, and delta queueing before transport wiring.
2. Ready-operation budget regression from bounded work added to request turns. Mitigation: both `ready_spawn_stays_within_budget_*` tests must pass under focused repetition with unchanged budgets; keep one bounded slice per turn per [[Owner loop must not stack maintenance and pump ahead of queued control]].
3. Round-robin fairness drift from running a baseline page inside the JournalPull slot. Mitigation: either/or per turn, policy-neutral, per [[Hub background fairness must stay policy-neutral]].
4. Merge conflict with `ticket_1786977409_499180` in the ReadScreen handler. Mitigation: additive helper, small diff, rebase second-lander.
5. Flake-oracle discipline: new test must print full typed error bodies and use instance-scoped counters, per the charter's flake notes.

## Acceptance checks and tests

1. New integration test in `tests/hub_daemon_lifecycle/sessions.rs`: spawn 24 sessions (more than one observe slice), issue production ReadScreen for a named late session, then drive Subscribe catch-up turns whose count derives from journal/baseline work size. Assert the named identity arrives on the entity stream. No sleeps, no wall-clock bounds. Pass focused repetition (at least 10 runs).
2. In-tree parent oracle `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (first snapshot >= 24) passes focused repetition at unchanged budgets.
3. `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` passes unchanged.
4. Unit tests: the named upsert does not advance the cursor, does not clear `gap` or `baseline_complete`, survives an in-flight baseline recovery, and queues the subscriber delta. `run_journal_pull_slice` makes bounded progress every turn during recovery.
5. Repo gates: `./test.sh` (workspace-wide; the `--workspace` flag is load-bearing), `cargo fmt --check`, `cargo clippy` strict.
6. Downstream proof (charter requirement): record the passing 24-identity oracle evidence at this branch as the unblock proof for `ticket_1786938984_190098`. That ticket resumes its own focused Implement and Review gates after this lands; its oracle is not weakened here.

## Vault gaps worth capturing

- A note that Core journals a session at spawn and re-journals only on registry-state change — the journal-publication half of named-session catch-up is spawn-owned, not observe-owned.
- A note that Subscribe catch-up turns must never be zero-progress turns while baseline recovery is in flight (once implemented and proven).
