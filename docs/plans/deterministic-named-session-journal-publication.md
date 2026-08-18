# Deterministic named-session journal publication for entity projection catch-up

Ticket: `ticket_1787007684_566852`
Run: `run_1787007689_823119`
Target repository: `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`)
Pipeline step: Plan (`botster_stack_plan`)
Revision: 2 — addresses Plan Review `review_1787012075_184614` and durable answer `question_1787011706_241109`.

## Required guarantee (from the ticket)

After a client issues a production operation for a named `session_id`, Hub must publish that session into the lifecycle journal and into the session entity projection through a deterministic state transition. A later Subscribe `JournalPull` plus `ProjectionApply` turn must be able to deliver that identity on the entity stream. The transition must name the exact session. A bounded global Observe slice of eight sessions is not sufficient.

## Durable ruling (question_1787011706_241109)

The named production operation must make the exact session's current lifecycle state available to a later `JournalPull` and `ProjectionApply` turn **even when the session state is unchanged**. The earlier spawn row is not sufficient. The mechanism must be Core-owned (append or republish an idempotent lifecycle row, or an equivalent Core mechanism with the same deterministic result). Hub must not implement journal internals. If the current Core API cannot provide the guarantee, register a botster-core ticket and dependency. Hub ownership stays limited to invoking the exact-session operation and publishing the resulting Core state.

## Context loaded

Playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-hub-playbook]] (repository charter), [[botster-architecture]], [[cli-patterns]].

Effect of the two map notes: [[botster-architecture]] confirms the lifecycle cluster this plan already applies (journal/observe/owner-loop conventions) and adds the pipeline-context rules used below ([[dependency ticket creation must start its run or emit an operator action]], [[dependency closure must requeue the blocked parent step]]). [[cli-patterns]] is a mixed-generation index; per its own header and [[botster-architecture]], it must not drive current ownership. Neither note changes the product mechanism.

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

1. `DaemonRequest::ReadScreen` (`src/daemon_transport.rs:3468`) runs only `observe_lifecycle_turn` — a bounded **global** observe slice (32 sessions, 25 ms, `resume: None`; `src/daemon_transport.rs:4952`). It has no exact-session step and no deterministic journal publication.
2. `HubRuntime::observe_session_lifecycle` (`src/runtime.rs:3539`) exposes the exact-session Core query. Only ShutdownSession classification uses it today (`src/daemon_transport.rs:4681`).
3. Pinned Core `fc541a5` (Cargo.lock): `spawn` appends a lifecycle journal upsert (Core `daemon.rs:540`). Observation journals a row **only when the registry state changes** (`reconcile_lifecycle_observations`). No Core API republishes an unchanged named session into the journal — verified by enumerating the public lifecycle/journal APIs at the pin. This is the Core gap the durable ruling anticipated.
4. Hub projection catch-up: `run_journal_pull_slice` returns with **zero progress** while baseline recovery is in flight (`src/daemon_maintenance.rs:596`). Subscribe entity turns run exactly one `JournalPull` plus one `ProjectionApply` (`src/daemon_entity_subscriptions.rs:278-289`), so a Subscribe catch-up turn during baseline recovery makes no progress. `Baseline` slices run only on owner-loop idle round-robin turns, and every authoritative mutation resets the round-robin pointer to `JournalPull` (`prefer_journal_pull`), which can starve `Baseline` under a spawn burst. Snapshot assembly gates on `baseline_complete && !gap`.
5. The parent 24-identity oracle is in-tree: `ready_spawn_stays_within_budget_during_session_snapshot_assembly` asserts a complete first snapshot with at least 24 items (`tests/hub_daemon_lifecycle/sessions.rs:3596-3644`).

## Cross-repository dependency (registered)

- Dependency ticket: `ticket_1787012251_733106` — "Core: exact-session lifecycle journal republication for named-session catch-up", target `botster-core` (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`). Registered as a blocking dependency of this ticket (`dependency_1787012255_438086`), with an active `botster_stack_delivery` run (`run_1787012310_229185`).
- Contract: a Core-owned exact-session operation publishes the named session's current lifecycle state into the journal even when unchanged; Core owns cursor authorship, idempotency/dedup, capacity policy, and the `journal_advanced` wake. After it returns, `lifecycle_changes_page` from any cursor at or before the operation watermark delivers that session's current state.
- Consumption seam: botster-hub consumes Core by pinned git revision. This ticket's Implement starts by bumping the Cargo.lock pin to the merged Core revision that carries the primitive.

## Scope (revised)

Two independent Hub changes, both consuming Core-authored journal state only:

### 1. Named-session publication turn in ReadScreen (scheduling only, no stacked slices)

In `DaemonRequest::ReadScreen`, after the existing bounded global slice:

- Call the Core exact-session republication operation (via `HubRuntime`) for the request's `session_id`. This is one bounded exact-session unit of request-classified work, the same class as ShutdownSession classification. It appends the Core-authored journal row (idempotent, even when unchanged) and sets the `journal_advanced` wake.
- Then **schedule only**: call `state.maintenance.note_authoritative_mutation()` so the existing bounded scheduler prefers `JournalPull` on the next maintenance turn. Do **not** run `JournalPull` or `ProjectionApply` synchronously inside the request turn. One owner turn keeps at most one bounded background slice, per [[Owner loop must not stack maintenance and pump ahead of queued control]].

Delivery path: the row is durable in the Core journal (capacity 1024) the moment the operation returns. Any later Subscribe turn's `JournalPull` + `ProjectionApply` — the production path quoted by the ticket — delivers the identity with Core-authored cursors and ordinary contiguous sequencing. Hub fabricates no sequence and touches no projection row directly. This retires revision 1's cursor-preserving direct upsert entirely.

State-corner behavior (per Plan Review finding on sequence contract):
- Cursor `None` / baseline in flight: the journal row waits in Core; catch-up turns apply it after the baseline seals (the registry-backed baseline also already contains the session). No Hub-side special case.
- Overwrite: repeated republication flows through `apply_change` upserts keyed by session UUID — last Core-authored change wins, the existing contract.
- Existing subscribers: `run_projection_apply_slice` queues the family delta from the Core-authored change exactly as for any journal change.

### 2. No zero-progress Subscribe catch-up turns

While baseline recovery is in flight, `run_journal_pull_slice` currently returns without progress. Change it to run one bounded `Baseline` page in that case (one bounded slice per turn — either a journal page or a baseline page, never both). Every Subscribe catch-up turn then advances recovery deterministically, so a bounded number of turns — derived from work size, not wall clock — completes the baseline and unlocks snapshot assembly.

## Non-scope

- No change to `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, or `BASELINE_PAGE_BUDGET` values.
- No wall-clock bounds. No turn-count inflation as proof. No weakening of the 24-identity oracle on `ticket_1786938984_190098`.
- No Hub-side journal internals, fabricated sequences, or direct projection writes.
- No wiring of ReadModeFlags, SendInput, Resize, or Attach in this ticket.
- No absorption of `ticket_1786938984_190098`, `ticket_1786977409_499180`, `ticket_1786937228_425608`, or `ticket_1786913892_208903`.

## Ownership boundaries

- Core owns the journal: republication semantics, cursor authorship, idempotency, capacity, wake. Delivered by dependency ticket `ticket_1787012251_733106`.
- Hub owns invoking the exact-session operation on the named production path, scheduling catch-up through the existing bounded scheduler, and Subscribe delivery. This matches the durable ruling and [[botster hub is a first party host profile over core]].
- Sibling seam: `ticket_1786977409_499180` touches the same ReadScreen handler for the ShutdownSession OperatorError repair. Merge-order conflict risk only; whichever lands second rebases.

## Assumptions and unknowns

- A1 (was U1, now resolved by `question_1787011706_241109`): the guarantee requires Core-owned republication even for unchanged sessions; the dependency ticket carries it.
- A2: the Core primitive's exact API shape (extended `observe_session_lifecycle` versus sibling method) is Core's decision; Hub Implement adapts the `HubRuntime` facade call to whatever the merged Core revision exposes.
- A3: the parent failure shape at this base reproduces through the zero-progress / baseline-starvation path; Implement reproduces it first with focused repetition of the in-tree oracle before changing code.

## Affected surfaces and files

- `Cargo.toml` / `Cargo.lock` — Core pin bump to the dependency-carrying revision.
- `src/runtime.rs` — `HubRuntime` facade for the Core republication operation.
- `src/daemon_transport.rs` — ReadScreen handler: exact-session call plus scheduling.
- `src/daemon_maintenance.rs` — baseline-progress change in `run_journal_pull_slice`, unit tests.
- `tests/hub_daemon_lifecycle/sessions.rs` — two new isolated integration tests.
- `docs/plans/deterministic-named-session-journal-publication.md` — this plan.

## Risks

1. Core dependency latency: this ticket blocks on `ticket_1787012251_733106`; the parent ticket blocks on this one. Mitigation: dependency run already active; [[dependency closure must requeue the blocked parent step]].
2. Ready-operation budget regression from the exact-session call in request turns. Mitigation: it is one bounded exact-session unit (same class as ShutdownSession classify); both `ready_spawn_stays_within_budget_*` tests must pass under focused repetition with unchanged budgets.
3. Round-robin fairness drift from paging the baseline in the JournalPull slot. Mitigation: either/or per turn, policy-neutral, per [[Hub background fairness must stay policy-neutral]].
4. Merge conflict with `ticket_1786977409_499180` in the ReadScreen handler. Mitigation: additive, small diff; second-lander rebases.
5. Flake-oracle discipline: new tests must print full typed error bodies and use instance-scoped counters, per the charter's flake notes.

## Acceptance checks and tests (each proof isolated)

1. **Named-publication proof (fails if only change 1 or the Core primitive is reverted; independent of change 2).** Integration test: spawn sessions, subscribe, and drain until the subscriber is fully caught up (baseline complete, journal drained, no pending deltas). With the target session unchanged, issue one production ReadScreen for its named `session_id`. Drive Subscribe catch-up turns bounded by the journal delta size and assert the named identity arrives as a Core-authored change on the entity stream. Without the named publication, no new row exists and the assertion fails; change 2 cannot mask it because the baseline is already complete. No sleeps, no wall-clock bounds. At least 10 focused repetitions.
2. **Baseline-progress proof (fails if only change 2 is reverted; independent of change 1).** Integration test: force in-flight baseline recovery with more sessions than one baseline page (more than 16), then drive only Subscribe catch-up turns — no ReadScreen. Assert each turn makes bounded progress and that a work-size-derived number of turns completes the baseline and delivers the identities. Without change 2 the turns are zero-progress and the test fails deterministically.
3. In-tree parent oracle `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (first snapshot >= 24) passes focused repetition at unchanged budgets.
4. `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` passes unchanged.
5. Unit tests in `daemon_maintenance`: JournalPull slot runs exactly one bounded slice per turn (journal page or baseline page, never both); scheduling after ReadScreen sets the journal-pull preference without running slices in the request turn.
6. Repo gates: `./test.sh` (workspace-wide; the `--workspace` flag is load-bearing), `cargo fmt --check`, strict clippy.
7. Downstream proof (charter requirement): record the passing 24-identity oracle evidence at this branch as the unblock proof for `ticket_1786938984_190098`, without weakening that oracle.

## Vault gaps worth capturing

- Core journals a session at spawn and re-journals only on registry-state change; named-session catch-up therefore needs a Core-owned republication primitive (capture once the Core ticket lands).
- Subscribe catch-up turns must never be zero-progress while baseline recovery is in flight (capture after Implement proves it).
