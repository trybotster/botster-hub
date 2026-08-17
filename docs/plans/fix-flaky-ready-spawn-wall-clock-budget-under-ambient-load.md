# Plan: Fix flaky ready_spawn wall-clock MAX_READY_OPERATION_WAIT_MS budget tests

Ticket: `ticket_1786938984_190098`
Run: `run_1786944939_873939` (re-attached; first authored under `run_1786939902_746312`, which was cancelled during Plan Review because its run record carried a corrupted target_id)
Revision: v2, addressing Plan Review `review_1786945973_864427` (changes_required). The load-window liveness oracle is withdrawn. The ordering proof moves to decision-level unit tests whose negative controls ablate the actual scheduling decisions.
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

Two lifecycle-suite tests assert wall-clock elapsed around one `Spawn` request `<= MAX_READY_OPERATION_WAIT_MS` (50 ms) through a real CLI daemon child:

- `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (`tests/hub_daemon_lifecycle/sessions.rs:3597`, assertion at `:3634`). Observed: 93.4 ms in the suite run; 108.4 ms alone.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` (`tests/hub_daemon_lifecycle/sessions.rs:3547`, assertion at `:3587`). Observed: 69.6 ms in the suite run; 110.7 ms alone.

Both failures reproduce on clean base `origin/main` `547ca38`, in the suite and in single-test runs, under ambient workspace load. This is the same failure class as the merged separators and near-limit lib-suite repairs: wall-clock duration is not a valid oracle under load. This plan repairs the test oracle. Production budgets and scheduling stay unchanged.

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`, confirmed from the ticket worktree `origin` remote and from the prior botster-hub plan that used the same target).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22` (from the ticket record; it is a registered project target).
- Discrepancy resolved: the cancelled run `run_1786939902_746312` carried a corrupted `target_id` (`tgt_7e208a0c76a449f4ac0c99953a799869`, a merge of the ticket target prefix and the `tgt_40abcf71ccf049f4ac0c99953a799869` suffix). The replacement run `run_1786944939_873939` carries the correct `tgt_7e208a0c76a44980a83b63af976b1f22`, which `list_spawn_targets` resolves to `botster-hub` at `trybotster/botster-hub`.
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786938984_190098`, base `547ca38` (clean).
- The worktree path contains no colon. `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon transport, the owner loop, and this lifecycle test suite.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene.
- [[botster-architecture]] -- current modular repository map; confirms Hub owns the daemon transport and this suite (required by the planning overlay; recorded per Plan Review finding `finding_1786945973_896149`).
- [[cli-patterns]] -- Rust CLI/runtime pattern index; [[hub daemon runtime stays on one owner thread while socket handlers submit requests]] confirms the single-owner control-submission model this plan reasons about.
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] -- the flake class: scheduler preemption makes wall-clock elapsed a load-sensitive oracle; the durable pattern asserts work or behavior, not time.
- [[conformance harnesses gate on deterministic invariants not timing]] -- gate on deterministic invariants; record durations as observations only.
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] -- sibling repair precedent inside the lib suite.
- [[Owner loop must not stack maintenance and pump ahead of queued control]] -- the production invariant these two tests protect.
- [[Hub background fairness must stay policy-neutral]] -- production owner-turn and ready-operation budgets stay unchanged.
- [[a regression test must be shown to go red with the fix reverted]] -- the repaired oracle needs a sabotage red-proof.
- Runtime-teardown class: does not apply. This is a test oracle repair for wall-clock latency assertions. No peer lifecycle, teardown, ownership, or resource-spin surface changes. [[botster runtime teardown lenses]] was not loaded, per the class rule.

## Context loaded

- Ticket record, run record, gate, and empty prior artifacts/checklists via `project_pipelines_current_context`.
- Prior art in this repository:
  - `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` and commit `cd5e7a8` -- the merged sibling repair idiom and the plan format Plan Review accepted.
  - `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` -- first repair in this class.
  - `docs/reports/*-implement.md` -- implement-report destination convention.
  - `docs/loaded-daemon-lifecycle-runner.md` -- the isolated GitHub Actions runner for repeated loaded lifecycle campaigns.
- Code read:
  - The two test bodies (`tests/hub_daemon_lifecycle/sessions.rs:3546-3647`).
  - `MAX_READY_OPERATION_WAIT_MS = 50` and `MAX_OWNER_TURN_MS = 25` (`src/daemon_maintenance.rs:27-29`), `OBSERVE_SLICE_BUDGET` (8 sessions / 64 KiB / 8 ms).
  - The owner loop (`src/daemon_transport.rs:265-285`): each iteration calls `control_rx.try_recv()` before it decides to run one maintenance slice, and the blocking wait uses a `biased` select that prefers control (`src/daemon_transport.rs:170-175`). Serving queued control before a second slice is structural.
  - `run_one_owner_maintenance_slice` (`src/daemon_transport.rs:177-228`): one slice per turn, `reconciliation_wakes` counts slices.
  - Client surface: `DaemonRequest::ListSessions` returns `DaemonSession { session_id, lifecycle }` rows (`crates/botster-hub-client/src/lib.rs:892`, `:2371`); `DaemonRequest::Status` exposes `session_count` and `lifecycle_counters`.
  - `tests/session_projection_owner_loop.rs` -- const-asserts the published budget relations; that coverage keeps the constants meaningful without wall-clock measurement.
- `test.sh` -- the repo wrapper: asset-sync check, `BOTSTER_ENV=test`, `cargo test --workspace`. Targeted forms such as `./test.sh --locked --test hub_daemon_lifecycle_test` keep working.
- Blocking relation: `ticket_1786937228_425608` binds its acceptance on zero-failure `./test.sh --locked --test hub_daemon_lifecycle_test` runs and stays red until this ticket merges. This ticket must not absorb that one or `ticket_1786913892_208903`.

## Failure mechanism

Each test starts a real daemon child, spawns 24 `sleep 8` load sessions, applies subscriber pressure, then measures the wall-clock round-trip of one more `Spawn` request and asserts it is at most 50 ms.

That round-trip includes client socket writes and reads, tokio connection-task scheduling in the daemon child, the control channel hop, owner-loop service, the real process spawn of `sleep 0.05`, and reply flushing — across two processes. Under ambient workspace load (concurrent agent builds and tests), OS scheduling of either process inflates the round-trip past 50 ms even when the daemon's own behavior is correct and even in single-test runs (observed 108-110 ms alone). Wall-clock latency through a daemon child is a machine-speed measurement, not a daemon-behavior measurement.

The invariant the tests exist to protect is behavioral: the owner loop must serve queued control while heavy maintenance (observe slices over 24 live sessions; first-snapshot assembly) still has pending work, instead of draining maintenance first. The production mechanism is control-first `try_recv` before each single slice (`src/daemon_transport.rs:273`) plus the `biased` select. A starved owner loop would answer the ready `Spawn` only after the live-session maintenance backlog drained — in these fixtures, only after the 8-second load window collapsed.

This is a test oracle defect, not a production regression. Production budgets, observe slices, and owner-loop scheduling are correct and stay unchanged.

### Why the v1 load-window liveness oracle was also invalid

Plan Review finding `finding_1786945973_629867` is accepted. The v1 plan proposed: after the `Spawn` reply, `ListSessions` must show the `sleep 8` load rows still live, as proof that control was served while maintenance work remained. That proof does not hold. The maintenance backlog is bounded by item, byte, and elapsed slices, and it drains (or idles between reconciliation ticks) well inside the 8-second child lifetime. An ablated maintenance-first owner loop could drain its whole backlog and then serve `Spawn` with every load row still live. Load-row liveness therefore cannot witness control-before-maintenance ordering, and the v1 nine-second negative control only forced fixture expiry, not a scheduling ablation.

The general lesson: through a real daemon child, every observable delta (wall-clock, slice counters between two requests) includes client write, connection-task, and OS-scheduling transit that ambient load inflates without bound. No end-to-end observation is a deterministic ordering oracle within this ticket's constraints (no production DTO or scheduling changes). The ordering invariant must be proven at the decision points themselves:

- The busy-path decision: `control_rx.try_recv()` is consulted before `slice_due` in the owner loop (`src/daemon_transport.rs:273-284`), so a queued control message preempts the next maintenance slice.
- The blocking-path decision: `receive_owner_event` uses a `biased` select that prefers a ready control message over an expired reconciliation timer (`src/daemon_transport.rs:170-176`).

`MAX_READY_OPERATION_WAIT_MS` itself is referenced by no production code path (only `src/daemon_maintenance.rs:29` defines it and `src/lib.rs:128` exports it); it is a documented budget relation over `MAX_OWNER_TURN_MS`, const-asserted in `tests/session_projection_owner_loop.rs`. The 50 ms figure is the derived consequence of "at most one owner turn can precede a queued control", which is exactly the decision-level property tested below.

## Scope

Two layers. Layer 1 proves the control-before-maintenance ordering invariant deterministically at the production decision points. Layer 2 repairs the two flaky integration tests into functional-under-load oracles with the measured duration as an observation.

### Layer 1: decision-level ordering proof (unit tests in `src/daemon_transport.rs`)

The existing `mod tests` in `src/daemon_transport.rs` (at `:7810`) already unit-tests `receive_owner_event` directly (`due_reconciliation_precedes_an_already_ready_control_message`), so this seam is established idiom.

1. Extract the owner loop's busy-path event classification — the `match control_rx.try_recv()` arms at `src/daemon_transport.rs:273-284` — into a private pure helper the loop calls (for example `fn classify_owner_poll(poll: Result<ControlMessage, TryRecvError>, slice_due: bool) -> OwnerPollDecision` with variants `ServeControl`, `RunSlice`, `Block`). The helper preserves the exact current arm order and meaning: `Ok(message)` serves control regardless of `slice_due`; `Err` with `slice_due` runs one maintenance slice; `Err` otherwise blocks. This is a structural refactor only. Scheduling behavior is identical, and the full lifecycle suite gates that claim.
2. New unit test `queued_control_precedes_a_due_maintenance_slice`: with a queued control message and `slice_due == true`, the helper must choose `ServeControl`. This is the deterministic statement of the invariant the two integration tests were reaching for: at most one already-running owner turn can precede a queued control message, which is what makes the documented `MAX_READY_OPERATION_WAIT_MS` relation over `MAX_OWNER_TURN_MS` true.
3. New unit test `queued_control_wins_a_ready_biased_select_every_time`: call the production `receive_owner_event` with a prefilled control queue and a tiny nonzero wait chosen so both select arms are ready at first poll (for example one nanosecond; verify readiness in Implement and adjust the harness if the timer is not reliably ready). Loop 64 iterations, re-queueing the message each time; every iteration must return `OwnerEvent::Control`.

### Layer 2: integration tests become functional oracles (`tests/hub_daemon_lifecycle/sessions.rs`)

4. Keep the fixtures unchanged: the real daemon child, 24 `sleep 8` load sessions, the subscriber pressure (two entity subscribers in the observe-slice test; one unread subscriber in the snapshot-assembly test), and the ready `Spawn` of `sleep 0.05`.
5. Delete the assertion `waited <= Duration::from_millis(botster_hub::MAX_READY_OPERATION_WAIT_MS)` (`:3587` and `:3634`). Keep measuring `waited` and report it as an observation only (`eprintln!`), per [[conformance harnesses gate on deterministic invariants not timing]]. Do not assert on it.
6. Keep every existing deterministic functional assertion: the `Spawn` reply succeeds, the snapshot-assembly test's first snapshot frame carries at least 24 items, unsubscribes succeed, and the daemon shuts down cleanly.
7. Rename the tests to state the claim they now prove: `ready_spawn_completes_when_live_sessions_exceed_one_observe_slice` and `ready_spawn_completes_during_session_snapshot_assembly`. The old names claimed a wall-clock budget the tests no longer assert; keeping them would misdescribe the oracle. This plan records the old-name-to-new-name mapping for ticket traceability, and the Implement report repeats it.
8. Add a short comment in each: the control-before-maintenance ordering is proven by the decision-level unit tests in `src/daemon_transport.rs`; end-to-end wall-clock latency through a daemon child measures ambient machine load and is recorded as an observation only.
9. Do not add the v1 `ListSessions` load-window liveness assertion as an ordering proof. Plan Review finding `finding_1786945973_629867` stands: a bounded maintenance backlog can drain while every `sleep 8` row is still live.

### Negative controls (red-proofs), tied to the actual scheduling decisions

Per [[a regression test must be shown to go red with the fix reverted]], both temporary and reverted after capture:

10. Ablation A: invert the extracted helper's precedence so `slice_due` wins over a ready control message. `queued_control_precedes_a_due_maintenance_slice` must fail deterministically. Record the nonzero exit and assertion message.
11. Ablation B: remove `biased;` from the select in `receive_owner_event`. With both arms ready, an unbiased select chooses randomly, so `queued_control_wins_a_ready_biased_select_every_time` fails in practice within its 64 iterations (accidental full-pass probability 2^-64). Record the failure output.
12. The v1 nine-second fixture-expiry sabotage is dropped. It ablated the fixture, not the scheduling decision.

Quarantine fallback, ticket-authorized: only if a repaired Layer 2 test still fails the acceptance repetitions, quarantine that test with `#[ignore]` and an Implement report naming the remaining mechanism. The Layer 1 ordering tests are deterministic and are not candidates for quarantine. Do not start from quarantine.

## Non-scope

- Do not change `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, owner-loop scheduling behavior, or Pump/Maintenance fairness. The ticket forbids it. The Layer 1 extraction moves the existing busy-path decision into a named pure function with identical arms and order; it changes structure for testability, not scheduling. If Implement cannot keep the extraction exactly behavior-preserving, stop and fall back to the quarantine path instead of altering scheduling.
- Do not touch the sibling wall-clock site `tests/hub_daemon_lifecycle/package_event_plane.rs:180`. It has not flaked and the established rule is one flake ticket per test. It is recorded under Risks for the sweep inventory.
- Do not touch the three unit-level wall-clock sites in `src/daemon_entity_subscriptions.rs` already inventoried in [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]].
- Do not absorb `ticket_1786937228_425608` (unix_adapter flake; it depends on this ticket) or `ticket_1786913892_208903`. The ticket forbids both.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins. `ListSessions` already exists on the client.
- Do not create a pull request. The pipeline merge policy is direct.

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon transport, the owner loop, the lifecycle test suite, and both test bodies. The work stays in Hub, inside two test functions in `tests/hub_daemon_lifecycle/sessions.rs`.

No cross-repository prerequisite exists. Do not register a Core, client, Web, or TUI dependency.

Related tickets (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786937228_425608` | unix_adapter_unbound_printf_stream_attach_completes flake | Discovered this ticket during its Plan Review base re-verification; its acceptance stays red until this ticket merges. |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Named by the ticket as forbidden to absorb. |

## Assumptions and unknowns

Assumption: the observed failures are ambient-load scheduling of the test process and the daemon child, not owner-loop starvation. Evidence: both tests fail on clean `origin/main` `547ca38` alone and in the suite with ~2x budget latencies, and the control-first owner-loop structure (`src/daemon_transport.rs:273`) is unchanged since the tests last passed.

Assumption: the busy-path classification can be extracted into a pure function with identical behavior. The arms are a plain three-way match on `try_recv` result and `slice_due`; no state is consumed beyond the poll result. The full lifecycle suite plus the strict Rust gates verify the claim. If Implement finds hidden coupling, the fallback is quarantine, not scheduling change.

Assumption: a tiny nonzero wait (about one nanosecond) makes the reconciliation timer arm of `receive_owner_event` ready at first poll alongside the prefilled control message, so the biased-select test genuinely exercises a ready-versus-ready choice. Implement verifies this; if tokio timer granularity leaves the timer pending, adjust the harness (for example, a slightly larger elapsed-before-poll wait) until both arms are ready, and record the choice in the Implement report.

Unknown until Implement: the exact `OwnerPollDecision` naming and whether `ControlMessage` construction for the unit tests reuses `ControlMessage::RejectedConnection` as the existing unit test does. Follow the existing `mod tests` idiom.

Unknown until Implement: whether pre-change reproduction occurs on this worktree. Reproduction is load-dependent and probabilistic. The ticket already carries exact base-`547ca38` failure evidence, so pre-change reproduction is corroborating, not required.

## Affected surfaces/files

- `src/daemon_transport.rs` -- the busy-path classification extraction (structural only, identical arms) and two new unit tests in the existing `mod tests`.
- `tests/hub_daemon_lifecycle/sessions.rs` -- only the two named test bodies (renamed per Scope item 7).
- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` -- this plan.
- `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` -- Implement report (Implement step).

No production behavior changes. The only production-file diff is the pure extraction in `src/daemon_transport.rs`. No dependency, DTO, or lockfile changes.

## Risks

- Extraction risk: an accidental behavior change in the owner loop while moving the classification. Mitigation: the helper must carry the exact current arms and order, reviewed against the pre-change match; the full lifecycle suite (5 consecutive zero-failure runs) plus fmt/clippy gate the refactor; fallback is quarantine, never scheduling change.
- Coverage boundary: the decision-level unit tests prove the classification and the biased select, not the whole loop wiring. A future regression that bypasses the helper (for example, a new pre-control drain inserted directly in the loop) would not fail them. Mitigation: the extraction makes the helper the single busy-path classification site, and the loop's call site is one line a reviewer can check; end-to-end latency regressions belong to isolated observability, not a loaded functional gate.
- The integration tests no longer assert any latency bound: a real regression from 50 ms to 500 ms passes them. Accepted: wall-clock through a daemon child was never a reliable loaded-CI detector (this ticket exists because it fails on healthy code); `waited` stays visible as an observation and `tests/session_projection_owner_loop.rs` keeps the budget relations const-asserted.
- Ablation B's red-proof is probabilistic with accidental-pass probability 2^-64 per run — practically deterministic, recorded honestly in the Implement report.
- Renaming the two tests changes the identifiers the ticket and vault notes reference. Mitigation: this plan and the Implement report record the old-to-new mapping; the acceptance filter below matches the new names.
- The unix_adapter flake owned by `ticket_1786937228_425608` may appear during full-suite acceptance runs. Rule: record exact evidence for that ticket; do not absorb and do not retry it away silently.
- `tests/hub_daemon_lifecycle/package_event_plane.rs:180` keeps the same wall-clock assertion class and can flake next. Recommendation to the orchestrator: include it in a sweep ticket for remaining wall-clock assertion sites rather than paying one ticket per future flake.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency. Suite commands use the Hub wrapper `./test.sh`. Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Decision-level repetition: `./test.sh --locked --lib` filtered to the two new unit tests (exact filter resolved in Implement from the wrapper's argument pass-through; the tests live in `daemon_transport::tests`) passes 20 consecutive runs. These must be load-insensitive by construction.
3. Targeted integration repetition: `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` (matches exactly the two renamed tests) passes 20 consecutive runs (shell loop, stop on first failure).
4. Binding gate: `./test.sh --locked --test hub_daemon_lifecycle_test` passes 5 consecutive runs with zero failures. This is the command the ticket and the dependent ticket bind on. If an unrelated test fails, record exact evidence and route it to its owning ticket; do not absorb.
5. Red-proofs per Scope items 10-11, both temporary and reverted after capture: Ablation A (inverted helper precedence) must fail `queued_control_precedes_a_due_maintenance_slice`; Ablation B (`biased;` removed) must fail `queued_control_wins_a_ready_biased_select_every_time`. Record both nonzero exits and failing assertion messages in the Implement report. These ablate the actual scheduling decisions, per Plan Review `finding_1786945973_629867`.
6. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
7. Non-binding corroboration (optional): one Loaded daemon lifecycle diagnostics workflow dispatch per `docs/loaded-daemon-lifecycle-runner.md` with `test_target: lifecycle-suite` and `repetitions: 20` against the implement commit, for isolated-runner evidence of the ambient-load class.
8. Implement report at `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` records: pre-change reproduction attempts, the extraction diff and its behavior-preservation argument, the new unit oracles, both red-proof outputs, the old-to-new test-name mapping, and the acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Extend [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] or add a sibling note for the integration variant: wall-clock ready-operation bounds through a real daemon child are ambient-load-sensitive by construction, and no end-to-end observation (latency or counter deltas) can witness owner-loop ordering because transit time contaminates it; the durable idiom is decision-level unit oracles at the scheduling decision points plus functional-under-load integration coverage, with the measured duration reported as an observation. Also capture the anti-pattern this review caught: load-window liveness (fixture rows still live at reply time) does not prove control-before-maintenance ordering, because a bounded backlog drains while fixtures stay live. Inventory the remaining site `tests/hub_daemon_lifecycle/package_event_plane.rs:180` so a sweep ticket has the list.
- Capture the run-record target_id corruption shape (run `target_id` not matching any project target while the ticket target is valid) if it recurs, so orchestrators check the ticket record first.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Optionally attempt bounded pre-change reproduction with the old targeted wrapper command (corroborating only).
3. Extract the busy-path classification helper per Scope item 1; diff-review it against the pre-change match for identical arms and order.
4. Add the two decision-level unit tests per Scope items 2-3, following the existing `mod tests` idiom in `src/daemon_transport.rs`; verify the both-arms-ready harness assumption for the biased-select test.
5. Repair and rename the two integration tests per Scope items 4-9. Keep the diff inside the two test functions.
6. Run acceptance checks 2-6; capture both red-proofs (check 5) and revert the ablations.
7. Write the Implement report.
8. Commit the extraction, the tests, and the report. Do not create a PR.
