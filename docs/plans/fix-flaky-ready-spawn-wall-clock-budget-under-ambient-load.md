# Plan: Fix flaky ready_spawn wall-clock MAX_READY_OPERATION_WAIT_MS budget tests

Ticket: `ticket_1786938984_190098`
Run: `run_1786939902_746312`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

Two lifecycle-suite tests assert wall-clock elapsed around one `Spawn` request `<= MAX_READY_OPERATION_WAIT_MS` (50 ms) through a real CLI daemon child:

- `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (`tests/hub_daemon_lifecycle/sessions.rs:3597`, assertion at `:3634`). Observed: 93.4 ms in the suite run; 108.4 ms alone.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` (`tests/hub_daemon_lifecycle/sessions.rs:3547`, assertion at `:3587`). Observed: 69.6 ms in the suite run; 110.7 ms alone.

Both failures reproduce on clean base `origin/main` `547ca38`, in the suite and in single-test runs, under ambient workspace load. This is the same failure class as the merged separators and near-limit lib-suite repairs: wall-clock duration is not a valid oracle under load. This plan repairs the test oracle. Production budgets and scheduling stay unchanged.

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`, confirmed from the ticket worktree `origin` remote and from the prior botster-hub plan that used the same target).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22` (from the ticket record; it is a registered project target).
- Discrepancy surfaced to the orchestrator: the run record carries `target_id` `tgt_7e208a0c76a449f4ac0c99953a799869`, which is not any registered project target. It looks like a corrupted merge of the ticket target prefix and the `tgt_40abcf71ccf049f4ac0c99953a799869` suffix. The ticket target is authoritative for this plan.
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786938984_190098`, base `547ca38` (clean).
- The worktree path contains no colon. `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon transport, the owner loop, and this lifecycle test suite.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene.
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

## Scope

Repair both test oracles in `tests/hub_daemon_lifecycle/sessions.rs` so ambient load cannot fail them, while they keep proving that a ready `Spawn` is served while maintenance load is still live.

Required test behavior, applied to both tests:

1. Keep the fixtures unchanged: the real daemon child, 24 `sleep 8` load sessions, the subscriber pressure (two entity subscribers in the observe-slice test; one unread subscriber in the snapshot-assembly test), and the ready `Spawn` of `sleep 0.05`.
2. Delete the assertion `waited <= Duration::from_millis(botster_hub::MAX_READY_OPERATION_WAIT_MS)` (`:3587` and `:3634`).
3. Keep measuring `waited` and report it as an observation only (`eprintln!`), per [[conformance harnesses gate on deterministic invariants not timing]]. Do not assert on it.
4. Add the deterministic readiness proof in its place. Immediately after the `Spawn` reply, issue `DaemonRequest::ListSessions` on the same endpoint and assert both:
   - At least 24 load-session rows (`load-session-*` / `assemble-session-*`) report a live, not-ended lifecycle. The load window is 8 seconds of `sleep 8`; if the owner loop had starved queued control behind the live-session maintenance backlog, the reply could only arrive after that window drained. Live load rows at reply time prove control was served while maintenance work remained.
   - The ready session (`load-ready-spawn` / `assemble-ready-spawn`) is present in the reply. Presence, not liveness: `sleep 0.05` may already have ended.
5. Keep every existing deterministic assertion, including the snapshot-assembly test's completeness check that the first snapshot frame carries at least 24 items.
6. Keep both test names. The ticket, the vault note, and this plan reference them, and the bounded-behavior proof is still the "stays within budget" guarantee stated behaviorally.
7. Add a short comment in the `cd5e7a8` idiom: the test proves the owner loop serves queued control while load maintenance is live; wall-clock latency through a daemon child is recorded as an observation because it measures machine speed under ambient load.

Implementation detail to resolve during Implement: the exact `DaemonSession.lifecycle` strings that distinguish live from ended rows. Take them from the DTO and existing suite assertions; do not invent new classification.

Prefer this repair over quarantine. Use `#[ignore]` only if a repaired test still fails the acceptance repetitions, and then only with an Implement report naming the remaining mechanism. Do not start from quarantine.

## Non-scope

- Do not change `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, owner-loop scheduling, or Pump/Maintenance fairness. The ticket forbids it.
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

Assumption: 24 `sleep 8` load sessions remain live across the two control round-trips (`Spawn` reply, then `ListSessions` reply). Both round-trips complete in well under a second even under heavy load. If more than the whole 8-second window elapses, the liveness assertion fails — and a multi-second control stall is a genuine failure signal, not a flake.

Assumption: `ListSessions` reflects the ready session immediately after its `Spawn` reply, because `Spawn` is served by the same owner that answers `ListSessions`, and request ordering on one endpoint is serial.

Unknown until Implement: the exact live vs ended `DaemonSession.lifecycle` string values. The implementer verifies them from the DTO and existing tests before writing the classification.

Unknown until Implement: whether pre-change reproduction occurs on this worktree. Reproduction is load-dependent and probabilistic. The ticket already carries exact base-`547ca38` failure evidence, so pre-change reproduction is corroborating, not required.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/sessions.rs` -- only the two named test bodies.
- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` -- this plan.
- `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` -- Implement report (Implement step).

No production code changes. No dependency or lockfile changes.

## Risks

- The liveness-window oracle is coarser than 50 ms: a real latency regression from 50 ms to, say, 500 ms would pass. Mitigation: wall-clock through a daemon child was never a reliable CI detector of that regression; `waited` stays visible as an observation, and `tests/session_projection_owner_loop.rs` keeps the published budget relations const-asserted. Latency regression detection belongs to isolated observability, not a loaded functional gate.
- The unix_adapter flake owned by `ticket_1786937228_425608` may appear during full-suite acceptance runs. Rule: record exact evidence for that ticket; do not absorb and do not retry it away silently.
- `tests/hub_daemon_lifecycle/package_event_plane.rs:180` keeps the same wall-clock assertion class and can flake next. Recommendation to the orchestrator: include it in a sweep ticket for remaining wall-clock assertion sites rather than paying one ticket per future flake.
- The two extra `ListSessions` round-trips lengthen each test slightly. They are single control requests; the cost is negligible against the existing 24-spawn setup.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency. Suite commands use the Hub wrapper `./test.sh`. Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Targeted repetition: `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_stays_within_budget` (matches exactly the two repaired tests) passes 20 consecutive runs (shell loop, stop on first failure).
3. Binding gate: `./test.sh --locked --test hub_daemon_lifecycle_test` passes 5 consecutive runs with zero failures. This is the command the ticket and the dependent ticket bind on. If an unrelated test fails, record exact evidence and route it to its owning ticket; do not absorb.
4. Red-proof, per [[a regression test must be shown to go red with the fix reverted]], one negative control per test, both temporary and reverted after capture:
   - In each repaired test, delay the ready `Spawn` until after the load window has drained (for example `thread::sleep(Duration::from_secs(9))` before the ready `Spawn`). The load rows then report ended, and the run must fail at the new liveness assertion. Record both nonzero exit codes and both failing assertion messages in the Implement report, then revert both sabotages.
   - This control proves the liveness oracle is live: it fails exactly when the ready operation is only served after the load window, which is the observable shape of owner-loop starvation.
5. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
6. Non-binding corroboration (optional): one Loaded daemon lifecycle diagnostics workflow dispatch per `docs/loaded-daemon-lifecycle-runner.md` with `test_target: lifecycle-suite` and `repetitions: 20` against the implement commit, for isolated-runner evidence of the ambient-load class.
7. Implement report at `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` records: pre-change reproduction attempts, the repaired oracles, the verified lifecycle string classification, red-proof output, and the acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Extend [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] or add a sibling note for the integration variant: wall-clock ready-operation bounds through a real daemon child are ambient-load-sensitive by construction; the durable idiom is functional completion plus a load-window liveness proof (`ListSessions` while `sleep`-backed load sessions remain live), with the measured duration reported as an observation. Inventory the remaining site `tests/hub_daemon_lifecycle/package_event_plane.rs:180` so a sweep ticket has the list.
- Capture the run-record target_id corruption shape (run `target_id` not matching any project target while the ticket target is valid) if it recurs, so orchestrators check the ticket record first.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Optionally attempt bounded pre-change reproduction with the targeted wrapper command (corroborating only).
3. Verify the live vs ended `DaemonSession.lifecycle` values from the DTO and existing suite assertions.
4. Edit the two test bodies per Scope items 1-7. Keep the diff inside the two test functions.
5. Run acceptance checks 2-5; capture the red-proof (check 4) and revert the sabotages.
6. Write the Implement report.
7. Commit the test repair and the report. Do not create a PR.
