# Plan: Fix flaky unix_adapter_unbound_printf_stream_attach_completes

Ticket: `ticket_1786937228_425608`
Run: `run_1786937300_850110`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

Revision 2. Revision 2 addresses Plan Review `review_1786938887_392539`: the red base lifecycle gate now has an owner ticket (`ticket_1786938984_190098`) registered as a blocking dependency (`dependency_1786938989_522783`), the acceptance checks sequence the binding suite gate after that dependency lands, and the planning context now records [[botster-architecture]] and [[cli-patterns]].

The write-budget sibling continuation (`ticket_1786913892_208903`) hit one lifecycle-suite failure after integrating Hub main `547ca38`. The failed test was `unix_adapter_unbound_printf_stream_attach_completes`. The panic was `ProcessExited must not shut down the host session: [DaemonSession { session_id: "uap-session", lifecycle: "exited" }]` at `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:752`. The same test passed in isolation on the branch and on base `origin/main` `547ca38`. This ticket repairs that default-concurrency root on botster-hub.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`, resolved from `list_spawn_targets` for `tgt_7e208a0c76a44980a83b63af976b1f22`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22` (from the ticket record).
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786937228_425608`, base `547ca38` (clean).
- The worktree path contains no colon. `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.
- No duplicate ticket exists: `project_pipelines_search_tickets` for the test name returns only this ticket.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon transport, the session lifecycle projection, and this lifecycle test.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene.
- [[botster-architecture]] -- Botster domain map; confirms Hub owns this lifecycle surface and lists the flake-class notes below.
- [[cli-patterns]] -- historical mixed CLI/runtime index (loaded per the Botster planning overlay; its header states current ownership comes from [[botster-architecture]] and the repository playbook, which this plan follows).
- [[proposed ProcessExited closes terminal subscriptions but not the host session]] -- pending-ratification direction consistent with the test's intent message; the repair preserves that intent without depending on ratification.
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] -- the sibling flake class: default-concurrency load changes scheduler timing, so timing-coupled test oracles fail while the production behavior stays correct.
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] -- prior repair in the same class; the durable idiom asserts deterministic outcomes, not timing.
- [[conformance harnesses gate on deterministic invariants not timing]] -- the repaired oracle must gate on state, not on observation order.
- [[Hub session projection continues without subscribers or terminal Drain]] -- the projection must reach the ended row without any subscriber. An `exited` row after a fast-exit command is required behavior, not a defect.
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]] -- lifecycle progress is journal-driven and independent of terminal egress.
- [[Hub owner loop wakes only for mutations and pending resync]] -- control reads do not schedule projection maintenance; observation timing therefore varies with request mix and background pump timing.
- [[a regression test must be shown to go red with the fix reverted]] -- the repaired oracle needs negative controls.
- [[plugin worker unload deadline can flake under default-concurrency workspace load]] -- distinguishes isolated diagnosis from the default-concurrency gate.
- Runtime-teardown class: does not apply. [[botster runtime teardown lenses]] was loaded for classification only. This ticket changes one test oracle. It does not change any peer, session, ClientWorker, or SessionIo teardown path, ownership set, or resource lifecycle. Production teardown behavior is untouched. Plan Review may force the class if it disagrees with this classification.

## Context loaded

- Ticket record, run record, gates, and empty prior artifacts/checklists via `project_pipelines_current_context`.
- Prior art in this repository:
  - `cb9be95` + `cd5e7a8` plan and repair `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` -- the sibling flake plan and its merged repair idiom: replace a scheduler-sensitive oracle with deterministic assertions and prove the new oracle with negative controls.
  - `e13ce5e` -- the commit that added the failing test. Its message states the smoke proof uses ReadScreen "instead of waiting for ProcessExit". The `lifecycle == "running"` equality was an incidental over-strict oracle, not the proof target.
  - `tests/hub_daemon_lifecycle/sessions.rs:2070-2094` and `tests/hub_daemon_lifecycle/webrtc_proofs.rs:587` -- existing deterministic pattern: poll `ListSessions` until the session row reports `exited` while it stays listed.
- Code read:
  - `unix_adapter_unbound_printf_stream_attach_completes` (`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:684-762`).
  - `DaemonRequest::Drain` handler (`src/daemon_transport.rs:3446-3467`): a Drain for a known session always returns empty events; a missing session returns `missing_session_drain_error`.
  - `DaemonRequest::ReadScreen` handler (`src/daemon_transport.rs:3468-3484`): each ReadScreen calls `observe_lifecycle_turn`, which consumes lifecycle journal entries. `DaemonRequest::ListSessions` (`src/daemon_transport.rs:3094`) does not observe.
  - `session_lifecycle_class` (`src/session_projection.rs:228-247`): Core `Exited` maps to the `ended` class while the session row stays listed until `RemoveSession` or shutdown.
  - Background pump `pump_bound_unix_routes` (`src/daemon_transport.rs:4964+`) also calls `observe_lifecycle_slice`, so observation advances without client requests.
- `test.sh` -- the repo test wrapper. It checks hub-test-support asset sync, sets `BOTSTER_ENV=test`, and runs workspace scope. Its header comment blesses the targeted form `./test.sh --test hub_daemon_lifecycle_test`. The lifecycle test binary uses `include!`, so the exact test name is the bare function name.

## Blocking dependency: base lifecycle suite is red

Plan Review (`review_1786938887_392539`) ran the binding command on fetched base `origin/main` `547ca38` and got exit 101 with 217 passed, 2 failed, 1 ignored. The two failures are unrelated to this ticket's test and reproduce when run alone:

- `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (`tests/hub_daemon_lifecycle/sessions.rs:3597`, assertion at `:3634`): waited 93.444875 ms in the suite, 108.439167 ms alone.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` (`tests/hub_daemon_lifecycle/sessions.rs:3547`, assertion at `:3587`): waited 69.609041 ms in the suite, 110.668334 ms alone.

Both assert wall-clock elapsed around one Spawn request `<= MAX_READY_OPERATION_WAIT_MS` (50 ms) through a real CLI daemon child under 24 live sessions. This is the known wall-clock-under-load class ([[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]), surfacing under ambient workspace load.

Disposition:

- Owner ticket created: `ticket_1786938984_190098` ("Hub tests: fix ready_spawn wall-clock MAX_READY_OPERATION_WAIT_MS budget failures under ambient load"), botster-hub target, with the exact base evidence preserved.
- Blocking dependency registered: `dependency_1786938989_522783` (this ticket depends on `ticket_1786938984_190098`).
- The five-run binding suite gate stays at full strength. It is not waived and not weakened. It becomes executable after the dependency ticket's repair merges into Hub main and Implement integrates that base into this branch.
- Engine caveat: pipeline advancement does not hard-block on open dependencies. Implement must therefore check the dependency ticket status explicitly before running the binding suite gate, and must re-verify the integrated base is green on those two tests first.
- The targeted acceptance checks (worker prebuild, 20 exact-test runs, red-proof controls, fmt, clippy) do not touch the two red tests and stay executable before the dependency lands.
- Do not absorb the ready_spawn repairs into this ticket. One flake ticket per root, per the established project pattern.

## Failure mechanism

The test spawns `printf 'smoke:<marker>\n'`. The child prints one line and exits almost immediately. The test then polls ReadScreen until the marker is visible, and finally asserts through one `ListSessions` call that the session row still reports `lifecycle == "running"`.

The projected lifecycle of that session lawfully becomes `exited` as soon as an observation turn consumes the ProcessExited journal entry. Observation runs inside every ReadScreen request and inside the background pump. The `running` assertion therefore encodes an observation-timing race, not an invariant:

- In a fast isolated run, the first ReadScreen shows the marker before any observation turn consumes the exit entry. The loop breaks, `ListSessions` still reports `running`, and the test passes.
- Under default-concurrency suite load, scheduling delay widens the window between spawn and the marker break. An observation turn consumes the exit entry first, and `ListSessions` correctly reports `exited`. The test panics.

Both orderings are legitimate. The projection is required to reach the ended row without any subscriber ([[Hub session projection continues without subscribers or terminal Drain]]). This is a test-oracle defect, not a production regression. The property the test intends to prove -- ProcessExited must not shut down the host session -- means the session row stays listed and serviceable after exit, not that observation has not happened yet.

## Scope

Repair `unix_adapter_unbound_printf_stream_attach_completes` so the lifecycle suite at default concurrency cannot fail it through observation timing, while it proves more than before: the exit is observed, the session row is retained, and the host session stays serviceable.

Keep the spawn, attach, empty-drain, and ReadScreen marker sections (lines 684-748) unchanged. The in-loop `drain.events.is_empty()` assertion is deterministic: a Drain for a known session always returns empty events.

Replace the single `ListSessions` assertion (lines 749-759) with:

1. A bounded retention-and-exit poll (5-second deadline, matching the file convention). Each iteration calls `ListSessions` and finds the session row by `session_id`:
   - Row absent: panic immediately with the retained intent message `ProcessExited must not shut down the host session` plus the listed sessions.
   - `lifecycle == "exited"`: break the loop.
   - `lifecycle == "failed"`: panic; a successful printf exit must not classify as failed.
   - Any other value (transient `running`): call ReadScreen for the session (each call runs `observe_lifecycle_turn`, so observation advances deterministically), sleep 25 ms, and continue.
   - Deadline exhausted: panic with a message that the session must reach `exited`.
2. Post-exit serviceability proofs on the same connection, after the poll observes `exited`:
   - ReadScreen still returns text that contains `smoke:<marker>` (screen content survives process exit).
   - `drain_subscription(session_id, subscription_id)` still returns an Events response with empty events (a shut-down or removed session would return `missing_session_drain_error` instead).
3. A short comment stating the oracle: exit observed, row retained, host session serviceable; observation order is not asserted.

Keep the test name. Keep the final `hub.shutdown()`.

## Non-scope

- No production changes. Do not touch `src/daemon_transport.rs`, `src/session_projection.rs`, observation budgets, owner-loop scheduling, or lifecycle classification.
- Do not touch the other `lifecycle == "running"` assertions in the suite (`unix_terminal_adapter.rs:1368`, `sessions.rs:775`, `sessions.rs:1989`, `shutdown.rs:2473`, `shutdown.rs:2542`, `webrtc_proofs.rs:1699`, `webrtc_proofs.rs:1707`). Each guards a long-lived session (`sleep 30`, read loops, unbounded loops), so the fast-exit race does not apply to them.
- Do not absorb write-budget `ticket_1786913892_208903`, and do not retry its binding suite on that ticket. The ticket text forbids both.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins.
- Do not create a pull request.

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon transport, the session lifecycle projection, and this lifecycle test. The work stays in Hub, in one test function in `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`.

No cross-repository prerequisite exists. Do not register a Core, client, Web, or TUI dependency. The session worker prebuild uses the lockfile-pinned `botster-core-daemon` package target; no repin.

Same-target siblings (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Discovered this flake. Ticket text forbids absorption. |
| `ticket_1786921010_869253` | near-limit lib-suite flake (merged as `cd5e7a8`) | Same flake class, different suite and test. Prior-art idiom source. |
| `ticket_1786912572_610381` | Deterministic PTY process lifecycle fixtures | Out of scope; this repair does not change fixtures. |
| `ticket_1786938984_190098` | ready_spawn wall-clock budget failures on base | Registered blocking dependency (`dependency_1786938989_522783`); gates the binding suite runs. Do not absorb. |

## Assumptions and unknowns

Assumption: the observed failure is an observation-timing race, not a production regression. The lifecycle value in the panic (`exited`) is the required eventual projection state for a fast-exit command. Isolation passes on the branch and on base `547ca38` with the identical command. The failed assertion is the lifecycle string equality, and only load changes the observation order.

Assumption: the retention-and-exit poll converges within the 5-second deadline. Each poll iteration issues a ReadScreen, and each ReadScreen runs `observe_lifecycle_turn`, so the ProcessExited journal entry is consumed after a bounded number of iterations. Prior art (`sessions.rs:2070`, `webrtc_proofs.rs:587`) already relies on the same eventual-exited poll shape.

Assumption: ReadScreen and Drain stay serviceable after exit. The registry row persists until `RemoveSession`, the screen buffer is retained, and the Drain handler answers any known session with empty events.

Unknown until Implement: whether the failure reproduces on this worktree before the change. Reproduction is load-dependent and probabilistic. The Implement report should attempt a bounded number of pre-change default-concurrency suite runs and must not treat non-reproduction as proof of absence.

Known since Plan Review: the two ready_spawn wall-clock tests fail on base under ambient load. They are owned by dependency `ticket_1786938984_190098`; see the Blocking dependency section. If a further, different lifecycle-suite test flakes during the acceptance runs, register a new ticket with exact evidence. Do not expand this repair mid-run.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- only the `unix_adapter_unbound_printf_stream_attach_completes` test body.
- `docs/plans/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite.md` -- this plan.
- `docs/reports/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite-implement.md` -- Implement report (Implement step).

No production code changes. No dependency or lockfile changes.

## Risks

- Requiring eventual `exited` adds a new wait. Mitigation: the poll drives observation itself through ReadScreen on every iteration, so convergence does not depend on background pump timing; the 5-second deadline matches the file's existing waits.
- The original oracle also guarded against premature teardown while the test believed the session was running. Mitigation: the new poll asserts row presence on every iteration from marker-visibility until `exited`, so retention coverage is continuous and strictly stronger than the old single probe.
- The lifecycle suite is currently red on base through the two ready_spawn tests; the binding gate waits on dependency `ticket_1786938984_190098`. If the dependency stalls, this ticket stalls with it; escalate through the orchestrator rather than weakening the gate. A further unrelated flake during acceptance runs follows the prior-art rule: exact evidence or a new ticket; do not absorb.
- The lib suite retains three known wall-clock assertion sites (`paged_delivery_stays_within_owner_turn_for_a_large_registry`, `first_session_snapshot_is_complete_and_assembled_in_pages`, `no_removal_scan_stays_within_owner_turn` in `src/daemon_entity_subscriptions.rs`). A full `./test.sh --locked` run can flake there. Those roots belong to the sweep recommendation recorded by the near-limit plan, not to this ticket.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency. All suite commands use the Hub wrapper `./test.sh`, which checks asset sync, sets `BOTSTER_ENV=test`, and runs workspace scope. Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`. The near-limit review proved suite runs without this build produce worker-missing failures.
2. Targeted repetition: `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes` passes 20 consecutive runs (shell loop, nonzero exit stops the loop).
3. Binding default-concurrency gate: `./test.sh --locked --test hub_daemon_lifecycle_test` (full lifecycle binary, default test threads) passes 5 consecutive runs with zero failures. The pre-existing 1 ignored test stays ignored. Preconditions, per the Blocking dependency section: `ticket_1786938984_190098` is closed with its repair merged into Hub main, Implement has integrated that base into this branch, and one base re-verification shows the two ready_spawn tests green before the five binding runs start.
4. Red-proof, per [[a regression test must be shown to go red with the fix reverted]], with two separate negative controls, both run under the targeted command from check 2:
   - Control A (retention oracle): temporarily insert `ShutdownSession` plus `RemoveSession` for `uap-session` before the poll. The run must fail at the retention panic (`ProcessExited must not shut down the host session`). This proves the row-presence assertion is live.
   - Control B (eventual-exit oracle): temporarily change the spawn command to `printf 'smoke:<marker>\n'; sleep 30`. The marker still prints, the process stays alive, and the run must fail at the deadline panic (session must reach `exited`). This proves the exit-observation assertion is live and gives a first-failure site distinct from Control A.
   Record both nonzero exit codes and both failure sites in the Implement report, then revert both sabotages.
5. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
6. Non-binding smoke: one full `./test.sh --locked` run (the ticket's discovery command) may be reported for information. A failure in a known lib-suite wall-clock root does not bind this ticket; any other failure needs exact evidence and a new ticket.
7. Implement report at `docs/reports/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite-implement.md` records: pre-change reproduction attempts, the repaired oracle, red-proof output, and the acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Add a sibling gotcha note to [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]: asserting `lifecycle == "running"` after a fast-exit spawn is an observation-timing oracle, because the projection lawfully reaches `exited` without any subscriber. The durable idiom is a retention-and-exit poll plus post-exit serviceability proofs.
- Capture the mechanism asymmetry: `ReadScreen` runs `observe_lifecycle_turn` while `ListSessions` does not, so a `ListSessions`-only poll depends on background pump timing while a ReadScreen-driven poll observes deterministically.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Optionally attempt bounded pre-change reproduction with the check-3 suite command (corroborating only).
3. Edit the test body per Scope items 1-3. Keep the diff inside the one test function.
4. Run acceptance checks 2, 4, and 5 immediately. Run check 3 (the binding suite gate) only after the Blocking dependency preconditions hold, and check 6 after check 3.
5. Write the Implement report with red-proof output and run tallies.
6. Commit the test repair and report. Do not create a PR.
