# Plan: Fix flaky unix_adapter_unbound_printf_stream_attach_completes

Ticket: `ticket_1786937228_425608`
Run: `run_1786937300_850110`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

Revision 5. Revision 2 addressed Plan Review `review_1786938887_392539`: the red base lifecycle gate received an owner ticket (`ticket_1786938984_190098`) registered as a blocking dependency (`dependency_1786938989_522783`), the acceptance checks sequenced the binding suite gate after that dependency landed, and the planning context added [[botster-architecture]] and [[cli-patterns]]. Revision 3 applies the orchestrator suite-concurrency policy (`msg_device-2_1787003453_936ef9`, 2026-08-17): full lifecycle-suite runs are a serialized command class that needs an explicit orchestrator slot, the ready_spawn failures are known-baseline failures recorded on their owning ticket rather than a serial dependency, and strict zero-failure convergence moves to final integration. Revision 4 applies Implement question `question_1787005265_458413` path D: the exit oracle is the attached terminal subscription's `process_exit` frame, not `ListSessions.lifecycle`. Revision 5 applies Review `review_1787012453_488679` / `finding_1787012453_619797`: keep the named test as the default-Hello unbound fast-exit path, accept retained lifecycle `running` or `exited`, and move bound `process_exit` proof to a separate test that holds the child on a release file until Attach completes.

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

## Known-baseline failures and suite serialization

Plan Review (`review_1786938887_392539`) ran the binding command on fetched base `origin/main` `547ca38` and got exit 101 with 217 passed, 2 failed, 1 ignored. The two failures are unrelated to this ticket's test and reproduce when run alone:

- `ready_spawn_stays_within_budget_during_session_snapshot_assembly` (`tests/hub_daemon_lifecycle/sessions.rs:3597`, assertion at `:3634`): waited 93.444875 ms in the suite, 108.439167 ms alone.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` (`tests/hub_daemon_lifecycle/sessions.rs:3547`, assertion at `:3587`): waited 69.609041 ms in the suite, 110.668334 ms alone.

Both assert wall-clock elapsed around one Spawn request `<= MAX_READY_OPERATION_WAIT_MS` (50 ms) through a real CLI daemon child under 24 live sessions. This is the known wall-clock-under-load class ([[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]), surfacing under ambient workspace load.

Disposition (Revision 3, per orchestrator policy `msg_device-2_1787003453_936ef9`):

- Owner ticket: `ticket_1786938984_190098` ("Hub tests: fix ready_spawn wall-clock MAX_READY_OPERATION_WAIT_MS budget failures under ambient load"), botster-hub target, with the exact base evidence preserved. The two failures are **known-baseline failures** recorded on that owning ticket.
- The Revision 2 blocking dependency (`dependency_1786938989_522783`) is retired: the orchestrator policy forbids serial ticket dependencies created from suite contamination. The Plan agent's removal call was permission-blocked, so the orchestrator was notified to remove it; the dependency carries no gate meaning in this plan from Revision 3 on.
- Suite serialization policy: `./test.sh --locked --test hub_daemon_lifecycle_test` without a test filter is a serialized command class. Implement must obtain an explicit orchestrator slot before starting one, must never run one concurrently with another full suite, and must notify the orchestrator if a full suite is already running instead of starting a second.
- Focused and filtered commands (the exact-test repetition, red-proof controls, fmt, clippy) may run concurrently without a slot.
- Binding gate accounting: within an orchestrator slot, the suite runs bind on zero failures **excluding** the known-baseline ready_spawn pair. If either ready_spawn test fails during a run, record the occurrence on `ticket_1786938984_190098` and do not count it against this ticket. Any other failure needs exact evidence and a new owner ticket.
- Strict zero-failure convergence (no exclusions) remains the final-integration gate at the project level, after the ready_spawn owner ticket repairs merge.
- Do not absorb the ready_spawn repairs into this ticket. One flake ticket per root, per the established project pattern.

## Failure mechanism

The test spawned `printf 'smoke:<marker>\n'`, polled ReadScreen until the marker was visible, and then asserted through one `ListSessions` call that the session row still reported `lifecycle == "running"`.

`ListSessions.lifecycle` is a separate host projection. Observation can consume ProcessExited into `exited` before or after the marker loop. Both `running` and `exited` are lawful. The `running` equality was an observation-timing race, not an invariant.

Implement also proved that a ReadScreen-driven wait for `lifecycle == "exited"` does not converge after marker ReadScreen parks ProcessExited. A printf-only child exits before attach, and the bound adapter then receives `attach_state` plus `terminal_output` without a later `process_exit` frame.

## Scope

Repair `unix_adapter_unbound_printf_stream_attach_completes` so default-concurrency load cannot fail it through host-projection timing. Keep that named test on the default-Hello unbound fast-exit path. Do not convert it into a bound unix-adapter attach.

Revision 5 sequence for the named unbound test, per `review_1787012453_488679`:

1. Spawn a child that waits on a release file, then prints the marker, on the default Hello connection. A printf-only child can exit before ReadScreen has a producer.
2. Attach on that same default Hello connection. Host Attach and Drain stay empty of terminal bodies. Write the release file after Attach returns.
3. Call `ListSessions`. The same `session_id` must remain present. Accept lifecycle `running` or `exited`. Panic immediately if the row is absent (`ProcessExited must not shut down the host session`) or `failed`.
4. Run the existing ReadScreen marker proof on the default host connection.
5. Prove the retained host session stays serviceable with host Drain on that same connection: Events, empty events.

Keep the test name. Keep the final `hub.shutdown()`. Do not add production exact-session observation to ReadScreen. Do not call ShutdownSession as an observation stimulus.

Add a separate bound test `unix_adapter_bound_printf_stream_attach_delivers_process_exit` for path D `process_exit` proof:

1. Spawn a child that waits on a release file, then prints the marker, then sleeps 1 second so Core can emit `process_exit` after attach. The sleep is not an attach deadline.
2. Arm `SessionCleanupGuard` immediately after Spawn.
3. Attach through a unix adapter connection. Prime one host Drain while the child is still held.
4. Write the release file.
5. Poll opaque unix envelopes until one payload has `type == "process_exit"` for that session and subscription, or fail at a 5-second deadline.
6. Assert host-row retention (`running` or `exited`), ReadScreen marker, and owning-connection Drain serviceability.
7. Disarm the cleanup guard. Shut down the isolated hub.

## Non-scope

- No production changes. Do not touch `src/daemon_transport.rs`, `src/session_projection.rs`, observation budgets, owner-loop scheduling, or lifecycle classification.
- Do not touch the other `lifecycle == "running"` assertions in the suite (`unix_terminal_adapter.rs:1368`, `sessions.rs:775`, `sessions.rs:1989`, `shutdown.rs:2473`, `shutdown.rs:2542`, `webrtc_proofs.rs:1699`, `webrtc_proofs.rs:1707`). Each guards a long-lived session (`sleep 30`, read loops, unbounded loops), so the fast-exit race does not apply to them.
- Do not absorb write-budget `ticket_1786913892_208903`, and do not retry its binding suite on that ticket. The ticket text forbids both.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins.
- Do not create a pull request.

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon transport, the session lifecycle projection, and this lifecycle test. The work stays in Hub, in `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`.

No cross-repository prerequisite exists. Do not register a Core, client, Web, or TUI dependency. The session worker prebuild uses the lockfile-pinned `botster-core-daemon` package target; no repin.

Same-target siblings (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Discovered this flake. Ticket text forbids absorption. |
| `ticket_1786921010_869253` | near-limit lib-suite flake (merged as `cd5e7a8`) | Same flake class, different suite and test. Prior-art idiom source. |
| `ticket_1786912572_610381` | Deterministic PTY process lifecycle fixtures | Out of scope; this repair does not change fixtures. |
| `ticket_1786938984_190098` | ready_spawn wall-clock budget failures on base | Owns the known-baseline failures excluded from this ticket's binding suite accounting; the Revision 2 dependency is retired per orchestrator policy. Do not absorb. |

## Assumptions and unknowns

Assumption: the observed failure is an observation-timing race on `ListSessions.lifecycle`, not a production regression. Isolation passed on the branch and on base `547ca38` with the original printf-only command. The failed assertion was the lifecycle string equality.

Assumption: `process_exit` on the bound unix subscription is the durable exit oracle. Implement focused runs showed a printf-only child never delivered that frame, and `printf ...; sleep 1` did. Host Drain for the same subscription must use the owning unix adapter connection; a second default-hello Drain returns `snapshot_stream_forbidden`.

Assumption: after `process_exit`, the host row stays listed and ReadScreen still shows the marker. Lifecycle may be `running` or `exited`.

Unknown until Implement: whether the failure reproduces on this worktree before the change. Reproduction is load-dependent and probabilistic. The Implement report should attempt a bounded number of pre-change default-concurrency suite runs and must not treat non-reproduction as proof of absence.

Known since Plan Review: the two ready_spawn wall-clock tests fail on base under ambient load. They are known-baseline failures owned by `ticket_1786938984_190098`; see the Known-baseline failures section. If a further, different lifecycle-suite test flakes during the acceptance runs, register a new ticket with exact evidence. Do not expand this repair mid-run.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- restore `unix_adapter_unbound_printf_stream_attach_completes` and add `unix_adapter_bound_printf_stream_attach_delivers_process_exit`.
- `docs/plans/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite.md` -- this plan.
- `docs/reports/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite-implement.md` -- Implement report (Implement step).

No production code changes. No dependency or lockfile changes.

## Risks

- A printf-only spawn exits before attach, so the `process_exit` oracle never arms. Mitigation: keep `sleep 1` after the marker printf so the unix adapter attaches first.
- Host Drain from a second connection is forbidden once the unix adapter owns the subscription. Mitigation: run the serviceability Drain on the owning unix adapter connection.
- The lifecycle suite is red on base through the two known-baseline ready_spawn tests. The binding gate excludes exactly that owned pair and nothing else; the exclusion ends when the ready_spawn repairs merge, and final integration stays strict zero-failure. A further unrelated flake during acceptance runs follows the prior-art rule: exact evidence or a new ticket; do not absorb.
- Full-suite runs depend on orchestrator slot scheduling; if no slot is available, Implement reports the wait instead of running unslotted suites or weakening the gate.
- The lib suite retains three known wall-clock assertion sites (`paged_delivery_stays_within_owner_turn_for_a_large_registry`, `first_session_snapshot_is_complete_and_assembled_in_pages`, `no_removal_scan_stays_within_owner_turn` in `src/daemon_entity_subscriptions.rs`). A full `./test.sh --locked` run can flake there. Those roots belong to the sweep recommendation recorded by the near-limit plan, not to this ticket.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency. All suite commands use the Hub wrapper `./test.sh`, which checks asset sync, sets `BOTSTER_ENV=test`, and runs workspace scope. Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`. The near-limit review proved suite runs without this build produce worker-missing failures.
2. Targeted repetition: `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes unix_adapter_bound_printf_stream_attach_delivers_process_exit` passes 20 consecutive runs (shell loop, nonzero exit stops the loop).
3. Binding default-concurrency gate: `./test.sh --locked --test hub_daemon_lifecycle_test` (full lifecycle binary, default test threads) passes 5 consecutive runs with zero failures excluding the known-baseline ready_spawn pair owned by `ticket_1786938984_190098`. The pre-existing 1 ignored test stays ignored. Preconditions, per the known-baseline section: an explicit orchestrator slot for each full-suite run set, runs strictly serialized, and any ready_spawn occurrence recorded on the owning ticket. Both ticket tests must pass in all 5 runs. If the ready_spawn repairs have already merged by Implement time, integrate that base and bind on strict zero failures instead.
4. Red-proof, per [[a regression test must be shown to go red with the fix reverted]], with two separate negative controls:
   - Control A (unbound retention): in `unix_adapter_unbound_printf_stream_attach_completes`, temporarily insert `ShutdownSession` plus `RemoveSession` for `uap-session` before the `ListSessions` presence check. The run must fail at the retention panic (`ProcessExited must not shut down the host session`).
   - Control B (bound `process_exit`): in `unix_adapter_bound_printf_stream_attach_delivers_process_exit`, skip writing the release file. The run must fail at the `process_exit` deadline. This first-failure site must be distinct from Control A.
   Record both nonzero exit codes and both failure sites in the Implement report, then revert both sabotages.
5. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
6. Non-binding smoke: one full `./test.sh --locked` run (the ticket's discovery command) may be reported for information. It contains the full lifecycle suite, so it needs its own orchestrator slot. A failure in a known lib-suite wall-clock root or the known-baseline ready_spawn pair does not bind this ticket; any other failure needs exact evidence and a new ticket.
7. Implement report at `docs/reports/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite-implement.md` records: pre-change reproduction attempts, the repaired oracle, red-proof output, and the acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Add a sibling gotcha note to [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]: asserting `lifecycle == "running"` after process exit is an observation-timing oracle. The durable idiom is `process_exit` on the attached terminal subscription, then host-row retention that accepts `running` or `exited`.
- Capture that marker `ReadScreen` parks ProcessExited, so a later wait for `ListSessions.lifecycle == "exited"` does not converge. `ReadScreen` is not an exact-session observe.
- Capture that a printf-only child can exit before attach, after which the bound adapter may never deliver `process_exit`.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Optionally attempt bounded pre-change reproduction with the check-3 suite command (corroborating only).
3. Edit the test body per Revision 4 Scope items 1-6. Keep production code unchanged.
4. Run acceptance checks 2, 4, and 5 immediately (focused commands, no slot needed). Request a new exclusive lifecycle-suite slot only after those focused gates pass. Then run check 3 inside that slot. Run check 6 after check 3 in a slot of its own.
5. Write the Implement report with red-proof output and run tallies.
6. Commit the test repair and report. Do not create a PR.
