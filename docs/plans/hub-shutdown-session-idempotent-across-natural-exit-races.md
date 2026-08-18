# Plan: Hub makes ShutdownSession idempotent across natural-exit races

Ticket: `ticket_1786977409_499180`
Run: `run_1787012955_256937` (supersedes the oracle-repair run `run_1786977413_341616` after the ticket consolidated to the strict clean contract)
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

The user chose the strict clean contract after independent Cursor and Fable audits. This plan replaces the prior oracle-repair plan (`docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md`). The prior plan treated the blind-call typed `OperatorError` as legal host behavior. The consolidated ticket makes that behavior a product defect: after a finite session process exits, `ShutdownSession` must not return `OperatorError` only because `ProcessExited` is still in flight, on both the WebRTC and Unix transports. `ticket_1787004132_469467` is closed as an explicit duplicate; its Unix proof is required here.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the ticket record through `list_spawn_targets`. The run record carries the same target_id.
- Worktree: the pipeline ticket worktree, branch `project-pipelines/ticket_1786977409_499180`. The branch carries the prior run's commits through `a24ac2e` plus a provenance commit (`b49dcfb`) that preserves the prior run's uncommitted Verify-round isolation work verbatim.
- The worktree path contains no colon. No `CARGO_TARGET_DIR` override is required.
- Tracked `.gitignore` is present and non-empty (5 lines). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon control plane and `ShutdownSession` classification. Charter gate in scope: "For `ShutdownSession`, prove exact-session `Found`, `Absent`, and `Err` behavior. Reject `Drain`, baseline, or capped-page classification."

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay: completion evidence, worktree hygiene, runtime-teardown class trigger.
- [[botster-architecture]] and [[cli-patterns]] -- Must Load context.
- [[botster runtime teardown lenses]] -- the class applies; answers below.
- [[host ShutdownSession classification must call the exact-session Core query]] -- the shipped classify convention this contract extends.
- [[observed-exit waits must issue a production exact-session observe turn]] -- `ListSessions` cannot advance exit state; observe turns can.
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]] -- the prior run applied this note. The consolidated ticket changes the codified host contract itself, so both oracles in the file now move to the strict contract together. Vault gap below.
- [[flake oracles over typed response frames must print the full typed error body]] -- diagnosis requirement carried into every changed assert.
- [[hub shutdown preserves durable session workers]] -- Hub-process shutdown evidence stays separate from session cleanup evidence.
- [[conformance harnesses gate on deterministic invariants not timing]] -- the required proofs use deterministic forced windows, not suite-load luck.
- [[a regression test must be shown to go red with the fix reverted]] -- red-on-revert is a ticket requirement.
- [[botster-core-playbook]] boundary rule only (reusable policy-free mechanisms belong to Core) -- consulted for the cross-repo decision gate; this run stays routed to botster-hub.
- Memory note: botster-hub consumes Core by git branch; merged Core main is the consumable artifact. This governs the Rule B repin below.

## Context loaded

- Ticket, run, gates, prior checklist (`checklist_1786978235_747825`) via `project_pipelines_current_context`. No open questions. No artifacts yet on this run.
- Prior-run lineage read in the worktree: the approved oracle-repair plan, the Implement report (including Verify return `review_1786989262_776285` with the 2/28 pair-run stall evidence, `last=Some("running")` for 10 s with the producer worker already dead), and the inherited diffs now preserved at `b49dcfb`.
- Hub code read: `ShutdownSession` arm (`src/daemon_transport.rs:3401-3445`), `classify_shutdown_session` (`:4676-4694`), `classify_found_session_lifecycle` (`:4696-4729`), `recover_after_core_shutdown_error` (`:4485-4494`, propagates classify `Err` with `?`), `shutdown_error_response` (`:4652-4674`, `Active` + real error returns the raw typed error), `shutdown_error_is_already_gone` (`:4641`), `shutdown_lookup_error` (`:4731`), worker path config (`src/config.rs:535`, `src/runtime.rs:4490`).
- Core code read at the locked pin `fc541a5` (`~/.cargo/git/checkouts/botster-core-ea2698e4cbd07384/fc541a5`):
  - `CoreDaemon::shutdown_session` (`crates/botster-core-daemon/src/daemon.rs:1614-1690`): engine shutdown error is saved, a 2-second drain loop follows, a non-`session_not_found` drain error returns immediately, and deadline expiry returns the saved error or `ShutdownFailed`. When the loop observes exit, the call returns `Ok` even after an engine shutdown error.
  - `CoreDaemon::observe_session_lifecycle` (`daemon.rs:800-825`): the observe drain runs before the registry read, so a drain failure returns `Err` without reading recorded registry truth.
  - `WorkerProcessRuntime::drain_output` (`crates/botster-core/src/runtime/worker_process.rs:1287-1346`): the worker-reported `ProcessExited` payload is surfaced only when the reader finished AND the worker child's `try_wait()` reports a successful exit (`status.success()`); an adopted session (`child: None`) passes unconditionally. A live-but-exiting worker yields `try_wait() == None` (payload delayed); a non-success worker exit suppresses the payload permanently.
  - `pump_session_output` (`worker_process.rs:841-894`): channel-based; it does not error on worker socket death.
  - `ManagedSessionRuntime::shutdown_session` (`crates/botster-core/src/engine/managed_session_runtime.rs:929-967`): a failed shutdown-input flush rolls the lifecycle back to the previous state and returns the error, erasing `Stopping` evidence.
- Test code read: `external_hub_webrtc_live_output_preserves_exact_bytes` (current observed-exit-wait shape, `tests/hub_daemon_lifecycle/webrtc_proofs.rs:404-481`), `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (blind branch admits typed `OperatorError`, `:629-680`), `unix_shutdown_session_from_another_connection_classifies_attached_exit` (`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1681-1776`, already blind-call strict shape: `assert_ne!(shutdown.kind, OperatorError)`), `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` (true-error and sibling-survival pin, `tests/hub_daemon_lifecycle/sessions.rs`), isolated hub worker-binary override (`tests/hub_daemon_lifecycle/session_fixtures.rs:362-380`, `.session_worker_bin(...)`).

## Failure mechanism (code-grounded prediction, validated in Phase 1)

The natural-exit race decomposes into sub-cases at the locked Core pin:

1. Exit already recorded (registry `Exited`) -> classify returns `Cleanup` -> works today.
2. Payload drainable (reader finished, worker reaped with success) -> classify's observe drains it -> `Cleanup` -> works today.
3. Payload present, worker not yet reaped, window shorter than 2 s -> classify says `Active`, Core's shutdown drain loop observes exit inside the deadline and returns `Ok` -> `Events` -> works today. This is why isolated runs stay green.
4. Payload present, worker reap exceeds Core's 2-second deadline (suite load) -> Core returns the saved flush error or `ShutdownFailed`; Hub's recover re-classify runs microseconds later, the payload is still gated, classification stays `Active`, and `shutdown_error_response` returns the transient `OperatorError`. This is the recorded flake class.
5. Worker exits non-success after the session child exited (including a signal kill of an exiting worker) -> `status.success()` is false, the payload is suppressed permanently, the registry stays `running` forever, and every blind `ShutdownSession` returns `OperatorError` after the 2-second loop. This matches the Verify pair-run evidence (`last=Some("running")` for 10 s with the producer worker already dead).
6. Recover-path classify `Err` (drain injection, registry I/O) is propagated by `?` (`src/daemon_transport.rs:4492`) even when the registry already records `Exited` -- recorded truth is never consulted because `observe_session_lifecycle` drains before the registry read.

The distinguishing evidence between "natural exit in flight" and "true failure" is the worker-written `ProcessExited` payload held in Core's completion state. In the true-failure construction (worker SIGKILLed while the session child lives), no payload exists. No current Core query exposes payload presence to Hub, and Hub must not add a wall-clock or retry mechanism. Sub-cases 4 and 5 therefore predict a Core semantics change (Rule B below). Sub-case 6 is Hub-owned regardless.

## Runtime-teardown lens answers

`teardown_class_applies`: yes. `ShutdownSession` raced against natural worker/session exit observation; sub-case 5 is a live terminal-state vs live-runtime divergence (dead worker, registry `running` forever).

`teardown_isolation`: `ShutdownSession` targets exactly one `session_id` through the exact-session Core query; the reconciliation keys on exact-session evidence only. Each test uses an isolated hub with a private data directory, endpoint, and worker set. One session's reconciliation cannot classify through another session's state.

`teardown_bounds`: no new production wall-clock, retry count, or timing mechanism -- the ticket forbids them as correctness mechanisms. Core's 2-second shutdown deadline, the worker 500 ms grace, and Hub's bounded observe turn stay unchanged unless Phase 1 evidence proves a budget is the defect. Hub's recover path stays one bounded re-observe plus, new, one non-draining recorded-truth read; no unbounded `block_on`. The Rule B Core dependency contract explicitly forbids replacing the `try_wait` gate with a blocking `child.wait()` on the drain path.

`late_message_matrix`: this ticket adds no ownership-creating message. `ShutdownSession` destroys ownership and creates none. The complete matrix (Spawn, IssueLocalWebrtcBootstrap+Signal, encrypted Hello, Attach, Subscribe/UnsubscribeEntities, ShutdownSession) with owner tags, production-handler rejection tests, and race sweeps is recorded in the prior approved plan (`docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md`, "Runtime-teardown lens answers") and carries forward unchanged; no row's behavior changes here. The binding stale-peer lib filter set stays an acceptance check (check 7).

`production_path_proof`: the live path is Unix or WebRTC control frame -> `ShutdownSession` arm (`src/daemon_transport.rs:3401`) -> `classify_shutdown_session` -> Core `shutdown_session` -> `recover_after_core_shutdown_error`. The forced-window tests spawn a real session through a controlled worker wrapper registered via the production `core_engine.session_worker_path` config, exit it naturally, and drive blind `ShutdownSession` through the production handler on both transports. Red-on-revert: with the reconciliation removed, the forced-window test must fail with the transient `OperatorError` (acceptance check 6).

`ownership_identity`: sessions stay keyed by exact `session_id`. The Absent probe uses a never-spawned id. The wrapper changes the worker process identity only, never the session identity. Each isolated hub uses unique session ids; no reused-id hazard.

`sibling_fail_closed_policy`: on cleanup/success, the hub, the calling connection, and sibling sessions stay fully serviceable (pinned by the idempotency test's sequential rounds and the adapter-isolation tests). On true failure (`Active` + real error), the existing production policy is unchanged: victim-session adapters close, the typed error returns, the connection and siblings survive -- pinned live by `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`, which stays green under the strict contract because its construction (worker SIGKILL before any `ProcessExited` payload, plus drain injection) is a true failure, not a natural exit. Blast radius stays one request's reply plus victim-session adapters.

## Scope

### Phase 0 -- inherited-state disposition

Keep from the prior run: the full-error-body assert diagnostics, the Absent-leg `unknown_session` probe, the leftover-worker reap helpers, the IsolatedHub injection-env stripping, the env-gated drain-injection hook in `src/runtime.rs`, and the true-error sibling-survival test. Replace: the 10-second observed-exit wait and `ReadScreen` pump in the exact-bytes test (Phase 3 restores the blind call). Tighten: the idempotency sibling's blind branch (Phase 3).

### Phase 1 -- deterministic diagnosis (ticket-required)

1. Build a controlled worker wrapper fixture: an executable script registered through the isolated hub's `.session_worker_bin(...)` (production `core_engine.session_worker_path` surface) that runs the real `botster-session-worker` as a child, waits for it, then:
   - Variant W1: sleeps an env-configured window (longer than Core's 2-second deadline) before exiting with the worker's status. This forces sub-case 4 deterministically: payload present, wrapper unreaped past the deadline.
   - Variant W2: exits non-zero after the worker succeeded. This forces sub-case 5 deterministically: payload suppressed by the `status.success()` gate.
2. On both transports (WebRTC exact-bytes shape and the Unix another-connection shape), run a natural finite exit under W1 and W2, call blind `ShutdownSession`, and capture `error.code`, `error.operation`, and `error.message` from every failing path, plus the recover-path classification. Record verbatim output in the Implement report.
3. Validate the mechanism citations above against the captures. If a capture contradicts a cited sub-case, stop and re-diagnose before any production edit.

### Phase 2 -- decision gate (ticket-required: "decide whether Hub exact-session reconciliation is sufficient")

- Rule A (Hub-sufficient): if the W1/W2 captures show existing Core surfaces already give Hub evidence that distinguishes "session process exited, delivery in flight" from "session active, shutdown truly failed", implement the reconciliation in Hub only. No Core ticket.
- Rule B (Core change required -- predicted): if the captures confirm the exit evidence is Core-internal (payload presence gated by worker reap timing or worker exit status; recorded truth erased by the managed rollback), register one blocking dependency ticket against the `botster-core` target (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) carrying: the deterministic W1/W2 reproduction; the required Core contract -- a received `ProcessExited` payload is session-exit truth, and its delivery to drains and observes must not gate on the worker process's own reap timing or exit status, must not block the daemon on `child.wait()`, and a worker connection that dies without a payload keeps its current true-error semantics; Core chooses the mechanism (policy-free mechanism per the Core charter; an acceptable alternative is exposing pending-exit evidence through `observe_session_lifecycle`). This run then blocks on that dependency; after the Core fix merges to Core main, this ticket repins Hub's locked Core revision as required integration (memory note: merged Core main is the consumable artifact) and completes the proofs. Do not silently repin; record the pin change in the Implement report.
- Under both rules, the Hub-owned legs land in this ticket:
  1. `recover_after_core_shutdown_error` stops propagating classify `Err` blindly (`src/daemon_transport.rs:4492`). On classify `Err` after a Core shutdown error, Hub falls back to a non-draining recorded-truth read of the exact session (registry-backed, the store `ListSessions` reads): recorded `Exited` -> `SessionCleanup{already_exited}`; recorded `Stale`/`Failed` -> `SessionCleanup{stale_session}`; recorded `Stopping` -> `SessionCleanup{already_exited}`; recorded `Running` or fallback failure -> the original typed Core error, preserved. This fixes sub-case 6 and never invents exit evidence.
  2. True-error preservation stays byte-for-byte: `Active` classification plus a real Core error still returns the typed `OperatorError`; `shutdown_error_is_already_gone` stays the only `Active` escape hatch.
  3. Unit tests define the strict Active-to-Exited reconciliation contract (Phase 3, item 4), extending the existing shutdown unit family in `src/daemon_transport.rs`.

### Phase 3 -- strict-contract proofs (ticket-required)

1. `external_hub_webrtc_live_output_preserves_exact_bytes`: keep the byte-exactness proof; remove the 10-second observed-exit wait and `ReadScreen` pump; restore the blind `ShutdownSession` immediately after the byte proof and peer close; assert `shutdown.kind` is `Events` or `SessionCleanup` with `outcome == "already_exited"` when cleanup, and never `OperatorError`, with the full typed error body in the panic message. Keep the Absent-leg `unknown_session` probe and the worker reap.
2. `unix_shutdown_session_from_another_connection_classifies_attached_exit`: keep the blind-call strict shape; extend the `assert_ne!` to print the full typed error body on failure.
3. New deterministic forced-window tests (the primary gate): under W1 and W2 on one transport each (W1 on WebRTC, W2 on Unix, or as Implement finds smaller -- both windows must be covered), blind `ShutdownSession` after natural exit returns `Events` or `SessionCleanup` and never `OperatorError`; under W2, the session must also reach a terminal lifecycle on the control plane (this proves the stuck-`running` divergence is gone). These tests bind the Rule A or Rule B fix.
4. Unit strict-contract tests in `src/daemon_transport.rs`: recover-fallback legs (recorded `Exited` -> `already_exited`; recorded `Stale` -> `stale_session`; recorded `Stopping` -> `already_exited`; recorded `Running` -> original typed error preserved; fallback failure -> original typed error preserved), plus the existing seven shutdown unit tests staying green.
5. Tighten `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`: the blind branch no longer admits typed `OperatorError`; every round's blind call must return `Events` or `SessionCleanup`.
6. Red-on-revert: with the exact-session reconciliation removed (Rule A: revert the Hub reconciliation; Rule B: additionally run the W1 forced-window test against the pre-fix pinned Core), the W1 forced-window test must fail with the transient `OperatorError`, and the recover-fallback unit tests must fail. Revert restored and proven green afterward.
7. `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` stays green unmodified: true errors and sibling survival are preserved.

Ordering: Phase 1 lands first and its captures gate Phase 2. No production edit before the diagnosis captures are recorded.

## Non-scope

- No new wall-clock delay, retry count, or suite-load-timing correctness mechanism anywhere (ticket requirement).
- Production budgets (Core 2 s shutdown deadline, worker 500 ms grace, Hub observe-turn bounds) stay unchanged unless a Phase 1 capture proves a budget is the defect; that finding routes through Rule B, not a silent edit.
- No changes to `botster-hub-client` DTOs or the wire contract: `Events`, `SessionCleanup`, and `OperatorError` kinds already exist.
- Do not absorb `ticket_1786938984_190098` (ready_spawn budget flake owner; depends on this ticket), `ticket_1786937228_425608`, or `ticket_1786913892_208903`. `ticket_1787004132_469467` stays closed as duplicate; its Unix proof is check 2 here.
- Do not modify the true-error sibling-survival test's contract.
- Do not edit Core in this worktree; Core changes go through the Rule B dependency ticket only.
- Do not create a pull request (merge policy is direct).

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon control plane, `ShutdownSession` classification and recovery, the lifecycle tests, and the host response contract. Core owns worker exit-evidence mechanics (`drain_output`'s completion gate), the shutdown deadline loop, the managed-runtime rollback, and `observe_session_lifecycle` semantics. The predicted Rule B fix (payload delivery must not gate on worker reap timing or exit status) is a policy-free mechanism change and belongs to Core; Hub must not fork or emulate it with timing.

Cross-repo dependency: none registered at plan time. Rule B registers exactly one blocking `botster-core` dependency ticket if and only if the deterministic captures confirm the Core-internal evidence gap. After the Core merge, this ticket repins and completes; the repin is recorded, not silent.

`ticket_1786938984_190098` depends on this ticket (registered previously). Verify enforcement per run rather than assuming it (memory note: dependency enforcement is version-dependent).

## Assumptions and unknowns

- Assumption: the sub-case decomposition above is correct at Core pin `fc541a5`. Basis: direct code reading with citations. Phase 1 validates every sub-case with captures before any production edit.
- Assumption: a wrapper script through `core_engine.session_worker_path` is admissible on the production spawn path. Basis: the config surface exists and the isolated hub builder exposes `.session_worker_bin(...)`. Risk: worker-census helpers in `tests/hub_daemon_lifecycle/process.rs` match the worker executable path; the wrapper may need census-helper awareness (test-only change). Implement verifies both before building on the fixture.
- Unknown: which sub-case fired in the original suite failure and in Verify pair-run 21 (W1 reap delay vs W2 non-success suppression vs lost payload). The captures classify them; fixing the class does not require attributing the historical instance.
- Unknown: the exact `HubClientError` kind/message for the shutdown flush failure vs a drain failure. Phase 1 captures both.
- Boundary stated explicitly: a worker that dies without ever writing a `ProcessExited` payload (lost-payload/SIGKILL) is not "ProcessExited in flight". The strict contract does not cover it; it keeps true-error or stale semantics. The sibling-survival test pins this boundary.

## Affected surfaces/files

- `src/daemon_transport.rs` -- recover-fallback reconciliation (Hub leg), new and extended shutdown unit tests.
- `src/runtime.rs` -- only if the non-draining recorded-truth read needs a narrow accessor; prefer the existing registry-backed read paths.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` -- restored blind exact-bytes oracle; tightened idempotency sibling; W1 forced-window test (if placed here).
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- error-body diagnosis on the Unix strict assert.
- `tests/hub_daemon_lifecycle/sessions.rs` -- W2 forced-window test (if placed here).
- `tests/hub_daemon_lifecycle/session_fixtures.rs`, `package_fixtures.rs`, `process.rs` -- wrapper-worker fixture and census awareness (test-only).
- `Cargo.toml` / `Cargo.lock` -- only under Rule B, the recorded Core repin after the dependency merges.
- `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` -- this plan.
- `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` -- Implement report.

## Risks

- Rule B blocks this run on a Core dependency and a repin. That is schedule risk the ticket explicitly authorizes; the alternative (Hub-side timing heuristics) is forbidden by the ticket.
- The wrapper fixture could interact with worker-census reap helpers (wrapper pid vs worker pid). Mitigation: census awareness is a test-only adjustment verified in Phase 1 before anything builds on it.
- Tightening the idempotency sibling's blind branch converts previously-admitted outcomes into failures anywhere the product still errs. Intended tripwire; the deterministic forced-window tests must be green first, so ordering inside Implement is: fix, forced-window green, then tighten oracles.
- A Core fix under Rule B could change `Events` vs `SessionCleanup` frequency in existing lifecycle tests. Mitigation: the strict oracles accept both; any other test that pins one kind gets exact-evidence attribution, not a blanket waiver.
- The known ready_spawn co-flake (`ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice`) can fail the final smoke run. It stays owned by `ticket_1786938984_190098`; record attribution evidence and do not absorb.
- Repin under Rule B pulls in unrelated Core main changes. Mitigation: record the pin delta; if the delta breaks unrelated Hub tests, route with exact evidence to new tickets rather than absorbing.

## Acceptance checks/tests

All commands run in the ticket worktree with the repo wrapper `./test.sh` (asset-sync check, `BOTSTER_ENV=test`). Direct `cargo test` does not satisfy these gates. Prebuild before daemon suites: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.

1. Phase 1 captures recorded in the Implement report: W1 and W2 on both transports, each with verbatim `error.code`, `error.operation`, `error.message`, and the recover-path classification. The decision-gate outcome (Rule A or Rule B) is stated with the capture lines that decided it.
2. Unix strict proof: `./test.sh --locked --test hub_daemon_lifecycle_test unix_shutdown_session_from_another_connection_classifies_attached_exit` -- 10 consecutive passes (shell loop, tallies recorded).
3. WebRTC strict proof: `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_live_output_preserves_exact_bytes` -- 10 consecutive passes with the restored blind oracle.
4. Forced-window deterministic tests (primary gate): the W1 and W2 tests each pass 10 consecutive targeted runs; the W2 test also proves the session reaches a terminal lifecycle on the control plane.
5. Unit strict-contract tests: exact `--lib` filter listing the recover-fallback legs (Exited, Stale, Stopping, Running-preserves-error, fallback-failure-preserves-error) plus the existing seven shutdown unit tests -- all pass; filters and counts recorded.
6. Red-on-revert (ticket-required): with the exact-session reconciliation removed, the W1 forced-window test fails with the transient `OperatorError` and the recover-fallback unit tests fail; the revert is restored and a green re-run recorded. Under Rule B, the W1 red-proof against pre-fix Core is the recorded Phase 1 capture itself.
7. Charter and teardown binding checks, deterministic: `shutdown_after_observed_exit_returns_session_cleanup`, `shutdown_session_classifies_parked_exit_beyond_one_baseline_page`, the Absent probe inside check 3, the seven-test stale-peer `--lib` filter from the prior plan, and `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` (unmodified) -- all pass with recorded filters.
8. Tightened idempotency sibling: `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` -- 5 consecutive passes with the no-OperatorError blind branch.
9. Strict Rust gates: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings`.
10. Final smoke (exactly one, exclusive): `./test.sh --locked --test hub_daemon_lifecycle_test` at default concurrency. Passing means both ticket tests pass and any other failure is attributed with exact evidence (ready_spawn -> `ticket_1786938984_190098`; anything else -> new ticket, no absorption). This is the ticket's "one exclusive full-suite run as a final smoke test"; it is not the primary gate.
11. Implement report at `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` records: captures, the decision-gate outcome, the Rule B dependency ticket id and repin delta (if fired), all filters and tallies, red-on-revert output, and any attribution evidence.

Downstream proof: the production entry points are the Unix and WebRTC `ShutdownSession` request paths through `src/daemon_transport.rs:3401`; checks 2-4 drive them live on both transports with real spawned workers. No DTO or public-surface change; no live-Hub admission/supervision/package proof class is touched. Under Rule B the repin is proven by the forced-window tests running against the new locked Core.

## Vault gaps worth capturing

- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]: the exact-bytes/idempotency instance in that note is superseded -- the product decision changed the codified host contract itself. The note needs the instance updated and a pointer to the strict natural-exit ShutdownSession contract.
- [[host ShutdownSession classification must call the exact-session Core query]] still says the convention is not shipped; Hub main ships it. Stale shipped-status (carried over from the prior run, still uncaptured).
- New capture candidate after the decision gate resolves: "worker ProcessExited delivery must not gate on worker reap timing or worker exit status" (Core contract) and "ShutdownSession strict natural-exit idempotency is Events-or-SessionCleanup on every transport" (Hub contract).
- New capture candidate: the controlled worker wrapper via `core_engine.session_worker_path` as the reusable deterministic lever for exit-delivery-window tests.

## Implement steps

1. Prebuild the worker binary; confirm hygiene (gitignore, no-colon path).
2. Build the wrapper-worker fixture (W1, W2) and verify census-helper compatibility.
3. Run Phase 1 captures on both transports; record verbatim; validate the sub-case citations.
4. Resolve the decision gate. Rule B: register the `botster-core` dependency ticket with the reproduction and required contract, then block until the Core merge and repin. Rule A: proceed directly.
5. Implement the Hub legs (recover fallback, unit tests). Under Rule B, integrate the repinned Core.
6. Make the forced-window tests green, then restore the blind exact-bytes oracle, extend the Unix assert diagnosis, and tighten the idempotency sibling.
7. Run acceptance checks 2-9, then the single exclusive smoke (check 10).
8. Write the Implement report; commit; no PR.
