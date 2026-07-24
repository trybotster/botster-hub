# Fix Stale Adoption Socket Startup Deadlock

## Context Loaded

- Pipeline context: ticket `ticket_1782926514_450843`, run `run_1782926545_127162`, active Plan step `run_step_1782927428_757333`, gate `botster_plan_gate`, prior Plan Review `review_1782927401_809833`, and all open review findings loaded through Project Pipelines.
- Ticket intent: fix the pre-existing main-branch deadlock where `daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues` hangs during daemon startup reconciliation over a stale worker adoption socket.
- Review correction accepted: the bug is not a missing stale-socket recovery branch. That branch, the helper, and the named test already exist on `origin/main`; the existing branch deadlocks because it re-locks the same non-reentrant `std::sync::Mutex`.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[adoption restart evidence must come from real protocol primitives not defaults]], [[broker reconnect snapshot reads must be timeout bounded to avoid startup stalls]], and [[full suite hangs need source and behavior proof before unrelated waivers]].
- Repo context inspected: `src/runtime.rs`, `src/daemon.rs`, `tests/hub_daemon_lifecycle_test.rs`, `docs/adr/local-runtime-production-readiness.md`, and this plan artifact.
- Existing checklist: `checklist_1782926655_993745` updated with vault notes loaded, no convention conflicts, plan-stage verification evidence, and no plan-time durable capture.

## Root Cause

`HubRuntime::load_from_store` calls `reconcile_sessions(0)` during the real `HubDaemon::start` production path.

Inside `HubRuntime::reconcile_sessions`, the `SessionAdoptionState::Adoptable` arm currently matches directly on:

```rust
self.core_daemon
    .lock()
    .expect("core daemon mutex")
    .adopt_session(&report.record.session_id, now_seconds)
```

That match scrutinee holds the `MutexGuard` temporary for the whole `match`. When `adopt_session` returns the stale worker-control-socket error, the `Err(error) if is_stale_worker_control_socket_adoption_error(&error)` arm calls `self.core_daemon.lock()` again to `mark_stale`. This re-locks the same `std::sync::Mutex<CoreDaemon>` on the same thread and deadlocks. The named test drives exactly this path, so it hangs instead of returning an error or reaching assertions.

## Scope

- Fix `HubRuntime::reconcile_sessions` so the `adopt_session` mutex guard is dropped before stale recovery calls `mark_stale`, or restructure the code to avoid the second lock entirely.
- Keep the existing stale worker-control-socket classification helper unless implementation proves a narrower structured dependency error is available.
- Preserve existing behavior: valid worker-backed sessions are adopted and listed in `recovered_sessions`; stale, unhealthy, duplicate, terminal-running, and missing-evidence records are marked stale and listed in `stale_sessions`.
- Keep the production entry point wired through `HubDaemon::start` -> `HubRuntime::load_from_store` -> `reconcile_sessions`.
- Correct `docs/adr/local-runtime-production-readiness.md` so it no longer claims stale adoption socket behavior is proven until the deadlock fix and named test completion evidence exist.

## Non-Scope

- No WebRTC, LocalWebrtcSignal, LocalWebrtcTransport, browser, DataChannel, SPA, or TUI changes.
- No broad adoption-state rewrite, new runtime lifecycle abstraction, new pipeline/plugin primitive, or optional configurability.
- No fallback to in-process PTY ownership.
- No weakening of real startup failures: durable-state load errors, core runtime setup errors, invalid worker path errors for new spawns, package restore errors, and non-stale `adopt_session` errors must remain fatal.
- No replacement of the existing worker-backed architecture or core/session-worker ownership boundary.

## Assumptions And Unknowns

- Assumption: the correct smallest fix is to bind the `adopt_session` result inside an inner block so the first `MutexGuard` drops before the stale arm calls `mark_stale`.
- Assumption: a single-lock restructure is also acceptable if it stays smaller and clearer, but it must not hold mutable core-daemon state while mutating hub-side reconciliation vectors in a way that widens ownership or borrow scope unnecessarily.
- Assumption: the named test already exercises the production path because it calls `HubDaemon::start(config)`.
- Unknown: whether adding an explicit in-test timeout/watchdog is needed, or whether test-runner-level bounded completion evidence is enough for this ticket. At minimum, acceptance must include wall-clock completion through `./test.sh ... -- --test-threads=1`; if the implementation changes the test harness, it must avoid leaving child processes or sockets behind on timeout.
- Unknown: whether `docs/adr/local-runtime-production-readiness.md` should be corrected before or after implementation evidence. The implementation pass should choose the least misleading wording: either mark the stale adoption socket row as temporarily unproven until tests pass, or update it alongside the verified fix.

## Affected Surfaces / Files

- `src/runtime.rs`: primary production fix in `HubRuntime::reconcile_sessions`; ensure `CoreDaemon` mutex guard lifetime ends before stale recovery re-locking, or avoid re-locking.
- `src/daemon.rs`: production entry point context only; `HubDaemon::start` should continue to use `HubRuntime::load_from_store` and status should continue reporting `stale_sessions`.
- `tests/hub_daemon_lifecycle_test.rs`: named regression must complete and retain assertions that startup reports the stale session and can spawn a fresh session afterward.
- `docs/adr/local-runtime-production-readiness.md`: correct the false "proven" readiness claim for stale adoption socket reconciliation.
- `test.sh`: no expected change; use it for acceptance because Botster tests require the repo wrapper.

## Implementation Plan

1. Change `HubRuntime::reconcile_sessions` in the `Adoptable` arm only.
   - Compute `let adoption_result = { let mut core_daemon = self.core_daemon.lock().expect("core daemon mutex"); core_daemon.adopt_session(&report.record.session_id, now_seconds) };`
   - Match on `adoption_result` after the inner block ends.
   - Keep `mark_stale` in the stale-error branch, now after the first guard has dropped, or use an equivalent single-lock structure with no re-entrant lock.
   - Do not change the stale-error classifier unless required by compiler/lint feedback.

2. Preserve the existing reconciliation outcomes.
   - `Ok(session)` still pushes `session.session_id` into `recovered_sessions`.
   - Stale worker-control-socket adoption errors still call `mark_stale` and push the original report session id into `stale_sessions`.
   - Other `adopt_session` errors still return `Err(error)`.
   - Existing non-adoptable stale branches remain unchanged unless compiler refactoring requires local movement.

3. Correct readiness documentation.
   - Update the stale adoption socket row in `docs/adr/local-runtime-production-readiness.md` so it does not cite a hanging test as already proven without qualification.
   - After implementation verification, the row may claim proof only with the bounded-completion command evidence.

4. Verify runtime path and liveness.
   - Run the named ticket command through `./test.sh`.
   - Record that the test completes, not merely that it compiles or reaches an assertion.
   - Run adjacent startup reconciliation and live-worker adoption coverage to prove the fix did not turn valid adoption into stale marking.
   - Run broader `./test.sh` or isolate any remaining unrelated failures with exact evidence.

## Risks

- Mutex lifetime regression: a direct `match` or `if let` over `self.core_daemon.lock().adopt_session(...)` can keep the guard alive longer than it looks. The implementation must use an explicit inner scope or otherwise make the guard lifetime obvious.
- Over-fixing risk: replacing the reconciliation flow could accidentally change adoption semantics beyond the stale socket deadlock. Keep the diff to lock lifetime and documentation.
- False verification risk: an error-message assertion does not prove a deadlock is gone. Acceptance must prove bounded completion of the named test.
- Documentation overclaim risk: the ADR currently cites the deadlocking test as proof. The docs change must not continue claiming proof without the new command evidence.
- Full-suite risk: if `./test.sh` still fails elsewhere, implementation must show the named test no longer hangs and isolate unrelated failures by test name/output rather than waiving the whole suite.

## Acceptance Checks / Tests

- Required liveness check: `./test.sh daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues -- --test-threads=1` completes reliably and reports success. This is the primary ticket acceptance; a hang is a failure.
- Startup reconciliation regression: `./test.sh daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions -- --test-threads=1` passes.
- Live worker recovery regression: run `./test.sh cli_daemon_restart_recovers_worker_backed_session_through_transport -- --test-threads=1`, or if too expensive/environment-blocked, run the narrowest existing runtime adoption test and document why it proves valid worker-backed sessions are still adopted.
- Full-suite disposition: run `./test.sh` when practical. If it cannot complete, provide exact evidence that any remaining failure/hang is unrelated to `daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues`.
- Documentation check: `docs/adr/local-runtime-production-readiness.md` no longer contains a false stale-adoption-socket "proven" claim that cites a hanging test.
- Production path proof: implementation report must identify `HubDaemon::start` -> `HubRuntime::load_from_store` -> `reconcile_sessions` as the path changed by the mutex lifetime fix.

## Vault Gaps Worth Capturing

- Capture a durable Botster/Rust gotcha if implementation confirms this match-scrutinee `MutexGuard` lifetime pattern is not already represented in the vault: "match scrutinee mutex guards can live across arms and deadlock stale recovery re-locks."
- Capture a reusable verification note if a bounded deadlock/liveness harness is added to the test itself.
- No new convention conflict found. The corrected plan stays within Rust hub startup reconciliation, worker-backed core ownership, explicit daemon startup, and Project Pipelines path-neutral artifact conventions.
