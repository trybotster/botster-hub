# Do Not Fail Daemon Startup On Stale Worker Adoption Sockets

## Context Loaded

- Pipeline context: ticket `ticket_1780618833_550363`, run `run_1780618913_745895`, Plan step `botster_plan`, gate `botster_plan_gate`.
- Ticket intent: stale per-session worker control socket evidence must not abort `HubDaemon::start` or `botster-hub start --data-dir`; it must be surfaced as stale-session reconciliation state. Real configuration/runtime setup errors must still fail startup.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], [[pipeline artifacts should cite vault notes by wikilink not home path]], [[pipeline artifacts should use path neutral worktree references]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected: `Cargo.toml`, `Cargo.lock`, `README.md`, `src/daemon.rs`, `src/runtime.rs`, `src/main.rs`, `src/lib.rs`, `src/daemon_transport.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_runtime_test.rs`, `docs/adr/local-runtime-dogfood-readiness.md`, and prior `docs/plans/*`.
- Locked core context inspected: `botster-core-daemon` `CoreDaemon::adoption_scan`, `CoreDaemon::adopt_session`, `SessionAdoptionState`, `CoreDaemonError`, `RegistryRecord`, and `botster-core` worker-process adoption code at the `Cargo.lock` git revision.
- Checklist evidence fallback: creating a Project Pipelines checklist timed out with `plugin worker invoke timeout`, so checklist provenance is preserved in this plan and gate evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add a failing regression test for startup over a data directory containing a persisted `running` registry record with real restart/adoption evidence and a `worker_control_socket` path that is stale at adoption time.
- Make startup reconciliation tolerate stale/refused/missing per-session worker control sockets by marking/reporting the session stale instead of returning `HubDaemonError::Runtime` from `HubDaemon::start`.
- Preserve the existing worker-backed architecture: `HubDaemon` owns startup policy, `HubRuntime` bridges to `CoreDaemon`, and core/session workers remain the PTY/process owners.
- Preserve existing status visibility: `HubDaemonStatus.stale_sessions`, daemon transport `stale_sessions`, and CLI `stale_session_count`/`stale_session id=...` output should report the stale record deterministically.
- Add or update a concise readiness/doc note stating that stale worker records are tolerated and surfaced during startup reconciliation.

## Non-Scope

- No fallback to in-process PTY ownership.
- No broad adoption rewrite, new daemon lifecycle type, new plugin workflow primitive, or frontend change.
- No suppression of all `adopt_session` errors.
- No change that makes missing worker executable, invalid runtime setup, durable state load failures, package registry restore failures, or registry corruption non-fatal.
- No cleanup of unrelated historical docs or stale plan artifacts.

## Assumptions And Unknowns

- Assumption: the ticket's refused-socket failure occurs after `adoption_scan` classifies a registry record as `Adoptable`, then `CoreDaemon::adopt_session` calls the worker runtime and gets a `SpawnFailed` error whose message includes `connect worker control socket failed`.
- Assumption: the smallest hub-side fix is acceptable if `botster-core-daemon` does not expose a more structured stale-adoption error in the locked dependency. Prefer a narrow helper that recognizes the exact stale worker control socket adoption failure over a broad formatted-error catch.
- Assumption: status visibility through existing `stale_sessions` is the intended "stale_session_count or equivalent"; do not invent a second counter unless implementation discovers a contract gap.
- Unknown: whether the locked core dependency exposes enough structured source fields to avoid message classification. The implementer should inspect `CoreDaemonError::Engine` -> `ManagedSessionRuntimeError::Runtime` -> `SessionRuntimeError` before settling. If only formatted classification is possible, keep it private and exact.
- Unknown: whether a missing socket and connection-refused socket can both be reproduced deterministically in one platform-independent test. Cover at least one exact failing path, and prefer both if the fixture stays small.

## Affected Surfaces And Files

- `src/runtime.rs`: `HubRuntime::reconcile_sessions` is the likely production-path change. It currently marks stale states returned by `adoption_scan`, but an error from `core_daemon.adopt_session` inside the `Adoptable` arm aborts startup.
- `src/daemon.rs`: `HubDaemon::start` should continue to call `HubRuntime::load_from_store`; status should continue deriving `stale_sessions` from runtime reconciliation.
- `src/main.rs`: CLI status/start output already prints `stale_session_count`; only touch if tests expose missing user-path output.
- `src/daemon_transport.rs`: daemon transport already serializes stale sessions; only touch if the regression test proves transport status misses startup reconciliation.
- `tests/hub_daemon_lifecycle_test.rs`: primary regression home for `HubDaemon::start` and `botster-hub start/status --data-dir` behavior.
- `tests/hub_runtime_test.rs`: optional lower-level coverage if the classification helper is easier to pin at the runtime boundary.
- `README.md` or `docs/adr/local-runtime-dogfood-readiness.md`: readiness note for tolerated stale worker records.
- Locked dependency context, not directly edited unless required: `botster-core-daemon` adoption API and `botster-core` worker-process adoption error shape.

## Implementation Plan

1. Write the regression first.
   - Build a test data directory using `SessionRegistry`.
   - Persist a `RegistryRecord::running` with process identity, current protocol evidence, `handshake_verified`, `ping_pong_supported`, and `recovery_identity.worker_control_socket`.
   - Point the socket path at a stale endpoint: preferably a Unix socket path whose listener has closed so `UnixStream::connect` returns connection refused; if that is too platform-sensitive, use a missing socket path that still exercises the same adoption-failure branch.
   - Assert current pre-fix behavior fails with `SpawnFailed: connect worker control socket failed` or document that the newly added test encodes that failure before the fix is applied.

2. Implement the smallest startup reconciliation change.
   - In `HubRuntime::reconcile_sessions`, keep `adoption_scan` errors fatal.
   - For `SessionAdoptionState::Adoptable`, call `adopt_session` and recover only when the error is the stale worker-control-socket adoption failure.
   - On that narrow stale adoption failure, call `mark_stale`, push the session id into `self.reconciliation.stale_sessions`, and continue reconciling other records.
   - On any other adoption error, return the original error so startup remains fatal.

3. Keep runtime/user-path visibility wired.
   - Assert `HubDaemon::start(config).expect(...)` succeeds with the stale fixture.
   - Assert `daemon.status().stale_sessions` contains the stale session id.
   - Add a CLI or daemon-transport status assertion if the test harness can start the daemon cheaply: `status --data-dir` should show `stale_session_count=1` and the stale id, without local path leakage.

4. Prove live behavior still works.
   - Reuse existing spawn/restart recovery tests or add an assertion that a fresh session can still spawn after stale reconciliation.
   - Keep existing live-worker adoption tests passing so valid worker-backed sessions are still recovered, not marked stale.

5. Update the readiness/docs note.
   - Mention stale worker control socket records are tolerated during startup reconciliation and surfaced as stale sessions.
   - Avoid absolute local paths and PII in examples.

## Risks

- Overbroad error swallowing could hide real startup misconfiguration, especially a missing `botster-session-worker` binary during new session spawn. The helper must classify only stale per-session adoption socket failures.
- String matching against nested dependency errors is brittle. Prefer structured matching if the public error chain exposes `SessionRuntimeErrorKind::SpawnFailed` and the exact worker-control-socket message.
- A missing socket can be classified as `StaleWorker` during `adoption_scan` before `adopt_session`; the regression must force the stale-at-adoption race or explicitly cover the missing-socket branch plus document why connection refused cannot be made deterministic.
- Tests that spawn real daemon processes are slower and serialized. Keep the main regression at the `HubDaemon::start`/`HubRuntime` level, then add a focused CLI status check only if needed for the user path.
- Documentation can overclaim durability. Say stale worker records are tolerated and reported; do not imply uncoordinated process-crash PTY recovery is solved.

## Acceptance Checks And Tests

- Failing-first evidence: the new regression test fails before the fix with the stale adoption socket startup error, or the implementation report includes the exact pre-fix failure the test encodes.
- Targeted test: `cargo test --test hub_daemon_lifecycle_test <new_stale_socket_test_name>`.
- Existing startup reconciliation coverage: `cargo test --test hub_daemon_lifecycle_test daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions`.
- Live worker recovery coverage: `cargo test --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport` or the narrower runtime adoption test if process-level CLI coverage is too expensive for the implementer pass.
- Fresh-spawn proof after stale reconciliation: either included in the new test or covered by a nearby daemon lifecycle test with explicit evidence.
- Full repo-approved verification for final handoff: `./test.sh --unit` and strict clippy if this repo's current lint policy requires it for the touched Rust code.
- CLI/user-path smoke when practical: `botster-hub start --data-dir <test-data-dir>` followed by `status --data-dir <same>` reports `stale_session_count` and remains responsive.

## Vault Gaps Worth Capturing

- Capture a durable note only if implementation confirms the hub must classify a dependency error by formatted message because `botster-core-daemon` lacks a structured stale-adoption error. That would be a reusable Botster boundary gap.
- Capture a durable note if the reproducible connection-refused fixture needs a specific Unix socket pattern worth reusing in future daemon tests.
- No convention conflict found in planning: the plan follows worker-backed core ownership, explicit-data-dir daemon startup, path-neutral plan artifacts, and Project Pipelines checklist-fallback discipline.

## Checklist Evidence Fallback

- Checklist persistence attempted for this run and failed with `plugin worker invoke timeout`.
- Vault/context checklist item evidence: notes loaded are listed in `Context Loaded`; they constrain this ticket to Rust hub/session-worker startup reconciliation and repo-visible plan artifacts.
- Convention-conflict checklist item evidence: no conflicts found; the plan does not add new primitives or bypass worker-backed architecture.
- Verification checklist item evidence: `project_pipelines_current_context` loaded run/ticket/gate context; repo searches and file inspections identified `src/runtime.rs` as the likely change point and existing stale status surfaces.
- Capture checklist item evidence: no durable vault capture is needed at plan time; implementation may capture a structured-error gap if it proves real.

