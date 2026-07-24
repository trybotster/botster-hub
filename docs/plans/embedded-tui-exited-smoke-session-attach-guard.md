# Embedded TUI Exited Smoke Session Attach Guard

## Context Loaded

- Project Pipelines context for `ticket_1781065921_836565`, `run_1781065926_490279`, step `botster_plan`, gate `botster_plan_gate`.
- Vault notes: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo code: `src/tui.rs`, `src/main.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_runtime_test.rs`.
- Checklist attempt: `project_pipelines_create_vault_checklist` and `project_pipelines_create_checklist` both timed out in the plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence preserve the checklist facts.

## Scope

- Change only the embedded hub CLI TUI behavior in `src/tui.rs`.
- Keep exited sessions visible in the session list.
- Treat sessions whose `DaemonSession.lifecycle == "exited"` as non-attachable.
- Surface a clear operator diagnostic such as `exited - cannot attach` when the selected or clicked session is exited.
- Prevent every embedded TUI attach entry point from issuing `DaemonRequest::Attach` for exited sessions:
  - scripted driver `attach_selected`
  - Enter key attach
  - session row activation
  - terminal panel activation/click attach fallback
  - mouse double-select attach path
- Preserve running-session attach, detach, input, resize, and daemon reconnect behavior.
- Preserve the existing drain-time `UnknownSession` recovery path that detaches, refreshes, and records one actionable diagnostic.
- Add focused tests covering the exited `legacy-runtime-worker-smoke` scenario, attach guard, running-session attach still works, and no repeated duplicate UnknownSession diagnostic loop.

## Non-Scope

- Do not edit standalone `botster-tui`.
- Do not edit `botster-core`, `botster-core-daemon`, `botster-web`, Rails relay, Project Pipelines plugin policy, or browser UI.
- Do not change daemon attach protocol semantics unless implementation proves the TUI cannot correctly classify lifecycle locally.
- Do not introduce new configuration, plugin policy, broad UI refactors, or cross-client abstractions.
- Do not hide exited sessions unless an existing test proves visibility itself is broken.

## Assumptions And Unknowns

- The daemon session list lifecycle string is the authoritative local TUI guard input; current code already checks `"running"` on reconnect and renderer tests use `"exited"`.
- The manual bug came from the removed legacy launcher creating an exited `legacy-runtime-worker-smoke` session that remains listed, then `botster-hub tui --data-dir <dir>` selecting or activating it.
- The correct UX is visible but inert: show the row and diagnostic, but leave `active_session_id` and `subscription_id` unset.
- Unknown: whether initial selection should skip exited sessions when a running session is also present. The smallest ticket-satisfying behavior is to keep selection as-is and guard attach; skipping selection would be a broader navigation policy change.
- Unknown: exact wording of the diagnostic can be finalized by implementation, but it must be clear and test-pinned.
- Worktree/target assumption: this run is bound to target `tgt_7e208a0c76a44980a83b63af976b1f22` in the current ticket worktree.

## Affected Surfaces And Files

- Botster layer: Rust hub embedded TUI client.
- Primary implementation: `src/tui.rs`.
- Existing production entry point: `src/main.rs` routes `botster-hub tui --data-dir <path>` into `run_tui(config)`, which builds `TuiClient` and handles key/mouse/scripted attach paths.
- Existing daemon behavior read for context: `src/daemon_transport.rs` and `crates/botster-hub-client/src/lib.rs`.
- Focused integration tests: `tests/hub_daemon_lifecycle_test.rs`.
- Possible unit/render tests: `src/tui.rs` test module.
- Production runtime reference path: `src/main.rs` production runtime smoke setup and `tests/hub_local_runtime_test.rs`.

## Implementation Shape

- Add a small `TuiClient` helper that returns the selected `DaemonSession` or tests whether a session id is attachable.
- Make `attach_selected` fail before `detach()` and before `DaemonRequest::Attach` when the selected lifecycle is not `"running"`.
- Record a single clear diagnostic for exited selected sessions, preferably without duplicating the same row repeatedly on repeated Enter/click attempts.
- Keep `attach_session(session_id)` as the low-level attach path for already validated attach/reconnect flows, or add validation there only when lifecycle is available and it does not break reconnect.
- Update `session_row_node` meta/subtitle rendering to expose `exited - cannot attach` for exited rows while preserving `attached` metadata for the active running session.
- Keep `clear_stale_attached_session` behavior narrow: it should only handle drain-time `UnknownSession` for an already attached session.

## Risks

- Accidentally blocking attach for valid non-`running` transitional states if the daemon emits lifecycle labels other than `running` and `exited`. Mitigate by inspecting current `DaemonSession` lifecycle producers and pinning the guard specifically to `exited` if needed.
- Duplicate diagnostics from repeated key/mouse attempts could recreate the spam class in a new path. Tests should count diagnostic rows.
- Rendering-only changes would not fix runtime behavior. Tests must prove no subscription id is created and no active session is set after attempting to attach the exited smoke session.
- A broad protocol change could break CLI attach or browser attach, which are out of scope. Keep the guard client-local unless daemon behavior must be corrected.
- Existing long-running daemon tests are serialized and can be slow/flaky; use targeted filters first, then broader `./test.sh` or `BOTSTER_ENV=test cargo test` if time allows.

## Acceptance Checks And Tests

- Add or update an integration test in `tests/hub_daemon_lifecycle_test.rs` that creates or observes an exited `legacy-runtime-worker-smoke` session, connects `ScriptedTuiDriver`, selects it, calls `attach_selected`, and asserts:
  - attach is rejected or returns an actionable error
  - `active_session_id()` remains `None`
  - `subscription_id()` remains `None`
  - errors contain `exited - cannot attach` or the chosen equivalent
  - repeated attach attempts do not add duplicate diagnostics
- Keep or strengthen the existing `scripted_tui_detaches_and_refreshes_when_drain_reports_unknown_session` test so it continues proving one refresh/detach diagnostic and no generic repeated `unknown_session` spam.
- Add a positive assertion that a running replacement session remains attachable after the exited-session guard.
- Add a unit/render assertion in `src/tui.rs` that an exited row remains visible and is marked non-attachable.
- Run targeted tests first:
  - `BOTSTER_ENV=test cargo test --test hub_daemon_lifecycle_test scripted_tui_detaches_and_refreshes_when_drain_reports_unknown_session`
  - `BOTSTER_ENV=test cargo test --test hub_daemon_lifecycle_test <new exited smoke session test name>`
  - `BOTSTER_ENV=test cargo test tui::tests::<new or touched tui renderer test>`
- If targeted tests pass, run the repo-preferred Rust gate if feasible: `./test.sh` or `BOTSTER_ENV=test cargo test`.

## Vault Gaps Worth Capturing

- Capture a Botster CLI/TUI note if implementation confirms the durable rule: embedded TUI session lists may show exited sessions, but terminal attach actions must be guarded client-side before daemon attach.
- Capture a short Project Pipelines operational note only if checklist worker timeouts recur after this run; this run already follows the artifact fallback from [[project pipelines checklist worker timeouts require artifact evidence fallback]].

