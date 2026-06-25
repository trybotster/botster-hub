# Replace Hub TUI With Standalone Launcher Or Deprecation Plan

## Context Loaded

- Pipeline context: ticket `ticket_1782338844_576329`, run `run_1782348895_376850`, step `botster_plan`, gate `botster_plan_gate`.
- Dependency context: closed dependency "Bring standalone botster-tui to required session and package dogfood parity".
- Required vault context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected: `src/main.rs`, `src/lib.rs`, `src/tui.rs`, `Cargo.toml`, `README.md`, `docs/client-protocol.md`, and `tests/hub_daemon_lifecycle_test.rs`.
- Checklist context: the Project Pipelines checklist instruction tool was loaded. `project_pipelines_create_vault_checklist` timed out with `plugin worker invoke timeout`, so checklist evidence is preserved in this plan artifact and gate evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Change the production `botster-hub tui --data-dir <path>` path so it no longer enters the embedded `src/tui.rs` ratatui implementation.
- Prefer the smallest deprecation/explicit-command path unless Implementation can prove a standalone `botster-tui` binary contract is locally available and stable enough to delegate to without adding broad process-management policy.
- Make the intended standalone command explicit everywhere the hub currently advertises the embedded TUI: CLI usage/help, dogfood launcher output, README dogfood/TUI docs, and any tests asserting those strings.
- Remove or quarantine embedded hub TUI exports and tests so the hub is not maintaining duplicate rendering logic as a supported dogfood path.
- Drop hub-only TUI dependencies (`ratatui`, `crossterm`) if no remaining non-test hub code needs them after the embedded path is removed.
- Keep the hub daemon/client protocol, `botster-hub-client`, and `botster-hub-test-support` as the supported boundary for standalone first-party clients.

## Non-Scope

- Do not implement or modify standalone `botster-tui` in this repository.
- Do not change daemon socket protocol fields, client compatibility descriptors, session worker protocol, or package lifecycle behavior unless a compile failure requires a narrow import/export cleanup.
- Do not preserve the embedded renderer behind a new feature flag or compatibility command; that would keep the duplicate implementation alive.
- Do not add a new launcher configuration system, package resolver, installation manager, or PATH discovery abstraction.
- Do not alter Project Pipelines plugin workflow policy or UI behavior.

## Botster Layers Touched

- Rust hub CLI: `botster-hub tui`, dogfood output, usage text.
- Rust hub library boundary: remove embedded TUI module exports if the module is deleted.
- Docs: README and any client protocol wording that still implies an embedded hub TUI is first-party production behavior.
- Tests: Rust unit/integration tests that currently depend on `ScriptedTuiDriver`, `run_scripted_probe`, or `botster-hub tui` dogfood output.

## Assumptions And Unknowns

- Assumption: the ticket's explicit "or deprecation/removal path" makes an instructional `botster-hub tui` response acceptable if it clearly names the equivalent standalone `botster-tui --data-dir <path>` command.
- Assumption: because the standalone parity dependency is closed, downstream `botster-tui` owns live terminal UI conformance through `botster-hub-client` and `botster-hub-test-support`; this hub ticket should stop maintaining an embedded renderer rather than porting more behavior.
- Assumption: run target/worktree are the current pipeline-assigned run target and worktree; no additional agent spawn is needed for Plan.
- Unknown: the exact standalone `botster-tui` invocation shape may include flags beyond `--data-dir <path>`. Implementation must confirm from available dependency docs or local binary/help before choosing a delegating launcher. If the command shape cannot be verified, use the deprecation/instruction path rather than guessing.
- Unknown: deleting `src/tui.rs` may expose compile/test dependencies in integration tests that were using the embedded driver as a daemon client harness. Those tests should be replaced with daemon/client API or `botster-hub-test-support` coverage where still relevant.

## Affected Surfaces And Files

- `src/main.rs`: remove `run_tui` import and call; update `operator_tui`; update dogfood printed `tui=` command and usage text.
- `src/lib.rs`: remove `pub mod tui` and public re-exports for `ScriptedTuiDriver`, `ScriptedTuiProof`, `TuiError`, `TuiResult`, `run_tui`, and `run_scripted_probe` if the embedded module is removed.
- `src/tui.rs`: delete or fully remove from production/library ownership if no longer referenced.
- `Cargo.toml` and `Cargo.lock`: remove `ratatui` and `crossterm` when no longer used by the hub crate.
- `tests/hub_daemon_lifecycle_test.rs`: update dogfood output assertions; remove or rewrite embedded scripted TUI tests so runtime coverage uses daemon/socket/client APIs rather than the removed renderer.
- `README.md`: replace "Minimal local TUI" embedded hub instructions with standalone `botster-tui` guidance and clarify that `botster-hub tui` is deprecated or instructional.
- `docs/client-protocol.md`: preserve downstream `botster-tui` client guidance; update any wording that describes an embedded hub TUI as part of the client boundary if touched.
- Historical `docs/plans/*` files are not in scope for retroactive edits.

## Risks

- Silent dogfood regression: changing docs/help without changing `operator_tui` would leave the real user path on embedded `run_tui`. Acceptance must inspect the production entry point.
- Dead duplicate code: leaving `src/tui.rs` exported or tested as a first-party path would keep the maintenance burden the ticket is meant to remove.
- Over-deletion: existing integration tests may prove daemon reconnect, session loss, and exited-session guard behavior through the scripted embedded TUI. Implementation must preserve important daemon/client behavior through lower-level tests or explicitly document that it moved to downstream `botster-tui` conformance.
- Command-shape mismatch: printing a guessed `botster-tui` invocation could mislead operators. Verify help/docs/binary before delegating; otherwise print a conservative explicit instruction.
- Dependency cleanup risk: removing `ratatui`/`crossterm` requires a workspace build/test to catch stale imports and lockfile drift.

## Acceptance Checks And Tests

- Production path proof: inspect or test that `botster-hub tui --data-dir <dir>` no longer calls `run_tui` or any embedded renderer. It should either execute a verified `botster-tui` command or print a clear standalone equivalent command.
- Dogfood output proof: focused test around the dogfood launcher output should assert it advertises standalone `botster-tui --data-dir <path>` or an explicit deprecated hub command that prints that equivalent, not `botster-hub tui` as the primary TUI.
- Duplicate-code proof: `rg -n "run_tui|ScriptedTui|run_scripted_probe|ratatui|crossterm|pub mod tui|botster_hub::tui" src tests Cargo.toml` should show no supported embedded TUI path remains, except historical plan/docs references if intentionally left.
- CLI behavior proof: add or update a focused Rust test for `operator_tui`/CLI usage behavior if the existing test harness can invoke the binary; otherwise document the exact command output and exit behavior in README and gate evidence.
- Build/test commands: run `./test.sh` or at minimum the focused daemon lifecycle tests touched plus a workspace compile check. Because [[test script required for rust tests not cargo test]] applies, prefer `./test.sh <focused-filter>` and a broader `./test.sh` when feasible.
- Docs proof: README no longer instructs users to operate the embedded hub TUI as the dogfood UI; client protocol docs still point standalone clients at `botster-hub-client` and conformance helpers.
- PII proof: scan the plan and changed docs for local home paths or run-specific filesystem paths before review.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Plan gate evidence should cite this file and include the checklist fallback evidence because checklist creation timed out.
- Plan Review should reject a plan or implementation that keeps `botster-hub tui` wired to `src/tui.rs`, preserves embedded ratatui rendering as a supported path, or only updates documentation without proving the production entry point changed.

## Vault Gaps Worth Capturing

- Capture a durable note if Implementation confirms a stable local convention for hub commands that deprecate to standalone first-party clients, especially the expected exit code and wording for instructional commands.
- Capture a note if removing embedded client tests reveals a reusable pattern for migrating hub-owned scripted client harnesses to `botster-hub-test-support` conformance.
- No convention conflict found. The plan follows the loaded notes: Botster clients should use the hub client protocol boundary, Project Pipelines policy remains plugin-owned, the plan artifact uses wikilinks instead of local paths, and checklist timeout evidence is preserved in durable plan/gate surfaces.
