# Allow Client-App-Only Packages Through Prepare Policy

## Context Loaded
- Project Pipelines context: run `run_1782410314_137825`, step `botster_plan`, ticket `ticket_1782410304_700230`, gate `botster_plan_gate`; no prior artifacts, questions, answers, findings, reviews, or blocking dependencies were present.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Required Botster overlay notes: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]].
- Ticket-specific vault notes: [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster runnable entrypoints are hub owned launch contracts]], [[local runnable packages still need core entrypoint for enable prepare]], [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[durable package snapshots must reconstruct admission through live helpers]].
- Checklist discipline: attempted `project_pipelines_create_vault_checklist` for this run, but the Project Pipelines worker returned `plugin worker invoke timeout`. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist-equivalent evidence is preserved in this artifact and should also be included in gate evidence.

## Scope
- Update hub package prepare/admission policy so a local package with `entrypoints: []` is accepted when it declares one or more valid `runnable_entrypoints`.
- Preserve rejection for local packages with neither core `entrypoints` nor `runnable_entrypoints`.
- Preserve existing local path, symlink, traversal, command, working-directory, and runnable-entrypoint contract validation.
- Add regression coverage for a botster-tui-style manifest with no Lua/plugin entrypoints and one `terminal_app` / `foreground_stdio` runnable entrypoint.
- Prove the production user path, not only a helper path: daemon local package enable/prepare, app registry projection, `ResolveAppLaunch`, and CLI `apps list/show/open` or equivalent foreground terminal launch behavior.

## Non-Scope
- Do not change the core `entrypoints` ABI or rename `runnable_entrypoints`.
- Do not add compatibility shims, optional configuration flags, or broad package lifecycle refactors.
- Do not move app launch resolution from the daemon into the CLI.
- Do not change browser/TUI rendering, Project Pipelines UI, package marketplace behavior, or supervised process semantics beyond what the admission fix requires.
- Do not loosen capability admission or unsafe local path checks.

## Assumptions And Unknowns
- Assumption: the intended new policy is "core entrypoint required unless at least one runnable entrypoint exists and validates." A package with an empty core `entrypoints` array and a valid terminal runnable entrypoint should enable.
- Assumption: `PreparedLocalPackage` can represent a runnable-only package without a `selected_entrypoint`; implementers should avoid fabricating an inert Lua entrypoint because the ticket explicitly removes that requirement.
- Assumption: plugin load behavior for runnable-only packages should be a no-op for code-load entrypoints while still preserving package enabled/app projection state.
- Unknown: whether the current prepare call sites require `selected_entrypoint` unconditionally for enabled packages. Implementer must inspect every `PreparedLocalPackage` consumer and make the smallest type/API adjustment needed so runnable-only packages do not trigger plugin loading.
- Unknown: whether durable snapshot reload also calls prepare-like validation for enabled local packages. If it does, the same admission semantics must be used there to avoid live-vs-reload drift.

## Affected Surfaces And Files
- Botster layer touched: Rust hub package policy, daemon package/app lifecycle, CLI app path tests. No SPA, TUI UI, Rails relay, or MCP behavior should change.
- Primary implementation file: `src/packages.rs`.
  - Current rejection is in `PreparedLocalPackage::from_record`, which fails with `UnsafeEntrypoint("local package has no entrypoints")` when `record.manifest.entrypoints.first()` is absent.
  - Existing runnable contract validation is in `validate_runnable_entrypoints` / `validate_runnable_entrypoint_contract`; keep this intact.
- Likely production call sites: package enable/prepare flows that call `PreparedLocalPackage::from_record`, plugin lifecycle loading of prepared entrypoints, daemon package mutation handlers, app projection from `record.runnable_entrypoints`, and `ResolveAppLaunch`.
- Primary tests: `src/packages.rs` unit tests for admission policy and `tests/hub_daemon_lifecycle_test.rs` daemon/CLI regression tests.
- Existing helper likely to update: `write_botster_tui_package` currently writes an inert `plugin.lua` and core entrypoint; the new regression should remove the core entrypoint for the botster-tui-style fixture or add a sibling fixture that does.
- Documentation/vault follow-up: [[local runnable packages still need core entrypoint for enable prepare]] becomes stale if the implementation lands and should be revised or superseded.

## Risks
- Over-loosening admission could allow an empty package to enable; guard explicitly rejects no core entrypoints plus no runnable entrypoints.
- Skipping `canonical_entrypoint_path` for runnable-only packages is correct only because there is no core entrypoint path; runnable command and working-directory validation must remain unchanged.
- Existing `PreparedLocalPackage` consumers may assume `selected_entrypoint` exists. A broad optionality change can ripple unnecessarily, so isolate the no-code-load path as tightly as possible.
- App registry tests can pass while prepare/admission is still bypassed; regression coverage must drive the daemon enable path that currently fails.
- CLI `apps open` must continue using daemon `ResolveAppLaunch` for foreground terminal apps; do not reconstruct launch command in CLI tests or implementation.
- Checklist persistence failed during planning, so gate/artifact evidence must carry the workflow proof.

## Acceptance Checks And Tests
- Add a focused `src/packages.rs` unit test:
  - local manifest with `entrypoints: []` and a valid `terminal_app` / `foreground_stdio` `runnable_entrypoints` row installs and prepares/enables successfully;
  - local manifest with `entrypoints: []` and `runnable_entrypoints: []` remains rejected with an unsafe-entrypoint style reason.
- Add or update a daemon lifecycle regression in `tests/hub_daemon_lifecycle_test.rs`:
  - create a botster-tui-style package with no core `entrypoints`;
  - prove `EnablePackageLocalPath` or `packages install` plus `packages enable` succeeds;
  - prove `apps list` and `apps show` expose the terminal app row;
  - prove `ResolveAppLaunch` and/or `botster-hub apps open --data-dir <dir> botster-tui` returns/runs the foreground launch contract.
- Run focused tests with the repo wrapper, for example:
  - `./test.sh packages`
  - `./test.sh --test hub_daemon_lifecycle_test daemon_resolves_terminal_app_foreground_launch_contract`
  - `./test.sh --test hub_daemon_lifecycle_test cli_apps_open_terminal_and_tui_alias_use_foreground_launch_contract`
- If test names change, run the nearest focused package admission and daemon app launch tests. If failures occur outside touched code, preserve exact evidence instead of blanket-waiving.

## Vault Gaps Worth Capturing
- Update or supersede [[local runnable packages still need core entrypoint for enable prepare]] once implementation proves runnable-only packages now pass prepare/admission.
- Capture the new stable policy if it holds: local package prepare admits either a valid core code-load entrypoint or at least one valid runnable entrypoint, while empty manifests still fail closed.
- Capture any implementation-specific rule discovered for `PreparedLocalPackage` consumers, especially if runnable-only packages intentionally skip plugin load while still becoming enabled app packages.
