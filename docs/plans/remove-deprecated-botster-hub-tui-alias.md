---
description: Plan for removing the deprecated botster-hub tui alias and stale embedded-TUI guidance
---

# Remove deprecated botster-hub tui alias

## Context loaded

- Pipeline context: ticket `ticket_1782753875_199116`, run `run_1782755088_935946`, step `botster_plan`, gate `botster_plan_gate`; Plan Review returned changes required with findings that the current top-level unknown-command path falls through to `boot_summary()` and exits 0.
- Dependency context: closed dependency `ticket_1782519710_637411`, "Add durable device and repo session-template override sources".
- Vault and playbook context: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected: `src/main.rs`, `tests/hub_daemon_lifecycle_test.rs`, `README.md`, `docs/client-protocol.md`, and existing `docs/plans/*tui*` / production runtime plan artifacts.
- Checklist evidence: initial `project_pipelines_create_vault_checklist` returned `plugin worker invoke timeout`, but the checklist later persisted as `checklist_1782755133_870169`; checklist items should be updated with this revised plan evidence. Fallback evidence remains embedded here and in gate evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add an explicit top-level unknown-command error path in `src/main.rs` before or inside the current catch-all branch: preserve bare `botster-hub` no-subcommand behavior, but make any unrecognized subcommand print the top-level usage/unknown-command error to stderr and exit non-zero.
- Remove the top-level `Some("tui")` command branch from `src/main.rs` so `botster-hub tui` uses that explicit unknown-command path.
- Delete the `operator_tui` compatibility handler from `src/main.rs`.
- Remove the `tui` usage entry and remove `tui` from the top-level command list.
- Keep `removed_legacy_ready_output` advertising only `botster-hub apps open --data-dir <dir> botster-tui` for terminal client launch.
- Update `README.md` so production runtime and local TUI instructions no longer recommend or describe `botster-hub tui`; keep the package/app path as the only hub-owned terminal client path.
- Update integration tests that currently assert alias delegation:
  - Keep production runtime package enablement, app registry, and `apps open botster-tui` assertions.
  - Replace alias-success assertions with a focused assertion that `botster-hub tui --data-dir <dir>` fails through normal usage/unknown-command behavior and does not run the `botster-tui` foreground launch contract.
  - Remove the missing-installed-app alias test or rewrite it to the same unknown-command behavior if a separate regression is useful.
- Preserve `--tui-package-path` production runtime support and all `apps open botster-tui` behavior.

## Non-scope

- Do not edit standalone `botster-tui` or `botster-web`.
- Do not reintroduce embedded renderer code, standalone binary probing, PATH lookup, package auto-install, or compatibility/deprecation scaffolding.
- Do not refactor package/app lifecycle, daemon DTOs, supervised entrypoint policy, or hub-client protocol.
- Do not clean up historical plan artifacts beyond search-proof classification; historical `docs/plans/` mentions may remain if intentionally identified as prior-plan history, not supported docs.

## Botster layers touched

- Rust hub CLI/operator layer: `src/main.rs`.
- Hub production runtime/docs layer: `README.md` and production runtime output assertions.
- Rust integration test harness: `tests/hub_daemon_lifecycle_test.rs`.

The production user path to prove is the actual `main` command dispatcher plus `apps open` through the daemon-resolved app launch contract, not only helper code existence. Plan Review verified that the current `_ => {}` catch-all in `main` falls through to `boot_summary()`, so the implementation must change the dispatcher behavior, not only delete the alias branch.

## Assumptions and unknowns

- Assumption: bare `botster-hub` with no subcommand should continue to print the host-profile boot summary and exit successfully.
- Corrected finding: top-level unrecognized commands do not currently print usage or exit non-zero; they fall through to `boot_summary()`. The implementation must create a real unknown-command path before removing `tui` can satisfy acceptance.
- Assumption: `botster-hub apps open --data-dir <path> botster-tui` is already covered by existing terminal app tests and should remain the positive runtime proof.
- Assumption: README is the only non-historical supported docs surface in this repo that still presents the alias as a user command; `docs/plans/` entries are historical planning artifacts unless implementation finds current operator docs elsewhere.
- Unknown: exact stderr text for the new unknown top-level command path should be chosen during implementation and tested through stable behavior: non-zero status, usage-like stderr, and no app launch side effects.
- Unknown: whether a separate test is needed for the alias when no TUI package is installed; because the command should be unknown before package resolution, one unknown-command regression may cover both installed and missing-package states.

## Affected surfaces/files

- `src/main.rs`
  - add explicit unknown top-level command handling while preserving no-arg `boot_summary()`.
  - remove `Some("tui")` dispatch branch.
  - remove `operator_tui`.
  - remove `usage_for("tui")`.
  - remove `tui` from the top-level usage command list.
  - keep `removed_legacy_ready_output` TUI line unchanged if it already points at `apps open`.
- `tests/hub_daemon_lifecycle_test.rs`
  - `removed_legacy_launcher_launcher_enables_local_tui_package_for_apps_open`.
  - `cli_apps_open_terminal_and_tui_alias_use_foreground_launch_contract` should be renamed to reflect only `apps open` foreground launch plus alias removal.
  - `cli_tui_alias_reports_missing_installed_app` should be removed or repurposed into unknown-command coverage.
- `README.md`
  - production runtime command block and surrounding prose around the deprecated alias.
  - "Standalone local TUI" section wording.
- Search proof surfaces:
  - `rg -n "botster-hub tui|deprecated.*tui|operator_tui|usage: botster-hub tui" src tests README.md docs`

## Risks

- False-positive cleanup risk: leaving `operator_tui` unused but present would still preserve stale compatibility code; implementation should delete it, not just remove docs.
- Runtime proof risk: changing README/tests without removing the dispatcher branch would leave the real command supported; acceptance must exercise the binary path.
- Unknown-command fallthrough risk: deleting the `tui` branch without adding an explicit unknown-command arm would make `botster-hub tui --data-dir <dir>` print the host-profile boot summary and exit 0, violating ticket acceptance.
- Overbroad docs churn risk: historical plan artifacts contain old decisions and should not be rewritten unless they are presented as current docs.
- Test fragility risk: unknown-command wording may be generic; assertions should prove non-zero usage and absence of `botster-tui-fixture`, not depend on a brittle sentence unless that sentence is already stable.
- Regression risk: production runtime `--tui-package-path` could be accidentally removed while deleting alias references; tests must still prove package enablement, app listing, and `apps open botster-tui`.

## Acceptance checks/tests

- Focused search before/after:
  - `rg -n "operator_tui|usage: botster-hub tui|botster-hub tui|deprecated.*tui" src tests README.md docs`
  - Expected after implementation: no live code/tests/README supported alias references; only historical plan artifacts if intentionally retained and called out.
- Focused Rust tests:
  - `BOTSTER_ENV=test cargo test removed_legacy_launcher_launcher_enables_local_tui_package_for_apps_open --test hub_daemon_lifecycle_test`
  - `BOTSTER_ENV=test cargo test cli_apps_open_terminal --test hub_daemon_lifecycle_test` or the renamed focused test filter.
  - Any new/repurposed unknown-command test for `botster-hub tui`; it must prove non-zero exit, usage-like stderr, no `botster-tui-fixture`, and no daemon package resolution.
  - A no-arg CLI regression check, if not already covered, should prove bare `botster-hub` still prints the host-profile summary and exits successfully.
- Broader gate if time/runtime permits:
  - `./test.sh --no-run` or `BOTSTER_ENV=test cargo test --test hub_daemon_lifecycle_test` if the repo's current test cost is acceptable for the pipeline step.
- Manual/runtime proof:
  - With a TUI package fixture installed/enabled, `botster-hub apps open --data-dir <dir> botster-tui` prints the fixture marker.
  - `botster-hub tui --data-dir <dir>` exits non-zero through the newly explicit usage/unknown-command handling and does not print the fixture marker or the host-profile boot summary.

## Vault gaps worth capturing

- Capture if the final implementation confirms a durable rule that first-party terminal clients must only be exposed through package/app lifecycle commands, with no hub-owned client-specific aliases.
- Capture if the unknown-command behavior becomes a convention for cold-turkey CLI removals in Botster, especially how tests should assert removed commands without depending on incidental usage text.
- Capture if preserving no-arg host-profile summary while rejecting unknown subcommands becomes the durable Botster hub CLI convention.
- No convention conflict found. The plan follows cold-turkey migration guidance, keeps hub policy focused on package/app lifecycle, avoids compatibility scaffolding, and does not edit standalone client repos.
