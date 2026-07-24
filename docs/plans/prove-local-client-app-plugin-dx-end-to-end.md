---
ticket: ticket_1782361547_201925
title: Prove local client app plugin DX end to end
run: run_1782408170_994215
step: botster_plan
---

# Prove Local Client App Plugin DX End To End

## Context Loaded

- Project Pipelines context loaded with `project_pipelines_current_context` for run `run_1782408170_994215`, run step `run_step_1782408170_374879`, current step `botster_plan`, gate `botster_plan_gate`, ticket `ticket_1782361547_201925`.
- Ticket: "Prove local client app plugin DX end to end". Acceptance asks for docs/tests proving fresh-checkout commands, stable data-dir persistence of local package installs, obvious production runtime URL/TUI output, and no PII.
- Dependencies are closed:
  - "Add botster-hub apps CLI with open/list/show for web and terminal clients"
  - "Render hub-provided apps and open actions in botster-web"
  - "Render hub-provided apps and launch diagnostics in botster-tui"
- Prior pipeline context had no artifacts, findings, reviews, questions, or answers.
- Required playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]].
- Required Botster overlay/vault context loaded: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], and [[test script required for rust tests not cargo test]].
- Checklist context loaded with `project_pipelines_checklist_instructions`. Creating the run vault checklist initially returned `plugin worker invoke timeout`, but the checklist persisted as `checklist_1782408214_346084`.
- Repo context inspected: `README.md`, `src/main.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_runtime_test.rs`, `docs/plans/expose-installed-app-registry-and-structured-app-launch-dtos.md`, `docs/plans/document-and-test-standalone-botster-tui-production runtime-flow-from-hub.md`, and related production runtime/app plan artifacts.
- Current implementation shape:
  - the removed legacy launcher can install/enable `botster-web` from a local path and optionally install/enable `botster-tui` from `--tui-package-path`.
  - Production runtime output prints `web=http://127.0.0.1:<port>/?legacy=removed`, `bridge=...`, `tui=botster-hub apps open --data-dir <dir> botster-tui`, MCP/status/shutdown commands, and package state rows.
  - `apps list/show/open` exist and terminal apps are launched through daemon-resolved `ResolveAppLaunch` contracts.
  - Existing tests cover structured web URL, generated data dir, explicit data-dir rerun idempotency, production runtime TUI package enablement plus `apps open botster-tui`, app registry DTOs, structured web open URL, terminal open, and deprecated `botster-hub tui` delegation.

## Scope

- Add or tighten final docs/tests that prove the combined local client app plugin developer experience from a stable local data dir:
  - install `botster-web` and `botster-tui` from local package paths;
  - list installed apps from the stable production runtime data dir;
  - open `botster-web` and assert the structured browser URL is obvious and usable;
  - launch/verify `botster-tui` through the `terminal_app` / `apps open` path, using the existing controlled headless fixture if a real TUI binary is not available in this repo;
  - confirm `botster-hub tui` delegates through the package-based `botster-tui` app path;
  - preserve stable data-dir package state across production runtime reruns;
  - keep production runtime output path-neutral and free of local package source paths.
- Update `README.md` only where the fresh-checkout command sequence or expected production runtime output is still ambiguous for first-party client package DX.
- Prefer extending existing daemon lifecycle tests and fixtures over adding a new harness.

## Non-Scope

- No new daemon protocol surfaces, app DTO fields, package manifest vocabulary, package registry abstractions, or client launch modes.
- No changes to standalone `botster-web` or `botster-tui` repositories.
- No browser SPA, TUI UI, Rails relay, Project Pipelines plugin, or MCP behavior changes beyond docs that name the existing path.
- No broad cleanup of production runtime startup, package lifecycle, entrypoint supervision, or CLI formatting outside the proof path.
- No hidden fallback from package-based TUI launch to embedded TUI behavior; the current ticket names `terminal_app` / package-based TUI path.

## Assumptions And Unknowns

- Assumption: "local client app plugin DX" means the first-party hub package/app path, not a new client protocol. The dependencies that introduced apps CLI, botster-web app rendering, and botster-tui diagnostics are closed.
- Assumption: `botster-tui` should be proven as a local package declaring a `terminal_app` `foreground_stdio` entrypoint. A controlled headless fixture is acceptable for hub-owned proof because the ticket explicitly allows a controlled headless substitute.
- Assumption: `botster-hub tui` should remain a deprecated compatibility alias that delegates to `apps open botster-tui`, not a standalone binary launcher or embedded renderer.
- Assumption: the production runtime path to prove is the removed legacy launcher -> local package install/enable -> daemon `ListApps` / `ResolveAppLaunch` / `StartPackageEntrypoint` -> CLI `apps open`.
- Unknown: whether Review will require a live downstream `botster-tui` binary smoke. If unavailable in this repo/worktree, implementation should document that the hub-owned proof uses the package fixture and cite downstream conformance for full UI behavior rather than fabricating a live TUI run.
- Unknown: whether README should document the full two-checkout command sequence with placeholder paths or point to an existing package fixture command. Prefer exact placeholder commands that work from fresh main checkouts once sibling client checkouts are available.

## Botster Layers Touched

- Rust hub CLI/production runtime launcher.
- Rust daemon package/app launch path and app registry reads, through existing APIs.
- Rust integration tests using the real `botster-hub` binary, daemon socket, local package fixtures, and controlled terminal app execution.
- Operator documentation for local first-party client production runtime.

No Lua plugin runtime policy, Project Pipelines workflow policy, React SPA implementation, Rails relay, or new MCP surface is expected.

## Affected Surfaces And Files

- `README.md`
  - Tighten the local production runtime operator section so a fresh-checkout operator can run the web/TUI local package proof with stable `--data-dir`.
  - Make expected output lines obvious: `web=.../?legacy=removed`, `tui=botster-hub apps open --data-dir <dir> botster-tui`, and optional `apps list/show/open` commands.
- `src/main.rs`
  - Only touch if implementation discovers production runtime output omits necessary app-list/open guidance, or if `botster-hub tui` no longer visibly delegates through `apps open`.
  - Preserve thin CLI behavior; terminal launch contracts must continue coming from daemon `ResolveAppLaunch`.
- `tests/hub_daemon_lifecycle_test.rs`
  - Primary place to extend end-to-end proof. Likely extend `removed_legacy_launcher_launcher_enables_local_tui_package_for_apps_open` or add a focused neighboring test to:
    - run `production runtime` with both `--web-package-path` and `--tui-package-path`;
    - assert `apps list --data-dir <dir>` shows `botster-web` and `botster-tui`;
    - assert `apps show` shows the web app structured `local_url` after open/start and terminal app launch semantics without fake URL;
    - assert `apps open botster-tui` prints the fixture marker;
    - assert `botster-hub tui --data-dir <dir>` reaches the same package-based fixture path;
    - assert no local package source paths appear in production runtime/CLI output.
- `tests/hub_local_runtime_test.rs`
  - Keep as lower-level lifecycle regression proof; touch only if shared fixture behavior changes.
- `docs/client-protocol.md`
  - Optional only if README wording reveals a client-boundary gap. Preserve the rule that clients use `botster-hub-client`/daemon DTOs rather than hub internals.
- `docs/plans/prove-local-client-app-plugin-dx-end-to-end.md`
  - This plan artifact.

## Risks

- Under-proving risk: existing tests prove individual pieces, but the ticket wants the combined operator DX. At least one test or documented command sequence should compose both local client packages from one stable data dir.
- Runtime path risk: asserting only helper code or DTO structs would miss the production path. Acceptance needs real `botster-hub` binary commands against a running daemon.
- Fixture realism risk: a headless TUI fixture can prove hub launch wiring but not downstream UI behavior. Keep claims scoped to hub-owned package launch unless a real downstream binary is available.
- Ambiguous TUI precedent risk: older plans mention standalone `botster-tui --data-dir`; this ticket explicitly asks for the `terminal_app` / package-based path, so implementation should not revive standalone-only wording.
- PII/path leakage risk: production runtime output, test assertions, README examples, and plan/report artifacts must not expose local package roots, home paths, or run-specific worktree paths.
- Flake risk: real daemon/socket tests use ports and Unix sockets. Keep tests serialized with existing daemon locks, generated short tmp dirs where needed, and bounded readiness polling.

## Acceptance Checks And Tests

- Focused end-to-end production runtime proof:
  - `./test.sh --test hub_daemon_lifecycle_test removed_legacy_launcher_launcher_enables_local_tui_package_for_apps_open`
  - Should prove production runtime with stable explicit data dir, local `botster-web` and `botster-tui` package paths, structured `web=` URL, package enablement, `apps open botster-tui`, and no path leakage.
- Add or extend focused app listing/open proof if not already covered by the prior command:
  - `./test.sh --test hub_daemon_lifecycle_test <focused client app plugin dx test>`
  - Must prove `apps list/show/open` against the same production runtime data dir after installing both local client packages.
- Existing web production runtime proof:
  - `./test.sh --test hub_daemon_lifecycle_test removed_legacy_launcher_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`
  - Must continue proving verified HTML shell, structured `web=` URL, daemon/package status, clean shutdown, and path-neutral output.
- Stable data-dir persistence proof:
  - `./test.sh --test hub_daemon_lifecycle_test removed_legacy_launcher_launcher_reruns_against_existing_explicit_data_dir`
  - Should continue proving explicit data-dir reruns preserve installed package state and print usable web URLs.
- App registry/launch contract regressions:
  - `./test.sh --test hub_daemon_lifecycle_test cli_apps_list_show_and_open_web_use_structured_app_url`
  - `./test.sh --test hub_daemon_lifecycle_test cli_apps_open_terminal_and_tui_alias_use_foreground_launch_contract`
  - `./test.sh --test hub_daemon_lifecycle_test daemon_resolves_terminal_app_foreground_launch_contract`
- Lower-level lifecycle guard:
  - `./test.sh --test hub_local_runtime_test local_runtime_runs_daemon_package_lifecycle_session_and_clean_shutdown`
- Documentation/path scan:
  - `rg -n "production runtime|apps list|apps show|apps open|botster-tui|web=|tui=|BOTSTER_HUB_SOCKET|--tui-package-path" README.md docs/client-protocol.md`
  - Scan changed files for `/Users/`, run/worktree paths, local package fixture roots, socket paths, and secrets before Review.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/prove-local-client-app-plugin-dx-end-to-end.md`.
- Gate evidence should include the context loaded, scope/non-scope, assumptions/unknowns, affected surfaces/files, risks, acceptance checks, and vault gaps from this plan.
- Checklist evidence should record:
  - vault/project notes loaded;
  - convention conflicts: none;
  - Plan-stage verification evidence: pipeline context and repo/vault inspection commands, plus this plan artifact;
  - durable knowledge capture: defer until implementation proves whether the composed DX path needs a new reusable convention.
- Plan Review should send implementation back if it adds new protocol surfaces, proves only code existence without the real production runtime/CLI runtime path, leaks local paths, or documents TUI behavior beyond what the hub-owned fixture/downstream evidence proves.

## Worktree And Target Assumptions

- Work happens in the pipeline-assigned worktree for `ticket_1782361547_201925`.
- Run target is `tgt_7e208a0c76a44980a83b63af976b1f22`.
- No helper agents are needed for Plan. If later stages spawn agents, prompts must include explicit target id and assigned worktree.

## Vault Gaps Worth Capturing

- Capture after implementation if the composed first-party client production runtime command sequence becomes the durable convention: stable explicit data dir, local `botster-web` and `botster-tui` package paths, app list/show/open proof, and path-neutral production runtime output.
- Capture if package-based `botster-hub tui` delegation becomes the settled compatibility rule for hub-side TUI proof.
- Capture if the boundary between hub-owned headless TUI package launch proof and downstream `botster-tui` full UI conformance needs a sharper note.
- No new vault note is required from Plan alone.

## Convention Check

- No convention conflicts found.
- The plan follows loaded Botster constraints: hub CLI remains thin, package/app state flows through daemon DTOs, terminal app launch uses daemon-resolved foreground contracts, production runtime uses explicit data dirs and verified runtime output, Project Pipelines remains plugin-owned, and repo-visible artifacts cite vault notes by wikilink rather than local filesystem paths.
