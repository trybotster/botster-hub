# Document And Test Standalone Botster TUI Dogfood Flow From Hub Plan

## Context Loaded

- Pipeline context: ticket `ticket_1782338845_233338`, run `run_1782353175_421942`, step `botster_plan`, gate `botster_plan_gate`.
- Dependency context: closed dependency `ticket_1782338844_576329`, "Replace botster-hub tui implementation with standalone botster-tui launcher or deprecation path".
- Prior pipeline context: no prior artifacts, questions, answers, reviews, or findings were present for this run.
- Required vault context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster hub daemon startup requires explicit data dir]], [[botster session worker requires explicit build in dogfood launchers]], [[botster review and verify must scan all committed artifacts for pii]], [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected: `src/main.rs`, `README.md`, `docs/client-protocol.md`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_dogfood_test.rs`, and predecessor plan `docs/plans/replace-hub-tui-with-standalone-launcher-or-deprecation.md`.
- Checklist context: `project_pipelines_checklist_instructions` was loaded. Creating the standard vault checklist timed out with `plugin worker invoke timeout`, so checklist evidence is preserved in this plan artifact and gate evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Finalize the hub-side documentation and tests for the standalone `botster-tui` dogfood path.
- Preserve the replacement direction already visible in code: `botster-hub dogfood` prints `tui=botster-tui --data-dir <path>`, and `botster-hub tui --data-dir <path>` is only a deprecated compatibility command that prints the standalone equivalent.
- Tighten tests so they prove the printed standalone command is valid for the dogfood data directory and does not depend on the old embedded renderer.
- Tighten README and protocol docs only where wording still leaves ambiguity about data-dir/socket usage, lifecycle ownership, or the supported TUI boundary.
- Keep all outputs path-neutral and free of local package source paths or home-directory paths.

## Non-Scope

- Do not add new TUI features or change standalone `botster-tui` behavior.
- Do not reintroduce embedded hub TUI rendering, `ratatui`, `crossterm`, scripted TUI drivers, or a hub-owned renderer compatibility layer.
- Do not change daemon socket protocol fields, `botster-hub-client` compatibility descriptors, session-worker protocol, package lifecycle policy, or Project Pipelines plugin policy unless a focused test exposes a direct bug in the documented path.
- Do not add a package installer, binary resolver, PATH probing abstraction, or optional configurability for `botster-tui`.

## Assumptions And Unknowns

- Assumption: the ticket is a follow-on to the closed replacement dependency, so the intended change is final dogfood docs/tests around the replacement path, not another architecture change.
- Assumption: the accepted invocation for the standalone client is `botster-tui --data-dir <path>`, matching current README, dogfood output, `botster-hub tui` deprecation output, and `docs/client-protocol.md`.
- Assumption: `botster-tui` resolves the daemon socket from the same explicit hub data directory through the public `botster-hub-client` protocol, while `botster-web` receives `BOTSTER_HUB_SOCKET` because it is launched as a supervised existing-hub package entrypoint.
- Assumption: the current pipeline-assigned target/worktree is authoritative; no additional spawned agent is needed for Plan.
- Unknown: whether downstream `botster-tui` currently supports every lifecycle claim documented in README, such as reconnect/session-lost behavior. If this hub repo cannot prove a claim directly, implementation should either cite downstream conformance docs or narrow the README wording to what the hub dogfood path proves.
- Unknown: whether Review will require a live standalone `botster-tui` binary smoke check. If the binary is not available in this repo/worktree, hub acceptance should be limited to printed command validity, deprecation output, daemon lifecycle, and public client-protocol conformance references rather than fabricating a TUI run.

## Affected Surfaces And Files

- `src/main.rs`
  - `print_dogfood_ready`: primary dogfood output must keep printing `tui=botster-tui --data-dir <dir>`.
  - `operator_tui`: compatibility command must keep printing the standalone equivalent and must not start or embed a renderer.
  - usage text: `botster-hub tui` help should remain explicit that it prints the standalone command.
- `tests/hub_daemon_lifecycle_test.rs`
  - Strengthen `cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down` if needed so it asserts the full printed TUI command includes the exact dogfood data dir, not only the prefix.
  - Preserve `cli_tui_prints_standalone_command_and_exits_successfully`.
  - Preserve dogfood checks that prove hub/web/MCP/status/shutdown next steps, existing-hub `BOTSTER_HUB_SOCKET` bridge mode, daemon status, package lifecycle, and clean shutdown.
- `README.md`
  - Keep the dogfood section explicit about `--data-dir`, `BOTSTER_HUB_SOCKET`, foreground launcher ownership, and the standalone local TUI workflow.
  - Narrow any standalone TUI behavior claims that cannot be backed by this hub repo or downstream conformance docs.
- `docs/client-protocol.md`
  - Preserve the rule that downstream clients, including `botster-tui`, use `botster-hub-client` and not hub internals, embedded TUI code, or session-worker frames.
  - Preserve downstream test guidance through `botster-hub-test-support`.
- Optional only if gaps are found: a focused doc assertion or CLI output test helper near existing dogfood tests.

## Botster Layers Touched

- Rust hub CLI and daemon dogfood launcher.
- Public same-device client boundary documentation.
- Rust integration tests using the real `botster-hub` binary, daemon socket, supervised `botster-web` package fixture, and daemon shutdown path.
- Docs for first-party local dogfood operators.

## Risks

- Weak assertion risk: a prefix-only assertion for `tui=botster-tui --data-dir` could pass while the command omits or corrupts the actual data directory.
- Runtime proof gap: docs could claim a working standalone TUI lifecycle that this repo cannot execute. Keep hub tests to hub-owned command output and daemon/client protocol guarantees unless a real `botster-tui` binary is available.
- Regression to old behavior: any implementation that re-adds or references embedded renderer paths would contradict the closed dependency and the ticket's "does not rely on old embedded renderer behavior" acceptance.
- Socket/data-dir confusion: docs must distinguish the supervised web package's `BOTSTER_HUB_SOCKET` environment from the standalone TUI's `--data-dir` operator command.
- PII leakage: dogfood output, docs, and plan artifacts must avoid absolute local package paths and home-directory paths.
- Flake risk: real daemon/socket tests must stay serialized and should use short generated data dirs where Unix socket pathname length matters.

## Acceptance Checks And Tests

- Focused dogfood launcher test:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`
  - Must prove `dogfood=ready`, `botster-web` existing-hub socket mode, HTML shell readiness, daemon status/package lifecycle, clean shutdown, and a printed `tui=botster-tui --data-dir <exact data dir>` command.
- Deprecated hub TUI command test:
  - `./test.sh --test hub_daemon_lifecycle_test cli_tui_prints_standalone_command_and_exits_successfully`
  - Must prove `botster-hub tui --data-dir <dir>` exits successfully and prints only the standalone `botster-tui --data-dir <dir>` equivalent.
- Lower-level dogfood lifecycle regression:
  - `./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown`
  - Keeps daemon package/session lifecycle proof green.
- Replacement-path guard:
  - `rg -n "run_tui|ScriptedTui|run_scripted_probe|ratatui|crossterm|pub mod tui|botster_hub::tui" src tests Cargo.toml`
  - Should show no supported embedded renderer path. Historical docs/plans may still mention the removed path.
- Documentation checks:
  - `rg -n "botster-hub tui|botster-tui|BOTSTER_HUB_SOCKET|data-dir|embedded TUI|standalone TUI" README.md docs/client-protocol.md`
  - Confirm README/protocol wording matches hub-owned proof and downstream client boundary.
- PII/path scan:
  - Scan changed files for home-directory paths, local package source paths, and run-specific worktree paths before Review.
- Broader verification, if time allows:
  - `./test.sh --test hub_daemon_lifecycle_test`
  - Broader `./test.sh` only if focused changes touch shared daemon/package behavior.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Gate evidence should include the loaded context above, checklist timeout fallback, no convention conflicts, and the acceptance checks/tests listed here.
- Plan Review should send implementation back if it only edits docs without proving the production dogfood output path, or if it reintroduces embedded renderer behavior.

## Vault Gaps Worth Capturing

- Capture after implementation if this ticket settles a durable convention for follow-on dogfood tickets: printed first-party client commands must assert the exact data directory/socket binding, not only command prefixes.
- Capture if downstream `botster-tui` lifecycle claims need a clearer contract between hub-owned dogfood proof and downstream client conformance proof.
- No new durable vault note is required from Plan alone; the checklist timeout recurrence is already covered by [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Convention Check

- No convention conflicts found.
- The plan follows the loaded Botster constraints: hub CLI stays thin, standalone clients use `botster-hub-client`, dogfood uses explicit data dirs and explicit worker materialization, Project Pipelines policy remains plugin-owned, and repo-visible artifacts cite vault notes by wikilink rather than local filesystem paths.
