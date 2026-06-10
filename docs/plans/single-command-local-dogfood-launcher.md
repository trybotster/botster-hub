---
description: Plan for adding a single-command local dogfood launcher to botster-hub
---

# Single-command local dogfood launcher

## Context loaded

- Project Pipelines context: ticket `ticket_1781054950_659298`, run `run_1781061909_132637`, step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, questions, or answers.
- Dependency context: "Implement local path package install enable disable remove flow" is closed and present in this worktree. The launcher should use the existing daemon-owned package registry path, not reimplement package setup.
- Playbooks and vault notes: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], plus `identity` and `goals`.
- Repo context inspected: `src/main.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/packages.rs`, `src/tui.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_dogfood_test.rs`, `crates/botster-hub-test-support/src/lib.rs`, `README.md`, `examples/project-pipelines/README.md`, `test.sh`, and prior `docs/plans/*dogfood*` / package lifecycle plans.
- Existing runtime shape: `botster-hub start --data-dir` owns the daemon lifecycle; package commands route through daemon transport; `examples/project-pipelines` is the first-party local package fixture; TUI and MCP are separate client entrypoints over the same data dir.

## Scope

- Add a thin `botster-hub dogfood` command in `src/main.rs`.
- Default to an isolated disposable data directory under `target/` or the OS temp directory, and accept an explicit `--data-dir <path>` override. The default must not read or mutate HOME/XDG Botster identity or state.
- Resolve the current `botster-hub` executable and `botster-session-worker` binary in the same style as existing integration helpers:
  - use explicit `--hub-bin <path>` / `--session-worker-bin <path>` only if implementation needs them for tests or manual local builds;
  - otherwise prefer current executable plus existing worker resolution, with actionable errors when missing.
- Start the production daemon path with session-worker support, wait for readiness through the socket protocol, then install/enable configured first-party local package fixtures through the existing package registry daemon request path.
- Use `examples/project-pipelines` as the default first-party dogfood package because it exercises real plugin worker, PluginDb, MCP descriptor, and TUI surface behavior. Keep `examples/synthetic-plugin` as a narrower test fixture only if needed for faster diagnostics.
- Print compact actionable next steps after readiness:
  - data dir label, without leaking an absolute user-home path for default temp dirs;
  - package installed/enabled state;
  - `botster-hub tui --data-dir <dir>`;
  - `botster-hub mcp-serve --data-dir <dir>`;
  - `botster-hub status --data-dir <dir>`;
  - foreground shutdown guidance: press Ctrl-C in the launcher; optionally include `botster-hub shutdown --data-dir <dir>` as "from another terminal" guidance only if the launcher leaves the daemon reachable while foregrounded.
- Reconcile the ticket's web-entrypoint language explicitly in launcher output and docs. This repo currently has no local web command or browser bridge/dev-mode entrypoint; `src/main.rs` exposes `tui` and `mcp-serve`, while README documents WebRTC/browser/Rails surfaces as excluded. The launcher must not fabricate a web command. It should print that local web is unavailable in this repo today and point to TUI/MCP as the current local client entrypoints unless implementation discovers an existing supported web command in the worktree.
- Provide lifecycle handling:
  - foreground mode should keep the launcher process alive while the daemon child runs;
  - Ctrl-C / termination should request daemon shutdown, then kill/wait if the child does not exit;
  - readiness failure must kill and wait for the child before returning;
  - diagnostics should distinguish missing hub binary, missing session-worker binary, readiness timeout, package install/enable failure, and daemon shutdown failure.
- Document the single-command flow in `README.md` and, only if Project Pipelines-specific details change, `examples/project-pipelines/README.md`.

## Non-scope

- No production WebRTC/browser transport, Rails relay, cloud pairing, marketplace fetch, final installer, or provider credential bootstrapping.
- No new package registry policy, package manager abstraction, lockfile redesign, or network package source.
- No direct mutation of `hub-state.json` outside the running daemon owner.
- No broad TUI, MCP, Project Pipelines workflow, PluginDb schema, or SPA changes unless required by the launcher user path.
- No import of live monolith Project Pipelines data.
- No background process manager, launchd/systemd integration, or permanent local identity setup.

## Assumptions and unknowns

- Assumption: the intended command name can be `dogfood`; adding `dev` as an alias is optional only if it falls out trivially from the parser. Prefer one documented command.
- Assumption: foreground mode is sufficient for "one command" because the user can open TUI/MCP in separate terminals from the printed entrypoints. A detached mode is optional and should not be added unless implementation can keep it small and tested.
- Assumption: `examples/project-pipelines` is the configured first-party package fixture for this ticket because the README already frames it as the constrained local coordination dogfood package.
- Assumption: "web/TUI next steps" is satisfied by documenting the current no-local-web state and printing current TUI/MCP entrypoints, because the repository has no supported web command and the ticket forbids starting production WebRTC/browser transport.
- Assumption: if `--data-dir` is explicit, printing that path is acceptable because the user supplied it. Default generated paths should be summarized or labeled to avoid unnecessary PII in output/tests.
- Unknown: whether current worker binary resolution already works from `cargo run -- dogfood` without an explicit `--session-worker-bin`. Implementation must inspect `CoreEngineOptions` resolution before adding flags.
- Unknown: whether adding a `ctrlc` dependency is necessary. Prefer standard-library signal/child cleanup patterns or existing project dependencies first; add a dependency only if the implementation cannot meet cleanup acceptance otherwise.
- No human question blocks planning. If implementation discovers that "one command" must mean opening TUI or browser automatically, ask the human before adding GUI launch behavior.

## Botster layers touched

- Rust hub CLI: new `dogfood` command, argument parsing, output, and error types in `src/main.rs`.
- Rust daemon/socket path: reused through `serve_daemon`, `daemon_transport_request`, and `botster_hub_client` requests; touch only if a narrow reusable readiness/shutdown helper is needed.
- Package registry/lifecycle: reused via existing daemon package install/enable flow for `examples/project-pipelines`.
- Session worker/local runtime: started through existing worker-backed daemon options.
- TUI/MCP: no runtime changes expected; docs should point to their existing entrypoints.
- Docs/tests: README dogfood command section, durable plan artifact, and integration tests.

## Affected surfaces/files

- `src/main.rs`
  - Add top-level `dogfood` dispatch and usage text.
  - Add `DogfoodOptions` parsing for `--data-dir`, maybe `--session-worker-bin`, and maybe `--package-path`.
  - Add a small launcher function that spawns the daemon child, waits for socket readiness, enables Project Pipelines through daemon transport, prints next steps, and owns cleanup.
- `crates/botster-hub-test-support/src/lib.rs`
  - Reuse or extract readiness/cleanup helpers if doing so avoids duplicating fragile child-process handling. Keep the public test-support crate boundary narrow.
- `tests/hub_daemon_lifecycle_test.rs` or a focused new Unix integration test
  - Add the production binary smoke for `botster-hub dogfood`.
- `README.md`
  - Replace or supplement the multi-command Project Pipelines local readiness flow with the one-command launcher plus optional TUI/MCP commands.
- `examples/project-pipelines/README.md`
  - Update only if the launcher changes the package's manual dogfood instructions.
- `docs/plans/single-command-local-dogfood-launcher.md`
  - This plan artifact.

## Risks

- Underwired launcher risk: spawning a child and printing docs without verifying package lifecycle would not satisfy the ticket. The test must assert status and package/lifecycle state through the daemon socket.
- Cleanup risk: failed readiness or Ctrl-C can leave a daemon/session-worker child running. Use a cleanup guard and prove failed readiness cleanup in tests where feasible.
- State isolation risk: defaulting to an implicit config path would mutate real Botster state. Build config and child args from explicit/generated data dirs only.
- Binary resolution risk: `cargo run -- dogfood` may not know the session-worker binary path outside Cargo integration tests. Errors must name the missing binary and the flag/env needed to fix it.
- Package path risk: enabling by local path can leak source paths in output. Success output should report package name/state and entrypoint commands, not raw local package paths.
- Scope creep risk: trying to open browser/TUI automatically turns this into an installer/process manager. Keep launcher output actionable and leave clients as explicit next commands.
- Signal handling risk: robust Ctrl-C in Rust may need a dependency. If a dependency is added, verify latest version before changing `Cargo.toml` and keep it scoped to launcher cleanup.
- Fixture weight risk: `examples/project-pipelines` gives the right first-party plugin surface coverage but is heavier than the `examples/synthetic-plugin` fixture used by the existing library-level `tests/hub_local_dogfood_test.rs`. Prefer Project Pipelines for the CLI smoke; fall back to `synthetic-plugin` only if the richer fixture proves slow or flaky, and document that reduction as a coverage tradeoff.

## Acceptance checks/tests

- Add an automated Unix integration test, for example:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_isolated_hub_enables_project_pipelines_and_shuts_down`
  - It runs the `botster-hub` binary with `dogfood --data-dir <isolated-dir>` and an explicit session-worker path if needed.
  - It waits for launcher readiness output or daemon status.
  - It proves the daemon is reachable through `status`.
  - It proves `project-pipelines` is installed/enabled through `packages show` or daemon package DTOs.
  - It proves plugin lifecycle loaded or Project Pipelines conformance succeeds through the existing daemon/plugin surface path.
  - It requests shutdown or terminates the launcher and asserts the child exits cleanly.
  - It asserts output contains TUI/MCP/status next steps, foreground Ctrl-C shutdown guidance, and an explicit "local web unavailable in this repo" note instead of a fake web command.
  - It asserts output does not contain user-home paths for default generated state.
- The new CLI smoke complements, rather than replaces, `tests/hub_local_dogfood_test.rs`: that existing test proves the library-level `HubDaemon::start` + `HubClientApi` path with `examples/synthetic-plugin`; the new test must prove the actual `botster-hub dogfood` binary entrypoint.
- Add a missing-worker or readiness-timeout test if the implementation adds custom resolution/cleanup code:
  - command exits nonzero;
  - diagnostic identifies the missing binary or readiness timeout;
  - daemon child is killed/waited and socket is not left live.
- Existing required checks should remain green:
  - `./test.sh --test hub_daemon_lifecycle_test cli_packages_local_path_install_enable_disable_remove_flow`
  - `./test.sh --test hub_daemon_lifecycle_test cli_packages_enable_local_path_routes_through_running_daemon_and_persists`
  - Project Pipelines conformance coverage in `hub_daemon_lifecycle_test` / `botster-hub-test-support`.
- Manual acceptance after implementation:
  - `cargo run -- dogfood`
  - open a second terminal with the printed `tui` command;
  - run the printed `mcp-serve` command if an agent client is being tested;
  - press Ctrl-C in the launcher, or run the printed shutdown command from another terminal only if implementation explicitly supports that foreground lifecycle;
  - confirm no real identity/home state was mutated.

## Pipeline gates and artifacts

- Plan gate artifact: this file plus the gate evidence payload for `botster_plan_gate`.
- Implement gate should require committed code and a PR link before review, per the loaded Project Pipelines convention.
- Review/Verify should reject code that only adds helper functions without proving the top-level `botster-hub dogfood` production entrypoint.

## Vault gaps worth capturing

- Capture if implementation settles a durable convention for `botster-hub dogfood` as the local daily launcher command and its default data-dir policy.
- Capture if worker binary resolution for local dogfood becomes a reusable pattern distinct from test-support explicit paths.
- Capture if Ctrl-C cleanup requires a standard signal-handling dependency or a reusable child-process cleanup guard.
- No convention conflict is known at plan time.
