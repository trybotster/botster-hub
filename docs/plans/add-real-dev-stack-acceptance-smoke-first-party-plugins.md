# Add Real Dev-Stack Acceptance Smoke For First-Party Plugins

## Context Loaded

- Project Pipelines current context loaded for ticket `ticket_1782761743_112870`, run `run_1782781522_403241`, run step `run_step_1782781522_483523`, step `botster_plan`, gate `botster_plan_gate`.
- Ticket: add one conclusive hub-side dev acceptance smoke proving the new Botster stack can replace the old one by exercising persistent dev hub, first-party packages, web/TUI app entrypoints, workspace/session-template PTY path, Project Pipelines step path, package reload/update, and clean shutdown.
- No prior artifacts, findings, reviews, questions, answers, or open dependencies were present. All listed dependencies were already closed.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster/vault context loaded: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], and [[test script required for rust tests not cargo test]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` returned the known `plugin worker invoke timeout`, but the run checklist persisted as `checklist_1782781575_280101`; this plan and gate evidence carry the same workflow evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add one hub-side acceptance smoke, preferably in `tests/hub_daemon_lifecycle_test.rs`, that composes the existing real runtime paths instead of creating dogfood-only shims.
- The smoke should:
  - bootstrap or attach to a persistent local dev hub through `botster-hub dev-stack bootstrap`;
  - install/enable local first-party packages from configured paths: `project-pipelines`, `botster-web`, `botster-tui`, and `botster-workspaces`;
  - verify the web app descriptor and `apps open botster-web/web-client` URL against the daemon-resolved app row;
  - verify the TUI app descriptor or launch contract through `apps open botster-tui`, without relying on the removed `botster-hub tui` alias;
  - create or use workspace state through the `botster-workspaces` package path, only as far as current package capabilities make real runtime evidence possible;
  - spawn a real PTY through the hub session-template path, then attach/drain scrollback through the daemon/client protocol;
  - run a minimal Project Pipelines flow through the same first-party hub path, including MCP tool registration/call evidence and `project_pipelines.start` with explicit `target_id` and `worktree`;
  - reload at least one local package using the production `ReloadPackage` / `packages reload` path and prove refreshed daemon state;
  - shut down the dev hub cleanly and assert post-shutdown status.
- Reuse existing test fixture writers and helper APIs where possible. Extract small helpers only when they reduce repetitive daemon request/CLI boilerplate in the new smoke.
- Add README or plugin README wording only if implementation changes a user-visible smoke command or exposes a new documented limitation.

## Non-Scope

- No new Project Pipelines workflow primitives, UI workbench features, gates, review semantics, or plugin state model changes unless a direct runtime gap blocks the smoke.
- No new package lifecycle API, daemon protocol variant, session-template abstraction, or MCP server path unless the existing public paths cannot satisfy the ticket.
- No browser automation or full visual inspection of `botster-web`; this ticket asks for hub-side acceptance. Verifying the daemon-resolved web URL/HTML shell is enough unless implementation discovers the descriptor is unwired.
- No replacement of existing focused tests. Existing tests for package reload, app launch descriptors, client conformance, Project Pipelines conformance, and session-template spawn should remain as focused regression coverage.
- No broad refactor of `src/main.rs`, `HubRuntime`, package policy, or plugin Lua code just to make the smoke cleaner.

## Assumptions And Unknowns

- Assumption: "first-party plugins" means the locally configured first-party package paths named by `dev-stack bootstrap`: `project-pipelines`, `botster-web`, `botster-tui`, and `botster-workspaces`.
- Assumption: "open web and TUI app entrypoints or verify their launch descriptors" permits descriptor/launch-contract verification for TUI and URL/HTML shell verification for web in a headless Rust acceptance test.
- Assumption: the correct proof is one serialized real-daemon integration test guarded by the existing daemon test lock, not a new CLI subcommand.
- Assumption: workspace proof can use current `botster-workspaces` real package state/capabilities without inventing a full workspace product workflow.
- Unknown: whether the checked-in `examples/project-pipelines` start path already drives session-template spawning end-to-end in the dev-stack flow or whether the smoke should add a minimal first-party package fixture with a session template and have Project Pipelines reference that explicit template path.
- Unknown: whether the best package reload target is `botster-web` or a small first-party-style local fixture. Prefer `botster-web` if its generated manifest can be rewritten safely in the test; otherwise use the smallest local package fixture and document why the reload proof is for the same package lifecycle path.

## Botster Layers Touched

- Rust hub daemon and CLI acceptance surface: `dev-stack bootstrap`, package lifecycle, apps, sessions, daemon shutdown.
- Package lifecycle and app descriptor projection for first-party local packages.
- Session/client worker path for real PTY spawn, attach, drain, input, and shutdown.
- Lua plugin/MCP path for Project Pipelines tool registration and calls through `mcp-serve` / daemon plugin provider.
- Plugin-owned workflow policy remains in `examples/project-pipelines`; hub/core should stay generic.
- TUI and web are treated as first-party clients consuming daemon app descriptors, not as privileged runtime owners.

## Affected Surfaces And Files

- `tests/hub_daemon_lifecycle_test.rs`: add the conclusive real dev-stack acceptance smoke and likely compose existing fixture helpers such as dev-stack package writers, bridge URL checks, app open checks, session attach/drain helpers, package reload helpers, and shutdown cleanup.
- `crates/botster-hub-test-support/src/lib.rs`: only if reusable external-client conformance helpers need a narrow extension for Project Pipelines start/session-template evidence. Keep public reports stable and additive.
- `examples/project-pipelines/plugin.lua`: only if the implementation finds `project_pipelines.start` is currently descriptor-only and cannot exercise the required session-template path. Any change must keep Project Pipelines policy plugin-owned.
- `examples/project-pipelines/README.md` or `README.md`: only if the new smoke changes documented dev-stack acceptance behavior or reveals a residual limitation operators need to know.
- `src/main.rs`, `src/runtime.rs`, `src/session_templates.rs`, `src/packages.rs`, `crates/botster-hub-client/src/lib.rs`: should remain untouched unless the smoke exposes an actual production-path wiring bug.

## Runtime Proof Requirements

- The smoke must prove production entrypoints, not code existence:
  - `dev-stack bootstrap` invokes `ensure_dev_stack_daemon`, `EnablePackageLocalPath`, `start_botster_web_dogfood`, and first-party package enablement through the running daemon.
  - app checks use `apps list/show/open` or daemon `ListApps` / `ResolveAppLaunch`, not local manifest parsing.
  - terminal proof uses real `SpawnSessionTemplate` or plugin session-template spawn into the core daemon, followed by `Attach`/`Drain` over the daemon/client path.
  - Project Pipelines proof uses live plugin MCP registration and tool calls through the loaded `examples/project-pipelines` package.
  - reload proof uses `ReloadPackage` or `botster-hub packages reload`, then verifies refreshed daemon-visible package/app state.
  - shutdown proof uses `shutdown --data-dir` or `DaemonShutdown` and verifies the daemon is no longer running.

## Risks

- Flaky long-running integration coverage: the smoke spans daemon startup, package entrypoint supervision, local sockets, Lua worker calls, PTY IO, and shutdown. Keep it serialized with the existing daemon test lock and use bounded readiness waits.
- Hidden dogfood shims: reusing helper fixtures can accidentally prove synthetic packages rather than first-party package paths. Assertions should name first-party package rows and descriptor ids.
- Path leaks: dev-stack output and failure diagnostics must not include local package source paths or personal home paths.
- Unwired implementation risk: Project Pipelines `start` may record workflow state without spawning a session template. The implementation must either wire the required runtime path or document a human question if the ticket would require changing Project Pipelines semantics beyond a smoke.
- Cleanup risk: failed readiness must kill spawned daemons/entrypoints and remove subscriptions so later daemon tests do not inherit stale sockets or processes.

## Acceptance Checks And Tests

- Add and pass a focused acceptance command using the repo wrapper, for example:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_acceptance_smoke_exercises_first_party_plugins_project_pipelines_session_templates_reload_and_shutdown`
- Re-run nearby focused coverage touched by helper changes:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_bootstrap_starts_daemon_enables_first_party_packages_and_prints_apps`
  - `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_bootstrap_reuses_live_daemon_and_preserves_state_after_restart`
  - `./test.sh --test hub_daemon_lifecycle_test local_package_reload_rereads_manifest_restarts_running_app_and_cli_open_uses_refreshed_state`
  - `./test.sh --test hub_lua_runtime_test real_lua_plugin_spawns_session_template_through_worker_capability`
- If `crates/botster-hub-test-support/src/lib.rs` changes, also run the downstream-shaped conformance test that uses it.
- Run `cargo fmt` through the approved repo command if Rust files change.
- Run strict lint verification if the implementation changes shared Rust APIs or public test-support reports; otherwise record why the focused smoke plus existing targeted tests are the bounded proof.
- Manual evidence expected in the implementation report: command outputs summarized with pass/fail, package names/states, app ids, session id, observed scrollback marker, Project Pipelines run id/context evidence, reload version or descriptor change, and shutdown status.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this file and include the loaded context, checklist timeout fallback, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Implement should persist a report artifact summarizing actual changed files, runtime proof path, verification commands, and any deviations from this plan.
- Review should reject code-only evidence that does not show the real production entrypoint path changed or was exercised.
- Verify should rerun the new smoke or inspect exact prior command evidence and confirm cleanup/shutdown behavior.

## Convention Conflicts

None found. The plan follows the loaded Botster constraints: project workflow policy remains plugin-owned, hub owns local daemon/package/session-template runtime policy, clients consume daemon descriptors, tests use real runtime paths, and repo artifacts cite vault context by wikilink/note title rather than local home paths.

## Vault Gaps Worth Capturing

- If implementation confirms that Project Pipelines `start` was not actually coupled to session-template spawning before this ticket, capture the boundary as a new Botster note.
- If the smoke needs a specific reusable first-party package fixture shape for dev-stack acceptance, capture the accepted fixture boundary after implementation.
- No new vault note is needed at Plan time for checklist timeout, plan artifact discipline, or Rust test wrapper usage; existing notes already cover those constraints.
