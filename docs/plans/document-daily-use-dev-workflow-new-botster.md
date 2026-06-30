# Document Daily-Use Dev Workflow For New Botster

## Context Loaded

- Project Pipelines current context loaded for ticket `ticket_1782761743_997138`, run `run_1782790423_797589`, run step `run_step_1782790423_263515`, step `botster_plan`, gate `botster_plan_gate`.
- Ticket: document the intended daily local workflow for the new multi-repo Botster stack: required repos, build prerequisites, persistent data dir, first-party local package install/update/reload, opening web and TUI clients, workspace/session-template conventions, running Project Pipelines, and the dev acceptance smoke. Docs must distinguish dev workflow from dogfood/bootstrap tests and must not imply embedded hub TUI or old monolith behavior. Include troubleshooting for stale package build output, missing entrypoints, missing provider config, session-template spawn failure, and terminal attach/scrollback issues. No PII.
- Current pipeline context has no prior artifacts, findings, reviews, open questions, or prior answers.
- Closed dependency loaded from context: `ticket_1782761743_112870` / "Add real dev-stack acceptance smoke for first-party plugins".
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster/vault context loaded: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster hub daemon startup requires explicit data dir]], [[botster plugins reload through mcp not file watching]], and [[botster runnable entrypoints are hub owned launch contracts]].
- Project Pipelines checklist discipline loaded with `project_pipelines_checklist_instructions`; run checklist `checklist_1782790459_598067` was created and updated for context loading.
- Repo context inspected: `README.md`, `examples/project-pipelines/README.md`, `examples/project-pipelines/botster-package.json`, `examples/project-pipelines/plugin.lua`, `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, `docs/reports/add-persistent-dev-stack-bootstrap-implement-report.md`, `docs/plans/add-real-dev-stack-acceptance-smoke-first-party-plugins.md`, `docs/plans/add-local-package-reload-and-update-dx-for-dev-mode-packages.md`, `docs/plans/document-and-test-standalone-botster-tui-dogfood-flow-from-hub.md`, `docs/plans/add-hub-owned-session-templates-and-botster-context-injection.md`, and relevant `src/main.rs` / test references.

## Scope

- Update the repo-visible daily-use documentation, primarily `README.md`, so a new Botster developer can run the new multi-repo stack without reading scattered historical plans.
- Reframe the existing "Local dogfood operator CLI" area into a clearer separation:
  - daily persistent dev stack;
  - lower-level daemon/package/session commands;
  - dogfood/bootstrap/test launchers and when to use them.
- Document required local repos and package paths:
  - `botster-hub` as this repo;
  - checked-in `examples/project-pipelines`;
  - sibling or explicitly provided `botster-web`, `botster-tui`, and `botster-workspaces` package roots.
- Document build/runtime prerequisites that are directly implied by the current code and reports:
  - build `botster-hub` / use `cargo run --`;
  - provide or build a co-located `botster-session-worker`, or pass `--session-worker-bin`;
  - ensure first-party package entrypoints exist in their package roots before expecting app launches to work.
- Make the persistent data-dir contract explicit:
  - daily path defaults to `target/botster-hub-dev-stack-data`;
  - it persists `hub-state.json`, package registry state, plugin data, and Project Pipelines state;
  - use the same `--data-dir` for `dev-stack bootstrap`, `apps`, `mcp-serve`, `status`, package reload, and shutdown.
- Document the daily workflow commands:
  - `cargo run -- dev-stack bootstrap ...`;
  - use printed `web=`, `tui=`, `mcp=`, `status=`, `apps=`, and `shutdown=` lines;
  - `botster-hub apps open --data-dir <dir> botster-web/web-client`;
  - `botster-hub apps open --data-dir <dir> botster-tui`;
  - `botster-hub packages reload --data-dir <dir> <package-name>` after local package edits.
- Document Project Pipelines local workflow:
  - use `botster-hub mcp-serve --data-dir <dir>`;
  - create/list/start/current-context/gate/advance tool family lives in the loaded local plugin;
  - `project_pipelines.start` requires explicit `target_id` and assigned worktree;
  - current implementation spawns the package `agent-step` session template and records `session_uuid`, `session_template_id`, `session_context_id`, and `session_lifecycle`.
- Fix stale README wording that currently says `session_uuid` is intentionally absent from the constrained local Project Pipelines flow.
- Document workspace/session-template conventions from the current hub contract:
  - session templates are hub-owned PTY launch contracts, not core plugin entrypoints;
  - Project Pipelines uses `project-pipelines/agent-step`;
  - prompts and workflow calls must carry explicit target id and worktree, not ambient cwd.
- Add a troubleshooting subsection for the ticket's named real failures:
  - stale package build output;
  - missing package app or Lua entrypoints;
  - missing provider/config/auth for optional features;
  - session-template spawn failure;
  - terminal attach, late scrollback, and scrollback payload expectations.
- Add or update a short acceptance-smoke section that points to the real dev-stack smoke and focused commands, without making the smoke the daily workflow.

## Non-Scope

- No Rust, Lua, daemon protocol, package lifecycle, session-template, TUI, SPA, or MCP behavior changes are planned.
- No new CLI commands, package metadata fields, troubleshooting diagnostics, or configuration knobs.
- No edits to out-of-tree sibling repos such as `botster-web`, `botster-tui`, or `botster-workspaces`.
- No migration/import guidance for old monolith Project Pipelines data beyond the existing cutover warning.
- No attempt to document cloud/provider/GitHub PR lifecycle as complete; optional provider/config prerequisites should be named as not daily-local-ready unless configured.
- No replacement of the existing focused design docs and plans. The README should be the operator entrypoint and link or summarize, not duplicate all protocol details.

## Assumptions And Unknowns

- Assumption: this is a documentation-only ticket. The current code already contains `dev-stack bootstrap`, local package reload, app open, session templates, Project Pipelines MCP tools, and the first-party dev-stack smoke.
- Assumption: the canonical daily data dir should remain `target/botster-hub-dev-stack-data` because `src/main.rs` and README already document that default.
- Assumption: package path examples may remain relative (`../botster-web`, `../botster-tui`, `../botster-workspaces`) because they avoid PII and match current discovery behavior.
- Assumption: `botster-hub apps open --data-dir <dir> botster-tui` is the correct TUI path; docs must not mention an embedded hub TUI or `botster-hub tui` as daily use.
- Assumption: "new Botster" means the hub/plugin/client/package stack in this repo, not the old TryBotster monolith.
- Unknown: whether `botster-workspaces` currently has a fully user-facing workflow beyond package enablement. Plan should document it as a first-party local package included in bootstrap and avoid claiming unsupported workspace UI behavior.
- Unknown: whether sibling repo prerequisites need exact binary build commands. If the README cannot verify downstream commands from this repo, document the generic requirement that package manifests and entrypoints exist, and point to the downstream repos for their own build commands.
- Unknown: the exact final name of the dev-stack acceptance smoke after the closed dependency. Implementation should inspect current tests and cite the exact test names that exist.
- Worktree/target assumption: downstream agents must work in this assigned pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`; no ambient checkout paths should appear in docs or gate evidence.

## Botster Layers Touched

- Docs: primary layer.
- Rust hub CLI surface: described, not changed.
- Package lifecycle and app registry: described, not changed.
- Lua Project Pipelines plugin and MCP surface: described, not changed.
- Session-template/PTTY client path: described, not changed.
- TUI and web clients: described as first-party clients launched through package app descriptors, not as hub internals.

## Affected Surfaces And Files

- `README.md`
  - Primary implementation target. Add/restructure daily workflow, dev-stack, package reload, client launch, Project Pipelines, troubleshooting, and acceptance-smoke sections.
  - Fix stale Project Pipelines `session_uuid` statement.
  - Keep normal examples path-neutral and PII-free.
- `examples/project-pipelines/README.md`
  - Optional narrow edit only if needed to align wording with the README daily workflow and current session-template spawn behavior. It already states the correct session-template spawn fields.
- `docs/client-protocol.md`
  - Optional only if implementation finds a direct contradiction in terminal attach/scrollback wording. Prefer linking/summarizing from README rather than expanding protocol docs.
- `docs/lua-plugin-abi.md`
  - Optional only if implementation finds the Project Pipelines/session-template troubleshooting language belongs beside the ABI. Current ABI already describes `session_templates.spawn` correctly.
- No code files should change unless docs reveal a CLI usage string or test name is stale in code-generated help. If that happens, stop and treat it as implementation scope drift.

## Risks

- Stale-doc risk: README currently mixes old dogfood, daily dev-stack, and Project Pipelines readiness language. Implementation must reconcile against current code and tests, not only rearrange old paragraphs.
- Overclaim risk: docs could imply `botster-workspaces`, provider/GitHub automation, or monolith import is complete. Keep those as package/config/cutover caveats unless current code proves them.
- Embedded-TUI regression risk: any daily path mentioning `botster-hub tui` or an embedded renderer contradicts the ticket. Daily TUI launch must go through `apps open botster-tui`.
- Runtime-proof risk: documentation-only changes still need verification against actual command names, usage text, package manifest fields, and current tests.
- PII/path leakage risk: avoid local home paths, agent session worktree paths, and personal checkout paths in README examples and plan/gate evidence.
- Troubleshooting usefulness risk: generic troubleshooting would not satisfy the ticket. The section should map each named failure to a specific command or check a developer can run.
- Historical-plan drift risk: prior plan docs are useful context but not authoritative if current code has advanced. Implementation should cite current files (`src/main.rs`, plugin README/manifest, tests) when resolving conflicts.

## Acceptance Checks / Tests

- Documentation content checks:
  - README includes daily dev-stack workflow, required repos/package paths, build/session-worker prerequisite, persistent data-dir contract, package reload, web/TUI app open commands, Project Pipelines MCP/start workflow, workspace/session-template conventions, dogfood-vs-dev distinction, and troubleshooting for all named failures.
  - README does not imply embedded hub TUI, old monolith Project Pipelines behavior, imported monolith state, or file-watcher hot reload.
  - README Project Pipelines wording matches current plugin behavior: start uses explicit `target_id` and worktree and records spawned session-template fields.
- Static verification:
  - `rg -n "session_uuid is intentionally absent|embedded hub TUI|old monolith|file-watcher|file watcher|botster-hub tui" README.md examples/project-pipelines/README.md docs/client-protocol.md docs/lua-plugin-abi.md`
  - Expected result: no stale claims in changed docs; historical/cutover mentions only where explicitly labeled unsupported.
  - `rg -n "dev-stack bootstrap|packages reload|apps open --data-dir|botster-tui|mcp-serve|session_templates|project_pipelines.start|target_id|worktree|scrollback" README.md`
  - Expected result: daily workflow and troubleshooting paths are discoverable from README.
  - `rg -n "[/]Users/|[/]home/|botster[-]sessions|sess-[0-9]|run_[0-9]" README.md docs/plans/document-daily-use-dev-workflow-new-botster.md examples/project-pipelines/README.md`
  - Expected result: no PII/run-specific path leaks in changed docs, except this plan's generic run id if reviewers accept pipeline ids in plan artifacts. If not, remove run ids before implementation handoff.
- Repo verification:
  - `git diff --check`
  - If only Markdown changes: no Rust test is required, but implementation should identify the exact existing acceptance smoke from `tests/hub_daemon_lifecycle_test.rs`.
  - If any code or command help changes unexpectedly: run the focused affected test with `./test.sh --test hub_daemon_lifecycle_test <test_name> -- --test-threads=1`.
- Runtime/user-path proof expected in the implementation report:
  - Show the README command sequence maps to actual CLI branches in `src/main.rs`: `dev-stack bootstrap`, `packages reload`, `apps open`, `mcp-serve`, session-template commands, and shutdown.
  - Show Project Pipelines docs map to `examples/project-pipelines/plugin.lua` and `botster-package.json`, not stale monolith behavior.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Plan gate evidence should attach this plan and checklist evidence.
- Implement should report exact changed docs, stale statements removed, command names verified, and any deviations from docs-only scope.
- Review should reject:
  - broad code changes;
  - documentation that claims unimplemented behavior;
  - daily workflow text that points to dogfood/bootstrap tests as the normal path;
  - mentions of embedded hub TUI or old monolith behavior as supported.
- Verify should run the static doc checks and confirm all named ticket bullets are visible in README.

## Convention Conflicts

None found. The plan follows the loaded Botster conventions: product workflow policy remains plugin-owned, hub owns daemon/package/session-template policy, clients consume daemon app descriptors, daily docs live in the repo rather than only in the vault, plugin reload is explicit through MCP/package reload rather than file watching, and pipeline artifacts cite vault context by note title instead of absolute vault paths.

## Vault Gaps Worth Capturing

- Capture after implementation if the README establishes `dev-stack bootstrap` as the durable canonical daily workflow label across Botster repos.
- Capture after implementation if the troubleshooting section settles a reusable convention for documenting stale local package build output versus package manifest reload.
- Capture after implementation if the docs define a stable boundary for what Project Pipelines local mode can claim about spawned session templates versus future agent supervision.
- No new vault note is needed at Plan time for explicit data dirs, Project Pipelines target/worktree binding, plugin reload, or standalone TUI boundaries; existing notes already cover those constraints.
