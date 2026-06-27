---
ticket: ticket_1782498396_570680
title: Add hub-owned session templates and botster context injection
run: run_1782517092_744255
step: botster_plan
---

# Add hub-owned session templates and botster context injection

## Context loaded

- Pipeline context: ticket `ticket_1782498396_570680`, run `run_1782517092_744255`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, open questions, or answers were present.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Self/context notes: [[identity]], [[goals]].
- Botster architecture/vault constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster runnable entrypoints are hub owned launch contracts]], [[device hub owns admitted spawn targets not ambient repo cwd]], and [[manifest required injections must be consumed by the launched runtime]].
- Artifact/checklist discipline: [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Project Pipelines checklist workflow: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` timed out with `plugin worker invoke timeout`, so checklist evidence is preserved in this plan and should also be copied into gate evidence.
- Repo context inspected: `src/main.rs`, `src/client_api.rs`, `src/runtime.rs`, `src/daemon_transport.rs`, `src/config.rs`, `src/persistence.rs`, `src/packages.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_client_api_test.rs`, `tests/hub_runtime_test.rs`, `examples/project-pipelines/botster-package.json`, `examples/synthetic-plugin/botster-package.json`, and prior `docs/plans/*` package/session plans.

## Scope

- Add a hub-owned session-template contract for package, device, and repo templates. The contract should be adjacent to existing package `runnable_entrypoints`; it must not overload core plugin `entrypoints` or create a first-class agent runtime abstraction.
- Add a resolver that materializes a template request into the existing generic core spawn shape: executable, args, cwd, env, metadata, and PTY size. Core should still see only `SessionSpawnRequest` data.
- Add hub-owned context assembly for spawned scripts, including allowed values such as `worktree_path`, `repo_path`, `branch_name`, `prompt`, `session_dir`, `hub_socket`, optional ticket/workspace ids, and sanitized metadata.
- Add a `botster context ...` surface available inside spawned sessions. Prefer a narrow CLI/daemon command that reads context by session/template request identity from hub-owned state, rather than logging or embedding all context in command strings.
- Add package/device/repo override resolution with documented precedence:
  1. package-provided template defaults;
  2. device-level hub defaults;
  3. repo-local additions/overrides for an admitted spawn target;
  4. explicit spawn request values allowed by policy.
- Add a minimal hub-owned admitted spawn-target policy if this compact hub still lacks one at implementation time. It should record trusted target id, repo/worktree root, and allowed repo-local template config location; it should not introduce workspace, ticket, agent, Codex, or Claude semantics.
- Add CLI/API commands to list, show, resolve, and spawn session templates through the running daemon. Preserve the existing template-free `sessions spawn -- <command>` path.
- Add a fixture equivalent to Codex/Claude-style initialization scripts that proves template launch and `botster context` through a real PTY session path.
- Document the template manifest shape, override precedence, trusted context values, PII redaction expectations, and the deliberate core boundary.

## Non-scope

- No first-class `AgentRuntime`, Codex/Claude-specific enum, accessory/workspace/ticket semantics in core, or package-owned spawn execution mechanics.
- No replacement of existing plain shell spawn.
- No broad package-system refactor beyond adding template declarations and resolution hooks needed by this ticket.
- No Project Pipelines workflow policy changes beyond allowing a later generic template id request.
- No React/TUI UI work unless needed to expose existing daemon command results in tests.
- No secrets or host-resolved environment snapshots in template manifests, context output, logs, artifacts, or committed fixtures.

## Assumptions and unknowns

- Assumption: existing `runnable_entrypoints` remain local app launch contracts; session templates are a separate hub-owned contract because they produce PTY sessions and trusted per-session context.
- Assumption: package templates can be parsed from `botster-package.json` as a hub-owned extension such as `session_templates`, while core `entrypoints` remains the code-load ABI.
- Assumption: `botster context` should default to JSON output with subcommands or selectors for individual keys. The spawned script can consume it without requiring shell-specific interpolation.
- Assumption: context should be stored per spawned session/request and retrieved by `BOTSTER_SESSION_ID` or an explicit `BOTSTER_CONTEXT_ID`, not by untrusted cwd discovery.
- Assumption: explicit spawn request overrides may set only fields admitted by the resolved template and target policy. They must not bypass target cwd/path/env restrictions.
- Unknown: the final repo-local override path. Prefer an existing Botster config convention if implementation finds one; otherwise use a narrow path under the admitted target such as `.botster/session-templates/`.
- Unknown: whether this repo already has an unmerged spawn-target branch in another pipeline. If it appears during implementation, use that target API instead of inventing a second one.
- Unknown: exact compatibility behavior for package templates when a package is installed but disabled. Prefer listing disabled package templates as unavailable with diagnostics, and allowing spawn only from enabled packages or device/repo templates admitted by policy.

## Botster layers touched

- Rust hub policy/config layer for template manifests, override resolution, context assembly, and admitted targets.
- Rust local client API and daemon protocol for list/show/resolve/spawn template commands plus context reads.
- Rust runtime facade only to accept already-materialized generic spawn requests; core remains unchanged except for any required public DTO imports already exposed by dependencies.
- Thin CLI for `sessions spawn` preservation, session-template commands, and `botster context`.
- Package fixtures and docs.

No Lua Project Pipelines policy, browser SPA, TUI rendering, Rails relay, or MCP workflow behavior is required.

## Affected surfaces/files

- `src/config.rs`
  - Add hub startup/default config fields for device template roots and admitted spawn-target records if no existing target API is present.
  - Validate template/config paths without trusting ambient cwd.
- `src/persistence.rs`
  - Persist hub-owned template registry inputs or admitted targets when they are durable device policy.
- `src/packages.rs`
  - Parse package-provided session template declarations from local package manifests.
  - Validate template ids, script paths, allowed env names, declared context keys, and unsafe path traversal.
  - Preserve package source path internally for materialization while keeping client DTOs sanitized.
- New `src/session_templates.rs` or equivalent hub module
  - Define template manifests, override precedence, resolution diagnostics, materialized spawn command, context payload, and admission errors.
  - Keep all policy here or in nearby hub modules, not in core.
- `src/client_api.rs`
  - Add `HubClientRequest` variants for list/show/resolve/spawn template and read session context.
  - Return sanitized DTOs and structured rejection diagnostics.
- `crates/botster-hub-client/src/lib.rs`
  - Add public daemon request/response DTOs and feature flags for session templates/context.
  - Update generated TypeScript protocol output if the existing workflow requires it.
- `src/daemon_transport.rs`
  - Route daemon requests through `HubClientApi`.
  - Ensure `SpawnFromTemplate` materializes through the same `runtime.spawn_session` path as plain spawn.
  - Preserve bounded diagnostics without dumping context payloads or local absolute paths.
- `src/main.rs`
  - Add CLI commands for template list/show/resolve/spawn and `context`.
  - Preserve current `sessions spawn --data-dir ... -- <command>`.
- `tests/hub_runtime_test.rs`
  - Focused resolver/admission tests if the template module can run without the daemon.
- `tests/hub_client_api_test.rs`
  - API tests proving sanitized template DTOs, resolution precedence, context retrieval, and policy rejection.
- `tests/hub_daemon_lifecycle_test.rs`
  - Real daemon/PTY tests proving package fixture registration, template spawn, `botster context` inside the spawned script, restart-safe context behavior where intended, plain spawn preservation, and rejection of unauthorized cwd/env/path/template requests.
- `examples/project-pipelines/botster-package.json` or a new fixture under `examples/`
  - Add a minimal package-provided template fixture, ideally with an initialization script that writes selected context keys to stdout or a temp artifact for test assertion.
- `README.md` and/or `docs/client-protocol.md`
  - Document session-template contract, override precedence, context command, policy rejection behavior, and no-agent-runtime boundary.

## Risks

- Core-boundary regression: adding template or agent vocabulary below hub/client policy would violate the ticket. Mitigation: materialize to `SessionSpawnRequest` before entering `HubRuntime::spawn_session`; core sees only generic command/args/cwd/env/metadata.
- Spawn-target scope creep: the repo currently lacks a visible first-class spawn-target registry. Mitigation: add only the minimal admitted target state needed to authorize cwd/repo overrides, or reuse an existing target API if it lands before implementation.
- PII leakage: context values can contain local paths, prompts, ticket metadata, and operator-provided metadata. Mitigation: context command output is explicit user-path behavior inside the spawned session; logs, diagnostics, gate evidence, docs, and client list/show output must be sanitized and avoid raw payload dumps.
- Underwired implementation: parser-only tests would miss the production path. Mitigation: acceptance requires daemon-driven template spawn and a spawned script invoking `botster context`.
- Env/cwd bypass: explicit overrides could silently undo target policy. Mitigation: resolve all overrides before spawn, then validate the final materialized request against target/template policy.
- Dead injection declarations: manifest-declared context/env keys are meaningless unless the fixture script consumes them. Mitigation: test the launched script reading `botster context` and asserting the expected values.
- Compatibility churn: daemon protocol changes require client DTO/type updates and may require generated TypeScript drift checks if touched.

## Acceptance checks/tests

- Focused unit/API tests:
  - parse package/device/repo session templates;
  - reject duplicate ids, missing script/command, path traversal, absolute unauthorized paths, unsafe env names, unknown context keys, and disabled/unavailable package templates;
  - prove override precedence: package defaults < device defaults < repo target overrides < explicit allowed request values;
  - prove sanitized list/show/resolve DTOs do not expose PII or local raw context payloads.
- Real daemon path tests:
  - install/enable a package fixture with a Codex/Claude-like initialization template;
  - list/show/resolve the template through the daemon;
  - spawn it through the daemon as a PTY session;
  - attach/drain output and assert the script successfully calls `botster context` for expected values;
  - assert unauthorized cwd/env/path/template requests are rejected before core spawn;
  - assert existing `sessions spawn -- <command>` still works.
- Docs checks:
  - README or client protocol documents manifest/config shape, override precedence, `botster context`, target policy, and the deliberate no-agent-runtime boundary.
- Commands expected after implementation:
  - `cargo fmt`
  - `./test.sh session_template` or equivalent focused filters
  - `./test.sh --test hub_daemon_lifecycle_test <focused_template_spawn_test>`
  - `cargo test -p botster-hub-client` if public daemon DTOs changed
  - strict clippy if the repo gate requires it; attribute any baseline failures to touched or untouched files.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Checklist fallback evidence: checklist instructions were loaded; checklist creation timed out; vault notes and convention checks are recorded here and should be mirrored in the gate evidence.
- Implement gate should require committed code plus runtime evidence that the daemon-spawned PTY path changed, not just template parser tests.
- Review should reject any solution that adds Codex/Claude/agent semantics to core, logs raw context payloads, or leaves template spawn unwired from the production daemon path.

## Worktree and target assumptions

- Assigned worktree: this pipeline run's ticket worktree.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Downstream agents must operate in the assigned worktree and keep any spawned workflow requests bound to explicit target ids and worktree paths.

## Convention conflict check

No conflicts found. The plan follows the loaded Botster conventions: hub owns policy and orchestration, core remains generic, packages may contribute manifests but do not execute policy, Project Pipelines remains a plugin-level workflow, plan context is cited by note title, and acceptance proves the real runtime path.

## Vault gaps worth capturing

- Capture the final `session_templates` manifest vocabulary, including context key names and override precedence, after implementation settles it.
- Capture whether this compact hub adds a minimal spawn-target registry here or reuses a concurrently landed target API.
- Capture the exact `botster context` retrieval contract and redaction boundary after the real spawned-script test proves it.
- Capture any recurring checklist timeout only if this run's known `plugin worker invoke timeout` pattern reveals a new failure mode.
