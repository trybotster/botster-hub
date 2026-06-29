---
ticket: ticket_1782519710_637411
title: Add durable device and repo session-template override sources
run: run_1782751705_892070
step: botster_plan
---

# Add durable device and repo session-template override sources

## Context Loaded

- Pipeline context: ticket `ticket_1782519710_637411`, run `run_1782751705_892070`, current step `botster_plan`, gate `botster_plan_gate`; prior answered question confirmed the earlier Hotwire run was misrouted and this Rust Botster hub run is the correct pipeline.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Self/context notes: [[identity]], [[goals]].
- Botster architecture and workflow constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[device hub owns admitted spawn targets not ambient repo cwd]], [[botster packages should enforce core hub cli plugin provider boundaries]], and [[durable package snapshots must reconstruct admission through live helpers]].
- Artifact/checklist discipline: [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- Project Pipelines checklist workflow: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` timed out with `plugin worker invoke timeout`, so checklist evidence is preserved in this plan and should be mirrored in gate evidence.
- Repo context inspected: `src/session_templates.rs`, `src/client_api.rs`, `src/runtime.rs`, `src/config.rs`, `src/persistence.rs`, `src/packages.rs`, `src/daemon_transport.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_lua_runtime_test.rs`, `README.md`, `Cargo.toml`, and the predecessor plan `docs/plans/add-hub-owned-session-templates-and-botster-context-injection.md`.

## Scope

- Add durable device-level session-template defaults to the hub-owned state/config boundary.
- Add repo-local template additions and overrides for explicitly admitted spawn targets.
- Update session-template discovery so list/show/resolve/spawn use package, device, and repo sources with precedence: package < device < repo < explicit admitted request.
- Keep target admission authoritative in the hub: repo-local configs should only be read for an admitted target/root, and final cwd/env/path values must still pass target/template policy before core spawn.
- Persist and reload device defaults and any admitted target metadata needed to rediscover repo-local overrides after daemon restart.
- Add reload/discovery behavior for repo-local template changes through an explicit daemon or CLI path, or by recomputing from admitted target roots on each list/resolve if that is simpler and deterministic.
- Update docs for the device/repo template config shape, precedence, reload behavior, rejection behavior, and core boundary.
- Add focused resolver/API tests plus at least one real daemon/PTY test proving the production path uses device/repo override sources.

## Non-Scope

- Do not reimplement the first slice: package-owned templates, explicit request overrides, daemon spawn, Lua plugin spawn helper, and `botster context` already exist.
- Do not add Codex, Claude, agent, workspace, ticket, or Project Pipelines semantics to `botster-core`.
- Do not move session-template policy into package execution, Lua plugin policy, browser SPA, TUI UI, or Project Pipelines workflow code.
- Do not add broad configuration machinery, watchers, background sync, marketplace behavior, network Git resolution, or speculative target-management UI.
- Do not change existing plain `sessions spawn -- <command>` behavior except as required by shared refactoring tests.

## Assumptions and Unknowns

- Assumption: the durable device source belongs in hub-owned state/config, not in package manifests, because it is local host policy.
- Assumption: repo-local overrides should be discovered from an admitted spawn target root, not from ambient process cwd.
- Assumption: the smallest acceptable admitted-target model can be local and narrow if no existing spawn-target API is available in this checkout: target id, root path, optional repo config path, enabled flag, and admitted env/cwd/path policy.
- Assumption: repo-local config should use a conventional checked-in location such as `.botster/session-templates.json` or `.botster/session-templates/*.json`; choose the simpler shape that fits existing file helpers and document it.
- Assumption: request-time explicit overrides keep their current request DTO shape unless implementation proves public protocol fields are required for device/repo source selection.
- Unknown: whether another merged dependency has already added a spawn-target registry. Implementation should inspect current `origin/main` if needed and reuse it rather than creating a second target store.
- Unknown: whether repo-local changes should require explicit reload or can be discovered fresh per request. Prefer fresh deterministic reads if cheap; otherwise add an explicit reload command and test restart plus reload behavior.

## Botster Layers Touched

- Rust hub policy/config/persistence layer for durable device defaults and admitted repo-target metadata.
- Rust session-template resolver for source merging, precedence, target-scoped repo discovery, and final admission checks.
- Rust local client API and daemon transport only where necessary to feed the resolver with durable target/template sources and expose reload if chosen.
- Thin CLI only for documented operator paths to add/show/reload device or repo template sources if needed.
- Docs and tests.

Core, Lua plugin policy, Project Pipelines plugin code, browser SPA, TUI rendering, Rails relay, and MCP workflow policy should remain untouched unless the implementation discovers a direct compile break from the narrow Rust API change.

## Affected Surfaces/Files

- `src/session_templates.rs`: introduce a source-neutral resolver input instead of package-only helpers; represent package/device/repo rows with source labels; apply merge precedence; validate final cwd/env/path admission before materializing `SessionSpawnRequest`.
- `src/config.rs`: add narrow startup/default fields only if the hub needs a configured device template path or admitted target config root.
- `src/persistence.rs`: persist durable device defaults and admitted target metadata in `HubState`; reload must reconstruct derived admission through shared helpers, not a separate branch.
- `src/client_api.rs`: route list/show/resolve/spawn through the new merged resolver; keep sanitized DTOs and structured `SessionTemplate` errors.
- `src/runtime.rs`: pass the same resolver/source snapshot into plugin-triggered session-template spawns so Lua/MCP plugin paths see device/repo overrides too.
- `src/daemon_transport.rs` and `crates/botster-hub-client/src/lib.rs`: update daemon DTOs only if reload/source-management commands or new source metadata are public; preserve generated TypeScript if public DTOs change.
- `src/main.rs`: add the minimal operator CLI for device source persistence or repo-template reload only if required; do not make CLI own policy.
- `tests/hub_client_api_test.rs`: focused precedence and rejection coverage.
- `tests/hub_daemon_lifecycle_test.rs`: production daemon path coverage across package/device/repo source selection and restart/reload.
- `tests/hub_lua_runtime_test.rs`: update or add a focused case if plugin `session_templates.spawn` must observe merged sources.
- `README.md` and possibly `docs/client-protocol.md`: document config shape, precedence, reload/restart behavior, and no-core-template boundary.

## Risks

- Source precedence drift: package/device/repo/explicit logic could split between list/show/resolve/spawn. Mitigation: one resolver path should produce both display rows and materialized spawn requests.
- Admission bypass: repo-local config could admit cwd/env/command paths outside the target root. Mitigation: final materialized command, cwd, env overrides, and context paths must be validated after all merges.
- Durable reload divergence: loading hub state could interpret device defaults differently than live update paths. Mitigation: share validation/admission helpers and add save/load tests.
- Underwired implementation: unit tests can pass while daemon spawn still uses package-only records. Mitigation: real daemon test must prove a repo or device override changes spawned script behavior.
- Public DTO churn: adding source metadata can break generated protocol output. Mitigation: only add public fields that clients need; if added, update client crate tests and generated TypeScript.
- PII leakage: repo and worktree paths can leak into plan/docs/gate output. Mitigation: docs use generic examples; diagnostics remain path-neutral unless the operator explicitly resolves/spawns.
- Over-abstraction: this ticket does not require a general config framework or watcher service. Keep the implementation to explicit device defaults, target-scoped repo discovery, resolver merge, docs, and tests.

## Acceptance Checks/Tests

- Resolver/API tests prove:
  - package template rows still list and resolve;
  - device defaults can add a template and override package defaults;
  - repo-local target config can add a template and override device/package values for that admitted target;
  - explicit allowed request values override repo/device/package values;
  - duplicate ids follow the documented precedence or return structured ambiguity only when no precedence can disambiguate;
  - disabled or unadmitted targets cannot contribute repo templates;
  - unsafe command paths, cwd outside target root, absolute unauthorized paths, path traversal, and unallowed env overrides are rejected.
- Durable tests prove:
  - device defaults and admitted target metadata save to `hub-state.json`;
  - `HubRuntime::load` or daemon restart rediscovers the same effective templates;
  - repo-local additions/overrides are refreshed through the documented reload/discovery path.
- Real runtime tests prove:
  - daemon `ListSessionTemplates` and `ResolveSessionTemplate` expose the effective merged source;
  - daemon `SpawnSessionTemplate` launches a script whose output or `botster context` consumption reflects a device/repo override, not just package defaults;
  - unauthorized repo/env/cwd overrides fail before core spawn;
  - existing package-only session-template spawn and plain session spawn still work.
- Expected commands after implementation:
  - `cargo fmt`
  - `./test.sh session_template`
  - `./test.sh daemon_spawns_session_template_and_script_reads_botster_context` or the new focused daemon test name
  - `./test.sh --test-threads=1 <focused daemon filter>` if parallel daemon tests poison shared locks
  - `cargo test -p botster-hub-client` only if public daemon DTOs change
  - strict clippy per repo gate if Rust code changes are ready for review, with failures attributed to touched or untouched files.

## Pipeline Gates and Artifacts

- Plan artifact: this file.
- Checklist evidence fallback: checklist instructions were loaded and checklist creation timed out; vault notes, convention checks, and verification plan are recorded here and should be mirrored in gate evidence.
- Implement gate should require committed code plus runtime evidence showing the daemon-spawned PTY path used a device or repo template source.
- Review should reject implementations that only parse config files, leave plugin spawns on package-only resolution, add agent/product semantics to core, or weaken cwd/env/path admission.

## Worktree and Target Assumptions

- Assigned worktree: this pipeline run's ticket worktree.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Downstream agents must keep any spawned workflow requests bound to explicit target ids and the assigned worktree rather than ambient cwd.

## Convention Conflict Check

No conflicts found. The plan follows loaded Botster conventions: hub owns admission and durable policy, core remains generic, CLI stays thin, Project Pipelines remains plugin-owned, repo-local overrides are target-scoped, and plan context is cited by note title rather than local filesystem paths.

## Vault Gaps Worth Capturing

- Capture the final durable device template config vocabulary and repo-local file path after implementation selects the smallest shape.
- Capture the effective-source precedence rule if the final code reveals a reusable convention for package/device/repo/explicit layering.
- Capture the reload/discovery decision once runtime tests prove whether repo-local changes are read fresh or through an explicit reload command.
- No new vault note is needed at plan time for checklist timeout, core/hub boundaries, or explicit target/worktree orchestration; existing notes already cover those constraints.
