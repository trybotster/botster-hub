# Port Project Pipelines as a Local Plugin

## PR Review Merge-Conflict Addendum

### Context Loaded

- Pipeline context: the run returned to `botster_plan` after GitHub PR review `pullrequestreview-4440428794` on PR #34 requested: "Please fix merge conflicts."
- PR context: PR #34 is `https://github.com/trybotster/botster-hub/pull/34`, head `project-pipelines/ticket_1780628470_952911`, base `main`, review decision `CHANGES_REQUESTED`, merge state `DIRTY`.
- Local context: this worktree is detached at PR head `ce365f5`; `origin/main` is `b98bb88`; unrelated local files remain dirty/untracked (`.gitignore`, `.env`, `mise.local.toml`, `target/`) and must be ignored.
- Conflict inspection: `git merge-tree HEAD origin/main` reports content conflicts in `src/daemon.rs`, `src/daemon_transport.rs`, `src/lib.rs`, `src/lifecycle.rs`, `src/main.rs`, `src/mcp.rs`, `src/runtime.rs`, and `tests/hub_mcp_test.rs`.
- Mainline drift: `origin/main` now includes Lua plugin runtime work (`src/lua_runtime.rs`, `docs/lua-plugin-abi.md`, `tests/hub_lua_runtime_test.rs`) and no longer has the branch-only Project Pipelines plan/package/runtime files. The merge resolution must preserve main's Lua runtime and adapt Project Pipelines to it where needed.

### Scope

- Update the existing PR branch only; do not create a new run or a new PR.
- Merge or rebase the PR branch with current `origin/main`, resolving conflicts surgically in the eight conflicting files named above.
- Preserve the already-implemented Project Pipelines behavior from PR #34: daemon-backed plugin MCP tools, package enable/startup loading, plugin-data persistence, restart tool re-registration, disable/unload removal, persist-failed error reporting, docs/cutover posture, and existing targeted tests.
- Preserve new mainline Lua runtime/API work rather than deleting it. If main's Lua runtime supersedes branch scaffold assumptions, integrate Project Pipelines with the new runtime boundary instead of keeping stale "Lua unavailable" wording.
- Keep branch-only Project Pipelines package/docs/runtime artifacts where still required: `examples/project-pipelines/*`, `src/project_pipelines.rs` or its mainline-adapted successor, `docs/plans/port-project-pipelines-as-local-plugin.md`, and MCP tests.

### Non-Scope

- Do not broaden the PR beyond conflict resolution and required adaptation to main's new Lua runtime APIs.
- Do not rework unrelated daemon, MCP, lifecycle, Lua ABI, or client API behavior from main unless the conflict requires it.
- Do not remove or rewrite mainline `src/lua_runtime.rs`, `docs/lua-plugin-abi.md`, or `tests/hub_lua_runtime_test.rs`.
- Do not touch unrelated local dirty files.

### Assumptions And Unknowns

- Assumption: the correct conflict posture is "main plus Project Pipelines," preserving current mainline Lua runtime additions and PR #34's Project Pipelines acceptance behavior.
- Assumption: conflict resolution may need to replace earlier host-supplied Project Pipelines runtime scaffolding with real Lua runtime integration if `origin/main` now exposes the required adapter.
- Unknown: whether Project Pipelines can immediately move all workflow policy into Lua on top of main's new runtime without expanding the ticket. If not, retain the smallest compatibility bridge and document the residual limitation accurately.
- Unknown: whether Cargo dependency and lockfile updates from main require re-running broader test suites beyond the prior targeted MCP tests.

### Affected Surfaces And Files

- Definite conflict files: `src/daemon.rs`, `src/daemon_transport.rs`, `src/lib.rs`, `src/lifecycle.rs`, `src/main.rs`, `src/mcp.rs`, `src/runtime.rs`, `tests/hub_mcp_test.rs`.
- Definite preservation/adaptation files: `src/lua_runtime.rs`, `docs/lua-plugin-abi.md`, `tests/hub_lua_runtime_test.rs`, `src/project_pipelines.rs`, `examples/project-pipelines/*`, `README.md`, `Cargo.toml`, `Cargo.lock`.
- Plan/docs: this plan artifact may be updated only to reflect the conflict-resolution posture and any changed residual risks.

### Risks

- Accidentally resolving conflicts by deleting main's new Lua runtime would regress the dependency work this ticket was waiting on.
- Accidentally resolving conflicts by deleting Project Pipelines branch files would satisfy merge mechanics but fail the ticket acceptance.
- Prior residual-risk wording about "real Lua unavailable" may become false after mainline changes; stale docs are a review risk.
- Cargo.lock/Cargo.toml drift can hide compile or clippy failures if only the old targeted MCP test is rerun.
- Detached worktree and unrelated dirty files make it easy to commit local noise or push the wrong ref.

### Acceptance Checks And Tests

- `git status --short --branch` before and after resolution; final staged/committed changes must exclude `.gitignore`, `.env`, `mise.local.toml`, and `target/`.
- Conflict resolution check: no conflict markers remain (`rg '<<<<<<<|=======|>>>>>>>'`).
- `cargo fmt`
- `cargo clippy --all-targets -- -D warnings`
- Re-run prior Project Pipelines proofs:
  - `./test.sh mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools`
  - `./test.sh mutating_handler_reports_persist_failed_when_state_write_fails`
- Re-run or add Lua-runtime coverage from main:
  - `./test.sh hub_lua_runtime` or the exact test-filter equivalent that actually executes `tests/hub_lua_runtime_test.rs`
- Re-run the bounded merge-affected suites because main and the PR both touched daemon, MCP, lifecycle, runtime, package, and capability paths:
  - `./test.sh --test hub_mcp_test`
  - `./test.sh --test hub_capability_runtime_test`
  - `./test.sh --test hub_daemon_lifecycle_test`
  - `./test.sh --test hub_plugin_lifecycle_test`
  - `./test.sh --test hub_local_dogfood_test`
  - `./test.sh --test hub_runtime_test`
  - `./test.sh --test hub_lua_runtime_test`
- Re-read the Project Pipelines README, root README, and cutover/residual-risk wording against main's Lua runtime and plugin ABI docs. Correct stale claims, or record why each limitation still holds.
- Push the resolved existing PR branch `project-pipelines/ticket_1780628470_952911` and verify PR #34 no longer reports `DIRTY`.

### Vault Gaps Worth Capturing

- If resolving this conflict requires a new rule for Project Pipelines moving from host-supplied Rust policy to main's Lua runtime adapter, capture that boundary after implementation.
- If `./test.sh` filter semantics make `hub_lua_runtime` easy to run as zero tests, capture the exact working filter if not already documented.

## Context Loaded

- Pipeline context: ticket `ticket_1780628470_952911`, run `run_1780688521_435653`, step `botster_plan`, gate `botster_plan_gate`.
- Prior pipeline state: Plan Review returned changes required in `review_1780688939_892262` with open findings requiring a corrected MCP process-boundary design, definite daemon plugin loading, daemon transport request/response surfaces, stronger restart checks, and primitive-backed coordination evidence. Dependencies are closed.
- Vault/playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Review-required vault constraints added: [[hub daemon runtime stays on one owner thread while socket handlers submit requests]], [[package mutations require the running daemon owner]], [[adoption restart evidence must come from real protocol primitives not defaults]].
- Skill context: `botster-customize-plugin`, because this ticket is a Botster Lua plugin with MCP tools, plugin.db/plugin-store persistence, timers, HTTP, and agent orchestration.
- Repo context inspected: `src/mcp.rs`, `src/runtime.rs`, `src/lifecycle.rs`, `src/capabilities.rs`, `src/packages.rs`, `src/persistence.rs`, `src/lib.rs`, `src/main.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `tests/hub_mcp_test.rs`, `tests/hub_plugin_lifecycle_test.rs`, `tests/hub_capability_runtime_test.rs`, `tests/hub_local_dogfood_test.rs`, `examples/synthetic-plugin/*`, `README.md`, `Cargo.toml`.
- Checklist evidence: run checklist `checklist_1780688579_245011` exists and was updated for this revised Plan pass.

## Scope

- Add a first real local Project Pipelines plugin package fixture under a repo-owned plugin path, replacing the synthetic example as the meaningful dogfood plugin.
- Define a constrained Project Pipelines data model in plugin-owned durable state through the new plugin store/capability runtime. Minimum workflow records should cover pipelines, tickets, runs, run steps, gate evidence, events, questions/answers, and agent/session correlation fields needed by MCP context and advancement.
- Expose Project Pipelines MCP tools through `tools/list` and `tools/call` using the shared `McpToolRegistry`, not a parallel MCP server. The constrained set should support create/list/update/start plus current context, gate submission, and step advancement enough to run a local workflow.
- Add daemon transport request/response plumbing for plugin MCP list/call. `mcp-serve` is a separate process and must not construct a second `HubRuntime`; it should register a plugin-backed `McpToolProvider` that forwards to the running daemon via `daemon_transport_request`, just as `NativeHubToolProvider` does.
- Service plugin MCP list/call on the daemon owner thread. New daemon request handlers should inspect loaded plugin-owned MCP descriptors and dispatch tool calls through the live `HubRuntime::invoke_plugin` path, preserving [[hub daemon runtime stays on one owner thread while socket handlers submit requests]].
- Load enabled plugin packages into the live daemon runtime, both when the daemon starts from persisted package state and when package enable/enable-local succeeds. This is required for MCP tools to exist after enable and after restart.
- Source the `HubPluginRuntimeBundle`, MCP descriptors, handlers, resources, and selected Lua entrypoint from the Project Pipelines package load path. The Project Pipelines package should be the first real package whose descriptors feed the daemon-backed MCP provider.
- Persist state under `<data-dir>/plugin-data/project-pipelines/` and prove it survives hub restart.
- Add agent coordination at the constrained local-runtime level: starting a run should produce primitive-backed evidence such as a real plugin invocation outcome, request-id acknowledgement, or persisted-then-reloaded correlation record carrying explicit `target_id`, assigned worktree identity, request id, and owned-session metadata. If full spawn is not available through current public plugin APIs, the implementer must state that plainly in docs and tests rather than implying full orchestration.
- Update docs, preferably plugin-local README plus root README references, to name unsupported monolith features and the cutover posture for live monolith data.

## Non-Scope

- Do not port Rails/cloud/WebRTC/browser/marketplace/provider supervision or broad monolith compatibility layers.
- Do not copy monolith internals wholesale; re-author only the constrained local workflow needed for this milestone.
- Do not add GitHub integration unless a constrained local workflow cannot meet acceptance without it. If included, use admitted direct HTTP only.
- Do not add a speculative UI workbench in this ticket. Document the UI contract and unsupported UI features, but keep implementation focused on MCP/runtime/store parity.
- Do not persist plugin runtime data in plugin source directories or monolith SQLite.

## Assumptions And Unknowns

- Assumption: this repo is the target `trybotster/botster-hub` scaffold for the new stack, and the correct first milestone is a constrained Project Pipelines plugin rather than full monolith parity.
- Assumption: plugin store JSON records are the current local equivalent of plugin.db for this hub repo; use `CapabilitySurface::PluginDb` and `plugin-data/project-pipelines` instead of introducing a separate SQLite dependency.
- Assumption: real Lua execution may still be limited by the available core worker contracts in this repo. The production path must nevertheless run through `HubPluginLifecycle`/`PluginWorkerEngine`, daemon owner-thread invocation, daemon transport, and the shared MCP registry, not a native-only bypass.
- Assumption: `mcp-serve` is a client of the running daemon, not the runtime owner. It must never instantiate a separate runtime to reach plugin handlers.
- Unknown: whether current `botster-core` exposes plugin-owned MCP descriptors in enough detail to directly convert loaded plugin descriptors into daemon MCP responses. If not, implement the smallest descriptor bridge on the daemon owner side, then keep `mcp-serve` as a transport-forwarding provider.
- Unknown: where the reduced repo should construct real Lua `HubPluginRuntimeBundle` values from local package entrypoints. Implementer must name and wire this source rather than using fake runtime-only test bundles.
- Unknown: whether current session spawn APIs expose admitted spawn target ids and assigned worktree creation in this reduced hub repo. If not, the constrained workflow must store explicit target/worktree fields and prove correlation without pretending full worktree orchestration exists.

## Affected Surfaces And Files

- Plugin package: add `examples/project-pipelines/botster-package.json`, plugin source files, and a plugin-local README. `catalog/templates/plugins/project-pipelines` is not present in this repo; use the existing examples package convention and keep runtime data out of source.
- MCP process adapter: `src/mcp.rs` for a plugin-backed provider that lists/calls tools by forwarding to daemon transport, plus JSON-RPC tests.
- Daemon transport and owner-thread handlers: `src/daemon_transport.rs` and `src/daemon.rs` for new daemon request/response variants such as plugin MCP list/call or generic plugin invoke, handled on the daemon owner thread.
- Runtime/lifecycle: `src/runtime.rs` and `src/lifecycle.rs` for loaded plugin descriptor visibility, owner-thread invocation, and enabled-package loading at daemon start and after enable.
- Capability/store: `src/capabilities.rs` if plugin store access needs a small typed helper or event-drain convenience; avoid changing existing store semantics unless required. `src/persistence.rs` is probably not a direct modification surface unless package-state reload needs a small hub-state shape change.
- Package lifecycle and daemon CLI: `src/packages.rs` and `src/main.rs` only as needed to construct Project Pipelines package bundles and keep CLI entrypoints thin.
- Tests: extend existing `tests/hub_mcp_test.rs`, `tests/hub_plugin_lifecycle_test.rs`, `tests/hub_capability_runtime_test.rs`, and `tests/hub_local_dogfood_test.rs` for restart/lifecycle proof.
- Docs: plugin README and root `README.md`; this plan artifact.

## Risks

- Biggest risk: native Rust MCP plumbing can accidentally become a second Project Pipelines implementation. Keep workflow policy in the plugin and make Rust a daemon transport, descriptor, dispatch, and store bridge.
- Process-boundary risk: invoking plugin handlers from `mcp-serve` directly would create a second daemon-disconnected runtime. The design must keep `HubRuntime::invoke_plugin` on the daemon owner thread.
- Lifecycle risk: enabling a package currently mutates/persists registry state only; if live load and start-time reload are missed, MCP tools remain unwired.
- Persistence risk: plugin store operations are asynchronous capability events. Tests must drain completions and restart the hub before asserting survival.
- Runtime wiring risk: `mcp-serve` currently only registers `NativeHubToolProvider`; code existence in plugin lifecycle tests does not prove agent-facing MCP tools use the plugin.
- Orchestration risk: name-based or ambient worktree routing would violate vault constraints. Agent/run-step state must carry explicit target id, assigned worktree, request id, and owner metadata.
- Cutover risk: live monolith Project Pipelines data may not be importable. Docs must state either one-shot import/export mechanics or that cutover requires no in-flight monolith tickets.
- Schema risk: if the implementation introduces versioned plugin records with required fields, add seeded prior-version upgrade coverage or keep v1 records additive/simple.
- PII risk: fixtures and logs must use synthetic ids, paths, ticket titles, and agent labels only.

## Acceptance Checks And Tests

- `cargo fmt`
- `cargo test --test hub_mcp_test`
- `cargo test --test hub_plugin_lifecycle_test`
- `cargo test --test hub_capability_runtime_test`
- Add or extend an end-to-end daemon test that:
  - starts `botster-hub start --data-dir <tmp>`;
  - installs/enables/loads Project Pipelines through package lifecycle;
  - calls `botster-hub mcp-serve --data-dir <tmp>`;
  - verifies `tools/list` includes Project Pipelines tools;
  - calls create/list/update/start/current-context/gate/advance tools through JSON-RPC;
  - shuts down and restarts the hub;
  - calls `botster-hub mcp-serve --data-dir <tmp>` again after restart;
  - verifies `tools/list` still includes Project Pipelines tools after restart;
  - verifies a `tools/call` against the restarted daemon returns the persisted workflow state from the new storage path.
- Add at least one coordination test proving a run step records or requests coordination through a real primitive: an actual plugin invocation outcome, daemon request-id acknowledgement, lifecycle/correlation event, or persisted-then-reloaded correlation record. The evidence must include explicit `target_id`, assigned worktree, request id, and plugin ownership metadata, and must not pass solely by echoing fields supplied by the test.
- Documentation check: plugin README or root docs name unsupported monolith features, data/cutover posture, and local plugin UI/MCP constraints.
- Runtime proof must cite the production entry point: `serve_mcp_stdio`/`botster-hub mcp-serve` uses a registry that includes a daemon-forwarding plugin MCP provider, and the daemon owner thread dispatches Project Pipelines tool calls through the live loaded plugin.

## Vault Gaps Worth Capturing

- The checklist worker timeout recurred during Plan; if it is not already fully captured, add/update a vault note with the exact `project_pipelines_create_vault_checklist` timeout path and the durable gate-artifact fallback used here.
- If implementation discovers that `botster-core` plugin descriptors cannot yet bridge into hub MCP providers cleanly, capture the missing descriptor/handler contract as Botster architecture knowledge.
- If this repo treats plugin store JSON as the new local equivalent of plugin.db, capture that naming/contract explicitly to avoid future agents adding SQLite in the hub scaffold by habit.
