---
ticket: Make plugin_db.get missing records return absent data instead of failing
run: run_1783462040_322096
step: botster_plan
---

# plugin_db.get missing-record absence plan

## Context Loaded

- Project Pipelines context: ticket `ticket_1783462026_694748`, run `run_1783462040_322096`, active step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, questions, or answers were present at plan time.
- Required vault/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], [[plugin db grants must update admission and runtime sources together]], [[plugin db schema upgrades fail on required columns and unique constraints]], and [[plugin owned surface route renders run in plugin worker vms]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` timed out through the plugin worker, but the checklist persisted as `checklist_1783462117_872692`; this artifact and gate evidence duplicate the checklist facts per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Plan Review context: `review_1783462647_846185` returned changes required with three findings. This revision resolves them by naming the get-vs-patch/delete disambiguation seam, requiring negative patch/delete missing-record tests, and making hub-local Lua-boundary absence mapping the decisive implementation direction.
- Repo evidence inspected:
  - `src/capabilities.rs:859` has `LocalPluginStoreBackend::get` already returning `Result<Option<PluginStoreRecord>, _>`.
  - `src/capabilities.rs:966` currently turns `None` for `PluginStoreOperation::Get` into `StoreNotFound` with `plugin-store record was not found`.
  - `src/lua_runtime.rs:596` exposes `botster.capabilities.plugin_db.get/set/patch/delete/list` and `src/lua_runtime.rs:725` converts capability completion/failure events into Lua return values or runtime errors.
  - `examples/project-pipelines/plugin.lua:26` currently uses `pcall(plugin_db.get)` in `load_state`, which hides the missing-read failure instead of proving ordinary absent-data branching.
  - `tests/hub_lua_runtime_test.rs:489` already exercises Project Pipelines surface render/action through `HubClientApi` and the plugin worker.
  - `tests/hub_capability_runtime_test.rs:351` already covers concrete hub PluginDb persistence and successful reads.
  - `tests/hub_mcp_test.rs:687` is the packaged Project Pipelines MCP smoke over a real daemon and proves plugin state persists under `plugin-data/project-pipelines`.
  - `docs/lua-plugin-abi.md:54` currently documents missing `plugin_db.get` records as runtime errors.

## Scope

- Change the hub-owned Lua/plugin DB runtime behavior so `botster.capabilities.plugin_db.get({ key = ... })` can return an absent-data result for a missing record without requiring plugin authors to wrap first-install reads in `pcall`.
- Implement the missing-read semantic at a get-specific seam. Do not string-match `plugin-store record was not found` inside the generic `plugin_store_event_to_lua` failure branch because get, patch, and delete currently produce indistinguishable `StoreNotFound` failures there.
- Preferred seam: keep this hub-local and thread enough operation/action context from `submit_plugin_store_and_wait` into the Lua event conversion so only `action == "get"` plus `StoreNotFound` becomes the absent result. If an implementer instead changes `execute_plugin_store`, it must still be get-specific and must not require a `botster-core` DTO change.
- Preserve the existing success shape for present records. Existing successful reads return `{ kind = "record", record = ... }` through serde/Lua conversion; absent reads should remain the least surprising same-family shape, preferably `{ kind = "record", record = nil }` as requested.
- Update Project Pipelines' checked-in plugin to branch on `result.record == nil` for first-install state rather than relying on `pcall` for expected absence.
- Add focused regression coverage for:
  - fresh plugin DB missing `get` through the real Lua helper;
  - existing successful `get` preserving current shape;
  - Project Pipelines surface render on fresh state through `HubClientApi` and plugin worker without missing-state failure.
- Update docs/API notes to say missing `get` is absent data, not an exceptional failure.
- Include the existing packaged smoke fixture only if implementation finds a first-party packaged plugin-surface smoke that already covers this path. The known `hub_mcp_test` Project Pipelines smoke is MCP/state persistence oriented, not a browser/package surface smoke; it should remain in the verification set if touched or if the implementation changes Project Pipelines state loading.

## Non-Scope

- Do not make `patch`, `update`/`set`, or `delete` idempotent on missing records unless existing tests prove that was already the contract.
- Do not map all `StoreNotFound` plugin-store failures to absence. Missing patch/delete must still raise.
- Do not add compatibility flags, aliases, versioned APIs, or dual behavior. This is a cold-turkey semantic correction.
- Do not move Project Pipelines persistence policy into the plugin as a workaround; the runtime should make ordinary plugin code safe on a fresh plugin DB.
- Do not broaden package admission, PluginDb namespace grants, schema upgrade behavior, package registry state, client protocol DTOs, or browser renderer code unless a compile-time contract forces it.
- Do not introduce a new database abstraction or dependency.

## Assumptions and Unknowns

- Assumption: The intended public behavior is the Lua helper behavior, not every lower-level `PluginStoreOperation::Get` consumer in `botster-core` test support. The ticket explicitly asks for botster-hub's concrete Lua/plugin DB runtime.
- Assumption: Lua code can branch on missing state with `if result.record == nil then ... end`. The implementation should verify actual `mlua` serialization of whatever absent shape is chosen.
- Decision: implement hub-local Lua-boundary synthesis as the default. `botster-core` currently defines `PluginStoreResult::Record { record: PluginStoreRecord }` with a non-optional record and no absent variant, so a core DTO change would be a cross-repo dependency and is out of ticket scope unless hub-local synthesis is proven impossible.
- Assumption: `submit_plugin_store_and_wait` already knows the `action` string and can pass it to event conversion, or equivalent typed context can be carried, so `get` missing can be distinguished from patch/delete missing before returning to Lua.
- Assumption: No human question is needed for the current ticket meaning; it is specific about missing read semantics and explicitly excludes missing write/delete idempotence.
- Worktree/target assumption: downstream agents operate only in the assigned Project Pipelines worktree for run `run_1783462040_322096`; artifacts should cite vault context by wiki link or note title, not local home-directory paths.

## Affected Surfaces and Files

- Botster layers touched: Rust hub capability runtime, Lua runtime helper, Project Pipelines first-party Lua plugin, Rust integration tests, docs/API notes. Possible packaged test-support fixture only if an existing surface smoke directly covers plugin surface rendering.
- Likely files:
  - `src/lua_runtime.rs:677`: pass the known action/operation kind into the event conversion path, or otherwise make the missing-result handling get-specific.
  - `src/lua_runtime.rs:725`: map only missing `plugin_db.get` into an absent Lua result instead of `RuntimeError`; keep patch/delete missing failures exceptional. Do not string-match an unqualified failure reason in the generic branch.
  - `src/capabilities.rs:966`: leave lower-level `StoreNotFound` behavior alone unless implementation chooses a get-specific hub-local mapping here without changing `botster-core` DTOs; keep non-get missing operations exceptional.
  - `tests/hub_lua_runtime_test.rs:489`: add or adjust Lua/plugin-surface regression tests.
  - `tests/hub_capability_runtime_test.rs:351`: add lower-level concrete hub regression only if changing `execute_plugin_store`, otherwise keep Lua helper as the user-path proof.
  - `examples/project-pipelines/plugin.lua:26`: replace expected-missing `pcall` handling with direct absent-record branching.
  - `docs/lua-plugin-abi.md:54`: update the documented `plugin_db.get` contract.
  - `tests/hub_mcp_test.rs:687`: include in verification if Project Pipelines state load/persist path changes.

## Risks

- Contract-shape risk: changing `PluginStoreResult::Record` to hold `Option<PluginStoreRecord>` would touch a shared core DTO and may cascade across hub/core tests and consumers. Prefer proving the Lua-boundary contract first.
- Disambiguation risk: `plugin_store_event_to_lua` currently lacks action context, and get/patch/delete missing records can emit the same `StoreNotFound` reason. The implementation must carry action context or handle the missing case at a get-specific seam; reason-string matching alone is not acceptable.
- False-positive test risk: a surface render test that keeps `pcall` in Project Pipelines would still pass while plugin authors still need exception handling. The test should cover direct `get` absent branching, not only "render eventually works."
- Runtime-path risk: code-only tests are insufficient. At least one test must use the production entry path from `HubClientApi::PluginSurfaceRender` to `HubRuntime::render_plugin_surface` to plugin worker route render to `botster.capabilities.plugin_db.get`.
- Semantics regression risk: successful reads, `set`, `patch`, `delete`, and `list` must retain existing behavior; missing `patch`/`delete` failures must remain failures.
- Serialization risk: Lua `nil` fields are omitted in tables, while serde JSON may encode `null`. The implementation must assert the actual Lua-side shape plugin authors branch on.
- Checklist workflow risk: Project Pipelines checklist creation timed out once in this Plan step. Checklist evidence must remain duplicated in this plan and gate evidence.
- PII risk: plan/report/test fixture data must avoid local usernames, real emails, and secrets. Use neutral names and synthetic ids.

## Acceptance Checks and Tests

- Focused behavior tests expected:
  - `./test.sh --test hub_lua_runtime_test <new_or_updated_missing_get_test_name>`
  - `./test.sh --test hub_lua_runtime_test <new_or_updated_missing_patch_delete_still_raise_test_name>`
  - `./test.sh --test hub_lua_runtime_test project_pipelines_surface_action_round_trip_uses_client_api_and_plugin_worker`
  - If `src/capabilities.rs` lower-level result semantics change: `./test.sh --test hub_capability_runtime_test <plugin_store_missing_get_test_name_or_file_filter>`
  - If Project Pipelines MCP/persistence path changes: `./test.sh --test hub_mcp_test mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools -- --test-threads=1`
- Broader verification before implementation gate:
  - Run the touched Rust integration tests through `./test.sh`, not direct `cargo test`.
  - Run `cargo fmt` after Rust edits.
  - Consider strict `cargo clippy` if shared DTO/core-facing types change; otherwise document why scoped tests cover this surgical Lua-runtime fix.
- Manual/evidence expectations for the implementation report:
  - Exact commands run and pass/fail summaries.
  - Evidence that missing `plugin_db.get({ key = "state" })` returns an absent result to Lua code, not a runtime failure.
  - Evidence that missing `plugin_db.patch(...)` and `plugin_db.delete(...)` still raise runtime errors.
  - Evidence that a fresh Project Pipelines surface render reaches `HubClientApi`/plugin worker and returns a default surface.
  - Evidence that successful reads still return the same record shape.

## Pipeline Gates and Artifacts

- Plan gate evidence should point to this artifact and checklist `checklist_1783462117_872692`.
- Implement gate should include a report artifact with changed runtime path, tests, command output summaries, and any decision about hub-local mapping vs. shared core DTO change.
- Review should specifically inspect for unwired implementation: code must be on the production Lua helper / surface render path, not only a helper or fake.

## Vault Gaps Worth Capturing

- No new durable vault note is required at Plan time. Existing notes already cover the relevant constraints: PluginDb grant/source drift, plugin.db schema upgrade hazards, plugin worker surface execution, repo-visible plan artifacts, checklist timeout fallback, and `./test.sh` usage.
- Capture candidate after implementation only if a new stable lesson emerges about representing absent Lua data across `mlua`/serde/core DTO boundaries, because that is likely reusable for future Botster plugin APIs.
