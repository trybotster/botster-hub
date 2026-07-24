# Wire Project Pipelines and Workspaces to hub spawn target and worktree APIs

## Context Loaded

- Pipeline context: ticket `ticket_1783463498_683100`, run `run_1783474634_627573`, current step `botster_plan`, run step `run_step_1783474634_863830`, gate `botster_plan_gate`; dependency `ticket_1783463498_456085` is closed; no prior artifacts, findings, questions, or answers were present.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Required identity/goals context: [[identity]] and [[goals]].
- Botster vault context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Skill context: `botster:botster-customize-hub`, because this plan may add a narrow hub Lua capability and changes hub-side plugin/runtime acceptance proof.
- Repo context inspected: `src/spawn_targets.rs`, `src/worktrees.rs`, `src/session_templates.rs`, `src/lua_runtime.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `examples/project-pipelines/plugin.lua`, `examples/project-pipelines/botster-package.json`, `examples/project-pipelines/README.md`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_lua_runtime_test.rs`, `tests/hub_mcp_test.rs`, and dependency plans `docs/plans/add-hub-owned-spawn-target-registry-and-daemon-crud-api.md`, `docs/plans/add-hub-worktree-crud-model-over-spawn-targets-without-requiring-git.md`, and `docs/plans/emit-worktree-lifecycle-events-to-plugins-and-clients.md`.
- Existing runtime baseline: hub-owned spawn targets and worktrees already exist, persist, expose daemon/client DTOs, support non-git directories, emit worktree lifecycle daemon events, and Lua plugins can list/validate spawn targets through `botster.capabilities.spawn_targets`.
- Gap found: Lua plugins cannot currently read hub-owned worktrees by id/list, so the Project Pipelines fixture still accepts a raw `worktree` string and forwards it into session-template context. That is the only planned hub API addition.
- Project Pipelines checklist evidence: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` timed out in the plugin worker, so checklist-equivalent evidence is preserved in this plan and the gate payload per [[project pipeline orchestration belongs in a device-level botster plugin]].

## Scope

- Add the smallest read-only Lua capability needed for plugins to consume hub-owned worktrees:
  - `botster.capabilities.worktrees.list()`;
  - `botster.capabilities.worktrees.show({ worktree_id = "..." })` or equivalent typed not-found result;
  - no create/update/delete mutation methods on the plugin capability.
- Update the Project Pipelines example fixture to start runs from a hub-managed `worktree_id` instead of a raw opaque `worktree` argument.
- In the Project Pipelines fixture, resolve the worktree through the new capability, pass the resolved path into `session_templates.spawn` as `context.worktree_path`, and retain `worktree_id`, `target_id`, request id, owner plugin, and agent name in run coordination metadata.
- Update `examples/project-pipelines/botster-package.json` and `README.md` to document the new local fixture contract: callers provide explicit `target_id` and hub `worktree_id`; hub owns target/worktree CRUD; Project Pipelines owns ticket/run/gate state only.
- Extend the generated Workspaces acceptance fixture inside `tests/hub_daemon_lifecycle_test.rs` so it proves Workspaces-style target validation via plugin surface/tool results:
  - list/validate spawn target refs through `botster.capabilities.spawn_targets`;
  - return useful statuses for valid, disabled, and missing target ids;
  - keep workspace records plugin-owned and only reference hub target ids.
- Strengthen the existing local runtime acceptance smoke to create a spawn target and hub worktree through public daemon requests before invoking first-party plugins, then prove:
  - Project Pipelines `start` consumes the hub worktree id and spawns the session template with the resolved context path;
  - Workspaces validates valid/invalid target refs through plugin results;
  - no plugin code hardcodes the raw worktree path string.
- Add focused Lua/runtime tests for the new read-only worktree capability, parallel to the existing spawn-target capability test.
- Update hub-test-support Project Pipelines conformance only if that helper needs to prove the changed fixture contract. Keep this as test-support compatibility proof, not a new product workflow.

## Non-Scope

- Do not change hub-owned spawn target CRUD semantics, worktree CRUD semantics, path admission, lifecycle event payloads, or filesystem delete behavior.
- Do not add Project Pipelines ticket/run/gate/PR/workspace fields to hub worktree records.
- Do not add Workspaces or Project Pipelines product policy to Rust core or hub worktree models.
- Do not update separate first-party plugin repositories in this ticket; document follow-up plugin-repo changes if the hub fixture proves a contract those repos should adopt.
- Do not build browser/operator workbench UI, workspace dashboard behavior, GitHub provider behavior, or broad package lifecycle changes.
- Do not preserve a second Project Pipelines fixture helper path for raw `worktree` if the example can cold-turkey move to `worktree_id`.

## Assumptions And Unknowns

- Assumption: `worktree_id` should become the Project Pipelines example fixture input. The plugin can resolve the hub-owned path at runtime and still pass `worktree_path` into the already-existing session template context contract.
- Assumption: exposing trusted same-device local paths from `worktrees.show` to a plugin is acceptable because hub worktree DTOs already expose paths through daemon/client APIs, and plugin execution is local and capability-gated.
- Assumption: read-only worktree Lua capability is the narrow hub API gap allowed by ticket scope. Mutating worktrees from plugins remains out of scope.
- Assumption: Workspaces acceptance can use the existing generated first-party fixture in `tests/hub_daemon_lifecycle_test.rs`; no checked-in `examples/botster-workspaces` package is required for this ticket.
- Assumption: `session_templates.spawn` does not need a new `worktree_id` field if Project Pipelines resolves the id before spawning and records the id in metadata. Add `worktree_id` to context only if implementation finds that is necessary to avoid raw-path-only context for downstream readers.
- Unknown: whether `worktrees.show` should throw a Lua runtime error on missing id or return `{ ok = false, status = "not_found" }`. Prefer structured false/not-found results for plugin diagnostics unless existing Lua capability conventions strongly favor errors.
- Unknown: whether the Project Pipelines MCP test in `tests/hub_mcp_test.rs` should be migrated in the same change. It likely should, because it currently exercises `project_pipelines.start` with raw `worktree`.
- Worktree/target assumption: implementation and verification must run in this pipeline-assigned worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`, not an ambient checkout.

## Affected Surfaces And Files

- `src/lua_runtime.rs`: add `worktrees` capability table beside `spawn_targets`, backed by shared hub worktree state and target state for reconciled list/show projections.
- `src/runtime.rs` and related host API wiring if needed: pass shared worktree state into Lua plugin runtimes the same way spawn targets are passed today.
- `examples/project-pipelines/plugin.lua`: replace raw `worktree` input handling with `worktree_id` resolution; record both `worktree_id` and resolved `assigned_worktree`/path in coordination only where needed for proof; fail with useful diagnostics for missing/stale worktree.
- `examples/project-pipelines/botster-package.json`: update tool schema and session template context metadata keys if the fixture records `worktree_id`.
- `examples/project-pipelines/README.md`: document explicit target id plus hub worktree id usage and list separate plugin-repo follow-ups.
- `tests/hub_lua_runtime_test.rs`: add real Lua plugin coverage for worktree list/show and absence of mutation methods.
- `tests/hub_daemon_lifecycle_test.rs`: update `write_botster_workspaces_local_package` to validate target refs; update the local runtime acceptance smoke to create target/worktree through `DaemonRequest`, then call Project Pipelines and Workspaces through plugin MCP.
- `tests/hub_mcp_test.rs`: migrate Project Pipelines MCP fixture calls from raw `worktree` to `worktree_id` if the example schema changes.
- `crates/botster-hub-test-support/src/lib.rs`: optionally extend `run_project_pipelines_conformance` only if it should assert the changed start/worktree path. Do not add broad test-support abstractions.
- `docs/client-protocol.md` or `docs/lua-plugin-abi.md`: update only if the Lua worktree capability becomes a public plugin ABI surface requiring docs.

## Risks

- Unwired proof risk: a test that only creates DTOs would not prove the production path. Mitigation: local runtime smoke must create records through `DaemonRequest`, then consume them through live plugin MCP calls and a spawned session template.
- API creep risk: adding plugin mutation methods for worktrees would blur hub ownership. Mitigation: expose list/show only and assert mutation methods are absent.
- Raw path regression risk: Project Pipelines could still accept or persist caller-supplied raw worktree strings. Mitigation: migrate tests and schema to `worktree_id`, then scan example/test fixture references for legacy raw `worktree` helper paths.
- Diagnostics risk: Workspaces validation could collapse disabled and missing targets into the same failure. Mitigation: assert returned statuses for `ok`, `disabled`, and `not_found`.
- Ownership drift risk: Workspaces or Project Pipelines could start storing hub-owned target/worktree records as their own authority. Mitigation: fixture records store references and plugin-owned workflow/workspace state only.
- Test runtime risk: the existing local runtime smoke is already broad and daemon-backed. Mitigation: add focused Lua/runtime tests for capability shape so failures localize before the heavy smoke runs.

## Acceptance Checks And Tests

- Focused Lua runtime:
  - real Lua plugin lists worktrees and shows a worktree by id through `botster.capabilities.worktrees`;
  - missing worktree returns structured diagnostics or a clearly asserted error path;
  - `create`, `update`, and `delete` mutation methods are absent from the plugin capability.
- Project Pipelines fixture:
  - `project_pipelines.start` requires or accepts `worktree_id`, resolves it through hub worktree APIs, and records `coordination.worktree_id`;
  - session-template spawn receives the resolved hub-owned worktree path in context and the spawned script/session remains observable through daemon attach/send/drain;
  - test fixture source no longer has a live raw `worktree` argument path for start.
- Workspaces fixture:
  - tool result validates an enabled target id with `ok`;
  - disabled target reports `disabled`;
  - missing target reports `not_found`;
  - workspace create/use state stays plugin-db owned.
- Hub acceptance smoke:
  - install/enable Project Pipelines and Workspaces fixture packages;
  - create spawn target through `DaemonRequest::CreateSpawnTarget`;
  - create worktree through `DaemonRequest::CreateWorktree`;
  - call Project Pipelines start with target id and worktree id;
  - call Workspaces validation path with valid, disabled, and missing target refs;
  - prove the Project Pipelines session template activation uses the hub-managed worktree context by attaching to the spawned session and observing its runtime path/echo behavior.
- Static cleanup:
  - scan `examples/project-pipelines`, `tests/hub_daemon_lifecycle_test.rs`, and `tests/hub_mcp_test.rs` for legacy `worktree` raw-input paths after migration.
- Final verification commands:
  - `./test.sh --test hub_lua_runtime_test worktree`
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_acceptance_smoke_exercises_first_party_plugins_project_pipelines_session_templates_reload_and_shutdown`
  - `./test.sh --test hub_mcp_test project_pipelines`
  - `cargo fmt`
  - `cargo clippy --all-targets --all-features -- -D warnings`

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Plan gate should include the loaded context, the scoped hub API gap, acceptance checks above, and the checklist fallback evidence.
- Implement gate should require committed changes plus proof that the daemon/plugin runtime path was exercised, not only unit-level code presence.
- Review/verify should reject an implementation that leaves Project Pipelines using raw caller-supplied worktree strings as the primary path.

## Vault Gaps Worth Capturing

- Capture a durable note if implementation confirms the rule: first-party workflow plugins consume hub worktrees by `worktree_id`, resolve paths through a read-only capability, and keep workflow associations plugin-owned.
- Capture the final Lua worktree capability shape and missing-worktree diagnostic convention if it becomes public ABI.
- Capture whether Project Pipelines session context should eventually carry `worktree_id` natively instead of only `worktree_path` plus metadata.
- Capture the recurring Project Pipelines checklist plugin-worker timeout if it appears outside this plan run.
