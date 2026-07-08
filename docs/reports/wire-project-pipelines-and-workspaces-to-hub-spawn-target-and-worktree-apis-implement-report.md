# Wire Project Pipelines and Workspaces to Hub Spawn Target and Worktree APIs - Implement Report

## Assumptions

- `worktrees.show` should return structured `{ ok = false, status = "not_found" }` diagnostics for normal absence, matching `spawn_targets.validate`.
- Lua worktree access is read-only. Hub daemon/CLI APIs remain the authority for spawn target and worktree CRUD and lifecycle events.
- Project Pipelines may resolve a hub-owned `worktree_id` into `worktree_path` only at session-template spawn time; run coordination keeps the hub-owned ids.
- Workspaces fixture records remain plugin-owned and store target references only.

## Files Changed

- `src/lua_runtime.rs`
- `src/runtime.rs`
- `src/lib.rs`
- `examples/project-pipelines/plugin.lua`
- `examples/project-pipelines/botster-package.json`
- `examples/project-pipelines/README.md`
- `docs/lua-plugin-abi.md`
- `tests/hub_lua_runtime_test.rs`
- `tests/hub_mcp_test.rs`
- `tests/hub_daemon_lifecycle_test.rs`

## Implementation Summary

- Added `botster.capabilities.worktrees.list()` and `worktrees.show({ worktree_id = "..." })` backed by shared hub worktree and spawn-target projections.
- Added live `HubRuntime::replace_state` refresh for worktrees alongside spawn targets.
- Migrated the Project Pipelines fixture from raw `worktree` input to `worktree_id`, resolving through the new read-only capability before session-template spawn.
- Extended the Workspaces fixture with target validation over `botster.capabilities.spawn_targets`.
- Updated focused and smoke tests to create spawn targets and worktrees through daemon requests, then consume them through plugin MCP/runtime paths.
- Documented the Lua worktree capability and follow-up plugin-repo migration notes.

## Verification

- `cargo fmt` - passed
- `./test.sh --test hub_lua_runtime_test worktree` - passed
- `./test.sh --test hub_mcp_test project_pipelines` - passed
- `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_acceptance_smoke_exercises_first_party_plugins_project_pipelines_session_templates_reload_and_shutdown` - passed
- `cargo clippy --all-targets --all-features -- -D warnings` - passed

## Deviations

- The plan listed hub-test-support conformance as optional. No change was made there because the changed contract is already proven through the hub MCP and dev-stack acceptance tests.
- Project Pipelines checklist creation timed out in the plugin worker, matching the plan-stage known issue, so checklist-equivalent evidence is recorded here and in the implement gate.

## Residual Risk

- Separate first-party plugin repositories are not updated in this ticket; the Project Pipelines example README records that follow-up.
- No vault note was written from this session; the durable ABI/contract knowledge was captured in repo documentation instead.
