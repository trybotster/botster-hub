# Local Runtime Dogfood Readiness

## Status

Ready for local dogfood of the explicit daemon-backed Unix runtime path plus
constrained Project Pipelines MCP coordination over the Lua plugin runtime.

This note is scoped to the local `botster-hub` stack: hub as host profile and
control plane, `botster-core-daemon` as session supervisor/router,
worker-backed sessions as PTY owners, daemon-backed MCP stdio, the minimal local
TUI, and the constrained Project Pipelines Lua plugin package. It does not
cover cloud, Rails, WebRTC, browser, marketplace, provider process supervision,
GitHub/PR automation, hosted preview feature parity, broad monolith migration,
or uncoordinated crash PTY recovery.

## Evidence Matrix

| Failure mode | Proven? | Evidence | Remaining gap |
| --- | --- | --- | --- |
| Hub lifecycle restart over the same data directory preserves a live worker-backed session | Yes | `cli_daemon_restart_recovers_worker_backed_session_through_transport` starts `botster-hub start --data-dir`, spawns and attaches over daemon transport, shuts down the hub process, restarts the binary over the same data directory, observes `recovered_sessions`, reattaches, sends input, drains `echo:after-restart`, then shuts down the session and daemon. | None for the documented local Unix daemon path. |
| Startup reconciliation rejects stale or evidence-free registry records | Yes | `daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions` covers stale registry records and live worker-backed recovery through the current core adoption state machine. `daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues` covers records with restart evidence whose worker control socket is stale or missing; startup surfaces them through `stale_sessions` and continues. | Broader classifier branches remain core-contract coverage, not hub policy. |
| Full core-daemon process exit preserves PTYs | No | Current proof intentionally stops the hub daemon process through `shutdown --data-dir`, which calls the hub restart release path before exit. | Durable PTY adoption after an uncoordinated full core-daemon/session-worker process failure remains future work. |
| Package/provider state persists across hub state reload | Yes | `local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown` and `cli_packages_enable_local_path_persists_and_lists_through_client_api` install/enable/list the synthetic local package through hub policy and durable state. | Package fetch/index, provider supervision, and marketplace flows remain out of scope. |
| Agent-facing MCP tools route through the running daemon | Yes | `mcp_serve_supports_initialize_list_and_native_status_over_stdio` and `mcp_native_coordination_tools_route_messages_through_daemon_envelopes` start `botster-hub start --data-dir`, launch `botster-hub mcp-serve --data-dir`, list tools, call daemon-backed native status, publish/drain/ack caller-scoped routed envelopes, and verify guarded notification fallback without a second runtime. | Routed-envelope queues are in memory and are intentionally not restart-durable in this milestone. |
| Project Pipelines local workflow runs over the Lua plugin runtime | Yes | `mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools` enables `examples/project-pipelines` through the public package command, calls create/update/start/gate/advance/current-context over `mcp-serve`, asserts explicit target id, assigned worktree, owner plugin, request id, agent name, envelope id, publish status, drain cursor, ack status, and intentionally absent `session_uuid`, then restarts the daemon and verifies Project Pipelines tools re-register and PluginDb state remains visible. | Full agent spawn/worktree orchestration, GitHub/PR automation, broad monolith SQLite compatibility, Rails/cloud/WebRTC/browser/marketplace surfaces, and one-shot export/import tooling remain out of scope. |

## Command Split

Session and package commands use the long-running daemon transport. `status`,
`sessions list`, `sessions spawn`, `sessions attach`, `sessions send-input`,
`sessions resize`, `sessions detach`, `sessions shutdown`, `packages enable`,
`packages list`, `providers list`, and top-level `shutdown` connect to the
daemon socket created by `start --data-dir`.

Package commands mutate the package registry on the daemon owner thread and
persist the refreshed snapshot to `hub-state.json`. When the daemon is not
running they fail with `daemon not running` instead of starting a short-lived
hub that could silently diverge from live daemon state.

## Readiness Conclusion

The new stack is ready for local dogfood where the operator uses an explicit
data directory, starts the local hub daemon, drives session operations through
the daemon-backed CLI or minimal local TUI, coordinates agents through native
MCP routed-envelope tools, and runs the constrained Project Pipelines MCP
workflow through `examples/project-pipelines/plugin.lua` over the Lua ABI.

The readiness claim is intentionally narrower than full monolith parity.
Project Pipelines state persists through PluginDb, and intentional daemon
restart/reconnect is proved for package/plugin reload and worker-backed session
recovery, but routed-envelope inboxes are in-memory. Secrets are not imported by
the proof; provider credentials must be re-entered when deferred provider
integrations land. Live monolith Project Pipelines state is not imported in this
milestone; cutover requires no in-flight monolith tickets or a future explicit
one-shot export/import tool.

Remaining feature-parity work is provider process supervision, package
fetching/index behavior, GitHub/PR automation, install/update packaging,
cloud/Rails/WebRTC/browser/marketplace adapters, public socket self-heal,
long-running attach signal handling, broad migration compatibility, and
uncoordinated full daemon/process-crash PTY recovery.
