# Local Runtime Dogfood Readiness

## Status

Ready for local dogfood of the explicit daemon-backed Unix runtime path.

This note is scoped to the local `botster-hub` stack: hub as host profile and
control plane, `botster-core-daemon` as session supervisor/router, and
worker-backed sessions as PTY owners. It does not cover WebRTC, cloud, Rails,
browser, TUI, marketplace, provider process supervision, or hosted preview
feature parity.

## Evidence Matrix

| Failure mode | Proven? | Evidence | Remaining gap |
| --- | --- | --- | --- |
| Hub lifecycle restart over the same data directory preserves a live worker-backed session | Yes | `cli_daemon_restart_recovers_worker_backed_session_through_transport` starts `botster-hub start --data-dir`, spawns and attaches over daemon transport, shuts down the hub process, restarts the binary over the same data directory, observes `recovered_sessions`, reattaches, sends input, drains `echo:after-restart`, then shuts down the session and daemon. | None for the documented local Unix daemon path. |
| Startup reconciliation rejects stale or evidence-free registry records | Yes | `daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions` covers stale registry records and live worker-backed recovery through the current core adoption state machine. `daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues` covers records with restart evidence whose worker control socket is stale or missing; startup surfaces them through `stale_sessions` and continues. | Broader classifier branches remain core-contract coverage, not hub policy. |
| Full core-daemon process exit preserves PTYs | No | Current proof intentionally stops the hub daemon process through `shutdown --data-dir`, which calls the hub restart release path before exit. | Durable PTY adoption after an uncoordinated full core-daemon/session-worker process failure remains future work. |
| Package/provider state persists across hub state reload | Yes | `local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown` and `cli_packages_enable_local_path_persists_and_lists_through_client_api` install/enable/list the synthetic local package through hub policy and durable state. | Package fetch/index, provider supervision, and marketplace flows remain out of scope. |

## Command Split

Session commands use the long-running daemon transport. `status`, `sessions
list`, `sessions spawn`, `sessions attach`, `sessions send-input`, `sessions
resize`, `sessions detach`, `sessions shutdown`, and top-level `shutdown`
connect to the daemon socket created by `start --data-dir`.

Package commands are intentionally short-lived hub-policy operations over the
same durable state. `packages enable`, `packages list`, and `providers list`
start a `HubDaemon`, mutate or read `hub-state.json`, route reads through
`HubClientApi`, and stop. They do not attach to the long-running session daemon
today.

## Readiness Conclusion

The new stack is ready for local dogfood where the operator uses an explicit
data directory, starts the local hub daemon, and drives session operations
through the daemon-backed CLI. Remaining feature-parity work is provider process
supervision, package fetching/index behavior, browser/TUI/cloud adapters,
public socket self-heal, long-running attach signal handling, and uncoordinated
full daemon/process-crash PTY recovery.
