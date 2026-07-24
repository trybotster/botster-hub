---
ticket: ticket_1781065270_520493
title: Supervise local package entrypoint processes in botster-hub
run: run_1781068756_818127
step: botster_plan
---

# Supervise local package entrypoint processes in botster-hub

## Context loaded

- Pipeline context: ticket `ticket_1781065270_520493`, run `run_1781068756_818127`, current step `botster_plan`, gate `botster_plan_gate`; dependency `ticket_1781065269_190384` is closed; no prior artifacts, findings, questions, or answers.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Required Botster/vault context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Ticket-specific process constraints: [[hub event loop blocking must use spawn_blocking for IO-bound tasks]], [[pty master fd close sends sighup but ignores it needs killpg]], [[subprocess harnesses must kill child on failed readiness]], [[graceful-termination-requires-explicit-cleanup-hooks]].
- Plan Review context: `review_1781069203_878651` returned changes required. The blocker was the original plan's unbacked claim that production runtime Ctrl-C cleanup would work even though the removed legacy launcher waits on a separate daemon child and `serve_daemon` only runs `daemon.stop()` on `DaemonShutdown`; this revision chooses a daemon-side signal cleanup mechanism and adds a matching acceptance test.
- Prior dependency artifact: `docs/plans/package-entrypoint-manifest-and-registry-state-contracts.md` established `runnable_entrypoints` as the hub-owned local/dev process contract adjacent to core package `entrypoints`.
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` loaded. `project_pipelines_create_vault_checklist` was attempted for this run and failed with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and should be copied into gate evidence.
- Repo context inspected: `src/packages.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_client_api_test.rs`, `docs/client-protocol.md`, and `examples/project-pipelines/botster-package.json`.
- Current repo state: `src/packages.rs` already parses, validates, persists, and exposes `PackageRunnableEntrypoint` declarations with `may_supervise` and static `PackageRunnableProcessState::NotStarted`. `crates/botster-hub-client` and CLI package output already expose `runnable_entrypoints`. No process supervisor or entrypoint lifecycle daemon requests exist yet.

## Scope

- Add hub-owned local process supervision for installed package `runnable_entrypoints` where `may_supervise == true` and mode is local/dev.
- Add explicit daemon/client requests for entrypoint lifecycle:
  - start one package entrypoint;
  - stop one package entrypoint;
  - restart one package entrypoint as stop then start;
  - status/list via existing package list/show rows, with optional direct status response only if it keeps the surface smaller.
- Track runtime process state in daemon-owned memory and expose it through the existing public package DTO path:
  - state (`not_started`, `starting`, `running`, `exited`, `failed`, `stopped`);
  - pid when the child is still live and safe to report;
  - start time;
  - exit status or signal-derived diagnostic;
  - bounded stdout/stderr diagnostics with explicit byte/line caps.
- Resolve command, args, and working directory from the existing sanitized `runnable_entrypoints` contract without shell expansion.
- Ensure supervised processes are stopped during package disable/remove, daemon shutdown, `botster-hub shutdown`, and the removed legacy launcher Ctrl-C/shutdown paths.
- Add daemon-side SIGINT/SIGTERM handling in the `botster-hub start` serve path so a foreground production runtime Ctrl-C causes the daemon process to run `HubDaemon::stop()` before exiting. Prefer `signal-hook` as the narrow signal primitive if no direct dependency already exists; verify the current version before adding it as a direct dependency.
- Add fixture-backed tests for successful start, missing command, failed command, stop, restart/status, and shutdown cleanup.
- Keep all process output capture bounded and test scrubbed; no host environment snapshots, no arbitrary terminal capture, and no real user identity mutation.

## Non-scope

- No production installer, sandbox, marketplace fetcher, dependency solver, or persistent process manager.
- No hardcoded `botster-web` behavior beyond fixture-compatible manifests and examples.
- No broad package registry refactor; build on existing `PackageRegistry`, `HubDaemon`, `HubClientApi`, and daemon transport paths.
- No TUI, React SPA, Rails relay, Lua plugin policy, or Project Pipelines workflow changes.
- No persistence of live pids across daemon restarts. A restarted hub should report no live supervised processes unless implementation intentionally adds adopt-by-proof behavior, which is out of scope for this ticket.
- No shell command strings. Execute command plus args directly.

## Assumptions and unknowns

- Assumption: "package entrypoints marked supervisable/local" maps to existing `PackageRunnableEntrypoint { may_supervise: true, mode: dev|local }`.
- Assumption: supervision state is daemon-owned runtime state, not durable registry state. Persisting pids/start times would be misleading after daemon restart.
- Assumption: start should require the package to be installed and enabled. Starting a disabled package entrypoint should return a structured operator error.
- Assumption: `PackageRunnableWorkingDirectory::PackageRoot`, `EntrypointDir`, and `Relative` should be resolved under the local package root already stored in the package source, but raw local roots should not leak into public DTOs.
- Assumption: command resolution should first support package-relative executable paths. Bare command names may be treated as host `PATH` commands only if tests prove no shell is involved and diagnostics stay bounded.
- Assumption: stop should use a graceful timeout and then force kill. On Unix, prefer process-group cleanup where the implementation can safely create a child process group; otherwise document the narrower child-only cleanup and add a follow-up risk.
- Assumption: ticket-required production runtime Ctrl-C cleanup should be solved in the daemon process, not only in the production runtime parent. The daemon owns supervised child handles, so `serve_daemon` must observe SIGINT/SIGTERM, run `daemon.stop()`, clean up the socket path, and then return.
- Assumption: supervised child processes can still use their own process groups for explicit stop/restart cleanup because the daemon signal handler, not inherited terminal SIGINT propagation, is the cleanup mechanism.
- Unknown: exact CLI spelling. Prefer `botster-hub packages entrypoints start|stop|restart --data-dir <dir> <package> <entrypoint>` only if the parser stays simple; otherwise `packages start-entrypoint|stop-entrypoint|restart-entrypoint` is acceptable. The public daemon DTO names matter more than the human spelling.
- Unknown: whether process supervision belongs inside `HubDaemon` directly or a new small `PackageEntrypointSupervisor` module. Prefer a small owned struct if it isolates child handles, reader threads, caps, and cleanup without becoming a speculative framework.
- Unknown: whether `signal-hook` is already a direct dependency by implementation time. It appears in `Cargo.lock` transitively, but the implementing agent must verify the latest crate version before adding or promoting it per dependency-version convention.

## Botster layers touched

- Rust hub package/registry runtime state.
- Rust hub daemon lifecycle.
- Rust daemon socket and same-device `botster-hub-client` DTO/request surface.
- Rust daemon foreground signal/shutdown path.
- Thin CLI package command surface.
- Rust integration/unit tests and local disposable fixtures.
- Client protocol docs if request/DTO fields are added.

No Lua core/plugin worker, TUI, React SPA, Rails relay, MCP workflow, or cloud provider layer should change.

## Affected surfaces/files

- `src/packages.rs`
  - Add minimal helpers to locate a supervisable runnable entrypoint on an enabled local package.
  - Add validation/error variants if current package errors cannot distinguish missing package, disabled package, missing entrypoint, and not-supervisable entrypoint.
  - Do not store live process handles in `PackageRecord`.
- New `src/entrypoint_supervisor.rs` or equivalent narrow module
  - Own child process handles keyed by `(package_name, entrypoint_id)`.
  - Start commands without shell expansion, resolve package-relative command/working directory, set process group when practical, spawn bounded stdout/stderr readers, poll exits, and implement stop/restart/status/cleanup.
  - Keep output caps small and deterministic; expose diagnostic rows, not raw unbounded logs.
- `src/daemon.rs`
  - Add supervisor ownership to `HubDaemon`.
  - Stop all supervised entrypoints in `HubDaemon::stop`.
  - Ensure disable/remove paths can ask the supervisor to stop package-owned entrypoints before mutating/removing registry state.
- `src/daemon_transport.rs`
  - Route daemon requests to the live `HubDaemon` owner.
  - Refresh package list/show responses from supervisor status so public rows prove the runtime path changed.
  - Stop entrypoints on package disable/remove and daemon shutdown.
  - Add a daemon-owned signal notification path for SIGINT/SIGTERM in `serve_daemon`; it must cause the same cleanup path as `DaemonShutdown`, including `daemon.stop()` and socket cleanup.
- `src/client_api.rs`
  - Extend package DTO mapping so runtime process snapshots override the static manifest default when a supervisor status snapshot is supplied.
  - Add request/response structs only if the lifecycle commands route through `HubClientApi`; otherwise keep the runtime mutation in daemon transport but still reuse DTO mapping for package rows.
- `crates/botster-hub-client/src/lib.rs`
  - Add public `DaemonRequest` variants for package entrypoint start/stop/restart/status.
  - Extend `DaemonPackageProcess` with additive `Option` fields such as pid, started_at, exited_at, or exit_status using `#[serde(default, skip_serializing_if = "Option::is_none")]` so older clients and out-of-scope DTO mirrors remain compatible.
  - Add/update serde compatibility tests.
- `src/main.rs`
  - Add thin package command parsing and output for entrypoint start/stop/restart/status.
  - Keep output path-neutral and diagnostic-bounded.
  - Keep production runtime parent cleanup as a parent-side fallback, but do not rely on parent `SIGKILL` for supervised-entrypoint cleanup because it bypasses daemon cleanup hooks.
- `Cargo.toml` / `Cargo.lock`
  - Add a direct signal-handling dependency only if needed for the daemon signal path; verify the latest version before changing dependency metadata.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add disposable local package fixtures whose entrypoint commands succeed, fail, stay alive for stop/restart, and intentionally point to a missing command.
  - Prove CLI/daemon start, stop, restart, status/list/show, package disable/remove cleanup, and daemon shutdown cleanup.
- `tests/hub_client_api_test.rs`
  - If `HubClientApi` gains lifecycle requests, test request admission and package DTO process-state mapping.
- `crates/botster-hub-client/src/lib.rs` tests
  - Prove new request and process-state fields are serde stable and backward compatible.
- `docs/client-protocol.md` and possibly `README.md`
  - Document the local/dev-only lifecycle requests, state fields, bounded diagnostics, and non-sandbox boundary.

## Risks

- Orphan risk: ordinary `Child::kill` may not stop grandchildren. Implementation should use process-group cleanup where practical and test the specific child disappears.
- Production runtime Ctrl-C risk: without daemon-side signal handling, the removed legacy launcher can terminate the daemon child without running `HubDaemon::stop`, orphaning supervised entrypoint processes. This plan requires a signal path in `serve_daemon` and a test for that exact path.
- Synchronous serve-loop blocking risk: process wait/output reads must not block the daemon owner loop. Use reader threads, `try_wait`, bounded polling, and bounded stop timeouts; do not call `child.wait()` in status/stop handling while the daemon owner loop is expected to keep servicing control messages.
- PII/logging risk: stdout/stderr can contain arbitrary plugin output. Capture only bounded snippets and expose them as diagnostics with caps; do not persist them into registry state.
- Underwiring risk: adding a supervisor type without routing through daemon requests and package DTO rows would fail the ticket. Tests must drive `botster-hub packages ...` or `botster_hub_client::request` against a running daemon.
- State drift risk: static manifest `process` fields currently live in `PackageRecord`. Runtime process state must override those values for responses without mutating durable package manifests.
- Restart ambiguity: after daemon restart, old child processes should not be adopted without strong protocol evidence. Treat them as out of scope and ensure shutdown cleanup prevents ordinary leftovers.
- Cross-platform risk: process-group APIs are Unix-specific; this repo currently uses Unix sockets and macOS/Linux-oriented tests, so Unix-specific cleanup is acceptable if guarded clearly.
- Scope creep risk: do not add restart policies, health checks, process dependency graphs, or generic supervisors beyond one-shot start/stop/restart/status for package entrypoints.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_starts_and_reports_running`
  - Installs/enables a disposable local package, starts a supervisable entrypoint through the running daemon, and proves package list/show exposes `running`, pid, and start time through public DTOs.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_reports_missing_command`
  - Starts an entrypoint with a missing command and proves structured diagnostics plus `failed` state, without panics or unbounded stderr.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_reports_failed_command`
  - Starts a command that exits non-zero and proves exit status and bounded diagnostics.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_stops_and_restarts`
  - Starts a long-lived fixture command, stops it, verifies the specific pid exits, restarts it, and verifies the pid/state changes.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_cleans_up_on_disable_remove_and_shutdown`
  - Proves package disable/remove and `DaemonShutdown`/`HubDaemon::stop` clean up live entrypoints.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_cleans_up_on_daemon_signal`
  - Starts a daemon subprocess, starts a long-lived package entrypoint, sends SIGINT or SIGTERM to the daemon process, and polls the specific supervised entrypoint pid until it exits. This is the regression for the the removed legacy launcher Ctrl-C path because production runtime runs the same `botster-hub start` daemon process.
- `./test.sh --test hub_daemon_lifecycle_test local_runtime_runs_daemon_package_lifecycle_session_and_clean_shutdown`
  - Existing production runtime shutdown path still passes.
- `./test.sh --test hub_daemon_lifecycle_test cli_packages_enable_local_path_routes_through_running_daemon_and_persists`
  - Existing package entrypoint DTO path still passes.
- `./test.sh --test hub_client_api_test package_and_lifecycle_queries_are_sanitized_and_explicitly_pulled`
  - Update or pair with a focused test proving runtime process state remains sanitized.
- `cargo test -p botster-hub-client`
  - Public daemon request/DTO serde compatibility.
- `cargo fmt`.
- `cargo clippy --all-targets --all-features -- -D warnings` if the repo baseline allows it; if not, Verify must attribute failures to touched vs untouched code.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan gate evidence should include context loaded, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Re-plan gate evidence should explicitly reference Plan Review findings `finding_1781069203_309164`, `finding_1781069203_250731`, `finding_1781069203_502028`, and `finding_1781069203_565067` and state how this artifact resolves each.
- Implementation gate must prove the production user path changed: running daemon requests update package entrypoint state visible through `botster-hub-client` package DTOs and CLI package list/show/status output.
- Verification should include command evidence for focused lifecycle tests plus existing package/session lifecycle regressions.

## Worktree and target assumptions

- Assigned worktree: the pipeline-created worktree for `ticket_1781065270_520493`.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must operate in the assigned worktree, not an ambient checkout.

## Vault gaps worth capturing

- Capture the final supervisor ownership decision: `HubDaemon` owns local package entrypoint processes while `PackageRecord` remains durable manifest policy only.
- Capture the concrete process cleanup pattern chosen for supervised entrypoints, especially whether process groups are used for non-PTY package processes.
- Capture the daemon signal cleanup pattern chosen for `botster-hub start` / production runtime Ctrl-C.
- Capture the exact bounded stdout/stderr diagnostic cap once implemented.
- Capture the public CLI spelling for package entrypoint lifecycle commands after implementation settles it.
- No convention conflicts found. The plan follows Botster hub-as-host-profile boundaries, keeps product policy out of core/Lua, and uses the existing public daemon/client package DTO path.
