# Recover Owned Stale Incompatible Daemons

## Context Loaded

- Pipeline context: `ticket_1783212380_606511`, run `run_1783212385_768819`, step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, open questions, or answers.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]].
- Vault/project context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[botster hub client compatibility descriptors belong in client crate]], [[botster hub diagnostics use daemon diagnostic rows in client dtos]], [[operator diagnostic remediation must survive the diagnosed failure]], [[hub singleton requires OS-level flock not pid checks]], and [[test script required for rust tests not cargo test]].
- Repo context inspected: `src/main.rs`, `src/daemon_transport.rs`, `src/entrypoint_supervisor.rs`, and `tests/hub_daemon_lifecycle_test.rs`.
- Current production path: `botster-hub up` enters `local_runtime_up -> prepare_local_runtime -> ensure_dev_stack_daemon`; `botster-hub down` enters `local_runtime_down`; both currently treat `DaemonTransportError::Compatibility` and `DaemonTransportError::Protocol` as terminal `IncompatibleDaemon` errors before any recovery path runs.
- Current tests: `cli_local_runtime_up_reports_incompatible_daemon_without_deleting_socket` asserts the old behavior for both `up` and `down`; this should be replaced or split into owned recovery and unowned diagnostic coverage.

## Scope

- Add local-runtime daemon ownership metadata scoped to the selected data dir when `up` or `dev-stack bootstrap` spawns a daemon through `ensure_dev_stack_daemon`.
- Add a small recovery path used by `up` and `down` when the daemon handshake fails with compatibility/protocol staleness.
- Recovery should prove ownership before killing: exact resolved data dir, exact socket path, recorded PID, recorded hub binary path, and live process command evidence matching `botster-hub start --data-dir <that data dir>`.
- When ownership is proven, terminate the stale daemon process directly, wait briefly for exit, remove only the selected data dir's local socket if it remains stale, and retry the original operation once.
- Preserve actionable manual diagnostics when ownership cannot be proven.
- Keep `doctor` diagnostic-only unless implementation discovers shared helper reuse that preserves its current intent without silently killing processes.

## Non-Scope

- No protocol compatibility redesign, feature negotiation change, or `botster-hub-client` DTO shape change.
- No broad daemon supervisor rewrite or new singleton primitive.
- No cleanup of unrelated old Botster MCP processes, unrelated ticket worktree daemons, or daemons for other data dirs.
- No Rails, SPA, Project Pipelines plugin, or old monolith edits.
- No new runtime dependency unless implementation proves std/libc/process inspection cannot satisfy the ownership check safely.

## Assumptions And Unknowns

- Assumption: the selected data dir is the authority boundary for recovery; a daemon for another data dir must never be killed.
- Assumption: metadata alone is not enough because of PID reuse; live process evidence must still match the recorded command/data dir before termination.
- Assumption: direct recovery can use existing Unix process primitives and `std::process::Command` for process inspection rather than adding a crate.
- Unknown: whether the first implementation should support legacy stale daemons with no metadata via a tightly-scoped process-table fallback. The safer default is no, unless exact current binary plus exact `--data-dir` matching makes ownership as strong as metadata.
- Unknown: whether package entrypoint children always die with the daemon in all stale cases. Current `EntrypointSupervisor` stops process groups during normal shutdown; direct daemon termination may bypass that, so the implementation must either prove process-group cleanup by test or document the safe limit and avoid overclaiming.

## Affected Surfaces And Files

- `src/main.rs`: primary change surface for local-runtime spawn metadata, stale recovery helpers, `up` retry, `down` retry, and operator diagnostics.
- `src/daemon_transport.rs`: likely read-only; only touch if socket-path helper visibility is needed and cannot stay local to `main.rs`.
- `src/entrypoint_supervisor.rs`: likely read-only; inspect only if package-entrypoint cleanup needs a narrow exported stop/metadata hook.
- `tests/hub_daemon_lifecycle_test.rs`: replace old incompatible-daemon assertions and add owned/unowned recovery tests.
- Potential new local metadata file under the selected data dir, for example `local-runtime-daemon.json`; format should be private to `botster-hub` and minimal.

## Implementation Plan

1. Introduce a private local-runtime metadata type in `src/main.rs` with `pid`, resolved `data_dir`, resolved `socket_path`, resolved `hub_bin`, optional `session_worker_bin`, spawn timestamp, and owner marker such as `botster-hub-local-runtime`.
2. After `ensure_dev_stack_daemon` successfully spawns and observes readiness, persist metadata in the selected data dir. Remove or mark it stale after a successful compatible `down`.
3. Add `recover_stale_owned_daemon(config, data_dir, stale_error)` used only after compatibility/protocol errors. It should load metadata, verify path equality, verify the PID is alive, inspect the live command, and refuse recovery if any proof is missing.
4. If verified, send SIGTERM to the recorded daemon PID, wait for exit, escalate only if the process remains alive after a short bounded grace window, then remove the local socket path only if it is under the selected data dir and still exists.
5. In `local_runtime_up`, on stale handshake failure, attempt recovery and then retry `ensure_dev_stack_daemon` once. If recovery is refused, return the existing manual diagnostic, updated to explain that ownership was unproven.
6. In `local_runtime_down`, on stale handshake failure, attempt recovery and print a shutdown/recovered response if it succeeds. If recovery is refused, return the existing manual diagnostic.
7. Keep recovery logging path-neutral where possible, but retain the explicit user-facing `--data-dir` command in diagnostics because the current local-runtime UX already prints that operator path.

## Risks

- Killing the wrong process is the highest risk. Mitigation: require exact data-dir/socket metadata plus live command evidence; fail closed to manual instructions.
- PID reuse can make stale metadata dangerous. Mitigation: never kill on PID alone.
- Socket cleanup can delete another daemon's active socket. Mitigation: cleanup only after verified owned process termination and only for the selected config socket.
- Direct daemon kill can leave package entrypoint children alive if process groups are not coupled. Mitigation: verify or explicitly document limits; do not claim package cleanup without evidence.
- Tests that use fake listeners may prove socket behavior but not process termination. Mitigation: add at least one subprocess-backed fixture for owned recovery, plus an unowned fake-listener negative test.

## Acceptance Checks And Tests

- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_recovers_owned_incompatible_daemon -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_down_recovers_owned_incompatible_daemon -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_refuses_unowned_incompatible_daemon -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_recovery_removes_only_selected_data_dir_socket -- --test-threads=1`
- Existing focused coverage: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_bootstraps_packages_and_reuses_daemon -- --test-threads=1`
- Run broader focused hub lifecycle coverage if helper code affects common daemon command paths: `./test.sh --test hub_daemon_lifecycle_test -- --test-threads=1`.
- Runtime-path proof required in review: show `local_runtime_up` and `local_runtime_down` both call the new stale-recovery helper before returning `IncompatibleDaemon`, and show the retry reaches `serve_daemon` startup or direct shutdown completion.

## Vault Gaps Worth Capturing

- Capture a durable note if implementation settles a reusable convention for local-runtime ownership metadata and PID verification.
- Capture a durable note if process-group cleanup for package entrypoints under direct daemon termination has a non-obvious safe limit.
- No convention conflict found in planning: the plan stays in Rust hub CLI/runtime, uses existing daemon client boundaries, avoids new product workflow primitives, and preserves fail-closed diagnostics.
