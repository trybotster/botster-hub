# Harden Stale Daemon Recovery Residual Safety Tests

## Context Loaded

- Pipeline context: `ticket_1783296452_641053`, run `run_1783296527_248329`, step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, open questions, or answers.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]].
- Vault/project context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[plan steps need reviewable plan artifacts]].
- Repo context inspected: `docs/plans/recover-owned-stale-incompatible-daemons.md`, `src/main.rs`, `src/daemon.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `Cargo.toml`/test wrapper shape via existing repo files.
- Current production path: `botster-hub up` enters `local_runtime_up -> prepare_local_runtime -> ensure_local_runtime_daemon`; stale compatibility/protocol failures call `recover_owned_stale_local_runtime_daemon` before spawning a replacement daemon. `botster-hub down` calls `daemon_transport_request(DaemonShutdown)` and uses the same stale recovery helper before returning `IncompatibleDaemon`.
- Current recovery proof: recovery loads `.botster-hub-local-runtime-daemon.json`, checks selected data dir/socket metadata, inspects the live PID with `ps -p <pid> -o command=`, requires a command containing the hub binary name, ` start `, `--data-dir`, and the recorded data dir argument, then sends `SIGTERM`, waits for process exit, removes the selected socket, and removes metadata.
- Current tests already cover owned incompatible recovery, unowned fake socket refusal, scoped socket deletion, and doctor diagnostic behavior. The uncovered adversarial case is forged-looking metadata whose PID belongs to a live non-Botster process.

## Scope

- Add an adversarial lifecycle test in `tests/hub_daemon_lifecycle_test.rs` where metadata points at a live decoy process that is not `botster-hub start --data-dir ...`.
- Drive the actual `botster-hub up` and `botster-hub down` runtime paths against an incompatible socket while that forged metadata is present.
- Assert both commands refuse automatic recovery, leave the decoy process alive, keep the connectable incompatible socket in place, and return the existing manual stale/incompatible diagnostic.
- Tighten `src/main.rs` test-fixture gating for `BOTSTER_HUB_TEST_INCOMPATIBLE_DAEMON` if practical so release-sensitive paths cannot accidentally start the incompatible fixture with only `BOTSTER_ENV=test`.
- Document the accepted PID reuse limit precisely if no further practical macOS verification is added: PID alone is never sufficient; recovery is limited by command-line evidence from the live PID and cannot cryptographically prove process start identity across PID reuse.

## Non-Scope

- No daemon lifecycle rewrite, supervisor redesign, protocol compatibility redesign, or new background ownership primitive.
- No broad process-table scanner or attempt to recover daemons without metadata.
- No changes to `botster-hub-client` DTOs or generated TypeScript protocol artifacts.
- No Rails, SPA, Project Pipelines plugin UI, package lifecycle, or session-worker adoption changes.
- No speculative dependency addition unless implementation proves the standard Unix/macOS process inspection is inadequate for the ticket.

## Assumptions And Unknowns

- Assumption: the decoy test can use a long-lived `/bin/sh -c "sleep ..."` or equivalent child as the non-Botster live process, then clean it up explicitly after assertions.
- Assumption: a fake incompatible Unix socket listener is enough to route `up/down` into stale recovery while the forged metadata provides the dangerous PID.
- Assumption: the existing command-match check should already refuse the decoy because it lacks the hub binary name and `botster-hub start --data-dir` shape; the test should fail if that guard regresses.
- Unknown: whether macOS exposes reliable process start time in a way worth adding without a new crate or brittle `ps` parsing. The default plan is to document the limit unless a small, robust `ps -o lstart=` or equivalent check proves practical.
- Unknown: whether the incompatible fixture should be compiled behind `#[cfg(test)]`; integration tests execute the production binary, so the practical guard may need to be an additional env var with a test-fixture name rather than compile-time removal.

## Botster Layers Touched

- Rust hub CLI/runtime lifecycle.
- Rust integration test harness.
- No plugin, Lua core, TUI, React SPA, Rails relay, MCP, or browser surface changes expected.

## Worktree And Target Assumptions

- Target repo is `trybotster/botster-hub`.
- The pipeline-assigned worktree is the only implementation worktree for this ticket.
- The run target is already bound by Project Pipelines as `tgt_7e208a0c76a44980a83b63af976b1f22`; no additional agent spawning is planned.

## Affected Surfaces And Files

- `tests/hub_daemon_lifecycle_test.rs`: primary change surface for the forged-metadata decoy-PID safety regression test and any helper extraction needed to avoid duplicating fake incompatible socket setup.
- `src/main.rs`: possible narrow change to `TEST_INCOMPATIBLE_DAEMON_ENV` gating and/or a precise comment or diagnostic documenting PID reuse limits.
- `docs/plans/harden-stale-daemon-recovery-residual-safety-tests.md`: this plan artifact.
- Likely read-only: `docs/plans/recover-owned-stale-incompatible-daemons.md`, `src/daemon.rs`.

## Implementation Plan

1. Add or reuse a helper that binds the configured local socket and writes the old/incomplete daemon hello response used by the existing incompatible-daemon tests.
2. Add a decoy child helper that starts a live non-Botster process and returns its PID, with cleanup that terminates it if still alive.
3. Write forged `.botster-hub-local-runtime-daemon.json` for the selected data dir with matching `data_directory`, `data_directory_arg`, `socket_path`, and `hub_bin`, but `pid` set to the decoy process.
4. Run `botster-hub up --data-dir <dir>` with the required local runtime package args only if necessary to reach recovery; otherwise use the smallest command path that triggers `ensure_local_runtime_daemon`.
5. Assert `up` fails with the stale/incompatible diagnostic, the decoy PID still exists, the socket still exists, and the fake listener handled the expected request count.
6. Run `botster-hub down --data-dir <dir>` against the same class of forged metadata and assert the same refusal/alive/socket behavior.
7. Tighten the incompatible fixture guard in `start_daemon` only if this can be done without breaking integration tests that spawn the production binary. Prefer an additional explicit fixture env var over broad runtime configurability.
8. If PID reuse risk cannot be further reduced practically, add precise documentation near `local_runtime_daemon_command_matches` or in the relevant diagnostic path explaining that live command evidence reduces but does not eliminate PID reuse risk.

## Risks

- The decoy cleanup path can leave a long-lived process if the test panics. Mitigation: use a small RAII guard or explicit cleanup after each assertion block.
- Fake listener request counts can make the test flaky if `up` performs more probes than expected. Mitigation: allow enough accepts for current `up/down` probes and fail on timeout rather than exact overfitting.
- Test runtime can grow if full `up` bootstraps packages. Mitigation: keep helper packages minimal and prefer direct command paths already used by existing lifecycle tests.
- Over-tightening fixture gates can break integration tests because they run the compiled binary rather than unit-test-only code. Mitigation: preserve the existing `BOTSTER_ENV=test` plus explicit fixture env requirement and add a second unmistakable fixture token only if needed.
- PID reuse remains a theoretical macOS limit if process start time is not verified. Mitigation: document the exact limit and keep recovery fail-closed unless command evidence matches.

## Acceptance Checks And Tests

- Focused new test: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_refuses_forged_metadata_for_live_non_botster_pid -- --test-threads=1`
- Existing owned recovery still passes: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_recovers_owned_incompatible_daemon -- --test-threads=1`
- Existing down recovery still passes: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_down_recovers_owned_incompatible_daemon -- --test-threads=1`
- Existing unowned refusal still passes: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_refuses_unowned_incompatible_daemon -- --test-threads=1`
- Existing scoped socket cleanup still passes: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_recovery_removes_only_selected_data_dir_socket -- --test-threads=1`
- If `src/main.rs` changes beyond comments/tests, run broader lifecycle coverage: `./test.sh --test hub_daemon_lifecycle_test -- --test-threads=1`.
- Runtime-path proof required for review: show the new test invokes the real `botster-hub up/down` binaries and verifies the decoy process remains alive after recovery is refused.

## Pipeline Gates And Artifacts

- Gate evidence should include this plan artifact path, the loaded vault notes, the checklist id if available, and explicit assumptions about PID reuse and fixture gating.
- No human question is needed unless implementation discovers that the fixture guard cannot be tightened without disabling required integration tests or that the ticket requires a stronger PID reuse guarantee than macOS can support with repo-local primitives.

## Vault Gaps Worth Capturing

- Capture a durable note if implementation settles a Botster convention for forged daemon metadata tests using decoy live processes.
- Capture a durable note if macOS PID reuse mitigation lands on a repeatable `ps`/process-start-time pattern.
- Capture a durable note if the incompatible-daemon fixture guard reveals a general rule for test-only runtime modes in production binaries.
- No convention conflict found in planning: the plan stays in the Rust hub lifecycle layer, uses existing daemon boundaries, avoids broad rewrites, and prioritizes fail-closed safety tests.
