# Eliminate loaded external hub-test-support winsize output loss

## Context loaded

- Project Pipelines run `run_1784249678_139323`, Plan step `botster_plan`, and gate `botster_plan_gate`; no prior artifacts, reviews, findings, questions, answers, or blocking dependencies exist.
- Ticket `ticket_1784227268_801839` and retained GitHub Actions run `29522677842` at exact subject `1e96eff80a05f638c84c3dabfb44453db328657b`. The residual-tail campaign used 48 CPU workers, default lifecycle-suite parallelism, and failed in repetition 1 with `MissingOutput { needle: "winsize:33 102", output: "conformance-ready\r\nfrom-conformance\r\necho:from-conformance\r\nsize-check\r\nquit\r\n" }` while load average exceeded 60.
- Required vault authority: `[[planner-playbook]]`, `[[botster-planner-playbook]]`, `[[botster-architecture]]`, `[[cli-patterns]]`, `[[spa-patterns]]`, `[[prefer framework and library components over custom solutions]]`, `[[project pipeline orchestration belongs in a device-level botster plugin]]`, `[[project pipelines needs an operator workbench not more primitives]]`, `[[project pipelines ui contract belongs in the plugin readme]]`, `[[botster orchestration should spawn agents with explicit target ids]]`, `[[botster orchestration prompts must bind agents to explicit worktrees]]`, `[[botster pipeline needs continuous product owner between agent steps]]`, and `[[plan agents must author vault context as wikilinks not home paths]]`.
- Repository evidence from `crates/botster-hub-test-support/src/lib.rs`, `crates/botster-hub-client/src/lib.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/runtime.rs`, `tests/hub_daemon_lifecycle_test.rs`, and the `botster-core` revision pinned in `Cargo.lock`.
- Botster layer: Rust same-device hub client and real-daemon lifecycle/conformance tests. Target/worktree assumption: implementation stays on this ticket's explicit Botster Hub target and assigned Project Pipelines worktree.

## Diagnosis and production path

`run_client_conformance` uses the public production path:

`botster_hub_test_support::run_client_conformance` -> `botster_hub_client::stream_attach` and daemon requests -> `src/daemon_transport.rs` -> `HubClientApi` -> `HubRuntime` -> pinned core `CoreDaemon` -> `SessionIo`/`ClientWorker` -> session-worker PTY.

The resize response proves the hub accepted and forwarded resize. In the pinned core, resize and the following PTY inputs are serialized onto the same session-worker control writer, so their order is preserved. The failed transcript contains TTY echo for `size-check` and `quit`, but not the later `stty` result or `conformance-bye`. The public `stream_attach` helper currently returns unconditionally after 20 empty drains (about 500 ms), even when its `ListSessions` readback reports the session is still running. Under starvation it therefore detaches before the worker processes and publishes the final output. The evidence points to premature client detachment/output loss, not a lost PTY resize acknowledgement.

This is a production-path correction, not test-only masking: the helper's public contract already says it streams until session exit or connection closure.

## Scope

1. Add a focused `botster-hub-client` regression that scripts an attached session through at least the current idle-drain threshold, reports the session still running, then emits terminal output plus process exit. Assert the helper remains attached and returns the late output. The test must fail when the fix is reverted.
2. Change `stream_attach_connected` so an idle lifecycle readback only completes attachment when the session is actually exited. A running session continues draining; completion remains driven by `ProcessExit`, exited lifecycle readback, or transport closure/error. Do not increase the threshold or add another elapsed-time escape hatch.
3. Update `cli_daemon_restart_recovers_worker_backed_session_through_transport`, which currently joins a still-running attach only because of the undocumented idle return, so it explicitly terminates the recovered session before joining and still proves the post-restart echo was delivered.
4. Keep the existing external `run_client_conformance` assertions strict: ready, echo, and exact `winsize:33 102` must all arrive through the real isolated daemon path.

## Non-scope

- No retries, new fixed sleeps, timeout inflation, test serialization, reduced residual-tail load, changed Cargo parallelism, or weakened conformance assertions.
- No changes to PTY resize framing, core/session-worker actor ordering, egress capacities, daemon protocol DTOs, support matrix contents, workflow runner, or campaign budgets unless implementation evidence disproves the diagnosis and Plan Review re-scopes the ticket.
- No new public configurability or generalized streaming abstraction.
- No changes to React SPA, TUI presentation, Lua plugins, Rails relay, MCP, npm package assets, or Project Pipelines workflow policy.

## Assumptions and unknowns

- Assumption: the retained failure's missing final lines are stranded late egress caused by `stream_attach` returning while lifecycle is still `running`; source ordering and transcript evidence support this.
- Assumption: `ProcessExit` remains the normal completion signal and the existing lifecycle readback is only a fallback for an already-exited session.
- Assumption: no compatibility boundary requires preserving the undocumented “return after roughly 500 ms of idle while still running” behavior; it contradicts the helper's documented contract and the CLI attach behavior.
- Unknown to settle during implementation: whether the focused scripted socket test can reuse existing frame helpers and DTO fixtures without adding test-only production API. Prefer private module-test helpers.
- Unknown to verify, not silently assume: whether any other test or caller relies on idle return. Current Rust call-site search finds only the conformance flow and the restart lifecycle test in addition to production CLI wiring.
- If evidence shows resize/input ordering is not preserved at the pinned worker boundary, stop and ask for re-scope rather than adding synchronization policy to this ticket.

## Affected surfaces/files

- `crates/botster-hub-client/src/lib.rs` — production `stream_attach_connected` lifecycle rule and focused regression test.
- `tests/hub_daemon_lifecycle_test.rs` — necessary correction to the recovered-session test's explicit exit/join ordering; existing external conformance test remains the real runtime acceptance entry point.
- `crates/botster-hub-test-support/src/lib.rs` — inspected and exercised, but no planned assertion or timing change.
- `src/daemon_transport.rs`, `src/client_api.rs`, `src/runtime.rs`, `Cargo.lock`, `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, and `docs/loaded-daemon-lifecycle-runner.md` — inspected verification/production-path evidence, not planned edits.
- This plan document is the reviewable Plan artifact.

## Implementation sequence

1. Write the deterministic client regression first and record it red against the current unconditional idle return.
2. Make the smallest branch correction: continue draining when lifecycle readback says the session is running; retain existing exit and transport-error completion paths.
3. Correct the adjacent restart lifecycle test to explicitly end the session before joining its attach thread, then preserve its echo and cleanup assertions.
4. Run focused crate and real-daemon tests, prove red on revert, restore the fix, and run the unchanged loaded campaign at the exact fixed SHA.

## Risks

- Removing the premature idle return exposes callers/tests that treated a held-open attach as a bounded read. Mitigation: audit all call sites and update only the one test that conflicts with the documented production contract.
- A missed `ProcessExit` could leave attachment open. Mitigation: preserve the existing lifecycle readback and test the `running` versus `exited` decision explicitly; do not replace it with a longer timeout.
- Concurrent lifecycle state could flip after an empty drain. Continuing while `running` is safe; the next drain/readback must observe terminal egress or exit rather than silently detaching.
- Loaded campaign noise includes sibling failures. Require exact evidence for every first red and do not call unrelated failures ticket-owned without matching the signature and path.

## Acceptance checks/tests

1. `cargo test -p botster-hub-client` passes, including a regression proving late terminal output after the current idle window is retained while lifecycle remains running.
2. Red-on-revert proof: revert only the lifecycle decision while keeping the focused regression; the regression fails because attach returns before the late terminal output. Restore the fix and rerun green.
3. `./test.sh --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport -- --nocapture` passes with explicit session exit before attach join.
4. `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol -- --nocapture` passes through real binaries and preserves exact `winsize:33 102`, ready, and echo assertions.
5. Run the complete repo-approved test wrapper required by the implementation/review gate; record any failure with exact test, command, and why it is related or unrelated.
6. Dispatch `.github/workflows/loaded-daemon-lifecycle.yml` at the exact fixed subject SHA with `test_target=lifecycle-suite`, `repetitions=20`, and `stress_profile=residual-tail`. Preserve default Cargo parallelism, existing budgets, first-red behavior, artifacts, resource samples, and owned-process cleanup. The ticket-owned external hub-test-support test must pass in every completed repetition; no new unmapped first-root failure is acceptable.
7. Verify repository wiring with `rg -n "stream_attach|external_hub_test_support_drives_isolated_daemon_socket_protocol|cli_daemon_restart_recovers_worker_backed_session_through_transport" crates src tests docs` and confirm the changed helper remains the production CLI/test-support entry point.

## Pipeline gates and artifacts

- Plan gate: this committed plan plus Project Pipelines plan and vault checklists.
- Implement evidence: focused red/green output, changed-file rationale, call-site audit, and exact commands.
- Review evidence: correctness, regression, public-contract fit, no forbidden timing workaround, no unwired/dead code, and exact treatment of unrelated failures.
- Verify evidence: restored green focused tests, exact-SHA residual-tail run URL/artifact, cleanup evidence, and resolved-finding rechecks against the live worktree.

## Vault gaps worth capturing

- Capture candidate after verification: `[[held-open stream helpers must not convert idle polls into successful completion]]` if the regression confirms this general Botster client rule. It is durable because the same idle-window assumption already appears in older plan artifacts and can recur in other streaming helpers.
- Capture candidate after verification: `[[pty resize and input ordering share the session worker control stream]]` only if implementation evidence confirms the pinned-core mechanism is stable enough to guide future diagnosis.
- Do not capture either as established knowledge during Plan; record final evidence and provenance through the inbox-first vault pipeline after verification.

## Convention check

- Conflicts: none. The plan uses the existing client protocol and lifecycle primitives, changes the smallest production branch, avoids new abstractions/configuration, and keeps every test assertion tied to the ticket.
- Durable artifact references use wiki-link note titles rather than runtime-local vault paths.
