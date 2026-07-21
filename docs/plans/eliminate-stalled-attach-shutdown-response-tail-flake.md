# Eliminate Stalled-Attach Shutdown Response Tail Flake

## Context loaded

- Current Project Pipelines context: replacement ticket `ticket_1784608438_764334`, run `run_1784608438_712965`, active step `botster_plan` (`run_step_1784608438_373347`), required gate `botster_plan_gate`, no current-run findings, reviews, artifacts, or unanswered questions at plan time.
- Replacement provenance: routed artifact `artifact_1784608426_222915`, replacement artifact `artifact_1784608469_398593`, and binding human answer `question_1784608185_592035` require a genuinely new clean branch/worktree at exact base `a0b61235b21824814f86e540912988ef8e3ec932`; PR #147 and commits `1c4af77`/`e6ed8fa` are historical source evidence only and must not be merged or cherry-picked wholesale.
- Base proof at planning time: `HEAD`, local `main`, local `origin/main`, and live `refs/heads/main` all resolve to `a0b61235b21824814f86e540912988ef8e3ec932`; `git status --short --branch` reports no worktree changes before this fresh plan artifact.
- Captured failure contract: `stalled_attach_stdout_does_not_block_other_daemon_commands` reached the shutdown stage and returned status 1 with stderr `botster-hub shutdown error: client disconnected` while the deliberately stalled `attach_child` remained running. This exact error remains a failure, not successful cleanup.
- Required planning authority: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Test and evidence constraints: [[full suite hangs need source and behavior proof before unrelated waivers]], [[a poisoned test lock is a symptom not a waiver]], [[suite wide acceptance criteria make every observed test failure in scope]], [[test script required for rust tests not cargo test]], [[botster test sh forwards arguments to cargo not custom unit flags]], [[a regression test must be shown to go red with the fix reverted]], [[narrow ablation at the enforcement point is the cleanest regression negative control]], [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]], and [[refresh target branches before mitigating failures owned by sibling tickets]].
- Repository evidence inspected: current `src/daemon_transport.rs`, `src/local_webrtc.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/support/mod.rs`, `crates/botster-hub-test-support/src/lib.rs`, `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, `test.sh`, README/prior `docs/plans/` placement, PR #147 metadata/diff, and the ticket-owned historical commits.
- Production runtime path: `src/main.rs::operator_shutdown` sends `DaemonRequest::DaemonShutdown`; `serve_daemon` accepts the Unix connection; `handle_connection` submits it to `handle_control_message`; the control loop creates the shutdown response and currently returns `true` immediately after sending it to the connection thread; `serve_daemon` can therefore call `daemon.stop()` and exit before `write_frame` finishes.
- Runner path: the committed workflow checks out an exact full SHA, precompiles `hub_daemon_lifecycle_test`, and invokes `script/run-loaded-daemon-lifecycle` at default Rust test concurrency under bounded `residual-tail` load with first-red preservation and cleanup artifacts.
- Workflow discipline: run checklists `Plan workflow discipline` and `Plan vault discipline` hold context, convention, verification, and capture evidence. The same facts are duplicated here and in gate evidence so the plan remains reviewable.

## Scope

1. Audit PR #147 and commits `1c4af77`/`e6ed8fa` against exact current main, then manually reapply only the smallest ticket-owned diagnostics and shutdown-response ordering behavior that current code still needs.
2. Preserve behavior-neutral stalled-attach diagnostics for backpressure samples, elapsed command status/stdout/stderr, and attach-child state at each assertion boundary.
3. Gate daemon stop on the originating shutdown transport completing a response delivery attempt. The live Unix path must not stop between handing the response to its connection thread and that thread returning from `write_frame`.
4. Keep the shared `ControlMessage::Request` contract coherent for both current request producers: Unix socket and local WebRTC. Adapt the acknowledgement to main's current queued/chunked WebRTC sender and peer-close diagnostics; do not replace that state machine with stale code.
5. Keep signal-initiated shutdown response-less because it has no requesting transport client.
6. Remove the narrow cleanup-harness tolerance that reclassifies exact `client disconnected` after a clean daemon exit as success.
7. Add deterministic success, close/drop, and write/send-error coverage so the single control loop cannot deadlock waiting for an acknowledgement that never arrives.
8. Prove the production CLI/Unix runtime path and then bind the exact committed SHA through the required 20-run loaded lifecycle-suite campaign.

## Non-scope

- No wholesale merge or cherry-pick of PR #147, its stale branch, or either historical commit.
- No timeout increase, new retry, fixed sleep, suite serialization, skipped test, reduced flood/backpressure pressure, or weaker attach-liveness/concurrent-command assertion.
- No replacement or simplification of merged WebRTC response chunking, flow control, terminal-cause persistence, peer-close behavior, or sibling-ticket fixes.
- No change to PTY/session-worker data flow, list/send-input/resize semantics, CoreDaemon ownership, response vocabulary, public client DTOs, or dependencies.
- No new abstraction, feature flag, operator configuration, or general delivery protocol beyond the shutdown-only completion signal required by the existing control loop.
- No edits to the loaded-runner workflow/script unless current-main execution proves the harness itself broken. Such a result requires a specific finding and re-plan, not silent scope growth.
- No Lua plugin, TUI, React SPA, Rails relay, MCP, or unrelated documentation work.

## Botster layers touched

- Rust hub control plane and its Unix/local-WebRTC transport adapters.
- Rust real-daemon lifecycle integration tests and downstream-shaped hub test-support cleanup behavior.
- Repository plan artifact and Project Pipelines gate/checklist evidence.

## Assumptions and unknowns

- Fact: the failure stage and observable contract are known; reproduction discovery is no longer a prerequisite to planning the repair.
- Root-cause hypothesis to prove: the control loop's stop decision wins a scheduler race with the connection thread's final shutdown response write.
- Assumption: a shutdown-only completion receiver on the existing control request is the smallest suitable synchronization. The control loop should wait only for a response delivery attempt, not unrelated connection teardown.
- Assumption: existing Unix write bounds and current WebRTC bounded sender behavior remain the timeout owners. The repair adds no independent wall-clock sleep or inflated budget.
- Unknown until implementation audit: the precise acknowledgement point inside main's current WebRTC queue/chunk sender. It must mean the terminal response send attempt has completed (success or terminal failure), not merely that a response was queued.
- Unknown until implementation audit: whether an ownership/drop-based acknowledgement can cover every Unix/WebRTC cancellation path more safely than explicit sends. The selected shape must have deterministic dropped-client and failed-write tests.
- Assumption: diagnostics from `1c4af77` remain behavior-neutral after rebasing onto current test helpers; retain only fields useful to identify the first red root.
- Worktree binding: implementation remains in this pipeline-provided worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`. Before implementation and before dispatching loaded CI, re-fetch/verify main; if it advances beyond the ticket's exact allowed base, stop and ask rather than silently rebasing.
- No Plan-time human question is required: the replacement ruling, captured assertion, and non-goals select one surgical behavior change without a waiver.

## Affected surfaces and files

- `src/daemon_transport.rs`
  - Carry shutdown-only delivery completion through the existing control request.
  - Delay the final stop decision until the Unix/WebRTC transport has attempted the shutdown response.
  - Preserve non-shutdown and signal behavior.
  - Add deterministic ordering and Unix write-failure/drop coverage.
- `src/local_webrtc.rs`
  - Wire the shared completion contract into the current response queue/chunk/peer-close path without reverting merged WebRTC behavior.
  - Acknowledge only at the existing bounded terminal send outcome, including failure/close.
  - Add focused tests at the current sender seam if daemon transport tests cannot exercise those outcomes.
- `tests/hub_daemon_lifecycle_test.rs`
  - Selectively restore stage diagnostics from `1c4af77`.
  - Keep the real stalled-attach CLI test's concurrency meaning unchanged.
  - Reject exact shutdown disconnect after a clean daemon exit.
- `tests/support/mod.rs`
  - Remove exact-disconnect success masking from shared CLI-daemon shutdown validation.
- `crates/botster-hub-test-support/src/lib.rs`
  - Remove matching masking from `IsolatedHub` and update its deterministic classification coverage.
- `docs/plans/eliminate-stalled-attach-shutdown-response-tail-flake.md`
  - This fresh reviewable plan artifact.
- Acceptance inputs, not expected edits: `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, and `test.sh`.

## Implementation sequence

1. Reconfirm clean exact-base provenance and compute the current diff for every PR #147 file. Classify each hunk as ticket-owned, already superseded by main, or unrelated; apply patches manually rather than cherry-picking commits.
2. Restore only the stalled-attach diagnostic observations that do not change timing or behavior.
3. Add a shutdown-only delivery-completion owner to the existing control request. Unix signals after `write_frame` returns on success or error; signal shutdown has no owner.
4. Integrate the same contract at the current WebRTC terminal-send seam, preserving queue order, chunking, flow control, peer-close wakeups, and terminal diagnostics already merged to main.
5. Add deterministic tests proving: response availability alone does not release stop; successful write/send releases it; client/ack owner drop releases it without deadlock; and failed Unix/WebRTC delivery releases it while preserving the delivery error.
6. Remove exact-disconnect masking from both cleanup harnesses and update their tests. Keep `client disconnected` visible as the captured regression signature.
7. Run focused tests and a narrow ablation that bypasses only the wait/ack enforcement. The committed regression filter must return nonzero under ablation, then return green after exact restoration with `git diff` proving no ablation remains.
8. Run the default-concurrency lifecycle suite plus workspace formatting, strict lint, full test, and whitespace checks. Commit only after all local gates are green.
9. Dispatch the existing runner against the exact 40-character committed SHA: a focused stalled-attach smoke may diagnose quickly, but only 20 consecutive `lifecycle-suite` runs at default concurrency under `residual-tail` satisfy acceptance.
10. Stop at the first red. Preserve the assertion/panic, subject SHA, run URL, resource samples, process cleanup, and first non-cascade root. Every newly observed suite root remains blocking until fixed, consumed from a merged sibling owner and rerun, or explicitly re-scoped by a human.

## Risks

- **Control-loop deadlock:** a transport exits without signalling. Mitigate with ownership/drop semantics or exhaustive success/error/close tests for every request producer.
- **Acknowledgement too early:** signalling when queued rather than after the final write/send attempt recreates the race. Test the blocked interval deterministically.
- **WebRTC regression:** replaying `e6ed8fa` verbatim would overwrite the newer sender state machine. Make only additive, seam-local changes against current main and run focused WebRTC coverage.
- **Error masking:** a completion signal must not convert a failed response write/send into success. Preserve the original transport error and terminal diagnostics.
- **Stale provenance:** PR #147 is currently open, dirty, and non-mergeable; using its branch as a base would violate the binding ruling. Revalidate live main and exact subject SHA at each evidence boundary.
- **Diagnostic observer effect:** extra formatting/allocation can worsen load-sensitive tests. Keep bounded recent samples and avoid work proportional to total output.
- **False regression proof:** helper-only green tests do not prove the live path. Require both narrow-ablation red and compiled CLI/daemon integration evidence.
- **Suite-wide newly exposed roots:** poisoned-lock cascades or different lifecycle failures remain real blockers. Isolate the first root; do not serialize or retry past it.

## Acceptance checks and tests

- Provenance before edits: clean worktree and `HEAD == origin/main == live refs/heads/main == a0b61235b21824814f86e540912988ef8e3ec932` (unless a human explicitly revises the exact-base ruling).
- Focused deterministic ordering/error/drop tests through `./test.sh` using valid Cargo-shaped filters.
- Focused real path:
  - `./test.sh --test hub_daemon_lifecycle_test stalled_attach_stdout_does_not_block_other_daemon_commands -- --exact --nocapture`
  - exact-disconnect rejection tests in the lifecycle and hub-test-support surfaces.
- Narrow-ablation evidence: record the single enforcement expression bypassed, nonzero command status, exact red tests, exact restoration, and final green rerun.
- Local gates:
  - `./test.sh --test hub_daemon_lifecycle_test` at default concurrency.
  - `./test.sh` for the workspace.
  - `cargo fmt --all -- --check`.
  - strict workspace clippy using the repository/Cargo lint policy discovered at implementation time; record the exact command and result rather than substituting a lighter check.
  - `git diff --check` and a final diff audit proving every changed line maps to the ticket.
- Runtime-path proof: the compiled CLI sends shutdown over a live Unix daemon while attach stdout remains blocked; list/send-input/resize stay responsive; shutdown receives a successful response; only then does `serve_daemon` stop.
- Binding CI evidence: exact committed SHA, exact-subject checkout proof, exact-target precompile, then 20 consecutive default-concurrency `lifecycle-suite` repetitions under `residual-tail`; no retry masking and no root dismissal. Attach run URL/artifact id, command/status table, resource samples, and cleanup/no-survivor evidence.

## Pipeline gates and artifacts

- Plan gate: attach this document plus the required structured fields and checklist evidence; no waiver is requested.
- Implementation handoff artifact: exact audited hunk classification, changed-file rationale, production entry-point proof, local command/status table, narrow-ablation red/green proof, and committed SHA.
- Verification artifact: exact-SHA loaded workflow URL/artifact, all 20 repetition results, first-root disposition for any red, and cleanup evidence.
- Product decision ledger remains binding: fresh exact-main base; historical PR only as selective source evidence; preserve merged WebRTC; no timeout/retry/serialization/assertion weakening; every suite root blocks unless human-re-scoped.

## Required docs or plugin README updates

- This plan is the only planned documentation change. The ticket changes an internal shutdown ordering guarantee and test behavior, not a public CLI, plugin, MCP, or UI contract; README/client-protocol updates are not required unless implementation reveals a user-visible contract change, which would require re-plan.

## Vault gaps worth capturing

- Capture candidate after successful replacement completion: Project Pipelines restart attempts can deterministically reuse a stale ticket branch/worktree even when a binding answer requires fresh-main convergence; replacement child/sibling ticket routing may be required to obtain a unique workspace.
- Capture candidate only if implementation proves a reusable invariant not already covered: shared control-loop shutdown responses need transport-owned completion/drop acknowledgement before runtime teardown across Unix and WebRTC.
- No convention conflict found. The plan follows the existing control-plane boundary, current transport primitives, repo wrapper, exact-SHA runner, and suite-wide acceptance notes without adding an abstraction or waiver.
