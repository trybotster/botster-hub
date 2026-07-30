# Leave no orphan session workers, zombies, or stale sockets

## Target repository and context loaded

- Project Pipelines ticket `ticket_1785199707_999968`, run `run_1785384360_722049`, Plan step `run_step_1785384361_735600`, and gate `botster_stack_plan_gate`.
- Authoritative target `tgt_7e208a0c76a44980a83b63af976b1f22` resolves to `trybotster/botster-hub`. This ticket worktree's `origin` is the same repository. Planning subject and `origin/main` are `527ba0a58215531bf5b777a438887bd61f77b6fc`.
- Repository ownership charter loaded: [[botster-hub-playbook]].
- Role and surface playbooks loaded in required order: [[planner-playbook]], [[botster-planner-playbook]], [[botster-hub-playbook]], targeted notes, then [[project-pipelines-playbook]]. The Project Pipelines playbook applies to checklist, artifact, gate, and advancement discipline; no Project Pipelines package/plugin implementation is in scope.
- Architecture and surface guidance loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], and [[botster-runtime-reviewer-playbook]]. The affected product surface is Rust Hub/Core/session-worker lifecycle and test orchestration; Rails, SPA, TUI, and Lua/plugin implementation guidance is not implicated.
- Targeted ownership and lifecycle notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[may supervise permits the hub to supervise the package entrypoint]], [[hub supervision admission changes require exact live hub launch proof]], [[live hub proof records distinct hub and locked core binary provenance]], [[webrtc bootstrap origin must be requested after the package server binds]], [[plugin worker queue capacity and executor concurrency are independent host profile knobs]], [[durable state version preflight must precede shape deserialization after cold turkey changes]], [[pty master fd close sends sighup but ignores it needs killpg]], [[subprocess harnesses must kill child on failed readiness]], [[worker shutdown completion requires lifecycle transport and process termination]], [[daemon shutdown disconnects count as success only after clean owned process exit]], [[botster runtime artifact resolution should be read only]], [[botster hub socket cleanup must preserve connectable sockets and repair missing socket paths]], [[plugin worker watchers can block tokio runtime shutdown]], [[bounded command execution requires process group termination and reaping]], [[test script required for rust tests not cargo test]], [[daemon session cleanup should report typed cleanup frames for shutdown races]], [[supervised entrypoint terminal states wait for bounded output finalization after child reaping]], [[hub shutdown responses should report post stop status or label pre stop snapshots]], [[workflow cancellation cleanup is idempotent across campaign traps and outer steps]], [[sid scoped census is blind to setsid session leaks]], and [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]].
- Project Pipelines planning notes loaded: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[pipeline artifacts should cite vault notes by wikilink not home path]], and [[vault example paths are not repository placement conventions]].
- Repository context inspected: `README.md`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, `src/main.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/entrypoint_supervisor.rs`, `src/managed_git_worktrees.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/support/mod.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_mcp_test.rs`, and related plans under `docs/plans/`.
- Closed prerequisite `ticket_1785199697_321375` is present in pipeline context. Its bounded daemon connection-task cleanup is treated as landed, not reimplemented here.

## Binding lifecycle decision

Human answer `question_1785384715_650936` resolves the ticket's only material ambiguity:

- Preserve `HubRuntime::release_for_restart()` and the documented intentional-restart adoption contract. Ordinary `botster-hub down` or daemon shutdown must not silently terminate user sessions.
- A worker may survive only if it has durable session identity, secure attributable control state, and is successfully adoptable by the restarted Hub. This is an intentional survivor, not an orphan.
- Boundedly terminate and reap the Hub daemon, supervised package entrypoints and helpers, failed or partially spawned workers, explicitly shut-down or deleted sessions, test-owned processes, and every worker that is not validly adoptable.
- Remove only owned sockets/directories, and only after the owning process is confirmed exited. Tests must prove both sides: intentional restart adoption followed by explicit session shutdown, and zero unowned workers, zombies, or stale sockets.

Any proposal to change `release_for_restart()`, overload `down` as “terminate all sessions,” kill a merely name-matched process, or waive an unclassified survivor requires a new human decision.

## Scope

- Audit every process-spawn boundary reachable from Hub production paths and Hub-owned tests: local runtime daemon, Core-backed session worker observation, package entrypoints, managed Git helpers, isolated Hub fixtures, CLI/PTY helpers, and the loaded lifecycle wrapper.
- Establish ownership immediately after successful spawn, before readiness, metadata publication, socket connection, or assertions can fail. Use surface-local guards and existing process-group primitives; share a helper only where ownership and cleanup semantics are genuinely identical.
- Make failure, timeout, unwind/drop, cancellation, explicit shutdown, and normal completion converge on bounded group termination where appropriate, direct-child `wait`/reaping, reader/task finalization, and identity-checked artifact removal.
- Preserve diagnostic context with the original failure as primary and a typed/structured cleanup failure containing the spawn source, PID/process group or durable session identity, attempted phases, timeout, and remaining owned resources.
- Give every test/run-owned process an attributable label. Prefer the existing inherited loaded-run token plus exact executable, PID/process group, data directory, session ID, and control socket. Do not rely on SID alone because workers may call `setsid`.
- Extend test and loaded-run teardown proof to classify all survivors. Require an intentional survivor to be found in durable session/control state, adopted successfully after restart, explicitly shut down, and then absent. Any other new process, zombie, listener, socket path, or test data directory is a failure.
- Keep cleanup idempotent so a normal teardown followed by `Drop`, or CI cancellation followed by final verification, cannot kill an unrelated replacement or turn success into a false failure.

## Non-scope

- No change to the intentional session restart/adoption contract or `release_for_restart()`.
- No user-facing “terminate all sessions” mode.
- No broad `pkill`, process-name sweep, global socket deletion, unrelated-process kill, or cleanup outside exact run/test ownership.
- No speculative lifecycle framework, optional cleanup configuration, timeout inflation, test serialization, sleep-based masking, or adjacent refactor.
- No replacement of Core session-worker protocol, control state, or worker implementation in this repository.
- No Rails, web UI, TUI, plugin package, Project Pipelines package, or unrelated documentation work.
- No four-package live workload, long-lived reconnect-churn threshold, or captured cross-platform operator diagnostic recipe. Those downstream integration deliverables belong to sibling `ticket_1785199716_875648`, which is sequenced after the lifecycle fixes. This ticket owns spawn-boundary fixes plus focused and repository-wrapper zero-residual proof on both Linux and macOS.

## Repository ownership boundaries and cross-repository dependencies

| Boundary | Owner | This ticket's responsibility |
| --- | --- | --- |
| Hub daemon, socket/metadata, package entrypoints, Git helpers, Hub connection tasks | `botster-hub` | Implement and prove bounded, identity-scoped lifecycle cleanup. |
| Hub integration fixtures, CLI/PTY children, temp data roots, loaded lifecycle wrapper/workflow | `botster-hub` | Make ownership panic/cancel safe and prove zero residual owned state. |
| Session-worker spawn, worker control socket, explicit session shutdown, restart adoption internals | `botster-core` | Consume and verify the pinned contract; do not patch Core from this run. |
| Terminal child tree below a session worker | `botster-core` and its terminal backend | Verify through the Hub's real runtime path; defects require a dependency ticket in the owning repository. |

`Cargo.lock` pins Core revision `5846fc776d31e2b6c98a8d932f50a31078743901`. Its `PendingWorker` already takes ownership immediately after spawn, kills/waits a worker on startup failure, and removes only an unchanged refused control socket. This audit does not yet establish a required Core change. If focused implementation tests show that a worker descendant escapes that contract, adoption cannot be securely attributed with existing APIs, or explicit shutdown returns before Core has reaped/removed its owned state, register a blocking dependency against botster-core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`; do not add a Hub-side broad kill workaround. The existing closed Hub prerequisite remains the only current pipeline dependency.

## Assumptions and unknowns

### Assumptions

- The inherited loaded-run token reaches Hub-spawned Core workers and remains observable even if a worker creates a new session, so the wrapper can census escaped descendants without name-only matching.
- Process groups are appropriate for Hub-owned daemon, entrypoint, Git-helper, and test-helper trees. Durable worker adoption is classified through session/control identity instead of assuming group membership.
- A socket or directory can be removed only after exit/reap evidence and an identity check showing it still belongs to the same process/run; a live replacement always wins over cleanup.
- Cleanup errors must remain visible without replacing the triggering test or production error. Successful cleanup is not a reason to erase the original failure.
- Default-parallel tests are the acceptance path. A serial-only green result is diagnostic evidence, not completion.

### Unknowns

- The spawn audit may find that some raw test children already have equivalent guards under different helper types. Consolidate only touched, identical semantics rather than bulk-retrofitting unrelated test code.
- macOS and Linux expose zombie state, process groups, environments, and Unix listener ownership differently. Implementation must close this difference with Darwin and Linux read-only census adapters behind the same ownership predicate; it is not deferred to the sibling integration ticket.
- A package entrypoint may fork or `setsid`. The implementation must prove whether process-group ownership plus run-token attribution is sufficient before claiming cleanup.
- Rust cannot reap a successfully detached daemon after the spawning CLI exits because it is no longer the parent. The shutdown path must distinguish “boundedly observed exited” from “directly waited/reaped by this process,” while the actual parent-owned test path must still wait the child and assert no zombie.
- Exact new test/helper names should follow the smallest seam found during implementation. The behaviors below are fixed; names are not.

## Affected surfaces/files

- `src/main.rs` — local runtime daemon process-group ownership, readiness/metadata failure cleanup, shutdown completion, and artifact ordering.
- `src/daemon.rs` — preserve the delegation to runtime restart release and aggregate/report bounded daemon shutdown outcomes.
- `src/runtime.rs` — own the legitimate-survivor/non-adoptable classification where `release_for_restart`, `adoption_scan`, `adopt_session`, `shutdown_session`, the restart adoption loop, and stale-worker-control-socket recovery already live. The new predicate must preserve `is_stale_worker_control_socket_adoption_error` recovery rather than duplicating or contradicting it.
- `src/daemon_transport.rs` — read-only or minimal wiring changes only if connection-task completion must participate in the final cleanup result.
- `src/entrypoint_supervisor.rs` — readiness/watch failure, stop/drop group termination, direct-child wait, output finalization, launch-result cleanup, and source-labelled diagnostics.
- `src/managed_git_worktrees.rs` — bounded group termination and wait for timed-out or failed Git helper trees.
- `crates/botster-hub-test-support/src/lib.rs` — isolated Hub ownership from spawn through panic/drop, real shutdown, process wait, and data-root cleanup.
- `tests/support/mod.rs` — shared test ownership/census helpers where semantics are common.
- `tests/hub_daemon_lifecycle_test.rs` — production-path failure, restart adoption, explicit shutdown, timeout, panic/drop, and residual-state coverage.
- `tests/hub_mcp_test.rs` — replace raw lifecycle gaps with the same owned fixture semantics and prove MCP test teardown.
- `script/run-loaded-daemon-lifecycle` — add a closed-enum `focused-process-ownership` target bound to the new `process_ownership_` lifecycle/adoption and negative-path tests; split Linux load generation from the common campaign/census path; add Darwin and Linux read-only adapters for exact run-owned process/zombie/listener/socket/data-root baselines; retain idempotent cancellation cleanup and the final zero-residual report.
- `.github/workflows/loaded-daemon-lifecycle.yml` — expose `focused-process-ownership` as a workflow input and run/upload the Linux loaded evidence. The required Darwin run is a named local acceptance procedure because this workflow has no macOS runner; adding a macOS CI matrix or the sibling ticket's captured operator recipe is non-scope.
- `README.md` or lifecycle documentation only if externally visible shutdown/adoption diagnostics change; do not restate unchanged behavior.
- This plan artifact.

Every changed line must trace to a spawn owner, a required cleanup transition, identity-safe artifact handling, an acceptance assertion, or cleanup made necessary by those changes.

## Implementation plan

1. Build a checked spawn ledger for each affected production and test boundary. Record executable/source label, parent owner, PID, process group/session behavior, readiness point, socket/data-root ownership, normal handoff, and every early-return/unwind edge. Mark the exact owner before proceeding past `spawn`.
2. In the local runtime daemon path, use the already-created daemon process group for every pre-handoff failure, including metadata publication, readiness EOF/error/timeout, and probe failure. Terminate the owned group with a bounded TERM/KILL sequence, wait the direct child whenever still owned, finalize piped readers, and remove metadata/socket only after exit and identity validation. Successful daemon detachment remains independently managed through its PID metadata.
3. In `EntrypointSupervisor`, make readiness and watch failures actively stop the registered process group and wait the direct child before returning. Make explicit stop, `stop_all`, and a last-resort `Drop` path idempotent; finalize output before terminal publication and delete the owned launch-result file only after process completion. Report cleanup phase/source identity rather than silently swallowing kill/wait failures.
4. Put managed Git commands in owned groups on Unix and apply the same bounded group terminate plus direct-child wait on timeout/error. Preserve bounded output collection and the original Git diagnostic.
5. Strengthen Hub test fixtures so Hub, CLI/PTY helpers, requested sessions, reader threads, sockets, and temporary data roots are registered as they are created. Teardown order is: explicitly stop test-owned sessions/entrypoints that are not restart survivors; stop the Hub through the real command path; boundedly terminate only remaining owned groups; wait/reap direct children and join readers; verify listeners are gone; then remove owned sockets/directories. `Drop` must cover assertion panic and partial construction without hiding the panic.
6. Preserve daemon restart release semantics in `src/runtime.rs`. Add an explicit test predicate for a legitimate survivor around the existing `release_for_restart` -> `adoption_scan`/`adopt_session` path: durable session ID, matching attributable control socket/registry state, successful adoption by a restarted Hub, and subsequent `shutdown_session` with confirmed process/control-socket removal. Preserve the existing stale-control-socket recovery classification. Treat partial workers, failed spawns, deleted sessions, failed adoption, or unmatched control state as leaks.
7. Extend the wrapper's clean baseline and final census behind one ownership predicate. The Linux adapter may use `/proc`, while the Darwin adapter must use bounded `ps`/`lsof` and recorded run metadata; both combine run token where observable, exact executable path, recorded PID/group, data-root boundary, session/control identity, and listener ownership, and both count zombie state separately. Separate Linux-only synthetic load validation/generation from common no-load campaigns so `--stress-profile none` and the new `focused-process-ownership` target run on macOS. Track intentional survivors as a finite expected set, prove adoption and explicit shutdown, and fail on every unexplained delta. Cancellation cleanup may act only on recorded/run-attributable ownership and must be safe to run twice.
8. Add deterministic negative-path injection at existing seams rather than timing luck: metadata write/readiness failure, entrypoint readiness/watch failure, helper timeout, partial fixture construction, panic/unwind, and explicit cancellation. Each test asserts the original error plus bounded cleanup and absence of its exact process/socket/data-root.
9. Keep changes surgical. If an enforcement point must live in Core, stop that portion, register the Core dependency with the failing evidence and required contract, and keep the Hub plan scoped to its owner.

## Risks

- **Killing user or concurrent-test work:** a process name, socket prefix, or SID is insufficient ownership. Require recorded/run-attributable identity and validate it immediately before signaling or unlinking.
- **Breaking durable sessions:** treating every post-`down` worker as a leak would destroy restart continuity. Require the explicit survivor/adoption predicate and test both adoption and later explicit cleanup.
- **Missing `setsid` descendants:** process-group/SID census can miss workers that establish a new session. Retain inherited run-token and durable session/control-state evidence.
- **Zombie false negatives:** “PID absent or zombie” is enough for detached-daemon liveness, but not for a direct child still owned by a test. Direct owners must call `wait` and the final census must report zombie state separately.
- **Stale-socket race:** unlinking before process exit or without inode/identity validation can remove a replacement listener. Preserve live listeners and remove only unchanged owned paths.
- **Root error loss:** cleanup failure can overwrite readiness/test diagnostics, while swallowed cleanup errors produce false green. Preserve both with ordered, typed evidence.
- **Pipe/thread deadlock:** killing a process without draining/finalizing stderr/stdout readers can hang teardown. Bound reader/task joins and finalize output before cleanup success.
- **Parallel-suite interference:** global cleanup can make tests pass while corrupting neighbors. Run under default concurrency and use per-test/run ownership only.
- **Over-abstraction:** one universal lifecycle manager would widen risk. Prefer small guards beside each distinct ownership boundary.

## Acceptance checks and downstream proof

- Focused unit/integration tests deterministically cover failure before readiness ownership handoff, daemon metadata publication failure, entrypoint readiness/watch failure, Git/helper timeout, partial fixture construction, panic/unwind `Drop`, explicit cancellation, explicit session shutdown, and normal shutdown. Every case asserts the originating diagnostic and zero exact owned residuals.
- Restart durability test: spawn a real worker-backed session through the production Hub path; run ordinary daemon shutdown; prove the one intentional survivor has durable identity and control state; restart the Hub; prove adoption and session usability; explicitly shut down/delete the session; then prove the worker PID, control socket, and registry ownership are gone.
- Invalid survivor test: a failed/partial or non-adoptable worker is boundedly terminated and cannot be admitted to the intentional survivor set.
- Name the new lifecycle/adoption and negative-path tests with the `process_ownership_` filter and add the closed-enum wrapper target `focused-process-ownership`, bound to `./test.sh --test hub_daemon_lifecycle_test process_ownership_ -- --nocapture`. Add the same target to the workflow input choices.
- On macOS, from a clean baseline, run `script/run-loaded-daemon-lifecycle --subject-dir "$PWD" --artifact-dir <owned-artifact-dir> --subject-sha <exact-sha> --test-target focused-process-ownership --repetitions 20 --stress-profile none`. The Darwin adapter must record before/after owned-process, zombie, listener/socket, and data-root census evidence and end at zero unexplained deltas.
- On macOS, run the repository wrapper through the same census path with `--test-target full-suite-contention --repetitions 1 --stress-profile none`. This is the ticket's bounded wrapper proof, not the sibling ticket's four-package live diagnostic recipe.
- On Linux, run `focused-process-ownership` for at least 20 repetitions under the existing loaded workflow and run `full-suite-contention` under default parallelism. Stop on the first failure and retain its ownership report rather than retrying it away.
- Run `./test.sh -p botster-hub-test-support`, `./test.sh --test hub_mcp_test -- --nocapture`, `./test.sh --test hub_daemon_lifecycle_test -- --nocapture`, and `./test.sh --workspace`. `test.sh` is mandatory; direct `cargo test` is not acceptance evidence.
- Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `git diff --check`.
- Run the loaded lifecycle workflow's Linux focused 20-repetition campaign and full default-parallel suite on the exact implementation SHA, plus the two named Darwin procedures above on the same SHA. Record workflow/run or local artifact ID, platform and OS version, executable/Core provenance, inputs, baseline, each intentional survivor/adoption result, zombie/socket/data-root deltas, cancellation cleanup result, and final zero-residual status.
- Downstream runtime proof must exercise the real path `botster-hub` CLI -> `HubDaemon`/`HubRuntime` -> pinned Core daemon/runtime -> `botster-session-worker`; helper-only or code-existence evidence is insufficient.
- Final success is: zero newly unowned session workers, zero owned zombie children, zero owned stale listeners/socket paths, and zero owned stale test data roots. The only temporary survivor is the explicitly enumerated restartable session, which must be adopted and then explicitly shut down to reach zero.
- Produce red-on-revert/ablation evidence at each enforcement class: remove the production cleanup call or survivor classification check while retaining the test, demonstrate the matching focused assertion fails, restore the change, and rerun green. Do not use an unrelated suite failure as red proof.
- Review the final diff for correctness, regressions, architecture fit, missing tests/docs, overcomplication, hidden assumptions, dead/deprecated paths, and unwired implementation. No pre-existing failure is waived without exact evidence that it is unrelated.

## Pipeline and vault evidence

- Vault checklist `checklist_1785384756_808779` records the notes that constrained the plan, the resolved `release_for_restart()` convention conflict, Plan-stage verification, and the capture decision.
- Workflow checklist `checklist_1785384805_230508` records authoritative target routing, repository evidence, the human decision, plan artifact, and gate submission.
- Plan-stage verification: authoritative target/remote/subject inspection, spawn-boundary searches, pinned Core `PendingWorker` audit, `bash -n script/run-loaded-daemon-lifecycle`, repository wrapper inspection, and `git diff --check`. Runtime tests are intentionally implementation/verification evidence, not Plan-stage claims.

## Vault gaps worth capturing

- The human decision sharpens a durable lifecycle rule: a surviving worker is legitimate only when durable identity, secure attributable control state, and successful adoption all hold; ordinary restart release must not be confused with an orphan or with “terminate all sessions.”
- Implementation may validate a reusable ownership-census rule that combines inherited run token, exact binary provenance, PID/group, durable session identity, control socket identity, and owned data root because SID-only census misses `setsid` workers.
- Existing notes already cover process groups, readiness failure, worker shutdown phases, socket identity, CI cancellation, and zombie-aware cleanup. Do not create duplicates.
- Capture only after implementation and loaded runtime proof establish a durable rule not already represented, using the inbox-first vault workflow and linking it to [[worker shutdown completion requires lifecycle transport and process termination]], [[sid scoped census is blind to setsid session leaks]], [[workflow cancellation cleanup is idempotent across campaign traps and outer steps]], and [[botster-architecture]]. Plan-stage `capture_path` remains `nil`.
