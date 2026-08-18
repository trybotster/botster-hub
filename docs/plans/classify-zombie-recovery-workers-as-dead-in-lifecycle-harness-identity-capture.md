# Classify zombie recovery workers as dead in lifecycle-harness identity capture

Ticket: ticket_1787076374_645547 — Hub tests: shutdown-failure-sibling identity capture taints default-concurrency lifecycle suite.
Run: run_1787076383_328340. Plan rev 2.

Rev 2 addresses Plan Review review_1787078204_780938: corrected the Cargo test target name (`hub_daemon_lifecycle_test`), corrected the inaccurate `prove_owned_absence` zombie-precedent claims, and ran the corrected focused commands.

## Target repository and target

- Target repository: botster-hub (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Worktree branch: `project-pipelines/ticket_1787076374_645547` at Hub `8908a92` (Core pin 302c7f7 unchanged).
- The target was resolved from the ticket through `list_spawn_targets`, not from the ambient directory.

## Playbooks and notes loaded

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]] (repository ownership charter)
- [[botster runtime teardown lenses]] (worker-teardown evidence classification is in scope)
- [[harness identity capture errors taint later daemon starts]]
- [[missing recovery worker identity is not worker absence proof]]
- [[benign command exit races do not taint the harness latch]]
- [[process global test latches require daemon guard serialization]]
- Referenced from the charter without full re-read: [[session registry process pid identifies the pty command not the session worker]], [[hub shutdown preserves durable session workers]], [[host exhaustion markers identify each failed test]].

## Context loaded

- `tests/hub_daemon_lifecycle/sessions.rs:3406` — `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` (the SIGKILL fixture).
- `tests/hub_daemon_lifecycle/harness.rs` — `collect_owned_session_processes`, `collect_registry_record`, `retain_recovery_worker`, `reap_registry_backed_workers`, `prove_owned_absence`, `signal_worker_group`.
- `tests/hub_daemon_lifecycle/cli.rs` — `PanicSafeCliDaemon::shutdown_at`, `cleanup_owned_resources`, `retain_identity_capture` (the "identity capture incomplete" taint site).
- `tests/hub_daemon_lifecycle/process.rs` — `process_exists` (`kill(pid, 0) == 0`), `ProcessSnapshot.stat`.
- `tests/hub_daemon_lifecycle/harness_isolation.rs` — the taint-latch proofs that must stay intact.
- `script/run-lifecycle-suite` — verdict classifier (`environment_tainted` counts `environment_tainted:` markers).
- Ticket description, run context, and parent-run artifact conventions (`docs/plans/*.md`).

## Diagnosis (mechanism)

The taint is deterministic and comes from a dead-but-unreaped (zombie) worker:

1. The fixture SIGKILLs the victim session workers, then proves `ShutdownSession` returns `OperatorError`. The daemon was started with runtime drain-failure injection for the victim session, so Core never observes `ProcessExited` for it. Core therefore never reaps its worker child. The registry record stays nonterminal with `recovery_identity.worker_pid` set to the killed worker.
2. `daemon.shutdown()` calls `cleanup_owned_resources(Explicit)`, which runs `collect_owned_session_processes` **while the Hub child is still alive**. The killed worker is at that moment a zombie child of the live Hub process.
3. In `retain_recovery_worker` (`harness.rs:428`), `process_exists(worker_pid)` is `kill(pid, 0) == 0`, which returns true for a zombie. `worker_pid_matches_worktree_session_worker` then fails: `ps -o command=` reports a parenthesized zombie name and the worktree-executable check cannot pass for a zombie. Result: error "resolved worker {pid} is live but unverifiable for session shutdown-failure-victim".
4. `retain_identity_capture` (`cli.rs:500`) records that error as process-global harness taint. Every later test panics at `check_harness_taint`, which yields exactly the observed suite verdict: `environment_tainted failed=110 tally=1`.
5. The fixture test itself passes: after Hub exit the zombie is reaped by the system before `prove_owned_absence` runs, so the proof succeeds. Note: `prove_owned_absence` is **not** zombie-aware precedent — for a live zombie, `process_exists` at `harness.rs:827-829` returns "still live" first, so its Z-stat branch at `harness.rs:831-838` never sees a zombie (that branch only fires when `kill(pid, 0)` fails while `ps` still reports the process). The evidence base for the classifier is the Darwin process-state contract (`ps` stat `Z` marks a dead unreaped process) plus the deterministic fixture in this plan.

This explains both ticket runs producing the same sentence with different PIDs, and why the pre-run process census was 0/0: no real survivor ever exists.

Plan-time repro (rev 2, base 8908a92): `cargo test --test hub_daemon_lifecycle_test external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable -- --nocapture` passed in 9.37s and printed the exact originating taint on stderr — `identity capture incomplete (Explicit): resolved worker 59557 is live but unverifiable for session shutdown-failure-victim` — a third distinct PID. The trigger is deterministic in isolation, not census- or load-dependent. The step-1 probe still confirms the Z-stat state of that pid before the classifier ships.

A zombie is dead evidence, not a live unverifiable worker. The harness already distinguishes zombie state in `prove_owned_absence`; the capture and reap classifiers do not. Per [[benign command exit races do not taint the harness latch]], "a dead command with a dead recovery worker is exited in transition and does not taint" — a zombie recovery worker is a dead recovery worker awaiting parent reap.

## Scope

1. **Mechanism confirmation (decision gate).** Run the single fixture test with `--nocapture` and confirm the stderr line `identity capture incomplete (Explicit): resolved worker ... is live but unverifiable for session shutdown-failure-victim`. Add a temporary probe (or use the new fixture) to confirm the recovery worker's `ps` stat contains `Z` at capture time. If the process is not a zombie (genuinely live worker or PID reuse), STOP and re-diagnose before implementing; do not ship the zombie classification on an unconfirmed mechanism.
2. **Regression fixture first (red-on-revert).** Add `dead_command_with_zombie_recovery_worker_does_not_taint` to `tests/hub_daemon_lifecycle/harness_isolation.rs`: spawn a `sleep` child, SIGKILL it, do **not** `wait()` it (the test process holds the `Child`, so the zombie is deterministic), record it via `save_running_recovery_record` with a dead command pid, run `collect_owned_session_processes`, and assert: no capture errors, latch clear, worker pid retained in `capture.owned.pids`. Also assert `reap_registry_backed_workers` reports no error and does not signal the zombie (it lands in the retained set, mirroring the dead-recovery branch). Reap the child at the end. Hold `daemon_test_guard` across every latch reset and proof, per [[process global test latches require daemon guard serialization]]. Prove the fixture fails (taints) before the classifier change.
3. **Classifier fix.** In `tests/hub_daemon_lifecycle/harness.rs`, add one helper, e.g. `fn recovery_worker_is_live(pid: u32) -> bool { process_exists(pid) && !process_snapshot(pid).is_some_and(|s| s.stat.contains('Z')) }`. The `Z` test rests on the Darwin process-state contract (`ps` stat begins with `Z` for a dead unreaped process) and is proven by the deterministic fixture in step 2; `prove_owned_absence` uses the same textual idiom but is not behavioral precedent (see Diagnosis note). Use the helper in:
   - `retain_recovery_worker`: a zombie recovery worker returns early as dead (exited in transition), with `command_pid` and `worker_pid` still pushed into `capture.owned` so the post-shutdown absence proof still covers them.
   - `reap_registry_backed_workers` recovery-worker match arms: a zombie folds into the existing dead-worker branch (retain both pids, no error, no signal). `signal_worker_group` cannot kill a zombie, so classifying it live would otherwise produce "survived bounded TERM/KILL".
   - If `process_snapshot` returns `None` while `process_exists` is true, keep the live classification (fail-closed falls through to the identity check and taints).
4. **Proof.** Targeted tests, then two consecutive `script/run-lifecycle-suite` runs with `verdict=clean` (mirroring the two failing runs in the ticket).

## Non-scope

- No production `src/` changes; the change is confined to the lifecycle test harness.
- No change to `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` itself; its product proof (sibling survives victim shutdown failure) stays byte-identical.
- No weakening of the existing taint proofs: `dead_command_with_live_unverified_recovery_worker_taints` (live non-zombie decoy), `dead_command_without_recovery_identity_taints_and_does_not_signal`, `identity_capture_error_taints_and_blocks_next_start`, `taint_latch_refuses_next_daemon_start_without_spawning` must all stay green and unmodified.
- No change to `script/run-lifecycle-suite` or its classifier, and no waiver of that gate.
- No changes to `prove_owned_absence`. Its `process_exists` check (`harness.rs:827-829`) already fails closed after Hub exit: a zombie that somehow survives teardown reports "owned worker pid still live".

## Ownership boundaries and cross-repo dependencies

- All changes live in botster-hub test harness files; Hub owns its lifecycle-suite isolation policy per [[botster-hub-playbook]].
- Core pin 302c7f7 is untouched. `RegistryRecord`, `recovery_identity`, and worker spawn mechanics are Core-owned and are only read here.
- No cross-repository prerequisite exists; no dependency registration is needed.
- This ticket is not a consumer of the Hub session-type eligibility parent; no parent pins apply.

## Runtime-teardown lenses

- `teardown_class_applies`: yes — the ticket classifies session-worker teardown evidence (worker identity, sibling policy, fail-closed latch) in the lifecycle harness. Production teardown code paths are untouched.
- `teardown_isolation`: the taint latch is process-global by design; one false "live but unverifiable" verdict killed 110 sibling tests. The fix narrows the latch to true uncertainty. Genuine uncertainty still fails the whole suite deliberately (fail-closed isolation policy unchanged).
- `teardown_bounds`: capture keeps bounded registry rereads (8 × 25ms) and bounded TERM/KILL grace in `signal_worker_group`. The fix adds one bounded `ps` snapshot per recovery worker. No unbounded waits are added.
- `late_message_matrix`: the ownership-creating records here are registry records and recovery identities, not control-plane messages. Registry record: owner = session id; excluded when `Exited`; swept by bounded reread. Recovery worker pid: owner-tagged by `recovery_identity`; verified by worktree-executable identity; zombie now classified as dead; swept by `prove_owned_absence` after Hub exit. Command pid: retained as evidence, never signaled as a worker.
- `production_path_proof`: the proven path is the real fixture teardown — SIGKILL → live `ShutdownSession` `OperatorError` → guard capture with Hub alive → Hub shutdown → absence proof — via the fixture test plus two clean full-suite runs. The new harness_isolation fixture is red-on-revert for the classifier.
- `ownership_identity`: worker identity remains worktree-executable verification, never bare pid. The zombie pid stays in the owned set, so its absence is still proven after teardown. PID reuse after reap is a pre-existing, out-of-scope exposure.
- `sibling_fail_closed_policy`: sibling tests survive an expected dead-but-unreaped worker; siblings still fail closed on live unverifiable workers, registry read failures, and missing recovery identity.

## Assumptions and unknowns

- Assumption: the recovery worker pid equals the SIGKILLed worker, which is a zombie child of the still-live Hub daemon at capture time. Both ticket runs producing the identical sentence supports this; step 1 confirms it empirically before the fix ships.
- Assumption: `ps -o stat=` reports `Z` for zombies on Darwin 25.5.0 (Darwin process-state contract; pinned by the new deterministic fixture).
- Unknown: whether the second `ShutdownSession` issued by `shutdown_owned_sessions` ever succeeds under drain injection. Irrelevant to the taint site (capture runs first) but worth observing during confirmation.
- Unknown: exact zombie `command` rendering (`(botster-session-worker)`); only the stat field is load-bearing for the fix.

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/harness.rs` — `retain_recovery_worker`, `reap_registry_backed_workers`, new liveness helper.
- `tests/hub_daemon_lifecycle/harness_isolation.rs` — new zombie-recovery fixture.
- No other files.

## Risks

- **Wrong mechanism**: if the live pid is a reused pid rather than a zombie, the zombie classification would not repair the suite. Mitigated by the step-1 decision gate; on refutation, stop and re-plan.
- **Over-classification**: a stat string that contains `Z` for a non-zombie would skip the taint. Darwin state letters make this a non-collision (`Z` appears only as the zombie state, never as a modifier); the new fixture pins this contract deterministically.
- **Snapshot race**: the process dies between `process_exists` and `process_snapshot`; the plan keeps the fail-closed fall-through (taint), identical to today's exposure.
- **Suite cost/flakiness**: two full lifecycle-suite runs are slow and exclusive; run them via the wrapper only, after targeted tests are green.
- **Latch bleed**: the new fixture mutates the global latch; guard serialization per the vault note prevents cross-test bleed.

## Acceptance checks and tests

1. New fixture `dead_command_with_zombie_recovery_worker_does_not_taint` fails before the classifier change and passes after (recorded red-on-revert evidence).
2. Targeted: `cargo test --test hub_daemon_lifecycle_test -- dead_command taint_latch identity_capture_error unresolved_worker_ancestor` green, including unmodified `dead_command_with_live_unverified_recovery_worker_taints`, `dead_command_and_dead_recovery_worker_do_not_taint`, `dead_command_without_recovery_identity_taints_and_does_not_signal`. (Verified against `cargo test --test hub_daemon_lifecycle_test -- --list`: the harness flattens module paths, so test names carry no `harness_isolation::` prefix; explicit name filters are required. A bare `harness_isolation` filter matches zero tests.)
3. Targeted: `cargo test --test hub_daemon_lifecycle_test external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable -- --nocapture` passes with **no** `identity capture incomplete` stderr line. (Both corrected commands were executed at Plan time on base 8908a92: command 2 ran 6 tests green in 2.81s; command 3 passed in 9.37s and currently prints the taint line — the post-fix assertion is its disappearance.)
4. Full proof: two consecutive `script/run-lifecycle-suite` runs, each `verdict=clean failed=0 tainted=0 tally=1`. Do not waive; do not substitute a filtered run.
5. Process census 0/0 before and after the suite runs (`script/process-census`).
6. Strict Rust gates per repo wrappers (`test.sh` / clippy as repo-configured) on the changed files.
7. Worktree hygiene: tracked `.gitignore` intact (verified: 5 lines, matches HEAD); worktree path has no colon, so no `CARGO_TARGET_DIR` override is needed.

## Vault gaps

- Capture candidate (gotcha): "an unreaped zombie recovery worker is dead evidence, not a live unverifiable worker" — the harness must classify `Z`-stat recovery workers as exited-in-transition; `kill(pid, 0)` liveness conflates zombie with live. Links: [[benign command exit races do not taint the harness latch]], [[missing recovery worker identity is not worker absence proof]], [[harness identity capture errors taint later daemon starts]].
- Possible follow-up note: drain-failure injection keeps Core from reaping the victim worker, so SIGKILL fixtures on injected daemons always leave a zombie until Hub exits.
