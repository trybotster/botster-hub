# Implement report: classify zombie recovery workers as dead in lifecycle-harness identity capture

| Field | Value |
| --- | --- |
| Ticket | `ticket_1787076374_645547` |
| Run | `run_1787076383_328340` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus spawn-target `botster-hub` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1787076374_645547` |
| Plan | `docs/plans/classify-zombie-recovery-workers-as-dead-in-lifecycle-harness-identity-capture.md` revision 3 plus Implement resync for question_1787080034_752539 |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| Human scope answer | `question_1787080034_752539` option A |

Independent routing: `project_pipelines_current_context` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. The approved plan used the same routing. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[harness identity capture errors taint later daemon starts]]
- [[missing recovery worker identity is not worker absence proof]]
- [[benign command exit races do not taint the harness latch]]
- [[process global test latches require daemon guard serialization]]
- [[session registry process pid identifies the pty command not the session worker]]
- [[test script required for rust tests not cargo test]]
- [[suite wide acceptance criteria make every observed test failure in scope]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[implementation artifacts must match actual git state]]
- [[rust repo strict lints must be verified before dismissing warnings]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths are out of scope.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- No production `src/` changes.
- Do not weaken live-unverifiable, missing-identity, or capture-error taint proofs.
- Do not change the SIGKILL fixture product proof.
- Use `./test.sh` and `script/run-lifecycle-suite`, not bare `cargo test`.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior (harness only):

- `tests/hub_daemon_lifecycle/harness.rs` — `recovery_worker_is_live` treats Darwin `ps` stat `Z` as dead. `retain_recovery_worker` and `reap_registry_backed_workers` use it. Zombie pids stay in the owned/retained set and are not signaled.
- `tests/hub_daemon_lifecycle/harness_isolation.rs` — `dead_command_with_zombie_recovery_worker_does_not_taint` (red-on-revert for the classifier). `injected_taint_cannot_race_an_unguarded_real_daemon_start` (red-on-revert for the start-path race).
- `tests/hub_daemon_lifecycle/sessions.rs` — after shutdown, assert the forged pid-42 missing-identity taint, then `reset_harness_taint_after_proof`. IsolatedHub starts go through `start_isolated_hub`.
- `tests/hub_daemon_lifecycle/common.rs` — reentrant `daemon_test_guard`. First acquire checks taint after the lock. `start_installed_daemon` takes the guard.
- `tests/hub_daemon_lifecycle/cli.rs`, `process.rs`, `session_fixtures.rs` — every `start_cli_daemon*` and IsolatedHub start takes the reentrant guard. IsolatedHub starts share `start_isolated_hub`.
- `tests/hub_daemon_lifecycle/packages.rs`, `package_event_plane.rs` — IsolatedHub starts use `start_isolated_hub`.

Handoff:

- `docs/plans/classify-zombie-recovery-workers-as-dead-in-lifecycle-harness-identity-capture.md` — resyncs stale-row reset and start-path guard into scope, files, and acceptance.
- `docs/reports/classify-zombie-recovery-workers-as-dead-in-lifecycle-harness-identity-capture-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns the lifecycle-suite isolation policy. Production `src/` was not edited. Core pin `302c7f7` is unchanged. `RegistryRecord` and recovery identity are Core-owned and are only read here. No hub-client, Web, TUI, or package/plugin product paths were edited.

## Cross-repo dependencies or separately routed work

None. No dependency ticket was registered.

## Deviations from plan

Accepted by `question_1787080034_752539` option A, then written back into the committed plan:

1. After the SIGKILL taint disappeared, `session_entity_subscription_projects_stale_row_as_indeterminate` became the next first-writer (`dead command 42` / no recovery worker). The test now asserts that taint and resets the latch under `daemon_test_guard`.
2. The next first-writer was `taint_latch_refuses_next_daemon_start_without_spawning` racing unguarded `start_cli_daemon` calls. Implement added one reentrant start-path guard covering every `start_cli_daemon*` and IsolatedHub start.

No other product-scope change.

## Runtime-teardown lenses

| Lens | Implementation |
| --- | --- |
| Isolation | False live-unverifiable taint no longer kills 110 siblings. Genuine uncertainty still taints the process. |
| Bounds | One extra bounded `ps` snapshot per recovery worker. Capture rereads and TERM/KILL grace are unchanged. |
| Late-message matrix | Registry records and recovery pids only. Zombie recovery workers classify as dead and stay in the owned set. |
| Production-path proof | Live SIGKILL fixture: Z-stat probe on worker 4877, then post-fix run with no `identity capture incomplete` line. Two clean `script/run-lifecycle-suite` runs. |
| Ownership identity | Worktree-executable verification is unchanged. Zombie pids remain in the owned set for absence proof. |
| Sibling fail-closed | Siblings survive expected dead-but-unreaped workers. Live unverifiable workers and missing recovery identity still taint. |

## Tests and downstream proof run

Mechanism confirmation (before the classifier):

- `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable -- --nocapture --exact` — passed and printed `identity-capture probe ... worker=4877 exists=true stat=Some("Z")` plus `identity capture incomplete (Explicit): resolved worker 4877 is live but unverifiable for session shutdown-failure-victim`.

Red-on-revert:

- Zombie fixture before classifier: failed with `resolved worker 16696 is live but unverifiable for session zombie-recovery`.
- Race fixture before start-path guard: failed with `environment_tainted: injected race taint`.

Targeted after the fixes:

- `./test.sh --locked --test hub_daemon_lifecycle_test -- injected_taint_cannot_race taint_latch dead_command session_entity_subscription_projects_stale identity_capture_error unresolved_worker_ancestor` — 9 passed.
- `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable -- --nocapture --exact` — passed with no `identity capture incomplete` line.

Full suite (required):

- Pre-run census: botster-zombies 0, dev-artifacts 0.
- `script/run-lifecycle-suite` run 1: `verdict=clean failed=0 tally=1 survivors=0 tainted=0` (250 passed, 1 ignored).
- `script/run-lifecycle-suite` run 2: `verdict=clean failed=0 tally=1 survivors=0 tainted=0` (250 passed, 1 ignored).
- Post-run census: botster-zombies 0, dev-artifacts 0.

Strict clippy on the lifecycle test target is recorded in the commit message / gate evidence after this report.

## Unverified behavior or residual risk

- PID reuse after reap remains a pre-existing, out-of-scope exposure.
- `prove_owned_absence` is still fail-closed through `process_exists`. It is not zombie-aware precedent.
- The start-path guard serializes real-daemon starts. Tests that do not start a real daemon stay concurrent.
- Drain-failure injection still leaves a zombie until Hub exits. The classifier treats that as dead evidence.

## Missing vault guidance discovered

Capture candidates (not written in this step):

- An unreaped zombie recovery worker is dead evidence, not a live unverifiable worker. `kill(pid, 0)` conflates zombie with live.
- After a process-global taint first-writer is removed, the next isolation test that sets the latch can race unguarded real-daemon starts.

No vault note already stated the Darwin `Z`-stat classifier for recovery workers.
