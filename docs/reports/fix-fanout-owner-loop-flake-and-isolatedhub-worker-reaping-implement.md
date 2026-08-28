# Implement report: fanout owner-loop flake and IsolatedHub worker reaping

Ticket: `ticket_1787894962_603665`
Run: `run_1787895426_736357`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Implement (`botster_stack_implement`)

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Resolved through `list_spawn_targets`. The ticket worktree is the same repository.
- Approved plan artifact `artifact_1787903322_118221` (commit `f58e94a`) used the same routing.

## Repository playbook and other playbooks/notes applied

Repository charter:

- [[botster-hub-playbook]]

Role and class:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster runtime teardown lenses]]

Targeted atomic notes:

- [[Hub session worker census requires the worker binary under the worktree]]
- [[session registry process pid identifies the pty command not the session worker]]
- [[process group absence requires membership proof not leader pid absence]]
- [[zombie recovery workers are dead for liveness but remain in absence proof]]
- [[hub shutdown preserves durable session workers]]
- [[harness identity capture errors taint later daemon starts]]
- [[real daemon start boundaries serialize against process global taint]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
- [[worker isolation now has a Core try-admit non-blocking primitive]]
- [[plugin worker queue capacity and executor concurrency are independent host profile knobs]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[admitted event holders survive producer unload until Core completion]]
- [[package event handler timeouts are discarded as successful completions]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[Hub background fairness must stay policy-neutral]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[test names do not prove their bodies can fail on the named claim]]
- [[vault example paths are not repository placement conventions]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]

[[project-pipelines-playbook]] was not loaded. This ticket does not change Project Pipelines package or plugin paths.

## Files changed

| File | Why |
|------|-----|
| `src/daemon_maintenance.rs` | Production `Backpressured` requeue arm; fanout retry loop and yield; red-on-revert unit test. |
| `src/runtime.rs` | Deterministic `try_admit_plugin` Backpressured seam using the existing test flag. |
| `src/package_event_router.rs` | Repeated-requeue occupancy and queue-age expiry proof. |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | Bounded IsolatedHub teardown: retained Child, pipe drains, freeze/confirm/kill, unconfirmed child-only handoff, lifecycle state, taint. |
| `crates/botster-hub-test-support/src/lib.rs` | IsolatedHub teardown proofs. |
| `tests/hub_daemon_lifecycle/shutdown.rs` | Live IsolatedHub spawn, positive census, shutdown reap. |
| `docs/plans/fix-fanout-owner-loop-flake-and-isolatedhub-worker-reaping.md` | Record the `runtime.rs` test-seam file. |
| `docs/reports/fix-fanout-owner-loop-flake-and-isolatedhub-worker-reaping-implement.md` | This report. |

No change to `packages/hub-test-support`, `Cargo.toml`, `Cargo.lock`, fixtures, or client DTOs.

## Ownership boundaries preserved

- Hub owns package-event delivery policy. The new arm consumes Core `PluginAdmissionResult::Backpressured` and does not add a Core mechanism.
- IsolatedHub lives in `botster-hub-test-support` inside this repository.
- Freeze, confirm, kill, and reap stay limited to the verified owned Hub process group and captured descendant leader groups.
- No process-global recovery registry and no live OS resources retained across fixture instances. Taint stores only `(pgid, data_dir)` and refuses the next start.

## Cross-repo dependencies or separately routed work

None. No Core pin roll. No hub-client, web, or TUI change. No published-package cutover.

Cross-repository behavioral re-check (plan assumption 5): pinned Core `7eafa47` `worker_process.rs` spawns with `command.spawn()` and does not set `pre_exec` or `process_group`. Session workers inherit IsolatedHub's `setpgid(0, 0)` group.

## Deviations from plan

1. `src/runtime.rs` was not in the original affected-files table. The plan already required a deterministic Backpressured seam. The existing `force_plugin_admit_backpressure` flag only drove the test-event settlement path. Implement wired that flag into production `try_admit_plugin` under `BOTSTER_ENV=test`. The plan table now names this file.
2. Plan reproduction items 1 and 2 (official default-concurrency failure and leftover worker on unmodified `main`) were not re-run in this worktree. The ticket branch already existed. Implement used the cancelled-branch and Plan evidence rather than a fresh `main` reproduction.
3. Fanout completion-drain deadline stays at 2 s. The queue loop uses 5 s. Isolated runs did not need a raised drain deadline.
4. IsolatedHub census treats a shebang worker as owned when argv0 is `sh`/`bash`/`dash` and argv1 basename is `botster-session-worker`. Production workers still match argv0 basename `botster-session-worker`. This is fixture identity, not the rejected substring match.
5. Not every acceptance-14 fault injection arm has a dedicated red-on-revert test (`skip_freeze`, `skip_stop_confirmation`, remembered-set reuse). Covered: unconfirmed child-only handoff, taint, Drop residual budget, isolation, descendant reap, stalled shutdown command, live positive census plus reap.

## Runtime-teardown lenses implemented

All six lenses are implemented in IsolatedHub teardown, not deferred.

- Isolation: census and signals use this instance's Hub-child pgid. Sibling groups stay live.
- Bounds: one 22.5 s `TeardownBudget`; shutdown command 5 s; Hub-child wait 5 s; freeze-confirm 2.5 s; reap remaining with 10 s ceiling. Typed `TeardownTimeout { phase }`.
- Late-message matrix: workers before shutdown, workers during shutdown, PTY descendants, foreign IsolatedHub workers, zombies.
- Production-path proof: live IsolatedHub `shutdown` after Spawn. Positive census then absence after reap.
- Ownership identity: `(session-worker basename, pgid == Hub-child pid)` plus descendant closure. Unconfirmed path taints `(pgid, data_dir)`.
- Sibling fail-closed: confirmed path freeze/kill/reap owned set only. Unconfirmed path sends no group signal; kills only the retained Child; does not claim descendant cleanup.

## Tests and downstream proof run

Toolchain: `RUSTUP_TOOLCHAIN=1.97.0`, `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` unset.

Prebuild:

- `cargo build --locked -p botster-core-daemon --bin botster-session-worker` exit 0
- `cargo build --locked --bin botster-hub` exit 0

Strict gates:

- `cargo fmt --all -- --check` exit 0
- `cargo clippy --workspace --all-targets --locked -- -D warnings` exit 0 after one repair rerun
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` assets current
- `cd packages/hub-test-support && npm install --no-save && npm test` passed
- `git diff --check` clean

Focused proofs:

- `daemon_maintenance::tests::owner_loop_queues_and_completes_two_fanout_plugin_handlers` passed
- `daemon_maintenance::tests::backpressured_admission_requeues_holder_instead_of_retiring` passed
- Red-on-revert: removing the `Backpressured` arm made that test fail on `snapshot.queued_holders` left 0 right 1
- With the arm reverted, the fanout loop test still passes in isolation. The loop is for elapsed-slice cuts. The unit test is the red control for holder retirement.
- `package_event_router::tests::repeated_requeue_occupancy_stays_net_zero_until_queue_age_expires` passed
- IsolatedHub crate proofs for census, repeated reap, isolation, stalled shutdown, Hub-child wait descendants, unconfirmed handoff, taint, Drop budget: passed
- `isolated_hub_shutdown_reaps_live_session_workers` passed (live Spawn through IsolatedHub, non-empty census, empty after shutdown)

Official wrapper:

- First `./test.sh --locked`: failed `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status`. Isolation on this branch and on `main` at `a0c7141` both passed. The failure is an occupancy ablation race under default concurrency, not IsolatedHub census.
- Second `./test.sh --locked`: failed `unconfirmed_quiescence_kills_only_the_hub_child_and_taints` because a descendant pid file was not published within 2 s. Wait for new IsolatedHub fixtures is now 5 s.
- Third `./test.sh --locked`: exit 0. Hub lib 496 passed in 14.48 s. Lifecycle 319 passed, 2 ignored, in 326.97 s. Test-support 53 passed in 13.23 s. Wrapper elapsed about 403 s.

Downstream client proof: none required and none claimed. No DTO, protocol, fixture, or published package change.

## Unverified behavior or residual risk

- Unconfirmed quiescence can leave descendants. Tests assert typed error, taint, bounded return, no stopped processes, and no leaked drain threads. They do not assert zero owned descendants on that path.
- Drop retry under residual time can still freeze remaining group members if confirmation later succeeds. That is the one fresh-sample retry. Residual zero skips the retry.
- First official-suite ablation failure was not reproduced in isolation. Residual default-concurrency flake risk remains for `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status`.
- Host still had unrelated leftover `botster-session-worker` processes from other worktrees during census debugging. This change cannot reap those foreign groups.

## Missing vault guidance discovered

The plan already listed the vault-gap candidates. Implement confirms these are still accurate and were not captured into `~/knowledge/inbox/` from this step:

1. A catch-all admission arm turns transient backpressure into silent event loss.
2. A retry loop cannot recover work the producer already retired.
3. Hub owns two session-worker censuses across the library and test crates.
4. Recover a cancelled branch from its tip, not from named intermediate commits.
5. Hub session workers are identified by process group, not by a data-directory argv.
6. Prove the census non-empty before trusting an absence assertion.
7. Bound the teardown path from its first blocking call.
8. A retry bound needs a monotonic quantity, not a counter the retry restores.
9. A bounded child wait keeps the Child and moves only its pipes.
10. Freeze the group before snapshotting the set you are about to kill.
11. A stop request is not a stop, and a freeze owes a resume.
12. A cleanup that cannot prove its set must fail, not fall back.
13. A consuming teardown needs a Drop-visible state, not a promise in the error.
14. A document completeness claim needs a stem search and a field count, not a phrase grep.

No new gap beyond those 14 was required to finish the implementation. Capture remains a later vault-pipeline action unless the owner wants inbox notes from this report.
