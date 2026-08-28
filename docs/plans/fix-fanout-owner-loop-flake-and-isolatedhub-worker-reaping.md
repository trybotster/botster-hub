# Plan: fix the fanout owner-loop flake and IsolatedHub worker reaping

Ticket: `ticket_1787894962_603665`
Run: `run_1787895426_736357`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Base ref: `main` at `a0c7141`

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Resolved from the run `target_id` through `list_spawn_targets`, not from the process working directory. The ambient worktree resolves to the same repository.

## Repository playbook loaded

- [[botster-hub-playbook]] -- the exact repository ownership charter for `botster-hub`.

## Other role and surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Class overlay (runtime-teardown class applies, see below):

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

[[project-pipelines-playbook]] is not loaded. This ticket touches no Project Pipelines package or plugin path and no workflow policy.

## Context loaded

- Ticket description, run context, and gate `botster_stack_plan_gate` required fields.
- Vault capture `/knowledge/ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md`. Decomposition step 0 is exactly this ticket: restore the default-concurrency Hub gate and IsolatedHub worker cleanup before move-only decomposition starts.
- Repository README section on the `docs/plans/**` and `docs/reports/**` audit exclusion. `docs/plans/` is the confirmed destination for this artifact.
- `test.sh`, which runs `node packages/hub-test-support/scripts/sync-assets.mjs --check` and then `BOTSTER_ENV=test cargo test --workspace "$@"`.
- Current `src/daemon_maintenance.rs`, including `run_package_event_delivery_slice`, the `EVENT_DELIVERY_*` budgets, and the fanout test.
- Current `crates/botster-hub-test-support/src/isolated_hub.rs`.
- Current `tests/hub_daemon_lifecycle/process.rs` worker census and reap helpers.
- Cancelled branch `project-pipelines/ticket_1787600674_500120` and its commits `e2f1995`, `c32cbe0`, `81b42a0`, `e053265`, `5c30ebc`, `6012eca`, `cb36f64`, `a39d9ea`.
- Answered blocking question `question_1787895620_428538`.

## Findings that change the ticket as written

Two facts from the cancelled branch change the work. The Plan agent raised both as a blocking question. Question `question_1787895620_428538` is answered and both decisions below are the owner's.

### Finding 1: `c32cbe0` alone does not apply to `main`

`c32cbe0` only raises a deadline from 2 s to 5 s and adds a 10 ms sleep inside a retry loop. That loop does not exist on `main`. Commit `e2f1995` introduced it. `main` still calls `run_package_event_delivery_slice` exactly once and asserts two in-flight handlers immediately.

Consequence: the plan re-authors the bounded retry-loop shape from `e2f1995` together with the `c32cbe0` yield.

### Finding 2: the test-side loop was never sufficient; the real defect is production event loss

After `e2f1995` and `c32cbe0`, GitHub Verify still failed twice with one queued handler instead of two. Commit `81b42a0` found the cause.

In `run_package_event_delivery_slice`, the `match admission` block handles `PluginAdmissionResult::Queued` and sends every other variant to one catch-all `_ =>` arm that retires the event holder. Core documents `PluginAdmissionResult::Backpressured` as transient, non-waiting pressure from class count or byte saturation, completion-reservation saturation, or a busy admission lock. Hub therefore destroys an admitted second handler on transient pressure, and no later slice can ever queue it. No amount of test-side retrying can recover a holder that Hub already retired.

Owner decision (question `question_1787895620_428538`): the production `Backpressured` requeue is in scope. It is a correctness repair, not a scheduling-policy change.

Owner constraints recorded verbatim in scope below.

## Scope

### In scope

1. **Production: preserve a `Backpressured` package-event delivery.** In `src/daemon_maintenance.rs`, `run_package_event_delivery_slice`, add an explicit `PluginAdmissionResult::Backpressured { .. }` arm before the catch-all. The arm releases the causal-scope lease it just minted, requeues the delivery through the existing `PackageEventRouter::requeue_delivery`, and wakes the scheduler. If `requeue_delivery` itself fails, the arm falls back to the existing retire path. `src/runtime.rs` already uses `requeue_delivery` this way, so this is the established Hub idiom, not a new mechanism.
2. **Test: make the fanout test survive an elapsed slice cut.** Re-author the bounded retry loop and yield around `run_package_event_delivery_slice` in `owner_loop_queues_and_completes_two_fanout_plugin_handlers`. `EVENT_DELIVERY_MAX_ELAPSED` is 8 ms, so one production slice can legitimately return one ready handler under default-concurrency lib load. The loop repeats the production slice, re-arms the delivery wake, and yields between attempts.
3. **Test: add a red-on-revert unit test for the `Backpressured` arm.** A focused test that fails when a `Backpressured` admission retires the holder instead of requeueing it.
4. **Test support: reap IsolatedHub session-worker descendants.** In `crates/botster-hub-test-support/src/isolated_hub.rs`, add a bounded, idempotent reap of session workers owned by the IsolatedHub data directory, plus their descendants, on every shutdown path: successful `shutdown_inner`, failed shutdown, `wait_for_ready` failure during build, and `Drop`.

### Out of scope

- Terminal `SlotReady` behavior, persistence bursts, coalesced persist ticks, and doorbell rate limiting. The ticket forbids these. Commits `da9d529`, `d436915`, `21c7f48`, `4e2183a`, `d558851`, `813fa75`, `ad7fd20`, `e68996a`, `81fafa3`, `63dfd24`, `88a9e97`, `ae7176e`, `1b9e2de` are not recovered.
- Owner-loop observation changes. `e053265`, `920c0b7`, and `a39d9ea` each carry owner-loop or starved-bind observation edits alongside the IsolatedHub edits. Only their `crates/botster-hub-test-support/src/isolated_hub.rs` hunks are taken.
- Existing slice budgets `EVENT_DELIVERY_MAX_ITEMS`, `EVENT_DELIVERY_MAX_BYTES`, `EVENT_DELIVERY_MAX_ELAPSED`, wake cadence, owner-turn ordering, and non-blocking admission. All stay exactly as they are.
- Any waiting or retry loop inside the authoritative mutation path. The requeue is a single non-blocking call. The retry loop lives only in the test.
- Promoting the mature census in `tests/hub_daemon_lifecycle/process.rs` into a shared crate. See "Vault gaps" and the follow-up note below.
- WebRTC, DataChannel, admission, and route work from the wider decomposition project.
- Any change to `packages/hub-test-support` npm version, fixtures, protocol, or revision. This ticket changes no client-visible DTO or fixture.

## Repository ownership boundaries and cross-repository dependencies

- Both changes are owned by `botster-hub`.
  - The `Backpressured` arm is Hub control-plane package-event delivery policy. Hub owns package-event production, bounds, and completion accounting per [[botster-hub-playbook]]. Core owns the non-waiting `try_admit` primitive and the `PluginAdmissionResult` taxonomy. This plan consumes that taxonomy and adds no Core change.
  - `IsolatedHub` lives in the `botster-hub-test-support` crate inside this repository.
- **No cross-repository dependency is required.** No Core pin roll, no `botster-hub-client` DTO change, no `botster-web` or `botster-tui` change, and no published-package cutover. No dependency ticket is registered against another target.
- Boundary preserved: Hub does not duplicate a Core mechanism. It stops discarding a Core result that Core defines as transient.

## Runtime-teardown class

`teardown_class_applies`: **yes.** The IsolatedHub work is session-worker ownership teardown that currently leaves OS processes alive after the Hub child exits. It is a resource-leak and FD/process-spin class. It is not UI, copy, docs-only, or single-field client work.

The fanout half is control-plane delivery correctness. The lenses below are answered for the teardown half, which is where the class applies.

| Field | Answer |
|-------|--------|
| `teardown_isolation` | The ownership set is exactly the session workers whose argv names this `IsolatedHub` instance's data directory, plus their process descendants. One IsolatedHub instance never reaps another instance's workers, and never reaps a worker outside this worktree. Sibling IsolatedHub instances and concurrently running `cargo test` threads are unaffected. Isolation is the reason the plan rejects `cb36f64`'s basename substring match. |
| `teardown_bounds` | The reap loop is bounded twice. It sends `SIGTERM` first, escalates to `SIGKILL` after 400 ms, and returns unconditionally after 15 s even when the census is still non-empty. It sleeps 50 ms between census turns. It never blocks forever on a hung worker, and it never propagates a panic out of `Drop`. |
| `late_message_matrix` | Ownership-creating surfaces for this teardown, with tag, reject, and sweep. (1) *A session worker spawned before shutdown*: tagged by the `--data-dir` argv token and by an argv0 executable under this worktree; rejected after shutdown because the Hub child is already reaped; swept by the census loop. (2) *A worker spawned during the shutdown race*: the reap runs after `child.wait_with_output()` returns, so the Hub child can no longer spawn; a worker that appeared just before the exit is still caught, because the census re-runs each 50 ms turn until empty. (3) *A PTY command descendant of a worker*: not directly tagged; swept transitively through the `ps` parent/child closure, per [[session registry process pid identifies the pty command not the session worker]], which records that the registry PID names the PTY command and not the worker. (4) *A worker owned by a different live IsolatedHub or a different worktree*: rejected by the exact `--data-dir` token match and the worktree-executable rule; never swept. (5) *A zombie worker*: retained in the census for absence proof and never signalled, per [[zombie recovery workers are dead for liveness but remain in absence proof]]. |
| `production_path_proof` | The exact path is: test calls `IsolatedHub::shutdown` or drops the guard -> `shutdown_inner` sends the daemon shutdown request -> `child.wait_with_output()` returns -> `reap_owned_session_workers` runs -> census empty -> `remove_data_dir`. The oracle is a live process census through `ps`, taken after the reap returns, asserting zero owned workers and zero owned descendants. The proof drives the real `IsolatedHub` shutdown path, not a helper. Red-on-revert control: with the reap call removed, the same test must observe a surviving worker. Every shutdown path is exercised, not only the happy path. |
| `ownership_identity` | Owner identity is the pair (this worktree's built worker executable, this IsolatedHub instance's `--data-dir` token). `a39d9ea` already carries the `data_dir_arg` field that records the exact argv token the Hub child was launched with, which is the stable owner id. Because each IsolatedHub mints a fresh unique data directory, a reused id cannot name another live instance's worker. Matching is exact-token or canonicalized-path equality, never substring. |
| `sibling_fail_closed_policy` | On a successful reap, siblings keep working; nothing outside the owned set is signalled. On ultimate failure, that is a still-non-empty census after 15 s, the reap returns without escalating and without sacrificing any sibling. Blast radius is bounded to the owned set in both cases. `killpg` is sent only when the target PID is itself a process-group leader and its group is not this process's own group, so the reap can never signal the `cargo test` harness group. The plan explicitly rejects `6012eca`'s unconditional `killpg` on a non-leader PID for this reason. |

## Assumptions and unknowns

Assumptions:

1. `PackageEventRouter::requeue_delivery` returns the delivery on error and is safe to call from the owner-loop slice. `src/runtime.rs` already calls it in exactly that shape, so this is established, not new.
2. Releasing the minted causal-scope lease before requeueing is correct, because the delivery has not been admitted to a worker and no completion will arrive to release it. This mirrors `81b42a0`.
3. `Backpressured` is reachable under default-concurrency workspace load with the shipped plugin worker queue and executor values. The branch's two GitHub Verify failures are the evidence. Implement must confirm reachability through a deterministic seam rather than ambient load. If no deterministic seam exists, Implement records that and the red-on-revert unit test drives the arm directly.
4. `ps -axo pid=,ppid=` and `ps -axo pid=,command=` are available on every supported host. Both forms are already used in `tests/hub_daemon_lifecycle/process.rs`.
5. `IsolatedHub` can anchor worker identity on `self.hub_bin.parent()`, since the session-worker binary is built into the same target directory as the Hub binary. This is a stronger anchor than an argv substring. Implement verifies this and falls back to the `a39d9ea` `data_dir_arg` rule if it does not hold.

Unknowns for Implement or Review to resolve:

1. Whether the requeued delivery can loop indefinitely if pressure never clears. The router owns holder bounds and shed policy. Implement must confirm that an existing bound retires the holder eventually and must not add a new retry counter if one already exists.
2. Whether the fanout test's completion-drain loop, which already exists on `main` with a 2 s deadline, also needs the raised deadline. Implement decides from the reproduction, not by default.
3. Whether the `Backpressured` requeue needs a counter in `event_plane_counters`. [[Hub event plane lacks seven load campaign signals]] suggests observability gaps exist. Adding a counter is allowed only if Review agrees it is required by this ticket; otherwise it is a follow-up.

## Affected surfaces and files

| File | Change | Kind |
|------|--------|------|
| `src/daemon_maintenance.rs` | Add the `PluginAdmissionResult::Backpressured { .. }` arm in `run_package_event_delivery_slice` before the catch-all. | Production |
| `src/daemon_maintenance.rs` | Re-author the bounded retry loop and yield in `owner_loop_queues_and_completes_two_fanout_plugin_handlers`. | Test |
| `src/daemon_maintenance.rs` | Add the focused red-on-revert `Backpressured` requeue unit test. | Test |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | Add `data_dir_arg` to `IsolatedHub` and `IsolatedHubBuilder`; add the owned-worker census, the descendant closure, the leader-guarded signal helper, and the bounded reap; call the reap on all four shutdown paths. | Test support |
| `docs/plans/fix-fanout-owner-loop-flake-and-isolatedhub-worker-reaping.md` | This plan. | Docs |
| `docs/reports/fix-fanout-owner-loop-flake-and-isolatedhub-worker-reaping-implement.md` | Implement report with gate evidence. | Docs |

No change to `packages/hub-test-support`, `Cargo.toml`, `Cargo.lock`, fixtures, or any client DTO.

## Re-authoring map against the named cancelled commits

Review must compare the final implementation against every named commit and check the stated divergence.

| Cancelled commit | Disposition | Reason |
|------------------|-------------|--------|
| `e2f1995` | Re-author the loop shape. Not named by the ticket but required. | `c32cbe0` is a delta on this loop and does not apply without it. |
| `c32cbe0` | Re-author the yield. Take the yield only as far as deterministic proof requires. | Named by the ticket. |
| `81b42a0` | Re-author the `Backpressured` arm. | Owner-approved in `question_1787895620_428538`. This is the actual root cause. |
| `e053265` | Selectively recover the `isolated_hub.rs` hunks only. | Introduces the census and reap functions the three named commits build on. Its owner-loop drain change is out of scope. |
| `5c30ebc` | Superseded. Take the intent, reject the mechanism. | Its `unique` basename substring match can match an unrelated process. `a39d9ea` removed it deliberately. |
| `6012eca` | Superseded. Take the intent, reject the mechanism. | Its unconditional `killpg` on a non-leader PID can signal the `cargo test` harness process group. `a39d9ea` replaced it with a leader-and-not-our-group guard. |
| `cb36f64` | Partially recover. Keep the descendant closure and the all-paths wiring. Reject `command.contains(unique)`. | The descendant closure and the `wait_for_ready` and `Drop` wiring are correct and required. The widened substring match is not. |
| `a39d9ea` | Selectively recover the `isolated_hub.rs` hunks. This is the target shape. | Owner-approved final shape. Its starved-bind owner-loop change is out of scope. |

## Risks

1. **The requeue changes production behavior under real pressure.** A delivery that used to be dropped now returns to the queue. Mitigation: the arm is reached only on `Backpressured`; every other non-`Queued` variant keeps the existing retire path; router-owned holder bounds still apply; Review checks that no slice budget, wake cadence, or turn order changed.
2. **A requeue loop under sustained pressure.** If the router has no bound, a delivery could cycle. Mitigation: unknown 1 above must be resolved before Implement submits. If no bound exists, Implement stops and asks rather than adding a new one.
3. **Over-broad process reaping.** The rejected `5c30ebc` and `6012eca` mechanisms could kill unrelated processes or the test harness group. Mitigation: exact-token and canonicalized-path matching, a worktree-executable rule, and the leader-and-not-our-group `killpg` guard. Review must confirm all three.
4. **Reaping masks a real Hub leak.** A test-support reap can hide a genuine failure to stop workers. Mitigation: [[hub shutdown preserves durable session workers]] records that Hub stop intentionally preserves durable workers, so the reap is fixture hygiene rather than a cover for a Hub defect. Review must confirm the reap is not called in place of an assertion that a production path already owns.
5. **`Drop` panics.** A panic in `Drop` during unwinding aborts. Mitigation: the reap swallows every error and the existing `thread::panicking()` guards stay.
6. **The gate stays flaky for a third cause.** Two causes are now known. A third may exist. Mitigation: the gate must pass at default workspace concurrency without a waiver, and isolated reruns do not count as proof.
7. **Strict Clippy hides later diagnostics.** Per [[strict clippy can hide later crate diagnostics behind the first compile failure]], one green run after a repair is not proof. Mitigation: rerun the full workspace Clippy after every repair.
8. **Wrong toolchain in the pipeline shell.** Per [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]], a bare strict Clippy can exit 0 on unfixed code. Mitigation: `RUSTUP_TOOLCHAIN=1.97.0` on every gate command, with `rustc --version` recorded from that shell.

## Acceptance checks and tests

### Reproduction, required first

1. Reproduce `daemon_maintenance::tests::owner_loop_queues_and_completes_two_fanout_plugin_handlers` failing under the official default-concurrency workspace gate on unmodified `main`. Record the exact command, the observed `event_in_flight` length, and the run. An isolated single-test rerun is not a reproduction.
2. Reproduce a surviving owned session worker after repeated IsolatedHub shutdown on unmodified `main`.

### Focused proofs

3. `owner_loop_queues_and_completes_two_fanout_plugin_handlers` passes.
4. The new `Backpressured` requeue unit test passes, and **fails** when the `Backpressured` arm is reverted to the catch-all retire path. This is the red-on-revert control. Per [[test names do not prove their bodies can fail on the named claim]], Implement must state the exact assertion that goes red and show it going red.
5. The fanout loop proof must not pass for the wrong reason. Implement states whether the fanout test still passes with the `Backpressured` arm reverted. If it does, the loop alone is masking the defect and the loop's role must be re-justified.
6. Repeated IsolatedHub shutdown leaves no owned session-worker descendants. The test performs at least two shutdown cycles, asserts an empty owned-worker census through a live `ps` census after each, and **fails** when the reap call is removed. Idempotence proof: calling the reap twice on an already-clean data directory returns without error and signals nothing.
7. Bound proof: the reap returns within its stated bound when a worker refuses `SIGTERM`.
8. Isolation proof: the reap leaves a second, concurrently live IsolatedHub instance's workers running.

### Official gates, all in one shell

Per [[Hub official gates must not set CARGO TARGET DIR]] and [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]:

- Worktree path contains no `:`, so no `CARGO_TARGET_DIR` workaround applies. `CARGO_TARGET_DIR` must be **unset** for the official gate. Prebuild in the default worktree `target/`.
- Export `RUSTUP_TOOLCHAIN=1.97.0` and record `rustc --version`.

| Gate | Requirement |
|------|-------------|
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0, before the wrapper, per [[Hub suite runs prebuild the session worker before the locked test wrapper]] |
| `cargo build --locked --bin botster-hub` | exit 0 |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0, rerun in full after every repair |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | assets current |
| `./test.sh --locked` | exit 0 at default workspace concurrency, **no waiver**. Record Hub lib and lifecycle pass counts and elapsed time. |
| `cd packages/hub-test-support && npm install --no-save && npm test` | passes |
| `git diff --check main...HEAD` | clean |

### Downstream proof

The charter requires downstream proof when a Hub change crosses a client seam or closes a consumer failure. This ticket changes no DTO, no protocol, no fixture, and no published package. It closes a failure inside this repository's own gate. **No downstream client proof is required, and none is claimed.** Implement must state this explicitly rather than leaving it unmentioned.

### Review requirements

- Review compares the re-authored changes against `e2f1995`, `c32cbe0`, `81b42a0`, `e053265`, `5c30ebc`, `6012eca`, `cb36f64`, and `a39d9ea`, and checks each divergence in the re-authoring map.
- Review confirms that no `SlotReady`, persistence, or owner-loop observation change entered the diff.
- Review confirms that every slice budget, wake cadence, and owner-turn ordering value is byte-identical to `main`.
- Review confirms that no waiting or retry loop entered the authoritative mutation path.
- Review checks the teardown lens answers in this plan against the diff, per [[botster-runtime-reviewer-playbook]].

## Vault gaps worth capturing

1. **Hub retires transient Core `Backpressured` admissions as permanent failures.** A catch-all `_ =>` arm over `PluginAdmissionResult` silently loses an admitted package-event handler. The durable lesson is that a catch-all over a typed admission result must not collapse transient pressure into permanent failure. Candidate note: "a catch-all admission arm turns transient backpressure into silent event loss".
2. **A test-side retry loop can mask a production drop.** The cancelled branch spent two GitHub Verify cycles adding deadline and sleep before finding that the holder was already destroyed. The durable lesson is that when a bounded retry loop does not fix a count-based flake, the missing item was probably destroyed rather than delayed. Candidate note: "a retry loop cannot recover work the producer already retired".
3. **`IsolatedHub` and the lifecycle test crate own two different worker censuses.** `tests/hub_daemon_lifecycle/process.rs` has the mature implementation with worktree-executable identity, zombie retention, and descendant capture. `crates/botster-hub-test-support/src/isolated_hub.rs` needs its own because a library crate cannot depend on a test binary. This duplication is a real seam and a candidate for a shared census module in a later ticket. Candidate note: "Hub owns two session-worker censuses across the library and test crates".
4. **Cancelled-branch commits are not a recovery unit.** Three named commits here were superseded by a fourth that deliberately reverted their heuristics. The durable lesson is that recovering named commits from a cancelled branch requires reading the branch tip first, because intermediate debug states can be known-unsafe. Candidate note: "recover a cancelled branch from its tip, not from its named intermediate commits".
