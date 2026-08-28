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

## Plan Review response (`review_1787897017_758691`)

Plan Review returned `changes_required` with two blocking findings, one major, and one process-only finding. This visit resolves all four. The reviewer's blocking claim was verified independently against the repository and the pinned Core source; it is correct, and the previous plan's ownership identity was wrong.

### `finding_1787897017_344600` (blocking): the proposed worker census cannot identify real workers

Verified and accepted. Evidence:

- Core spawns the worker in `botster-core/src/runtime/worker_process.rs` around line 1298. The only path argument it passes is `--control-socket <path>`.
- That control socket lives under `worker_socket_dir` in `botster-core-daemon/src/daemon.rs:3771`, which returns `std::env::temp_dir().join(format!("bcd-{:x}", hasher.finish()))`, a hash of the data directory under `$TMPDIR`.
- The Hub data-directory string therefore **never appears in a session worker's argv**.

Consequence: the `a39d9ea` census, which requires an exact `--data-dir` token or a canonicalized data-directory path in argv, would match zero workers. The reap would silently no-op, and the red-on-revert control could never go red. Corroborating evidence that this is a known weakness: `tests/hub_daemon_lifecycle/process.rs:498` computes `owned_by_dir` by data-directory match and then **falls back to `live_alive`** when that set is empty.

Resolution: replace the argv-based census with a live per-instance identity that already exists in the code. `crates/botster-hub-test-support/src/isolated_hub.rs` spawns the Hub child with `pre_exec` calling `setpgid(0, 0)`, so the Hub child is a process-group leader whose pgid equals its pid, and `cleanup_child` already depends on this. Core spawns the worker with no `pre_exec` and no `process_group`, so every worker inherits that pgid. Owner identity becomes `(argv0 basename == "botster-session-worker", pgid == Hub-child pid)`. This is the same shape as the shipped `OwnedWorkerIdentity { pid, pgid, control_socket }` in `src/local_webrtc.rs:5445`.

This diverges further from `a39d9ea` than the previous plan did. The divergence is required, and it honors both owner constraints from `question_1787895620_428538`: no basename substring matching, and no unconditional `killpg` on a non-leader PID. The `data_dir_arg` field from `a39d9ea` is no longer needed and is not added.

### `finding_1787897017_291849` (blocking): the 15-second bound did not cover the complete Drop path

Accepted. The previous plan bounded each reap **call** at 15 s, and `Drop` can call the reap up to three times, so the real worst case compounded. Resolution: one shared `TeardownBudget`. **This was still incomplete, and `finding_1787898274_776728` on visit 3 caught the remainder:** bounding only the reaps left `Command::output()` for the shutdown command and `child.wait_with_output()` unbounded ahead of them, so `Drop` could hang before reaping began. The budget now covers all four phases with typed timeout transitions. See `teardown_bounds` above.

### `finding_1787897017_328086` (major): the requeue bound was an unresolved assumption

Resolved during this visit rather than deferred to Implement. **Corrected on visit 3 after `finding_1787898274_940865`: the capacity check is not the cycle bound. The queue age is.**

Why the capacity claim was wrong. `pull_ready_batch` (`src/package_event_router.rs:686`) pops the copy and **decrements** occupancy before admission: `queue.events = queue.events.saturating_sub(1)` and `queue.bytes = queue.bytes.saturating_sub(size)` at lines 727 and 728. `requeue_delivery` then adds the same delivery back with `consumer.events += 1` and `consumer.bytes += delivery.size`. Both touch the same `inner.consumers` entry, so a pull-then-requeue cycle is **net zero** on occupancy. `consumer_queue_max_events` and `consumer_queue_max_bytes` therefore never grow across repeated cycles of one delivery, and `ShedFull` cannot end that cycle.

The actual bound is `policy.queue_age` on the preserved envelope timestamp. `pull_ready_batch` reads `envelope.enqueued_at.elapsed() > inner.policy.queue_age` at line 717, and on expiry records `record_router_queue_age_expiry()` and calls `retire_holder_locked` at lines 735 to 741, dropping the copy instead of delivering it. `requeue_delivery` pushes a `QueuedCopy { envelope_id, holder }` and **never touches `envelope.enqueued_at`**, so the original enqueue time survives arbitrarily many pull and requeue cycles. Elapsed age is monotonic, so the next pull after `queue_age` expires retires the holder. That is a real, monotonic termination bound, and it belongs to the router, which is the correct owner.

`ShedFull` remains a genuine bound for a different case: concurrent new events raising real occupancy while a delivery is out for admission. The plan's `Err` branch retires the holder there through the existing path. It is simply not the bound that terminates a repeated requeue of one delivery.

Existing proofs to cite rather than re-derive: `expired_queued_copy_does_not_deliver` (`src/package_event_router.rs:2486`) covers queue-age expiry at pull, and `consumer_expiry_and_byte_limit_requeue_refresh_oldest_age` (line 3655) together with `consumer_oldest_age_tracks_front_envelope_across_mutations` (line 3594) cover age preservation across requeue, including the assertion that "requeue to the front must restore the older envelope age". Acceptance adds one focused repeated-requeue expiry proof so the termination argument is executable rather than inferred.

**No new retry counter and no new `event_plane_counters` signal are needed**, which also closes plan unknown 3.

### `finding_1787897017_789743` (process-only): Plan completion evidence omitted `artifact_id`

The Plan gate evidence did include `artifact_id`; the engine's `step.completed` payload carried an empty evidence map. This visit passes the completion evidence explicitly on step advance, including `artifact_id`, so the `step.completed` event records it.

### Visit 3 response to `review_1787898274_676211`

Plan Review returned `changes_required` a second time with one blocking and one major product finding. Both were verified against the source and both are correct.

**`finding_1787898274_776728` (blocking): the complete shutdown path still had unbounded waits.** Confirmed. `shutdown_inner` (`crates/botster-hub-test-support/src/isolated_hub.rs:255`) calls `Command::new(&self.hub_bin).arg("shutdown")...output()` and then `child.wait_with_output()`. Neither has a deadline, and `Drop` calls `shutdown_inner`, so `Drop` could hang indefinitely before the reap budget ever applied. Bounding reaps alone did not satisfy the bounded-close lens. Resolution: the single `TeardownBudget` now bounds four phases with typed timeout transitions. The visit-3 draft named a helper thread plus `recv_timeout` as the mechanism; visit 4 replaced that with an explicit spawn, pipe-only drain threads, and `try_wait` polling, because `wait_with_output` consumes the `Child`. See `teardown_bounds` above and the visit-4 response below.

**`finding_1787898274_940865` (major): the plan named the wrong bound for repeated Backpressured requeue.** Confirmed. `pull_ready_batch` decrements `queue.events` and `queue.bytes` on the same `inner.consumers` entry that `requeue_delivery` increments, so a pull-then-requeue cycle is net zero on occupancy and `ShedFull` can never end it. The real bound is `policy.queue_age` measured on the envelope's preserved `enqueued_at`, which `requeue_delivery` never resets. The incorrect claim, the stale Risk 2 text, and the stale unknown text are all corrected, and acceptance adds a focused repeated-requeue expiry proof.

### Visit 4 response to `review_1787899206_465984`

One blocking finding, verified against the Rust ownership rules and the existing code, and correct.

**`finding_1787899206_353296` (blocking): the timeout helper loses the `Child` handle required for cleanup.** Confirmed. `Child::wait_with_output(self)` takes `self` by value, so moving the Hub child into a helper thread destroys the teardown owner's access to it. The visit-3 plan then said the Hub-child timeout "falls through to `cleanup_child`", but `cleanup_child(child: &mut Child)` needs a `&mut Child` that no longer exists. The same defect applies to the shutdown command, because `Command::output()` spawns and consumes its `Child` internally and never exposes it, so the timeout path had nothing to kill or reap. Passing a PID does not restore a `Child`, and it does not permit `cleanup_child` or `wait()` reaping.

Resolution, exactly the shape the reviewer prescribed and applied identically to both commands:

1. Spawn explicitly with `Command::spawn()` and piped stdio instead of `output()`.
2. `take()` the `ChildStdout` and `ChildStderr` and move **only those pipes** into one drain thread each. The `Child` never leaves the teardown owner.
3. Poll `Child::try_wait()` against the shared `TeardownBudget` deadline.
4. On timeout, signal and reap through the retained `Child`: `cleanup_child(&mut child)` for the Hub child, a direct kill plus `wait()` for the shutdown command.
5. Then `join()` both drain threads and collect bounded diagnostics.

Ordering matters: reaping before joining is what unblocks the drain threads, since each returns when its pipe closes at process exit. Each drain thread caps its buffer so a chatty stalled child cannot grow memory without bound, while still supplying `ShutdownFailed { stderr }` and `DaemonExited { status, stdout, stderr }`. Acceptance now requires proof that **every** drain thread exits after a timeout.

## Scope

### In scope

1. **Production: preserve a `Backpressured` package-event delivery.** In `src/daemon_maintenance.rs`, `run_package_event_delivery_slice`, add an explicit `PluginAdmissionResult::Backpressured { .. }` arm before the catch-all. The arm releases the causal-scope lease it just minted, requeues the delivery through the existing `PackageEventRouter::requeue_delivery`, and wakes the scheduler. If `requeue_delivery` itself fails, the arm falls back to the existing retire path. `src/runtime.rs` already uses `requeue_delivery` this way, so this is the established Hub idiom, not a new mechanism.
2. **Test: make the fanout test survive an elapsed slice cut.** Re-author the bounded retry loop and yield around `run_package_event_delivery_slice` in `owner_loop_queues_and_completes_two_fanout_plugin_handlers`. `EVENT_DELIVERY_MAX_ELAPSED` is 8 ms, so one production slice can legitimately return one ready handler under default-concurrency lib load. The loop repeats the production slice, re-arms the delivery wake, and yields between attempts.
3. **Test: add a red-on-revert unit test for the `Backpressured` arm.** A focused test that fails when a `Backpressured` admission retires the holder instead of requeueing it.
4. **Test support: reap IsolatedHub session-worker descendants.** In `crates/botster-hub-test-support/src/isolated_hub.rs`, add a bounded, idempotent reap of the session workers in this instance's Hub-child process group, plus their captured descendants, on every shutdown path: successful `shutdown_inner`, failed shutdown, `wait_for_ready` failure during build, and `Drop`. One shared `TeardownBudget` bounds the whole path.

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
| `teardown_isolation` | The ownership set is exactly the session workers in this IsolatedHub's Hub-child process group, plus their process descendants. IsolatedHub already places its Hub child in a dedicated process group: the spawn block calls `setpgid(0, 0)` in `pre_exec`, so the Hub child is a process-group leader whose pgid equals its own pid. Core spawns `botster-session-worker` with no `pre_exec` and no `process_group`, so every worker inherits that pgid. One IsolatedHub instance therefore never reaps another instance's workers, even when both run concurrently under the same worktree and the same user. Sibling IsolatedHub instances and the `cargo test` harness are unaffected. |
| `teardown_bounds` | **Revised again on visit 4 after `finding_1787899206_353296`.** One `TeardownBudget` deadline is created on entry to `shutdown` and to `Drop`, and it bounds **every phase**. Visit 3 bounded the right phases but named an unworkable mechanism: `Child::wait_with_output(self)` **consumes** the `Child`, so a helper thread that owns it leaves the teardown path with no `&mut Child` to pass to `cleanup_child`, and `Command::output()` never exposes its `Child` at all. A PID alone cannot restore either. **Corrected mechanism, applied identically to the shutdown command and the Hub child:** spawn each command explicitly with `Command::spawn()` and piped stdio; immediately `take()` the `ChildStdout` and `ChildStderr` and move **only those pipes** into one drain thread each, so the `Child` itself never leaves the teardown owner; poll `Child::try_wait()` against the shared deadline. On timeout, signal and reap through the **retained** `Child` — `cleanup_child(&mut child)` for the Hub child, which already escalates `SIGTERM` then `SIGKILL` over its process group, and a direct kill plus `wait()` for the shutdown command — then `join()` both drain threads and collect their bounded output. Reaping before joining is what lets the drain threads finish: each returns when its pipe closes at process exit. Each drain thread caps what it buffers, so a chatty stalled child cannot grow memory without bound while still filling `ShutdownFailed { stderr }` and `DaemonExited { status, stdout, stderr }`. Phases and budgets: (1) shutdown command, 5 s, then `TeardownTimeout { phase: ShutdownCommand }` and fall through to phase 3; (2) Hub-child wait, 5 s, then `TeardownTimeout { phase: HubChildWait }` and fall through to phase 3; (3) `cleanup_child`, existing internal bound of about 2.5 s; (4) reap, remaining budget with a 10 s ceiling, `SIGTERM` first, `SIGKILL` after 400 ms, 50 ms between census turns, unconditional return on expiry. Repeated reap calls share the one deadline and cannot compound. Worst-case whole-path teardown is roughly 22.5 s. Every timeout transition is typed and recorded rather than silent, no phase can block forever, no drain thread outlives the teardown, and no phase propagates a panic out of `Drop`. |
| `late_message_matrix` | Five ownership-creating surfaces, each with tag, reject, and sweep. (1) *A session worker spawned before shutdown*: tagged by argv0 basename `botster-session-worker` **and** pgid equal to this instance's Hub-child pid; swept by the census loop. (2) *A worker spawned during the shutdown race*: the reap runs after `child.wait_with_output()` returns, so the Hub child can no longer spawn; a worker that appeared just before the exit still carries the inherited pgid and is caught, because the census re-runs every 50 ms until empty or the budget expires. A process group outlives its dead leader while any member is alive, so pgid identity stays valid after the Hub child exits. (3) *A PTY command descendant of a worker*: **not** in the worker's process group. Core's PTY path makes each session command its own process-group leader, so `killpg` on the Hub group cannot reach it. It is tagged by the `ps` parent/child closure of each owned worker, captured **before** the worker is signalled, because a dead worker's children are reparented and the ppid link is lost. This is the exact case the ticket names as "descendant cleanup", per [[session registry process pid identifies the pty command not the session worker]]. (4) *A worker owned by a different live IsolatedHub or a different worktree*: rejected by the pgid mismatch; never swept. (5) *A zombie worker*: retained in the census for absence proof and never signalled, per [[zombie recovery workers are dead for liveness but remain in absence proof]]. |
| `production_path_proof` | Exact path: the test calls `IsolatedHub::shutdown` or drops the guard, `shutdown_inner` snapshots the owned worker set while the Hub child is still alive, sends the daemon shutdown request, `child.wait_with_output()` returns, `reap_owned_session_workers` runs under the shared teardown budget, the census empties, and `remove_data_dir` runs. The oracle is a live `ps` census taken after the reap returns, asserting zero owned workers and zero owned descendants. The proof drives the real `IsolatedHub` shutdown path, not a helper. Red-on-revert control: with the reap call removed, the same test must observe a surviving worker. Because the previous plan's data-directory census could never match a real worker, Implement must additionally show the census returning a **non-empty** owned set on a live Hub before shutdown. A reap that silently finds nothing is indistinguishable from a reap that works, and that positive control is what makes the red-on-revert arm meaningful. All four shutdown paths are exercised, not only the happy path. |
| `ownership_identity` | Owner identity is `(argv0 basename == "botster-session-worker", pgid == this instance's Hub-child pid)`. The Hub-child pid is captured into the `IsolatedHub` struct at spawn so it survives `self.child = None` on the `Drop` path. This identity needs no argv inspection of the data directory, no path canonicalization, and no worktree heuristic. To close PID recycling after the Hub child exits, `shutdown_inner` also snapshots the owned worker PID set while the Hub is still alive, and the reap never signals a PID that is neither in that snapshot nor a captured descendant of one. Each IsolatedHub has a distinct Hub-child pid for its lifetime, so a concurrent instance can never be captured. |
| `sibling_fail_closed_policy` | On a successful reap, siblings keep working; nothing outside the owned set is signalled. On ultimate failure, meaning a still-non-empty census when the shared teardown budget expires, the reap returns without escalating and without sacrificing any sibling. Blast radius is bounded to the owned set in both cases. `killpg` is sent only to a group this instance owns: the Hub-child group, or a captured PTY descendant's own leader group. It is never sent to a non-leader PID and never to this process's own group, so the reap cannot signal the `cargo test` harness. The plan explicitly rejects `6012eca`'s unconditional `killpg` on a non-leader PID for this reason. |

## Assumptions and unknowns

Assumptions:

1. `PackageEventRouter::requeue_delivery` returns the delivery on error and is safe to call from the owner-loop slice. `src/runtime.rs` already calls it in exactly that shape, so this is established, not new.
2. Releasing the minted causal-scope lease before requeueing is correct, because the delivery has not been admitted to a worker and no completion will arrive to release it. This mirrors `81b42a0`.
3. `Backpressured` is reachable under default-concurrency workspace load with the shipped plugin worker queue and executor values. The branch's two GitHub Verify failures are the evidence. Implement must confirm reachability through a deterministic seam rather than ambient load. If no deterministic seam exists, Implement records that and the red-on-revert unit test drives the arm directly.
4. `ps -axo pid=,ppid=` and `ps -axo pid=,command=` are available on every supported host. Both forms are already used in `tests/hub_daemon_lifecycle/process.rs`.
5. A session worker inherits the Hub child's process group. Verified this visit: IsolatedHub's spawn block calls `setpgid(0, 0)` in `pre_exec`, and Core's `worker_process.rs` spawn sets neither `pre_exec` nor `process_group`. Implement must re-verify this against the pinned Core revision in the lockfile, because it is a cross-repository behavioral dependency. If a future Core revision gives the worker its own group, the fallback is to snapshot the Hub child's descendant closure before the Hub exits and reap that captured set.

Unknowns for Implement or Review to resolve:

1. ~~Whether the requeued delivery can loop indefinitely.~~ **Resolved. Corrected on visit 3.** The bound is `policy.queue_age` on the envelope's preserved `enqueued_at`, not consumer capacity: a pull decrements occupancy and the requeue restores it, so capacity is net zero across cycles. `pull_ready_batch` retires the holder once the preserved age expires. No new counter is needed. See the Plan Review response above.
2. Whether the fanout test's completion-drain loop, which already exists on `main` with a 2 s deadline, also needs the raised deadline. Implement decides from the reproduction, not by default.
3. ~~Whether the `Backpressured` requeue needs a new `event_plane_counters` signal.~~ **Resolved this visit: no.** The router already accounts the outcome through its existing shed and retire paths. Adding a counter would exceed the smallest surgical change and is not in scope.

## Affected surfaces and files

| File | Change | Kind |
|------|--------|------|
| `src/daemon_maintenance.rs` | Add the `PluginAdmissionResult::Backpressured { .. }` arm in `run_package_event_delivery_slice` before the catch-all. | Production |
| `src/daemon_maintenance.rs` | Re-author the bounded retry loop and yield in `owner_loop_queues_and_completes_two_fanout_plugin_handlers`. | Test |
| `src/daemon_maintenance.rs` | Add the focused red-on-revert `Backpressured` requeue unit test. | Test |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | Capture the Hub-child pid into `IsolatedHub` at spawn so it survives `self.child = None`; add the pgid-scoped owned-worker census, the pre-signal descendant closure, the leader-guarded signal helper, a shared four-phase `TeardownBudget` with a typed `IsolatedHubError::TeardownTimeout { phase }`; replace `Command::output()` and `wait_with_output()` with explicit `spawn()`, pipe-only drain threads with a bounded buffer, and `try_wait` polling that keeps the `Child` on the teardown owner for `cleanup_child` and `wait()`; and the bounded idempotent reap; call the reap on all four shutdown paths. No `data_dir_arg` field is added. | Test support |
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
| `a39d9ea` | Recover its **safety** rules only: the leader-and-not-our-group `killpg` guard and the removal of substring matching. Do **not** recover its data-directory census or its `data_dir_arg` field. | Plan Review `finding_1787897017_344600`, verified: a real worker's argv carries only `--control-socket $TMPDIR/bcd-<hash>/...`, never the data directory, so the `a39d9ea` census matches zero workers and the reap would silently no-op. The pgid identity replaces it. Its starved-bind owner-loop change stays out of scope. |

## Risks

1. **The requeue changes production behavior under real pressure.** A delivery that used to be dropped now returns to the queue. Mitigation: the arm is reached only on `Backpressured`; every other non-`Queued` variant keeps the existing retire path; router-owned holder bounds still apply; Review checks that no slice budget, wake cadence, or turn order changed.
2. **A requeue loop under sustained pressure.** Closed. The termination bound is `policy.queue_age` measured on the envelope's preserved `enqueued_at`, which `requeue_delivery` never resets, so `pull_ready_batch` retires the holder once the age expires. Consumer capacity is **not** the cycle bound, because a pull decrements occupancy and the requeue restores it, leaving the cycle net zero. Acceptance requires a focused repeated-requeue expiry proof.
3. **Over-broad process reaping.** The rejected `5c30ebc` and `6012eca` mechanisms could kill an unrelated process or the test harness group. Mitigation: pgid equality against this instance's Hub-child pid, a snapshot set that bounds which PIDs may be signalled, and a `killpg` guard that fires only on a group this instance owns. Review must confirm all three.
4. **A silently empty census.** This is the defect Plan Review caught: a census that matches nothing passes every negative assertion. Mitigation: Implement must show the census returning a non-empty owned set on a live Hub before shutdown, as a positive control, in addition to the red-on-revert arm. Review must confirm the positive control exists.
5. **PID recycling after the Hub child exits.** The pgid equals the dead Hub child's pid, which the kernel can reuse. Mitigation: the reap never signals a PID that is neither in the pre-shutdown snapshot nor a captured descendant of one.
6. **Core changes the worker's process group.** Assumption 5 is a cross-repository behavioral dependency on the pinned Core revision. Mitigation: Implement re-verifies it against the lockfile revision and records the check; the named fallback is a pre-exit descendant snapshot.
7. **The bounded wait reintroduces a pipe deadlock.** `wait_with_output` exists partly to drain the child's piped stdout and stderr; a bare `try_wait` poll would let a chatty child block on a full pipe buffer. Mitigation, corrected on visit 4: `take()` the `ChildStdout` and `ChildStderr` and drain each on its own thread while the teardown owner keeps the `Child` and polls `try_wait`. Draining continues and the `Child` stays available for `cleanup_child`. Acceptance check 11(c) is the control.
8. **A leaked drain thread after a phase timeout.** A timed-out phase can leave a drain thread blocked reading a pipe that never closes. Mitigation: the timeout path signals and reaps the child through the retained `Child` **before** it joins, so each pipe closes at process exit and both drain threads return. Acceptance check 11(d) requires proof that every drain thread exits after a timeout, and Implement must confirm no drain thread outlives the teardown.
9. **Reaping masks a real Hub leak.** A test-support reap can hide a genuine failure to stop workers. Mitigation: [[hub shutdown preserves durable session workers]] records that Hub stop intentionally preserves durable workers, so the reap is fixture hygiene rather than a cover for a Hub defect. Review must confirm the reap is not called in place of an assertion that a production path already owns.
10. **`Drop` panics.** A panic in `Drop` during unwinding aborts. Mitigation: the reap swallows every error and the existing `thread::panicking()` guards stay.
11. **The gate stays flaky for a third cause.** Two causes are now known. A third may exist. Mitigation: the gate must pass at default workspace concurrency without a waiver, and isolated reruns do not count as proof.
12. **Strict Clippy hides later diagnostics.** Per [[strict clippy can hide later crate diagnostics behind the first compile failure]], one green run after a repair is not proof. Mitigation: rerun the full workspace Clippy after every repair.
13. **Wrong toolchain in the pipeline shell.** Per [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]], a bare strict Clippy can exit 0 on unfixed code. Mitigation: `RUSTUP_TOOLCHAIN=1.97.0` on every gate command, with `rustc --version` recorded from that shell.

## Acceptance checks and tests

### Reproduction, required first

1. Reproduce `daemon_maintenance::tests::owner_loop_queues_and_completes_two_fanout_plugin_handlers` failing under the official default-concurrency workspace gate on unmodified `main`. Record the exact command, the observed `event_in_flight` length, and the run. An isolated single-test rerun is not a reproduction.
2. Reproduce a surviving owned session worker after repeated IsolatedHub shutdown on unmodified `main`.

### Focused proofs

3. `owner_loop_queues_and_completes_two_fanout_plugin_handlers` passes.
4. The new `Backpressured` requeue unit test passes, and **fails** when the `Backpressured` arm is reverted to the catch-all retire path. This is the red-on-revert control. Per [[test names do not prove their bodies can fail on the named claim]], Implement must state the exact assertion that goes red and show it going red.
5. The fanout loop proof must not pass for the wrong reason. Implement states whether the fanout test still passes with the `Backpressured` arm reverted. If it does, the loop alone is masking the defect and the loop's role must be re-justified.
6. **Positive control first.** Before any absence assertion, the test must show the owned-worker census returning a **non-empty** set while the Hub child is still alive, with at least one running session. An always-empty census satisfies every absence assertion without doing anything, which is exactly the defect Plan Review caught in `finding_1787897017_344600`.
7. Repeated IsolatedHub shutdown leaves no owned session-worker descendants. The test performs at least two shutdown cycles, asserts an empty owned-worker census through a live `ps` census after each, and **fails** when the reap call is removed. Idempotence proof: calling the reap twice on an already-clean instance returns without error and signals nothing.
8. **Descendant proof.** At least one session must run a PTY command that outlives a plain worker kill, so the test proves the captured descendant closure is reaped and not only the worker itself. A worker-only proof would pass even if descendant handling were removed.
9. **Isolation proof.** A second, concurrently live IsolatedHub instance keeps its workers running across the first instance's full teardown. This is the assertion the old data-directory census could not make.
10. **Whole-path bound proof.** With a worker that ignores `SIGTERM`, a full `Drop` teardown returns within the stated overall bound. The assertion is on total elapsed teardown time, not on one reap call.
11. **Unbounded-wait proofs, added on visit 3 for `finding_1787898274_776728`.** (a) A stalled `shutdown` command: teardown still returns within the stated whole-path bound and records `TeardownTimeout { phase: ShutdownCommand }`. (b) A Hub child that does not exit after a successful shutdown command: teardown still returns within the bound, records `TeardownTimeout { phase: HubChildWait }`, and still reaps its owned workers. Both assert total elapsed teardown time and the typed timeout, not one phase. (c) A pipe-drain control: the stalled child writes more than one pipe buffer to stdout, proving the bounded wait does not reintroduce the deadlock that `wait_with_output` exists to prevent. (d) A drain-thread-exit control, added on visit 4: after each timeout path, **every** drain thread has been joined and returned, and the retained `Child` was reaped, so no thread and no zombie outlives the teardown. (e) Diagnostics survive the redesign: `ShutdownFailed { stderr }` and `DaemonExited { status, stdout, stderr }` still carry their captured output on the ordinary non-timeout paths, and carry bounded output on the timeout paths.
12. **Repeated-requeue expiry proof, added on visit 3 for `finding_1787898274_940865`.** A delivery is pulled and requeued repeatedly under a short `policy.queue_age`. The proof asserts that consumer occupancy does **not** grow across cycles, so capacity cannot be the terminating bound, and that `pull_ready_batch` eventually retires the holder and records `record_router_queue_age_expiry()` once the preserved `enqueued_at` age expires. Cite `expired_queued_copy_does_not_deliver` (`src/package_event_router.rs:2486`), `consumer_expiry_and_byte_limit_requeue_refresh_oldest_age` (line 3655), and `consumer_oldest_age_tracks_front_envelope_across_mutations` (line 3594) for existing age-preservation and expiry coverage.
13. **Cross-repository re-check.** Confirm against the lockfile Core revision that the session worker still inherits the Hub child's process group.


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
- Review confirms the owned-worker census is proved non-empty on a live Hub before any absence assertion is trusted, and that no assertion in the new tests can pass on an always-empty census.
- Review confirms one shared teardown budget bounds **every** phase of the `Drop` and shutdown path, including the external shutdown command and the Hub-child wait, that each timeout transition is typed and recorded rather than silent, and that repeated reap calls cannot compound the budget.
- Review confirms the teardown owner retains each `Child` and moves only `ChildStdout` and `ChildStderr` to drain threads, that no code path moves a `Child` into a thread or uses `Command::output()` on the teardown path, that every timeout reaps through the retained `Child` before joining, and that each drain thread buffers a bounded amount.
- Review confirms the plan and the diff name `policy.queue_age` on the preserved `enqueued_at` as the repeated-requeue bound, and that no text still claims consumer capacity or `ShedFull` ends that cycle.

## Vault gaps worth capturing

1. **Hub retires transient Core `Backpressured` admissions as permanent failures.** A catch-all `_ =>` arm over `PluginAdmissionResult` silently loses an admitted package-event handler. The durable lesson is that a catch-all over a typed admission result must not collapse transient pressure into permanent failure. Candidate note: "a catch-all admission arm turns transient backpressure into silent event loss".
2. **A test-side retry loop can mask a production drop.** The cancelled branch spent two GitHub Verify cycles adding deadline and sleep before finding that the holder was already destroyed. The durable lesson is that when a bounded retry loop does not fix a count-based flake, the missing item was probably destroyed rather than delayed. Candidate note: "a retry loop cannot recover work the producer already retired".
3. **`IsolatedHub` and the lifecycle test crate own two different worker censuses.** `tests/hub_daemon_lifecycle/process.rs` has the mature implementation with worktree-executable identity, zombie retention, and descendant capture. `crates/botster-hub-test-support/src/isolated_hub.rs` needs its own because a library crate cannot depend on a test binary. This duplication is a real seam and a candidate for a shared census module in a later ticket. Candidate note: "Hub owns two session-worker censuses across the library and test crates".
4. **Cancelled-branch commits are not a recovery unit.** Three named commits here were superseded by a fourth that deliberately reverted their heuristics. The durable lesson is that recovering named commits from a cancelled branch requires reading the branch tip first, because intermediate debug states can be known-unsafe. Candidate note: "recover a cancelled branch from its tip, not from its named intermediate commits".
5. **A session worker's argv never names the Hub data directory.** Core passes only `--control-socket $TMPDIR/bcd-<hash of data_dir>/...`, so every data-directory argv census silently matches zero workers. The existing lifecycle-test census hides this behind a fallback to the worktree-only set. The durable identity is the Hub child's process group, which IsolatedHub already creates with `setpgid(0, 0)`. Candidate note: "Hub session workers are identified by process group, not by a data-directory argv".
6. **An absence oracle needs a presence control.** A census that matches nothing passes every absence assertion. The durable lesson is that a cleanup proof must first show the census finding the thing it later proves absent. Candidate note: "prove the census non-empty before trusting an absence assertion".
7. **A phase budget is not a path budget.** Bounding the reaps left two unbounded blocking waits ahead of them, so the control path could still hang before the bounded work began. The durable lesson is that a teardown bound must start at the first blocking call on the path, not at the phase the author was thinking about. Candidate note: "bound the teardown path from its first blocking call, not from the phase you are editing".
8. **A net-zero counter cannot bound a retry cycle.** Consumer capacity looked like the requeue bound, but the pull decrements the same counter the requeue increments, so the cycle is net zero and the capacity check never fires. The durable lesson is that a termination argument needs a quantity that moves monotonically in one direction; here that quantity is the preserved envelope age. Candidate note: "a retry bound needs a monotonic quantity, not a counter the retry restores".
9. **A bounded wait must keep the `Child` and move only the pipes.** `Child::wait_with_output` and `Command::output` both consume the handle the timeout path needs for cleanup, so the obvious "run it on a thread with a timeout" shape cannot also kill and reap. The durable shape is: spawn explicitly, take the pipes onto drain threads, poll `try_wait`, reap through the retained `Child`, then join. Candidate note: "a bounded child wait keeps the Child and moves only its pipes".
