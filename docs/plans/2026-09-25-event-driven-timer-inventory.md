# Botster Hub timer inventory — event-driven rewrite (2026-09-25)

Writer: Claude Hub writer, session `sess-1790388284-006e-2c0553fc5128dca3e436b363414462b1`.
Checkout: branch `delivery/event-driven-20260925` at Hub main `23b0feaa`. Read only; no cargo.
Rules: `/private/tmp/botster-event-driven-rewrite-brief-20260925.md`. The allowed categories are deadline, backoff, rate-limit, os-no-event, ui-lifetime (the Hub has no sites) and measurement-window (resource probes and benchmarks only). Everything else is DEFECT. NOT-TIMER marks only true false positives: `set_read_timeout(None)`, a comment or doc, a constant or function definition, a Lua `select`, or a timer API identifier that is not a call (an import, a test name). A fixture `sleep N` is never NOT-TIMER.

## Revision 2 (after reviewer + orchestrator rulings)

What changed from the committed inventory (176e6c72):

1. **Fixture sleeps are DEFECT.** Every `sleep N` / `exec sleep N` / `while true; do sleep 1; done` / Python `time.sleep` / Ruby `sleep` / Node `setInterval` that keeps a test child alive or delays a fixture is now DEFECT. Replacement for all of them: *blocking read the test ends: `cat` on stdin or a FIFO the test closes.* Every former NOT-TIMER row was re-examined; each now states its new category. The subtype **DEFECT (mechanical)** marks a timer call that does not wait at all, such as `recv_timeout(Duration::ZERO)`. Its replacement drops the timer API, for example `try_recv()`.
2. **Timeout values never change.** No row suggests a new value any more. Suggestions moved to "Follow-ups (not applied)" at the end, among them allocation_oracle.rs:534.
3. **os-no-event must hold on macOS and on Linux.** Child-process exit is never os-no-event.
   - run-loaded-daemon-lifecycle-selftest 332 and 410 are now DEFECT. Both are inside `runner_platform == Linux` blocks, so pidfd EPOLLHUP at reap is the event.
   - publish-npm-packages 144 is now DEFECT. It needs an orchestrator decision.
   - isolated_hub.rs 2187 stays os-no-event (candidate), with the facts for both platforms.
   - plugin_bounds.rs:231 is now DEFECT (rule 6).
   - The group-empty "os-no-event part" of the C and D rows is gone (M2).
4. **Library APIs that poll internally are polls.** Python `Popen.wait(timeout)`, `subprocess.run(timeout=)` and `communicate(timeout=)` use WNOHANG plus sleep.
   - Replacements that proposed them now say: blocking wait on a thread plus a completion queue (or pidfd/kqueue).
   - 18 script sites whose own mechanism is one of these calls are added to group D as DEFECT (extra, not in grep).
5. **M2 (process group empty)** now describes the final implementation in `src/process_exit.rs` and has no os-no-event residual.
   - macOS takes atomic `proc_listpgrppids` snapshot rounds with NOTE_EXIT per member.
   - Linux watches running members with pidfd POLLIN and zombie members with POLLHUP. The group is empty when `killpg(pgid, 0) == ESRCH`.
   - The group-empty rows in B2, C and D now point to M2.
6. **measurement-window.** It is applied to probe-hub-resources:232 and test-production-package-runtime:590. It is also applied to the scheduler-lag instruments in event_plane_saturation.rs (1646, 1877), which are host-validity probes whose interval is the metric; the reviewer should confirm. It is not applied to plugin_bounds.rs:231, which asserts an idle-CPU bound for correctness, so that site is DEFECT.
7. **Reviewer corrections.**
   - Zombie-state waits (D mechanism H, process-census 229/272, selftest 308/355/378) prefer blocking `waitid(WEXITED|WNOWAIT)` for a child, or pidfd/kqueue. Pipe EOF does not prove zombie state.
   - The allocation-oracle shrink suggestion was removed from its row.
8. **Coverage additions.**
   - Bare `webrtc::runtime::timeout(` calls were outside the original grep. All 63 are now classified one by one in group C: 24 in `src/` (all deadline) and 39 in the lifecycle test files (25 DEFECT, 12 deadline, 2 NOT-TIMER).
   - Sites added between 23b0feaa and 20b201bd are in a new section at the end, with line numbers at 20b201bd.
   - **All other line numbers are at 23b0feaa.** The same section maps the moved lines.

## Method

1. The scan covers all git-tracked files except `vendor/` and except md/json/lock/toml/yml/txt/png/svg files: 298 files.
2. The grep patterns were `sleep(`, shell `sleep N`, `recv_timeout|wait_timeout|park_timeout`, `set_read_timeout|set_write_timeout`, `setTimeout|setInterval|waitForTimeout`, `time::timeout|timeout_at|interval(`, `yield_now|spin_loop`, and `poll(|kevent(|epoll_wait|select(`. False-positive `.select(` and Lua `select('#'` lines were removed. That left **875 grep lines**.
3. Each line was classified against the source around it. Each group also scanned its files for poll loops the grep cannot see: `try_recv`/`try_wait`/`Instant` loops with no timer call, busy spins, and embedded shell, Python and JS fixtures. Those rows are marked "(extra, not in grep)".
4. The five groups are appended below: A = `src/daemon*`, B1 = four large lifecycle test files, B2 = the other lifecycle test files, C = the rest of `src/`, D = crates, scripts, and the other `tests/*.rs`.

## Totals (the 875 grep lines)

| category | A | B1 | B2 | C | D | total |
|---|---|---|---|---|---|---|
| DEFECT | 128 | 114 | 118 | 141 | 118 | **619** |
| deadline | 58 | 44 | 17 | 78 | 20 | **217** |
| rate-limit | 0 | 4 | 0 | 0 | 2 | **6** |
| backoff | 0 | 0 | 0 | 1 | 0 | **1** |
| measurement-window | 0 | 2 | 0 | 0 | 2 | **4** |
| os-no-event (candidate) | 0 | 0 | 0 | 0 | 1 | **1** |
| NOT-TIMER | 0 | 7 | 8 | 1 | 11 | **27** |
| grep lines | 186 | 171 | 143 | 221 | 154 | **875** |
| extra DEFECT sites (not in grep) | 13 | 5 | ~56 | 10 | 44 | ~128 |
| bare `timeout(` calls (not in grep): DEFECT / deadline / NOT-TIMER | 0 | 2 / 0 / 0 | 23 / 12 / 2 | 0 / 24 / 0 | 0 | **25 / 36 / 2** |

Revision 1 totals were DEFECT 539, deadline 214, rate-limit 7, backoff 1, os-no-event 5 and NOT-TIMER 109. The moves:
- NOT-TIMER to DEFECT: 72 lines. 59 are fixture sleeps (B1 18, B2 20, C 1, D 20). 13 are mechanical zero-duration or scheduler-yield calls (A 7, B1 1, C 5).
- NOT-TIMER to deadline: 9 lines (primitive forwarders and restores, and allocation_oracle.rs:534).
- os-no-event to DEFECT: 4 lines (selftest 332 and 410, publish-npm 144, plugin_bounds 231).
- deadline to DEFECT (slice): 6 lines (event_plane_saturation 3317, packages 7166 and 7648, sessions 1917, 1920 and 4113), applying B2's slice rule to B1. The sessions 3878 extra also became DEFECT (slice).
- DEFECT to measurement-window: 2 lines (probe-hub-resources 232, test-production-package-runtime 590).
- NOT-TIMER and rate-limit to measurement-window: 2 lines (event_plane_saturation 1877 and 1646).

Bare `timeout(` calls (the webrtc runtime form, outside the original grep) are classified one by one in two tables in group C:
- 24 in `src/`: all deadline.
- 39 in the lifecycle test files: 25 DEFECT, 12 deadline, 2 NOT-TIMER.
- Of the 25 DEFECT, 22 repeat extras already listed. The 3 new ones are webrtc_fixtures 2618 (B2) and event_plane_saturation 3385 and 3444 (B1), and they are counted in the extra DEFECT row. Sites added between 23b0feaa and 20b201bd are counted separately in that section: 10 new grep lines and 2 extras.

Production DEFECTs:
- A: 0. Only the deadlines `owner_loop.rs:659` and `:1466` are production.
- C: 30, plus 1 mechanical extra (control_channel.rs:750).
- D: the production parts of `crates/botster-hub-installer` and the Hub client, and `script/publish-npm-packages:144`.

Every other DEFECT is in tests, harnesses, or scripts.

## Cross-cutting mechanisms (one each, shared by many sites)

- **M1 — Process exit event.** The Hub and Core have no Rust process-exit watcher today. The only kqueue EVFILT_PROC code is `script/measure-processes.c`. Plan: one Hub module.
  - Own child: the wait handle runs on a thread and sends on a channel.
  - Other pid: kqueue EVFILT_PROC NOTE_EXIT on macOS. On Linux, pidfd: it turns readable (EPOLLIN) at exit and reports EPOLLHUP at reap, per pidfd_open(2).
  - The caller receives with `recv_timeout(deadline)`, marked `timer: deadline`.
  - Child-process exit is never os-no-event, on either platform.
  - Python and shell users must not use `Popen.wait(timeout)`, `subprocess.run(timeout=)` or `communicate(timeout=)`: these poll with WNOHANG plus sleep. Use a blocking `wait()` on a thread that puts the status on a `queue.Queue`, then `get(timeout=remaining)`. Alternatively use pidfd/kqueue.
  - Users: local_runtime_process, entrypoint_supervisor stop, managed_git_worktrees, update.rs, installer run.rs, test-support, and the lifecycle harness (harness/common/process/cli).
- **M2 — Process-group empty.** This is the final implementation in `src/process_exit.rs`, and it has no os-no-event residual.
  - **macOS.** `proc_listpgrppids(pgid)` returns an atomic snapshot.
    1. Each round takes snapshot S1 and registers NOTE_EXIT for every member of S1.
    2. After all registrations, it takes snapshot S2. New pids in S2 extend the round and are registered too.
    3. The group is empty when a round ends with no new pid and zero live registrations.
    - Zombie members count as exited, because no reap event reaches a non-parent on macOS.
  - **Linux.** `/proc` is not atomic, so "empty" means `killpg(pgid, 0) == ESRCH`.
    - Running members are watched for exit (pidfd POLLIN). Zombie members are watched for reap (pidfd POLLHUP).
    - Every round starts on one of those events.
    - If `/proc` lists no member while `killpg` still succeeds, the code re-reads `/proc` once and then returns an error.
  - The caller's absolute deadline bounds every round on both platforms. That phase budget (TERM grace, KILL grace) stays a `timer: deadline`, and its value is unchanged.
  - In the rows below, "M2" means this design.
- **M3 — Hub daemon readiness pipe (production).** No readiness signal exists. Tests and the local runtime poll Status or connect every 20–50 ms; `cli.rs wait_for_status` spawns a `botster-hub status` process every 20 ms. Plan: the daemon writes one line to an inherited fd after it binds and admits. The launcher reads it with a deadline, and child exit shows as EOF.
  - This touches `src/main.rs` / daemon startup, which are in the other writer's in-flight set. Deferred until that work lands on main.
- **M4 — Test owner-turn driver.** About 62 sites in A and many in C spin `drive_ready_test_turn` with yield or sleep. Plan: one test driver that binds the production owner wakes (`bind_host_owner_wake`, `bind_data_plane_owner_wake`, the plugin result wake) to a test channel and blocks on `control_rx.recv()` under one deadline. `pending.rs:1013`, `sessions.rs:2086`, and `session_spawn.rs:906` already use this pattern.
- **M5 — HostExecutor / Job completion wait for tests.** `poll_completion` Empty => yield, and `job.poll()` until Disposed. Plan: a blocking receive on the executor's existing completion wake, bounded by a deadline. `TestHostGate` gets `wait_started(deadline)` on its existing Condvar.
- **M6 — Shared bounded-wait helper.** `tests/hub_daemon_lifecycle/common.rs:420 wait_for_child_condition_with_budget` is itself a 20 ms poll. Plan: rebuild it as the single allowed test helper. It will be a channel fed by the producers (PTY reader append and EOF, child-wait thread) and received with one deadline.

## Orchestrator rulings (2026-09-25)

1. Status lifecycle counters: do not add a new host-event frame kind. Publish Hub lifecycle status (cleanup_completed, live_connections, live_entity_subscriptions, attach occupancy, and similar) as a subscribable **entity** through the existing entity-subscription mechanism. The owner publishes it on change, with the existing snapshot/delta, capacity, and resync semantics. Tests wait for the delta. The Status request stays a one-shot read. This is a protocol addition, so it comes after protocol 10 and is coordinated with the Foundation writer for docs/client-protocol.md.
2. Data-plane watchdog: APPROVED. `close_work.requeue()` raises the existing owner wake, and the 1 s watchdog is removed. `DATA_PLANE_STOP_BOUND` keeps its value as `// timer: deadline`. Proof: a test shows that requeued close work progresses without the watchdog and fails when the requeue wake is removed (September timer audit item W01).
3. New category `// timer: measurement-window — <what rate is measured>`: allowed ONLY in resource probes and benchmarks where the interval is the metric. It applies to probe-hub-resources:232 and test-production-package-runtime:590. It applies to plugin_bounds.rs:231 only if that test is a measurement, not a correctness assertion. **Outcome:** plugin_bounds.rs:231 asserts an idle-CPU bound when `BOTSTER_ASSERT_IDLE_CPU_BOUND` is set, so it is a correctness assertion and is classified DEFECT (see its row).
4. There is no NOT-TIMER exemption for fixture sleeps. A test child that must stay alive blocks on a read the test ends explicitly: `cat` on stdin, or a read on a FIFO the test closes.
5. Timeout VALUES do not change in this rewrite. Suggestions are recorded under "Follow-ups (not applied)".
6. An os-no-event claim must name the OS fact and hold on both macOS (kqueue EVFILT_PROC) and Linux (pidfd: EPOLLIN at exit, EPOLLHUP at reap; waitid). Child-process exit is never os-no-event. Library APIs that poll internally, such as Python `Popen.wait(timeout)`, are polls.

Line numbers below are at 23b0feaa, except in the section "Sites added between 23b0feaa and 20b201bd", which uses 20b201bd. That section also maps the inventoried lines that moved in files changed since 23b0feaa.

## Design questions (need a ruling before the related sites change) — superseded by the rulings above

1. **Status lifecycle counters have no push source (B1 group D, B2 D4, D).** About 30 test sites poll Status counters (`cleanup_completed`, `live_connections`, `live_entity_subscriptions`, attach occupancy). They cannot become event waits unless the Hub emits an event for these transitions: a host-event frame, or a synchronous release acknowledgement. **This is a product change to the Hub protocol.** Writer proposal: add a host-event subscription frame for disconnect cleanup / subscription close, and wait on it.
2. **Data-plane 1 s watchdog is load-bearing (C, `src/data_plane/driver.rs:1484/1490`).** `close_work.requeue()` raises no wake, so requeued close work only moves when the watchdog fires. Writer proposal: requeue raises the existing data-plane owner wake, and the watchdog goes. `DATA_PLANE_STOP_BOUND` is defined as 2× the watchdog. It needs a new basis at the same value, because values must not change. **This is a production behavior change.**
3. **Idle-rate measurement windows** (`script/probe-hub-resources:232`, `script/test-production-package-runtime:590`, `tests/hub_daemon_lifecycle/plugin_bounds.rs:231`). These tests measure the wake/CPU rate while idle, so the interval is the measured quantity. Writer proposal: accept them as `timer: os-no-event — CPU/wake accounting has no event; the interval is the measurement`. The alternative is a new category.
4. **os-no-event candidates for the reviewer:** `crates/botster-hub-test-support/src/isolated_hub.rs:2187` (SIGSTOP effect on non-children: macOS kqueue has no stop note), `script/run-loaded-daemon-lifecycle-selftest:332,410` (init reaps an orphan zombie; nothing notifies a non-parent of a reap), `script/publish-npm-packages:144` (the npm registry has no push; this is not an OS fact, so it may need a different marker), and the M2 residual. **Revision 2 outcome:**
   - isolated_hub.rs:2187 stays an os-no-event candidate, with facts for both platforms.
   - Selftest 332 and 410 are DEFECT: a pidfd reports the reap with EPOLLHUP.
   - publish-npm 144 is DEFECT and needs an orchestrator decision.
   - M2 has no residual.

## Commit plan (order follows the parallel-writer rule)

Non-overlapping files first:
1. M1 process-exit module + `src/local_runtime_process.rs`, `src/entrypoint_supervisor.rs` stop, `src/managed_git_worktrees.rs`, `src/update.rs`.
2. Entrypoint readiness: re-read the launch result on every watcher event, and deliver child exit on the same channel. This removes the 50 ms fallback at `entrypoint_supervisor.rs:329` and arms `output_finalization_deadline` as a real deadline.
3. `src/client_api.rs:261` HubClientPending::wait and the Core ticket wait: wait on the completion notification.
4. `src/daemon_maintenance.rs`, `src/package_event_router.rs`, and the other non-overlapping `src/` test modules (M4/M5).
5. Lifecycle harness outside the in-flight set: common/harness/process/cli/session_fixtures/operator_console*/harness_isolation/terminal_stream/unix_route_smokes/sessions/event_plane_saturation (M1, M6).
6. Installer crate and scripts.

After the other writer's work reaches main: owner_loop.rs, control/sessions.rs, runtime.rs, transport/*, main.rs (M3), lua_runtime*, the Hub client and test-support crates, and the in-flight lifecycle test files.
Last: the source guard, with only markers left, ablation-proven.

---



# Group A

## Group A timer inventory: src/daemon.rs, src/daemon/**

Source read at worktree HEAD 23b0feaa (read only, no cargo). Test-module boundaries used:
daemon.rs tests from 518; status.rs tests from 606 (line 85 is a `#[cfg(test)]` block inside production fn);
publication_owner.rs from 283; pending.rs from 980; owner_loop.rs: production up to 2295, `#[cfg(test)]` helpers 2296-2344, `mod tests` from 2345;
sessions.rs from 2051; entities/worker.rs from 437; session_spawn.rs test mods from 732/841; client_events.rs from 400.

### Counts (186 grep lines)

| category | count |
|---|---|
| DEFECT | 128 (7 of them mechanical: session_spawn.rs zero-duration receives) |
| deadline | 58 |
| NOT-TIMER | 0 |
| backoff / rate-limit / measurement-window / os-no-event | 0 |

Production sites: only owner_loop.rs:659 and :1466, and both are deadlines. **Every DEFECT is in test code.**
os-no-event candidates: **none**. Every waited condition has an in-process event source: Host/Core/data-plane owner wakes via `bind_owner_wake(ControlSender)`, a Core completion notification, a plugin completion notifier, the TestHostGate Condvar, channels, or join/reply channels. Filesystem polls (H) wait on Host work whose completion receipt is the event, so FSEvents is not needed.
Extras not in the grep: 13 lines (listed at the end). They are DEFECT candidates and are not in the counts.

### DEFECT mechanism groups (for commit planning)

- **A. Owner-turn spin**: `drive_ready_test_turn` or a manual `pop_next`/`dispatch_owner_ready_item`, then yield/sleep until some state changes. That state changes because a Host worker, Core thread or plugin thread makes progress. **Replacement:** one shared test driver. It binds the production owner wakes (`runtime.bind_host_owner_wake`, `bind_data_plane_owner_wake`, `plugin_result_budget.bind_owner_wake`, `entity_capacity_wake.bind`, Core completion) to a test `ControlSender`, then alternates `drive_ready_test_turn` with `control_rx.recv()` under one deadline. The repo already uses this pattern at pending.rs:1013, sessions.rs:2086 and session_spawn.rs:906.
  Lines: daemon.rs 861; status 733, 891, 1016, 1305, 1457; publication_owner 332; owner_loop 2903 (helper, 17 callers), 3072, 3145, 3332, 3453, 3486, 3573, 3629, 3773, 3876, 3902, 4055, 4123, 4230, 4293, 4316, 4350, 4483, 4618, 4658, 4736, 4917, 5212, 6637, 6673, 6739, 6877, 6892, 7000, 7037, 7159, 7222, 7260, 7288, 7453 (helper, 3 callers), 7580, 7618, 7735, 7858, 8477, 8739, 8763, 8977, 9053, 9101, 9175, 9311, 9428, 9483, 9618, 9739; sessions 4471, 4671, 4691, 4857, 4874, 5167, 5200, 5256.
- **B. Host completion poll spin**: `host_executor().poll_completion()` returns `Empty`, then yield. **Replacement:** `HostExecutor::bind_owner_wake` to a test channel; recv the wake, then poll.
  Lines: owner_loop 3230, 3946, 6292 (unbounded, no deadline), 6608, 9364; entities/worker.rs 453 (helper `receive`, 2 callers).
- **C. Host executor counter or disposal spin** (`outstanding()`, `prepared_bytes()`, `host_disposal::Job::poll`, `test_disposed()`, `dispose_terminal_requests` loop). **Replacement:** the executor's capacity/completion notification, delivered through the bound owner wake. For destructor-side proof, use the existing `TestDisposalProbe` channel.
  Lines: status 1119, 1131, 1536; pending 1360; client_events 502, 598; sessions 4983.
- **D. `TestHostGate::has_started()` spin.** **Replacement:** in `TestHostGate::wait()`, notify the gate's existing Condvar after setting `started`, and add `wait_started(deadline)`.
  Lines: status 639; owner_loop 4640, 9384; sessions 5357.
- **E. Core ticket / continuation poll spin** (`tracker.poll`, `continuation.poll` returning `ControlPoll::Pending`, `PluginSpawnPoll::Pending`, `poll_handoff`, `shutdown/remove.poll`). **Replacement:** Core owner completion wake. Recv on the bound receiver, then `take_owner_core_completions` and poll, as sessions.rs:2086 already does.
  Lines: owner_loop 10483; sessions 2524, 2527, 2544 (helper `wait_reservation`, 7 callers), 2659, 2668, 2810, 2827, 3059, 3062 (helper `poll_spawn_until_ready`), 3697, 3825, 3862, 4146, 4148, 5088 (helper `wait_spawn_installed`), 5228, 5238.
- **F. Fixed-window negative assertions.** Each needs a positive progress event first; see the rows for which event.
  Lines: owner_loop 2736, 2757, 5678; sessions 4810, 5044, 5381.
- **G. Other-thread finish or reply spin** (`JoinHandle::is_finished()`, reply `try_recv` from a spawned thread). **Replacement:** the worker thread sends on a done/reply channel. Block on it together with the owner wake: select, or route both into one channel.
  Lines: sessions 3607, 4240 (helper `pump_until_join`), 4537, 4794, 4914.
- **H. Filesystem poll** (`walkdir_exists`). **Replacement:** the Host spawn/rollback completion receipt that creates or removes the directory (owner wake, as in A). Do not poll the filesystem.
  Lines: sessions 4651, 4934.
- **I. Plugin, coordination or Core probe spins.**
  - Plugin `undrained_completions`: use `install_plugin_completion_notifier` into a channel. Line 2601.
  - Completion-drain slice: use the plugin completion notifier or owner wake. Line 6785.
  - Coordination `test_admitted_waiters`: use `coordination_bridge().take_progress_notification` via the owner wake. Line 8131.
  - Core waiter retirement probe: needs a retirement notification. Line 8209.
  - `wait_for_test_plugin_invocation_gate(Duration::ZERO)` in a loop: call the blocking form with the deadline. Line 9576.
  - Session-type spawner queue length: use a spawner queue notification. sessions 4524.
- **J. Sleep-poll of a request until the session is terminal** (`remove_when_terminal`, 25 ms sleep). **Replacement:** wait for Core's process-exit / session-terminal event, then issue RemoveSession once. Line sessions 3316. Also note that on deadline expiry the helper returns the non-removed response as success-shaped; expiry should be an error.

---

### src/daemon.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 861 | `loop { drive_ready_test_turn; reply_rx.try_recv; yield_now }` | DEFECT | test: spins owner turns until the mutation reply arrives; the Host/Core work finishes on another thread | A: owner-wake-blocking test driver, then the reply |

### src/daemon/control/status.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 85 | `release.recv_timeout(5s)` inside `#[cfg(test)]` result gate | deadline | test hook in a production fn: blocks on the fixture's release channel; 5 s is a give-up | |
| 639 | `while gates.any(!has_started) yield` | DEFECT | test: spins until Host workers enter the gate | D: gate Condvar `wait_started` |
| 733 | `while pending.contains_key { publish_completion_wakes; drive; yield }` | DEFECT | test: waits for the Host delivery completion | A |
| 891 | `while !pending_requests.is_empty() { drive; yield }` | DEFECT | test: waits for Core/Host Status completion | A |
| 894, 1020 | `observation.recv_timeout(5s)` | deadline | test: channel recv of an expected observation | |
| 933, 1080 | `gate.recv_timeout(5s)` inside the `submit_core` blocker | deadline | test: Core blocker waits on its release channel | |
| 935, 1082, 1088 | `ready.recv_timeout(5s)` | deadline | test: waits for the blocker-entered signal | |
| 1016 | `while !pending_requests.is_empty() { drive; yield }` | DEFECT | test: waits for the Status capacity response after Core release | A |
| 1119 | `loop { job.poll() Pending => yield }` | DEFECT | test: spins on Host disposal job completion | C: Host completion/owner wake, or the job's disposal receipt channel |
| 1131 | `while executor.prepared_bytes() != 0 yield` | DEFECT | test: waits for Core result destruction to release the reservation | C: capacity notification via the owner wake |
| 1305 | `while !pending_requests.is_empty() { drive; yield }` | DEFECT | test: waits for Status dispatch | A |
| 1457 | `loop { if drive() break; yield }` | DEFECT | test: waits for admitted shutdown to finish | A |
| 1536 | `while executor.outstanding() != N \|\| prepared_bytes() != M yield` | DEFECT | test: waits for retained failure disposal on Host | C |

### src/daemon/publication_owner.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 332 | `await_completion`: `while completion.is_none() { absorb; yield }` | DEFECT | test helper: spins until the Host catalog/publication completion lands (2 call paths via `phase`) | A/B: bound Host owner wake |

### src/daemon/control/pending.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 1013 | `tokio::time::timeout(10s, loop { receiver.recv().await; take_owner_core_completions })` | deadline | test: event-driven (Core wake recv) with a give-up; the model pattern | |
| 1071 | `release_rx.recv_timeout(10s)` in the Core blocker | deadline | test: release channel | |
| 1074 | `entered_rx.recv_timeout(10s)` | deadline | test: blocker-entered signal | |
| 1334 | `entered_rx.recv_timeout(5s)` | deadline | test: Host worker enters the destructor gate | |
| 1360 | `while pending \|\| prepared_bytes != 0 { dispose_terminal_requests; yield }` | DEFECT | test: waits for Host workers to dispose slots after `gate.release()` | C: Host completion wake, or disposed-probe channel count |

### src/daemon/owner_loop.rs

Production and `#[cfg(test)]` helpers (lines 659-2336):

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 659 | `select! { control_rx.recv(), sleep_until(next deadline) }` | deadline | production: the owner blocks on control events. The timer fires only for the earliest armed `DeadlineIndex` entry: reservation expiry, managed-Git op timeout, provider-resync policy deadline. Not a poll. Check the provider-resync "policy deadline" in `subscription/entity_resync.rs` (outside group A); it may be a rate-limit | |
| 1466 | `response_delivery_rx.recv_timeout(DAEMON_CLIENT_WRITE_TIMEOUT)` | deadline | production: waits for the transport's delivery ack (or sender drop) of the Shutdown response; 2 s give-up. Expiry proceeds to stop, which is a give-up, not a receipt | |
| 2305 | `receive_test_control_message`: `timeout(1s, receiver.recv())` | deadline | cfg(test) helper: channel recv with give-up | |
| 2336 | `receive_test_control_reply`: `timeout(1s, receiver)` | deadline | cfg(test) helper: oneshot recv | |

Test module (from 2345):

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 2601 | `while lifecycle.debug_snapshot().undrained_completions != 1 yield` | DEFECT | test: waits for the plugin thread to publish its result | I: plugin completion notifier → channel |
| 2712, 2720, 2750, 2773, 2797, 2814 | `inspect_rx` / `entered_rx` / `engine_entered_rx` / `publication_entered_rx` / `spawner_drops` / `finished_rx` `.recv_timeout(10s)` | deadline | test: expected-event channel recvs | |
| 2716 | `finished_rx.recv_timeout(1s)` inside the panic message | deadline | test: diagnostic only, on the failure path | |
| 2729, 2781, 2824, 2836, 2842 | `bridges_rx` / `coordination_disposed_rx` / `stopped_rx` / `publication_disposed_rx` / `engine_disposed_rx` `.recv_timeout(1s)` | deadline | test: expected-event recvs | |
| 2736 | `assert!(stopped_rx.recv_timeout(50ms).is_err(), "daemon.stop must wait for disposal acknowledgement")` | DEFECT | test, negative window: 50 ms passes whether or not `daemon.stop` has even started. `entered_rx` proves Host disposal began, not that stop is blocked on it. `inspect()` shows stopping=false at this point | F: add a test hook in the serve/stop path that signals "stop entered its disposal-ack wait", recv it, then `stopped_rx.try_recv().is_err()` |
| 2757 | `assert!(stopped_rx.recv_timeout(50ms).is_err())` | DEFECT | test, negative window after engine gate release. `engine_entered_rx` already proves Host is parked in gated engine destruction, so the 50 ms adds nothing | F: once the stop-entered-wait signal from 2736 exists, `try_recv().is_err()` (later checks at 2771/2794 already use `try_recv`) |
| 2903 | `settle_cleanup_test_owner`: loop drive until ready empty && `host.outstanding()==0`, yield | DEFECT | test helper (17 callers: 3038…4078): waits for Host cleanup workers | A |
| 2955 | `client.set_read_timeout(3s)` | deadline | test: bounds socket reads of expected frames | |
| 3072 | `while !connection.test_cleanup_started() { drive; yield }` | DEFECT | test: waits for connection cleanup to reach the held router | A (or a cleanup-started channel hook) |
| 3079 | `resume_rx.recv_timeout(3s)` in `test_on_empty_read` | deadline | test: hook blocks on the resume channel | |
| 3081 | `entered_rx.recv_timeout(3s)` | deadline | test: reader-entered signal | |
| 3145, 3332, 3453 | `while budget.outstanding() != 0 { drive; yield }` | DEFECT | test: waits for connection cleanup to release admission (Host work) | A |
| 3230, 3946 | `loop { poll_completion() Empty => yield }` | DEFECT | test: waits for the cleanup Host worker | B |
| 3486 | `while !host_completion_drain_faulted { drive; yield }` | DEFECT | test: waits for the Host wake to record the scheduling failure | A |
| 3573 | `while !test_recovery(...) { drive; yield }` | DEFECT | test: cleanup fault reaching recovery | A |
| 3629 | `while !has_capacity_waiters() { drive; yield }` | DEFECT | test | A |
| 3773 | `while (recovery \| outstanding) { drive; yield }` | DEFECT | test: capacity release completing cleanup | A |
| 3876 | `while router.test_client_holder_count != 0 { drive; yield }` | DEFECT | test: Host worker detaches the holder | A |
| 3902, 4123 | `while budget.outstanding() != 0 { drive; yield }` | DEFECT | test | A |
| 4055 | `while holder_count != 0 \|\| host.outstanding() != 0 { drive; yield }` | DEFECT | test | A |
| 4230, 4293 | manual `publish_completion_wakes` / `pop_next` / `dispatch` loop, yield | DEFECT | test: waits for the Host package-effect / family completion | A/B |
| 4316 | `while !family_cleanup_waiters.contains_key { drive; yield }` | DEFECT | test | A |
| 4350 | `while pending_requests.contains_key { drive; yield }` | DEFECT | test | A |
| 4483 | `while host_recovery.is_empty() { drive; yield }` | DEFECT | test | A |
| 4618 | loop until `last_core_phase > 0 && ready empty`, yield | DEFECT | test: waits for the Core phase | A/E |
| 4640 | `while !barrier.has_started() yield` | DEFECT | test | D |
| 4658, 9483, 9739 | `while !drive_ready_test_turn() yield` | DEFECT | test: waits for shutdown completion via Host/Core | A |
| 4736, 5212 | `while event_plane_owner_ops_pending() { drive; yield }` | DEFECT | test: queued event-owner ops completed by Host | A |
| 4917 | `loop { drive; shutdown.try_recv; yield }` | DEFECT | test | A |
| 5678 | `timeout(80ms, control_rx.recv())` expected `Err` | DEFECT | test, negative window: 80 ms says nothing about whether the connection has read the Status frame and parked on the admission ack | F: test hook in `handle_connection` that signals "request frame decoded, awaiting admission ack"; recv it, then `control_rx.try_recv().is_err()` |
| 5709 | `client.set_read_timeout(1s)` | deadline | test: bounds reads | |
| 6078, 6103, 6168 | `stopped_rx.recv_timeout(1s)` | deadline | test: expected stop decision | |
| 6292 | `loop { poll_completion() Empty => yield }` with **no deadline** | DEFECT | test: unbounded spin on Host | B |
| 6608 | `loop { poll_completion; pop/drive; reply try_recv; yield }` | DEFECT | test | B + A |
| 6637, 6673, 6739, 7580 | `drive_plugin_entity_ready_item` / `collect_entity_test_host_completions` loops, yield | DEFECT | test: waits for Host selection / disposal | A/B |
| 6785 | `loop { run_completion_drain_slice_for_owner; if count>0 break; yield }` | DEFECT | test: waits for the Core/plugin completion | I: plugin completion notifier / owner wake |
| 6877, 7618 | `while plugin_entities.has_waiter { drive; yield }` | DEFECT | test | A |
| 6892, 7260 | `while host.outstanding() != 0 { drive; yield }` | DEFECT | test | A/C |
| 7000 | provider admission loop (drive, reply try_recv, outstanding), yield | DEFECT | test | A |
| 7037 | `while budget.outstanding \|\| package_entity_work_pending { drive; yield }` | DEFECT | test | A |
| 7159 | `completion_drain_slice` / dispatch loop, yield | DEFECT | test | A/I |
| 7222 | `while cleanup_pending \|\| has_package_entity_fanout { drive; yield }` | DEFECT | test | A |
| 7288 | loop until publication/fanout/cleanup idle, yield | DEFECT | test | A |
| 7453 | `finish_async_plugin_control`: `loop { ...; sleep(5ms) }` | DEFECT | test helper (3 callers: 7470, 8495, 9091): sleep-then-check for the plugin reply | A |
| 7735 | loop until causal/event/publication idle, `sleep(1ms)` | DEFECT | test | A |
| 7858 | same predicate, yield | DEFECT | test | A |
| 8057, 8296, 8315 | `release_rx.recv_timeout(10s)` in Core blockers | deadline | test: release channel | |
| 8097 | `started_rx.recv_timeout(10s)` | deadline | test | |
| 8099 | `entered_rx.recv_timeout(5s)` | deadline | test | |
| 8103, 8106, 8173 | client `set_read_timeout(5s)` / `set_write_timeout(5s)` | deadline | test: bounds socket I/O of expected frames | |
| 8131 | `while bridge.test_admitted_waiters().len() != N yield` (under a 500 ms setup deadline) | DEFECT | test: waits for coordination admission on Core | I: coordination bridge progress notification via the owner wake (`take_progress_notification`) |
| 8149 | `client.set_read_timeout(remaining)` | deadline | test: expected ListPackages frame. **Note:** this is part of a latency assertion (500 ms held window, also checked by `elapsed()` at 8154); the reviewer should decide whether a latency bound is acceptable | |
| 8209 | `while waiters.any(retains_waiter) yield` | DEFECT | test: waits for Core to retire waiters | I: Core waiter-retirement notification / owner wake |
| 8346 | `started_rx.recv_timeout(5s)` | deadline | test | |
| 8350, 8362, 8394 | `first_entered_rx` / `second_entered_rx` / `finished_rx` `.recv_timeout(5s)` | deadline | test | |
| 8366, 8386 | `first_disposed_rx` / `second_disposed_rx` `.recv_timeout(5s)` | deadline | test | |
| 8371, 8391 | `response_*.recv_timeout(5s)` expecting `Disconnected` | deadline | test: sender drop is the event | |
| 8477, 8977 | `loop { drive; reply.try_recv; Empty => sleep(5ms) }` | DEFECT | test | A |
| 8550, 8697 | client `set_read_timeout(5s/2s)` | deadline | test | |
| 8739, 8763 | `while pending_requests ... { drive; sleep(5ms) }` | DEFECT | test | A |
| 9053 | `while !has_capacity_waiters { drive; sleep(5ms) }` | DEFECT | test | A |
| 9101 | `while plugin_controls.has_pending { drive; sleep(5ms) }` | DEFECT | test | A |
| 9175 | `loop { drive; receiver.try_recv; sleep(1ms) }` | DEFECT | test: entity snapshot frame | A: select the owner wake and the frame receiver |
| 9311 | manual dispatch loop until the entity row keeps its payload, yield | DEFECT | test | A |
| 9364 | `take_owner_core_completions` + `poll_completion` loop, yield | DEFECT | test | B/E |
| 9384 | `while !gate.has_started() yield` | DEFECT | test | D |
| 9428 | loop until shutdown consumes preparation, yield | DEFECT | test | A |
| 9576 | `while !wait_for_test_plugin_invocation_gate(ZERO) \|\| outstanding != 0 { drive; yield }` | DEFECT | test: zero-timeout poll of a gate that has a blocking form | I: `wait_for_test_plugin_invocation_gate(deadline)`, then A for outstanding |
| 9618 | `while outstanding != baseline { drive; sleep(5ms) }` | DEFECT | test | A |
| 10483 | `loop { continuation.poll; Pending => sleep(5ms) }` | DEFECT | test: attach waits on a Core turn | E |

### src/daemon/control/sessions.rs (all test code, from 2051)

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 2086 | `timeout(10s, loop { receiver.recv().await; take_owner_core_completions })` | deadline | test: event-driven Core wake recv | |
| 2524, 2527 | `loop { drive; continuation.poll Pending/Again/_ => yield }` | DEFECT | test | E |
| 2544 | `wait_reservation`: `tracker.poll Pending => yield` | DEFECT | test helper (7 callers) | E |
| 2659, 2668 | retry-retained loop, `Pending`/`_ => yield` | DEFECT | test | E |
| 2810, 2827 | `drive_accepting_queue` loop, `Pending`/`_ => yield` | DEFECT | test | E |
| 3059, 3062 | `poll_spawn_until_ready`: `Pending\|Again`/`_ => yield` | DEFECT | test helper (called from 3084, 3289 → `request_until_ready`) | E |
| 3316 | `remove_when_terminal`: request, then `sleep(25ms)` until `SessionRemoved` | DEFECT | test helper (4 callers: 3329, 3839, 3891, 4352): sleep-poll waiting for the worker to exit. On expiry it returns the unremoved response instead of failing | J: Core process-exit / session-terminal event, then one RemoveSession; make expiry an error |
| 3607 | `while !worker.is_finished() { drain owner_rx try_recv; pump; yield }` | DEFECT | test: spins while it already holds the owner wake receiver `owner_rx` | G: block on `owner_rx.recv` + a done channel from the scoped worker |
| 3697 | `release.poll Pending => yield` | DEFECT | test | E |
| 3825 | `PluginSpawnPoll::Pending => yield` | DEFECT | test | E |
| 3862 | `loop { pump; if poll_handoff break; yield }` | DEFECT | test | E |
| 4146, 4148 | `test_poll_spawn` `Pending`/`_ => yield` | DEFECT | test | E |
| 4240 | `pump_until_join`: `pump; if handle.is_finished break; yield` | DEFECT | test helper | G |
| 4471 | loop until `installed_len()==3 && pending empty`, yield | DEFECT | test | A/E |
| 4524 | `while spawner.test_managed_queue_len() < 2 yield` | DEFECT | test: waits for two spawn threads to enqueue | I: spawner queue notification (owner wake) |
| 4537 | `while !(a.is_finished() && b.is_finished()) { pump; yield }` | DEFECT | test | G |
| 4651 | loop until `walkdir_exists(managed)`, yield | DEFECT | test: FS poll | H |
| 4671 | loop until `!walkdir_exists(managed)` (rollback), yield | DEFECT | test: FS poll + counters | H |
| 4691 | loop until `hub_worktree_ids` is empty, yield | DEFECT | test: waits for the Host rollback to commit record removal | A |
| 4794, 4914 | `loop { pump; reply.try_recv; Empty => deadline check }; yield` | DEFECT | test: reply from a spawn thread | G |
| 4810 | `while now < hold(2s) { pump; assert worktree exists; yield }` | DEFECT | test, negative window: 2 s of "still exists" does not prove Released was processed | F: wait for the positive receipt that the post-reuse Released path finished (release begin count / reservation retired / cleanup-transfer counter), then assert once |
| 4857 | loop until `test_inherited_managed_cleanup_transfers()==1`, yield | DEFECT | test | A |
| 4874 | loop until worktree gone && counters zero, yield | DEFECT | test | A/H |
| 4934 | loop until `!walkdir_exists && ids empty`, yield | DEFECT | test | H |
| 4983 | `while !pending_requests.is_empty() { dispose_terminal_requests; yield }` | DEFECT | test | C |
| 5044 | `while now < hold(2s) { pump; yield }`, then assert the live worktree survived | DEFECT | test: "let it settle" negative window | F: wait for the stale rollback's Host outcome (confirmed/refused counter or completion receipt), then assert |
| 5088 | `wait_spawn_installed`: `tracker.poll Pending => yield` | DEFECT | test helper (1 caller) | E |
| 5167 | loop until cleanup count==1 && release idle, yield | DEFECT | test | A |
| 5200 | loop until `release_session_reservation_begins >= 2`, yield | DEFECT | test | A |
| 5228, 5238 | `shutdown.poll` / `remove.poll` Pending, yield | DEFECT | test | E |
| 5256 | loop until `confirmed_worktree_rollback_count >= 1`, yield | DEFECT | test | A |
| 5357 | `loop { pump; if gate.has_started break; yield }` | DEFECT | test | D (+A for pump) |
| 5381 | `while now < wait_reuse(1s) { pump; assert !handle.is_finished; yield }` | DEFECT | test, negative window | F: signal when reuse reaches its deferral point behind the in-flight rollback (test counter/hook), then assert `!is_finished` once |

### src/daemon/control/entities/worker.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 453 | `receive`: `poll_completion Empty => yield` | DEFECT | test helper (2 callers) | B |

### src/daemon/control/session_spawn.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 769, 804, 1035, 1133, 1180 | `receiver.recv_timeout(Duration::ZERO).unwrap()` | DEFECT (mechanical; was NOT-TIMER) | test: a zero-duration non-blocking take after a synchronous same-thread delivery (`deliver_admitted_failure` / `operation.poll`). Nothing waits, but it is a timer-API call and not a listed false positive | `receiver.try_recv().unwrap()` |
| 1024, 1166 | `receiver.recv_timeout(ZERO)` expecting `Timeout` | DEFECT (mechanical; was NOT-TIMER) | test: an absence check right after a synchronous poll on the same thread. Delivery would have been synchronous, so it is deterministic, not a window, but it still uses a timer API | `assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)))` |
| 906 | `timeout(10s, loop { take_owner_core_completions; receiver.recv().await })` | deadline | test: event-driven Core wake | |

### src/daemon/client_events.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 487, 582 | `*_drop_rx.recv_timeout(5s)` | deadline | test: disposal probe channel | |
| 502 | `while !terminal.test_disposed() yield` | DEFECT | test: waits for the Host terminal job | C |
| 598 | `while executor.outstanding() != 0 \|\| prepared_bytes() != 0 yield` | DEFECT | test | C |

### Extras (not in grep): spins with no timer call

| file:line | site | category | reason | replacement |
|---|---|---|---|---|
| sessions.rs:2726 | `while !(done_a && done_b) { drive; poll a; poll b }` | DEFECT (extra, not in grep) | test: tight spin with no yield/sleep, waiting for Core. The grep missed it because there is no timer call | E |
| owner_loop.rs:3967 | `while !test_recovery(...) { drive }` | DEFECT (extra, not in grep) | test: busy spin, no yield. If the work is purely owner-local it should instead be "drive until `owner_ready` is empty, then assert" | A |
| owner_loop.rs:4389 | `while causal_operation_count > 0 \|\| resync running { drive }` | DEFECT (extra, not in grep) | test: busy spin | A |
| owner_loop.rs:5172 | `while causal_owner_ops_pending() { drive }` | DEFECT (extra, not in grep) | test: busy spin. The test name says "without control traffic", so this may be owner-local multi-turn draining. If so, replace with a deterministic turn loop that asserts progress per turn | A, or deterministic |
| publication_owner.rs:344 | `while waiting_for_progress { apply_causal_owner_ops; absorb; drive }` | DEFECT (extra, not in grep) | test: unbounded spin, no deadline | A |
| publication_owner.rs:550, 565; owner_loop.rs:5001, 6896; entities/worker.rs:777 | `while causal_operation_count() > 0 { apply_causal_owner_ops() }` | DEFECT (extra, not in grep; verify) | test: `apply_causal_owner_ops` restores the head on `CausalWaitResult::Waiting` (runtime.rs:1127), so this spins unbounded while a scope waits on another actor. It is not a spin if the fixture guarantees readiness | Deterministic: apply until empty, and assert it never returns Waiting; otherwise use the causal scope ready wake |
| publication_owner.rs:562 | `while causal_family_release_ready() { retry_family_resync_release() }` | DEFECT (extra, not in grep; verify) | test: same concern | same |

Not a defect and out of scope: `owner_loop.rs:1332` terminal drain parks with `thread::park()` on a bound terminal owner (unparked by the notifications), so it is event-driven. `managed_git.rs:1309 deadline_elapsed` is a give-up deadline armed in `DeadlineIndex` (production).


# Group B1

## Timer inventory, group B1

Files: `tests/hub_daemon_lifecycle/{sessions,packages,event_plane_saturation,shutdown}.rs` (171 grep hits, plus 2 extra loops the grep missed).

### Counts

| category | sessions | packages | event_plane_saturation | shutdown | total |
|---|---|---|---|---|---|
| DEFECT | 51 (+3 extra) | 26 | 22 (+2 extra: 3385, 3444) | 15 | **114 (+5 extra = 119)** |
| deadline | 17 | 13 | 3 | 11 | **44** |
| rate-limit | 1 | 0 | 3 | 0 | **4** |
| measurement-window | 0 | 0 | 2 | 0 | **2** |
| backoff | 0 | 0 | 0 | 0 | 0 |
| os-no-event | 0 | 0 | 0 | 0 | **0** |
| NOT-TIMER | 0 | 0 | 7 | 0 | **7** (all `set_read_timeout(None)`) |
| hits | 69 | 39 | 37 | 26 | 171 |

Revision 2 moved 19 former NOT-TIMER lines to DEFECT: 18 fixture sleeps and the packages.rs:7324 1 ms read, which is mechanical. event_plane_saturation 1877 (was NOT-TIMER) and 1646 (was rate-limit) moved to measurement-window. B2's slice rule is now applied here too: event_plane_saturation 3317, packages 7166 and 7648, and sessions 1917, 1920 and 4113 moved from deadline to DEFECT (slice). The sessions 3878 extra also became DEFECT (slice).

**os-no-event: none claimed.** Every process wait has an OS event source:
- Own child: `Child::wait` on a thread, sending to a channel.
- Any other pid: kqueue `EVFILT_PROC`/`NOTE_EXIT` on macOS (works for any pid the user can signal), or `pidfd_open` + poll on Linux 5.3+.
- A process appearing, or a file being written: the fixture can signal readiness through a pipe or FIFO. As a fallback, kqueue `EVFILT_VNODE` watches the directory.

**The one real gap is Status lifecycle counters.** The client API has no push source for `lifecycle_counters` (`live_entity_subscriptions`, `cleanup_completed`, `live_connections`, `rejected_connections`, `package_entity_resync_*`, `event_shed_by_reason`). The client has only three push streams:
- entity subscriptions (`session`, `session_type`, package families)
- terminal route events
- package-event subscriptions

So group D below needs a new Hub event before its polls can be removed. That is a product change, not a test-only change.

### DEFECT mechanism groups (for commit planning)

- **A. ReadScreen / ReadModeFlags poll → terminal route output event.** Attach, then do a blocking `poll_route_events`/route read with one deadline. For mode flags, wait for a marker printed after the mode sequence, then do one ReadModeFlags.
  - sessions 91 (`wait_for_read_screen_contains` helper), 463, 1045, 1253, 1339, 1403, 1499, 2425, 3219, 3825, 4606, 4634, 4705, 4803
  - shutdown 2437, 2512, 2534
  - shutdown 2110 is in-process: `HubClientApi` ReadScreen plus `observe_lifecycle_slice`. Use the runtime's output/lifecycle wake or a route subscription.
- **B. Short route or connection read timeouts multiplexed with other work, plus sleep → dedicated reader thread per stream feeding a channel, with `recv_timeout(deadline)`.**
  - sessions 3960 (80 ms entity reads interleaved with `poll_adapter_events`), 4760
  - event_plane 3745, 3884, 4423, 4905
- **C. ListSessions lifecycle poll → session entity subscription frames (Patch `exited` / Upsert / Remove) with a deadline.**
  - sessions 1669, 1966, 2006 (RemoveSession retry), 2026 (the Remove frame is already read right after, at 2033, so reorder), 2900, 3373
  - event_plane 3583 (count `running` Upserts), 4688, 5484, 5562 (the list part; the counter part is D)
  - Caveat: 2900 and 3373 test the zero-subscriber path on purpose, so subscribing would change the scenario. Use kqueue on the session child pid from the registry, then one ListSessions. That works only if the Hub guarantees the exit is reflected once the child has exited; otherwise it needs a D-style event.
- **D. Status lifecycle-counter poll → no event exists today; needs a new Hub push.** Options: a lifecycle-counter entity family, or synchronous release acks such as an Unsubscribe/close response that returns only after owner cleanup.
  - sessions 2058 (subscribe-retry until the old id is released), 2591, 2612, 2651, 2740, 2757, 2769, 2841, 4872 (sleep standing in for dropped-attach cleanup), 5255
  - packages 409, 6740, 7318, 7494
  - event_plane 5373 (sleep before shed counters; an EventGap on a subscribed connection may serve as the event), 5562
- **E. Fixed observation window before a negative or rate assertion → a positive barrier event, then assert on exact counter deltas or on stream order.**
  - sessions 56, 2242, 2372, 2503 (1.1–1.2 s idle windows asserting counter deltas ≤4 / ==0), 4987 (5 s "echo must not arrive"), extra 780–788, extra 3896
  - packages 7074 (400 ms), 7352 (3 s), 7361 (400 ms), 7517 (3 s)
  - shutdown 44 (100 ms "waiter must not acquire"; use `Mutex::try_lock` → WouldBlock while the guard is held), 987 (500 ms "shutdown must not return"; have shutdown emit a "waiting for daemon exit" line on its piped stdout/stderr, read it, then assert `try_wait` is None)
  - Note: sessions 56/2242/2503 also measure a production "idle backstop" wake rate (`<= 4` per window). If that backstop timer is removed in production, these become exact `== 0` checks after a barrier.
- **F. Settle sleep before the next step → the specific event.**
  - sessions 2273 (wait for the first `x` route output)
  - sessions 4109 (wait for the output to be pending/received on the route before dropping the terminal)
  - sessions 5055 (wait for the first bytes on the attach child's stdout pipe)
  - event_plane 489 (watchdog sends its first sample on a channel)
  - event_plane 5512 (the journal-hold fault hook must ack the hold. Whatever reads `hold_path` is likely a file poll in the fixture or daemon; out of this group, flag for the owner.)
- **G. Timing inside child fixture commands → gate on stdin or a FIFO the test writes.**
  - sessions 1466 (python `time.sleep(2)` waiting for OSC replies; read the replies from stdin instead)
  - sessions 3768 (`while [ ! -e release ]; do sleep 0.01`; block on reading a FIFO)
  - sessions 3933 (`sleep 0.15` ×2; `read` a line sent after Attach)
  - sessions 4085 (`sleep 0.3` / `sleep 0.6`; same)
- **H. File existence/content poll → fixture writes to a FIFO (or to the PTY, observed as a route event) and the test does a blocking read with a deadline.**
  - sessions 3796
  - packages 1280, 1379, 2766, 3903, 4739, 4855
  - shutdown 1164 (metadata file, alongside `up.try_wait`; kqueue `EVFILT_VNODE` on the data dir, or an `up` stdout readiness line)
  - shutdown 1061, 1114 (socket removal; wait for the daemon's exit via kqueue/child wait, then check once)
- **I. Process presence/absence poll.**
  - Foreign pid → kqueue `NOTE_EXIT` / pidfd: sessions 1638, 3855 (the worker-socket half follows from the worker pid exit or the entity exit frame).
  - Own child → `Child::wait` thread + channel: shutdown 3226.
  - `ps` census absence → one census for pids, then kqueue per pid: shutdown 1250.
  - Appearance → fixture readiness pipe: sessions 3091. For worker appearance, use the entity Upsert `running`, then one census: shutdown 3304.
- **J. Plugin/entrypoint state poll → package-event subscription.**
  - packages 2596 (`worktree_recorder.seen` tool poll; subscribe to the worktree event subjects)
  - packages 3992 (PackageEntrypointStatus until not running; needs an entrypoint-state event if the Hub does not emit one, so partly D)
- **K. Nonblocking accept + 10 ms sleep checking a stop flag → blocking accept, woken by a self-connect or listener shutdown.** shutdown 1681.
- **L. Best-effort drain with timeout.** packages 7705: read until the Upsert for that `seq` arrives, with a deadline.

### sessions.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 56 | `wait_for_idle_lifecycle_window`: sleep 1_200 then Status | DEFECT | settle sleep so spawn catch-up "cannot spill" into the idle sample | E: barrier (receive last expected entity frame / explicit no-op change), then exact counter deltas |
| 91 | `wait_for_read_screen_contains`: ReadScreen loop, sleep 25 | DEFECT | poll helper on screen text; route output exists | A: attached route output event with deadline |
| 463 | ReadModeFlags loop until mouse_mode==9, sleep 20 | DEFECT | poll for mode state | A: route output marker printed after mode sequence, then one ReadModeFlags |
| 1045, 1253, 1339, 1403, 1499 | ReadScreen loop until marker, sleep 20/30/30/30/50 | DEFECT | screen poll | A |
| 1466 | python child `time.sleep(2)` after writing OSC queries | DEFECT | fixture sleeps standing in for "replies arrived" | G: child reads the OSC replies from stdin, then prints `done` |
| 1638 | pid liveness loop over worker+shell pids, sleep 40 | DEFECT | process-absence poll | I: kqueue EVFILT_PROC NOTE_EXIT per pid (pidfd on Linux) |
| 1669 | ListSessions loop until exited/absent, sleep 50 | DEFECT | lifecycle poll | C: entity Patch exited / Remove |
| 1767, 1783 | entity sub `set_read_timeout(5s)` then `next_frame()` | deadline | bounded give-up on expected Snapshot | – |
| 1917, 1920 | 5 s read timeouts inside 15 s exit_deadline loop over two subs | DEFECT (slice; was deadline) | 5 s chunks alternate between two subscriptions and only re-check the 15 s deadline (B2 slice rule) | a reader thread per subscription feeding one channel, `recv_timeout(exit_deadline - now)` until both exited Patches; 15 s unchanged |
| 1966 | ListSessions loop (also re-sends resize), sleep 200 | DEFECT | poll after exit Patch already received | C: exit Patch already received, so do one ListSessions (or a Hub ordering guarantee) |
| 2006 | RemoveSession retry loop, sleep 200 | DEFECT | retry until state allows | C: after exit Patch, one RemoveSession |
| 2026 | ListSessions absence loop, sleep 50 | DEFECT | poll after SessionRemoved | C: read Remove frame (2033) first, then one ListSessions |
| 2030 | `set_read_timeout(5s)` for Remove frame | deadline | expiry panics | – |
| 2058 | subscribe retry until old id released, sleep 20 | DEFECT | retry loop waiting for server-side EOF cleanup | D: synchronous release ack / new event |
| 2070, 2123, 2226, 2319, 2466, 2628, 2912, 3269, 3378, 3925, 4077 | entity sub `set_read_timeout(2–5s)` before expected frames | deadline | bounded give-up on expected frame | – |
| 2242 | sleep 1_100 idle window then Status deltas | DEFECT | fixed window before negative/rate assertion | E |
| 2269 | fixture `printf x; sleep 0.05` ×80 | rate-limit | deliberate producer pacing so output spans the flood (the progress assertion it supports is still time-shaped) | – |
| 2273 | sleep 150 after Spawn before ReadScreen | DEFECT | settle | F: first route output `x` |
| 2372 | sleep 1_100 during flood before Status | DEFECT | fixed window before assertion | E |
| 2425 | ReadScreen loop until 80 `x`, sleep 50 | DEFECT | screen poll | A (or entity exit Patch, since producer exits after 80, then one ReadScreen) |
| 2503 | sleep 1_100 many-session idle window | DEFECT | fixed window before rate assertion | E |
| 2591, 2612, 2651 | Status loop on cleanup_completed / live_entity_subscriptions, sleep 20/20/10 | DEFECT | counter poll | D |
| 2740, 2757, 2769 | Status loop on rejected_connections / live_connections (2757 also retries request errors), sleep 20 | DEFECT | counter poll / retry | D |
| 2789, 2810 | raw socket `set_read_timeout(4s)` expecting EOF | deadline | event is server close; expiry = failure | – |
| 2841 | Status loop on cleanup_by_reason/live_connections, sleep 20 | DEFECT | counter poll | D |
| 2900 | ListSessions loop until exited (zero subscribers), sleep 50 | DEFECT | lifecycle poll | C caveat: kqueue on session pid, then one ListSessions, or new event |
| 3091 | ps marker census loop, sleep 20 | DEFECT | process-appearance poll | I: fixture readiness pipe |
| 3219 | ReadScreen loop, sleep 25 | DEFECT | screen poll | A |
| 3373 | sleep 800 after Spawn `sleep 0.05` before late subscribe | DEFECT | fixed delay; following loop already accepts Snapshot/Upsert/Patch | C caveat: drop sleep, or kqueue on session pid |
| 3564 | `set_read_timeout(60s)` first snapshot | deadline | liveness bound | – |
| 3768 | fixture `while [ ! -e release ]; do sleep 0.01` | DEFECT | shell file poll | G: FIFO read |
| 3796 | `for 0..500 marker_path.exists()`, sleep 10 | DEFECT | file poll | H: marker on PTY route output or FIFO |
| 3825 | loop poll_route_events(25ms)+ReadScreen, sleep 10 | DEFECT | route/screen poll | A |
| 3855 | loop pid exists && worker socket connect, sleep 10 | DEFECT | process/socket absence poll | I: kqueue NOTE_EXIT on pty child + worker pid |
| 3933 | fixture `sleep 0.15; printf; sleep 0.15; printf; exit 7` | DEFECT | fixture timing standing in for "attach happened" | G: `read` gate from terminal input after Attach |
| 3960 | entity `set_read_timeout(80ms)` interleaved with `poll_adapter_events` | DEFECT | short timeout used to re-check another stream | B: reader threads, channel |
| 4085 | fixture `sleep 0.3; printf; sleep 0.6; exit 7` | DEFECT | fixture timing vs attach/disconnect | G |
| 4109 | sleep 500 after Attach before drop(terminal) | DEFECT | settle for pending output | F / G: fixture gated, observe output then drop |
| 4113 | `set_read_timeout(2s)` in 8 s loop for exit Patch | DEFECT (slice; was deadline) | 2 s chunks retried until the 8 s deadline (B2 slice rule) | reader thread feeding a channel, `recv_timeout(deadline - now)` until the exit Patch; 8 s unchanged |
| 4606, 4634, 4705 | Status+ReadScreen loop, sleep 25 | DEFECT | screen poll | A |
| 4760 | `for 0..20` poll_route_events(25)+sleep 25 until attached | DEFECT | route poll | B: blocking route read with deadline |
| 4803 | poll_route_events(30)+ReadScreen loop, sleep 30 | DEFECT | route/screen poll | A |
| 4872 | sleep 150 after dropped-connection Attach | DEFECT | stands in for server EOF cleanup before occupancy assert | D |
| 4987 | 5 s loop asserting echo never arrives, sleep 30 | DEFECT | negative assertion over fixed time | E: positive barrier (e.g. Detach/another marker) then assert absence |
| 5055 | sleep 200 after spawning `sessions attach` child | DEFECT | settle | F: read first bytes from attach child stdout |
| 5255 | Status loop on cleanup_completed, sleep 20 | DEFECT | counter poll | D |
| 780–788 (extra, not in grep) | `for 0..5` poll_route_events(20) asserting no second frame | DEFECT | negative assertion over fixed 100 ms | E: rely on order of frames after release |
| 3896 (extra, not in grep) | poll_route_events(100ms) asserting no duplicate exit | DEFECT | negative assertion over fixed time | E: barrier (Detach → Detached) then assert none before it |
| 3878 (extra, not in grep) | chunked 25 ms route reads until PROCESS_EXIT | DEFECT (slice; revision 2, was "reviewed, not a defect") | 25 ms chunks only re-check the deadline (B2 slice rule) | reader thread feeding a channel, or `poll_route_events(deadline - now)`; deadline value unchanged |

### packages.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 363, 1914, 2010, 2114, 2189, 2782, 2793, 3012, 3320, 3437, 3638, 6715, 6759 | entity sub/connection `set_read_timeout(2–5s)` before expected frames | deadline | bounded give-up | – |
| 409 | Status loop live_entity_subscriptions==0, sleep 20 | DEFECT | counter poll | D |
| 1041, 4018, 4090, 4151 | supervised fixture `while true; do sleep 1; done` | DEFECT (was NOT-TIMER) | fixture sleep that keeps a supervised entrypoint alive | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 1358 | expected `...; sleep 30` in asserted shell contract args | DEFECT (was NOT-TIMER) | the assertion's string is the arguments of the session-type fixture that really runs (`... > shell-output.txt; sleep 30`); it changes with that fixture | rewrite the fixture command to end in a blocking read the test ends (`cat` on stdin or a FIFO the test closes), and update this expected string with it |
| 3873 | fixture writes env then `while true; do sleep 1` | DEFECT (was NOT-TIMER) | fixture keep-alive sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 1280 | read context-output.json loop, sleep 30 | DEFECT | file poll | H: FIFO / PTY output |
| 1379 | exists() loop for two output files, sleep 30 | DEFECT | file poll | H |
| 2596 | `worktree_recorder.seen` tool call loop, sleep 50 | DEFECT | plugin state poll | J: package-event subscription on worktree subjects |
| 2766 | read repo-template-output loop, sleep 30 | DEFECT | file poll | H |
| 3903 | `for 0..100` read entrypoint env file, sleep 20 | DEFECT | file poll | H |
| 3992 | PackageEntrypointStatus loop until != running, sleep 20 | DEFECT | state poll | J/D: entrypoint state event (new if absent) |
| 4739 | output/context/fifo existence loop, sleep 25 | DEFECT | file poll | H |
| 4855 | output/context loop, sleep 25 | DEFECT | file poll | H |
| 6740 | Status loop live_entity_subscriptions==0, sleep 20 | DEFECT | counter poll | D |
| 7074 | sub_a `set_read_timeout(400ms)`: A must not get behind snapshot | DEFECT | negative assertion over fixed window | E: publish follow-up mutation, read A to it, assert no seq-0 snapshot |
| 7166 | `remaining.min(200ms)` chunked read in 10 s loop | DEFECT (slice; was deadline) | the 200 ms chunk only re-checks the deadline (B2 slice rule) | B mechanism: a reader thread per subscription feeding a channel, `recv_timeout(deadline - now)`; or one read with `remaining`; the 10 s value is unchanged |
| 7318 | Status loop until resync_degraded, sleep 50 | DEFECT | counter poll | D (or a degraded/Error frame on sub_b, if the Hub emits one) |
| 7324 | 1 ms timeout census read on failure path | DEFECT (mechanical; was NOT-TIMER) | a diagnostic drain before the panic that uses a 1 ms timer per read (up to 16) instead of a non-blocking read | `set_nonblocking(true)` and read until `WouldBlock`; no timer |
| 7352 | 3 s loop asserting counters unchanged, sleep 50 | DEFECT | negative assertion over fixed time | E |
| 7361 | 400 ms loop A must not roll back | DEFECT | negative window | E |
| 7494 | Status loop until degraded, sleep 50 | DEFECT | counter poll | D |
| 7517 | 3 s loop asserting attempts unchanged, sleep 100 | DEFECT | negative window | E |
| 7648 | 200 ms chunked reads until 3 Upserts, 10 s deadline | DEFECT (slice; was deadline) | the 200 ms chunks only re-check the deadline (B2 slice rule) | reader thread feeding a channel, `recv_timeout(deadline - now)` per frame until 3 Upserts; 10 s unchanged |
| 7705 | per-mutation `set_read_timeout(200ms)`; `let _ = next_frame()` | DEFECT | best-effort drain, a 200 ms delay per missing frame | L: read until that seq's Upsert with deadline, or a drain thread |

Helper calls (not hits; mechanism belongs to the helper files): `wait_for_process_exit` 4049, 4115, 4139, 4179 (pid-exit poll, so I: kqueue); `wait_for_app_local_url` 1162, 5605, 5626; `wait_for_managed_git_session_exit` 4953; `wait_for_entity_frame` (event read, 200 ms chunks, deadline).

### event_plane_saturation.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 101 | noisy python producer `time.sleep(0.1)` | rate-limit | deliberate 10 lines/s output rate | – |
| 489 | unit test sleep 15 before stopping watchdog, asserts samples≥1 | DEFECT | sleep standing in for "first sample taken" | F: watchdog signals first sample on channel |
| 1646 | watchdog `thread::sleep(period=1ms)` measuring oversleep | measurement-window (was rate-limit) | the sampling period of the scheduler-lag instrument, whose metric is the oversleep of this interval. Its output feeds `classify_host_validity` (host contention), not a Hub correctness assertion. Reviewer: confirm that a host-validity probe inside the saturation suite counts as a resource probe | – |
| 1877 | `spin_loop()` in `probe_scheduler_lag` | measurement-window (was NOT-TIMER) | a fixed-duration busy measurement: the requested busy interval (`MAX_OWNER_TURN_MS`) is the metric, and the observed lag feeds host-validity classification. It does not wait for another actor. Same reviewer check as 1646 | – |
| 3317 | `set_read_timeout(250ms)` in 5 s marker/gap loop | DEFECT (slice; was deadline) | the 250 ms chunk only re-checks the deadline (B2 slice rule) | reader thread feeding a channel, `recv_timeout(deadline - now)` until the marker or gap; 5 s unchanged |
| 3323, 3327, 3333, 3888, 4752, 5427, 5624 | `set_read_timeout(None)` | NOT-TIMER | clears timeout | – |
| 3523 | burst driver `sleep(interval - elapsed)` | rate-limit | deliberate bursts/sec cap | – |
| 3537, 5025, 5040, 5063, 5072, 5095, 5134, 5458, 5517, 5528 | session command `exec sleep N` | DEFECT (was NOT-TIMER) | fixture sleep: the session's lifetime is a duration, not an event the test controls | blocking read the test ends: `exec cat` on stdin, or a FIFO the test closes |
| 3551 | `sleep(EVENT_PLANE_WAVE_GAP=200ms)` between spawn waves | rate-limit | deliberate spawn pacing (Spawn is synchronous). If it exists to let a wave reach running, it becomes C | – |
| 3583 | ListSessions loop until N running, sleep 50 | DEFECT | lifecycle poll | C: count `running` Upserts on session entity sub |
| 3745 | `set_read_timeout(20ms)` on unix events inside measurement loop | DEFECT | short timeout multiplexed with other work | B: event reader thread → channel |
| 3884 | sleep 20 per measurement-loop iteration (collects noisy output) | DEFECT | output poll cadence (the window `end_at` itself is a deliberate measurement duration) | B: blocking route reader threads, `recv_timeout` to `end_at` |
| 4423 | Status+collect_noisy_attach loop until first_post_window_seq, sleep 20 | DEFECT | route poll | B |
| 4688 | ListSessions loop re-issuing ShutdownSession, sleep 50 | DEFECT | lifecycle poll/retry | C: entity exit Patches (or route ProcessExit) |
| 4729 | `set_read_timeout(50ms)`, ≤8 reads, first Err breaks | deadline | expects event; expiry is failure (50 ms budget is tight) | – |
| 4905 | collect_attach_events loop until ProcessExit, sleep 30 | DEFECT | route poll | B |
| 5373 | sleep 400 before shed counters snapshot | DEFECT | fixed delay before assertion | D (EventGap on a subscribed connection may serve) |
| 5415, 5611 | `set_read_timeout(3s)`, ≤8 reads for EventGap/PackageEvent | deadline | expected event; expiry falls through to assertion | – |
| 5484 | ListSessions loop until wake-probe exited, sleep 50 | DEFECT | lifecycle poll | C |
| 5512 | sleep 50 after writing hold file | DEFECT | settle for fault-hook pickup | F: hook ack (and the hold reader is itself likely a file poll, outside this group) |
| 5562 | Status+ListSessions loop, sleep 50 | DEFECT | counter + lifecycle poll | C + D |

Reviewed, not defects (not in grep): 813 (`try_recv` drain after `drop(tx)`), 3444 (250 ms async timeout chunks to a 5 s deadline; **revision 2: DEFECT (slice)**, see "Bare webrtc timeouts in the lifecycle test files" in group C), 3691 (churn load generator, no wait), 3730 (`try_recv` drain inside the 3884 loop, which is covered there).

### shutdown.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 44 | `recv_timeout(100ms).is_err()`: waiter must not acquire | DEFECT | negative assertion over fixed time | E: `try_lock` → WouldBlock while guard held |
| 49 | `recv_timeout(1s)` expect acquisition | deadline | event channel | – |
| 326, 386, 420, 433 | `recv_timeout(2–5s)` on fixture/update channels | deadline | event channels; expiry is failure | – |
| 389, 397 | `recv_timeout(3s/5s)` inside failure-diagnostic branch | deadline | error path only | – |
| 449 | fixture `...; exec sleep 60` | DEFECT (was NOT-TIMER) | fixture keep-alive sleep (a fixture that never becomes ready) | blocking read the test ends: `exec cat` on stdin or a FIFO the test closes |
| 987 | 500 ms window loop `try_wait` on shutdown cmd, sleep 20 | DEFECT | negative assertion over fixed window | E: shutdown progress line on piped output, then `try_wait` None |
| 1061, 1114 | `for 0..100` socket exists, sleep 20 | DEFECT | file-absence poll | H/I: wait for daemon exit (kqueue/child wait), then check once |
| 1164 | `for 0..500` metadata exists + `up.try_wait`, sleep 10 | DEFECT | file poll | H: kqueue EVFILT_VNODE on data dir or `up` stdout line; child exit via wait thread |
| 1250 | `ps` census loop for attributable rows, sleep 20 | DEFECT | process-absence poll | I: one census → kqueue NOTE_EXIT per pid |
| 1681 | nonblocking accept + sleep 10 checking stop flag | DEFECT | poll loop | K: blocking accept, wake by self-connect/shutdown |
| 1687, 1690 | fake daemon stream read/write timeout 2s | deadline | bounded I/O | – |
| 2110 | in-process observe slice + ReadScreen, `for 0..100`, sleep 20 | DEFECT | screen poll | A: runtime output/lifecycle wake |
| 2243 | session command `printf ...; sleep 1` | DEFECT (was NOT-TIMER) | fixture sleep: the session's lifetime is 1 s instead of an event the test controls | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 2264 | `recv_timeout(15s)` deadlock watchdog | deadline | error path | – |
| 2437, 2512, 2534 | Status+ReadScreen (+poll_route_events) loops, sleep 25 | DEFECT | screen/route poll | A |
| 2646 | entity `set_read_timeout(5s)` | deadline | bounded | – |
| 3226 | `try_wait` loop on own SIGKILLed child, sleep 20 | DEFECT | child poll | I: `Child::wait` thread → channel deadline |
| 3304 | `owned_session_worker_pids()` census until non-empty, sleep 50 | DEFECT | process-appearance poll | I: entity Upsert running, then one census |

Helper calls (not hits): 510 `wait_for_child_condition_with_budget`, 569 `wait_for_buffered_child_stdout`, 947 `wait_for_process_snapshot` (ps poll for zombie state; OS note: there is no kqueue event for "became zombie" of one's own child without reaping, but `NOTE_EXIT` fires at exit, which is the zombie transition), 1188 `wait_for_process_exit`, 412 `wait_for_cli_daemon_shutdown`.


# Group B2

## Timer inventory, group B2 (tests/hub_daemon_lifecycle/* minus sessions/packages/event_plane_saturation/shutdown, plus tests/hub_daemon_lifecycle_test.rs)

Checkout: 23b0feaa. Read-only; no cargo run. Every line in g-B2.txt (143) is classified; extras found by scanning for try_wait / try_recv / short recv slices / Instant loops are marked "(extra, not in grep)".

### Counts (grep lines only, 143)

| category | count |
|---|---|
| DEFECT | 118 (4 of them are the "slice" subtype: a short read/recv period that only re-checks the deadline) |
| NOT-TIMER | 8 (7 `set_read_timeout(None)`, 1 function definition) |
| deadline | 17 |
| os-no-event | 0 |
| measurement-window | 0 |
| backoff | 0 |
| rate-limit | 0 |

Revision 2 moves:
- 20 former NOT-TIMER fixture sleeps are now DEFECT.
- plugin_bounds.rs:231 moved from os-no-event to DEFECT, because it is a correctness assertion.
- terminal_stream.rs:106 and hub_daemon_lifecycle_test.rs:63 moved from NOT-TIMER to deadline, because they are the primitive forwarders that carry the marker.

Extras (not in grep): about 56 more DEFECT sites, listed per file. Revision 2 added webrtc_fixtures.rs 2618, which is listed in group C's table of bare webrtc timeouts in the lifecycle test files.

### DEFECT mechanism groups (for commit planning)

- **A. Waiting for a child or pid to exit** → own child: a wait thread for the child sends on a channel, then `recv_timeout(grace)`. Non-child pid: kqueue EVFILT_PROC NOTE_EXIT (macOS) or pidfd_open plus poll (Linux). Both work for any pid the test can signal, so os-no-event does not apply. The grace or budget stays as a deadline.
  - harness.rs 380, 390, 813, 823, 886
  - common.rs 450, 468
  - process.rs 205, 833, 902, 959, 1025, plus the zombie predicate at 113
  - cli.rs 864
  - operator_console_fixtures.rs 613, and 260 (socket unlink after exit)
  - unix_route_smokes.rs 440
  - Call-site extras: harness_isolation.rs 755, 857, 1078
- **B. The shared bounded-wait helper is itself a poll.** common.rs:420 `wait_for_child_condition_with_budget` calls `condition_met()` plus `try_wait` every 20 ms. operator_console_fixtures.rs 477 and 631 poll the same way. Rebuild it as the single allowed helper: `wait_until(rx, deadline)` over a channel (or Condvar), fed by (a) the producer (the PTY reader thread notifies on each append and at EOF) and (b) a child-wait thread. Callers:
  - cli.rs:341 (wait_for_status)
  - operator_console_fixtures.rs:120, :563
  - harness_isolation.rs:235
  - shutdown.rs:510 (out of group)
- **C. Daemon readiness polled by probing.** cli.rs:330-353 `wait_for_status` spawns a `botster-hub status` process every 20 ms. It runs for every CLI daemon start. operator_console_fixtures.rs:120-134 sends a Status request every 20 ms. Both are extras. Replacement: read a readiness line from the already-piped daemon stdout (or a readiness fd) against a deadline, racing the child-wait event.
- **D. Hub state polled through requests** → subscription or route events:
  - **D1. Session exit.** session_fixtures.rs 462; common.rs 847 → SessionLifecycle host event (`subscribe_events`) or a session entity `Patch lifecycle=exited` (`subscribe_session_entities`).
  - **D2. Output via ReadScreen.** session_fixtures.rs 191, 529; webrtc_terminal_adapter.rs 561; webrtc_proofs.rs 812; unix_terminal_adapter.rs 337, 828, 898 → a route Output frame containing the marker (then one ReadScreen if the screen itself is asserted).
  - **D3. Mode flags.** session_fixtures.rs 146, 157; paste_transaction.rs 224, and 369 (extra) → the `TerminalEvent::Modes` route event.
  - **D4. Cleanup counters and attach occupancy through Status.**
    - Sites: webrtc_terminal_adapter.rs 652, 1444; webrtc_proofs.rs 997, 1572; unix_terminal_adapter.rs 206, 592, 650, 661, 723, 1758, 1909; unix_route_smokes.rs 128-152 (extra).
    - Replacement: a host-event subscription frame such as TerminalSubscriptionClosed, a RuntimeObservation, or a peer-disconnected event.
    - At 592 and 650 the Detach or Attach response is itself the event, so assert once.
    - **Hub gap:** where the Hub only bumps a counter and emits no event, that is a production gap the writer must add. A test cannot wait on counter-only state.
  - **D5. Apps and local_url.** common.rs 284; webrtc_fixtures.rs 2236, 2371 → an apps entity or PackageEvent on publish, or the fixture's `web_listening=` stdout line; then one health probe.
  - **D6. Plugin state via MCP tool.** package_event_plane.rs 120, 260 → a host event (`subscribe_events` PackageEvent) emitted by the consumer fixture on receipt.
  - **D7. A request sent only to "flush" or poke delivery.** package_event_plane.rs 518; unix_terminal_adapter.rs 1006, 1257, 1495, 1530, 1663 → a blocking read with the remaining deadline and no poke. package_event_plane.rs 597 and 692 show that unsolicited delivery works without a request. If any of these needs the poke, that is a Hub flush defect.
- **E. "Slice" reads.** A short `timeout` or `recv` slice inside an outer deadline, re-checking only that deadline. Replacement: one read with `deadline - now`.
  - Grep lines: common.rs 1036; terminal_stream.rs 219; webrtc_proofs.rs 941; unix_terminal_adapter.rs 369.
  - Extras:
    - session_fixtures.rs 649
    - subscription_ownership_baseline.rs 41, 218, 232, 804
    - webrtc_terminal_adapter.rs 201, 327, 402, 459, 939
    - webrtc_proofs.rs 1877
    - paste_transaction.rs 362
    - unix_route_smokes.rs 46, 83, 226, 413
  - Round-robin polling across receivers (extras) → `select!` over all receivers against one deadline: webrtc_fixtures.rs 923-935, 1550-1590.
- **F. Fixed sleeps, quiet windows and fixed negative windows** → a positive barrier. Options: the response to a request on the same mux (`request_collecting(Status)` returns only after earlier frames); a sentinel event after the point being tested; or deleting the sleep where the next wait already carries the event.
  - Grep lines: operator_console.rs 545; webrtc_terminal_adapter.rs 782; webrtc_proofs.rs 986, 1519; unix_terminal_adapter.rs 152, 397, 694, 1296, 1375; terminal_stream.rs 198; paste_transaction.rs 116.
  - Extras:
    - session_fixtures.rs 670
    - webrtc_terminal_adapter.rs 470, 982, 1182, 1636
    - webrtc_fixtures.rs 684
    - paste_transaction.rs 406-420
    - unix_route_smokes.rs 320, 335, 426
    - subscription_ownership_baseline.rs 227 (re-emit retry)
- **G. Fixture-side polls or timing windows** (`while [ ! -f x ]; do sleep 0.01`, Python `os.path.exists` loops, a Node `setTimeout` delay, `sleep 5; exit 7`) → the fixture blocks on a FIFO read or a stdin line, and the test writes the release.
  - subscription_ownership_baseline.rs 957, 961
  - webrtc_terminal_adapter.rs 583
  - unix_terminal_adapter.rs 359, 787, 1326, 1360, plus 877 (the test-side ready-file poll)
  - package_fixtures.rs 1261, 1282, 1304, 1326
  - webrtc_fixtures.rs 1981
  - unix_route_smokes.rs 396
- **H. Process discovery by `ps` census in a loop** → wait for the session readiness event (Spawn response plus ready-marker output), then take one census. Optionally use kqueue NOTE_FORK/NOTE_EXEC on the worker pid.
  - process.rs 113 (setsid/pgid predicate), 435, 474, 524
  - harness_isolation.rs 9, and 986 (extra; pgid is set in pre_exec, so no wait is needed)
  - unix_route_smokes.rs 404
- **I. Registry or state-file polls** → kqueue EVFILT_VNODE / FSEvents on the file or directory, or a Hub event.
  - harness.rs 545; unix_terminal_adapter.rs 523
  - harness_isolation.rs 727: sleep-timed race → handshake channel
  - Dead helpers (no callers), delete: subscription_ownership_baseline.rs 85; paste_transaction.rs 50
- **J. Misc.**
  - cli.rs 295: nonblocking accept plus a 10 ms cancel poll → blocking accept or `poll()` on the listener fd plus a cancel pipe.
  - common.rs 719: buffered-stdout stability sampling (see os-no-event candidates).
  - hub_daemon_lifecycle_test.rs 66-67: the async `sleep` helper. Its only callers (webrtc_proofs.rs 812, 986) are DEFECT, so delete it.

### os-no-event candidates (explicit)

- **plugin_bounds.rs:231. Reclassified DEFECT in revision 2** (formerly os-no-event).
  - It sleeps 5 s between two `/proc/pid/stat` CPU-tick reads (Linux only), inside the lifecycle correctness test `plugin_bounds.rs`.
  - When `BOTSTER_ASSERT_IDLE_CPU_BOUND` is set, it asserts `delta_ticks * 4 <= CLK_TCK`, an idle-CPU bound of at most 250 ms of CPU in 5 s. Otherwise it only prints the number.
  - Because it asserts a bound for correctness, the measurement-window category does not apply (ruling 3).
  - Replacement, in two parts:
    - (a) Move the CPU measurement into `script/probe-hub-resources`, which is a resource probe, as a `timer: measurement-window` with the same 5 s value.
    - (b) In this test, assert the structural cause of idle CPU instead of its effect. After the reload, read one Status/PluginLifecycleStatus and assert that no owner deadline is armed, that `active_timer_resources == 0` (already asserted at 224) and that no Host/plugin work is queued. An owner blocked on `control_rx.recv()` with no armed deadline cannot wake without an event.
  - Status does not expose the owner's armed deadlines today. Part (b) needs that field (or the lifecycle-status entity from ruling 1).
- **common.rs:719. Classified DEFECT.**
  - It samples FIONREAD every 20 ms until the pipe byte count is ≥ min and stable for 5 samples, as a proxy for "attach child is blocked on stdout".
  - OS fact: no kernel event tells a pipe's reader that its writer is blocked; kqueue EVFILT_READ gives only readability.
  - So the pipe side really is os-no-event. The preferred replacement is a Hub-side stall or backpressure event (the Hub knows when the route is stalled). If no such event exists, this becomes a legitimate os-no-event site.
  - Revision 2 check on both platforms: macOS kqueue EVFILT_READ/EVFILT_WRITE and Linux epoll EPOLLIN/EPOLLOUT report only the readiness of the caller's own end. Neither tells a reader that the writer is blocked. The pipe-side fact therefore holds on both. The site stays DEFECT because the Hub-side event is the replacement.
- **process.rs:113 (setsid/pgid predicates at harness_isolation.rs 282, 543, 580, 986).** No kernel event reports a pgid or sid change; kqueue EVFILT_PROC has no such note. It is still DEFECT, for two reasons: setsid/setpgid run in pre_exec before exec, so the session-ready event (or `spawn()` returning, at 986) implies the state; and one snapshot after that event suffices. The zombie predicate (shutdown.rs 947, harness_isolation.rs 857) has an event: NOTE_EXIT or pidfd-readable fires at exit, before reaping.
- **process.rs 833/902/959/1025 and harness.rs 886: group-empty and argv-marker census.**
  - No kernel event reports "process group now empty" or "no process with argv X".
  - Use M2 (`src/process_exit.rs`; see the header), which is event-driven with no os-no-event residual.
    - macOS: atomic snapshot rounds with NOTE_EXIT per member.
    - Linux: pidfd POLLIN for running members and POLLHUP for zombie members, with emptiness decided by `killpg(pgid, 0) == ESRCH`.
  - The argv-marker variant has no pgid. It enumerates by marker and watches each pid the same way, and the set is empty when a round started by an event finds no member.
- **unix_terminal_adapter.rs:898.** The test must not attach before `stream_attach`, and ReadScreen is the only way to see Core screen state without a route. An observer route on a second connection gives an Output event. If that changes the test's semantics, the Hub needs a screen or session-output host event. Classified DEFECT.

---

### tests/hub_daemon_lifecycle/common.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 284 | `wait_for_app_local_url` ListApps ×50, sleep 20 | DEFECT | polls ListApps until local_url published | apps entity / PackageEvent host event on local_url publish, deadline |
| 420 | `wait_for_child_condition_with_budget` sleep 20 between `condition_met()`/`try_wait` | DEFECT | the shared helper polls both condition and child | rebuild as the single allowed helper: channel/Condvar fed by producer + child-wait thread, `recv_timeout(deadline)` |
| 450 | `terminate_and_reap_pty_child` 25×20ms `try_wait` after SIGTERM | DEFECT | polls PTY child exit during grace | portable_pty `wait()` on thread → channel `recv_timeout(500ms grace)`, then SIGKILL |
| 468 | `wait_for_owned_pid_exit` `kill(pid,0)` loop | DEFECT | polls non-child pid exit | kqueue NOTE_EXIT / pidfd with budget |
| 719 | `wait_for_buffered_child_stdout` FIONREAD stable-sample loop | DEFECT | "stable for N samples" settle heuristic for backpressure | Hub stall/backpressure event; os-no-event candidate for the pipe side (see above) |
| 847 | `wait_for_managed_git_session_exit` ListSessions sleep 10 | DEFECT | polls lifecycle=exited | SessionLifecycle host event / session entity Patch, deadline |
| 1036 | `wait_for_entity_frame` read timeout `min(remaining,200ms)` + string-matched retry | DEFECT (slice) | 200 ms cap only re-checks the deadline | `set_read_timeout(remaining)`; timeout → budget panic |

### tests/hub_daemon_lifecycle/harness.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 380 | `try_terminate_and_reap_child` TERM-grace `try_wait` + sleep 20 | DEFECT | polls own child exit | child wait thread → channel `recv_timeout(HUB_STOP_TERM_GRACE)` (or kqueue NOTE_EXIT) |
| 390 | same, KILL grace | DEFECT | same | same, `HUB_STOP_KILL_GRACE` |
| 545 | `reread_until_exited_or_bound` 8×25ms registry reload | DEFECT | polls registry file for state Exited | kqueue EVFILT_VNODE/FSEvents on registry record (or SessionLifecycle host event), deadline |
| 813 | `signal_worker_group` TERM grace `process_exists` loop | DEFECT | polls non-child worker exit | NOTE_EXIT / pidfd on worker pid, grace deadline |
| 823 | same, KILL grace | DEFECT | same | same |
| 886 | `wait_for_owned_absence` probe every 20 ms (pids, pgids, ps census) | DEFECT | generic absence poll | NOTE_EXIT on each captured pid (members captured before), then one census (see os-no-event note) |

### tests/hub_daemon_lifecycle/process.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 113 | `wait_for_process_snapshot` ps every 20 ms | DEFECT | polls ps for predicate (zombie / setsid / pgid) | zombie: NOTE_EXIT/pidfd; setsid/pgid: readiness event (or spawn return) then one snapshot |
| 205 | `wait_for_process_exit` 100×20ms `kill(pid,0)` | DEFECT | polls pid exit (callers include daemon-supervised pids) | NOTE_EXIT / pidfd, deadline |
| 435 | 80 ms "settle" before descendant census | DEFECT | let-it-settle for shell child to appear | session ready-marker output (shell has exec'd), or NOTE_FORK on worker, then census |
| 474 | `capture_new_session_workers_for_data_dir` loop sleep 30 | DEFECT | polls registry + ps for new worker | Spawn response + ready-marker event, then one census |
| 524 | `capture_new_session_workers_for_marked_pty` loop sleep 30 | DEFECT | polls ps for worker + marker process | wrapper's `ready` output event, then one census |
| 725 | `stdout_pipe_is_closed` `libc::poll(POLLIN/POLLHUP, 500)` | deadline | kernel event wait; caller (sessions.rs:3167) asserts closed, so expiry is failure | — |
| 833 | `reap_captured_pty_children` signal + census loop sleep 50 | DEFECT | polls group absence | NOTE_EXIT per captured pid/member; SIGKILL on deadline |
| 902 | `reap_processes_matching_marker` loop sleep 50 | DEFECT | polls argv census | capture once, NOTE_EXIT each, final census |
| 959 | `assert_cli_fixture_absent` 40 ms multi-probe loop | DEFECT | polls hub/worker/group/socket/list | NOTE_EXIT on hub pid + captured pids, then single check of socket/census/list |
| 1025 | `reap_session_workers_for_data_dir` loop sleep 50 | DEFECT | polls worker census | NOTE_EXIT on captured workers + descendants, final census |

### tests/hub_daemon_lifecycle/cli.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 189 | fixture stream read timeout 2 s | deadline | bounds one request read | — |
| 248 | stalled fixture read timeout 2 s | deadline | bounds one read | — |
| 254 | `release_rx.recv_timeout(5s)` | deadline | test-driven release channel | — |
| 295 | nonblocking accept + `cancel_rx.recv_timeout(10ms)` loop | DEFECT | 10 ms slice polls accept readiness and cancel | blocking accept on thread + wake connection, or `poll()` on listener fd + cancel pipe |
| 306 | timeout fixture read timeout 2 s | deadline | bounds one read | — |
| 311 | `cancel_rx.recv_timeout(4s)` | deadline | holds the stalled connection until cancel, bounded | — |
| 864 | `run_command_with_timeout_diagnostics` `try_wait` sleep 20 | DEFECT | polls child exit | child wait thread (plus pipe drain threads) → channel `recv_timeout(timeout)` |
| 340-353 (extra, not in grep) | `wait_for_status` runs `botster-hub status` subprocess per 20 ms via common.rs:420 | DEFECT | readiness poll on every CLI daemon start | readiness line on piped daemon stdout / readiness fd, raced with child-wait |

### tests/hub_daemon_lifecycle/harness_isolation.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 9 | `wait_for_registry_worker` sleep 20 | DEFECT | polls registry file for a pid | spawn readiness event (or registry vnode watch), then one read |
| 332 | `boundary.matched.recv_timeout(1s)` | deadline | channel event | — |
| 392 | `recv_timeout(5s)` | deadline | channel event | — |
| 443 | `recv_timeout(5s)` | deadline | channel event | — |
| 488 | `foreign.recv_timeout(5s)` | deadline | channel event | — |
| 505 | `recv_timeout(5s)` | deadline | channel event | — |
| 727 | updater thread sleep 100 before writing Exited | DEFECT | sleep-timed race against the collector's 25 ms reread poll | handshake: collector signals after first registry load (test hook), updater writes on signal; reread waits on vnode event |
| 1055 | `while true; do sleep 1; done` entrypoint body | DEFECT (was NOT-TIMER) | fixture keep-alive sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 755 (extra, not in grep) | `spawn_and_reap_sleep` → `wait_for_process_exit` after `child.wait()` | DEFECT | redundant poll; `wait()` already reaped | delete the call |
| 282, 543, 580 (extra) | `wait_for_process_snapshot(.. "own setsid")` | DEFECT | ps poll for pgid/sid change | session-ready event then one snapshot |
| 857 (extra) | wait for zombie state | DEFECT | ps poll | NOTE_EXIT / pidfd on the pid |
| 986 (extra) | wait for member to join leader group | DEFECT | pgid set in pre_exec before `spawn()` returns | none; assert once |
| 1078 (extra) | `wait_for_process_exit(pid)` supervised entrypoint | DEFECT | polls non-child exit | NOTE_EXIT / pidfd |

### tests/hub_daemon_lifecycle/operator_console.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 78 | fixture `sleep 60` | DEFECT (was NOT-TIMER) | fixture sleep sets the process lifetime | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 145 | fixture `exec sleep 60` | DEFECT (was NOT-TIMER) | intentionally wedged console (never ready); the wedge is a duration | `exec cat` on stdin or a FIFO the test closes |
| 468 | session command `sleep 300` | DEFECT (was NOT-TIMER) | sentinel session lifetime is a duration | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 545 | sleep 100 after `up\n`, before Ctrl-C | DEFECT | fixed delay for inline work to start | wait for console output showing `up` began (checkpoint + `wait_for_output_after`), then send ^C |
| 305-306 (extra, not in grep) | `wait_for_owned_pid_exit` ×2 | DEFECT | via common.rs:468 | NOTE_EXIT / pidfd |

### tests/hub_daemon_lifecycle/operator_console_fixtures.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 55 | node `setInterval(() => {}, 1000)` in script | DEFECT (was NOT-TIMER) | a timer that keeps the foreground fixture alive | keep it alive on a blocking read the test ends: `process.stdin.resume()` and exit on stdin `end`, or a FIFO the test closes |
| 260 | socket-path `exists()` loop sleep 20 after shutdown | DEFECT | polls unlink | NOTE_EXIT on owned daemon pid (awaited just above), then one check; or EVFILT_VNODE on dir |
| 477 | `try_wait_for_output_after` sleep 20 | DEFECT | polls output buffer + `try_wait` | reader thread notifies Condvar/channel per chunk and EOF; child wait thread on same channel; deadline |
| 613 | `wait_for_exit` `try_wait` sleep 20 (30 s) | DEFECT | polls child exit | portable_pty wait thread → channel `recv_timeout(30s)` |
| 631 | `finish_reader_after_exit` polls `reader_done` AtomicBool | DEFECT | spin-sleep on another thread's flag | reader sends on channel at EOF; `recv_timeout(budget)` then join |
| 120-134 (extra, not in grep) | readiness: Status request per 20 ms via common.rs:420 | DEFECT | readiness poll | console readiness output / readiness fd via reader channel |

### tests/hub_daemon_lifecycle/session_fixtures.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 68 | spawn request `printf 'daemon-ready'; sleep 1` | DEFECT (was NOT-TIMER) | fixture sleep: a caller that waits for its exit inherits a built-in 1 s | blocking read the test ends: `cat` on stdin or a FIFO the test closes; callers that need the exit close it |
| 146 | `wait_for_mode_flags` sleep 20 (non-ReadModeFlags reply) | DEFECT | polls ReadModeFlags | `TerminalEvent::Modes` route event, deadline |
| 157 | `wait_for_mode_flags` sleep 20 (predicate false) | DEFECT | same | same |
| 191 | `collect_attach_events` ReadScreen + sleep 30 | DEFECT | polls screen for marker | route Output event containing marker (read with remaining deadline) |
| 462 | `wait_for_authoritative_session_exit` sleep `AUTHORITATIVE_OBSERVE_BACKOFF` | DEFECT | "backoff" is a poll interval (no error precedes it) | SessionLifecycle host event / entity Patch exited, deadline; drop the const |
| 529 | `wait_for_producer_ready` ReadScreen sleep 25 | DEFECT | polls screen for marker | route Output frame with marker |
| 649-653 (extra, not in grep) | `wait_until_adapter_event_until` `poll_route_events(20ms)` loop | DEFECT (slice) | 20 ms slices re-check deadline | `poll_route_events(deadline - now)` |
| 670-672 (extra) | `poll_adapter_events` drain until a 20 ms quiet window | DEFECT | quiet period stands in for "nothing pending" | ordering barrier (request/response on same connection) |

### tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 85 | `wait_for_path` sleep 50 | DEFECT | file-exists poll; helper has no callers | delete |
| 168 | fixture `sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep (at 20b201bd this is line 170, `IFS= read -r go; ...; sleep 30`) | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 957 | shell gate `while [ ! -f gate ]; do sleep 0.01` | DEFECT | fixture-side poll | fixture blocks on FIFO read / stdin line; test writes release |
| 961 | same, peer B | DEFECT | same | same |
| 41 (extra, not in grep) | `wait_for_webrtc_marker` 200 ms slices, 45 s deadline | DEFECT (slice) | slices only re-check deadline | one `timeout(remaining)` per frame |
| 216-232 (extra) | entity/host-event 250 ms slices; re-emit `sample.ready` after 8 s | DEFECT | slices + retry that waits for state | recv with remaining deadline; no re-emit (a missed first event is the bug) |
| 804 (extra) | `await_next_webrtc_terminal_frame` 200 ms, used in loops 833/858 | DEFECT (slice) | slice inside backstop loop | recv with remaining backstop |

### tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 301 | `sleep 30` session cmd | DEFECT (was NOT-TIMER) | fixture sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 376 | same | DEFECT (was NOT-TIMER) | fixture sleep | same |
| 434 | same | DEFECT (was NOT-TIMER) | fixture sleep | same |
| 509 | same | DEFECT (was NOT-TIMER) | fixture sleep | same |
| 561 | ReadScreen loop with blocking `std::thread::sleep(25)` inside async block | DEFECT | polls screen (and blocks the runtime thread) | marker Output event on a Unix observer route (peer is unbound), then one ReadScreen |
| 583 | shell gate poll | DEFECT | fixture-side poll | FIFO/stdin release |
| 652 | Status `bound_adapter_close` counter poll | DEFECT | polls counter after peer close | host-event subscription on Unix connection for the cleanup (Hub gap if none emitted) |
| 782 | sleep 400 after peer close | DEFECT | settle sleep; assertion (grant one-use) does not depend on close timing | delete; if ordering needed, await peer Closed state / peer-disconnected event |
| 971 | `sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 979 | `sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep | same |
| 1444 | Status occupancy poll after WebRTC peer loss | DEFECT | polls occupancy | peer-disconnected / TerminalSubscriptionClosed host event on the Unix sibling, then one Status |
| 201, 215 (extra, not in grep) | `wait_for_webrtc_subscription_closed` 200 ms slices; 20 ms terminal drain on miss | DEFECT (slice) | slices re-check deadline | `select!` host-event / terminal recv with remaining deadline |
| 327, 402, 459, 939 (extra) | first-frame wait in 200 ms slices | DEFECT (slice) | same | one `timeout(remaining)` |
| 470-473 (extra) | 400 ms window counting extra-channel frames | DEFECT | fixed negative window | await `extra.closed` (reject) then assert zero frames |
| 982-1002 (extra) | 6 s loop runs full length (Status each 200 ms) | DEFECT | fixed negative window | after sibling frame, one Status response as barrier, then assert |
| 1182-1185 (extra) | 2 s drain of host events before "no close" assertion | DEFECT | fixed negative window | sentinel host event after shutdown (e.g. a later observable event) as barrier |
| 1636-1639 (extra) | 1 s drain before "no close" assertion | DEFECT | fixed negative window | same barrier after `extra.closed` |

### tests/hub_daemon_lifecycle/webrtc_proofs.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 812 | 120× ReadScreen + `sleep(30)` | DEFECT | polls screen for ready marker | `next_terminal_frame` with deadline until marker |
| 941 | driver-exit report 200 ms slices | DEFECT (slice) | slices re-check deadline | `timeout(remaining, next_host_event)` |
| 986 | `sleep(100)` after invalid request | DEFECT | waits for fail-closed handling | await data channel close / host event from Hub |
| 997 | retry `subscribe_session_entities` sleep 25 | DEFECT | retry loop waiting for prior subscription release | peer-cleanup host event (peer-disconnected), then subscribe once |
| 1519 | sleep 800 after peer close | DEFECT | settle sleep; socket attach does not depend on it | delete; cleanup proven by event at 1572 |
| 1572 | Status poll (cleanup counter + occupancy), 20 s | DEFECT | comment admits the poll | `webrtc_peer_disconnected` cleanup host event on Unix subscription |
| 1877 (extra, not in grep) | entity upsert 500 ms slices | DEFECT (slice) | slices re-check deadline | `timeout(remaining)` |

### tests/hub_daemon_lifecycle/webrtc_fixtures.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 1981 | node `setTimeout(listen, startupDelayMs)` (3000 ms from common.rs:56) | DEFECT | injected timed delay | gate `listen()` on explicit release (stdin line / FIFO) sent by the test after asserting pre-ready state |
| 2145 | health probe read timeout 1 s | deadline | bounds one read | — |
| 2236 | `wait_for_published_web_origin` ListApps sleep 20 | DEFECT | polls app local_url | apps entity / PackageEvent, or fixture `web_listening=` line |
| 2371 | `wait_for_botster_web_readiness` ListApps + health, sleep 20 | DEFECT | polls publish + health | same event, then one health probe |
| 684-690 (extra, not in grep) | `count_terminal_frames(bound)` 50 ms slices over a fixed window | DEFECT | fixed negative window | await channel close/reject event, then count |
| 923-935 (extra) | alternate 50 ms waits on two open receivers | DEFECT | round-robin poll | `select!` both with one deadline |
| 1550-1590 (extra) | round-robin 50 ms `timeout` across subscription inbounds | DEFECT | round-robin poll (also cancels in-flight receives) | merge inbounds into one channel / `select!`, one deadline |

### tests/hub_daemon_lifecycle/terminal_stream.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 104 | `fn set_read_timeout` wrapper | NOT-TIMER | function definition (an identifier, not a call) | — |
| 106 | inner `set_read_timeout(timeout)` | deadline (was NOT-TIMER) | the primitive forwarder: it applies the caller's read bound to the socket, and the marker goes here. Callers are classified at their own sites | — |
| 198 | `poll_unsolicited` reads until socket quiet for `timeout` | DEFECT | quiet window stands in for "no more frames" | `request_collecting(Status)` ordering barrier, or read until marker with deadline |
| 209 | `set_read_timeout(None)` | NOT-TIMER | clears timeout | — |
| 219 | `read_terminal_until` 200 ms slice | DEFECT (slice) | slice re-checks deadline; `done` only changes on reads | set read timeout to `deadline - now` each read |
| 229 | `set_read_timeout(None)` | NOT-TIMER | clears timeout | — |

### tests/hub_daemon_lifecycle/paste_transaction.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 50 | `wait_for_ingress_admissions` file poll sleep 10 | DEFECT | polls a log file; helper has no callers | delete (or EVFILT_VNODE) |
| 66 | sink `... ; sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep after `done` | blocking read the test ends: `cat` on a FIFO the test closes (stdin is the paste sink, so use a FIFO) |
| 116 | `collect_unix_mux_for(duration)` 50 ms reads over fixed windows (100 ms waiting; trailing 500 ms) | DEFECT | fixed windows; trailing 500 ms is a negative "exactly one result" window | read with remaining deadline until result + done; then `request_collecting(Status)` barrier for the negative |
| 130 | `set_read_timeout(None)` | NOT-TIMER | clears timeout | — |
| 224 | `wait_for_unix_ready_and_mode` ReadModeFlags sleep 20 | DEFECT | polls mode flags | ready marker Output + `TerminalEvent::Modes` route event |
| 362 (extra, not in grep) | ready marker 200 ms slices | DEFECT (slice) | slices re-check deadline | `timeout(remaining)` |
| 369-385 (extra) | ReadModeFlags loop with no sleep | DEFECT | request spin until modes readable | `TerminalEvent::Modes` event |
| 406-420 (extra) | 100 ms slices + 500 ms post-completion hold | DEFECT | fixed negative window | request/response barrier on the peer after completion |

### tests/hub_daemon_lifecycle/plugin_bounds.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 231 | `thread::sleep(5s)` between CPU-tick reads (Linux) | DEFECT (was os-no-event) | a correctness assertion, not a measurement: with `BOTSTER_ASSERT_IDLE_CPU_BOUND` set it asserts idle CPU ≤ 250 ms per 5 s, and it lives in a correctness test, so measurement-window does not apply | move the CPU measurement (5 s, unchanged) into `script/probe-hub-resources` as measurement-window. Here, assert once that no owner deadline is armed and no timer resource or queued work exists (needs a Status field; see the os-no-event candidates note) |

### tests/hub_daemon_lifecycle/package_fixtures.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 636 | `...; sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep. It is the session-type command whose arguments packages.rs:1358 asserts | blocking read the test ends: `cat` on stdin or a FIFO the test closes; update packages.rs:1358 with it |
| 675 | `while true; do sleep 1; done` | DEFECT (was NOT-TIMER) | entrypoint keep-alive sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 714 | same | DEFECT (was NOT-TIMER) | entrypoint keep-alive sleep | same |
| 771 | node `setInterval` keep-alive | DEFECT (was NOT-TIMER) | a timer keeps the entrypoint alive | `process.stdin.resume()` and exit on `end`, or a FIFO the test closes |
| 1261 | python `while not os.path.exists: time.sleep(0.01)` ×2 | DEFECT | fixture-side poll | `os.open(fifo, O_RDONLY)` / `sys.stdin.readline()` release |
| 1282 | same ×2 | DEFECT | same | same |
| 1304 | same ×3 | DEFECT | same | same |
| 1326 | same ×3 | DEFECT | same | same |
| 1408 | fixture HTTP read timeout 5 s | deadline | bounds one request read | — |

### tests/hub_daemon_lifecycle/package_event_plane.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 120 | MCP `event_plane.last_received` poll sleep 20 | DEFECT | polls plugin state | host event (PackageEvent) emitted by consumer on receipt |
| 260 | MCP `event_plane.cycle_status` poll sleep 40 | DEFECT | polls plugin state | same |
| 518 | Status poke + `min(remaining,50ms)` read loop | DEFECT | request sent to "flush" events + slices | blocking `next_event` with remaining deadline; poke need = Hub flush defect |
| 597 | event read timeout 3 s | deadline | single `next_event` | — |
| 692 | event read timeout 3 s | deadline | single `next_event` | — |

### tests/hub_daemon_lifecycle/unix_terminal_adapter.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 126 | `sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 152 | `poll_unsolicited(50ms)` + sleep 50 loop | DEFECT | quiet-window read + sleep until any envelope | `read_terminal_until` with remaining deadline |
| 206 | Status counter poll after disconnect | DEFECT | polls `bound_adapter_close` | host-event subscription (Hub gap if none) |
| 337 | ReadScreen poll sleep 25 | DEFECT | polls screen for echo (connection is bound) | route Output event, then one ReadScreen |
| 359 | shell gate poll | DEFECT | fixture-side poll | FIFO/stdin release |
| 369 | 200 ms slice waiting for Attached envelope | DEFECT (slice) | slice re-checks deadline | read with remaining deadline |
| 386 | `set_read_timeout(None)` | NOT-TIMER | clears | — |
| 397 | 200 ms quiet drain before negative checks | DEFECT | quiet window | `request_collecting(Status)` barrier |
| 415 | `set_read_timeout(None)` | NOT-TIMER | clears | — |
| 523 | `wait_for_live_close_routes` JSON file poll sleep 10 | DEFECT | polls state file | EVFILT_VNODE on file / Hub test-hook event |
| 592 | occupancy poll after Detach response | DEFECT | Detach response already the event | assert once (or TerminalSubscriptionClosed/host event if async) |
| 650 | occupancy poll after TerminalAttached | DEFECT | Attach response already the event | assert once |
| 661 | `cleanup_completed` poll after `drop(owner_a)` | DEFECT | polls counter | host-event subscription for cleanup |
| 694 | `poll_unsolicited(50)` + sleep 50 until echo | DEFECT | poll | `read_terminal_until` deadline |
| 723 | occupancy poll | DEFECT | polls occupancy after cleanup | assert once after cleanup event |
| 787 | shell gate poll | DEFECT | fixture-side poll | FIFO/stdin |
| 828 | ReadScreen poll sleep 25 | DEFECT | polls screen (route attached) | route Output event, then one ReadScreen |
| 869 | `printf x > ready; sleep 30` | DEFECT (was NOT-TIMER) | fixture sleep (the ready file itself is handled at 877) | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 877 | ready-file `exists()` poll | DEFECT | polls file | child writes to FIFO the test reads with deadline (or EVFILT_VNODE) |
| 898 | ReadScreen poll before stream_attach | DEFECT | polls screen; no route by design | Output event on a second observer route; else Hub screen-change event (see os-no-event note) |
| 1003 | `set_read_timeout(None)` | NOT-TIMER | clears | — |
| 1006 | 100 ms slice; on timeout sends Status | DEFECT | slice + poke per timeout | read with remaining deadline, no poke |
| 1014 | `set_read_timeout(None)` | NOT-TIMER | clears | — |
| 1257 | Status request + sleep 50 loop | DEFECT | poke-poll for echo + entity snapshot | blocking reads (terminal + entity frames) with deadline |
| 1296 | sleep 2 s before `wait_for_subscription_closed` | DEFECT | fixed delay; the next wait carries the event | delete |
| 1326 | shell gate poll | DEFECT | fixture-side poll | FIFO/stdin |
| 1360 | shell gate poll | DEFECT | fixture-side poll | FIFO/stdin |
| 1375 | sleep 200 after `drop(replacement)` | DEFECT | vacuous: `replacement_events` cannot change after drop | real oracle: host-event subscription on another connection + barrier |
| 1495 | Status + sleep 50 until echo | DEFECT | poke-poll | `read_terminal_until` deadline |
| 1530 | same | DEFECT | same | same |
| 1663 | same | DEFECT | same | same |
| 1758 | `wait_for_cleanup_completed` Status sleep 20 | DEFECT | polls counter | host-event subscription |
| 1909 | ListSessions poll after Spawned | DEFECT | Spawned already proves presence; poll cannot catch the later removal under test | wait for EOF-cleanup event, then one ListSessions |

### tests/hub_daemon_lifecycle/unix_route_smokes.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 396 | `printf ready; sleep 5; exit 7` | DEFECT | 5 s timing window for observing the worker | fixture `read _; exit 7`; test releases after observing worker |
| 404 | `owned_session_worker_pids()` poll sleep 20 | DEFECT | polls for worker after ready marker already observed | assert once after `attach_ready` |
| 440 | worker reap poll sleep 25 | DEFECT | polls worker exit | NOTE_EXIT / pidfd on captured worker pid |
| 46, 83, 226-227, 413 (extra, not in grep) | `poll_route_events(20-25ms)` loops | DEFECT (slice) | slices re-check deadline | `poll_route_events(deadline - now)` |
| 128-152 (extra) | occupancy Status poll with 10 ms route poll | DEFECT | polls occupancy | response-is-event / host event |
| 320, 335, 426 (extra) | `poll_route_events(100ms)` before negative assertion | DEFECT | fixed negative window | request/response barrier on same connection |

### tests/hub_daemon_lifecycle_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 63 | `webrtc::runtime::timeout(...)` inside the `timeout()` wrapper | deadline (was NOT-TIMER) | the shared async deadline primitive: the call inside the wrapper is where the marker goes. Callers are classified at their own sites | — |
| 66 | `async fn sleep` helper | DEFECT | plain sleep; only callers webrtc_proofs.rs 812, 986 are DEFECT | delete with callers |
| 67 | `webrtc_runtime().sleep(duration)` | DEFECT | same helper body | delete |

### Notes for the writer

- Out-of-group callers of these shared helpers: packages.rs (20 calls), sessions.rs (10), shutdown.rs (4), and update_command_test.rs:146 (`wait_for_process_exit`). They inherit these fixes.
- `crates/botster-hub-test-support/src/unix_route.rs::poll_route_events` blocks on `poll_terminal(timeout)`, so it is event-driven. Only the short slice arguments at call sites are defects.
- `terminal_stream.rs::read_terminal_until` and `unix_terminal_adapter.rs:1006` treat every read error as a timeout. With the slices removed, EOF would spin, so the replacement must distinguish WouldBlock/TimedOut from EOF.


# Group C

## Timer-site inventory, group C (src/ except src/daemon.rs and src/daemon/**)

Source: `g-C.txt` (221 grep hits). Rules: `classify-rules.md`. Repo read at branch `delivery/event-driven-20260925`. I edited nothing and ran no cargo.
Scope column: **P** = production code, **T** = test code (`#[cfg(test)]` module/fn or `*_tests.rs` / `test_support.rs`), **F** = `feature = "allocation-oracle"` diagnostic build. "P (smoke CLI)" / "P (run-one CLI)" means operator smoke commands that ship in the binary.

### Counts (the 221 grep hits)

| category | hits |
|---|---|
| DEFECT | 141 (production 30, test 111) |
| deadline | 78 |
| NOT-TIMER | 1 (control_channel.rs:28, an import) |
| backoff | 1 (listener.rs:186) |
| rate-limit | 0 |
| measurement-window | 0 |
| os-no-event | 0. The former group-empty "os-no-event part" of 3 sites is gone (M2) |

Revision 2 moves:
- Six former NOT-TIMER lines are now DEFECT:
  - entrypoint_supervisor.rs:1162 is a fixture sleep.
  - session_type_spawn.rs:292 and session_spawn/reply.rs:126 and 128 are mechanical zero-duration receives.
  - package_event_router.rs:6106 and peer.rs:3387 are scheduler yields.
- allocation_oracle.rs:534 moved from NOT-TIMER to deadline.

How rows were counted: a row written as `a+b` is one `tokio::time::timeout` (deadline) wrapped around a `yield_now` spin (DEFECT), so it counts as one of each.

Extra sites the grep missed are marked "(extra, not in grep)": 37 rows (10 DEFECT, 26 deadline, 1 NOT-TIMER).
- The first 13 include the `wait_until` / `soft_wait_until` callers, the bare `webrtc::runtime::timeout(` calls in control_channel.rs, and the no-sleep spin loops.
- In revision 2, control_channel.rs:750 moved from NOT-TIMER to DEFECT (mechanical).
- Revision 2 adds 24 deadline rows: the bare `webrtc::runtime::timeout(` calls in local_webrtc_smoke.rs, signaling.rs, peer.rs tests and test_support.rs. See the "Bare webrtc timeouts" table at the end of group C.

### Reviewer hints: all three confirmed
- **runtime.rs:1618**: confirmed DEFECT. `invoke_plugin` loops `recv_timeout(1ms)` and calls `fulfill_pending_plugin_requests()` on every timeout. The timeout exists only to pump bridge requests.
- **entrypoint_supervisor.rs:329**: confirmed DEFECT. The wait is capped at 50 ms. On expiry it rereads the launch-result file and re-polls the child with `try_wait` and a drain of the output readers.
- **transport/unix/connection.rs:313**: confirmed DEFECT. A 25 ms `sleep` select arm retries flushes. It works with **mux_write.rs:464**, which gives each write a 50 ms `timeout` slice and returns `Pending` on expiry, and with **mux_write.rs:251** (extra), which loops `flush_pending_responses` the same way. All three should be fixed in one commit.

### DEFECT mechanism groups (commit planning)

1. **Owner control channel: replace `try_recv` + `sleep` with a blocking recv under a deadline** (tests, WebRTC harness)
   - test_support.rs 904, 1051, 1135, 1185, 1291, 1343, 1393, 1437, 1494, 1529
   - subscription_channel.rs 1719, 2202, 2344, 2539, 2590
   - peer.rs 3229
   - Change: `runtime.block_on(timeout(remaining, control_rx.recv()))`, handle each message, then re-check the predicate.
2. **Host executor completion or disposal: replace spins with a blocking wait on the executor's completion wake** (tests)
   - `poll_completion` Empty => yield: runtime.rs 8328; entity_model.rs 59; entity.rs 2993; entity_resync.rs 765; host_executor.rs 2044; plugin_entity.rs 785
   - `job.poll()` until Disposed: host_disposal.rs 322, 347; lua_runtime.rs 3967; runtime.rs 7374, 7418; entity_model.rs 313, 543; entity.rs 3379; host_executor.rs 1467
   - Retry `dispose_terminal` or `test_disposed` in a spin: entity.rs 3240, 3295, 3358; coordination_lifecycle_tests.rs 629, 693
   - Flags or counters written by host threads: host_executor.rs 1504, 1550, 1567, 1680, 1902, 2322, 2411, 2433; coordination_lifecycle_tests.rs 705
   - Needs a real wait API: a blocking `HostExecutor` completion receive and a `Job` wait, or a completion channel, bounded by a deadline.
3. **Owner-turn driver spins (`drive_ready_test_turn` + yield/sleep): block on the owner wake, then run one turn**
   - coordination_lifecycle_tests.rs 105 (the `drive_until` helper)
   - entity_resync.rs 475, 514, 553, 589, 630, 641, 689, 820, 874, 942, and 678-681 (extra)
   - entity_resync.rs 653, 831: these wait for the owner's own retry deadline. Wait until `state.deadlines.next_deadline()` once, or inject a clock.
   - daemon_maintenance.rs 879 (`run_maintenance_kind_to_completion`, waits for a Core read), 3704, 3811, 3840, 3895, 4105, and 4008 (extra)
4. **Coordination bridge enqueue: replace `test_pending_count` / `take_pending` spins with the bridge's enqueue wake or a test hook channel**
   - coordination_lifecycle_tests.rs 251, 367, 410, 480, 661
   - collection_capacity_tests.rs 245
   - acknowledge_input.rs 1134
   - lifecycle.rs 902 (admission-lock busy retry) and 942 (completion drain poll) are the same kind of spin over plugin lifecycle state.
5. **Core ticket and bridge pumping in production**
   - runtime.rs 6176 (`CoreTicket::wait`, 1 ms poll)
   - client_api.rs 261 (`HubClientPending::wait`, 1 ms poll)
   - runtime.rs 1618 (`invoke_plugin`, 1 ms pump)
   - Change: one completion wake (condvar or channel) raised by the data-plane driver and by every bridge enqueue, bounded by a deadline.
6. **Data-plane watchdog (production)**
   - driver.rs 1484/1490: `wait_pump(DATA_PLANE_WATCHDOG = 1s)`. `close_work.requeue()` (close_work.rs:226) and the overflow remainder raise no wake, so requeued close decisions only advance on the next Core wake or when the watchdog fires.
   - Make requeue and overflow raise the pump wake (or let the Core session-registry transition drive them), then drop the watchdog.
   - `DATA_PLANE_STOP_BOUND` (driver.rs 1449) is defined as `2*WATCHDOG + slack`. Its value stays the same (ruling 2). Only its definition changes to a literal with the same value, stated in its `timer: deadline` marker.
7. **Child-process exit (production and tests): a wait thread feeding a channel, or kqueue `EVFILT_PROC` / pidfd, bounded by a deadline**
   - Production: local_runtime_process.rs 245, 261, 511, 525; managed_git_worktrees.rs 968, 1006, 1015; entrypoint_supervisor.rs 445, 461; update.rs 241, 305, 313
   - Tests: entrypoint_supervisor.rs 952, 995; peer.rs 3368; test_support.rs 1947, and the `is_fully_gone` callers 992, 1016, 1964 (extra)
   - Settle sleeps: local_runtime_process.rs 180 (read stderr to EOF instead), test_support.rs 1637.
8. **Readiness polls in production and the CLI**
   - local_runtime_process.rs 194 (daemon readiness: use a readiness pipe)
   - entrypoint_supervisor.rs 329 (fold child exit and reader EOF into the notify event channel)
   - main.rs 2122 (app URL)
   - Marker, screen, and status polls: main.rs 848, 4075, 4084 (`sleep 1` in the run-one shell wrapper); local_webrtc_smoke.rs 147, 208
   - operator_console.rs 257: 50 ms `poll(2)` that re-checks the interrupt flag. Use a signal self-pipe.
9. **Unix connection writes (production)**
   - connection.rs 313, mux_write.rs 464, mux_write.rs 251 (extra): await socket writability.
   - connection.rs 713: shutdown polls `is_finished` every 10 ms. Use `block_on(select(join tasks, control_rx.recv()))` under one deadline.
10. **Wall-clock expiry in tests: pass explicit instants or inject a clock**
    - package_events.rs 1371, 2091
    - package_event_router.rs 6153, 7006, 7072, 7098, and 7154-7196 (extra busy cycle)
    - peer.rs 1683 (`sleep(1100ms)` for reservation expiry)
    - lua_runtime.rs 1770 (`TEST_EVENT_HANDLER_HOLD_MS` sleep): replace with a condvar gate.
11. **Async spins on mock flags: await the Notify the mock already has** (test_support.rs:72/84: `usage_notify`, `send_notify`)
    - subscription_channel.rs 1300, 1420, 1433, 1531
    - control_channel.rs 1195, 1272, 1474, and 2107/2145/2147 (single `yield_now` used to order "send is hung")
    - listener.rs 530 (socket rebind: use a rebind notify)
12. **Fixed-time negative assertions: wait for a positive "reached the pressured wait" event, then do one non-blocking poll**
    - control_channel.rs 1109, 1676, 1776 (extra): `timeout(20ms, delivery).is_err()`
    - entrypoint_supervisor.rs 1076 (100 ms delay in the fixture)
    - event_plane_counters.rs 914 (5 ms sleep in a reader whose result is discarded; delete it)
13. **Inventory or state polls on Core retirement: use the Core `terminal_inventory_changed` / lifecycle wake**
    - attach_routes.rs 1319; subscription_channel.rs 1697; test_support.rs 934, 963 (extra)
    - peer.rs 1424, 2039, 2345, 3291 (extra, `dedicated_runtime_worker_threads()==0`): join the runtime threads.
    - test_support.rs 1590 (extra, scans the process table for a new worker pid): take the pid from the spawn reply.
    - peer.rs 1937: ShedBusy retry spin on the mailbox.
14. **Test helper to delete**
    - test_support.rs 761: the `wait_until` / `soft_wait_until` poll helper (10 ms sleep). Every caller is listed in groups 7 and 13.

### os-no-event candidates (revision 2: none)
- **Process-group emptiness** is DEFECT with no os-no-event part. Affected sites: local_runtime_process.rs 245/261, managed_git_worktrees.rs 1006/1015, entrypoint_supervisor.rs 445/461.
  - The group leader is the owned child. Its exit has an event: the wait handle on a thread, kqueue `EVFILT_PROC NOTE_EXIT`, or pidfd.
  - The other members use M2 (`src/process_exit.rs`; see the header).
    - macOS: rounds of atomic `proc_listpgrppids` snapshots with NOTE_EXIT per member. The group is empty when a round ends with no new pid and zero live registrations.
    - Linux: pidfd POLLIN for running members and POLLHUP for zombies. The group is empty when `killpg(pgid, 0) == ESRCH`.
  - Every round starts on an event, so no timed `killpg(pgid, 0)` residual remains.
  - The STOP_GRACE / SIGTERM / SIGKILL budgets stay `timer: deadline` on the blocking wait, with unchanged values.
- No other site qualifies. File readiness (entrypoint_supervisor.rs 329, listener.rs 530) has FSEvents or inotify parent-directory events. Waiting on a foreign pid (local_runtime_process.rs 511, test_support.rs 952/1947) has kqueue `EVFILT_PROC` or pidfd.

### Unverified candidates (not counted; one closer read would settle them)
- daemon_maintenance.rs 3187-3194 and 3356-3363 repeat `for _ in 0..8 { run_maintenance_kind_to_completion(HostBridge); if baseline.is_some() break }`. If the baseline depends on host-thread work, this is a spin bounded by a count. If each slice finishes the work synchronously, it is not a timer.
- subscription/entity.rs 3992-3999 and 4091-4098 repeat `drive_all_maintenance` + `try_recv` up to 32 times, waiting for the first snapshot frame. Same question.

### Notes for the writer
- allocation_oracle.rs:534 (`recv_timeout(1s)` asserted `Timeout`) is classified deadline in revision 2. The allocation oracle's `Phase::Wait` deliberately drives the reply channel's deadline to expiry, which is the error path production reply waits take, so that the oracle can account the allocations on that path. The 1 s value is unchanged. A value suggestion is recorded under "Follow-ups (not applied)".
- package_event_router.rs:6106 is DEFECT in revision 2 (it was NOT-TIMER). Every iteration publishes, as load generation during an unload, and the loop ends on the worker's `done` flag. The `yield_now` is a scheduler-timing call, not load. Remove it; the publications are the load.
- entrypoint_supervisor.rs:505 (extra) is a real deadline (`output_finalization_deadline`). Today it is only evaluated when a poll loop calls `refresh()`, so the event-driven rewrite must arm it as an actual timer.

### src/client_api.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 261 | P | HubClientPending::wait: loop poll()+sleep(1ms) until deadline | DEFECT | 1ms poll of Core ticket/spawn handoff progress | block on a completion wake (Core ticket reply channel/condvar signalled when the pending stage can advance) with the caller's deadline |

### src/daemon_maintenance.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 879 | T (cfg(test) fn) | run_maintenance_kind_to_completion: while reads.in_flight() { sleep(1ms); rerun } | DEFECT | poll for Core read completion | Core completion wake (owner Core wake / ticket reply channel) then rerun the slice once, with deadline |
| 3704 | T | while causal_operation_count()>0 { apply_causal_owner_ops; yield_now } | DEFECT | spin waiting for admitted releases from other threads | causal-op admission wake (the owner notify raised when causal ops are queued) then apply, with deadline |
| 3811 | T | delivery slice + set_delivery_wake + sleep(10ms) until 2 in flight | DEFECT | poll delivery (and self-raises the wake it should wait for) | block on the router delivery wake / plugin admission capacity wake, then run one slice, with deadline |
| 3840, 4105 | T | run_completion_drain_slice + sleep(10ms) until no flights/retirements (drain_event_flights helper at 4105) | DEFECT | poll for plugin completions | plugin completion wake (Core plugin completion -> owner wake) then one drain slice, with deadline |
| 3895 | T | while !drain_plugin_completions(0,..).has_remaining { sleep(5ms) } | DEFECT | poll for Core plugin completion publish | plugin completion wake with deadline |
| 4008 (extra, not in grep) | T | while causal_owner_ops_pending() or pending_retirements non-empty { apply_event_plane_owner_ops; flush } (no sleep) | DEFECT | spin waiting for event release from plugin/host threads | causal-op/retirement wake (owner notify) then one apply+flush, with deadline |

### src/data_plane/driver.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 782 | P | typed CoreTicket::wait: recv_timeout(remaining) loop, dropping stale phases | deadline | completion channel is the event; stale phases consumed without extending the deadline | - |
| 1449 | P | stop_and_join: done.recv_timeout(DATA_PLANE_STOP_BOUND) | deadline | driver done channel is the event. The bound is derived from 2*DATA_PLANE_WATCHDOG today; when the watchdog goes, the bound keeps its value as a literal (ruling 2) | - |
| 1484, 1490 | P | run_loop: core_daemon.wait_pump(DATA_PLANE_WATCHDOG=1s) when no request pending | DEFECT | 1s watchdog wake re-runs close_work: requeue() (close_work.rs:226) and the overflow remainder send no wake, so requeued close decisions and overflow keys only progress on the next Core wake or watchdog expiry | wait with no timeout; requeue/overflow-remainder must raise the pump wake (or be re-driven by the Core session-registry transition that makes session_close_event_decision Some); stop already interrupts via request_stop+unpark |

### src/data_plane/driver/allocation_oracle.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 534 | F | callback_reply: assert recv_timeout(1s)==Timeout | deadline (was NOT-TIMER) | allocation-oracle diagnostic (`Phase::Wait`): it drives the reply channel's deadline to its expiry (error) path so the oracle can account that path's allocations. The value is unchanged | - |

### src/entrypoint_supervisor.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 329 | P | readiness: events.recv_timeout(min(remaining,50ms)); on Timeout reread launch result + refresh child | DEFECT | 50ms cap re-checks child exit/output and rereads file on expiry (reviewer hint confirmed) | feed child-exit (wait thread) and stdout/stderr EOF into the same event channel as the notify watcher; any watcher event on file or parent triggers reread; single recv_timeout(remaining) deadline. FS fact: FSEvents/inotify coalesce but still deliver a parent-dir event, so no timed fallback is needed |
| 445, 461 | P | stop: refresh + supervised_process_group_exists every 20ms (STOP_GRACE, then SIGKILL 2s) | DEFECT | poll for child exit + group empty | child exit via wait thread/kqueue EVFILT_PROC/pidfd -> channel with deadline; group emptiness via M2 (`src/process_exit.rs`); STOP_GRACE and 2s stay deadlines |
| 945 | T | wait_for_pid_file: read file + sleep(10ms) | DEFECT | poll for fixture pid file | fixture writes pid to a pipe (stdout) the test reads with deadline, or notify watcher on the file |
| 952 | T | assert_pid_gone: kill(pid,0) + sleep(20ms) | DEFECT | poll for descendant exit | kqueue EVFILT_PROC NOTE_EXIT / pidfd_open on the pid with 2s deadline |
| 995 | T | observe_child_exit: refresh + yield_now until exited_at | DEFECT | spin on try_wait | child-exit event (wait thread -> channel, same source production should use) with deadline |
| 1076 | T | fixture thread sleep(100ms) before sending output EOF | DEFECT | fixed delay to order "output arrives after exit observed" | gate the fixture on a positive event: test hook signalled when wait_for_launch_result has recorded exit with pending output |
| 1162 | T | sh -c "while :; do sleep 1; done" fixture | DEFECT (was NOT-TIMER) | fixture keep-alive sleep | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 505 (extra, not in grep) | P | output_finalization_deadline checked in refresh | deadline | give-up for output readers after exit; but it is only evaluated when a poll loop calls refresh (329/445/461), so the event-driven rewrite must arm it as a real timer | - |

### src/event_plane_counters.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 914 | T | reader thread sleep(5ms) then sample(); result discarded | DEFECT | fixed delay trying to interleave threads; asserts nothing | delete the reader thread (the deterministic version-bracket assertions below carry the test) or drive interleaving with a barrier |

### src/host_disposal.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 311 | T | recv_timeout(5s) on PanicDrop thread-name send | deadline | destructor sends; 5s give-up | - |
| 322, 347 | T | loop job.poll()/sibling.poll() + yield_now until PartialDestruction/Disposed | DEFECT | spin waiting for host worker thread to finish the job | host job completion wake (executor completion channel/condvar signalled on job finish) with deadline |

### src/host_executor.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 1422 | T | wait_for_worker_exit: while any !is_finished { yield_now } | DEFECT | spin waiting for worker threads to exit | join each worker (exit is the event); if a bound is needed, a per-worker exit-notify channel with recv_timeout |
| 1467 | T | job.poll() + yield_now until Disposed | DEFECT | spin waiting for host disposal | host job completion wake with deadline |
| 1471, 1517, 1606, 1669, 1707, 1872, 1923 | T | receiver/dropped_rx.recv_timeout(1s/5s) of worker thread name | deadline | payload Drop sends the event | - |
| 1504 | T | while !executor.take_completion_notification() { yield_now } | DEFECT | spin on completion notification flag | the executor's owner completion wake (owner_rx channel / notify) with deadline |
| 1550, 2411 | T | while !gate.has_started() { yield_now } | DEFECT | spin waiting for host job to enter the gate | gate "entered" channel/condvar signalled by the job, with deadline |
| 1567, 1680, 2433 | T | while pool.outstanding/prepared_bytes/permits.outstanding != 0 { yield_now } | DEFECT | spin waiting for permit release on host thread | permit-release notify (drop-signal channel on the permit / payload) with deadline |
| 1902 | T | while !executor.stopping { yield_now } | DEFECT | spin on stopping flag set by worker | a stop-notify channel/condvar raised where `stopping` is set (or owner wake), with deadline |
| 2044 | T | receive_host_completion: poll_completion Empty => yield_now | DEFECT | spin on host completion queue | blocking completion receive on the executor's completion wake with deadline |
| 2322 | T | while !executor.wake.completion_pending { yield_now } | DEFECT | spin on completion_pending flag | owner_rx.recv_timeout (the owner wake the executor publishes) with deadline |
| 2419 | T | dropped_rx.recv_timeout(250ms), asserted Ok | deadline | executor drop completion is the event; expiry = failure | - |

### src/lifecycle.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 902 | T | admit(): try_admit retry + yield_now while "admission lock busy" | DEFECT | spin waiting for another thread to drop the admission lock | blocking admission (lock acquire) or a lock-release notify; keep 5s deadline |
| 942 | T | drain(): drain_completions + sleep(1ms) | DEFECT | poll for worker completion | completion wake from the plugin worker (Core completion notify / channel) with deadline |

### src/local_runtime_process.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 180 | P | child exited -> sleep(20ms) then drain stderr | DEFECT | let-it-settle for stderr reader thread | read stderr to EOF: stderr reader thread closes channel on EOF; recv until Disconnected with deadline |
| 194 | P | readiness loop: Status probe then sleep(50ms) | DEFECT | poll for daemon readiness | readiness pipe fd inherited by daemon (written after listener bind) + child-wait thread -> channel, select with readiness_budget deadline |
| 245, 261 | P | terminate: try_wait + process_group_exists every 20ms (SIGTERM 500ms, SIGKILL 2s) | DEFECT | poll for child exit and group emptiness | leader exit: child-wait thread -> channel (or kqueue EVFILT_PROC/pidfd) with deadline. Group emptiness: M2 (`src/process_exit.rs`); no os-no-event residual. 500ms/2s stay deadlines |
| 511 | P | wait_for_runtime_daemon_exit(non-child pid): ps -p every 50ms | DEFECT | poll for foreign pid exit | kqueue EVFILT_PROC NOTE_EXIT (macOS) / pidfd_open+poll (Linux) with 10s deadline |
| 525 | P | wait_for_owned_runtime_daemon_reaped: waitpid WNOHANG / ps every 50ms | DEFECT | poll for owned child exit | kqueue EVFILT_PROC/pidfd on pid then waitpid; or blocking waitpid on a thread -> channel with deadline |

### src/local_webrtc_smoke.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 147 | P (smoke CLI) | up to 120x ReadScreen + sleep(30ms) until marker | DEFECT | poll screen for marker | subscribe to session terminal output over the peer (or a daemon wait-for-output request) and scan frames, bounded by one deadline |
| 208 | P (smoke CLI) | poll Status every <=10ms for local_webrtc_terminal_record | DEFECT | poll for terminal record publication | daemon event subscription / blocking request that returns when the grant's terminal record is recorded, with deadline |

### src/lua_runtime.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 293, 364 | P | coordination reply recv_timeout(COORDINATION_REQUEST_TIMEOUT_MS) | deadline | owner reply channel is the event; expiry maps to Timeout error | - |
| 816, 843 | T (cfg(test) fns) | test plugin gate condvar wait_timeout_while(entered/released) | deadline | condvar notified by gate; bound is test safety | - |
| 1770 | T (cfg(test) block in invoke) | TEST_EVENT_HANDLER_HOLD_MS: sleep(hold_ms) in handler (used by daemon_maintenance.rs:4133 as EVENT_INVOCATION_TIMEOUT_MS+500) | DEFECT | fixed hold to outlast the invocation deadline; global static | replace with a condvar gate (like controlled_gate): hold until the test observes the timeout classification, then release |
| 3867 | T | PendingDropGate::wait recv_timeout(5s) | deadline | entered channel | - |
| 3967 | T | finish_host_clear: job.poll() + yield_now until Disposed | DEFECT | spin waiting for host worker disposal | host job completion wake (executor completion channel) with deadline |
| 4021 | T | finished.recv_timeout(5s) | deadline | finished signal | - |

### src/lua_runtime/acknowledge_input.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 387, 391 | P | AcknowledgeReplyReceiver::recv_timeout wrapper | deadline | thin wrapper over channel recv with caller bound | - |
| 1134 | T | loop bridge.take_pending() + yield_now (comment: "does not prove a production wake") | DEFECT | spin waiting for wrapper thread to enqueue | bridge enqueue wake (the owner notify the bridge raises on enqueue) with deadline |

### src/lua_runtime/collection_capacity_tests.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 245 | T | while bridge.test_pending_count()<2 { yield_now } | DEFECT | spin waiting for producer thread enqueue | bridge enqueue wake / test hook channel with deadline |

### src/lua_runtime/coordination_lifecycle_tests.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 82 | T | assert_operation_drop: recv_timeout(5s) on destructor thread-name send | deadline | operation Drop sends on channel; 5s is give-up | - |
| 105 | T | drive_until helper: drive_ready_test_turn + yield_now until predicate | DEFECT | spin waiting for Core/host threads to publish completion wakes | block on the owner wake (the owner loop's completion/maintenance wake source that publish_completion_wakes reads) with deadline, then run one turn |
| 251, 367, 410, 480, 661 | T | while bridge.test_pending_count()!=1 { yield_now } | DEFECT | spin waiting for caller thread to enqueue on the coordination bridge | the bridge's existing enqueue wake to the owner (coordination ingress notify) or a test hook channel signalled on enqueue, with deadline |
| 318, 320 | T | finished.recv_timeout(5s); response.recv_timeout(5s)==Disconnected | deadline | host-clear finished signal / sender drop are events | - |
| 350, 352 | T | hold_core gate: recv_timeout(5s) release / entered | deadline | test gate handshake channels | - |
| 629, 693 | T | while pending_requests non-empty { dispose_terminal_requests; yield_now } | DEFECT | spin waiting for host executor disposal receipt | host executor completion wake (host disposal receipt channel) then one dispose_terminal_requests call, with deadline |
| 705 | T | while usage()!=slot { yield_now } | DEFECT | spin waiting for last lease endpoint release on host thread | host disposal completion receipt / lease-drop signal (e.g. join the host job or a drop-notify channel) with deadline |

### src/lua_runtime/entity_publish.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 411 | P | publish: reply recv_timeout(ENTITY_PUBLISH_REQUEST_TIMEOUT_MS) then try_retract | deadline | owner reply channel is the event; expiry retracts as error path | - |
| 580 | T | finished.recv_timeout(5s) | deadline | host clear finished signal | - |

### src/lua_runtime/session_type_spawn.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 292 | T | recv_timeout(Duration::ZERO) | DEFECT (mechanical; was NOT-TIMER) | non-blocking receive of an already-sent value through a timer API | `try_recv()` |

### src/main.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 848 | P (smoke CLI) | smoke_session_round_trip: ReadScreen + sleep(25ms) until marker | DEFECT | poll screen for marker | subscribe to the session's terminal output over the daemon connection and scan frames under one deadline |
| 2122 | P | wait_for_app_url: ListApps + sleep(50ms) until running with local_url (5s) | DEFECT | poll app registry for readiness | daemon request that completes on the app lifecycle transition (or an app-state event subscription) with deadline |
| 4075 | P (run-one CLI) | read_screen_until_marker: CoreTicket ReadScreen + sleep(20ms) | DEFECT | poll screen for marker | Core session output/terminal-change wake (Core terminal subscription) then read, under SMOKE_TIMEOUT |
| 4084 | P (run-one CLI) | shell wrapper "...; \"$@\"; sleep 1" | DEFECT | 1s hold keeps the process alive so the screen read can catch the marker before exit | read the retained final screen after the Core process-exit event, or block the shell on stdin that the Hub closes after observing the marker |

### src/managed_git_worktrees.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 968 | P | wait_for_child: try_wait + sleep(10ms) until MANAGED_GIT_COMMAND_TIMEOUT | DEFECT | poll for git child exit | child wait thread -> channel (or kqueue EVFILT_PROC / pidfd) with command deadline; timeout path terminates |
| 1006, 1015 | P | terminate_owned_child_group: try_wait + owned_process_group_exists every 10ms | DEFECT | poll for leader exit and group empty | leader exit event as above; group emptiness via M2 (no os-no-event residual) |

### src/operator_console.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 257 | P | libc::poll(stdin,50ms) loop re-checking signals.take_interrupt() | DEFECT | short poll timeout exists only to re-check the interrupt flag | add a signal self-pipe (handler writes a byte) to the pollfd set; poll with -1 |

### src/package_event_router.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 6106 | T | load-generator loop: try_ingress + yield_now until worker sets done | DEFECT (was NOT-TIMER) | each iteration performs a publication (concurrent load during unload), but the `yield_now` is scheduler timing inside a loop that ends on another thread's flag | drop the `yield_now`; the publications are the load, and the loop still ends on the worker's `done` flag |
| 6153, 7006, 7072, 7098 | T | sleep(3ms) to age/expire queued copy before pull/ingress | DEFECT | fixed delay to advance wall clock | APIs already take `Instant` arguments: pass synthetic instants (t0, t0+ttl) instead of Instant::now() after a sleep |
| 7154-7196 (extra, not in grep) | T | loop pull_ready_batch(Instant::now())/requeue until batch empty, 200ms deadline | DEFECT | busy cycle waiting for wall-clock queue_age expiry | pass a synthetic `now` beyond queue_age to pull_ready_batch (API already takes the instant) |

### src/plugin_entity.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 785 | T | completion(): poll_completion Empty => yield_now | DEFECT | spin on host completion queue | blocking completion receive with deadline |

### src/runtime.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 394 | T (cfg(test) Drop) | payload destructor gate recv_timeout(5s) | deadline | test release channel | - |
| 1618 | P | invoke_plugin: loop outcome_receiver.recv_timeout(1ms) -> fulfill_pending_plugin_requests on each timeout | DEFECT | 1ms spin to pump coordination/entity-publish/session-spawn requests (reviewer hint confirmed) | one wake object (condvar or channel) signalled by the invocation thread's outcome AND by each bridge enqueue (coordination, entity publish, session-type spawn, managed spawn, causal ops); block on it, pump only when woken |
| 5368, 5452 | P | session-type / managed spawn reply recv_timeout(SESSION_TYPE_SPAWN_TIMEOUT_MS) | deadline | owner reply channel; expiry is error | - |
| 6176 | P | CoreTicket::wait: poll + sleep(1ms) until deadline (callers client_api.rs:80 startup, main.rs:4018/4028/4061 smoke) | DEFECT | 1ms poll of Core ticket | Core completion wake: ticket completion notify (condvar/channel signalled by the data-plane driver when the ticket completes) with deadline |
| 7374, 7418 | T | engine_job/bridge_job.poll() + yield_now until Disposed | DEFECT | spin waiting for host worker disposal | host job completion wake with deadline |
| 7391, 7406 | T | observed.recv_timeout(5s) | deadline | destructor-observed channel | - |
| 8328 | T | executor.poll_completion() Empty => yield_now | DEFECT | spin on host executor completion queue | blocking HostExecutor completion receive (existing completion wake) with deadline |
| 7495-7499 (extra, not in grep) | T | while causal_owner_ops_pending() { apply_causal_owner_ops } with deadline assert | NOT-TIMER | same-thread budgeted drain of ops already admitted synchronously; no other actor awaited | - |

### src/runtime/entity_model.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 59 | T | completion(): poll_completion Empty => yield_now | DEFECT | spin on host executor completion queue | blocking completion receive with deadline |
| 294, 483 | T | entered_rx.recv_timeout(5s) | deadline | probe entered channel | - |
| 313, 543 | T | job.poll() + yield_now until Disposed | DEFECT | spin waiting for host worker disposal | host job completion wake with deadline |

### src/runtime/session_spawn.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 1026 | T | timeout(10s) around receiver.recv() of Core wake | deadline | Core wake channel is the event | - |

### src/runtime/session_spawn/reply.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 75, 76 | P (dead_code) | SpawnReplyReceiver::recv_timeout wrapper | deadline | thin wrapper over channel recv with caller-supplied bound; no poll | - |
| 126, 128 | T | recv_timeout(Duration::ZERO) | DEFECT (mechanical; was NOT-TIMER) | zero timeout = non-blocking receive of an already-sent value through a timer API | `try_recv()` (add it to `SpawnReplyReceiver` if missing) |

### src/subscription/attach_routes.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 1319 | T | loop list inventory + sleep(10ms) until lost route gone | DEFECT | poll Core inventory for route retirement | Core terminal-inventory-changed wake (DataPlaneProgress.terminal_inventory_changed -> owner wake/control message) then one inventory read, with deadline |

### src/subscription/entity.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 2993 | T | executor.poll_completion() Empty => yield_now | DEFECT | spin on host executor completion queue | blocking HostExecutor completion receive with deadline |
| 3240, 3358 | T | while !dispose_terminal(&executor) { yield_now } | DEFECT | spin retrying disposal until host worker releases capacity/finishes | host executor completion/capacity wake, then one dispose_terminal call, with deadline |
| 3295 | T | while !terminal.test_disposed() { yield_now } | DEFECT | spin waiting for host worker disposal | host job completion wake with deadline |
| 3379 | T | disposal.poll() + yield_now until Disposed | DEFECT | spin waiting for host worker disposal | host job completion wake with deadline |

### src/subscription/entity_resync.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 475, 514, 553, 589, 630, 641, 689, 820, 874, 942 | T | drive_ready_test_turn + yield_now until scan/table predicate | DEFECT | spin waiting for host/causal-table progress | block on the owner wake (host completion / causal table progress notify) with deadline, then run one turn |
| 678-681 (extra, not in grep) | T | while !scan.running { drive_ready_test_turn } (no yield) | DEFECT | spin waiting for scan start | owner wake then one turn, with deadline |
| 653, 831 | T | drive_ready_test_turn + sleep(1ms) until attempts changes (retry deadline) | DEFECT | poll until the owner's retry deadline elapses | wait until state.deadlines.next_deadline() (the owner deadline is the wake; recv_timeout/sleep_until that exact instant once) then one turn; or inject a test clock |
| 765 | T | poll_completion Empty => yield_now | DEFECT | spin on host completion queue | blocking completion receive with deadline |

### src/subscription/package_events.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 1297 | T | run_while_pool_is_contended: result recv_timeout(100ms) "must not wait for contended pool" | deadline | result send is the event; expiry = failure (bound is tight but it is a give-up) | - |
| 1371, 2091 | T | sleep(3ms) so mailbox event TTL expires before take_ready_event | DEFECT | fixed delay waiting for wall-clock expiry | inject time: pass an explicit `now` (push instant + ttl) to take_ready_event / a test clock instead of sleeping |

### src/transport/shared/ingress.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 284 | T | done_rx.recv_timeout(1s) "producer must not wait for consumer lock" | deadline | producer result send is the event | - |

### src/transport/shared/wake.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 69 | T | timeout(50ms, notify.notified()) expected Ok | deadline | stored Notify permit resolves immediately | - |

### src/transport/unix/adapter.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 750 | T | timeout(50ms, mux.wait_for_write()) expected Ok (stored permit) | deadline | stored wake permit resolves immediately; expiry = failure | - |

### src/transport/unix/connection.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 313 | P | select arm tokio::time::sleep(25ms) if unsent mux writes / pending write / event_output_ready -> retry flush | DEFECT | 25ms retry of writes that previously returned Pending (reviewer hint confirmed) | select on `write_half.writable()` readiness while mux_write.has_pending(); unsent mux writes and event output already have wakes (mux.wait_for_write, event_reader.wait) - make those arms level-correct instead of timer-backed |
| 713 | P | wait_for_connection_tasks: while !all finished { drain_shutdown_cleanups; sleep(10ms) } | DEFECT | poll for connection task completion during shutdown | runtime.block_on(select { join of all tasks, control_rx.recv() -> drain one cleanup }) under one DAEMON_CLIENT_WRITE_TIMEOUT deadline, then abort |

### src/transport/unix/listener.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 186 | P | accept Err -> sleep(100ms) | backoff | pause after accept error (EMFILE etc.) | - |
| 523+530 | T | timeout(5s){ loop symlink_metadata(socket).is_socket() else task::yield_now } | DEFECT (outer timeout = deadline) | async spin on filesystem state | await a rebind event: the accept loop's rebind notification (test hook/control message) or a notify watcher on the socket path, inside the 5s deadline |
| 541, 568, 586 | T | timeout(1-2s) around accept_task join / control_rx.recv() | deadline | join / channel recv are events | - |

### src/transport/unix/mux_write.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 464 | P | write_frame_bytes_resumable: timeout(50ms) around poll_write_vectored; Err(_) => Pending | DEFECT | 50ms write slice then give up to the connection's 25ms retry timer (pair with connection.rs:313) | return Pending on Poll::Pending immediately (no timer) and have the connection loop await socket writability; keep DAEMON_CLIENT_WRITE_TIMEOUT as the only stall deadline |
| 533 | P | timeout(first_byte_timeout, read first byte) | deadline | handshake deadline; socket read is the event | - |
| 554 | P | timeout(DAEMON_INCOMPLETE_FRAME_TIMEOUT, read rest of frame) | deadline | incomplete-frame give-up | - |
| 635 | P | timeout(DAEMON_CLIENT_WRITE_TIMEOUT, write_all) | deadline | client write give-up | - |
| 251 (extra, not in grep) | P | flush_pending_responses: loop flush_unix_mux_writes until no pending response or started.elapsed() >= DAEMON_CLIENT_WRITE_TIMEOUT | DEFECT | retry loop; each iteration re-enters the 50ms write slice (mux_write.rs:464) instead of awaiting writability | await socket writability between attempts; keep DAEMON_CLIENT_WRITE_TIMEOUT as the single deadline |

### src/transport/webrtc/adapter.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 892 | T | rx.recv_timeout(1s) "close must not wait for permit lock" | deadline | close thread send is the expected event | - |
| 1057 | T | timeout(50ms, handle.wait_for_write()) expected Ok | deadline | stored permit resolves immediately | - |

### src/transport/webrtc/control_channel.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 28 | P | use webrtc::runtime::timeout | NOT-TIMER | import; its call sites are listed below as extras | - |
| 177 | P | send_text_or_peer_terminal: sleep(LOCAL_WEBRTC_PEER_CLOSE_BOUND) raced in select with the send and peer_terminal_rx | deadline | send completion / peer terminal watch are the events | - |
| 492 | P | timeout(5s, reply_rx) for owner reply | deadline | oneshot reply is the event; expiry -> OPERATOR_ERROR_RUNTIME_REQUEST_TIMED_OUT | - |
| 641, 656 | P | close_data_channel: timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND) around close-frame sends and local_close | deadline | send/close futures are the events | - |
| 750 (extra, not in grep) | P | timeout(webrtc runtime, LOCAL_WEBRTC_EVENT_PROBE=Duration::ZERO, local_poll()) | DEFECT (mechanical; was NOT-TIMER) | a zero-duration non-blocking probe for pressure events between chunks. Nothing is awaited, but it uses a timer API | poll `local_poll()` once without a timer (`now_or_never()`, or the runtime's non-blocking form); verify it has the same single-poll semantics |
| 1185+1195 | T | timeout(2s){ loop read sent log until HelloAck chunks complete; task::yield_now } | DEFECT (outer timeout = deadline) | async spin on mock sent log | mock send_notify (test_support.rs:84) notified on each send; await it inside the deadline |
| 1226, 1253, 1282, 1291 | T | timeout(2s) around runtime_rx.recv() / driver join | deadline | channel recv / task join are events | - |
| 1270+1272 | T | timeout(2s){ while !send_entered { yield_now } } | DEFECT (outer timeout = deadline) | async spin on mock flag | await data_channel.send_notify.notified() (register before flag check) |
| 1368 | T | response_delivery_rx.recv_timeout(1s) | deadline | delivery outcome channel | - |
| 1474 | T | while sent.len()<3 { sleep(1ms) } | DEFECT | poll mock sent log from another thread | mock send notify (condvar/channel per send) with deadline |
| 1109, 1676, 1776 (extra, not in grep) | T | assert timeout(20ms, delivery).is_err() "scheduler time must not close a live pressured peer" | DEFECT | fixed-time negative assertion | wait for the positive event that delivery reached its pressured wait (record_response_progress / send_entered notify), then assert pending with a single non-blocking poll (now_or_never) |
| 1120, 1686 (extra, not in grep) | T | timeout(250ms, delivery).expect(...) | deadline | delivery completion is the event | - |
| 2107, 2145, 2147 | T | single task::yield_now in a select arm before publishing terminal / pushing event / releasing hung send | DEFECT | scheduler yield used to order "send is now hung" before the injected event | await the mock's send_entered notify (send_notify) before publishing the terminal / event |

### src/transport/webrtc/peer.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 385 | P | close_peer_on_runtime: timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, peer.close()) | deadline | close future is the event | - |
| 1683 | T | sleep(1100ms) after 1s reservation expiry, then inspect == Expired | DEFECT | fixed delay waiting for wall-clock expiry | inject the reservation clock (now_seconds override / explicit now passed to lookup) and advance it past expiry |
| 1937 | T | mailbox.try_push retry on ShedBusy with yield_now (<=1000 attempts) | DEFECT | spin waiting for mailbox lock/capacity held by another thread | mailbox capacity/lock-release wake (event-plane capacity notification) or a blocking admission call with deadline |
| 3229 | T | control_rx.try_recv Empty => sleep(5ms) until PeerClosed | DEFECT | poll owner control channel | blocking control_rx.recv under the 10s deadline |
| 3368 | T | hang-close child: try_wait + sleep(20ms) until HANG_CLOSE_CHILD_DEADLINE | DEFECT | poll for child exit | child.wait() on a thread -> channel recv_timeout(HANG_CLOSE_CHILD_DEADLINE) (or kqueue/pidfd) |
| 3387 | T | task::yield_now inside spawned task body | DEFECT (was NOT-TIMER) | a scheduler yield used as the suspension point that proves a detached task runs to completion | await a oneshot/Notify the test fires after spawning; that is a deterministic suspension point with no scheduler timing |
| 3391, 3395 | T | started_rx/done_rx.recv_timeout(2s) | deadline | task sends are the events | - |
| 1424, 2039, 2345, 3291 (callers, extra) | T | wait_until(dedicated_runtime_worker_threads()==0, 2s) | DEFECT | poll a thread counter | join the dedicated runtime's worker threads (or a condvar notified when the counter reaches 0) with deadline |

### src/transport/webrtc/subscription_channel.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 600 | P | select arm sleep_until(close_deadline) after ChannelClosed (LOCAL_WEBRTC_PEER_CLOSE_BOUND) | deadline | normal exit is the OnClose event from local_poll; expiry closes/fails peer | - |
| 848 | P | timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, local_close()) | deadline | close future is the event | - |
| 1134, 1148, 1340, 1456, 1541, 1817, 1900 | T | tokio timeout(1-5s / bound+1s) around notify, driver/flush join, or frame recv | deadline | awaited future (Notify, task join, channel recv) is the event | - |
| 1298+1300, 1416+1420, 1431+1433, 1527+1531 | T | timeout(2s) { while !send_entered / sent.len()!=1 / !usage_entered { task::yield_now } } | DEFECT (outer timeout = deadline) | async spin on mock flags | mock already has send_notify/usage_notify (test_support.rs:72/84): await notified() (register before checking flag) inside the 2s deadline; add a notify for sent-log growth |
| 1697 | T | loop: inventory + reservation lookup + sleep(5ms) until Core route gone and reservation Unknown | DEFECT | poll Core inventory / reservation table | drive the owner control channel with a blocking recv under deadline (RetireReservedSubscription is the event) and assert state after it; Core retirement via Core wake |
| 1719 | T | while is_adapter_bound { try_receive_owner_message -> handle; sleep(5ms) } | DEFECT | poll owner control channel | blocking recv on the owner control channel with deadline (runtime.block_on(timeout(control_rx.recv()))) |
| 2202, 2539, 2590 | T | try_receive_owner_message Empty => sleep(5ms) until deadline | DEFECT | poll owner control channel | blocking control_rx.recv under deadline |
| 2344 | T | pump_test_control_until helper: try_receive + handle + sleep(5ms) | DEFECT | poll owner control channel | blocking control_rx.recv under deadline, handle each message, check predicate after each |

### src/transport/webrtc/test_support.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 761 | T | soft_wait_until / wait_until helper: predicate + sleep(10ms) | DEFECT | generic poll helper (definition) | delete; each caller waits on its own event (see caller rows) |
| 934, 963 (callers, extra) | T | soft_wait_until(list_session_lifecycle(...) terminal / None) | DEFECT | poll Core session lifecycle | Core session lifecycle/close wake (owner control message or Core completion wake) then one read, with deadline |
| 992, 1016, 1964 (callers, extra) | T | soft_wait_until/wait_until(workers all is_fully_gone: pid dead + control socket gone + group pids dead) | DEFECT | poll process liveness and socket file | kqueue EVFILT_PROC NOTE_EXIT / pidfd per known pid (worker + captured group members) and a notify watcher on the control socket path, with deadline |
| 1590 (caller, extra) | T | wait_until(session_worker_identities() shows a new worker pid) | DEFECT | poll process table for spawned worker | take the worker pid from the Spawn completion / Core session record (spawn reply is the event) |
| 904, 1051, 1135, 1185, 1291, 1343, 1393, 1437, 1494, 1529 | T | harness helpers: try_receive_owner_message Empty => sleep(5ms) until deadline, handling each message | DEFECT | poll owner control channel | blocking recv on the owner control channel with deadline (runtime.block_on(timeout(remaining, control_rx.recv()))), handle each message, re-check predicate after each |
| 1637 | T | spawn_and_attach_on_peer: sleep(50ms) "settle" before capturing worker tree | DEFECT | let-it-settle | capture the worker tree after the worker's readiness event (worker handshake / Core session record with pid); no sleep |
| 1947 | T | reap_owned_worker: SIGTERM, sleep(50ms), then check/kill | DEFECT | fixed grace instead of waiting for exit | kqueue EVFILT_PROC / pidfd per target pid with a grace deadline, then SIGKILL survivors |
| 2032 | T | timeout(1s, receiver.recv()) | deadline | channel recv is the event | - |

### src/update.rs

| line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| 241 | P | capture: try_wait + sleep(25ms) until deadline | DEFECT | poll for child exit | child wait thread -> channel with recv_timeout(deadline) (stdout/stderr readers already threads) |
| 305, 313 | P | stream: try_wait + sleep(25ms) (main loop and 2s SIGTERM grace) | DEFECT | poll for child exit | child wait thread -> channel; recv_timeout(deadline) then recv_timeout(grace) after SIGTERM |

### Bare webrtc timeouts (revision 2; extra, not in grep)

The original grep matched `time::timeout`, not a bare `timeout(` imported from `webrtc::runtime`. These are the `src/` sites it missed. Each wraps one expected event, so each is a deadline. Values are unchanged.

| file:line | scope | site (short code) | category | reason (one line) | replacement |
|---|---|---|---|---|---|
| src/local_webrtc_smoke.rs:302 | P (smoke CLI) | `let _ = timeout(5s, gather_complete_rx.recv())` | deadline | ICE gathering completion is the event. On expiry the offer goes out with the candidates gathered so far, which is a give-up path and not a receipt. The reviewer should confirm that proceeding is acceptable, or make expiry an error | - |
| src/local_webrtc_smoke.rs:335, 342, 397, 416, 452, 497 | P (smoke CLI) | timeout(10-15s) around connected / data-channel open / message recv | deadline | channel recv is the event; expiry maps to `SmokeError` | - |
| src/transport/webrtc/signaling.rs:114 | P | `let _ = timeout(5s, gather_complete_rx.recv())` | deadline | same give-up as smoke 302: expiry proceeds with the candidates gathered so far. Same reviewer check | - |
| src/transport/webrtc/peer.rs:3496, 3517, 3534, 3542, 3550, 3558, 3615, 3652, 3660, 3673 | T | timeout(5-15s) around gather / connected / open / incoming / message recv | deadline | each awaits one expected event; expiry fails the test | - |
| src/transport/webrtc/test_support.rs:402 | T | `let _ = timeout(5s, gather_complete_rx.recv())` | deadline | same gather give-up (test harness) | - |
| src/transport/webrtc/test_support.rs:435, 443, 591, 645, 673 | T | timeout(10-15s) around connected / open / reserved message recv | deadline | expected-event recv; expiry fails | - |

### Bare webrtc timeouts in the lifecycle test files (revision 2; extra, not in grep)

These are the 39 bare `timeout(` calls in `tests/hub_daemon_lifecycle/*` at 23b0feaa, each classified on its own. File:line is at **20b201bd**, and the 23b0feaa line is in parentheses where it differs.
- webrtc_proofs.rs:1735 is the 40th call at 20b201bd. It is new since 23b0feaa and is classified in "Sites added between 23b0feaa and 20b201bd".
- Rows marked "already listed" repeat an extra that B1 or B2 already has, so they add no new site.
- Tally: 25 DEFECT (22 already listed, 3 new: webrtc_fixtures 2618, event_plane_saturation 3385 and 3444), 12 deadline, 2 NOT-TIMER. Values are unchanged throughout.

| file:line (20b201bd) | site | category | reason | replacement |
|---|---|---|---|---|
| webrtc_fixtures.rs:435 | `recv_raw(bound)`: `timeout(bound, rx.recv())` | deadline | primitive: one receive bounded by the caller's `bound`; callers are classified at their own sites | - |
| webrtc_fixtures.rs:449 | `loop { timeout(10s, rx.recv()) }` while reassembling a response | deadline | each receive expects the next chunk, and expiry returns an error. Note: the 10 s restarts after each skipped message, so the absolute bound is 10 s per message, not per response | - (optionally `remaining` against one absolute deadline; the value is unchanged) |
| webrtc_fixtures.rs:687 | `count_terminal_frames(bound)`: 50 ms slices over a fixed window | DEFECT | fixed negative window (already listed at 684-690) | await the channel close/reject event, then count |
| webrtc_fixtures.rs:852 | `let _ = timeout(5s, gather_complete_rx.recv())` | deadline | ICE gathering give-up: expiry proceeds with the candidates gathered so far (same as signaling.rs:114) | - |
| webrtc_fixtures.rs:915 | `timeout(15s, connected_rx.recv())` | deadline | expected connection event; expiry errors | - |
| webrtc_fixtures.rs:926, 933 | alternating `timeout(50ms, data_channel_open_rx)` / `timeout(50ms, extra_open_rx)` | DEFECT | round-robin poll of two receivers (already listed at 923-935) | `select!` both receivers with one 10 s deadline |
| webrtc_fixtures.rs:948 | `timeout(10s, data_channel_open_rx.recv())` | deadline | expected open event | - |
| webrtc_fixtures.rs:984 | `timeout(3s, encrypted_hello(...))` | deadline | expected HelloAck; expiry errors | - |
| webrtc_fixtures.rs:1416 (1420) | `timeout(5s, extra_channel.open_rx.recv())` | deadline | expected open-or-fail event | - |
| webrtc_fixtures.rs:1440 (1444) | `timeout(500ms, data_channel.ready_state())` on the failure path | deadline | bounds a diagnostic read so that the failure path cannot hang; it is not a wait for progress | - |
| webrtc_fixtures.rs:1554, 1583 (1553, 1582) | round-robin `timeout(50ms, receive_delivery)` across subscription inbounds and the main inbound | DEFECT | round-robin poll that also cancels in-flight receives (already listed at 1550-1590) | merge the inbounds into one channel or `select!`, one deadline |
| webrtc_fixtures.rs:2618 (2617) | unit test: `timeout(50ms, receive_delivery)` expected `Err` after admitting only the first chunk | DEFECT (mechanical; new) | fixed 50 ms negative window. Admission is synchronous, so "still waiting for the remainder" is decidable with one poll | poll `receive_delivery` once (`now_or_never()` is `None`), then drop the future and assert that reassembly is kept |
| webrtc_terminal_adapter.rs:200, 214 (201, 215) | `wait_for_webrtc_subscription_closed`: 200 ms host-event slices; 20 ms terminal drain on a miss | DEFECT (slice) | slices re-check the deadline (already listed) | `select!` host event / terminal recv with the remaining deadline |
| webrtc_terminal_adapter.rs:326, 401, 458, 938 (327, 402, 459, 939) | first-frame wait in 200 ms slices | DEFECT (slice) | already listed | one `timeout(remaining)` |
| webrtc_terminal_adapter.rs:472 (473) | 400 ms window counting extra-channel frames, 50 ms slices | DEFECT | fixed negative window (already listed at 470-473) | await `extra.closed` (reject), then assert zero frames |
| webrtc_terminal_adapter.rs:984 (985) | 6 s loop of 200 ms frame reads that runs its full length | DEFECT | fixed negative window (already listed at 982-1002) | after the sibling frame, use one Status response as a barrier, then assert |
| webrtc_terminal_adapter.rs:1028 (1029) | `timeout(deadline - now, next_terminal_frame_with_label)` | deadline | already the correct form: the remaining absolute deadline for an expected frame | - |
| webrtc_terminal_adapter.rs:1183 (1184) | 2 s loop of `let _ = timeout(100ms, next_host_event)` before "no close" | DEFECT | fixed negative window (already listed at 1182-1185) | a later host event as the barrier, then assert |
| webrtc_terminal_adapter.rs:1510 (1511) | `timeout(5s, next_host_event)`, "package event arrives without later traffic" | deadline | expected event; expiry fails | - |
| webrtc_terminal_adapter.rs:1632 (1633) | `timeout(5s, extra.closed.recv())` | deadline | close is the expected event | - |
| webrtc_terminal_adapter.rs:1637 (1638) | 1 s loop of `timeout(100ms, next_host_event)` before "no close" | DEFECT | fixed negative window (already listed at 1636-1639) | barrier event after `extra.closed` |
| webrtc_terminal_adapter.rs:1700 (1701) | `timeout(5s, wrong_channel.closed.recv())` | deadline | close is the expected event | - |
| subscription_ownership_baseline.rs:41 | `wait_for_webrtc_marker`: 200 ms slices to a 45 s deadline | DEFECT (slice) | already listed | one `timeout(remaining)` per frame |
| subscription_ownership_baseline.rs:121, 125 | `"... timeout(local_close) ..."` inside assertion message strings | NOT-TIMER | text in a string literal, not a call | - |
| subscription_ownership_baseline.rs:223 (218) | entity-frame wait in 250 ms slices to 20 s | DEFECT (slice) | already listed at 216-232 | recv with the remaining deadline |
| subscription_ownership_baseline.rs:237 (232) | host-event 250 ms slices, re-emitting `sample.ready` after 8 s | DEFECT | slices plus a re-emit retry that waits for state (already listed at 216-232) | recv with the remaining deadline; no re-emit |
| subscription_ownership_baseline.rs:809 (804) | `await_next_webrtc_terminal_frame`: 200 ms | DEFECT (slice) | slice inside backstop loops (already listed) | recv with the remaining backstop |
| paste_transaction.rs:375 (362) | ready-marker wait in 200 ms slices to 8 s | DEFECT (slice) | already listed | `timeout(remaining)` |
| paste_transaction.rs:420 (407) | 100 ms slices plus a 500 ms post-completion hold | DEFECT | fixed negative window (already listed at 406-420) | request/response barrier on the peer after completion |
| event_plane_saturation.rs:3385 | `drain_webrtc_host_events`: `timeout(10ms, next_host_event)` until 10 ms quiet or a 100 ms slice end | DEFECT (new, group B1) | quiet window stands in for "nothing pending" | non-blocking drain: poll `next_host_event` with `now_or_never()` until `None`; no timer |
| event_plane_saturation.rs:3444 | `timeout(250ms, next_host_event)` chunks to a 5 s deadline, waiting for a token event or EventGap | DEFECT (slice; new, group B1) | slice that only re-checks the deadline. B1 had noted this as a deadline under "Reviewed, not defects"; the B2 slice rule governs | `timeout(deadline - now, next_host_event)` |
| webrtc_proofs.rs:2046 (1877) | entity upsert in 500 ms slices to 10 s | DEFECT (slice) | already listed | `timeout(remaining)` |


# Group D

## Group D timer inventory (crates/**, packages/**, script/**, test.sh, other tests/*.rs)

Worktree: delivery/event-driven-20260925 at 23b0feaa. Read only. Line numbers are current-tree.

### Counts (the 154 grep lines in g-D.txt)

| category | count |
|---|---|
| DEFECT | 118 |
| deadline | 20 |
| NOT-TIMER | 11 |
| rate-limit | 2 |
| measurement-window | 2 (probe-hub-resources 232, test-production-package-runtime 590) |
| backoff | 0 |
| os-no-event | 1 (isolated_hub.rs 2187, candidate; facts below) |

Revision 2 moves:
- 20 fixture sleeps went from NOT-TIMER to DEFECT: run.rs 367/417/459; isolated_hub 1506; lib.rs 6890/7067/7091/7114/7255; selftest 75/292/428/460/469; process-census 213/256; test-harness-control.py 34/41; hub_client_api_test 3578/3670.
- 6 Hub-client forwarders and restores went from NOT-TIMER to deadline: 1017, 1102, 1246, 1269, 1488, 1489.
- 3 lines went from os-no-event to DEFECT: selftest 332/410 and publish-npm 144.
- 2 lines went from DEFECT to measurement-window.

Extras not in the grep: 44 DEFECT.
- test-support lib.rs 4412 and update_command_test.rs 745.
- 24 fixture-lifetime sleeps (these were NOT-TIMER in revision 1).
- 18 Python library-timeout waits (new in revision 2; see "Python library timeouts").

The other extras are non-defect: deadline primitives, owner-drive loops, and sampling cadence. They are listed at the end.

packages/** has no timer sites. The only matches are the protocol type fields `active_timer_resources` and `stalled_write_timeouts` in packages/hub-test-support/daemon-protocol.ts at 914 and 1061. tests/support/mod.rs has none, and test.sh has none. The fixture plugin.lua copies have no timers.

### os-no-event candidates (OS facts)

- **isolated_hub.rs 2187 (os-no-event candidate; the reviewer must agree).** It waits for SIGSTOP to take effect on every Hub-group member. `group_quiescent` requires every non-zombie member in state `T`.
  - Checked on both platforms.
  - **macOS:** kqueue EVFILT_PROC offers NOTE_EXIT, NOTE_FORK, NOTE_EXEC, NOTE_SIGNAL, NOTE_EXITSTATUS and the deprecated NOTE_REAP (SDK `sys/event.h`). None of them is a stop note. NOTE_SIGNAL reports that a signal was posted, not that the target has stopped.
  - **Linux:** a pidfd reports only exit (EPOLLIN) and reap (EPOLLHUP). `waitid(WSTOPPED)` works only for the caller's own children. The only stop notification for a non-child is ptrace (`PTRACE_SEIZE`, then `waitpid` reports group-stop). It is rejected here. It makes the harness a tracer of every Hub member, which changes their stop/continue semantics: FreezeGuard's SIGCONT would no longer resume a traced group-stop without `PTRACE_LISTEN` handling. It also collides with any debugger. macOS has no counterpart, so the harness would split by platform.
  - **The Hub itself** is our child, so `waitid(P_PID, hub, WSTOPPED|WNOWAIT)` gives a real event for it, and that part must become an event wait. The os-no-event claim covers only the Hub's children, which are not our children.
  - The remaining bounded census loop keeps `WAIT_POLL` and `FREEZE_CONFIRM_BUDGET` unchanged. It is marked `timer: os-no-event — SIGSTOP taking effect on non-child processes: no stop note in kqueue EVFILT_PROC (macOS), no stop event in pidfd and waitid is children-only (Linux)`.
  - Unverified: XNU may apply SIGSTOP synchronously inside kill(2) (psignal, then task suspend). If so, the first census already confirms on macOS, and the loop body runs once there.
- **run-loaded-daemon-lifecycle-selftest 332 and 410. Reclassified DEFECT.** Both sites are inside `if [[ "$(runner_platform)" == Linux ]]` blocks (lines 279-341 and 343-422 at 23b0feaa), so they run only on Linux. They wait for an orphaned zombie to be reaped by init after its parent exits.
  - A pidfd reports the reap: per pidfd_open(2) it turns readable at exit and reports EPOLLHUP when the process is reaped.
  - Replacement: open a pidfd on each zombie pid while its parent is still alive, then signal the parent. This is a small Python helper, `os.pidfd_open` plus `select.poll`. Wait for POLLHUP with the existing 5 s budget (100 × 0.05 s) as a `timer: deadline`.
  - The tested claim (reap by init) is unchanged. No subreaper is needed.
  - Verify that the CI kernel reports EPOLLHUP at reap. If it does not, stop and escalate; do not fall back to a poll.
- **publish-npm-packages 144. Reclassified DEFECT; needs an orchestrator decision.** Registry visibility is not an OS fact, so os-no-event cannot apply.
  - Proposed replacement: remove the post-publish visibility poll. `npm publish` success is the receipt. Run one `npm view <pkg>@<version>` check that fails with a clear error and does not retry.
  - Tradeoff for the orchestrator: registry read-after-write lag can make that single check fail spuriously right after a successful publish. The alternative is to drop the check entirely.
- **Process-group empty** (run.rs 264, test-support lib.rs 7989, run-loaded-daemon-lifecycle 664/670) is DEFECT with no os-no-event part. It uses M2 (`src/process_exit.rs`; see the header).
  - macOS: atomic snapshot rounds with NOTE_EXIT per member.
  - Linux: pidfd POLLIN for running members and POLLHUP for zombies. The group is empty when `killpg(pgid, 0) == ESRCH`.
  - The shell script (run-loaded-daemon-lifecycle) needs a small helper binary or Python helper to reach these APIs.
- **Third-party zombie reaping** (process-census 87, run-loaded-daemon-lifecycle 411) has no os-no-event residue.
  - On Linux, a pidfd taken while the zombie exists reports its reap with EPOLLHUP.
  - Where these run on macOS, the wait must end on the owner's own reap receipt (the owner reaps its children). The zero-settle census then runs once. It must not wait for a third party's reap.

### DEFECT mechanism groups (for commit planning)

- **A. Direct-child exit polled with try_wait/poll()**
  - Replacement: a child-wait thread that sends to a channel, received with `recv_timeout(deadline)`. Alternatives are kqueue NOTE_EXIT or pidfd, or bash `wait` plus a watchdog.
  - In Python, do not use `Popen.wait(timeout)`: it polls with WNOHANG plus sleep. Use a blocking `wait()` on a thread that puts the result on a `queue.Queue`, then `get(timeout=remaining)`.
  - Sites: run.rs 92, 278; isolated_hub.rs 1626, 1635, 2282; hub_mcp_test.rs 132, 141; prove-north-star 324, 440, 472, 724; run-loaded-daemon-lifecycle 971.
- **B. Non-child pid exit polled with kill -0, ps or killpg(0)**
  - Replacement: kqueue EVFILT_PROC NOTE_EXIT on macOS or `pidfd_open` on Linux per pid, with a deadline.
  - Sites: run.rs 385, 435, 476; isolated_hub.rs 1471, 2240; test-support lib.rs 7979; update_command_test.rs 732; test-production-package-runtime 717; process-census 157, 170.
  - Group-empty variant (enumerate members, then per-pid events): run.rs 264; lib.rs 7989; run-loaded-daemon-lifecycle 664, 670.
- **C. Daemon or server readiness polled with Status, `status` CLI or connect retries**
  - No readiness signal exists today. `git grep` finds no ready-fd or notify mechanism in src.
  - Replacement: a new readiness pipe. The Hub writes one byte or line to an inherited fd after listen. The harness reads it with a deadline, and child exit shows up as EOF.
  - Sites: isolated_hub.rs 1607; hub_mcp_test.rs 115; update_command_test.rs 890; test-production-package-runtime 282, 474, 896; prove-north-star 131.
  - Same pattern for fixture servers (the server prints a ready line after bind): test-production-package-runtime 747.
- **D. Screen or terminal content polled with ReadScreen or ReadModeFlags plus sleep**
  - Replacement over a daemon socket: terminal route output frames on the already-attached route, via `poll_terminal(remaining)` against one deadline, then a single ReadScreen to confirm.
  - Replacement in-process (HubRuntime): block on the Core or owner wake the Hub loop already uses, then pump `observe_lifecycle_slice` once per wake instead of every 20 ms.
  - Sites: test-support lib.rs 883, 994, 1503, 1540, 1560, 4412 (extra); prove-north-star 200; hub_local_runtime_test.rs 361, 416; hub_runtime_test.rs 215; hub_client_api_test.rs 2975, 3022, 3611, 3745; hub_capability_runtime_test.rs 268.
- **E. Session state polled with ListSessions or Status**
  - Replacement: session entity subscription frames (`subscribe_session_entities`, Upsert/Patch/Remove) with a deadline.
  - Sites: test-support lib.rs 952 (quiet sessions exited); prove-north-star 178 (lifecycle) and 594 (attach occupancy; occupancy needs to be carried on an entity or event, so verify one exists); hub_runtime_test.rs 281 (resize, via a session entity patch or a resize completion).
  - isolated_hub.rs 1460 (worker exists after Spawn) should await the `OLD-READY` output frame on an attach, then run one census.
- **F. Package, app, update, capability or resource state polled**
  - Replacement: the owning event stream, or a new one where none exists.
  - prove-north-star 219 (list_apps local_url): needs an app or package entity subscription.
  - prove-north-star 239 (HTTP /health): publishing local_url should itself mean the server is listening. If it does not, the entrypoint needs a readiness event.
  - update_command_test.rs 904, 913 (GetHubUpdateExecution across a daemon restart): needs an update-execution event or subscription, plus the readiness pipe (C) for the new daemon.
  - hub_capability_runtime_test.rs 99, 124, 154 (drain_capability_events): needs a capability-completion wake on HubRuntime. Only a drain API exists (src/runtime.rs 1706).
  - test-support lib.rs 1970 (a 50 ms read slice plus Status "flush" requests): use one `next_event` read with `remaining` as the timeout.
  - probe-hub-resources 168 (counter convergence after connection churn): needs a Hub cleanup-complete event, or a barrier request that returns after pending disconnect cleanup.
  - hub_lua_runtime_test.rs 1937 (marker file written by the spawned command): wait for the session exit or lifecycle event, or the command's output frame.
- **G. File-publication polling (pid files, rendezvous files)**
  - Replacement: a pipe or FIFO handshake. Read a line with a deadline, and treat EOF as fixture death.
  - Sites: installer tests/support/mod.rs 395; installer src/inject.rs 143 (release file; expiry silently continues, which is not an error path); test-support lib.rs 7943; run-loaded-daemon-lifecycle-selftest 81, 298, 441, 487; process-census 196, 318; run-lifecycle-suite 173; run-loaded-daemon-lifecycle 775, 786, 808 (pgid/sid established after launch: have the launcher signal after setpgid/setsid).
  - test-support lib.rs 7137, 7171, 7194: the worker must publish from inside itself after exec. For 7171 and 7194, the descendant pid file has already been awaited and proves the worker shell is alive, so a single census would do.
- **H. Zombie-state polling**
  - Replacement: have the fixture parent observe the child's exit without reaping, then publish.
  - Prefer a blocking `waitid(P_PID, child, WEXITED|WNOWAIT)` in the parent. It returns once the child has exited, and the child stays a zombie because WNOWAIT does not reap.
  - Alternatively use a pidfd (Linux: EPOLLIN at exit) or kqueue NOTE_EXIT (macOS). NOTE_EXIT fires at exit, when the child is a zombie of its unreaped parent.
  - Revision 2: a pipe EOF is not acceptable. It proves only that the write end was closed, which can happen before exit, or be delayed past it by an inherited copy. It does not prove zombie state.
  - Sites: process-census 229, 272; run-loaded-daemon-lifecycle-selftest 308, 355, 378.
- **I. Fixed delays, race windows and negative assertions after a time window**
  - Replacement: a rendezvous channel, or a positive event proving the system passed the point.
  - isolated_hub.rs 1886: the hook sleeps 50 ms so that the test injects taint first. Replace with a hook that blocks on a resume channel the test sends to.
  - test-support lib.rs 7109: `time.sleep(2)` so the descendant appears after the snapshot. Gate the fork on a FIFO released by a seam after the snapshot.
  - hub-client lib.rs 5116: the writer is kept alive by a sleep so that `!is_finished()` holds. Loop until a stop channel instead.
  - hub_lua_runtime_test.rs 1314: 2 s before asserting no outcomes. Wait for the requeue or admission-refused event instead.
  - hub_client_api_test.rs 3043 (`observe_for(200ms)`, called at 3407): a settle before shutdown.
  - hub_capability_runtime_test.rs 188: the HTTP server delay models an in-flight request. Gate the response on a channel released after hot-path responsiveness is proven.
  - probe-hub-resources 232 and test-production-package-runtime 590: idle windows for wake-rate measurement. **Resolved by ruling 3.** Both are measurement-window, with values unchanged.
- **J. Spin waits**
  - core_ticket_allocations/main.rs 278 (yield until START_WORKER): replace with a Barrier or park/unpark.
  - core_ticket_allocations/main.rs 291 (yield until `is_finished`, then join): plain `join()` blocks and is enough.
  - Allocation-capture constraint: verify on the pinned std that park, unpark and join do not allocate while RECORDING is set.
- **K. Disconnect-cleanup retry**
  - test-support lib.rs 479: re-subscribe with the same id until the Hub frees it.
  - No client-visible event exists. Needs a Hub subscription-closed or connection-cleanup event, or an explicit unsubscribe ack before drop (the test is proving disconnect cleanup, so the event must come from the Hub).
- **L. Zombie settle windows**
  - run-loaded-daemon-lifecycle 411 and process-census 87: they stand in for the owner's cleanup-complete receipt. Once the owner reaps its own children, run a zero-settle census. Third-party reaping is not os-no-event either: a pidfd reports the reap on Linux (see the os-no-event notes above).
  - process-census 157 and 170 (live processes) belong in B.

---

### crates/botster-hub-installer/src/run.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 92 | `run_bounded` try_wait loop + sleep(POLL_INTERVAL) | DEFECT | polls the direct child's exit every 20 ms | child-wait thread -> channel `recv_timeout(deadline)` (or kqueue NOTE_EXIT / pidfd on the child) |
| 224 | `rx.recv_timeout(DRAIN_TIMEOUT)` | deadline | the drain thread sends at EOF; expiry is reported as an error | - |
| 264 | `wait_for_group_exit` killpg(0) poll | DEFECT | polls for the process group to empty | M2 (`src/process_exit.rs`): macOS snapshot rounds with NOTE_EXIT; Linux pidfd POLLIN/POLLHUP with `killpg(0)==ESRCH` as empty; caller's deadline |
| 278 | `terminate_group` try_wait + group poll | DEFECT | polls leader reap and group emptiness during TERM grace | child-wait channel for the leader + per-member NOTE_EXIT, grace as deadline |
| 367, 417, 459 | `sleep 60` in test child scripts | DEFECT (was NOT-TIMER) | fixture sleep keeps a descendant alive for the code under test to kill | blocking read the test ends: `cat` on a FIFO the test closes (the test may also leave it open, since the code under test kills the descendant) |
| 385, 435, 476 | test loop `kill(descendant,0)` + sleep | DEFECT | polls a non-child descendant's exit; run_bounded already swept the group, so an immediate assert may suffice | kqueue NOTE_EXIT / pidfd on the descendant pid with deadline |

### crates/botster-hub-installer/tests/support/mod.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 71 | accepted stream `set_read_timeout(5s)` | deadline | bounds the read of the expected HTTP request | - |
| 395 | `install_while_held` loop: `reached` file exists / try_wait + sleep 10ms | DEFECT | polls a rendezvous file and child exit | installer writes "reached" to an inherited pipe/FIFO; parent blocking read with deadline (EOF = child died) |

### crates/botster-hub-installer/src/inject.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 143 | `hold()` loop on `release.exists()` + sleep 20ms | DEFECT | the installer polls for a release file; expiry silently continues (not an error) | blocking read on a FIFO/inherited pipe with HOLD_LIMIT deadline; expiry -> error |

### crates/botster-hub-test-support/src/isolated_hub.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 1460 | `wait_for_owned_workers` census poll | DEFECT | polls ps until a worker appears after Spawn | attach the session and await the `OLD-READY` output frame (deadline), then one census |
| 1471 | `assert_process_gone` process_pgid poll | DEFECT | polls pid disappearance (old Hub was reaped by restart; workers are non-children) | NOTE_EXIT / pidfd per pid with deadline; a reaped child needs no wait |
| 1506 | session command `printf OLD-READY; sleep 60` | DEFECT (was NOT-TIMER) | fixture sleep keeps the session alive | `printf OLD-READY; cat` on stdin or a FIFO the test closes |
| 1607 | `wait_for_ready` Status request + try_wait + sleep 50ms | DEFECT | readiness by polling Status | new Hub readiness pipe/fd written after listen; read with deadline, EOF = exit |
| 1626, 1635 | `cleanup_child` try_wait + group poll in TERM/KILL phases | DEFECT | polls child reap and group emptiness | child-wait channel + per-member NOTE_EXIT; phases as deadlines |
| 1886 | `run_after_taint_check_hook` sleep 50ms after `matched.send` | DEFECT | race-window sleep so the test thread injects taint before start continues | hook blocks on a `resume` channel `recv_timeout` that the test sends after injecting |
| 2187 | `freeze_confirm_snapshot` census loop + sleep(WAIT_POLL) | os-no-event (candidate) | waits for SIGSTOP to stop all non-zombie group members. Stop of a non-child has no event on macOS (EVFILT_PROC has no stop note; NOTE_SIGNAL is posting only) or on Linux (pidfd reports exit/reap only; `waitid(WSTOPPED)` is children-only; ptrace rejected, see the facts) | the Hub itself (our child): blocking `waitid(WSTOPPED\|WNOWAIT)` first; then the bounded census for the members, with values unchanged, marked os-no-event |
| 2240 | `reap_owned_session_workers` census/signal loop + sleep(REAP_POLL) | DEFECT | polls for captured workers/descendants to exit | register NOTE_EXIT / pidfd on the captured set, re-census once for the fork race, SIGKILL escalation on the deadline |
| 2282 | `wait_child_bounded` try_wait + sleep | DEFECT | polls direct child exit | child-wait thread -> channel `recv_timeout` |

### crates/botster-hub-test-support/src/lib.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 257, 287, 498, 3085 | subscription `set_read_timeout(5s)` before `next_frame` | deadline | bounds the read of an expected frame | - |
| 479 | re-subscribe retry loop + sleep 20ms | DEFECT | retries until the Hub frees the dropped connection's subscription id | Hub-emitted connection-cleanup / subscription-closed event (none exists; K) |
| 883 | ReadScreen poll for MANY_PTY_LIVE_MARKER | DEFECT | polls the screen after input | terminal route output frames with deadline, then one ReadScreen |
| 952 | `wait_for_quiet_sessions` ListSessions poll | DEFECT | polls lifecycle=exited | session entity subscription exit/upsert frames with deadline |
| 994 | `wait_for_many_pty_screen_marker` ReadScreen poll | DEFECT | polls the screen for the history marker | terminal route output frames with deadline |
| 1503 | attach loop: frames + ReadScreen + sleep 25ms | DEFECT | polls for attached frame + READY text | block on route frames (`poll_terminal(remaining)`), one ReadScreen |
| 1540, 1560 | echo / resize ReadScreen polls + sleep 25ms | DEFECT | polls the screen for echo and winsize text | route output frames with deadline |
| 1970 | `set_read_timeout(min(remaining,50ms))` + Status "flush" loop | DEFECT | short read slice that re-issues Status each turn | single `next_event` with read timeout = remaining deadline |
| 6890 | fake hub `while :; do sleep 1; done` | DEFECT (was NOT-TIMER) | fixture keep-alive sleep (never-ready hung process) | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 7067, 7091, 7114 | python `time.sleep(60)` descendant | DEFECT (was NOT-TIMER) | fixture sleep keeps a descendant alive | `sys.stdin.read()` or a read on a FIFO the test closes |
| 7109 | python `time.sleep(2)` before fork | DEFECT | fixed delay so the descendant appears after the early snapshot (skip_freeze test 7513) | gate the fork on a FIFO released by a seam after the snapshot |
| 7137, 7171, 7194 | loop until `owned_session_worker_pids()` non-empty + sleep 20ms | DEFECT | census poll for worker visibility | worker publishes readiness from inside itself after exec (pipe); 7171/7194 already awaited the descendant pid, so one census suffices |
| 7255 | shutdown script `dd ...; sleep 60` | DEFECT (was NOT-TIMER) | fixture sleep models a stalled command (the tested path is the deadline) | `dd ...; cat` on a FIFO the test never writes and closes at teardown; the stall is still unbounded from the Hub's view |
| 7278 | test name `hub_child_wait_timeout_...` | NOT-TIMER | identifier only | - |
| 7710, 7762 | boundary `matched`/`foreign` `.recv_timeout(2s)` | deadline | channel event with bounded give-up; 7765 `try_recv` negative is proven by the foreign event | - |
| 7943 | `wait_for_fake_pid_within` file poll + sleep 10ms | DEFECT | polls a pid file | fixture writes pid to a pipe/FIFO; read with deadline |
| 7979 | `assert_process_exits` kill -0 poll + sleep 25ms | DEFECT | polls non-child pid exit | NOTE_EXIT / pidfd with deadline |
| 7989 | `assert_process_group_exits` killpg(0) poll | DEFECT | polls group emptiness | M2 (`src/process_exit.rs`), caller's deadline |

### crates/botster-hub-client/src/lib.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 1004 | `poll_frame` set_read_timeout(timeout) | deadline | primitive: one frame read bounded by the caller's timeout (callers are classified at their own sites) | - |
| 1017, 1246 | restore previous read timeout | deadline (was NOT-TIMER) | part of the `poll_frame` / `poll_terminal` deadline primitive: restores the caller's bound; marker goes with the primitive | - |
| 1100, 1268 | `pub fn set_read_timeout` signatures | NOT-TIMER | function definition (identifier, not a call) | - |
| 1102, 1269 | forwarding `set_read_timeout(timeout)` calls | deadline (was NOT-TIMER) | the primitive forwarder that applies the caller's bound; callers are classified at their own sites | - |
| 1210 | `poll_terminal` read timeout = remaining absolute deadline | deadline | blocks on frames; skipped events do not restart the deadline | - |
| 1479, 1484 | handshake write/read deadlines | deadline | bounds the Hello/HelloAck exchange | - |
| 1488, 1489 | restore handshake timeouts | deadline (was NOT-TIMER) | part of the `with_handshake_deadlines` primitive (1479/1484) | - |
| 1508, 1511, 1534, 1537 | test sets 80/90 ms timeouts to check restoration | NOT-TIMER | sentinel configuration values that `read_timeout()`/`write_timeout()` read back. No I/O blocks under them: the handshake op runs under its own 20-40 ms bounds, and the 80/90 ms values are only compared. A configuration, not a wait | - |
| 5114 | test writer thread: event every 5 ms for 600 ms | rate-limit | deliberate pacing of a continuous event stream (the loop is also bounded at 600 ms) | - |
| 5116 | writer `sleep(250ms)` to stay alive for `!is_finished()` | DEFECT | timing keeps the thread alive for an assertion | writer loops until a stop channel; test asserts, then signals stop |
| 8248 | socketpair `set_read_timeout(30ms)` | deadline | expiry on a partial frame is the tested path | - |

### crates/botster-hub-client/examples/harness_control.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 126 | fixture `set_read_timeout(3s)` | deadline | bounds the read of the expected Hello | - |

### script/probe-hub-resources

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 168 | `wait_for_convergence` status poll + sleep(0.02) | DEFECT | polls counters until disconnect cleanup converges | Hub cleanup-complete event / barrier request (F) |
| 232 | `sleep(0.25)` idle window, then wake-counter delta | measurement-window (was DEFECT) | resource probe: the idle wake rate over this interval is the metric (ruling 3). The value is unchanged | marker `timer: measurement-window — idle Hub wake-counter rate` |

### script/prove-north-star-shared-session

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 131 | `wait_for_socket` connect retry + sleep 0.1 | DEFECT | readiness by connect polling | Hub readiness pipe (C) |
| 178 | `wait_for_session_lifecycle` list_sessions poll | DEFECT | polls lifecycle | session entity subscription over a persistent adapter connection |
| 200 | `wait_for_history` read_screen poll | DEFECT | polls the screen | attach route output frames |
| 219 | `wait_for_package_app_url` list_apps poll | DEFECT | polls app launch_target | app/package entity subscription (verify it exists) |
| 239 | `wait_for_http_ok` /health poll | DEFECT | polls HTTP readiness | local_url publication should imply listening; else an entrypoint readiness event |
| 324 | `run_*` readline loop: after EOF `poll()` + sleep 0.05 | DEFECT | polls child exit after stdout EOF; blocking readline also defeats the deadline | selectors on stdout with deadline; after EOF, a blocking `process.wait()` on a thread feeding a `queue.Queue`, `get(timeout=remaining)` (not `wait(timeout=)`, which polls) |
| 440 | `wait_for_line` EOF branch sleep 0.05 | DEFECT | same pattern | same |
| 472 | `wait_exit` EOF branch sleep 0.05 | DEFECT | same pattern | blocking `process.wait()` on a thread + queue, `get(timeout=remaining)` |
| 594 | `wait_for_empty_occupancy` status poll | DEFECT | polls live_attach_occupancy | occupancy change carried on a session entity / detach event |
| 724 | `hub.poll()` loop + sleep 0.1 after shutdown | DEFECT | polls direct child exit | blocking `hub.wait()` on a thread + queue, `get(timeout=8)` (8 s unchanged; not `hub.wait(timeout=8)`, which polls) |

### script/run-loaded-daemon-lifecycle

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 411 | `capture_settled_zombie_survivors` settle loop | DEFECT | settle window stands in for the owner's cleanup receipt; no os-no-event residue (L: third-party reap is reported by pidfd EPOLLHUP on Linux) | zero-settle census after the owner's reap receipt; for a third-party reap, a pidfd POLLHUP wait bounded by ZOMBIE_SETTLE_SECONDS (unchanged) |
| 664 | TERM grace `group_is_alive` + sleep 1 | DEFECT | polls group emptiness | per-member NOTE_EXIT / pidfd with grace deadline |
| 670 | `sleep 1` after KILL | DEFECT | fixed settle | `wait $pid` + per-member exit events |
| 775 | `record_group` pgid poll | DEFECT | polls until the launched process setpgid's itself | launcher signals on a pipe/FIFO after setpgid, before exec |
| 786 | Darwin member-count poll | DEFECT | polls until the test tree forks | launched tree signals readiness |
| 808 | `record_session` sid poll | DEFECT | polls until setsid | launcher signals after setsid |
| 895 | sampler `sleep 5` loop | rate-limit | deliberate periodic sampling | - |
| 971 | `direct_child_is_running` + sleep 0.1 until timeout | DEFECT | polls direct child exit | `wait $TEST_PID` with a background watchdog (`sleep T; kill`) as the deadline |

### script/run-loaded-daemon-lifecycle-selftest

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 75, 292 | python `time.sleep(300)` parent | DEFECT (was NOT-TIMER) | fixture sleep keeps the zombie's parent alive (and the zombie unreaped) | `sys.stdin.read()` or a read on a FIFO the test closes; the test's `kill -TERM` still ends it |
| 81, 298 | pid-file poll + sleep 0.05 | DEFECT | polls file publication | FIFO/pipe handshake (G) |
| 308, 355, 378 | poll census until zombie row appears | DEFECT | waits for the child to become a zombie | parent blocks in `waitid(P_PID, child, WEXITED\|WNOWAIT)` before publishing the pid (H); pipe EOF does not prove zombie state |
| 332, 410 | poll `kill -0` until zombie reaped after parent exit | DEFECT (was os-no-event) | Linux-only (inside `runner_platform == Linux` blocks). A pidfd reports the reap with EPOLLHUP (pidfd_open(2)), so an event exists | open a pidfd on each zombie pid before signalling its parent (Python `os.pidfd_open` + `select.poll`), wait for POLLHUP with the existing 5 s budget (100 × 0.05 s) as `timer: deadline`; the tested claim (reap by init) is unchanged |
| 428, 460, 469 | `setsid sleep 300` fixtures | DEFECT (was NOT-TIMER) | fixture sleeps keep escaped setsid processes alive | `setsid cat` on a FIFO the test closes (stdin of a setsid'd background process is not usable) |
| 441 | poll run-token rows for escaped setsid child | DEFECT | polls visibility after fork/setsid | child signals on a FIFO after setsid |
| 487 | poll until 85 workers + nested pgid visible | DEFECT | polls fixture readiness | inner bash writes a ready line to a FIFO after spawning, then one census |

### script/process-census

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 87 | `assert_no_new_zombies` settle loop | DEFECT | settle for zombie reaping; no os-no-event residue (L) | owner cleanup receipt, then zero-settle census; a third-party reap uses a pidfd POLLHUP wait (Linux) bounded by `settle_seconds` (unchanged) |
| 157 | `assert_no_dev_artifacts` settle loop | DEFECT | polls for live processes to exit | NOTE_EXIT / pidfd on captured pids, or owner cleanup receipt |
| 170 | `assert_no_live_executables` settle loop | DEFECT | same | same |
| 196 | self-test poll until marker visible | DEFECT | polls exec visibility | marker writes a ready line to a FIFO, then one census |
| 213, 256 | ruby `sleep(30)` parent | DEFECT (was NOT-TIMER) | fixture sleep keeps the zombie holder alive | `$stdin.read` or a read on a FIFO the test closes |
| 229, 272 | poll until child is a zombie | DEFECT | waits for child exit state | parent blocks in `waitid(P_PID, child, WEXITED\|WNOWAIT)` (Ruby: via Fiddle/`syscall`, or a tiny C/Python helper), then publishes (H); pipe EOF does not prove zombie state |
| 318 | poll until dev-artifact workers visible | DEFECT | polls exec visibility | ready-line handshake |

### script/run-lifecycle-suite

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 173 | self-test poll until fixture visible to census | DEFECT | polls exec visibility | fixture ready-line FIFO |

### script/test-production-package-runtime

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 282 | poll for runtime metadata file | DEFECT | polls file publication | readiness pipe / stdout line from `up` (C) |
| 474 | `wait_for_status` poll | DEFECT | readiness by status polling | readiness pipe (C) |
| 590 | `sleep 5` idle window before probe | measurement-window (was DEFECT) | the idle interval is the measured quantity (ruling 3). The value is unchanged | marker `timer: measurement-window — idle Hub CPU/wake rate before the probe` |
| 717 | `assert_failed_up_cleanup` socket/pid poll | DEFECT | polls non-child exit and socket removal | failed `up` exit (already waited) should be the cleanup receipt; else NOTE_EXIT on the pid |
| 747 | poll TCP connect to the fixture server | DEFECT | polls bind readiness | ruby server prints a ready line after bind; read with deadline |
| 896 | pre-cutover `status` poll | DEFECT | readiness polling (old binary; may need a ready line from the old Hub) | readiness pipe |

### script/publish-npm-packages

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 144 | `sleep 5` x12 visibility poll | DEFECT (was os-no-event) | registry visibility is not an OS fact, so os-no-event cannot apply; this is a poll for external state | **needs orchestrator decision.** Proposed: remove the post-publish visibility poll; `npm publish` success is the receipt; one `npm view` check that fails with a clear error, no retry loop |

### script/measure-processes.c, script/test-measure-processes/stable-unit.c

| line | site | category | reason | replacement |
|---|---|---|---|---|
| measure 460 | `kevent(..., &zero)` drain | NOT-TIMER (re-examined) | this is the kqueue event API itself, with a zero timeout: a non-blocking drain of already-queued events. It is not a timer and not a wait | - |
| measure 502 | `kevent` EV_RECEIPT registration, zero timeout | NOT-TIMER (re-examined) | event-source registration, no wait | - |
| stable-unit 1, 12 | `#define kevent` test shim | NOT-TIMER | shim definition | - |

### script/test-harness-control.py

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 34, 41 | FAKE adapter `time.sleep(60)` | DEFECT (was NOT-TIMER) | fixture sleep models a hung adapter for the deadline path | `sys.stdin.read()` (or a FIFO the test closes); still hung from the caller's view |
| 77-79 (extra, not in grep) | `subprocess.run(..., timeout=8)` | DEFECT | `run(timeout=)` waits through `communicate(timeout)`/`wait(timeout)`, which poll (WNOHANG + sleep) | `Popen` + blocking `communicate()` on a thread feeding a `queue.Queue`; `get(timeout=8)` (value unchanged); on expiry kill and fail |

### tests/core_ticket_allocations/main.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 278 | worker `while !START_WORKER { yield_now }` | DEFECT | spin wait on another thread's flag | Barrier or park/unpark (verify no allocation on pinned std) |
| 291 | `while !worker.is_finished() { yield_now }` | DEFECT | spin wait before join | plain `join()` |

### tests/hub_lua_runtime_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 1314 | sleep to 2 s, then assert `outcomes.is_empty()` | DEFECT | fixed delay before a negative assertion | positive requeue / admission-refused event from the router, then assert |
| 1937 | marker-file poll x100 | DEFECT | polls a file written by the spawned command | session exit/lifecycle event or output frame |

### tests/hub_local_runtime_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 361, 416 | ReadScreen + observe_lifecycle_slice pump + sleep 20ms | DEFECT | polls the screen | Core/owner wake -> pump once per wake, deadline |

### tests/hub_runtime_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 215 | `drain_until` ReadScreen poll | DEFECT | polls the screen | Core/owner wake |
| 281 | list_sessions poll for rows=30/cols=100 | DEFECT | polls resize application | resize completion / session patch event |

### tests/hub_mcp_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 115 | `wait_for_status` CLI poll + try_wait | DEFECT | readiness polling | readiness pipe (C) |
| 132, 141 | `terminate_daemon_group` try_wait + sleep | DEFECT | polls direct child exit | child-wait channel with TERM/KILL deadlines |

### tests/hub_client_api_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 2975 | `drain_until` ReadScreen poll | DEFECT | polls the screen | Core/owner wake |
| 3022 | `read_screen_until` poll | DEFECT | same | same |
| 3043 | `observe_for(duration)` pump + sleep (caller 3407: 200 ms before shutdown) | DEFECT | settle window | positive event (output observed / cursor caught up), then proceed |
| 3578, 3670 | session `...; sleep 5` | DEFECT (was NOT-TIMER) | fixture sleep sets the session lifetime | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| 3611 | ReadScreen poll for `screen-ready` | DEFECT | polls the screen | Core/owner wake |
| 3745 | ReadModeFlags poll for mouse_mode 9 | DEFECT | polls mode flags | Core/owner wake (output processed), then one read |

### tests/update_command_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 732 | `wait_for_process_exit` kill -0 poll | DEFECT | polls non-child worker exit | NOTE_EXIT / pidfd |
| 890 | `wait_for_status` poll | DEFECT | readiness polling | readiness pipe (C) |
| 904 | request-failure retry + sleep 25ms | DEFECT | retries while the daemon restarts | readiness pipe for the new daemon, then one request |
| 913 | GetHubUpdateExecution state poll | DEFECT | polls update state | update-execution event/subscription (F) |

### tests/hub_capability_runtime_test.rs

| line | site | category | reason | replacement |
|---|---|---|---|---|
| 99, 124, 154 | `drain_until_*` capability events + sleep 10ms | DEFECT | polls capability completion | capability-completion wake on HubRuntime (none exposed; only drain) |
| 188 | LocalHttpServer `thread::sleep(delay)` (500 ms at 664, 873) | DEFECT | fixed delay models an in-flight request; the hot-path claim races it | server gated on a release channel sent after the hot-path check |
| 268 | `drain_session_until` ReadScreen poll | DEFECT | polls the screen | Core/owner wake |

---

### Extras not in the grep

| file:line | site | category | reason | replacement |
|---|---|---|---|---|
| crates/botster-hub-test-support/src/lib.rs:4412 | `take_attached_terminal_frame` `poll_terminal(25ms)` slices inside the 1503 poll loop (extra, not in grep) | DEFECT | short read slice used to interleave ReadScreen polling | one `poll_terminal(remaining)` against the attach deadline |
| tests/update_command_test.rs:745 | `collect_attach_frames` `poll_terminal(25ms)` in a deadline loop (extra, not in grep) | DEFECT | short slice re-checking only the deadline; `Err` is swallowed, so a persistent error hot-spins | `poll_terminal(remaining)`; propagate errors |
| crates/botster-hub-test-support/src/unix_route.rs:450 | `poll_route_events(timeout)` | deadline | primitive; its 20-100 ms callers are in tests/hub_daemon_lifecycle (another group) | - |
| crates/botster-hub-installer/src/fetch.rs:33-36 | ureq global/connect/recv timeouts | deadline | bounded HTTP give-up | - |
| script/probe-hub-resources:27, 36 | `Timeout.timeout` around adapter reads | deadline | bounded read of expected line | - |
| script/test-measure-processes/stable_checks.py:18 | `select.select(..., remaining)` | deadline | event read with deadline | - |
| script/measure-processes.c:744, 873 | `nanosleep(interval)` between samples | rate-limit | `--interval-ms` sampling cadence | - |
| script/run-loaded-daemon-lifecycle:865 | `bash -c "while :; do :; done"` | NOT-TIMER | CPU-load generator, no wait | - |
| script/test-production-package-runtime:780 | explicit-port retry loop (no sleep) | NOT-TIMER | retry after a bind failure; no timer and no state wait | - |
| tests/hub_lua_runtime_test.rs:33-39, 3858-3861, 3985-3991, 4030-4036, 4063-4069 | `while *_ops_pending { apply_event_plane_owner_ops() }` + deadline assert | NOT-TIMER | the test is the owner; each turn applies queued work (verify pending cannot stay true with nothing runnable, or it becomes a hot spin) | - |
| tests/hub_lua_runtime_test.rs:4129-4381 | `try_recv().is_err()` after `test_fulfill_pending_publishes` | NOT-TIMER | deterministic synchronous stepping | - |
| tests/hub_lua_runtime_test.rs:2471 | Lua `while true do timer_once(1) end` | NOT-TIMER | runaway-budget fixture | - |
| tests/hub_capability_runtime_test.rs:748-1115 | plugin Timer capability requests, `drain_capability_events_at(now_ms)` | NOT-TIMER | product timer feature under test on a logical clock | - |
| fixture lifetimes: test-support lib.rs 6947, 6982, 7257, 7417, 7431, 7459, 7533, 7543, 7559, 7595, 7621, 7642, 7662, 7688, 7743; hub_client_api_test.rs 755, 3667; update_command_test.rs 435, 509; process-census 191, 294-297; run-lifecycle-suite 158 (22 sites) | `sleep 30/60/120` or `sleep 5` commands | DEFECT (extra, not in grep; was NOT-TIMER) | fixture sleeps set process/session lifetimes | blocking read the test ends: `cat` on stdin or a FIFO the test closes |
| script/process-census:211, 254 | zombie child `exec sleep 0.01` | DEFECT (extra, not in grep; was NOT-TIMER) | fixture delay: the child sleeps briefly, then exits to become a zombie | `exec cat` on a FIFO the test closes: the zombie transition then happens when the test says, and the parent observes it with blocking `waitid(WEXITED\|WNOWAIT)` (229/272) |

The remaining extras marked NOT-TIMER above were re-examined in revision 2, and they stay NOT-TIMER because none of them waits on a timer call:
- run-loaded-daemon-lifecycle:865 is a CPU-load generator with no timer.
- test-production-package-runtime:780 is a bind retry with no timer and no state wait.
- hub_lua_runtime_test.rs 33-39 etc. and 4129-4381 are same-thread owner stepping.
- hub_lua_runtime_test.rs:2471 and hub_capability_runtime_test.rs:748-1115 exercise the product Timer capability on a logical clock or budget. They are the feature under test, not waits.

### Python library timeouts (revision 2; extra, not in grep)

Ruling 6 applies to all of these. `Popen.wait(timeout)`, `subprocess.run(timeout=)` and `communicate(timeout=)` poll internally (WNOHANG plus sleep), so they are DEFECT even though they read like deadlines.

The common replacement: call `Popen` and run a blocking `wait()` or `communicate()` on a thread that puts the result on a `queue.Queue`. The caller does `get(timeout=<same value>)`, marked `timer: deadline`. On expiry the caller kills the child and fails. Pidfd or kqueue is an alternative where one is already in use. Values are unchanged. test-harness-control.py:77-79 is listed in its own table above.

| file:line | site | category | reason | replacement |
|---|---|---|---|---|
| script/core-ticket-allocation-oracle:73 | `wait_bounded`: `process.wait(timeout=seconds)` then killpg | DEFECT | `wait(timeout)` polls | wait thread + queue, `get(timeout=seconds)`; killpg on expiry |
| script/prove-north-star-shared-session:144-149 | `subprocess.run(adapter request, timeout=30)` | DEFECT | `run(timeout=)` polls | Popen + `communicate()` thread + queue, `get(timeout=30)` |
| script/prove-north-star-shared-session:301, 481, 728 | `process.wait(timeout=5)` after SIGTERM, then kill | DEFECT | `wait(timeout)` polls | wait thread + queue, `get(timeout=5)`, then SIGKILL |
| script/test-build-dev-artifacts.py:89-91 | `subprocess.run(..., timeout=15)` | DEFECT | `run(timeout=)` polls | Popen + `communicate()` thread + queue, `get(timeout=15)` |
| script/test-measure-processes/run.py:46-47, 50, 62, 67, 73, 89-91 | `subprocess.run(..., timeout=10/30)` (compile, unit, native, stable-unit, invalid-args, sampler) | DEFECT | `run(timeout=)` polls | one shared helper: Popen + `communicate()` thread + queue, `get(timeout=<same>)` |
| script/test-measure-processes/run.py:83 | fixture string: `child.wait(timeout=10)` | DEFECT | `wait(timeout)` polls, inside the fixture | plain blocking `child.wait()`: the child exits when its stdin closes, and the test's own deadline bounds the fixture |
| script/test-measure-processes/run.py:124 | `owned.wait(timeout=15)` in `finally` | DEFECT | `wait(timeout)` polls | wait thread + queue, `get(timeout=15)` |
| script/test-measure-processes/stable_checks.py:55, 112, 115 | `sampler.wait(timeout=10)`, `owned.wait(timeout=10)` | DEFECT | `wait(timeout)` polls | wait thread + queue, `get(timeout=10)` |

These are not in scope for ruling 6: `script/probe-hub-resources:27,36` (Ruby `Timeout.timeout`, a watchdog thread around one blocking read; it stays a deadline), `stable_checks.py:18` (`select.select(remaining)`, a kernel wait; deadline), and prove-north-star `urlopen(timeout=2)` (a socket timeout inside the 239 poll row, which is already DEFECT).

---

# Sites added between 23b0feaa and 20b201bd

Scope: `git diff 23b0feaa..20b201bd` (origin/main 20b201bd, which contains a69b70cc).
- Grep patterns: `sleep(`, shell `sleep N`, `recv_timeout|wait_timeout|park_timeout`, `set_read_timeout|set_write_timeout`, `setTimeout|setInterval|waitForTimeout`, `time::timeout|timeout_at|interval(`, `yield_now|spin_loop`, plus bare `timeout(`.
- Every added or changed line that matched is listed, plus the poll-shaped extras found by reading the new tests.
- a69b70cc..20b201bd (0a1842a2, ff8b22bb, 20b201bd) added no timer lines. So the new sites all come from 004391dc..a69b70cc, and their line numbers are the same at a69b70cc and at 20b201bd.
- **Line numbers in this section are at 20b201bd. Every other line number in this document is at 23b0feaa.**

## New sites

| file:line (20b201bd) | site | category | reason | replacement |
|---|---|---|---|---|
| src/transport/webrtc/peer.rs:4337 | `mod tests`, duplicate-channel overlap test (a69b70cc): `try_receive_owner_message` Empty => `thread::sleep(5ms)` until both `BindReservedSubscription` messages are held | DEFECT | 5 ms receive poll of the owner control channel (C mechanism 1) | blocking `control_rx.recv()` under the existing 10 s deadline; hold `BindReservedSubscription` messages, handle the others |
| src/transport/webrtc/peer.rs:4357-4360 (extra, not in grep) | `let settle = now + 1s; pump_until(settle + 1s, "the settle window", \|_\| Instant::now() >= settle)` | DEFECT | a one-second settle window ("let every Core completion of both binds apply") before asserting that exactly one channel was acknowledged, which is a negative claim about the other channel | wait for the positive events: both binds' Core completions applied (no bind pending in `pending_runtime` for the subscription) and the losing channel's host finished or rejected; then assert once. With mechanism 1 this is a blocking control recv, not `pump_until` |
| src/transport/webrtc/test_support.rs:518 | `drain_host_events(key, quiet)`: collect host events until none arrives for `quiet` | DEFECT | quiet window stands in for "no more events". Callers: peer.rs 4004, 4021, 4029, 4050, 4089, each with 500 ms, asserting "exactly one signal at expiry" | positive barrier: trigger a later observable host event on the same peer (or a request whose response is ordered after the events), read up to it, and count the events before it |
| src/transport/webrtc/test_support.rs:1246 (caller of 518) | sync `drain_host_events` wrapper that `block_on`s the quiet window | DEFECT | same | same |
| src/transport/webrtc/test_support.rs:681 | `timeout(10s, open_rx.recv())` in `open_reserved_expecting_close` | deadline | channel open is the event; expiry panics | - |
| src/transport/webrtc/test_support.rs:692 | `timeout(10s, message_rx.recv())` | deadline | HelloAck or close is the event; expiry panics | - |
| src/transport/webrtc/test_support.rs:1266 | `try_receive_owner_message` Empty => `sleep(5ms)` while waiting for the offer response | DEFECT | owner control-channel poll (mechanism 1) | blocking `control_rx.recv()` under a deadline, selected with `response_rx` |
| src/transport/webrtc/test_support.rs:1545 | `open_reserved_expecting_reject`: Empty => `sleep(5ms)` | DEFECT | owner control-channel poll (mechanism 1) | blocking recv under the existing deadline |
| src/transport/webrtc/test_support.rs:1913 | `pump_until(deadline, what, done)` helper: Empty => `sleep(5ms)` | DEFECT | new generic poll helper (mechanism 1). Callers: peer.rs 4171, 4236, 4245, 4351, 4358 | rebuild as a blocking `control_rx.recv()` under the deadline, re-checking `done` after each handled message |
| tests/hub_daemon_lifecycle/paste_transaction.rs:76 | `paste_sink_command_after_go`: `...; printf '{done}'; sleep 30` | DEFECT | fixture sleep (same as 23b0feaa:66) | blocking read the test ends: `cat` on a FIFO the test closes (stdin is the sink) |
| tests/hub_daemon_lifecycle/webrtc_proofs.rs:1698 | `sleep(Duration::from_secs(2)).await` then `assert!(held_open >= 2s)` in `local_webrtc_attach_to_a_flooding_session_binds_after_a_slow_channel_open` | DEFECT | a fixed 2 s stands in for "the session flooded output while the channel was slow to open" | wait for a positive event: an observer route on a Unix connection has received N flood lines, or a Hub output/backpressure counter crossed the tested bound. If the claim is about the reservation surviving elapsed time, inject the reservation clock (as peer.rs 1683) |
| tests/hub_daemon_lifecycle/webrtc_proofs.rs:1735 | `timeout(200ms, next_terminal_frame)` inside a 20 s deadline loop | DEFECT (slice) | the slice only re-checks the deadline | `timeout(deadline - now, next_terminal_frame)` |

## Moved and removed sites

- **subscription_ownership_baseline.rs:168 → 170.** The line was rewritten to `IFS= read -r go; printf 'so-4cls-ready\n'; sleep 30`, and it is still a fixture sleep. The row at 168 above covers it (DEFECT).
- **Removed: tests/hub_lua_runtime_test.rs:1937** (marker-file poll, group D). 004391dc moved that coverage to `tests/hub_daemon_lifecycle/packages.rs:5035` (20b201bd), which uses `wait_for_managed_git_session_exit`. That helper is common.rs:847, already DEFECT (D1). The removal also deletes the 1937 row's replacement work.

## Line numbers moved in files changed since 23b0feaa (23b0feaa → 20b201bd)

These are only the grep-hit lines of the files the diff touched. Files not listed have unchanged line numbers.

- crates/botster-hub-client/src/lib.rs: 1004→1002, 1017→1015, 1100→1098, 1102→1100, 1210→1208, 1246→1244, 1268→1266, 1269→1267, 1479→1477, 1484→1482, 1488→1486, 1489→1487, 1508→1506, 1511→1509, 1534→1532, 1537→1535, 8248→8267
- src/daemon.rs: 861→866
- src/daemon/control/sessions.rs: 2086→2098, 2524→2536, 2527→2539, 2544→2556, 2659→2671, 2668→2680, 2810→2822, 2827→2839, 3059→3071, 3062→3074, 3316→3328, 3350→3362, 3607→3624, 3697→3714, 3825→3842, 3862→3879, 4146→4163, 4148→4165, 4240→4257, 4471→4488, 4524→4541, 4537→4554, 4651→4668, 4671→4688, 4691→4708, 4794→4811, 4810→4827, 4857→4874, 4874→4891, 4914→4931, 4934→4951, 4983→5000, 5044→5061, 5088→5105, 5118→5135, 5167→5184, 5200→5217, 5228→5245, 5238→5255, 5256→5273, 5357→5374, 5381→5398
- src/daemon/owner_loop.rs: every line from 1466 onward is +1 (1466→1467 … 10483→10484); 659 is unchanged
- src/local_webrtc_smoke.rs: 147→146, 208→207, 302→301, 335→334, 342→341, 397→396, 416→415, 452→453, 497→498
- src/lua_runtime.rs: 3867→3870, 3967→3970, 4021→4024
- src/main.rs: 4075→4074, 4084→4083
- src/runtime.rs: 394→437, 1618→1689, 5368→5426, 5452→5512, 6176→6251, 7374→7466, 7391→7483, 7406→7498, 7418→7510, 8328→8420
- src/transport/webrtc/control_channel.rs: every line from 492 onward is +1 (492→493 … 2147→2148)
- src/transport/webrtc/subscription_channel.rs: 848→931, 1134→1217, 1148→1231, 1298→1382, 1300→1384, 1340→1424, 1416→1500, 1420→1504, 1431→1515, 1433→1517, 1456→1540, 1527→1615, 1531→1619, 1541→1629, 1697→1785, 1719→1807, 1817→1905, 1900→1988, 2202→2295, 2344→2437, 2539→2632, 2590→2683
- src/transport/webrtc/test_support.rs: 591→608, 645→729, 673→757, 761→845, 904→988, 1051→1135, 1135→1219, 1185→1316, 1291→1422, 1343→1484, 1393→1596, 1437→1640, 1494→1697, 1529→1732, 1637→1855, 1947→2218, 2032→2303
- tests/hub_daemon_lifecycle/package_fixtures.rs: 636→754, 675→793, 714→832, 771→889, 1261→1379, 1282→1400, 1304→1422, 1326→1444, 1408→1526
- tests/hub_daemon_lifecycle/packages.rs: 6715→7081, 6740→7106, 6759→7125, 7074→7440, 7166→7532, 7318→7684, 7324→7690, 7352→7718, 7361→7727, 7494→7860, 7517→7883, 7648→8014, 7705→8071
- tests/hub_daemon_lifecycle/paste_transaction.rs: 116→126, 130→140, 224→234, 362→375, 407→420
- tests/hub_daemon_lifecycle/shutdown.rs: 2110→2111, 2243→2244, 2264→2265, 2437→2438, 2512→2513, 2534→2535, 2564→2565, 2646→2647, 3226→3227, 3291→3292, 3304→3305
- tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs: 168→170 (rewritten), 218→223, 232→237, 692→697, 804→809, 957→962, 961→966
- tests/hub_daemon_lifecycle/webrtc_fixtures.rs: 1420→1416, 1444→1440, 1553→1554, 1582→1583, 1981→1982, 2145→2146, 2236→2237, 2371→2372, 2617→2618
- tests/hub_daemon_lifecycle/webrtc_proofs.rs: 941→944, 986→989, 997→1000, 1519→1522, 1572→1575, 1877→2046
- tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs: every line from 201 onward is −1 (201→200 … 1701→1700)

## Counts for this section

- New grep lines: 10. 8 are DEFECT (peer 4337, test_support 518, 1266, 1545 and 1913, paste 76, webrtc_proofs 1698 and 1735) and 2 are deadline (test_support 681 and 692).
- Extras: 2 DEFECT (peer 4357-4360 and test_support 1246).
- Moved: 1 (subscription_ownership_baseline 168→170). Removed: 1 (hub_lua_runtime_test 1937).
- At 20b201bd the grep therefore has 875 + 10 − 1 = 884 lines. The totals table at the top stays at 23b0feaa.

---

# Follow-ups (not applied)

These are value and structure suggestions recorded under ruling 2. None of them is part of this rewrite, and all values stay as they are.

1. **allocation_oracle.rs:534.** The 1 s `recv_timeout` exists only so the oracle can measure the expiry path. `Duration::ZERO` (or a few ms) would remove 1 s of dead time per oracle run with the same allocation evidence. The value is unchanged in this rewrite.
2. **event_plane_saturation.rs:4729.** The 50 ms read budget per frame looks tight under host load (flake risk). The value is unchanged.
3. **package_events.rs:1297.** The 100 ms "must not wait for contended pool" bound is tight under load. The value is unchanged.
4. **owner_loop.rs:8149/8154.** This test asserts a 500 ms latency bound with `elapsed()`. The reviewer should decide whether a latency assertion belongs in a correctness test. The value is unchanged.
5. **DATA_PLANE_STOP_BOUND (driver.rs 1449).** When the 1 s watchdog goes, the bound keeps its value as a literal. Whether 2 s + slack is still the right stop bound without a watchdog is a later question.
6. **plugin_bounds.rs:231.** When the idle-CPU measurement moves to probe-hub-resources, it keeps its 5 s window. Whether a shorter window gives the same signal is a later question.
