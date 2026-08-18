# Isolate lifecycle-suite workers and host resources

- Ticket: `ticket_1787011770_110683` — Hub test harness: isolate lifecycle-suite workers and host resources
- Run: `run_1787013171_779998`, step `botster_stack_plan`
- Target repository: **botster-hub** (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` `66ca79cee26422eb8233d672d30a9efaa63271d7` (branch `project-pipelines/ticket_1787011770_110683` is at this base plus plan commits)
- Date: 2026-08-17
- Revision 2. Addresses Plan Review `review_1787014932_417965`: finding_1787014932_221545 (durable session-worker cleanup), finding_1787014932_434901 (ownership-request teardown matrix), finding_1787014933_248609 (deterministic host-exhaustion rule), finding_1787014933_655962 (structured gate evidence fields).

## Playbooks and notes loaded

- Repository playbook: [[botster-hub-playbook]]
- Role playbooks: [[planner-playbook]], [[botster-planner-playbook]]
- Class overlay: [[botster runtime teardown lenses]] (class applies; answers below)
- Targeted atomic notes:
  - [[hub shutdown preserves durable session workers]] (added in revision 2; defines the ordered two-layer cleanup contract for S1)
  - [[host ShutdownSession classification must call the exact-session Core query]]
  - [[daemon shutdown disconnects count as success only after clean owned process exit]]
  - [[worker shutdown completion requires lifecycle transport and process termination]]
  - [[argv marker censuses cannot see zombie survivors]]
  - [[sid scoped census is blind to setsid session leaks]]
  - [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
  - [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
  - [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] (constraint: do not change production budgets)
  - [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
  - [[flake oracles over typed response frames must print the full typed error body]]
  - [[webrtc bootstrap origin must be requested after the package server binds]]
  - [[PeerClosed attach occupancy must use the live attach route set]]
  - [[Unix EOF occupancy must share the live attach route set]]
  - [[Client event holders are connection-scoped]]
  - [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
  - [[subprocess harnesses must kill child on failed readiness]]
  - [[a regression test must be shown to go red with the fix reverted]]
- [[project-pipelines-playbook]]: not loaded; no Project Pipelines package/plugin path is in scope.

## Context loaded

- Repo surfaces read: `test.sh`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_daemon_lifecycle/*` (helpers and the nine cited failure sites), `tests/support/mod.rs`, `script/process-census`, `script/run-loaded-daemon-lifecycle`, `script/probe-hub-resources`, `docs/loaded-daemon-lifecycle-runner.md`, `.github/workflows/ci.yml`, `src/entrypoint_supervisor.rs`, `src/local_webrtc.rs`, `src/main.rs` (up/web probe path), `crates/botster-hub-installer/src/run.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/isolated_hub.rs`.
- Sibling tickets read to pin boundaries: `ticket_1786938984_190098`, `ticket_1786977409_499180`, `ticket_1786937228_425608`, `ticket_1786912572_610381`, `ticket_1786912569_840742`.
- Plan Review `review_1787014932_417965` findings read and addressed in this revision.

## Root-cause model (from repo evidence)

The lifecycle suite is one integration binary with 220 tests. 187 tests serialize on the process-local `REAL_DAEMON_TEST_LOCK` (`tests/hub_daemon_lifecycle/common.rs:54,407-413`). The suite spawns real daemons, session workers, PTYs, node web servers, operator consoles, and WebRTC peers. The observed failure class has four independent harness defects:

1. **Leaked children on panic.** `start_cli_daemon` (`tests/hub_daemon_lifecycle/cli.rs:54-81`) returns a bare `std::process::Child` at ~107 call sites. Rust `Child::drop` neither kills nor waits. Many wait helpers panic without reaping (for example `wait_for_entity_frame` at `common.rs:913-947`, `drain_until_subscription_deadline` at `session_fixtures.rs:444-485`). One panicking test therefore leaks a daemon tree (daemon + CoreDaemon + session workers + PTY shells + supervised node server). The leaked tree then loads the host and breaks later tests' fixed budgets. This is the mechanism behind the decaying tallies (218/1 → 215/4 → 212/7). Panic-safe ownership already exists but is used by only 7 tests (`PanicSafeCliDaemon`, `cli.rs:340-474`) plus 2 `ReapingChild` uses (`process.rs:120-148`). Critically, session workers call `setsid` and survive Hub death by design ([[hub shutdown preserves durable session workers]]), so reaping the Hub process group alone is never sufficient cleanup.
2. **Abort hazard during unwind.** `terminate_and_reap_child` (`common.rs:450-474`) panics and `.expect()`s. `ReapingChild::drop` calls it (`process.rs:145`). A panic there during unwinding is a double panic → `SIGABRT` → the whole suite process dies and orphans every other test's children. This can also explain a truncated tally.
3. **Non-unique host resources.**
   - `unique_test_dir`/`unique_short_test_dir` (`common.rs:61-79`) omit the process id; `update_command_test.rs:941` and `hub-test-support` `unique_root` include it. Two suite processes can collide on `/tmp/bh-<name>-<nanos>`.
   - `unused_loopback_port()` (`common.rs:227-233`) binds `127.0.0.1:0`, drops the listener, and hands the number to a child later (bind-and-drop TOCTOU; 14 call sites). A stolen port surfaces as the product's own occupied-port failure shape.
   - `src/entrypoint_supervisor.rs:660-667` keys the launch-result file as `temp_dir()/botster-launch-result-<pkg>-<entrypoint>-<started_at_seconds>.json`. Two daemons that start `botster-web`/`web-client` in the same wall-clock second share one file, and `refresh_launch_result` (`:403-421`) re-reads it after readiness. A colliding writer swaps the recorded `local_url`, and `IssueLocalWebrtcBootstrap` then answers `local_webrtc_bootstrap_origin_mismatch` → bootstrap `None` (`src/daemon_transport.rs:4312-4383`). This mechanism explains both `webrtc_terminal_adapter.rs:175` and the fixture 502 (`webrtc_fixtures.rs:729-732`) behind `shutdown.rs:1584`.
   - `sessions.rs:2146` hard-codes the isolated-hub root `/tmp/bh-slc` instead of `unique_short_test_dir`.
4. **The harness cannot classify host exhaustion.** A stolen port, PTY/fd exhaustion (`spawn_failed` via Core's 2 s `WORKER_STARTUP_TIMEOUT`), or an expired 5 s `SO_RCVTIMEO` (`sessions.rs:3873/4051` `.expect("spawn upsert")` → `WouldBlock`) all panic with the same shape as a product regression. The tolerant reader (`wait_for_entity_frame`, whitelisting `WouldBlock`/`os error 35`/`os error 11`) exists but the cited sites bypass it. `start_webrtc_adapter_hub` (`webrtc_terminal_adapter.rs:165-175`) discards the typed `local_webrtc_bootstrap_*` error body.

Existing assets to reuse, not rebuild: `PanicSafeCliDaemon` (already lists and shuts owned sessions on panic at `cli.rs:436` — the seed of the S1 guard), `ReapingChild`, `terminate_and_reap_pty_child`, `OwnedOperatorConsoleDaemon::cleanup` (identity-revalidated), `wait_for_entity_frame`, `wait_for_child_condition_with_budget` (reaps on budget expiry), and `script/process-census` (two-arm live/zombie census with positive controls, Darwin + Linux).

## Scope

Smallest set of changes that satisfies every ticket invariant:

**S1. Two-layer, data-dir-scoped cleanup guard for every lifecycle-suite daemon child.**
Make the `start_cli_daemon` family return one panic-safe guard (extend `PanicSafeCliDaemon`, whose panic path already does session shutdown at `cli.rs:436-450`). The guard's cleanup scope is the test's isolated data directory, and it runs the ordered contract from [[hub shutdown preserves durable session workers]] on **success, panic, and timeout** alike:
1. Enumerate every session in the owned data directory (bounded production `ListSessions` over the daemon socket, while the daemon is still reachable).
2. Call the production `ShutdownSession` path for every nonterminal session (exact-session classification preserved; typed `Found`/`Absent`/`Err` results recorded, not swallowed).
3. Stop and reap the Hub child (existing `shutdown` request, then bounded TERM → 500 ms → KILL → 2 s on the Hub's process group as backstop).
4. Prove, with separate live oracles, that the exact owned session workers, their PTY process groups, and the daemon socket no longer survive, and that the Hub child was reaped (`waitpid` complete, no zombie). If `ShutdownSession` reported success but a worker, group, or socket survives, the guard fails the test with that typed attribution (a Core lifecycle defect signal, per the vault note), never with a silent broad kill.
The guard never uses process-name-wide kills; identity is exact PID + `setpgid` group + data-dir marker, revalidated before any signal (existing `operator_console_fixtures.rs:301-317` pattern). Restart-durability tests (for example `cli_daemon_restart_recovers_worker_backed_session_through_transport`, `daemon_restart_reconnects_worker_backed_session_through_client_api`) keep their semantics through an explicit `transfer`/`disarm_sessions` guard mode that intentionally skips step 2 across the restart boundary and re-arms on the successor daemon; the intent is visible at the call site.
Also: give the direct node spawn at `webrtc_proofs.rs:22` its own process group and make `ChildCleanup::drop` (`process.rs:182-189`) kill the group, not only the direct child.

**S2. Unwind-safe reaping.**
Split `terminate_and_reap_child` into a `Result`-returning core. Drop paths log and continue on failure (never panic during unwind); direct call sites keep asserting. This removes the double-panic `SIGABRT` path.

**S3. Unique per-test host resources.**
- Add `std::process::id()` to `unique_test_dir` and `unique_short_test_dir`.
- Replace the hard-coded `/tmp/bh-slc` root (`sessions.rs:2146`) with `unique_short_test_dir`.
- Uniquify `launch_result_path` in `src/entrypoint_supervisor.rs` (add supervising-process id plus a per-process monotonic counter to the file name). This is the **only production code change** in this plan. Justification: the path is generated by the supervisor and passed to the child via `BOTSTER_ENTRYPOINT_LAUNCH_RESULT`; same-second collisions are possible for any two concurrently supervising daemons, not only tests. The env override and the readiness watcher contract stay unchanged.
- Remove the bind-and-drop port reservation where the fixture supports it: pass `BOTSTER_WEB_PORT=0` and derive the origin from the recorded `local_url` after the server binds (this is also the vault contract: origin is requested after bind). Keep held-listener reservations only where a test deliberately proves occupied-port behavior.
- Bound `capture_new_session_workers_for_data_dir` (`process.rs:389-463`) to exact data-dir attribution; drop the "any live worker from this worktree" fallback so one test never adopts another test's worker.

**S4. Host-exhaustion classification with a deterministic rule.**
Two layers, with fixed precedence (finding_1787014933_248609):

*Per-failure typed markers.* Budget-expiry paths in the shared wait helpers emit a structured, machine-parseable marker line in the panic message: `harness_budget_expired kind=<wait-kind> budget_ms=<n> resource=<observed>` plus captured evidence. `resource=` is filled only from **typed OS evidence observed on the failing operation itself**: `EAGAIN`/`EWOULDBLOCK` (os error 35/11), `EMFILE`/`ENFILE`, PTY allocation failure (`TerminalBackendConstruction`-shaped `spawn_failed`), or `ETIMEDOUT` on a readiness socket. Ambient observations (load average, census count, fd count) are attached as evidence but are **never classification inputs on their own**.

*Wrapper verdict rule (deterministic, ordered).* `script/run-lifecycle-suite` classifies in this precedence order; the first matching rule wins:
1. `environment_dirty` — the pre-run census found live botster-role executables from this worktree or a nonempty zombie delta **before** the suite started. Emitted without running the suite (or recorded alongside, if the operator forces a run).
2. `product_failure` — any failed test whose panic does **not** carry the `harness_budget_expired` marker, or any test that fails a semantic assertion. A product assertion always outranks host pressure: high ambient load never reclassifies a semantic failure.
3. `host_exhaustion` — every failed test carries the `harness_budget_expired` marker **with a typed `resource=` value**. A marker without a typed resource stays `product_failure` (fail-closed toward the stronger claim).
4. `clean` — zero failures, exactly one tally, and both post-run census arms empty.
Survivors after the run fail the command regardless of the verdict above (fail-closed; the verdict is annotated `survivors_present`).
Deterministic classifier tests (required): (a) an injected typed host-resource failure (helper driven with an injected `EMFILE`/budget-expiry marker) classifies as `host_exhaustion`; (b) a deliberate semantic assertion failure, run with seeded high-ambient-pressure evidence attached, stays `product_failure`. Both are unit tests of the classifying function (shell-level, in the `script/process-census --self-test` style, or Rust unit if the classifier lands in the harness).
Also in S4: route the cited single-shot entity reads through the tolerant reader with the module's proven patience bound: `sessions.rs:3873` and `:4051` (`.expect("spawn upsert")`), plus the identical bare `next_frame().expect(...)` single-shot sites in `sessions.rs` and `webrtc_proofs.rs:905`. Keep every semantic assertion identical; only the read discipline changes. Do not touch `sessions.rs:3458`'s oracle (already tolerant; if it still fails after isolation, that is a product signal for a separate ticket). Surface the typed error body in `start_webrtc_adapter_hub` (`webrtc_terminal_adapter.rs:165-175`) and in `install_real_release`'s failure output, per [[flake oracles over typed response frames must print the full typed error body]].

**S5. One-command / one-tally suite wrapper with survivor proof.**
Add `script/run-lifecycle-suite` (thin, Darwin+Linux, reusing `script/process-census`):
1. capture a pre-run zombie baseline and a live-executable paths file (hub, session-worker, node fixture, console binaries from this worktree),
2. apply the classification rule from S4 (pre-run dirty check first),
3. run exactly one `./test.sh --locked --test hub_daemon_lifecycle_test "$@"` after the session-worker prebuild,
4. assert exactly one result tally in the captured output,
5. run `assert-no-live-executables` and `assert-no-new-zombies` with a bounded settle window,
6. emit one structured verdict per the S4 rule.
No arbitrary sleeps; the settle loop is the census's existing bounded retry.

**S6. Deterministic injected-failure cleanup proof.**
New focused tests beside the existing prior art (`shutdown.rs:417` proves the timeout path already):
- a test that spawns a guard-owned daemon with a worker-backed session, panics inside `catch_unwind` while holding the guard, then proves the four ordered layers separately: (1) `ShutdownSession` was issued and typed-classified, (2) the exact worker PID and PTY process group are gone, (3) the daemon socket is absent, (4) the Hub child was reaped with no new zombie — all before the test returns;
- the same shape for an injected **timeout** (budget-expiry path through `wait_for_child_condition_with_budget`), for an operator-console-owned daemon, and for a supervised node entrypoint;
- a restart-durability control: the `transfer` guard mode keeps the worker alive across an intentional Hub stop and the successor guard cleans it up (proves S1 does not break production durability semantics);
- red-on-revert: with the guard disarmed, the census check must fail (pattern from `script/process-census --self-test`).

## Non-scope

- `ready_spawn_*` wall-clock pair and session-projection completion: `ticket_1786938984_190098`.
- `ShutdownSession` `OperatorError` idempotency across natural exit: `ticket_1786977409_499180`.
- `unix_adapter_unbound_printf_stream_attach_completes`: `ticket_1786937228_425608`.
- PTY PID/marker lifecycle oracles: `ticket_1786912572_610381`; owner-loop scheduler: `ticket_1786912569_840742`.
- Production budgets stay fixed: Core `WORKER_STARTUP_TIMEOUT` (2 s), installer `RUN_DEADLINE` (10 s), `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` (30 s), `LAUNCH_RESULT_READINESS_BUDGET` (15 s), `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, `OBSERVE_SLICE_BUDGET`.
- No botster-core changes. No changes to the Linux-only loaded-runner campaign (`script/run-loaded-daemon-lifecycle`, workflow); its `-p botster-core` vs `-p botster-core-daemon` prebuild drift is recorded as a vault gap, not fixed here.
- No `--test-threads=1`, no `serial_test`, no nextest: repo policy forbids serialization as acceptance evidence.
- No semantic weakening of any product assertion. No change to production `ShutdownSession` semantics (that contract belongs to `ticket_1786977409_499180`; the guard only *calls* the production path).

## Ownership boundaries and cross-repo dependencies

- All changes live in botster-hub: `tests/hub_daemon_lifecycle/*`, `tests/support/`, `script/`, and one bounded `src/entrypoint_supervisor.rs` naming change. Hub owns its harness and its supervisor policy.
- `spawn_failed` budgets and worker readiness live in botster-core (pinned rev `fc541a5`); this plan treats them as fixed contracts. If the S6 proofs show `ShutdownSession` success while a worker, PTY group, or socket survives, that is a Core session-lifecycle defect: stop and register a botster-core dependency ticket; do not patch around it here.
- `hub-test-support` (crate and npm 0.1.37 / conformance revision 43) is consumed read-only; no fixture-byte mutation under a published version.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes. The ticket is daemon/session-worker/WebRTC-runtime/console child teardown in the suite harness, plus FD/PTY/CPU spin from leaked children.
- `teardown_isolation`: the per-test ownership set is {CLI daemon child, its embedded CoreDaemon, its durable session workers and their PTY groups, its supervised node entrypoint, its operator-console PTY child and detached console daemon, its WebRTC peer runtime}. One failed test's cleanup acts only on its own identity-revalidated PIDs/groups inside its own data-dir scope. Healthy sibling tests and their children are never swept by name.
- `teardown_bounds`: session cleanup uses bounded production requests (`ListSessions`, `ShutdownSession`, `shutdown`) with the harness's existing wait budgets; process backstops use the bounded TERM→500 ms→KILL→2 s sequence; drop paths never panic (S2); no unbounded `block_on(close)` is added; census settle loops are bounded.
- `late_message_matrix` — every ownership-creating runtime request the affected tests issue, with owner tag, post-teardown rejection, race sweep, bound/hard stop, and live production-path proof (finding_1787014932_434901):

| Ownership-creating request | Owner identity (tag) | Rejection after teardown | Race sweep | Bound and hard stop | Live production-path proof |
|---|---|---|---|---|---|
| `Spawn` / `SpawnSessionType` (session + durable worker + PTY) | session id in Core registry; worker PID + its own `setsid` group; data-dir scope | requests against a stopped daemon fail on the dead socket (unique path dies with the data dir); Core rejects operations on exited sessions with typed errors | guard step 1-2: bounded `ListSessions` then exact-session `ShutdownSession` per nonterminal session before Hub stop, on success/panic/timeout; typed `Found`/`Absent`/`Err` preserved per [[host ShutdownSession classification must call the exact-session Core query]] | production shutdown first; TERM→KILL on the exact worker group only if the production path fails, with typed attribution | S6 proofs: worker PID gone, PTY group gone, socket absent, distinct from Hub-child reap evidence per [[worker shutdown completion requires lifecycle transport and process termination]] |
| Terminal `Attach` (subscription route) | session + subscription + connection; Unix EOF and PeerClosed occupancy share the live attach route set ([[Unix EOF occupancy must share the live attach route set]], [[PeerClosed attach occupancy must use the live attach route set]]) | a closed connection's routes are removed by the production EOF/PeerClosed path; late attach on a dead daemon fails on the dead socket | guard closes client sockets before daemon shutdown so production route cleanup runs; route-set occupancy is the oracle | production cleanup path; daemon stop bounds the rest | existing occupancy oracles in the suite; S6 asserts no live attach subscriptions in the pre-stop status probe |
| `SubscribeEntities` (entity subscription) | connection-scoped subscription id ([[Client event holders are connection-scoped]]) | dead socket; daemon cleanup counters advance on disconnect | dropping the client socket triggers production disconnect cleanup (cleanup_completed observed by `script/probe-hub-resources` pattern) | daemon-side production cleanup; bounded by daemon stop | pre-stop status probe shows zero live entity subscriptions in S6 |
| WebRTC bootstrap grant + peer/DataChannel | single-use origin-bound grant; peer id in the daemon's peer map; test-side peer on `default_runtime` | grants die with the daemon; late channel traffic hits the bounded peer-close path ([[WebRTC DataChannel local close uses the peer close bound before cleanup]], 200 ms test bound) | test drops its peer before guard shutdown; daemon `stop_all` drops the dedicated runtime at stop | bounded peer close; runtime drop is the hard stop | instance-scoped worker census (`src/local_webrtc.rs:133-137`) already proves runtime park in lib tests; suite-level live census proves no runtime thread survives the daemon child |
| `StartPackageEntrypoint` (supervised node server) | supervisor-owned child group in the daemon; uniquified launch-result path (S3) | supervisor rejects status/bootstrap for a non-running entrypoint with typed `local_webrtc_bootstrap_*` errors | late launch-result write from a dying node child lands in its dead unique file and is never re-adopted (S3) | `SupervisedProcess::stop`: TERM → 500 ms → KILL → 2 s on the group (`src/entrypoint_supervisor.rs:423-470`) | S6 supervised-entrypoint case: node child and its group gone after guard cleanup; collision regression test for the path |
| Operator console open (PTY child + detached daemon) | console PTY child group; detached daemon PID from metadata, command-line revalidated before signal (`operator_console_fixtures.rs:301-317`) | `OwnedOperatorConsoleDaemon::new` refuses a data dir with a live daemon | console `Drop` reaps the PTY group; owned-daemon cleanup shuts down, then bounded TERM/KILL per PID | bounded per-PID 2 s waits, then KILL | S6 console case: socket-gone, status-stopped, metadata assertions plus census absence |
| Installer lease (`flock` on `<prefix>/daemon.lock`) | kernel lock owned by the holding process | released by the kernel on process death | none needed beyond process reap | process reap bounds it | existing `real_daemons_on_custom_data_directories_hold_the_installation_lease` SIGKILL-release proof stays green |

  A process census is an oracle over the result, never the cleanup mechanism; cleanup always goes through the production requests above with the OS group kill as bounded backstop.
- `production_path_proof`: S6 drives the guard's real panic/timeout paths and proves each ownership layer with live oracles (worker PID + PTY group + socket + Hub child, separately), plus the two-arm census with positive controls; the one production change (launch-result path) is proved through the real `StartPackageEntrypoint` readiness path plus a collision regression test. Terminal records are never accepted as teardown proof.
- `ownership_identity`: exact child PID plus process group created via `setpgid(0,0)` (or the worker's own `setsid` group id) plus unique data-dir marker; kill only after re-validating the PID's command line so a reused PID is never signaled.
- `sibling_fail_closed_policy`: on successful cleanup, siblings are untouched. If the production `ShutdownSession` path succeeds but a worker survives, the guard fails the test with typed Core-lifecycle attribution. If a guard cannot reap within bounds, it records evidence and fails only its own test; the suite wrapper then fails the whole command fail-closed on the survivor census without killing non-owned processes (evidence over broad kills).

## Assumptions and unknowns

- Assumption: the three tallies in the discovery log came from three suite invocations under external retry, not from one `cargo test` run; the wrapper (S5) makes this impossible to conflate by asserting one tally per command. Implement verifies `cargo test --workspace --test hub_daemon_lifecycle_test` runs exactly one test binary.
- Assumption: the launch-result uniqueness fix is in-scope production work under the ticket invariant "each test owns unique … worker resources"; it is flagged for Plan Review as the single production seam.
- Assumption: guard session-cleanup (`ListSessions` + `ShutdownSession`) is feasible on the panic path because the daemon is usually still alive when a test assertion fails; when the daemon itself is dead, step 1-2 degrade to typed "daemon unreachable" evidence and the guard falls through to worker-group backstop cleanup using the worker PIDs it captured at spawn/attach time.
- Unknown: which of the 14 `unused_loopback_port` sites can move to `BOTSTER_WEB_PORT=0` without changing what the test proves. Implement audits per site; sites proving occupied-port behavior keep their held listener.
- Unknown: exact guard-adoption mechanics at each of the ~107 `start_cli_daemon` sites. The compiler enumerates them; the guard must keep `shutdown_cli_daemon(data_dir, child)` flows working via disarm/into-inner, and restart tests use the explicit `transfer` mode.
- Unknown: Darwin census cost at suite boundary; expected negligible (two `ps` sweeps per command, none per test).

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/cli.rs` — guard-returning `start_cli_daemon` family; `PanicSafeCliDaemon` extension (ordered two-layer cleanup, transfer mode).
- `tests/hub_daemon_lifecycle/common.rs` — unique dirs (+pid), port helper policy, unwind-safe reap core, `harness_budget_expired` marker emission with typed resource evidence.
- `tests/hub_daemon_lifecycle/process.rs` — `ReapingChild`/`ChildCleanup` group semantics; census attribution fallback removal.
- `tests/hub_daemon_lifecycle/sessions.rs` — tolerant reads at the cited and sibling single-shot sites; `/tmp/bh-slc` root fix.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`, `webrtc_proofs.rs` — typed-error surfacing; node spawn group; `:905` read.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — installer failure diagnostics passthrough.
- `tests/hub_daemon_lifecycle/shutdown.rs` (or a new sibling file) — deterministic injected-failure cleanup tests (panic, timeout, console, supervised entrypoint, restart-durability control).
- `tests/support/mod.rs` — shared helpers if guard plumbing needs them.
- `src/entrypoint_supervisor.rs` — `launch_result_path` uniqueness + unit regression test.
- `script/run-lifecycle-suite` — new wrapper with the deterministic classifier; reuses `script/process-census`; classifier unit tests.
- `README.md` / `docs/` — short harness-contract note for the wrapper and the classification rule (placement per repo prior art).

## Risks

- ~107 mechanical call-site conversions can change test flow subtly (double kill, changed drop order). Mitigation: guard disarms on explicit shutdown; reaping is idempotent by PID identity; convert file-by-file with compilation as the enumerator.
- Guard session-cleanup on the panic path adds daemon round-trips during unwind. Mitigation: bounded requests, and the daemon-unreachable degradation path keeps unwind short.
- The `transfer` mode could be misused to skip cleanup. Mitigation: it requires a successor guard at the call site, and the S6 restart-durability control plus the suite-level census catch a dropped hand-off.
- The port-0 migration can weaken a test that intended to pin the origin before bind. Mitigation: per-site audit; the vault contract already requires origin after bind.
- The launch-result rename touches production supervision. Mitigation: env override unchanged; watcher watches the exact generated path; regression test for two supervisors in one second; full workspace gate.
- Drop-time logging instead of panicking (S2) could hide a real cleanup failure. Mitigation: the guard converts cleanup failure into a test failure with typed attribution when not already panicking, and the suite wrapper's survivor census fails the command whenever anything survives, so silence cannot pass.
- Classifier false `host_exhaustion` claims. Mitigation: rule 3 requires typed OS resource evidence on the failing operation itself; ambient pressure alone never reclassifies; product assertions always win (rule 2 before rule 3).

## Acceptance checks and tests

Primary (deterministic, focused; no arbitrary sleeps, no repeated full suites):
1. New injected-failure cleanup tests (S6) pass, proving the four ordered layers separately on the panic path and the timeout path: `ShutdownSession` issued and typed-classified → exact worker PID and PTY group gone → daemon socket absent → Hub child reaped without zombie. Includes the operator-console and supervised-entrypoint cases, the restart-durability `transfer` control, and red-on-revert with the guard disarmed.
2. `src/entrypoint_supervisor.rs` collision regression test: two same-second launch-result paths are distinct; supervised readiness still works through a real `StartPackageEntrypoint` flow.
3. Classifier determinism tests: injected typed host-resource failure → `host_exhaustion`; semantic assertion failure under seeded ambient pressure → `product_failure`; pre-run dirty environment → `environment_dirty` before any suite run.
4. Repeated focused runs without cooldown, back-to-back, census-clean between repetitions:
   - `./test.sh --locked --test hub_daemon_lifecycle_test process_ownership_` × 5
   - `./test.sh --locked --test hub_daemon_lifecycle_test cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops -- --exact` × 5
5. `script/process-census --self-test` passes (both arms keep their positive controls).
6. `script/run-lifecycle-suite` self-evidence: one command → one suite process, one tally, no live survivors, no new zombies, and the `environment_dirty` verdict when seeded with a marker process.

Final smoke (exclusive, once):
7. After `cargo build --locked -p botster-core-daemon --bin botster-session-worker`: one exclusive `script/run-lifecycle-suite` run of `hub_daemon_lifecycle_test` with zero failures and a `clean` verdict.
8. One full `./test.sh --locked` workspace gate (CI parity).

Downstream proof per charter: startup, reuse, shutdown, restart, and cleanup behavior is covered by the existing suite plus S6. Hub-process shutdown evidence and durable-session cleanup evidence are produced and asserted **separately** (guard step 3 vs step 4 oracles), per the charter gate and [[hub shutdown preserves durable session workers]]; the restart-durability control proves the guard does not erase production durability semantics.

## Vault gaps worth capturing

- Captured during this Plan: hub entrypoint supervisor launch-result temp-file collision on concurrent startup (vault inbox).
- Bare `std::process::Child` daemon handles in integration harnesses leak trees on panic; panic-safe guards must be the only spawn path (new gotcha).
- Bind-and-drop ephemeral port reservation is a TOCTOU that mimics the product's occupied-port failure (new gotcha).
- Reaping helpers that panic inside `Drop` convert one test failure into a suite-wide `SIGABRT` (new gotcha).
- `loaded-daemon-lifecycle.yml:157` prebuilds `-p botster-core` while every other site builds `-p botster-core-daemon` (drift; follow-up candidate).
