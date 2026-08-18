# Isolate lifecycle-suite workers and host resources

- Ticket: `ticket_1787011770_110683` — Hub test harness: isolate lifecycle-suite workers and host resources
- Run: `run_1787013171_779998`, step `botster_stack_plan`
- Target repository: **botster-hub** (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` `66ca79cee26422eb8233d672d30a9efaa63271d7` (branch `project-pipelines/ticket_1787011770_110683` is exactly at this base)
- Date: 2026-08-17

## Playbooks and notes loaded

- Repository playbook: [[botster-hub-playbook]]
- Role playbooks: [[planner-playbook]], [[botster-planner-playbook]]
- Class overlay: [[botster runtime teardown lenses]] (class applies; answers below)
- Targeted atomic notes:
  - [[argv marker censuses cannot see zombie survivors]]
  - [[sid scoped census is blind to setsid session leaks]]
  - [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
  - [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
  - [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] (constraint: do not change production budgets)
  - [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
  - [[flake oracles over typed response frames must print the full typed error body]]
  - [[webrtc bootstrap origin must be requested after the package server binds]]
  - [[subprocess harnesses must kill child on failed readiness]]
  - [[a regression test must be shown to go red with the fix reverted]]
- [[project-pipelines-playbook]]: not loaded; no Project Pipelines package/plugin path is in scope.

## Context loaded

- Repo surfaces read: `test.sh`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_daemon_lifecycle/*` (helpers and the nine cited failure sites), `tests/support/mod.rs`, `script/process-census`, `script/run-loaded-daemon-lifecycle`, `script/probe-hub-resources`, `docs/loaded-daemon-lifecycle-runner.md`, `.github/workflows/ci.yml`, `src/entrypoint_supervisor.rs`, `src/local_webrtc.rs`, `src/main.rs` (up/web probe path), `crates/botster-hub-installer/src/run.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/isolated_hub.rs`.
- Sibling tickets read to pin boundaries: `ticket_1786938984_190098`, `ticket_1786977409_499180`, `ticket_1786937228_425608`, `ticket_1786912572_610381`, `ticket_1786912569_840742`.

## Root-cause model (from repo evidence)

The lifecycle suite is one integration binary with 220 tests. 187 tests serialize on the process-local `REAL_DAEMON_TEST_LOCK` (`tests/hub_daemon_lifecycle/common.rs:54,407-413`). The suite spawns real daemons, session workers, PTYs, node web servers, operator consoles, and WebRTC peers. The observed failure class has four independent harness defects:

1. **Leaked children on panic.** `start_cli_daemon` (`tests/hub_daemon_lifecycle/cli.rs:54-81`) returns a bare `std::process::Child` at ~107 call sites. Rust `Child::drop` neither kills nor waits. Many wait helpers panic without reaping (for example `wait_for_entity_frame` at `common.rs:913-947`, `drain_until_subscription_deadline` at `session_fixtures.rs:444-485`). One panicking test therefore leaks a daemon tree (daemon + CoreDaemon + session workers + PTY shells + supervised node server). The leaked tree then loads the host and breaks later tests' fixed budgets. This is the mechanism behind the decaying tallies (218/1 → 215/4 → 212/7). Panic-safe ownership already exists but is used by only 7 tests (`PanicSafeCliDaemon`, `cli.rs:340-474`) plus 2 `ReapingChild` uses (`process.rs:120-148`).
2. **Abort hazard during unwind.** `terminate_and_reap_child` (`common.rs:450-474`) panics and `.expect()`s. `ReapingChild::drop` calls it (`process.rs:145`). A panic there during unwinding is a double panic → `SIGABRT` → the whole suite process dies and orphans every other test's children. This can also explain a truncated tally.
3. **Non-unique host resources.**
   - `unique_test_dir`/`unique_short_test_dir` (`common.rs:61-79`) omit the process id; `update_command_test.rs:941` and `hub-test-support` `unique_root` include it. Two suite processes can collide on `/tmp/bh-<name>-<nanos>`.
   - `unused_loopback_port()` (`common.rs:227-233`) binds `127.0.0.1:0`, drops the listener, and hands the number to a child later (bind-and-drop TOCTOU; 14 call sites). A stolen port surfaces as the product's own occupied-port failure shape.
   - `src/entrypoint_supervisor.rs:660-667` keys the launch-result file as `temp_dir()/botster-launch-result-<pkg>-<entrypoint>-<started_at_seconds>.json`. Two daemons that start `botster-web`/`web-client` in the same wall-clock second share one file, and `refresh_launch_result` (`:403-421`) re-reads it after readiness. A colliding writer swaps the recorded `local_url`, and `IssueLocalWebrtcBootstrap` then answers `local_webrtc_bootstrap_origin_mismatch` → bootstrap `None` (`src/daemon_transport.rs:4312-4383`). This mechanism explains both `webrtc_terminal_adapter.rs:175` and the fixture 502 (`webrtc_fixtures.rs:729-732`) behind `shutdown.rs:1584`.
   - `sessions.rs:2146` hard-codes the isolated-hub root `/tmp/bh-slc` instead of `unique_short_test_dir`.
4. **The harness cannot classify host exhaustion.** A stolen port, PTY/fd exhaustion (`spawn_failed` via Core's 2 s `WORKER_STARTUP_TIMEOUT`), or an expired 5 s `SO_RCVTIMEO` (`sessions.rs:3873/4051` `.expect("spawn upsert")` → `WouldBlock`) all panic with the same shape as a product regression. The tolerant reader (`wait_for_entity_frame`, whitelisting `WouldBlock`/`os error 35`/`os error 11`) exists but the cited sites bypass it. `start_webrtc_adapter_hub` (`webrtc_terminal_adapter.rs:165-175`) discards the typed `local_webrtc_bootstrap_*` error body.

Existing assets to reuse, not rebuild: `PanicSafeCliDaemon`, `ReapingChild`, `terminate_and_reap_pty_child`, `OwnedOperatorConsoleDaemon::cleanup` (identity-revalidated), `wait_for_entity_frame`, `wait_for_child_condition_with_budget` (reaps on budget expiry), and `script/process-census` (two-arm live/zombie census with positive controls, Darwin + Linux).

## Scope

Smallest set of changes that satisfies every ticket invariant:

**S1. Panic-safe ownership for every lifecycle-suite daemon child.**
Make the `start_cli_daemon` family return a panic-safe guard (extend `PanicSafeCliDaemon` or a thinner wrapper over `ReapingChild`) instead of a bare `Child`. The guard:
- reaps the exact owned process group with the existing bounded TERM→500 ms→KILL sequence on drop,
- disarms when the test hands the child to `shutdown_cli_daemon`, so the clean path is unchanged,
- keeps ownership identity: exact PID + `setpgid` group + unique data dir; re-validate identity before any kill (existing `operator_console_fixtures.rs:301-317` pattern).
Also: give the direct node spawn at `webrtc_proofs.rs:22` its own process group and make `ChildCleanup::drop` (`process.rs:182-189`) kill the group, not only the direct child.

**S2. Unwind-safe reaping.**
Split `terminate_and_reap_child` into a `Result`-returning core. Drop paths log and continue on failure (never panic during unwind); direct call sites keep asserting. This removes the double-panic `SIGABRT` path.

**S3. Unique per-test host resources.**
- Add `std::process::id()` to `unique_test_dir` and `unique_short_test_dir`.
- Replace the hard-coded `/tmp/bh-slc` root (`sessions.rs:2146`) with `unique_short_test_dir`.
- Uniquify `launch_result_path` in `src/entrypoint_supervisor.rs` (add supervising-process id plus a per-process monotonic counter to the file name). This is the **only production code change** in this plan. Justification: the path is generated by the supervisor and passed to the child via `BOTSTER_ENTRYPOINT_LAUNCH_RESULT`; same-second collisions are possible for any two concurrently supervising daemons, not only tests. The env override and the readiness watcher contract stay unchanged.
- Remove the bind-and-drop port reservation where the fixture supports it: pass `BOTSTER_WEB_PORT=0` and derive the origin from the recorded `local_url` after the server binds (this is also the vault contract: origin is requested after bind). Keep held-listener reservations only where a test deliberately proves occupied-port behavior.
- Bound `capture_new_session_workers_for_data_dir` (`process.rs:389-463`) to exact data-dir attribution; drop the "any live worker from this worktree" fallback so one test never adopts another test's worker.

**S4. Host-exhaustion classification.**
- Add a small `host_pressure_evidence()` helper (load average, live census count of `botster-hub`/`botster-session-worker` for this worktree, open-fd count of the test process) and append it to budget-expiry panic messages in the shared wait helpers. A timed-out wait then names host pressure separately from a product assertion.
- Route the cited single-shot entity reads through the tolerant reader with the module's proven patience bound: `sessions.rs:3873` and `:4051` (`.expect("spawn upsert")`), plus the identical bare `next_frame().expect(...)` single-shot sites in `sessions.rs` and `webrtc_proofs.rs:905`. Keep every semantic assertion identical; only the read discipline changes. Do not touch `sessions.rs:3458`'s oracle (already tolerant; if it still fails after isolation, that is a product signal for a separate ticket).
- Surface the typed error body in `start_webrtc_adapter_hub` (`webrtc_terminal_adapter.rs:165-175`) and in `install_real_release`'s failure output, per [[flake oracles over typed response frames must print the full typed error body]].

**S5. One-command / one-tally suite wrapper with survivor proof.**
Add `script/run-lifecycle-suite` (thin, Darwin+Linux, reusing `script/process-census`):
1. capture a pre-run zombie baseline and a live-executable paths file (hub, session-worker, node fixture, console binaries from this worktree),
2. report a dirty pre-run environment as `environment_dirty` (distinct from a test failure),
3. run exactly one `./test.sh --locked --test hub_daemon_lifecycle_test "$@"` after the session-worker prebuild,
4. assert exactly one result tally in the captured output,
5. run `assert-no-live-executables` and `assert-no-new-zombies` with a bounded settle window,
6. emit one structured verdict: `product_failure`, `host_exhaustion` (census/pressure evidence), `environment_dirty`, or `clean`.
No arbitrary sleeps; the settle loop is the census's existing bounded retry.

**S6. Deterministic injected-failure cleanup proof.**
New focused tests beside the existing prior art (`shutdown.rs:417` proves the timeout path already):
- a test that spawns a guard-owned daemon with a worker-backed session, panics inside `catch_unwind` while holding the guard, then proves by exact PID/data-dir census that daemon, session worker, and supervised children are gone and were reaped (no new zombie) before the test returns;
- the same shape for an operator-console-owned daemon and for a supervised node entrypoint;
- red-on-revert: with the guard disarmed, the census check must fail (pattern from `script/process-census --self-test`).

## Non-scope

- `ready_spawn_*` wall-clock pair and session-projection completion: `ticket_1786938984_190098`.
- `ShutdownSession` `OperatorError` idempotency across natural exit: `ticket_1786977409_499180`.
- `unix_adapter_unbound_printf_stream_attach_completes`: `ticket_1786937228_425608`.
- PTY PID/marker lifecycle oracles: `ticket_1786912572_610381`; owner-loop scheduler: `ticket_1786912569_840742`.
- Production budgets stay fixed: Core `WORKER_STARTUP_TIMEOUT` (2 s), installer `RUN_DEADLINE` (10 s), `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` (30 s), `LAUNCH_RESULT_READINESS_BUDGET` (15 s), `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, `OBSERVE_SLICE_BUDGET`.
- No botster-core changes. No changes to the Linux-only loaded-runner campaign (`script/run-loaded-daemon-lifecycle`, workflow) beyond none; its `-p botster-core` vs `-p botster-core-daemon` prebuild drift is recorded as a vault gap, not fixed here.
- No `--test-threads=1`, no `serial_test`, no nextest: repo policy forbids serialization as acceptance evidence.
- No semantic weakening of any product assertion.

## Ownership boundaries and cross-repo dependencies

- All changes live in botster-hub: `tests/hub_daemon_lifecycle/*`, `tests/support/`, `script/`, and one bounded `src/entrypoint_supervisor.rs` naming change. Hub owns its harness and its supervisor policy.
- `spawn_failed` budgets and worker readiness live in botster-core (pinned rev `fc541a5`); this plan treats them as fixed contracts. If Implement proves a Core semantic must change, stop and register a botster-core dependency ticket; do not patch around it here.
- `hub-test-support` (crate and npm 0.1.37 / conformance revision 43) is consumed read-only; no fixture-byte mutation under a published version.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes. The ticket is daemon/session-worker/WebRTC-runtime/console child teardown in the suite harness, plus FD/PTY/CPU spin from leaked children.
- `teardown_isolation`: the per-test ownership set is {CLI daemon child, its embedded CoreDaemon, its session workers and PTYs, its supervised node entrypoint, its operator-console PTY child and detached console daemon, its WebRTC peer runtime}. One failed test's cleanup kills only its own exact PIDs/process groups (identity-revalidated). Healthy sibling tests and their children are never swept by name.
- `teardown_bounds`: all drop-path reaping uses the existing bounded TERM→500 ms→KILL→2 s wait sequence; drop paths never panic (S2); no unbounded `block_on(close)` is added; the suite wrapper's census retries are bounded settle loops.
- `late_message_matrix` (harness-scope ownership-creating surfaces):
  - daemon child spawn → tagged by exact PID + setpgid group + unique data dir; rejected after teardown because the socket path dies with the unique data dir; swept by guard drop with identity re-check.
  - launch-result file write from a dying node child → tagged by the uniquified per-supervisor path (S3); a late write lands in a dead unique file and can never be adopted by another daemon's `refresh_launch_result`.
  - census attribution of workers → tagged by exact data dir; the worktree-wide fallback adoption is removed (S3) so a late-exiting worker from test A is never counted for test B.
  - zombie rows → swept because guard drop always `wait()`s; the suite wrapper's zombie census is the backstop oracle.
- `production_path_proof`: the harness proofs are live-process censuses (live arm + zombie arm with positive controls), never terminal JSON records. The one production change (launch-result path) is proved through the real daemon `StartPackageEntrypoint` readiness path plus a collision regression test.
- `ownership_identity`: exact child PID plus process group created via `setpgid(0,0)` plus unique data-dir marker; kill only after re-validating the PID's command line (existing console pattern) so a reused PID is never signaled.
- `sibling_fail_closed_policy`: on successful cleanup, siblings are untouched. If a guard cannot reap within bounds, it records evidence and fails only its own test; the suite wrapper then fails the whole command fail-closed on the survivor census without killing non-owned processes (evidence over broad kills).

## Assumptions and unknowns

- Assumption: the three tallies in the discovery log came from three suite invocations under external retry, not from one `cargo test` run; the wrapper (S5) makes this impossible to conflate by asserting one tally per command. Implement verifies `cargo test --workspace --test hub_daemon_lifecycle_test` runs exactly one test binary.
- Assumption: the launch-result uniqueness fix is in-scope production work under the ticket invariant "each test owns unique … worker resources"; it is flagged for Plan Review as the single production seam.
- Unknown: which of the 14 `unused_loopback_port` sites can move to `BOTSTER_WEB_PORT=0` without changing what the test proves. Implement audits per site; sites proving occupied-port behavior keep their held listener.
- Unknown: exact guard-adoption mechanics at each of the ~107 `start_cli_daemon` sites. The compiler enumerates them; the guard must keep `shutdown_cli_daemon(data_dir, child)` flows working via disarm/into-inner.
- Unknown: Darwin census cost at suite boundary; expected negligible (two `ps` sweeps per command, none per test).

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/cli.rs` — guard-returning `start_cli_daemon` family; `PanicSafeCliDaemon` reuse.
- `tests/hub_daemon_lifecycle/common.rs` — unique dirs (+pid), port helper policy, unwind-safe reap core, `host_pressure_evidence()`.
- `tests/hub_daemon_lifecycle/process.rs` — `ReapingChild`/`ChildCleanup` group semantics; census attribution fallback removal.
- `tests/hub_daemon_lifecycle/sessions.rs` — tolerant reads at the cited and sibling single-shot sites; `/tmp/bh-slc` root fix.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`, `webrtc_proofs.rs` — typed-error surfacing; node spawn group; `:905` read.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — installer failure diagnostics passthrough.
- `tests/hub_daemon_lifecycle/shutdown.rs` (or a new sibling file) — deterministic injected-failure cleanup tests.
- `tests/support/mod.rs` — shared helpers if guard plumbing needs them.
- `src/entrypoint_supervisor.rs` — `launch_result_path` uniqueness + unit regression test.
- `script/run-lifecycle-suite` — new wrapper; reuses `script/process-census`.
- `README.md` / `docs/` — short harness-contract note for the wrapper (placement per repo prior art).

## Risks

- ~107 mechanical call-site conversions can change test flow subtly (double kill, changed drop order). Mitigation: guard disarms on explicit shutdown; reaping is idempotent by PID identity; convert file-by-file with compilation as the enumerator.
- The port-0 migration can weaken a test that intended to pin the origin before bind. Mitigation: per-site audit; the vault contract already requires origin after bind.
- The launch-result rename touches production supervision. Mitigation: env override unchanged; watcher watches the exact generated path; regression test for two supervisors in one second; full workspace gate.
- Drop-time logging instead of panicking (S2) could hide a real cleanup failure. Mitigation: the suite wrapper's survivor census fails the command whenever anything survives, so silence cannot pass.
- The wrapper must not become the only proof: focused deterministic tests (S6) are the primary gates; the wrapper is the boundary oracle.

## Acceptance checks and tests

Primary (deterministic, focused; no arbitrary sleeps, no repeated full suites):
1. New injected-failure cleanup tests (S6) pass, including red-on-revert with the guard disarmed.
2. `src/entrypoint_supervisor.rs` collision regression test: two same-second launch-result paths are distinct; supervised readiness still works through a real `StartPackageEntrypoint` flow.
3. Repeated focused runs without cooldown, back-to-back, census-clean between repetitions:
   - `./test.sh --locked --test hub_daemon_lifecycle_test process_ownership_` × 5
   - `./test.sh --locked --test hub_daemon_lifecycle_test cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops -- --exact` × 5
4. `script/process-census --self-test` passes (both arms keep their positive controls).
5. `script/run-lifecycle-suite` self-evidence: one command → one suite process, one tally, no live survivors, no new zombies, and a distinct `environment_dirty` verdict when seeded with a pre-existing marker process.

Final smoke (exclusive, once):
6. After `cargo build --locked -p botster-core-daemon --bin botster-session-worker`: one exclusive `script/run-lifecycle-suite` run of `hub_daemon_lifecycle_test` with zero failures and a `clean` verdict.
7. One full `./test.sh --locked` workspace gate (CI parity).

Downstream proof per charter: startup, reuse, shutdown, restart, and cleanup behavior for lifecycle changes is covered by the existing suite plus S6; Hub-process shutdown evidence and durable-session cleanup evidence stay separate (the guard never deletes durable state, only reaps processes).

## Vault gaps worth capturing

- Bare `std::process::Child` daemon handles in integration harnesses leak trees on panic; panic-safe guards must be the only spawn path (new gotcha).
- Launch-result temp files keyed by wall-clock seconds collide across concurrent supervisors (new gotcha, fixed here).
- Bind-and-drop ephemeral port reservation is a TOCTOU that mimics the product's occupied-port failure (new gotcha).
- Reaping helpers that panic inside `Drop` convert one test failure into a suite-wide `SIGABRT` (new gotcha).
- `loaded-daemon-lifecycle.yml:157` prebuilds `-p botster-core` while every other site builds `-p botster-core-daemon` (drift; follow-up candidate).
