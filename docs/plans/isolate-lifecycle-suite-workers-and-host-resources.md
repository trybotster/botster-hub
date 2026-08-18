# Isolate lifecycle-suite workers and host resources

- Ticket: `ticket_1787011770_110683` — Hub test harness: isolate lifecycle-suite workers and host resources
- Run: `run_1787013171_779998`, step `botster_stack_plan`
- Target repository: **botster-hub** (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` `e864c3c8bbfb74068de21bd2ae9b843dbf0ccda7`, merged into the plan branch as `2b5c0f4` (base history: revision 2 declared `66ca79c`; revision 3 merged `c1ce7e5` as `25b7553`; revision 5 merged `e864c3c`)
- Date: 2026-08-17
- Revision 5. Addresses Plan Review `review_1787026660_890328`: finding_1787026660_168050 (stale Core/Ghostty base) and finding_1787026660_235426 (checklist revision recording). Revision 4 addressed `review_1787026289_143951` (host-wide dev-artifact dirty rule); revision 3 addressed `review_1787015591_731805`; revision 2 addressed `review_1787014932_417965`.

## Fresh-base verification (revision 3)

- `git fetch origin main` → `origin/main` = `c1ce7e525aef080e10eee79a306482d5bfc66860` ("Merge ticket: Hub tests: fix unix adapter unbound printf lifecycle flake" = sibling `ticket_1786937228_425608`, now **merged**).
- Merged into the plan branch: commit `25b7553`, clean ort merge. Upstream delta touches only `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` (+211/−15) plus that sibling's plan/report docs. No file this plan cites for line references changed (`sessions.rs`, `shutdown.rs`, `common.rs`, `cli.rs`, `process.rs`, `webrtc_*`, `package_fixtures.rs`, `src/entrypoint_supervisor.rs` are byte-identical to the revision 2 base), so all cited line references remain valid.
- Baseline on the new base: after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, one `./test.sh --locked --test hub_daemon_lifecycle_test` run. Result recorded below (see "Baseline result").

### Revision 5 base advance (origin/main `e864c3c`)

- `git fetch origin main` → `origin/main` = `e864c3c8bbfb74068de21bd2ae9b843dbf0ccda7`; merged into the plan branch as `2b5c0f4` (clean ort merge).
- The `c1ce7e5..e864c3c` delta is a coordinated pin roll plus one workflow fix, 26 lines total:
  - Core pin: `botster-core`, `botster-core-daemon`, `botster-terminal-protocol`, `botster-core-test-support`, `botster-terminal-ghostty` roll from rev `fc541a5` to rev **`fd66efd`** (`Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/*`). The Ghostty ABI rides the same Core rev roll.
  - Provenance strings only in four test files (`webrtc_terminal_adapter.rs:986`, `unix_terminal_adapter.rs`, `package_event_plane.rs`, `session_projection_owner_loop.rs`): the embedded `locked_core=` SHA. **None of the lines this plan cites drifted** (`webrtc_terminal_adapter.rs` `:151`, `:165-175`, `:684`, `webrtc_proofs.rs:905`, and all `sessions.rs`/`shutdown.rs`/`common.rs`/`cli.rs`/`process.rs` citations are untouched).
  - `.github/workflows/loaded-daemon-lifecycle.yml:157` now prebuilds `-p botster-core-daemon` — the prebuild drift this plan carried as a vault gap is **fixed upstream** and removed from the gap list.
- Core `fd66efd` API verification for the S1 backstop: recorded under "Baseline result" after the prebuild against the new rev (the harness compiles `SessionRegistry`/`RegistryRecord`/`ProcessIdentity` from the pinned checkout, so a successful `--locked` prebuild plus source check is the verification).

## Playbooks and notes loaded

- Repository playbook: [[botster-hub-playbook]]
- Role playbooks: [[planner-playbook]], [[botster-planner-playbook]]
- Class overlay: [[botster runtime teardown lenses]] (class applies; answers below)
- Targeted atomic notes:
  - [[hub shutdown preserves durable session workers]] (defines the ordered two-layer cleanup contract for S1)
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

- Repo surfaces read: `test.sh`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_daemon_lifecycle/*` (helpers and the nine cited failure sites), `tests/support/mod.rs`, `script/process-census`, `script/run-loaded-daemon-lifecycle`, `script/probe-hub-resources`, `docs/loaded-daemon-lifecycle-runner.md`, `.github/workflows/ci.yml`, `src/entrypoint_supervisor.rs`, `src/local_webrtc.rs`, `src/main.rs` (up/web probe path), `crates/botster-hub-installer/src/run.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/isolated_hub.rs`; revision 3 also re-read the merged `unix_terminal_adapter.rs` delta and the on-disk `SessionRegistry`/`RegistryRecord` usage (`sessions.rs:2531-2545`, `shutdown.rs:2200-2280`).
- Sibling tickets read to pin boundaries: `ticket_1786938984_190098`, `ticket_1786977409_499180`, `ticket_1786937228_425608` (now merged into base), `ticket_1786912572_610381`, `ticket_1786912569_840742`.
- Plan Review `review_1787014932_417965` and `review_1787015591_731805` findings read and addressed.

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

Existing assets to reuse, not rebuild: `PanicSafeCliDaemon` (already lists and shuts owned sessions on panic at `cli.rs:436` — the seed of the S1 guard), `ReapingChild`, `terminate_and_reap_pty_child`, `OwnedOperatorConsoleDaemon::cleanup` (identity-revalidated), `wait_for_entity_frame`, `wait_for_child_condition_with_budget` (reaps on budget expiry), the on-disk `SessionRegistry`/`RegistryRecord` store under each data dir (`botster-core-daemon`, already a harness dependency), and `script/process-census` (two-arm live/zombie census with positive controls, Darwin + Linux).

## Scope

Smallest set of changes that satisfies every ticket invariant:

**S1. Two-layer, data-dir-scoped cleanup guard for every lifecycle-suite daemon child.**
Make the `start_cli_daemon` family return one panic-safe guard (extend `PanicSafeCliDaemon`, whose panic path already does session shutdown at `cli.rs:436-450`). The guard's cleanup scope is the test's isolated data directory, and it runs the ordered contract from [[hub shutdown preserves durable session workers]] on **success, panic, and timeout** alike:
1. Enumerate every session in the owned data directory (bounded production `ListSessions` over the daemon socket, while the daemon is still reachable).
2. Call the production `ShutdownSession` path for every nonterminal session (exact-session classification preserved; typed `Found`/`Absent`/`Err` results recorded, not swallowed).
3. Stop and reap the Hub child (existing `shutdown` request, then bounded TERM → 500 ms → KILL → 2 s on the Hub's process group as backstop).
4. Prove, with separate live oracles, that the exact owned session workers, their PTY process groups, and the daemon socket no longer survive, and that the Hub child was reaped (`waitpid` complete, no zombie). If `ShutdownSession` reported success but a worker, group, or socket survives, the guard fails the test with that typed attribution (a Core lifecycle defect signal, per the vault note), never with a silent broad kill.

*Dead-daemon backstop identity (finding_1787015591_794130).* When the daemon is unreachable (killed, crashed, or shutdown request fails), steps 1-2 degrade to a **registry-backed identity source**: a new helper `registry_backed_worker_identities(data_dir)` in `tests/hub_daemon_lifecycle/process.rs` reads the on-disk session registry under the guard's own data directory via `SessionRegistry::new(data_dir)` (already imported by every harness module; records carry `ProcessIdentity { pid, runtime_id }` — see the forged-record prior art at `sessions.rs:2531-2545` and `shutdown.rs:2200-2280`) and collects the worker PID of every nonterminal record. Each PID is revalidated against the worktree `botster-session-worker` executable identity (`ps -p <pid> -o command=` realpath check, the `operator_console_fixtures.rs:301-317` pattern) before its `setsid` group receives bounded TERM/KILL. A record without a PID degrades to the exact data-dir-scoped census (`capture_new_session_workers_for_data_dir` with the worktree-wide fallback removed — S3), which attributes strictly by data-dir marker. Call sites: guard drop paths only. Negative test (S6): foreign-worker adoption — with two guard-owned daemons A and B each holding one worker, SIGKILL daemon A; guard A's backstop cleans only A's registry-listed worker while B's daemon and worker stay live and functional.

*Suite taint latch (finding_1787015591_965278).* If the guard cannot **prove absence** of any owned child within its bounds, it records the survivor evidence and sets a process-wide fail-closed latch (`HARNESS_TAINT`, a monotonic set-once cell holding the taint evidence). Every real-daemon acquisition point — `daemon_test_guard()`, the `start_cli_daemon` family, and the isolated-hub start helpers — checks the latch **before spawning any resource** and panics immediately with a typed `environment_tainted: <original evidence>` harness error, without starting a daemon and without running product assertions. Later tests therefore fail fast with taint attribution instead of running against a contaminated host; the wrapper (S5) reports them under the taint, not as independent product failures. This latch is deliberately process-global and is compatible with [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]: it is a monotonic fail-closed latch that only converts runs to failures, never a zero-wait counter that a concurrent test could satisfy or starve. Deterministic test (S6): inject a prove-absence failure through a guard test hook and prove the next daemon start refuses without spawning (no new daemon process appears in the census).

The guard never uses process-name-wide kills; identity is exact PID + `setpgid` group (or the worker's own `setsid` group id) + data-dir marker, revalidated before any signal. Restart-durability tests (for example `cli_daemon_restart_recovers_worker_backed_session_through_transport`, `daemon_restart_reconnects_worker_backed_session_through_client_api`) keep their semantics through an explicit `transfer`/`disarm_sessions` guard mode that intentionally skips step 2 across the restart boundary and re-arms on the successor daemon; the intent is visible at the call site.
Also: give the direct node spawn at `webrtc_proofs.rs:22` its own process group and make `ChildCleanup::drop` (`process.rs:182-189`) kill the group, not only the direct child.

**S2. Unwind-safe reaping.**
Split `terminate_and_reap_child` into a `Result`-returning core. Drop paths log, record taint when absence cannot be proven, and continue (never panic during unwind); direct call sites keep asserting. This removes the double-panic `SIGABRT` path.

**S3. Unique per-test host resources.**
- Add `std::process::id()` to `unique_test_dir` and `unique_short_test_dir`.
- Replace the hard-coded `/tmp/bh-slc` root (`sessions.rs:2146`) with `unique_short_test_dir`.
- Uniquify `launch_result_path` in `src/entrypoint_supervisor.rs` (add supervising-process id plus a per-process monotonic counter to the file name). This is the **only production code change** in this plan. Justification: the path is generated by the supervisor and passed to the child via `BOTSTER_ENTRYPOINT_LAUNCH_RESULT`; same-second collisions are possible for any two concurrently supervising daemons, not only tests. The env override and the readiness watcher contract stay unchanged.
- Remove the bind-and-drop port reservation where the fixture supports it: pass `BOTSTER_WEB_PORT=0` and derive the origin from the recorded `local_url` after the server binds (this is also the vault contract: origin is requested after bind). Keep held-listener reservations only where a test deliberately proves occupied-port behavior.
- Bound `capture_new_session_workers_for_data_dir` (`process.rs:389-463`) to exact data-dir attribution; drop the "any live worker from this worktree" fallback so one test never adopts another test's worker.

**S4. Host-exhaustion classification with a deterministic, fail-toward-product rule.**
Two layers, with fixed precedence (finding_1787014933_248609, tightened per finding_1787015591_733333):

*Per-failure typed markers.* Budget-expiry paths in the shared wait helpers emit a structured, machine-parseable marker line in the panic message: `harness_budget_expired kind=<wait-kind> budget_ms=<n> resource=<class or none> probe=<confirmed|unconfirmed|n/a>` plus captured evidence. Classification of the `resource` field:
- **Unambiguous exhaustion evidence** (marker alone suffices): `EMFILE` or `ENFILE` from the failing operation, or a PTY allocation failure whose errno from `posix_openpt`/`openpty` (surfaced through the `TerminalBackendConstruction`-shaped `spawn_failed` body) is `EMFILE`, `ENFILE`, or `EAGAIN`-from-the-PTY-allocation-call. Implement enumerates the exact PTY errno set from the Core error body during S4 work and freezes it in the marker code.
- **Ambiguous errors are NOT exhaustion evidence**: `EAGAIN`/`EWOULDBLOCK` on a socket read (an ordinary `SO_RCVTIMEO` expiry — the tolerant reader already treats it as retryable) and `ETIMEDOUT` on a readiness socket (the product may simply never have become ready). These classify as `product_failure` **unless** a same-operation causal probe, run at marker-emission time, independently confirms the specific resource limit: for fd-class evidence, the process's open-fd count is at `RLIMIT_NOFILE` minus a fixed small margin; for PTY-class evidence, an immediate PTY allocation probe fails with the same errno. Only `probe=confirmed` upgrades an ambiguous error.
- Ambient observations (load average, census count, fd totals) are attached as evidence but are **never classification inputs on their own**.

*Host-wide dev-artifact dirty rule (finding_1787026289_192240).* The exact-path interface of `script/process-census assert-no-live-executables` can only see this worktree's binaries, so it cannot detect the foreign-worktree contamination that starves a suite. The pre-run dirty check therefore uses a new, bounded, host-wide predicate added to `script/process-census` as a `dev-artifact-rows` mode. A live process is a **Botster dev artifact** when both hold:
1. its command matches a botster-role name (`botster-hub`, `botster-session-worker`, or a harness fixture command carrying a harness data root), and
2. its executable path or argv contains a cargo build-output segment (`/target/debug/`, `/target/managed-install-proof/`) **or** a harness data root (`/tmp/bh-`, `botster-hub-test-data`).
These segments are harness-and-build-only by construction, in any worktree, so the rule detects other worktrees' leftovers without name-only matching. **Ownership/allowlist inputs, named:** processes whose executable path lacks every dev-artifact segment are valid host services and are never flagged — this exempts installed prefixes (for example `~/.local/share/...` generations) and the legacy `~/Rails/trybotster` runtime that serves the operator's live Hub. The wrapper's own not-yet-started suite tree is excluded by capture order (the scan runs before the suite starts). The wrapper never kills a flagged foreign process; it refuses with a pid/path evidence TSV, and the cross-worktree kill decision stays with the operator/orchestrator. Required census self-tests (positive and negative, across two distinct fake worktree paths): (a) a marker child executed from `<tmpA>/target/debug/botster-session-worker` and another from `<tmpB>/target/debug/botster-session-worker` are both flagged; (b) the same binary executed from an installed-prefix-shaped path with no dev-artifact segment is not flagged; (c) an unrelated process that merely mentions `botster` in its argv without a role name + dev-artifact segment is not flagged.

*Wrapper verdict rule (deterministic, ordered).* `script/run-lifecycle-suite` classifies in this precedence order; the first matching rule wins:
1. `environment_dirty` — the pre-run check found live Botster dev-artifact processes anywhere on the host (the rule above) or a nonempty botster-role zombie delta **before** the suite started. The wrapper refuses to run and emits the evidence TSV; an explicit operator override records `environment_dirty_forced` alongside whatever result follows.
2. `product_failure` — any failed test whose panic does **not** carry the `harness_budget_expired` marker, carries a marker with `resource=none`, or carries an ambiguous resource without `probe=confirmed`, or any test that fails a semantic assertion. A product assertion always outranks host pressure: high ambient load never reclassifies a semantic failure.
3. `host_exhaustion` — every failed test carries the marker with unambiguous resource evidence or `probe=confirmed`.
4. `clean` — zero failures, exactly one tally, and both post-run census arms empty.
`environment_tainted` failures emitted by the S1 latch are grouped under their originating taint, never counted as independent product failures. Survivors after the run fail the command regardless of the verdict above (fail-closed; the verdict is annotated `survivors_present`).
Deterministic classifier tests (required; they exercise the **real error-to-marker path**, not injected marker strings):
(a) real `EMFILE`: a child harness process lowers `RLIMIT_NOFILE`, exhausts fds, drives the failing operation, and the marker carries `EMFILE` → `host_exhaustion`;
(b) negative: an `EAGAIN` socket-read stall (daemon deliberately withholding a frame) classifies `product_failure`;
(c) negative: an `ETIMEDOUT`/readiness stall (child never becomes ready) classifies `product_failure`;
(d) pre-run dirty environment → `environment_dirty` before any suite run.
Also in S4: route the cited single-shot entity reads through the tolerant reader with the module's proven patience bound: `sessions.rs:3873` and `:4051` (`.expect("spawn upsert")`), plus the identical bare `next_frame().expect(...)` single-shot sites in `sessions.rs` and `webrtc_proofs.rs:905`. Keep every semantic assertion identical; only the read discipline changes. Do not touch `sessions.rs:3458`'s oracle (already tolerant; if it still fails after isolation, that is a product signal for a separate ticket). Surface the typed error body in `start_webrtc_adapter_hub` (`webrtc_terminal_adapter.rs:165-175`) and in `install_real_release`'s failure output, per [[flake oracles over typed response frames must print the full typed error body]].

**S5. One-command / one-tally suite wrapper with survivor proof.**
Add `script/run-lifecycle-suite` (thin, Darwin+Linux, reusing `script/process-census`):
1. run the host-wide dev-artifact scan (S4 rule) and capture a pre-run zombie baseline, plus a live-executable paths file for this worktree's binaries (hub, session-worker, node fixture, console),
2. apply the classification rule from S4 (pre-run dirty refusal first),
3. run exactly one `./test.sh --locked --test hub_daemon_lifecycle_test "$@"` after the session-worker prebuild,
4. assert exactly one result tally in the captured output,
5. post-run: `assert-no-live-executables` on this worktree's exact paths (owned-survivor arm), `assert-no-new-zombies` against the baseline, and a host-wide dev-artifact delta against the pre-run scan (foreign-leak arm) — all with the census's bounded settle window,
6. emit one structured verdict per the S4 rule (with taint grouping and `survivors_present` annotation).
No arbitrary sleeps; the settle loop is the census's existing bounded retry.

**S6. Deterministic injected-failure cleanup proof.**
New focused tests beside the existing prior art (`shutdown.rs:417` proves the timeout path already):
- a test that spawns a guard-owned daemon with a worker-backed session, panics inside `catch_unwind` while holding the guard, then proves the four ordered layers separately: (1) `ShutdownSession` was issued and typed-classified, (2) the exact worker PID and PTY process group are gone, (3) the daemon socket is absent, (4) the Hub child was reaped with no new zombie — all before the test returns;
- the same shape for an injected **timeout** (budget-expiry path through `wait_for_child_condition_with_budget`), for an operator-console-owned daemon, and for a supervised node entrypoint;
- dead-daemon backstop: SIGKILL the daemon, then prove `registry_backed_worker_identities` cleanup reaps the exact registry-listed worker; the foreign-worker adoption negative control (daemons A/B) proves no cross-adoption;
- taint latch: inject a prove-absence failure through the guard test hook; prove the next daemon start panics `environment_tainted` **without spawning** (census shows no new daemon), and that the wrapper groups the tainted failures under the originating test;
- a restart-durability control: the `transfer` guard mode keeps the worker alive across an intentional Hub stop and the successor guard cleans it up (proves S1 does not break production durability semantics);
- red-on-revert: with the guard disarmed, the census check must fail (pattern from `script/process-census --self-test`).

## Non-scope

- `ready_spawn_*` wall-clock pair and session-projection completion: `ticket_1786938984_190098`.
- `ShutdownSession` `OperatorError` idempotency across natural exit: `ticket_1786977409_499180`.
- `unix_adapter_unbound_printf_stream_attach_completes`: owned by `ticket_1786937228_425608`, whose repair is now **merged into this plan's base** (`c1ce7e5`); this plan consumes it and must not modify `unix_terminal_adapter.rs` semantics.
- PTY PID/marker lifecycle oracles: `ticket_1786912572_610381`; owner-loop scheduler: `ticket_1786912569_840742`.
- Production budgets stay fixed: Core `WORKER_STARTUP_TIMEOUT` (2 s), installer `RUN_DEADLINE` (10 s), `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` (30 s), `LAUNCH_RESULT_READINESS_BUDGET` (15 s), `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, `OBSERVE_SLICE_BUDGET`.
- No botster-core changes. No changes to the Linux-only loaded-runner campaign (`script/run-loaded-daemon-lifecycle`, workflow); its `-p botster-core` vs `-p botster-core-daemon` prebuild drift is recorded as a vault gap, not fixed here.
- No `--test-threads=1`, no `serial_test`, no nextest: repo policy forbids serialization as acceptance evidence.
- No semantic weakening of any product assertion. No change to production `ShutdownSession` semantics (that contract belongs to `ticket_1786977409_499180`; the guard only *calls* the production path).

## Ownership boundaries and cross-repo dependencies

- All changes live in botster-hub: `tests/hub_daemon_lifecycle/*`, `tests/support/`, `script/`, and one bounded `src/entrypoint_supervisor.rs` naming change. Hub owns its harness and its supervisor policy.
- `spawn_failed` budgets and worker readiness live in botster-core (pinned rev `fd66efd` as of base `e864c3c`); this plan treats them as fixed contracts. The registry-backed backstop only **reads** the on-disk `SessionRegistry` through the existing `botster-core-daemon` API; it does not change Core. If the S6 proofs show `ShutdownSession` success while a worker, PTY group, or socket survives, that is a Core session-lifecycle defect: stop and register a botster-core dependency ticket; do not patch around it here.
- `hub-test-support` (crate and npm 0.1.37 / conformance revision 43) is consumed read-only; no fixture-byte mutation under a published version.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes. The ticket is daemon/session-worker/WebRTC-runtime/console child teardown in the suite harness, plus FD/PTY/CPU spin from leaked children.
- `teardown_isolation`: the per-test ownership set is {CLI daemon child, its embedded CoreDaemon, its durable session workers and their PTY groups, its supervised node entrypoint, its operator-console PTY child and detached console daemon, its WebRTC peer runtime}. One failed test's cleanup acts only on its own identity-revalidated PIDs/groups inside its own data-dir scope. Healthy sibling tests and their children are never swept by name.
- `teardown_bounds`: session cleanup uses bounded production requests (`ListSessions`, `ShutdownSession`, `shutdown`) with the harness's existing wait budgets; process backstops use the bounded TERM→500 ms→KILL→2 s sequence; drop paths never panic (S2); no unbounded `block_on(close)` is added; census settle loops are bounded.
- `late_message_matrix` — every ownership-creating runtime request the affected tests issue, with owner tag, post-teardown rejection, race sweep, bound/hard stop, and live production-path proof:

| Ownership-creating request | Owner identity (tag) | Rejection after teardown | Race sweep | Bound and hard stop | Live production-path proof |
|---|---|---|---|---|---|
| `Spawn` / `SpawnSessionType` (session + durable worker + PTY) | session id in Core registry; worker PID from the on-disk `RegistryRecord` `ProcessIdentity` + its own `setsid` group; data-dir scope | requests against a stopped daemon fail on the dead socket (unique path dies with the data dir); Core rejects operations on exited sessions with typed errors | guard step 1-2: bounded `ListSessions` then exact-session `ShutdownSession` per nonterminal session before Hub stop, on success/panic/timeout; typed `Found`/`Absent`/`Err` preserved per [[host ShutdownSession classification must call the exact-session Core query]]; dead-daemon path uses `registry_backed_worker_identities(data_dir)` with per-PID identity revalidation | production shutdown first; TERM→KILL on the exact worker group only if the production path fails or the daemon is dead, with typed attribution | S6 proofs: worker PID gone, PTY group gone, socket absent, distinct from Hub-child reap evidence per [[worker shutdown completion requires lifecycle transport and process termination]]; foreign-worker adoption negative control |
| Terminal `Attach` (subscription route) | session + subscription + connection; Unix EOF and PeerClosed occupancy share the live attach route set ([[Unix EOF occupancy must share the live attach route set]], [[PeerClosed attach occupancy must use the live attach route set]]) | a closed connection's routes are removed by the production EOF/PeerClosed path; late attach on a dead daemon fails on the dead socket | guard closes client sockets before daemon shutdown so production route cleanup runs; route-set occupancy is the oracle | production cleanup path; daemon stop bounds the rest | existing occupancy oracles in the suite; S6 asserts no live attach subscriptions in the pre-stop status probe |
| `SubscribeEntities` (entity subscription) | connection-scoped subscription id ([[Client event holders are connection-scoped]]) | dead socket; daemon cleanup counters advance on disconnect | dropping the client socket triggers production disconnect cleanup (cleanup_completed observed by `script/probe-hub-resources` pattern) | daemon-side production cleanup; bounded by daemon stop | pre-stop status probe shows zero live entity subscriptions in S6 |
| WebRTC bootstrap grant + peer/DataChannel | single-use origin-bound grant; peer id in the daemon's peer map; test-side peer on `default_runtime` | grants die with the daemon; late channel traffic hits the bounded peer-close path ([[WebRTC DataChannel local close uses the peer close bound before cleanup]], 200 ms test bound) | test drops its peer before guard shutdown; daemon `stop_all` drops the dedicated runtime at stop | bounded peer close; runtime drop is the hard stop | instance-scoped worker census (`src/local_webrtc.rs:133-137`) already proves runtime park in lib tests; suite-level live census proves no runtime thread survives the daemon child |
| `StartPackageEntrypoint` (supervised node server) | supervisor-owned child group in the daemon; uniquified launch-result path (S3) | supervisor rejects status/bootstrap for a non-running entrypoint with typed `local_webrtc_bootstrap_*` errors | late launch-result write from a dying node child lands in its dead unique file and is never re-adopted (S3) | `SupervisedProcess::stop`: TERM → 500 ms → KILL → 2 s on the group (`src/entrypoint_supervisor.rs:423-470`) | S6 supervised-entrypoint case: node child and its group gone after guard cleanup; collision regression test for the path |
| Operator console open (PTY child + detached daemon) | console PTY child group; detached daemon PID from metadata, command-line revalidated before signal (`operator_console_fixtures.rs:301-317`) | `OwnedOperatorConsoleDaemon::new` refuses a data dir with a live daemon | console `Drop` reaps the PTY group; owned-daemon cleanup shuts down, then bounded TERM/KILL per PID | bounded per-PID 2 s waits, then KILL | S6 console case: socket-gone, status-stopped, metadata assertions plus census absence |
| Installer lease (`flock` on `<prefix>/daemon.lock`) | kernel lock owned by the holding process | released by the kernel on process death | none needed beyond process reap | process reap bounds it | existing `real_daemons_on_custom_data_directories_hold_the_installation_lease` SIGKILL-release proof stays green |

  A process census is an oracle over the result, never the cleanup mechanism; cleanup always goes through the production requests above with the OS group kill as bounded backstop.
- `production_path_proof`: S6 drives the guard's real panic/timeout paths and proves each ownership layer with live oracles (worker PID + PTY group + socket + Hub child, separately), plus the two-arm census with positive controls; the one production change (launch-result path) is proved through the real `StartPackageEntrypoint` readiness path plus a collision regression test. Terminal records are never accepted as teardown proof.
- `ownership_identity`: exact child PID plus process group created via `setpgid(0,0)` (or the worker's own `setsid` group id, read from the data dir's `RegistryRecord`) plus unique data-dir marker; kill only after re-validating the PID's command line so a reused PID is never signaled.
- `sibling_fail_closed_policy`: on successful cleanup, siblings are untouched. If the production `ShutdownSession` path succeeds but a worker survives, the guard fails the test with typed Core-lifecycle attribution. If a guard cannot prove absence of its children within bounds, it sets the suite taint latch: later real-daemon tests fail fast as `environment_tainted` without spawning, so a contaminated host can never silently change a later product assertion (ticket invariant 4). The suite wrapper fails the whole command fail-closed on the survivor census without killing non-owned processes (evidence over broad kills).

## Assumptions and unknowns

- Assumption: the three tallies in the discovery log came from three suite invocations under external retry, not from one `cargo test` run; the wrapper (S5) makes this impossible to conflate by asserting one tally per command. Implement verifies `cargo test --workspace --test hub_daemon_lifecycle_test` runs exactly one test binary.
- Assumption: the launch-result uniqueness fix is in-scope production work under the ticket invariant "each test owns unique … worker resources"; it is flagged for Plan Review as the single production seam.
- Assumption: the on-disk `SessionRegistry` under the data dir reliably lists worker `ProcessIdentity` for nonterminal sessions (the suite already forges and reads such records at `sessions.rs:2531-2545` and `shutdown.rs:2200-2280`); Implement verifies the write timing on the real Spawn path before relying on it, and falls back to the exact data-dir census if a record lags.
- Unknown: which of the 14 `unused_loopback_port` sites can move to `BOTSTER_WEB_PORT=0` without changing what the test proves. Implement audits per site; sites proving occupied-port behavior keep their held listener.
- Unknown: exact guard-adoption mechanics at each of the ~107 `start_cli_daemon` sites. The compiler enumerates them; the guard must keep `shutdown_cli_daemon(data_dir, child)` flows working via disarm/into-inner, and restart tests use the explicit `transfer` mode.
- Unknown: the exact PTY-allocation errno set on Darwin vs Linux for the unambiguous-exhaustion list; Implement enumerates it from the Core `TerminalBackendConstruction` error body and freezes it in the marker code with a unit test per platform arm.
- Unknown: Darwin census cost at suite boundary; expected negligible (two `ps` sweeps per command, none per test).

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/cli.rs` — guard-returning `start_cli_daemon` family; `PanicSafeCliDaemon` extension (ordered two-layer cleanup, transfer mode, taint latch check).
- `tests/hub_daemon_lifecycle/common.rs` — unique dirs (+pid), port helper policy, unwind-safe reap core, `harness_budget_expired` marker emission with typed resource evidence and causal probes, taint latch storage.
- `tests/hub_daemon_lifecycle/process.rs` — `ReapingChild`/`ChildCleanup` group semantics; `registry_backed_worker_identities(data_dir)`; census attribution fallback removal.
- `tests/hub_daemon_lifecycle/sessions.rs` — tolerant reads at the cited and sibling single-shot sites; `/tmp/bh-slc` root fix.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`, `webrtc_proofs.rs` — typed-error surfacing; node spawn group; `:905` read.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — installer failure diagnostics passthrough.
- `tests/hub_daemon_lifecycle/shutdown.rs` (or a new sibling file) — deterministic injected-failure cleanup tests (panic, timeout, console, supervised entrypoint, dead-daemon backstop + foreign-worker control, taint latch, restart-durability control).
- `tests/support/mod.rs` — shared helpers if guard plumbing needs them.
- `src/entrypoint_supervisor.rs` — `launch_result_path` uniqueness + unit regression test.
- `script/process-census` — new `dev-artifact-rows` mode (host-wide dev-artifact predicate) plus its positive/negative self-test arms across two fake worktree paths.
- `script/run-lifecycle-suite` — new wrapper with the deterministic classifier; reuses `script/process-census`; classifier tests (real EMFILE positive, EAGAIN/ETIMEDOUT negatives, dirty-environment).
- `README.md` / `docs/` — short harness-contract note for the wrapper and the classification rule (placement per repo prior art).

## Risks

- ~107 mechanical call-site conversions can change test flow subtly (double kill, changed drop order). Mitigation: guard disarms on explicit shutdown; reaping is idempotent by PID identity; convert file-by-file with compilation as the enumerator.
- Guard session-cleanup on the panic path adds daemon round-trips during unwind. Mitigation: bounded requests, and the dead-daemon degradation path keeps unwind short.
- The `transfer` mode could be misused to skip cleanup. Mitigation: it requires a successor guard at the call site, and the S6 restart-durability control plus the suite-level census catch a dropped hand-off.
- A spurious taint latch could fail the rest of a suite run. Mitigation: the latch sets only when absence cannot be proven after the full bounded backstop; the taint evidence names the owning test; the wrapper reports one taint root instead of cascade noise — strictly better than today's silent contamination.
- Registry records could lag the real Spawn (worker pid missing when the daemon dies immediately after Spawn). Mitigation: the backstop falls through to the exact data-dir census; the S6 dead-daemon test covers the SIGKILL-right-after-spawn window.
- The port-0 migration can weaken a test that intended to pin the origin before bind. Mitigation: per-site audit; the vault contract already requires origin after bind.
- The launch-result rename touches production supervision. Mitigation: env override unchanged; watcher watches the exact generated path; regression test for two supervisors in one second; full workspace gate.
- Drop-time logging instead of panicking (S2) could hide a real cleanup failure. Mitigation: unprovable absence sets the taint latch and fails the run fail-closed; the suite wrapper's survivor census fails the command whenever anything survives, so silence cannot pass.
- Classifier false `host_exhaustion` claims. Mitigation: ambiguous timeout errors (`EAGAIN`/`EWOULDBLOCK` socket reads, `ETIMEDOUT` readiness) classify as `product_failure` unless a same-operation causal probe confirms the limit; only `EMFILE`/`ENFILE`/PTY-allocation errnos stand alone; negative tests lock this in.
- Host-wide dev-artifact scan could misflag a valid service or miss a leak. Mitigation: the predicate flags only role name **plus** a build-output or harness-data segment, so installed and legacy services are exempt by path shape, and any worktree's test binaries match by construction; the two-worktree positive and installed-prefix negative self-tests prove both directions, and the wrapper refuses-and-lists instead of killing foreign processes.

## Acceptance checks and tests

Primary (deterministic, focused; no arbitrary sleeps, no repeated full suites):
1. New injected-failure cleanup tests (S6) pass, proving the four ordered layers separately on the panic path and the timeout path: `ShutdownSession` issued and typed-classified → exact worker PID and PTY group gone → daemon socket absent → Hub child reaped without zombie. Includes the operator-console and supervised-entrypoint cases, the dead-daemon registry-backed backstop with the foreign-worker adoption negative control, the taint-latch test (next daemon start refuses without spawning), the restart-durability `transfer` control, and red-on-revert with the guard disarmed.
2. `src/entrypoint_supervisor.rs` collision regression test: two same-second launch-result paths are distinct; supervised readiness still works through a real `StartPackageEntrypoint` flow.
3. Classifier determinism tests through the real error-to-marker path: real `EMFILE` (child with lowered `RLIMIT_NOFILE`) → `host_exhaustion`; `EAGAIN` socket-read stall → `product_failure`; `ETIMEDOUT` readiness stall → `product_failure`; pre-run dirty environment → `environment_dirty` before any suite run.
3a. Host-wide dev-artifact census self-tests (in `script/process-census`): marker children under two distinct fake worktree `target/debug/` paths are both flagged; the same binary from an installed-prefix-shaped path is not flagged; a `botster`-mentioning process without role name + dev-artifact segment is not flagged.
4. Repeated focused runs without cooldown, back-to-back, census-clean between repetitions:
   - `./test.sh --locked --test hub_daemon_lifecycle_test process_ownership_` × 5
   - `./test.sh --locked --test hub_daemon_lifecycle_test cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops -- --exact` × 5
5. `script/process-census --self-test` passes (both arms keep their positive controls).
6. `script/run-lifecycle-suite` self-evidence: one command → one suite process, one tally, no live survivors, no new zombies, taint grouping, and the `environment_dirty` verdict when seeded with a marker process.

Final smoke (exclusive, once):
7. After `cargo build --locked -p botster-core-daemon --bin botster-session-worker`: one exclusive `script/run-lifecycle-suite` run of `hub_daemon_lifecycle_test` with zero failures and a `clean` verdict.
8. One full `./test.sh --locked` workspace gate (CI parity).

Downstream proof per charter: startup, reuse, shutdown, restart, and cleanup behavior is covered by the existing suite plus S6. Hub-process shutdown evidence and durable-session cleanup evidence are produced and asserted **separately** (guard step 3 vs step 4 oracles), per the charter gate and [[hub shutdown preserves durable session workers]]; the restart-durability control proves the guard does not erase production durability semantics.

## Baseline result (fresh base)

Recorded from the revision 3 baseline run on merge commit `25b7553` (origin/main `c1ce7e5`), after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, command `./test.sh --locked --test hub_daemon_lifecycle_test`:

- Prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker` completed (01:15:12Z → 01:17:13Z).
- Suite: `./test.sh --locked --test hub_daemon_lifecycle_test` started 2026-08-18T01:17:13Z as one suite process and was **stopped at 2026-08-18T04:03:16Z (exit 101) after 2 h 46 m of starved progress**: 9 tests passed, 1 failed (`buffered_child_stdout_wait_observes_backpressure_condition`), and four `cli_local_runtime_*` tests were each pinned past 60 s. Estimated completion at that rate was multiple days.
- Host evidence captured during the run (04:02:52Z): load average `{22.54 31.30 53.28}` (earlier `{33.50 34.09 55.39}`), and **226 botster-role processes host-wide**, attributed by path: 73 from the ambient `/Users/jasonconigliari/Projects/botster-hub` checkout, 64 from worktree `ticket_1786661010_198387`, 38 from `ticket_1786661008_634435`, 15 from `ticket_1786937228_425608`, 11 from `ticket_1786977409_499180`, 10+8+2+2 from four further ticket worktrees — and only 2–4 from this worktree. One leftover PTY producer shell carried a data-dir nanos stamp from 2026-08-11.
- Classification under this plan's own S4/S5 rule: **`environment_dirty`** — every one of the 222-224 foreign leftovers ran from a `<worktree>/target/debug/` or ambient `Projects/botster-hub/target/debug/` binary or carried a `/tmp/bh-` / `botster-hub-test-data` data root, so the host-wide dev-artifact predicate (S4) flags them all, while the 5 installed-prefix processes under `~/.local/share` are exempt as valid services. The pre-run refusal would therefore have stopped this run before it started. The run is recorded as invalid suite-environment evidence (the same disposition the merged sibling used for its "Run 4"), and it is direct live proof of the ticket's failure class: leftover workers from prior runs across other worktrees starving the current suite.
- Post-stop hygiene: the suite tree (runner → `test.sh` → cargo → test binary) was TERM/KILL-stopped; a census confirmed **no live survivor and no zombie attributable to this worktree's run** afterward.
### Revision 5 addendum (base `e864c3c`, merge `2b5c0f4`)

- Session-worker prebuild rerun on the new base: `cargo build --locked -p botster-core-daemon --bin botster-session-worker` compiled `botster-core-daemon` from rev `fd66efd` and finished cleanly in 1 m 09 s (exit 0 at 2026-08-18T04:20:19Z).
- Core `fd66efd` API verification for the S1 backstop: the pinned checkout carries `ProcessIdentity` (`crates/botster-core/src/runtime/mod.rs:136`), `RegistryRecord` with the `running` constructor (`crates/botster-core-daemon/src/registry.rs:29,65`), and `SessionRegistry::save` (`registry.rs:130,154`) — the registry-backed identity design is unchanged by the pin roll.
- Baseline disposition on the new base: **dirty-lane refusal recorded instead of another starved suite run** (per Plan Review's stated alternative). The host-wide dev-artifact scan at 2026-08-18T04:19:16Z flagged **231 dev-artifact rows** under the same foreign worktrees as the revision 3 census (72 ambient `Projects/botster-hub`, 64 `ticket_1786661010_198387`, 38 `ticket_1786661008_634435`, 15 `ticket_1786937228_425608`, 11 `ticket_1786977409_499180`, remainder across further worktrees) and left **26 role-named rows exempt** (no dev-artifact segment: installed/legacy services), at load average `{18.68 26.33 35.23}`. The lane is still dirty, so under this plan's own rule the suite must not run; the earlier starved-run record stands as the load evidence.
- Capture-method note for Implement: the quick evidence scan matched role names as substrings, which also matched two rows whose argv merely contained this worktree's `...-botster-hub-...` path; the census implementation must match roles by `comm`/basename (as `script/process-census` already does), which removes that noise. The required negative self-test ("`botster` mention without role name + dev-artifact segment") locks this in.
- Consequence for acceptance: the exclusive final smoke (acceptance check 7) must run through `script/run-lifecycle-suite`, whose host-wide pre-run dirty check makes this contamination a first-class refusal instead of a multi-day starved run. **Clean-lane definition:** the final smoke requires the host-wide dev-artifact scan green before the suite starts; the lane is obtained by the operator/orchestrator stopping the flagged foreign dev-artifact processes (they are build/test artifacts by construction, never valid services) — the wrapper itself only refuses and lists them, it never kills a non-owned process. A clean-host exclusive baseline is therefore deliverable evidence of Implement, not a precondition this Plan can obtain on the currently contaminated shared host.

## Vault gaps worth capturing

- Captured during this Plan: hub entrypoint supervisor launch-result temp-file collision on concurrent startup (vault inbox).
- Bare `std::process::Child` daemon handles in integration harnesses leak trees on panic; panic-safe guards must be the only spawn path (new gotcha).
- Bind-and-drop ephemeral port reservation is a TOCTOU that mimics the product's occupied-port failure (new gotcha).
- Reaping helpers that panic inside `Drop` convert one test failure into a suite-wide `SIGABRT` (new gotcha).
- Ambiguous timeout errnos (`EAGAIN`/`ETIMEDOUT`) are not host-exhaustion evidence without a same-operation causal probe (candidate after Implement proves the classifier).
- Exact-path survivor censuses cannot see foreign-worktree dev artifacts; detection needs a build-output/harness-root path predicate with comm-based role matching (candidate after Implement proves the census mode).
- ~~loaded-daemon-lifecycle.yml prebuild drift~~ — fixed upstream at base `e864c3c` (workflow now builds `-p botster-core-daemon`); no capture needed.
