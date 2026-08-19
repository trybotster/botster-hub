# Plan: replace PTY process PID and marker lifecycle oracles

Ticket: `ticket_1786912572_610381` — Hub tests: replace PTY process PID and marker lifecycle oracles.
Run: `run_1787136288_939918`. Base: `origin/main` (`0a3458a`).

This ticket replaces the process-fixture portion of superseded `ticket_1786875812_242946`. Work starts from current `main`. The superseded branch is not cherry-picked.

## Target

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- This ticket is test-only. It changes files under `tests/` and this plan document. It changes no file under `src/`, no Core pin, and no `packages/hub-test-support` content.

## Context loaded

- Repository playbook: [[botster-hub-playbook]].
- Role playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Class overlay: [[botster runtime teardown lenses]] (this ticket exists because terminal-state markers diverge from live-runtime session completion; answers are in the lens section below).
- Targeted atomic notes:
  - [[observed-exit waits must issue a production exact-session observe turn]]
  - [[host ShutdownSession classification must call the exact-session Core query]]
  - [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
  - [[flake oracles over typed response frames must print the full typed error body]]
  - [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
  - [[Hub session projection continues without subscribers or terminal Drain]]
  - [[Hub owner loop wakes only for mutations and pending resync]]
  - [[live acceptance tests must not depend on a loop tick window]]
  - [[a regression test must be shown to go red with the fix reverted]]
  - [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
  - [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
  - [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
  - [[real daemon start boundaries serialize against process global taint]]
  - [[session registry process pid identifies the pty command not the session worker]]
  - [[hub drain advances non attached session lifecycle]] (historical; Drain-based discovery stays forbidden)
- Repository code surveyed:
  - `src/daemon_transport.rs:3546-3548` — production `ReadScreen` calls `observe_session_lifecycle` for its exact `session_id`. This is the shipped exact-session observe stimulus.
  - `src/daemon_transport.rs:4738-4743` — production `ShutdownSession` classification calls `observe_session_lifecycle` for its exact `session_id` and returns typed `Cleanup` / `Missing` / `Stopping` / `Active` results.
  - `tests/hub_daemon_lifecycle/package_fixtures.rs:1254-1285` — `write_python_wait_then_write_script` and `write_python_split_utf8_script` (release-file-gated Python producers, exact bytes through `os.write(1, ...)`).
  - `tests/hub_daemon_lifecycle/sessions.rs:1194`, `:1252`, `:1349` — Unix finite-producer exact-byte tests; completion handled by `shutdown_short_lived_session` (accepts `Events` or `SessionCleanup`, so exit observation is never proven).
  - `tests/hub_daemon_lifecycle/webrtc_proofs.rs:304-401` — WebRTC exact-bytes test ends with a blind `ShutdownSession` under `assert_shutdown_strict_natural_exit`.
  - `tests/hub_daemon_lifecycle/webrtc_proofs.rs:494-631` — round-based WebRTC shutdown test uses a `(0..40)` `ListSessions` poll with 50 ms sleeps and then a soft dual-branch oracle.
  - `tests/hub_daemon_lifecycle/sessions.rs:3428-3489` — `shutdown_after_observed_exit_returns_session_cleanup` waits on `ListSessions` alone, which does not advance lifecycle.
  - `tests/hub_daemon_lifecycle/common.rs`, `session_fixtures.rs`, `mod.rs` — fixture module layout; test bodies are `include!`d from `tests/hub_daemon_lifecycle_test.rs`.
  - `test.sh`, `docs/lifecycle-suite-harness.md` — gate wrappers.
- Core pin: `8fce204` (Cargo.toml). The pin exposes `observe_session_lifecycle`; no Core change is required.

## Problem statement

The superseded branch tried to prove Core session completion with Python PIDs, descendant PIDs, and done files or markers written before process exit. None of those artifacts prove Core session completion: the PTY command can exit while the worker, registry, and journal still show the session as running, and a marker byte reaches the terminal plane before the process exits.

Current `main` no longer carries the PID and done-file oracles, but the finite-producer tests still lack a deterministic completion signal:

1. `shutdown_short_lived_session` accepts `Events` or `SessionCleanup`. The test never learns whether exit was observed. This is a soft residual.
2. The WebRTC exact-bytes test issues a blind `ShutdownSession` and demands `Events` or `SessionCleanup`. Under suite load a legal typed `OperatorError` (documented by the sibling contract in the same file) fails the test. See [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]].
3. The round-based WebRTC shutdown test polls `ListSessions` up to 40 times with 50 ms sleeps, then branches on whether exit happened to be observed. Sleep count acts as the oracle and non-determinism is preserved in the soft branch.
4. `shutdown_after_observed_exit_returns_session_cleanup` waits on `ListSessions` alone. `ListSessions` does not call `observe_session_lifecycle` and does not wake the owner loop, so a missed journal wake can expire the wait. See [[observed-exit waits must issue a production exact-session observe turn]].
5. No held-live negative control exists. Nothing proves that observed marker bytes do not count as completion.

## Design

All changes are test-only, in `tests/hub_daemon_lifecycle/`.

### 1. Authoritative completion wait (new shared fixture)

Add to `tests/hub_daemon_lifecycle/session_fixtures.rs`:

- `wait_for_authoritative_session_exit(endpoint, session_id)`. Loop under one explicit deadline (10 s, matching existing observed-exit waits):
  1. Issue production `ReadScreen { session_id }`. This is the exact-session observe stimulus (`src/daemon_transport.rs:3548`).
  2. Issue `ListSessions` and read the target row's `lifecycle`.
  3. Return when `lifecycle == "exited"`.
  On deadline expiry, panic with the full last listing, the last `ReadScreen` response kind and typed error body, and the elapsed time. Sleep (20-50 ms) is polling backoff between production observe turns, never the assertion.
- `assert_session_stays_running_across_observe_turns(endpoint, session_id, turns)`. Run `turns` (default 5) iterations of the same `ReadScreen` + `ListSessions` pair and assert `lifecycle == "running"` after each. The negative control is counted in production observation turns, not wall-clock time, per [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]].

Neither helper calls `Drain`, reads a PID, or touches the filesystem for lifecycle state. Neither helper adds observation to any production operation path; both consume the shipped `ReadScreen` and `ShutdownSession` contracts.

### 2. Held-live producer script (new fixture)

Add to `tests/hub_daemon_lifecycle/package_fixtures.rs`:

- `write_python_held_live_script(release_path, exit_release_path, bytes)`. The script:
  1. Prints one `producer-ready` text line and flushes (explicit startup signal).
  2. Blocks until `release_path` exists.
  3. Writes the exact byte sequence with `os.write(1, bytes([...]))`.
  4. Blocks until `exit_release_path` exists (held-live phase).
  5. Exits 0.
- Extend the finite path with the same explicit startup signal: either add the `producer-ready` line to `write_python_wait_then_write_script` or add a parallel finite writer that emits it. The ready line is a separate text frame before the release gate, so exact-byte window assertions and per-frame equality assertions in the existing consumers stay valid.

Fixture startup becomes explicit and bounded: the test observes `producer-ready` (via drained live output or `ReadScreen` text) under a deadline before it writes the release file. Today the release write races Python interpreter startup and a failure surfaces only as an opaque drain timeout.

### 3. Focused tests (new, in `tests/hub_daemon_lifecycle/sessions.rs` next to the exact-bytes family)

- `external_hub_finite_producer_completion_uses_production_lifecycle_signal`:
  1. `daemon_test_guard()`; isolated hub (`start_isolated_live_output_hub`).
  2. Spawn the finite producer with exact non-UTF-8 bytes (reuse the `[0x00, 0x1b, 0x5b, 0x31, 0x6d, 0xff, 0xc0]` family).
  3. Wait bounded for `producer-ready`; write the release file.
  4. Observe the exact byte window on the live plane; assert no U+FFFD replacement.
  5. `wait_for_authoritative_session_exit`.
  6. `ShutdownSession` must return `SessionCleanup { outcome: "already_exited" }` deterministically — a hard assert that prints the full typed error body on failure per [[flake oracles over typed response frames must print the full typed error body]].
  7. `RemoveSession`; hub shutdown.
- `external_hub_held_live_producer_defers_completion_until_exit_release`:
  1. Same isolation; spawn the held-live producer with the same exact non-UTF-8 bytes.
  2. Bounded ready wait; release; observe the exact byte window.
  3. Negative control: `assert_session_stays_running_across_observe_turns`. The marker bytes are already on the terminal plane, yet repeated production exact-session observation must keep reporting `running`. This is the in-tree proof that a marker written before process exit cannot prove Core session completion; it also serves as the red control demanded by [[a regression test must be shown to go red with the fix reverted]] — an oracle that treated observed bytes as completion would fail here.
  4. Write the exit release file; `wait_for_authoritative_session_exit` — the same oracle flips only on real Core completion.
  5. `ShutdownSession` returns `SessionCleanup { already_exited }`; `RemoveSession`; hub shutdown.
  6. A `SessionCleanupGuard` (or `IsolatedHub` shutdown) stays armed through the held-live phase so a mid-test panic cannot leak the held-live worker tree.

### 4. Replace the racy completion oracles at the existing finite-producer call sites

Migrate exactly these six call sites to the shared helpers; nothing else:

1. `sessions.rs` `external_hub_live_output_preserves_exact_bytes` — after byte observation: `wait_for_authoritative_session_exit`, then deterministic `SessionCleanup { already_exited }` (replaces the soft `shutdown_short_lived_session`).
2. `sessions.rs` `external_hub_live_output_preserves_split_utf8_frames` — same replacement after the second fragment.
3. `sessions.rs` `external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes` — same replacement.
4. `webrtc_proofs.rs` `external_hub_webrtc_live_output_preserves_exact_bytes` — replace the blind strict `ShutdownSession` (`assert_shutdown_strict_natural_exit`) with the wait plus deterministic `SessionCleanup` assert.
5. `webrtc_proofs.rs` round-based shutdown-after-live-exit test — replace the `(0..40)` `ListSessions` poll and the soft dual-branch with the wait plus a single deterministic `SessionCleanup { already_exited }` branch per round. The blind-call typed-error contract stays codified by its dedicated sibling tests; this observed-state test stops duplicating it.
6. `sessions.rs` `shutdown_after_observed_exit_returns_session_cleanup` — replace the `ListSessions`-only poll with `wait_for_authoritative_session_exit` (adds the production exact-session observe stimulus the vault note requires).

Blind-call contract tests (typed `OperatorError` before exit observation) are intentionally untouched; they codify a different, still-valid contract.

## Scope and non-scope

In scope:
- New fixtures: authoritative exit wait, held-live negative-control helper, held-live producer script, explicit bounded producer startup signal.
- Two new focused tests (finite, held-live).
- The six oracle migrations listed above.
- This plan document.

Non-scope:
- Any `src/` change. No lifecycle observation is added to Spawn, SpawnSessionType, Attach, Drain, Input, Resize, or any other operation path. Terminal Drain does not discover lifecycle.
- Core changes or Core pin bumps.
- `packages/hub-test-support` changes (no fixture-byte mutation; no version bump).
- Owner-loop scheduling, snapshot paging, WebRTC teardown behavior (owned by adjacent closed tickets).
- Other soft waits in the suite that are not finite-PTY-producer completion oracles (for example `wait_for_idle_lifecycle_window`, `wait_for_managed_git_session_exit`). If Implement finds one of these blocking the clean `./test.sh --locked` gate, that is a separate blocker ticket, not silent scope growth.

## Ownership boundaries and cross-repo dependencies

- All work is Hub-owned test code. The completion signal consumes two shipped Hub production surfaces: exact-session observe inside `ReadScreen` and exact-session classification inside `ShutdownSession`. Both are Hub control-plane surfaces over the pinned Core (`8fce204`) `observe_session_lifecycle` API.
- No missing production seam was found. If Implement discovers one (for example `ReadScreen` losing its exact-session observe), the instruction is: stop, register a repository-owned dependency ticket against the owning repository, and do not change production code inside this ticket.
- No cross-repository dependency is registered. This ticket is not a consumer of the Hub session-type eligibility parent; the parent-pin injection rules do not apply.

## Assumptions and unknowns

- Assumption: `python3` is available on the test host. Existing merged tests already assume this.
- Assumption: production `ReadScreen` keeps its exact-session `observe_session_lifecycle` call. Verified on the current merge (`src/daemon_transport.rs:3548`), and the merged scheduler ticket's notes state reads do not remake Pump while `ReadScreen` retains exact-session observation.
- Assumption: migrating the six listed call sites is the "replace" action of the ticket title. On current `main` these soft and over-strict oracles are the remaining nondeterministic completion oracles attached to PTY producers, and the "one clean `./test.sh --locked` without retry" proof depends on them. If Plan Review reads the ticket as fixtures-plus-focused-tests only, the migration list is severable, but the plan recommends keeping it.
- Unknown: exact deadline headroom under full default-concurrency workspace load. The wait bound starts at 10 s (matching existing observed-exit waits). The bound is a deadline for a production-signal poll, not a correctness assertion; widening it is legal, softening the assertion is not.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/session_fixtures.rs` — new wait and negative-control helpers.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — held-live producer script; explicit ready signal for the finite producer.
- `tests/hub_daemon_lifecycle/sessions.rs` — two new focused tests; migrations 1, 2, 3, 6.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` — migrations 4, 5.
- `tests/hub_daemon_lifecycle/common.rs` — only if `shutdown_short_lived_session` loses its last caller in the migrated set (it keeps other callers; leave it).
- `docs/plans/replace-pty-process-pid-and-marker-lifecycle-oracles.md` — this plan.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes, narrowly. The ticket's subject is terminal-state vs live-runtime divergence (marker bytes on the terminal plane vs Core session completion). The ticket is test-only; no production teardown path changes.
- `teardown_isolation`: each test owns one isolated hub (unique data directory, `daemon_test_guard`). A failed producer or session affects only its own hub instance. Harness taint rules ([[real daemon start boundaries serialize against process global taint]]) already protect sibling tests.
- `teardown_bounds`: every wait carries an explicit deadline with full diagnostics on expiry. The held-live producer is released (exit release file) or reaped by the armed cleanup guard / `IsolatedHub` shutdown on panic. No unbounded `block_on`; no control-plane hang is introduced.
- `late_message_matrix`: not applicable — no new ownership-creating message surface is added or modified. Tests consume existing Spawn, Attach, ReadScreen, ShutdownSession, RemoveSession contracts unchanged.
- `production_path_proof`: the completion oracle is itself the production path: `ReadScreen` → `observe_session_lifecycle(exact session)` → registry/journal advance → `ListSessions` shows `exited`; `ShutdownSession` → exact-session classification → `SessionCleanup { already_exited }`. The held-live test is the live negative control proving the signal does not fire before real exit.
- `ownership_identity`: sessions are identified by unique per-test `session_id` strings; cleanup guards key on data directory plus session id. No reused-id hazard is introduced.
- `sibling_fail_closed_policy`: isolated hubs mean no sibling sacrifice on success or failure. Ultimate cleanup failure falls into the existing harness taint machinery, which blocks the next daemon start rather than corrupting siblings.

## Risks

1. Strict deterministic `SessionCleanup` assertions can expose a real product defect (a missed journal wake that never recovers under exact-session observation). That would be a production bug: per the ticket, stop and register a repository-owned dependency instead of softening the oracle.
2. Suite load can slow observation. Mitigation: generous deadlines, production-turn counting for the negative control, no wall-clock correctness assertions ([[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]).
3. The ready-line changes the producer byte stream. Mitigation: existing assertions use window containment or locate their frame by exact payload; the focused tests assert the exact window, not whole-stream equality.
4. The held-live test can fail in reverse if the Python script errors out and exits early. Mitigation: `python3 -u`, `os.write` only for the exact bytes, ready line flushed before the gate, and the negative-control failure message prints the session listing so an early exit is distinguishable from a false completion signal.
5. Removing the soft branch in the round-based WebRTC test narrows its contract. Mitigation: the blind-call typed-error contract remains codified in its dedicated sibling tests; this is verified before deletion.

## Acceptance checks/tests

1. New focused tests pass under default concurrency:
   - `external_hub_finite_producer_completion_uses_production_lifecycle_signal`
   - `external_hub_held_live_producer_defers_completion_until_exit_release`
2. Both focused tests assert exact non-UTF-8 PTY bytes (window match, no U+FFFD).
3. Negative control: with marker bytes already observed, repeated production exact-session observe turns keep reporting `running` until the exit release. This is the red control: any oracle that counted markers, PIDs, or done files as completion fails here.
4. No sleep as a correctness oracle: sleeps appear only as polling backoff inside deadline-bounded production-signal waits; the negative control counts observation turns.
5. Fixture startup is explicit and bounded: tests observe `producer-ready` under a deadline before writing the release file.
6. Diff discipline: `git diff --stat origin/main` shows only `tests/` and `docs/plans/` paths. No PID, descendant-PID, done-file, or pre-exit-marker completion oracle is introduced (reviewable by inspection of the new fixtures).
7. Repository gates, in order:
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --locked -- -D warnings`
   - `cargo test --doc --workspace`
   - One clean default-concurrency `./test.sh --locked` without retry. If a distinct unrelated failure appears, register a separate blocker with exact evidence instead of retrying to green.
8. Focused lifecycle-suite evidence may additionally use `script/run-lifecycle-suite` (the exclusive wrapper for `hub_daemon_lifecycle_test`, with process census hygiene) during development; the required gate remains the clean `./test.sh --locked`.

## Vault gaps

1. [[host ShutdownSession classification must call the exact-session Core query]] still says "This convention is not shipped behavior yet." The exact-session classification ships at `src/daemon_transport.rs:4743`. The note's status is stale and worth updating.
2. Capture candidate after implementation: "finite and held-live PTY producer completion uses the production exact-session observe wait" — the fixture convention this ticket establishes, so later test authors do not reinvent PID or marker oracles.
3. Capture candidate: "release-file-gated python producers prove startup with a ready marker before release" — the bounded-startup contract.
