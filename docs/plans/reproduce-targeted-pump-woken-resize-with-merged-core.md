# Prove targeted wake progress and migrate residual polling test seams

Revision 2. Plan Review `review_1788405975_230755` returned revision 1 because
its red arm was described as failing before the resize request. The ticket was
also rewritten during that review to add the Core candidate arm and three test
migrations. This revision covers the rewritten ticket.

## Routing

- Ticket: `ticket_1788206393_323469`
- Run: `run_1788405008_320393`
- Step: `botster_stack_plan`, gate `botster_stack_plan_gate`
- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Repository playbook: [[botster-hub-playbook]]
- Worktree assumption: the run starts from `origin/main` commit
  `bb1a330543bc06888f894edd5f40a0f867753a12`. The worktree path contains no
  `:` character. Official gates run in the default `target/` directory with
  `CARGO_TARGET_DIR` unset.

Project Pipelines resolved the target. The project
`Botster Isolated Subscription Data Plane` registers
`tgt_7e208a0c76a44980a83b63af976b1f22`, and earlier Hub plans in this
repository bind that id to `botster-hub`. The Core target is
`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.

## Context loaded

Role and repository guidance:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]] (class applies; answers below)

Vault notes that constrain this ticket:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[core terminal progress is wake driven and targeted]]
- [[terminal adapters emit coalesced writable and closed wakes]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[session registry size follows the worker applied resize]]
- [[worker resize acknowledgment precedes the next control frame]]
- [[resize completion wake durability has one ablation point and needs three core armed pumps]]
- [[core one slot adapters preserve resize input and echo wake obligations]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[exact Rust test ablations require a one test baseline]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[plan agents must author vault context as wikilinks not home paths]]

Project memory applied: Hub consumes Core by merged git revision. A Core pin
roll touches every active revision literal and six `Cargo.lock` sources. Hub
gates run under `RUSTUP_TOOLCHAIN=1.97.0` with `CARGO_TARGET_DIR` unset. The
lifecycle suite needs a quiet host. `BOTSTER_HUB_TEST_PAUSE_DATA_PLANE` pauses
the Hub wake driver without a source edit.

Core candidate context read from the Core clone: commit
`05464a186c974e2d1b21b190679a0486f066f8d6` on
`origin/project-pipelines/ticket_1787894967_973951`, merge base with Core
`main` at `48a4370`, and its archived plan
`docs/archive/plans/delete-the-core-polling-adapter-path.md`.

## Dependency state (verified 2026-09-02)

- Core ticket `ticket_1788198279_441580` is closed. Its resize completion
  commits `8bebab4` through `873df1c` are ancestors of Core `main` tip
  `48a437032791e678010254708259568ce4ad02bf`.
- Hub `main` already pins `48a4370` at every active site and six `Cargo.lock`
  sources. Hub commit `5c8d463` rolled the pin from the failing revision
  `a781556` to `873df1c` on 2026-08-31. The pin roll the ticket names is
  already merged. This run does not roll the pin again.
- Core ticket `ticket_1787894967_973951` (delete the polling adapter path) is
  open at Implement. It registered this Hub ticket as its dependency
  `dependency_1788405916_426816`. This Hub ticket must merge first.
- The Core candidate deletes `ClientWorker::pump`,
  `ClientWorker::intake_terminal_input`, the non-waking
  `bind_terminal_adapter`, `drain_runtime_once_without_pump`,
  `apply_terminal_input`, and `pump_bound_adapters`. After the deletion the
  only bound-adapter progress path is `bind_waking_terminal_adapter` plus
  `pump_woken` on a `TerminalWakeBatch`. Both of those exist on Core `48a4370`
  today, so a migrated Hub test compiles on the current pin and on the
  candidate.

## What the three named tests rely on today

The candidate keeps `pump_woken`, which pumps the routes named by adapter
wakes and every waking route of each named ingress session. Core counts one
unsuccessful write per pump of a pressured route and hard-stops the route at
`WRITE_ATTEMPT_BUDGET` (512). Under wake-only progress those 512 attempts
must come from wakes.

1. `src/transport/webrtc/adapter.rs::sustained_aggregate_pressure_reaches_core_hard_stop_and_retires_route`
   binds through the non-waking `bind_terminal_adapter` and calls
   `worker.pump()` 512 times. Both symbols disappear in the candidate. The
   Hub production path binds through `bind_waking_terminal_adapter` in
   `src/runtime.rs`, and `WebRtcTerminalAdapter` already implements
   `WakingTerminalAdapter`. The obsolete trigger is the polling pump.
2. `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs::webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`
   arms `BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION=wwb-stall` and
   spawns `printf 'write-budget-stall\n'; sleep 30`. That fixture emits one
   burst and then stays silent. On Core `48a4370` the 512 attempts accrue
   because Hub lifecycle observation calls `drain_runtime_once`, which calls
   `pump_bound_adapters` on every observed session. The candidate deletes
   that call. The obsolete trigger is the silent fixture that depends on
   drain-time polling.
3. `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs::webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close`
   arms the same seam for `wrm-stall` and spawns
   `printf 'remove-session-still-live\n'; sleep 30`. Same obsolete trigger.

The Unix analog `core_write_budget_hard_stop_emits_core_adapter_closed` in
`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` already migrated. Its
stall fixture is `sleep 3; exec yes write-budget-stall`. Sustained PTY output
raises session ingress wakes, each targeted pump retries the pressured route,
and the attempt budget expires from wakes alone. That is the accepted
waking-contract pattern for the two WebRTC lifecycle tests.

## Scope

1. Resize reproduction on the current pin. Run
   `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect`
   under the official gate environment. Both phases of that test request a
   `31x101` resize. The published conformance runner
   `run_session_lifecycle_subscription_conformance` in
   `crates/botster-hub-test-support/src/lib.rs` sends
   `terminal_resize_frame_bytes(31, 101)` at its `lifecycle attach` stage and
   then waits five seconds for the `rows=31`, `cols=101` patch under the
   stage label `lifecycle patch`. The direct phase near
   `tests/hub_daemon_lifecycle/sessions.rs:2512` sends the same resize and
   asserts the same patch for two subscribers.
2. Add one persisted-record assertion in the direct phase, after the two
   patch assertions: read `<data_dir>/sessions/entity-session.json` and
   require top-level `rows == 31` and `cols == 101`. Core
   `persist_session_size` writes those fields and then appends the lifecycle
   upsert, so the file holds the new size before the patch can arrive. No
   poll loop.
3. Red arm for the resize oracle. In a separate colon-free clone checked out
   at Hub `4f85817` (the last Hub commit that pinned Core `a781556`), prebuild
   the worker and Hub, then run the same exact test. Require the panic at
   `tests/hub_daemon_lifecycle/sessions.rs:2399` whose conformance error
   names stage `lifecycle patch` with the message
   `timed out waiting for entity frame`. The positive execution marker is
   that the runner already passed its `spawn`, `lifecycle attach`, and resize
   send stages: those stages return distinct labels (`spawn`,
   `lifecycle attach`) and the runner reaches `lifecycle patch` only after
   the `31x101` frame was sent. Plan Review measured this exact outcome on
   2026-09-02. This arm proves the ticket's stated defect: the observed
   record stays `24x80` after the request. It does not exercise the new
   record assertion, which cannot run on a pin that fails at the earlier
   patch oracle; the green arm plus the Core ticket's own persistence
   ablation cover that assertion.
4. Migrate the adapter unit test. Replace the non-waking bind with
   `bind_waking_terminal_adapter` on a `ClientWorker` that has a
   `TerminalWakeSource` installed through `set_wake_source`. Replace each
   `worker.pump()` with `worker.pump_woken(&batch)` where `batch` names only
   the bound route in `adapter_routes` and has empty `ingress_sessions`.
   Keep the 511 retained pumps, the teardown on the 512th pump, the exact
   owner fields, `handle.is_closed()`, the closed-event count of one, and the
   absent live handle.
5. Migrate the two lifecycle tests. Change the stall fixture command from a
   one-line print followed by `sleep 30` to a short delay followed by
   `exec yes <marker>`, mirroring the Unix test. Keep every assertion:
   `core_adapter_closed` reason, no `host_adapter_closed` for the stall
   session, sibling `wwb-live` still `running`, sibling scoped Drain still
   owned and content blind, `session_not_terminal` on the failed
   `RemoveSession`, later Core close still delivered, provenance line, and
   cleanup. Keep the `sleep` prefix so the bind completes before pressure
   begins.
6. Core candidate arm. In a second colon-free scratch clone of this branch,
   override every Core-family revision literal and the six `Cargo.lock`
   sources from `48a4370` to `05464a186c974e2d1b21b190679a0486f066f8d6`, with
   `cargo update` limited to the Core-family packages. Do not commit that
   override. Run the strict gates and the full locked suite there. Record
   the candidate commit in the report.
7. Full locked Hub suite at default concurrency on this branch through
   `script/run-lifecycle-suite` and `./test.sh --locked`.
8. Report at
   `docs/reports/reproduce-targeted-pump-woken-resize-with-merged-core-implement-report.md`
   with Hub commit, committed Core pin, candidate commit, `rustc --version`,
   both binary realpaths per arm, and every arm's tally.

## Non-scope

- No Core pin roll in the committed tree. The pin stays `48a4370`. The
  candidate is unmerged, and a mixed or unmerged pin is forbidden.
- No polling compatibility seam in Hub production code, no retry of the
  forced would-block seam on a timer, and no change to
  `src/data_plane/driver.rs`, `src/transport/webrtc/adapter.rs` production
  code, or `src/transport/shared/adapter_slot.rs`.
- No Hub workaround for resize and no terminal-content policy. Hub stays
  content blind.
- No change to `src/session_projection.rs`.
- No change to the Unix tests, which already follow the waking contract.
- No change to hub-test-support conformance fixtures or the npm package.
- No new dependency edges. The Core ticket already depends on this one.

## Ownership boundaries and cross-repository dependencies

- Core owns resize application, `sessions/<id>.json` persistence, the
  lifecycle patch, the write-attempt budget, `pump_woken`, and the deletion
  of the polling path. Hub consumes those and edits none of them.
- Hub owns the data-plane driver, the WebRTC and Unix adapters, admission
  budgets, its Core pin, and every test in this ticket.
- botster-hub-client owns `DaemonSessionEntity`, which already carries `rows`
  and `cols`. No DTO change.
- Dependency graph: Core `ticket_1787894967_973951` depends on this ticket
  (`dependency_1788405916_426816`, open). This ticket depends on the closed
  Core resize ticket (`dependency_1788208433_625703`).

## Assumptions and unknowns

- Assumed green on the current pin: measured 3 of 3 during Plan revision 1.
- Assumed the `exec yes` fixture produces enough ingress wakes to expire the
  512-attempt budget within the existing 20-second close wait on the
  candidate. The Unix analog does so within its 30-second deadline. If the
  WebRTC arm needs longer, raise only that wait, not the budget.
- Assumed the WebRTC forced seam arms at `register` regardless of
  `BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_DELAY_MS`. The delay variable
  only affects the Unix adapter. Keep the variable in the write-budget test
  unchanged.
- Unknown: whether the 18 active revision literals still match the count at
  Hub `bb1a330`. The Implementer must use the zero-old-SHA grep, not a fixed
  count, when preparing the candidate scratch clone.
- Unknown: host quiet window. Two other pipeline worktrees currently hold
  live Hub daemons. The lifecycle suite refuses on a dirty host.

## Affected surfaces and files

- `src/transport/webrtc/adapter.rs` (test module only): the sustained
  aggregate pressure test.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`: two stall fixture
  commands.
- `tests/hub_daemon_lifecycle/sessions.rs`: one record assertion.
- `docs/reports/reproduce-targeted-pump-woken-resize-with-merged-core-implement-report.md`:
  new evidence report.
- `docs/plans/reproduce-targeted-pump-woken-resize-with-merged-core.md`:
  this plan.

Botster layers touched: Rust hub unit and integration tests, docs. No
production code.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes. The three migrated tests are the Hub proofs
  for Core hard stop, route retirement, and sibling survival on the WebRTC
  transport. The migration changes how those proofs drive progress, so each
  lens must still hold after the change.
- `teardown_isolation`: the ownership set is one `OwnerKey` (session id,
  subscription id) with its generation, bound adapter, aggregate permit, and
  wake route. A hard stop closes and drops only that adapter. The tests keep
  the sibling `wwb-live` running and its scoped Drain owned. The unit test
  keeps `mux.live_handle("session","terminal")` absent while
  `queue_closed_subscription_events` reports exactly one close.
- `teardown_bounds`: the close is synchronous on the pump that exhausts the
  budget. No test may add a sleep, a timer, or a retry loop as the progress
  source. The 20-second close wait in the lifecycle tests is the observation
  bound, and the driver stop bound `DATA_PLANE_STOP_BOUND` remains the
  control-plane hard stop.
- `late_message_matrix`: `Attach` creates the owner (generation tagged,
  rejected after retirement by the live-generation ladder in
  `bind_waking_terminal_adapter`, swept by `retire_route`). `RemoveSession`
  on a live session returns `session_not_terminal` and must not suppress the
  later Core close; the failed-remove test keeps that row. `Detach` and peer
  close are not changed by this ticket. `SubscribeEntities` is out of scope.
- `production_path_proof`: the live tests drive the real Hub child, the real
  WebRTC data channel, the real forced-pressure seam, and the real Core
  budget. The oracle is the negotiated `TerminalSubscriptionClosed` event
  with reason `core_adapter_closed`, plus `Status` and `ListSessions` after
  close. The candidate arm is the red-on-revert control for the trigger
  change: on the candidate, the old silent fixture must fail the close wait
  and the migrated fixture must pass.
- `ownership_identity`: teardown rows carry client id, session id,
  subscription id, and generation. The unit test asserts all four.
- `sibling_fail_closed_policy`: on successful close the sibling keeps
  working, and the tests assert it. Ultimate close failure is not exercised
  here and keeps the existing bounded-abort policy in the driver.

## Risks

- A migrated lifecycle fixture that prints too little never expires the
  budget on the candidate, and the test fails only there. The candidate arm
  catches this before merge.
- `exec yes` floods the PTY. The Unix analog shows the Hub and worker stay
  bounded. Keep `shutdown_short_lived_session` for the stall sessions.
- The unit test must not use `worker.pump()` even on the current pin, or
  the candidate arm fails to compile.
- Editing the record assertion into the direct phase must not change frame
  order. Place it after the second subscriber's patch assertion.
- Full-suite runs collide with other sessions' daemons. Use the process
  census to find a quiet window; do not force past `environment_dirty`.

## Acceptance checks and tests

Environment for every arm:

```sh
export RUSTUP_TOOLCHAIN=1.97.0
unset CARGO_TARGET_DIR
rustc --version   # must print 1.97.0
zig version       # must print 0.16.0
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
```

Arm A, green resize proof on this branch:

```sh
./test.sh --locked --test hub_daemon_lifecycle_test \
  session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect \
  -- --exact hub_daemon_lifecycle::sessions::session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect
```

Require one baseline test, three passes, and the record assertion in the
diff.

Arm B, red resize oracle at Hub `4f85817` in a separate clone:

```sh
git clone <hub remote> /path/without/colon && git -C /path/without/colon checkout 4f85817
# prebuild worker and hub as above, then the same exact test command
```

Require exit 101 at `sessions.rs:2399` with stage `lifecycle patch` and
message `timed out waiting for entity frame`. Quote the panic text.

Arm C, migrated tests on this branch:

```sh
./test.sh --locked --lib sustained_aggregate_pressure_reaches_core_hard_stop_and_retires_route -- --exact transport::webrtc::adapter::tests::sustained_aggregate_pressure_reaches_core_hard_stop_and_retires_route
./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable -- --exact hub_daemon_lifecycle::webrtc_terminal_adapter::webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable
./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close -- --exact hub_daemon_lifecycle::webrtc_terminal_adapter::webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close
```

Require one baseline test per command and three passes each. Confirm the
module path with a one-test tally before any ablation.

Arm D, Core candidate in a second scratch clone of this branch:

```sh
grep -rn 48a437032791e678010254708259568ce4ad02bf --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git . | grep -v '^./docs/'
# replace every listed active site with 05464a186c974e2d1b21b190679a0486f066f8d6
cargo update -p botster-core -p botster-core-daemon -p botster-terminal-protocol -p botster-core-test-support -p botster-terminal-ghostty -p botster-terminal-protocol-client
grep -c 05464a186c974e2d1b21b190679a0486f066f8d6 Cargo.lock   # must be 6
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
./test.sh --locked
```

Require zero old-SHA matches outside `docs/`, the three migrated tests
green, and one full-suite tally with zero failures. Also run the old
fixture as a red control on the candidate: revert only the two fixture
commands in the scratch clone and require both lifecycle tests to fail at
the close wait. Then restore the migrated fixtures.

Arm E, strict gates and full suite on this branch:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
node packages/hub-test-support/scripts/sync-assets.mjs --check
script/run-lifecycle-suite      # verdict=clean on a quiet host
./test.sh --locked              # default concurrency
```

Provenance for the report, per arm: Hub `git rev-parse HEAD`,
`grep -c <core sha> Cargo.lock` equal to 6, and `realpath` of
`target/debug/botster-hub` and `target/debug/botster-session-worker`.

## Baseline measured during Plan (2026-09-02)

Hub `bb1a330`, Core pin `48a4370`, `rustc 1.97.0 (2d8144b78 2026-07-07)`,
Zig `0.16.0`, `CARGO_TARGET_DIR` unset, default `target/`.

| Run | Tally | Wall time |
| --- | --- | --- |
| 1 | `1 passed; 0 failed; 343 filtered out` | 4.75s |
| 2 | `1 passed; 0 failed; 343 filtered out` | 2.99s |
| 3 | `1 passed; 0 failed; 343 filtered out` | 2.99s |

The host was not quiet. Two other pipeline worktrees held live Hub daemons.
The targeted test passed anyway.

Plan also ran the same test with `BOTSTER_HUB_TEST_PAUSE_DATA_PLANE=1`. It
failed at `sessions.rs:2399`, the same conformance-runner site as the old-pin
arm. The paused driver is therefore not a usable red arm for the direct
phase, and the plan uses the old-pin arm instead.

## Vault gaps worth capturing

- A downstream reproduction ticket written before its parent merges can be
  overtaken by an unrelated pin roll. Plan must merge-base the parent fix
  against the current pin before planning a roll. Captured to the vault
  inbox during this Plan visit.
- The published session lifecycle conformance runner sends its own `31x101`
  resize before the direct reproduction phase does. A red arm on this test
  fails at the runner's `lifecycle patch` stage, not at the direct phase.
  Captured to the vault inbox during this Plan visit.
- Under wake-only Core, a stalled-adapter proof needs a fixture that keeps
  producing PTY output, because only ingress or writable wakes retry a
  pressured route. Capture after the candidate arm confirms it.
