# Reproduce targeted pump_woken resize with merged Core

## Routing

- Ticket: `ticket_1788206393_323469`
- Run: `run_1788405008_320393`
- Step: `botster_stack_plan`, gate `botster_stack_plan_gate`
- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Repository playbook: [[botster-hub-playbook]]
- Worktree assumption: the run starts from `origin/main` commit
  `bb1a330543bc06888f894edd5f40a0f867753a12`. The worktree path contains no
  `:` character, so official gates run in the default `target/` directory with
  `CARGO_TARGET_DIR` unset.

Project Pipelines resolved the target. The project
`Botster Isolated Subscription Data Plane` registers
`tgt_7e208a0c76a44980a83b63af976b1f22` and earlier Hub plans in this repository
bind that id to `botster-hub`. The Core dependency target is
`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.

## Context loaded

Role and repository guidance:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]

Vault notes that constrain this ticket:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[core terminal progress is wake driven and targeted]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[session registry size follows the worker applied resize]]
- [[worker applies the latest attach resize before barrier release]]
- [[worker resize acknowledgment precedes the next control frame]]
- [[resize completion wake durability has one ablation point and needs three core armed pumps]]
- [[core one slot adapters preserve resize input and echo wake obligations]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[exact Rust test ablations require a one test baseline]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[vault example paths are not repository placement conventions]]

Project memory applied: Hub consumes Core by merged git revision, not by a
package release. A Hub Core pin roll touches every active revision literal and
six `Cargo.lock` sources. Hub gates run under `RUSTUP_TOOLCHAIN=1.97.0` with
`CARGO_TARGET_DIR` unset. The lifecycle suite needs a quiet host.

Runtime-teardown class: does not apply. The ticket reproduces a resize record
update on a live session. It does not change peer lifecycle, worker teardown,
multi-peer ownership, or resource spin. [[botster runtime teardown lenses]] was
read for the applicability test only.

## Dependency state (verified 2026-09-02)

- Core ticket `ticket_1788198279_441580` is closed. Its run closed on
  2026-08-31 at 16:02 local time.
- Core `origin/main` is `48a437032791e678010254708259568ce4ad02bf`
  (`git ls-remote`). The resize completion commits `8bebab4`, `6f52148`,
  `9a6ef57`, `120eb3a`, `8b2e758`, and `873df1c` are ancestors of that tip
  (`git merge-base --is-ancestor`).
- Hub `origin/main` already pins `48a4370` at every active site: `Cargo.toml`,
  `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-test-support/Cargo.toml`, `build.rs`,
  `conformance_data.rs`, `lib.rs`, seven test literals, and six `Cargo.lock`
  sources. The old failing Core revision `a781556` no longer appears outside
  `docs/`.
- Hub commit `5c8d463` (2026-08-31 16:38) moved the pin from `a781556` to
  `873df1c`, the last commit of the Core ticket. Commits `74a7478` and
  `0417317` moved it further to `bfba598` and `48a4370`.

Conclusion: the pin roll the ticket describes is already merged. This run
must not roll the pin again. The remaining deliverable is the live
reproduction proof against the merged Core and its durable record.

## Scope

1. Prove the reproduction on the current pin. Run
   `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect`
   under the official Hub gate environment. Require the `rows=31`, `cols=101`
   entity patch for both subscribers as the first post-resize frame.
2. Strengthen the same test so it observes the session record, not only the
   patch. After the patch assertion, read
   `<data_dir>/sessions/entity-session.json` and require top-level `rows == 31`
   and `cols == 101`. Core `persist_session_size` writes those fields before it
   appends the lifecycle upsert, so the file must already hold the new size
   when the patch arrives.
3. Prove the red arm. In a separate colon-free clone at Hub commit
   `4f85817` (the parent of `5c8d463`, which still pins Core `a781556`), run
   the same test. Require the failure diagnostic to show the record at
   `rows=24`, `cols=80` after the requested `31x101` resize. This is the
   ticket's stated failure and satisfies
   [[a regression test must be shown to go red with the fix reverted]] without
   editing Core.
4. Record the evidence in a report at
   `docs/reports/reproduce-targeted-pump-woken-resize-with-merged-core-implement-report.md`
   with Hub commit, Core pin, `rustc --version`, and both binary realpaths.

## Non-scope

- No Core pin roll. The pin is already at the Core tip and includes the fix.
- No Hub workaround, retry, or resend of the resize frame in production code.
- No terminal-content policy in Hub. Hub stays content blind.
- No change to `src/session_projection.rs` or any production path. The
  projection reads `record.session.size.rows` and `cols` from the Core
  lifecycle record and needs no change.
- No change to the existing resend loop in the exit phase of the test. That
  loop runs after the resize assertion and does not affect it.
- No conformance fixture or `hub-test-support` npm package change. The
  conformance revision does not change because no daemon event shape changes.

## Ownership boundaries and cross-repository dependencies

- botster-core owns resize application, `sessions/<id>.json` persistence,
  and the lifecycle patch. Those landed under `ticket_1788198279_441580`.
- botster-hub owns the lifecycle projection, the session entity subscription,
  and this reproduction test. Hub only consumes the Core record.
- The formal dependency edge `dependency_1788208433_625703` on the Core ticket
  is registered and closed. No new dependency is needed.
- botster-hub-client owns `DaemonSessionEntity`, which already carries `rows`
  and `cols`. No DTO change.

## Assumptions and unknowns

- Assumption: the test is green on the current pin. The Plan step measured
  this; see the baseline section.
- Assumption: Hub commit `4f85817` still builds with Rust 1.97.0 and Zig
  0.16.0. If it fails to build, the red arm falls back to a throwaway pin of
  Core `a781556` on a scratch branch. The Implementer records which arm ran.
- Unknown: whether the red arm fails at the entity patch assertion or at the
  new record assertion. Either failure that shows `rows=24`, `cols=80`
  satisfies the ticket. The report must quote the exact diagnostic.
- Unknown: host quiet window. Other pipeline sessions hold live Hub daemons.
  The full lifecycle suite refuses to start on a dirty host.

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/sessions.rs`: add the persisted record
  assertion inside
  `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect`.
- `docs/reports/reproduce-targeted-pump-woken-resize-with-merged-core-implement-report.md`:
  new evidence report.
- `docs/plans/reproduce-targeted-pump-woken-resize-with-merged-core.md`:
  this plan.

Botster layers touched: Rust hub integration test and docs only.

## Risks

- A green test alone does not prove the Core fix. The red arm at the old pin
  is required.
- The test resends the resize frame in the exit loop. A reviewer could read
  that as a workaround. It is pre-existing, runs after the assertions this
  ticket cares about, and stays unchanged.
- The record assertion reads a file the worker writes. If Core wrote the file
  after the patch, the read could race. Core writes the file before it
  appends the upsert, so the order is safe. The Implementer must not add a
  poll loop; a race here is a Core defect to report, not to mask.
- The lifecycle suite is host sensitive. Use the process census to find a
  quiet window. Do not force past `environment_dirty`.

## Acceptance checks and tests

Environment for every Hub gate:

```sh
export RUSTUP_TOOLCHAIN=1.97.0
unset CARGO_TARGET_DIR
rustc --version   # must print 1.97.0
zig version       # must print 0.16.0
```

Green arm on this branch:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
./test.sh --locked --test hub_daemon_lifecycle_test \
  session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect \
  -- --exact hub_daemon_lifecycle::sessions::session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect
```

Require one baseline test in the tally, three consecutive passes, and the
record assertion present in the diff.

Red arm in a separate clone at `4f85817`:

```sh
git clone <hub remote> /path/without/colon && git checkout 4f85817
# apply the record assertion hunk from this branch
./test.sh --locked --test hub_daemon_lifecycle_test \
  session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect \
  -- --exact hub_daemon_lifecycle::sessions::session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect
```

Require a failure whose diagnostic shows `rows=24`, `cols=80`, or the resize
patch absent, after the `31x101` request.

Strict gates on this branch:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
node packages/hub-test-support/scripts/sync-assets.mjs --check
script/run-lifecycle-suite      # verdict=clean on a quiet host
./test.sh --locked
```

Live provenance for the report:

- `git rev-parse HEAD` for Hub.
- `grep -c 48a437032791e678010254708259568ce4ad02bf Cargo.lock` equals 6.
- `realpath target/debug/botster-hub` and
  `realpath target/debug/botster-session-worker`.

## Baseline measured during Plan

See the section appended below after the measurement completed.

## Vault gaps worth capturing

- A downstream reproduction ticket can be created before its parent merges
  and then be overtaken by an unrelated pin roll. The Plan step must diff the
  current pin against the parent's merge commit before it plans a roll.
- The Hub red arm for a Core fix is a checkout of the last Hub commit on the
  old pin, not a Core revert. That keeps the proof inside Hub ownership.

## Baseline measured during Plan (2026-09-02)

Environment: Hub `bb1a330543bc06888f894edd5f40a0f867753a12`, Core pin
`48a437032791e678010254708259568ce4ad02bf` (six `Cargo.lock` sources),
`rustc 1.97.0 (2d8144b78 2026-07-07)`, Zig `0.16.0`, `CARGO_TARGET_DIR`
unset, default worktree `target/`.

Command: the green arm command above, after prebuilding
`botster-session-worker` and `botster-hub` with `--locked`.

| Run | Tally | Wall time |
| --- | --- | --- |
| 1 | `1 passed; 0 failed; 343 filtered out` | 4.75s |
| 2 | `1 passed; 0 failed; 343 filtered out` | 2.99s |
| 3 | `1 passed; 0 failed; 343 filtered out` | 2.99s |

Binary realpaths: `target/debug/botster-hub` and
`target/debug/botster-session-worker` under this worktree.

The host was not quiet during the measurement. Two other pipeline worktrees
held live `botster-hub` and `botster-session-worker` processes. The targeted
test passed anyway. The full lifecycle suite still needs a quiet host.

The current test already proves the `rows=31`, `cols=101` patch. The
Implement step adds the persisted record assertion and runs the red arm.
