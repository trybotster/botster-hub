# Implement report: Hub ShutdownSession natural-exit idempotency

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786977409_499180` |
| Run | `run_1787012955_256937` |
| Run step | `run_step_1787034732_641506` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Plan | `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` @ `075e9e6` |
| Decision gate | Rule B, then orchestrator option 3 |
| Closed Core dependency | `ticket_1787015956_494734` / `dependency_1787015963_708930` closed |
| Closed Core dependency | `ticket_1787034922_646556` / `dependency_1787034941_200828` closed at `302c7f7` |
| Core pin | `302c7f7b61f3970a0151b8c6646fc21ae7bd6c67` |
| Hub main integrated | `bf249af` via merge commit `6cc0c12` |
| Prior Review return | `review_1787034725_875591` diagnosed at `9300a22` |
| Current Review return | `review_1787034725_875591` repaired by Core merge + Hub repin |
| Merge policy | direct; no pull request |
| Review requested | yes, after Core `302c7f7` repin and full live-proof table |

Inventory source: `git diff --name-only origin/main...HEAD` after this visit's commit. Do not treat an earlier intra-branch pin inventory as current.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[flake oracles over typed response frames must print the full typed error body]]
- [[hub shutdown preserves durable session workers]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[project-pipelines-playbook]] because Rule B changed workflow state
- [[dependency ticket creation must start its run or emit an operator action]]
- [[cross repo dependency registration must use dependency repo target]]
- [[test owned orphan workers consume machine wide pty and cpu capacity]]
- [[sid scoped census is blind to setsid session leaks]]
- [[retention without a reachable flush is data loss]]
- [[dependency ticket creation must start its run or emit an operator action]]
- [[cross repo dependency registration must use dependency repo target]]

Convention conflicts: none.

## Constraints applied before edits

- Work only in this run worktree for `botster-hub`.
- Hub owns ShutdownSession classification and recover. Core owns worker exit-evidence mechanics.
- Classification stays on `observe_session_lifecycle`. A query error must not become invented cleanup.
- No production wall-clock, retry, or suite-load correctness mechanism.
- No Core edits in this worktree.
- No pull request. Merge policy is direct.
- No full lifecycle suite.

## Review-return repairs this visit

`review_1787034725_875591` returned two open findings. The prior visit diagnosed Core-side live-output loss and registered `ticket_1787034922_646556`. That Core ticket is now closed. Core `origin/main` is `302c7f7b61f3970a0151b8c6646fc21ae7bd6c67`. `d981bb03` is an ancestor of `302c7f7`. The pin-moving commit is `968792d` (`Keep bound live bytes visible across ProcessExited writer-barrier rounds`).

`finding_1787034725_966366`: Hub now pins Core `302c7f7`. The Hub live-byte oracle is unchanged. `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` passed 5/5 at the new pin (15.00s, 5.71s, 8.19s, 8.30s, 6.28s). The pin was not rolled back to `fd66efd`.

`finding_1787034725_136720`: the test table below has a fresh row for every previously cited live proof plus units at pin `302c7f7`.

Prior `review_1787033530_630528` findings remain resolved at `57cbeb2`. Prior `review_1787032735_498956` findings remain resolved at `e9683de`. Prior `review_1787030054_829721` findings remain resolved at `f4d9f0f`. Prior `review_1787029110_848811` findings remain resolved at `2320ba4`. Prior `review_1787028521_313736` findings remain resolved at `286c1ab`. Prior `review_1787027565_578625` findings remain resolved at `7071f42`.

## Prior Review-return repairs at `57cbeb2`

`review_1787033530_630528` accepted the Core `d981bb03` pin and the happy-path setsid reap at `e9683de`. It returned one high finding.

`finding_1787033530_256513`: `assert_cli_fixture_absent_fails_when_setsid_child_survives` now wraps the fixture in `PanicSafeSetsidChild` immediately after spawn. Drop kills the exact child and the stored PGID. It does not run a process census before those signals. `reap_captured_pty_children` also signals known pids and PGIDs first; census stays a post-cleanup oracle. `panic_safe_setsid_owner_reaps_group_and_pipe_after_forced_error` panics before the old cleanup block and proves the exact PGID and stdout pipe are gone.

Prior `review_1787032735_498956` findings remain resolved at `e9683de`. Prior `review_1787030054_829721` findings remain resolved at `f4d9f0f`. Prior `review_1787029110_848811` findings remain resolved at `2320ba4`. Prior `review_1787028521_313736` findings remain resolved at `286c1ab`. Prior `review_1787027565_578625` findings remain resolved at `7071f42`.

## Prior Review-return repairs at `7071f42`

`review_1787027565_578625` returned three open findings. That visit repaired all three.

1. `finding_1787027565_879300`: recover no longer reads `runtime.list_sessions()`. `recover_after_core_shutdown_error` reclassifies only through `classify_shutdown_session` (`observe_session_lifecycle`). Classify `Err` preserves the original typed Core error. Cleanup returns only from exact-query `Cleanup` (`Exited` or `Stale`) or `Missing`. Exact-query `Stopping` still uses the existing host `already_exited` map in `shutdown_error_response` because that row comes from the exact-session query, not a collection fallback. Recorded `Stopping` after classify `Err` now preserves `runtime_error`.
2. `finding_1787027565_484505`: merged `origin/main` `e864c3c`. Every pin conflict kept current-main Core `fd66efdcb4769b2b3a75cbd580a5b98b82825790`. Unix attach/print-release/exit-release/`process_exit` survived the auto-merge.
3. `finding_1787027565_931444`: this report is rebuilt from `origin/main...HEAD` after that merge and the recover repair.

## Files changed versus `origin/main`

Twenty-three paths differ from current main `bf249af`. This visit moves the locked Core pin from `d981bb03` to `302c7f7`.

### This Review-return visit

- `Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/{Cargo.toml,build.rs,src/conformance_data.rs,src/lib.rs}`, `tests/session_projection_owner_loop.rs`, `tests/hub_daemon_lifecycle/{package_event_plane.rs,unix_terminal_adapter.rs,webrtc_terminal_adapter.rs}` -- Core pin `302c7f7`.
- `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` -- this report.
- `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` -- closed second Core dependency and new pin.

### Inherited same-ticket changes still on the branch

- `src/daemon_transport.rs` -- exact-session recover only; no `list_sessions` fallback.
- `src/runtime.rs` -- test inject for observe drain failure.
- `crates/botster-hub-test-support/src/isolated_hub.rs` -- IsolatedHub extra env used by drain-failure proofs.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- Unix natural-exit and stuck-Stopping proofs.
- `tests/hub_daemon_lifecycle/session_fixtures.rs` -- `assert_shutdown_strict_natural_exit` and IsolatedHub env helper.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` -- blind exact-bytes `ShutdownSession`.
- `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` -- approved plan at `075e9e6`, plus this visit's second Core-dependency note.
- `tests/hub_daemon_lifecycle/process.rs` -- panic-safe setsid owner from `57cbeb2`.
- `tests/hub_daemon_lifecycle/sessions.rs` -- setsid forced-error proof from `57cbeb2`.
- `tests/hub_daemon_lifecycle/cli.rs` -- panic-safe CLI daemon owner.
- `docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md` -- superseded plan kept on the branch.
- `docs/reports/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load-implement.md` -- superseded report kept on the branch.

## Ownership boundaries preserved

Hub still owns classification, recover, and host response kinds. Core still owns payload delivery, shutdown deadline, managed rollback, and observe drain. No hub-client DTO change. No Core edit in this worktree.

## Cross-repo dependencies or separately routed work

- Created `ticket_1787015956_494734` on Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.
- Registered `dependency_1787015963_708930`. That Core ticket is closed.
- Created `ticket_1787034922_646556` on the same Core target. Registered `dependency_1787034941_200828`. Started `run_1787034943_217745`. That ticket is closed. Core main is `302c7f7`.
- Downstream blocker `dependency_1787014444_456296` remains: `ticket_1786938984_190098` depends on this ticket.
- Hub now consumes Core pin `302c7f7b61f3970a0151b8c6646fc21ae7bd6c67`. That pin is a descendant of `d981bb03` and `fd66efd`. It contains closed Core tickets `ticket_1787015956_494734` and `ticket_1787034922_646556`. Do not resolve later pin conflicts by copying Hub main when that rolls a closed dependency backward.

## Deviations from plan

- Phase 1 did not obtain live blind-ShutdownSession `OperatorError` bodies under a successful W1/W2 spawn. A Hub parent-wrapper cannot satisfy Core welcome `worker_pid`.
- Orchestrator option 3 removed the Hub wrapper tests. Core owns W1/W2 mechanism proof.
- Five-round idempotency is not a gate.
- Review rejected the planned `list_sessions` recover fallback. Recover now preserves the typed error on classify `Err` and does not read collection APIs.
- No full lifecycle suite ran, as required by acceptance check 10.
- This visit integrates current Hub main `bf249af` and does not change the ShutdownSession product path.
- This visit repins Core to `d981bb03` even though current Hub main still names `fd66efd`. Ancestry, not Hub-main matching, owns the pin.
- This Review-return visit repairs only `finding_1787033530_256513`. It does not change the ShutdownSession product path.
- This visit does not change Hub product code. It registers a second Core dependency instead of rolling the pin back or weakening the live-byte oracle.
- After that Core ticket closed, this visit pin-forwarded to Core main `302c7f7` and reran the full live-proof table. The Hub live-byte oracle stayed strict.

## Tests and downstream proof run

All test commands used `./test.sh`. Worker prebuild, fmt, and clippy are the documented exceptions.

| Check | Command | Result |
| --- | --- | --- |
| Recover units | `./test.sh --locked --lib recover_` | 5 passed |
| Active-error units | `./test.sh --locked --lib shutdown_active_` | 2 passed |
| WebRTC idempotent cleanup | `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup -- --exact` | 5/5 pass at `302c7f7` (15.00s, 5.71s, 8.19s, 8.30s, 6.28s) |
| SetSID owner forced-error | `./test.sh --locked --test hub_daemon_lifecycle_test panic_safe_setsid_owner_reaps_group_and_pipe_after_forced_error -- --exact` | pass in 0.16s |
| SetSID negative control | `./test.sh --locked --test hub_daemon_lifecycle_test assert_cli_fixture_absent_fails_when_setsid_child_survives -- --exact` | pass in 7.78s |
| Deliberate panic survivors | `./test.sh --locked --test hub_daemon_lifecycle_test panic_safe_cli_daemon_deliberate_failure_leaves_no_owned_survivors -- --exact` | pass in 7.97s |
| True-error sibling | `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable -- --exact` | pass in 17.27s |
| Unix natural-exit | `./test.sh --locked --test hub_daemon_lifecycle_test unix_shutdown_session_from_another_connection_classifies_attached_exit -- --exact` | pass in 3.07s |
| Live stuck-Stopping negative | `./test.sh --locked --test hub_daemon_lifecycle_test unix_shutdown_session_stuck_stopping_without_exit_evidence_stays_operator_error -- --exact` | pass in 9.20s |
| WebRTC exact-bytes | `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_live_output_preserves_exact_bytes -- --exact` | pass in 4.90s |
| Fmt | `cargo fmt --all -- --check` | pass |
| Clippy | `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Diff check | `git diff --check origin/main...HEAD` | pass |
| PII scan | non-docs working tree vs `origin/main` Users-prefix / name | clean |
| Leftover census | worktree hubs and `sleep 3600` orphans | none |

Unix natural-exit proof: default Hello spawn, unix-adapter Attach and Drain, print-release, live `pse-ready`, exit-release, `process_exit`, then blind `ShutdownSession`. Sleep duration is not the oracle.

WebRTC exact-bytes proof: held producer, exact byte receipt, explicit release, blind `ShutdownSession`.

Live stuck-Stopping negative proof: IsolatedHub drain-inject on the exact session, live `sleep 3600` worker, SIGKILL, then production `ShutdownSession`. The response stays `OperatorError` with `runtime_error` or `state_error` and `operation=shutdown`. It does not become `SessionCleanup`.

Core W1/W2 mechanism proof stays at Core pin `302c7f7` (contains `d981bb03`) tests:

- `drain_output_delivers_process_exited_while_worker_holds_stdout_open`
- `drain_output_delivers_process_exited_when_worker_exits_nonzero`

in `crates/botster-core/tests/local_session_worker_process_test.rs`.

Production entry: `DaemonRequest::ShutdownSession` in `src/daemon_transport.rs` still calls `classify_shutdown_session`, then Core `shutdown_session`, then `recover_after_core_shutdown_error`.

## Runtime-teardown lenses

Every lens from the approved plan remains in force. No lens was dropped to informal follow-up. Closed Core `ticket_1787015956_494734` owns the payload-delivery lens. Hub still owns classify, recover, both transport host-path proofs, and the stuck-Stopping negative proof. The true-error sibling fixture now has panic-safe hard-stop evidence for its owned Hub process group, plus an all-session marker and captured PTY pid/PGID oracle. A representative `setsid` child makes that oracle fail. The setsid fixture itself now has a panic-safe owner that reaps the stored PGID without a prior census.

## Unverified behavior or residual risk

- Exact-query `Found(Stopping)` after a Core shutdown error still maps to host `SessionCleanup{already_exited}`. Review required that path only when the exact-session query itself returns Stopping, not when classify `Err` is replaced by a collection row.
- `ticket_1786938984_190098` is now on Hub main at `bf249af`. This leaf ticket still does not run a full lifecycle suite.
- Current Hub main still pins Core `fd66efd`. This branch intentionally keeps `302c7f7` so both closed Core dependencies stay in ancestry.
- No full lifecycle suite ran.

## Missing vault guidance discovered

- The suite-load oracle note still documents the superseded legal-OperatorError contract. Capture after this ticket closes.
- [[host ShutdownSession classification must call the exact-session Core query]] still says it is not shipped. Hub main already ships the exact-session query. This visit also removes the collection recover fallback that violated that note.
- New capture candidates after close: worker ProcessExited must not gate on reap timing or worker exit status; ShutdownSession strict natural-exit idempotency is Events-or-SessionCleanup on every transport; Core welcome `worker_pid` prevents a parent wrapper from standing in for the real worker; a ShutdownSession classify error must not become cleanup from `list_sessions`.
