# Implement report: Hub tests: fix WebRTC write-budget sibling continuation failure

Ticket: `ticket_1786913892_208903`
Run: `run_1786914416_283641`
Step: `botster_stack_implement`
Status: production repair committed at `6d69028`; granted-slot suite recorded; orchestrator option 3: advance to Review now. Review `review_1787005207_243300` returned Implement for report hygiene only.

Human answer `question_1786916746_614820`: commit the focused write-budget repair now; do not waive clean-suite acceptance; park Implement on the separators blocker; after that ticket merges, update from Hub main and run `./test.sh --locked` once; advance to Review only if that suite passes.

Resume `msg_device-2_1786920855_84ce4a`: separators blocker merged at `a55f62d`. Integrated that revision. One `./test.sh --locked` failed on `near_limit_snapshot_assembly_stays_within_owner_turn`. Did not retry.

Resume `msg_device-2_1786936526_3c77f6`: near-limit blocker merged at `547ca38`. Integrated that revision. One `./test.sh --locked` after the worker build: lib 352/0; named write-budget test PASSED; lifecycle 218/1/1 on `unix_adapter_unbound_printf_stream_attach_completes`. Did not retry. Did not advance.

Resume `msg_device-2_1787003545_653fe5`: orchestrator granted the only default-concurrency full-suite slot. Ran the planned one `./test.sh --locked` on HEAD `921d525` after the worker build. Did not retry. Did not create serial dependencies. Slot released after the command finished.

Resume `msg_device-2_1787004353_7ccc54`: orchestrator disposition option 3. Advance to Review now. Do not run another full suite. Do not wait for unrelated owner tickets. Final integration remains the zero-failure gate on those owner tickets.

## Target repository and target_id

| Field | Value |
| --- | --- |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| `target_repository` | `botster-hub` (`trybotster/botster-hub`) |
| Worktree | the pipeline-provided ticket worktree |
| Base SHA | original `c72712e`; later integrated `a55f62d` then Hub main `547ca3826a4719d1e448e8ae694cafc4c8591747` |
| Integrate merge | `6d376f3` (`a55f62d`); `fc655ee9f61b351fc8d5289a10e4c5fe49821717` (`547ca38`) |
| Locked Core SHA | `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Merge policy | direct into `main` after Review/Verify; no PR |
| Implement commit | `6d69028bd6ec3a3d36f319c82fca58c01ee2d249` |

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[Unix mux host frames flush before new terminal slots]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[a poisoned test lock is a symptom not a waiver]]
- [[suite wide acceptance criteria make every observed test failure in scope]] (plan-scoped: different root becomes a separate blocker)
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[pipeline artifacts should use path neutral worktree references]]

Did not load [[project-pipelines-playbook]]. No Project Pipelines package/plugin paths changed.

## Review return (`review_1787005207_243300`)

Review approved Hub routing and focused runtime behavior. Review required report hygiene before Review admission.

- Finding `finding_1787005207_131757`: replaced personal absolute worktree and Hub-main-checkout paths with path-neutral wording.
- Finding `finding_1787005207_117422`: removed markdown hard-break trailing spaces so `git diff --check` is clean.

No production code change in this Implement visit. No new suite run. Orchestrator option 3 still applies.

## Diagnosis

Exact panic from prior suite evidence (`/tmp/hub-write-budget.log`, finding lineage for this ticket):

```
tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:947:9
sibling daemon_terminal_frame must continue: [<only wwb-stall terminal_output frames>]
```

Isolated exact test on this worktree before the repair: PASS (11.21s).
Classification: production Hub path. Mux-wide `WebRtcConnectionMux::set_would_block` on `OnBufferedAmountHigh` / `OnBufferedAmountLow` marks every registered handle `WouldBlock`, which can silence a healthy sibling while one stalled generation fills the DataChannel send buffer. Per-handle `Full` retention remains the write-budget pressure signal.

Sequential pressured `flush_webrtc_adapter_frames` was not separately repaired. Host-first flush, send-first bias, and `LOCAL_WEBRTC_PEER_CLOSE_BOUND` were preserved.

## Files changed

| Path | Change |
| --- | --- |
| `src/local_webrtc.rs` | Stop mux-wide WouldBlock on buffered-amount high/low; keep `flow_control.pressured`; add registered-route unit proof |
| `src/webrtc_terminal_adapter.rs` | Remove unused mux/handle `set_would_block` API; keep per-handle test force/clear |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Unix-shaped sibling oracle (`ListSessions` running, content-blind owned Drain, `echo:wwb-sibling-live`); refresh locked Core provenance print |
| `docs/plans/fix-webrtc-write-budget-sibling-continuation.md` | Approved plan (untracked until commit) |
| `docs/reports/fix-webrtc-write-budget-sibling-continuation-implement.md` | This report |

## Ownership boundaries preserved

- Hub owns WebRTC peer flush, route records, host-event emit, IsolatedHub proofs.
- Core write-budget (512 ticks), inventory, and non-blocking `adapter.close()` were not edited.
- No hub-client DTO / protocol change.
- No Web / TUI / Workspaces edits.

## Cross-repo dependencies or separately routed work

Already registered downstream consumers unchanged:

- `ticket_1786661010_115885` (north-star)
- `ticket_1786912570_127968` (snapshot paging)

Same-repo blockers:

- `ticket_1786916741_161067` / `dependency_1786916742_692599` — separators flake; merged at Hub main `a55f62d`
- `ticket_1786921010_869253` / `dependency_1786921011_357767` — near-limit owner-turn flake; merged at Hub main `547ca38`
- `ticket_1786937228_425608` / `dependency_1786937228_989504` — `unix_adapter_unbound_printf_stream_attach_completes` default-concurrency lifecycle failure

Blocking human question: `question_1786916746_614820` (commit/park vs waive clean-suite for Review). Later orchestrator `msg_device-2_1787004353_7ccc54` directed Review admission on focused write-budget proof plus known-baseline owner tickets. This ticket does not wait on those owners. Final integration still requires a zero-failure suite.

## Deviations from plan

Oracle strengthening matched plan step 6 after production classification. Binding `./test.sh --locked` was not retried after a different root failed.

Orchestrator option 3 (`msg_device-2_1787004353_7ccc54`) changes Review admission: this Implement step advances without a clean default-concurrency `./test.sh --locked`. The three granted-slot lifecycle failures stay on their owner tickets. Final integration remains the zero-failure gate and is not claimed here.

## Tests and downstream proof run

Worker build:

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
```

Focused:

```
cargo test --offline --locked --lib buffered_amount_high_does_not_mark_sibling
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact core_write_budget_hard_stop_emits_core_adapter_closed
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline --locked -- -D warnings
```

Ablation: temporarily restored mux-wide `set_would_block` on registered routes. Unit test failed at sibling `Ready` vs `WouldBlock` (not at an earlier assertion). Fix restored from fixed-file backup.

Binding first visit (one run, no retry) on `c72712e` plus write-budget repair:

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Result: lib suite `351 passed; 1 failed` on `daemon_transport::daemon_entity_subscriptions::tests::separators_close_when_item_bytes_fit_but_commas_do_not` (`SnapshotAssemble::Closed { frame_too_large: true }` at `src/daemon_entity_subscriptions.rs:3179`). Lifecycle suite did not start. Isolated that test PASS on branch and base `c72712e`.

Binding resume after integrating `a55f62d` (one run, no retry):

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Result: lib suite `351 passed; 1 failed` on `daemon_transport::daemon_entity_subscriptions::tests::near_limit_snapshot_assembly_stays_within_owner_turn` (`started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` at `src/daemon_entity_subscriptions.rs:3033`). Lifecycle suite did not start.

Isolation for that root (`cargo test --offline --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn`):

- Branch after integrate: PASS
- Base `a55f62d` (the Hub main checkout): PASS

Binding resume after integrating `547ca38` (one run, no retry):

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Result: lib `352 passed; 0 failed`. Lifecycle `218 passed; 1 failed; 1 ignored`. Named write-budget test PASSED. Failure: `unix_adapter_unbound_printf_stream_attach_completes` at `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:752` (`ProcessExited must not shut down the host session`; `uap-session` lifecycle `exited`).

Isolation (`cargo test --offline --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes`):

- Branch after integrate: PASS (2.88s)
- Base `547ca38` (the Hub main checkout): PASS (2.67s)

Granted-slot binding on HEAD `921d525` (one run, no retry; slot then released):

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Result: lib `352 passed; 0 failed`. Lifecycle `216 passed; 3 failed; 1 ignored`. `SUITE_EXIT=101`. Named write-budget test PASSED. Failures recorded on existing or new owner tickets, with no serial dependency from this write-budget ticket:

- `ready_spawn_stays_within_budget_during_session_snapshot_assembly` — `sessions.rs:3634` waited 57.421125ms. Owner `ticket_1786938984_190098`.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` — `sessions.rs:3587` waited 52.535125ms. Owner `ticket_1786938984_190098`.
- `unix_shutdown_session_from_another_connection_classifies_attached_exit` — `unix_terminal_adapter.rs:1736` `ShutdownSession` kind was `OperatorError`. Owner `ticket_1787004132_469467` (no write-budget dependency).

Production entry point: `apply_data_channel_event` in `src/local_webrtc.rs` on the live `run_data_channel` path. IsolatedHub keep-reading write-budget proof exercises `flush_webrtc_host_events` / `flush_webrtc_adapter_frames` through production peer delivery.

Downstream north-star / snapshot suites are not claimed here.

## Unverified behavior or residual risk

- Granted-slot `./test.sh --locked` is not green. Named write-budget proof passed. Remaining failures are recorded on other owner tickets, not as serial blockers of this ticket.
- Final integration remains the zero-failure gate on those owner tickets. This Review request does not claim that gate.
- Sequential pressured flush of a closed stall handle was not separately changed; residual sibling delay under sustained high water remains possible but was not the confirmed mux-wide WouldBlock class.
- Authentic browser/TUI same-session proof remains with north-star after merge.

## Missing vault guidance discovered

None that blocked the repair. After merge of the confirmed mux-wide WouldBlock fix, capture is still optional per the plan (conflict already implied by the shipped write-budget plan and [[Unix mux host frames flush before new terminal slots]]).

## Runtime-teardown lenses (implemented)

| Lens | Evidence |
| --- | --- |
| Isolation | Mux-wide WouldBlock removed; sibling stays Ready/Full under high water |
| Bounds | Unchanged peer close bound; no new blocking control-thread wait |
| Late-message matrix | As planned; High/Low no longer convert sibling handles |
| Production-path proof | IsolatedHub keep-reading write-budget + sibling echo path |
| Ownership identity | Content-blind sibling Drain + ListSessions running |
| Sibling policy | Sibling continuation oracle strengthened to Unix shape |

## Convention conflicts

None. Existing mux-wide `set_would_block` conflicted with the approved write-budget sibling rule; this repair restores that rule.
