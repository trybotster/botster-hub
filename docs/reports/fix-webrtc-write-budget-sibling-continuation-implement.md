# Implement report: Hub tests: fix WebRTC write-budget sibling continuation failure

Ticket: `ticket_1786913892_208903`  
Run: `run_1786914416_283641`  
Step: `botster_stack_implement`  
Status: production repair committed; Implement parked on `ticket_1786916741_161067` until a clean `./test.sh --locked` after that merge

Human answer `question_1786916746_614820`: commit the focused write-budget repair now; do not waive clean-suite acceptance; park Implement on the separators blocker; after that ticket merges, update from Hub main and run `./test.sh --locked` once; advance to Review only if that suite passes.

## Target repository and target_id

| Field | Value |
| --- | --- |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| `target_repository` | `botster-hub` (`trybotster/botster-hub`) |
| Worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786913892_208903` |
| Base SHA | `origin/main` `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| Locked Core SHA | `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Merge policy | direct into `main` after Review/Verify; no PR |

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

Did not load [[project-pipelines-playbook]]. No Project Pipelines package/plugin paths changed.

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

New same-repo blocker from this Implement visit:

- `ticket_1786916741_161067` — flaky `separators_close_when_item_bytes_fit_but_commas_do_not` under default-concurrency lib suite
- `dependency_1786916742_692599` — this ticket depends on that blocker

Blocking human question: `question_1786916746_614820` (commit/park vs waive clean-suite for Review).

## Deviations from plan

None that change the committed plan contract. Oracle strengthening matched plan step 6 after production classification. Binding `./test.sh --locked` was not retried after the different root failed.

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

Binding (one run, no retry):

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Result: lib suite `351 passed; 1 failed` on `daemon_transport::daemon_entity_subscriptions::tests::separators_close_when_item_bytes_fit_but_commas_do_not` (`SnapshotAssemble::Closed { frame_too_large: true }` at `src/daemon_entity_subscriptions.rs:3179`). Lifecycle suite did not start.

Pre-existing / unrelated isolation for that root:

```
cargo test --offline --locked --lib separators_close_when_item_bytes_fit_but_commas_do_not
```

- Branch: PASS  
- Base `c72712e` (`/Users/jasonconigliari/Projects/botster-hub`): PASS  

Production entry point: `apply_data_channel_event` in `src/local_webrtc.rs` on the live `run_data_channel` path. IsolatedHub keep-reading write-budget proof exercises `flush_webrtc_host_events` / `flush_webrtc_adapter_frames` through production peer delivery.

Downstream north-star / snapshot suites are not claimed here.

## Unverified behavior or residual risk

- Binding default-concurrency full `./test.sh --locked` is not green on this visit because of the separators root.
- Lifecycle suite under default concurrency was not reached after the lib failure.
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
