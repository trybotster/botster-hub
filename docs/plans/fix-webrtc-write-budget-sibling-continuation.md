# Hub tests: fix WebRTC write-budget sibling continuation failure

Ticket: `ticket_1786913892_208903`
Run: `run_1786914416_283641`
Parent run: `run_1786867245_870799` (Terminal Transport North Star integration)
Pipeline: Botster Stack Delivery, direct merge

## Target repository and target_id

| Field | Value |
| --- | --- |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| `target_repository` | `botster-hub` (`trybotster/botster-hub`) |
| Spawn-target name | `botster-hub` |
| Resolved by | `list_spawn_targets`, not the ambient working directory |
| Base SHA | `origin/main` `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| Locked Core SHA | `fc541a59338d0591ba4fb3fa522a030d212d26d0` from this checkout `Cargo.toml` / lockfile |
| Merge policy | direct into `main`; do not open a pull request |

This run worktree is already at that Hub SHA. Official spawn-target `botster-hub` is on the same SHA. Restore nothing in `.gitignore`. The worktree path has no `:`. Do not set `CARGO_TARGET_DIR`.

This ticket is not a consumer of Hub session-type eligibility. Do not inject `list_session_types_for_target` pins or Option A spawn.

## Repository playbook loaded

[[botster-hub-playbook]]

Hub owns adapter instances, WebRTC peer flush, route records, host-event emit, and IsolatedHub proofs. Core owns the 512-tick write-budget, subscription inventory, and non-blocking `adapter.close()` on the same host tick. Do not edit Core.

## Other role/surface playbooks and atomic notes loaded

Role and overlay:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] (loaded; this ticket is not SPA work)
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[vault example paths are not repository placement conventions]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Repository charter notes used for this surface:

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster runtime teardown lenses]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[Unix mux host frames flush before new terminal slots]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[a poisoned test lock is a symptom not a waiver]]
- [[suite wide acceptance criteria make every observed test failure in scope]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]

Did not load [[project-pipelines-playbook]]. This ticket does not change Project Pipelines package or plugin paths.

## Context loaded

Ticket text: IsolatedHub test `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable` failed on `origin/main` `c72712e` after 218 passed and 1 ignored. Snapshot Plan Review `review_1786913961_687902` / `finding_1786913961_210404` recorded that result after the documented session-worker build. That review created this ticket. It told snapshot paging not to absorb this root.

Sequencing is already confirmed:

- This run is a child of north-star run `run_1786867245_870799`.
- North-star ticket `ticket_1786661010_115885` depends on this ticket (`dependency_1786914413_945322`).
- Snapshot paging ticket `ticket_1786912570_127968` depends on this ticket (`dependency_1786913896_996295`).
- Event-plane tickets stay out of this repair.

The same test also failed once under superseded default-concurrency work (`run_1786875818_402849`). Isolated rerun passed. Isolated pass is diagnosis, not a suite waiver.

Shipped IsolatedHub proof in `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`:

1. Keep the peer readable.
2. Bind `yes write-budget-stall` and a sibling echo loop.
3. Wait up to 20s for `TerminalSubscriptionClosed` on the stall pair.
4. Require reason exactly `core_adapter_closed`.
5. Reject `host_adapter_closed` as the write-budget oracle.
6. Send `Status`, then `SendInput` `wwb-sibling-live` to the sibling.
7. Wait up to 8s of `next_terminal_frame` for that needle.

Unix counterpart `core_write_budget_hard_stop_emits_core_adapter_closed` is stronger after close: it proves `ListSessions` `running`, then pumps a content-blind sibling `Drain` that must stay owned while it collects `echo:cwb-sibling-live`.

Production WebRTC path still does both of these:

- Per-handle `Full` slot retention until `complete_active` after `send_response_frames`.
- Mux-wide `WebRtcConnectionMux::set_would_block` from `OnBufferedAmountHigh` / `OnBufferedAmountLow` in `src/local_webrtc.rs`. That walks every route.

The shipped write-budget WebRTC plan forbade mux-wide `set_would_block` because it stalls siblings. That contradiction is the leading production hypothesis.

`flush_webrtc_adapter_frames` is one sequential loop. An in-flight stall send waits while `flow_control.pressured` is true. Sibling slots sit behind that wait. That is the second production hypothesis.

The WebRTC test print still names locked Core `f4f6bf5babe92dfb9241a760c414187f711c2c42`. Current lockfile Core is `fc541a59`. Treat that string as stale provenance, not as a Core pin change.

Plan placement: this repository already stores living plans under `docs/plans/` and reports under `docs/reports/`. README excludes those trees from the PII audit. That is repository prior art, not a vault example path.

## Scope

1. Make the named IsolatedHub write-budget test pass under default-concurrency `./test.sh --locked` on this Hub target. Do not retry that binding run.
2. Diagnose first. Capture the exact panic line. Compare isolated `-- --exact webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable` with the binding suite.
3. If mux-wide `set_would_block` or sequential pressured flush blocks sibling writes after Core close, repair that Hub production path. Keep per-handle `Full` retention. Keep host-event flush before new terminal slots. Keep send-first bias and `LOCAL_WEBRTC_PEER_CLOSE_BOUND`.
4. If production already delivers the sibling frame and the 8s `next_terminal_frame` oracle misses it, tighten the IsolatedHub oracle only. Match the Unix sibling shape: content-blind owned `Drain`, live `echo:wwb-sibling-live` bytes, peer still readable. Do not use Status-on-timeout as the write-budget oracle. Do not use sleep as correctness.
5. Keep exact `core_adapter_closed`. Do not rewrite it to `host_adapter_closed`.
6. Keep keep-reading. Do not stop the client to force write-budget.
7. Record Hub SHA and lockfile Core SHA separately. Build `botster-session-worker` from `botster-core-daemon` before focused runtime tests and before the binding suite.
8. If a different default-concurrency root appears, create a separate blocker. Do not fold it into this ticket.

## Non-scope

- Snapshot paging, owner-loop scheduling, and event-plane delivery.
- Core write-budget (512 ticks), Core inventory, or Core `adapter.close()`.
- Web, TUI, TUI Kit, Workspaces, or hub-client DTO / protocol changes unless a production Hub change forces a consumed DTO. Prefer no DTO change.
- hub-test-support publish or conformance revision bump unless a public DTO actually changes.
- Unix mux policy except a shared helper that already classifies both transports.
- Authentic browser or TUI live proof. Downstream north-star owns that after this blocker closes.
- Cherry-picks from superseded branch `run_1786875818_402849`.
- Session-type eligibility pins.

## Repository ownership boundaries and cross-repo dependencies

| Layer | Owns here | Does not own here |
| --- | --- | --- |
| Hub | WebRTC mux flush, per-handle slots, host-event queue, IsolatedHub peer, route occupancy | Terminal bodies, 512-tick budget, adapter `close()` semantics |
| Core | Write-budget hard-stop, subscription + generation identity, non-blocking close | Hub peer maps, DataChannel flow control |
| hub-client | Consumed IsolatedHub DTOs only | New protocol work |
| Web / TUI | Nothing in this ticket | Downstream same-session proof |

No new cross-repo ticket is required if the repair stays in Hub runtime and Hub tests.

Already registered downstream consumers (do not re-register):

- `ticket_1786661010_115885` on this same Hub target (north-star integration)
- `ticket_1786912570_127968` on this same Hub target (snapshot paging)

If diagnosis proves a missing Core write-budget seam, stop. Register a Core ticket against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. Do not broaden this Hub run.

If IsolatedHub loopback cannot prove keep-reading sibling continuation without a new Hub terminal policy queue, ask a human. Do not add a test-only skip of the production flush.

## Assumptions and unknowns

1. Orchestrator spawn of this Plan run is the sequencing confirmation the ticket required. Do not wait again.
2. The snapshot review failure is the sibling assertion or a timeout that the ticket named as sibling continuation. Implement must paste the exact panic. Do not guess past that evidence.
3. Isolated green plus suite red remains likely. Isolated green is not acceptance.
4. Mux-wide `set_would_block(true)` on `OnBufferedAmountHigh` is the leading production cause. It is still in `src/local_webrtc.rs`. Confirm with ablation before calling it the fix.
5. Sequential `flush_webrtc_adapter_frames` can hide a sibling slot behind a pressured stall send. Confirm or reject with the same diagnosis pass.
6. WebRTC sibling proof today is weaker than Unix. Strengthening the oracle is allowed only after the production path is classified.
7. Lockfile Core `fc541a59` already ships 512-tick hard-stop. No Core pin change.
8. This ticket is test-titled. A production Hub flush or pressure bug that fails the shipped keep-reading oracle is still in scope. A test-only weakening of sibling continuation is out of scope.

## Affected surfaces/files

Likely production (only if diagnosis names them):

| Path | Why |
| --- | --- |
| `src/local_webrtc.rs` | `OnBufferedAmountHigh` mux-wide `set_would_block`; `flush_webrtc_adapter_frames` sequential pressured send |
| `src/webrtc_terminal_adapter.rs` | Per-handle `Full` / `WouldBlock` pressure; do not add mux-wide pressure here |

Likely tests and docs:

| Path | Why |
| --- | --- |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Named failing proof; sibling oracle; stale Core SHA print |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Regression: Unix write-budget sibling must stay green |
| `docs/reports/fix-webrtc-write-budget-sibling-continuation-implement.md` | Implement report |
| `docs/client-protocol.md` | Only if the documented keep-reading sibling contract changes |

Do not touch snapshot paging files, event-router files, or owner-loop scheduler policy.

## Runtime-teardown answers

`teardown_class_applies`: yes. The ticket is WebRTC adapter write-budget close, keep-reading peer, sibling isolation, and terminal-state versus live-runtime (`core_adapter_closed` versus `host_adapter_closed`).

`teardown_isolation`: One stalled generation dies. The peer, DataChannel, sibling subscription, host Status/SendInput/Drain path, and other peers stay up. Mux-wide `WouldBlock` that silences a healthy sibling is a failed isolation policy.

`teardown_bounds`: Adapter `close` stays non-blocking. DataChannel `local_close` stays inside `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. A pressured or abandoned terminal send must not `block_on` the Hub control thread and must not `close_all`. Only true peer or channel failure retires the peer.

`late_message_matrix`:

| Message | Tag | Reject after this generation is terminal | Residual sweep |
| --- | --- | --- | --- |
| Attach | grant + peer + live generation | Fail-closed if peer or grant is gone. Same-peer re-attach may host-close generation N | Replacement N+1 stays bound |
| Drain | owner grant/peer + session + subscription | Content-blind. No terminal bodies | No close event. Sibling Drain must stay owned |
| SendInput / Resize | host session | Stay available after one generation closes | No close event |
| Detach | session + subscription + live generation | `AlreadyGone` or generation mismatch | Suppress that generation. No emit |
| SubscribeEntities / UnsubscribeEntities | peer-owned subscription id | Drop frames the peer does not own | Existing peer cleanup. No `TerminalSubscriptionClosed` |
| Encrypted Hello | grant + peer + `terminal_subscription_closed` | After dying / `cleanup_sent`, no new live close-event admission | No emit without the feature |
| DataChannel OnClose / peer death | peer / grant | `close_all`, dying | No emit. One cleanup path |
| `OnBufferedAmountHigh` / `Low` | peer flow control | Must not convert healthy sibling handles into a shared `WouldBlock` after the stall generation is gone | Clear or never apply mux-wide pressure |
| ShutdownSession / process exit / session removal | host session | Lifecycle / `ProcessExit` only | `suppress_session`. No emit |
| Stale close for generation N after N+1 is live | generation | Ignore for N+1 | Must not close N+1 |
| Control request after peer death | peer gone | No durable ownership | Existing forget/remove path |

`production_path_proof`: Live IsolatedHub peer through production `run_data_channel` / `flush_webrtc_host_events` / `flush_webrtc_adapter_frames`. Path: Core 512-tick hard-stop → non-blocking adapter `close()` → control-thread queue → host-event `daemon_event` with `core_adapter_closed` → sibling `daemon_terminal_frame` `echo:wwb-sibling-live` while the observer keeps reading. Status-on-timeout, a terminal JSON file, and a unit `close()` helper are not proof.

`ownership_identity`: session + subscription + generation + grant/peer. Delayed PeerClosed or stall cleanup must not sweep a live sibling or a replacement generation. Mux envelope delivery is not the ownership oracle. Content-blind sibling Drain or Hub-visible live-attach occupancy is.

`sibling_fail_closed_policy`: On successful stall close, siblings keep working and the host session stays. On ultimate peer-close failure, that peer retires once through existing cleanup. Other peers are untouched. A pressured stall must not sacrifice the sibling. Test both the success path and the existing peer-death negative.

## Implementation steps

1. Fetch and confirm HEAD is `origin/main` `c72712e` or later main. Do not cherry-pick the superseded branch.
2. `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
3. Isolated: `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`. Record pass or fail and the panic if any.
4. Classify from the panic and from flush/pressure traces if needed:
   - Production if sibling writes are `WouldBlock` or stuck behind the stall send after Core close.
   - Test-oracle if the sibling frame is already on the peer and the 8s collector misses it.
   - Human question if keep-reading IsolatedHub cannot create Core write-budget without a new Hub terminal policy queue.
5. Apply the smallest repair that matches that class. Preferred production repair if confirmed: stop applying DataChannel buffer pressure to every mux handle. Keep per-handle `Full` for the in-flight stall slot. Keep host events first. Do not add a second write-budget.
6. If the oracle is weak after production is correct, add the Unix-shaped sibling checks: `ListSessions` running or content-blind owned Drain, then live `echo:wwb-sibling-live`. Keep keep-reading. Keep the 20s close wait as an upper bound, not as the oracle.
7. Ablate the chosen repair. The named sibling assertion must go red. An earlier close-reason failure does not vouch for the sibling assertion.
8. Keep Unix `core_write_budget_hard_stop_emits_core_adapter_closed` green. Keep `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` as a focused regression, not as this ticket's repair target.
9. Strict gates: rustfmt, workspace clippy `-D warnings`, then one clean default-concurrency `./test.sh --locked` after the worker build. No retry. No `--test-threads=1` as acceptance.
10. Commit on this run branch. Write `docs/reports/fix-webrtc-write-budget-sibling-continuation-implement.md`. Direct-merge only after Review and Verify. Downstream snapshot and north-star tickets re-verify their own suites after this merge.

## Risks

1. Isolated pass hides a mux-wide pressure bug that only appears after other WebRTC tests fill host pipes.
2. Removing mux-wide `set_would_block` can put the sibling slot into `Full` during the stall flood and trip sibling write-budget. The repair must keep sibling `Full` ticks from reaching 512.
3. Inflating the 8s wait without an ownership pump will mask the same stall.
4. Status-on-timeout can hide the queue-versus-reconcile reason race. Do not restore that oracle.
5. `reconcile_inventory` can still rewrite Core close to host close if `close_from_host` runs on an already-closed handle. Do not change that order.
6. A later default-concurrency root can appear. Create a new blocker. Do not expand this ticket.
7. Leftover IsolatedHub daemons from a crashed suite can shrink pipes. Tear sessions down. Do not serialize the suite.

## Acceptance checks/tests

Required proofs:

1. Isolated keep-reading write-budget: exact `core_adapter_closed` for `wwb-stall` / `sub-stall`. No `host_adapter_closed` for that pair. Peer stays readable.
2. Sibling continuation on the same peer: `echo:wwb-sibling-live` as `daemon_terminal_frame` after close. Content-blind sibling Drain stays owned if the oracle is tightened. Mux delivery alone is not the ownership oracle.
3. Status remains available after Core close.
4. Unix write-budget sibling `echo:cwb-sibling-live` still passes.
5. Focused adapter unit tests and `assert_terminal_adapter_conformance` still pass.
6. Ablation: revert the repair; the sibling assertion fails at that assertion.
7. One clean default-concurrency `./test.sh --locked` after the locked worker build. Record Hub binary realpath, worker realpath, Hub SHA, and lockfile Core SHA. Isolated pass is not this gate.
8. If a different test fails in that binding run, stop and open a separate blocker.

Commands:

```
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline --locked -- -D warnings
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact core_write_budget_hard_stop_emits_core_adapter_closed
./test.sh --locked
```

Do not use `./test.sh --test hub_daemon_lifecycle_test webrtc_terminal_adapter` as the sole acceptance command. That name filter is not the binding suite.

Downstream: after merge, snapshot paging and north-star re-run their own required suites. This ticket does not claim those suites.

## Vault gaps worth capturing

No new inbox note in this Plan visit. The mux-wide `OnBufferedAmountHigh` versus sibling `WouldBlock` conflict is already implied by the shipped write-budget plan and [[Unix mux host frames flush before new terminal slots]]. Capture only after Implement confirms that production path with ablation. Do not capture an unconfirmed diagnosis.

Convention conflicts: none that change this plan. The existing production mux-wide `set_would_block` conflicts with the already-approved write-budget sibling rule. This plan restores that rule. It does not invent a new one.
