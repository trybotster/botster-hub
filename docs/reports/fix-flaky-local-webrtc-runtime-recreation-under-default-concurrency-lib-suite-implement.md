# Implement report: fix flaky local_webrtc runtime recreation under default-concurrency lib suite

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786919221_923340` |
| Run | `run_1786944941_256532` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786919221_923340` |
| Base | Hub `origin/main` `547ca3826a4719d1e448e8ae694cafc4c8591747` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown (`teardown_class_applies: yes` — WebRTC peer lifecycle and dedicated-runtime teardown oracle) |
| Plan | `docs/plans/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite.md` @ `7a3c99b` |
| Implement checklist | `checklist_1786946424_127899` (run-scoped). Timeout-duplicate `checklist_1786946433_791156` skipped. |

Independent routing: `project_pipelines_current_context` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan uses the same target. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[botster runtime teardown lenses]]
- [[botster hub is a first party host profile over core]]
- [[test script required for rust tests not cargo test]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Keep all changes `#[cfg(test)]` in `src/local_webrtc.rs`.
- Do not change production park, fail-closed drop, `stop_all`, or `LOCAL_WEBRTC_PEER_CLOSE_BOUND`.
- Keep the four worker-join deadlines at 2 s unless an instance-local 2 s expiry appears.
- Keep `teardown_test_lock` coverage. Update only the process-global counter sentence in its doc comment.
- Prefer repair over quarantine. Do not start from `#[ignore]` or `--test-threads=1`.
- Use `./test.sh`. Do not use bare `cargo test`.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/local_webrtc.rs` — `#[cfg(test)]` only. Replace process-global `LOCAL_WEBRTC_WORKER_THREADS` with `LocalWebrtcTransport.worker_threads: Arc<AtomicUsize>`. Runtime-builder hooks clone that `Arc`. Convert `dedicated_runtime_worker_threads()` to an instance method. Update all 8 read sites through `harness.daemon.local_webrtc()`. Update the `teardown_test_lock` doc comment. Keep all four 2 s wait deadlines.

Handoff:

- `docs/reports/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns the local WebRTC transport, the daemon control plane, and this lib test. Compiled non-test production code is unchanged. Core, hub-client, Web, TUI, hub-test-support, and package/plugin paths were not edited. No lockfile change.

## Cross-repo routing

No cross-repository prerequisite and no PR. Same-target siblings were not absorbed:

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Parent-run owner; parked on this ticket's sibling chain. |
| `ticket_1786916741_161067` | separators_close lib flake | Closed; discovered this root and forbids absorption. |
| `ticket_1786921010_869253` | near_limit lib flake | Closed; repair merged at `547ca38`. |
| `ticket_1786937228_425608` | unix_adapter lifecycle failure | Open; lifecycle suite, outside `--lib`. |

## Deviations from plan

None. Scope item 3 contingency did not fire: no 2 s worker-join wait expired after instance scoping. Deadlines stay at 2 s.

Control B used the plan's allowed option: a temporary process-global counter that the thread hooks also updated, plus one two-harness fixture. The red row waited on that global. The green row waited on the first daemon's instance counter and asserted the second daemon's instance counter was `>= 1` during the wait. Both fixtures and the temporary global were reverted before the committed diff.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | Production `remove_peer` still removes one peer and parks only when the map is empty. The worker-join oracle is now instance-scoped, so one test cannot observe another test's live runtime. |
| Bounds | Production close bound and `runtime.take()` hard stop are unchanged. Test waits stay at 2 s. |
| Late-message matrix | Unchanged. No new ownership-creating message. Existing SubscribeEntities, UnsubscribeEntities, Attach, and Spawn tests keep their tags, rejections, and sweeps. Their counter read sites were converted only where they already used the shared oracle. |
| Production-path proof | The named test still drives `inject_peer_connection_state_for_test` → production `on_connection_state_change` → `LocalWebrtcPeerClosed` → `remove_peer` → `park_runtime_if_idle`. The oracle remains live `on_thread_start` / `on_thread_stop` evidence. Control A proves the stop hook feeds the wait. |
| Ownership identity | Peer rows still key on `grant_id`. Counted workers now belong to one `LocalWebrtcTransport`. |
| Sibling / fail-closed | `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime` and `local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads` keep their meaning under the instance oracle. |

No lens was dropped to informal follow-up.

## Unlocked dedicated-runtime mutators

Mechanical enumeration of `signal_peer` callers in `src/local_webrtc.rs` (22 call sites plus the method). Early FakeDataChannel tests do not call `LocalWebrtcTransport::runtime()`.

Take `teardown_test_lock`:

- `local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads`
- `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime`
- `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds`
- `webrtc_hello_bind_echoes_capability_set_and_closes_adapter_on_peer_loss`
- `local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads`
- `local_webrtc_attach_owner_sweep_on_empty_snapshot`
- `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner`
- `run_close_hang_fail_closed_body` (child body for `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline`)

Do not take the lock (unlocked counter mutators before this repair):

- `local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state`
- `local_webrtc_spawned_session_is_cleaned_even_if_attach_proof_panics_after_ready`
- `local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner`
- `local_webrtc_subscribe_before_peer_closed_is_swept_by_owner_grant`
- `local_webrtc_late_attach_after_peer_closed_does_not_recreate_state`
- `local_webrtc_late_spawn_after_peer_closed_does_not_create_session`
- `local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row`

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 (cached) |
| Pre-change 5× `./test.sh --locked --lib local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` | all exit 0 |
| Pre-change 2× `./test.sh --locked --lib` | both exit 0; hub lib 351 passed, 0 failed |
| Post-change focused wrapper | exit 0 |
| Control A: comment out `on_thread_stop` decrement, then focused wrapper | exit 101; `timed out waiting for first dedicated runtime workers to join` |
| Control B red: two-harness fixture, wait on restored process-global counter | exit 101; `timed out waiting for first dedicated runtime workers to join` |
| Control B green: same fixture, instance-scoped wait plus second-daemon `>= 1` scratch assert | exit 0 |
| Restore Control A/B fixtures | committed tree has no ablation hooks or temporary global |
| Post-change 20× focused wrapper | 20 pass, 0 fail |
| Post-change 5× `./test.sh --locked --lib` | 5 pass, 0 fail; each hub lib 351 passed, 0 failed |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

`-- --test-threads=1` was not used as a suite command. Full `./test.sh --locked` (lifecycle) was not a binding gate.

Production entry point already using the behavior: last-peer `PeerClosed` → `remove_peer` → `park_runtime_if_idle` → `runtime.take()`, then a later `signal` recreates the dedicated runtime. This ticket does not add a production branch. It makes the live worker-join oracle observe only the daemon under test.

Downstream consumer proof: not required. No public surface, DTO, pin, or compiled runtime behavior changed.

## Unverified behavior or residual risk

- Pre-change default-concurrency `--lib` did not reproduce the original flake in two runs. Non-reproduction does not prove absence. The interference channel is proven by code inspection and by Control B red.
- Instance scoping no longer fails this wait when an unrelated test leaks workers. That global property was never this test's contract.
- A later 2 s instance-local expiry under heavier load would belong in a new ticket. Scope item 3 contingency did not apply here.
- Full workspace `./test.sh --locked` (lifecycle suite) remains owned by `ticket_1786937228_425608`.
- Remaining wall-clock assertion sites in `src/daemon_entity_subscriptions.rs` stay on the recorded sweep inventory.

## Missing vault guidance discovered

[[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] covers wall-clock owner-turn oracles. It does not record that a process-global thread-hook counter makes `== 0` waits observe other tests' runtimes.

Captured after Implement confirmed the repair:

- inbox `process-global-test-counters-make-zero-waits-observe-other-tests-under-default-concurrency-lib-load.md`

The prior-run `target_id` anomaly is not captured. Plan Review did not confirm it as an engine defect.

No convention conflict. Hub charter, runtime-teardown lenses, and the approved plan agree: repair this named lib-suite oracle here; leave production teardown and lifecycle-suite roots to their tickets.
