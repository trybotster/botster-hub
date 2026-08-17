# Plan: Fix flaky local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds

Ticket: `ticket_1786919221_923340`
Run: `run_1786940212_977356`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

The separators Implement binding (`ticket_1786916741_161067`) hit one lib-suite failure on `local_webrtc::tests::local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds`. The panic was `timed out waiting for first dedicated runtime workers to join` at `src/local_webrtc.rs:4237` (the shared `wait_until` panic site). Suite result: 349 passed; 2 failed. The same test passed in isolation on the ticket branch and on base `origin/main` `c72712e` with the exact wrapper command. This ticket repairs or quarantines that default-concurrency root on botster-hub.

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the ticket record through `list_spawn_targets` to `trybotster/botster-hub`, and confirmed by the ticket worktree `origin` remote.
- Routing anomaly, recorded for Plan Review: the run record (`run_1786940212_977356`) carries `target_id` `tgt_7e208a0c76a449f4ac0c99953a799869`, which matches no spawn target. The ticket record is the authoritative routing source per the step prompt, and the ticket target resolves cleanly. Planning proceeds against the ticket target.
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786919221_923340`, clean, at `547ca38` = current `origin/main` (the separators and near-limit flake repairs are already merged beneath it).
- The worktree path contains no colon. `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon control plane, the local WebRTC transport, and this lib test.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene, runtime-teardown class trigger.
- [[botster runtime teardown lenses]] -- the class applies; answers below.
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] -- the sibling flake class: wall-clock and load-sensitive oracles are not valid under default-concurrency lib load. This ticket is the same disease in a different organ: a process-global counter predicate plus a tight wall-clock deadline.
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] -- prior-art repair idiom and one-ticket-per-root discipline.
- [[a regression test must be shown to go red with the fix reverted]] -- the repaired oracle needs ablation red-proof.
- [[terminal webrtc failure records do not prove peer runtime teardown]] -- the worker-join oracle must remain live thread evidence; the repair must not degrade it to state-file checks.
- [[plugin worker unload deadline can flake under default-concurrency workspace load]] -- distinguishes isolated diagnosis from the default-concurrency gate.

## Context loaded

- Ticket, run, gates, and empty prior artifacts/checklists via `project_pipelines_current_context` for `run_1786940212_977356`; parent run `run_1786914416_283641` context for lineage (write-budget Implement is parked on this ticket's sibling chain).
- Prior art in this repository:
  - `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` (`ef88ad1`) and `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` (`cb9be95`, merged through `547ca38`) -- the flake-repair plan format, wrapper-only acceptance commands, and the one-root-per-ticket rule.
  - `a1c0e5a`/`a55f62d` and `cd5e7a8`/`547ca38` -- the merged repair idiom: replace load-sensitive oracles with deterministic ones; never change production budgets.
- Code read in `src/local_webrtc.rs`:
  - Failing test `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` (`:5237-5273`).
  - Panic site `wait_until` (`:4233-4238`) and `soft_wait_until` (`:4241-4249`).
  - Process-global counter `LOCAL_WEBRTC_WORKER_THREADS` (`:145-147`), maintained by `#[cfg(test)]` `on_thread_start`/`on_thread_stop` hooks on the dedicated runtime builder (`:433-442`), read by associated fn `dedicated_runtime_worker_threads()` (`:481-484`).
  - Production teardown path: `remove_peer` (`:234-267`) -> `park_runtime_if_idle` (`:294-302`) / `fail_closed_drop_dedicated_runtime` (`:312-333`) -> `self.runtime.take()`; `stop_all` (`:215-227`).
  - `teardown_test_lock` (`:3971-3978`): serializes only the tests that opted in (8 lock sites: `:5091`, `:5173`, `:5238`, `:5340`, `:5454`, `:6096`, `:6185`, `:6317`).
  - All 8 counter read sites: `== 0` waits at `:5163`, `:5258`, `:5553`, `:6446`; `>= 1` asserts at `:5100`, `:5268`, `:5477`, `:6342`. No uses outside this file.
- `test.sh` -- the repo wrapper: checks hub-test-support asset sync, sets `BOTSTER_ENV=test`, runs `cargo test --workspace`. Workspace scope is load-bearing.
- `project_pipelines_search_tickets` for the test name returns only this ticket. No duplicate exists.
- Fresh corroboration that the test is healthy in a quiet suite: parent-run Implement artifact `artifact_1786937280_416739` records one `./test.sh --locked` at integrated main `547ca38` with `lib_suite: 352 passed, 0 failed`, which includes this test.

## Failure mechanism

The test proves last-peer teardown and runtime recreation: inject `RTCPeerConnectionState::Failed` through the production handler, process until `LocalWebrtcPeerClosed`, assert the peer map is empty and the dedicated runtime is parked, then wait up to 2 seconds for `LocalWebrtcTransport::dedicated_runtime_worker_threads() == 0`, then signal a second peer and assert the runtime is recreated.

Two load-sensitive couplings exist in the waited predicate:

1. **Process-global counter interference.** `LOCAL_WEBRTC_WORKER_THREADS` is one `static AtomicUsize` for the whole test process. Every dedicated local-WebRTC runtime in any concurrently running test increments it. `teardown_test_lock` exists to serialize counter-sharing tests, but only 8 tests take it, while the module has about 37 `signal_peer` harness call sites. Unlocked peer tests that hold a live dedicated runtime during this test's 2-second window keep the global counter above zero, so the `== 0` predicate can never become true regardless of this test's own correct teardown. Examples of unlocked counter mutators: `local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state` (`:5276`), `local_webrtc_spawned_session_is_cleaned_even_if_attach_proof_panics_after_ready` (`:5581`), `local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner` (`:5625`), `local_webrtc_subscribe_before_peer_closed_is_swept_by_owner_grant` (`:5743`), `local_webrtc_late_attach_after_peer_closed_does_not_recreate_state` (`:5841`). Implement must enumerate the complete set mechanically.
2. **Tight wall-clock deadline under suite load.** Even without interference, the 2-second bound is a wall-clock patience budget for OS worker-thread exit after `runtime.take()`. Under default-concurrency lib load the scheduler can delay thread exit and hook execution past 2 seconds. The sibling waits at `:5163`, `:5553`, and `:6446` carry the same 2-second bound, while the same tests use 10-second bounds for `process_until_peer_closed`.

Isolated runs pass because no other test holds the counter and thread exit is prompt on an unloaded machine. This is a test-oracle coupling defect, not a production teardown regression. Production park, fail-closed drop, and close-bound behavior are correct and stay unchanged.

The mechanism-1 interference channel is proven by code inspection (global static + unlocked mutators + `== 0` predicate); which mechanism fired in the recorded failure is not recoverable from the panic line. The repair below removes both.

## Runtime-teardown lens answers

`teardown_class_applies`: yes. The ticket's subject test is WebRTC peer lifecycle and dedicated-runtime teardown proof. The repair changes only the test-side worker-join oracle; every production teardown path stays untouched, and the lens answers below record what the repaired test must keep proving.

`teardown_isolation`: production unchanged -- `remove_peer` removes exactly the closed peer's ownership (`take_remove_result`), parks the runtime only when the peer map is empty, and `fail_closed_drop_dedicated_runtime` names the deliberate sibling-sacrifice path. Test-side: the oracle becomes instance-scoped, so one test's teardown observation cannot be corrupted by another test's live runtime -- the same isolation principle applied to test observation.

`teardown_bounds`: production bounds unchanged -- `LOCAL_WEBRTC_PEER_CLOSE_BOUND` on close, `runtime.take()` as the hard stop for driver loops. Test-side: the worker-join wait stays bounded (raised from 2 s to 10 s, matching the module's `process_until_peer_closed` patience bound). A hang still fails the test at the deadline; the bound is patience for progress, not a production latency claim.

`late_message_matrix`: unchanged. Late `SubscribeEntities` and late `Attach` after `PeerClosed` are owned by the existing dedicated tests (`:5276`, `:5841`) and are not modified. This repair adds no ownership-creating message surface.

`production_path_proof`: preserved. The test keeps driving `inject_peer_connection_state_for_test` -> production `LocalWebrtcHandler::on_connection_state_change` -> `LocalWebrtcPeerClosed` -> `remove_peer` -> `park_runtime_if_idle`. The worker-join oracle remains live-thread evidence from the runtime builder's `on_thread_start`/`on_thread_stop` hooks -- not a terminal record, per [[terminal webrtc failure records do not prove peer runtime teardown]]. Instance scoping strengthens the recreation assertions: today the `>= 1` asserts (`:5100`, `:5268`, `:5477`, `:6342`) can be satisfied by another test's runtime; after the repair they can only be satisfied by this daemon's runtime.

`ownership_identity`: unchanged for peers -- grant_id keys every peer-owned row. The counter gains an owner: it becomes per-`LocalWebrtcTransport` state, so counted workers are attributable to the daemon under test.

`sibling_fail_closed_policy`: unchanged and still tested -- `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime` (`:5172`) proves sibling preservation on success; `local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads` (`:5453`) proves the named fail-closed blast radius. Both keep their meaning under the instance-scoped oracle.

## Scope

Repair the worker-join oracle so default-concurrency `--lib` cannot fail `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` through cross-test counter interference or tight wall-clock patience, while the test keeps proving that last-peer cleanup joins this daemon's dedicated-runtime workers and that a new signal recreates the runtime.

All changes are `#[cfg(test)]`-only. Compiled non-test production code stays byte-identical.

1. Replace the process-global `static LOCAL_WEBRTC_WORKER_THREADS` with an instance-scoped counter on `LocalWebrtcTransport`: a `#[cfg(test)] worker_threads: Arc<AtomicUsize>` field (the struct already derives `Default`; `Arc<AtomicUsize>` satisfies it). The `#[cfg(test)]` `on_thread_start`/`on_thread_stop` hooks in `runtime()` clone that `Arc` instead of touching a global.
2. Convert `dedicated_runtime_worker_threads()` from an associated fn to an instance method reading the instance counter. Update all 8 read sites mechanically to read through `harness.daemon.local_webrtc()`. The named flake root is the behavioral target; the sibling waits (`:5163`, `:5553`, `:6446`) and `>= 1` asserts take the same mechanical conversion because the shared oracle is one surface -- this is one oracle repair, not four test rewrites.
3. Raise the four worker-join wait deadlines from 2 s to 10 s, matching the module's existing `process_until_peer_closed` patience bound. A longer patience bound does not weaken the proof: a genuine join failure still fails at the deadline.
4. Keep `teardown_test_lock` exactly as is. Its close-hang serialization rationale still stands, and shrinking lock coverage is not this ticket's risk to take. Update only its doc comment sentence that claims the worker counter is process-global.
5. Keep the test name, the production injection path, the peer-map and `has_dedicated_runtime` assertions, and the recreation assertions unchanged.
6. Add a short comment on the counter field stating why it is instance-scoped: a process-global counter made `== 0` waits observe other tests' runtimes under default-concurrency lib load.

Prefer this repair over quarantine. Use `#[ignore]`, `--test-threads=1`, or a skip only if the repaired test still fails a default-concurrency `--lib` run, and only with an Implement report that names the remaining mechanism. Do not start from quarantine.

## Non-scope

- No production (non-`cfg(test)`) behavior change: runtime creation, `park_runtime_if_idle`, `fail_closed_drop_dedicated_runtime`, `stop_all`, `LOCAL_WEBRTC_PEER_CLOSE_BOUND`, and close semantics stay untouched.
- Do not absorb write-budget `ticket_1786913892_208903` (parent-run owner) or lifecycle `ticket_1786937228_425608` (`unix_adapter_unbound_printf_stream_attach_completes`); the lifecycle root is outside `--lib` scope entirely.
- Do not touch the three known wall-clock assertion sites in `src/daemon_entity_subscriptions.rs` recorded in [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]; they belong to the recommended sweep ticket.
- Do not widen or remove `teardown_test_lock` coverage beyond the doc-comment correction.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins. No dependency or lockfile changes.
- Do not create a pull request (merge policy is direct).

## Repository ownership boundaries and cross-repo dependencies

Hub owns the local WebRTC transport, the daemon control plane, and this lib test. The work stays in Hub, inside `#[cfg(test)]` code in `src/local_webrtc.rs`.

No cross-repository prerequisite exists. Do not register a Core, client, Web, or TUI dependency.

Same-target siblings (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Parent-run owner; parked on this ticket's sibling chain. |
| `ticket_1786916741_161067` | separators_close lib flake | Closed; discovered this root and forbids absorption. |
| `ticket_1786921010_869253` | near_limit lib flake | Closed; repair merged at `547ca38`. |
| `ticket_1786937228_425608` | unix_adapter lifecycle failure | Open; lifecycle suite, outside `--lib`. |

## Assumptions and unknowns

Assumption: the recorded failure came from one or both named mechanisms. The interference channel is proven by inspection; the deadline-tightness channel matches the sibling flake class and the 2 s vs 10 s asymmetry inside the same tests. The repair removes both, so distinguishing which fired is not required for correctness.

Assumption: `Runtime::drop` via `runtime.take()` reliably stops worker threads and fires `on_thread_stop`; only the timing under load is uncertain. The existing green history and the `>= 1`/`== 0` transitions in isolated runs support this.

Assumption: instance scoping cannot mask a real leak of this daemon's workers, because the instance counter still counts exactly the threads this transport's runtime builder started.

Unknown until Implement: whether the failure reproduces pre-change on this worktree. Reproduction is probabilistic and load-dependent; Implement should attempt a bounded number of pre-change targeted and suite runs and must not treat non-reproduction as proof of absence.

Unknown: whether an unrelated lib test flakes during acceptance runs. If one does, record exact evidence and register a new ticket. Do not absorb.

## Affected surfaces/files

- `src/local_webrtc.rs` -- `#[cfg(test)]` items only: the counter static -> instance field, the runtime-builder hooks, `dedicated_runtime_worker_threads`, the 8 read sites, the four wait deadlines, and the `teardown_test_lock` doc comment.
- `docs/plans/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite.md` -- this plan.
- `docs/reports/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite-implement.md` -- Implement report (Implement step).

No compiled production code changes. No dependency or lockfile changes.

## Risks

- Instance scoping removes the only oracle that could catch a cross-daemon worker-thread leak (a runtime leaked by an unrelated test would no longer fail this wait). Mitigation: that global property was never this test's contract, it is exactly the coupling that flakes, and each test still fully audits its own daemon's workers.
- A 10 s patience bound under extreme ambient load could still expire. Mitigation: 10 s matches the module's proven `process_until_peer_closed` bound; a residual expiry would be honest evidence of a real join stall and belongs in a new ticket with its trace.
- The `Arc` clone into `on_thread_start`/`on_thread_stop` closures must not keep the transport alive; it holds only the counter, so no ownership cycle exists. Clippy `-D warnings` gates the wiring.
- The lib suite may flake on an unrelated test during acceptance runs. Follow the prior-art rule: exact evidence and a new ticket; do not absorb.
- Remaining predictable flake inventory for the orchestrator's sweep ticket: the three wall-clock sites in `src/daemon_entity_subscriptions.rs` (already recorded in the vault note); any future `== 0`-style global-state predicates should be caught by the capture below.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency unless stated. All suite commands use the Hub wrapper `./test.sh` (asset-sync check, `BOTSTER_ENV=test`, workspace scope). Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`. Without it the lib/lifecycle runs produce worker-missing failures.
2. Pre-change reproduction probe (bounded, corroborating only): up to 5 runs of `./test.sh --locked --lib local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` and up to 2 default-concurrency `./test.sh --locked --lib` runs on the unchanged worktree. Record outcomes; non-reproduction does not gate the repair.
3. Targeted repetition: `./test.sh --locked --lib local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` passes 20 consecutive runs (shell loop).
4. Binding default-concurrency gate: `./test.sh --locked --lib` (full workspace lib suites, default test threads) passes 5 consecutive runs with zero failures.
5. Ablation red-proof, per [[a regression test must be shown to go red with the fix reverted]], both under the targeted wrapper command, both reverted afterward:
   - Control A (join-wait liveness): comment out the `on_thread_stop` decrement. The `== 0` wait must time out and the run must fail with `timed out waiting for first dedicated runtime workers to join`. This proves the wait is a live oracle over the thread hooks.
   - Control B (interference demonstration / instance isolation): inside the test, temporarily construct a second `PeerHarness` with one signaled live peer before the first harness's cleanup wait. The first daemon's instance counter must still reach 0 (run passes), and a scratch assertion must show the second daemon's instance counter `>= 1` at that moment. Record that under the old global counter this exact configuration holds the summed counter above zero for the whole window -- the deterministic reproduction of mechanism 1. Then revert.
   Record both outcomes (exit codes, failure text for A) in the Implement report.
6. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass (clippy compiles the `cfg(test)` changes).
7. Non-binding smoke: one full `./test.sh --locked` run may be reported for information. Its lifecycle-suite outcome does not bind this ticket; `ticket_1786937228_425608` owns that root.
8. Implement report at `docs/reports/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite-implement.md` records: the enumerated unlocked counter-mutator test list, pre-change reproduction attempts, the oracle change, red-proof output, and acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or compiled runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Add a sibling note to [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]: process-global test counters (thread hooks, census statics) make `== 0` predicates observe other tests' runtimes under default-concurrency lib load; the durable idiom is instance-scoped counters owned by the harness under test plus patience bounds matched to the module's slowest proven wait. Name `LOCAL_WEBRTC_WORKER_THREADS` as the repaired instance and `spawn_capture_lock`'s process-global "new pid" baseline as the remaining inventory of the class.
- Capture the run-record `target_id` anomaly if Plan Review confirms it is an engine defect: a child run carrying a `target_id` that matches no spawn target while the ticket routes correctly.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Run the bounded pre-change reproduction probes (acceptance check 2).
3. Enumerate every test that can start a dedicated runtime and whether it takes `teardown_test_lock`; record the list in the report.
4. Apply Scope items 1-6. Keep the diff inside `#[cfg(test)]` code in `src/local_webrtc.rs`.
5. Run acceptance checks 3-6.
6. Write the Implement report.
7. Commit the test repair and report. Do not create a PR.
