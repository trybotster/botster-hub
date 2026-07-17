# Eliminate oversized local WebRTC response close under load

## Context loaded

- Current Project Pipelines context: run `run_1784249675_657712`, returned Plan step `botster_plan`, active run step `run_step_1784250432_534001`, ticket `ticket_1784168176_163113`, required plan gate, prior plan artifact/gate/checklists, Plan Review `review_1784250383_314307` and its four findings, no dependencies, and the durable prior human answer were loaded with `project_pipelines_current_context`.
- The prior human answer remains binding: accept focused target-path success for this leaf ticket, map independent lifecycle-suite roots to their sibling tickets, and do not add retries, timeout increases, serialization, or unrelated fixes. The umbrella stalled-attach ticket owns eventual suite-wide green.
- Required planning authority loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], and every note in the Botster planner overlay's Must Load list. No convention conflicts were found.
- The previous plan and implementation are already in history. PR #139 merged commit `0c38b76`; its subject commits `1e96eff` and `2ff2246` preserve flow state across responses and route idle high/low-water events through the production channel loop. The tested recurrence subject `11407a41eceb8497786b722b5d23d23ed35f3f69` contains that merge.
- Reopened evidence: GitHub Actions run `29539022952` ran the exact subject with `test_target=lifecycle-suite`, `stress_profile=residual-tail`, 20 requested repetitions, and default test parallelism. Run 1 failed at load average `73.76`; `local_webrtc_chunks_oversized_encrypted_daemon_response` received 58 of 85 ordered chunks before `channel_closed`. The campaign stopped at the first red, cleaned the test/sampler/load process groups, and ended with `campaign_exit_status=101`, `cleanup_status=0`.
- This is materially different from the original post-response symptom. It is a mid-response close, so the merged response-boundary fix cannot be assumed to address the active root.
- Production path inspected: `LocalWebrtcHandler::on_data_channel` -> `run_data_channel_with_deadline` -> daemon `ControlMessage::Request` -> `framed_daemon_response` -> `send_response_frames_with_deadline` -> real `DataChannel::send_text`. High water sets channel-owned `pressured`; the next unsent chunk starts a five-second absolute pressure deadline; expiry becomes a typed send failure and unconditionally closes the data channel and cleans up the peer.
- Runtime-library contract inspected: `webrtc 0.20.0-rc.1` exposes high/low-water events but not current buffered bytes through the public `DataChannel` trait. Its own flow-control example waits for low water while the channel remains open; Botster's five-second pressure-expiry policy is local policy, not a dependency requirement.
- Test path inspected: the real offer peer sends encrypted requests, receives ordered chunks, and reports `channel_closed` versus `response_timeout` with chunk progress. The spawned daemon's stderr is piped but is not surfaced when this test panics, so run `29539022952` does not reveal whether the sender ended on `pressure_deadline`, `send_text`, `OnClose`, `OnError`, or ended polling.
- Plan-time repository state: the pipeline worktree is clean at `2ff2246` and is an ancestor of current `origin/main` `e864567`. Implement must first update this ticket branch to current `origin/main`; relevant production code is unchanged on main, while sibling lifecycle-test changes must be preserved.
- Plan Review verified the ancestry, production trace, and existing bounded sender log, then identified a blocker in the first revision: `on_connection_state_change` cleans peer state on `Disconnected`, `Failed`, or `Closed` but cannot wake a task parked in `local_poll()`. Removing the deadline without carrying that peer-connection signal into the channel loop would leak the sender task. Review also identified three existing deadline-policy tests whose disposition must be explicit.

## Scope

Botster layer: Rust hub local-WebRTC data-plane delivery plus the real-daemon lifecycle harness and existing loaded verification campaign.

1. Update the ticket branch to current `origin/main` before editing so the reopened fix is tested with the same sibling changes and base as the recurrence campaign.
2. Make the target test surface the bounded sender terminal record already emitted by `run_data_channel_with_deadline`: message id, next/last/total chunk, pressure state, and typed cause. This is a harness-only diagnostic change; do not add duplicate production logging, payloads, secrets, absolute paths, or unbounded daemon output.
3. Reproduce the proven sender cause with the existing fake `LocalWebrtcDataChannel` seam. Model an oversized multi-chunk response that reaches high water mid-response and whose low-water event is scheduler-delayed while the channel remains open. The deterministic oracle is delivery/lifecycle state, not elapsed wall time.
4. Repair only the confirmed branch in `src/local_webrtc.rs`:
   - If the sender record is `pressure_deadline` as expected, replace the deadline with a concrete peer-liveness signal, not an unbounded bare `local_poll()`. Use Tokio's existing `watch` primitive (already available through the configured `tokio` dependency) from `LocalWebrtcPeerState`: `on_connection_state_change` publishes the first terminal `Disconnected`, `Failed`, or `Closed` state, and every blocking channel-loop wait selects between the DataChannel event and that watched peer termination. A terminal peer state returns a distinct typed send failure carrying the specific connection state, then follows the existing close and exactly-once cleanup path.
   - Apply this consistently to both active mid-response pressure and idle pressure between responses. Scheduler delay alone must close neither path; low water resumes delivery, while DataChannel close/error/end, send failure, or watched peer termination ends it. Remove the pressure-deadline constant/state/helper and deadline-suffixed function vocabulary cold turkey so no dual elapsed/liveness policy remains.
   - Preserve the bounded FIFO while waits consume inbound requests. Do not send another frame while pressure is active, and do not treat receiver-side timeouts as hub-task cancellation.
   - If the record proves a different typed cause, repair that exact send or lifecycle branch and update this plan artifact before implementation proceeds. Do not silently apply the pressure hypothesis.
5. Preserve the production protocol and real acceptance path: encrypted framing, ordered reliable data channel, exact payload equality, chunk ordering/count, frame ceiling, same-peer follow-up request, grant cleanup, and idempotent peer cleanup.
6. Prove red when the focused lifecycle fix is reverted, then prove the exact fixed SHA under the existing residual-tail default-parallel campaign.

Every changed line must provide sender evidence, correct the confirmed mid-response lifecycle decision, exercise that decision, or record required verification.

## Non-scope

- No retries, timeout inflation, fixed sleeps, reduced stress, test serialization, `--test-threads=1` acceptance substitute, or weaker payload/chunk/frame assertions.
- No response framing, encryption, chunk size/count, response assembly cap, DTO, TypeScript, browser, SPA, TUI, Lua/plugin, Rails, or conformance-fixture changes.
- No WebRTC dependency upgrade or patch, configurable watermarks/deadlines, generic transport abstraction, background send queue, concurrent response tasks, or adjacent cleanup.
- No fixes for the other failures in run `29539022952`. Each must be mapped to its existing sibling ticket with run/artifact evidence; if any root lacks an owner, create one before this ticket advances.
- No rewrite of the loaded runner or workflow. Those files are verification inputs unless exact evidence shows they cannot execute the focused path; that would require Plan Review or human re-scope.

## Assumptions and unknowns

- Strong hypothesis, not yet fact: Botster's five-second `pressure_deadline` fired while the reliable channel and receiver were still viable but starved, and Botster then caused the observed `channel_closed` through `close_data_channel`.
- Unknown: the exact sender cause in run `29539022952`; the target harness currently loses the daemon stderr record on panic. Capturing this is the first implementation gate.
- Unknown: why the failing `Drain` response contained 85 chunks. The plan does not assume this is malformed; exact response kind, encrypted/frame byte counts, and sender message id should establish whether it is legitimate accumulated terminal output or response-correlation drift before behavior changes.
- Assumption: one channel task remains the sole owner of ordered requests, pressure state, and response sends. The repair must not introduce concurrent senders or unbounded buffering.
- Assumption: an ordered reliable DataChannel that is pressured but has emitted no DataChannel or peer-connection terminal signal is still live; scheduler-dependent wall-clock expiry is not transport failure evidence. This assumption must be proven by the fake-channel red/green test and loaded real path.
- Assumption: `tokio::sync::watch` is the smallest race-safe existing primitive because it preserves the latest peer terminal state for a channel task that subscribes after termination. No new dependency or generic cancellation abstraction is needed.
- Assumption: the existing receiver's per-chunk timeout and daemon request bounds remain unchanged for caller failure reporting, but they are not hub-task liveness controls. Hub task termination is bounded by explicit DataChannel or watched peer-connection lifecycle signals.
- Worktree/target: only the pipeline-provided worktree on target `tgt_7e208a0c76a44980a83b63af976b1f22` is authorized. The branch must incorporate current `origin/main` without dropping sibling-ticket changes.

## Affected surfaces and files

- `src/local_webrtc.rs` — channel-owned flow/lifecycle policy, typed terminal diagnostics, and deterministic fake-channel regression tests.
- `tests/hub_daemon_lifecycle_test.rs` — target-scoped sender diagnostic capture plus preservation of exact chunk/payload/continuity assertions on the real daemon and real peer.
- `docs/plans/eliminate-oversized-local-webrtc-response-close-under-load.md` — reopened regression plan and evidence contract.
- `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, and `docs/loaded-daemon-lifecycle-runner.md` — verification inputs only, not planned edits.

Production wiring that must be proven, not merely compiled:

Current entry path: `LocalWebrtcHandler::on_data_channel` -> `run_data_channel_with_deadline` -> daemon response -> `framed_daemon_response` -> channel-owned pressure loop -> real `DataChannel::send_text` -> `LocalWebrtcOfferPeer` ordered reassembly -> subsequent encrypted request on the same peer. The planned production change inserts the peer-owned watched terminal state into every blocking channel-loop wait on that path; compilation-only evidence is insufficient.

## Risks

- **Wrong root cause:** receiver `channel_closed` does not identify the sender branch. Mitigation: require the bounded sender record before changing policy; update the plan if it contradicts `pressure_deadline`.
- **Immortal dead peer:** peer-connection death may not emit a DataChannel close/error and would leave a bare poll parked. Mitigation: publish `Disconnected`/`Failed`/`Closed` through peer-owned watch state, select it at every blocking channel wait, return a typed peer-termination cause, and prove the task exits with exactly-once cleanup in both active and idle pressure cases.
- **Unbounded buffering:** ignoring high water could enqueue the whole response. Mitigation: remain paused after high water and resume only on low water; do not bypass flow control.
- **Request starvation or reordering:** polling for low water can also consume inbound requests. Mitigation: preserve the bounded FIFO and one-response-at-a-time order, including overflow responses.
- **Diagnostic deadlock/noise:** unread child pipes or full payload dumps can hide the cause or block the daemon. Mitigation: surface one bounded typed terminal record and never log encrypted frames or whole child output.
- **Correlation bug hidden as pressure:** the failing response was a `Drain` and unexpectedly large. Mitigation: capture response kind/message id/total chunks and retain exact ordered reassembly before deciding the pressure hypothesis is sufficient.
- **False confidence from focused passes:** the defect requires heavy scheduler starvation. Mitigation: deterministic state-machine negative control plus exact-SHA residual-tail default-parallel evidence; neither substitutes for the other.
- **Independent suite failures:** unrelated roots can obscure result interpretation. Mitigation: identify first non-cascade failures and map each to a sibling ticket, following the prior human scope disposition rather than waiving them generically.

## Existing test disposition

- Rewrite `missing_low_water_event_terminates_partial_response_and_cleans_peer` as the active-pressure peer-death regression: after the first chunk raises high water, publish peer `Failed` without DataChannel close/error/low-water; require a typed peer-connection failure, unchanged next/last/total progress, no second send, task completion, channel close, and exactly-once cleanup.
- Rewrite `next_response_starts_deadline_only_when_pressure_blocks_its_first_frame` as the response-boundary pressure regression: response one may complete under high water, response two must send nothing before low water, and low water must resume its first frame. This carries forward the merged guarantee without retaining elapsed deadline policy.
- Split the negative half of `outer_loop_routes_idle_pressure_before_next_request_delivery`: retain the current idle high-then-low ordering assertion; replace its no-low-water `PressureDeadline` expectation with peer `Failed` and require no send, typed termination, no parked task, channel close, and exactly-once cleanup.
- Retain `post_final_high_water_survives_response_boundary_and_idle_low_clears_it`, `high_then_low_water_resumes_and_completes_response_in_order`, FIFO/overflow coverage, and distinct DataChannel close/error/end/send-failure coverage. Update names/signatures mechanically only where removal of deadline vocabulary requires it; do not weaken their behavioral assertions.

## Acceptance checks and tests

1. Sender-evidence gate: the strengthened target path reports the sender's typed cause and chunk/pressure progress when forced red; no secrets, payload bodies, usernames, or absolute paths appear.
2. Deterministic `src/local_webrtc.rs` tests prove:
   - high water mid-response pauses before the next unsent frame;
   - scheduler-delayed low water on an otherwise open channel resumes ordered delivery without closing;
   - elapsed time alone cannot turn that live pressured state into peer cleanup;
   - peer `Failed` while pressured mid-response, with no DataChannel close/error, wakes the sender, reports the specific typed peer-connection cause and chunk progress, leaves no parked task, closes once, and cleans up exactly once;
   - peer `Failed` while idle-pressured before the next response provides the same bounded termination and sends no premature frame;
   - `OnClose`, `OnError`, ended polling, and `send_text` failure still terminate with distinct typed causes and exactly-once cleanup;
   - requests received while paused remain bounded and FIFO, including overflow responses;
   - post-final pressure and idle low water still preserve the already-merged response-boundary behavior.
3. Focused real path: run `for run in 1 2 3 4 5; do ./test.sh local_webrtc_chunks_oversized_encrypted_daemon_response -- --nocapture || exit 1; done`. All five runs must preserve the 300,000-byte payload equality, response kind, ordered chunk count, maximum frame size, same-peer follow-up request, grant cleanup, and peer cleanup.
4. Adjacent local-WebRTC coverage: run the focused `src/local_webrtc.rs` tests and peer-close/subscription cleanup tests via `./test.sh`. A single-threaded diagnostic run may identify a first root but is not acceptance evidence.
5. Negative control: with diagnostics/tests retained, revert only the new lifecycle behavior and show the deterministic mid-response test fails for the expected pressured-close/partial-delivery state. Preserve the red command/output and reverted subject SHA or commit.
6. Quality gates: `cargo fmt --check`, repository-configured strict Clippy, and `./test.sh`. Record exact commands, SHAs, and results; investigate every failure rather than claiming a blanket pre-existing failure.
7. Loaded proof: dispatch the existing workflow for the exact fixed SHA with `test_target=lifecycle-suite`, `stress_profile=residual-tail`, at least 20 repetitions, and default parallelism. The target test must pass every requested repetition with unchanged assertions and cleanup must report zero owned process groups. Map every other first-root failure to an existing sibling ticket with artifact/run evidence, per the prior human answer.
8. Diff/artifact audit: verify every changed line traces to the ticket and scan committed artifacts for secrets, personal data, and absolute home/worktree paths.

## Pipeline gates and checklist evidence

- Plan artifact: this committed repo document plus the Project Pipelines artifact/gate evidence carries all required plan sections and the explicit sender-cause unknown.
- Implement handoff: attach current-main integration evidence, bounded sender record, deterministic red/green proof, focused real-path results, exact fixed SHA, and the sibling-ticket map for other observed roots.
- Review/Verify handoff: attach local quality-gate output, complete diff audit, loaded workflow URL/artifact id, target-test result for every repetition, first-root disposition, and cleanup interpretation.
- Project Pipelines checklist `checklist_1784249926_382722` records context loading, runtime-path tracing, bounded scope, and acceptance discipline.
- Vault checklist `checklist_1784249931_202805` records notes loaded, convention conflict result (`none`), verification evidence, and capture disposition. Its creation calls timed out client-side but both checklist records persisted and were reconciled before advancement.

## Vault gaps worth capturing

- If confirmed, capture that reliable local-WebRTC backpressure should wait on transport progress/lifecycle signals rather than a scheduler-sensitive response deadline; wall-clock starvation alone must not close a live peer.
- Capture the harness gotcha that piped daemon stderr must be surfaced on target-test failure or typed production diagnostics disappear precisely when loaded evidence is needed.
- If the 85-chunk `Drain` response reveals a separate correlation or retained-output invariant, capture it only after code and red/green evidence establish the mechanism; otherwise do not create a speculative note.
