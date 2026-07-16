# Eliminate oversized local WebRTC response close under load

## Context loaded

- Project Pipelines run `run_1784222794_491610`, Plan step `botster_plan`, ticket `ticket_1784168176_163113`, its required gate, and the absence of prior artifacts, reviews, findings, questions, or answers were loaded through `project_pipelines_current_context`. The ticket has no registered dependency.
- Required planning authority: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[identity]], and [[goals]]. The Botster overlay's required pipeline/plugin notes were also loaded; they constrain the pipeline handoff but do not expand this Rust transport ticket into plugin or SPA work.
- Applicable implementation and verification constraints: [[webrtc peer cleanup removes every per peer owner together]], [[test script required for rust tests not cargo test]], [[botster test sh forwards arguments to cargo not custom unit flags]], [[conformance harnesses gate on deterministic invariants not timing]], [[poisoned rust mutex test locks cascade one failure across parallel suite]], [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]], and [[suite wide acceptance criteria make every observed test failure in scope]].
- The motivating evidence is GitHub Actions run `29457052741` at subject `ac502aa91a14c42943c320d04d38938b272f2d65`, corresponding to pipeline artifact `artifact_1784157068_797370`. Under residual-tail load and default lifecycle-suite parallelism, the oversized response passed its byte-length, chunk-count, and maximum-frame assertions; the following request then failed with `local WebRTC data channel closed before response`. The same run recorded load average above 60 and five other independent lifecycle failures already split into separate tickets.
- Current production path inspected: `LocalWebrtcHandler::on_data_channel` receives and decrypts one request, routes it through `ControlMessage::Request`, frames the encrypted response, and calls `send_response_frames`; any false send outcome exits the handler, `close_data_channel` clears queued requests, closes the channel, and sends idempotent peer cleanup. The current send helper resets `paused` and its deadline for each response and probes flow/lifecycle events after every frame, including the final frame.
- Current real-path test inspected: `LocalWebrtcOfferPeer::encrypted_request_with_metrics` sends an encrypted request, consumes ordered response chunks from a bounded event channel, reassembles/decrypts them, and reports chunk metrics. `local_webrtc_chunks_oversized_encrypted_daemon_response` then makes another request on the same peer, which is the continuity assertion exposed by the loaded failure.
- Plan Review confirmed the matching code path: the sender probes after the final frame, a final `OnBufferedAmountHigh` enters the per-response paused loop, expiry returns false, and the outer handler closes the channel. Review also identified the repair boundary: `on_data_channel` currently discards idle high/low events through `Some(_) => continue`, so cross-response pressure state and event routing must change together.
- Plan-time baseline: `./test.sh local_webrtc_chunks_oversized_encrypted_daemon_response -- --nocapture` passed once locally (1 selected test, 95 filtered out). This proves the checked-out happy path only; it does not reproduce or waive the loaded failure.

## Scope

Botster layer: Rust hub local-WebRTC data-plane delivery and its real-daemon integration harness.

1. Capture the response-delivery and peer lifecycle state at the production send boundary. A failed send outcome must identify the response message id, next/last chunk position, total chunks, pressure state, and terminal cause (send error, channel event/closure, or pressure deadline) before cleanup. Extend the browser-shaped test peer only enough to preserve received chunk progress and distinguish receiver closure from response timeout. Keep diagnostics bounded and free of encrypted payloads, secrets, absolute paths, or user data.
2. Reproduce the failure or its state transition with a deterministic fake-DataChannel test before changing behavior. The test should model a multi-chunk response reaching high water at or after its final enqueued chunk, delayed low-water delivery, and a following request on the same peer. This converts the loaded symptom into a stable lifecycle invariant without asserting runner timing.
3. Repair flow-control ownership at the channel loop, not in framing or daemon response production. Thread one small channel-owned flow state through consecutive `send_response_frames` calls and the outer `on_data_channel` request loop; the existing `LocalWebrtcDataChannel` trait remains the test seam. The outer loop must feed `OnBufferedAmountHigh` and `OnBufferedAmountLow` into that same state instead of discarding them through `Some(_) => continue`, while preserving its current `OnMessage`, `OnClose`, `OnError`, and ended-poll behavior. A completed final chunk must not be reclassified as partial merely because high water is observed afterward. The persistent fact is whether the channel is pressured; the five-second deadline is not persistent idle state. Start that unchanged deadline only when pressure actually blocks a frame that still needs sending, never while a complete response sits idle, and do not reset an active pending-delivery deadline on repeated or unrelated events. Low water observed during the idle gap between responses must clear pressure before the next send. Preserve the bounded inbound FIFO and route real send/close failures through existing idempotent peer cleanup.
4. Preserve and instrument the real test's existing same-peer `ShutdownSession` request immediately after oversized reassembly; do not add a duplicate continuity request. Strengthening is limited to bounded receiver chunk/close evidence that distinguishes receiver closure from response timeout while retaining the existing 300,000-byte payload, multi-chunk, frame-ceiling, ordered/reliable-channel, and cleanup assertions.
5. Prove the regression red with the lifecycle fix reverted, then verify the fixed commit through the existing loaded default-parallel lifecycle campaign. Record exact subject SHAs, workflow inputs, run URLs/artifacts, first non-cascade failures, resource samples, and owned-process cleanup.

Every implementation change must trace to delivery-state observability, correct response-boundary flow control, continuity of the real same-peer path, or required negative/loaded verification.

## Non-scope

- No response framing, encryption, chunk size/count, 16 MiB assembly cap, DTO, TypeScript, conformance-fixture, npm package, or browser decoder changes; the campaign proved the oversized bytes reassembled before the peer became unusable.
- No daemon shutdown-response work, terminal readiness, dev-stack restart, dogfood diagnostic, or stale-daemon recovery repairs. Those were separate roots in the same campaign and have separate tickets.
- No retries, timeout increases, fixed sleeps, test serialization, reduced synthetic load, `--test-threads=1` acceptance substitute, weaker payload/frame assertions, or failure waiver.
- No WebRTC dependency upgrade, optional tuning knob, generic transport abstraction, plugin/Lua/TUI/SPA/Rails change, or adjacent cleanup.
- No new focused loaded-runner mode unless the existing workflow cannot produce required red-on-revert evidence; the existing `lifecycle-suite` target is the acceptance surface.

## Assumptions and unknowns

- Confirmed by code review, still requiring runtime diagnostics: because the large response's final assertions passed and only the following request saw closure, the matching defect path is response-boundary flow-control state causing sender teardown after the final chunk, rather than malformed framing or lost response bytes.
- Unknown: the cited artifact has the receiver-side close symptom but no sender chunk/pressure/peer state at cleanup. The first implementation task must identify whether `send_response_frames` returned false because of the pressure deadline, `OnClose`/`OnError`, `poll()` ending, or `send_text` failure.
- Assumption: the current `webrtc` trait exposes high/low events and ready state but no buffered-byte getter, so the repair should preserve event-driven pressure control rather than introduce a dependency patch or infer drain from `send_text().await`.
- Assumption: one handler task remains the owner of ordered request processing and response emission for a channel. The fix must not create concurrent response tasks or allow an unbounded queue.
- Assumption: the existing injectable deadline plus `LocalWebrtcDataChannel` trait are sufficient deterministic seams. Threading a small mutable flow state through two consecutive send-helper calls should prove the response boundary without testing the spawned concrete `Arc<dyn DataChannel>` loop directly or adding runtime configuration.
- Worktree/target: all work applies only to this pipeline-provided worktree on explicit target `tgt_7e208a0c76a44980a83b63af976b1f22`, based on `origin/main` commit `28077153be6051a2fc70db7d725c60d97e455945`. No sibling checkout is authorized.
- There is no convention conflict in the planned scope. The plan preserves the existing bounded encrypted protocol and complete peer cleanup. The only tension to resolve is ensuring the five-second bound applies to genuinely pending delivery rather than treating already-complete output as incomplete; changing the duration itself is forbidden.

## Affected surfaces and files

- `src/local_webrtc.rs` — a small channel-owned pressure/delivery state threaded through the outer `on_data_channel` poll loop and consecutive send-helper calls, explicit send outcome/diagnostics, cleanup integration, and deterministic fake-channel regression tests.
- `tests/hub_daemon_lifecycle_test.rs` — receiver chunk/close evidence and same-peer post-oversized-response continuity assertion on the production entry path.
- `docs/plans/eliminate-oversized-local-webrtc-response-close-under-load.md` — this reviewable Plan artifact and workflow evidence contract.
- `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, and `docs/loaded-daemon-lifecycle-runner.md` are verification inputs, not planned edits. Touch them only if exact evidence proves the existing `lifecycle-suite` campaign cannot exercise the fixed path; that would require Plan Review or human re-scope.

Production wiring to preserve and prove:

`LocalWebrtcHandler::on_data_channel` -> `ControlMessage::Request` -> daemon response -> `framed_daemon_response` -> channel-owned flow control -> real `DataChannel::send_text` -> `LocalWebrtcOfferPeer` chunk reassembly -> a subsequent encrypted request on the same open channel.

## Risks

- **Wrong root-cause repair:** receiver closure alone does not reveal the sender branch. Mitigation: capture bounded send/peer state first and require it to match the repair.
- **Backpressure regression:** simply ignoring a final high-water event could let a later large response enqueue without pressure control. Mitigation: pressure state persists across response boundaries and gates the next unsent frame.
- **Unreachable idle low-water event:** persisting pressure while the outer request loop still discards flow events would make pressure stick and close the next response deterministically. Mitigation: route idle outer-loop high/low events through the same channel-owned state and test that idle low water clears it.
- **Stale idle deadline:** carrying a deadline from response N across an arbitrary idle gap would immediately close response N+1. Mitigation: persist only pressure across idle time; create the unchanged five-second deadline when an unsent frame is actually blocked and clear it when no delivery is pending.
- **False partial-response classification:** waiting for low water after the last frame can close an otherwise complete channel. Mitigation: track next/total chunk progress and distinguish complete output from pending output.
- **Unbounded or immortal peer:** removing the deadline would contradict the established fail-closed design. Mitigation: retain the current absolute bound for pending delivery and existing idempotent cleanup; do not inflate or reset it on unrelated events.
- **Request correlation drift:** polling while pressured can consume later requests. Mitigation: preserve the existing bounded FIFO and one-response-at-a-time ordering, including overflow response ordering.
- **Observability leaks or noise:** dumping frames could expose encrypted envelopes or flood loaded logs. Mitigation: log only identifiers, counts, pressure/peer state, and a bounded close cause.
- **Timing-dependent regression test:** a wall-clock race test would remain flaky. Mitigation: fake channel events and deterministic progress assertions; loaded timing is external verification, not the unit oracle.
- **Suite-wide blockers:** any failure in the required default-parallel loaded campaign blocks acceptance even if a sibling ticket exists, unless a human explicitly re-scopes after exact unrelatedness evidence.

## Acceptance checks and tests

1. Deterministic `src/local_webrtc.rs` tests prove:
   - high then low water still resumes ordered multi-chunk delivery;
   - high water observed after the final enqueued chunk records a complete response and does not close the channel;
   - the same flow-state instance can be driven across two consecutive send-helper calls: post-final high water completes response N, idle low water clears pressure, and response N+1 sends normally;
   - when response N completes under pressure and response N+1 arrives before low water, the first unsent frame of response N+1 starts the unchanged five-second pending-delivery deadline; idle time before that blocked frame does not consume the deadline, and repeated/unrelated events do not extend it;
   - the outer request-loop event path applies high/low events to the same state instead of discarding them, while `OnMessage`, lifecycle termination, and bounded FIFO behavior remain unchanged;
   - missing low water while an unsent chunk remains still returns the typed failure, clears bounded pending work, closes once, and invokes peer cleanup once;
   - `OnClose`, `OnError`, ended polling, and `send_text` failure report distinct bounded outcomes with chunk progress;
   - requests consumed during pressure remain bounded and FIFO, including overflow responses.
2. Run the focused real path repeatedly with the repository wrapper, keeping all existing assertions and the post-large-response continuity request: `for run in 1 2 3 4 5; do ./test.sh local_webrtc_chunks_oversized_encrypted_daemon_response -- --nocapture || exit 1; done`.
3. Run adjacent local-WebRTC lifecycle coverage, including peer-close subscription cleanup and the focused `src/local_webrtc.rs` unit tests, through `./test.sh`; isolated `-- --test-threads=1` runs are diagnostic only if a poisoned-lock cascade obscures the first root.
4. Negative control: commit the fix, revert only the lifecycle change while keeping the strengthened test/diagnostics, and show the deterministic regression test fails for the expected complete-response/closed-peer state. If the load-only scheduler condition cannot be made deterministic without changing product semantics, dispatch the reverted subject SHA through the same loaded campaign and retain that red artifact instead; do not weaken the requirement.
5. Run `cargo fmt --check`, the strict Clippy mode configured by the repository, and `./test.sh`. Record exact commands and results; no pre-existing-failure blanket waiver.
6. Dispatch the existing loaded workflow against the exact fixed commit SHA with `test_target=lifecycle-suite`, `stress_profile=residual-tail`, and at least the documented 20 repetitions. It must precompile the exact test target, run at default parallelism, stop on the first red run, and retain resource/cleanup artifacts. Every lifecycle test in every required repetition must pass; elapsed times are observations only.
7. Inspect the complete branch diff and generated artifacts for secrets, absolute home/worktree paths, usernames, and unrelated changes.

## Pipeline gates and artifacts

- Plan gate: this committed document plus the Project Pipelines plan artifact must expose all required sections and the root-cause hypothesis/unknown explicitly.
- Implement handoff: attach the captured sender outcome, deterministic red-on-revert evidence, focused real-path results, and exact fixed commit SHA.
- Review/Verify handoff: attach local quality-gate output and loaded workflow URLs/artifact identifiers, including first-failure and cleanup interpretation. A passed focused test is not a substitute for default-parallel loaded evidence.
- Work must remain on the explicit target/worktree above. No target-id, worktree, plugin README, UI, or product-decision ledger change is required because this ticket changes a core Rust transport primitive rather than Project Pipelines workflow policy.

## Project Pipelines and vault checklist evidence

- Pipeline context was loaded before planning, including the empty prior artifact/review/finding/question surfaces and no ticket dependencies.
- Applicable vault notes and both required playbooks are named above. Convention conflict result: none; the plan preserves bounded delivery, wrapper-based tests, deterministic oracles, complete peer cleanup, and default-parallel loaded acceptance.
- Verification evidence at Plan time: repository/source/history inspection; GitHub Actions run `29457052741` exact failure inspection; local focused wrapper run passed once. Loaded success and negative control remain Implement/Verify work.
- The initial checklist calls appeared to time out client-side, but both records persisted successfully: vault checklist `checklist_1784222868_645231` and project checklist `checklist_1784222874_839278`. All items are done with notes read, conflicts `none`, command evidence, acceptance proof, and capture disposition; no checklist fallback is needed for this run.
- Durable capture disposition: no vault write during Plan. Implementation evidence may justify a new atomic note that WebRTC flow-control pressure state belongs to the channel lifecycle rather than one response, plus an update to the prior chunking decision if the final root confirms that boundary.

## Vault gaps worth capturing

- If confirmed, capture that local WebRTC flow-control state spans response boundaries: a high-water transition after a final chunk is not proof of partial delivery, but it must still gate the next unsent response.
- Capture the diagnostic pattern that chunk progress plus terminal close cause is necessary to distinguish completed-response peer teardown from framing loss under load.
- If the existing high/low event API proves insufficient under starvation without duration changes, capture the exact library constraint and chosen observable lifecycle condition only after implementation and loaded proof establish it.
