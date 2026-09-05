# Nonblocking resize completion

Status: approved by Fable after revisions. Coordinator: Codex. Implementer: Grok. Reviewer: Fable.

Review record: Core worktree `foundation/resize-review`, report commit `b5a7201`.
Review file: `docs/reports/2026-09-04-nonblocking-resize-plan-review.md` in that worktree.

## Objective

Core must process later input and output for session B while session A waits for a resize acknowledgement.
The terminal pump must return without waiting for A's worker.
Core must preserve input order, applied geometry, timeout handling, and subscription generation checks.

The user authorized direct orchestration and changes without backward compatibility for the unused modular product.
Grok implements this change. Fable reviews the plan and the exact implementation commit.
The coordinator owns scope, decisions, and downstream validation.
This work does not use a Project Pipelines run.

## Revisions and ownership

- Core base: `93acae3f98adbc21dc981d113c4eb2f31ead4ad0`.
- Audited Hub main: `bb1a330543bc06888f894edd5f40a0f867753a12`, with Core `48a4370`.
- Hub main at plan review: `ae6a0b1fe99d97215fa82d796da8f01a904171f0`, still with Core `48a4370`.
- Pending Hub branch: `project-pipelines/ticket_1787600679_990088`.
- Hub commit `e50e0f0` updates Core to `93acae3` on that branch.
- The pending branch was `7164c38` when this plan was written.

Record full commit IDs before validation. Do not modify another agent's active worktree.
Use an isolated Core branch named `foundation/nonblocking-resize`.
Use the pending Hub implementation for downstream validation if it remains the relevant consumer.
Copy or check out that revision separately. Do not update its active worktree.
Keep Hub source and the locked Core revision separate in every result.

Core owns resize mechanics and terminal wakes. Hub owns host policy and the opaque adapter boundary.
This change must not move terminal semantics into Hub.

## Read before implementation

Read the repository instructions and the Core ownership charter.
Read these vault notes:

- [[core terminal progress is wake driven and targeted]]
- [[worker resize acknowledgment precedes the next control frame]]
- [[session registry size follows the worker applied resize]]
- [[resize completion wake durability has one ablation point and needs three core armed pumps]]
- [[core one slot adapters preserve resize input and echo wake obligations]]
- [[session ingress wakes retire after bound route delivery not lifecycle commit]]
- [[a wake test must not consume the one shot edge it asserts]]
- [[a regression test must be shown to go red with the fix reverted]]

The earlier single-pump completion assertion describes the old implementation.
Preserve the ordering guarantee. Replace tests that require synchronous completion with explicit acknowledgement-driven completion tests.
Explain each changed test expectation in the implementation report.

## Current failure path

`CoreDaemon::pump_woken` calls `complete_pending_terminal_resize` after processing the current session batch.
That function calls `WorkerProcessRuntime::wait_for_resize_applied` for pending resizes.
The wait sleeps and pumps one worker until acknowledgement or timeout.
The wait reuses `DEFAULT_MODE_GATED_INPUT_TIMEOUT`, which defaults to five seconds.
Hub runs this function on its only Core data-plane thread.

The existing sibling test submits both sessions before the same pump call.
It does not prove progress for input that arrives after the resize wait starts.

The nonblocking completion chain already exists at Core `93acae3`:

1. Accepted ingress resize records a pending entry and returns its input result.
2. The worker acknowledges each applied resize in control-frame order.
3. The reader thread queues the acknowledgement and notifies the session wake.
4. The facade pump calls `reconcile_terminal_resize_acknowledgments` without waiting.
5. The daemon persists geometry from `take_applied_terminal_resize`.

Reuse this chain. Do not add a second completion path or a second pending-resize collection.

Inspect these files:

- `crates/botster-core-daemon/src/daemon.rs`
- `crates/botster-core/src/engine/managed_session_runtime.rs`
- `crates/botster-core/src/runtime/worker_process.rs`
- `crates/botster-core/src/engine/client_worker.rs`
- `crates/botster-core-daemon/tests/terminal_wake_test.rs`

## Implementation requirements

1. Add the regression test before changing production behavior.
2. Keep the existing acknowledgement reconciliation as the only completion path.
3. Extend each existing pending entry with an absolute `Instant` deadline recorded at acceptance.
4. Return from the pump without sleeping or waiting for worker completion.
5. Complete pending resize state when Core receives the matching worker acknowledgement.
6. Preserve acknowledgement-based registry updates for ingress resize. Do not publish unconfirmed geometry on expiry.
7. Preserve ordered input and output across the resize boundary.
8. Keep timeout handling finite without requiring another client request or the Hub watchdog.
9. Wake only the affected session when a resize deadline expires.
10. Clear pending state when the session or relevant operation is retired.
11. Reject stale completion after teardown or identity replacement.
12. Keep pending state bounded under repeated resize input.

Remove the daemon pump's `complete_pending_terminal_resize` call.
Remove the associated engine helper, hidden facade method, daemon dispatch arm, and `wait_for_resize_applied` helper.
Check for remaining callers before deletion. Report any caller outside this chain before extending scope.
Replace the pending tuple with a small struct that retains dimensions and logical time, and adds the deadline.
Reuse `mode_gated_input_timeout` for this deadline. Do not add a new timeout option in this repair.

Keep ordered acknowledgement matching and ignore unmatched acknowledgements.
The explicit `CoreDaemon::resize` path can send a resize without an ingress pending entry.
Do not add a wire identifier or coalesce pending operations in this repair.
The writer frees queue capacity before acknowledgement, so it cannot bound pending entries alone.
Cap pending ingress resizes per session at the ordinary worker lane capacity: 30 entries at the base revision.
Derive this cap from the existing queue constants. Do not add a new tunable.
Before removing the next owner command, check whether it is a resize and the pending cap is full.
If full, park the owner through the existing capacity path and retain its command order.
Use a narrow head-command accessor rather than removing and rebuilding the command.
Retry the parked owner on the acknowledgement's session ingress wake through `parked_route_keys`.
Do not require another client frame or a timer to resume the owner after acknowledgement.

### Explicit resize safety guard

Keep the explicit `CoreDaemon::resize` API and its current behavior when no ingress resize is pending.
Reject an explicit resize before any worker or registry mutation while that session has pending ingress resizes.
Use a small facade accessor to check the session's pending entries.
Return a distinct busy error variant that callers can handle after a completion wake. Do not wait for completion.
Without this guard, a later explicit resize can persist new geometry before an older ingress acknowledgement overwrites it.
The guard prevents the interleaving that the old blocking pump excluded.
Do not redesign explicit resize persistence or migrate its local-engine tests in this repair.

### Deadline mechanism and failure rule

Generalize the paste wait clamp to the earliest paste deadline or pending-resize front deadline.
Include expired resize sessions in the returned batch's `ingress_sessions`.
Merge expired sessions with ordinary wake batches, including batches that already contain sibling traffic.
Do not depend on an empty wake batch to process an expired deadline.
Do not add a thread per resize or a periodic correctness poll.

Reconcile available acknowledgements before checking expiry for each named session.
If a pending resize expires, fail only that session's control path through the existing writer-failure teardown mechanism.
Use `mark_control_plane_failed` and the associated owner cleanup; calling the marker alone is not sufficient.
Keep the last confirmed registry geometry. Clear that session's pending resize entries.
Do not return a resize-timeout error from `pump_woken` that triggers the existing commit retry loop.
Do not silently discard the expired operation while allowing uncertain control state to continue.

This rule treats missing acknowledgement as an uncertain control state, not proof that the worker stopped consuming frames.
The session cannot safely claim confirmed resize completion after the deadline.
Preserve unrelated sessions and their routes.
Remove pending resize entries when reconciliation encounters an exited session with no remaining worker.
Do not generate repeated deadline wakes for removed or failed sessions.
Preserve final-output delivery and the existing rules for retiring session wakes.

Hub already uses Core's wait/pump contract, so its scheduling code needs no change.
Downstream compilation found a required Hub error-classification arm for the new `ExplicitResizeBusy` variant.
Implement that narrow integration change separately from the active Hub ticket, then validate the updated dependency source.
Record remaining blocking paths honestly. This change does not fix all shared-pump blocking.

## Required behavioral tests

### Later sibling arrival

Use real worker-backed sessions and Core-produced wakes.
Complete both attachments before the experiment.
Use a scoped test control to hold A's resize acknowledgement.
Prefer a parent-side gate in the reader thread's `FRAME_RESIZE_APPLIED` arm.
Keep the gate isolated to test support and one worker session.
Do not rely on an arbitrary startup sleep to establish the hold.
Send A's resize and pump its wake.
Require that pump to return while the acknowledgement remains held.
Only then inject B's input.
Require B's input result and PTY echo before releasing A or reaching A's configured deadline.
Release A and prove correct applied geometry and later A input.

The old implementation must fail this test for the named liveness reason.
A test timeout must clean up its own workers and preserve diagnostic output.
Do not accept an attachment setup failure as the expected regression failure.
Run the daemon scenario on one thread. Bound each observed step with `recv_timeout` on the test thread.
Use bounds shorter than A's deadline after attachment completes.
Release the gate on cleanup so the reader thread cannot remain blocked during teardown.
Record a failing run with the blocking path restored and a passing run with the repair applied.
The failing run must reach the liveness assertion, not fail during attachment or source-shape checks.

### Deadline and cleanup

Omit A's acknowledgement and produce no further A input.
Prove that Core handles the deadline through its normal wait/pump contract.
Prove that B remains live before and after A's failure handling.
Prove that the failure affects only the intended session or operation.
Assert session-local control failure and unchanged last-confirmed registry geometry.
Keep B producing wakes across A's deadline to prove that nonempty batches cannot starve expiry.
Prove that deadline wakes stop after A's pending state is removed.
Test teardown while resize remains pending.
Test a late acknowledgement after teardown or replacement.

### Ordering and pressure

Retain the existing resize-then-input tests and adapt only synchronous completion expectations.
Test repeated resize operations, including equal dimensions, with ordered acknowledgements.
Fill the pending cap while A's acknowledgements are held.
Prove the next resize parks and later input remains behind it in the same owner queue.
Release acknowledgements and prove the parked owner resumes through acknowledgement wakes alone.
Prove the pending collection never exceeds its cap.
Test explicit resize rejection during pending ingress resize, with no worker or registry mutation.
After ingress completion, prove that the explicit resize can succeed and a sibling remains unaffected.
Prove that the registry does not publish geometry before acknowledgement.
Prove that one-slot adapter pressure preserves resize, input, and echo wakes.
Keep snapshot attach, final output, and process-exit behavior correct.
Preserve every observed one-shot wake until the test pumps or asserts it.

### Existing tests

- `pump_woken_worker_resize_updates_live_pty_registry_and_one_patch`: assert geometry after the completion wake.
- `pump_woken_worker_resize_isolates_the_named_sibling`: assert geometry after the completion wake.
- `stalled_resize_acknowledgment_does_not_block_a_later_named_sibling`: replace its old timeout-error expectation with the chosen deadline behavior.
- `drain_resize_persist_failure_still_emits_bound_queue_wake`: check which pump now persists geometry.
- `observe_resize_persist_failure_still_emits_bound_queue_wake`: check which pump now persists geometry.
- `pump_woken_same_wake_resize_then_input_survives_resize_completion`: preserve the wake and ordering assertions.
- `one_slot_adapter_preserves_resize_input_and_echo_wake_obligations`: preserve the pressure and wake assertions.

Explain each changed expectation. Do not weaken an ordering or delivery assertion to accommodate timing.

## Verification

Use the repository test wrapper and the required Rust toolchain.
Initialize the declared Ghostty submodule when needed.
Build the session worker from the same Core revision as the tested library.
Run focused tests first. Then run the required formatting, Clippy, and repository test gates.
Do not repeat passing full gates unless code changes or new evidence require them.

For downstream proof, use an isolated checkout of the selected Hub revision.
Point all Core-family dependencies at the same candidate source for a local comparison.
Build the Hub and its revision-matched worker in that checkout.
Run a representative real-daemon resize/input test and the applicable wake-retirement test.
Record any remaining publication or dependency-update step separately.
Do not publish packages, tags, or merge another active integration branch.

## Review and handoff

Grok owns implementation writes. Fable reads the implementation worktree during review.
Freeze the implementation commit while Fable reviews it.
Grok repairs findings on the same branch.
Fable reviews semantic repairs before the coordinator accepts the result.

Send messages to coordinator session `sess-1788561261-002e-6e11191cb68e3da8e22b8f8cbf0c82d0` with Botster `post_message`.
Include the worktree path, commit, findings, test commands, and next required action.
Store detailed reports in files. Do not use messages as the only evidence store.

## Exclusions

- Session startup scheduling changes.
- Registry filename repairs.
- Consolidation of the server Ghostty models.
- Retained-history policy.
- Client event-loop changes.
- General serialization changes.
- Plugin changes.
- Unrelated repairs in the pending Hub integration branch.
- Explicit `CoreDaemon::resize` persistence redesign, except for the required pending-ingress safety guard.
- Mode-gated input deadline clamping outside the existing paste mechanism.
- Blocking mode-gated control calls, snapshot synchronization, and shutdown draining.
