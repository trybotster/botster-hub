# Eliminate Stalled-Attach Shutdown Response Tail Flake

## Context loaded

- Pipeline context: ticket `ticket_1784087788_242994`, run `run_1784592795_631788`, active Plan step `botster_plan` (`run_step_1784592796_294140`), gate `botster_plan_gate`, no current-run artifacts, findings, reviews, or open questions, and nine closed prerequisite tickets.
- Durable human rulings: do not generate host-wide load on a shared machine; deterministic test-owned fault injection is acceptable capture evidence when it goes red with the fix removed; the captured target failure is the shutdown stage returning `botster-hub shutdown error: client disconnected` while `attach_child` remained running; and the final fix still requires the merged isolated runner's loaded acceptance campaign. Timeout inflation, serialization, retry masking, and weakened concurrency assertions are forbidden.
- Required planning authority: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], and the Botster pipeline/worktree notes required by the overlay.
- Controlling test context: [[full suite hangs need source and behavior proof before unrelated waivers]], [[a poisoned test lock is a symptom not a waiver]], [[suite wide acceptance criteria make every observed test failure in scope]], [[botster test sh forwards arguments to cargo not custom unit flags]], and [[plan agents must author vault context as wikilinks not home paths]].
- Repository baseline: the ticket worktree is clean at merge commit `c843378`. Its ticket-owned inputs remain diagnostics commit `1c4af77`, unreviewed response-ordering implementation `e6ed8fa`, and prior plan `93ac234`; `c843378` merges current `origin/main` at `afbf412`. Main now includes every closed prerequisite, including the reopened WebRTC owner fix merged by PR #148, plus the isolated loaded-runner workflow. Implementation must review the ticket commits against this refreshed base and rerun the binding campaign rather than carry forward stale failure premises.
- Captured runtime evidence: the diagnostic campaign identified the assertion in `stalled_attach_stdout_does_not_block_other_daemon_commands` at the shutdown command, with status 1, stderr `botster-hub shutdown error: client disconnected`, and the intentionally stalled attach child still alive.
- Production path inspected: `src/main.rs::operator_shutdown` sends `DaemonRequest::DaemonShutdown`; `serve_daemon` accepts the Unix connection; `handle_connection` sends the request to the control loop; `handle_control_message` creates the shutdown response; and `serve_daemon` calls `daemon.stop()` when that handler returns `true`. On current main, the control loop can choose to stop immediately after handing the response to the connection thread, before that thread completes `write_frame`.
- Runner path inspected on current main: `.github/workflows/loaded-daemon-lifecycle.yml` checks out an exact subject SHA on a fresh `ubuntu-24.04` VM and `script/run-loaded-daemon-lifecycle` runs the lifecycle suite at default test concurrency under bounded `residual-tail` CPU stress, stopping at the first red and retaining full diagnostics.
- Workflow discipline: run checklists `Plan vault discipline` (`checklist_1784592948_874053`) and `Plan workflow discipline` (`checklist_1784592954_201106`) record loaded context, repository inspection, convention fit, verification expectations, and gate handoff. Both creation calls timed out after persisting; listing the run checklists recovered their durable ids without creating duplicates.

## Scope

1. Confirm the existing `origin/main` integration remains current before implementation and merge/rebase again only if main advances, so every closed prerequisite and the merged loaded-runner harness are present before the target fix is evaluated.
2. Preserve or reapply only the ticket-owned stage diagnostics needed to report backpressure samples, command elapsed/status/stdout/stderr, and attach-child state at each assertion boundary.
3. Change the production daemon shutdown ordering so a successful shutdown response does not allow `serve_daemon` to stop the daemon until the originating transport handler has attempted to deliver that response.
4. Apply the ordering contract to both current producers of `ControlMessage::Request`: the local Unix socket handler used by the failing CLI path and the local WebRTC handler that shares the same shutdown request/control-loop contract. Signal-initiated shutdown remains response-less.
5. Remove the narrow test-harness acceptance of `client disconnected` after a clean daemon exit. Once production guarantees response delivery ordering, this outcome must remain visible as a failure rather than being reclassified as successful cleanup.
6. Add deterministic regression proof for the response-before-stop ordering and retain the real stalled-attach integration assertion unchanged in meaning.
7. Verify the exact subject commit through the merged loaded runner for at least 20 consecutive default-parallel lifecycle-suite runs under `residual-tail` load.

## Non-scope

- No timeout increase, fixed sleep, command retry, suite serialization, skipped test, reduced flood volume, lower backpressure threshold, or relaxation of the attach-liveness/concurrent-command assertions.
- No change to session-worker terminal flow control, PTY output volume, CoreDaemon ownership, attach data-plane routing, or the list/send-input/resize request semantics that already passed in the captured failure.
- No new transport abstraction, async runtime, dependency, feature flag, operator configurability, or general response-ack protocol beyond the shutdown ordering signal required by the existing control loop.
- No redesign of daemon shutdown response vocabulary or broad error suppression. `client disconnected` remains an error.
- No changes to the loaded-runner workflow, stress profile, repetition semantics, or internal test deadlines unless current-main integration reveals the runner itself is broken; that would require a separate explicit finding rather than silent scope growth.
- No adjacent cleanup of lifecycle tests or diagnostic helpers beyond conflicts made necessary by integrating current main.

## Botster layers touched

- Rust hub control plane and local daemon transports.
- Rust real-daemon integration harness and public hub test-support cleanup assertions.
- Repository Plan artifact and Project Pipelines evidence.
- No Lua plugin policy, session/client-worker data plane, TUI, React SPA, Rails relay, MCP schema, or docs contract changes.

## Assumptions and unknowns

- Determined: the exact residual is no longer ambiguous; it is the shutdown client losing its final response while the stalled attach child remains alive.
- Root-cause hypothesis to prove, not assume: under scheduler pressure, the control loop sends the shutdown response to the connection thread and immediately returns the stop decision; `serve_daemon` then stops runtime state and exits before the connection thread completes the socket write, so the CLI observes disconnect instead of `response=shutdown`.
- Assumption: the smallest correct synchronization is a shutdown-only completion acknowledgement carried with the existing control request. The control loop may stop after the transport has attempted the response write; it need not wait for unrelated connection cleanup.
- Assumption: the Unix socket's existing write timeout and the WebRTC response sender's existing bounded delivery behavior remain the timeout owners. The change must not add a new arbitrary sleep or inflate those bounds.
- Unknown until implementation review: whether the existing branch attempt guarantees acknowledgement on every success, write-error, channel-close, and WebRTC close path. A missing acknowledgement could strand the single control loop; implementation must audit and test cancellation/error paths before retaining that shape.
- Assumption: signal-forwarded shutdown supplies no delivery waiter because there is no requesting client response to preserve.
- Assumption: reversing the earlier test-support tolerance is required ticket cleanup, not unrelated behavior expansion, because accepting the exact captured error would mask regression of the production guarantee.
- Worktree/target binding: all work stays in the pipeline-provided worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`. Ticket commits `1c4af77` and `e6ed8fa` are inputs for review, not authority over current main.
- No human question is needed at Plan time: the captured assertion and human rulings select one production ordering defect and explicitly constrain the repair.

## Affected surfaces and files

- `src/daemon_transport.rs`
  - Carry a shutdown-only transport-delivery completion receiver with the existing control request.
  - Separate response creation from the final stop decision so the control loop waits for the connection handler's write attempt before returning `true`.
  - Keep signal shutdown response-less and preserve all non-shutdown request behavior.
  - Add deterministic unit coverage that the stop decision cannot occur before the delivery acknowledgement.
- `src/local_webrtc.rs`
  - Participate in the same shutdown response-delivery acknowledgement because it emits the shared control request and can carry `DaemonShutdown`.
  - Acknowledge only after the bounded response-frame send attempt completes, including failed/closed delivery.
- `tests/hub_daemon_lifecycle_test.rs`
  - Preserve the diagnostic-only observations from `1c4af77` after current-main integration.
  - Keep `stalled_attach_stdout_does_not_block_other_daemon_commands` as the real CLI/Unix-socket runtime proof.
  - Reject exact shutdown disconnect even when the daemon process exits cleanly.
- `tests/support/mod.rs`
  - Remove the former exact-disconnect success tolerance so lifecycle cleanup reports a lost shutdown response.
- `crates/botster-hub-test-support/src/lib.rs`
  - Remove the same tolerance from the downstream-shaped isolated-hub harness and update its deterministic classification test.
- `docs/plans/eliminate-stalled-attach-shutdown-response-tail-flake.md`
  - This reviewable Plan artifact.
- Current-main runner files are acceptance inputs, not expected modifications: `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, and `docs/loaded-daemon-lifecycle-runner.md`.

## Implementation sequence

1. Verify `origin/main` has not advanced beyond the integrated `afbf412`; if it has, integrate it first. Inspect the five Rust surfaces above, confirm all nine closed prerequisites and the loaded runner remain present, and recompute the ticket-owned diff before making further edits.
2. Restore the captured test diagnostics without overwriting newer sibling-test fixes. Keep diagnostic helpers private to the integration test and behavior-neutral.
3. Implement the shutdown-only response-delivery acknowledgement in the existing `ControlMessage::Request` path. Ensure the Unix and WebRTC handlers signal after the write/send attempt on both success and error, while signal shutdown uses no waiter.
4. Add a deterministic ordering test that blocks the acknowledgement, proves the shutdown response reaches the transport handler while the daemon stop decision remains pending, then acknowledges delivery and proves the stop decision completes. Capture a temporary ablation run showing this test red when the wait is removed, then restore the production code and rerun green.
5. Remove the two exact-disconnect cleanup tolerances and update their negative tests. This prevents the harness from hiding recurrence of the captured product error.
6. Run focused, lifecycle-suite, workspace, formatting, lint, and whitespace checks on the integrated branch.
7. Commit the exact subject SHA and dispatch a focused loaded smoke followed by the binding 20-run `lifecycle-suite` / `residual-tail` campaign. Attach workflow URL, artifact identity, subject SHA, command/status table, resource samples, and cleanup status.
8. If any campaign run fails, preserve the first red and classify the first non-cascade root. Integrate a merged sibling fix when one owns it; otherwise keep the ticket blocked or ask a human to re-scope. Do not retry past a red result or call a different root unrelated without the required proof and human disposition.

## Runtime-path proof

The deterministic unit test proves the ordering primitive, but it is not sufficient by itself. The focused and loaded `stalled_attach_stdout_does_not_block_other_daemon_commands` runs must execute the compiled `botster-hub` CLI against a live `serve_daemon` Unix socket: a stalled attach fills its unread stdout pipe, list/send-input/resize remain responsive, `operator_shutdown` receives and prints the shutdown response successfully, and only then does `serve_daemon` call `daemon.stop()` and exit. Evidence that the acknowledgement field exists without this live path is insufficient.

## Risks

- **Control-loop deadlock:** a delivery waiter whose producer exits without signalling could block the only control loop forever. Mitigation: make both transport handlers signal after every attempted shutdown response delivery, including errors, and add deterministic close/error coverage or use an ownership shape whose drop is observable.
- **Acknowledging too early:** signalling when the response is merely queued recreates the race. Mitigation: acknowledge only after the Unix `write_frame` or bounded WebRTC frame-send attempt returns.
- **Cross-transport drift:** fixing only the Unix path would leave shared `DaemonShutdown` semantics inconsistent and may fail exhaustive `ControlMessage` construction. Mitigation: enumerate Unix, WebRTC, detach, and signal producers explicitly.
- **Stale-branch overwrite:** replaying `1c4af77` wholesale could erase or conflict with the nine merged sibling fixes in the same large lifecycle test. Mitigation: integrate main first and reapply only ticket-owned hunks with a post-integration diff audit.
- **Masking recurrence:** retaining the old `client disconnected`-means-success test helpers could let the loaded suite pass despite the production bug. Mitigation: remove both tolerances and keep deterministic rejection tests.
- **False confidence from a focused green:** the residual historically appears only in the suite tail under load. Mitigation: require the exact 20-run default-parallel loaded campaign and preserve the first red.
- **Suite-wide roots:** another lifecycle test failing in the binding campaign still blocks acceptance. Mitigation: follow [[suite wide acceptance criteria make every observed test failure in scope]] and the durable human ruling; do not waive from status prose.
- **Diagnostic behavior drift:** broad helper changes could alter timing enough to hide the failure. Mitigation: retain condition thresholds and budgets exactly, and limit diagnostics to observations emitted on success/failure boundaries.

## Acceptance checks and tests

- Freshness and diff discipline:
  - Confirm the implementation is based on current `origin/main` (integrated `afbf412` at Plan time) and contains the merged loaded-runner files plus all closed prerequisite fixes, including PR #148's pressured-WebRTC close repair.
  - `git diff --check` passes.
  - Final production/test diff is limited to the five named Rust surfaces plus this plan unless a required current-main conflict is documented.
- Deterministic ordering and negative control:
  - `./test.sh --lib daemon_shutdown_waits_for_response_write_before_stopping` passes.
  - A temporary ablation that removes/bypasses only the delivery wait makes that exact test fail because the stop decision arrives before acknowledgement; restoring the fix makes it green. Attach the ablation diff and raw command output without committing the ablated state.
  - Add or run bounded error-path coverage proving a failed/closed transport cannot strand shutdown indefinitely.
- Harness strictness:
  - `./test.sh -p botster-hub-test-support shutdown_rejects_client_disconnect_after_clean_daemon_exit` passes.
  - `./test.sh --test hub_daemon_lifecycle_test cli_daemon_shutdown_rejects_exact_disconnect_after_clean_exit -- --exact --nocapture` passes.
- Live runtime path:
  - `./test.sh --test hub_daemon_lifecycle_test stalled_attach_stdout_does_not_block_other_daemon_commands -- --exact --nocapture` passes with the original backpressure, attach-liveness, and concurrent command assertions intact.
  - `./test.sh --test hub_daemon_lifecycle_test` passes at default Cargo concurrency. Do not add `--test-threads=1` to acceptance evidence.
  - `./test.sh` passes at default concurrency. Any red requires exact first-root attribution rather than a blanket pre-existing-failure claim.
- Static quality:
  - `cargo fmt --all -- --check` passes.
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes with raw exit status.
- Loaded acceptance:
  - Dispatch the merged `loaded-daemon-lifecycle.yml` workflow against the exact committed subject SHA with `test_target=lifecycle-suite`, `repetitions=20`, and `stress_profile=residual-tail`.
  - All 20 consecutive runs pass at default test concurrency; focused runs and `--test-threads=1` runs do not count.
  - Artifacts prove the requested/resolved subject SHA, precompiled exact target, observed resource load, per-run exit status, first-red stopping behavior, and successful owned-process cleanup.
  - If a different root appears, it remains blocking until fixed, consumed from a merged prerequisite, or explicitly re-scoped by a human after exact evidence.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Plan gate evidence must include the seven required fields, the stale-branch/current-main disposition, Botster layers, explicit target/worktree binding, both checklist ids, the runtime-path proof, and all durable human rulings.
- Implement evidence must attach the integrated branch diff, deterministic red/green ablation, focused commands, exact subject SHA, and loaded workflow artifact.
- Review must check response ordering, error/cancellation acknowledgement, cross-transport parity, removal of masking tolerances, preservation of the target assertion, current-main prerequisite retention, and absence of speculative refactors.
- Verify must rerun focused checks and inspect the loaded artifact rather than trusting checklist or finding status.

## Vault gaps worth capturing

- Candidate after verified implementation: a daemon must not stop its control plane merely because a terminal response was handed to another thread; destructive request completion should be gated on the transport's bounded delivery attempt when the client response is part of the command contract.
- Candidate only if observed: carrying a raw receiver in a control message can deadlock lifecycle shutdown when a transport task disappears; document the proven ownership/cancellation pattern if implementation needs one.
- No Plan-time capture is required. Existing notes already cover condition-driven loaded tests, red-when-reverted proof, suite-wide acceptance, poisoned-lock attribution, and loaded runner discipline. Record `capture_path=nil` unless implementation produces one of the verified reusable findings above.

## Convention check

No convention conflict or waiver is required. The plan uses existing standard-library channels and transport timeouts, changes the narrow production path identified by diagnostics, preserves default concurrency and observable conditions, consumes merged sibling work before local edits, and adds no dependency or speculative abstraction.
