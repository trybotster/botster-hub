# Stabilize session entity resize transition under load

## Routing

- Ticket: `ticket_1785437787_254990`
- Run: `run_1785447809_642444`
- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Worktree assumption: this run is bound to the ticket target and currently
  starts from `origin/main` commit
  `fab44c5de7b28a8756268608662d2b870efb001a`.
- Repository playbook: [[botster-hub-playbook]]

The authoritative target came from Project Pipelines context plus the Hub spawn
target registry, not from the ambient directory.

## Context loaded

Role and repository guidance:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]
- [[botster-runtime-reviewer-playbook]]
- [[project-pipelines-playbook]] for this run's durable question, checklist,
  artifact, gate, and advancement policy only

Maps and planning notes:

- [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[botster pipeline needs continuous product owner between agent steps]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[vault example paths are not repository placement conventions]]

Hub ownership and runtime notes:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[lifecycle guards evaluated before the reconciling drain are one call stale]]
- [[retention without a reachable flush is data loss]]
- [[worker shutdown completion requires lifecycle transport and process termination]]
- [[an mpsc round trip is not a durability barrier]]
- [[PTY integration tests poll for readiness not fixed sleeps]]
- [[botster core hosts need an explicit drain loop contract]]
- [[clients subscribe to entities not ptys]]
- [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]
- [[subprocess harnesses must kill child on failed readiness]]

The remaining mandatory Hub charter notes concerning package supervision,
WebRTC bootstrap, plugin-worker sizing, and durable-state preflight were also
checked and do not enlarge this lifecycle-test ticket.

Repository/runtime context inspected:

- `README.md` and its single production topology:
  `HubDaemon` / `HubRuntime` -> `CoreDaemon` -> `botster-session-worker`
- `test.sh`, which supplies `BOTSTER_ENV=test` and is the required Rust test
  wrapper
- `src/daemon_transport.rs`, especially owner-loop reconciliation,
  request-triggered reconciliation after resize, lifecycle journal projection,
  ordered patch delivery, and reconnect snapshots
- `src/client_api.rs` and `src/runtime.rs` resize/lifecycle facade seams
- `tests/hub_daemon_lifecycle_test.rs`, especially
  `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect`
- `crates/botster-hub-test-support/src/lib.rs` and its published real-Hub
  lifecycle conformance runner
- `script/run-loaded-daemon-lifecycle` and
  `.github/workflows/loaded-daemon-lifecycle.yml`
- Prior lifecycle/subscription implementation and release reports under
  `docs/reports/`
- Subject run `30569320772` at
  `076dbc4f16c29e06d8908f0982c02ea8f549729e` and authoritative-base run
  `30570139134` at
  `95e829ab039198177e14e17a494f93963951ea6f`

Both cited loaded runs show the same actual failure: the first patch read after
resize had no `rows`, so the assertion observed `None` instead of `Some(31)`.
The base run's separate metadata-owned daemon cleanup failure remains unrelated.

## Human ruling and assumptions

Durable question `question_1785448022_413436` resolved an ambiguity in the
original ticket:

- Preserve resize `rows=31`, `cols=101`.
- Preserve the fixture's natural process exit and `exit_code=0`.
- Treat the original `exit_code=31` wording as a transcription error that
  conflated the row value with process status.
- Do not change the fixture to exit 31.

The ticket description was corrected accordingly.

Working assumptions:

- The likely race is in test readiness: the current fixture prints
  `entity-after`, sleeps only 250 ms, and may naturally exit before a loaded
  test thread observes the marker and submits resize.
- That diagnosis is not accepted solely from source inspection. Controlled
  frame/lifecycle evidence must show whether the first frame is an early exit
  patch, a late resize patch, a dropped delivery, or stale lifecycle truth.
- A bounded deadline remains acceptable only as hang protection. Elapsed time
  must not be the readiness condition.
- Current locked Core revision
  `5846fc776d31e2b6c98a8d932f50a31078743901` remains unchanged unless evidence
  proves a Core lifecycle-journal defect.

Unknowns to resolve during implementation:

- Exact first unexpected frame and both subscribers' matching sequence numbers
  at the loaded failure boundary.
- Whether the resize request reaches Core while the session is still current,
  or races a lifecycle-exit journal entry.
- Whether the in-repository published conformance runner's separate two-second
  fixture has enough post-spawn liveness margin under `residual-tail`. Measure
  elapsed spawn-to-resize-ack time against its two-second process budget and
  record an explicit keep-or-change decision in the implementation report.
  Keeping it unchanged is acceptable only with that measured basis.

## Scope

1. Reproduce and classify the transition with controlled evidence.
   Record the resize response, ordered frames seen by both entity subscribers,
   relevant lifecycle counters/state, and cleanup outcome. Make diagnostics
   identify the first non-resize patch rather than failing with a field-only
   `None`.
2. Replace the short-lived fixture's scheduler-dependent tail with a semantic,
   test-owned barrier. The process must remain live after publishing a readiness
   marker, accept resize, and be explicitly released through the real terminal
   input path so it still exits naturally with code 0.
3. Assert the production path in order for both subscribers:
   authoritative snapshot -> session upsert -> rows/cols resize patch -> natural
   exit patch. Keep strict increasing sequence checks and require the fresh
   reconnect snapshot to reflect current authoritative retained/removed state.
4. Add a focused loaded-runner target for this exact regression. Update the
   validation allowlist, command dispatch, and workflow `test_target` choices in
   lockstep; the current runner has no selector for this test. Reuse the existing
   load profile, exact-SHA checkout, process tracking, and cleanup artifact
   machinery.
5. Change production reconciliation code only if the controlled live-session
   barrier still proves delivery, ordering, or lifecycle truth is wrong. Any
   such change must be the smallest correction at the existing HubRuntime /
   CoreDaemon seam and must add a deterministic production-path regression.

Every changed line must implement the barrier, expose actionable failure
evidence, invoke the focused proof, or update the required plan/report evidence.

## Non-scope

- No timeout inflation, extra sleeps, polling fallback, weakened rows/cols or
  sequence assertions, or acceptance of either resize/exit order.
- No `exit_code=31` behavior.
- No changes to
  `cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup` or ticket
  `ticket_1785390881_372348`.
- No broad daemon transport refactor, queue/cadence configurability, new
  lifecycle abstraction, protocol/DTO/fixture revision, package publication,
  or adjacent cleanup.
- No Web, TUI, Project Pipelines package, legacy monolith, Ghostty, or package
  supervision changes.

## Ownership boundaries and dependencies

- `botster-core` remains policy-free authority for session size, lifecycle
  journal ordering, process exit truth, and the session worker.
- `botster-hub` owns the production request/reconciliation schedule, sanitized
  session entity projection, bounded ordered delivery, real-daemon test
  topology, and loaded lifecycle harness.
- `botster-hub-client` remains the DTO/socket boundary; no contract change is
  planned.
- Terminal output/input remains on the existing SessionIo/ClientWorker data
  path. The test may use that path as its barrier but must not move terminal
  bytes into Hub entity state.
- Project Pipelines owns workflow evidence only; its package code is not in
  implementation scope.

No cross-repository dependency is currently required. If controlled evidence
shows Core records exit/resize out of contract or loses an accepted resize
transition, stop Hub implementation, register a dependency ticket against the
`botster-core` target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`, and keep this
run blocked on that dependency rather than patching Core behavior in Hub.

## Affected surfaces and files

Expected:

- `tests/hub_daemon_lifecycle_test.rs` -- deterministic live-session barrier,
  ordered frame assertions, and failure diagnostics
- `script/run-loaded-daemon-lifecycle` -- focused regression selector
- `.github/workflows/loaded-daemon-lifecycle.yml` -- expose the focused selector
  to exact-SHA loaded proof
- `docs/reports/` -- implementation/verification evidence using current
  repository naming prior art

Conditional, only if evidence requires it:

- `crates/botster-hub-test-support/src/lib.rs` -- measure its two-second
  fixture's spawn-to-resize-ack liveness margin under the same loaded profile,
  then explicitly keep or change it; any temporary instrumentation must be
  recorded as a reproducible patch in the report
- `src/daemon_transport.rs`, `src/client_api.rs`, or `src/runtime.rs` -- only if
  the live barrier proves a production defect rather than fixture readiness

No generated client/package assets are expected because the wire contract does
not change.

## Implementation sequence

1. Preserve a reproducible red evidence packet from the existing fixture:
   exact Hub SHA, locked Core SHA, exact command/load profile, first unexpected
   frame for both subscribers, resize response, and cleanup artifact. Use
   either the cited pre-fix SHA with a complete invocation recipe or record the
   verbatim temporary diagnostic/delay patch hunk in the implementation report.
   Temporary instrumentation must not remain in production tests, but Review
   and Verify must be able to re-derive the red from the report.
2. Make the fixture publish semantic readiness and then block on test-controlled
   input. Observe readiness through real terminal output, submit resize while
   the process is provably live, and require both subscribers to receive the
   same rows/cols patch sequence.
3. Release the process through real input, require natural `exit_code=0`, then
   preserve remove/disconnect/fresh-subscription assertions and exact cleanup.
4. If frame evidence still shows a defect, trace
   `DaemonRequest::Resize` -> `HubClientApi` -> `HubRuntime` -> `CoreDaemon` ->
   request-triggered `drive_entity_subscriptions`; fix only the proven seam.
5. Measure the published conformance runner's elapsed spawn-to-resize-ack time
   under `residual-tail` against its two-second fixture budget. Record the
   measurement and an explicit keep/change decision. If temporary timing
   instrumentation is used, preserve its exact patch and command in the report.
6. Add the focused loaded campaign and produce repeated local/default and Linux
   loaded results without changing time budgets.

## Risks

- A barrier can accidentally test a synthetic helper rather than the real
  daemon/session-worker path. Use only public daemon requests and entity frames.
- Releasing the process too early recreates the race; never infer readiness from
  spawn acknowledgement, queue round trip, or elapsed sleep.
- Reading until a desired frame while silently discarding earlier frames could
  mask an ordering regression. Capture and classify every intervening frame.
- Updating only one subscriber can hide divergence. Both subscribers must agree
  on resize sequence and ordered lifecycle state.
- The published conformance runner's discard-until-match loops prove eventual
  matching frames and matching-frame sequence arithmetic only. Its report
  booleans cannot prove that no out-of-contract frame preceded a match.
- Test failure/panic before release can strand a worker. Existing owned harness
  cleanup must remain active, and teardown artifacts must show zero survivors.
- A broad production change could disturb the fixed owner loop or 500 ms
  backstop. Do not change those paths without controlled evidence.
- Loaded full-suite red may include unrelated failures. Attribute each failure
  to exact branch/base evidence; do not use a pre-existing red as a blanket
  waiver.

## Acceptance checks

Red/green and focused proof:

- Demonstrate the pre-fix failure under a controlled negative condition or the
  cited exact pre-fix loaded checkout, including the first unexpected frame;
  record an exact reproducible SHA/command/profile recipe or verbatim temporary
  patch hunk, then show the same focused path green with the deterministic
  barrier.
- Run the exact regression repeatedly through:

  `./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect -- --exact --nocapture`

- Run the lifecycle integration binary at default parallelism repeatedly; do
  not serialize it to hide contention.
- If `botster-hub-test-support` changes, run
  `./test.sh -p botster-hub-test-support` and its real isolated-Hub conformance
  caller.

Repository gates:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `./test.sh --workspace --no-run`
- `./test.sh`
- `git diff --check`

Production/downstream proof:

- Run the focused exact-SHA Linux campaign for multiple repetitions under
  `residual-tail`, followed by the existing `full-suite-contention` campaign.
- The focused selector must be present in all three runner surfaces:
  `script/run-loaded-daemon-lifecycle` validation, its command dispatch, and
  `.github/workflows/loaded-daemon-lifecycle.yml` input choices.
- Record the tested Hub SHA and locked Core SHA separately, build the session
  worker with `--locked`, and verify both binary realpaths belong to the fresh
  target directory.
- Prove both subscribers observe the same strictly ordered resize and natural
  exit transitions by capturing and classifying every intervening frame in the
  direct live-daemon section. Replace the current unchecked
  `let _ = second.next_frame()` with explicit second-subscriber field and
  sequence assertions. Prove reconnect receives a fresh authoritative snapshot.
- Do not count `SessionLifecycleSubscriptionConformanceReport` booleans as proof
  that no out-of-contract frame preceded resize or exit; its current
  discard-until-match loops establish eventual matching delivery only. The
  direct two-subscriber capture is the ordered-delivery authority for this
  ticket.
- Record the published conformance runner's spawn-to-resize-ack measurement
  under `residual-tail`, its two-second budget, and the explicit keep/change
  decision in the implementation report.
- Preserve exact branch/base attribution for every new red. The known
  metadata-owned cleanup failure is not in scope and may be waived only with
  matching authoritative-base evidence.
- Require `cleanup_status=0`, every tracked role `post_clean=gone`, zero session
  survivors, no zombie/owned worker survivors, and no stale owned sockets.

## Pipeline evidence

- Run vault checklist `checklist_1785448031_165946` records notes loaded,
  convention fit, planning verification, and capture disposition.
- Implement gate evidence must link committed work, the implementation report,
  exact commands/results, red/green proof, loaded run ids/artifacts, and cleanup
  evidence.
- Plan Review should verify the target mapping, human ruling, conditional Core
  dependency threshold, and that the plan does not silently turn a test
  readiness defect into a production refactor.

## Vault gaps

No new durable vault note is required from Plan. Existing notes already cover
semantic readiness, stale-before-drain lifecycle decisions, asynchronous
barrier fallacies, retained output, shared conformance authority, exact runtime
provenance, and subprocess cleanup.

If implementation proves a reusable invariant that is not covered -- for
example, a resize acknowledgement that does not imply lifecycle-journal
projection -- capture it inbox-first and link the resulting atomic note from
the implementation report. The corrected rows-versus-exit-code transcription
is ticket-specific and is already durable in the ticket, question answer, and
this plan.
