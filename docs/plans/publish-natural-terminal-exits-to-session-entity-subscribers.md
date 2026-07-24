# Publish natural terminal exits to session entity subscribers

## Target and context

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through the Hub spawn-target registry rather than inferred from the working directory.
- Pipeline context: ticket `ticket_1784783843_374428`, run `run_1784825153_116437`, Plan step `botster_stack_plan`, required gate `botster_stack_plan_gate`; no prior artifacts, reviews, findings, dependencies, questions, or answers were present when planning started.
- Worktree/base: the pipeline-provided target worktree at Hub merge `3c1b3dc` (PR #160), which contains the production WebRTC entity subscription path named by the ticket.
- Repository charter: [[botster-hub-playbook]].
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], and [[botster-runtime-reviewer-playbook]]. [[project-pipelines-playbook]] was intentionally not loaded because this ticket changes neither Project Pipelines package/plugin paths nor workflow policy.
- Planner maps and required Botster context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Hub ownership notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster hub events use bounded priority lanes instead of unbounded queue fuses]].
- Ticket-specific notes loaded: [[botster hub client state sync is entity frame only]], [[botster client subscriptions should not hydrate global state]], [[botster entity snapshots are authoritative reconnect baselines]], [[botster web production runtime session readiness can arrive as entity snapshot]], [[botster core hosts need an explicit drain loop contract]], [[lifecycle guards evaluated before the reconciling drain are one call stale]], [[retention without a reachable flush is data loss]], [[worker shutdown completion requires lifecycle transport and process termination]], [[botster durable terminal egress is owned by sessionio and clientworker actors]], and [[botster terminal clients share one sessionio data plane subscription path]].
- Verification guidance loaded: [[test script required for rust tests not cargo test]], [[rust repo strict lints must be verified before dismissing warnings]], and [[a regression test must be shown to go red with the fix reverted]].

## Current repository evidence

- The documented production topology is `HubDaemon / HubRuntime -> CoreDaemon -> botster-session-worker`. Hub owns the lifecycle projection and delivery; CoreDaemon remains lifecycle truth, and terminal bytes remain on the SessionIo/ClientWorker data plane.
- `serve_daemon` calls `drive_entity_subscriptions` every 20 ms. That pump takes a Core lifecycle baseline, drives `HubRuntime::drain_runtime_once` so asynchronous worker lifecycle can advance, then converts Core lifecycle changes into ordered `DaemonEntityFrame` patches.
- The pump stores drained terminal events in `PendingRuntimeState.events` so a later explicit daemon `Drain` can still deliver them. However, a session with an existing pending event is skipped on later pump iterations. If the first drain yields final terminal output and the worker's `ProcessExited` evidence becomes observable only on a subsequent drain, the lifecycle source stays one call stale indefinitely and no `entity_patch` is emitted.
- CoreDaemon's pinned `879f55e` drain contract already takes and merges retained output before attempting the next runtime drain, tolerates terminal `SessionNotFound`, and reconciles lifecycle observations afterward. The Hub can therefore keep advancing the existing Core drain without inventing lifecycle truth, but it must append newly drained events to its transport-owned pending batch instead of overwriting or discarding them.
- Existing Hub coverage proves different shapes:
  - `session_entity_subscription_observes_natural_exit_without_terminal_attach` passes because no terminal egress becomes pending.
  - `session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress` passes because disconnect cleanup removes the active attachment/pending state before the eventual exit.
  - The published conformance runner claims natural-exit coverage but does not attach a terminal client, so it does not cover the production failure.
- The active botster-web ticket worktree (`ticket_1784752211_333506`) provides the downstream reproducer: subscribe over production WebRTC, spawn and attach, cross two same-URL WebRTC reconnect generations, print `botster-web-runtime-exiting`, and request shutdown. Against Hub merge `3c1b3dc`, the terminal path completes but no authoritative session entity exit patch is observed.
- The active repository convention is a reviewable plan under `docs/plans/`; no README redirects that destination.

## Scope

1. Keep the Hub entity pump driving CoreDaemon lifecycle while terminal egress is already pending for an attached session.
2. Preserve every pending terminal event in order and make it reachable through the existing explicit `Drain`; do not consume, replace, duplicate, or reroute terminal bytes through the entity subscription.
3. Publish the resulting Core lifecycle change through the existing ordered `DaemonEntityFrame::Patch` path without adding a second lifecycle source or counter.
4. Add a focused real HubDaemon/CoreDaemon/session-worker regression for an attached self-exiting process whose output becomes pending before terminal lifecycle is reconciled.
5. Tighten the reusable Hub test-support natural-exit proof so its `natural_exit_patch_observed` claim exercises an attached/pending-egress shape, or add an equally reusable adjacent scenario if changing the existing runner would obscure its other contract checks.
6. Prove the repaired production path with the botster-web packaged WebRTC harness after two reconnect generations, including an explicit authoritative `exited` session entity assertion.

## Non-scope

- No `botster-core` lifecycle, worker protocol, or registry change unless implementation proves the current exported drain contract cannot safely make progress. In that case, stop and register a blocking dependency against target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.
- No botster-web polling, `list_sessions` refresh, legacy session event projection, client-side inferred exit, or repeated-shutdown workaround.
- No new entity DTO, protocol version, conformance fixture revision, npm release, generated TypeScript change, or support-matrix claim: the public contract already promises this behavior.
- No terminal data-plane redesign, WebRTC reconnect redesign, queue-capacity configurability, new dependency, broad daemon transport split, or adjacent cleanup.
- No persistent changes in the botster-web repository in this Hub run. Its active ticket worktree is downstream proof; Web-owned assertion changes remain with that target.
- No automatic removal of exited sessions. Natural exit emits a patch; explicit remove remains the separate remove operation.

## Ownership boundaries and cross-repository dependencies

- `botster-core` owns worker/process lifecycle evidence, lifecycle cursor ordering, drain retention, and reconciliation. The Hub must consume those APIs and must not add a process watcher or shadow lifecycle registry.
- `botster-hub` owns the owner-thread polling schedule, pending daemon-event retention, session entity projection, WebRTC/socket delivery, and subscriber cleanup. The defect and the surgical fix belong here.
- SessionIo/ClientWorker own terminal egress. The Hub's pending batch is only transport retention for later delivery; entity publication must never become the terminal-byte path.
- The in-repository `botster-hub-client` crate owns public DTOs. No client contract change is expected.
- `botster-hub-test-support` owns reusable real-topology conformance. Its current natural-exit claim is a test gap worth correcting in this repository.
- `botster-web` target `tgt_40abcf71ccf049f4ac0c99953a799869` is a downstream consumer/evidence seam, not a blocking implementation dependency. Its active ticket worktree already has the WebRTC entity consumer and two-generation harness. If that harness cannot assert the existing exit contract without new Web behavior, register the Web-owned follow-up rather than editing it in this run.
- No cross-repository prerequisite is currently registered. If Hub cannot advance Core lifecycle without losing retained egress, create a Core dependency instead of broadening this ticket.

## Implementation plan

1. Add a bounded regression that attaches a terminal subscriber, lets output become pending, allows the process to exit naturally, and waits for the entity subscriber's sparse patch containing `lifecycle: "exited"` and the real exit code without issuing a list query or shutdown to manufacture the transition.
2. Demonstrate the regression fails or times out on unmodified `3c1b3dc`; keep timeouts bounded and process cleanup owned by the test.
3. In `drive_entity_subscriptions`, remove the pending-event condition as a lifecycle-progress gate. Continue using the existing CoreDaemon drain path for non-terminal baseline rows, convert the result through the existing event projection, and append new events to the session's retained batch in production order.
4. Preserve current reachability rules:
   - an explicit daemon `Drain` receives all retained events once and in order;
   - detach/disconnect cleanup can still release the session's transport-owned pending state;
   - observation-only drains may advance lifecycle even when they add no public daemon event;
   - a missing/terminal runtime follows CoreDaemon's typed tolerance rather than becoming an infinite retry or synthetic exit.
5. Keep entity delivery unchanged: after the progress drain, read Core's ordered lifecycle changes and emit the existing sparse patch through each independent bounded subscriber queue. Do not mutate `DaemonSessionEntity`, sequence rules, reconnect snapshots, or overflow/resync behavior.
6. Update the reusable test-support scenario or focused daemon lifecycle test so the attached/pending-egress case is durable, while retaining the existing no-terminal and disconnect cleanup cases as distinct regressions.
7. Run the Hub gates and then the active botster-web packaged harness against binaries built from this branch. Require the harness/event ledger to observe the authoritative `exited` entity state after its two reconnect generations and final sentinel, with no `list_sessions` synchronization fallback.

## Assumptions and unknowns

- Assumption: the stall is caused by `pending_runtime.events.contains_key(&session_id)` preventing the next Core drain. This is strongly supported by source shape, the ticket chronology, and the two passing control tests, but the new red test must prove it before the production edit is accepted.
- Assumption: appending `events_from_drain` results to the existing pending batch is the smallest correct fix because CoreDaemon already merges its own retained drain state and the Hub already owns final transport projection.
- Assumption: continuing to poll a terminal Core record is safe because CoreDaemon's current drain contract tolerates exited runtime disappearance after delivering retained evidence.
- Unknown: whether the deterministic red case needs one, two, or more pump iterations to separate terminal output from worker completion. Use an invariant-based bounded wait, not a fixed sleep as the oracle.
- Unknown: whether the best reusable coverage is to strengthen `run_session_lifecycle_subscription_conformance` or add a named attached-final-egress scenario beside it. Prefer strengthening the existing natural-exit claim if its public report remains truthful and stable.
- Unknown: whether the downstream Web worktree already records the exit patch as a hard assertion or only in its event ledger. The Hub acceptance result must still name concrete observed `entity_patch` evidence; a harness exit code without that assertion is insufficient.
- No convention conflict or requested waiver is known. The plan keeps lifecycle authority in Core, delivery policy in Hub, bytes on the data plane, and uses existing framework/repository primitives.

## Affected surfaces and likely files

- `docs/plans/publish-natural-terminal-exits-to-session-entity-subscribers.md` — this reviewable Plan artifact.
- `src/daemon_transport.rs` — lifecycle pump progress and ordered pending-event accumulation; focused unit coverage only if useful for append/cleanup invariants.
- `tests/hub_daemon_lifecycle_test.rs` — real daemon/worker attached natural-exit and retained-egress regression.
- `crates/botster-hub-test-support/src/lib.rs` — only if strengthening the reusable natural-exit conformance runner/report.
- No public client DTO, generated protocol, package asset, README, or Cargo dependency file is expected to change. Any such change requires a ticket-linked justification and plan update.

## Risks and mitigations

- **Duplicate or reordered terminal output:** appending the wrong projection or repeatedly retaining the same Core batch could duplicate bytes. Assert exact marker counts/order and one-time explicit drain delivery.
- **Lifecycle advances but terminal output is lost:** require both the entity exit patch and later delivery of retained terminal marker/process-exit events through the existing drain path.
- **Pending memory grows without a consumer:** preserve current bounded ownership/cleanup behavior and do not introduce another retention store. The fix should only accumulate events Core actually produced for the existing active attachment until normal drain/detach cleanup.
- **Busy polling terminal sessions:** stop relying on fixed iteration counts; once Core reports terminal lifecycle, the baseline guard already excludes later progress drains. Confirm no repeated drain loop remains after patch publication.
- **Regression only covers socket transport:** production state is shared on the daemon owner thread, but downstream proof must still traverse encrypted WebRTC after two fresh subscriptions/reconnects.
- **Explicit shutdown masks natural exit:** the focused test must first prove self-exit publication without shutdown. A separate assertion may show a post-sentinel shutdown remains harmless/idempotent, but it cannot be the lifecycle oracle.
- **Conformance claim stays misleading:** if the shared runner remains unattached, add a clearly named attached-final-egress proof and stop calling the original result sufficient for this bug.
- **Pre-existing test failure misuse:** any broader gate failure must be isolated to the first root and compared with the base; the two focused baseline passes are not a blanket waiver.

## Acceptance checks and downstream proof

1. Negative control:
   - Run the new exact regression against `3c1b3dc` or temporarily revert the enforcement edit and show a bounded failure waiting for the exit entity patch.
   - Reapply the fix and show the same command passes.
2. Focused real-runtime checks through the repository wrapper:
   - `./test.sh --test hub_daemon_lifecycle_test <attached-natural-exit-regression> -- --exact --nocapture`.
   - `./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_observes_natural_exit_without_terminal_attach -- --exact --nocapture`.
   - `./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress -- --exact --nocapture`.
   - If the reusable runner changes, run its exact conformance test and `./test.sh -p botster-hub-test-support`.
3. Regression assertions:
   - final output marker remains deliverable exactly once through daemon `Drain`;
   - entity subscriber receives one ordered patch with `lifecycle: "exited"` and the expected exit code without polling `list_sessions`;
   - ProcessExit/observation-only progress is not discarded;
   - explicit detach/disconnect still cleans pending terminal and entity subscription state;
   - a second healthy entity subscriber is not delayed by the attached terminal path.
4. Repository gates required by the Hub charter:
   - `cargo fmt --all -- --check`;
   - `./test.sh`;
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`;
   - `git diff --check`;
   - inspect the final diff for generated/client/package churn, local paths, secrets, dead compatibility paths, and changes not traceable to this ticket.
5. Production entry-point proof:
   - Build this branch's `botster-hub` and matching `botster-session-worker`.
   - In the active botster-web ticket worktree, run `BOTSTER_HUB_BIN=<branch hub binary> BOTSTER_SESSION_WORKER_BIN=<matching worker binary> npm run smoke:live-packaged-protocol`.
   - The run must cross two fresh WebRTC reconnect generations, print the exit sentinel, detach the terminal path, and observe an authoritative `botster-web.session` exited entity patch/state. It must also retain the existing external-session upsert/exit/remove proof and assert no `list_sessions` hydration.
   - Record redacted binary provenance and the exit entity frame/sequence in the implementation report. Code existence or a passing harness that never asserts the patch is not sufficient.

## Pipeline artifacts and vault disposition

- Plan artifact: this file.
- Plan-time verification evidence:
  - `session_entity_subscription_observes_natural_exit_without_terminal_attach` passed: 1 test, 0 failures.
  - `session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress` passed: 1 test, 0 failures.
  - These are control cases and explicitly do not satisfy the ticket reproducer.
- Project Pipelines checklists:
  - Repository-routed Plan checklist records target resolution, source/CI/downstream inspection, scope, and acceptance discipline.
  - Vault checklist records notes loaded, no convention conflict, focused baseline evidence, and capture disposition.
  - Both checklist creation calls returned `plugin worker invoke timeout`, but durable listing confirmed each checklist was created; no blind retry was performed.
- Vault gap worth capturing after implementation: a Hub host drain loop must continue lifecycle reconciliation while retaining already-pending client egress, appending rather than using pending data as a progress gate.
- Do not capture that as shipped knowledge at Plan time. If the red/green regression confirms it is a reusable rule not already fully covered by [[lifecycle guards evaluated before the reconciling drain are one call stale]] and [[retention without a reachable flush is data loss]], capture it through the vault inbox/pipeline; otherwise record `nil` because the existing notes already cover the durable principle.
