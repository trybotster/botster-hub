# Plan: Hub makes ShutdownSession idempotent across natural-exit races

Ticket: `ticket_1786977409_499180`
Run: `run_1787012955_256937` (supersedes the oracle-repair run `run_1786977413_341616` after the ticket consolidated to the strict clean contract)
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

The user chose the strict clean contract after independent Cursor and Fable audits. This plan replaces the prior oracle-repair plan (`docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md`). The prior plan treated the blind-call typed `OperatorError` as legal host behavior. The consolidated ticket makes that behavior a product defect: after a finite session process exits, `ShutdownSession` must not return `OperatorError` only because `ProcessExited` is still in flight, on both the WebRTC and Unix transports. The strict contract forbids the transient `OperatorError` only when natural exit is provable; when exact evidence shows the worker remains truly active or stuck, `OperatorError` is preserved. `ticket_1787004132_469467` is closed as an explicit duplicate; its Unix proof is required here.

Revision note (audit ratification corrections, ticket update 1787013609): the ticket added corrections while this plan was being written. This revision binds them: the two `shutdown_active_*_remains_operator_error` unit tests are updated in the same strict-contract change (acceptance check 5); the idempotency sibling's unobserved-exit branch is updated in the same change (Phase 3 item 5, already planned); and no full lifecycle suite runs as this leaf ticket's gate -- the harness ticket and final integration own controlled full-suite smoke tests (acceptance check 10 replaced).

Revision note (Plan Review `review_1787014361_115204`): this revision addresses all four findings. `finding_1787014361_663892`: the complete late-message admission matrix is now inline in this plan (lens answers below), not a reference to the superseded plan. `finding_1787014361_336715`: [[project-pipelines-playbook]] is loaded and Rule B now specifies the full dependency workflow that causes progress (create on the Core target, register the blocking edge, start the dependency run, or emit one explicit operator action). `finding_1787014361_760019`: the prior plan's registration claim was false; the downstream blocker is now actually registered as `dependency_1787014444_456296` (ticket_1786938984_190098 depends on this ticket), verified through `project_pipelines_list_ticket_dependencies`, with the downstream-Implement containment stated in the ownership section. `finding_1787014361_739602`: a fresh plan artifact pins this revision's commit.

Revision note (orchestrator option 3, Implement): a Hub parent-wrapper cannot satisfy Core welcome `worker_pid` / readiness identity, so Hub W1/W2 wrapper tests are removed rather than ignored. Cross-repo mechanism proof is Core `d981bb03` tests `drain_output_delivers_process_exited_while_worker_holds_stdout_open` and `drain_output_delivers_process_exited_when_worker_exits_nonzero`. Hub focused contract proof is one held-producer WebRTC exact-bytes path and one Unix cross-connection path with explicit attach, print-release, byte receipt, exit-release, and `process_exit`. Do not require five idempotency rounds or a full lifecycle suite. Gates are focused tests, units, fmt, and clippy.

Revision note (`review_1787034725_875591`): closed Core `ticket_1787015956_494734` at pin `d981bb03` regressed repeated WebRTC live-output rounds. Hub kept that pin and registered `ticket_1787034922_646556`. That Core ticket is now closed. Hub pins Core main `302c7f7b61f3970a0151b8c6646fc21ae7bd6c67` and reruns every live proof, including `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`. Do not roll the pin back to `fd66efd` or `d981bb03`.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the ticket record through `list_spawn_targets`. The run record carries the same target_id.
- Worktree: the pipeline ticket worktree, branch `project-pipelines/ticket_1786977409_499180`. The branch carries the prior run's commits through `a24ac2e` plus a provenance commit (`b49dcfb`) that preserves the prior run's uncommitted Verify-round isolation work verbatim.
- The worktree path contains no colon. No `CARGO_TARGET_DIR` override is required.
- Tracked `.gitignore` is present and non-empty (5 lines). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon control plane and `ShutdownSession` classification. Charter gate in scope: "For `ShutdownSession`, prove exact-session `Found`, `Absent`, and `Err` behavior. Reject `Drain`, baseline, or capped-page classification."

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay: completion evidence, worktree hygiene, runtime-teardown class trigger.
- [[botster-architecture]] and [[cli-patterns]] -- Must Load context.
- [[botster runtime teardown lenses]] -- the class applies; answers below.
- [[host ShutdownSession classification must call the exact-session Core query]] -- the shipped classify convention this contract extends.
- [[observed-exit waits must issue a production exact-session observe turn]] -- `ListSessions` cannot advance exit state; observe turns can.
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]] -- the prior run applied this note. The consolidated ticket changes the codified host contract itself, so both oracles in the file now move to the strict contract together. Vault gap below.
- [[flake oracles over typed response frames must print the full typed error body]] -- diagnosis requirement carried into every changed assert.
- [[hub shutdown preserves durable session workers]] -- Hub-process shutdown evidence stays separate from session cleanup evidence.
- [[conformance harnesses gate on deterministic invariants not timing]] -- the required proofs use deterministic forced windows, not suite-load luck.
- [[a regression test must be shown to go red with the fix reverted]] -- red-on-revert is a ticket requirement.
- [[botster-core-playbook]] boundary rule only (reusable policy-free mechanisms belong to Core) -- consulted for the cross-repo decision gate; this run stays routed to botster-hub.
- [[project-pipelines-playbook]] -- loaded because Rule B changes workflow state (dependency creation, blocking edge, run start). Applied notes: [[dependency ticket creation must start its run or emit an operator action]], [[cross repo dependency registration must use dependency repo target]], [[dependency closure must requeue the blocked parent step]], [[project pipeline step activation gates open ticket dependencies before side effects]].
- Memory note: botster-hub consumes Core by git branch; merged Core main is the consumable artifact. This governs the Rule B repin below.

## Context loaded

- Ticket, run, gates, prior checklist (`checklist_1786978235_747825`) via `project_pipelines_current_context`. No open questions. No artifacts yet on this run.
- Prior-run lineage read in the worktree: the approved oracle-repair plan, the Implement report (including Verify return `review_1786989262_776285` with the 2/28 pair-run stall evidence, `last=Some("running")` for 10 s with the producer worker already dead), and the inherited diffs now preserved at `b49dcfb`.
- Hub code read: `ShutdownSession` arm (`src/daemon_transport.rs:3401-3445`), `classify_shutdown_session` (`:4676-4694`), `classify_found_session_lifecycle` (`:4696-4729`), `recover_after_core_shutdown_error` (`:4485-4494`, propagates classify `Err` with `?`), `shutdown_error_response` (`:4652-4674`, `Active` + real error returns the raw typed error), `shutdown_error_is_already_gone` (`:4641`), `shutdown_lookup_error` (`:4731`), worker path config (`src/config.rs:535`, `src/runtime.rs:4490`).
- Core code read at the locked pin `fc541a5` (`~/.cargo/git/checkouts/botster-core-ea2698e4cbd07384/fc541a5`):
  - `CoreDaemon::shutdown_session` (`crates/botster-core-daemon/src/daemon.rs:1614-1690`): engine shutdown error is saved, a 2-second drain loop follows, a non-`session_not_found` drain error returns immediately, and deadline expiry returns the saved error or `ShutdownFailed`. When the loop observes exit, the call returns `Ok` even after an engine shutdown error.
  - `CoreDaemon::observe_session_lifecycle` (`daemon.rs:800-825`): the observe drain runs before the registry read, so a drain failure returns `Err` without reading recorded registry truth.
  - `WorkerProcessRuntime::drain_output` (`crates/botster-core/src/runtime/worker_process.rs:1287-1346`): the worker-reported `ProcessExited` payload is surfaced only when the reader finished AND the worker child's `try_wait()` reports a successful exit (`status.success()`); an adopted session (`child: None`) passes unconditionally. A live-but-exiting worker yields `try_wait() == None` (payload delayed); a non-success worker exit suppresses the payload permanently.
  - `pump_session_output` (`worker_process.rs:841-894`): channel-based; it does not error on worker socket death.
  - `ManagedSessionRuntime::shutdown_session` (`crates/botster-core/src/engine/managed_session_runtime.rs:929-967`): a failed shutdown-input flush rolls the lifecycle back to the previous state and returns the error, erasing `Stopping` evidence.
- Test code read: `external_hub_webrtc_live_output_preserves_exact_bytes` (current observed-exit-wait shape, `tests/hub_daemon_lifecycle/webrtc_proofs.rs:404-481`), `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (blind branch admits typed `OperatorError`, `:629-680`), `unix_shutdown_session_from_another_connection_classifies_attached_exit` (`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1681-1776`, already blind-call strict shape: `assert_ne!(shutdown.kind, OperatorError)`), `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` (true-error and sibling-survival pin, `tests/hub_daemon_lifecycle/sessions.rs`), isolated hub worker-binary override (`tests/hub_daemon_lifecycle/session_fixtures.rs:362-380`, `.session_worker_bin(...)`).

## Failure mechanism (code-grounded prediction, validated in Phase 1)

The natural-exit race decomposes into sub-cases at the locked Core pin:

1. Exit already recorded (registry `Exited`) -> classify returns `Cleanup` -> works today.
2. Payload drainable (reader finished, worker reaped with success) -> classify's observe drains it -> `Cleanup` -> works today.
3. Payload present, worker not yet reaped, window shorter than 2 s -> classify says `Active`, Core's shutdown drain loop observes exit inside the deadline and returns `Ok` -> `Events` -> works today. This is why isolated runs stay green.
4. Payload present, worker reap exceeds Core's 2-second deadline (suite load) -> Core returns the saved flush error or `ShutdownFailed`; Hub's recover re-classify runs microseconds later, the payload is still gated, classification stays `Active`, and `shutdown_error_response` returns the transient `OperatorError`. This is the recorded flake class.
5. Worker exits non-success after the session child exited (including a signal kill of an exiting worker) -> `status.success()` is false, the payload is suppressed permanently, the registry stays `running` forever, and every blind `ShutdownSession` returns `OperatorError` after the 2-second loop. This matches the Verify pair-run evidence (`last=Some("running")` for 10 s with the producer worker already dead).
6. Recover-path classify `Err` (drain injection, registry I/O) is propagated by `?` (`src/daemon_transport.rs:4492`) even when the registry already records `Exited` -- recorded truth is never consulted because `observe_session_lifecycle` drains before the registry read.

The distinguishing evidence between "natural exit in flight" and "true failure" is the worker-written `ProcessExited` payload held in Core's completion state. In the true-failure construction (worker SIGKILLed while the session child lives), no payload exists. No current Core query exposes payload presence to Hub, and Hub must not add a wall-clock or retry mechanism. Sub-cases 4 and 5 therefore predict a Core semantics change (Rule B below). Sub-case 6 is Hub-owned regardless.

## Runtime-teardown lens answers

`teardown_class_applies`: yes. `ShutdownSession` raced against natural worker/session exit observation; sub-case 5 is a live terminal-state vs live-runtime divergence (dead worker, registry `running` forever).

`teardown_isolation`: `ShutdownSession` targets exactly one `session_id` through the exact-session Core query; the reconciliation keys on exact-session evidence only. Each test uses an isolated hub with a private data directory, endpoint, and worker set. One session's reconciliation cannot classify through another session's state.

`teardown_bounds`: no new production wall-clock, retry count, or timing mechanism -- the ticket forbids them as correctness mechanisms. Core's 2-second shutdown deadline, the worker 500 ms grace, and Hub's bounded observe turn stay unchanged unless Phase 1 evidence proves a budget is the defect. Hub's recover path stays one bounded re-observe plus, new, one non-draining recorded-truth read; no unbounded `block_on`. The Rule B Core dependency contract explicitly forbids replacing the `try_wait` gate with a blocking `child.wait()` on the drain path.

`late_message_matrix`: this ticket adds no ownership-creating message, and no row's production behavior changes. The complete current matrix for every ownership-creating message on the control plane these tests exercise (Unix control endpoint plus the encrypted WebRTC terminal adapter of one isolated external hub) is inline here, with the exact tests that prove each column.

The production terminal-failure boundary is one shared gate: after `PeerClosed`, `has_live_peer` (`src/local_webrtc.rs:293`) guards the control-plane dispatch, so every grant-tagged `Request` (Spawn, Attach, ShutdownSession, ReadScreen -- any `DaemonRequest` arriving over WebRTC) is rejected with the typed `local_webrtc_peer_gone` frame before dispatch (`src/daemon_transport.rs:2160-2171`, `:5280`), a late `SubscribeEntities` is rejected the same way (`:2069-2086`), and a late Hello's admission is dropped instead of inserted (`:2060-2068`).

| Message | Ownership effect | Owner tag | Rejection after terminal failure (production handler driven) | Race sweep / replacement protection |
| --- | --- | --- | --- | --- |
| `Spawn` (grant-tagged Request) | Creates a durable session worker | `grant_id` on the request; `session_id` under host policy (durable by design, per [[hub shutdown preserves durable session workers]]) | Late Spawn after `PeerClosed` is rejected `local_webrtc_peer_gone` and creates no session lifecycle row: `local_webrtc_late_spawn_after_peer_closed_does_not_create_session` (`src/local_webrtc.rs:5958`, rejection asserted at `:6001`) | Durable workers are intentionally not swept by peer loss; explicit shutdown owns teardown (this ticket's strict blind-call oracles plus `shutdown_after_observed_exit_returns_session_cleanup`, `tests/hub_daemon_lifecycle/sessions.rs:3240`). Duplicate-id integrity on the Unix path: `external_hub_client_duplicate_botster_web_runtime_spawn_is_rejected_without_cleanup` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:1756`) |
| `IssueLocalWebrtcBootstrap` + `LocalWebrtcSignal` (Unix control) | Creates grant-owned peer and signaling state | single-use `grant_id`/`grant_secret` bound to the admitted origin | The non-owning boundary is grant validation (`src/local_webrtc.rs:554-568`): a redeemed, expired, secret-mismatched, or origin-mismatched signal is a typed rejection, so a stale re-signal on a consumed grant cannot recreate peer state; each new peer requires a fresh bootstrap: `botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:1129`) | Peer close detaches every peer-owned terminal subscription together: `local_webrtc_peer_close_detaches_terminal_subscriptions` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:1499`) |
| Encrypted WebRTC `Hello` | Creates the WebRTC terminal admission entry | keyed by `grant_id` | The non-owning boundary is the admission insert gate: a Hello admission for a peer that is no longer live is dropped, not inserted (`src/daemon_transport.rs:2060-2068`), and any ownership a stale admission could try to exercise afterward is a grant-tagged Request caught by the universal `local_webrtc_peer_gone` gate proven by the late Attach/Spawn tests in this table. Hello content policy: `webrtc_terminal_adapter_unnegotiated_adapter_never_receives_or_decodes_daemon_event` (`tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:969`) | Admission entry retires with the peer (peer-close sweep row above) |
| `Attach` (grant-tagged Request) | Creates terminal attach route ownership and lifecycle counts | grant plus `subscription_id` (route identity) | Late Attach after `PeerClosed` is rejected `local_webrtc_peer_gone` and does not recreate daemon state: `local_webrtc_late_attach_after_peer_closed_does_not_recreate_state` (`src/local_webrtc.rs:5883`, rejection asserted at `:5929`). Complementary fresh-owner claim: `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route` (`webrtc_terminal_adapter.rs:684`) | Bound peer loss closes the adapter without a Hub-invented Detach: `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach` (`:527`); a stale-generation close cannot sweep a replacement owner: `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` (`:1150`); a stale peer attach snapshot cannot detach a replacement owner: `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` (`src/local_webrtc.rs:6226`); explicit Detach stays separate from peer loss: `webrtc_terminal_adapter_explicit_detach_is_separate_from_peer_loss` (`:614`) |
| `SubscribeEntities` / `UnsubscribeEntities` (control messages) | Create/delete entity-subscription rows | `grant_id` on the control message | Late Subscribe after `PeerClosed` is rejected `local_webrtc_peer_gone` with no row created: `local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state` (`src/local_webrtc.rs:5306`, rejection asserted at `:5349`) | Subscribe-first order swept by owner grant: `local_webrtc_subscribe_before_peer_closed_is_swept_by_owner_grant` (`:5785`); delayed snapshot cannot remove a replacement owner's reused id: `local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner` (`:5667`); late Unsubscribe cannot delete a replacement owner's row: `local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row` (`:6013`) |
| `ShutdownSession` (this ticket's message) | Destroys ownership; creates none | exact `session_id` (over WebRTC additionally grant-tagged and covered by the universal `local_webrtc_peer_gone` gate) | Strict natural-exit contract: blind call after provable natural exit returns `Events` or `SessionCleanup{already_exited}` (this ticket's forced-window tests, checks 2-4); a miss is the typed `unknown_session` frame (Absent probe inside check 3); true failure with exact truly-active/stuck evidence stays typed `OperatorError` (`external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`, check 7) | Close-event suppression on explicit shutdown: `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event` (`webrtc_terminal_adapter.rs:1022`); idempotent repeat cleanup: tightened `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (check 8) |

`ReadScreen`, `ListSessions`, `Status`, and `StartPackageEntrypoint` create no per-peer durable ownership rows on this plane (`StartPackageEntrypoint` is owned by package supervision, outside this ticket's message scope and untouched). No other peer-originated message class in these tests' design creates durable ownership. The rejection-column stale-peer tests stay binding through acceptance check 7's exact lib filter. None of those production-handler tests is modified by this ticket.

`production_path_proof`: the live path is Unix or WebRTC control frame -> `ShutdownSession` arm (`src/daemon_transport.rs:3401`) -> `classify_shutdown_session` -> Core `shutdown_session` -> `recover_after_core_shutdown_error`. The forced-window tests spawn a real session through a controlled worker wrapper registered via the production `core_engine.session_worker_path` config, exit it naturally, and drive blind `ShutdownSession` through the production handler on both transports. Red-on-revert: with the reconciliation removed, the forced-window test must fail with the transient `OperatorError` (acceptance check 6).

`ownership_identity`: sessions stay keyed by exact `session_id`. The Absent probe uses a never-spawned id. The wrapper changes the worker process identity only, never the session identity. Each isolated hub uses unique session ids; no reused-id hazard.

`sibling_fail_closed_policy`: on cleanup/success, the hub, the calling connection, and sibling sessions stay fully serviceable (pinned by the idempotency test's sequential rounds and the adapter-isolation tests). On true failure (`Active` + real error), the existing production policy is unchanged: victim-session adapters close, the typed error returns, the connection and siblings survive -- pinned live by `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`, which stays green under the strict contract because its construction (worker SIGKILL before any `ProcessExited` payload, plus drain injection) is a true failure, not a natural exit. Blast radius stays one request's reply plus victim-session adapters.

## Scope

### Phase 0 -- inherited-state disposition

Keep from the prior run: the full-error-body assert diagnostics, the Absent-leg `unknown_session` probe, the leftover-worker reap helpers, the IsolatedHub injection-env stripping, the env-gated drain-injection hook in `src/runtime.rs`, and the true-error sibling-survival test. Replace: the 10-second observed-exit wait and `ReadScreen` pump in the exact-bytes test (Phase 3 restores the blind call). Tighten: the idempotency sibling's blind branch (Phase 3).

### Phase 1 -- deterministic diagnosis (ticket-required)

1. Build a controlled worker wrapper fixture: an executable script registered through the isolated hub's `.session_worker_bin(...)` (production `core_engine.session_worker_path` surface) that runs the real `botster-session-worker` as a child, waits for it, then:
   - Variant W1: sleeps an env-configured window (longer than Core's 2-second deadline) before exiting with the worker's status. This forces sub-case 4 deterministically: payload present, wrapper unreaped past the deadline.
   - Variant W2: exits non-zero after the worker succeeded. This forces sub-case 5 deterministically: payload suppressed by the `status.success()` gate.
2. On both transports (WebRTC exact-bytes shape and the Unix another-connection shape), run a natural finite exit under W1 and W2, call blind `ShutdownSession`, and capture `error.code`, `error.operation`, and `error.message` from every failing path, plus the recover-path classification. Record verbatim output in the Implement report.
3. Validate the mechanism citations above against the captures. If a capture contradicts a cited sub-case, stop and re-diagnose before any production edit.

### Phase 2 -- decision gate (ticket-required: "decide whether Hub exact-session reconciliation is sufficient")

- Rule A (Hub-sufficient): if the W1/W2 captures show existing Core surfaces already give Hub evidence that distinguishes "session process exited, delivery in flight" from "session active, shutdown truly failed", implement the reconciliation in Hub only. No Core ticket.
- Rule B (Core change required -- predicted): if the captures confirm the exit evidence is Core-internal (payload presence gated by worker reap timing or worker exit status; recorded truth erased by the managed rollback), execute the full Project Pipelines dependency workflow so the dependency causes progress (per [[project-pipelines-playbook]], [[dependency ticket creation must start its run or emit an operator action]], and [[cross repo dependency registration must use dependency repo target]]):
  1. `project_pipelines_create_ticket` on the `botster-core` target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` (the dependency repository's own target, never this run's target), carrying the deterministic W1/W2 reproduction and the required Core contract: a received `ProcessExited` payload is session-exit truth; its delivery to drains and observes must not gate on the worker process's own reap timing or exit status; the mechanism must not block the daemon on `child.wait()`; a worker connection that dies without a payload keeps its current true-error semantics. Core chooses the mechanism (policy-free mechanism per the Core charter; an acceptable alternative is exposing pending-exit evidence through `observe_session_lifecycle`).
  2. `project_pipelines_add_ticket_dependency` registering this ticket (`ticket_1786977409_499180`) as depending on the new Core ticket, and verifying the edge through `project_pipelines_list_ticket_dependencies`.
  3. `project_pipelines_start_run` for the new Core ticket so the dependency makes progress. If automatic start fails or is unavailable, emit exactly one explicit operator action via `project_pipelines_ask_human` naming the created Core ticket id and the requested start -- do not park silently behind an inactive dependency.
  4. On dependency closure, this run resumes ([[dependency closure must requeue the blocked parent step]]); this ticket then repins Hub's locked Core revision to merged Core main as required integration (memory note: merged Core main is the consumable artifact) and completes the proofs. Do not silently repin; record the pin change in the Implement report.
- Under both rules, the Hub-owned legs land in this ticket:
  1. `recover_after_core_shutdown_error` stops propagating classify `Err` blindly (`src/daemon_transport.rs:4492`). On classify `Err` after a Core shutdown error, Hub falls back to a non-draining recorded-truth read of the exact session (registry-backed, the store `ListSessions` reads): recorded `Exited` -> `SessionCleanup{already_exited}`; recorded `Stale`/`Failed` -> `SessionCleanup{stale_session}`; recorded `Stopping` -> `SessionCleanup{already_exited}`; recorded `Running` or fallback failure -> the original typed Core error, preserved. This fixes sub-case 6 and never invents exit evidence.
  2. True-error preservation stays byte-for-byte: `Active` classification plus a real Core error still returns the typed `OperatorError`; `shutdown_error_is_already_gone` stays the only `Active` escape hatch.
  3. Unit tests define the strict Active-to-Exited reconciliation contract (Phase 3, item 4), extending the existing shutdown unit family in `src/daemon_transport.rs`.

### Phase 3 -- strict-contract proofs (ticket-required)

1. `external_hub_webrtc_live_output_preserves_exact_bytes`: keep the byte-exactness proof; remove the 10-second observed-exit wait and `ReadScreen` pump; restore the blind `ShutdownSession` immediately after the byte proof and peer close; assert `shutdown.kind` is `Events` or `SessionCleanup` with `outcome == "already_exited"` when cleanup, and never `OperatorError`, with the full typed error body in the panic message. Keep the Absent-leg `unknown_session` probe and the worker reap.
2. `unix_shutdown_session_from_another_connection_classifies_attached_exit`: keep the blind-call strict shape; extend the `assert_ne!` to print the full typed error body on failure.
3. Cross-repo W1/W2 mechanism proof is owned by Core pin `d981bb03`, not a Hub parent wrapper. Cite `drain_output_delivers_process_exited_while_worker_holds_stdout_open` and `drain_output_delivers_process_exited_when_worker_exits_nonzero` in `crates/botster-core/tests/local_session_worker_process_test.rs`. Do not add ignored Hub wrapper tests.
4. Unit strict-contract tests in `src/daemon_transport.rs`: recover-fallback legs (recorded `Exited` -> `already_exited`; recorded `Stale` -> `stale_session`; recorded `Stopping` -> `already_exited`; recorded `Running` -> original typed error preserved; fallback failure -> original typed error preserved). Update `shutdown_active_runtime_error_remains_operator_error` and `shutdown_active_state_error_remains_operator_error` in the same strict-contract change so they pin the corrected boundary: `OperatorError` is preserved exactly when exact evidence shows the worker remains truly active or stuck, and never survives provable natural exit. The remaining shutdown unit family stays green.
5. Do not require five repeated idempotency rounds. The WebRTC held-producer exact-bytes test is the one deterministic blind `ShutdownSession` proof.
6. Red-on-revert: with the exact-session reconciliation removed (Rule A: revert the Hub reconciliation; Rule B: additionally run the W1 forced-window test against the pre-fix pinned Core), the W1 forced-window test must fail with the transient `OperatorError`, and the recover-fallback unit tests must fail. Revert restored and proven green afterward.
7. `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` stays green unmodified: true errors and sibling survival are preserved.

Ordering: Phase 1 lands first and its captures gate Phase 2. No production edit before the diagnosis captures are recorded.

## Non-scope

- No new wall-clock delay, retry count, or suite-load-timing correctness mechanism anywhere (ticket requirement).
- Production budgets (Core 2 s shutdown deadline, worker 500 ms grace, Hub observe-turn bounds) stay unchanged unless a Phase 1 capture proves a budget is the defect; that finding routes through Rule B, not a silent edit.
- No changes to `botster-hub-client` DTOs or the wire contract: `Events`, `SessionCleanup`, and `OperatorError` kinds already exist.
- Do not absorb `ticket_1786938984_190098` (ready_spawn budget flake owner; depends on this ticket), `ticket_1786937228_425608`, or `ticket_1786913892_208903`. `ticket_1787004132_469467` stays closed as duplicate; its Unix proof is check 2 here.
- Do not modify the true-error sibling-survival test's contract.
- Do not edit Core in this worktree; Core changes go through the Rule B dependency ticket only.
- Do not create a pull request (merge policy is direct).

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon control plane, `ShutdownSession` classification and recovery, the lifecycle tests, and the host response contract. Core owns worker exit-evidence mechanics (`drain_output`'s completion gate), the shutdown deadline loop, the managed-runtime rollback, and `observe_session_lifecycle` semantics. The predicted Rule B fix (payload delivery must not gate on worker reap timing or exit status) is a policy-free mechanism change and belongs to Core; Hub must not fork or emulate it with timing.

Cross-repo dependency: none registered at plan time. Rule B creates, registers, and starts exactly one blocking `botster-core` dependency ticket (workflow above) if and only if the deterministic captures confirm the Core-internal evidence gap. After the Core merge, this ticket repins and completes; the repin is recorded, not silent.

Downstream blocker (correcting the prior plan's false claim): authoritative reads showed no dependency edge existed, so this Plan visit registered it: `dependency_1787014444_456296` -- `ticket_1786938984_190098` depends on `ticket_1786977409_499180` -- verified through `project_pipelines_list_ticket_dependencies` (depends_on_status open). Containment for the already-active downstream Implement step: Project Pipelines gates step activation on open ticket dependencies ([[project pipeline step activation gates open ticket dependencies before side effects]]), so the downstream run cannot activate its next step past the now-registered open blocker; within its currently active step, the downstream worktree consumes only merged `origin/main`, which cannot contain this ticket's unmerged branch, and its own plan binds the final clean integration-suite proof to after this ticket merges. Verify enforcement per run rather than assuming it (memory note: dependency enforcement is version-dependent).

## Assumptions and unknowns

- Assumption: the sub-case decomposition above is correct at Core pin `fc541a5`. Basis: direct code reading with citations. Phase 1 validates every sub-case with captures before any production edit.
- Assumption: a wrapper script through `core_engine.session_worker_path` is admissible on the production spawn path. Basis: the config surface exists and the isolated hub builder exposes `.session_worker_bin(...)`. Risk: worker-census helpers in `tests/hub_daemon_lifecycle/process.rs` match the worker executable path; the wrapper may need census-helper awareness (test-only change). Implement verifies both before building on the fixture.
- Unknown: which sub-case fired in the original suite failure and in Verify pair-run 21 (W1 reap delay vs W2 non-success suppression vs lost payload). The captures classify them; fixing the class does not require attributing the historical instance.
- Unknown: the exact `HubClientError` kind/message for the shutdown flush failure vs a drain failure. Phase 1 captures both.
- Boundary stated explicitly: a worker that dies without ever writing a `ProcessExited` payload (lost-payload/SIGKILL) is not "ProcessExited in flight". The strict contract does not cover it; it keeps true-error or stale semantics. The sibling-survival test pins this boundary.

## Affected surfaces/files

- `src/daemon_transport.rs` -- recover-fallback reconciliation (Hub leg), new and extended shutdown unit tests.
- `src/runtime.rs` -- only if the non-draining recorded-truth read needs a narrow accessor; prefer the existing registry-backed read paths.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` -- restored blind exact-bytes oracle; tightened idempotency sibling; W1 forced-window test (if placed here).
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- error-body diagnosis on the Unix strict assert.
- `tests/hub_daemon_lifecycle/sessions.rs` -- W2 forced-window test (if placed here).
- `tests/hub_daemon_lifecycle/session_fixtures.rs`, `package_fixtures.rs`, `process.rs` -- wrapper-worker fixture and census awareness (test-only).
- `Cargo.toml` / `Cargo.lock` -- only under Rule B, the recorded Core repin after the dependency merges.
- `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` -- this plan.
- `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` -- Implement report.

## Risks

- Rule B blocks this run on a Core dependency and a repin. That is schedule risk the ticket explicitly authorizes; the alternative (Hub-side timing heuristics) is forbidden by the ticket.
- The wrapper fixture could interact with worker-census reap helpers (wrapper pid vs worker pid). Mitigation: census awareness is a test-only adjustment verified in Phase 1 before anything builds on it.
- Tightening the idempotency sibling's blind branch converts previously-admitted outcomes into failures anywhere the product still errs. Intended tripwire; the deterministic forced-window tests must be green first, so ordering inside Implement is: fix, forced-window green, then tighten oracles.
- A Core fix under Rule B could change `Events` vs `SessionCleanup` frequency in existing lifecycle tests. Mitigation: the strict oracles accept both; any other test that pins one kind gets exact-evidence attribution, not a blanket waiver.
- The known ready_spawn co-flake (`ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice`) stays owned by `ticket_1786938984_190098`. This leaf ticket runs no full lifecycle suite, so the co-flake cannot gate here; if it appears in any targeted run, record attribution evidence and do not absorb.
- Repin under Rule B pulls in unrelated Core main changes. Mitigation: record the pin delta; if the delta breaks unrelated Hub tests, route with exact evidence to new tickets rather than absorbing.

## Acceptance checks/tests

All commands run in the ticket worktree with the repo wrapper `./test.sh` (asset-sync check, `BOTSTER_ENV=test`). Direct `cargo test` does not satisfy these gates. Prebuild before daemon suites: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.

1. Phase 1 captures recorded in the Implement report: W1 and W2 on both transports, each with verbatim `error.code`, `error.operation`, `error.message`, and the recover-path classification. The decision-gate outcome (Rule A or Rule B) is stated with the capture lines that decided it.
2. Unix strict proof: one focused run of `unix_shutdown_session_from_another_connection_classifies_attached_exit` with attach, print-release, live-byte receipt, exit-release, and `process_exit` before blind `ShutdownSession`. Sleep duration is not the oracle.
3. WebRTC strict proof: one focused run of `external_hub_webrtc_live_output_preserves_exact_bytes` with the held-producer release and restored blind oracle.
4. Cross-repo W1/W2 mechanism proof: Core `d981bb03` tests `drain_output_delivers_process_exited_while_worker_holds_stdout_open` and `drain_output_delivers_process_exited_when_worker_exits_nonzero`. No Hub wrapper tests.
5. Unit strict-contract tests: exact `--lib` filter listing the recover-fallback legs (Exited, Stale, Stopping, Running-preserves-error, fallback-failure-preserves-error) plus the existing seven shutdown unit tests -- all pass; filters and counts recorded.
6. Red-on-revert (ticket-required): with the exact-session reconciliation removed, the W1 forced-window test fails with the transient `OperatorError` and the recover-fallback unit tests fail; the revert is restored and a green re-run recorded. Under Rule B, the W1 red-proof against pre-fix Core is the recorded Phase 1 capture itself.
7. Charter and teardown binding checks, deterministic: `shutdown_after_observed_exit_returns_session_cleanup`, `shutdown_session_classifies_parked_exit_beyond_one_baseline_page`, the Absent probe inside check 3, the seven-test stale-peer `--lib` filter from the prior plan, and `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` (unmodified) -- all pass with recorded filters.
8. No five-round idempotency gate. Check 3 is the one held-producer WebRTC proof.
9. Strict Rust gates: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings`.
10. No full lifecycle suite runs as this leaf ticket's gate (audit ratification correction, superseding the earlier "one exclusive full-suite run" line in the ticket body). The harness ticket and final integration own controlled full-suite smoke tests. This ticket's gates are the focused deterministic checks 2-9 only.
11. Implement report at `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` records: captures, the decision-gate outcome, the Rule B dependency ticket id and repin delta (if fired), all filters and tallies, red-on-revert output, and any attribution evidence.

Downstream proof: the production entry points are the Unix and WebRTC `ShutdownSession` request paths through `src/daemon_transport.rs:3401`; checks 2-4 drive them live on both transports with real spawned workers. No DTO or public-surface change; no live-Hub admission/supervision/package proof class is touched. Under Rule B the repin is proven by the forced-window tests running against the new locked Core.

## Vault gaps worth capturing

- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]: the exact-bytes/idempotency instance in that note is superseded -- the product decision changed the codified host contract itself. The note needs the instance updated and a pointer to the strict natural-exit ShutdownSession contract.
- [[host ShutdownSession classification must call the exact-session Core query]] still says the convention is not shipped; Hub main ships it. Stale shipped-status (carried over from the prior run, still uncaptured).
- New capture candidate after the decision gate resolves: "worker ProcessExited delivery must not gate on worker reap timing or worker exit status" (Core contract) and "ShutdownSession strict natural-exit idempotency is Events-or-SessionCleanup on every transport" (Hub contract).
- New capture candidate: a Hub parent of `botster-session-worker` cannot satisfy Core welcome `worker_pid` / readiness identity, so delayed-reap and nonzero-worker-exit windows stay Core-owned.

## Implement steps

1. Prebuild the worker binary; confirm hygiene (gitignore, no-colon path).
2. Do not build a Hub parent-wrapper fixture. Core identity rejects it.
3. Run Phase 1 captures on both transports; record verbatim; validate the sub-case citations.
4. Resolve the decision gate. Rule B: register the `botster-core` dependency ticket with the reproduction and required contract, then block until the Core merge and repin. Rule A: proceed directly.
5. Implement the Hub legs (recover fallback, unit tests). Under Rule B, integrate the repinned Core.
6. Restore the blind exact-bytes oracle and the Unix attach / print-release / exit-release / `process_exit` path. Cite Core W1/W2 tests. Do not add ignored Hub wrapper tests.
7. Run focused Unix and WebRTC proofs, recover and Active units, fmt, and clippy. Run no full lifecycle suite.
8. Write the Implement report; commit; no PR.
