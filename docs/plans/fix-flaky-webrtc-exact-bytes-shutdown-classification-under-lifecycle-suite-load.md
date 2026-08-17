# Plan: Fix external_hub_webrtc_live_output_preserves_exact_bytes suite-load ShutdownSession OperatorError

Ticket: `ticket_1786977409_499180`
Run: `run_1786977413_341616` (parent run `run_1786944939_873939`, the Implement binding that discovered this failure)
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)
Revision: addresses Plan Review `review_1786979254_603314` (complete in-plan late-message matrix and sibling proof; ready_spawn suite-failure attribution to its existing owner `ticket_1786938984_190098`; binding charter Found/Absent/Err checks with exact test filters). Plan Review's independent base verification also reproduced this ticket's failure pre-change (217 passed, 2 failed, 1 ignored: this test plus the sibling-owned `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` at 50.919583 ms).

The parent Implement binding ran `./test.sh --locked --test hub_daemon_lifecycle_test` after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`. The suite exited 101 with 218 passed, 1 failed, 1 ignored in 341.37s. The one failure was `external_hub_webrtc_live_output_preserves_exact_bytes` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:423`): `shutdown should complete the write(2) session, got OperatorError`. The same test passed isolated (exit 0, 3.45s). The test file is byte-identical to `origin/main`, and Plan Review on the parent worktree ran the same suite command green (219 passed). This ticket repairs the suite-load ShutdownSession oracle or the production classify path it protects, per the advisor disposition in `question_1786977344_650479`.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the ticket record through `list_spawn_targets` to `trybotster/botster-hub`. The run record carries the same target_id.
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786977409_499180`, clean at `c71e22d`. The branch base includes the parent-run lineage commits (local WebRTC lib-flake plan chain); this plan adds its own document and does not touch those files.
- The worktree path contains no colon. A `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (5 lines). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon control plane, `ShutdownSession` classification, and this lifecycle test. Charter gate directly in scope: "For `ShutdownSession`, prove exact-session `Found`, `Absent`, and `Err` behavior. Reject `Drain`, baseline, or capped-page classification."

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay: completion evidence, worktree hygiene, runtime-teardown class trigger.
- [[botster-architecture]] and [[cli-patterns]] -- mandatory Must Load context; nothing moves this work outside Hub.
- [[botster runtime teardown lenses]] -- the class applies; answers below.
- [[host ShutdownSession classification must call the exact-session Core query]] -- the convention this oracle protects. The note still says "not shipped"; Hub main now ships it (`classify_shutdown_session`, `src/daemon_transport.rs:4679`, calling `HubRuntime::observe_session_lifecycle`, `src/runtime.rs:3539`). Vault gap below.
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] and [[process-global test counters make zero waits observe other tests under default-concurrency lib load]] -- the sibling suite-load flake class and its repair discipline: repair the oracle, never the production budget, unless evidence proves the budget is the defect.
- [[conformance harnesses gate on deterministic invariants not timing]] -- the repaired oracle must gate on a deterministic invariant, with bounded waits only as patience for progress.
- [[a regression test must be shown to go red with the fix reverted]] -- ablation red-proof requirement.
- [[hub shutdown preserves durable session workers]] -- explicit session shutdown owns durable-worker teardown; this repair must not blur Hub-process shutdown evidence with session cleanup evidence.
- Prior art: `docs/plans/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite.md` (this branch's lineage) -- the flake-repair plan format, wrapper-only acceptance commands, one-root-per-ticket rule.

## Context loaded

- Ticket, run, gates, empty prior artifacts/checklists via `project_pipelines_current_context`.
- Hub code read: `src/daemon_transport.rs` `ShutdownSession` arm (`:3401-3446`), `classify_shutdown_session` (`:4679-4694`), `classify_found_session_lifecycle` (`:4697-4738`), `recover_after_core_shutdown_error` (`:4485-4494`), `shutdown_error_response` (`:4652-4674`), `daemon_unknown_session_cleanup` (`:6214`, kind `OperatorError`, code `unknown_session`), `daemon_operator_error` (`:6226`), `observe_lifecycle_turn` (`:4952`, bounded 32 sessions / 64 KiB / 25 ms), `ListSessions` arm (`:3094`).
- Core code read at the locked pin `fc541a5` (`Cargo.toml` git rev; `~/.cargo/git/checkouts/botster-core-ea2698e4cbd07384/fc541a5`): `CoreDaemon::observe_session_lifecycle` (`daemon.rs:807`), `CoreDaemon::shutdown_session` (`daemon.rs:1614-1690`; 2-second exit-confirmation deadline loop; a non-`session_not_found` drain error aborts the loop immediately), `observe_session` (`daemon.rs:2487`), `lifecycle_record` (`daemon.rs:1951`, engine-live lifecycle), `ManagedSessionRuntime::shutdown_session` (`managed_session_runtime.rs:929-967`; on runtime-input flush failure it rolls the lifecycle back to the previous state and returns the error), worker transport constants (`local_process.rs:23`, `DEFAULT_SHUTDOWN_GRACE` 500 ms).
- Test code read: `external_hub_webrtc_live_output_preserves_exact_bytes` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:261-427`), sibling `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (`:430-634`), `daemon_test_guard` (`tests/hub_daemon_lifecycle/common.rs:411`; the suite serializes guard-holding tests), `write_python_wait_then_write_script` (`tests/hub_daemon_lifecycle/package_fixtures.rs:1254`; the producer writes 4 bytes then exits), `start_isolated_live_output_hub` (`tests/hub_daemon_lifecycle/session_fixtures.rs:362`; external hub process plus explicit worker binary).
- `test.sh` -- repo wrapper: asset-sync check, `BOTSTER_ENV=test`, `cargo test --workspace`; targeted `--test` forms keep working.
- `project_pipelines_current_context` shows no open questions and no existing checklists for this ticket or run.

## Failure mechanism

The test proves byte-exact live WebRTC output, closes the peer, then immediately calls `ShutdownSession` over the Unix control endpoint and requires `Events` or `SessionCleanup` (`webrtc_proofs.rs:407-426`). The producer script writes its 4 bytes and exits, but the test breaks out of its read loop the moment the bytes match. At `ShutdownSession` time the python process exit, PTY EOF, worker `ProcessExited` report, and engine drain observation are all typically still in flight. The exact-session classify (`classify_shutdown_session`) then legitimately returns `Active`, and Hub drives the real Core shutdown.

From code inspection, every path from that point that yields a `DaemonResponseKind::OperatorError` requires a Core shutdown error plus a recover re-classify that does not land on `Cleanup`/`Stopping`/`Missing`-with-`UnknownSession`:

1. **Runtime-input flush failure with lifecycle rollback.** `ManagedSessionRuntime::shutdown_session` rolls the lifecycle back to `Running` when the post-shutdown input flush to the worker fails (`managed_session_runtime.rs:955-960`). If the exit is then not observed within Core's 2-second deadline loop, Core returns the engine error, Hub re-classifies `Active`, and `shutdown_error_response` returns the typed error (`daemon_transport.rs:4672`) -- an `OperatorError` frame.
2. **Drain error aborting the deadline loop.** Inside Core's 2-second loop, any drain error that is not `session_not_found` returns immediately (`daemon.rs:1647-1651`). A transient worker-socket error while the worker is simultaneously tearing down the PTY produces a fast Core error; the single instantaneous Hub re-classify then decides everything.
3. **Recover classify error.** `recover_after_core_shutdown_error` propagates its own classify error with `?` (`daemon_transport.rs:4492`); a transient drain or registry error during recovery becomes the response.
4. **Genuine 2-second starvation.** Under a 341-second suite run, the worker can fail to confirm an already-dead child within Core's deadline. This is a production budget; per the ticket, it stays unchanged unless proven to be the defect.

Which sub-path fired in the recorded failure is unknown, because the assert prints only `shutdown.kind` and drops the `DaemonOperatorError` body (code, operation, message) that names the path.

The decisive inspection fact is a contract divergence inside the same file. The sibling test `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` codifies the host contract for exactly this situation (`webrtc_proofs.rs:576-631`): it first polls `ListSessions` for `lifecycle == "exited"`, and only when exit was observed does it require `SessionCleanup` with outcome `already_exited`; when exit was not yet observed, it explicitly admits a typed `OperatorError` (`runtime_error` or `state_error`, operation `shutdown`) as a legal outcome of a blind call. That sibling passed all 5 rounds in the same failing suite run. The exact-bytes oracle demands strictly more than this codified contract -- first-call completion with no observed-exit precondition -- so under suite load it can fail on behavior the same file defines as correct and fail-closed.

This is a test-oracle contract defect, not a proven production classify defect. The production exact-session classify path (`observe_session_lifecycle` wiring) is present, charter-conformant, and stays unchanged. Production budgets (Core's 2-second shutdown deadline, worker 500 ms grace, Hub's bounded observe turn) stay unchanged; no evidence indicts them.

## Runtime-teardown lens answers

`teardown_class_applies`: yes. The subject is `ShutdownSession` classification for a worker-backed session raced against SessionIo/worker exit observation -- terminal-state vs live-runtime divergence. The repair changes only the test oracle; every production teardown path stays untouched, and the answers below record what the repaired test must keep proving.

`teardown_isolation`: production unchanged -- `ShutdownSession` targets exactly one session; classify uses the exact-session Core query, so one session's shutdown cannot classify through another session's state. The isolated hub owns a private data directory, endpoint, and worker set, so the repair cannot couple this test to sibling tests' hubs.

`teardown_bounds`: production bounds unchanged -- Core's shutdown wait stays at its 2-second deadline with a typed `ShutdownFailed` error on expiry; Hub's recover path stays a single bounded re-observe (25 ms observe turn plus one exact-session query); no new control-plane wait is added on the Hub owner path. Test-side: the new observed-exit wait is a bounded client-side poll (10 s of 50 ms `ListSessions` polls) that fails the test with diagnosis on expiry -- patience for progress, not a production latency claim.

`late_message_matrix`: the repair adds no ownership-creating message surface, and no row's behavior changes. The complete matrix for every ownership-creating message on the control plane this test exercises (Unix control endpoint plus the encrypted WebRTC terminal adapter of one isolated external hub), with the exact tests that prove each column:

| Message | Ownership effect | Owner tag | Rejection after terminal failure | Race sweep / replacement protection |
| --- | --- | --- | --- | --- |
| `Spawn` (Unix control) | Creates a durable session worker that survives peer loss and Hub restart | `session_id` under host policy (durable by design, per [[hub shutdown preserves durable session workers]]) | Duplicate spawn is rejected without destroying the existing owner's session: `external_hub_client_duplicate_botster_web_runtime_spawn_is_rejected_without_cleanup` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:1756`); invalid spawn payload returns a typed operator error without dropping the transport, per [[incomplete repo local session types drop the hub client connection]] | Durable workers are intentionally not swept by peer or connection loss; explicit `ShutdownSession`/`RemoveSession` own teardown -- this ticket's repaired oracle plus `shutdown_after_observed_exit_returns_session_cleanup` (`tests/hub_daemon_lifecycle/sessions.rs:3240`) prove the explicit path |
| `IssueLocalWebrtcBootstrap` + `LocalWebrtcSignal` (Unix control) | Creates grant-owned peer and signaling state | `grant_id` / `grant_secret` bound to the admitted package origin | Wrong origin or grant is a typed rejection at signal time (bootstrap issuance is origin-bound, per [[webrtc bootstrap origin must be requested after the package server binds]]) | Peer close detaches every peer-owned terminal subscription together: `local_webrtc_peer_close_detaches_terminal_subscriptions` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:1499`) |
| Encrypted WebRTC `Hello` | Creates the WebRTC terminal admission entry | keyed by `grant_id` (connection-scoped admission) | An unnegotiated adapter never receives or decodes daemon events: `webrtc_terminal_adapter_unnegotiated_adapter_never_receives_or_decodes_daemon_event` (`tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:969`); admission requires the encrypted DataChannel Hello, per [[WebRTC terminal admission requires an encrypted DataChannel Hello]] | Admission entry retires with the peer (same peer-close sweep row above) |
| `Attach` (encrypted WebRTC) | Creates terminal attach route ownership and lifecycle counts | grant plus `subscription_id` (route identity) | Late Attach after peer close does not recreate the route: `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route` (`tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:684`) | Bound peer loss closes the adapter without a Hub-invented Detach: `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach` (`:527`); a stale-generation close cannot sweep a replacement owner: `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` (`:1150`); explicit Detach stays separate from peer loss: `webrtc_terminal_adapter_explicit_detach_is_separate_from_peer_loss` (`:614`) |
| `ShutdownSession` (Unix control; this ticket's message) | Destroys ownership; creates none | exact `session_id` | After completion the call is idempotent (`already_exited` cleanup) and a miss is the typed `unknown_session` frame -- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (`tests/hub_daemon_lifecycle/webrtc_proofs.rs:430`) plus this plan's Absent-leg probe (Scope item 5) | Close-event suppression on explicit shutdown is proven by `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event` (`tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:1022`) |

`ReadScreen`, `ListSessions`, `Status`, and `StartPackageEntrypoint` create no per-peer durable ownership rows on this plane (`StartPackageEntrypoint` starts the supervised package entrypoint, owned by package supervision, outside this ticket's message scope and untouched). No other peer-originated message class in this test's design creates durable ownership. None of these tests is modified by this repair.

`production_path_proof`: preserved and sharpened. The repaired oracle still drives the full production path: real spawned worker process -> live PTY output over the encrypted WebRTC adapter -> worker exit -> Core lifecycle observation -> `ListSessions` projection reports `exited` -> `ShutdownSession` -> `classify_shutdown_session` -> exact-session `Found`/`Exited` -> `SessionCleanup{already_exited}` without any worker interaction. Requiring `SessionCleanup` with outcome `already_exited` (instead of accepting `Events` too) makes the test fail if classification regresses to `Active` for an observed-exited session -- the exact regression the charter's "reject Drain/baseline/capped-page classification" gate exists to catch. Control A below is the red-on-revert proof that the oracle pins this classify path.

`ownership_identity`: unchanged -- sessions are keyed by exact `session_id`; classify queries exactly that id; `Exited` is a terminal lifecycle state, so the observed-exit precondition cannot be invalidated between the poll and the shutdown call. No reused-id hazard is introduced: the test uses one unique session id in one isolated hub.

`sibling_fail_closed_policy`: unchanged, and bound to named checks. On success: cleanup of one session leaves the hub, the control connection, and other owners fully serviceable -- binding proofs are `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` (`webrtc_proofs.rs:430`; five sequential session rounds on one hub, each cleanup leaving the hub serving the next), `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames` (`webrtc_terminal_adapter.rs:349`; per-owner frame isolation), and `cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive` (`tests/hub_daemon_lifecycle/sessions.rs:640` region; a typed error frame leaves the connection and daemon responsive, `Status` verified after each error). On ultimate teardown failure: the bounded policy is that Hub returns the typed `OperatorError` for that one request, keeps the adapters open (`daemon_transport.rs:3406-3408`), suppresses duplicate close events, and sacrifices no sibling session or connection -- the blast radius is one request's reply. Binding proofs: `shutdown_active_runtime_error_remains_operator_error` and `shutdown_active_state_error_remains_operator_error` (`src/daemon_transport.rs:7958`, `:7984`; the typed error frame with codes `runtime_error`/`state_error`, operation `shutdown`), and `webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close` (`webrtc_terminal_adapter.rs:1097`; a failed teardown request does not corrupt later close delivery). These tests are unmodified; acceptance check 5 makes the deterministic ones binding through exact filters.

## Scope

Repair the suite-load oracle in `external_hub_webrtc_live_output_preserves_exact_bytes` so it proves the production exact-session classify path deterministically under suite load, instead of demanding first-call completion for a blind shutdown that the same file's codified contract allows to fail typed. All changes are test-only; compiled production code stays byte-identical.

1. After the byte-exactness proof and peer close, add a bounded observed-exit wait: poll `ListSessions` until the session reports `lifecycle == "exited"` (the idempotency sibling's idiom at `webrtc_proofs.rs:576-593`, extracted into a small shared helper or inlined -- Implement picks the smaller diff). Bound: 10 seconds of 50 ms polls. Empirical baseline: the sibling's 2-second window succeeded 5/5 rounds in the failing suite run; 10 s is patience headroom, not a production budget. On expiry, panic with the last observed lifecycle value for the session.
2. Replace the `Events | SessionCleanup` assert with the sharp deterministic contract: `shutdown.kind == SessionCleanup`, `cleanup.outcome == "already_exited"`, `cleanup.session_id` matches. The panic message must include both `shutdown.kind` and the full `shutdown.error` body so any future failure names its sub-path (repairing the diagnosis gap that left this ticket without an error code).
3. Rewrite the oracle comment (`webrtc_proofs.rs:407-409`) to state the repaired claim: under `./test.sh --locked` suite load, once `ListSessions` reports the finite producer exited, exact-session classification must return `SessionCleanup{already_exited}`; the blind-call typed-`OperatorError` contract is owned by `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`.
4. Keep everything else in the test unchanged: the byte-exactness proof, the U+FFFD asserts, spawn/attach/hello flow, the final `hub.shutdown()`.
5. Absent-leg probe (Plan Review-required charter binding): after the `SessionCleanup{already_exited}` assert, issue one `ShutdownSession` for a never-spawned session id (for example `webrtc-exact-bytes-missing`) on the same hub and assert the typed miss: `DaemonResponseKind::OperatorError` with `error.code == "unknown_session"` and `error.operation == "shutdown"` (the `Missing -> daemon_unknown_session_cleanup` path, `src/daemon_transport.rs:3410-3413`, `:6214`). This is the smallest deterministic live proof of the charter's `Absent` leg; no separate test is added.
6. Contingency, evidence-gated: if during this ticket's acceptance runs the repaired test fails -- exit never observed within 10 s, or an observed-exited session does not classify to `SessionCleanup{already_exited}` -- that failure is live evidence of a production defect. Record the exact output in the Implement report, then: a Hub-owned classify or projection defect is fixed in this ticket with the smallest surgical change; a Core-owned defect (lifecycle rollback erasing `Stopping`, drain-error abort semantics, or the 2-second deadline itself) is registered as a blocking dependency ticket against the `botster-core` target, not silently patched or repinned here.

Prefer this repair over quarantine. Use `#[ignore]` or a skip only if the repaired test still fails a suite run, and only with an Implement report that names the remaining mechanism. Do not start from quarantine.

## Non-scope

- No production (non-test) behavior change: `classify_shutdown_session`, `recover_after_core_shutdown_error`, `shutdown_error_response`, the `Missing -> unknown_session` typed miss, Core's 2-second shutdown deadline, worker grace, and Hub observe-turn budgets all stay untouched (the contingency in Scope item 6 is the only, evidence-gated, exception on the Hub side).
- Do not modify `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`; its blind-call contract is correct and green.
- Do not absorb `ticket_1786938984_190098` (parent Implement binding, blocked on this ticket), `ticket_1786937228_425608` (unix_adapter lifecycle failure -- that test passed in this run), or `ticket_1786913892_208903` (write-budget sibling).
- Do not repair, re-own, or open a new ticket for `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice`. Plan Review's independent base run reproduced it (50.919583 ms) alongside this ticket's failure; `ticket_1786938984_190098` already owns the ready_spawn work and already depends on this ticket, so a new ticket would duplicate ownership and a dependency here would create a cycle. Acceptance check 4 defines the attribution evidence when it fails during this ticket's suite runs; the final clean integration-suite proof stays bound in that owner ticket.
- No changes to `botster-hub-client` DTOs, hub-test-support, packages, pins, or lockfiles. The `ShutdownSession` wire contract is unchanged.
- Do not create a pull request (merge policy is direct).

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon control plane, `ShutdownSession` classification, this lifecycle test, and the oracle. The work stays in Hub test code (`tests/hub_daemon_lifecycle/webrtc_proofs.rs`, plus a fixture helper file if extracted).

Core (pinned `fc541a5`) owns `observe_session_lifecycle`, the shutdown deadline loop, the managed-runtime rollback, and worker exit observation. No Core change is planned. If the Scope item 5 contingency proves a Core defect, register a blocking dependency ticket against the `botster-core` target (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) instead of broadening this run.

This ticket is a blocking dependency of `ticket_1786938984_190098` (registered per the advisor disposition; the parent Implement resumes after this merges). Verify enforcement per run rather than assuming it, per the memory note on version-dependent dependency enforcement.

## Assumptions and unknowns

- Assumption: the recorded failure came from one of the four enumerated blind-call sub-paths. The repair is mechanism-complete for the class because it removes the blind call entirely: after observed exit, the classify path returns early with `Cleanup` and never touches the worker, the deadline loop, or the recover path. Distinguishing which sub-path fired in the original record is not required for this repair, and the sharpened panic message makes any future occurrence self-identifying.
- Assumption: `ListSessions` `lifecycle == "exited"` and `classify_found_session_lifecycle`'s `complete_registry`/`complete_lifecycle` predicates read the same reconciled registry/engine state, so observed-exit implies `Cleanup` classification deterministically. Empirical support: the idempotency sibling asserts exactly this implication and passed 5/5 rounds in the failing suite run. Implement verifies the projection source once while wiring the helper; if the projection reads a different store, the wait predicate moves to the store classify reads.
- Assumption: lifecycle observation progresses while the test merely polls `ListSessions` (owner-loop maintenance advances the journal without a terminal client). The sibling's passing 2-second window under the same suite load supports this.
- Resolved by Plan Review: the failure reproduces pre-change. The reviewer's independent base verification of this worktree's lineage ran the same suite command and got 217 passed, 2 failed, 1 ignored, with this ticket's test among the failures. Implement's bounded pre-change probes (acceptance check 2) become corroborating-only; non-reproduction in those probes does not weaken the already-recorded reproduction.
- Known co-flake with an existing owner: `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` failed in the same reviewer base run (50.919583 ms). `ticket_1786938984_190098` owns it and depends on this ticket. Acceptance check 4 defines its attribution handling; it is not this ticket's defect and gets no new ticket.
- Unknown: whether some other, previously unrecorded lifecycle test flakes during acceptance runs. If one does, record exact evidence and register a new ticket. Do not absorb.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` -- the observed-exit wait, the sharpened shutdown assert with error-body diagnosis, the Absent-leg `unknown_session` probe, and the rewritten oracle comment in `external_hub_webrtc_live_output_preserves_exact_bytes` only.
- `tests/hub_daemon_lifecycle/session_fixtures.rs` (optional) -- the extracted `ListSessions` exit-wait helper, if extraction beats inlining.
- `docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md` -- this plan.
- `docs/reports/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load-implement.md` -- Implement report (Implement step).

No compiled production code changes. No dependency or lockfile changes.

## Risks

- The sharpened `SessionCleanup{already_exited}`-only assert could over-constrain if an observed-exited session can legally classify another way. Mitigation: `Exited` is terminal; classify maps registry `Exited` to `Cleanup{already_exited}` and a `failed`/`stale` lifecycle would not satisfy the `"exited"` wait predicate. The assumption above pins the projection-source check in Implement.
- The 10-second exit-wait is a new bounded patience wait and could itself expire under extreme load, converting a latent stall into a test failure. That is intended: an exit-observation stall beyond 10 s with a serialized suite is a real defect signal, and Scope item 6 routes it with evidence instead of hiding it.
- Losing the blind-call coverage from this test. Mitigation: the idempotency sibling explicitly owns and keeps exercising blind calls (its `observed_exit == false` branch), so the file-level coverage of both contracts is preserved and no longer contradictory.
- The known ready_spawn co-flake can fail suite runs in acceptance check 4. Mitigation: check 4's attribution rule routes exactly that named failure to its existing owner `ticket_1786938984_190098` with recorded evidence, without waiving this ticket's own test and without a new ticket or a dependency cycle.
- The suite may flake on some other, previously unrecorded test during acceptance runs. Follow prior art: exact evidence, new ticket, no absorption.

## Acceptance checks/tests

All commands run in the ticket worktree. Suite commands use the Hub wrapper `./test.sh` (asset-sync check, `BOTSTER_ENV=test`). Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Pre-change reproduction probe (bounded, corroborating only): up to 2 runs of `./test.sh --locked --test hub_daemon_lifecycle_test` on the unchanged worktree. Record outcomes; non-reproduction does not gate the repair.
3. Targeted repetition: `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_live_output_preserves_exact_bytes` passes 10 consecutive runs (shell loop, record tallies). This binds the repaired oracle including the Found/Exited leg (`SessionCleanup{already_exited}`) and the Absent leg (`unknown_session` probe, Scope item 5).
4. Binding suite gate with owner-routed attribution: 3 consecutive runs of `./test.sh --locked --test hub_daemon_lifecycle_test` at default concurrency (the exact command that produced the ticket failure). A run counts as passing when `external_hub_webrtc_live_output_preserves_exact_bytes` passes and the run has no failure other than `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice`. If that named test fails, record the attribution evidence in the Implement report -- exact test name, panic text with the observed elapsed value, and the run tally -- and attribute it to its existing owner `ticket_1786938984_190098` (which depends on this ticket and keeps the final clean integration-suite proof); do not create a new ticket for it and do not add a dependency on it here. Any other failure fails the gate for this run and is routed per the assumptions section.
5. Binding Hub charter checks for `ShutdownSession` `Found`, `Absent`, and `Err`, all deterministic, exact wrapper commands and filters:
   - Found/Exited, live daemon: `./test.sh --locked --test hub_daemon_lifecycle_test shutdown_after_observed_exit_returns_session_cleanup` (1 passed) and `./test.sh --locked --test hub_daemon_lifecycle_test shutdown_session_classifies_parked_exit_beyond_one_baseline_page` (1 passed; the exact-session classify beyond a capped baseline page, the charter's reject-capped-page leg), plus the repaired ticket test from check 3.
   - Absent/unknown-session, live daemon: the Scope item 5 probe inside the repaired ticket test (typed `unknown_session`, operation `shutdown`), bound by check 3.
   - Err Runtime/State, unit: `./test.sh --locked --lib -- production_core_shutdown_error_keeps_active_runtime_as_operator_error production_core_shutdown_error_keeps_active_state_as_operator_error shutdown_active_runtime_error_remains_operator_error shutdown_active_state_error_remains_operator_error shutdown_unknown_session_error_while_active_is_already_exited_cleanup shutdown_exited_classification_returns_cleanup_for_any_shutdown_error shutdown_stopping_record_is_host_cleanup_not_active` (7 passed; typed `runtime_error`/`state_error` frames preserved for `Active`, `already_exited` cleanup for the `UnknownSession` recover, `Cleanup`/`Stopping` classification stability).
   Record each command's exact filter and pass count in the Implement report.
6. Ablation red-proofs, per [[a regression test must be shown to go red with the fix reverted]], both under the targeted wrapper command from check 3, both reverted afterward with the revert verified by a green re-run:
   - Control A (classify-path liveness): temporarily force `classify_shutdown_session` to return `Ok(Active)` unconditionally. The repaired test must fail: the observed-exited session then takes the live Core shutdown path and returns `Events`, violating the `SessionCleanup{already_exited}` assert. This proves the oracle pins the production exact-session classify path, red-on-revert.
   - Control B (exit-wait liveness): temporarily point the observed-exit wait at a nonexistent session id. The wait must expire at its bound and fail the test with the lifecycle diagnosis. This proves the precondition is a live bounded oracle, not a pass-through.
   Record exit codes and failure text for both controls in the Implement report.
7. Strict Rust gates: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
8. Non-binding smoke: one full `./test.sh --locked` run may be reported for information; unrelated-suite outcomes do not bind this ticket.
9. Implement report at `docs/reports/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load-implement.md` records: pre-change probe outcomes, the projection-source verification (assumption 2), the oracle diff summary, both red-proof outputs, the charter-check filters and pass counts (check 5), any ready_spawn attribution evidence (check 4), acceptance tallies, and -- if the contingency fired -- the exact evidence and the routing decision (Hub fix here vs Core dependency ticket).

Downstream proof: not required. No public surface, DTO, pin, or compiled runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched. The charter's `ShutdownSession` gate (exact-session `Found`/`Absent`/`Err`) keeps its existing production coverage, and this repair adds a sharper suite-load regression tripwire on the `Found`/`Exited` leg.

## Vault gaps worth capturing

- Update [[host ShutdownSession classification must call the exact-session Core query]]: it still says "This convention is not shipped behavior yet", but Hub main ships `classify_shutdown_session` over `observe_session_lifecycle`. Stale shipped-status is exactly what [[vault convention notes can document unimplemented behavior as shipped]] warns about, in the opposite direction.
- Capture the oracle-contract idiom this ticket proves: a suite-load oracle must not demand more than the host contract another test in the same file codifies; blind-call fail-closed typed errors and observed-state deterministic completions are two different contracts and need two different oracles. Name the exact-bytes/idempotency pair as the instance.
- Capture the diagnosis idiom: flake oracles over typed response frames must print the full typed error body in the panic message, not only the response kind -- this ticket lost its first failure's sub-path to that gap.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Run the bounded pre-change reproduction probes (acceptance check 2).
3. Verify the `ListSessions` lifecycle projection source against `classify_found_session_lifecycle` (assumption 2); record the finding.
4. Apply Scope items 1-5.
5. Run acceptance checks 3-7. If the contingency (Scope item 6) fires, stop and route with evidence before widening the diff.
6. Write the Implement report.
7. Commit the test repair and report. Do not create a PR.
