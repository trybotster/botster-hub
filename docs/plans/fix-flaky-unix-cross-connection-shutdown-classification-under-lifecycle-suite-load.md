# Plan: Fix flaky unix_shutdown_session_from_another_connection_classifies_attached_exit

Ticket: `ticket_1787004132_469467`
Run: `run_1787004222_247527`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

One granted-slot `./test.sh --locked` run (parent run `run_1786914416_283641`, HEAD `921d525`) failed `unix_shutdown_session_from_another_connection_classifies_attached_exit` at `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1736`. The panic was `assert_ne!(shutdown.kind, OperatorError)`: a `ShutdownSession` issued from a separate connection returned `OperatorError`. The lifecycle suite reported 216 passed, 3 failed, 1 ignored. This ticket owns only this failure.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the ticket record through `list_spawn_targets`, which maps it to `trybotster/botster-hub` at `/Users/jasonconigliari/Projects/botster-hub`.
- Worktree: pipeline-provided ticket worktree, branch `project-pipelines/ticket_1787004132_469467`, clean, at `c71e22d`, which equals current `origin/main` (independently re-verified with `git fetch origin main`).
- The worktree path contains no colon. A `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns the daemon control plane, the Unix terminal adapter, `ShutdownSession` classification policy, and this lifecycle test. The charter's `ShutdownSession` gate rows bind this plan: prove exact-session `Found`, `Absent`, and `Err` behavior; reject `Drain`, baseline, or capped-page classification.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene, runtime-teardown class trigger.
- [[botster-architecture]] -- domain map (Must Load). Hub owns this surface; nothing moves the work outside Hub.
- [[cli-patterns]] -- Rust runtime constraints (Must Load). Binding entry: [[conformance harnesses gate on deterministic invariants not timing]] -- the repaired oracle must be deterministic, and this plan must not raise production or patience budgets speculatively.
- [[host ShutdownSession classification must call the exact-session Core query]] -- the production contract this test protects: classify one session through `observe_session_lifecycle`, return `Found`/`Absent`/`Err`, never classify through Drain, baselines, or capped pages.
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]] -- the diagnosed defect class. A blind `ShutdownSession` before exit observation can legally return typed `OperatorError` (`runtime_error` or `state_error`, operation `shutdown`); the codified repair is: wait for the exit precondition, then assert the deterministic `SessionCleanup { already_exited }` result.
- [[flake oracles over typed response frames must print the full typed error body]] -- the current assertion prints only `shutdown.kind` and loses the classification sub-path; the repaired assertions must print `error.code`, `error.operation`, and `error.message`.
- [[observed-exit waits must issue a production exact-session observe turn]] -- `ListSessions` does not advance lifecycle; a bounded global observe slice can miss the target; production `ReadScreen` must exact-observe its named session. This note records the repair owned by `ticket_1786977409_499180`, which is not yet merged (see the dependency section).
- [[a regression test must be shown to go red with the fix reverted]] -- red-on-revert controls in Acceptance check 5.
- [[suite wide acceptance criteria make every observed test failure in scope]] -- non-target suite failures during acceptance need exact attribution, not dismissal.
- [[live acceptance tests must not depend on a loop tick window]] -- the wait needs a deterministic production observe stimulus inside its bound, not incidental owner-loop ticks.
- [[botster runtime teardown lenses]] -- the class applies (terminal-state vs live-runtime divergence); answers below.
- [[observe-first attached Drain can return SessionLifecycle without ProcessExit]] -- context for the test's final Drain assertion, which stays unchanged.

## Context loaded

- Ticket, run, gates, empty artifacts/checklists via `project_pipelines_current_context`. `project_pipelines_search_tickets` for the test name returns only this ticket; no duplicate exists.
- Sibling ticket record `ticket_1786977409_499180` (open, same target): owns the WebRTC twin of this exact defect (`external_hub_webrtc_live_output_preserves_exact_bytes`, blind `ShutdownSession` after live output returned `OperatorError` under suite load) and owns "the suite-load ShutdownSession OperatorError oracle or the production classify path it protects".
- Code read in this worktree:
  - Failing test `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1681-1775`: finite producer `printf 'pse-ready\n'; exit 0`; live-bytes wait `:1699-1727`; blind cross-connection `ShutdownSession` `:1729-1735`; failing kind-only oracle `:1736-1739`; ListSessions terminal assert `:1741-1754`; Drain no-`ProcessExit` assert `:1755-1773`.
  - Production `ShutdownSession` handler `src/daemon_transport.rs:3401-3444`: `observe_lifecycle_turn` (bounded slice, `:4952-4962`: 32 sessions / 64 KiB / 25 ms) then `classify_shutdown_session` (`:4676-4694`) through `HubRuntime::observe_session_lifecycle` (`src/runtime.rs:3539-3548`, the exact-session Core query); `Active` proceeds to Core `Shutdown`; a Core error runs `recover_after_core_shutdown_error` (`:4485-4494`) which re-observes, re-classifies, and for a still-`Active` classification with a non-`UnknownSession` error returns the original error (`shutdown_error_response`, `:4652-4673`) as a typed `OperatorError`.
  - `classify_found_session_lifecycle` (`:4696-4729`): `Exited` maps to `Cleanup { outcome: "already_exited" }`; `Stale`/`Failed` map to `outcome: "stale_session"`.
  - Production `ReadScreen` handler (`:3468-3484`) at this HEAD runs only the bounded global slice plus Core `read_screen`. It does not issue the exact-session observe that [[observed-exit waits must issue a production exact-session observe turn]] requires; that repair belongs to `ticket_1786977409_499180` and is absent from `origin/main` `c71e22d` (verified by grep: `observe_session_lifecycle` is called only from shutdown classification).
  - Codified sibling contract `tests/hub_daemon_lifecycle/webrtc_proofs.rs:576-630`: observed exit demands deterministic `SessionCleanup` with `outcome == "already_exited"`; a blind call may legally return `OperatorError` with code `runtime_error` or `state_error` and operation `shutdown`.
  - Shared helper `tests/hub_daemon_lifecycle/common.rs:591-612` `shutdown_short_lived_session`: blind call asserting `Events | SessionCleanup`; remaining latent inventory of this class at finite-producer call sites (out of scope here).
- Prior art: `docs/plans/fix-flaky-local-webrtc-runtime-recreation-under-default-concurrency-lib-suite.md` and the merged flake-repair idiom (`a1c0e5a`, `cd5e7a8`, `c71e22d`): replace load-sensitive oracles with deterministic ones; never change production budgets from a flake ticket.
- `test.sh`: asset-sync check, `BOTSTER_ENV=test`, `cargo test --workspace`; workspace scope is load-bearing. Ticket command precondition: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.

## Failure mechanism

The producer `printf 'pse-ready\n'; exit 0` exits immediately after one line. The test's only precondition is that the attached adapter observed the bytes. Byte delivery does not imply that Core has observed `ProcessExited` for the session: the worker's exit report can still be in flight.

The blind cross-connection `ShutdownSession` then races that report:

1. If Core still sees the process alive, classification is `Active`, Core `Shutdown` succeeds, and the response is `Events`. Pass.
2. If the exit is already observable, classification is `Cleanup`, and the response is `SessionCleanup { already_exited }`. Pass.
3. In the window between process death and the exit report becoming observable: classification is `Active`, Core `Shutdown` fails against the dead process with an error that is not `UnknownSession`, recovery re-observes inside the same window, classification is still `Active`, and the handler returns the original error as a typed `OperatorError` (`src/daemon_transport.rs:4672`). Fail.

Outcome 3 is the legal blind-call contract that `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` already codifies in the sibling lifecycle file. The failing oracle `assert_ne!(shutdown.kind, OperatorError)` demands observed-state behavior from a blind call, so it contradicts the host contract. Suite load widens the window; isolated runs almost always land in outcomes 1 or 2. This is a test-oracle defect, not a production classification regression. The recorded failure also printed only `shutdown.kind`, so the exact sub-path (error code and operation) was lost, exactly the evidence loss [[flake oracles over typed response frames must print the full typed error body]] records.

## Runtime-teardown lens answers

`teardown_class_applies`: yes. The flake mechanism is literally terminal-state vs live-runtime divergence: the process is dead while the control plane still classifies the session `Active`. The repair is test-only; every production teardown path stays untouched, and the lens answers record what the repaired test keeps proving.

`teardown_isolation`: production unchanged. `ShutdownSession` affects exactly the named session; a `Cleanup` classification returns early and keeps adapters open (`src/daemon_transport.rs:3405-3408`), and the test keeps proving that the attached sibling connection survives to issue `Drain`. Test-side: each test runs an isolated hub (`start_isolated_live_output_hub`), so the wait observes only its own single-session registry.

`teardown_bounds`: production bounds unchanged (25 ms / 32-session / 64 KiB observe slice; exact-session query; bounded recovery). Test-side: the new observed-exit wait is bounded at 10 seconds with a hard panic that prints its final evidence; no unbounded wait is added. Per [[observed-exit waits must issue a production exact-session observe turn]], the bound is not widened beyond the class's recorded 10-second wait bound.

`late_message_matrix`: this repair adds no ownership-creating message and changes no admission path. The Unix-adapter ownership-creating messages and their existing coverage, all unchanged: `Spawn` (creates a durable session worker; invalid payloads return a typed operator error without dropping transport, per [[incomplete repo local session types drop the hub client connection]]); `Attach` (creates route ownership; pending attach is admitted only for missing HandleCache sessions, per [[terminal pending attach is only for missing handlecache sessions]]; EOF cleanup shares the live attach route set, per [[Unix EOF occupancy must share the live attach route set]]; stale-generation close protection is proven by `stale_generation_close_does_not_sweep_replacement_owner` in this file); `Drain` (subscription-authorized in the handler; unknown sessions get a typed error); `ShutdownSession` (destroys ownership; close-event suppression on cleanup unchanged). No row changes.

`production_path_proof`: preserved and strengthened. The repaired test still drives the production wire path: Spawn, Attach, live output, then cross-connection `ShutdownSession` through `observe_lifecycle_turn` plus the exact-session `classify_shutdown_session`. The new wait adds production exact-session observe stimuli (`ReadScreen` for the named session, once the dependency lands) and confirms observed state through `ListSessions`. The deterministic oracle then proves the charter's `Found`-exited classification (`SessionCleanup { already_exited }`) instead of merely forbidding one kind.

`ownership_identity`: unchanged. Sessions are keyed by `session_id`; attach routes keep their existing identity; the repair creates no new identity surface.

`sibling_fail_closed_policy`: unchanged. The legal blind-call typed error remains the fail-closed contract and stays codified in the WebRTC idempotent sibling test; this repair converts only the observed-state test to the observed-state oracle. On cleanup classification, sibling adapters stay open (proven by the unchanged Drain assertion).

## Scope

Repair the test oracle so lifecycle-suite load cannot fail `unix_shutdown_session_from_another_connection_classifies_attached_exit` through the legal blind-call window, while the test becomes a stronger proof of the charter's exact-session `Found`-exited classification. All changes stay inside `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`.

1. After the live-bytes assertion (`:1724-1727`) and before `ShutdownSession`, add a bounded observed-exit precondition wait: deadline 10 seconds, cadence 50 ms. Each iteration issues one cross-connection production `ReadScreen { session_id: "pse-session" }` via `botster_hub_client::request` (the production exact-session observe stimulus, per the dependency below), then one cross-connection `ListSessions`, and exits when the `pse-session` row reports lifecycle `"exited"`. On expiry, panic with the elapsed time, the last `ReadScreen` response kind and full typed error body if any, and the last `ListSessions` rows.
2. Replace `assert_ne!(shutdown.kind, OperatorError)` with the deterministic observed-state oracle: `shutdown.kind == SessionCleanup`, `cleanup.session_id == "pse-session"`, `cleanup.outcome == "already_exited"`. Every failure message prints `shutdown.kind` and the full `shutdown.error` body (`code`, `operation`, `message`) plus the cleanup body when present, per [[flake oracles over typed response frames must print the full typed error body]].
3. Keep the live-bytes wait, the ListSessions terminal assertion, the Drain no-`ProcessExit` assertion, and `hub.shutdown()` unchanged.
4. Add one short comment above the wait: a blind `ShutdownSession` may legally return a typed error before exit observation (contract codified in `webrtc_proofs.rs`); this test proves the observed-state classification, so it waits for the observed exit first.

Contingency, evidence-gated: if the merged `ticket_1786977409_499180` repair provides a different production exact-session observe stimulus than `ReadScreen`, the wait uses that landed stimulus instead. If that ticket merges without any production exact-session observe stimulus reachable from the wire API, stop and ask a human before inventing a second wait idiom.

## Non-scope

- No production (`src/`) code changes. The production classify path and any `ReadScreen` observe repair belong to `ticket_1786977409_499180`.
- Do not touch `tests/hub_daemon_lifecycle/webrtc_proofs.rs`: the exact-bytes oracle and the idempotent blind-call contract are owned by `ticket_1786977409_499180`.
- Do not modify `shutdown_short_lived_session` (`tests/hub_daemon_lifecycle/common.rs:591-612`) or its call sites. Its finite-producer call sites are remaining latent inventory of this class; record them in the vault capture, do not absorb them.
- Do not modify `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` (`:1621-1678`): its `ShutdownSession` targets a long-lived `sleep 30` session, which is the active-shutdown contract, not this class.
- Do not change deadlines, budgets, or concurrency anywhere else. Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream pins. No dependency or lockfile changes.
- Do not create a pull request (merge policy is direct).

## Repository ownership boundaries and cross-repo dependencies

Hub owns the daemon control plane, `ShutdownSession` classification policy, the Unix terminal adapter, and this lifecycle test. All work stays in Hub test code. No cross-repository prerequisite exists; do not register a Core, client, Web, or TUI dependency.

Registered same-repository ordering dependency: `dependency_1787004771_540563` -- this ticket depends on `ticket_1786977409_499180` (open). Reasons:

1. That ticket owns the production side of this defect class ("the suite-load ShutdownSession OperatorError oracle or the production classify path it protects"). Implementing the Unix twin first would fork ownership of the same diagnosis.
2. The canonical observed-exit wait requires the production `ReadScreen` exact-session observe turn that [[observed-exit waits must issue a production exact-session observe turn]] records for that ticket. At `origin/main` `c71e22d` that repair is absent, and the note itself records that a slice-only `ReadScreen` wait still failed 4 of 30 targeted runs. Building the wait now would either duplicate that production change or ship a wait with a proven flake mode.

Same-target siblings (do not absorb): `ticket_1786913892_208903` (parent write-budget owner; the ticket text forbids a serial dependency from it, and none is added -- the registered dependency above points at `ticket_1786977409_499180` only); the other two failures in the recorded suite run belong to their own owner tickets and must be attributed by exact test name if they appear during acceptance.

## Assumptions and unknowns

- Assumption: the recorded `OperatorError` was the legal blind-call window (outcome 3 above). The recorded panic printed only the kind, so the body is unknown. If Implement's pre-change reproduction captures an `OperatorError` body outside code `runtime_error`/`state_error` with operation `shutdown`, stop and escalate: that would indicate a production classification defect on the surface `ticket_1786977409_499180` owns.
- Assumption: `ticket_1786977409_499180` lands the production `ReadScreen` exact-session observe as the vault note records. The Scope contingency covers a different landed shape; Implement must re-verify the merged base independently (Acceptance check 0) rather than trusting this plan.
- Assumption: in the isolated single-session hub, once the worker's exit report is observable, one exact-session observe stimulus makes `ListSessions` report `"exited"` promptly; 10 seconds is patience for report delivery, not a production latency claim.
- Unknown: whether the failure reproduces pre-change on this worktree. Reproduction is probabilistic and load-dependent; non-reproduction does not gate the repair (the recorded suite failure is the red baseline).
- Unknown: whether registry state could reach `Stale` between the observed `"exited"` wait and the shutdown call (which would return `outcome: "stale_session"`). No ageing path is expected within milliseconds on an isolated hub; the oracle's full-body failure message makes any such occurrence diagnosable, and it would be new evidence, not a silent tolerance.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- one test: the observed-exit precondition wait, the deterministic cleanup oracle, full typed-body failure messages, one comment.
- `docs/plans/fix-flaky-unix-cross-connection-shutdown-classification-under-lifecycle-suite-load.md` -- this plan.
- `docs/reports/fix-flaky-unix-cross-connection-shutdown-classification-under-lifecycle-suite-load-implement.md` -- Implement report (Implement step).

No production code changes. No dependency or lockfile changes.

## Risks

- Dependency serialization: this run cannot enter Implement until `ticket_1786977409_499180` closes and merges. That ordering is correct for ownership, but dependency enforcement at advance time is version-dependent; if the engine blocks the Plan-to-Plan-Review advance itself on the open dependency, ask a human rather than removing the dependency.
- Dependency shape: the landed repair may differ from the vault note. The Scope contingency and Acceptance check 0 handle this; the plan must not be implemented against an assumed base.
- The strengthened oracle is stricter than today: after an observed exit, an `Events` response would now fail. That is intended -- the charter requires the `Found`-exited path to classify as cleanup -- and the wait makes the precondition deterministic.
- The lifecycle suite may fail on a non-target test during acceptance. Follow [[suite wide acceptance criteria make every observed test failure in scope]]: exact evidence, attribution to an existing owner ticket by test name, or a new ticket. Do not absorb and do not waive.
- The 10-second wait could expire under extreme load. That expiry is recorded evidence (the panic prints the final observe and listing state), not a silent flake; do not widen the bound without that instance-local evidence.

## Acceptance checks/tests

All commands run in the ticket worktree. Suite commands use the Hub wrapper `./test.sh` (asset-sync check, `BOTSTER_ENV=test`, workspace scope). Direct `cargo test` invocations do not satisfy these gates.

0. Dependency and base re-verification (before any change): `ticket_1786977409_499180` is closed and its repair is merged into `origin/main`. Rebase or fast-forward the ticket branch onto that main. Independently verify the production exact-session observe stimulus exists on the merged base (for `ReadScreen`: grep the handler for the exact-session `observe_session_lifecycle` call) and record the merge commit SHA in the Implement report. If the landed shape differs, apply the Scope contingency.
1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Pre-change reproduction probe (bounded, corroborating only): up to 5 runs of `./test.sh --locked --test hub_daemon_lifecycle_test unix_shutdown_session_from_another_connection_classifies_attached_exit` and up to 1 full `./test.sh --locked` on the unchanged worktree. Record outcomes and any captured `OperatorError` body; non-reproduction does not gate the repair.
3. Targeted repetition: `./test.sh --locked --test hub_daemon_lifecycle_test unix_shutdown_session_from_another_connection_classifies_attached_exit` passes 20 consecutive runs (shell loop, default concurrency).
4. Binding suite gates: 3 consecutive `./test.sh --locked --test hub_daemon_lifecycle_test` runs in which the target test passes, plus one full `./test.sh --locked` run (the ticket's recorded command shape, after the check-1 prebuild). Any non-target failure requires exact per-test attribution to an existing open ticket or a new ticket; none may be waived as generically pre-existing.
5. Red-on-revert, per [[a regression test must be shown to go red with the fix reverted]], both controls reverted afterward:
   - Control A (precondition liveness, statistical): with the deterministic cleanup oracle in place, remove only the observed-exit wait and run the targeted wrapper command up to 10 times. Expected red evidence: at least one failure, either `Events` against the cleanup oracle (blind call caught the process alive) or a typed `OperatorError` with its printed body (the recorded window). Record every outcome; if all 10 runs pass, record that the control was load-dependent and rely on the recorded suite failure plus Control B.
   - Control B (oracle liveness, deterministic): with the full repair in place, temporarily ablate production classification for one run -- force `classify_shutdown_session` to return `Active` and force `shutdown_error_is_already_gone` to `false`. Required result: the test fails red at the cleanup oracle with a printed typed `OperatorError` body (code, operation, message), proving the oracle is live over the exact-session classification surface and that the failure path prints its evidence. Revert the ablation.
6. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings`.
7. Implement report at `docs/reports/fix-flaky-unix-cross-connection-shutdown-classification-under-lifecycle-suite-load-implement.md`: dependency/base verification evidence (check 0), reproduction probe outcomes and any captured error bodies, the oracle change, both control outputs with exit codes and failure text, and acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or compiled runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched. The charter's `ShutdownSession` proof rows are satisfied by the strengthened test itself: `Found`-exited is proven deterministically here; `Absent` and blind-call `Err` behavior remain proven by the existing sibling tests, which stay unchanged.

## Vault gaps worth capturing

- After merge, extend [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]] (or add a sibling instance note) with this Unix twin, and record the remaining latent inventory: `shutdown_short_lived_session` call sites whose sessions are finite producers carry the same blind-call window behind an `Events | SessionCleanup` oracle.
- [[observed-exit waits must issue a production exact-session observe turn]] describes a production `ReadScreen` behavior that is not on `origin/main` `c71e22d`. Once `ticket_1786977409_499180` merges, the note should record the merge commit as repository evidence, per [[vault convention notes can document unimplemented behavior as shipped]].

## Implement steps

1. Complete Acceptance check 0 (dependency merged; base re-verified; branch updated).
2. Run the prebuild (check 1) and the bounded pre-change reproduction probe (check 2).
3. Apply Scope items 1-4 inside the one test.
4. Run checks 3-6, including both red-on-revert controls, reverting all ablations.
5. Write the Implement report and commit the test repair plus report. Do not create a PR.
