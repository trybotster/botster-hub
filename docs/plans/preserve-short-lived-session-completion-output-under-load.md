# Preserve short-lived session completion output under load

## Context loaded

- Project Pipelines ticket `ticket_1784229592_834310`, run `run_1784249677_876331`, Plan step `botster_plan`, gate `botster_plan_gate`, and the initially empty artifact, review, finding, dependency, question, and answer surfaces were loaded through `project_pipelines_current_context` before planning.
- Required planning authority: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[identity]], and [[goals]], plus the Botster overlay's required pipeline notes.
- Applicable lifecycle and verification authority: [[retention without a reachable flush is data loss]], [[coredaemon must expose terminal truth used by the production hub path]], [[a regression test must be shown to go red with the fix reverted]], [[test script required for rust tests not cargo test]], [[suite wide acceptance criteria make every observed test failure in scope]], [[closed dependency tickets signal merged source not a consumable release]], and [[plan steps need reviewable plan artifacts]].
- The ticket's retained loaded run `29526604665` tested exact subject `2ff224626c26ea10d506319aef5bdd9eff598163`. `cli_short_lived_session_shutdown_returns_structured_cleanup` failed because attach stdout lacked the exact `dogfood:done` marker, while the independently owned oversized local-WebRTC test passed.
- Current checkout `e8645678e526e9fa23eac741fa50af11af937636` remains on the vulnerable Core lock revision `db69456c14d3c4ee870a24f0ffaba913ac945aca`. The failing product path is `src/main.rs` `sessions attach` -> `src/daemon_transport.rs::stream_attach` -> `botster_hub_client::stream_attach` -> daemon Attach/Drain -> `HubClientApi::DrainRuntime` -> `HubRuntime::drain_runtime_once` -> CoreDaemon/session worker. The client writes all `TerminalOutput` events in each response and stops at `ProcessExit`; the missing marker originated below this boundary when final PTY bytes became unreachable before exit publication.
- Closed Core ticket `ticket_1784242148_775559` merged revision `84c2ff20f3607ff24fb87d196e132c54365c31c5`, which publishes final PTY egress before `ProcessExited` and session removal. Active Hub ticket `ticket_1784168175_972093` owns consuming that revision, the `Cargo.lock` update, fast-exit diagnostics, focused loaded target, and loaded proof.
- Human answer `question_1784249984_836951` chose dependency-first routing. Blocking dependency `dependency_1784250061_527940` now requires `ticket_1784168175_972093` to close before this ticket proceeds. This ticket must not absorb or duplicate that Hub branch. Final suite-wide convergence remains owned by `ticket_1784087788_242994`.

## Scope

Botster layer: Rust hub integration tests and their existing real-daemon attach path, after the blocking Hub dependency merges.

1. Rebase from current `main` only after `ticket_1784168175_972093` closes, then verify that `Cargo.lock` resolves all Core git packages to the merged final-egress revision and that the dependency's focused diagnostics and loaded target are present.
2. Run and inspect the merged tests to determine whether they already prove the exact product-shaped invariant: the short-lived command accepts `done`, attach stdout contains `dogfood:done` before attach completes on `ProcessExit`, and a later shutdown still returns structured `session_cleanup` with `outcome=already_exited`.
3. If merged coverage proves that invariant, make no production or test change. Attach exact source, command, negative-control, and loaded-run evidence and close this ticket as covered by its dependency.
4. If a product-shaped coverage gap remains, add only the narrow missing `dogfood:done` regression/acceptance proof through the existing CLI spawn/attach/input/shutdown path. Replace the test's fixed 150 ms attach guess only if necessary with an existing observable attach/readiness condition; do not add a new timing abstraction or broaden production behavior.
5. Reuse the dependency's focused loaded-runner machinery. Adjust its focused command only if exact inspection proves it does not execute the narrow short-lived regression; do not create a second diagnostics framework or duplicate Core-consumption work.
6. Prove the narrow regression red against the exact pre-Core/pre-consumer revision and green on the merged revision, then run target-specific residual-tail acceptance at default test parallelism.

Every changed line must be required for the missing short-lived marker assertion, an observable readiness prerequisite for that assertion, or routing the existing focused loaded target to the narrow test.

## Non-scope

- No `Cargo.lock` or Core dependency pin owned by this ticket; no Core, session-worker, CoreDaemon, HubRuntime, daemon transport, or hub-client lifecycle repair.
- No Hub-side buffering, post-exit readback contract, snapshot decoding, event reordering, new retry/drain loop, or alternate attach implementation.
- No retries, fixed sleeps, timeout increases, test serialization, reduced load, weaker marker assertions, or acceptance via `--test-threads=1`.
- No duplicate fast-exit diagnostic, loaded workflow, or harness framework. The dependency owns those surfaces.
- No oversized local-WebRTC response work; ticket `ticket_1784168176_163113` owns that independent path.
- No repairs for other lifecycle-suite roots. Final suite-wide convergence stays with `ticket_1784087788_242994`; any observed failure is recorded and routed rather than silently waived.
- No plugin, Lua, TUI, SPA, Rails, UI-contract, README, or product-policy change.

## Assumptions and unknowns

- Assumption: the dependency will merge the Core revision and Hub diagnostics currently visible on its active branch without changing the final-egress invariant.
- Assumption: source-coupled availability is proven only after the Hub dependency merges to `main` and `Cargo.lock` resolves the intended Core commit; a closed Core ticket alone is insufficient consumer proof.
- Assumption: `cli_short_lived_session_shutdown_returns_structured_cleanup` remains the correct product-shaped test because it exercises the actual operator CLI and preserves structured cleanup semantics after natural exit.
- Unknown until rebase: whether the dependency's merged focused diagnostic plus existing short-lived test already supply complete exact-marker, ordering, negative-control, and loaded evidence. This unknown deliberately selects between no-code closure and one narrow test/harness adjustment.
- Unknown until merged inspection: whether the fixed 150 ms pre-input sleep is merely fixture startup or an unproven attach-readiness prerequisite. It may not be increased or retained as the proof boundary if the loaded failure shows it can send before subscription readiness.
- Stop and ask for re-plan if complete coverage requires any production lifecycle change, a new Core pin, buffering, post-exit readback, protocol/API changes, timing-only convergence, or absorption of the dependency branch.
- Worktree/target assumption: all work remains in the pipeline-provided worktree for explicit target `tgt_7e208a0c76a44980a83b63af976b1f22`; no sibling checkout is an implementation surface.
- Convention conflict: none after the human routing decision. The dependency split preserves one production owner and the plan uses the repository wrapper, observable completion, red-on-revert proof, and the existing load harness.

## Affected surfaces and files

- `docs/plans/preserve-short-lived-session-completion-output-under-load.md` — this durable Plan artifact and evidence contract.
- `tests/hub_daemon_lifecycle_test.rs` — read and run after dependency merge; change only if exact `dogfood:done` output-before-exit plus structured-cleanup coverage remains missing.
- `script/run-loaded-daemon-lifecycle` and `.github/workflows/loaded-daemon-lifecycle.yml` — dependency-owned verification inputs. Reuse unchanged when they already execute the narrow regression; minimally adjust the existing focused target only if it otherwise cannot provide the required target-specific loaded proof.
- `Cargo.lock` — dependency-owned and read-only here; verify it resolves Core revision `84c2ff20f3607ff24fb87d196e132c54365c31c5` or a later merged revision containing the same verified repair.
- `crates/botster-hub-client/src/lib.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/runtime.rs`, and `src/main.rs` — read-only production wiring proof. No planned edits.

Expected implementation outcomes:

- Coverage complete: no files changed after this plan; close with exact evidence.
- Coverage gap: change `tests/hub_daemon_lifecycle_test.rs` only, plus the existing focused target mapping files only when necessary to execute that test under residual-tail load.

## Risks

- **Duplicate ownership or conflicting Core pins:** concurrent Hub tickets could land different lock revisions or overlapping diagnostics. Mitigation: blocking dependency, rebase after merge, and no lockfile/harness duplication.
- **False covered-by-dependency closure:** a generic immediate-output diagnostic may prove final egress but not the input-driven `dogfood:done` plus structured cleanup contract. Mitigation: require exact test/source and runtime evidence for every part of the product-shaped invariant before no-code closure.
- **Timing-dependent test remains flaky:** the existing 150 ms delay may not prove attach readiness under starvation. Mitigation: inspect event ordering and use an existing observable attach/readiness signal if the gap is real; never inflate the sleep.
- **Tautological regression proof:** a test can pass on both fixed and reverted code if it exits before draining the relevant state. Mitigation: require nonzero red-on-revert against the exact pre-Core/pre-consumer revision and retain raw event/output evidence.
- **Structured cleanup regression:** fixing or strengthening completion output could accidentally alter the later already-exited shutdown response. Mitigation: preserve all exact `session_cleanup`, session id, outcome, disconnect, and path-scrubbing assertions.
- **Focused harness drift:** adding a second target could split acceptance or weaken default behavior. Mitigation: reuse the dependency's single focused target and change its mapping only when the merged target does not run the narrow regression.
- **Suite-wide failure misrouting:** the target-specific run may expose other lifecycle roots. Mitigation: capture the first non-cascade failure and route it to its owner; do not claim this leaf owns final suite convergence or waive failures silently.

## Acceptance checks and tests

1. Dependency/rebase proof:
   - Confirm `ticket_1784168175_972093` is closed and merged to current `main`.
   - Inspect `Cargo.lock` and Core history to prove the resolved Core revision contains the merged final-egress ordering repair.
   - Inspect the merged branch diff and focused workflow mapping before deciding whether any code is needed.
2. Exact product-path proof through the repository wrapper:
   - `./test.sh --test hub_daemon_lifecycle_test cli_short_lived_session_shutdown_returns_structured_cleanup -- --exact --nocapture`
   - Require attach stdout to contain both `dogfood-ok` and exact `dogfood:done` before attach returns.
   - Require shutdown stdout to contain `response=session_cleanup`, `session_id=dogfood-session`, and `outcome=already_exited`; stdout/stderr must not contain `client disconnected` or the generated data directory.
3. Coverage decision:
   - If the merged dependency already makes check 2 protective and supplies its loaded execution, attach exact evidence and close with no code.
   - Otherwise add the narrow regression/readiness proof, then rerun check 2 and the dependency's focused fast-exit diagnostic to show the production path and diagnostic path agree.
4. Negative control:
   - Run the committed narrow test against exact pre-Core/pre-consumer subject `2ff224626c26ea10d506319aef5bdd9eff598163`, or an equivalent ablation that reverts only the Core-consumer ordering while retaining the test.
   - The wrapper command must exit nonzero because `dogfood:done` is absent at the production completion boundary. Restore the merged revision and require the same command to pass.
5. Target-specific loaded proof:
   - Dispatch the existing loaded lifecycle workflow against the exact fixed Hub SHA using the dependency-owned focused target, `stress_profile=residual-tail`, default Rust test parallelism, and the established repetition count.
   - Every repetition must execute the narrow short-lived regression and preserve exact marker, structured cleanup, resource, subject-SHA, and owned-process teardown evidence. A focused run that executes only a generic diagnostic does not satisfy this ticket.
6. Local quality and regression checks if files change:
   - `cargo fmt --check`
   - repository strict Clippy command discovered from the merged checkout
   - `git diff --check`
   - `./test.sh --test hub_daemon_lifecycle_test cli_short_lived_session_shutdown_returns_structured_cleanup -- --exact --nocapture`
   - relevant dependency-owned focused diagnostic through `./test.sh`
   - `./test.sh` at default parallelism when proportionate to the final diff; any red is classified with exact evidence rather than blanket-waived.
7. Review the complete diff and durable artifacts for unrelated edits, secrets, usernames, emails, absolute home/worktree paths, and stale capability claims.

## Pipeline gates and artifacts

- Plan gate: this committed plan plus the Project Pipelines `kind=plan` artifact and gate evidence.
- Dependency gate: no implementation starts until `dependency_1784250061_527940` clears and the run is explicitly reactivated from merged `main`.
- Implement handoff: attach the coverage decision, resolved Core SHA, exact merged files inspected, focused commands, and either no-code closure evidence or the minimal files changed.
- Review/Verify handoff: attach raw red-on-revert and fixed-green results, exact loaded workflow URL/artifact, default-parallelism inputs, and cleanup status.
- Product decision ledger: human answer `question_1784249984_836951` is binding. This ticket is a post-dependency coverage leaf; final suite convergence belongs to `ticket_1784087788_242994`.

## Project Pipelines and vault checklist evidence

- Pipeline context was loaded before planning. The later human question and answer were received durably, and the resulting dependency was registered.
- Applicable playbooks and notes are named above. Convention conflicts: none after dependency-first routing.
- Plan-time repository evidence: clean worktree at `e8645678e526e9fa23eac741fa50af11af937636`; exact failing test and CLI/daemon/client/runtime path inspected; dependency branch and merged Core revision inspected; repository wrapper and loaded harness inspected. No test was claimed as loaded-green during Plan.
- Both checklist creations initially returned plugin-worker timeouts but persisted successfully. Their final item evidence is also repeated in this plan and gate artifact per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Durable capture disposition: no new vault capture during Plan. Existing notes already describe final-egress reachability, production CoreDaemon truth, dependency consumption, negative controls, wrapper use, and suite-wide acceptance.

## Vault gaps worth capturing

- No immediate gap. If post-merge evidence shows that a generic zero-input fast-exit diagnostic does not protect an input-driven completion marker, capture the narrower durable testing rule only after the distinction is proven.
- If the existing 150 ms delay is shown to conceal attach-readiness ordering under load, capture the exact observable readiness boundary and update the relevant PTY integration-test note after implementation evidence establishes it.
- If no-code closure succeeds, no new note is warranted; the existing final-egress and red-on-revert notes already explain the reusable mechanism.
