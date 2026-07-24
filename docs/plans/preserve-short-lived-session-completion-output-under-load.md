# Preserve short-lived session completion output under load

## Context loaded

- Project Pipelines ticket `ticket_1784229592_834310`, fresh run `run_1784306016_385967`, Plan run step `run_step_1784306016_920757`, gate `botster_plan_gate`, closed dependency `dependency_1784250061_527940`, and all artifact, review, finding, question, and prior-answer surfaces were loaded through `project_pipelines_current_context` before planning.
- Required planning authority: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[identity]], and [[goals]], plus the Botster overlay's required pipeline notes.
- Applicable lifecycle and verification authority: [[retention without a reachable flush is data loss]], [[coredaemon must expose terminal truth used by the production hub path]], [[a regression test must be shown to go red with the fix reverted]], [[test script required for rust tests not cargo test]], [[suite wide acceptance criteria make every observed test failure in scope]], [[closed dependency tickets signal merged source not a consumable release]], and [[plan steps need reviewable plan artifacts]].
- The ticket's retained loaded run `29526604665` tested exact subject `2ff224626c26ea10d506319aef5bdd9eff598163`. `cli_short_lived_session_shutdown_returns_structured_cleanup` failed because attach stdout lacked the exact `runtime:done` marker, while the independently owned oversized local-WebRTC test passed.
- The pipeline worktree was cut from `e8645678e526e9fa23eac741fa50af11af937636`, so its checked-out lockfile is stale. Current `main` is `dfd26cd8c3f50bb26a49f760ddadaabc1940c22b`; inspection used `main`/merged commit objects without treating the stale worktree as current evidence. The production path remains `sessions attach` -> `botster_hub_client::stream_attach` -> daemon Attach/Drain -> HubRuntime/CoreDaemon/session worker.
- Dependency ticket `ticket_1784168175_972093` merged as PR #143 at `f5e839476152f9bed52a9d17e994a65568de2840`. Its merged changes consume Core revision `84c2ff20f3607ff24fb87d196e132c54365c31c5`, retain final PTY egress before exit, keep the CLI attachment open through terminal exit, and add the generic fast-exit diagnostic and focused harness target.
- Human answers `question_1784249984_836951` and `question_1784250676_465927` are binding: do not duplicate that dependency's Core pin, lifecycle repair, diagnostics, or harness; after merge, close this ticket with no code if the exact product-shaped behavior is covered.
- Coverage is complete. The unchanged `cli_short_lived_session_shutdown_returns_structured_cleanup` test still drives spawn -> CLI attach -> CLI input -> natural process exit -> structured cleanup and asserts exact `runtime:done`. It failed on captured subject `2ff224626c26ea10d506319aef5bdd9eff598163`, then passed under residual-tail load with default test parallelism in post-merge lifecycle-suite runs `29551641598` (`c81a1b1`), `29552856473` (`27851a0`), and `29553895040` (`6508317`). Each campaign later failed only on separately owned tests.

## Scope

Botster layers inspected: Rust hub integration test, hub-client attach loop, loaded lifecycle harness, and merged Core dependency. No product layer needs a change.

1. Preserve the existing test and production implementation unchanged.
2. Carry the exact pre-fix red and three post-merge loaded-green executions into Implement/Review/Verify artifacts.
3. Classify the later failures in those lifecycle-suite runs as separately owned rather than claiming suite-wide success: stalled attach and oversized WebRTC remained red in those subjects and were subsequently handled by their own tickets.
4. Close this ticket as covered by merged dependency PR #143. Do not add a ticket-specific focused target: the required default-parallel production-shaped test already ran under load, while the dependency's focused diagnostic is intentionally a different diagnostic surface.

## Non-scope

- No `Cargo.lock` or Core dependency pin owned by this ticket; no Core, session-worker, CoreDaemon, HubRuntime, daemon transport, or hub-client lifecycle repair.
- No Hub-side buffering, post-exit readback contract, snapshot decoding, event reordering, new retry/drain loop, or alternate attach implementation.
- No retries, fixed sleeps, timeout increases, test serialization, reduced load, weaker marker assertions, or acceptance via `--test-threads=1`.
- No new focused target, duplicate fast-exit diagnostic, loaded workflow, or harness framework. Existing exact lifecycle-suite executions already satisfy this ticket's loaded proof.
- No oversized local-WebRTC response work; ticket `ticket_1784168176_163113` owns that independent path.
- No repairs for other lifecycle-suite roots. Final suite-wide convergence stays with `ticket_1784087788_242994`; any observed failure is recorded and routed rather than silently waived.
- No plugin, Lua, TUI, SPA, Rails, UI-contract, README, or product-policy change.

## Assumptions and unknowns

- Assumption confirmed: dependency PR #143 is merged to `main`, and `main:Cargo.lock` resolves all four Core git packages to `84c2ff20f3607ff24fb87d196e132c54365c31c5`.
- Assumption: `cli_short_lived_session_shutdown_returns_structured_cleanup` remains the correct product-shaped test because it exercises the actual operator CLI and preserves structured cleanup semantics after natural exit.
- Assumption: the original captured failure at the exact assertion is a valid negative control because the same committed test returned nonzero on the pre-repair subject and green on merged subjects.
- Unknown: none that requires implementation. The test's existing 150 ms pre-input delay is not the completion-output proof boundary and did not prevent the exact assertion from failing on the captured subject; changing it would be adjacent cleanup without evidence.
- Stop and ask for re-plan if a later reviewer rejects the preserved pre-fix run as the negative control or produces a new exact-marker failure on current `main`.
- Worktree/target assumption: all work remains in the pipeline-provided worktree for explicit target `tgt_7e208a0c76a44980a83b63af976b1f22`; no sibling checkout is an implementation surface.
- Convention conflict: none after the human routing decision. The dependency split preserves one production owner and the plan uses the repository wrapper, observable completion, red-on-revert proof, and the existing load harness.

## Affected surfaces and files

- `docs/plans/preserve-short-lived-session-completion-output-under-load.md` — update this durable Plan artifact to record the selected no-code closure and current-run evidence.
- `tests/hub_daemon_lifecycle_test.rs` — read-only; existing exact regression and structured-cleanup contract are complete.
- `script/run-loaded-daemon-lifecycle` and `.github/workflows/loaded-daemon-lifecycle.yml` — read-only; existing lifecycle-suite runs provide default-parallel loaded proof.
- `Cargo.lock` — read-only and dependency-owned; current `main` resolves Core revision `84c2ff20f3607ff24fb87d196e132c54365c31c5`.
- `crates/botster-hub-client/src/lib.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/runtime.rs`, and `src/main.rs` — read-only production wiring proof. No planned edits.

Expected implementation outcome: no production, test, harness, dependency, or documentation change after this Plan artifact; Implement records coverage evidence and prepares no-code closure.

## Risks

- **False covered-by-dependency closure:** the generic fast-exit diagnostic is not the product proof. Mitigation: closure cites the exact input-driven `runtime:done` test and its three post-merge loaded executions, not the diagnostic campaign.
- **Weak negative control:** the same test happened to pass once on an older vulnerable lock, so Core SHA alone does not deterministically force red. Mitigation: use the exact captured failing subject/run as historical red evidence and do not overstate a universal revision-level failure.
- **Stale worktree evidence:** the checked-out plan branch predates PR #143. Mitigation: source and lock assertions were inspected from current `main`; downstream work must start from/reconcile with `main` before creating any PR.
- **Suite-wide failure misrouting:** the cited lifecycle runs are red overall. Mitigation: report the exact ticket-owned test as green and name the separately owned red tests; do not describe the campaigns as suite-green or waive them.
- **Redundant harness change:** adding a focused target would create code solely to repeat evidence already obtained under the stronger default-parallel suite shape. Mitigation: no harness edit.

## Acceptance checks and tests

1. Dependency proof:
   - PR #143 is merged at `f5e839476152f9bed52a9d17e994a65568de2840`.
   - Current `main:Cargo.lock` resolves Core to `84c2ff20f3607ff24fb87d196e132c54365c31c5`.
   - Merged production wiring reaches `botster_hub_client::stream_attach`, writes terminal events before honoring exit, and performs one final drain after an exited lifecycle readback.
2. Exact local product-path confirmation through the repository wrapper, if downstream gates require a fresh local run:
   - `./test.sh --test hub_daemon_lifecycle_test cli_short_lived_session_shutdown_returns_structured_cleanup -- --exact --nocapture`
   - Require attach stdout to contain both `production runtime-ok` and exact `runtime:done` before attach returns.
   - Require shutdown stdout to contain `response=session_cleanup`, `session_id=runtime-session`, and `outcome=already_exited`; stdout/stderr must not contain `client disconnected` or the generated data directory.
3. Historical red:
   - GitHub Actions run `29526604665`, exact subject `2ff224626c26ea10d506319aef5bdd9eff598163`, failed this exact test at the exact `runtime:done` assertion.
4. Post-merge loaded green:
   - Runs `29551641598` (`c81a1b1`), `29552856473` (`27851a0`), and `29553895040` (`6508317`) each used `test_target=lifecycle-suite`, `stress_profile=residual-tail`, default Rust parallelism, and reported `cli_short_lived_session_shutdown_returns_structured_cleanup ... ok`.
   - Preserve each run's later separately owned failures in the handoff; these are exact-test green proofs, not suite-green claims.
5. No-code integrity:
   - `git diff --check`
   - Confirm the eventual branch diff contains only the synchronized Plan artifact and no production/test/harness changes.
   - Scan the artifact and gate evidence for secrets, usernames, emails, absolute home/worktree paths, and stale capability claims.

## Pipeline gates and artifacts

- Plan gate: this committed plan plus structured gate evidence.
- Dependency gate: satisfied by merged PR #143 and closed dependency `dependency_1784250061_527940`.
- Implement handoff: attach the no-code coverage decision, resolved Core SHA, exact production path inspected, historical red, three post-merge loaded greens, and a zero implementation diff.
- Review/Verify handoff: independently confirm exact run inputs/results, current-main production wiring, separately owned red classifications, and absence of ticket implementation changes.
- Product decision ledger: human answer `question_1784249984_836951` is binding. This ticket is a post-dependency coverage leaf; final suite convergence belongs to `ticket_1784087788_242994`.

## Project Pipelines and vault checklist evidence

- Fresh pipeline context was loaded before planning, including both prior human answers and the now-closed dependency.
- Applicable playbooks and notes are named above. Convention conflicts: none after dependency-first routing.
- Plan-time repository evidence: exact failing test, merged PR #143/Core lock, CLI/client/runtime path, repository wrapper, loaded harness, and authoritative Actions logs inspected. The worktree's stale base is explicitly separated from current-main evidence.
- Both fresh-run checklist creations returned plugin-worker timeouts but persisted successfully; listing reconciled them before any retry per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Durable capture disposition: no new vault capture during Plan. Existing notes already describe final-egress reachability, production CoreDaemon truth, dependency consumption, negative controls, wrapper use, and suite-wide acceptance.

## Vault gaps worth capturing

- No immediate gap. Existing notes already distinguish production-path proof, reachable final-egress drains, exact red/green regression evidence, loaded default-parallel verification, dependency availability, and checklist timeout reconciliation.
- The evidence did show that a generic fast-exit diagnostic is not a substitute for an input-driven completion marker, but that distinction is already enforced by the binding human answer and this plan; one ticket instance is not yet enough to justify another durable note.
- No vault write is planned. Capture only if Review/Verify finds a repeatable new rule beyond the existing notes.
