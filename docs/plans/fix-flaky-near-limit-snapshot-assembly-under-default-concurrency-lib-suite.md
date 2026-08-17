# Plan: Fix flaky near_limit_snapshot_assembly_stays_within_owner_turn

Ticket: `ticket_1786921010_869253`
Run: `run_1786926789_708317`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

Revision 4. Revision 2 addressed Plan Review `review_1786927857_391209`: acceptance commands now use the Hub `./test.sh` wrapper with the `botster-session-worker` prebuild and exact strict gates, and the plan records the duplicate ticket `ticket_1786919220_649402` disposition and preserves its failure evidence. Revision 3 addresses `review_1786930516_935692`: the per-call work-bound assertions now cover every page including the final `more = false` page, and the red-proof uses two separate negative controls so the item bound and the byte bound each have a proven first-failure site. Revision 4 addresses Review `finding_1786935491_150462`: the worktree line uses path-neutral wording instead of a personal absolute path.

The write-budget sibling continuation (`ticket_1786913892_208903`) hit one lib-suite failure after integrating Hub main `a55f62d`. The failed test was `daemon_transport::daemon_entity_subscriptions::tests::near_limit_snapshot_assembly_stays_within_owner_turn`. The panic was `assertion failed: started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` at `src/daemon_entity_subscriptions.rs:3033`. The same test passed in isolation on the branch and on base `origin/main` `a55f62d`. This ticket repairs or quarantines that default-concurrency root on botster-hub.

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`, confirmed from the ticket worktree `origin` remote).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22` (from the ticket record; `list_spawn_targets` timed out twice, so the worktree remote is the confirmation path).
- Worktree: the pipeline-provided ticket worktree, branch `project-pipelines/ticket_1786921010_869253`, base `a55f62d` (clean).
- The worktree path contains no colon. `CARGO_TARGET_DIR` override is not required.
- Tracked `.gitignore` is present and non-empty (53 bytes). No restore is required.

## Repository playbook loaded

- [[botster-hub-playbook]] -- Hub owns daemon transport, entity subscription assembly, and this lib test.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] -- generic Plan role contract.
- [[botster-planner-playbook]] -- Botster planning overlay, completion evidence, worktree hygiene.
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] -- the sibling flake class: default-concurrency load makes elapsed paging and wall-clock assertions non-deterministic. The repair must use a non-load-sensitive elapsed budget and a bounded production loop.
- [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]] -- production `take_snapshot_item_page` accounting is sibling-owned work; this note also warns that this exact test can pass in isolation while the defect class remains.
- [[Hub background fairness must stay policy-neutral]] -- the production 25 ms owner-turn budget stays unchanged.
- [[a regression test must be shown to go red with the fix reverted]] -- the repaired assertions need a sabotage red-proof.
- [[plugin worker unload deadline can flake under default-concurrency workspace load]] -- distinguishes isolated diagnosis from the default-concurrency gate.
- Runtime-teardown class: does not apply. This is a unit-test timing flake in snapshot assembly. There is no peer, session teardown, or resource-spin surface. [[botster runtime teardown lenses]] was not loaded, per the class rule.

## Context loaded

- Ticket record, run record, gates, and empty prior artifacts/checklists via `project_pipelines_current_context`.
- Prior art in this repository:
  - `ef88ad1` plan `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` -- the sibling flake plan. Its Non-scope section deferred this exact test to a new ticket. This ticket is that follow-up.
  - `a1c0e5a` + `a55f62d` -- the merged repair idiom: drive the production assembler with `Duration::MAX` so elapsed time cannot cut a page, bound the page loop with a named constant, assert `page.items > 0`, and assert only deterministic outcomes.
  - `docs/reports/fix-flaky-separators-close-under-default-concurrency-lib-suite-implement.md` -- implement-report destination convention.
- Code read: `near_limit_snapshot_assembly_stays_within_owner_turn` (`src/daemon_entity_subscriptions.rs:2979-3055`), `take_snapshot_item_page` (`:1065`), `continue_session_snapshot_assembly` (`:1117`), constants `SESSION_DELIVERY_MAX_ITEMS = 16`, `SESSION_DELIVERY_MAX_BYTES = 64 KiB`, `SESSION_DELIVERY_MAX_ELAPSED = 8 ms`, `MAX_OWNER_TURN_MS = 25`, `MAX_READY_OPERATION_WAIT_MS = 50`, `DAEMON_MAX_FRAME_BYTES = 1 MiB`.
- `test.sh` -- the repo test wrapper. It checks hub-test-support asset sync, sets `BOTSTER_ENV=test`, and runs `cargo test --workspace`. Workspace scope is load-bearing; bare `cargo test` runs only the root crate.
- Duplicate ticket `ticket_1786919220_649402` (closed) and the operator answer to `question_1786927488_804511`; details in the Duplicate ticket disposition section.

## Duplicate ticket disposition

Earlier ticket `ticket_1786919220_649402` ("Hub tests: fix flaky near_limit_snapshot_assembly_stays_within_owner_turn under default-concurrency lib suite") owned the same test and failure class. It was discovered during the separators Implement binding (`ticket_1786916741_161067`).

Operator disposition (`question_1786927488_804511`, answered 2026-08-16): keep `ticket_1786921010_869253` as the authoritative owner because its run is active and `ticket_1786913892_208903` already depends on it. `ticket_1786919220_649402` is closed as a duplicate without merge. This plan and the Implement report must preserve its evidence.

Preserved evidence from `ticket_1786919220_649402`:

- Command: after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, one default-concurrency `./test.sh --locked --lib` on Hub worktree `project-pipelines/ticket_1786916741_161067`.
- Suite failure: `near_limit_snapshot_assembly_stays_within_owner_turn`, panic `assertion failed: started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` at `src/daemon_entity_subscriptions.rs:3033`. Suite result: 349 passed; 2 failed.
- Isolation of the exact test through the wrapper: `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` => FAIL (exit 101) on the ticket branch AND on base `origin/main` `c72712e` with the same exact command.

This evidence is stronger than the current ticket's: the failure reproduces even in a filtered single-test wrapper run. Ambient workspace load (concurrent agent builds and tests on the machine), not only intra-suite thread concurrency, can preempt the test thread past the 25 ms wall-clock bound. This confirms the wall-clock assertion is load-sensitive under any load source and cannot be repaired by reducing suite concurrency alone.

## Failure mechanism

The test builds 20 session rows. Each row id carries 40 KiB of padding, so each projected item encodes to about 41 KiB. With the 64 KiB page byte budget, each production page accepts exactly one item, because a two-item trial frame exceeds 64 KiB. The loop therefore drives about 20 `Continue` pages and one final send.

Two load-sensitive couplings exist:

1. Lines 3033-3034 assert wall-clock `started.elapsed() < 25 ms` and `< 50 ms` around every call. Under default-concurrency `--lib` (352 tests), the scheduler can preempt the test thread inside or after a call. Wall-clock elapsed includes that preemption, so a correct call can exceed 25 ms. This is the observed failure.
2. The test passes the production 8 ms `SESSION_DELIVERY_MAX_ELAPSED` as the page budget. If the scheduler pauses the thread between `Instant::now()` at `take_snapshot_item_page` entry and the first loop check, the page returns empty with `more = true`. `continue_session_snapshot_assembly` then classifies empty-and-more as `close_oversized_session_snapshot`, and the test's `let else` panics with `near-limit assembly`. This latent path is the same elapsed-empty defect that `a55f62d` removed from the separators test.

Isolated runs pass when the machine is unloaded, because an unloaded thread never loses 8 ms or 25 ms to preemption. The duplicate-ticket evidence shows the converse: under ambient workspace load, even a filtered single-test wrapper run fails on clean `origin/main`. This is a test coupling defect, not a production regression. Production budgets and paging behavior are correct and stay unchanged.

## Scope

Repair `near_limit_snapshot_assembly_stays_within_owner_turn` so default-concurrency `--lib` cannot fail it through scheduler preemption, while it keeps proving that near-limit snapshot assembly does bounded work per owner-loop call and completes one frame within `DAEMON_MAX_FRAME_BYTES`.

Keep the production assembler. Do not add a test-only assemble helper.

Required test behavior:

1. Keep the 20-row near-limit fixture (about 820 KiB total against the 1 MiB frame limit) and keep driving production `continue_session_snapshot_assembly`.
2. Replace the `SESSION_DELIVERY_MAX_ELAPSED` argument with `Duration::MAX`. Elapsed time then cannot cut a page, so no elapsed-empty `Closed` and no load-dependent page shape exist. Page cuts become purely byte-driven and deterministic: one item per page.
3. Delete the wall-clock assertions `started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` and `started.elapsed() < Duration::from_millis(crate::MAX_READY_OPERATION_WAIT_MS)` and the per-iteration `Instant::now()`. Both constants remain referenced by other tests in this module, so no import cleanup is needed.
4. Replace them with deterministic per-call work bounds, which are what "stays within owner turn" means in production. Assert on every successful `Continue` page, before branching on `page.more`, in this order: `page.items >= 1` (progress, no empty pages), then `page.items <= SESSION_DELIVERY_MAX_ITEMS`, then `page.bytes <= SESSION_DELIVERY_MAX_BYTES`. The final `more = false` page also carries items and bytes, and the owner-turn claim applies to every call, so the final page gets the same three assertions. The fixed assertion order gives the two negative controls in acceptance check 4 distinct first-failure sites. Bounded bytes and items per call are the mechanism that keeps a 25 ms owner turn safe; wall-clock time under suite load is not a valid proxy for that mechanism.
5. Bound the loop with a named constant (for example `const MAX_NEAR_LIMIT_PAGES: usize = 21;`) and assert inside the loop that `more = true` pages do not exhaust it, in the style of `MAX_SEPARATOR_PAGES` in the separators test. Twenty one-item pages need at most 20 useful calls; the constant gives one page of slack.
6. Keep the existing deterministic assertions: no frames delivered while `more = true`, `assembled_items` non-empty while assembling, `assembled_items` drained at the end, exactly one `Snapshot` frame with 20 items, and final encoded size `<= DAEMON_MAX_FRAME_BYTES`.
7. Add a short comment in the `a55f62d` idiom stating that the test proves bounded per-call assembly work, not wall-clock latency, and that `Duration::MAX` cannot cut a page.
8. Keep the test name. The bounded-work assertions are the owner-turn guarantee, and the ticket, vault, and prior reports reference this name.

Prefer this repair over quarantine. Use `#[ignore]`, `--test-threads=1`, or a skip only if the repaired test still fails a default-concurrency `--lib` run. That fallback needs an Implement report that names the remaining mechanism. Do not start from quarantine.

## Non-scope

- Do not rewrite `take_snapshot_item_page` incremental accounting. That production change belongs to sibling `ticket_1786912570_127968` ("Hub: make session snapshot paging deterministic").
- Do not change `SESSION_DELIVERY_MAX_ELAPSED`, `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, `DAEMON_MAX_FRAME_BYTES`, owner-loop scheduling, or Pump/Maintenance fairness.
- Do not touch the three sibling tests that keep the same wall-clock assertion pattern: `paged_delivery_stays_within_owner_turn_for_a_large_registry` (`:2660`), `first_session_snapshot_is_complete_and_assembled_in_pages` (`:2726`), and `no_removal_scan_stays_within_owner_turn` (`:2973`). They have not flaked. The established project pattern (prior plan Non-scope) is one flake ticket per test. They are recorded under Risks so the orchestrator can register a sweep ticket.
- Do not absorb write-budget `ticket_1786913892_208903`. The ticket text forbids that explicitly.
- Do not bind full `./test.sh --locked` as this ticket's success criterion. The full script also runs the lifecycle write-budget root owned by the sibling. The binding gate is the default-concurrency `--lib` suite.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins.
- Do not create a pull request.

## Repository ownership boundaries and cross-repo dependencies

Hub owns daemon entity subscription assembly and this lib test. The work stays in Hub, in one test function in `src/daemon_entity_subscriptions.rs`.

No cross-repository prerequisite exists. Do not register a Core, client, Web, or TUI dependency.

Same-target siblings (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Discovered this flake. Ticket text forbids absorption. |
| `ticket_1786912570_127968` | Production incremental snapshot page accounting | Owns any `take_snapshot_item_page` rewrite. Blocked; do not start here. |
| `ticket_1786912569_840742` | Bounded fair owner-loop scheduling | Out of scope. |
| `ticket_1786912572_610381` | Deterministic PTY process lifecycle fixtures | Out of scope. |
| `ticket_1786919220_649402` | Same test, earlier discovery | Closed as duplicate without merge by operator disposition. Its evidence is preserved in this plan. |

## Assumptions and unknowns

Assumption: the observed suite failure is scheduler preemption inflating wall-clock elapsed around a correct bounded call. The isolation evidence in the ticket (branch and base both pass the identical command in isolation) matches that load-sensitive shape, and the assertion that failed is the wall-clock one.

Assumption: with `Duration::MAX`, page shape is fully deterministic at one item per page, because two 41 KiB items always exceed the 64 KiB trial budget. The elapsed budget did not shape pages in green runs either.

Assumption: `Duration::MAX` in this unit test does not weaken production. All production call sites keep `SESSION_DELIVERY_MAX_ELAPSED = 8 ms`. The elapsed-cut path stays exercised by production; the byte-cut and item-cut bounds this test asserts are the deterministic owner-turn work limits.

Assumption: `page.bytes <= SESSION_DELIVERY_MAX_BYTES` holds deterministically, because `take_snapshot_item_page` pops any item whose trial frame encode exceeds `max_bytes`, and `page.bytes` charges item bytes plus separators, which is below the trial encode.

Unknown until Implement: whether the failure reproduces on this worktree before the change. Reproduction is load-dependent and probabilistic; the Implement report should attempt a bounded number of pre-change `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` runs and must not treat non-reproduction as proof of absence. The duplicate ticket already proved that exact command failed on clean `origin/main` `c72712e` under ambient load, so pre-change reproduction is corroborating, not required.

Unknown: whether the three sibling wall-clock tests flake during the acceptance runs. If one does, register a new ticket for it. Do not expand this repair mid-run.

## Affected surfaces/files

- `src/daemon_entity_subscriptions.rs` -- only the `near_limit_snapshot_assembly_stays_within_owner_turn` test body inside `mod tests`.
- `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` -- this plan.
- `docs/reports/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite-implement.md` -- Implement report (Implement step).

No production code changes. No dependency or lockfile changes.

## Risks

- Removing wall-clock assertions could hide a future production latency regression in this path. Mitigation: the byte/item bounds are the production mechanism that limits per-turn work, and the sabotage red-proof shows they detect an unbounded page. Wall-clock latency under suite load was never a reliable detector.
- `Duration::MAX` removes elapsed-cut coverage from this test. Mitigation: production keeps the 8 ms budget; elapsed-cut behavior is a production-path property that the sibling determinism ticket (`ticket_1786912570_127968`) owns testing deterministically.
- The three sibling tests listed under Non-scope keep the same load-sensitive assertion class and will predictably flake next. Recommendation to the orchestrator: register one sweep ticket for the remaining `MAX_OWNER_TURN_MS` wall-clock assertions in this module instead of paying one ticket per future flake.
- The lib suite may flake on an unrelated test during acceptance runs. Follow the prior-art rule: exact evidence or a new ticket; do not absorb.

## Acceptance checks/tests

All commands run in the ticket worktree at default concurrency unless stated. All suite commands use the Hub wrapper `./test.sh`, which checks asset sync, sets `BOTSTER_ENV=test`, and runs workspace scope. Direct `cargo test` invocations do not satisfy these gates.

1. Prebuild precondition (before every suite run set): `cargo build --locked -p botster-core-daemon --bin botster-session-worker`. Review proved the impact: without this build, `./test.sh --locked --lib` produced five worker-missing failures in addition to the target failure; with it, the same suite produced 350 passes and only the target failure.
2. Targeted repetition: `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` passes 20 consecutive runs (shell loop). The duplicate ticket proved this exact command failed pre-repair on `origin/main` `c72712e`, so this command is also the preferred pre-change reproduction probe.
3. Binding default-concurrency gate: `./test.sh --locked --lib` (full workspace lib suites, default test threads) passes 5 consecutive runs with zero failures.
4. Red-proof, per [[a regression test must be shown to go red with the fix reverted]], with two separate negative controls, both run under `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn`:
   - Control A (byte bound): pass `DAEMON_MAX_FRAME_BYTES` as `max_bytes` and keep `SESSION_DELIVERY_MAX_ITEMS` as `max_items`. The first page then accepts exactly 16 items (about 656 KiB), so `page.items <= SESSION_DELIVERY_MAX_ITEMS` still passes and the run must fail first at `page.bytes <= SESSION_DELIVERY_MAX_BYTES`.
   - Control B (item bound): pass `DAEMON_MAX_FRAME_BYTES` as `max_bytes` AND raise `max_items` above 16 (for example `SESSION_DELIVERY_MAX_ITEMS * 2`). The first page then accepts all 20 items (about 820 KiB, under the 1 MiB trial budget), so `page.items` exceeds 16 and the run must fail first at the item assertion, which precedes the byte assertion in the fixed order.
   Record both nonzero exit codes and both failure locations (assertion text and line) in the Implement report, then revert both sabotages. One control cannot prove the other bound: Control A never exceeds the item limit, so only Control B proves the item assertion is live.
5. Strict Rust gates, exact commands: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` both pass.
6. Non-binding smoke: one full `./test.sh --locked` run (no `--lib` filter) may be reported for information. Its lifecycle-suite outcome does not bind this ticket; the write-budget sibling owns that root.
7. Implement report at `docs/reports/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite-implement.md` records: pre-change reproduction attempts, the repaired assertions, red-proof output, the acceptance run tallies, and the preserved `ticket_1786919220_649402` evidence (filtered wrapper failure on its branch and on `origin/main` `c72712e`).

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Extend [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] or add a sibling note: wall-clock `MAX_OWNER_TURN_MS` assertions in default-concurrency lib suites are load-sensitive by construction; the durable idiom is `Duration::MAX` paging plus byte/item work-bound assertions. Name the three remaining assertion sites so the sweep ticket has an inventory.
- Capture that `continue_session_snapshot_assembly` classifies an empty-and-more page as `close_oversized_session_snapshot`, so any test that passes a finite elapsed budget can see a spurious `Closed` under scheduler pause. This is the second ticket where that coupling surfaced.

## Implement steps

1. Run the prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
2. Optionally attempt bounded pre-change reproduction with the filtered wrapper command (corroborating only).
3. Edit the test body per Scope items 1-8. Keep the diff inside the one test function.
4. Run acceptance checks 2-5.
5. Write the Implement report with red-proof, run tallies, and the preserved `ticket_1786919220_649402` evidence.
6. Commit test repair and report. Do not create a PR.
