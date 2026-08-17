# Plan: Fix flaky near_limit_snapshot_assembly_stays_within_owner_turn

Ticket: `ticket_1786921010_869253`
Run: `run_1786926789_708317`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

The write-budget sibling continuation (`ticket_1786913892_208903`) hit one lib-suite failure after integrating Hub main `a55f62d`. The failed test was `daemon_transport::daemon_entity_subscriptions::tests::near_limit_snapshot_assembly_stays_within_owner_turn`. The panic was `assertion failed: started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` at `src/daemon_entity_subscriptions.rs:3033`. The same test passed in isolation on the branch and on base `origin/main` `a55f62d`. This ticket repairs or quarantines that default-concurrency root on botster-hub.

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`, confirmed from the ticket worktree `origin` remote).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22` (from the ticket record; `list_spawn_targets` timed out twice, so the worktree remote is the confirmation path).
- Worktree: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786921010_869253`, branch `project-pipelines/ticket_1786921010_869253`, base `a55f62d` (clean).
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

## Failure mechanism

The test builds 20 session rows. Each row id carries 40 KiB of padding, so each projected item encodes to about 41 KiB. With the 64 KiB page byte budget, each production page accepts exactly one item, because a two-item trial frame exceeds 64 KiB. The loop therefore drives about 20 `Continue` pages and one final send.

Two load-sensitive couplings exist:

1. Lines 3033-3034 assert wall-clock `started.elapsed() < 25 ms` and `< 50 ms` around every call. Under default-concurrency `--lib` (352 tests), the scheduler can preempt the test thread inside or after a call. Wall-clock elapsed includes that preemption, so a correct call can exceed 25 ms. This is the observed failure.
2. The test passes the production 8 ms `SESSION_DELIVERY_MAX_ELAPSED` as the page budget. If the scheduler pauses the thread between `Instant::now()` at `take_snapshot_item_page` entry and the first loop check, the page returns empty with `more = true`. `continue_session_snapshot_assembly` then classifies empty-and-more as `close_oversized_session_snapshot`, and the test's `let else` panics with `near-limit assembly`. This latent path is the same elapsed-empty defect that `a55f62d` removed from the separators test.

Isolated runs pass because an unloaded thread never loses 8 ms or 25 ms to preemption. This is a test coupling defect, not a production regression. Production budgets and paging behavior are correct and stay unchanged.

## Scope

Repair `near_limit_snapshot_assembly_stays_within_owner_turn` so default-concurrency `--lib` cannot fail it through scheduler preemption, while it keeps proving that near-limit snapshot assembly does bounded work per owner-loop call and completes one frame within `DAEMON_MAX_FRAME_BYTES`.

Keep the production assembler. Do not add a test-only assemble helper.

Required test behavior:

1. Keep the 20-row near-limit fixture (about 820 KiB total against the 1 MiB frame limit) and keep driving production `continue_session_snapshot_assembly`.
2. Replace the `SESSION_DELIVERY_MAX_ELAPSED` argument with `Duration::MAX`. Elapsed time then cannot cut a page, so no elapsed-empty `Closed` and no load-dependent page shape exist. Page cuts become purely byte-driven and deterministic: one item per page.
3. Delete the wall-clock assertions `started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` and `started.elapsed() < Duration::from_millis(crate::MAX_READY_OPERATION_WAIT_MS)` and the per-iteration `Instant::now()`. Both constants remain referenced by other tests in this module, so no import cleanup is needed.
4. Replace them with deterministic per-call work bounds, which are what "stays within owner turn" means in production: for every `Continue` page with `more = true`, assert `page.items >= 1` (progress, no empty pages), `page.items <= SESSION_DELIVERY_MAX_ITEMS`, and `page.bytes <= SESSION_DELIVERY_MAX_BYTES`. Bounded bytes and items per call are the mechanism that keeps a 25 ms owner turn safe; wall-clock time under suite load is not a valid proxy for that mechanism.
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

## Assumptions and unknowns

Assumption: the observed suite failure is scheduler preemption inflating wall-clock elapsed around a correct bounded call. The isolation evidence in the ticket (branch and base both pass the identical command in isolation) matches that load-sensitive shape, and the assertion that failed is the wall-clock one.

Assumption: with `Duration::MAX`, page shape is fully deterministic at one item per page, because two 41 KiB items always exceed the 64 KiB trial budget. The elapsed budget did not shape pages in green runs either.

Assumption: `Duration::MAX` in this unit test does not weaken production. All production call sites keep `SESSION_DELIVERY_MAX_ELAPSED = 8 ms`. The elapsed-cut path stays exercised by production; the byte-cut and item-cut bounds this test asserts are the deterministic owner-turn work limits.

Assumption: `page.bytes <= SESSION_DELIVERY_MAX_BYTES` holds deterministically, because `take_snapshot_item_page` pops any item whose trial frame encode exceeds `max_bytes`, and `page.bytes` charges item bytes plus separators, which is below the trial encode.

Unknown until Implement: whether default-concurrency `--lib` reproduces the failure on this clean worktree before the change. Reproduction is probabilistic; the Implement report should attempt a bounded number of pre-change runs and must not treat non-reproduction as proof of absence.

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

All commands run in the ticket worktree at default concurrency unless stated.

1. Targeted isolation: `cargo test --offline --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` passes.
2. Targeted repetition: the same targeted command passes 20 consecutive runs (shell loop).
3. Binding default-concurrency gate: `cargo test --locked --lib` (full lib suite, default test threads) passes 5 consecutive runs with zero failures.
4. Red-proof, per [[a regression test must be shown to go red with the fix reverted]]: temporarily pass `DAEMON_MAX_FRAME_BYTES` as the `max_bytes` argument in the repaired test. The `page.bytes <= SESSION_DELIVERY_MAX_BYTES` (and `page.items` bound) assertions must fail. Revert the sabotage. Record the output in the Implement report.
5. Strict Rust gates per repository convention (fmt/clippy wrappers as configured by the repo scripts) stay green for the touched file.
6. Non-binding smoke: one `./test.sh --locked` run may be reported for information. Its lifecycle-suite outcome does not bind this ticket; the write-budget sibling owns that root.
7. Implement report at `docs/reports/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite-implement.md` records: pre-change reproduction attempts, the repaired assertions, red-proof output, and the acceptance run tallies.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes; the charter's live-Hub proof classes (admission, supervision, package schema) are untouched.

## Vault gaps worth capturing

- Extend [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]] or add a sibling note: wall-clock `MAX_OWNER_TURN_MS` assertions in default-concurrency lib suites are load-sensitive by construction; the durable idiom is `Duration::MAX` paging plus byte/item work-bound assertions. Name the three remaining assertion sites so the sweep ticket has an inventory.
- Capture that `continue_session_snapshot_assembly` classifies an empty-and-more page as `close_oversized_session_snapshot`, so any test that passes a finite elapsed budget can see a spurious `Closed` under scheduler pause. This is the second ticket where that coupling surfaced.

## Implement steps

1. Edit the test body per Scope items 1-8. Keep the diff inside the one test function.
2. Run acceptance checks 1-5. Attempt bounded pre-change reproduction first if cheap (optional, probabilistic).
3. Write the Implement report with red-proof and run tallies.
4. Commit test repair and report. Do not create a PR.
