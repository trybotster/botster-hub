# Plan: Fix flaky separators_close_when_item_bytes_fit_but_commas_do_not

Ticket: `ticket_1786916741_161067`
Run: `run_1786916776_704854`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)

Discovered during write-budget sibling `ticket_1786913892_208903` Implement binding.
That sibling already depends on this ticket. Do not absorb write-budget work here.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` from `list_spawn_targets` |
| Plan worktree | this pipeline worktree (`project-pipelines/ticket_1786916741_161067`) |
| Worktree hygiene | tracked `.gitignore` has 53 bytes and matches `HEAD`; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| `src/daemon_entity_subscriptions.rs` vs origin/main | no diff |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** |
| `teardown_class_applies` | **false** — this is a lib-suite snapshot-assembly unit test flake, not WebRTC/peer, SessionIo/ClientWorker, multi-peer, resource-spin, or terminal-state vs live-runtime work |

Independent resolution: `project_pipelines_current_context` for this run plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Routing did not use the process working directory.

Ambient current_context without `run_id` first returned closed superseded run `run_1786875818_402849`. This plan uses explicit `run_id` `run_1786916776_704854`.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] — planner Must Load. Ownership comes from the Hub charter, not this mixed-generation index.
- [[spa-patterns]] — planner Must Load only. This ticket has no React/SPA edit surface.
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[plan steps need reviewable plan artifacts]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[cross repo dependency registration must use dependency repo target]]
- [[prefer framework and library components over custom solutions]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Targeted atomic notes:

- [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[lifecycle baseline continuation pages measure final encoded size]]
- [[suite wide acceptance criteria make every observed test failure in scope]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[botster test sh forwards arguments to cargo not custom unit flags]]
- [[a poisoned test lock is a symptom not a waiver]]
- [[refresh target branches before mitigating failures owned by sibling tickets]]
- [[Long multi-root test repair loops should be superseded by focused tickets]]
- [[plan review must check open sibling tickets that own part of the plan scope]]
- [[botster hub is a first party host profile over core]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[botster runtime teardown lenses]] — teardown class does not apply
- other repository charters (`botster-core`, `botster-hub-client`, `botster-web`, `botster-tui`, `botster-tui-kit`, `botster-terminal-ghostty`, `botster-workspaces`) — this run stays on Hub tests

## Context loaded

Ticket command: after a locked session-worker build, one `./test.sh --locked` on the write-budget sibling worktree.

Lib-suite failure only. Lifecycle suite did not start.

- Test: `daemon_transport::daemon_entity_subscriptions::tests::separators_close_when_item_bytes_fit_but_commas_do_not`
- Panic: `matches!(result, SnapshotAssemble::Closed { frame_too_large: true })` in `src/daemon_entity_subscriptions.rs`
- Suite result: 351 passed; 1 failed
- Isolated `cargo test --offline --locked --lib separators_close_when_item_bytes_fit_but_commas_do_not`: PASS on the sibling branch and on `origin/main` `c72712e`

Hub report `docs/reports/publish-exact-unix-attach-occupancy-after-connection-eof-implement.md` already records this same test as a pre-existing parallel `--lib` flake on `origin/main`, together with `near_limit_snapshot_assembly_stays_within_owner_turn`.

Production path under test:

1. `continue_session_snapshot_assembly` is the production snapshot assembler used by `SubscribeEntities` and catch-up.
2. It calls `take_snapshot_item_page`, then charges `encoded_item_bytes` plus `snapshot_separator_bytes` plus `snapshot_envelope_bytes`.
3. If that sum exceeds `DAEMON_MAX_FRAME_BYTES` (1 MiB), it calls `close_oversized_session_snapshot` and returns `Closed { frame_too_large: true }`.
4. `take_snapshot_item_page` still clone-and-re-encodes a growing `DaemonEntityFrame::Snapshot` for each candidate. It also yields when `started.elapsed() >= max_elapsed`.

The failing test builds two padded session rows so that:

- item bytes + envelope `<= DAEMON_MAX_FRAME_BYTES`
- item bytes + envelope + comma separators `> DAEMON_MAX_FRAME_BYTES`

It then calls `continue_session_snapshot_assembly` **once** with `max_elapsed = MAX_OWNER_TURN_MS` (25 ms).

Each padded item is about half a megabyte. Encoding a ~0.5 MiB snapshot and then a ~1 MiB snapshot in one 25 ms window is load-sensitive. Under default-concurrency `--lib`, the page often returns after the first item with `more = true`. Then:

- page bytes = one item + zero commas
- assembled + page + envelope still fits
- the function returns `Continue`
- the single-call `Closed` assertion fails

Isolation has enough CPU to take both items in one page, so the post-page comma check fires and the test passes.

This is a test coupling defect, not a separator-accounting policy defect. The comma close path is already present in production `continue_session_snapshot_assembly`.

## Scope

Repair `separators_close_when_item_bytes_fit_but_commas_do_not` so default-concurrency `--lib` cannot fail it by cutting the first page on elapsed time.

Keep the production assembler. Do not add a test-only assemble helper.

Required test behavior:

1. Keep the pad search that proves `without_commas <= DAEMON_MAX_FRAME_BYTES` and `with_commas > DAEMON_MAX_FRAME_BYTES`.
2. Stop requiring both huge items to be taken inside one 25 ms wall-clock page.
3. Drive production `continue_session_snapshot_assembly` with `Duration::MAX`. This test proves separator accounting, not owner-turn latency. A finite budget such as 5 s can still cut an empty page after `Instant::now()` if the scheduler pauses. `Duration::MAX` cannot.
4. If the first call returns `Continue` after accepting one item (byte-trial pop of the second row), loop at most three pages. Two items cannot need more than two useful pages.
5. Accept only `Closed { frame_too_large: true }` plus `DaemonEntityFrame::Error` with `entity_provider_frame_too_large`.
6. Treat an empty-item `Continue` as failure (`page.items > 0`). Do not probe a later `take_snapshot_item_page` after `Closed`. Elapsed-empty `Closed` is unreachable because the page elapsed budget cannot fire.

Prefer this repair over quarantine. Use `#[ignore]`, `--test-threads=1`, or a skip only if the repaired test still fails default-concurrency `--lib` after one clean run. That fallback needs an Implement report that names the remaining mechanism. Do not start from quarantine.

## Non-scope

- Do not rewrite `take_snapshot_item_page` incremental accounting. That production change belongs to sibling `ticket_1786912570_127968` ("Hub: make session snapshot paging deterministic").
- Do not change `DAEMON_MAX_FRAME_BYTES`, owner-loop scheduling, Pump/Maintenance fairness, WebRTC write-budget, PTY fixtures, or session-type eligibility.
- Do not absorb `near_limit_snapshot_assembly_stays_within_owner_turn`. It is a related wall-clock flake. If the binding `--lib` run fails it, register a new ticket. Do not fold it into this repair.
- Do not absorb write-budget `ticket_1786913892_208903`. That sibling already depends on this ticket.
- Do not require a full workspace `./test.sh --locked` as this ticket's success criterion. That command also runs the lifecycle write-budget root owned by the sibling.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream Web/TUI pins.
- Do not create a pull request.

## Repository ownership boundaries and cross-repo dependencies

Hub owns daemon entity subscription assembly and this lib test. The work stays in Hub.

No cross-repository prerequisite. Do not register a Core, client, Web, or TUI dependency.

Same-target siblings (do not absorb):

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Consumer. Already depends on this ticket. |
| `ticket_1786912570_127968` | Production incremental snapshot page accounting | Sibling on a different project, same Hub target. Blocked. Do not start that rewrite here. |
| `ticket_1786912569_840742` | Bounded fair owner-loop scheduling | Out of scope. |
| `ticket_1786912572_610381` | Deterministic PTY process lifecycle fixtures | Out of scope. |
| `ticket_1786661010_115885` | Terminal transport north-star integration | Out of scope. |

No new dependency edge is required from this ticket. The write-budget consumer edge already exists.

## Assumptions and unknowns

Assumption: the observed suite failure is the elapsed-time first-page cut described above. Isolated green on `origin/main` matches that load-sensitive shape.

Assumption: one production `continue_session_snapshot_assembly` path is enough. A second helper would hide the production charge.

Assumption: `Duration::MAX` on this unit test does not weaken production 8 ms `SESSION_DELIVERY_MAX_ELAPSED`. Production callers keep the 8 ms constant.

Assumption: looping one extra page after a one-item `Continue` still proves comma charge, because `snapshot_separator_bytes(1, 1) == 1` matches `snapshot_separator_bytes(0, 2)`.

Unknown until Implement: whether default-concurrency `--lib` also fails `near_limit_snapshot_assembly_stays_within_owner_turn` on this clean `c72712e` worktree. If it does, create a separate ticket. Do not expand this plan.

Unknown: whether Plan Review will ask for a full `./test.sh --locked`. This plan refuses that binding on purpose so write-budget ownership stays on the sibling.

## Affected surfaces/files

- `src/daemon_entity_subscriptions.rs` — only `separators_close_when_item_bytes_fit_but_commas_do_not` and any tiny local helper that test needs
- `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` — this plan
- `docs/reports/fix-flaky-separators-close-under-default-concurrency-lib-suite-implement.md` — Implement report

Production entry point that already uses the behavior: `continue_session_snapshot_assembly` → `close_oversized_session_snapshot` when item bytes plus separators plus envelope exceed the daemon frame limit. This ticket does not add a new production branch. It makes the existing separator-close proof independent of lib-suite CPU contention.

## Risks

- A large elapsed budget plus a single call can still `Continue` if `take_snapshot_item_page` pops the second item because its growing-frame trial exceeds `max_bytes`. The bounded loop covers that path.
- An empty-page `Closed` would be a false green. `Duration::MAX` makes elapsed-empty close unreachable. A post-close probe of a fresh page does not prove the closing call took an item. Do not add that probe.
- Binding `--lib` may expose `near_limit_snapshot_assembly_stays_within_owner_turn`. That is a sibling root, not permission to serialize the suite.
- Expanding into `take_snapshot_item_page` would duplicate `ticket_1786912570_127968` and reopen the superseded multi-root loop.
- Promising full `./test.sh --locked` would put the write-budget lifecycle failure back in this ticket's acceptance.

## Acceptance checks/tests

Use the repo wrapper. `./test.sh` forwards arguments to `cargo test --workspace` under `BOTSTER_ENV=test`.

1. Restore `.gitignore` from `HEAD` if it is empty or missing. Do not truncate it.
2. No `CARGO_TARGET_DIR` override is required. The worktree path has no `:`.
3. Focused: `./test.sh --locked --lib separators_close_when_item_bytes_fit_but_commas_do_not`
4. Binding for this root: one default-concurrency `./test.sh --locked --lib`. Do not add `-- --test-threads=1`. Do not retry for a chance-green result.
5. Strict format and clippy on the touched Hub crate before Review.
6. Isolation `-- --test-threads=1` is diagnostic only.
7. If `--lib` fails a different test, isolate the first non-cascade panic. If it is `near_limit_snapshot_assembly_stays_within_owner_turn` or another named root, register a new ticket. Do not fold it here.
8. Do not treat a green isolated run as suite proof.
9. Ablation: the repaired test must still fail if separator bytes are omitted from the close predicate, or if the test accepts `Continue` as success. The already-observed default-concurrency red row is the load-side negative for the old single-call 25 ms assertion.
10. Downstream consumer proof: none in this run. Write-budget `ticket_1786913892_208903` re-runs its own `./test.sh --locked` after this ticket closes. Do not run that sibling's lifecycle suite as this ticket's gate.
11. No Hub session-type pin, live Hub binary, or SPA request-state proof is required.

## Vault gaps worth capturing

The separator test currently uses `MAX_OWNER_TURN_MS` as a page elapsed budget while encoding two half-megabyte items. [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]] covers quadratic page encode. It does not record that a separator-boundary unit test can flake when that elapsed budget fires after the first item.

Capture after Implement confirms the mechanism. Do not capture from Plan diagnosis alone.

No convention conflict. The Hub charter, suite-wide acceptance note, and focused-ticket note agree: repair this named lib root here; leave production paging and write-budget to their tickets.

## Implement steps

1. Edit only the named test in `src/daemon_entity_subscriptions.rs`.
2. Keep the pad invariant. Remove the 25 ms single-call coupling.
3. Use `Duration::MAX` and a bounded production loop as specified in Scope.
4. Reject empty-item continue. Do not add a fresh page probe after `Closed`.
5. Run focused then one default-concurrency `--lib`.
6. Commit the test plus an Implement report under `docs/reports/`.
7. Do not advance if `--lib` fails this test. Do not retry the same command for luck.
