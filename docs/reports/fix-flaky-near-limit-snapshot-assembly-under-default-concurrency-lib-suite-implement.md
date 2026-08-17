# Implement report: fix flaky near_limit_snapshot_assembly_stays_within_owner_turn

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786921010_869253` |
| Run | `run_1786926789_708317` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786921010_869253` |
| Base | Hub `origin/main` `a55f62d9ba331c0389bc3a2ec79d5b9ed48c7ea7` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Plan | `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` revision 3 (`ea3ea01`) |
| Session-type eligibility consumer | false |
| Implement checklist | `checklist_1786934693_689181` (run-scoped; duplicate `checklist_1786934709_687682` skipped after MCP timeout retry) |

Independent routing: `project_pipelines_current_context(run_id=run_1786926789_708317)` and approved plan revision 3 both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]]
- [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]]
- [[Hub background fairness must stay policy-neutral]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[plugin worker unload deadline can flake under default-concurrency workspace load]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. [[botster runtime teardown lenses]] — teardown class does not apply. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Keep production `continue_session_snapshot_assembly` and `take_snapshot_item_page`.
- Do not rewrite incremental snapshot paging (`ticket_1786912570_127968`).
- Do not change `SESSION_DELIVERY_MAX_ELAPSED`, `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, or `DAEMON_MAX_FRAME_BYTES`.
- Do not absorb write-budget `ticket_1786913892_208903` or the three sibling wall-clock tests.
- Prefer repair over quarantine. Do not start from `#[ignore]` or `--test-threads=1`.
- Binding proof is default-concurrency `./test.sh --locked --lib`. Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/daemon_entity_subscriptions.rs` — repair `near_limit_snapshot_assembly_stays_within_owner_turn` only. Keep the 20-row near-limit fixture. Drive production `continue_session_snapshot_assembly` with `Duration::MAX` and `MAX_NEAR_LIMIT_PAGES = 21`. Assert `page.items >= 1`, then `page.items <= SESSION_DELIVERY_MAX_ITEMS`, then `page.bytes <= SESSION_DELIVERY_MAX_BYTES` on every successful `Continue` page, including the final `more = false` page. Delete the wall-clock `MAX_OWNER_TURN_MS` / `MAX_READY_OPERATION_WAIT_MS` assertions.

Handoff:

- `docs/plans/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite.md` — already committed as plan revision 3 (`ea3ea01`).
- `docs/reports/fix-flaky-near-limit-snapshot-assembly-under-default-concurrency-lib-suite-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns daemon entity subscription assembly and this lib test. Production paging budgets and `take_snapshot_item_page` accounting are unchanged. Core, hub-client, Web, TUI, and package/plugin paths were not edited.

## Cross-repo routing

No cross-repository prerequisite and no PR. Same-target siblings, not absorbed:

| Ticket | Owns |
| --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation that discovered this flake |
| `ticket_1786912570_127968` | Production incremental snapshot page accounting |
| `ticket_1786912569_840742` | Bounded fair owner-loop scheduling |
| `ticket_1786912572_610381` | Deterministic PTY process lifecycle fixtures |
| `ticket_1786919220_649402` | Same test, earlier discovery; closed as duplicate without merge |

## Deviations from plan

None in the test repair. Pre-change filtered-wrapper reproduction on this worktree was not run; the edit landed first. Duplicate-ticket evidence already proved the exact wrapper command failed on its branch and on `origin/main` `c72712e`, so this report treats that evidence as the pre-repair red row rather than a local non-reproduction.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

Production entry point: `continue_session_snapshot_assembly` still uses production `take_snapshot_item_page`. The test now passes `Duration::MAX` only as the test's page-elapsed argument. Production call sites keep `SESSION_DELIVERY_MAX_ELAPSED`.

### Preserved `ticket_1786919220_649402` evidence

- After `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, one default-concurrency `./test.sh --locked --lib` on Hub worktree `project-pipelines/ticket_1786916741_161067`.
- Suite failure: `near_limit_snapshot_assembly_stays_within_owner_turn`, panic `assertion failed: started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS)` at `src/daemon_entity_subscriptions.rs:3033`. Suite result: 349 passed; 2 failed.
- Isolation of the exact test through the wrapper: `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` => FAIL (exit 101) on that ticket branch AND on base `origin/main` `c72712e` with the same exact command.

### Red-proof

Both controls used `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn`. Both sabotages were reverted after the runs.

| Control | Sabotage | Exit | First failure |
| --- | --- | --- | --- |
| A (byte bound) | `max_bytes = DAEMON_MAX_FRAME_BYTES`, `max_items = SESSION_DELIVERY_MAX_ITEMS` | 101 | `assertion failed: page.bytes <= SESSION_DELIVERY_MAX_BYTES` at `src/daemon_entity_subscriptions.rs:3037` |
| B (item bound) | `max_bytes = DAEMON_MAX_FRAME_BYTES`, `max_items = SESSION_DELIVERY_MAX_ITEMS * 2` | 101 | `assertion failed: page.items <= SESSION_DELIVERY_MAX_ITEMS` at `src/daemon_entity_subscriptions.rs:3036` |

Control A never exceeded the item limit. Control B failed first at the item assertion, which precedes the byte assertion.

### Acceptance tallies

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `./test.sh --locked --lib near_limit_snapshot_assembly_stays_within_owner_turn` × 20 | 20/20 PASS |
| `./test.sh --locked --lib` × 5 | 5/5 PASS; each Hub lib crate `351 passed; 0 failed` |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes.

Non-binding full `./test.sh --locked` (lifecycle suite included) was not run. The write-budget sibling owns that root.

## Unverified behavior or residual risk

- Wall-clock owner-turn latency on this path is no longer asserted. Byte and item bounds are the production mechanism that keeps a 25 ms owner turn safe.
- `Duration::MAX` removes elapsed-cut coverage from this test. Production keeps the 8 ms budget. Deterministic elapsed-cut proof belongs to `ticket_1786912570_127968`.
- Three sibling tests still use wall-clock `MAX_OWNER_TURN_MS` assertions in this module: `paged_delivery_stays_within_owner_turn_for_a_large_registry`, `first_session_snapshot_is_complete_and_assembled_in_pages`, and `no_removal_scan_stays_within_owner_turn`. They did not fail these five lib suites. They remain a predictable future flake class.

## Missing vault guidance discovered

Captured to the vault inbox:

- Wall-clock `MAX_OWNER_TURN_MS` assertions in default-concurrency lib suites are load-sensitive by construction. The durable idiom is `Duration::MAX` paging plus per-call byte/item work-bound assertions. Remaining sites in this module are named above.
- `continue_session_snapshot_assembly` classifies an empty-and-more page as `close_oversized_session_snapshot`, so a finite elapsed budget can yield a spurious `Closed` under scheduler pause.
