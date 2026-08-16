# Implement report: fix flaky separators_close under default-concurrency lib suite

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786916741_161067` |
| Run | `run_1786916776_704854` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786916741_161067` |
| Base | Hub `origin/main` `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Plan | `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` |
| Session-type eligibility consumer | false |
| Implement checklist | `checklist_1786918885_659440` (run-scoped; Plan already owned `checklist_1786917253_254009`) |

Independent routing: `project_pipelines_current_context(run_id=run_1786916776_704854)` and the approved plan both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Work stayed in the ticket worktree.

Review return for `finding_1786919736_178031`: the first Implement visit used `Duration::from_secs(5)` plus a post-close `take_snapshot_item_page` probe. That oracle could accept elapsed-empty `Closed`. This visit uses `Duration::MAX` and removes the probe.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
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
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation reports separate merge cleanup from feature behavior]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]
- [[implementation deviations must resync committed plan acceptance checks]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. [[botster runtime teardown lenses]] — teardown class does not apply. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Keep production `continue_session_snapshot_assembly` and `take_snapshot_item_page`.
- Do not rewrite incremental snapshot paging (`ticket_1786912570_127968`).
- Do not absorb write-budget (`ticket_1786913892_208903`) or owner-loop/PTY siblings.
- Prefer repair over quarantine. Do not start from `#[ignore]` or `--test-threads=1`.
- Binding proof is one default-concurrency `./test.sh --locked --lib`. Do not retry that command for luck.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/daemon_entity_subscriptions.rs` — repair `separators_close_when_item_bytes_fit_but_commas_do_not` only. Keep the pad search. Drive production `continue_session_snapshot_assembly` with `Duration::MAX` and a bounded three-page loop. Reject empty-item `Continue`. Do not probe a later page after `Closed`. Accept only `Closed { frame_too_large: true }` plus `entity_provider_frame_too_large`.

Handoff:

- `docs/plans/fix-flaky-separators-close-under-default-concurrency-lib-suite.md` — resynced to `Duration::MAX` and no post-close probe after Review `finding_1786919736_178031`.
- `docs/reports/fix-flaky-separators-close-under-default-concurrency-lib-suite-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns daemon entity subscription assembly and this lib test. The production comma close path is unchanged. Core, hub-client, Web, TUI, and package/plugin paths were not edited.

## Cross-repo routing

No cross-repository prerequisite and no PR. Same-target follow-up tickets registered from the binding `--lib` run, not absorbed:

| Ticket | Owns |
| --- | --- |
| `ticket_1786919220_649402` | `near_limit_snapshot_assembly_stays_within_owner_turn` wall-clock flake |
| `ticket_1786919221_923340` | `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` lib-suite load flake |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation (already depends on this ticket) |
| `ticket_1786912570_127968` | Production incremental snapshot page accounting |

## Deviations from plan

Review `finding_1786919736_178031` required `Duration::MAX` instead of the plan's original 5 s example, and removal of the post-close `take_snapshot_item_page` probe. The committed plan now matches that contract. The 5 s budget could still cut an empty page after `Instant::now()`. A later probe does not prove the closing call took an item.

Process additions from the first Implement visit remain:

- Ran `cargo build --locked -p botster-core-daemon --bin botster-session-worker` before the binding `--lib` suite.
- Binding `--lib` still fails registered `near_limit` `ticket_1786919220_649402`. Did not retry `./test.sh --locked --lib`.
- Created run-scoped Implement vault checklist `checklist_1786918885_659440`.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

| Command | Result |
| --- | --- |
| First visit focused `./test.sh --locked --lib separators_close_when_item_bytes_fit_but_commas_do_not` | pass |
| First visit ablation omitting `snapshot_separator_bytes` | fail, exit 101 |
| Review return focused wrapper after `Duration::MAX` | pass (1 passed; 350 filtered) |
| Review return ablation omitting `snapshot_separator_bytes` | fail, exit 101, panic `completed snapshot without charging separators` |
| Restore then focused wrapper | pass |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| Review return one default-concurrency `./test.sh --locked --lib` | exit 101. 350 passed; 1 failed. Named test not among failures. Only registered `near_limit_snapshot_assembly_stays_within_owner_turn` failed. Did not retry. |
| `cargo fmt --all -- --check` | exit 0 |
| `git diff --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

`-- --test-threads=1` was not used as a suite command.

Production entry point already using the behavior: `SubscribeEntities` / catch-up → `continue_session_snapshot_assembly` → `close_oversized_session_snapshot` when item bytes plus separators plus envelope exceed `DAEMON_MAX_FRAME_BYTES`. This ticket does not add a production branch. It makes the existing separator-close proof independent of lib-suite CPU contention.

Downstream consumer proof: none in this run. Write-budget `ticket_1786913892_208903` re-runs its own `./test.sh --locked` after this ticket closes.

## Unverified behavior or residual risk

- Full workspace `./test.sh --locked` (lifecycle suite) was not this ticket's gate. Write-budget owns that surface.
- `near_limit_snapshot_assembly_stays_within_owner_turn` still fails the binding `--lib` run. Follow-up `ticket_1786919220_649402`.
- `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds` did not fail this return `--lib` run. Follow-up `ticket_1786919221_923340` remains for the earlier suite-load observation.
- `finding_1786919736_178031` is addressed: `Duration::MAX` cannot cut a page, the post-close probe is gone, and empty-item `Continue` still fails the test.
- `Duration::MAX` does not change production `SESSION_DELIVERY_MAX_ELAPSED` (8 ms).

## Missing vault guidance discovered

[[Snapshot page accounting must charge incremental item bytes not a growing frame encode]] covers quadratic page encode. It did not record that a separator-boundary unit test flakes when `MAX_OWNER_TURN_MS` fires after the first half-megabyte item.

Captured after Implement confirmed the mechanism:

- inbox `separator-boundary-unit-tests-flake-when-owner-turn-budget-cuts-the-first-page.md`

No convention conflict. Hub charter, focused-ticket note, and the approved plan agree: repair this named lib root here; leave production paging, write-budget, near-limit latency, and the WebRTC cleanup flake to their tickets.
