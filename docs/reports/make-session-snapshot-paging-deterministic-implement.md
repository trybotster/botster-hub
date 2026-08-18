# Implement report: Hub make session snapshot paging deterministic

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786912570_127968` |
| Run | `run_1787092386_910379` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id`; worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | ticket branch `project-pipelines/ticket_1786912570_127968` |
| Base | Hub `origin/main` `674d2df` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `302c7f7` |
| Delivery | direct-merge; no pull request (`merge_policy: direct`) |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Plan | `docs/plans/make-session-snapshot-paging-deterministic.md` revision 5 (Review return `finding_1787095524_314470`) |
| Session-type eligibility consumer | false |

Independent routing: `project_pipelines_current_context` and the approved plan both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Work stayed in the ticket worktree.

Review return `review_1787095524_288549` / `finding_1787095524_314470`: an empty sealed projection skipped the remaining-byte check because that check lived only inside `rows_after`. This visit adds a pre-iteration `ByteBudget` cut when `envelope > remaining` and a production-function test that yields at `envelope - 1` then sends one empty Snapshot at full budget. The committed plan is revision 5 so the acceptance checks match the shipped contract.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]]
- [[empty-and-more snapshot pages close as oversized under elapsed cuts]]
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub background fairness must stay policy-neutral]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]

**Not loaded:** [[project-pipelines-playbook]] — this ticket does not change Project Pipelines package or plugin paths. [[botster runtime teardown lenses]] — teardown class does not apply.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Change only session snapshot page assembly and its unit proofs in `src/daemon_entity_subscriptions.rs`.
- Do not change owner-loop scheduling, PTY routing, WebRTC teardown, local runtime ownership, event delivery priority, budget constants, Core pins, or `DaemonEntityFrame` wire shape.
- Only `OversizedRow` and the cumulative `DAEMON_MAX_FRAME_BYTES` check may close a subscription as oversized.
- Binding proof is one default-concurrency `./test.sh --locked` without retry. Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/daemon_entity_subscriptions.rs` — rewrite `take_snapshot_item_page` to exact incremental item charge with typed cuts (`Complete`, `ItemBudget`, `ByteBudget`, `Elapsed`, `OversizedRow`). Pass the real envelope and a caller-supplied fresh-page capacity from `continue_session_snapshot_assembly`. Close only on `OversizedRow` or the cumulative frame-limit check. Report `DeliveryPage.bytes` as `envelope + page_charge`. Delete the clone-and-reserialize trial frame and the second `encoded_item_bytes` production pass. Convert the three remaining wall-clock owner-turn asserts to per-call work bounds. Add construction tests for item, byte, envelope, oversized-capacity, later-page separator, elapsed yield, and empty-projection envelope admission.

Handoff:

- `docs/plans/make-session-snapshot-paging-deterministic.md` — revision 5 records the empty-projection envelope check.
- `docs/reports/make-session-snapshot-paging-deterministic-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns session snapshot page assembly and session-entity subscribe delivery. Core lifecycle page APIs are consumed unchanged. `DaemonEntityFrame` bytes on the wire are unchanged. Terminal bytes, owner-loop fairness, hub-client DTOs, Web, TUI, Workspaces, and Project Pipelines package/plugin paths were not edited.

## Cross-repo routing

No cross-repository prerequisite and no PR. The registered dependency `ticket_1786913892_208903` is closed and its fix is on the base. Same-target siblings not absorbed: owner-loop scheduling (`ticket_1786912569_840742`) and remaining `daemon_maintenance.rs` wall-clock asserts.

## Deviations from plan

None in product behavior. Revision 5 completes the locked envelope-admission rule for the empty-projection path found by Review. Small necessary adaptations:

- `take_snapshot_item_page` carries `#[allow(clippy::too_many_arguments)]` because the locked parameter list is eight arguments.
- `encoded_item_bytes` is `#[cfg(test)]` after production stopped re-encoding accepted items. The separator test still uses it.
- Existing assembly tests that were not the three named wall-clock sites (`catch_up_restarts_when_a_prefix_id_changes`, `first_session_snapshot_completes_when_caught_up`) now pass `Duration::MAX` so they cannot flip to empty `Elapsed` continue under load after the close-rule change.

## Tests and downstream proof run

Red-when-reverted (restored before commit):

1. Ablate the `ByteBudget` predicate (accept every candidate). `snapshot_byte_budget_includes_envelope_and_yields_empty_cuts` failed: accepted 2 items where remaining `envelope + c1` must accept 1. Restored.
2. Restore the old empty-page oversized close (`items.is_empty() && cut != Complete`). `empty_elapsed_cut_yields_instead_of_closing` failed: `Duration::ZERO` closed instead of yielding. Restored.
3. Ablate the pre-iteration envelope remaining-budget check. `empty_snapshot_yields_when_remaining_budget_is_below_envelope` failed: `page.more` was false because the empty Snapshot sent. Restored.

Focused `--exact` lib tests, each reporting `1 passed` / `382 filtered out`:

- `daemon_transport::daemon_entity_subscriptions::tests::snapshot_item_budget_cuts_and_resumes`
- `daemon_transport::daemon_entity_subscriptions::tests::snapshot_byte_budget_includes_envelope_and_yields_empty_cuts`
- `daemon_transport::daemon_entity_subscriptions::tests::snapshot_page_charges_the_real_envelope_not_a_stub`
- `daemon_transport::daemon_entity_subscriptions::tests::oversized_row_uses_the_fresh_page_capacity_parameter`
- `daemon_transport::daemon_entity_subscriptions::tests::later_page_separator_boundary_closes_oversized_without_livelock`
- `daemon_transport::daemon_entity_subscriptions::tests::empty_elapsed_cut_yields_instead_of_closing`
- `daemon_transport::daemon_entity_subscriptions::tests::empty_snapshot_yields_when_remaining_budget_is_below_envelope`
- `daemon_transport::daemon_entity_subscriptions::tests::first_session_snapshot_is_complete_and_assembled_in_pages`
- `daemon_transport::daemon_entity_subscriptions::tests::separators_close_when_item_bytes_fit_but_commas_do_not`
- `daemon_transport::daemon_entity_subscriptions::tests::oversized_first_snapshot_closes_the_subscription`
- `daemon_transport::daemon_entity_subscriptions::tests::near_limit_snapshot_assembly_stays_within_owner_turn`
- `daemon_transport::daemon_entity_subscriptions::tests::paged_delivery_stays_within_owner_turn_for_a_large_registry`
- `daemon_transport::daemon_entity_subscriptions::tests::no_removal_scan_stays_within_owner_turn`
- `daemon_transport::daemon_entity_subscriptions::tests::catch_up_restarts_when_a_prefix_id_changes`
- `daemon_transport::daemon_entity_subscriptions::tests::first_session_snapshot_holds_until_the_projection_is_caught_up`
- `daemon_transport::daemon_entity_subscriptions::tests::first_session_snapshot_completes_when_caught_up`

Repository gates:

1. `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
2. `cargo fmt --all -- --check`
3. `cargo clippy --workspace --all-targets --locked -- -D warnings`
4. `cargo test --doc --workspace --locked`
5. `./test.sh --locked` at Cargo default concurrency: exit 0, no retry. After the Review-return fix, lib `384 passed` (includes `empty_snapshot_yields_when_remaining_budget_is_below_envelope`). Workspace doctests and member crates stayed green.

Production-path proof:

- `DaemonRequest::SubscribeEntities` still registers through `try_resync_from_projection` → `continue_session_snapshot_assembly` → `take_snapshot_item_page`.
- Owner-loop session delivery still calls `continue_session_snapshot_assembly` while `DeliveryPhase::Assembling`.
- Focused tests execute those production functions. There is no test-only encoder.

Downstream: `DaemonEntityFrame` wire shape is unchanged. No hub-client, Web, TUI, or hub-test-support change.

## Unverified behavior or residual risk

- Default-concurrency `./test.sh --locked` completed exit 0 on this visit. Residual risk is future default-concurrency load, not an unverified gate.
- `DeliveryPage.bytes` now includes the envelope once per assembling page call. The outer owner loop therefore counts one envelope per assembling subscriber per turn. That is conservative and stays at or under `SESSION_DELIVERY_MAX_BYTES` of admitted encode work. It is not a product-visible frame change.
- A snapshot that previously passed the stub-envelope under-charge and now exceeds `SESSION_DELIVERY_MAX_BYTES` or `DAEMON_MAX_FRAME_BYTES` against the real envelope closes as oversized. That is the intended correctness fix.
- Remaining wall-clock asserts in `src/daemon_maintenance.rs` belong to the scheduler sibling and were not converted.

## Missing vault guidance discovered

Plan listed three captures after Implement confirms the change. Inbox captures (vault filenames, not home paths):

1. `empty-elapsed-and-byte-budget-snapshot-pages-are-a-production-oversized-close.md`
2. `typed-snapshot-page-cuts-yield-on-turn-budget-and-close-only-on-data.md`
3. `snapshot-budget-trials-must-charge-the-identity-that-will-be-sent.md`

No convention conflict with the loaded hub charter.
