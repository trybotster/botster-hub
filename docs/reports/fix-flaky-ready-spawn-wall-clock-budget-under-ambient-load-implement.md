# Implement report: complete session projection before ready Spawn snapshot delivery

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786938984_190098` |
| Run | `run_1787013066_187598` |
| Step | `botster_stack_implement` (`run_step_1787028395_474758`, third Review return) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786938984_190098` |
| Approved plan | `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` revision v4.3 |
| Plan artifacts | `artifact_1787013708_530188` (v4) plus `artifact_1787013854_780740` (v4.1 addendum) |
| Review bounce | `review_1787028380_171923` `changes_required` after `review_1787027652_739418` and `review_1787026690_762581` |
| Advisor answer | `question_1787017751_527748` option 1: keep the 24-identity invariant and close the Hub first-snapshot leak |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Implement checklist | ticket `checklist_1786943181_130677`; run `checklist_1787015597_621425` |
| Core pin | `fc541a59338d0591ba4fb3fa522a030d212d26d0` unchanged |

Independent routing: `project_pipelines_current_context` lists `tgt_7e208a0c76a44980a83b63af976b1f22`. The ticket worktree `origin` remote is `trybotster/botster-hub`. Approved plan v4 through v4.3 used the same `target_id` and repository.

This visit repairs the two open findings from `review_1787028380_171923`. It removes the unused production journal-capacity environment option and updates stale retained-floor plan text. Prior remint, pending-ack retire, and no-`list()` behavior stay.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[lifecycle baseline page freeze uses excluded IDs and copy on write]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

**Not loaded:** [[project-pipelines-playbook]] as a repository charter overlay. Project Pipelines package and plugin paths are out of scope. Workflow MCP tools were used. [[botster runtime teardown lenses]] was not loaded. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Follow approved plan v4.1 plus Review `review_1787026690_762581`. Keep the 24-identity invariant.
- Gate first session snapshots on the journal source watermark and pending Spawn acknowledgements. Do not use named Observe for Spawn identity publication.
- Do not change `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, journal/apply/baseline/delivery page caps, or owner-loop scheduling order except the proven wake-chain prefer after apply.
- Do not remint a freeze at the current watermark. Do not start first-subscriber baseline recovery. Do not treat Core `list()` as the identity source. Do not call unbounded `list()` from subscriber delivery.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or the Core pin.
- Do not absorb `ticket_1786977409_499180`.
- Do not start an unfiltered lifecycle suite.
- Use `./test.sh` for Rust tests. Direct merge. Do not create a pull request.

## Files changed

Second Review-return production path:

- `src/daemon_maintenance.rs` — `CursorExpired` starts fresh bounded baseline recovery. Normal seal still keeps the sealed snapshot cursor. One omitted-row recover may probe sequence 0; if that cursor expired, Hub remints. Pending-ack retire from the prior visit stays.
- `src/runtime.rs` — test-only `with_test_lifecycle_journal_capacity` so a discarded-prefix test can use journal capacity 2. `retire_acknowledged_spawn` stays. No production environment option.

Handoff:

- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` — resynced to v4.3.
- `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` — this report.

## Ownership boundaries preserved

Hub owns the projection-completion policy, pending-Spawn hold and retire, sequence-0 omitted-row probe, `CursorExpired` remint, owner-loop wake prefers, and the session first-snapshot delivery contract. Core still owns the journal, the watermark, and baseline pages. This branch consumes pinned Core `fc541a59` and does not edit Core, hub-client, Web, TUI, or package/plugin paths. Public `DaemonEntityFrame::Snapshot` shape is unchanged. Page budgets are unchanged. Subscriber delivery does not call Core `list()`.

## Cross-repo routing

No cross-repository prerequisite and no PR.

Same-target siblings, not absorbed:

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786977409_499180` | `ShutdownSession` idempotency and exact-bytes suite-load OperatorError | Known-baseline owner. Not absorbed. This leaf ticket does not run a full suite. |
| `ticket_1786937228_425608` | `unix_adapter_unbound_printf_stream_attach_completes` flake | Merged to Hub main at `c1ce7e5`. Integrated earlier. Not absorbed. |
| `ticket_1787007684_566852` | earlier duplicate production ticket | Closed as duplicate. This ticket owns the repair. |

## Deviations from plan

These deviations are accepted and written back into plan v4.3.

1. `CursorExpired` remints a fresh baseline (`finding_1787027652_648073`). Replaying the retained suffix cannot reconstruct discarded prefix Upserts or removals. Normal completion still keeps the sealed cursor, so seal does not rewind to 0 and remint forever.
2. The discarded-prefix proof uses a `cfg(test)` thread-local journal capacity of 2. The unused production `BOTSTER_HUB_TEST_LIFECYCLE_JOURNAL_CAPACITY` branch was removed (`finding_1787028380_382065`).
3. This visit ran `ready_spawn_completes` 20/20 on the tree that contains the remint (`finding_1787027652_881975`).
4. Earlier v4.2 deviations remain: pending-ack retire, no subscriber-delivery `list()`, production-guard ablation red-proof, observe-slice `Spawned` and first-Snapshot asserts.
5. Earlier v4.1 deviations remain: no register-time assembly, confirming empty pull, apply prefers `SubscriberDelivery`, in-process existing-subscriber proof, latest observed watermark, IsolatedHub stderr is unread, `Spawned`-kind oracle, no full suite.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

Production entry point: `continue_session_snapshot_assembly` is the only session first-snapshot producer. Registration warms journal and apply, then holds. The owner-loop `SubscriberDelivery` slice (`drive_entity_subscriptions`) is the path that sends the first complete snapshot after a confirming empty pull and after every pending Spawn id is in `projection.rows`.

### Red-proof

Temporary ablation: remove `!caught_up ||` from the production guard in `continue_session_snapshot_assembly`. Do not leave that ablation in the tree.

| Control | Command | Exit | First failure |
| --- | --- | --- | --- |
| Ablated production `caught_up` guard | `./test.sh --locked --lib first_session_snapshot_holds_until_the_projection_is_caught_up` | 101 | `src/daemon_entity_subscriptions.rs:3442` `left: 16` `right: 0` (`page.items`) |
| Restored production guard | same command | 0 | hold stays; no snapshot frames |
| Ablated integration completeness | `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes_during_session_snapshot_assembly` × 8 | 0 | stays green; registration no longer assembles, so the owner loop can consume the journal before the first Snapshot read |
| Retired ack after remove | `./test.sh --locked --lib projection_caught_up_after_pending_ack` | 0 | later first snapshots are not held |
| CursorExpired remint | `./test.sh --locked --lib cursor_expired` | 0 | stale rows clear; discarded-prefix test reconstructs `keep-session` and releases the pending hold |
| Partial-set readiness | `./test.sh --locked --test hub_daemon_lifecycle_test assemble_readiness_rejects_a_partial_identity_set` | 0 | helper rejects empty and 17-of-24 sets |
| Error-frame helper | `./test.sh --locked --test hub_daemon_lifecycle_test assemble_subscription_rejects_an_error_frame` | 0 | typed Error is not a projected identity set |

### Acceptance tallies

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `./test.sh --locked --lib projection_caught_up` | 6 passed |
| `./test.sh --locked --lib missing_pending` | 1 passed; one recover, no loop |
| `./test.sh --locked --lib cursor_expired` | 2 passed; remint plus discarded-prefix reconstruction |
| `./test.sh --locked --lib late_projection` | 1 passed; seal keeps a snapshot cursor |
| `./test.sh --locked --lib twenty_four_pending` | 1 passed |
| `./test.sh --locked --lib first_session_snapshot` | 3 passed |
| `./test.sh --locked --lib projection_apply_prefers` | 1 passed |
| `./test.sh --locked --lib queued_control_precedes` | 1 passed |
| `./test.sh --locked --lib existing_session` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test assemble_subscription_rejects` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test assemble_readiness_rejects` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test first_session_snapshot_arrives_after_projected_spawn_is_removed` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` × 20 on `c27b39c` | 20 pass, 0 fail. This cleanup visit did not rerun 20/20 because the production hold path did not change. |

Downstream proof: not required. No public DTO, pin, or client-contract change.

Non-binding loaded-daemon workflow dispatch was not run. A full lifecycle suite was not started.

### Known-baseline

`ticket_1786977409_499180` still owns `external_hub_webrtc_live_output_preserves_exact_bytes` suite-load OperatorError. This visit did not reproduce it because it did not run the unfiltered suite.

## Unverified behavior or residual risk

- The confirming empty pull adds one extra journal page after the last applied change before the first snapshot can complete. A busy hub that never observes an empty page at the watermark can delay first snapshots. Pulls outpace single-row mutations 16:1, and the fixture set is stable.
- If journal retention drops a pending Spawn Upsert before recover runs, that pending id stays outstanding and later first snapshots stay held. Default journal capacity is 1024. The 24-session fixture stays inside that window.
- The in-process existing-subscriber test drives owner-loop slices directly. It does not prove the daemon-child socket path for the post-snapshot Upsert. The ready-spawn integration test covers the first-snapshot identity contract through a real CLI daemon child.
- Leftover IsolatedHub children from earlier failed loops can exhaust session-worker capacity. The oracle fails at Spawn when a reply is not `Spawned`.
- A trimmed pending Spawn Upsert can still hold later first snapshots until a later remint includes that session. The discarded-prefix test proves the live-session remint path.
- Capture of the watermark-gated snapshot convention is deferred until after merge, as the plan states.

## Missing vault guidance discovered

None that blocked implementation. Existing notes already reject wall-clock ready-operation oracles, require a deterministic identity invariant, and require bounded owner-loop lifecycle pages.

Capture after merge, not this visit:

- Hub first session snapshots complete at the journal source watermark after a confirming empty pull, not at baseline seal.
- After a successful Spawn in this process, record a pending id. Empty pending after retire is not the vacuous leak.
- Update [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] to point at this oracle and to drop the `package_event_plane.rs` inventory sentence.
