# Implement report: complete session projection before ready Spawn snapshot delivery

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786938984_190098` |
| Run | `run_1787013066_187598` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786938984_190098` |
| Approved plan | `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` revision v4.1 |
| Plan artifacts | `artifact_1787013708_530188` (v4) plus `artifact_1787013854_780740` (v4.1 addendum) |
| Advisor answer | `question_1787017751_527748` option 1: keep the 24-identity invariant and close the Hub first-snapshot leak |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Implement checklist | ticket `checklist_1786943181_130677`; run `checklist_1787015597_621425` |
| Core pin | `fc541a59338d0591ba4fb3fa522a030d212d26d0` unchanged |

Independent routing: `project_pipelines_current_context` lists `tgt_7e208a0c76a44980a83b63af976b1f22`. The ticket worktree `origin` remote is `trybotster/botster-hub`. Approved plan v4 and addendum v4.1 used the same `target_id` and repository.

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
- Follow approved plan v4 plus addendum v4.1. Keep the 24-identity invariant. Do not weaken the oracle.
- Gate first session snapshots on the journal source watermark. Do not use named Observe for Spawn identity publication.
- Do not change `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, journal/apply/baseline/delivery page caps, or owner-loop scheduling order except a proven wake-chain prefer after apply.
- Do not remint a freeze at the current watermark. Do not start first-subscriber baseline recovery. Do not treat Core `list()` as the identity source.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or the Core pin.
- Do not absorb `ticket_1786977409_499180` or `ticket_1786937228_425608`.
- Do not start an unfiltered lifecycle suite.
- Use `./test.sh` for Rust tests. Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/daemon_maintenance.rs` — store `journal_source_watermark` and `journal_caught_up_confirmed`. `projection_caught_up()` requires a sealed baseline, no gap, no baseline recovery, empty `pending_changes`, matching `source_id`, `cursor.sequence >= watermark.sequence`, a confirming empty pull at the watermark, and every acknowledged Spawn id in `projection.rows`. After this process has returned a successful Spawn, an empty acknowledged set is not caught-up. `sync_acknowledged_spawns` unions runtime ids onto owner-loop state. When acknowledged ids are missing at the watermark, Hub rewinds the journal cursor and holds. After applied changes, `run_projection_apply_slice` prefers `SubscriberDelivery`.
- `src/daemon_entity_subscriptions.rs` — `continue_session_snapshot_assembly` holds with zero progress and `more: true` until `caught_up`. Registration warms journal and apply, inserts an `Assembling` subscription, clears confirmation, and prefers `JournalPull`. It does not assemble the first snapshot on the subscribe turn.
- `src/runtime.rs` — `HubRuntime` records every successful `spawn_session` id in a process-level set before the Spawn reply returns.
- `src/daemon_transport.rs` — a successful `DaemonRequest::Spawn` also records the request session id on maintenance state and on the runtime set, then sets `returned_successful_spawn`.
- `tests/hub_daemon_lifecycle/sessions.rs` — first Snapshot is the 24-identity oracle. Each assemble Spawn and the ready Spawn must return kind `Spawned`. ReadScreen and catch-up Subscribe machinery are gone. Helper oracles remain. Spawn duration is an observation only.
- `tests/hub_daemon_lifecycle/package_event_plane.rs` — keep the Worktrees and WorktreeLifecycle contract. Delete the 50 ms wall-clock gate. Record duration as an observation only.

Handoff:

- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` — resynced to v4.1, including accepted hold-path decisions, process-level Spawn recording, and the `Spawned`-kind oracle.
- `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` — this report.

Merge/rebase cleanup: integrated Hub main `c1ce7e525aef080e10eee79a306482d5bfc66860` (unix unbound-printf lifecycle repair). No file overlap with this ticket's first-snapshot hold. The Unix tests use host `ListSessions` lifecycle after Attach, not session-entity first Snapshot.

## Ownership boundaries preserved

Hub owns the projection-completion policy, the acknowledged-Spawn hold, the owner-loop wake prefers, and the session first-snapshot delivery contract. Core still owns the journal, the watermark, and baseline pages. This branch consumes pinned Core `fc541a59` and does not edit Core, hub-client, Web, TUI, or package/plugin paths. Public `DaemonEntityFrame::Snapshot` shape is unchanged. Page budgets are unchanged.

## Cross-repo routing

No cross-repository prerequisite and no PR.

Same-target siblings, not absorbed:

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786977409_499180` | `ShutdownSession` idempotency and exact-bytes suite-load OperatorError | Registered open dependency. Known-baseline owner. Not absorbed. This leaf ticket does not run a full suite. |
| `ticket_1786937228_425608` | `unix_adapter_unbound_printf_stream_attach_completes` flake | Merged to Hub main at `c1ce7e5`. Integrated here. Not absorbed. |
| `ticket_1787007684_566852` | earlier duplicate production ticket | Closed as duplicate. This ticket owns the repair. |

## Deviations from plan

These deviations are accepted and written back into plan v4.1.

1. Registration does not assemble the first snapshot when the predicate already passes. Subscribe-turn assembly still shipped 19 of 24 identities when Core's observed watermark lagged the last spawn rows. First-subscriber `start_baseline_recovery` was tried and rejected: it cleared journal-applied rows and still sealed a short freeze (3 of 24). Later subscribers still warm journal and apply only.
2. `projection_caught_up()` also requires `journal_caught_up_confirmed`. A non-empty pull can store a watermark that is still behind later spawn rows. Confirmation is true only when a successful pull returns no changes, `page.next == page.source_watermark`, and no journal-advanced wake arrived in that same slice.
3. After applied changes, `run_projection_apply_slice` prefers `SubscriberDelivery`. Plan allowed a wake-chain repair with a unit proof. `projection_apply_prefers_subscriber_delivery_after_applied_changes` is that proof.
4. Existing-subscriber proof is an in-process `HubDaemon` test, not a daemon-child sibling. A child test under `daemon_test_guard` contended after 24-session tests.
5. Comparison choice: latest observed watermark, not a subscribe-time capture. Pulls are 16:1 versus single-row mutations. The fixture set is stable, so the hold terminates. Subscribe-time `list()` is not the identity source: after 24 Spawn replies it can still return 2 rows.
6. After baseline seal, Hub rewinds `projection.cursor.sequence` to 0. Treating the freeze snapshot as the journal cursor made consume a no-op when the freeze already equaled the current watermark.
7. `acknowledged_spawn_ids.iter().all(...)` is vacuously true on an empty set. Advisor `question_1787017751_527748` named this leak. Hub now records every successful Spawn in this process on `HubRuntime` and on maintenance state. After a successful Spawn, an empty acknowledged set is not caught-up. Sync unions, and does not replace, so a later empty runtime read cannot wipe control-path inserts.
8. When acknowledged ids are missing at the watermark, Hub rewinds the journal cursor. It does not remint a freeze at the current watermark.
9. IsolatedHub child stderr is piped and unread. A large `eprintln` on the first-snapshot flush path blocked the owner loop on a full pipe and starved the ready-spawn hello. Production does not eprint that diagnostic.
10. The assemble-session oracle asserts reply kind `Spawned`. `request()` returning `Ok` is not a successful Spawn. A 22-row first Snapshot after an unverified 24-reply loop matched 22 recorded successful Spawns (`00` through `21`).
11. `src/runtime.rs` and `src/daemon_transport.rs` now record Spawn acknowledgements. Plan v4 listed `daemon_transport.rs` as expected untouched.
12. Acceptance check 7 from v4 is superseded: no full lifecycle suite on this leaf ticket.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

Production entry point: `continue_session_snapshot_assembly` is the only session first-snapshot producer. Registration warms journal and apply, then holds. The owner-loop `SubscriberDelivery` slice (`drive_entity_subscriptions`) is the path that sends the first complete snapshot after a confirming empty pull and after every acknowledged Spawn id is in `projection.rows`.

### Red-proof

| Control | Command | Exit | First failure |
| --- | --- | --- | --- |
| Inverted caught-up gate | `./test.sh --locked --lib inverted_caught_up_gate` | 0 | The test forces `caught_up=true` with 8 of 24 rows and asserts the completeness check fails. |
| Empty set after Spawn | `./test.sh --locked --lib projection_caught_up_holds_until_acknowledged` | 0 | An empty acknowledged set with `returned_successful_spawn` is not caught-up. |
| Partial-set readiness | `./test.sh --locked --test hub_daemon_lifecycle_test assemble_readiness_rejects_a_partial_identity_set` | 0 | Helper rejects empty and 17-of-24 sets. |
| Error-frame helper | `./test.sh --locked --test hub_daemon_lifecycle_test assemble_subscription_rejects_an_error_frame` | 0 | Typed Error is not a projected identity set. |

### Acceptance tallies

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 after `#[allow(clippy::too_many_arguments)]` on `continue_session_snapshot_assembly` and production reads of `frame_too_large` |
| `./test.sh --locked --lib caught_up` | 9 passed (predicate holds, complete, inverted gate, inventory rewind, assembly hold) |
| `./test.sh --locked --lib inventory_ahead` | 1 passed; sync unions control-path ids |
| `./test.sh --locked --lib late_projection` | 1 passed |
| `./test.sh --locked --lib existing_session` | 1 passed |
| `./test.sh --locked --lib twenty_four_pending` | 1 passed after starting the backlog unconfirmed |
| `./test.sh --locked --lib projection_apply_prefers` | 1 passed |
| `./test.sh --locked --lib queued_control_precedes` | 1 passed |
| `./test.sh --locked --lib authoritative_mutation` | 1 passed |
| `./test.sh --locked --lib start_baseline_recovery_clears` | 1 passed |
| `./test.sh --locked --lib live_session_entity` | 1 passed |
| `./test.sh --locked --lib oversized_first_snapshot` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test assemble_subscription_rejects` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test assemble_readiness_rejects` | 1 passed |
| `./test.sh --locked --test hub_daemon_lifecycle_test isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree` | 1 passed; duration is an observation only |
| `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` × 20 | 20 pass, 0 fail after the process-level ack hold and `Spawned`-kind asserts; both ready_spawn tests green |

Downstream proof: not required. No public DTO, pin, or client-contract change.

Non-binding loaded-daemon workflow dispatch was not run. A full lifecycle suite was not started.

### Known-baseline

`ticket_1786977409_499180` still owns `external_hub_webrtc_live_output_preserves_exact_bytes` suite-load OperatorError. This visit did not reproduce it because it did not run the unfiltered suite.

## Unverified behavior or residual risk

- The confirming empty pull adds one extra journal page after the last applied change before the first snapshot can complete. A busy hub that never observes an empty page at the watermark can delay first snapshots. Pulls outpace single-row mutations 16:1, and the fixture set is stable.
- The in-process existing-subscriber test drives owner-loop slices directly. It does not prove the daemon-child socket path for the post-snapshot Upsert. The ready-spawn integration test covers the first-snapshot identity contract through a real CLI daemon child.
- `continue_session_snapshot_assembly` now has eight arguments. The extra `caught_up` flag is required for the inverted-gate unit proof. Clippy is allowed on that function only.
- Leftover IsolatedHub children from earlier failed loops can exhaust session-worker capacity. The oracle now fails at Spawn when a reply is not `Spawned`, instead of flushing a short snapshot of the successful prefix.
- Open dependency `ticket_1786977409_499180` still blocks final integration. It does not block this focused Implement gate.
- Capture of the watermark-gated snapshot convention is deferred until after merge, as the plan states.

## Missing vault guidance discovered

None that blocked implementation. Existing notes already reject wall-clock ready-operation oracles and require a deterministic identity invariant.

Capture after merge, not this visit:

- Hub first session snapshots complete at the journal source watermark after a confirming empty pull, not at baseline seal.
- After a successful Spawn in this process, an empty acknowledged-id set must not count as caught-up.
- Update [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] to point at this oracle and to drop the `package_event_plane.rs` inventory sentence.
