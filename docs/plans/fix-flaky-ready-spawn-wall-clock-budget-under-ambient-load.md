# Plan: Complete session projection before ready Spawn snapshot delivery

Ticket: `ticket_1786938984_190098`
Run: `run_1787013066_187598` (fresh Plan visit on the consolidated scope)
Revision: v4.1. This revision amends v4 (`0005264`, `artifact_1787013708_530188`) with `artifact_1787013854_780740`. The user consolidated scope after independent Cursor and Fable audits. `ticket_1787007684_566852` is closed as an explicit duplicate. This ticket now owns both the Hub projection-completion repair and its deterministic ready-spawn proof. The park recorded by `question_1787007562_222481` option 3 is resolved: the production convergence path is now in this ticket. The 24-identity invariant stands. Production budgets stay unchanged. Audit ratification corrections that landed after v4 add `package_event_plane.rs` oracle repair and replace the full-suite smoke with focused proofs. Implement also records accepted hold-path decisions below.
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Step: Plan (`botster_stack_plan`)

## Problem

Hub marks a session subscription's first snapshot complete too early.

- `register_entity_subscription` runs one `JournalPull` slice and one `ProjectionApply` slice, then treats `baseline_complete && !gap` as complete (`src/daemon_entity_subscriptions.rs:278-292`).
- `continue_session_snapshot_assembly` holds only on `!baseline_complete || gap` (`src/daemon_entity_subscriptions.rs:1126`). It flushes the first `Snapshot` frame when it exhausts the current `projection.rows`, even when unapplied journal rows exist.
- `JournalPull` and `ProjectionApply` are capped at 16 changes per slice (`JOURNAL_PAGE_MAX_CHANGES`, `APPLY_MAX_CHANGES`, `src/daemon_maintenance.rs:45-47`). With 24 spawned sessions, one pull cannot cover the backlog.
- Result: after `Spawn(S)` succeeds, a later subscription can receive a "complete" first snapshot that omits S. The integration test `ready_spawn_completes_during_session_snapshot_assembly` observed 21/24, 17/24, and 8/24 identities across prior repair attempts. Test-only stimuli (first-frame count, 30-second quiet drain, catch-up Subscribes, per-session ReadScreen) were all nondeterministic, per `question_1787007562_222481`.

## Verified mechanism facts (read this session, current worktree)

1. Core appends the Spawn journal row before the Spawn reply. `CoreDaemon::spawn` ends with `self.append_lifecycle_upsert(&record, ...)` (Core pinned rev `fc541a5`, `crates/botster-core-daemon/src/daemon.rs:508-541`). So at subscribe time, every prior successful Spawn already has a journal row at or below the current source watermark.
2. Core already exposes the watermark. `SessionLifecyclePage.source_watermark: SessionLifecycleCursor` ("Current source watermark", Core `api.rs:157`). Hub reads it today only as a transient wake condition (`page.next != page.source_watermark`, `src/daemon_maintenance.rs:618`) and stores nothing. No Core change is needed.
3. The owner loop self-drives catch-up. Spawn's control handler calls `note_authoritative_mutation()` (`src/daemon_transport.rs:2395`) which sets the wake and prefers `JournalPull`. `run_journal_pull_slice` re-wakes while `page.next != page.source_watermark`; `run_projection_apply_slice` re-wakes while `pending_changes` remain; `run_one_owner_maintenance_slice` re-wakes while `session_subscribers_need_delivery` (`src/daemon_transport.rs:236-247`); the serve loop runs one slice per turn while `needs_work()` and has a 500 ms reconciliation floor (`src/daemon_transport.rs:285-374`). Queued control still precedes a due slice (`classify_owner_poll`, proven by `queued_control_precedes_a_due_maintenance_slice`, `src/daemon_transport.rs:7858`).
4. `MaintenanceState` has no watermark field today (`src/daemon_maintenance.rs:435-448`). `SessionProjection` tracks `cursor`, `baseline_complete`, `gap`, `rows` (`src/session_projection.rs:32-40`).

## Target repository and target_id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`. Resolution: `list_spawn_targets` timed out this session; I resolved the target from the project record instead. All 28 tickets on this target_id are "Hub:" tickets, the run's pipeline-provided worktree remote is `trybotster/botster-hub.git`, and the prior accepted plan for this ticket recorded the same resolution. The other project targets map to Core, Web, TUI, and TUI Kit ticket families.
- Worktree: branch `project-pipelines/ticket_1786938984_190098` at `4e1f0f0`, clean. Base ref: `main`.
- Hygiene: tracked `.gitignore` present, 53 bytes; no colon in the worktree path; no `CARGO_TARGET_DIR` override needed.

## Repository playbook loaded

- [[botster-hub-playbook]] — Hub owns the daemon transport, the owner loop, session projection, and this lifecycle suite.

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]] — generic Plan role contract.
- [[botster-planner-playbook]] — Botster planning overlay, completion evidence, worktree hygiene.
- [[Hub session projection continues without subscribers or terminal Drain]] — the projection is canonical with zero subscribers; lifecycle observation and journal pages are the source.
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]] — wake, page, and baseline reads do not advance session runtimes; observe remains the progress path for exits.
- [[lifecycle baseline page freeze uses excluded IDs and copy on write]] — a freeze minted at the current watermark excludes later Spawn ids; journal confirm at that watermark can still omit live registry rows.
- [[observed-exit waits must issue a production exact-session observe turn]] — Observe remains the exit-freshness path. This ticket must not reuse it for Spawn identity publication.
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] — end-to-end elapsed time is not an ordering oracle; the decision-level unit proof stands.
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] — unit oracles assert work bounds, not elapsed time.
- [[conformance harnesses gate on deterministic invariants not timing]] — durations stay observations.
- [[Owner loop must not stack maintenance and pump ahead of queued control]] — the ready-spawn precedence invariant this ticket keeps proving.
- [[a regression test must be shown to go red with the fix reverted]] — the completion gate needs a deterministic red proof.
- Runtime-teardown class: does not apply. This ticket changes projection completion and snapshot delivery. It has no WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, resource-spin, or terminal-state divergence surface. [[botster runtime teardown lenses]] not loaded, per the class rule.

## Context loaded

- `project_pipelines_current_context`: consolidated ticket text, four answered questions (`question_1786977344_650479`, `question_1787003911_553236`, `question_1787005268_112714`, `question_1787007562_222481`), fresh run record, gate contract.
- Code read this session: `src/daemon_entity_subscriptions.rs` (registration, `DeliveryPhase`, `continue_session_snapshot_assembly`, `take_snapshot_item_page`, reset), `src/daemon_maintenance.rs` (slice kinds, scheduler, `MaintenanceState`, journal/apply/baseline slices), `src/daemon_transport.rs` (serve loop, `classify_owner_poll`, `run_one_owner_maintenance_slice`, Spawn wake), `src/session_projection.rs`, `tests/hub_daemon_lifecycle/sessions.rs:3540-3825`, Core pinned sources for `spawn` and `SessionLifecyclePage`.
- Branch history: `48545e1`..`4e1f0f0`. Keep: the deterministic busy-path unit oracle (`38b27b7`), the single-allocation control fix (`c550f1c`), docs. Replace: the test-only catch-up drain machinery (`b3432e4`, `9763afd`, `4e1f0f0` test body) with the deterministic first-frame oracle backed by the production gate below.
- `test.sh`: `BOTSTER_ENV=test cargo test --workspace`; targeted `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` works.

## Scope

Production change (Hub only):

1. Capture the source watermark. Add watermark tracking to `MaintenanceState` (for example `journal_source_watermark: Option<SessionLifecycleCursor>`). `run_journal_pull_slice` stores `page.source_watermark` on every successful page. Clear it on resync and on baseline-recovery start. Compare `source_id` before trusting a stored watermark.
2. Define one projection-completion predicate on `MaintenanceState` (`projection_caught_up()`): baseline sealed (`projection.baseline_complete`), no gap, no in-progress baseline recovery, `pending_changes` empty, stored watermark present with matching `source_id`, `projection.cursor.sequence >= watermark.sequence`, and `journal_caught_up_confirmed`. Confirmation is true only after a successful pull returns no changes, `page.next == page.source_watermark`, and no journal-advanced wake arrived in that same slice. A non-empty pull or a wake-stale empty pull can store a watermark that is still behind later spawn rows, so cursor-versus-watermark alone is not enough.
3. Gate first-snapshot completion on that predicate. `continue_session_snapshot_assembly` holds (returns a zero-progress page with `more: true`, as the current `!baseline_complete || gap` branch does) until the predicate passes. Registration does not assemble the first snapshot. Registration does not start baseline recovery on the first session subscriber: that refresh cleared journal-applied rows and still sealed a short freeze (3 of 24 in a focused gate). Instead, `drive_entity_subscriptions` calls `refresh_projection_if_inventory_ahead` before assembly. That helper is a read of Core `list()`. It does not publish Spawn identity and does not replace Observe. When a baseline page completes, Hub seals membership and then rewinds `projection.cursor.sequence` to 0 so the owner loop consumes the journal to the source watermark. Treating the freeze snapshot as the journal cursor made `projection_caught_up()` true while omitted Spawn rows were already at that watermark. A successful `DaemonRequest::Spawn` records the session id in `acknowledged_spawn_ids`. `projection_caught_up()` stays false until the projection contains every acknowledged id. Core `list()` after 24 Spawn replies can still return 2 rows, so subscribe-time list() is not the identity source. When `list()` is still ahead after journal consume, Hub rewinds the cursor again and holds. It does not remint a freeze at the current watermark. Later session subscribers warm journal and apply only. Registration inserts an `Assembling` subscription, clears confirmation, and prefers `JournalPull` so a later owner-loop pull must confirm emptiness at the watermark. The held subscription keeps `needs_delivery = true`. After applied changes, `run_projection_apply_slice` prefers `SubscriberDelivery` so the owner loop can flush the complete snapshot without waiting for the reconciliation floor. A journal-advanced wake does not remint recovery while a gap pass is already armed.
4. Keep every bounded page budget unchanged: `JOURNAL_PAGE_MAX_CHANGES`, `APPLY_MAX_CHANGES`, `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, delivery page budgets. Completion is reached across turns, not inside one turn.
5. Do not use named Observe for Spawn identity publication. Spawn rows reach the projection through the journal (fact 1). Observe remains the exit-freshness path. A `ListSessions` read may hold first-snapshot completion when the sealed projection is short of Core inventory. That read does not advance lifecycle.

Test changes (`tests/hub_daemon_lifecycle/sessions.rs`, plus lib tests):

6. Rewrite `ready_spawn_completes_during_session_snapshot_assembly` on the production gate: spawn the 24 `assemble-session-NN` sessions, assert each reply kind is `Spawned`, subscribe, assert ready `Spawn` succeeds with kind `Spawned`, then read entity frames until the first `Snapshot` frame arrives. Assert the snapshot contains all 24 assemble identities (superset allowed: the ready-spawn row may legitimately be included). Fail immediately on `DaemonEntityFrame::Error`. Delete the `ReadScreen` observe loop and the catch-up-Subscribe machinery (`observe_assemble_sessions`, `drain_until_assemble_sessions_projected`, `MAX_CATCHUP_SUBSCRIBES`). Keep the helper-oracle tests (`assemble_subscription_rejects_an_error_frame`, `assemble_readiness_rejects_a_partial_identity_set`), adjusted to the surviving helpers. Keep unsubscribe and clean shutdown. Record the Spawn duration as an observation only. Use a generous read timeout as a liveness bound only, never as a pass/fail latency assertion.
7. Existing-subscriber proof: an in-process `HubDaemon` test (`existing_session_subscriber_receives_spawn_upsert_without_another_request`) subscribes, receives the first snapshot, spawns S, then reads an `Upsert`/`Patch` for S with no further client request. A daemon-child sibling under the shared `daemon_test_guard` is not required; it contended for the guard after 24-session tests and is not the production path. The apply-to-delivery unit test covers the wake after applied changes.
8. Cold/late-projection recovery proof: prove a projection that starts after sessions exist recovers all identities through baseline pages plus journal consume to the watermark. Primary form: a lib-level test that drives `run_maintenance_kind` (Baseline → JournalPull → ProjectionApply) against a real `HubRuntime` with more than 16 live sessions, then asserts the completion predicate and full identity coverage. If the existing daemon-restart integration idiom (durable workers per [[hub shutdown preserves durable session workers]]) supports it cheaply, add the integration variant; otherwise document why the lib-level proof is the charter-level recovery evidence.
9. Backlog-depth unit proof: completion predicate unit tests in `src/daemon_maintenance.rs` / `src/daemon_entity_subscriptions.rs`: (a) assembly holds while `pending_changes` is non-empty; (b) assembly holds while `cursor < watermark`; (c) assembly completes when caught up; (d) a 24-row backlog (more than one 16-row page) reaches completion across multiple slices with unchanged budgets.
10. Red-on-revert proof: an inverted-predicate unit helper (repo convention from `38b27b7`) proves the gate is load-bearing: with the gate forced to "caught up" while pending changes remain, the completeness assertion fails. Integration-level revert evidence is recorded as observational only, because with the gate reverted the first-frame identity count races (prior runs passed 3 of 4 repetitions); the deterministic red proof lives at the unit level.
11. Keep `ready_spawn_completes_when_live_sessions_exceed_one_observe_slice` and the Layer-1 unit ordering proof `queued_control_precedes_a_due_maintenance_slice` unchanged.

## Non-scope

- No Core changes. The watermark already exists at pinned Core `fc541a5`. No Core republish API.
- No changes to `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, journal/apply/baseline/delivery budgets, or owner-loop scheduling order (the wake/prefer calls stay as they are unless Implement proves a wake-chain gap with evidence; any such change needs its own unit proof and stays inside Hub).
- No wall-clock pass/fail assertion anywhere. No new wall-clock bound (per `question_1787007562_222481`).
- No client DTO changes; `DaemonEntityFrame::Snapshot` shape is unchanged. `botster-hub-client` is untouched.
- Do not absorb `ticket_1786977409_499180` (ShutdownSession idempotency / exact-bytes suite-load owner) or `ticket_1786937228_425608` (unix adapter flake). Do not treat the known exact-bytes suite-load failure as this ticket's regression; record it as known-baseline evidence owned by `ticket_1786977409_499180` (per `question_1787003911_553236`).
- No session_type or package-entity family behavior change. The gate applies to the `session` family assembly path only.

## Ownership boundaries and cross-repo dependencies

- Hub owns the projection, the completion policy, the owner loop, and the snapshot delivery contract. Core owns the journal, the watermark, and baseline pages; this plan consumes Core's existing pinned API (`fc541a5`) and requires no Core work.
- No cross-repository dependency registration is needed. Sibling Hub tickets `ticket_1786977409_499180` and `ticket_1786937228_425608` stay independent; both directly block final integration (flat graph, per `question_1787003911_553236`).

## Assumptions and unknowns

- Assumption (verified, fact 1): every successful Spawn reply implies its journal row is at or below the current source watermark. The first-frame 24-identity oracle is deterministic only because of this.
- Assumption: the pinned Core revision stays `fc541a59` or later within the same API. If Implement merges a Hub main that repins Core, re-verify fact 1 and fact 2 on the new pin.
- Diagnosed: subscribe-only catch-up starved Observe and froze at 8 of 24. A later subscribe-turn assembly still shipped 19 of 24 when Core's observed watermark lagged the last spawn rows. The hold path is a confirming empty pull after subscribe, not subscribe-turn assembly.
- Diagnosed (`question_1787017751_527748`): `acknowledged_spawn_ids.iter().all(...)` is vacuously true on an empty set. After this process has returned a successful Spawn, an empty acknowledged set must not count as caught-up. Sync unions runtime ids onto owner-loop state; it does not replace. When acknowledged ids are missing at the watermark, Hub rewinds the journal cursor. IsolatedHub child stderr is piped and unread, so production must not eprint large first-snapshot diagnostics on the delivery path.
- Diagnosed: the assemble-session oracle must assert reply kind `Spawned`. `request()` returning `Ok` is not a successful Spawn.
- Unknown: whether `sleep 8` sessions are restart-durable enough for an integration restart proof. Scope 8 names the lib-level recovery proof as the primary form.

## Affected surfaces/files

- `src/daemon_maintenance.rs` — watermark field, confirming-empty-pull flag, capture, clear-on-resync and clear-on-mutation, completion predicate, apply-to-delivery prefer, unit tests.
- `src/daemon_entity_subscriptions.rs` — assembly hold condition, no register-time first-snapshot assembly, unit tests (including updating `first_session_snapshot_is_complete_and_assembled_in_pages` and neighbors to the new gate signature), in-process existing-subscriber proof.
- `tests/hub_daemon_lifecycle/sessions.rs` — rewritten snapshot-assembly test and removal of catch-up machinery.
- `tests/hub_daemon_lifecycle/package_event_plane.rs` — delete the 50 ms worktree-create wall-clock gate; keep the functional Worktrees and WorktreeLifecycle contract; record duration as an observation only.
- `src/runtime.rs` — process-level `acknowledged_spawn_ids` after every successful `HubRuntime::spawn_session`.
- `src/daemon_transport.rs` — successful `DaemonRequest::Spawn` also records the request session id on maintenance state and on the runtime set. The apply-slice prefer stays in `daemon_maintenance.rs`.
- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` (this plan, v4.1) and `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` (updated implement report).

## Risks

1. Completion chase under continuous mutation: if the gate compares against the latest observed watermark, a busy hub could delay a first snapshot while mutations keep landing. Accepted for v4: the watermark advances only when pulls observe it, pulls outpace single-row mutations 16:1, and the fixture set is stable. Implement records the chosen comparison (latest-observed watermark vs captured-at-subscribe) and its termination argument in the report.
2. Mid-assembly cursor advance restarts assembly (`src/daemon_entity_subscriptions.rs:1144`). With the gate, assembly starts only when caught up, so restarts shrink to genuinely-new rows. Unit test (d) covers completion under backlog.
3. A held first snapshot changes timing for every session subscriber, including other lifecycle tests that subscribe while sessions exist. Those tests read frames with liveness timeouts and do not assert early partial snapshots, so they should only become more deterministic. This leaf ticket does not run a full suite; the focused 20/20 gates guard the changed path.
4. Oversized-snapshot close path (`close_oversized_session_snapshot`) must stay reachable; the gate must not bypass it. Existing unit `oversized_first_snapshot_closes_the_subscription` must stay green.
5. The full-suite smoke can still hit the known exact-bytes suite-load failure owned by `ticket_1786977409_499180`. Record exact evidence; do not absorb; do not waive silently.

## Acceptance checks/tests

Focused deterministic gates (primary, per ticket and `question_1787003911_553236`):

1. Unit: completion-predicate tests (scope 9) and the inverted-predicate red proof (scope 10) — `./test.sh --locked --lib` targeted filters, 20/20 for the new tests.
2. Unit ordering proof stays green: `queued_control_precedes_a_due_maintenance_slice`.
3. Integration: `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` — 20/20 consecutive passes outside the sandbox; both ready_spawn tests green; the snapshot-assembly test asserts the exact 24 assemble identities in the first Snapshot frame.
4. Existing-subscriber proof (scope 7) and recovery proof (scope 8) green in the same focused runs (include their filters in the 20/20 command set).
5. Repository gates: `cargo fmt --check`, `cargo clippy` per repo configuration.
6. Known-baseline evidence: if any run reproduces `external_hub_webrtc_live_output_preserves_exact_bytes`, record the typed failure and cite `ticket_1786977409_499180` ownership.
7. Do not run a full lifecycle suite as this leaf ticket's gate. The harness ticket and final integration own controlled full-suite smoke tests.
8. Production-path proof statement in the implement report: `continue_session_snapshot_assembly` is the only session first-snapshot producer. Registration warms journal and apply, then holds. The owner-loop `SubscriberDelivery` slice is the path that sends the first complete snapshot after a confirming empty pull.
9. Package-event oracle: `./test.sh --locked --test hub_daemon_lifecycle_test isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree` stays green on the functional worktree-create contract. Duration is an observation only.

## Vault gaps worth capturing

- Capture after merge: "Hub first session snapshots complete at the journal source watermark, not at baseline seal" — the completion predicate, the capture point, and why Spawn-before-reply journal append makes the first-frame oracle deterministic.
- Capture if diagnosed: the actual mechanism behind the historical 8-of-24 catch-up freeze, if Implement surfaces it.
- Update [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] with a pointer to the watermark-gated snapshot oracle as the durable replacement for readiness-by-latency.
