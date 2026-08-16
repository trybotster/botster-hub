# Hub: preserve held session-type subscription through CRUD

- **Ticket:** `ticket_1786841413_921609`
- **Run:** `run_1786841420_839050`
- **Target repository:** `botster-hub` (`trybotster/botster-hub`)
- **Target id:** `tgt_7e208a0c76a44980a83b63af976b1f22`
- **Baseline SHA:** `d52c3ebc4190286c4b7c3812f8c65251c646ade5` (current Hub main)
- **Locked Core SHA in this checkout:** `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- **Discovery Core SHA from the Web review:** `fc541a59338d0591ba4fb3fa522a030d212d26d0`
- **Merge policy:** merge directly into `main`. Do not create a PR.

## Target repository and target_id

Resolved from `list_spawn_targets`, not from the ambient session directory.

| Field | Value |
| --- | --- |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Path | `/Users/jasonconigliari/Projects/botster-hub` |
| Git remote | `trybotster/botster-hub` |
| Repository playbook | [[botster-hub-playbook]] |

This worktree is a Hub checkout at `d52c3eb`. That is the revision named by the ticket.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role and surface playbooks and atomic notes loaded

Role / overlay:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]

Charter must-loads used by this ticket:

- [[current botster is a modular repository family not the legacy trybotster monorepo]]
- [[botster hub is a first party host profile over core]]
- [[botster hub client state sync is entity frame only]]
- [[hub qualifies effective session type ids as source name slash id]]
- [[sanitized projection plus wholesale replacement update contracts silent data loss]]
- [[editor scoped reads sit in the mutation admission group not the sanitized read group]]
- [[live hub proof records distinct hub and locked core binary provenance]]

Targeted atomic notes:

- [[botster entity snapshots are authoritative reconnect baselines]]
- [[scoped entity snapshots preserve whole-family sequence gates]]
- [[botster web hub frame entity snapshots omit subscription identity]]
- [[botster client subscriptions should not hydrate global state]]
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[vault example paths are not repository placement conventions]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[plan agents must author vault context as wikilinks not home paths]]

Loaded and then excluded:

- [[botster-hub-client-playbook]] — DTO ownership context only. This ticket must not change the public protocol unless reproduction proves a DTO defect.
- [[project-pipelines-playbook]] — not loaded for implementation. Project Pipelines package paths are out of scope.
- [[botster runtime teardown lenses]] — not loaded. This ticket is entity-subscription CRUD, not WebRTC/peer/SessionIo teardown.

## Context loaded

Ticket: repair the current Hub main regression that forces a session-type entity resubscribe during CRUD.

Discovery path:

1. Web child ticket `ticket_1786840565_508953` ran `npm run smoke:live-packaged-protocol`.
2. Review used Hub `d52c3eb` and Core `fc541a59`.
3. `exerciseSessionTypes` failed before the alternate-screen oracle.
4. The harness reported `session_type CRUD triggered a resubscribe: 2 -> 3`.
5. The same run observed a sequence-gap discard and a replacement `subscribe_entities`.

Web production contract, already on current `botster-web` main:

- `WebrtcDaemonClient.receiveEntityFrame` accepts a snapshot as a new baseline.
- It accepts a later upsert/remove only when `snapshot_seq === current + 1`.
- Any other delta is discarded as `sequence_gap` or `delta_before_snapshot`.
- The client then unsubscribes and issues a new `subscribe_entities`.
- `exerciseSessionTypes` counts those requests and requires the count to stay at 2 across CRUD.

Hub current behavior:

- `register_entity_subscription` for `session_type` sends one snapshot whose `snapshot_seq` is `session_type_generation`.
- `drive_session_type_subscriptions` then stamps every remove and upsert in that generation with the same `snapshot_seq = generation`.
- If the published map is unchanged, it advances `definition_generation` and sends no frames.
- `mutate_session_type` increments `session_type_generation` by one on each successful create/update/delete.
- Package and spawn-target paths can also force a generation advance.

The existing Hub integration test `session_type_crud_pushes_authoritative_entity_deltas_without_polling` only covers an empty catalog and one changed row per generation (`0`, then `1`, then `2`). It cannot see a same-generation multi-frame burst or a skipped generation. That is why Hub tests can stay green while the live packaged Web harness fails.

This ticket is not a consumer of the Hub session-type eligibility parent. It is Hub-owned repair of the held subscription. Do not inject `list_session_types_for_target` parent pins.

Runtime-teardown class does not apply.

Sibling ticket `ticket_1786841441_227450` (`Hub: restore live Web session-type CRUD subscription stability`) overlaps. This run stays on held-subscription stability and `exerciseSessionTypes`. It does not absorb the alternate-screen Web ticket.

## Scope

- Reproduce the live `exerciseSessionTypes` resubscribe on current Hub main and current Core main.
- Record the first discarded frame: `current_snapshot_seq`, `rejected_snapshot_seq`, frame type, and whether more than one catalog row changed.
- Keep one held `session_type` entity subscription through create, update, and remove.
- Deliver ordered `entity_upsert` / `entity_remove` on that same `subscription_id`.
- Keep shared entity state on `entity_snapshot` / `entity_upsert` / `entity_patch` / `entity_remove` only.
- Make the delivered `snapshot_seq` contiguous for the subscriber (`N`, `N+1`, `N+2`, …) even when:
  - one generation changes more than one published row, or
  - `session_type_generation` jumps while the subscriber last applied `N`.
- Keep sequence-gap recovery available for a real missing frame. It must not fire on the normal CRUD path.
- Add Hub tests that fail on current main under the Web `+1` rule, then stay green after the fix.
- Prove Hub repository tests and the live packaged Web harness through `exerciseSessionTypes`.
- Merge the fix directly into Hub `main`.

Likely surgical fix, to confirm after the red reproduction:

1. Keep `session_type_generation` as the dirty detector.
2. Give `session_type` subscriptions the same per-subscriber monotonic `next_seq` already used by the `session` family.
3. On subscribe, set `next_seq` to the snapshot sequence.
4. On each delivered upsert or remove, send `next_seq + 1` and store it.
5. On overflow resync, send a snapshot at `next_seq + 1`. Do not reuse a generation number and do not move `snapshot_seq` backwards.

Do not replace the delta stream with a list refresh. Do not ask the client to resubscribe, reconnect, or refresh the surface.

## Non-scope

- `ticket_1786840565_508953` and any alternate-screen ReadScreen change.
- Web, TUI, Core, or Workspaces product edits.
- Hub session-type eligibility policy, qualified ids, or authoring-view losslessness unless reproduction proves they emit the extra frames.
- Public DTO / `hub-test-support` version bumps, unless reproduction proves a wire-shape defect. Current evidence points at delivery sequencing, not a new field.
- Package leftover / causal-lease cleanup except where it is the proven cause of a skipped fanout.
- Broad entity-subscription refactors for `session` or package families.
- Dual planning paths or planner-variety extras.

## Repository ownership boundaries and cross-repo dependencies

| Owner | Responsibility |
| --- | --- |
| `botster-hub` | Session-type authority, generation, subscribe, fanout, and delivered `snapshot_seq`. |
| `botster-hub-client` | Existing entity-frame DTOs. Do not change them unless a DTO defect is proven. |
| `botster-web` | Held subscription and `exerciseSessionTypes` oracle. Proof consumer only. |
| `botster-core` | Locked worker binary for live Hub launch. Not the session-type publisher. |

Cross-repo dependencies:

- Do not register a Core implementation dependency. Session-type CRUD is Hub-owned.
- Live proof must launch this Hub SHA and record the distinct locked-Core worker SHA from `Cargo.lock`, plus the Core main SHA actually used if the ticket's "current Core main" binary differs.
- Do not edit `botster-web`. If the live harness cannot run without a Web change, stop and ask a human.
- Sibling Hub ticket `ticket_1786841441_227450` is not a dependency of this run. Do not broaden this ticket to that title.

## Assumptions and unknowns

Assumptions:

- The `2 -> 3` count is one production `sequence_gap` resubscribe, not a second UI-driven subscribe.
- The first-party Web `+1` rule is the required live contract. Hub must satisfy it.
- Empty-catalog socket CRUD can stay green while a populated live catalog fails.
- This ticket is not a consumer of the eligibility parent.
- Runtime-teardown class does not apply.
- `docs/plans/` is the Hub plan home. Mainline already stores plans there.

Unknowns Implement must resolve before choosing the patch:

1. Which `exerciseSessionTypes` mutation first emits the rejected seq: form create, form update, target-id fixture, or later socket CRUD.
2. Whether the gap is same-seq multi-frame, skipped generation, or a snapshot/delta mix.
3. Whether owner-loop delay batches two successful mutations before one fanout.

If reproduction shows a Web client bug rather than a Hub delivery bug, stop and ask a human. Do not silently retarget this Hub ticket.

## Affected surfaces and files

Primary:

- `src/daemon_entity_subscriptions.rs` — `register_entity_subscription` (`session_type` branch), `drive_session_type_subscriptions`, and unit tests.
- `tests/hub_daemon_lifecycle/packages.rs` — extend `session_type_crud_pushes_authoritative_entity_deltas_without_polling` or add a sibling that uses a populated catalog and the Web `+1` rule.
- `src/daemon_transport.rs` / `src/session_types.rs` — only if reproduction shows a double generation advance or a published-map change that should not occur.

Likely untouched:

- `crates/botster-hub-client/**`
- `packages/hub-test-support/**`
- Web and Core trees

## Risks

- Stamping every frame with `session_type_generation` makes one generation with two diffs look like a gap to Web.
- Advancing generation without a delivered frame leaves the client at `N` and the next frame at `N+2`.
- A snapshot resync on ordinary CRUD would hide the gap and still violate "deltas through the existing subscription".
- Poison-recovery tests already allow `snapshot_seq > 1`. New assertions must keep that recovery path and still require contiguous seqs on the normal CRUD path.
- Changing `session` or package `next_seq` while fixing `session_type` can regress leftover/catch-up work on current main.
- Weakening `exerciseSessionTypes` would hide the defect. Do not change that oracle.

## Acceptance checks and tests

Hub, red first:

1. Add a unit test around `drive_session_type_subscriptions` where one generation changes two entities. Current main must emit two frames with the same `snapshot_seq` or a skip. After the fix, the frames must be `N+1` then `N+2` on the same `subscription_id`.
2. Add a unit test where `definition_generation` is `1` and the next generation is `3` with one changed row. Current main emits `snapshot_seq = 3`. After the fix, the subscriber must see `2`, not `3`.
3. Add or extend a real-daemon socket test with a pre-existing catalog row plus create/update/remove. Assert:
   - the same `subscription_id` for snapshot, upserts, and remove;
   - contiguous `snapshot_seq`;
   - ordered create → update → remove;
   - no second `SubscribeEntities` on that connection.
4. Ablate the new seq assignment and show the new tests fail ([[a regression test must be shown to go red with the fix reverted]]).
5. Keep `session_type_crud_pushes_authoritative_entity_deltas_without_polling` and the poison-recovery subscribe test green.

Hub wrappers:

```sh
./test.sh --test hub_daemon_lifecycle_test session_type_crud_pushes_authoritative_entity_deltas_without_polling -- --exact
./test.sh --test hub_daemon_lifecycle_test <new_populated_catalog_test> -- --exact
./test.sh --lib drive_session_type
./test.sh
```

Use `./test.sh`, not bare `cargo test`. The worktree path has no `:`, so no `CARGO_TARGET_DIR` override is required. `.gitignore` is present and non-empty.

Live packaged Web proof, no Web edits:

1. Build this Hub checkout.
2. Build `botster-session-worker` from the Core SHA actually launched.
3. Record Hub SHA, locked Core SHA from `Cargo.lock`, launched Core SHA if different, and both binary realpaths ([[live hub proof records distinct hub and locked core binary provenance]]).
4. Run current `botster-web` `npm run smoke:live-packaged-protocol` against those binaries.
5. Require `exerciseSessionTypes` to keep `session_type` `subscribe_entities` count at 2.
6. Require create, update, and remove to appear as entity frames on the held subscription, in order.
7. Require no `webrtc_entity_frame_discarded` with `reason=sequence_gap` during that CRUD span.
8. Do not change or weaken the alternate-screen oracle. This ticket is done when `exerciseSessionTypes` passes. Later terminal stages remain the Web ticket's problem.

Production-path proof: the live WebRTC client is the production entry point. Socket tests are necessary but not sufficient.

## Vault gaps worth capturing

- Hub `session_type` fanout currently uses one generation number as every frame's `snapshot_seq`. First-party Web requires per-frame `+1`. No vault note states that host-plane entity deltas are per-subscriber contiguous sequences.
- Hub tests that allow `snapshot_seq > 1` after recovery can hide a skipped-seq CRUD regression.
- Capture after Implement confirms the exact failing pair (same-seq burst vs skipped generation). Do not capture a guessed cause from this plan.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Default | Per-subscriber contiguous `snapshot_seq` on the existing `session_type` subscription. |
| Non-goal | Web workaround, list refresh, eligibility rewrite, protocol bump. |
| Follow-up OK | Sibling ticket `ticket_1786841441_227450` if it still has unique live-proof work after this merge. |
| Ask human | Reproduction shows a Web-owned bug, or the live harness cannot run without editing Web. |

## Worktree and pipeline

- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Worktree: this pipeline Hub checkout
- Hygiene: `.gitignore` restored/present (53 bytes). No colon in the path.
- Gates: plan artifact, vault checklist, Plan attestation, then Plan Review.
- Docs: this file under `docs/plans/`. Add an implement report later under `docs/reports/`.
- Consumer session-type eligibility pins: not applicable.
- Teardown-class fields: not applicable.

`teardown_class_applies`: false
