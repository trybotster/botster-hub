# Implement report: preserve held session-type subscription through CRUD

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Spawn target path | `/Users/jasonconigliari/Projects/botster-hub` |
| Ticket | `ticket_1786841413_921609` |
| Run | `run_1786841420_839050` |
| Worktree | pipeline-provided Hub ticket worktree |
| Branch | `project-pipelines/ticket_1786841413_921609` |
| Baseline SHA | `d52c3ebc4190286c4b7c3812f8c65251c646ade5` |
| Implement commit | `2434f90f8536922f30b20de475942ccb5e14155d` |
| Plan | `docs/plans/preserve-held-session-type-subscription-through-crud.md` |
| `teardown_class_applies` | false |

Independent target resolution via `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. This session `whoami` reports repo `trybotster/botster-hub` on the ticket branch.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[current botster is a modular repository family not the legacy trybotster monorepo]]
- [[botster hub is a first party host profile over core]]
- [[botster hub client state sync is entity frame only]]
- [[hub qualifies effective session type ids as source name slash id]]
- [[sanitized projection plus wholesale replacement update contracts silent data loss]]
- [[editor scoped reads sit in the mutation admission group not the sanitized read group]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[scoped entity snapshots preserve whole-family sequence gates]]
- [[botster web hub frame entity snapshots omit subscription identity]]
- [[botster client subscriptions should not hydrate global state]]
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[implementation steps must persist report artifacts for review]]
- [[implement gate must verify committed work and pr link before review]]
- [[pipeline artifacts should use path neutral worktree references]]

Not loaded for implementation:

- [[project-pipelines-playbook]] — Project Pipelines package and plugin paths are out of scope.
- [[botster runtime teardown lenses]] — this ticket is entity-subscription CRUD, not WebRTC or SessionIo teardown.
- [[botster-hub-client-playbook]] — DTO ownership context only. No public protocol change.

## Constraints applied before edits

- Work only in the routed Hub ticket worktree.
- Keep `session_type_generation` as the dirty detector.
- Give `session_type` subscriptions the existing per-subscriber `next_seq` used by the `session` family.
- Do not replace the delta stream with a list refresh.
- Do not edit Web, Core, Workspaces, or hub-client DTOs.
- Use `./test.sh`, not bare `cargo test`.
- Merge policy is direct into `main`. Do not create a pull request.
- Runtime-teardown class does not apply.

## Files changed

Feature behavior:

- `src/daemon_entity_subscriptions.rs` — on `session_type` subscribe, store `next_seq = snapshot_seq`. `drive_session_type_subscriptions` now stamps each delivered remove or upsert with `next_seq + 1` and stores it. Overflow resync sends a snapshot at `next_seq + 1` instead of the generation number. Added red-first unit tests for same-generation multi-row, skipped generation, and overflow resync.
- `tests/hub_daemon_lifecycle/packages.rs` — real-daemon held-subscription test with a pre-existing catalog row, a two-row package enable, then create, update, and remove. Asserts one `subscription_id` and contiguous `snapshot_seq`.

Pipeline handoff:

- `docs/plans/preserve-held-session-type-subscription-through-crud.md` — approved plan from the Plan step
- `docs/reports/preserve-held-session-type-subscription-through-crud-implement-report.md` — this report

Not changed: `src/daemon_transport.rs`, `src/session_types.rs`, `crates/botster-hub-client/**`, `packages/hub-test-support/**`, Web, and Core.

## Ownership boundaries preserved

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Session-type authority, generation, subscribe, fanout, delivered `snapshot_seq` | botster-hub | repaired |
| Entity-frame DTOs | botster-hub-client | unchanged |
| Held subscription oracle `exerciseSessionTypes` | botster-web | proof consumer only |
| Locked worker binary | botster-core | launched, not edited |

## Cross-repo dependencies or separately routed work

- No Core implementation dependency. Session-type CRUD is Hub-owned.
- No Web edits. Current `botster-web` `origin/main` `1e576852872bc78fead26c66dc10994447ba3b94` was the proof consumer.
- Sibling Hub ticket `ticket_1786841441_227450` remains a duplicate with no dependency, per `question_1786842281_584285`.
- Alternate-screen Web ticket `ticket_1786840565_508953` remains independent.

## Deviations from plan

None. The surgical fix matches the approved plan: keep generation as the dirty detector, assign per-subscriber contiguous `snapshot_seq`, keep overflow resync moving forward, and do not change `session` or package `next_seq`.

## Confirmed failing pair

Red-first unit tests on current main (`d52c3eb`) showed both production-shaped defects:

| Case | Current main | Required |
| --- | --- | --- |
| One generation, two changed rows | two upserts with `snapshot_seq = 2` | `2` then `3` |
| Definition generation `1`, next generation `3` | upsert `snapshot_seq = 3` | upsert `2` |
| Overflow resync at `next_seq = 7`, generation `3` | snapshot `3` (backwards) | snapshot `8` |

Ablating the new seq assignment back to `generation` reddened all three tests. Restoring `next_seq + 1` made them green.

The live Web catalog is populated by the installed `botster-web` package. That is the same-generation multi-row shape: one dirty generation can publish more than one row. The skipped-generation shape remains covered by the unit test and overflow-resync test.

## Tests and downstream proof run

Hub wrappers:

```sh
./test.sh --lib session_type
./test.sh --test hub_daemon_lifecycle_test session_type_crud_pushes_authoritative_entity_deltas_without_polling -- --exact
./test.sh --test hub_daemon_lifecycle_test session_type_held_subscription_stays_contiguous_through_populated_catalog_crud -- --exact
./test.sh --test hub_daemon_lifecycle_test poison_recovery_delete_succeeds_under_invalid_repo_session_types -- --exact
./test.sh --test hub_daemon_lifecycle_test poison_recovery_disable_succeeds_under_invalid_repo_session_types -- --exact
./test.sh
```

Results:

- `session_type` lib filter: 7 Hub lib tests passed, including the three new seq tests and the oversized resync test.
- Existing empty-catalog CRUD daemon test: passed.
- New populated-catalog held-subscription daemon test: passed.
- Poison-recovery subscribe tests: passed.
- Full `./test.sh`: workspace wrapper exit 0. No failed tests. Two ignored tests remain ignored (`external_hub_client_many_pty_adversarial_conformance_local` and one installer-adjacent ignore already on main).

Live packaged Web proof, no Web edits:

- Hub binary: this worktree `target/debug/botster-hub` built from implement commit `2434f90f8536922f30b20de475942ccb5e14155d`.
- Worker binary: this worktree `target/debug/botster-session-worker`, built with `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
- Locked Core SHA from `Cargo.lock`: `aef6516d5809d563961ed7fdd07da29a7b4edddc`.
- Launched Core SHA: `aef6516d5809d563961ed7fdd07da29a7b4edddc` (same as lockfile).
- Current Core `origin/main`, not launched: `fc541a59338d0591ba4fb3fa522a030d212d26d0`.
- Web consumer: `1e576852872bc78fead26c66dc10994447ba3b94`.
- Command: `BOTSTER_HUB_BIN=<this-worktree>/target/debug/botster-hub BOTSTER_SESSION_WORKER_BIN=<this-worktree>/target/debug/botster-session-worker npm run smoke:live-packaged-protocol` from current `botster-web` main.

Live result:

- `session-type-live-proof` recorded `session_type_subscribes_before_crud: 2` and `session_type_subscribes: 2`.
- Form create, form update, target-id fixture, and socket create/update/remove completed on the held subscription.
- The smoke log contains no `sequence_gap` and no `webrtc_entity_frame_discarded`.
- The full live packaged protocol harness passed, including later terminal stages.

Production-path proof: the live WebRTC client is the production entry point. `exerciseSessionTypes` kept count 2 across CRUD.

## Unverified behavior or residual risk

- The live smoke was not first run against unfixed Hub in this Implement visit. The failing pair was proved by red-first unit tests on current main, then the live harness was run against the fixed binary. That is enough to identify the discarded-frame shapes. It does not name which live mutation would have been first on unfixed Hub.
- Owner-loop batching of two successful mutations into one fanout was not observed as a separate live failure. The same-generation multi-row unit test and the two-row package daemon test cover that delivery shape.
- Current Core main `fc541a59` was recorded and not launched. Session-type fanout is Hub-owned. The launched worker is the lockfile pin required by [[live hub proof records distinct hub and locked core binary provenance]].
- `session` and package `next_seq` paths were not changed. Residual risk is limited to `session_type` overflow resync under a full subscriber queue, which the unit test covers.

## Missing vault guidance discovered

No vault note stated that host-plane `session_type` deltas must be per-subscriber contiguous sequences even when generation is the dirty detector. Captured to `knowledge/inbox/host-plane-session-type-deltas-use-per-subscriber-contiguous-snapshot-seq.md`.

Hub `Cargo.toml` has no `[lints]` table. Strict clippy was not a separate wrapper on this checkout. `./test.sh` is the repository gate.

## Assumptions

- The `2 -> 3` live count on unfixed Hub is one production `sequence_gap` resubscribe, not a second UI-driven subscribe. The fixed live run stayed at 2, which matches that assumption.
- Runtime-teardown class does not apply.
- Direct merge into `main` happens after Review and Verify. This Implement visit commits the ticket branch and does not create a pull request.
