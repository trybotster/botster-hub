# Hub: stale WebRTC peer attach snapshot must not detach replacement owner

## Plan metadata

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786690597_154692` |
| Run | `run_1786690609_367424` |
| Plan step | `botster_stack_plan` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` — not ambient cwd |
| Base | Hub `main` @ `173e528` (matches ticket current-lock baseline) |
| Locked Core | `033cd01` (ticket Plan Review baseline) |
| Delivery | direct-merge; no pull request; no human PR sign-off |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| Session-type eligibility consumer | no |
| Project Pipelines overlay | not loaded (no package/plugin/workflow paths) |

Resolved via `list_spawn_targets` against the ticket/run `target_id`. Do **not** infer ownership from the pipeline worktree directory name.

## Target repository and target_id

- **Target repository:** `botster-hub`
- **target_id:** `tgt_7e208a0c76a44980a83b63af976b1f22`
- **Repo:** `trybotster/botster-hub`

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for this run

## Other role/surface playbooks and atomic notes loaded

### Role / stack overlays

| Note | Plan impact |
| --- | --- |
| [[planner-playbook]] | Role contract: scope, assumptions, risks, acceptance |
| [[botster-planner-playbook]] | Botster layers, worktree binding, runtime-teardown class trigger, completion evidence |
| [[botster-architecture]] | Domain map; Hub vs Core vs client ownership; no new surfaces |
| [[cli-patterns]] | Mixed-generation index only; current ownership from Hub charter |
| [[botster runtime teardown lenses]] | **Required class overlay** — every lens answered below |
| [[botster-runtime-reviewer-playbook]] | Downstream Review overlay (named, not executed here) |
| [[botster-runtime-verifier-playbook]] | Downstream Verify overlay (named, not executed here) |

### Targeted atomic notes

| Note | Plan impact |
| --- | --- |
| [[webrtc peer cleanup removes every per peer owner together]] | PeerClosed must sweep the whole grant-owned set without deleting a replacement owner's row |
| [[terminal webrtc failure records do not prove peer runtime teardown]] | Passing the test is not a terminal-file claim; oracles are live peer + attach owner index + counter |
| [[Core terminal subscription ownership is session, subscription, and generation]] | Core identity is generation-based; Hub WebRTC reuse identity stays `(session, subscription)` + `grant_id`. Do not import Core generation into Hub |
| [[attach failed cleanup is route aware and idempotent]] | PeerClosed detach must be route-identity + owner-checked; no blind counter decrement across a reused route |
| [[pre READY attach failure creates no attach ownership]] | Do not invent attach ownership on failed routes; this ticket is live-then-replaced, not pre-READY failure |
| [[late webrtc messages after disconnect must not recreate clients]] | Late PeerClosed snapshot is stale transport residue; it must not mutate the live replacement owner |
| [[Hub route registry names describe ownership not attach queues]] | `AttachStreamRegistry` and `live_attach_routes` are ownership sets, not queues. Keep names/roles; do not reintroduce a Hub attach queue |
| [[test script required for rust tests not cargo test]] | Acceptance uses `./test.sh` (sets `BOTSTER_ENV=test`) plus ticket-required fmt/clippy |
| [[rust repo strict lints must be verified before dismissing warnings]] | Ticket clippy gate is `--locked -- -D warnings`; include `derivable_impls` if still present |
| [[a regression test must be shown to go red with the fix reverted]] | Ablate the accounting fix and the owner-check separately |
| [[an ablation that reddens at the first assertion does not vouch for later ones]] | Current red is `live_attach_before >= 1`; that does **not** prove the later stale-snapshot assertions |
| [[file descriptor exhaustion from stale webrtc connections]] | Related leak class; this ticket does not reopen socket-cleanup design |
| [[graceful-termination-requires-explicit-cleanup-hooks]] | Explicit PeerClosed handler remains the forget path; no implicit-drop rewrite |
| [[pipeline vault checklists must cite exact resolvable note titles]] | Evidence uses exact filenames |
| [[plan agents must author vault context as wikilinks not home paths]] | This artifact cites wiki links only |
| [[vault example paths are not repository placement conventions]] | Plan destination taken from Hub `docs/plans/` prior art + README exclusion note, not a vault example |

### Charter must-load notes with plan impact (Hub)

The Hub charter [[botster-hub-playbook]] must-load list was consulted. Notes that constrain this ticket:

| Note | Plan impact |
| --- | --- |
| [[botster hub is a first party host profile over core]] | Change stays in Hub control-plane attach ownership |
| [[botster data plane bypasses the hub through session and client actors]] | No SessionIo/ClientWorker byte-path edits |
| [[botster runtime teardown lenses]] | Class applies; full answers below |
| [[attach failed cleanup is route aware and idempotent]] | Owner-checked route cleanup |
| [[pre READY attach failure creates no attach ownership]] | Do not change failed-route ownership rules |
| [[Hub route registry names describe ownership not attach queues]] | Ownership sets only |
| Remaining charter must-loads (session types, package hydration, GHOSTSNP phase machine, plugin workers, durable HubState, editor reads, supervision admission) | **No product change.** Consulted only to confirm non-scope |

**Not loaded:** [[project-pipelines-playbook]] — package/plugin/workflow paths are out of scope.

## Context loaded

- Pipeline context: `project_pipelines_current_context` for `run_1786690609_367424`.
- Ticket split origin: Plan Review of sibling `ticket_1786663582_169720` (Hub session projection) recorded this as a **current-lock workspace failure unrelated to session projection**.
- Parent/base run `run_1786689005_381068` is the projection run that spawned this ticket. It is **not** a product dependency and is **not** registered against this run.
- Prior same-repo teardown work: `docs/reports/webrtc-teardown-follow-up-implement-report.md` added `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` and the owner-check filter. Later current-lock evidence (`docs/reports/publish-crates-io-botster-ui-contract-0.3.2.md`) shows the test still fails at `live_attach_before >= 1` on Hub `173e528`.
- Code inspected: `src/daemon_transport.rs` `LocalWebrtcPeerClosed` handler, `record_attached_subscription_change`, `detach_local_webrtc_subscriptions`, Attach/Detach request path; `src/daemon_attach_stream.rs` `AttachStreamRegistry`; `src/local_webrtc.rs` focused tests and `PeerHarness`.
- Worktree hygiene: tracked `.gitignore` present (53 bytes, matches HEAD). Path has no `:`. `CARGO_TARGET_DIR` override not required.

## Diagnosis

The focused test fails **before** the delayed stale snapshot is applied.

Sequence in `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner`:

1. Peer A Attach `(session S, sub X)` records `attach_owner_grant_ids[(S,X)] = A`, inserts `(S,X)` into `live_attach_routes`, and increments `live_attach_subscriptions`.
2. Production `PeerClosed` for A (`process_until_peer_closed`) builds `detach_list` from the peer snapshot **and** the owner-index sweep, then:
   - independently `live_attach_subscriptions.saturating_sub(detach_list.len())`
   - calls `detach_local_webrtc_subscriptions` → `handle_control_request(Detach)` → `cancel_stream`
   - **does not** call `record_attached_subscription_change(Detach)`, so `live_attach_routes` still contains `(S,X)`
3. Peer B Attach reuses `(S,X)`:
   - `start_attach` writes `attach_owner_grant_ids[(S,X)] = B`
   - response is `Events`, so `record_attached_subscription_change(Attach)` runs
   - `live_attach_routes.insert((S,X))` returns `false` (already occupied)
   - the function **returns without incrementing** `live_attach_subscriptions`
4. `assert!(live_attach_before >= 1)` fails. Later owner-check assertions never run.

This is attach-ownership accounting on the Hub control plane. The owner-check filter on the delayed snapshot is already present and must be **kept and independently proven**. It is not the current first failure.

The empty-snapshot owner sweep (`local_webrtc_attach_owner_sweep_on_empty_snapshot`) is the complementary same-grant path and must stay green.

## Scope and non-scope

### In scope

- Make replacement-owner Attach restore a live attach: `live_attach_routes` and `live_attach_subscriptions` must stay one source of truth across PeerClosed + reuse.
- Keep a delayed `LocalWebrtcPeerClosed` attach snapshot for grant A from detaching grant B's reused `(session_id, subscription_id)`.
- Keep same-grant residual sweep when the peer-side snapshot is empty.
- If this ticket lands first: remove the hand-written `Default` impl on `AttachStreamRegistry` (`clippy::derivable_impls` at `src/daemon_attach_stream.rs:54`). If the session-projection ticket already merged that one-liner, do not touch the file for lint-only reasons.
- Prove with the existing focused test plus the ticket's fmt / clippy / `./test.sh --locked` gates.

### Out of scope

- Session lifecycle projection, journal consume, plugin session-family delivery (sibling `ticket_1786663582_169720`).
- Core attach generation identity, ClientWorker bind, SessionIo teardown.
- Hub-client DTO / TypeScript / hub-test-support package version bumps.
- Per-peer dedicated runtime rewrite, close-bound retune, hang-oracle redesign.
- Package/plugin/Project Pipelines workflow, session-type eligibility, SPA request-state.
- Dual pipeline / planner-variety second run.

## Botster layers touched

- **Rust hub control plane** (`daemon_transport` attach ownership + PeerClosed forget).
- **Local WebRTC transport** only as the production entry that emits `LocalWebrtcPeerClosed` and as the existing test harness. No new signaling or peer-map design.

## Implementation plan (one Plan → Implement path)

Surgical change in `handle_control_message` for `ControlMessage::LocalWebrtcPeerClosed`:

1. Keep building `detach_candidates` from (a) the PeerClosed snapshot, (b) `remove_result.attached_subscriptions`, (c) owner-index rows whose owner is in `removed_grants`.
2. Keep the owner-check filter: detach only when the current `attach_owner_grant_ids` owner is absent or is one of `removed_grants`.
3. For each owner-checked detach, apply `record_attached_subscription_change(Detach, …)` so `live_attach_routes` is released and the counter decrements **once**. **Do not** also `saturating_sub(detach_list.len())`.
4. Keep `attach_owner_grant_ids.retain(|_, owner| !removed_grants.contains(owner))` after recording so residual same-grant index rows cannot survive a no-op Core Detach.
5. Keep `detach_local_webrtc_subscriptions` for Core/registry `cancel_stream` + Detach. That path must remain a Core detach, not a second counter mutation.
6. Do not change `record_attached_subscription_change(Attach)` into a silent replacement increment. The occupancy set must be cleared by the previous owner's forget path; replacement Attach should insert a vacant route.

Optional cheap companion (only if it isolates claim 1 without a full WebRTC peer): a `daemon_transport` unit test that Attach-records a route, applies the PeerClosed recording helper, then Attach-records the same route under a new grant and asserts `live_attach_subscriptions == 1` and `live_attach_routes` contains the route. Do **not** replace the production-path WebRTC test.

Clippy: `#[derive(Default)]` on `AttachStreamRegistry` and delete the identical manual impl, only if still present at Implement time.

## Repository ownership boundaries and cross-repo dependencies

| Boundary | This ticket |
| --- | --- |
| Hub | Owns control-plane attach ownership, PeerClosed forget, live attach counters |
| Core | Unchanged. Detach continues through existing `HubRuntime` / `CoreDaemon` request |
| Hub-client | Unchanged DTOs |
| Web / TUI / Workspaces / Project Pipelines | No consumer work |

**Cross-repo dependencies:** none. Do not register Core, hub-client, or web targets.

**Same-repo sibling, not a dependency:** `ticket_1786663582_169720` (session projection). Do not wait on it. Do not implement projection here. Clippy overlap is the one `Default` impl; last writer should not revert the other's product change.

## Assumptions and unknowns

- Assumed: the ticket's current-lock failure at `live_attach_before >= 1` is the same desync visible in this worktree at `173e528`. Implement must re-run the focused test first and record the exact assertion if it has moved.
- Assumed: the owner-check filter already in the PeerClosed handler is the intended stale-snapshot policy and should stay. If Implement finds the filter missing or inverted, restore it rather than inventing a second index.
- Assumed: socket-path `handle_connection_cleanup` independently decrementing `live_attach_subscriptions` after Detach is **out of scope** unless this change makes that path fail an existing test. If it does, stop and ask; do not silently expand into Unix-socket cleanup.
- Unknown: whether `./test.sh --locked` still hangs in `hub_daemon_lifecycle_test` smoke as recorded in the UI-contract report. Implement must run the command the ticket names. If an unrelated hang or timeout appears, isolate with the same command on unchanged files / `--test-threads=1` and do not waive the ticket's focused WebRTC proof.
- Not a session-type eligibility consumer: no parent pin injection, no hub-test-support 0.1.26 / conf 33 requirement.

## Affected surfaces/files

| Path | Expected change |
| --- | --- |
| `src/daemon_transport.rs` | PeerClosed attach detach uses `record_attached_subscription_change`; drop independent counter subtract |
| `src/daemon_attach_stream.rs` | Optional `#[derive(Default)]` if clippy still fails |
| `src/local_webrtc.rs` | No harness redesign; keep the existing focused test as the production-path oracle |
| `docs/plans/stale-webrtc-peer-attach-snapshot-must-not-detach-replacement-owner.md` | This plan |
| `docs/reports/` | Implement report at Implement (not this step) |

## Risks

- **Double decrement:** wiring `record_attached_subscription_change` while leaving `saturating_sub(detach_list.len())` would drive the counter below the true live set and break empty-snapshot sweep.
- **First assertion hides second claim:** fixing only the counter without keeping the owner-check would make `live_attach_before >= 1` pass while the delayed snapshot still detaches B.
- **Fail-closed sibling sacrifice:** `remove_peer` can return multiple `removed_grant_ids`. Owner-check must use the full `removed_grants` set, not only the closing grant.
- **Unrelated suite noise:** capability-runtime timeouts and daemon-lifecycle smoke hangs have been seen on this lock. Do not treat them as this ticket unless the diff introduced them.
- **Projection sibling collision:** both tickets can touch `daemon_attach_stream.rs`. Stay off projection/journal code.

## Runtime-teardown lens answers

| Field | Answer |
| --- | --- |
| `teardown_class_applies` | **yes** — WebRTC peer lifecycle, multi-peer attach ownership, delayed PeerClosed snapshot vs replacement owner |
| `teardown_isolation` | One grant's successful PeerClosed may detach only attaches whose current `attach_owner_grant_ids` owner is that grant (or unowned residual). Healthy sibling grant B keeps `(S,X)` and its live peer. Fail-closed close still drops the shared dedicated runtime (existing policy; not retuned here) |
| `teardown_bounds` | No new `block_on(close)`. Existing `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and fail-closed runtime drop stay. This ticket is ownership accounting after forget, not a new close wait |
| `late_message_matrix` | See table below |
| `production_path_proof` | Production path: peer terminal state → `ControlMessage::LocalWebrtcPeerClosed` → `handle_control_message` → `remove_peer` → owner-checked attach/entity sweep → `record_attached_subscription_change` + Core Detach. Oracle is the existing test, which already drives `handle_control_message` for both the first production close and the delayed snapshot. Live oracles: `has_live_peer(B)`, `attach_owner_grant_ids[(S,X)] == B`, `active_subscriptions` still contains X, `live_attach_subscriptions` unchanged by the delayed snapshot. Terminal JSON is not sufficient |
| `ownership_identity` | Hub WebRTC attach owner id is `grant_id` keyed by `(session_id, subscription_id)`. Reused subscription ids transfer to the replacement grant on successful Attach. Delayed PeerClosed for the previous grant must not delete the new owner. Entity rows already use `owner_grant_id` (keep; do not change). Do not add Core generation to Hub |
| `sibling_fail_closed_policy` | On successful close: siblings keep working (this test). On ultimate close failure: existing fail-closed drops the dedicated runtime and sweeps every grant that runtime owned (already tested; do not silently change). This ticket must not weaken that policy |

### Late-message admission matrix

| Message | Grant / owner tag | Reject after terminal failure | Residual sweep if it races PeerClosed |
| --- | --- | --- | --- |
| `Request::Attach` | `ControlMessage::Request.grant_id` | Existing live-peer fail-closed when grant not live | Successful replacement Attach records new `grant_id` in `attach_owner_grant_ids` + `live_attach_routes` |
| `Request::Detach` | grant on Request | Existing live-peer fail-closed when grant not live | `cancel_stream` is route-identity; must not decrement a foreign route |
| `Request::Spawn` and every other tagged Request | grant on Request | Existing universal live-peer fail-closed | Unchanged |
| `SubscribeEntities` | `owner_grant_id` on the row | Existing live-peer / owner rules | Unchanged |
| `UnsubscribeEntities` | owner-checked against current row | Late Unsubscribe from A must not delete B's row (existing test) | Unchanged |
| `LocalWebrtcPeerClosed` attach snapshot | closing `grant_id` + `attach_owner_grant_ids` | Snapshot rows owned by a different live grant are filtered out | Owner-index sweep still detaches rows this forget still owns, including empty snapshots |
| `LocalWebrtcPeerClosed` entity snapshot | closing `grant_id` + `owner_grant_id` | Already owner-checked | Independent owner-index sweep remains |

A plan that fixed only Subscribe/Unsubscribe and left Attach/`live_attach_routes` desynced would be incomplete. This ticket's missing world is **Attach occupancy after PeerClosed**.

## Acceptance checks/tests

Implement must prove the **live** path, not that the owner-check code exists.

1. **Baseline (red expected today):**
   `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
   then `./test.sh local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner`
   Record the exact assertion. Ticket baseline: `live_attach_before >= 1`.
2. **Focused green:** same command after the fix, consistently (more than one run if the first is not obviously deterministic).
3. **Replacement owner remains live after the stale snapshot:**
   `active_subscriptions[S]` contains X; `attach_owner_grant_ids[(S,X)] == grant_b`; `live_attach_subscriptions` equals the post-B-attach value; `has_live_peer(grant_b)`.
4. **Same-grant empty snapshot still sweeps:**
   `./test.sh local_webrtc_attach_owner_sweep_on_empty_snapshot`
5. **Sibling late-message regressions:**
   `./test.sh local_webrtc_stale_peer` (entity unsubscribe reuse + attach reuse).
6. **Assertion-specific ablation** ([[an ablation that reddens at the first assertion does not vouch for later ones]]):
   - Ablate the `live_attach_routes` / counter sync only → must fail at `live_attach_before >= 1`.
   - Keep that fix, ablate the owner-check filter only → must fail at “delayed PeerClosed for A must not detach B's reused attach” (or the owner-index / counter assertions after the delayed snapshot), **not** at the earlier counter assert.
7. `cargo fmt --all -- --check`
8. `cargo clippy --workspace --all-targets --locked -- -D warnings`
9. `./test.sh --locked`
10. Downstream Review loads [[botster-runtime-reviewer-playbook]]; Verify loads [[botster-runtime-verifier-playbook]] and re-runs the production-path oracles above. No new live-hub binary / package-admission proof (this is not supervision or session-type work).

## Vault gaps worth capturing

Capture after merge (do not invent notes in Plan):

1. Hub PeerClosed attach accounting must go through the same `live_attach_routes` occupancy set as Attach/Detach responses. An independent `detach_list.len()` subtract leaves a occupied route and a zero counter, so replacement Attach cannot become live.
2. A red at `live_attach_before >= 1` does not prove the stale-snapshot owner-check; those are two claims.

No capture during Plan: the gap is visible but the convention is not yet shipped behavior.

## Worktree / pipeline hygiene

- Restore `.gitignore` from HEAD if a later step wipes it. Current Plan visit: file present and matches HEAD.
- Path has no colon; no `CARGO_TARGET_DIR` override required.
- One vault checklist for this Plan visit (run-scoped). Skip duplicates if one already exists.
- Gate evidence must include `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, `target_repository` — never URI-only.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Default | Owner-check stays; occupancy set is the counter source of truth |
| Non-goals | Projection, Core generation-in-Hub, close-bound retune, package work |
| Follow-up-ok | Vault captures above; socket-path double-decrement only if a test forces it |
| Ask-human | If the focused test's first assertion has moved to a different failure that is not attach-ownership; if `./test.sh --locked` is blocked by an unrelated hang that cannot be isolated; if socket cleanup must change to land this ticket |
