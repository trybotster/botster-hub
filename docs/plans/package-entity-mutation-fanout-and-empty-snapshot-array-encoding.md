---
ticket: ticket_1786494180_266672
run: run_1786498793_923614
step: botster_stack_plan
target_id: tgt_7e208a0c76a44980a83b63af976b1f22
target_repository: botster-hub
plan_revision: 4
addresses_reviews:
  - review_1786499639_401571
  - review_1786500018_446268
  - review_1786500409_504789
---

# Hub: package entity mutation fanout and empty snapshot array encoding

## Plan revision 4 (Plan Review rework)

Addresses open findings from `review_1786500409_504789` while preserving locked contracts from rev2–rev3:

| Finding | Severity | Locked resolution |
| --- | --- | --- |
| Behind provider snapshot can roll advanced subscribers backward | high | **Per-subscriber snapshot targeting**: never deliver `snapshot_seq < sub.last_applied_seq` (and never broadcast a behind family resync to advanced subs) |
| Resync retry has no cross-tick pressure bound | high | **Cross-tick budget**: coalesced resync + exponential backoff + max provider calls / wall-clock window; control stays responsive |
| Outside-window convergence state undefined | medium | Explicit **`high_water_seq`** + clear/clear conditions; test `seq > last+W` |
| Process identifiers | low | Gate evidence always includes all five ids (no placeholders in completion section) |

**Still locked from earlier revisions (do not regress):**

- Field-exact empty `items` only (no whole-frame `encode_empty_tables_as_array`)
- Two-phase publish: HubRuntime admits during `invoke_plugin` pump; control fans out after invoke
- Monotonic family `last_accepted_seq` (never decreases)
- Bounded pending window W=16 + provider as durable truth
- Packages mutate first, then publish; no delivery wait in Lua

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| target_id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | `list_spawn_targets` path (not ambient cwd alone) |
| Base SHA | `90d0e1adac7a7d3c6efc815173014c68b95dbbf3` |
| Locked Core | `9d41ad4c614add7d15ff7e0f88b310a55627cd82` |

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]], [[botster-planner-playbook]]
- [[botster hub is a first party host profile over core]]
- [[package entity hydration uses explicit providers not mcp naming]]
- [[plugin-owned dynamic state uses plugin-namespaced entity frames]]
- [[botster plugin entity hydration has full id and scoped contracts]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[project pipelines mcp mutators avoid synchronous full entity snapshots]]
- [[mlua empty table serialization violates mcp spec claude code silently drops prompts]]
- [[plugin entity families publish filterable record supersets]]
- [[acceptance readiness requires the exact expected entity not any authoritative snapshot]]
- [[worker isolated and non blocking are different dispatch guarantees]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]] (control must stay bounded under resync pressure)

### Not loaded

- [[project-pipelines-playbook]] — not PP package policy
- [[botster runtime teardown lenses]] — `teardown_class_applies: false`

## Context loaded

- Ticket unblocks Workspaces Available sessions picker (`ticket_1786474780_590414`).
- Package `SubscribeEntities` is snapshot-only today; empty Lua `items = {}` fails `Vec` decode; no publish ABI.
- `invoke_plugin` pumps HubRuntime bridges only; cannot block on `DaemonControlState`.
- Entity **snapshots replace client store state** ([[botster entity snapshots are authoritative reconnect baselines]]); therefore a behind snapshot sent to an advanced subscriber is a product bug, not a harmless resync.

## Scope

### 1. Field-exact empty `items` encoding

- Default mlua decode.
- Coerce **only** top-level frame `items` when it is an empty JSON object → `[]`.
- Nested empty objects in rows / `entity` / `patch` remain `{}`.
- Forbidden: whole-frame `encode_empty_tables_as_array(true)`.

### 2. Live fanout — two-phase boundary

| Phase | Owner | When | Lua sees |
| --- | --- | --- | --- |
| Validate + sequence admit | `HubRuntime` (coordination-style bridge pumped by `invoke_plugin`) | During handler | Sync admission result |
| Fanout / targeted resync delivery | Daemon control path | After invoke returns (epilogue and/or drive) | N/A |

Forbidden: waiting on `DaemonControlState` or subscriber queues from the worker; re-entrant `invoke_plugin` for provider resync from inside publish admission.

### 3. Per-family runtime state (locked fields)

| Field | Meaning |
| --- | --- |
| `last_accepted_seq` | Monotonic family floor for admitted deltas / floor updates. **Never decreases.** |
| `high_water_seq` | Highest `snapshot_seq` observed on any **accepted, pending, or resync_scheduled** publish for the family (and max’d with applied provider snapshot seqs). **Never decreases.** Cleared/reset only when the package family is unloaded (state dropped), not on individual resync success. After successful convergence (`last_accepted_seq >= high_water_seq` and pending empty and no outstanding resync need), `high_water_seq` may equal `last_accepted_seq` (they stay in lockstep); it is not wiped independently. |
| `pending_by_seq` | Bounded map for `last+1 < seq ≤ last+W` with **W = 16** |
| `resync` | Coalesced schedule state: `needed: bool`, `next_eligible_at: Instant`, `attempts: u32`, `last_attempt_at: Option<Instant>` |

### 4. Admission rules (runtime → Lua)

| Condition | Action | Lua status |
| --- | --- | --- |
| `seq < last_accepted_seq` | Reject | `ok=false`, `stale_sequence` |
| `seq == last_accepted_seq` | Reject | `ok=false`, `duplicate_sequence` |
| `seq == last_accepted_seq + 1` | Accept; queue fanout; `last = seq`; `high_water = max(high_water, seq)`; drain consecutive pending | `ok=true`, `accepted` |
| `last+1 < seq ≤ last+W` | Store pending; `high_water = max(high_water, seq)`; mark resync needed | `ok=true`, `pending_gap` |
| `seq > last+W` | Do not store frame body; `high_water = max(high_water, seq)`; mark resync needed | `ok=true`, `resync_scheduled` |

Drain consecutive pending after each accept. Packages mutate durable state first, then publish.

### 5. Provider resync — targeted delivery + pressure bounds

#### 5a. Who receives a snapshot (locked)

Each live subscription tracks `last_applied_seq` (highest snapshot or delta seq successfully applied to that stream).

When a provider snapshot with `snapshot_seq = S` is obtained:

| Subscriber condition | Delivery |
| --- | --- |
| `S >= sub.last_applied_seq` **or** sub has never received a frame | Deliver snapshot; set `sub.last_applied_seq = S` |
| `S < sub.last_applied_seq` | **Do not deliver** this snapshot to that subscriber (would roll store backward) |

Family floor after provider result: `last_accepted_seq = max(last_accepted_seq, S)`; `high_water_seq = max(high_water_seq, S)`.

**Subscribe path:**

1. Invoke provider; deliver snapshot **only to the new subscription**.
2. Family floor: `last_accepted_seq = max(last_accepted_seq, S)` (never lower).
3. If `S < last_accepted_seq` (behind second subscriber while family already advanced): new sub is **catching_up** — do not send this behind snapshot to any other sub; schedule coalesced resync so the **catching_up** sub receives a later snapshot with `S' >= last_accepted_seq` (or at least `S' >= high_water` when high_water is the convergence target). Until then, **do not fanout family deltas with seq ≤ last_applied to them incorrectly** — for catching_up subs, hold delta delivery until they receive a non-behind snapshot (`sub.last_applied_seq` established via an allowed snapshot), then only deliver deltas with `seq == sub.last_applied_seq + 1` or resync again. Simplest implementable rule: **catching_up subs receive only targeted resync snapshots until `sub.last_applied_seq >= family.last_accepted_seq`**, then join normal delta fanout for `seq > sub.last_applied_seq` under the same last+1 rules relative to their applied seq **or** receive only family deltas after their applied seq matches floor (prefer: after catch-up snapshot `S' >= last_accepted_seq`, set applied=S' and thereafter receive the same accepted family deltas with `seq > S'` that other subs get — may need another resync if they missed intermediate deltas; safest catch-up is **only snapshots until `S' >= high_water_seq`**, then deltas).

**Locked catch-up rule (choose one, no options left):**

- A subscriber with `last_applied_seq < family.last_accepted_seq` is **catching_up**.
- Catching_up subscribers receive **only** provider snapshots that pass the non-behind rule for them (`S >= last_applied_seq`, and progress is `S > last_applied_seq` when possible).
- They **do not** receive shared delta fanout until `last_applied_seq >= family.last_accepted_seq`.
- Advanced subscribers **never** receive a snapshot with `S < their last_applied_seq`.
- Coalesced resync **targets** only: (a) catching_up subs, (b) overflowed subs, (c) gap-recovery when pending/high_water requires it — and when delivering, still filter per-sub by the non-behind rule. **Never** “send every snapshot to every live subscriber” without that filter.

#### 5b. Cross-tick pressure bound (locked)

Resync must not busy-loop the control path:

| Parameter | Locked value |
| --- | --- |
| Coalesce | At most **one** provider invocation attempt scheduled per family at a time |
| Initial backoff | **50 ms** after a needed flag is set (or immediate first attempt if no prior attempt this need cycle) |
| Backoff | Exponential: 50 ms → 100 → 200 → 400 → … cap **2 s** |
| Max attempts per need cycle | **8** provider calls; then mark family `resync_degraded` (typed daemon diagnostic / status counter), clear `needed` until a **new** publish or new catching_up subscribe re-arms need |
| Max rate | ≤ **2** provider calls per family per **1 s** wall clock (in addition to backoff) |
| Control isolation | Provider invoke uses existing worker timeout (`PLUGIN_EVENT_TIMEOUT_MS`); control loop must not block other clients beyond that single call; no nested publish admission during resync |

On attempt: if snapshot advances floor/high_water and pending drains / catching_up subs complete, clear `needed` and reset attempt counters. If still `last_accepted_seq < high_water_seq` or any catching_up sub remains, continue under backoff until max attempts.

**Degraded state:** does not permanently drop `high_water_seq`. A later successful publish or subscribe re-arms resync. Packages are still not required to re-publish solely for Hub races; degraded is a temporary pressure relief with observability.

### 6. Fanout of accepted deltas

For each accepted delta, deliver only to subscribers that are **not** catching_up and for whom `seq == sub.last_applied_seq + 1` (or `seq > sub.last_applied_seq` only when equal last+1 — stick to last+1 relative to sub). After delivery, `sub.last_applied_seq = seq`.

Overflow Full on one sub → mark that sub overflow/catching_up; schedule resync; do not fail other subs.

Unload: drop all family state and close subs.

### 7. Docs

Update `docs/lua-plugin-abi.md` with field-exact items, publish API, admission statuses, and authoring note that packages own increasing seq and durable provider truth.

## Non-scope

- Workspaces product; Web/TUI; Core redesign; auto plugin_db fanout; protocol bump unless DTO changes; session/session_type redesign; runtime-teardown class; PP package edits.
- Synchronous delivery ack to Lua.
- Broadcasting behind snapshots to advanced clients.

## Ownership boundaries and cross-repo dependencies

| Layer | Owns |
| --- | --- |
| **botster-hub** | Encoding, runtime admission, pending/high_water, targeted resync, pressure bounds, control fanout, proofs |
| **botster-core** | EntityFrame / EntityContract |
| **botster-workspaces** | Membership provider + publish after claim/remove |

Consumers `ticket_1786474780_590414` and `ticket_1786474783_285888` depend on this ticket. No Core prerequisite.

## Assumptions and unknowns

1. Provider reflects durable mutations (resync converges rows even when a delta frame was not retained outside W).
2. Snapshots replace client state → behind delivery is harmful.
3. Bounded degraded resync is acceptable under a stuck provider; control remains available.

Unknown (detail): exact diagnostic channel for `resync_degraded` (status counters vs frame error to catching_up only) — prefer lifecycle counters + logs; do not drop client connections.

## Affected surfaces/files

- `src/lua_runtime.rs`, `src/runtime.rs`, `src/daemon_transport.rs`, `src/local_webrtc.rs`
- `tests/hub_lua_runtime_test.rs`, `tests/hub_daemon_lifecycle_test.rs`
- `docs/lua-plugin-abi.md`

## Risks

| Risk | Mitigation |
| --- | --- |
| Behind snapshot rolls advanced sub | Per-sub filter; no broadcast without filter |
| Resync starves control | Backoff + max attempts + rate limit + test |
| Outside-W frame dropped forever | `high_water_seq` + resync until floor ≥ high_water or degraded+re-arm |
| Deadlock on control from invoke | Two-phase design |
| Nested empty → array | Field-exact items only |

## Acceptance checks/tests

### A. Encoding

1. `entity_provider_empty_items_table_becomes_json_array`
2. `entity_provider_empty_items_preserves_nested_empty_object_fields`
3. `entity_publish_patch_nested_empty_object_remains_object`

### B. Boundary

4. `daemon_package_entity_publish_from_surface_action_returns_before_fanout_and_stream_receives_frame`

### C. Order / convergence

5. `daemon_package_entity_held_open_receives_upsert_then_remove_without_resubscribe`
6. `daemon_package_entity_publish_rejects_stale_and_duplicate_sequence`
7. `daemon_package_entity_publish_gap_pending_then_accepts_in_order`
8. `daemon_package_entity_publish_out_of_order_with_behind_provider_converges_all_subscribers`
9. `daemon_package_entity_publish_concurrent_out_of_order_preserves_family_order`
10. `daemon_package_entity_publish_outside_pending_window_sets_high_water_and_converges`  
    Publish with `seq > last + 16`; provider initially behind; **no permanent loss**; all subs eventually reach durable high-water state **without package re-publish**; resync attempt count stays within the locked budget.
11. `daemon_package_entity_second_subscriber_behind_snapshot_does_not_roll_advanced_subscriber`  
    Sub A at N; Sub B’s first provider snapshot is N−1; **Sub A must not receive the N−1 snapshot** and must retain applied N state; Sub B catches up via later non-behind snapshot; family floor never decreases; no duplicate backward delta to A.
12. `daemon_package_entity_resync_under_stale_provider_is_pressure_bounded`  
    Force resync need with a provider that stays behind or slow; assert ≤ locked max attempts / rate; unrelated control requests (e.g. Status) still complete within a bound; degraded path observable.
13. `daemon_package_entity_held_open_fanout_over_local_webrtc`
14. `daemon_package_entity_publish_unload_closes_held_subscription`
15. `daemon_package_entity_subscriber_overflow_resyncs_from_provider`

### D. Exact commands

```sh
./test.sh --test hub_lua_runtime_test entity_provider_empty_items_table_becomes_json_array -- --exact --nocapture
./test.sh --test hub_lua_runtime_test entity_provider_empty_items_preserves_nested_empty_object_fields -- --exact --nocapture
./test.sh --test hub_lua_runtime_test entity_publish_patch_nested_empty_object_remains_object -- --exact --nocapture

./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_from_surface_action_returns_before_fanout_and_stream_receives_frame -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_held_open_receives_upsert_then_remove_without_resubscribe -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_rejects_stale_and_duplicate_sequence -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_gap_pending_then_accepts_in_order -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_out_of_order_with_behind_provider_converges_all_subscribers -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_concurrent_out_of_order_preserves_family_order -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_outside_pending_window_sets_high_water_and_converges -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_second_subscriber_behind_snapshot_does_not_roll_advanced_subscriber -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_resync_under_stale_provider_is_pressure_bounded -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_held_open_fanout_over_local_webrtc -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_publish_unload_closes_held_subscription -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test daemon_package_entity_subscriber_overflow_resyncs_from_provider -- --exact --nocapture

./test.sh --test hub_lua_runtime_test
./test.sh --test hub_daemon_lifecycle_test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Live evidence: Hub SHA, locked Core SHA, hub + `botster-session-worker` realpaths.

### E. Downstream

Hub acceptance = encoding + ordered/convergent held-open fanout (socket + WebRTC) under the contracts above. Workspaces exclude-set without surface refresh remains consumer-owned.

## Runtime-teardown class

`teardown_class_applies: false`

## Implementation sequence

1. Field-exact empty items tests.
2. Runtime state: last/high_water/pending/resync schedule + publish bridge.
3. Control fanout + **targeted** snapshot delivery filters.
4. Catching_up subscribe path; two-subscriber non-regression test.
5. Outside-W + pressure-bounded stale provider tests.
6. WebRTC, unload, overflow; docs; fmt/clippy.

## Vault gaps

After Implement: capture field-exact items; two-phase publish; monotonic floor + high_water; targeted snapshot delivery; resync pressure bounds.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Empty `items` | Field-exact empty object → `[]` only |
| Publish Lua result | Sync admission only |
| Admission owner | HubRuntime |
| Fanout owner | Control after invoke |
| Snapshot broadcast | **Never** behind to advanced subs |
| Catching_up sub | Snapshots only until applied ≥ family floor |
| Outside W | high_water + resync; no permanent silent loss |
| Resync pressure | Backoff, rate limit, max 8 attempts/cycle, then degraded+re-arm |
| Package re-publish for Hub races | Not required |

## Completion evidence (this Plan visit)

Filled at gate submission (not placeholders):

- `plan_uri`: `docs/plans/package-entity-mutation-fanout-and-empty-snapshot-array-encoding.md`
- `artifact_id`: set by `project_pipelines_add_artifact` on this visit
- `checklist_id`: set by vault checklist on this visit
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- `target_repository`: `botster-hub`
