# Hub: add the bounded package event router and exact plugin subscriptions

## Plan Review revision

Plan Review `review_1786780157_760971` returned `changes_required`.
The lossless owner-op and in-flight-lease findings stay resolved at
`7ce1ac6`. This fifth Plan visit answers one leftover: workers
cannot apply owner-thread-only ops.

| Finding | Response |
| --- | --- |
| Worker ingress cannot apply owner-thread-only operations | Only the owner loop calls `try_apply`. Workers never read `EventPlaneOwnerOps`. While an owner op is pending, the old package generation stays active. `Applied` removes contracts/subscriptions under `RouterInner` before the daemon response completes. Ingress during the pending window uses the old generation. Ingress after `Applied` sees the new generation or a typed reject. |

Earlier resolved findings stay resolved.
Duplicate vault checklist `checklist_1786776879_257442` remains unused.
This visit keeps `checklist_1786776870_999225`.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Spawn-target name: `botster-hub`.
- Authoritative target path is the admitted spawn-target path from
  `list_spawn_targets`, not the ambient process working directory.
- Pipeline ticket: `ticket_1786663582_483898`.
- Run: `run_1786776489_193956`.
- Project: Botster Non-Blocking Event Plane, Stage B Hub slice.
- Assigned worktree is the pipeline-created Hub worktree for this ticket.
- Plan commits: `97c1cdc`, `5a5aa73`, `4616ce3`, `7ce1ac6`.
- First Plan HEAD was `b1652b3`. `origin/main` at this visit: `b1652b3`.
  No new Hub main to merge.
- Required Core pin, exact, all Git-visible members:
  `https://github.com/trybotster/botster-core.git`
  rev `aef6516d5809d563961ed7fdd07da29a7b4edddc`.
  Do not float `branch = "main"`. This ticket does not need a new Core
  pin: `try_admit` / `PluginInvocationClass::Background` already exist
  and Hub Stage A already consumes them.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role overlays:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Planner must-load maps and orchestration notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[cross repo dependency registration must use dependency repo target]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[current botster is a modular repository family not the legacy trybotster monorepo]]

Workflow charter (loaded this visit because this run uses
direct-merge, artifacts, gates, and checklists):

- [[project-pipelines-playbook]]
- [[plan steps need reviewable plan artifacts]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[plan review routes process and infrastructure findings without full replanning]]
- [[verification evidence is scoped to a stable commit and clean tree]]
- [[pipeline run worktrees allow only one active writer]]
- [[project pipelines mcp create calls can time out after committing]]
- [[implement gate must verify committed work and pr link before review]]

Those notes constrain this run as follows:

- Plan must leave a committed `docs/plans/` artifact plus
  `artifact_id` on both the gate and `step.completed` evidence.
- This agent is the single writer of this run worktree until Plan
  Review takes it.
- Do not retry `create_vault_checklist` after a timeout. List first
  and reuse `checklist_1786776870_999225`.
- Direct-merge: Implement must commit on the ticket branch. A PR
  link is not required (`merge_policy: direct`).
- Gate commands must record commit + clean tracked state.

Hub charter notes implicated by this ticket:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[hub daemon runtime stays on one owner thread while socket handlers submit requests]]
- [[worker isolation now has a Core try-admit non-blocking primitive]]
- [[worker isolated and non blocking are different dispatch guarantees]]
- [[Core class-aware plugin admission reserves request-response executors]]
- [[plugin worker queue capacity and executor concurrency are independent host profile knobs]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[botster session worker requires explicit build in dogfood launchers]]
- [[live hub proof records distinct hub and locked core binary provenance]]

Process notes:

- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Intentionally not loaded:

- [[botster runtime teardown lenses]] — `teardown_class_applies` is no.
- [[botster-hub-client-playbook]] — no `SubscribeEvents`, no host-control
  DTO growth. Client `DaemonEvent::WorktreeLifecycle` on the mutating
  response stays as-is.
- [[botster-core-playbook]] — Core is a closed dependency, not this
  ticket's ownership charter.

This is not a Hub session-type eligibility consumer. Do not inject
`list_session_types_for_target` parent pins.

## Context loaded

Current production path (the thing to replace):

- Worktree create/delete in `src/daemon_transport.rs` persists, then
  calls `emit_worktree_lifecycle_event`.
- That helper serializes the payload and calls
  `HubRuntime::emit_plugin_event`, then also pushes
  `DaemonEvent::WorktreeLifecycle` onto the request response.
- `emit_plugin_event` looks up name-only Event handlers and maps each
  through blocking `invoke_plugin`. A slow handler adds latency to the
  CRUD request and to the single runtime owner thread
  ([[worker isolated and non blocking are different dispatch guarantees]]).
- Lua `events.on(name, fn)` records only an event name. There is no
  `events.emit`, no owner, no declared schema, no router, and no
  queue/rate/fanout/age bounds on this path.
- Session-family delivery is a different plane: owner-loop
  `HostBridge` / `CompletionDrain` slices already use
  `try_admit(Background)`. Do not merge session-family frames into the
  transient event router.

Stage A already landed on this HEAD (`b1652b3`): sliced projection,
`try_admit_plugin`, and `drain_plugin_completions`. Stage B adds the
generic package event plane those primitives were reserved for.

Hub package manifests already extend Core execution manifests
(`HubPackageManifest`). Event declarations belong on that Hub-owned
shape. Do not add them to Core `PackageManifest`.

Hub already owns startup policy in `src/config.rs`
(`HubStartupOptions` → validated `HubConfig`, including
`CoreEngineOptions`). Event-plane bounds follow that same host-policy
pattern. They are not compile-time constants inside the router.

Live package proof already exists as
`IsolatedHubBuilder` + `DaemonRequest::InstallPackageLocalPath` in
`tests/hub_daemon_lifecycle/packages.rs`. Stage B reuses that public
path, not in-process test-only package objects.

Repo placement: Hub `docs/plans/` is living prior art on `main`.

Worktree hygiene: tracked `.gitignore` is present and non-empty (53
bytes, matches HEAD). Worktree path has no `:`. No
`CARGO_TARGET_DIR` override is required for colon reasons.

## Scope

Replace the synchronous sequential worktree event adapter with one
generic package event plane.

### 1. Package-declared event contracts

Add a Hub-owned `events.emitted` array on `HubPackageManifest`:

```json
"events": {
  "emitted": [
    {
      "name": "sample.ready",
      "payload_schema": { "type": "object", "additionalProperties": false, "properties": { "...": {} } },
      "audience": ["plugins"]
    }
  ]
}
```

- Owner is the declaring package name. Reject a mismatched or
  wildcard owner field if one is supplied.
- Precompile each `payload_schema` during package admission
  (install/enable/load) using the bounded subset below. Fail
  admission on an uncompilable or unbounded schema.
- Register compiled contracts on the router keyed by exact
  `(owner, name)`.
- Built-in Hub contracts use reserved owner `hub` and are registered
  at runtime construction, not by a fake package:

  | owner | name | audience |
  | --- | --- | --- |
  | `hub` | `worktree_created` | `plugins` |
  | `hub` | `worktree_create_failed` | `plugins` |
  | `hub` | `worktree_deleted` | `plugins` |
  | `hub` | `worktree_delete_failed` | `plugins` |

  Payload schema matches the current sanitized worktree lifecycle
  shape. Reject any installable package named `hub`.

#### Bounded schema subset

Admission compiles only this closed subset. Any other keyword,
reference, or size is `rejected_invalid` at package admission.

| Limit | Value |
| --- | --- |
| Serialized schema document | 8 KiB |
| Nesting depth | 8 |
| Object properties per schema node | 32 |
| `enum` values | 32 |
| `required` names | 32 |

Allowed keywords: `type`, `properties`, `required`,
`additionalProperties` (bool or in-subset schema), `enum`, `const`,
`minLength`, `maxLength`, `minimum`, `maximum`, `exclusiveMinimum`,
`exclusiveMaximum`, `minItems`, `maxItems`, `items` (single in-subset
schema), `description`, `title`.

Rejected expansion, with adversarial tests:

- any `$ref`, `$dynamicRef`, `$recursiveRef`, `$anchor`, remote `$id`
- `pattern`, `patternProperties`, `unevaluatedProperties`,
  `unevaluatedItems`
- `allOf`, `anyOf`, `oneOf`, `not`, `if` / `then` / `else`
- `dependentSchemas`, `prefixItems`, `contains`, `propertyNames`
- remote `$schema` fetch or any network load during compile

#### Audience enforcement

`audience` is a non-empty set of `plugins` and/or `clients`.

- Plugin `events.on` succeeds only when the contract's audience
  contains `plugins`.
- Delivery to plugin workers happens only for `plugins` audience.
- A `clients`-only contract may be stored for Stage C. A plugin
  subscription to it fails. An emit of it does not deliver to any
  plugin.
- Missing or empty audience fails admission.

### 2. Configurable Hub policy, not router constants

Ticket values are **initial configurable defaults**, not hard-coded
router internals. Hub owns them under [[botster-hub-playbook]] the
same way it owns `CoreEngineOptions`.

Add `PackageEventPlaneOptions` on `HubStartupOptions` and
`HubConfig` (`src/config.rs`). `deny_unknown_fields`. Defaults:

| Field | Default |
| --- | --- |
| `payload_max_bytes` | 65536 (64 KiB) |
| `subscriptions_per_plugin_max` | 64 |
| `subscribers_per_event_max` | 64 |
| `fanout_per_emit_max` | 64 |
| `producer_queue_max_events` | 256 |
| `producer_queue_max_bytes` | 524288 (512 KiB) |
| `consumer_queue_max_events` | 128 |
| `consumer_queue_max_bytes` | 2097152 (2 MiB) |
| `global_in_flight_bytes` | 16777216 (16 MiB) |
| `package_rate_per_sec` | 100 |
| `package_burst` | 200 |
| `queue_age` | 1000 ms |

Validation (`HubConfigError`, same style as
`validate_positive_usize`):

- every count and byte field `>= 1`
- `queue_age >= 1ms`
- `payload_max_bytes <= producer_queue_max_bytes`
- `payload_max_bytes <= consumer_queue_max_bytes`
- `payload_max_bytes <= global_in_flight_bytes`
- `producer_queue_max_bytes <= global_in_flight_bytes`
- `fanout_per_emit_max <= subscribers_per_event_max`
- `package_burst >= package_rate_per_sec`

`build_config` produces one validated `PackageEventPlanePolicy`.
The router is constructed with that policy only. The router does not
read env, files, or `HubConfig` itself.

Tests in `src/config.rs` (and a router construction test):

- defaults serialize/deserialize and match the table
- an override set is accepted and is the policy the router reports
- each invalid and each cross-field violation is rejected by field
- restart: a second `HubStartupOptions` / new router with a
  different valid override does not keep the first policy

Do not add extra knobs beyond this table.

### 3. Send-safe router and try-only synchronization

New module `src/package_event_router.rs`. The type is `Send + Sync`.
It must not import or call `HubRuntime`, `CoreDaemon`, `mlua`, plugin
persistence, or the owner loop. Architecture tests fail if it does.

The router owns:

- compiled contract lookup
- exact `(owner, name)` subscription index
- token buckets from policy
- producer occupancy, per-consumer queues, global logical bytes
- payload, fanout, subscription, and subscriber caps from policy
- transient queue-age expiry from policy
- one coalesced `AtomicBool` delivery-wake bit
- admitted-holder table keyed by `(envelope_id, holder_id)`

`try_ingress` is one non-blocking attempt. It never waits.

#### Synchronization

Shared maps, indexes, token buckets, queues, and counters live in
one `std::sync::Mutex<RouterInner>`. This follows the existing
`try_lock` / `TryLockError` pattern in
`src/webrtc_terminal_adapter.rs`.

Rules:

- Every router API uses `try_lock` only. `lock()` is forbidden.
  Architecture/unit tests fail if the module calls `Mutex::lock`.
- `TryLockError::WouldBlock` never retries and never parks.
- `TryLockError::Poisoned` uses `into_inner()` and treats that call
  as `shed_busy` (fail closed for the caller, recover the mutex).
- Critical section is only the map/counter update. No I/O, no
  `try_admit`, no Lua, no owner-loop call under the lock.

Contention results:

| Caller | WouldBlock |
| --- | --- |
| `try_ingress` | `shed_busy` |
| `try_subscribe` | `shed_busy` |
| Delivery / expire slice | skip this slice, leave delivery-wake set |
| Owner `try_apply(OwnerOp)` | do not complete the daemon unload/reload; leave the op in the owner-loop registry and wake |

#### Lossless owner operations

Do **not** store unload/reload inside the router behind another
`try_lock` queue or an atomic name slot. That path is lossy.

The owner loop owns `EventPlaneOwnerOps` on `DaemonControlState` /
`MaintenanceState` (owner thread only, no mutex):

```
BTreeMap<PackageName, VecDeque<OwnerOp>>
OwnerOp { kind: Unload | Reload, generation }
```

Rules:

1. A package unload/reload request records the keyed `OwnerOp` and
   the owner loop calls `router.try_apply(op)`.
2. The daemon request stays open until `try_apply` returns
   `Applied`. `WouldBlock` is not success. The owner turn yields
   and retries on the next slice. The caller never waits on the
   router mutex.
3. Two packages have two keys. Concurrent unloads cannot overwrite
   each other.
4. Unload then reload on one package is two queued ops in order.
   Reload does not apply before its preceding unload.
5. **Only the owner loop calls `try_apply`.** Plugin workers never
   read `EventPlaneOwnerOps`. That map is owner-thread state and
   is not shared, mutexed, or visible to `try_ingress`.
6. While an `OwnerOp` is pending, the **old package generation
   remains active**. Ingress, subscribe, and delivery continue to
   use the pre-op contracts and subscriptions.
7. `Applied` is the only transition. It removes or replaces
   contracts and subscriptions under `RouterInner` **before** the
   daemon response completes.
8. After `Applied`, remove that op and then complete the daemon
   response. Later ingress sees the new generation or a typed
   reject (`rejected_undeclared` / `rejected_foreign` after
   unload; the replacement contracts after reload).

There is no worker-drain path and no discarded owner name.

Tests:

- A test hook holds `Inner`. Concurrent `try_ingress` returns
  `shed_busy` without blocking past a tight bound (for example 5 ms).
- Two concurrent emitters under the default producer cap never
  exceed producer count, producer bytes, global bytes, or fanout.
- Owner delivery `try_lock` WouldBlock leaves queues unchanged and
  keeps the wake bit.
- Held `Inner`: two owners' unloads both remain in
  `EventPlaneOwnerOps` and both `Applied` after the lock is
  released. Neither request completes early.
- Held `Inner`: unload then reload on one owner applies in that
  order. Reload never sees the pre-unload contracts.
- Pending window: after unload is recorded and before `Applied`,
  `try_ingress` still uses the old generation (accept or other
  typed result, not the post-unload reject). After `Applied`,
  the same emit is `rejected_undeclared` / `rejected_foreign`
  or follows the new generation.

`shed_busy` is a typed shed. It does not increment count, bytes,
fanout, or token-bucket tokens.

Token-bucket, occupancy, and fanout checks run only after `Inner`
is held, so two concurrent emitters cannot over-admit.

Typed ingress results (Lua and Rust share the same names):

- `accepted`
- `rejected_undeclared`
- `rejected_foreign`
- `rejected_invalid`
- `rejected_oversize`
- `rejected_over_rate`
- `rejected_over_fanout`
- `rejected_wildcard`
- `rejected_causal_scope`
- `rejected_audience`
- `shed_full`
- `shed_busy`

### 4. Queue state machine and retirement

One accepted envelope has one id, one `Arc` payload, and one logical
size (`serialized_bytes`). Global in-flight bytes count that size
**once**, not once per subscriber.

Producer occupancy is backpressure for that owner: how many of that
producer's envelopes are still live. It is not a second delivery
queue of payload copies.

Consumer queues are the delivery queues.

```
Envelope {
  id,
  owner, name,
  payload: Arc<[u8]>,
  size,
  enqueued_at,
  remaining_holders,   // queued consumer copies + admitted Background jobs
}

HolderId = (consumer_plugin_key, generation)

AdmittedHolder {
  envelope_id,
  holder_id,
  request_id,          // Core Background request
  retired: bool,       // idempotent retire flag
}
```

#### Increment

`try_ingress` after validation:

1. If selected exact `plugins`-audience subscribers exceed
   `fanout_per_emit_max`, return `rejected_over_fanout` with no
   enqueue.
2. If producer occupancy would exceed event or byte max, or global
   bytes would exceed the budget, return `shed_full` with no enqueue.
3. For each selected subscriber, if that consumer queue is full,
   shed that consumer only. Do not increment that consumer.
4. If zero consumers accepted the envelope (no subscribers, or all
   consumer queues full), do not increment producer or global.
   Return `accepted` when there were no subscribers.
   Return `shed_full` when every selected consumer was full.
5. Otherwise increment:
   - producer occupancy count += 1, bytes += size
   - global in-flight bytes += size (once)
   - each accepting consumer count += 1, bytes += size
   - `remaining_holders` = accepting consumer count
6. Set the delivery-wake bit.

#### Decrement / retire

An envelope is fully retired when `remaining_holders` reaches 0.
Retirement decrements producer occupancy count by 1 and bytes by
`size`, and decrements global in-flight bytes by `size`.

| Event | Consumer queue | remaining_holders | Producer / global |
| --- | --- | --- | --- |
| Delivery slice pops a consumer entry | count/bytes -= 1/size | unchanged yet | unchanged |
| `try_admit` returns `Queued` | already popped | stays; insert `AdmittedHolder` | unchanged |
| `try_admit` fails (`Backpressured`, `RejectedBudget`, `WorkerStopped`) | already popped | `retire_holder` once | retire if 0 |
| Completion drain (success or fail) | already popped | `retire_holder` once | retire if 0 |
| Age expiry while still **queued** | remove; count/bytes -= | `retire_holder` once | retire if 0 |
| Consumer unload/reload, **queued** copies | drop that plugin's queue and subscriptions | `retire_holder` once per queued copy | retire if 0 |
| Consumer unload/reload, **admitted** jobs | already popped | unchanged until Core completion or cancel | unchanged |
| Producer unload/reload, **queued** copies | drop that owner's contracts and queued copies from all consumer queues | `retire_holder` once per queued copy | decrement only those queued copies |
| Producer unload/reload, **admitted** jobs | already popped | unchanged until Core completion or cancel | occupancy/bytes stay until those holders retire |

`retire_holder(envelope_id, holder_id)` is the only decrement path
for a holder. If `AdmittedHolder.retired` is already true, it is a
no-op. Late completion after unload cannot underflow or double-decrement.

#### Unload versus admitted work

Producer unload/reload:

1. Remove that owner's contracts immediately. Later `events.emit`
   from that owner is `rejected_undeclared` or `rejected_foreign`.
2. Drop only **queued** holders (still in consumer queues).
3. Leave `AdmittedHolder` rows until `CompletionDrain` or an explicit
   Core cancel that still surfaces a completion.
4. Do not zero producer occupancy or global bytes for admitted
   envelopes. Those bytes stay reserved so a replacement load of the
   same package cannot over-admit against live jobs.
5. After the last admitted holder retires, producer occupancy for
   that owner is 0 and the name may be reused.

Consumer unload/reload:

1. Remove that plugin's subscriptions immediately.
2. Drop only **queued** copies for that consumer.
3. Leave that consumer's `AdmittedHolder` rows until completion or
   cancel. Completions still call `retire_holder` and are idempotent
   if unload already retired a queued copy of a different holder id.

Failed admission never leaves a stranded holder. Shed and expiry
never deliver stale payloads. Age expiry does **not** apply to
admitted holders; those are Core jobs with their own deadline.

Tests must show producer count/bytes, every consumer count/bytes,
and global in-flight bytes return to the empty baseline after:

- successful delivery + completion
- shed_full and shed_busy (no increment)
- queue-age expiry of queued copies
- failed `try_admit`
- consumer reload and unload with no in-flight jobs
- producer reload and unload with no in-flight jobs
- **blocked in-flight unload:** admit one job, unload producer,
  assert occupancy/bytes still reserved, emit from a replacement
  load sheds or rejects until completion, then one completion
  retires exactly once (no underflow, no second decrement)

Router debug snapshot (test-visible) exposes those counters. Do not
add a public client DTO for them in this ticket.

### 5. `events.emit` and exact subscriptions

Lua ABI, breaking and documented:

```lua
events.on("runtime.producer", "sample.ready", function(event) ... end)
local result = events.emit("sample.ready", { ... })
-- result.status is one of the typed ingress names
```

- `events.on(owner, name, fn)` is the only subscription form. Reject
  the old single-name form, empty strings, and `*` / glob wildcards.
- Subscribe also rejects when the contract is missing or its
  audience does not contain `plugins` (`rejected_audience` /
  `rejected_undeclared`).
- `events.emit(name, payload)` always emits as the calling package.
  Packages cannot emit `hub.*` or another package's events.
- Emit from the plugin worker thread calls
  `Arc<PackageEventRouter>::try_ingress` directly. Do **not** pump
  emit through the `HubEntityPublishBridge` wait-for-owner path.
- Subscriptions update the router index at load/reload/unload. They
  are not scanned from `HubPluginLifecycle` on each emit.
- `session_family` stays on the entity-family plane. Only the
  registration ABI changes: `events.on("hub", "session_family", ...)`.
  Session-family frames still do not expire and still go through
  `HostBridge`, not this router.

### 6. Delivery through Core Background admission

The router does not invoke plugins. A new owner-loop slice
`MaintenanceSliceKind::PackageEventDelivery` does:

1. Observe `router.take_delivery_wake()` the same way journal-advanced
   wake is observed. A set bit is a coalesced `scheduler.try_wake()`.
   Plugin workers never call the scheduler.
2. Pull a bounded ready batch (item, byte, elapsed).
3. Drop envelopes whose queue-age expired; those shed and decrement
   as in the table above.
4. `try_admit(PluginInvocationClass::Background)` one handler at a
   time. Never `invoke`. Never wait.
5. Track event-delivery in-flight separately from the session-family
   in-flight map. Reuse `CompletionDrain` to retire those request ids
   and close causal scopes.
6. Yield after one slice.

A full or slow consumer cannot block the producer or another
consumer: their queues and `try_admit` results are independent.

### 7. Causal scope lease machine

Hub-owned `CausalScopeTable` (Send; sits beside the router, not
inside Core or a public DTO). A scope is live while its lease count
is greater than zero. `events.emit` rejects with
`rejected_causal_scope` when the calling invocation carries a live
scope id.

```
CausalScope {
  id,
  leases: u32,
}

LeaseKind {
  EventInFlight { request_id },
  PendingEntityPublish { plugin_key },
  AdmittedEntityMutation { family, seq },
  ProviderResyncNeed { family },
  ProviderInFlight { request_id },
}
```

The invocation carries `scope_id` in host-API thread-local state
set by the delivery adapter or by a later scoped plugin invoke.
Do not add a public client or `EntityFrame` field.

#### Acquire / release

| Site | Acquire | Release |
| --- | --- | --- |
| Event handler `try_admit` returns `Queued` | +1 `EventInFlight` | `CompletionDrain` success or fail, or failed admit (no acquire) |
| Worker pushes `entity_publish` onto `HubEntityPublishBridge` while thread-local scope is live | +1 `PendingEntityPublish` | Owner fulfill error, drop, or timeout |
| Owner admits that publish (`accepted` / `pending_gap` / `resync_scheduled`) | convert pending lease → `AdmittedEntityMutation` (count unchanged) | Fanout of that mutation finishes |
| Admit schedules provider resync (`resync_scheduled` or later gap) | +1 `ProviderResyncNeed` | Resync no longer needed, degraded, or max attempts |
| `drive_package_entity_resync` `try_admit`s the package `entity_provider` | +1 `ProviderInFlight` | That provider completion or failed admit |
| Isolated drop of a pending publish channel that will never be fulfilled | | release `PendingEntityPublish` only |

Plugin reload/unload does **not** release `EventInFlight` or
`ProviderInFlight`. Those stay until `CompletionDrain` or a
confirmed Core cancellation that still produces a completion.

On unload/reload:

1. Stop new leases. No new `EventInFlight`, `PendingEntityPublish`,
   or `ProviderInFlight` for that plugin.
2. Drop only work that cannot execute: still-queued event copies
   (already in the holder table) and `PendingEntityPublish` rows
   whose bridge request will be failed without running plugin code.
3. Keep `EventInFlight` and `ProviderInFlight` until completion or
   confirmed cancel. A still-running admitted handler can call
   `events.emit`; that must stay `rejected_causal_scope`.
4. Keep `AdmittedEntityMutation` and `ProviderResyncNeed` until
   their owner-loop fanout/resync rows finish, even if the plugin
   is gone. Those rows cannot start a new plugin emit once the
   plugin is unloaded; they still hold the lease so a replacement
   load cannot emit inside the same scope.

Close the scope only when `leases == 0`. Releases are idempotent
per `LeaseKind` identity. Early unload must not drive the count to
zero while an admitted invocation is still live.

#### Real later plugin callback

The cycle the ticket names is not a second Event handler. After the
first Event handler returns, later owner work is:

1. `HubEntityPublishBridge` fulfill on the owner thread
2. package-entity fanout
3. `MaintenanceSliceKind::ProviderResync` →
   `drive_package_entity_resync` → the package's
   `entity_provider` handler through plugin admission

That `entity_provider` invocation is the real later plugin callback.
It inherits the scope id. Its `events.emit` is
`rejected_causal_scope`.

Acceptance tests:

1. Event handler `events.emit` → `rejected_causal_scope`.
2. After the handler returns, the inherited `entity_provider`
   resync invoke calls `events.emit` → still
   `rejected_causal_scope`.
3. After every lease is released (`leases == 0`), a fresh
   RequestResponse invoke (MCP/tool, no inherited scope) from the
   same plugin `events.emit`s successfully.
4. Blocked admitted-invocation unload: admit an Event or
   `entity_provider` job, unload the plugin, prove a derived
   `events.emit` from that still-running job is
   `rejected_causal_scope`, prove one completion releases the
   lease once, and prove a replacement-load independent emit
   succeeds only after `leases == 0`.

Do not invent a second event-handler re-entry to satisfy this
proof. Use the production `entity_provider` path.

### 8. Migrate worktree events and delete the old dispatcher

After a successful or failed worktree persist, call `try_ingress`
with owner `hub` and return the daemon response immediately.

- Keep `DaemonEvent::WorktreeLifecycle` on the mutating response.
  That is request-scoped host control, not plugin delivery, and not
  Stage C `SubscribeEvents`.
- Delete `HubRuntime::emit_plugin_event` and the sequential
  `invoke_plugin` map. Update
  `events_on_registers_exact_event_subscription_and_invokes_worker_handler`
  to the owner+name + router + Background path.
- Worktree operations must return without waiting for handlers.

### 9. Docs and fixtures

Update `docs/lua-plugin-abi.md`, the README event section, and
`examples/synthetic-plugin/plugin.lua` to `events.on("hub", ...)`.
Add two on-disk synthetic packages used by the live IsolatedHub
proof. Do not implement Project Pipelines `question.opened` here.

## Non-scope

- Client `SubscribeEvents` / `UnsubscribeEvents` (Hub ticket
  `ticket_1786663583_640263`, Stage C).
- Web or TUI event consumption.
- Project Pipelines `question.opened` product emit
  (`ticket_1786663583_568924`, already depends on this ticket).
- Saturated-event load campaign
  (`ticket_1786663585_879846`).
- Replay, public sequence, consumer cursor, durable event flag,
  recovery-family field, wildcard subscription, Hub event history.
- Changing session-family snapshot/gap/expiry semantics.
- Changing Core, ClientWorker, SessionIo, terminal Drain, or
  WebRTC teardown.
- Floating or bumping the Core pin.
- Publishing `@trybotster/hub-test-support` unless shipped fixture
  bytes actually change. Prefer not to.
- Dual-pipelining teardown-lens implementation.
- Extra event-plane knobs beyond the policy table above.

## Repository ownership boundaries and cross-repo dependencies

Hub owns package event admission, compiled contracts, the Send-safe
router, exact plugin subscriptions, configurable budgets, causal
scope, worktree migration, and the Lua ABI.

Core owns policy-free `try_admit`, `PluginInvocationClass`, and
`drain_completions`. Hub chooses `Background` for event handlers.

Packages own namespaced event names, payload schemas, and product
reactions. Hub contains no `botster-workspaces` or Project Pipelines
product names.

Clients do not gain a new event subscription API in this ticket.

Registered prerequisites (both closed):

| Ticket | Target | Repo | Status |
| --- | --- | --- | --- |
| `ticket_1786663582_169720` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (Stage A projection + Background adoption) |
| `ticket_1786663581_723222` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | closed (`try_admit`) |

No new Core, Web, TUI, or Workspaces dependency. Do not implement
those repos in this run.

Registered downstream consumer (already present; do not duplicate):

| Ticket | Target | Repo |
| --- | --- | --- |
| `ticket_1786663583_568924` | `tgt_a72ca1a83d504385b8648f71409119ab` | botster-project-pipelines |

Later Stage C / client / integration tickets stay on their own
targets and must not start from this Hub run.

## Assumptions and unknowns

Assumptions:

- Target routing from `list_spawn_targets` is authoritative.
- Built-in owner string is `hub`. Package owner is the package name.
- `events.on` takes two exact strings. Old name-only form is a
  typed reject, not a compatibility alias.
- `session_family` keeps its current delivery plane; only the
  subscription ABI gains an owner.
- `DaemonEvent::WorktreeLifecycle` on the mutating response stays.
- Causal scope is Hub-internal and is not a public DTO field.
- Audience `clients` may be stored on a contract but is not
  delivered in this ticket.
- Event-plane bounds are Hub startup policy, not router constants.
- Queue-age default is 1000 ms because the ticket listed no TTL.
  It is configurable.
- Router synchronization is `try_lock` only. Contention is
  `shed_busy`, not a wait.
- Unload/reload completion waits on owner-loop `try_apply`, not on
  a router mutex. Workers never apply owner ops. The old generation
  stays active until `Applied`.
- The later event-to-entity-to-event callback is the package
  `entity_provider` driven by `drive_package_entity_resync`.
- `EventInFlight` and `ProviderInFlight` survive plugin unload.
- Current Core pin `aef6516` is sufficient.
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty.
- This is not a session-type eligibility consumer.
- Direct-merge pipeline. No pull request.
- `teardown_class_applies`: no.
- This Plan visit reuses `checklist_1786776870_999225`.

Unknowns Implement must resolve by measurement, not invention:

- Exact owner-turn budget for the new delivery slice. Start from
  existing slice style (item + byte + elapsed) and keep
  `MAX_OWNER_TURN_MS` / `MAX_READY_OPERATION_WAIT_MS` unless
  isolated measurement requires a published change.

## Affected surfaces/files

- `src/config.rs` — `PackageEventPlaneOptions`, validation,
  defaults, serde, restart/override/invalid tests
- `src/package_event_router.rs` (new)
- `src/packages.rs` — `HubPackageManifest` event declarations,
  bounded schema compile, audience, reserved `hub` name
- `src/lua_runtime.rs` — `events.on(owner, name, fn)`, `events.emit`,
  causal-scope check on the worker
- `src/lifecycle.rs` — event handler records gain `owner`;
  subscription index updates on load/reload/unload
- `src/runtime.rs` — hold `Arc<PackageEventRouter>` constructed from
  validated policy; delete `emit_plugin_event`; keep `try_admit_plugin`
- `src/daemon_transport.rs` — worktree emit becomes `try_ingress`
- `src/daemon_maintenance.rs` — `PackageEventDelivery` slice,
  delivery-wake observe, event in-flight on `CompletionDrain`,
  owner-thread `EventPlaneOwnerOps`
- `src/daemon.rs` / `src/daemon_transport.rs` — hold unload/reload
  responses until `try_apply` returns `Applied`
- `src/lib.rs` — module export
- `Cargo.toml` — `jsonschema` if not already a hub-crate dep
  (ui-contract already uses `jsonschema` 0.49; do not add it to
  Core)
- `docs/lua-plugin-abi.md`, `README.md`
- `examples/synthetic-plugin/plugin.lua`
- `tests/hub_lua_runtime_test.rs`
- `tests/hub_daemon_lifecycle/` — mandatory IsolatedHub proof with
  two real package directories through
  `DaemonRequest::InstallPackageLocalPath` / enable / load
- Architecture test: router module import surface
- Router counter baseline tests listed in the state machine

## Risks

- Treating worker isolation as non-blocking again. Mitigation:
  delete `emit_plugin_event`; live IsolatedHub test fails if
  worktree CRUD waits on a slow handler.
- Pumping `events.emit` through the entity-publish owner wait.
  Mitigation: emit is a direct `try_ingress` on a `Send` router.
- Blocking `Mutex::lock` on the Send router. Mitigation: `try_lock`
  only, `shed_busy`, held-lock and concurrent-emitter tests.
- Router gravity: putting HubRuntime/Lua/Core inside the new
  module. Mitigation: architecture import test.
- Hard-coding ticket defaults inside the router. Mitigation:
  policy object from `HubConfig` only.
- Unload retiring admitted Background jobs and double-decrement on
  late completion. Mitigation: queued-only drop, idempotent
  `retire_holder`, in-flight unload tests.
- Lossy owner-op fallback dropping a second package unload.
  Mitigation: owner-loop keyed `EventPlaneOwnerOps`; request stays
  open until `Applied`.
- Workers applying owner-thread ops. Mitigation: only the owner
  loop calls `try_apply`; pending window keeps the old generation.
- Closing causal scope at handler return, or releasing in-flight
  leases on unload. Mitigation: lease table; `EventInFlight` /
  `ProviderInFlight` survive unload; `entity_provider` resync is
  the later callback.
- Leaking producer/global bytes across unload or failed admit.
  Mitigation: remaining_holders table and baseline counter tests.
- Starving session-family or ready operations with event delivery.
  Mitigation: one bounded slice per turn; Background cannot consume
  reserved RequestResponse capacity
  ([[Core class-aware plugin admission reserves request-response executors]]).
- Closing causal scope at handler return and allowing
  event → entity → event. Mitigation: scope outlives completion
  drain and derived owner work.
- Quietly doing Stage C client subscriptions or PP
  `question.opened`. Mitigation: explicit non-scope.
- Breaking in-repo `events.on("worktree_created")` and
  `events.on("session_family")` fixtures without updating them.

## Acceptance checks/tests

Production path that must be proven, not merely present:

1. **Mandatory live IsolatedHub proof.** Spawn the real
   `botster-hub` binary through `IsolatedHubBuilder` with
   `CARGO_BIN_EXE_botster-hub` and the lockfile-built
   `botster-session-worker`. Install, enable, and load two on-disk
   packages through `DaemonRequest::InstallPackageLocalPath` and the
   public enable/load path (same family as
   `tests/hub_daemon_lifecycle/packages.rs`). Package A declares and
   `events.emit`s one exact event. Package B subscribed with
   `events.on(A, name, fn)` receives it through Core
   `try_admit(Background)`. Then unload both packages. Record Hub
   SHA, lockfile Core SHA, and both binary realpaths under this
   checkout ([[live hub proof records distinct hub and locked core binary provenance]]).
   Test-only in-process package objects do not satisfy this item.
2. Foreign, undeclared, invalid schema, oversize, over-rate,
   over-fanout, wildcard, and `clients`-only audience operations
   return the typed results above and do not deliver to plugins.
3. Adversarial schema admission fails for oversize documents,
   excess nesting, `$ref`, and unsupported keywords.
4. A full or slow consumer cannot delay the producer or another
   consumer. On the live Hub, worktree create/delete returns while a
   subscribed handler is blocked or its queue is full. Measure that
   the mutating response stays within
   `MAX_READY_OPERATION_WAIT_MS`.
5. An expired queued event sheds and does not arrive late.
6. Causal-scope lease machine: Event handler emit is rejected.
   After that handler returns, the production `entity_provider`
   invoked by `drive_package_entity_resync` still gets
   `rejected_causal_scope`. Unload during an admitted Event or
   provider job keeps that lease until completion. After
   `leases == 0`, a fresh RequestResponse invoke emits
   successfully.
7. `HubRuntime::emit_plugin_event` is gone. Architecture tests
   reject router imports of HubRuntime / CoreDaemon / mlua /
   persistence, and reject `Mutex::lock` in the router module.
8. Existing worktree client response events still appear on the
   mutating `DaemonResponse`. Plugin delivery no longer uses
   blocking invoke.
9. Config defaults, overrides, invalid values, and restart policy
   replacement pass. Router counters return to baseline after
   delivery, shed_full, shed_busy, expiry, failed admit, reload,
   unload, and in-flight producer unload plus one late completion.
10. Held-lock `try_ingress` returns `shed_busy` without blocking.
    Concurrent emitters cannot over-admit count, bytes, or fanout.

Commands (after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
from this checkout's target dir):

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --doc --workspace`
- `./test.sh --locked`

Record commit and clean tracked state before and after those gates
([[verification evidence is scoped to a stable commit and clean tree]]).

Focused Cargo tests are permitted during development. Do not invent
a replacement test wrapper.

## Runtime-teardown class

`teardown_class_applies`: no.

Do not answer isolation/bounds/late-message/production-path
ownership fields from [[botster runtime teardown lenses]].

## Vault gaps worth capturing

After implementation, capture if still true and not already noted:

- Package event contracts live on `HubPackageManifest`, not Core
  `PackageManifest`.
- Event-plane bounds are Hub startup policy validated into one
  router policy object.
- `events.emit` is a non-blocking router ingress, not an
  owner-pumped host bridge.
- Producer occupancy and global bytes retire when
  `remaining_holders` reaches zero.
- Admitted holders survive producer unload until Core completion.
- Router ingress uses `try_lock` only; contention is `shed_busy`.
- Causal-scope leases close only at zero and follow
  `entity_provider` resync.
- Exact owner plus name is the only subscription key; name-only
  `events.on` is gone.

Do not capture those as inbox notes from Plan. Implement / Verify
should capture only after the code exists.

## Delivery policy

- Direct-merge pipeline.
- Merge the completed and verified change directly into `main`.
- Do not create a pull request.
- Do not require human pull-request sign-off.
