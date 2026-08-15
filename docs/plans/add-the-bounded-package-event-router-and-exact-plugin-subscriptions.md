# Hub: add the bounded package event router and exact plugin subscriptions

## Plan Review revision

Plan Review `review_1786777843_757483` returned `changes_required`.
This second Plan visit keeps the Stage B product shape and answers the
four product findings plus the two process findings.

| Finding | Response |
| --- | --- |
| Hard-coded defaults that the ticket requires to be configurable | Hub-owned `PackageEventPlaneOptions` on `HubStartupOptions` / `HubConfig`. Ticket numbers are defaults. One validated `PackageEventPlanePolicy` is passed into the router. Tests cover default, override, invalid, and restart. `src/config.rs` is in scope. |
| Producer queue and global byte budget have no retirement contract | Exact queue state machine below. Every increment has a matching decrement. Unload, reload, shed, expiry, and failed admission are accounted. Tests require counters to return to baseline. |
| Live Hub proof was optional | Mandatory. Isolated Hub binary, two real package directories, public install/enable/load, emit/consume, worktree latency independence, recorded Hub SHA + lockfile Core SHA + both binary realpaths. |
| Bounded schema and audience not verifiable | Closed schema subset with size/nesting/keyword/reference limits. Subscription and delivery enforce `plugins` audience. Adversarial admission and negative audience tests. |
| Omitted [[project-pipelines-playbook]] | Loaded this visit. Artifact, gate, clean-subject, single-writer, and create-timeout rules constrain this run. |
| Empty Plan completion evidence | This visit submits `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, and `target_repository` on both `submit_gate` and `request_step_advance`. Reuses `checklist_1786776870_999225`. Does not create another checklist. |

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
- First Plan commit: `97c1cdc`. First Plan HEAD was `b1652b3`.
- `origin/main` at this visit: `b1652b3`. No new Hub main to merge.
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

### 3. Send-safe router

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

`try_ingress` is one non-blocking attempt. It never waits.

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
  remaining_holders,   // consumers still queued or in Background admit
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
| `try_admit` returns `Queued` | already popped | stays (in-flight) | unchanged |
| `try_admit` fails (`Backpressured`, `RejectedBudget`, `WorkerStopped`) | already popped | -= 1; retire if 0 | retire if 0 |
| Completion drain (success or fail) | already popped | -= 1; retire if 0 | retire if 0 |
| Age expiry while still queued | remove; count/bytes -= | -= 1; retire if 0 | retire if 0 |
| Consumer plugin unload/reload | drop that plugin's queue and subscriptions | -= 1 per dropped envelope; retire if 0 | retire if 0 |
| Producer package unload/reload | drop that owner's contracts and every remaining envelope of that owner from all consumer queues | all those envelopes retire | producer occupancy and those global bytes go to 0 for that owner |

Failed admission never leaves a stranded holder. Shed and expiry
never deliver stale payloads.

Tests must show producer count/bytes, every consumer count/bytes,
and global in-flight bytes return to the empty baseline after:

- successful delivery + completion
- shed_full (producer and consumer)
- queue-age expiry
- failed `try_admit`
- consumer reload and unload
- producer reload and unload

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

### 7. Causal scope

Hub-owned `CausalScopeTable` (Send; may sit beside the router, not
inside Core):

- Delivery mints a scope id and marks it live.
- The event-handler invocation carries that scope on the worker
  thread / host API.
- `events.emit` rejects with `rejected_causal_scope` while any scope
  is live for that invocation or for derived host work.
- `botster.entity_publish` admissions inherit the live scope onto
  the resulting fanout / provider-resync work.
- The scope stays live after the handler returns until completion
  drain **and** later owner work for those entity frames completes.
- Acceptance: an event → entity_publish → later emit cycle remains
  rejected after the first handler returns and that later owner work
  has run. Independent later work (new MCP/UI/worktree mutation with
  no inherited scope) may emit.

Do not add a public client or entity-frame field for the scope.
Keep it Hub-internal.

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
  delivery-wake observe, event in-flight on `CompletionDrain`
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
- Router gravity: putting HubRuntime/Lua/Core inside the new
  module. Mitigation: architecture import test.
- Hard-coding ticket defaults inside the router. Mitigation:
  policy object from `HubConfig` only.
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
6. Event handler `events.emit` is rejected. After that handler
   returns and later entity/owner work from it has run, a
   scope-inherited emit is still `rejected_causal_scope`.
7. `HubRuntime::emit_plugin_event` is gone. Architecture tests
   reject router imports of HubRuntime / CoreDaemon / mlua /
   persistence.
8. Existing worktree client response events still appear on the
   mutating `DaemonResponse`. Plugin delivery no longer uses
   blocking invoke.
9. Config defaults, overrides, invalid values, and restart policy
   replacement pass. Router counters return to baseline after
   delivery, shed, expiry, failed admit, reload, and unload.

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
- Causal scope outlives the event handler and follows derived
  entity/owner work.
- Exact owner plus name is the only subscription key; name-only
  `events.on` is gone.

Do not capture those as inbox notes from Plan. Implement / Verify
should capture only after the code exists.

## Delivery policy

- Direct-merge pipeline.
- Merge the completed and verified change directly into `main`.
- Do not create a pull request.
- Do not require human pull-request sign-off.
