# Hub: add the bounded package event router and exact plugin subscriptions

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
- Plan HEAD: `b1652b3` (matches `origin/main` at plan time).
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
- [[plan steps need reviewable plan artifacts]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Intentionally not loaded:

- [[project-pipelines-playbook]] — this ticket is Hub event-plane
  infrastructure, not Project Pipelines package/plugin policy. The PP
  `question.opened` ticket is a registered downstream consumer.
- [[botster runtime teardown lenses]] — `teardown_class_applies` is no.
  This is not WebRTC/peer, SessionIo/ClientWorker teardown, multi-peer
  ownership, CPU/battery/FD spin, or terminal-state vs live-runtime.
- [[botster-hub-client-playbook]] — no `SubscribeEvents`, no host-control
  DTO growth. Client `DaemonEvent::WorktreeLifecycle` on the mutating
  response stays as-is.
- [[botster-core-playbook]] — Core is a closed dependency, not this
  ticket's ownership charter. Event contracts, routing, budgets, and
  causal scope are Hub host policy.

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

Repo placement: Hub `docs/plans/` is living prior art on `main`
(including the Stage A plan). This artifact lives there.

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
- Precompile each `payload_schema` with `jsonschema` during package
  admission (install/enable/load). Fail admission on an unbounded or
  uncompilable schema.
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

### 2. Send-safe router

New module `src/package_event_router.rs`. The type is `Send + Sync`.
It must not import or call `HubRuntime`, `CoreDaemon`, `mlua`, plugin
persistence, or the owner loop. Architecture tests fail if it does.

The router owns:

- compiled contract lookup
- exact `(owner, name)` subscription index
- token buckets (package rate 100/s, burst 200)
- per-producer queue: 256 events, 512 KiB
- per-consumer queue: 128 events, 2 MiB
- global logical in-flight bytes: 16 MiB
- payload cap: 64 KiB
- fanout cap: 64
- max subscriptions per plugin: 64
- max subscribers per exact event: 64
- transient queue-age expiry
- one coalesced `AtomicBool` delivery-wake bit

`try_ingress` is one non-blocking attempt:

1. Reject undeclared, foreign-owner, invalid schema, oversize,
   over-rate, over-fanout, and wildcard operations with a typed
   result. Do not wait.
2. Serialize one size-capped `Arc` envelope.
3. Enqueue a reference onto the producer queue and each exact
   subscriber queue. A full consumer sheds for that consumer only.
   A full producer queue or global byte budget sheds the whole
   ingress with a typed result.
4. Set the delivery-wake bit.

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
- `shed_full`

### 3. `events.emit` and exact subscriptions

Lua ABI, breaking and documented:

```lua
events.on("runtime.producer", "sample.ready", function(event) ... end)
local result = events.emit("sample.ready", { ... })
-- result.status is one of the typed ingress names
```

- `events.on(owner, name, fn)` is the only subscription form. Reject
  the old single-name form, empty strings, and `*` / glob wildcards.
- `events.emit(name, payload)` always emits as the calling package.
  Packages cannot emit `hub.*` or another package's events.
- Emit from the plugin worker thread calls `Arc<PackageEventRouter>::try_ingress`
  directly. Do **not** pump emit through the
  `HubEntityPublishBridge` wait-for-owner path. That wait is the
  old blocking shape.
- Subscriptions update the router index at load/reload/unload. They
  are not scanned from `HubPluginLifecycle` on each emit.
- `session_family` stays on the entity-family plane. Only the
  registration ABI changes: `events.on("hub", "session_family", ...)`.
  Session-family frames still do not expire and still go through
  `HostBridge`, not this router.

### 4. Delivery through Core Background admission

The router does not invoke plugins. A new owner-loop slice
`MaintenanceSliceKind::PackageEventDelivery` does:

1. Observe `router.take_delivery_wake()` the same way journal-advanced
   wake is observed. A set bit is a coalesced `scheduler.try_wake()`.
   Plugin workers never call the scheduler.
2. Pull a bounded ready batch (item, byte, elapsed).
3. Drop envelopes whose queue-age expired; those shed rather than
   arrive stale.
4. `try_admit(PluginInvocationClass::Background)` one handler at a
   time. Never `invoke`. Never wait.
5. Track event-delivery in-flight separately from the session-family
   in-flight map. Reuse `CompletionDrain` to retire those request ids.
6. Yield after one slice.

A full or slow consumer cannot block the producer or another
consumer: their queues and `try_admit` results are independent.

### 5. Causal scope

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

### 6. Migrate worktree events and delete the old dispatcher

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

### 7. Docs and in-repo fixtures

Update `docs/lua-plugin-abi.md`, the README event section, and
`examples/synthetic-plugin/plugin.lua` to `events.on("hub", ...)`.
Add two test-only synthetic packages that declare, emit, and
subscribe to one exact admitted event. Do not implement Project
Pipelines `question.opened` here.

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

## Repository ownership boundaries and cross-repo dependencies

Hub owns package event admission, compiled contracts, the Send-safe
router, exact plugin subscriptions, budgets, causal scope, worktree
migration, and the Lua ABI.

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
- Current Core pin `aef6516` is sufficient.
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty.
- This is not a session-type eligibility consumer.
- Direct-merge pipeline. No pull request.
- `teardown_class_applies`: no.

Unknowns Implement must resolve by measurement, not invention:

- Exact owner-turn budget for the new delivery slice. Start from
  existing slice style (item + byte + elapsed) and keep
  `MAX_OWNER_TURN_MS` / `MAX_READY_OPERATION_WAIT_MS` unless
  isolated measurement requires a published change.
- Queue-age default: use a short transient TTL (Implement: pick one
  documented constant, prove expiry, do not make it configurable
  beyond the ticket's listed defaults).

## Affected surfaces/files

- `src/package_event_router.rs` (new)
- `src/packages.rs` — `HubPackageManifest` event declarations and
  admission-time schema compile
- `src/lua_runtime.rs` — `events.on(owner, name, fn)`, `events.emit`,
  causal-scope check on the worker
- `src/lifecycle.rs` — event handler records gain `owner`;
  subscription index updates on load/reload/unload
- `src/runtime.rs` — hold `Arc<PackageEventRouter>`; delete
  `emit_plugin_event`; keep `try_admit_plugin`
- `src/daemon_transport.rs` — worktree emit becomes `try_ingress`
- `src/daemon_maintenance.rs` — `PackageEventDelivery` slice,
  delivery-wake observe, event in-flight on `CompletionDrain`
- `src/lib.rs` — module export
- `Cargo.toml` — `jsonschema` if not already a hub-crate dep
  (ui-contract already uses `jsonschema` 0.49; do not add it to
  Core)
- `docs/lua-plugin-abi.md`, `README.md`
- `examples/synthetic-plugin/plugin.lua`
- `tests/hub_lua_runtime_test.rs`, new focused router + two-package
  + causal-scope + worktree-nonblocking tests
- Architecture test: router module import surface

## Risks

- Treating worker isolation as non-blocking again. Mitigation:
  delete `emit_plugin_event`; tests fail if worktree CRUD waits on
  a slow handler.
- Pumping `events.emit` through the entity-publish owner wait.
  Mitigation: emit is a direct `try_ingress` on a `Send` router.
- Router gravity: putting HubRuntime/Lua/Core inside the new
  module. Mitigation: architecture import test.
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

1. Package A declares and `events.emit`s one exact event. Package B
   subscribed with `events.on(A, name, fn)` receives it through
   Core `try_admit(Background)`.
2. Foreign, undeclared, invalid schema, oversize, over-rate,
   over-fanout, and wildcard operations return the typed results
   above and do not deliver.
3. A full or slow consumer cannot delay the producer or another
   consumer. Worktree create/delete returns while a subscribed
   handler is blocked or its queue is full.
4. An expired queued event sheds and does not arrive late.
5. Event handler `events.emit` is rejected. After that handler
   returns and later entity/owner work from it has run, a
   scope-inherited emit is still `rejected_causal_scope`.
6. `HubRuntime::emit_plugin_event` is gone. Architecture tests
   reject router imports of HubRuntime / CoreDaemon / mlua /
   persistence.
7. Existing worktree client response events still appear on the
   mutating `DaemonResponse`. Plugin delivery no longer uses
   blocking invoke.

Commands (after `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
from this checkout's target dir):

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --doc --workspace`
- `./test.sh --locked`

Focused Cargo tests are permitted during development. Do not invent
a replacement test wrapper.

Record Hub SHA, lockfile Core SHA, and both binary realpaths if any
live isolated-daemon proof is used
([[live hub proof records distinct hub and locked core binary provenance]]).

## Runtime-teardown class

`teardown_class_applies`: no.

Do not answer isolation/bounds/late-message/production-path
ownership fields from [[botster runtime teardown lenses]].

## Vault gaps worth capturing

After implementation, capture if still true and not already noted:

- Package event contracts live on `HubPackageManifest`, not Core
  `PackageManifest`.
- `events.emit` is a non-blocking router ingress, not an
  owner-pumped host bridge.
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
