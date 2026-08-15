# Lua Plugin ABI

`botster-hub` loads local package entrypoints declared as Lua through
`HubRuntime::load_lua_plugin_package`. The entrypoint executes behind
`botster_core::PluginRuntime`; plugin code does not run in `mcp-serve` or in a
second MCP dispatcher.

## Initial ABI

Lua entrypoints return `botster.register({ ... })`.

```lua
return botster.register({
  tools = {
    {
      name = "example.echo",
      description = "Echo input.",
      input_schema = {
        type = "object",
        additionalProperties = false,
      },
      handler = "echo",
      call = function(args)
        return { message = args.message }
      end,
    },
  },
})
```

Supported registration fields:

- `tools`: array of MCP tool descriptors. Each tool needs `name`,
  `description`, `handler`, and `call`. `input_schema` defaults to an empty
  object schema when omitted.
- `handlers`: optional array for non-tool handlers with `id`, `kind`, and
  `call`. Initial supported kinds are `command`, `mcp_tool`, `event`, `hook`,
  `timer`, `surface_route`, and `entity_provider`.

Handler ids are stable strings. Hub registries store descriptor bodies and
handler refs, not Lua closure identities.

## Package-owned entity providers

An enabled package may declare one worker-owned provider for each exact entity
family in its package namespace:

```lua
{
  id = "runs",
  kind = "entity_provider",
  descriptor_id = "project-pipelines.run",
  descriptor = {
    entity_type = "project-pipelines.run",
    id_field = "id",
  },
  call = function(request)
    return {
      type = "entity_snapshot",
      entity_type = request.entity_type,
      snapshot_seq = 1,
      items = {{ id = "run-1", status = "active" }},
    }
  end,
}
```

Hub's protocol-visible package entity namespace v1 maps the authoritative
manifest name to Core's required single-segment owner token:

- a non-empty name with no `.` and no `bns1_` prefix is unchanged, so
  `project-pipelines` owns `project-pipelines.run`;
- every other name maps to `bns1_` followed by lowercase hexadecimal for its
  exact UTF-8 bytes, with no Unicode normalization. For example,
  `botster.plugin-contract-matrix` owns
  `bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run`.

The reserved marker makes the identity and encoded ranges disjoint, and the
byte encoding is reversible and collision-free. Marked tokens are canonical
only when their suffix is even-length lowercase hex, decodes as UTF-8, and
re-encodes to the same token. Package authors currently declare the resulting
exact family in descriptors, snapshots, and UiNode paths; a Lua accessor is
deliberately deferred until authoring friction justifies expanding the ABI.

`descriptor_id` is the exact family and, when supplied, `entity_type` must
match it. Plugin families use the default non-empty string `id` field. Reserved
built-ins, foreign namespaces, malformed families, duplicate declarations,
custom id fields, and missing `call` handlers fail package loading.

`entity_provider` registration is deliberately capability-free: an enabled Lua
package may publish read-only snapshots only inside its own exact mapped
namespace, and Hub validates that ownership plus reserved-family exclusion at
load time. Providers do not grant access to another Hub capability; any state
they read still requires that state surface's normal manifest capability and
Hub grant.

Surface `bind_list`/`bind_if`/absolute `$bind` paths may use `/session` or an
exact family declared by the same loaded package. Hub does not admit namespace
prefixes, undeclared families, or another package's providers.

Every `SubscribeEntities` connection invokes the provider through its isolated
plugin worker with the standard one-second dispatch bound. The result must be
an authoritative whole-family `entity_snapshot`; Hub validates the exact
family and every record before bounded delivery. Reconnect invokes the provider
again rather than replaying cached rows. Unload removes the descriptor and
resource and closes held subscriptions for that family.

### Empty `items` encoding

Lua empty tables serialize as JSON objects by default. For entity frames only,
Hub coerces a top-level `items = {}` into a JSON array `[]` after default mlua
decode. Nested empty tables inside rows, `entity`, or `patch` stay objects
(`{}`). Do not rely on whole-frame empty-table-as-array conversion.

### Live mutation publish (`botster.entity_publish`)

Packages that mutate durable entity state must update durable truth first, then
publish an ordered mutation frame for held-open subscribers:

```lua
local published = botster.entity_publish({
  type = "entity_upsert", -- or entity_patch / entity_remove
  entity_type = "project-pipelines.membership",
  snapshot_seq = next_seq, -- package-owned, strictly increasing per family
  id = "row-1",
  entity = { id = "row-1", status = "claimed" },
})
-- published.ok, published.status, published.last_accepted_seq, published.high_water_seq
```

Admission is synchronous on the HubRuntime bridge pumped during `invoke_plugin`:

| `status` | Meaning |
| --- | --- |
| `accepted` | Frame is next in family order; queued for control fanout |
| `pending_gap` | Within pending window (W=16); retained until hole fills |
| `resync_scheduled` | Outside window; high-water retained; provider resync scheduled |
| `stale_sequence` | `seq < last_accepted_seq` (rejected) |
| `duplicate_sequence` | `seq == last_accepted_seq` (rejected) |

Fanout and provider resync run on the daemon control path after the handler
returns. Lua does not wait for subscriber delivery. Packages own increasing
`snapshot_seq` and durable provider truth; Hub does not invent product rows.

Provider resync is coalesced per family with exponential backoff (50ms…2s), at
most 2 provider calls per second, and at most 8 attempts per need cycle before
`resync_degraded` observability. Snapshots never roll an advanced subscriber
backward (`snapshot_seq < sub.last_applied_seq` is not delivered to that sub).

## Rust-Emitted Events

Plugins subscribe to hub-emitted lifecycle events with the injected `events`
global:

```lua
events.on("worktree_created", function(event)
  return {
    worktree_id = event.worktree_id,
    target_id = event.target_id,
  }
end)

return botster.register({})
```

`events.on(name, fn)` is narrow sugar over an Event-kind handler registration.
Handlers run through the normal plugin worker invocation path, so a failing
callback does not roll back the hub operation that emitted the event. Event
delivery is bounded and isolated, but it is synchronous with the emitting
worktree CRUD request; a slow handler can add latency until the worker timeout.
Event names match exactly.

Authorized plugins consume the Hub-owned `/session` family through
`events.on("session_family", ...)`. Hub admits those frames as Background
work: `snapshot_begin`, bounded `snapshot_chunk`, `snapshot_end` at one
snapshot sequence, then live deltas. At most one session-family frame is
in flight per plugin. Admission, completion, or handler failure marks a
gap and requires a complete baseline. These frames do not expire.

Worktree lifecycle events are emitted by hub-owned worktree CRUD:

- `worktree_created`
- `worktree_create_failed`
- `worktree_deleted`
- `worktree_delete_failed`

Payloads include stable ids and sanitized metadata: `event`, optional
`worktree_id`, optional `target_id`, optional `status`, optional `label`,
optional relative `display_path`, and failure `failure_kind`/`message` when
applicable. They do not include raw absolute worktree paths by default.
`worktree_deleted` is the canonical successful delete event; the hub deletes the
record and does not delete filesystem contents.

## Capability Access

Lua has no ambient `os`, `io`, or `package` globals. Filesystem, network,
process, and dynamic module loading are not available through the standard Lua
environment.

The initial capability helper is:

- `botster.capabilities.timer_once(delay_ms)`: submits a timer capability
  request through the hub-owned `HubCapabilityRuntime` and returns structured
  handle/event metadata.
- `botster.capabilities.plugin_db.get({ key = "..." })`: reads one JSON
  record from the loaded plugin's namespace. Missing records return the same
  result family with no `record` field, so Lua code can branch with
  `if result.record == nil then ... end` instead of using `pcall`.
- `botster.capabilities.plugin_db.set({ key = "...", schema_version = 1,
  payload = {...}, expected_revision = nil })`: writes one JSON record through
  the declared `PluginDb` capability, drains the completion event, and returns
  the typed plugin-store result.
- `botster.capabilities.plugin_db.patch({ key = "...", patch = {...},
  expected_revision = nil })`: applies a merge patch through the plugin-store
  capability and waits for completion. Missing records remain runtime errors.
- `botster.capabilities.plugin_db.delete({ key = "..." })`: deletes one
  plugin-store record and waits for completion. Missing records remain runtime
  errors.
- `botster.capabilities.plugin_db.list({ prefix = "..." })`: lists deterministic
  plugin-store record metadata.
- `botster.capabilities.plugin_db.batch({ mutations = {...} })`: atomically
  applies an ordered, non-empty set of `set`, `patch`, and `delete` mutations
  to the loaded plugin's namespace. Every mutation requires
  `expected_revision` (`0` when creating a record), and each key may appear at
  most once. `set` also accepts `schema_version` (default `1`) and `payload`;
  `patch` accepts an object merge `patch`; `delete` accepts only its key and
  expected revision. Read/list operations and unknown fields are rejected.
  Success returns `{ ok = true, results = {...} }` with one ordered result per
  mutation. Failure returns `{ ok = false, error_kind, message,
  mutation_index?, key? }`, where `mutation_index` is 1-based and stable kinds
  include `invalid_request`, `revision_conflict`, `store_not_found`,
  `quota_exceeded`, `patch_failed`, and `backend_failed`. A failed batch changes
  no record. Mutation-specific failures include `mutation_index` and include
  `key` when the failing mutation supplied a string key; a mutation rejected
  for omitting `key` reports only its index. Whole-request validation failures
  and namespace-wide `max_plugin_keys`/`max_plugin_bytes` quota failures omit
  both. Capability or namespace denial raises a Lua runtime error instead
  of returning this failure table; callers that must survive a missing or
  revoked grant should invoke `plugin_db.batch` with `pcall`.
- `botster.capabilities.config.get()`: returns the loaded plugin's own
  sanitized effective package configuration as `{ values = {...},
  missing_required = {...}, diagnostics = {...} }`. Values use the package
  daemon DTO shape, including manifest defaults and operator-set non-secret
  values. Secret values are absent when unset and redacted when set.
- `botster.capabilities.session_types.spawn({ session_type_id = "...",
  session_id = nil, target_id = nil, cwd = nil, environment = {...},
  context = {...} })`: requests a hub-owned session-type spawn for a
  declared package session type. The loaded plugin package must be enabled and
  declare `{ surface = "session_actions", scope = "session_type_spawn" }`.
  The hub reuses the same session-type materialization policy as the daemon path:
  admitted target id, cwd below the package root, declared environment
  overrides only, and hub-owned context injection. The hub stamps lifecycle time
  at fulfillment. On success the helper returns `{ session_id, lifecycle,
  session_type_id, context_id, context_keys }`. Those fields are produced by the
  materialized session type and Core spawn outcome. Policy and runtime failures
  raise Lua runtime errors rather than returning placeholder diagnostics fields.
- `botster.capabilities.spawn_targets.list()`: returns sanitized hub-owned spawn
  target rows visible to plugins. Plugins receive ids, labels, enabled state,
  kind, root, and sanitized metadata, but they do not own or mutate the
  registry.
- `botster.capabilities.spawn_targets.validate({ target_id = "..." })`: returns
  `{ target_id = "...", ok = boolean, status = "ok"|"disabled"|"not_found" }`.
  Disabled targets exist but are unavailable for plugin references, so
  validation returns `ok = false` with `status = "disabled"`.
- `botster.capabilities.worktrees.list()`: returns deterministic sanitized
  hub-owned worktree rows with reconciled `status` values. Worktree rows include
  `worktree_id`, `target_id`, label, path, status, optional git metadata, and
  sanitized metadata.
- `botster.capabilities.worktrees.show({ worktree_id = "..." })`: returns
  `{ ok = true, status = "...", worktree = {...} }` for an existing worktree.
  Missing ids return `{ ok = false, status = "not_found", worktree_id = "...",
  message = "..." }` so plugins can produce diagnostics without wrapping normal
  absence in `pcall`.

`plugin_db` helpers always use the loaded plugin key as the namespace; Lua code
cannot select another plugin's namespace. The synchronous Lua helpers prepare
the admitted operation under `HubCapabilityRuntime`, release its shared lock,
and execute the filesystem operation inside that plugin's isolated worker before
returning. The general asynchronous `CapabilityOperation::PluginStore`
submit/event surface remains single-record.

`plugin_db.batch` validates revisions, patches, record size, final record count,
and final aggregate namespace size before staging any durable change. It holds
the concrete backend mutex continuously from restart recovery and snapshot load
through staging, whole-namespace promotion, parent-directory synchronization,
and cleanup, so existing single-record helpers cannot interleave or observe a
partial generation. Staging and backup are private non-JSON sibling directories
outside the live namespace. If a process stops between filesystem promotion
steps, the next store access—including `get` or `list`—repairs the transaction
shape under the same mutex and exposes either the complete old generation or
the complete new generation. A successful return means the promoted namespace
and its parent-directory durability barrier completed; a caller timeout remains
ambiguous and should be reconciled with an authoritative read.

`config.get` follows the same loaded-plugin namespace rule. It accepts no
package name and cannot read another package's configuration. Package
configuration writes remain hub CLI/API responsibilities, not Lua plugin
self-mutation.

`session_types.spawn` accepts session-type request fields only. It does not
accept command, args, shell, arbitrary process environment, or raw filesystem
execution data, or caller-supplied lifecycle time; direct process spawning
remains unsupported from Lua. Hub plugin invocations use a 30s timeout for this
helper path, and the hub cleans up spawned sessions if the worker result cannot
be delivered.

Session-type definition CRUD intentionally remains on the admitted local
daemon/CLI operator path. Lua workers can list, show, spawn, and invoke the
separately granted managed-worktree operation, but cannot edit package, device,
or repo authority directly. Package definitions are read-only on every surface.

`spawn_targets` is intentionally read-only in Lua. Create, update, and delete
are daemon/CLI operator responsibilities; exposing mutation helpers to plugin
workers would let plugins admit local host paths without the hub operator path.

`worktrees` is also intentionally read-only in Lua. Hub-owned daemon/CLI APIs
create, update, delete, persist, and emit lifecycle events for worktrees.
Plugins may reference worktree ids and resolve paths for session-type
context, but workflow ownership remains in plugin state while filesystem
authority stays with the hub.

## Coordination Access

Lua coordination helpers expose the CoreDaemon-backed generic routed-envelope
primitive without embedding Project Pipelines policy in Rust:

- `botster.coordination.publish({ id = "...", target = {...}, content_type =
  "...", body = "...", extension = {...}, created_at = 0 })`: publishes one
  envelope from the loaded plugin endpoint and returns
  `RoutedEnvelopePublishOutcome`.
- `botster.coordination.drain({ target = {...}, after = nil, limit = 16 })`:
  drains a routed target queue and returns `RoutedEnvelopeDrainOutcome`,
  including primitive-assigned cursors.
- `botster.coordination.acknowledge({ target = {...}, envelope_id = "..." })`:
  acknowledges one delivered target copy and returns its delivery state.

These helpers submit to the hub owner thread and use CoreDaemon's
routed-envelope APIs. They raise a Lua runtime error if the daemon rejects the
request, including the stopped-daemon case; callers that need to recover should
wrap coordination calls with `pcall`.

Coordination helpers are available during registered handler invocation only.
They are not available while the plugin entrypoint is loading, because the hub
owner thread is executing the plugin load and cannot also service coordination
requests.

Future capability helpers must continue to submit through hub/core capability
contracts. They must not expose raw host filesystem, network, process, or C
module access.

## MCP Routing

Lua-provided tools are listed and called through the existing
`McpToolRegistry`. The plugin MCP provider delegates to daemon requests against
the running `HubRuntime`; `mcp-serve` does not load plugin files or hold Lua VM
state.

The production invocation path is:

`mcp-serve -> McpToolRegistry -> PluginHubToolProvider -> daemon request -> HubRuntime -> HubPluginLifecycle -> botster_core::PluginWorkerEngine -> LuaPluginRuntime`.

## Unsupported Monolith Primitives

The old monolith-style Lua environment is not part of this ABI. Unsupported
primitives include:

- raw `hub.*` mutation surfaces
- ActionCable and browser shell APIs
- WebRTC and terminal transport adapters
- cloud/provider APIs
- direct process spawning
- raw `os`, `io`, `package`, `package.loadlib`, sockets, or C modules
- file-watcher reload
- full surface/entity framework registration
- closure-based hub registries

## Resource Limits

Lua execution currently has an instruction-count guard. If a handler exceeds
the budget, invocation returns a structured plugin handler failure. Memory
accounting, per-plugin restart/quarantine policy, and full resource ledgers are
not implemented in this slice.
