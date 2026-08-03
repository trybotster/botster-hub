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
  `timer`, and `surface_route`.

Handler ids are stable strings. Hub registries store descriptor bodies and
handler refs, not Lua closure identities.

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
  no record. Capability or namespace denial raises a Lua runtime error instead
  of returning this failure table; callers that must survive a missing or
  revoked grant should invoke `plugin_db.batch` with `pcall`.
- `botster.capabilities.config.get()`: returns the loaded plugin's own
  sanitized effective package configuration as `{ values = {...},
  missing_required = {...}, diagnostics = {...} }`. Values use the package
  daemon DTO shape, including manifest defaults and operator-set non-secret
  values. Secret values are absent when unset and redacted when set.
- `botster.capabilities.session_templates.spawn({ template_id = "...",
  session_id = nil, target_id = nil, cwd = nil, environment = {...},
  context = {...} })`: requests a hub-owned session-template spawn for a
  declared package template. The loaded plugin package must be enabled and
  declare `{ surface = "session_actions", scope = "session_template_spawn" }`.
  The hub reuses the same template materialization policy as the daemon path:
  admitted target id, cwd below the package root, declared environment
  overrides only, and hub-owned context injection. The hub stamps lifecycle time
  at fulfillment. On success the helper returns `{ session_id, lifecycle,
  template_id, context_id, context_keys }`. Those fields are produced by the
  materialized template and core spawn outcome. Policy and runtime failures
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

`session_templates.spawn` accepts template request fields only. It does not
accept command, args, shell, arbitrary process environment, or raw filesystem
execution data, or caller-supplied lifecycle time; direct process spawning
remains unsupported from Lua. Hub plugin invocations use a 30s timeout for this
helper path, and the hub cleans up spawned sessions if the worker result cannot
be delivered.

`spawn_targets` is intentionally read-only in Lua. Create, update, and delete
are daemon/CLI operator responsibilities; exposing mutation helpers to plugin
workers would let plugins admit local host paths without the hub operator path.

`worktrees` is also intentionally read-only in Lua. Hub-owned daemon/CLI APIs
create, update, delete, persist, and emit lifecycle events for worktrees.
Plugins may reference worktree ids and resolve paths for session-template
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
