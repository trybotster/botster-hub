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

`plugin_db` helpers always use the loaded plugin key as the namespace; Lua code
cannot select another plugin's namespace. Mutating helpers submit to
`HubCapabilityRuntime` and drain the matching completion before returning, so a
handler can read its just-committed state deterministically.

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

## Coordination Access

Lua coordination helpers expose the generic routed-envelope primitive without
embedding Project Pipelines policy in Rust:

- `botster.coordination.publish({ id = "...", target = {...}, content_type =
  "...", body = "...", extension = {...}, created_at = 0 })`: publishes one
  envelope from the loaded plugin endpoint and returns
  `RoutedEnvelopePublishOutcome`.
- `botster.coordination.drain({ target = {...}, after = nil, limit = 16 })`:
  drains a routed target queue and returns `RoutedEnvelopeDrainOutcome`,
  including primitive-assigned cursors.
- `botster.coordination.acknowledge({ target = {...}, envelope_id = "..." })`:
  acknowledges one delivered target copy and returns its delivery state.

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
