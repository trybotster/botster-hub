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
