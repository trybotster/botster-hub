# Project Pipelines Local Plugin

This package is the first real local Project Pipelines plugin fixture for the
new hub/plugin stack. It exposes constrained MCP tools through the shared
`botster-hub mcp-serve` registry and persists state under
`plugin-data/project-pipelines/`.

Supported in this milestone:

- create, list, update, start, current-context, gate submission, and step
  advance for a constrained local workflow
- daemon-backed package enablement and restart reload
- worker-backed MCP handler invocation with explicit target id, assigned
  worktree, request id, and plugin ownership metadata on started runs
- routed-envelope-backed start coordination with publish, drain, and
  acknowledge delivery evidence
- intentionally absent `session_uuid` on `start` because this constrained local
  flow records coordination before spawning any agent session

Unsupported monolith features:

- Rails/cloud/WebRTC/browser marketplace surfaces
- GitHub provider supervision and PR automation
- broad compatibility with monolith SQLite state
- full agent spawn/worktree orchestration from Project Pipelines Lua policy

Cutover posture: live monolith Project Pipelines data is not imported in this
milestone. Cutover requires no in-flight monolith tickets, or a future explicit
one-shot export/import tool before switching active work to this local plugin.

Runtime note: this package now registers descriptors and handlers through the
Lua ABI. Project Pipelines workflow policy lives in `plugin.lua`; Rust exposes
only reusable PluginDb and routed-envelope helpers needed by the plugin. The
production daemon/MCP/worker/storage path is proved without a second
`mcp-serve` runtime or a host-supplied Project Pipelines bundle.
