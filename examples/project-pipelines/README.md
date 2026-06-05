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

Unsupported monolith features:

- Rails/cloud/WebRTC/browser marketplace surfaces
- GitHub provider supervision and PR automation
- broad compatibility with monolith SQLite state
- full agent spawn/worktree orchestration from Lua

Cutover posture: live monolith Project Pipelines data is not imported in this
milestone. Cutover requires no in-flight monolith tickets, or a future explicit
one-shot export/import tool before switching active work to this local plugin.

Runtime note: this reduced hub crate does not yet execute Lua entrypoints. The
package manifest and entrypoint are real local package inputs, and the hub
currently supplies the Project Pipelines runtime bundle from the package load
path so the production daemon/MCP/worker/storage path can be proved without a
second `mcp-serve` runtime.
