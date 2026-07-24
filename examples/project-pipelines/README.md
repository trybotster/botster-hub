# Project Pipelines Local Plugin

This package is the first real local Project Pipelines plugin fixture for the
new hub/plugin stack. It exposes constrained MCP tools through the shared
`botster-hub mcp-serve` registry and persists state under
`plugin-data/project-pipelines/`.

Supported in this milestone:

- create, list, update, start, current-context, gate submission, and step
  advance for a constrained local workflow
- daemon-backed package enablement and restart reload
- worker-backed MCP handler invocation with explicit target id, hub worktree id,
  resolved worktree path, request id, and plugin ownership metadata on started
  runs
- a plugin-owned UiNode create-ticket surface at
  `project-pipelines.create-ticket` plus UI action
  `project_pipelines.create_ticket`
- plugin-owned form validation for title and pipeline id, returning
  `UiActionResult` failure payloads with `field_errors` keyed by stable UiNode
  id and `form_errors` for form-level feedback
- routed-envelope-backed start coordination with publish, drain, and
  acknowledge delivery evidence
- session-template spawn on `start`, returning `session_uuid`,
  `session_template_id`, `session_context_id`, and `session_lifecycle` from the
  spawned step session through the plugin worker `session_templates.spawn`
  capability

`project_pipelines.start` accepts `target_id` and `worktree_id`. The hub owns
spawn target and worktree CRUD; this fixture resolves the supplied worktree id
through `botster.capabilities.worktrees.show()` and passes the resolved
`worktree_path` only to the session-template context needed to start the local
step. Project Pipelines keeps workflow/run state and references the hub-owned
ids; it does not create worktrees or treat caller-supplied raw paths as the
workflow contract.

Unsupported monolith features:

- Rails/cloud/WebRTC/browser marketplace surfaces
- GitHub provider supervision and PR automation
- broad compatibility with monolith SQLite state
- multi-step agent supervision and worktree lifecycle management beyond the
  single package-owned step session spawned by this local fixture

Follow-up plugin-repo work: move first-party Project Pipelines and Workspaces
repos to the same hub-owned spawn target/worktree contract proved here:
Project Pipelines starts from `worktree_id`, and Workspaces validates target
refs through `botster.capabilities.spawn_targets`.

Cutover posture: live monolith Project Pipelines data is not imported in this
milestone. Cutover requires no in-flight monolith tickets, or a future explicit
one-shot export/import tool before switching active work to this local plugin.

Runtime note: this package now registers descriptors and handlers through the
Lua ABI. Project Pipelines workflow policy lives in `plugin.lua`; Rust exposes
only reusable PluginDb and CoreDaemon-backed routed-envelope helpers needed by
the plugin. The production daemon/MCP/worker/storage path is proved without a
second `mcp-serve` runtime or a host-supplied Project Pipelines bundle.

Manual runtime path: install and enable this package through the ordinary Hub
package commands, run bare `botster-hub up`, then open the reported Web URL or
the daemon-resolved TUI app. Focus the Project Pipelines create-ticket fields,
submit once with a blank
title to see field/form validation, then submit with a nonblank title to create
a local ticket. The TUI renders the plugin-authored UiNode tree and dispatches
the semantic action through the daemon; it does not build a Project Pipelines
form in Rust. The botster-web process is launched from the separate
`botster-web` package through hub supervision, not from this Project Pipelines
manifest.
