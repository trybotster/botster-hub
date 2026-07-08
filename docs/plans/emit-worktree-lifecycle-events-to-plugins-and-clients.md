# Emit worktree lifecycle events to plugins and clients

## Context Loaded

- Pipeline context: ticket `ticket_1783463498_456085`, run `run_1783470586_260302`, Plan step `botster_plan`, gate `botster_plan_gate`; dependency `ticket_1783463498_230570` is closed. Re-plan context loaded after Plan Review returned changes: findings `finding_1783470957_973265`, `finding_1783470957_994308`, and `finding_1783470957_352289`.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster/vault context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Skill context: `botster:botster-customize-hub`, because this changes hub lifecycle/event behavior. Its local authority names `events.on(...)` as the Rust-to-Lua event path and already lists `worktree_created` and `worktree_create_failed`.
- Repo context inspected: `src/worktrees.rs`, `src/runtime.rs`, `src/lua_runtime.rs`, `src/lifecycle.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`, `README.md`, and dependency plan `docs/plans/add-hub-worktree-crud-model-over-spawn-targets-without-requiring-git.md`.
- Current implementation baseline: hub-owned worktrees already persist in `HubState.worktrees`, mutate through `DaemonRequest::{CreateWorktree,DeleteWorktree}` in `src/daemon_transport.rs`, reconcile `present`/`missing`/`stale` in `src/worktrees.rs`, expose `DaemonWorktree` DTOs through `botster-hub-client`, and support non-git directories.
- Event implementation baseline from Plan Review: `events.on(...)` is not currently implemented in this repo. `src/lua_runtime.rs` installs the `botster` global and parses `PluginHandlerKind::Event`, but no `events` global exists, handler registration has no event-name subscription field, and no runtime dispatch path currently invokes Event handlers.
- Project Pipelines checklist evidence: `project_pipelines_current_context` returned empty `run_checklists` and `ticket_checklists`, so this plan records the checklist-required discipline directly in the repo artifact and gate evidence: vault notes loaded, no convention conflict found, verification targets named, and durable-knowledge gaps listed.

## Scope

- Add structured worktree lifecycle event payloads for:
  - `worktree_created`;
  - `worktree_create_failed`;
  - `worktree_deleted` or `worktree_destroyed` with one canonical emitted name and docs explaining any alias decision;
  - `worktree_delete_failed`;
  - `worktree_missing`/`worktree_stale` discovery during reconciliation if the implementation can observe a status transition without turning ordinary list/show into noisy repeated events.
- Emit from the hub-owned worktree CRUD/reconciliation path, not from plugin state. The source of truth remains `src/worktrees.rs` plus daemon persistence in `src/daemon_transport.rs`.
- Route plugin delivery through existing plugin worker isolation semantics by invoking `PluginHandlerKind::Event` handlers via `HubRuntime::invoke_plugin`, so a plugin callback cannot block the daemon mutation hot path.
- Add the missing Lua event subscription surface required by the ticket: install an `events` global in `src/lua_runtime.rs` with `events.on(name, fn)` as narrow sugar over Event-kind handler registration. This is a new Lua ABI surface, not an existing primitive.
- Add an event-name subscription data model to plugin registrations. The narrow shape should be an `event` or `event_name` string on event handler registrations, populated by `events.on(name, fn)` and optionally accepted in explicit `botster.register({ handlers = { ... } })` entries for testability. Non-event handlers must not grow worktree-specific fields.
- Add a runtime dispatch method, for example `HubRuntime::emit_plugin_event(name, payload)`, that asks `HubPluginLifecycle` for loaded Event handlers matching the event name and invokes each through `invoke_plugin` with a bounded timeout/request id. Matching is exact event-name equality.
- Add public client visibility through the normal daemon event path by extending `DaemonEvent` with worktree lifecycle variants or one structured `WorktreeLifecycle` variant, then returning/draining those events through the existing `DaemonResponse.events` path. This repo has no separate worktree entity family; do not create one for this ticket.
- Sanitize payloads by default. Include stable ids and safe metadata: `worktree_id`, `target_id`, `status`, optional `label` or relative/display path, and failure `kind`/`message`. Do not include raw absolute home paths or full canonical filesystem paths in lifecycle event DTOs.
- Preserve non-git semantics. Event names and payload fields should use `worktree`/`target` vocabulary, not repository/branch/git-only vocabulary; git metadata remains absent unless already available in the ordinary DTO path.
- Update docs and examples so plugin authors can react to create/delete without storing workflow policy in hub records.

## Non-Scope

- Do not change the existing worktree CRUD model, path admission rules, or delete semantics except where event emission needs a sanitized projection.
- Do not delete filesystem contents when a worktree record is deleted.
- Do not add Project Pipelines ticket/run/PR/gate fields to hub worktree records or events. Workflow associations stay plugin-owned and reference `worktree_id`.
- Do not add a second event bus, plugin-only worktree state, side-channel command path, or broad new plugin capability table. The only new Lua surface in scope is the ticket-required `events.on(name, fn)` helper over Event handler registration.
- Do not make worktree events git-specific or require `.git`.
- Do not perform broad SPA/operator-workbench work. Client acceptance should prove the daemon/client contract changes; rich UI placement belongs to later product work if needed.

## Assumptions And Unknowns

- Assumption: `worktree_deleted` should be the canonical delete-success event because current CRUD deletes the hub record only; `destroyed` should be avoided unless the implementation introduces an alias for compatibility with ticket wording. Docs should make this explicit.
- Assumption: controlled create/delete failures are the validation/operator-error cases already represented by `WorktreeError`, such as duplicate id, unknown target, disabled target, path rejection, and missing worktree on delete.
- Assumption: persistence I/O failures may not always be safely emitted to plugins because the runtime/store state can be unavailable; acceptance should focus on controlled validation failures unless implementation finds a safe post-error emission point.
- Assumption: lifecycle events should be best-effort and non-blocking from the caller perspective. Plugin handler failures should be observable in diagnostics/logs/tests but must not roll back successful CRUD mutations.
- Assumption: reconciliation discovery should emit only on status transition or startup reconciliation, not every `ListWorktrees`/`ShowWorktree` call returning an already-missing row. If no durable previous-status comparison exists, document reconciliation event emission as intentionally limited rather than spamming.
- Assumption: `events.on(name, fn)` must be added as a narrow new Lua ABI helper because repo inspection and Plan Review found no existing helper, no event-name subscription field, and no Event handler dispatch path.
- Assumption: the client-visible path for this repo is `DaemonEvent` via `DaemonResponse.events`; there is no separate worktree entity family to update in this ticket.
- Worktree/target assumption: downstream agents must work in this pipeline-assigned worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`, not an ambient checkout.

## Affected Surfaces And Files

- `src/worktrees.rs`: likely add a small sanitized event DTO/projection helper or status-transition helper if reconciliation events need row comparison.
- `src/daemon_transport.rs`: primary CRUD mutation hook. Emit create/delete success events after persistence and runtime state replacement; emit controlled failure events when `WorktreeError` is converted to operator responses; include events in daemon responses or queue them for client drain according to existing daemon event conventions.
- `src/runtime.rs`: add a narrow method such as `emit_plugin_event(name, payload)` for dispatching hub lifecycle events to loaded plugin Event handlers through `HubPluginLifecycle`/`invoke_plugin`, keeping handler execution off the daemon hot path.
- `src/lifecycle.rs`: store and expose loaded Event handler registrations with their subscribed event names; provide a matcher for exact event-name fan-out.
- `src/lua_runtime.rs`: install an `events` global with `events.on(name, fn)`, assign stable handler ids, store the callback in `__botster_handlers`, and include the event-name subscription in `LuaHandlerRegistration`/`PluginHandlerRegistration` metadata. Also accept explicit `botster.register({ handlers = { { kind = "event", event = "...", call = fn } } })` if that is the smallest way to keep registration testable.
- `crates/botster-hub-client/src/lib.rs`: add daemon event DTO variants and serde/generation tests; update event examples and generated union assertions. Because adding a client-visible `DaemonEvent` variant changes the public daemon event shape, bump `CONFORMANCE_FIXTURE_REVISION`.
- `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, and `packages/hub-test-support/daemon-protocol.ts`: regenerate/update TypeScript protocol artifacts for new event variants.
- `tests/hub_lua_runtime_test.rs` or `tests/hub_plugin_lifecycle_test.rs`: focused Lua/plugin worker tests proving a plugin receives worktree created and deleted events and records/responds to them.
- `tests/hub_daemon_lifecycle_test.rs`: daemon/client tests for create/delete success events, controlled failure event emission, client-visible event DTOs, and no default raw home path in event payloads.
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, `README.md`, and plugin examples such as `examples/synthetic-plugin/plugin.lua` or a small fixture plugin: document event names, payload shape, sanitization rules, and `events.on(...)` usage.

## Risks

- Event duplication/noise: emitting missing/stale events from every reconciled read can spam clients and plugins. Mitigate by emitting only on mutation failure/success and on durable status transition when available.
- Hot-path blocking: invoking Lua directly from the daemon request thread can block CRUD. Mitigate by going through existing plugin worker invocation and treating events as fire-and-forget/best-effort.
- New Lua event ABI risk: `PluginHandlerKind::Event` is parsed, but `events.on(...)`, event-name subscriptions, and dispatch do not exist yet. Mitigate with a real Lua runtime test using the public helper syntax, not just Rust handler registration.
- PII leakage: existing `DaemonWorktree.path` is trusted-client local path, but lifecycle events must be sanitized. Mitigate with explicit payload helpers and tests scanning serialized events for raw temp/home/worktree paths.
- Public protocol drift: daemon event variants require matching Rust serde tests, TypeScript generation, hub-test-support artifacts, and conformance revision handling.
- Ambiguous delete vocabulary: ticket says deleted/destroyed. Mitigate by choosing one canonical event name and documenting any alias/non-alias decision.
- Plugin failure semantics: a plugin event handler may fail after CRUD succeeds. Mitigate by not rolling back CRUD and by surfacing handler failures in bounded diagnostics/log evidence.

## Acceptance Checks And Tests

- Lua/plugin runtime:
  - Real Lua plugin fixture registers `events.on("worktree_created", ...)` and `events.on("worktree_deleted", ...)`, proving the new helper exists, records exact event-name subscriptions, receives events through the plugin worker path, and records the two lifecycle payloads in plugin-owned state or a test-visible result.
  - Explicit handler-registration test, if implemented, proves `botster.register({ handlers = { { kind = "event", event = "worktree_created", ... } } })` maps to the same subscription model as `events.on`.
  - Handler failure test proves a failing event callback does not fail or roll back the worktree CRUD response.
- Failure events:
  - Controlled create failure, such as duplicate `worktree_id` or path outside target, emits `worktree_create_failed` with `worktree_id` when available, `target_id`, `failure_kind`, and sanitized `message`.
  - Controlled delete failure, such as deleting a missing id, emits `worktree_delete_failed` with the missing `worktree_id`, `failure_kind`, and sanitized `message`.
- Client/daemon path:
  - Started-daemon test performs `CreateWorktree` and `DeleteWorktree` through `botster_hub_client::DaemonConnection` or `daemon_transport_request` and observes the new public `DaemonEvent` variant through `DaemonResponse.events`, not only direct helper return values.
  - If event delivery is response-attached, assert the response carries both `DaemonResponseKind::Worktrees` data and lifecycle events. If event delivery is drain/subscription-based, assert the documented drain path receives them.
  - Document that this repo's normal client path for this ticket is the daemon event plus worktree response DTO; no separate worktree entity family is created.
- Payload/privacy:
  - Serialize success and failure event DTOs and assert they do not contain `/Users/`, the test temp root, absolute worktree paths, tokens, or other raw local path strings by default.
  - Assert relative/display path, if present, is relative to the spawn target or otherwise sanitized.
- Protocol/docs:
  - Hub-client event serde examples and generated TypeScript union checks include the new event variant(s).
  - `CONFORMANCE_FIXTURE_REVISION` is bumped because the client-visible `DaemonEvent` union changes.
  - `docs/client-protocol.md` and `docs/lua-plugin-abi.md` list event names and payload fields.
  - `README.md` or a checked-in plugin example shows `events.on("worktree_created", function(event) ... end)` and keeps workflow associations in plugin state.
- Final verification commands:
  - `./test.sh --test hub_lua_runtime_test worktree`
  - `./test.sh --test hub_daemon_lifecycle_test worktree`
  - `cargo test -p botster-hub-client`
  - `cargo fmt`
  - `cargo clippy --all-targets --all-features -- -D warnings`

## Vault Gaps Worth Capturing

- Capture the durable rule after implementation: worktree lifecycle events are hub-emitted from spawn-target-scoped worktree CRUD and are consumed by plugins through worker-isolated `events.on(...)` handlers.
- Capture the sanitized event payload contract once final field names are chosen, especially the rule that lifecycle event DTOs do not include raw absolute worktree paths by default even though trusted worktree CRUD DTOs may expose local paths.
- Capture whether `worktree_deleted` versus `worktree_destroyed` becomes the stable vocabulary, because future agents will otherwise rediscover the same naming ambiguity.
- Capture the reconciliation event policy if implemented, especially whether missing/stale events are transition-only, startup-only, or intentionally omitted until a durable transition tracker exists.
