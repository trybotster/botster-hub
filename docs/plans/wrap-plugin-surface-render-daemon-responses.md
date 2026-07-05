# Wrap plugin_surface_render daemon responses with package and surface metadata

## Context Loaded

- Ticket `ticket_1783279289_147399`, run `run_1783279302_583945`, step `botster_plan`, gate `botster_plan_gate`.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Surface/protocol notes: [[botster plugin surfaces own navigation and plugin scoped sessions]], [[plugin owned surface route renders run in plugin worker vms]], [[plugin surface handlers must validate against hub locked uinode contract]], [[plugin surfaces request model state through ui bindings not hub subscribe]], [[botster web generated protocol drift checks need explicit hub artifact paths]], [[daemon event shape changes bump conformance fixture revision not protocol version]].
- Project Pipelines workflow notes: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo evidence:
  - `src/daemon_transport.rs` handles `DaemonRequest::PluginSurfaceRender`, calls `HubClientRequest::PluginSurfaceRender`, then serializes `HubClientResponseBody::PluginSurface(surface)` with `daemon_plugin_surface(surface)`.
  - `src/client_api.rs` currently defines `HubClientResponseBody::PluginSurface(UiNode)`.
  - `src/runtime.rs` renders through `HubRuntime::render_plugin_surface(...)`, validates the plugin-returned `UiNode`, and should remain the worker-rendered payload source.
  - `crates/botster-hub-client/src/lib.rs` currently exposes `DaemonResponse.plugin_surface: Option<Value>`.
  - `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts` currently type `plugin_surface?: JsonValue`.
  - `tests/hub_daemon_lifecycle_test.rs` already has focused real-daemon plugin surface descriptor and render-guard coverage.

Checklist note: `project_pipelines_create_vault_checklist` timed out twice at the plugin worker boundary. Per [[project pipeline orchestration belongs in a device-level botster plugin]], this plan preserves checklist-equivalent evidence in this artifact and the gate payload.

## Scope

- Define an authoritative daemon response envelope for `plugin_surface_render` in the hub-client protocol, with at least:
  - `package_name`
  - `surface_id`
  - rendered UI payload in a stable field, preferably `body`
- Change the hub/client response boundary so `HubClientResponseBody::PluginSurface` carries the route identity and rendered `UiNode`, or otherwise preserves request identity at the daemon serialization boundary without making web infer it.
- Update `daemon_plugin_surface` to serialize the wrapped payload into `DaemonResponse.plugin_surface`.
- Update generated TypeScript protocol output so browser clients see the wrapped `plugin_surface` shape instead of untyped raw `JsonValue`.
- Add focused tests proving a real daemon `PluginSurfaceRender` response for a plugin surface includes `kind=plugin_surface`, matching `package_name`, matching `surface_id`, and the rendered UI payload.
- If the generator output changes, regenerate/check `crates/botster-hub-client/generated/daemon-protocol.ts`.

## Non-Scope

- No botster-web workaround or package/surface inference in web.
- No old monolith, Rails, Hotwire, or PII work.
- No changes to plugin worker execution: plugin-owned render functions must continue to run in worker VMs.
- No broad protocol-version redesign, compatibility shim, or dual response shape unless tests reveal an actual deployment boundary requiring it.
- No adjacent cleanup of package descriptors, route descriptors, plugin actions, MCP tools, or UI bindings.

## Assumptions And Unknowns

- Assumption: the stable rendered UI payload field should be named `body`, matching the ticket wording and keeping `plugin_surface` as the outer daemon response slot.
- Assumption: `title` and `snapshot` are optional for this ticket. Include them only if an existing local descriptor/snapshot source is already wired and cheap; do not invent scaffold fields.
- Assumption: this is a response body shape change, not a daemon framing change. It may require generated artifacts and test fixture expectations, but not a `PROTOCOL_VERSION` bump unless local compatibility constants already treat response semantic shape changes as protocol-level.
- Unknown: whether the existing TypeScript generator has a named `UiNode` type available. If not, prefer a named `DaemonPluginSurface` interface with `body: JsonValue` rather than hand-writing a partial UiNode type in the generator.
- Unknown: whether current tests have a checked-in Workspaces package fixture named `botster-workspaces/workspaces`. If the repo lacks that exact package, use the existing real plugin surface fixture and assert the same contract; add Workspaces-specific coverage only if the fixture exists locally.

## Affected Surfaces And Files

- Botster layer: Rust hub daemon and external hub-client protocol boundary.
- Runtime path:
  - daemon socket reads `DaemonRequest::PluginSurfaceRender` in `src/daemon_transport.rs`
  - request enters `HubClientApi::handle_request` in `src/client_api.rs`
  - render executes through `HubRuntime::render_plugin_surface` in `src/runtime.rs`
  - daemon serializes the response through `daemon_plugin_surface`
  - browser consumes `DaemonResponse.plugin_surface` from generated `daemon-protocol.ts`
- Likely files:
  - `src/client_api.rs`
  - `src/daemon_transport.rs`
  - `crates/botster-hub-client/src/lib.rs`
  - `crates/botster-hub-client/src/typescript.rs`
  - `crates/botster-hub-client/generated/daemon-protocol.ts`
  - `tests/hub_daemon_lifecycle_test.rs`
  - possibly `tests/hub_lua_runtime_test.rs` if struct shape changes require direct HubClientApi assertions
  - possibly `docs/client-protocol.md` only if this repo documents plugin surface response bodies there

## Risks

- Existing tests or downstream code may construct `HubClientResponseBody::PluginSurface(UiNode)` directly; required-field changes need workspace tests.
- If `package_name` and `surface_id` are only wrapped in `daemon_transport.rs`, future non-daemon users of `HubClientResponseBody::PluginSurface` may still lack identity. Prefer placing identity in the hub-client response body when practical.
- Leaving `plugin_surface` typed as `JsonValue` in generated TypeScript would let botster-web drift persist even if Rust serialization is fixed.
- Accidentally wrapping the payload before `UiNode::validate()` would weaken the existing hub-locked UiNode validation boundary. Validation should stay on the rendered `UiNode`; wrapping happens after.
- A full-suite hang or unrelated strict-lint failure should not be waived broadly; attribute failures to touched files or known baseline evidence.

## Acceptance Checks And Tests

- Focused Rust/client protocol checks:
  - `./test.sh -p botster-hub-client`
  - This should cover serde examples and generated TypeScript drift for the client crate.
- Focused daemon integration:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_package_dtos_expose_declared_surfaces_and_validate_surface_ids`
  - Or the new/renamed focused test if the implementer splits the positive render assertion.
- If `tests/hub_lua_runtime_test.rs` is touched:
  - `./test.sh --test hub_lua_runtime_test <focused test name>`
- Runtime/user-path proof required in review:
  - A real daemon request for `PluginSurfaceRender` returns `kind=plugin_surface`.
  - `response.plugin_surface.package_name` equals the requested package.
  - `response.plugin_surface.surface_id` equals the requested surface.
  - `response.plugin_surface.body` contains the rendered validated `UiNode`.
  - Generated `daemon-protocol.ts` exposes the same shape.
- Optional downstream check if botster-web checkout is available to the implementer:
  - Run its generated-protocol drift check with explicit `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL=<this repo>/crates/botster-hub-client/generated/daemon-protocol.ts`; do not count a skipped check as evidence.

## Vault Gaps Worth Capturing

- Capture after implementation if a durable convention emerges for wrapped daemon response bodies: whether identity metadata belongs in the hub-client response body versus only daemon transport helpers.
- Capture if `plugin_surface` response fields settle on `body` plus optional descriptor/snapshot metadata, because botster-web and future plugin clients will rely on that contract.
- No convention conflict found. This plan follows the hub-client external boundary, worker-owned render execution, generated protocol drift guard, and no-web-inference rule.
