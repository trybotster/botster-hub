# Emit validated plugin surface UiTree snapshots from hub

## Context Loaded

- Ticket `ticket_1783299808_722651`, run `run_1783299838_130447`, step `botster_plan`, gate `botster_plan_gate`.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Surface/protocol notes: [[botster hub client crate is the external client boundary]], [[botster web dto field names must match authoritative rust serde structs]], [[generated typescript dtos must encode serde field optionality]], [[plugin surface handlers must validate against hub locked uinode contract]], [[botster wire v2 clients must consume ui tree snapshots and render composites with entity stores]], [[plugin surfaces request model state through ui bindings not hub subscribe]], [[daemon event shape changes bump conformance fixture revision not protocol version]].
- Project Pipelines workflow notes: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo evidence:
  - `src/runtime.rs` renders plugin surfaces through `HubRuntime::render_plugin_surface(...)`, deserializes plugin output into `UiNode`, and calls `node.validate()`. This is the validation boundary to preserve.
  - `src/client_api.rs` maps `PluginSurfaceRender` to `HubClientResponseBody::PluginSurface(HubClientPluginSurface { package_name, surface_id, body })`.
  - `src/daemon_transport.rs` handles `DaemonRequest::PluginSurfaceRender`, then serializes the response in `daemon_plugin_surface(...)`.
  - `crates/botster-hub-client/src/lib.rs` currently exposes `DaemonPluginSurface { package_name, surface_id, body: Value }`; there is no current `ui_tree_snapshot` field in this checkout.
  - `crates/botster-hub-client/src/typescript.rs` generates `DaemonPluginSurface` into `crates/botster-hub-client/generated/daemon-protocol.ts`.
  - `fixtures/plugins/plugin-contract-matrix` already has valid app/empty/settings surfaces and a handler-error blocked surface, but no invalid-UiNode/body surface.
  - `crates/botster-hub-test-support/src/lib.rs` runs the plugin contract matrix through real daemon requests and is the right place to assert cross-client conformance output.

Checklist note: `project_pipelines_create_vault_checklist` timed out at the plugin worker boundary. Per [[project pipeline orchestration belongs in a device-level botster plugin]] and its checklist-timeout fallback guidance, this plan preserves checklist-equivalent evidence in this artifact and the gate payload.

## Scope

- Add a hub-owned validated `ui_tree_snapshot` to `plugin_surface_render` responses when a plugin surface returns a UiNode/body payload.
- Build the snapshot from the already validated `UiNode` produced by `HubRuntime::render_plugin_surface`; do not add browser-side vocabulary interpretation or duplicate UiNode schema rules.
- Include package and surface identity in the blessed snapshot path, at minimum `package_name`, `surface_id`, and the validated tree/body payload.
- Preserve the existing `plugin_surface.body` field for compatibility unless implementation finds it is already unused inside this repo and a cold-turkey removal is explicitly accepted by review.
- Update the authoritative hub-client Rust DTO and generated TypeScript mirror together.
- Update `docs/client-protocol.md` and the plugin contract matrix README/expectations to document the response contract.
- Add an invalid plugin body fixture path that returns malformed UiNode data and assert the daemon returns a structured operator error/diagnostic from the hub validation boundary.

## Non-Scope

- No botster-web fallback removal in this ticket; this hub change only creates the blessed path for a later web cleanup.
- No new UiNode primitives, renderer composites, binding vocabulary, or browser/TUI rendering behavior.
- No duplication of core/hub UiNode validation in Lua, TypeScript, or test-only helpers.
- No broad daemon protocol redesign, package route redesign, plugin action response redesign, MCP changes, or Project Pipelines workflow UI changes.
- No PII, host paths, secrets, environment dumps, or user data in fixtures, diagnostics, docs, or test output.

## Assumptions And Unknowns

- Assumption: `ui_tree_snapshot` should be an additive field on `DaemonPluginSurface`, preserving `body` for compatibility while making the snapshot the documented browser/TUI consumption path.
- Assumption: because this checkout does not expose a distinct core `UiTreeSnapshot` type, the smallest correct hub-owned snapshot is a hub-client DTO that wraps the validated `UiNode` JSON with package/surface identity. If a canonical core snapshot type is available through the locked `botster-core` dependency, use that instead of inventing a parallel shape.
- Assumption: invalid body diagnostics can use the existing operator-error path for `HubClientError::Runtime` if it is extended to include an actionable `DaemonDiagnostic` for `operation=plugin_surface_render`; avoid adding a new diagnostic enum variant unless existing variants cannot express validation failure.
- Unknown: whether semantic response-shape changes require a conformance fixture revision bump in this repo. Check the existing hub-client/test-support constants before changing protocol or conformance version fields.
- Unknown: whether generated TypeScript should type the snapshot body as `JsonValue` or a named `UiNode` alias. Prefer matching the current generator style unless a named UiNode type already exists.

## Affected Surfaces And Files

- Botster layers touched: Rust hub daemon, hub-client public DTO/protocol, generated TypeScript client contract, plugin fixture/test-support conformance, docs.
- Production path to preserve and prove:
  - `DaemonRequest::PluginSurfaceRender` in `src/daemon_transport.rs`
  - `HubClientApi::handle_request` in `src/client_api.rs`
  - `HubRuntime::render_plugin_surface` validation in `src/runtime.rs`
  - `daemon_plugin_surface(...)` serialization in `src/daemon_transport.rs`
  - `DaemonResponse.plugin_surface.ui_tree_snapshot` in `crates/botster-hub-client/src/lib.rs`
  - generated browser DTO in `crates/botster-hub-client/generated/daemon-protocol.ts`
- Likely files:
  - `src/client_api.rs`
  - `src/daemon_transport.rs`
  - `crates/botster-hub-client/src/lib.rs`
  - `crates/botster-hub-client/src/typescript.rs`
  - `crates/botster-hub-client/generated/daemon-protocol.ts`
  - `crates/botster-hub-test-support/src/lib.rs`
  - `fixtures/plugins/plugin-contract-matrix/plugin.lua`
  - `fixtures/plugins/plugin-contract-matrix/botster-package.json`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `docs/client-protocol.md`

## Risks

- Snapshot/body drift: serializing `body` and `ui_tree_snapshot.body` through separate code paths could reintroduce the drift this ticket is meant to remove. Derive both from one validated `UiNode` value.
- Validation placement: wrapping before `node.validate()` would create a blessed-looking snapshot that is not hub-validated. Keep validation in `HubRuntime::render_plugin_surface` before response construction.
- Compatibility: downstream clients may still read `plugin_surface.body`; keep it intact while documenting `ui_tree_snapshot` as the blessed path.
- Test false positives: the existing blocked surface only proves plugin handler failure, not invalid UiNode validation. Add a separate invalid-body fixture.
- Diagnostic ambiguity: a validation failure with no diagnostic row would still force clients to infer drift from a generic error. Require structured operator error plus diagnostic evidence tied to `plugin_surface_render`.
- Type drift: changing Rust DTOs without regenerating `daemon-protocol.ts` would leave botster-web consuming stale shapes.
- PII/path leakage: operator errors and fixtures must not include local package paths, socket paths, or user-specific data.

## Acceptance Checks And Tests

- Focused hub-client/protocol check:
  - `./test.sh -p botster-hub-client`
  - Must prove generated TypeScript is current and `DaemonPluginSurface` includes the additive `ui_tree_snapshot` field with serde optionality matching Rust.
- Focused daemon/conformance check:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - Must prove a real daemon `plugin_surface_render` for the contract matrix valid app surface returns `DaemonResponseKind::PluginSurface`, `plugin_surface.package_name`, `plugin_surface.surface_id`, existing `plugin_surface.body`, and `plugin_surface.ui_tree_snapshot` with matching package/surface identity and validated tree payload.
- Invalid body check:
  - Add or extend the contract matrix conformance so an invalid surface returns `DaemonResponseKind::OperatorError`, `error.operation == "plugin_surface_render"`, an error code/message identifying invalid plugin surface/UiNode validation, and at least one structured diagnostic row for the same operation.
- Runtime/user-path proof required in review:
  - Evidence must show the response came through the production daemon request path, not only by constructing DTOs in a unit test.
  - Evidence must show `HubRuntime::render_plugin_surface` remains the validation source for snapshots.
- If docs or fixture metadata change:
  - Confirm `docs/client-protocol.md` and `fixtures/plugins/plugin-contract-matrix/README.md` describe `ui_tree_snapshot` as the blessed browser/TUI path.
- Broad safety check if touched files or compiler feedback require it:
  - `./test.sh`
  - If the full suite hangs or fails outside touched surfaces, record exact failing test names and attribution rather than waiving broadly.

## Vault Gaps Worth Capturing

- Capture after implementation if the settled `ui_tree_snapshot` DTO shape becomes a durable cross-client convention: package/surface identity plus validated tree payload, while `body` remains compatibility-only.
- Capture if implementation defines the standard diagnostic mapping for invalid plugin UiNode bodies, because future surface fixtures and web/TUI clients should rely on the same operation/error semantics.
- Capture if conformance revision rules are clarified for additive plugin surface response fields.
- No convention conflict found. The plan follows the hub-client external boundary, hub-owned UiNode validation, generated DTO drift guard, plugin-worker render ownership, and the no-browser-vocabulary-duplication rule.
