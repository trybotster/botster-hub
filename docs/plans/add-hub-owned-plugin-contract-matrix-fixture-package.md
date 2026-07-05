# Add hub-owned plugin contract matrix fixture package

## Context Loaded

- Ticket `ticket_1783280110_890320`, run `run_1783281931_152657`, step `botster_plan`, gate `botster_plan_gate`.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Required self context: [[identity]], [[goals]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Package/plugin notes: [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster runnable entrypoints are hub owned launch contracts]], [[structured output fields need producer paths or explicit scaffold disposition]].
- Project Pipelines workflow notes: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]].
- Repo evidence:
  - Existing checked-in fixtures are `examples/synthetic-plugin` and `examples/project-pipelines`.
  - Current daemon tests still generate several package/plugin fixtures inline in `tests/hub_daemon_lifecycle_test.rs`, including declared surfaces, configuration schema, and workspaces render coverage.
  - The closed dependency has already wrapped `DaemonResponse.plugin_surface` as `DaemonPluginSurface { package_name, surface_id, body }` in `crates/botster-hub-client/src/lib.rs` and `src/daemon_transport.rs`.
  - `HubRuntime::render_plugin_surface` and `HubRuntime::dispatch_plugin_surface_action` in `src/runtime.rs` execute through the plugin worker path and validate returned `UiNode` / action result shapes.

## Scope

- Add a checked-in public contract fixture package at `fixtures/plugins/plugin-contract-matrix` unless implementation discovers an existing preferred fixture root. The package should include:
  - `botster-package.json`
  - `plugin.lua`
  - `README.md`
- Use only public package/plugin contracts: manifest capabilities, declared `entrypoints`, declared `surfaces`, package `configuration`, route descriptors projected by the hub, plugin surface route handlers, and plugin surface action handlers.
- The fixture should expose a deliberate matrix:
  - app surface returning a valid `UiNode` payload;
  - empty or placeholder app surface returning a valid empty-state/placeholder `UiNode`;
  - blocked/error surface that returns a structured render failure through the real daemon path;
  - settings/config surface that reads sanitized package config through `botster.capabilities.config.get()`;
  - configuration schema with defaults plus at least one validation-rejecting value path;
  - `plugin_surface_action` success and error/rejected outcomes;
  - package route descriptors derived from the declared surfaces;
  - package lifecycle compatibility through install/enable/list/show.
- Refactor focused hub tests to install/enable the checked-in fixture from disk and assert the matrix through real daemon requests.
- Add docs explaining each surface/action/config row, keeping fixture data PII-free and explicitly saying client repos can use it for conformance.

## Non-Scope

- No product UI polish, marketplace UX, browser SPA changes, TUI changes, Rails/Hotwire work, or first-party Project Pipelines behavior changes.
- No private hub-internal fixture API. Tests may use existing daemon test harness helpers, but the fixture itself must be installable like an ordinary local package.
- No new plugin runtime primitives, new package manifest vocabulary, or broad protocol version redesign.
- No duplication of botster-web downstream conformance logic inside this repo. This repo should publish the fixture and prove hub behavior; downstream clients can consume it separately.
- No broad cleanup of existing generated inline fixtures except where replacing them is necessary to point tests at the new canonical fixture.

## Assumptions And Unknowns

- Assumption: `fixtures/plugins/plugin-contract-matrix` is an acceptable clear fixture path and better communicates conformance intent than placing this under `examples/`.
- Assumption: the fixture can use the existing `handlers` ABI with `surface_route` and `ui_action`, as shown by `examples/project-pipelines/plugin.lua`.
- Assumption: render-error coverage should assert the daemon/operator error shape produced by a failing surface handler, not add a fake `UiNode` error node unless the public contract already defines one.
- Assumption: configuration validation should be proven through `DaemonRequest::SetPackageConfiguration` using existing package registry validation, not by letting Lua self-mutate config.
- Unknown: whether the public manifest `surfaces` schema supports a distinct settings kind today. If it does, use that. If not, declare a normal app surface documented as the settings/config coverage row and do not invent a new kind.
- Unknown: whether `plugin_surface_action` currently wraps action results with package/surface metadata. This ticket only requires success/error round trips, so metadata wrapping should be left alone unless current public types already require it.

## Affected Surfaces And Files

- Botster layers touched: fixture package, Lua plugin ABI usage, Rust hub daemon tests, package docs.
- New fixture files:
  - `fixtures/plugins/plugin-contract-matrix/botster-package.json`
  - `fixtures/plugins/plugin-contract-matrix/plugin.lua`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
- Likely test updates:
  - `tests/hub_daemon_lifecycle_test.rs` for real daemon install/enable/list/show/render/action/config validation.
  - Possibly `crates/botster-hub-client/src/lib.rs` tests only if implementers discover missing public DTO coverage for the fixture’s existing response shapes.
- Reference-only production paths:
  - `src/daemon_transport.rs`
  - `src/client_api.rs`
  - `src/runtime.rs`
  - `src/lua_runtime.rs`
  - `src/packages.rs`
  - `crates/botster-hub-client/src/lib.rs`

## Risks

- A fixture that uses private helper-only Rust setup would not be useful to downstream clients. Keep the fixture package ordinary and checked in.
- Render-error coverage can become ambiguous if it only proves source code exists. It must drive `DaemonRequest::PluginSurfaceRender` and observe the operator error or plugin error response from the live daemon path.
- Configuration schema coverage can accidentally duplicate the Project Pipelines-specific acceptance test. The new fixture should become the generic conformance target; Project Pipelines tests can remain product-specific only where needed.
- Lua table arrays must stay sequential; nil gaps in handler or surface metadata arrays can silently truncate registration.
- PII/path leakage risk in docs and test assertions. Fixture docs should use relative paths and `example.invalid` data only.
- Over-broad inline fixture cleanup could churn unrelated tests. Replace only tests directly proving this contract matrix.

## Acceptance Checks And Tests

- Focused daemon integration:
  - `./test.sh --test hub_daemon_lifecycle_test <new plugin contract matrix test name>`
  - Expected proof: install/enable from `fixtures/plugins/plugin-contract-matrix`, then `ListPackages` and `ShowPackage` expose all declared surfaces, routes, config schema, and lifecycle state.
- Render path proof:
  - Real `DaemonRequest::PluginSurfaceRender` returns `DaemonResponseKind::PluginSurface` for the valid app surface with `plugin_surface.package_name`, `plugin_surface.surface_id`, and valid `body`.
  - Placeholder surface returns a valid empty/placeholder `UiNode`.
  - Error/blocked surface returns an operator/plugin error response through the daemon path with diagnostics still keeping the daemon responsive.
- Action path proof:
  - Real `DaemonRequest::PluginSurfaceAction` returns an accepted/success result for the success action.
  - Real `DaemonRequest::PluginSurfaceAction` returns rejected/error state plus diagnostics for the error action.
- Config proof:
  - `SetPackageConfiguration` rejects an invalid value.
  - `SetPackageConfiguration` accepts valid values.
  - The config/settings surface reads sanitized effective config via `botster.capabilities.config.get()`.
- Regression gates:
  - `./test.sh -p botster-hub-client` if public DTO/generated protocol tests are touched.
  - `./test.sh --test hub_daemon_lifecycle_test` or the repo-approved narrower equivalent if the implementer changes shared daemon test helpers.
- Runtime/user-path evidence required in review:
  - The test must use the real daemon request path, not only `PackageRegistry` or direct `HubRuntime` calls.
  - Docs must identify how client repos should install/use the fixture for conformance.

## Vault Gaps Worth Capturing

- Capture after implementation if `fixtures/plugins/plugin-contract-matrix` becomes the canonical fixture root for client conformance work.
- Capture if a stable convention emerges for representing render-blocked surfaces: operator error response versus valid blocked-state `UiNode`.
- No convention conflict found. The plan follows the package-as-public-contract boundary, plugin-worker execution model, hub-client external protocol boundary, and the checklist evidence fallback rule.
