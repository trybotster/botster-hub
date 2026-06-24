---
ticket: ticket_1782257625_962006
title: Expose package configuration to Lua plugins through hub capability runtime
run: run_1782266925_863523
step: botster_plan
---

# Expose package configuration to Lua plugins through hub capability runtime

## Context Loaded

- Pipeline context: ticket `ticket_1782257625_962006`, run `run_1782266925_863523`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, answers, or reviews. Dependency `ticket_1782257625_477566` is marked closed.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster/vault constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and the loaded self context.
- Checklist discipline: `project_pipelines_checklist_instructions` loaded. Creating the run vault checklist timed out in the Project Pipelines worker, so checklist evidence is preserved in this plan and should be copied into gate evidence per the known fallback pattern.
- Repo context inspected: `src/packages.rs`, `src/lua_runtime.rs`, `src/runtime.rs`, `src/capabilities.rs`, `src/persistence.rs`, `tests/hub_lua_runtime_test.rs`, `tests/hub_capability_runtime_test.rs`, `examples/synthetic-plugin/*`, `README.md`, `docs/client-protocol.md`, and dependency branch `project-pipelines/ticket_1782257625_477566`.
- Dependency freshness context: current `HEAD` is `d8aca2b` on `main` and does not contain package configuration structs. The dependency branch `project-pipelines/ticket_1782257625_477566` contains `PackageConfigurationState`, `PackageConfigurationView`, `PackageRegistry::set_configuration`, redacted package DTOs, `DaemonRequest::SetPackageConfiguration`, and synthetic-plugin configuration. Implementation must merge/rebase that dependency or otherwise update to a `main` containing it before touching the Lua runtime.

## Scope

- Expose the current Lua plugin's own effective package configuration through the existing `botster` Lua API, preferably under `botster.capabilities.config` or a similarly narrow capability-gated table.
- Use the hub package registry's existing redacted effective configuration view from the dependency work; do not create a second manifest/config parser in `src/lua_runtime.rs`.
- Pass only the currently loaded package's sanitized configuration into that package's Lua VM during `HubRuntime::load_lua_plugin_package`.
- Return manifest defaults and operator-set non-secret values to Lua.
- Return redacted or absent secret values to Lua. Raw secret material must not be exposed unless a future explicit secret-handle mechanism exists.
- Prove package isolation: a Lua plugin can read only its own configuration and cannot request another package's config by name.
- Keep config writes/admin mutation in daemon/CLI/API package admin paths from the dependency ticket, not Lua plugin self-mutation.

## Non-Scope

- No new package configuration persistence/admin API beyond integrating the dependency branch.
- No arbitrary plugin self-mutation of package config.
- No secret-handle or credential-store implementation.
- No broad filesystem, HTTP, network, or capability grant expansion.
- No TUI, React SPA, MCP workflow, Rails, marketplace, or package installer work.
- No runtime-global config table that lets plugins enumerate other package records.

## Assumptions And Unknowns

- Assumption: after dependency integration, `PackageRecord::configuration_view()` or equivalent is the authoritative source for redacted effective values.
- Assumption: the Lua read API can be read-only and zero-argument, for example `botster.capabilities.config.get()` returning the caller package's effective values plus missing/diagnostic metadata if useful.
- Assumption: "capability-gated" can be satisfied by requiring an explicit config-related capability if the core/hub dependency provides one; if no such surface exists, use the already-admitted package lifecycle boundary and keep the API scoped to the current `PluginKey`.
- Unknown: final capability surface name. If the dependency or core exposes `CapabilitySurface::Config`, use it. If not, do not invent a broad new taxonomy without reviewer agreement; a scoped read-only helper may be acceptable because isolation comes from the bound `PluginKey`.
- Unknown: exact public Lua table name. Prefer matching existing style (`botster.capabilities.plugin_db.get`) over adding globals.
- Unknown: whether missing required configuration should prevent Lua plugin load in all cases. Dependency branch already denies enable when required config is missing, so implementation should preserve that and add a focused regression if loading can bypass enable.

## Botster Layers Touched

- Rust hub package/lifecycle boundary.
- Rust Lua runtime API.
- Lua plugin fixture.
- Rust integration tests and Lua ABI docs.

No session/client data plane, daemon package admin flow, SPA, TUI, Rails relay, or MCP workflow policy should change except as needed to merge the closed dependency.

## Affected Surfaces/Files

- `src/runtime.rs`: when preparing/loading a local Lua package, compute the current package's redacted effective config from the enabled registry record and pass it into `LuaPluginRuntime::load_prepared`.
- `src/lua_runtime.rs`: extend `LuaPluginRuntime::load_prepared`, `LoadedLuaPlugin::load`, `LuaPluginRuntime::new`, and `install_botster_api` to install a read-only config helper bound to the current `PluginKey` and sanitized config payload.
- `src/packages.rs`: dependency merge surface only, plus a small public helper if the existing configuration view is not easy to consume from `runtime.rs`.
- `examples/synthetic-plugin/botster-package.json`: use the dependency's manifest configuration fixture with non-secret defaults and a secret field.
- `examples/synthetic-plugin/plugin.lua`: update the echo tool or add a narrow tool that calls the new config API.
- `tests/hub_lua_runtime_test.rs`: primary acceptance coverage using real Lua plugin load/invoke through `HubRuntime`.
- `docs/lua-plugin-abi.md`: document the read-only config helper, current-package scoping, defaults, and secret redaction.
- Possibly `src/lib.rs`: export only if a new type/helper is intentionally public; avoid public API growth if internal structs suffice.

## Implementation Shape

1. Bring the dependency code into the worktree first, then rerun a targeted compile check to confirm the package configuration types are available.
2. Add a small sanitized config payload type if needed, derived from `PackageConfigurationView`, with values represented as serde JSON for direct Lua conversion.
3. Change `HubRuntime::load_lua_plugin_package` to read the enabled `PackageRecord` for `package_name`, compute the redacted effective config, and pass that payload alongside `PreparedLocalPackage`.
4. Install a Lua helper in `install_botster_api` that closes over the current plugin's config payload. Keep it read-only and do not accept a package name argument.
5. Update the synthetic plugin to return config data from a real handler. Include assertions for a defaulted non-secret value, an operator-set non-secret value if practical, and a secret field redacted/absent.
6. Add a second package or adversarial Lua call only if needed to prove isolation. The simplest proof is that the API has no package-name parameter and a plugin loaded as package A receives only package A's payload while package B receives different/empty payload.
7. Update Lua ABI docs with the exact helper shape and explicit non-goals.

## Risks

- Stale dependency risk: current `HEAD` lacks package config support. Implementing without the dependency would duplicate schema/state work and violate the closed dependency boundary.
- Secret leakage risk: passing `PackageConfigurationView` into Lua must not accidentally include write-only/raw secret values, diagnostics with secret contents, or persisted raw values.
- Isolation risk: accepting a package name or returning registry-shaped data would let one plugin inspect another package's config.
- Underwired implementation risk: adding helper structs without passing them through `HubRuntime::load_lua_plugin_package` would not change the real runtime path.
- Capability ambiguity risk: if no config capability surface exists, reviewers may object to calling the API "capability-gated." Keep the helper bound to admitted plugin identity and document the exact gating decision.
- Regression risk: changing Lua runtime constructor signatures may affect existing Project Pipelines and synthetic plugin tests.

## Acceptance Checks/Tests

- `tests/hub_lua_runtime_test.rs` proves a real Lua plugin loaded through `HubRuntime::load_lua_plugin_package` can read its own effective config through the new Lua helper.
- Test proves defaulted non-secret config values are visible to Lua.
- Test proves operator-set non-secret config values are visible if the dependency API supports setting before enable/load in the test flow.
- Test proves secret config values are redacted or absent in Lua and raw secret sentinel strings do not appear in Lua return payloads.
- Test proves package isolation: another package's config cannot be requested or returned.
- Regression coverage keeps existing timer/plugin_db Lua helper behavior passing.
- Run `cargo fmt`.
- Run `./test.sh --test hub_lua_runtime_test` and `./test.sh --test hub_capability_runtime_test` if dependency/runtime changes touch capability behavior.
- Run a broader `./test.sh` if time allows after dependency merge, or document exact skipped scope.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Plan gate evidence should include this artifact plus the checklist fallback evidence below.
- Implement gate must show the production entry point changed: `HubRuntime::load_lua_plugin_package` passes sanitized current-package config into `LuaPluginRuntime`, and a real Lua handler reads it.
- Review/verify should scan diff and test output for raw secret sentinel leakage, local absolute home paths, and config APIs that accept arbitrary package names.

## Worktree And Target Assumptions

- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Worktree: this pipeline-assigned worktree, not an ambient checkout.
- Base ref: `main`. Because the dependency branch is not present in this worktree's `HEAD`, implementation must integrate or rebase onto the closed dependency before applying runtime changes.

## Checklist Evidence

- Vault/project notes read: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], `self/identity.md`, and `self/goals.md`.
- Convention conflicts: none, provided implementation consumes the dependency package-config contract and avoids a parallel schema/parser.
- Verification evidence at plan time: repo inspection plus dependency branch inspection; no tests run because this is a plan step and current `HEAD` lacks the required dependency API.
- Checklist persistence: `project_pipelines_create_vault_checklist` timed out in the plugin worker, so evidence is preserved here and should be attached to the gate.

## Vault Gaps Worth Capturing

- Capture after implementation if settled: "Lua plugin configuration reads must be current-plugin scoped and derived from redacted `PackageConfigurationView`, never from raw package registry records."
- Capture after implementation if settled: the exact Lua ABI name for package config reads.
- No new vault note is required at plan time; existing notes already cover hub/core/package boundaries, Lua plugin runtime scoping, checklist timeout fallback, and runtime-path proof.
