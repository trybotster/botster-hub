# Wire Hub Plugin Lifecycle Through Core Host-Profile APIs

Ticket: `ticket_1780447078_389483`

## Context Loaded

- Project Pipelines context loaded for run `run_1780460805_977080`, current step `botster_plan`, run step `run_step_1780460805_462735`, gate `botster_plan_gate`, ticket `Wire hub plugin lifecycle through core host-profile APIs`, closed dependencies, artifacts, findings, questions, answers, dependencies, and recent events.
- Ticket target: the botster-hub repository; active pipeline worktree is this run worktree.
- Required playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Required Botster planning overlay notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
- Ticket-specific architecture notes loaded:
  - [[botster hub is a first party host profile over core]]
  - [[botster packages should enforce core hub cli plugin provider boundaries]]
- Required self/context notes loaded:
  - `self/identity.md`
  - `self/goals.md`
- Repo context inspected:
  - `Cargo.toml`
  - `Cargo.lock`
  - `README.md`
  - `src/lib.rs`
  - `src/main.rs`
  - `src/runtime.rs`
  - `src/packages.rs`
  - `src/profile.rs`
  - `src/config.rs`
  - `src/auth.rs`
  - `src/persistence.rs`
  - `tests/hub_runtime_test.rs`
  - prior plan artifacts under `docs/plans/`
  - locked `botster-core` source from `Cargo.lock` at git revision `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`, including `PluginWorkerEngine`, `PluginWorkerRegistration`, `PluginHandlerRegistration`, `PluginLoadSpec`, `PluginReloadSpec`, `PluginUnloadSpec`, `PluginRuntime`, `PluginInvocationRequest`, `PluginInvocationOutcome`, `PluginInvocationResult`, and package/capability/host-profile contracts.
  - Plan Review context for `review_1780506650_244983`, including findings requiring correction of the core commit source of truth, `PluginInvocationOutcome` return type, HubRuntime lifecycle ownership, and hub-side load-refusal wording.
- Workflow checklist evidence:
  - `project_pipelines_create_vault_checklist` was attempted for this run and failed with a Project Pipelines SQLite write lock.
  - Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and gate payload: notes read, convention conflicts, verification plan, and durable capture decision.

## Scope

In scope:

- Wire hub-owned package policy to core plugin worker lifecycle mechanics for local deterministic plugin/provider execution.
- Add a hub lifecycle adapter, likely `src/lifecycle.rs`, around `botster_core::PluginWorkerEngine`.
- Load only installed-and-enabled `PackageRegistry` records. Admission and grants remain hub policy over core `PackageManifest`, `CapabilitySet`, `Capability`, `CapabilitySurface`, `ExtensionKind`, and `admit_host_profile`.
- Convert enabled package records plus host-supplied deterministic runtime fixtures into core `PluginWorkerRegistration` values.
- Route load, invoke, reload, unload, descriptor cleanup, resource cleanup, backpressure visibility, and runtime stop behavior through `PluginWorkerEngine`.
- Treat ordinary plugin and provider packages the same at the core worker boundary after hub policy admits them. Provider privilege is grant/admission policy, not a separate execution bypass.
- Extend `HubRuntime` with a plugin lifecycle field and narrow lifecycle facade methods so the production/library entry point actually uses the new behavior. A helper `HubPluginLifecycle` may exist internally, but it must be owned by `HubRuntime`, the same object constructed by `src/main.rs`.
- Add focused tests proving:
  - enabled fixture plugin packages load through core,
  - enabled fixture provider packages are admitted by package policy before loading,
  - invocations go through core worker handling,
  - missing handler capabilities are denied by core worker enforcement,
  - reload removes old plugin-owned descriptors/resources and stops the old runtime,
  - unload cleans resources and later invocation fails because the worker is stopped,
  - disabled or ungranted packages cannot be loaded.
- Update README/docs only where needed to replace the current "future lifecycle load/unload wiring remain excluded" wording with the new bounded lifecycle behavior.
- Keep no-PII discipline across docs, tests, and output.

Non-scope:

- No marketplace, git fetch, package download, cloud provider implementation, WebRTC provider implementation, Rails, ActionCable, OAuth, browser shell implementation, TUI, React SPA, MCP surface, or persistent database.
- No Lua interpreter implementation. Tests should use a fake `PluginRuntime`; production wiring should accept host/runtime objects through core's `PluginRuntime` trait rather than inventing a Lua runtime.
- No broad package manager, lockfile format, registry persistence, installer, update resolver, or generic plugin framework.
- No new capability vocabulary in `botster-hub`; consume `botster-core` capability and manifest types.
- No direct exposure of plugin runtime callbacks as a hub API. Public hub invocation should accept core `PluginInvocationRequest` or tightly mirror it and delegate to core.
- No data-plane or PTY/session runtime refactor. Existing `HubRuntime -> DefaultBotsterEngine` behavior must remain intact.

Botster layers touched:

- Rust hub crate: primary.
- Rust `botster-core` plugin worker and package contracts: consumed through current public API, not changed.
- Rust tests: new lifecycle test coverage plus existing runtime tests preserved.
- Docs: this plan and focused README wording.

Pipeline gates and artifacts:

- Plan gate evidence should point at this plan artifact.
- Implementation gate must include committed diff, test output, and proof that the production/library path invokes core lifecycle APIs rather than only defining helper structs.
- Review should reject unwired lifecycle code, direct fake-runtime invocation, duplicate capability vocabulary, or docs-only evidence.

## Assumptions And Unknowns

Assumptions:

- The closed dependency tickets mean core package lifecycle command surfaces, package/provider registry, and capability grant policy are available enough in this repo and locked `botster-core` to wire them together.
- The `Cargo.lock`-resolved `botster-core` revision `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c` is the source of truth for current lifecycle API shape unless implementation intentionally refreshes `Cargo.lock`.
- The ticket means "ordinary providers" as provider-classified packages admitted by hub policy and executed through core worker mechanics, not concrete cloud/WebRTC provider implementations.
- A fake deterministic `PluginRuntime` in hub tests is acceptable because the ticket is about hub-to-core lifecycle wiring, not Lua execution.
- Existing `PackageRegistry::enable` is the correct hub grant/admission gate. Lifecycle loading should not repeat all policy, but it should refuse non-enabled records.
- `HubRuntime` can own both `DefaultBotsterEngine` and a plugin lifecycle adapter without merging PTY/session data-plane behavior into plugin lifecycle behavior.
- The binary smoke path does not need to load plugins unless the implementation adds configured fixture package startup. `HubRuntime` owning the lifecycle adapter and exposing public lifecycle methods exercised by integration tests is sufficient runtime path proof for this scaffold.

Unknowns for the Implementer to resolve:

- Whether the cleanest public API is `HubRuntime::load_enabled_package(...)` methods or similarly named methods that delegate to an internal `HubPluginLifecycle`. `HubPluginLifecycle` must not be a free-floating public facade used only by tests.
- Whether `PackageRegistry` needs a tiny `enabled_packages()` helper or whether lifecycle tests can pass explicit `PackageRecord` references from existing accessors.
- Whether core's current `PluginLoadSpec.metadata` and descriptor body shapes are sufficient for provider fixtures without adding hub-owned metadata.
- Whether reload should be exposed as one method accepting the new runtime bundle, or as unload plus load. Prefer core `PluginWorkerEngine::reload_plugin` so cleanup behavior is tested directly.
- Whether README should add a lifecycle section or only adjust the Package Registry Policy section. Prefer the smaller README update unless public APIs need discovery context.

No blocking human question is required unless implementation discovers two incompatible core host-profile lifecycle APIs or would need to waive the "no direct bypass of core worker/capability enforcement" acceptance criterion.

## Affected Surfaces / Files

Expected changes:

- `src/lifecycle.rs` or equivalent new module
  - Define a hub lifecycle adapter over `PluginWorkerEngine`.
  - Define a small host-supplied runtime bundle type containing core `PluginRuntime`, handlers, descriptors, resources, and entrypoint selection.
  - Build `PluginWorkerRegistration`, `PluginLoadSpec`, `PluginReloadSpec`, and `PluginUnloadSpec` from enabled package records.
  - Return typed hub lifecycle errors for disabled/not-installed packages, missing runtime bundle, and missing entrypoint, while preserving core invocation results for worker/capability failures.
- `src/runtime.rs`
  - Add a plugin lifecycle field and narrow methods that delegate to the lifecycle adapter.
  - Preserve existing `DefaultBotsterEngine` session methods unchanged.
- `src/packages.rs`
  - Add only small read helpers if needed, such as deterministic enabled package iteration. Do not add new capability concepts.
- `src/lib.rs`
  - Export lifecycle types and update `ArchitectureSummary` facade decisions if the lifecycle methods become part of the public hub facade audit.
- `tests/hub_plugin_lifecycle_test.rs`
  - Add deterministic fake-runtime integration tests over `HubRuntime` lifecycle methods.
- `tests/hub_runtime_test.rs`
  - Preserve existing session runtime test. Add compile-shape assertions only if lifecycle methods live on `HubRuntime`.
- `README.md`
  - Update package/lifecycle wording to state that enabled local fixture packages can now be loaded through core worker lifecycle mechanics.

Not expected:

- `Cargo.toml` or `Cargo.lock`, unless locked core API is unusable and an intentional core refresh is required.
- `src/main.rs`, unless the implementer chooses a tiny smoke-summary line to show lifecycle readiness.
- `src/auth.rs`, `src/persistence.rs`, `src/profile.rs`, or `src/config.rs`, except documentation wording if strictly necessary.
- Any Rails, WebRTC, cloud, TUI, React, Lua plugin implementation, MCP, marketplace, or provider implementation files.

## Implementation Shape

Suggested hub lifecycle concepts:

```rust
pub struct HubPluginLifecycle {
    engine: botster_core::PluginWorkerEngine,
    loaded: BTreeMap<String, botster_core::PluginKey>,
}

pub struct HubPluginRuntimeBundle {
    pub runtime: Arc<dyn botster_core::PluginRuntime>,
    pub handlers: Vec<botster_core::PluginHandlerRegistration>,
    pub descriptors: Vec<botster_core::PluginOwnedDescriptor>,
    pub resources: Vec<botster_core::PluginResourceRef>,
    pub entrypoint: Option<String>,
}
```

The exact names can change. The important constraints are:

- `HubRuntime` owns a `HubPluginLifecycle` field or equivalent core worker adapter state; tests should use the `HubRuntime` lifecycle methods so the production/library path is wired.
- `PackageRecord.manifest` is the manifest passed into `PluginWorkerRegistration`.
- Package name maps deterministically to `PluginKey` unless core exposes a better key.
- Lifecycle loading refuses packages whose `PackageRecord::is_enabled()` is false.
- Disabled, not-installed, ungranted, or unadmitted package refusal is a hub lifecycle adapter pre-registration guard. `PluginWorkerEngine::load_plugin` is infallible and does not re-check hub package grants, so negative-path tests must exercise hub refusal before building/registering a core worker.
- Lifecycle loading picks a manifest entrypoint from core manifest entrypoints; do not hard-code `plugin.lua` in production methods except in tests/fixtures.
- Invocation delegates to `PluginWorkerEngine::invoke`, which returns `PluginInvocationOutcome`; caller-facing assertions unwrap `outcome.result` to inspect `PluginInvocationResult`.
- Reload delegates to `PluginWorkerEngine::reload_plugin`.
- Unload delegates to `PluginWorkerEngine::unload_plugin`.
- Descriptor/resource cleanup result comes from core `PluginCleanupResult`.
- Tests must call through the hub public lifecycle path, not directly through `PluginWorkerEngine`.

Suggested test fixtures:

- Build plugin and provider `PackageManifest` values with `botster_core` types.
- Install and enable them through `PackageRegistry`.
- Use fake runtime structs implementing `PluginRuntime` and recording invocations/stops.
- Register handlers with required capabilities from `botster_core::Capability`.
- Register descriptors/resources owned by core `PluginDescriptorRef`/`PluginResourceRef`.
- Invoke by core `PluginInvocationRequest`, receive core `PluginInvocationOutcome`, and assert `outcome.result` is `PluginInvocationResult::Completed`.
- Attempt invocation requiring a capability absent from `PackageManifest.capabilities` and assert core returns a failed invocation rather than the fake runtime being called.
- Reload with a new runtime bundle and assert old runtime `stop` was called and old descriptors/resources are reported removed.
- Unload and assert cleanup plus worker-stopped failure on subsequent invoke.

## Risks

- Direct bypass risk: calling `PluginRuntime::invoke` from hub code would skip core handler lookup, capacity, capability, timeout, and cleanup behavior. Tests must prove fake runtime invocation occurs only after `PluginWorkerEngine::invoke`.
- Policy duplication risk: rechecking grants manually in lifecycle code could diverge from `PackageRegistry::enable`. Lifecycle should require enabled records, not rebuild a parallel grant engine.
- Provider privilege ambiguity: provider packages need host-profile admission before enablement, but after enablement their execution should still go through the same core worker mechanics as plugins.
- Overbuilding risk: a runtime catalog, installer, marketplace, persistent store, or generic host-profile framework would exceed the ticket.
- Underwiring risk: adding `HubPluginLifecycle` without connecting it to `HubRuntime` would fail the "actual runtime/user path changed" requirement.
- Cleanup risk: reload/unload must use core cleanup scopes so descriptors/resources are not left in hub-owned side maps.
- Core commit/source-of-truth drift risk: `botster-core` tracks `main` in `Cargo.toml`, but implementation compiles the revision pinned in `Cargo.lock`. Implementers must read the lockfile-resolved checkout, currently `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`, and avoid lockfile churn unless the locked commit cannot satisfy the ticket.
- PII risk: the ticket prompt contains a local absolute path. Do not bake user paths, hostnames, keys, fingerprints, or environment values into committed tests/docs beyond this ticket context note.
- SQLite workflow risk: Project Pipelines checklist writes are currently locked. Gate evidence must carry checklist-equivalent proof.

## Acceptance Checks / Tests

Required commands after implementation:

- `cargo fmt`
- `cargo test`
- `cargo test --test hub_runtime_test`
- `cargo test --test hub_plugin_lifecycle_test`
- `./test.sh`
- `cargo clippy --all-targets --all-features -- -D warnings`

Targeted acceptance assertions:

- `HubRuntime` constructs and owns a lifecycle adapter that uses `botster_core::PluginWorkerEngine`.
- Enabled fixture plugin packages load through the hub public lifecycle path into core worker registration.
- Enabled provider fixture packages first pass `PackageRegistry::enable` / core host-profile admission, then load through the same core worker lifecycle path.
- Disabled, not-installed, ungranted, or unadmitted packages cannot load.
- Invocation uses core `PluginWorkerEngine::invoke` and returns core `PluginInvocationOutcome`; tests unwrap `outcome.result` to assert `PluginInvocationResult::Completed` or `PluginInvocationResult::Failed`.
- A handler requiring a capability absent from the manifest is denied by core worker capability enforcement and does not call the fake runtime.
- Disabled, not-installed, ungranted, or unadmitted package load attempts are rejected by the hub lifecycle adapter before core worker registration; tests should not imply core `load_plugin` enforces hub grants.
- Reload uses core `PluginWorkerEngine::reload_plugin` and returns cleanup for old descriptors/resources.
- Unload uses core `PluginWorkerEngine::unload_plugin`, calls runtime `stop`, removes resources/descriptors according to cleanup scope, and subsequent invocation fails as worker-stopped.
- Existing `tests/hub_runtime_test.rs` still proves `HubRuntime -> DefaultBotsterEngine` session behavior.
- README no longer says lifecycle load/unload wiring remains excluded without qualification.
- Branch diff contains no marketplace/git/cloud/WebRTC/Rails/Lua runtime/provider implementation and no PII.

Runtime path proof:

- Evidence that `botster-core` already has `PluginWorkerEngine` is not enough.
- The hub crate must expose and exercise a public lifecycle path that delegates to core worker APIs.
- Tests should instantiate `HubRuntime` or the exported hub lifecycle facade and invoke through that path.

## Vault Gaps Worth Capturing

No durable vault gap is required at plan time. Existing notes already cover the major constraints:

- hub as first-party host profile over core,
- core/hub/plugin/provider boundary ownership,
- plugin worker execution behind core mechanics,
- package manifests/capabilities as core-owned contracts,
- Project Pipelines checklist write-lock fallback.

Capture candidate after implementation only if a more specific reusable rule emerges, such as: "hub lifecycle adapters may require enabled `PackageRecord`s but must not reimplement package grants once `PackageRegistry::enable` has admitted them."

## Checklist Evidence

- Vault/project conventions loaded: listed in Context Loaded.
- Convention conflicts: none. The plan keeps core mechanisms in `botster-core`, hub policy in `botster-hub`, avoids new provider/cloud/Rails implementations, and requires a repo-visible plan artifact.
- Verification evidence planned: commands and targeted assertions listed in Acceptance Checks / Tests.
- Durable knowledge capture: no capture now; possible post-implementation capture noted above.
