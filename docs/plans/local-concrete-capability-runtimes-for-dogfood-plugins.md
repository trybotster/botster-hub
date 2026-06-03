# Local Concrete Capability Runtimes For Dogfood Plugins

Ticket: `ticket_1780508732_178953`
Run: `run_1780517597_516963`

## Context Loaded

- Project Pipelines context loaded for active Plan step `botster_plan`, run `run_1780517597_516963`, run step `run_step_1780517597_743390`, gate `botster_plan_gate`, ticket `Provide local concrete capability runtimes for dogfood plugins`, no prior artifacts, findings, reviews, open questions, or answers.
- Dependency state: prerequisite `Define durable hub state model and storage boundary` is closed.
- Ticket target: assigned pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster planning overlays loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Ticket-specific vault constraints loaded: [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster plugin runtime data must not live in the plugin source tree]], [[plugin hardening needs lifecycle resource and observability layers]], [[hub event loop blocking must use spawn_blocking for I/O-bound tasks]], and [[plugin event and http callbacks run in plugin worker vms]].
- Required self/context files loaded: `self/identity.md` and `self/goals.md`.
- Repo context inspected: `README.md`, `Cargo.toml`, `src/lib.rs`, `src/profile.rs`, `src/packages.rs`, `src/lifecycle.rs`, `src/runtime.rs`, `src/persistence.rs`, `src/config.rs`, `src/main.rs`, `tests/hub_runtime_test.rs`, `tests/hub_plugin_lifecycle_test.rs`, and prior `docs/plans/*`.
- Locked `botster-core` API context inspected at Cargo git checkout revision `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`: `CapabilityRuntimeRequest`, `CapabilityOperation`, `PluginCapabilityRuntime`, `CapabilityRuntimeHandle`, `CapabilityRuntimeEvent`, `CapabilityRuntimeErrorKind`, `HttpCapabilityRuntime`, `HttpCapabilityTransport`, `InMemoryWebSocketCapabilityRuntime`, `PluginStoreBackend`, `PluginStoreLimits`, scoped filesystem request/result types, and `PluginTimerScheduler`.
- Current repo state: hub package admission and plugin lifecycle are already wired. `HubRuntime` owns `HubPluginLifecycle`, and integration tests prove enabled plugin/provider packages load, invoke, reload, and unload through core worker mechanics. There is not yet a hub-owned concrete capability-runtime path for dogfood plugins to request scoped filesystem, plugin JSON store, timers, or bounded HTTP/WebSocket capability operations through the hub facade.
- Checklist discipline: Project Pipelines checklist instructions were loaded. Checklist creation should be attempted for this run; if the plugin DB is locked or times out, gate evidence and this plan preserve the checklist-equivalent notes read, conflict check, verification plan, and capture decision.

## Scope

In scope:

- Add local hub-owned concrete capability runtimes over core's non-blocking capability contracts.
- Prioritize three concrete local runtimes:
  - scoped filesystem operations rooted in explicit hub-owned scope directories,
  - plugin JSON store backed by durable hub state if an existing store boundary is present, otherwise a clear local store under the hub data directory,
  - timers using core `PluginTimerScheduler` or core timer request/event contracts.
- Add bounded HTTP and WebSocket stubs/adapters only where practical without adding product networking behavior:
  - HTTP may use core `HttpCapabilityRuntime` with a deterministic fake/local transport in tests and an explicit no-product default policy,
  - WebSocket may wrap core `InMemoryWebSocketCapabilityRuntime` for bounded queues and adapter-facing events, not real network/WebRTC.
- Preserve core hot-path isolation. Submitting capability operations through the hub must validate/enqueue quickly; filesystem/store/HTTP work must not execute inline on session/client hot paths.
- Keep hub policy ownership visible: the hub chooses capability grants, scope roots, storage paths, operation limits, endpoint allowlists, and cleanup policy while core owns typed request/result/error contracts.
- Wire capability runtimes into the production/library path, preferably as a field on `HubRuntime` or a narrow hub-owned adapter reachable through `HubRuntime`, not as free-floating test helpers.
- Enforce deterministic denials for missing capabilities, unknown scopes, path traversal, namespace mismatch, and resource ownership mismatch.
- Add unload/reload cleanup so plugin-owned capability resources are released when `HubRuntime::unload_plugin_package` and `HubRuntime::reload_plugin_package` run.
- Update README/facade audit only where needed to document the concrete local capability-runtime path and storage path boundaries.

Botster layers touched:

- Rust hub crate: primary.
- Rust `botster-core`: consumed through public contracts only.
- Plugin/provider runtime boundary: capability requests from admitted plugins, not new Lua execution.
- Tests: focused integration/unit tests for hub capability runtimes and existing lifecycle/runtime tests.
- Docs: README and this plan if public behavior changes need discovery.

Pipeline gates and artifacts:

- This file is the repo-visible Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Implementation gate must prove the runtime/user path changed: a hub public path must submit/drain/cleanup capability runtime requests, not merely define structs.

## Non-Scope

- No changes to `botster-core`.
- No product cloud, WebRTC, Rails, ActionCable, OAuth, device-code auth, browser shell, public webhook, marketplace, package fetcher, provider process, or real external API integration.
- No Lua VM implementation or plugin source loader changes beyond the hub runtime bridge needed to accept core capability requests from already-loaded package workers.
- No broad rewrite of `HubRuntime`, package admission, plugin lifecycle, or session/PTY data-plane behavior.
- No new capability taxonomy in `botster-hub`; use `botster_core::Capability`, `CapabilitySurface`, and `CapabilityRuntimeRequest`.
- No speculative persistent database framework. Plugin JSON store may use local files or in-memory tests, but the path and upgrade limitations must be explicit.
- No runtime data under `plugins/<name>/`; mutable plugin data belongs under a hub data namespace such as `plugin-data/<plugin>/`.
- No PII in fixtures, docs, store paths, audit records, or test payloads.

## Assumptions And Unknowns

Assumptions:

- The ticket means concrete local capability backends for already-admitted dogfood plugins, not another package-admission or plugin-lifecycle pass.
- Core's locked `PluginCapabilityRuntime` contracts are the source of truth for request, handle, event, and error shapes.
- `HubRuntime` is the correct production-facing owner because it already owns the session engine and plugin lifecycle adapter used by dogfood plugin tests.
- Scoped filesystem should be rooted in explicit scope definitions derived from `HubConfig.data_directory` or a small new hub capability-runtime config, never from plugin source paths or ambient process cwd.
- Plugin JSON store scope should be plugin-key namespaced. Durable local storage under the hub data dir is preferred if small enough; an in-memory backend is acceptable only for tests.
- Timers should be logical-time driven in tests and host-scheduler driven in production hooks; timer callbacks still route through core plugin worker invocation.
- HTTP/WebSocket behavior can remain bounded local stubs/adapters for this ticket because product cloud/WebRTC behavior is explicitly excluded.

Unknowns for implementer to resolve:

- Whether core exposes enough ready-made filesystem runtime mechanics, or whether the hub should implement a small local filesystem adapter over core `FilesystemCapabilityRequest`/`FilesystemCapabilityResult` types.
- Whether the local plugin JSON store should be a simple JSON-file backend, a directory of per-key JSON files, or an SQLite-backed store if the closed dependency introduced a durable state boundary not visible in this scaffold. Choose the smallest clear local store with deterministic tests.
- Whether capability-runtime config belongs in `src/config.rs`, a new `src/capabilities.rs`, or both. Prefer a new runtime adapter module plus only minimal config types if required.
- Whether `HubPluginRuntimeBundle` needs a capability-runtime handle/reference passed to plugin runtimes, or whether `HubRuntime` should expose direct `submit_capability_request`/`drain_capability_events` methods for tests and future Lua bridge wiring.
- Whether reload should call capability cleanup before or after core worker reload. Prefer a deterministic order and assert it in tests; do not leave resources alive across replacement.

No blocking human question is required at plan time. The ticket is specific enough if implementation treats HTTP/WebSocket as bounded local stubs and avoids product-networking behavior.

## Affected Surfaces / Files

Expected code surfaces:

- `src/capabilities.rs` or equivalent new module
  - Define the hub-owned capability runtime adapter over core `CapabilityRuntimeRequest`.
  - Own explicit filesystem scopes, store namespace/root policy, timer scheduler, and optional HTTP/WebSocket stub adapters.
  - Return core `CapabilityRuntimeHandle`, `CapabilityRuntimeEvent`, `CapabilityRuntimeError`, and `PluginCleanupResult` where possible.
  - Keep per-plugin resource cleanup and denied-scope reasons typed and deterministic.
- `src/runtime.rs`
  - Add capability-runtime ownership to `HubRuntime`.
  - Add narrow public methods such as `submit_capability_request`, `cancel_capability_operation`, `drain_capability_events`, `release_capability_resource`, `cleanup_plugin_capabilities`, and timer drain if needed.
  - Ensure plugin unload/reload delegates to capability cleanup in addition to core worker cleanup.
- `src/profile.rs`
  - Add `CapabilitySurface::Timers` to governed surfaces and default grants only if the hub is ready to serve local timer callbacks. The current profile does not govern `Timers`, while the ticket explicitly prioritizes timers.
  - Consider scoped defaults such as `Filesystem:workspace`, `PluginDb:<plugin>`, `Network:http`, `Network:websocket`, and `Timers:callbacks` only where tests prove hub policy uses those scopes.
- `src/config.rs`
  - Add minimal capability runtime config only if scope roots or limits need construction from `HubStartupOptions`.
  - Keep data-dir resolution explicit and avoid ambient user paths in tests.
- `src/persistence.rs`
  - Optional small wording or enum alignment if plugin-store persistence now has a concrete local bucket.
- `src/lib.rs`
  - Export capability runtime adapter/types only if they are part of the public hub facade.
  - Update facade audit so core capability runtime operations are described as hub-exposed policy paths.
- `src/main.rs`
  - Optional smoke summary only. Do not add real package loading or external networking to the binary path.
- `tests/hub_capability_runtime_test.rs`
  - New focused integration tests through `HubRuntime` capability methods.
- `tests/hub_plugin_lifecycle_test.rs`
  - Extend reload/unload tests if capability cleanup hooks are integrated with plugin lifecycle.
- `tests/hub_runtime_test.rs`
  - Preserve current session hot-path proof; add regression only if capability submissions could affect drain/write/attach behavior.
- `README.md`
  - Document local capability runtime behavior, explicit storage paths, and excluded product networking.

Reference-only surfaces:

- Locked core source at `crates/botster-core/src/runtime/capability.rs` and `src/engine/plugin_timer.rs`.
- Prior plan docs for hub package policy and lifecycle wiring.

## Risks

- Unwired implementation risk: adding a capability module without a `HubRuntime` entrypoint would fail the ticket. Tests must call through the hub facade.
- Hot-path blocking risk: local filesystem or store operations could run inline on a hub/session/client path. Mitigation: capability submission should be admission-only or bounded; blocking work belongs behind runtime-owned workers or explicit deferred drain.
- Scope escape risk: filesystem paths could allow `..`, absolute paths, Windows drive prefixes, symlink surprises, or ambient cwd access. Mitigation: use core `ScopedRelativePath` validation plus canonical scope-root checks before I/O.
- Data placement risk: plugin store files under `plugins/<name>/` would violate the source/runtime-data boundary. Mitigation: store under hub data dir, e.g. `plugin-data/<plugin>/`.
- Grant mismatch risk: package enablement currently grants unscoped capabilities by default, while core runtime requests require scoped capabilities for filesystem, plugin db, timers, HTTP, and WebSocket. Mitigation: implement a clear hub grant context for runtime requests and test denied scopes.
- Timer cleanup risk: timers can fire after plugin unload/reload if scheduler cleanup is not linked to lifecycle. Mitigation: cleanup tests must schedule timers, unload, drain, and prove no invocation occurs.
- Queue pressure risk: noisy plugin HTTP/WebSocket/timer events could grow unbounded. Mitigation: use core bounded configs and assert backpressure events/errors.
- Product-networking creep risk: HTTP/WebSocket adapters can turn into cloud/WebRTC behavior. Mitigation: keep these as local bounded stubs or adapter seams with synthetic hosts and no real network.
- PII risk: storage paths and fixtures can leak user paths. Mitigation: tests use `target/botster-hub-test-data/...`, synthetic plugin keys, and no real tokens or hostnames.

## Acceptance Checks / Tests

Required commands after implementation:

- `cargo fmt`
- `cargo test`
- `cargo test --test hub_runtime_test`
- `cargo test --test hub_plugin_lifecycle_test`
- `cargo test --test hub_capability_runtime_test`
- `./test.sh`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo metadata --format-version 1 --no-deps` to confirm no local/path override or unexpected dependency churn.

Targeted behavioral checks:

- Enabled plugin with allowed scoped filesystem capability can submit read/write/list/stat operations through `HubRuntime`, receives core-shaped completion events, and all paths stay under the explicit scope root.
- Denied filesystem scope, absolute path, parent traversal, and ungranted operation fail deterministically before I/O.
- Plugin JSON store supports get/set/list/delete or the selected minimal CRUD shape through `HubRuntime`, namespaces data by plugin key, preserves revision/expected-revision behavior if implemented, and stores data under an explicit hub data path outside plugin source.
- Plugin store quota/key validation uses core `PluginStoreKey`/`PluginStoreLimits` semantics or a stricter hub wrapper and returns typed core errors.
- Timers can be scheduled through the hub capability path, drained by logical time in tests, and callback invocation routes through core plugin worker mechanics.
- Timer cancellation and plugin cleanup prevent later timer invocation after unload/reload.
- HTTP stub/adaptor validates grants, host/scheme allowlist, body/header limits, timeout/cancellation, and backpressure without real external network calls.
- WebSocket stub/adaptor validates grants, bounded outbound/inbound queues, resource ownership, drain behavior, and cleanup using core in-memory runtime if practical.
- Slow/noisy capability operations do not block session/client hot paths. Add a regression that saturates a capability queue or holds a capability operation while `HubRuntime` still attaches/writes/drains a local session through `DefaultBotsterEngine`.
- Plugin unload/reload returns or records capability cleanup evidence in addition to core worker cleanup.
- Runtime path proof identifies the production entrypoint, expected to be `HubRuntime` capability methods plus lifecycle cleanup hooks.
- Static scan across README, source, tests, and docs returns no PII/source-tree data placement.
- Static scan: `rg -n "CapabilityRuntimeRequest|PluginCapabilityRuntime|PluginTimerScheduler|FilesystemCapabilityRequest|PluginStoreCapabilityRequest" src tests README.md` shows real hub usage, not only references in this plan.

## Vault Gaps Worth Capturing

- Capture candidate after implementation if proven: "botster-hub local capability runtimes must be owned by `HubRuntime` and cleanup must be linked to plugin reload/unload."
- Capture candidate after implementation if the store shape settles: "dogfood plugin JSON store paths live under hub data `plugin-data/<plugin>/`, not plugin source trees, with revision/quota behavior matching core store contracts."
- Capture candidate after implementation if HTTP/WebSocket stubs establish a reusable rule: "local capability runtime stubs may prove bounded contract behavior without product cloud/WebRTC adapters."
- No capture is needed now for the broad core/hub boundary or plugin-data source separation; existing vault notes already cover those.

## Checklist Evidence

- Vault/project conventions loaded: listed in Context Loaded.
- Convention conflicts: none. The plan keeps core contracts in `botster-core`, hub policy and concrete local adapters in `botster-hub`, avoids product cloud/WebRTC behavior, keeps mutable plugin data out of source trees, and requires runtime-path proof.
- Verification evidence planned: commands and targeted assertions listed in Acceptance Checks / Tests.
- Durable knowledge capture: no capture at plan time; capture candidates listed above for post-implementation findings.
