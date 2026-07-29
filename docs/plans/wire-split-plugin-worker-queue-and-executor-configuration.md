# Wire Split Plugin Worker Queue And Executor Configuration

Ticket: `ticket_1785200644_970622`

## Target And Context Loaded

- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Authoritative target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through the Hub spawn-target registry rather than inferred from the process directory.
- Pipeline context: run `run_1785293836_788876`, returned Plan step `botster_stack_plan`, current run step `run_step_1785295769_932133`, gate `botster_stack_plan_gate`, first Plan Review `review_1785294815_907277`, and second Plan Review `review_1785295699_721637`. The first review's five findings remain resolved; the second review's durable-state and documentation-scope findings are incorporated below.
- Repository playbook: [[botster-hub-playbook]].
- Role and surface playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
  - [[botster-runtime-reviewer-playbook]] for the Rust plugin-worker/runtime surface
  - [[botster-hub-client-playbook]] because the chosen diagnostics seam extends the externally owned daemon response DTO
- Required Botster maps and planning notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
  - [[botster pipeline needs continuous product owner between agent steps]]
  - [[plan agents must author vault context as wikilinks not home paths]]
  - [[vault example paths are not repository placement conventions]]
- Repository-charter atomic notes loaded:
  - [[botster hub is a first party host profile over core]]
  - [[botster hub gravity must be watched before it becomes the new monolith]]
  - [[botster data plane bypasses the hub through session and client actors]]
  - [[botster local client api lives over hubruntime not raw core routers]]
  - [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
  - [[may supervise permits the hub to supervise the package entrypoint]]
  - [[hub supervision admission changes require exact live hub launch proof]]
  - [[live hub proof records distinct hub and locked core binary provenance]]
  - [[webrtc bootstrap origin must be requested after the package server binds]]
- Hub-client and published-artifact notes loaded after Plan Review identified the external DTO seam:
  - [[botster hub client crate is the external client boundary]]
  - [[botster hub client compatibility descriptors belong in client crate]]
  - [[adding a hub client feature constant is a three site change]]
  - [[generated typescript dtos must encode serde field optionality]]
  - [[daemon event shape changes bump conformance fixture revision not protocol version]]
  - [[closed dependency tickets signal merged source not a consumable release]]
  - [[hub test support npm releases need external consumer smoke]]
  - [[conformance fixture revisions must be unique per published content]]
- Workflow note loaded after the checklist call timed out:
  - [[project pipelines checklist worker timeouts require artifact evidence fallback]]
- Project Pipelines context loaded: ticket, run, current step, gate, dependency, project tickets, artifacts, reviews, findings, questions, answers, and recent events. Plan Review returned five concrete findings: missing client charter, stale published package mirror, weak live-plugin proof, overstated stale-key rejection, and an omitted existing diagnostics channel. All five constrain this revision.
- Repository context inspected:
  - `README.md`, including the one production path
  - `Cargo.toml` and `Cargo.lock`
  - `src/config.rs`
  - `src/lifecycle.rs`
  - `src/runtime.rs`
  - `src/client_api.rs`, including the existing `PluginLifecycleStatus` route
  - `src/daemon.rs`
  - `src/daemon_transport.rs`
  - `src/persistence.rs`, including the root state version, load/validation order, embedded `LocalRuntimeSettings.core_engine`, and restart tests
  - `src/lib.rs`
  - `crates/botster-hub-client/src/lib.rs`
  - `crates/botster-hub-client/src/typescript.rs`
  - `crates/botster-hub-client/generated/daemon-protocol.ts`
  - `crates/botster-hub-test-support/src/lib.rs`
  - `crates/botster-hub-test-support/examples/node_package_assets.rs`
  - `packages/hub-test-support/package.json`, `metadata.json`, `daemon-protocol.ts`, `test.mjs`, and `scripts/sync-assets.mjs`
  - `tests/hub_plugin_lifecycle_test.rs`
  - `tests/hub_daemon_lifecycle_test.rs`
  - current plans and git history under `docs/plans/`
  - `docs/adr/durable-hub-state-v1.md` as the historical contract for versioned state rejection
  - the `Cargo.lock`-pinned `botster-core` source at `e36435f2cb583c344d6f6ba2d62c39da324c7a64`
- [[project-pipelines-playbook]] was not loaded: this ticket does not change Project Pipelines package/plugin code or workflow policy. Project Pipelines is only the delivery mechanism for this repository-scoped Hub change.

## Current Runtime Gap

`CoreEngineOptions` still exposes `plugin_worker_capacity`, even though the locked Core contract now names the independent knobs `per_plugin_queue_capacity` and `per_plugin_executor_concurrency`. More importantly, both `HubRuntime::new` and `HubRuntime::from_validated_state` construct `HubPluginLifecycle::new()`, which always creates `PluginWorkerEngine::new()` and silently discards Hub's configured plugin-worker value. The Hub therefore serializes and validates a setting that the production plugin lifecycle does not consume.

Core already provides the required policy-free mechanism and observability:

- `PluginWorkerEngineConfig::per_plugin_queue_capacity`
- `PluginWorkerEngineConfig::per_plugin_executor_concurrency`
- defaults of the public plugin-worker queue source and executor width `2`
- `PluginWorkerEngine::debug_snapshot()` with configured queue/executor values and aggregate/per-plugin live, queued, and in-flight counters

The Hub must consume those APIs; this run must not redesign Core execution.

## Scope

In scope:

- Cold-turkey rename `CoreEngineOptions::plugin_worker_capacity` to `plugin_worker_queue_capacity`.
- Add `CoreEngineOptions::plugin_worker_executor_concurrency`.
- Derive both Hub defaults from `PluginWorkerEngineConfig::default()` so Hub tracks the locked Core contract without duplicating numeric policy.
- Validate both fields with the existing `validate_positive_usize` helper and exact field paths.
- Apply `#[serde(deny_unknown_fields)]` to `CoreEngineOptions` so a stale `plugin_worker_capacity` key fails even when the two replacement fields are also present; this removes ambiguity instead of silently ignoring the old knob.
- Treat the persisted `LocalRuntimeSettings.core_engine` shape change as Hub state schema v2. Preflight the root `schema_version` before deserializing the complete state so an existing v1 file returns typed `UnsupportedVersion(1)` instead of the misleading `Corrupt` error.
- Convert the two Hub values into one `PluginWorkerEngineConfig` and use it when constructing `HubPluginLifecycle` in both fresh and durable-state `HubRuntime` constructors.
- Make the configured lifecycle constructor the only construction path and add a read-only debug snapshot path through `HubPluginLifecycle` and `HubRuntime`.
- Extend the existing `HubClientRequest::PluginLifecycleStatus` → `DaemonRequest::PluginLifecycleStatus` diagnostics channel with aggregate, sanitized plugin-worker counters. Do not create a parallel `DaemonStatus` resource surface or expose runtime objects/payloads.
- Treat the public daemon/client `PluginLifecycleStatus` request as the production diagnostics consumer. Hub has no CLI command for this request, so do not claim or add a private formatter-only consumer.
- Add an optional `DaemonResponse.plugin_worker_counters` projection with serde-accurate optional TypeScript output, populated only for the plugin-lifecycle response.
- Regenerate the checked TypeScript and `packages/hub-test-support` copy, refresh its hash metadata, and bump the prepared npm package to `0.1.15`. Registry inspection confirmed `0.1.14` is already published and contains the old protocol artifact, so it cannot be reused.
- Update Hub config/default/serde tests, runtime wiring tests, client DTO/generated TypeScript tests, published-package asset tests, and production-shaped plugin lifecycle tests.
- Keep this reviewable plan in the repository's established `docs/plans/` location.

Botster layers touched:

- Rust Hub configuration and startup composition.
- Rust Hub plugin lifecycle adapter over Core.
- Existing Hub plugin-lifecycle diagnostics route.
- Hub-client response DTO and generated TypeScript mirror, only for sanitized aggregate debug counters.
- Hub-test-support Rust/Node published artifact preparation and drift tests.
- Rust and Node tests, including a real subprocess-spawned Hub through shared test support.

## Non-Scope

- No changes to `botster-core`; prerequisite ticket `ticket_1785199689_140456` already owns and delivered the reusable execution mechanism.
- No compatibility field, serde alias, deprecated accessor, version suffix, or dual config path for `plugin_worker_capacity`.
- No automatic v1-to-v2 state migration and no deserialization fallback for the old persisted field. Operators must deliberately reinitialize state rather than silently retaining ambiguous worker policy.
- No new executor, queue, thread pool, scheduler, backpressure mechanism, or Hub-owned worker abstraction.
- No change to Core's queue or executor defaults.
- No plugin package behavior, Project Pipelines package code, Lua handler semantics, capability policy, PTY/session data plane, Web/TUI behavior, Rails code, or package supervision policy.
- No per-plugin names, handler data, payloads, paths, or other sensitive runtime details in public daemon diagnostics.
- No four-package resource campaign in this ticket; `ticket_1785199716_875648` owns that downstream production-shaped proof.
- No new daemon request, feature constant, protocol framing, or compatibility requirement.
- No new Hub CLI command or formatter-only plugin-worker counter surface.
- No npm publication in this ticket. `0.1.15` is prepared and pack-tested only; registry publication and clean external install proof remain a later explicit release action.
- No unrelated cleanup or broad status/DTO refactor.

## Ownership Boundaries And Dependencies

- Hub owns host-profile configuration names, validation, defaults derived from Core, startup composition, and sanitized product diagnostics.
- Hub also owns its durable state schema. Because `CoreEngineOptions` is embedded under `runtime_settings`, this ticket must advance the state schema and preserve an operator-meaningful unsupported-version boundary.
- Core owns `PluginWorkerEngineConfig`, queue/backpressure semantics, executor implementation, cleanup/join behavior, and authoritative debug counters.
- `botster-hub-client` owns the external `DaemonResponse` projection and generated TypeScript. The Hub server continues to own routing through `HubClientApi` and the existing `PluginLifecycleStatus` request.
- `botster-hub-test-support` owns the source-derived downstream artifact. Its checked `daemon-protocol.ts`, SHA-256 metadata, Node tests, and prepared package version must move with the client DTO.
- The closed cross-repository dependency is Core ticket `ticket_1785199689_140456` on target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` (`botster-core`). The Hub lockfile already pins its merged revision `e36435f2cb583c344d6f6ba2d62c39da324c7a64`; no dependency refresh is planned.
- Downstream integration ticket `ticket_1785199716_875648` already has an open dependency on this wiring ticket. It must remain separate and use the product counters delivered here plus OS/process evidence.
- That downstream ticket is source-coupled to this Hub repository, so it does not require an npm release. No current botster-web ticket consumes these counters. A future external consumer must register a Hub release ticket and verify the installed `@trybotster/hub-test-support@0.1.15` artifact before treating the counters as available.
- This run is bound to the managed Hub worktree and explicit Hub target id above; it must not edit the sibling Core checkout.

## Assumptions And Unknowns

Assumptions:

- “Match Core” means Hub field names `plugin_worker_queue_capacity` and `plugin_worker_executor_concurrency`, mapped directly to Core's `per_plugin_*` fields.
- Cold-turkey requires explicit rejection of stale keys, not incidental failure only when replacement fields are missing. `CoreEngineOptions` will deny unknown fields, and tests will reject both legacy-only JSON and mixed legacy-plus-new JSON.
- Existing state schema v1 is intentionally unsupported after this breaking persisted shape change. A minimal version-envelope read must distinguish syntactically corrupt JSON from a valid v1 document before full v2 deserialization; no v1 migration is implied.
- The additive `DaemonResponse.plugin_worker_counters` field is defaulted and omitted when absent; generated TypeScript must therefore emit it as optional. That client compatibility treatment does not apply to the cold-turkey Hub configuration input.
- Aggregate Core snapshot fields are sufficient for the downstream resource proof. Per-plugin snapshot rows are not required in the public daemon DTO and would expose unnecessary package identity.
- A dedicated sanitized `DaemonPluginWorkerCounters`/equivalent response object is preferable to mixing executor gauges into transport `DaemonLifecycleCounters`.
- This additive optional response projection changes neither socket framing nor request semantics and changes no conformance fixture JSON. Therefore `PROTOCOL_VERSION` remains `4` and `CONFORMANCE_FIXTURE_REVISION` remains `22`; the prepared npm package version alone moves to `0.1.15`.
- The public npm registry was inspected during replanning: versions through `0.1.14` exist, and the packed `0.1.14` artifact reports protocol `4`, conformance revision `22`, and the old daemon-protocol SHA-256 `83b4fa0f...`. Version `0.1.15` is the next non-colliding preparation coordinate.
- No feature constant is added: older daemons may omit the optional counters, while new clients remain able to deserialize their existing plugin-lifecycle response.

Unknowns for implementation:

- Whether a single helper on `CoreEngineOptions` should produce `PluginWorkerEngineConfig`, or whether conversion belongs in `runtime.rs`. Prefer one conversion site without a speculative general configuration abstraction.
- Exact internal report type names may follow existing `HubClientPluginLifecycle` conventions. The external seam is settled: counters travel only with `PluginLifecycleStatus`, not `DaemonStatus`.
- The non-default executor-width proof uses the existing `FakeRuntime` path in `tests/hub_plugin_lifecycle_test.rs`; the live daemon proof uses the existing `write_local_plugin_package` helper or the stronger published plugin-contract-matrix fixture through `IsolatedHubBuilder`.
- Current v2 state must survive a stop/start cycle with the configured queue/executor values intact. A pre-change v1 state file must stop startup with the exact typed unsupported-version error before the renamed nested fields are decoded.

No human question is blocking. Ask before implementation only if the locked Core API differs from the inspected contract or satisfying the ticket would require retaining the old field.

## Affected Surfaces And Files

Expected changes:

- `src/config.rs`
  - Rename the queue field, add executor concurrency, deny unknown Core-engine fields, derive both defaults from one Core default config, and validate both positive values.
  - Strengthen serde/default tests to assert the exact new keys and values, absence of the old key, successful round trips, and rejection of legacy-only, mixed legacy-plus-new, and zero-valued inputs.
- `src/lifecycle.rs`
  - Add a constructor accepting `PluginWorkerEngineConfig` and remove the zero-caller default/unconfigured constructors.
  - Delegate to `PluginWorkerEngine::with_config`.
  - Expose the Core public debug snapshot read-only for Hub runtime diagnostics and tests.
- `src/runtime.rs`
  - Build the Core plugin-worker config from `config.core_engine` before moving config.
  - Use it in both `HubRuntime::new` and `HubRuntime::from_validated_state`.
  - Expose a narrow read-only plugin-worker snapshot method.
- `src/persistence.rs`
  - Advance `HUB_STATE_SCHEMA_VERSION` from `1` to `2` because the persisted `runtime_settings.core_engine` shape changes.
  - Read and validate a minimal root schema-version envelope before deserializing `HubState`, preserving `Corrupt` for malformed JSON while returning `HubStateError::UnsupportedVersion(1)` for a valid pre-change state file.
  - Update state model comments and focused persistence tests for current v2 creation/reopen, v1 rejection, malformed JSON, and unknown future versions.
- `src/client_api.rs`
  - Extend the existing Hub-local `PluginLifecycleStatus` result with the sanitized aggregate worker snapshot while keeping routing over `HubRuntime`.
- `src/daemon_transport.rs`
  - Map that report through the existing `DaemonRequest::PluginLifecycleStatus` / `plugin_lifecycle_response` seam.
  - Populate an optional `DaemonResponse.plugin_worker_counters`; do not change `DaemonStatus`.
- `crates/botster-hub-client/src/lib.rs`
  - Add `DaemonPluginWorkerCounters` and the backward-compatible optional response field.
  - Add serde omission, deserialization-default, sanitization, and generated-protocol assertions.
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Regenerate/check the additive DTO mirror through repository-owned generation code, including `plugin_worker_counters?: ... | null`.
- `crates/botster-hub-test-support/src/lib.rs`
  - Preserve source equality between the Rust-generated artifact, checked client artifact, and Node package copy; add a token assertion for the new DTO.
- `packages/hub-test-support/package.json`
  - Bump the prepared package version to `0.1.15`; do not reuse already-published `0.1.14`.
- `packages/hub-test-support/daemon-protocol.ts`
- `packages/hub-test-support/metadata.json`
  - Regenerate the protocol copy and refresh `daemon_protocol.sha256` while retaining protocol version `4` and conformance revision `22`.
- `packages/hub-test-support/test.mjs`
  - Update the prepared package-version assertion and require the new DTO/optional field tokens.
- `packages/hub-test-support/README.md`
  - Preserve the repository's pre-existing `0.1.13` install coordinate and add only the prepared `0.1.15` note. Moving every published-coordinate reference to `0.1.14` is separate repo-wide cleanup.
- `tests/hub_plugin_lifecycle_test.rs`
  - Use the existing `FakeRuntime` fixture to prove a non-default Hub queue/executor configuration reaches the real Core worker engine, materializes the expected executor count after plugin load, and returns live counts to the pre-load baseline after unload.
- `tests/hub_daemon_lifecycle_test.rs`
  - Extend `daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts` or a focused sibling using `IsolatedHubBuilder` and the existing published fixture. Request `PluginLifecycleStatus` after the real Lua plugin is enabled and require non-zero `live_plugin_executors` and `live_executor_workers`, not configured values alone.
- `docs/plans/wire-split-plugin-worker-queue-and-executor-configuration.md`
  - This Plan artifact.

Likely unchanged:

- `Cargo.toml` and `Cargo.lock`.
- Core, package/plugin fixture behavior, capability runtime, session worker, terminal/client data plane, and package supervision modules.
- `src/main.rs`; no production CLI command issues `PluginLifecycleStatus`, so its private response formatter is not a supported counter consumer.
- `DaemonStatus`, `DaemonLifecycleCounters`, compatibility feature lists, support matrices, and conformance fixtures.
- Root `README.md` and `docs/client-protocol.md`; their pre-existing `0.1.13` registry coordinates are adjacent cleanup and remain outside this surgical ticket.

## Implementation Plan

1. Replace the Hub configuration contract cold-turkey.
   - Rename the queue field and add executor concurrency.
   - Read one `PluginWorkerEngineConfig::default()` and copy its two fields into Hub defaults.
   - Validate both with `validate_positive_usize` and precise `core_engine.*` field labels.
   - Add `#[serde(deny_unknown_fields)]` to `CoreEngineOptions`.
   - Update all struct literals and serde assertions; reject legacy-only and mixed legacy-plus-new JSON so an ignored old key cannot masquerade as compatibility.

2. Wire configuration into the production plugin lifecycle.
   - Add `HubPluginLifecycle::with_config`.
   - Delete the unconfigured `HubPluginLifecycle::new()` and `Default` construction paths; cold-turkey wiring leaves one explicit constructor.
   - Convert `CoreEngineOptions` to `PluginWorkerEngineConfig` once per `HubRuntime` construction.
   - Use the configured lifecycle in both fresh runtime and durable-state load paths. Do not leave `HubPluginLifecycle::new()` in either production constructor.

3. Make the persisted breaking change explicit at the state boundary.
   - Advance the current Hub state schema to v2.
   - Parse only the root `schema_version` first, reject v1 through `HubStateError::UnsupportedVersion(1)`, then deserialize the full v2 state.
   - Keep malformed JSON on `HubStateStoreError::Corrupt`; do not add a v1 migration, old-field alias, or fallback.
   - Prove a newly written v2 state reopens and supports a daemon stop/start with the configured worker policy, while a valid v1 fixture fails with the exact operator-facing unsupported-version error.

4. Consume Core's authoritative debug snapshot.
   - Delegate a read-only snapshot through lifecycle and runtime.
   - Extend the existing `HubClientRequest::PluginLifecycleStatus` report, then map it through `DaemonRequest::PluginLifecycleStatus`.
   - Project only aggregate configured/live/queued/in-flight counters into `DaemonPluginWorkerCounters` on the plugin-lifecycle `DaemonResponse`; leave `DaemonStatus` unchanged.
   - Populate it from the same runtime instance used to load and invoke production plugins, not from `CoreEngineOptions` alone.
   - Keep the public response field additive/defaulted/omittable and regenerate the TypeScript protocol with matching optionality.

5. Synchronize the downstream artifact without overclaiming compatibility or publication.
   - Run `npm run sync` in `packages/hub-test-support` (which invokes `cargo run --quiet -p botster-hub-test-support --example node_package_assets -- <temporary-output-dir>`).
   - Bump the prepared package to `0.1.15`, refresh `metadata.json` including `daemon_protocol.sha256`, and update Node token/version assertions.
   - Keep protocol version `4`, conformance revision `22`, feature lists, support matrices, and fixture JSON unchanged because the field is optional and no request/framing/fixture contract changes.
   - Run package check/test/pack gates. Do not publish; record that the repository retains its pre-existing `0.1.13` install coordinate while the registry has `0.1.14` and the branch prepares `0.1.15`.

6. Prove configuration and runtime behavior.
   - Unit-test exact defaults, serde names, legacy rejection, and both zero-value failures.
   - In `tests/hub_plugin_lifecycle_test.rs`, construct Hub runtime with distinct non-default queue/executor values, load through the existing `FakeRuntime` bundle, and assert the Core snapshot reports configured values plus the materialized executor width.
   - In the existing subprocess test-support path, start `IsolatedHub`, enable the published plugin-contract-matrix fixture (or use `write_local_plugin_package` in the focused live daemon fixture), request `DaemonRequest::PluginLifecycleStatus`, and require non-zero live plugin/executor counters.
   - Prove unload/stop returns live executor counts to the expected baseline where deterministic, leaving the broader four-package/process campaign to its dependent integration ticket.

## Risks

- Unwired-option risk: changing config names and tests without replacing both lifecycle constructors would leave production behavior unchanged.
- False-proof risk: copying `CoreEngineOptions` into diagnostics would prove serialization, not that Core consumed the values. Runtime assertions must read `PluginWorkerEngine::debug_snapshot()`.
- Compatibility drift risk: serde aliases/defaults or permissive unknown-field handling on Hub configuration would preserve the ambiguous old path and violate the cold-turkey requirement.
- Durable-state restart risk: every pre-change state file embeds the old field. Without a v2 bump and version preflight, startup reports a valid old schema as corrupt before `validate_version` can run.
- State-version regression risk: changing the nested persisted shape without proving current-state reopen/restart could wire fresh startup while breaking the `HubRuntime::from_validated_state` production path.
- Diagnostics-duplication risk: placing counters on `DaemonStatus` would create a second plugin diagnostics surface beside `PluginLifecycleStatus`.
- Client-contract risk: adding a response field without serde omission/default and optional generated TypeScript would unnecessarily break older clients.
- Published-artifact drift risk: the checked Rust-generated DTO can be correct while `packages/hub-test-support/daemon-protocol.ts` and its SHA-256 remain stale unless explicit sync/package gates run.
- Compatibility-signal risk: bumping protocol/conformance for an optional non-fixture field would overstate the change; reusing published `0.1.14` would assign different bytes to one immutable coordinate.
- Information-leak risk: forwarding per-plugin rows or debug formatting could reveal package identities or future internal fields. Project only explicit aggregate counters.
- Lifecycle race risk: asserting worker retirement immediately after unload could create flaky tests. Use existing deterministic cleanup/join behavior or bounded state polling, not arbitrary sleeps.
- Duplicate-construction risk: fresh and persisted runtime constructors can diverge if only one is updated.
- Scope-creep risk: the adjacent resource project includes connections, subscriptions, processes, zombies, and CPU. Those remain in their own tickets.

## Acceptance Checks And Tests

Repository gates:

- `cargo fmt --check`
- `cargo check --locked`
- `./test.sh --lib config`
- focused `./test.sh --lib persistence`
- `./test.sh -p botster-hub-client`
- `./test.sh --test hub_plugin_lifecycle_test`
- focused `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
- `./test.sh -p botster-hub-test-support daemon_protocol_typescript_artifact_matches_node_package_copy`
- `npm run check` in `packages/hub-test-support`
- `npm test` in `packages/hub-test-support`
- `npm pack --dry-run --json` in `packages/hub-test-support`
- `./test.sh`
- `cargo clippy --locked --all-targets --all-features -- -D warnings`

Static acceptance checks:

- `rg -n "plugin_worker_capacity" src tests crates README.md`
  - No production/config compatibility occurrence remains. The only allowed current-repository occurrences are explicit negative fixtures proving legacy config and v1 state rejection. Historical plan text elsewhere under `docs/plans/` is not rewritten unless touched for another reason.
- `rg -n "plugin_worker_queue_capacity|plugin_worker_executor_concurrency" src tests crates`
  - Both fields appear in defaults, validation, Core conversion, serde assertions, and production proof.
- Confirm `HubPluginLifecycle::new()` and its `Default` implementation are absent and both `HubRuntime` constructors use the explicit configured path.
- Inspect `FileHubStateStore::load_or_initialize` and confirm root version validation occurs before complete `HubState` deserialization.
- Assert `DaemonStatus` and `DaemonLifecycleCounters` did not gain plugin-worker fields; the counters belong only to the existing plugin-lifecycle response.
- Run `npm run sync` and then `npm run check` in `packages/hub-test-support` so the committed package assets are reproducible from the Rust source emitter.
- Check generated TypeScript drift using the repository's existing generator and Rust source-equality tests; do not hand-maintain a mismatched DTO.
- Assert `packages/hub-test-support/metadata.json` carries package `0.1.15`, refreshed `daemon_protocol.sha256`, protocol `4`, and conformance revision `22`.

Behavioral acceptance:

- Hub default queue capacity and executor concurrency equal the locked Core defaults.
- Zero queue capacity fails with `core_engine.plugin_worker_queue_capacity`.
- Zero executor concurrency fails with `core_engine.plugin_worker_executor_concurrency`.
- Serialized Hub options contain both new keys and no `plugin_worker_capacity`.
- Legacy-only and mixed legacy-plus-new serialized inputs are rejected by `CoreEngineOptions` unknown-field denial; there is no alias, ignored stale key, or dual path.
- Fresh state is written as schema v2; valid schema v1 state containing `runtime_settings.core_engine.plugin_worker_capacity` fails as `HubStateStoreError::State(HubStateError::UnsupportedVersion(1))`, not `Corrupt`.
- Malformed JSON remains `HubStateStoreError::Corrupt`, and unsupported future versions remain typed unsupported-version errors.
- Fresh and durable-state Hub runtime construction both pass both values into Core.
- A daemon started with non-default queue/executor values can stop and restart against its v2 state, then report the same configured values through `PluginLifecycleStatus`.
- The existing `FakeRuntime` lifecycle fixture reports configured queue capacity, configured executor concurrency, one live plugin executor, and the configured number of live executor workers through Core's public snapshot.
- Queue capacity remains independent from live executor count.
- The real subprocess-spawned Hub and published plugin-contract-matrix fixture return sanitized aggregate counters from `PluginLifecycleStatus`, including `live_plugin_executors >= 1` and `live_executor_workers >= 1`; configured-only assertions do not satisfy this proof.
- Reload/unload/shutdown evidence shows workers retire without creating a detached generation in the focused fixture.
- The public daemon/client `PluginLifecycleStatus` request returns sanitized configured/live/queued/in-flight counters through a real subprocess-spawned Hub; no CLI renderer claim is made.
- The optional response field is omitted when absent and generated TypeScript marks it optional.
- `DaemonRequest::Status` remains unchanged; no parallel plugin diagnostics DTO is added there.
- Prepared `@trybotster/hub-test-support@0.1.15` package bytes, metadata hash, Rust source artifact, checked TypeScript, and Node package copy agree.
- Protocol version remains `4`, conformance fixture revision remains `22`, and no feature/support-matrix change occurs.
- Existing plugin lifecycle, config serde, client DTO, generated protocol, shared subprocess harness, package asset, and full repository tests remain green.

Downstream proof:

- This ticket intentionally does not claim the four-package/macOS/Linux resource result.
- `ticket_1785199716_875648` is already dependency-blocked on this ticket and must use the daemon-exposed counters plus OS/process evidence to prove queue capacity no longer maps to OS thread count under the production-shaped workload.
- That same-repository proof can consume merged source and does not wait for npm. External repos cannot claim the counters from the registry until `0.1.15` is separately published and verified from a clean install; no current external consumer or botster-web dependency makes that release a prerequisite here.

## Vault Knowledge Captured

- `[[plugin worker queue capacity and executor concurrency are independent host profile knobs]]` records that waiting capacity and executor width are separate policy inputs and production proof reads Core's live snapshot rather than serialized Hub config.
- `[[durable state version preflight must precede shape deserialization after cold turkey changes]]` records that a breaking embedded shape advances the state schema and rejects old versions before full-shape decoding.
- Captured both proven invariants through the vault inbox/document/connect/verify pipeline as `[[plugin worker queue capacity and executor concurrency are independent host profile knobs]]` and `[[durable state version preflight must precede shape deserialization after cold turkey changes]]`.
- No convention conflict was found. The plan keeps reusable execution in Core, host policy and diagnostics in Hub, uses a cold-turkey rename, and avoids speculative abstractions.

## Project Pipelines Checklist Evidence

- Checklist instructions were loaded before checklist creation.
- `project_pipelines_create_vault_checklist` timed out, then `project_pipelines_list_checklists` confirmed that `checklist_1785294082_170918` persisted. It was adopted without retrying, per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Vault/project notes constraining the work are listed in Context Loaded.
- Convention conflicts: none.
- Baseline verification: `cargo check --locked` passed against Core revision `e36435f2cb583c344d6f6ba2d62c39da324c7a64`.
- Registry/package verification: npm reports `0.1.14` as published; its packed metadata retains protocol `4`, conformance revision `22`, and the pre-change daemon-protocol hash, establishing `0.1.15` as the next immutable preparation coordinate.
- Downstream verification commands and success criteria are listed above.
- Durable capture disposition: both deferred invariants were captured after implementation established their exact runtime and state-boundary contracts.
