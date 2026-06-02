# Reshape Hub Scaffold Around First-Party Host Profile

Ticket: `ticket_1780376607_554916`

## Context Loaded

- Project Pipelines context loaded for run `run_1780436384_567768`, current step `botster_plan`, gate `botster_plan_gate`, ticket `Reshape botster-hub scaffold around a first-party host profile boundary`, closed dependencies, artifacts, findings, questions, answers, and recent events.
- Required playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Botster planning overlay notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
- Ticket-specific architecture notes loaded:
  - [[botster hub is a first party host profile over core]]
  - [[botster packages should enforce core hub cli plugin provider boundaries]]
- Required self/context notes loaded:
  - `self/identity.md`
  - `self/goals.md`
- Prior plan artifact inspected:
  - `docs/plans/embed-default-botster-engine-hub-runtime.md`
- Repo context inspected:
  - `README.md`
  - `Cargo.toml`
  - `src/lib.rs`
  - `src/main.rs`
  - `src/runtime.rs`
  - `src/config.rs`
  - `src/core.rs`
  - `src/auth.rs`
  - `src/persistence.rs`
  - `src/packages.rs`
  - `src/providers.rs`
  - `src/adapters/mod.rs`
  - `src/adapters/clients.rs`
  - `src/adapters/cloud.rs`
  - `src/adapters/signaling.rs`
  - `src/adapters/api.rs`
  - `tests/hub_runtime_test.rs`

## Scope

In scope:

- Reshape the public scaffold vocabulary so `botster-hub` reads as a first-party host profile over `botster-core`, not just a generic product host.
- Add a minimal compile-checked profile manifest/metadata surface that names:
  - the profile identity,
  - first-party/trusted status,
  - embedded `botster-core` role,
  - profile-owned policy areas,
  - provider/package capability boundaries.
- Wire that profile metadata into the public facade and binary smoke path so the actual executable path reports the host-profile boundary, not only docs.
- Keep the existing `HubRuntime` path over `DefaultBotsterEngine`; this ticket should preserve that runtime proof rather than replace it.
- Reframe existing policy seam modules around profile-owned policy where names/docs currently say only "hub-owned" or "product host".
- Update `README.md` and this plan artifact so review can verify the crate layout and public story against the host-profile ADR.
- Add or adjust focused tests proving the profile metadata, README-aligned roles, and binary/library public path use the new host-profile vocabulary.
- Keep committed docs/tests free of PII and local absolute user paths.

Non-scope:

- No cloud, WebRTC, marketplace, provider process, Rails, ActionCable, browser shell, OAuth/device-code, database, package installer, or client transport implementation.
- No new plugin runtime, Lua worker, MCP, TUI, React, or Project Pipelines UI behavior.
- No broad physical crate split or package manager implementation.
- No speculative multi-profile framework. This ticket is for the first-party `botster-hub` scaffold, not a generic profile marketplace.
- No replacement of `DefaultBotsterEngine` or reimplementation of PTY/data-plane behavior in the hub.
- No dependency changes unless the locked `botster-core` profile/command primitives require a small compile fix.

Botster layers touched:

- Rust hub crate: primary.
- Rust `botster-core`: consumed through existing blessed public API; not changed.
- CLI/binary smoke path: thin entrypoint wording and profile construction proof.
- Docs: README and plan artifact.
- Tests: Rust unit/integration tests for compile-checked public structure.

Pipeline gates and artifacts:

- Plan gate evidence should point at this file and summarize context, scope, assumptions, risks, and acceptance checks.
- Implementation should produce a committed diff before review.
- Review/verify should require exact runtime-path proof: binary or exported public API must use the profile metadata, not only expose unused structs.

## Assumptions And Unknowns

Assumptions:

- The closed dependency "Define hub-as-host-profile architecture over botster-core" means the ADR direction is accepted; no human question is needed to choose between "hub as thick wrapper" and "hub as profile".
- The closed dependency "Add minimal core host-profile contract if architecture requires it" means any `botster-core` blessed primitive already available in `Cargo.lock` should be preferred, but this ticket can still define hub-side metadata if core has no stronger contract yet.
- The existing `HubRuntime` runtime skeleton is a dependency output and should remain intact.
- This is intentionally scaffold-level work, but the scaffold must be executable and compile-checked.
- The smallest coherent change is a new profile metadata module or equivalent public type plus README/test updates, not a full module tree rewrite.
- Use cold-turkey vocabulary where touched: prefer "host profile" over parallel "product host" wording when describing the public boundary.

Unknowns for the Implementer to resolve by inspecting the locked `botster-core` API:

- Whether `botster-core` exports a first-party profile/manifest primitive that should be used directly.
- Whether the cleanest module name is `src/profile.rs`, `src/manifest.rs`, or a small addition to `src/lib.rs`. Prefer a dedicated module if it keeps `lib.rs` as a facade.
- Whether `ArchitectureSummary` should be replaced by or wrap the profile manifest. Prefer one coherent public surface over duplicated summary and manifest types.
- Whether README wording should rename "product host" everywhere or retain it only as an explanatory phrase under the first-party host-profile frame.

No blocking human question is required unless core exposes two incompatible profile primitives or the Implementer would need to waive the "uses botster-core blessed command/profile primitives where available" acceptance criterion.

## Affected Surfaces / Files

Expected changes:

- `src/profile.rs` or equivalent
  - Define minimal first-party host profile metadata/manifest.
  - Keep it static and compile-checked; no loading, parsing, package install, or marketplace behavior.
- `src/lib.rs`
  - Export profile metadata.
  - Update `architecture_summary()` or replace it with a profile-backed summary so public roles derive from one source.
- `src/main.rs`
  - Construct/use the same public profile metadata in the smoke path.
  - Preserve thin binary behavior: config build, runtime construction, profile summary output.
- `src/config.rs`
  - Light documentation updates only, unless a profile primitive needs the policy area vocabulary.
- `src/core.rs`
  - Light documentation update to call `botster-core` the reusable mechanism layer consumed by the first-party profile.
- `src/auth.rs`, `src/persistence.rs`, `src/packages.rs`, `src/providers.rs`, `src/adapters/*`
  - Light documentation/vocabulary updates only where needed to make the scaffold read as profile-owned policy.
- `README.md`
  - Reframe the crate as a first-party trusted host profile/plugin-provider policy scaffold over `botster-core`.
  - Add or update the crate layout table to include the profile manifest/metadata entry.
  - Keep scaffold exclusions explicit.
- `tests/*` and/or `src/lib.rs` unit tests
  - Assert the profile identity, trusted first-party role, core dependency role, policy areas, provider capabilities, and README-aligned role labels.
  - Preserve `tests/hub_runtime_test.rs` as the runtime proof.

Not expected:

- `Cargo.toml` / `Cargo.lock`, unless core profile primitives require a dependency feature already intended by the ticket.
- New provider/client/cloud/Rails/Lua/SPA files.
- Any file containing local absolute paths or user-specific data.

## Risks

- Duplicate public surfaces: adding a manifest beside `ArchitectureSummary` could create two divergent sources of truth. Prefer one profile-backed summary.
- Overbuilding: a generic profile framework, marketplace manifest parser, capability grant engine, or provider lifecycle system would exceed the ticket.
- Underwiring: adding structs that no production/library entrypoint uses would fail the "actual runtime or user path changed" rule.
- Core primitive miss: if `botster-core` already exports a blessed profile/command primitive, ignoring it would violate acceptance.
- Vocabulary drift: retaining "product host" as the dominant README/API phrase would keep the scaffold from reading as first-party host profile.
- Runtime regression: renaming/reframing must not break the existing `HubRuntime` test path over `DefaultBotsterEngine`.
- Dependency drift: `botster-core` is git-main in `Cargo.toml`; avoid unnecessary lockfile changes.
- PII leakage: docs and test output must not include the local project target path from the ticket prompt.

## Acceptance Checks / Tests

Required commands after implementation:

- `cargo fmt`
- `cargo test`
- `cargo test --test hub_runtime_test`
- `./test.sh`
- `cargo clippy --all-targets --all-features -- -D warnings`

Targeted acceptance assertions:

- Public crate API exposes a first-party host profile metadata/manifest surface.
- The profile metadata says `botster-hub` is a trusted first-party profile over `botster-core`.
- Policy areas include auth, config, persistence, providers/packages, transports/adapters, admission/capabilities, lifecycle, and audit or equivalent minimal vocabulary.
- Provider capabilities remain declared as profile-governed contracts, not implemented behavior.
- `src/main.rs` or another actual entrypoint uses the profile metadata in the executable smoke path.
- `HubRuntime` still constructs and uses `botster_core::DefaultBotsterEngine`.
- README crate layout includes the profile manifest/metadata and preserves explicit scaffold-only exclusions.
- Tests fail if the profile role labels/capabilities drift away from README-aligned public structure.
- Branch diff contains no full cloud/WebRTC/marketplace/provider/Rails/client transport implementation and no PII.

Runtime/user path proof:

- The runtime proof is intentionally scaffold-level: `botster_hub::HubRuntime` remains the executable local-runtime path, and `src/main.rs` should report metadata from the new first-party host profile surface.
- Review should reject evidence that only docs changed. At least one compile-checked public API and the binary smoke path must use the new host-profile boundary.

## Vault Gaps Worth Capturing

No immediate durable vault gap blocks implementation. Existing notes already cover the main constraints:

- [[botster hub is a first party host profile over core]]
- [[botster packages should enforce core hub cli plugin provider boundaries]]
- [[botster engine command surface uses botsterengine as facade]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]

Possible capture after implementation:

- If `botster-core` lacks a reusable host-profile manifest primitive and `botster-hub` defines its own minimal metadata, capture the resulting boundary decision so future agents know whether the profile manifest belongs in core, hub, or package metadata.
