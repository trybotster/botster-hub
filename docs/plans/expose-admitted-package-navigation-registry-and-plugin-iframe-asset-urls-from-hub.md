# Expose admitted package navigation registry and plugin iframe asset URLs from hub

## Context loaded

- Pipeline context: `run_1783374681_864878`, step `botster_plan`, ticket `ticket_1783371372_931094`, dependency `ticket_1783371357_714397` closed, target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Gate prompt: attach plan evidence covering context, scope, assumptions/unknowns, affected files, risks, tests, and vault gaps.
- Checklist evidence: attempted `project_pipelines_create_vault_checklist`; Project Pipelines returned `plugin worker invoke timeout`, so this plan and the gate payload carry checklist evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Vault/playbook context: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster hub client crate is the external client boundary]], [[botster package registry persists through hub state json]], [[botster package daemon dto exposes sanitized package rows]], [[botster web dto field names must match authoritative rust serde structs]], [[generated typescript dtos must encode serde field optionality]], [[botster web plugin app routes are stable host routes]], [[plugin surfaces request model state through ui bindings not hub subscribe]], [[plugin asset message handlers run in plugin worker vms]], [[worker-rendered plugin assets remain readable from the hub]], and [[plugin surface handlers must validate against hub locked uinode contract]].
- Repo context: `Cargo.lock` pins `botster-core` to `42538009bc6f6291872c5657bedbe7370f504f8d`. The local cache for that revision has `PackageManifest.surfaces` and no obvious `navigation` manifest field yet; implement must re-check the locked core API after dependency refresh before coding.

## Scope

- Add a hub-owned admitted package navigation registry to the public daemon/client protocol. It should be a typed DTO/request/response surface in `botster-hub-client`, projected by hub policy from installed/admitted package state, not raw manifest parsing by clients.
- Normalize explicit core navigation descriptors when present. Preserve current behavior for packages without explicit navigation by deriving default navigation entries from `kind: app` package surfaces and existing route descriptors.
- Include package identity, item id, label/title, optional icon/category, route target/path, enabled/blocked state, diagnostics, and source surface/entrypoint metadata. Do not make plugin order/priority authoritative for global placement.
- Add hub-managed asset URL/reference support for iframe/custom HTML UiNodes so clients render a scoped asset reference or safe URL, never inline raw HTML in `plugin_surface.body` or `ui_tree_snapshot.body`.
- Extend real hub tests, protocol serde/generated TypeScript tests, and test-support fixture artifacts as needed.
- Update `docs/client-protocol.md` and fixture docs only where the new public client contract needs to be described.

## Non-scope

- No browser/TUI placement policy, pinning, hiding, global ordering, sidebar replacement, route layout, padding, or plugin-owned presentation authority.
- No raw HTML injection, client DOM mutation API, or browser-side manifest parsing.
- No broad package registry refactor, marketplace install-policy changes, unrelated lifecycle action changes, or new workflow primitives.
- No compatibility dual path beyond additive DTO fields/requests required by the daemon protocol; preserve existing route descriptors and app registry behavior.

## Assumptions and unknowns

- Assumption: the closed core dependency ticket provides or soon provides manifest/navigation and iframe/custom HTML contracts; implement must refresh/inspect the locked `botster-core` revision before choosing field names or mapping logic.
- Assumption: the smallest public surface is a new `DaemonRequest::ListPackageNavigation` plus `DaemonResponseKind::PackageNavigation` and `DaemonPackageNavigationEntry` rows, while existing `DaemonPackageRouteDescriptor` stays the route/direct-load contract.
- Assumption: current `DaemonPackageRouteDescriptor` fields can be reused or embedded to prevent route/nav drift; tests should prove navigation registry targets match package route descriptors.
- Assumption: disabled/blocked packages may contribute visible but blocked nav rows with diagnostics, but must not produce usable enabled entries.
- Unknown: the exact core UiNode shape for iframe/custom HTML. If the core contract lacks a typed iframe/asset node in the refreshed dependency, ask a human instead of inventing one in hub.
- Unknown: whether plugin asset reads should be served by parent-hub mirrored descriptors or delegated to the owning worker. Choose the smallest path that proves hub-readable URLs for worker-rendered assets.

## Affected surfaces/files

- Rust client protocol: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Hub API/transport: `src/client_api.rs`, `src/daemon_transport.rs`, `src/runtime.rs`, likely `src/lua_runtime.rs` or lifecycle descriptor plumbing if asset descriptors need to cross worker/parent boundaries.
- Package policy/projection: `src/packages.rs` only if new core manifest fields must be persisted/projected through `PackageRecord`/snapshot state.
- Tests: `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_lua_runtime_test.rs`, and protocol serde/generation tests in `crates/botster-hub-client/src/lib.rs`.
- Fixtures/support: `fixtures/plugins/plugin-contract-matrix/*`, `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/*`, `packages/hub-test-support/fixtures/plugin-contract-matrix/*`, `packages/hub-test-support/daemon-protocol.ts`, `packages/hub-test-support/metadata.json`.
- Docs: `docs/client-protocol.md`, and fixture README if the contract matrix gains explicit nav/iframe cases.

## Risks

- Core drift: planning against stale local core types could duplicate or contradict the dependency ticket. Mitigation: refresh/inspect the locked core API before implementation and map exact serde field names.
- Navigation authority leak: carrying manifest order/priority into global placement would violate the ticket. Mitigation: expose ordering hints only as source metadata if the core requires it, and test that clients are not required to sort by plugin priority.
- Route/nav divergence: separate projections can disagree. Mitigation: derive nav targets from the same helper as `DaemonPackageRouteDescriptor` or assert one-to-one target/path parity.
- Raw HTML leak: plugin iframe support can accidentally pass HTML strings through UiNode payloads. Mitigation: tests must scan the rendered `plugin_surface` response for asset URL/reference fields and absence of inline HTML content.
- Worker asset readability: worker-rendered assets can produce URLs the hub cannot serve. Mitigation: real render plus real hub asset read, not source-only tests.
- Generated protocol staleness: TypeScript mirrors can compile while omitting optionality or new fields. Mitigation: serde stability tests plus generated artifact sync/check.

## Acceptance checks/tests

- `cargo test -p botster-hub-client` or equivalent protocol tests proving new request/response/DTO serde field names and TypeScript generation, including optional skipped fields.
- `./test.sh --test hub_client_api_test` for in-process projection/admission behavior: explicit navigation, default app-surface navigation, disabled/blocked diagnostics, and no plugin-order authority.
- Targeted real daemon tests in `./test.sh --test hub_daemon_lifecycle_test <test-filter>` proving `ListPackageNavigation` returns admitted registry rows and row route targets match `ListPackages`/`ShowPackage` route descriptors.
- Plugin fixture test proving an iframe/custom HTML surface returns a hub-managed asset URL/reference and no raw inline HTML in `plugin_surface.body` or `ui_tree_snapshot.body`.
- Test-support sync/check: `node packages/hub-test-support/scripts/sync-assets.mjs --check`; if implementation updates source fixtures, run sync first and then the package test/smoke already used by this repo.
- Final gate should include `./test.sh` unless runtime cost or unrelated pre-existing failures are documented with exact failing tests and attribution.

## Vault gaps worth capturing

- Capture the final admitted navigation DTO vocabulary once implemented: how it relates to route descriptors, app rows, plugin ordering hints, and blocked diagnostics.
- Capture the settled iframe/custom HTML asset policy if implementation defines the durable rule for asset URL/reference shape and worker-to-hub asset readability.
- Capture any new conformance fixture revision rule if additive navigation/asset DTOs require fixture metadata updates.

