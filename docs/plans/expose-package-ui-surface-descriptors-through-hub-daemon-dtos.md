# Expose Package UI Surface Descriptors Through Hub Daemon DTOs

## Context loaded

- Pipeline context: ticket `ticket_1782259483_374994`, run `run_1782266928_563524`, step `botster_plan`, gate `botster_plan_gate`. Initial Plan Review returned changes required with four findings: blocker on unverified core dependency schema, high on missing concrete serde field names, medium on under-specified render/action validation, and low on checklist fallback evidence.
- Ticket dependencies are closed: "Add package UI surface descriptor metadata to botster-core manifests" and "Generate TypeScript daemon protocol from botster-hub-client".
- Vault/playbooks loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[plan steps need reviewable plan artifacts]], [[stale project pipeline worktrees can miss merged dependency apis]], [[botster package daemon dto exposes sanitized package rows]], [[botster package registry persists through hub state json]], [[botster package records persist trust compatibility and admitted capability lock metadata]], [[botster hub client crate is the external client boundary]], [[botster web dto field names must match authoritative rust serde structs]], and [[generated typescript dtos must encode serde field optionality]].
- Botster skill loaded: `botster-customize-hub`, because this changes hub daemon behavior and public client DTOs.
- Repo context inspected: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `src/packages.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/main.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `examples/project-pipelines/botster-package.json`, `examples/synthetic-plugin/botster-package.json`, `Cargo.toml`, and `Cargo.lock`.
- Dependency verification after Plan Review: remote `botster-core` main is `274fdb981bda883cd8752b6c1100f14313432219`; that revision exposes `PackageManifest.surfaces: Vec<PackageSurfaceDescriptor>` and re-exports `PackageSurfaceDescriptor`, `PackageSurfaceKind`, and `PackageSurfaceOperation`. `Cargo.lock` has been refreshed from `1548c0c...` to `274fdb981bda883cd8752b6c1100f14313432219` for both `botster-core` and `botster-core-daemon`.
- Authoritative core serde fields to project:
  - `PackageManifest.surfaces`
  - `PackageSurfaceDescriptor`: `id`, `kind`, `title`, optional `description`, optional `icon`, optional `order`, optional `category`, `supports`
  - `PackageSurfaceKind`: `app`, `settings`, `dashboard_widget`, `diagnostics`
  - `PackageSurfaceOperation`: `render`, `action`
- Project Pipelines checklist evidence: the first create call timed out, but the checklist was created as `checklist_1782267140_295134`; the fallback evidence is still preserved in this plan artifact and gate evidence.

## Scope

- Use the refreshed `botster-core`/`botster-core-daemon` lock state at rev `274fdb981bda883cd8752b6c1100f14313432219`.
- Add sanitized public daemon/client DTOs for package UI surface descriptors to `botster-hub-client`, exposed from `DaemonPackage.surfaces` on both `list_packages` and `show_package`.
- Project descriptor metadata from `PackageRecord.manifest.surfaces` through `HubClientPackage.surfaces`, then through `DaemonPackage.surfaces`, preserving the authoritative serde field names `id`, `kind`, `title`, `description`, `icon`, `order`, `category`, and `supports`.
- Update generated TypeScript daemon protocol output so downstream clients can discover package app/settings/dashboard-widget descriptors from `DaemonPackage` without guessing.
- Add focused serde compatibility tests for legacy package JSON without descriptors and current package JSON with descriptors.
- Add integration coverage that installs a local package declaring descriptors and proves both `ListPackages` and `ShowPackage` return those descriptors through the daemon DTO path.
- Add render/action validation with a fixed compatibility rule: packages with an empty descriptor set keep current pass-through behavior; packages with one or more descriptors reject `PluginSurfaceRender`/`PluginSurfaceAction` requests whose `surface_id` is not one of the declared descriptor `id` values. Return a structured daemon operator error/diagnostic for `operation=plugin_surface_render` or `operation=plugin_surface_action` without disconnecting the daemon.
- Update CLI package output only as compact operator-facing counts/IDs if useful and covered by existing package command tests.
- Update `docs/client-protocol.md` if public daemon DTO fields or client expectations change.

## Non-scope

- No web UI implementation.
- No invented web-specific fields such as `view_surface` or `settings_surface`.
- No raw `PackageRecord`, local install path, provenance path, lock metadata, or package-scoped diagnostics in public DTOs.
- No package registry policy rewrite, plugin lifecycle rewrite, or new package manager abstraction.
- No broad retrofit of plugin manifests beyond the local test fixture needed to prove descriptors.
- No protocol-version bump unless request framing or compatibility semantics change; DTO shape fixture/conformance revision is the likely compatibility signal if needed.

## Assumptions and unknowns

- Assumption: the refreshed core dependency at rev `274fdb981bda883cd8752b6c1100f14313432219` is the authoritative descriptor schema; hub should reuse `PackageSurfaceDescriptor` vocabulary rather than locally designing descriptor fields.
- Assumption: descriptor `id` values are the valid IDs for existing `PluginSurfaceRender.surface_id` and `PluginSurfaceAction.surface_id`; render/action request field names do not need to change.
- Assumption: descriptors are optional on package manifests, so legacy packages must deserialize and list/show with an empty descriptor array.
- Assumption: descriptors are client-visible manifest metadata, so exposing them on `DaemonPackage` is still within the sanitized DTO boundary.
- Assumption: descriptor arrays remain optional/empty on legacy packages via core serde defaults and hub DTO serde defaults.
- Unknown: exact diagnostic feature string for undeclared surface requests. Prefer existing daemon `OperatorError`/`DaemonDiagnostic` paths with `operation=plugin_surface_render` or `plugin_surface_action` and feature `plugin_surface` unless local error helpers already standardize another feature value.

## Affected surfaces/files

- Botster layers touched: Rust hub, package registry projection, daemon transport, public hub-client DTOs, generated TypeScript protocol, CLI package display, tests, and protocol docs.
- `Cargo.lock`: refreshed for the closed `botster-core` descriptor dependency.
- `src/packages.rs`: no policy change expected, but tests/helpers may need descriptor-bearing manifests.
- `src/client_api.rs`: extend `HubClientPackage` and `From<&PackageRecord>` projection with `surfaces`; add or reuse a small descriptor-ID predicate for render/action validation.
- `src/daemon_transport.rs`: map `HubClientPackage.surfaces` into `DaemonPackage.surfaces`; before runtime dispatch, reject undeclared render/action `surface_id` only when the target package declares a non-empty descriptor set.
- `crates/botster-hub-client/src/lib.rs`: add `DaemonPackageSurface`, `DaemonPackageSurfaceKind` if an enum is useful, and/or string DTO fields mirroring core serde values; add serde defaults and stability tests.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`: generated TypeScript DTO update.
- `src/main.rs`: compact package CLI output update if descriptor counts/IDs are useful.
- `tests/hub_client_api_test.rs`: package projection tests using descriptor-bearing manifests.
- `tests/hub_daemon_lifecycle_test.rs`: real daemon local-package install/list/show coverage and undeclared surface diagnostics if practical.
- `examples/*/botster-package.json`: only if needed as a stable fixture; prefer test-local fixture manifests to avoid changing example semantics unnecessarily.
- `docs/client-protocol.md`: document `DaemonPackage` descriptors and render/action descriptor-ID expectation if DTO changes.

## Risks

- Stale dependency risk: resolved for planning by refreshing `Cargo.lock` to core rev `274fdb981bda883cd8752b6c1100f14313432219`, where `PackageManifest.surfaces` exists. Implementer must not downgrade or re-lock to a revision lacking descriptors.
- DTO drift risk: TypeScript and docs can invent fields that do not match Rust serde. Mitigation: derive from `botster-hub-client` structs and run generator/drift tests.
- Backward compatibility risk: old packages and old serialized package rows must still deserialize. Mitigation: `#[serde(default)]` on descriptor arrays and explicit legacy serde test.
- Privacy/path leak risk: package DTOs must stay sanitized. Mitigation: keep descriptor projection to manifest-declared UI metadata only and assert output/debug text does not include package roots/provenance.
- Overreach risk: unknown-surface validation could block existing packages that render undeclared surfaces. Mitigation: strict validation applies only when a package declares a non-empty `surfaces` list; legacy empty-descriptor packages keep current pass-through behavior. Tests must cover both the legacy pass-through path and the rejecting descriptor path.
- Runtime proof risk: unit DTO tests alone would not prove the daemon path. Mitigation: include a local package install through the running daemon and assert `ListPackages`/`ShowPackage` responses.

## Acceptance checks/tests

- `./test.sh --test hub_client_api_test <focused package descriptor projection test>`
- `./test.sh --test hub_daemon_lifecycle_test <focused local package descriptor list/show test>`
- `cargo test -p botster-hub-client daemon_package_ui_surface_descriptors_are_serde_stable` or the repo-equivalent focused client-crate serde test asserting exact JSON field names: `surfaces`, `id`, `kind`, `title`, `description`, `icon`, `order`, `category`, `supports`.
- Regenerate/check `crates/botster-hub-client/generated/daemon-protocol.ts`; run the existing generated protocol drift test if present.
- Add/run focused daemon tests proving: legacy packages with empty `surfaces` keep current render/action pass-through; packages with declared descriptors allow declared `surface_id` values; packages with declared descriptors reject undeclared `surface_id` values with a structured operator error/diagnostic and no daemon disconnect.
- Run `./test.sh` if the implementation touches shared daemon/package surfaces beyond narrow DTO projection.
- Run strict clippy only if the final diff touches lint-sensitive shared Rust surfaces or introduces nontrivial new helpers: `cargo clippy --all-targets --all-features -- -D warnings`.
- Verify production entry path: `DaemonRequest::ListPackages` and `DaemonRequest::ShowPackage` route through `src/daemon_transport.rs`, `HubClientApi`, `PackageRegistry`, and return `DaemonResponse.packages` containing descriptors; render/action requests route through `PluginSurfaceRender`/`PluginSurfaceAction` and the new descriptor-ID validation before runtime dispatch.

## Vault gaps worth capturing

- Capture a Botster note after implementation if descriptor metadata lands with a stable schema: public package UI surface descriptors belong on sanitized `DaemonPackage` rows and render/action IDs should validate against descriptor IDs.
- Capture or update a note that a closed Project Pipelines dependency can require an explicit lock refresh even when the upstream schema has merged; this is related to [[stale project pipeline worktrees can miss merged dependency apis]] but specifically about git dependency locks.
- Capture a Project Pipelines operational note if checklist creation continues to hit plugin-worker timeouts during Plan, reinforcing the durable artifact fallback path.
