# Own package surface descriptors outside Core

## Target and context

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785294387_531161`
- Run: `run_1785294403_658595`
- Assigned worktree: clean at
  `35e92f46a98c445765b6ba7755e029f5dde702f8` when planned.
- Repository ownership charter: [[botster-hub-playbook]]
- Role and affected-surface playbooks:
  [[planner-playbook]], [[botster-planner-playbook]],
  [[botster-hub-client-playbook]], [[botster-package-reviewer-playbook]], and
  [[botster-package-verifier-playbook]],
  [[botster-runtime-reviewer-playbook]], and
  [[botster-runtime-verifier-playbook]].
- Architecture maps and required planner context:
  [[botster-architecture]], [[cli-patterns]], [[spa-patterns]],
  [[identity]], and [[goals]].
- Ownership and package/UI notes:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster core ui and capability contracts must avoid product gravity]],
  [[botster hub client crate is the external client boundary]],
  [[botster package daemon dto exposes sanitized package rows]],
  [[package navigation entries declare discoverability not host placement]],
  [[plugin owned surface route renders run in plugin worker vms]],
  [[plugin surface handlers must validate against hub locked uinode contract]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[plugin conformance packages prove shared contracts while examples prove product behavior]],
  [[runtime client acceptance must render delivered snapshots through real registry]],
  and [[external client hub tests use subprocess spawned hub test support]].
- Migration, artifact, and verification notes:
  [[cold turkey migrations eliminate dual code paths and version suffixes]],
  [[generated typescript dtos must encode serde field optionality]],
  [[generated dto drift tests need symmetric field and type checks]],
  [[botster web generated protocol drift checks need explicit hub artifact paths]],
  [[hub test support npm releases need external consumer smoke]],
  [[published fixture readmes are part of the shipped contract]],
  [[conformance fixture revisions must be unique per published content]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[live hub target dirs can cache stale same version client schema]], and
  [[live hub proof records distinct hub and locked core binary provenance]].
- [[project-pipelines-playbook]] was not loaded as an implementation overlay:
  this ticket uses Project Pipelines workflow tools, but does not change the
  Project Pipelines package, plugin paths, or workflow policy.
- Project Pipelines workflow checklist:
  `checklist_1785294639_269782`.
- Vault discipline checklist: `checklist_1785294619_341063`.

The binding answer to `question_1785294681_340718` permits temporary
cross-repository **dead-source** overlap only as dependency-ordered staging. It
does not permit a compatibility runtime. This Hub change must use the new
contract exclusively; Core may physically retain its old declarations until
the downstream consumers prove the merged Hub artifact, after which Core
deletes them without aliases. A final Hub ticket then refreshes the Core lock
and proves the converged single-source graph.

## Current repository facts

- `Cargo.lock` pins `botster-core`, `botster-core-daemon`, and Core test support
  at `e36435f2cb583c344d6f6ba2d62c39da324c7a64`.
- The pinned Core `PackageManifest` still owns `surfaces` and `navigation`;
  Core also exports `PackageSurfaceDescriptor`, `PackageSurfaceKind`,
  `PackageSurfaceOperation`, `PackageNavigationEntry`, and
  `PackageNavigationTarget`.
- Hub parses local and registry manifests directly into Core
  `PackageManifest` in `src/packages.rs`, stores that value in `PackageRecord`,
  and reads `record.manifest.surfaces/navigation` in client and daemon
  projections.
- `src/daemon_transport.rs` currently allows any surface id when the manifest
  declares no surfaces. For non-empty declarations it checks only id
  membership, not whether the requested render/action operation is declared.
- `crates/botster-ui-contract` is already the standalone Rust authority for
  renderer-neutral UiNode/action semantics and generates
  `@trybotster/ui-contract` TypeScript, schema, and fixtures.
- `botster-hub-client` already depends on `botster-ui-contract`, but still owns
  the duplicate stringly `DaemonPackageSurfaceDescriptor`; the generated
  daemon protocol repeats that interface.
- The packaged `plugin-contract-matrix` already drives install, enable,
  list/show, route projection, render, action, rejected/accepted presentation,
  and daemon liveness through a real isolated Hub and plugin worker. Its
  manifest has declared surfaces but no explicit navigation entries.
- Web currently imports the generated `DaemonPackageSurfaceDescriptor` mirror
  and contains an `app_surfaces` fallback. TUI pins `botster-hub-client` and
  `botster-ui-contract` to the same older Hub commit and consumes
  `package.surfaces`.
- The first-party manifest survey found one prerequisite defect:
  `botster-workspaces/botster-package.json` declares `supports: ["render"]` for
  `workspaces`, while its plugin registers create-workspace and spawn-session
  actions on that surface. Project Pipelines already declares both `render`
  and `action`; Web and TUI declare no plugin surfaces or surface action
  handlers.
- TUI-kit independently pins `botster-ui-contract` to the same old Hub revision
  as TUI. Repinning TUI alone would create two contract sources, so TUI-kit
  must repin first.
- The repo-approved test entrypoint is `./test.sh`; it sets
  `BOTSTER_ENV=test` and forwards arguments to `cargo test`.

## Scope

### 1. Make `botster-ui-contract` the package-surface semantic authority

- Move the renderer-neutral package surface and navigation vocabulary into
  `botster-ui-contract`: descriptors, kinds, supported operations,
  discoverability-only navigation entries/targets, exact serde names, and
  validation errors.
- Preserve the current wire inventory unless a ticket-required validation rule
  makes a field invalid. In particular, navigation remains discoverability
  intent rather than global placement authority.
- Move the relevant Core conformance examples/tests into the UI contract's
  Rust tests and generated TypeScript/schema/fixture parity checks.
- Validate stable non-empty ids, unique surface and navigation ids, navigation
  targets that resolve to declared surfaces, and operation declarations used
  by runtime render/action requests. Preserve renderer-neutral metadata without
  introducing Web/TUI layout policy.

### 2. Cold-switch Hub package manifest ownership

- Introduce one Hub-owned package manifest/registry projection in
  `src/packages.rs`. It owns package registry/presentation metadata, including
  `surfaces` and `navigation`, while continuing to reuse Core types for
  policy-free capability, extension, dependency, configuration, and runnable
  execution inputs where those remain Core responsibilities.
- Parse local manifests and local registry embedded manifests into the
  Hub-owned type exactly once. Do not deserialize the same JSON through Core's
  surface/navigation fields, and do not preserve a Core-manifest fallback.
- Store the Hub manifest on `PackageRecord`, persistence snapshots, catalog
  preparation, install/reload/update paths, and test helpers. Replace wording
  that still calls the whole persisted manifest Core-owned.
- Preserve the existing durable JSON field names and keep
  `HUB_STATE_SCHEMA_VERSION` at `2`; this is an ownership/type refactor, not a
  wire migration. Existing version-2 Hub state containing package surfaces and
  navigation must open and round-trip through the Hub-owned manifest.
- Project the minimal policy-free input required by current Core admission and
  plugin execution without exposing, serializing, validating, or routing
  Core's old package UI declarations. This one-way ownership boundary is not a
  compatibility adapter and must have no alternative path.
- Assert that this Core-facing projection always sets Core's old `surfaces` and
  `navigation` fields to empty even when the Hub manifest declares both.
- Tighten admission/request validation so a plugin surface request requires a
  declared surface id and the matching `render` or `action` operation. Remove
  the empty-descriptor pass-through. Return the existing structured
  operator-error/diagnostic shape without disconnecting the daemon.

### 3. Remove Hub/client/test-support mirrors

- Make `HubClientPackage`, `DaemonPackage`, route/navigation projection, and
  request validation consume the `botster-ui-contract` types directly.
- Remove `HubClientPackageSurfaceDescriptor`,
  `DaemonPackageSurfaceDescriptor`, Core surface-kind label conversion, and
  any equivalent duplicate struct/string enum. Do not replace them with type
  aliases or forwarding re-exports.
- Generate the daemon TypeScript artifact so `DaemonPackage.surfaces` refers to
  the canonical `@trybotster/ui-contract` type rather than embedding a second
  interface. Preserve serde optionality and assert symmetric field/type parity.
- Update `botster-hub-test-support` Rust and npm surfaces, metadata, fixtures,
  checksums, imports, and docs to consume the UI contract package normally.
  Keep Hub transport/harness material in test support; do not copy the
  canonical contract back into it.

### 4. Prove the real packaged path and prepare merged artifacts

- Extend the checked-in plugin-contract-matrix manifest with explicit
  navigation entries and negative fixtures needed to prove duplicate ids,
  missing navigation targets, undeclared surfaces, and unsupported operations.
- Through the exact built Hub, install and enable the copied package, prove
  list/show descriptor parity and navigation-to-route parity, render the
  declared app surface, dispatch an action id read from the delivered tree,
  and prove undeclared/wrong-operation requests are rejected while the daemon
  remains usable.
- Regenerate `@trybotster/ui-contract` and
  `@trybotster/hub-test-support` artifacts and update their shipped READMEs and
  metadata. Inspect current registry versions before assigning new package
  versions.
- If publication requires npm credentials or 2FA, stop after the merged,
  pack-verified artifacts and report one exact operator command that publishes
  the required coordinates in dependency order.

## Non-scope

- No edits to `botster-core`, botster-web, botster-tui, or botster-workspaces
  in this repository run.
- No Core-to-Hub dependency and no relocation of PTY, process, transport,
  capability mechanism, plugin worker, or session execution contracts.
- No compatibility manifest field, serde alias, dual parser, old/new runtime
  switch, forwarding alias, duplicate surface DTO, or empty-surface
  pass-through.
- No browser/TUI placement, grouping, pinning, hiding, responsive layout, or
  renderer policy in the shared descriptor/navigation types.
- No broad package registry rewrite, marketplace work, unrelated daemon
  protocol cleanup, or adjacent fixture redesign.
- No npm publishing-only ticket. Manual publication, if required, is an
  operator boundary after the merged producer artifact.

## Ownership boundaries and cross-repository dependencies

- **Hub / this ticket (`ticket_1785294387_531161`)** owns the producer change:
  shared renderer-neutral package UI vocabulary in `botster-ui-contract`;
  Hub package manifest/registry/admission policy; HubRuntime/client/daemon
  projection; generated artifacts; and packaged live-Hub conformance.
- **Web (`ticket_1785295078_550933`, target
  `tgt_40abcf71ccf049f4ac0c99953a799869`)** depends on this ticket. It removes
  the generated descriptor mirror and `app_surfaces` fallback, consumes the
  published merged contract, and proves the real React transport/component
  path.
- **TUI (`ticket_1785295085_796645`, target
  `tgt_c3d470bab78549df920a41e8fb0e58d8`)** depends on this ticket. It repins
  both Hub crates to one merged Hub commit and proves the public protocol plus
  real-frame/input path with one UI contract source. It also depends on the
  TUI-kit repin below.
- **TUI kit (`ticket_1785295913_493655`, target
  `tgt_3dfae49c02454037bf13554f552baf7f`)** depends on this Hub ticket and
  repins its independent `botster-ui-contract` dependency to the same merged
  revision before TUI repins, preventing a two-source Cargo graph.
- **Workspaces prerequisite (`ticket_1785295905_406600`, target
  `tgt_71266a8d976d4535902ffed09c18a7ba`)** is a blocking dependency of this
  Hub ticket. It adds `action` to the existing `workspaces` surface declaration
  and proves the already-registered create/spawn actions through the current
  permissive Hub before strict operation admission lands.
- **Core deletion (`ticket_1785192713_586798`, target
  `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`)** already depends on this ticket and
  now also depends on both downstream consumer tickets. It deletes the old
  surface/navigation types, manifest fields, validation, fixtures, and exports
  without aliases or a Hub dependency.
- **Final Hub convergence (`ticket_1785294898_993310`, target
  `tgt_7e208a0c76a44980a83b63af976b1f22`)** depends on Core deletion. It
  refreshes this repository's locked Core revision and proves the final
  single-source dependency graph.
- **Final integration (`ticket_1785192726_335558`)** depends on final Hub
  convergence and remains the full multi-product proof; it is not a repair
  surface for ownership defects.

No runtime/build in this chain may accept both contracts. The only permitted
overlap is the time-bounded period when Core's old source is still present but
all merged Hub and downstream runtime consumers treat it as dead and
non-authoritative.

## Assumptions and unknowns

- Binding assumption: the answered staging sequence is the sole permitted
  interpretation of the ticket; it is not a compatibility waiver.
- Assumption: Core's current non-UI package types can remain direct field types
  inside a Hub-owned manifest until Core narrows its final execution boundary.
  The Hub must not duplicate those types or expose Core UI fields.
- Assumption: the daemon request/response discriminants and protocol version do
  not need to change merely because a DTO now references the canonical UI
  contract type. Conformance/package revisions and TypeScript imports must
  change when the shipped bytes change.
- Assumption: packages without plugin surfaces may keep an empty `surfaces`
  array; only plugin render/action requests lose the undeclared pass-through.
- Assumption: preserving the exact persisted manifest field names makes the
  ownership switch schema-compatible, so `HUB_STATE_SCHEMA_VERSION` remains
  `2` and existing version-2 state is required input evidence. The approved
  plan originally named version 1; refreshing to Hub main before implementation
  incorporated the already-merged schema-v2 change, so this plan now preserves
  the actual pre-change baseline rather than reverting it.
- Unknown: the next available npm versions and whether publication will require
  2FA. Resolve from the registry after implementation; do not guess versions.
- Unknown: whether the smallest clean TypeScript artifact is an explicit
  type-only import or generator-composed declarations. Choose the existing
  package/export mechanism that yields one canonical definition and passes
  clean external package installation; do not embed a second interface.
- Unknown: whether Core's deletion ticket narrows or replaces
  `PackageManifest`. The final convergence ticket must adapt only to the merged
  Core API and may not restore a historical manifest shape.

## Affected surfaces and likely files

- Workspace/dependency/docs:
  `Cargo.toml`, `Cargo.lock`, `README.md`, `docs/client-protocol.md`, and the
  relevant ownership ADR if its current responsibility table still assigns
  package surfaces/navigation to Core.
- Shared contract:
  `crates/botster-ui-contract/src/lib.rs`,
  `crates/botster-ui-contract/src/assets.rs`,
  `crates/botster-ui-contract/tests/ui_contract_test.rs`,
  `crates/botster-ui-contract/tests/generated_assets_test.rs`,
  `crates/botster-ui-contract/examples/generate_assets.rs`, and
  `packages/ui-contract/{index.d.ts,index.js,schema.json,conformance-fixtures.json,README.md,package.json,test.mjs}`.
- Hub package authority:
  `src/packages.rs`, `src/persistence.rs`, `src/lifecycle.rs`,
  `src/client_api.rs`, `src/daemon_transport.rs`, and any package/runtime test
  helper that constructs Core `PackageManifest` directly.
- Public client/generated protocol:
  `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/examples/generate_typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Packaged proof/test support:
  `crates/botster-hub-test-support/{Cargo.toml,src/lib.rs}`,
  `fixtures/plugins/plugin-contract-matrix/**`,
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`,
  `packages/hub-test-support/{package.json,metadata.json,daemon-protocol.ts,index.d.ts,index.js,README.md,test.mjs}`,
  `packages/hub-test-support/scripts/sync-assets.mjs`, and
  `packages/hub-test-support/fixtures/plugin-contract-matrix/**`.
- Integration tests:
  `tests/hub_client_api_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`,
  `tests/hub_plugin_lifecycle_test.rs`, and focused `src/packages.rs` tests.

Implementer should use `rg` after each compile error to port every in-repo
manifest literal on touch; do not bulk-refactor unrelated runtime tests beyond
the field/type changes forced by the single new manifest authority.

## Risks

- **Hidden dual authority:** flattening or deserializing the raw package JSON
  through both Hub and Core would leave Core's old fields active. Prevent this
  with one Hub manifest parser and source/behavior tests that reject the old
  path.
- **Over-broad manifest duplication:** copying all Core package types into Hub
  would make Hub the mechanism owner. Reuse Core's policy-free field types and
  project only execution inputs; own only package registry/presentation policy.
- **Validation regression:** removing the legacy empty-list pass-through or
  enforcing `supports` can break undeclared/misdeclared first-party surfaces.
  The cross-repository survey found and routed the Workspaces defect before
  Hub enforcement; repeat the full survey during implementation and prove the
  negative path returns a typed operator frame without disconnecting.
- **Navigation authority leak:** moving navigation can accidentally turn
  `order` or other manifest hints into host placement policy. Keep route
  discovery separate from renderer placement and retain forbidden-field tests.
- **Generated artifact drift:** Rust can pass while generated TypeScript,
  schema, npm copies, or READMEs remain stale. Require generator `--check`,
  symmetric field/type/optionality tests, package sync checks, and `npm pack`
  content inspection.
- **Artifact availability:** merged source is not a usable Web artifact.
  Publish/install proof is a separate boundary; if 2FA blocks it, report the
  exact command and leave downstream tickets correctly blocked.
- **Stale build proof:** same-version Hub client artifacts can survive in live
  target directories. Use fresh target directories and record distinct Hub and
  lock-pinned Core binary provenance.
- **Conformance overclaim:** DTO existence or fixture inspection does not prove
  production behavior. The exact Hub binary must install/enable the package and
  drive public list/show/navigation/render/action requests.
- **Scope creep:** this touches many literals because the manifest type is
  public. Every change must trace to replacing the active package-surface
  authority or to proof/docs made necessary by that replacement.

## Acceptance checks and downstream proof

Use repository wrappers and exact function filters; a filename-like Cargo
filter that runs zero tests is not evidence.

1. Shared contract and generated artifacts:

   - `./test.sh -p botster-ui-contract`
   - `cargo run -p botster-ui-contract --example generate_assets -- --check`
   - `cd packages/ui-contract && npm test`
   - Contract tests cover descriptor/kind/operation serde, navigation
     forbidden placement fields, duplicate ids, unresolved navigation targets,
     and generated TypeScript/schema/fixture parity.

2. Hub manifest/admission and public client projection:

   - `./test.sh -p botster-hub-client`
   - `cargo run -p botster-hub-client --example generate_typescript -- --check`
   - `./test.sh --test hub_client_api_test package_navigation_uses_explicit_manifest_entries_and_route_diagnostics`
   - Focused `src/packages.rs` tests prove one Hub-owned parser, persisted
     round-trip, valid empty non-surface packages, duplicate/invalid rejection,
     and absence of a Core surface fallback.
   - Open a pre-change version-2 Hub state containing package surfaces and
     navigation, preserve `HUB_STATE_SCHEMA_VERSION == 2`, and prove the
     Hub-owned manifest round-trips it without loss.
   - A focused projection test constructs a Hub manifest with surfaces and
     navigation, passes it through the Core-facing admission projection, and
     asserts Core's old `surfaces` and `navigation` fields are both empty.
   - Generated protocol drift asserts `DaemonPackage.surfaces` references the
     canonical contract type, exact serde optionality, and no
     `DaemonPackageSurfaceDescriptor` declaration.

3. Real packaged Hub path:

   - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
   - `./test.sh --test hub_daemon_lifecycle_test daemon_package_dtos_expose_declared_surfaces_and_validate_surface_operations`
   - Build the exact Hub and its lock-pinned session worker in a fresh target
     directory, verify both executable realpaths and source SHAs, then run
     `script/test-production-package-runtime` or the repo-equivalent packaged
     acceptance entrypoint.
   - The live report must prove install/admission, enable, list/show descriptor
     parity, explicit navigation/route parity, render, action dispatch using
     metadata from the delivered node, rejected undeclared surface, rejected
     unsupported operation as the existing typed
     `DaemonResponseKind::OperatorError` plus structured diagnostics (never
     disconnect/free text), and a successful follow-up request. Per
     [[hub supervision admission changes require exact live hub launch proof]],
     this exact packaged-runtime evidence is a blocking gate for the new
     admission rule.

4. Test-support/package artifacts:

   - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
   - `cd packages/hub-test-support && npm test`
   - `npm pack --dry-run` (or inspect the produced tarball) for both changed npm
     packages, asserting canonical type imports, metadata, fixtures, schema,
     generated protocol, and READMEs are included.
   - If publishing is possible, install the exact published coordinates in a
     clean temporary consumer and assert package versions, asset verification,
     absence of the duplicate DTO token, canonical package-surface tokens, and
     contract-defining fixture content.

5. Repository-wide quality and cold-switch scans:

   - `./test.sh`
   - `cargo fmt --all -- --check`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `git diff --check`
   - `rg -n 'botster_core::(PackageSurface|PackageNavigation)|PackageSurfaceDescriptor|DaemonPackageSurfaceDescriptor|manifest\\.surfaces|manifest\\.navigation|app_surfaces' src crates tests fixtures packages README.md docs`
     with every remaining hit classified as the new authority, deliberate
     negative assertion, or stale path to remove.
   - Survey every admitted first-party package manifest, not only this
     repository: Workspaces is routed through
     `ticket_1785295905_406600`; Project Pipelines already declares
     `["render","action"]`; Web and TUI have no plugin surface/action handlers.
     Record any newly discovered mismatch as a dependency against its owning
     repository before enabling strict operation admission.
   - Ablate the new admission operation check and manifest validation boundary
     separately and show the focused negative tests fail.

6. Dependency-ordered downstream evidence before Core deletion:

   - Web ticket `ticket_1785295078_550933` installs the normal published merged
     artifact, runs the explicit authoritative daemon-protocol drift check, and
     renders/interacts through the real React transport/component path with no
     DTO mirror or `app_surfaces` fallback.
   - TUI ticket `ticket_1785295085_796645` repins both Hub crates to one merged
     commit only after TUI-kit ticket `ticket_1785295913_493655` repins its
     independent contract dependency. Both run repo format/test/strict-clippy
     gates and prove a one-source dependency graph before TUI proves package
     navigation and action through the real frame/input backend.
   - Only after both close may Core ticket `ticket_1785192713_586798` remove its
     old declarations. Final Hub ticket `ticket_1785294898_993310` then updates
     `Cargo.lock` and repeats the full Hub/runtime/source-graph proof.

## Vault gaps worth capturing

- Capture the implemented boundary as a durable note: package UI surface and
  discoverability semantics live in `botster-ui-contract`, while the Hub-owned
  manifest/registry projection owns package admission and presentation policy.
- Capture the staged cold-extraction rule: dependency-ordered dead-source
  overlap across repository merge events is allowed only when every merged
  runtime has one active contract and the deletion/convergence tickets are
  registered.
- Update [[package navigation entries declare discoverability not host placement]]
  if the final navigation validation vocabulary becomes stable.
- Capture the stricter rule that plugin render/action requests require both a
  declared surface id and the matching declared operation.
- Capture the TypeScript composition pattern if the final generated daemon
  protocol cleanly references an independently published canonical contract
  without duplicating its interface.
- Do not write directly to `notes/`; route any durable capture through the vault
  inbox/document/connect/verify pipeline after implementation establishes the
  final names and evidence.
