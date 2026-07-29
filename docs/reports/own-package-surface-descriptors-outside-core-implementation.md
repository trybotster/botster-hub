# Own package surface descriptors outside Core — implementation report

## Target

- Repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785294387_531161`
- Run: `run_1785294403_658595`
- Implementation commit used for exact-runtime proof:
  `ebe5db811590a9c49935a3cea2357a378f28c722`

## Guidance applied

- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]].
- Repository charter: [[botster-hub-playbook]].
- Surface overlays: [[botster-hub-client-playbook]],
  [[botster-package-reviewer-playbook]],
  [[botster-package-verifier-playbook]],
  [[botster-runtime-reviewer-playbook]], and
  [[botster-runtime-verifier-playbook]].
- Architecture and atomic notes: [[botster-architecture]], [[cli-patterns]],
  [[spa-patterns]], [[botster hub is a first party host profile over core]],
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
  [[external client hub tests use subprocess spawned hub test support]],
  [[cold turkey migrations eliminate dual code paths and version suffixes]],
  [[generated typescript dtos must encode serde field optionality]],
  [[generated dto drift tests need symmetric field and type checks]],
  [[hub test support npm releases need external consumer smoke]],
  [[published fixture readmes are part of the shipped contract]],
  [[conformance fixture revisions must be unique per published content]],
  [[test script required for rust tests not cargo test]], and
  [[rust repo strict lints must be verified before dismissing warnings]].
- [[project-pipelines-playbook]] and
  [[project pipelines is the configurable dev stack package schema acceptance target]]
  were applied during review rework because the checked-in Project Pipelines
  example manifest and its public conformance path were brought into scope.
  No Project Pipelines engine, workflow policy, or plugin behavior changed.

## Implementation

`botster-ui-contract` now owns the renderer-neutral package surface,
operation, and navigation vocabulary plus validation and generated
TypeScript/schema/fixture assets. `HubPackageManifest` is the single Hub parser
and durable package record type. Its Core-facing projection reuses Core's
policy-free execution types while forcing the historical Core `surfaces` and
`navigation` fields empty.

Hub client and daemon package rows now use the canonical
`PackageSurfaceDescriptor` directly. The generated daemon TypeScript imports
that type from `@trybotster/ui-contract` and no longer declares a duplicate
daemon-owned interface. Render/action admission requires both a declared
surface id and the matching declared operation. Admission lives at the
transport-neutral `HubClientApi` seam, so in-process and daemon clients cannot
disagree. The daemon maps those typed client errors to structured operator
diagnostics without disconnecting.

The packaged contract-matrix fixture now declares explicit app/settings
navigation. Its reusable exact-Hub conformance runner verifies list/show
descriptor parity, navigation ids and route paths, render/action behavior, and
daemon liveness. Focused daemon coverage proves undeclared surfaces and
unsupported operations are rejected.

Review rework also declares the checked-in
`examples/project-pipelines` surface as supporting render and action, then
executes the published `run_project_pipelines_conformance` helper against that
package through a real daemon socket. Persisted presentation validation remains
intentionally fail-closed, matching the registry's other restore checks; its
startup error now names the package and documents backup-first recovery.

## Files changed

- Workspace/docs: `Cargo.lock`, `README.md`, `docs/client-protocol.md`,
  `docs/plans/own-package-surface-descriptors-outside-core.md`.
- UI contract:
  `crates/botster-ui-contract/{Cargo.toml,src/lib.rs,src/assets.rs}`,
  its Rust tests, and
  `packages/ui-contract/{README.md,package.json,index.js,index.d.ts,schema.json,conformance-fixtures.json,test.mjs}`.
- Hub client:
  `crates/botster-hub-client/{src/lib.rs,src/typescript.rs,generated/daemon-protocol.ts,examples/generate_typescript.rs}`.
- Hub authority/runtime:
  `src/{packages.rs,persistence.rs,lifecycle.rs,client_api.rs,daemon_transport.rs,lib.rs}`.
- Test support and packaged fixtures:
  `crates/botster-hub-test-support/{src/lib.rs,examples/node_package_assets.rs,fixtures/plugin-contract-matrix/botster-package.json}`,
  `fixtures/plugins/plugin-contract-matrix/botster-package.json`, and the
  generated `packages/hub-test-support` protocol, metadata, fixtures,
  declarations, package manifest, tests, and README.
- First-party acceptance example:
  `examples/project-pipelines/botster-package.json`.
- Integration tests:
  `tests/{hub_client_api_test.rs,hub_daemon_lifecycle_test.rs,hub_plugin_lifecycle_test.rs}`.

## Ownership boundaries preserved

- Hub owns package manifest parsing, registry state, admission, persistence,
  route/navigation projection, and daemon request policy.
- `botster-ui-contract` owns only renderer-neutral descriptor semantics; it
  contains no Web/TUI placement or layout policy.
- Core remains the capability, host-profile, entrypoint, dependency,
  configuration, and execution-mechanism owner. The one-way Hub projection is
  policy-free and keeps Core's historical UI fields inert.
- No Core, Web, TUI, TUI-kit, Workspaces, or Project Pipelines repository was
  edited in this run. No aliases, compatibility fields, dual parsers, or
  empty-surface pass-throughs were added.

## Cross-repository routing

- The Workspaces prerequisite `ticket_1785295905_406600` is closed and its
  merged manifest declares `supports: ["render", "action"]`.
- Web `ticket_1785295078_550933`, TUI-kit
  `ticket_1785295913_493655`, and TUI `ticket_1785295085_796645` consume and
  prove the merged producer artifact in their own repositories.
- Core deletion `ticket_1785192713_586798` follows downstream proof; final Hub
  convergence `ticket_1785294898_993310` then refreshes the Core lock.
- A repeated first-party survey found Workspaces and Project Pipelines declare
  action support where they register actions; Web has an empty surface list;
  TUI declares no surfaces. A separate in-repository enumeration checked every
  `botster-package.json`: all three contract-matrix copies and
  `examples/project-pipelines` now match their handlers;
  `examples/synthetic-plugin` registers no surface or UI-action handler.

## Deviations from the approved plan

- The approved review described schema version 1. Hub main advanced to
  `HUB_STATE_SCHEMA_VERSION == 2` before implementation began. The branch was
  fast-forwarded first, and the synchronized plan now preserves and tests the
  actual version-2 baseline rather than reverting it.
- A checked daemon TypeScript generator entrypoint was added because the
  existing generator had no `--check` command. The plan's affected-file and
  acceptance lists were synchronized to include it.
- Duplicate ids and unresolved navigation targets are covered by canonical
  contract/admission tests rather than separate negative JSON fixture files.
  Review rework expanded the validator matrix to cover all three empty-id
  sites, duplicate navigation ids, and duplicate operations. Undeclared
  surfaces and unsupported operations are exercised through both the
  in-process client seam and exact daemon path.
- The initial implementation surveyed sibling first-party repositories but
  missed the Hub-owned Project Pipelines example. Review correctly exposed the
  gap through the published conformance helper. The example declaration,
  repository-local manifest survey, and real-daemon regression were added
  without restoring the removed permissive pass-through.
- The full seven-repository production campaign was not run because Web/TUI
  downstream tickets have not yet repinned to these prepared unpublished npm
  coordinates. The charter-required admission proof instead used freshly
  built exact Hub/session-worker binaries with the public packaged
  contract-matrix harness.

## Verification and downstream proof

Passed:

- `./test.sh -p botster-ui-contract` — 72 current tests, including every
  package-presentation validation error variant.
- `cargo run -p botster-ui-contract --example generate_assets -- --check`.
- `npm test` in `packages/ui-contract`.
- `./test.sh -p botster-hub-client` — 44 unit tests and 4 doctests.
- `cargo run -p botster-hub-client --example generate_typescript -- --check`.
- Focused Hub manifest projection, duplicate-admission, version-2 persistence,
  client navigation, contract-matrix, and strict surface-operation tests.
- `plugin_surface_admission_is_shared_by_the_in_process_client_api` — typed
  undeclared-surface and unsupported-operation rejection at the shared seam.
- `from_snapshot_rejects_invalid_presentation_with_recovery_guidance` —
  intentional fail-closed restore behavior and actionable startup guidance.
- `daemon_project_pipelines_example_exercises_published_surface_conformance` —
  checked-in first-party package installed through an isolated daemon and
  exercised by the public `run_project_pipelines_conformance` runner.
- `daemon_package_dtos_expose_declared_surfaces_and_validate_surface_operations`
  and
  `project_pipelines_surface_action_round_trip_uses_client_api_and_plugin_worker`
  — strict daemon diagnostics and positive in-process plugin-worker behavior.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`.
- `npm test` in `packages/hub-test-support` using a tarball-installed
  `@trybotster/ui-contract@0.1.1`.
- `npm pack` inspection for `@trybotster/ui-contract@0.1.1` and
  `@trybotster/hub-test-support@0.1.16`.
- Clean external consumer installed both tarballs, verified packaged
  checksums, found the canonical descriptor import, found no duplicate daemon
  descriptor, and read both navigation entries.
- Full `./test.sh` — all default tests passed; the one documented larger local
  adversarial test remained ignored.
- `cargo clippy --all-targets --all-features -- -D warnings`.
- `cargo fmt --all -- --check` and `git diff --check`.
- Cold-switch source scan: remaining `manifest.surfaces/navigation` hits are
  Hub-owned reads/tests; the removed DTO token remains only in a negative
  generator assertion and historical plan prose.
- First-party GitHub survey of current main manifests: Workspaces and Project
  Pipelines declare required operations; Web/TUI have no actionable surfaces.
  Repository-local enumeration separately covered every checked-in package
  manifest and its registered surface/action handlers.
- Ablation:
  ignoring manifest presentation validation made
  `install_rejects_duplicate_hub_owned_surface_ids` fail; bypassing the
  operation check made
  `daemon_package_dtos_expose_declared_surfaces_and_validate_surface_operations`
  fail. Both boundaries were restored and re-passed.
- Fresh target `/private/tmp/botster-hub-surface-fresh-target` built exact Hub
  commit `ebe5db811590a9c49935a3cea2357a378f28c722` and lock-pinned Core
  `e36435f2cb583c344d6f6ba2d62c39da324c7a64`.
  `daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  and the strict operation test both passed through those binaries.
  Hub SHA-256:
  `a7a6a4ee15f78cf523506aa5da4175a3d53f06e4dc120584e2036bd08cf906cd`;
  worker SHA-256:
  `baeded4263030e54406d0e0fc8eb9f441d68c55f274906e1c4e848820b7d4f1c`.

## Unverified behavior and residual risk

- npm registry inspection found current published versions
  `@trybotster/ui-contract@0.1.0` and
  `@trybotster/hub-test-support@0.1.14`. The prepared `0.1.1` and `0.1.16`
  artifacts are pack-verified but intentionally unpublished. After merge, an
  operator with npm credentials can publish them in dependency order from the
  repository root with:

  ```sh
  npm publish ./packages/ui-contract --access public && npm publish ./packages/hub-test-support --access public
  ```

- Browser and TUI production rendering remain the responsibility of the
  separately routed downstream tickets. The complete seven-repository
  fresh/upgrade campaign remains a downstream integration gate after those
  repositories consume the merged/published coordinates.
- Core still physically contains its old declarations until its deletion
  ticket lands. Focused projection and source-scan evidence proves they are
  inactive in this merged Hub path; final convergence must repeat that proof
  after the Core lock changes.
- A state file written before presentation validation may contain invalid
  descriptors and will now fail Hub startup by design. The error names the
  affected package and reason. Operators should back up `hub-state.json`, then
  remove or correct that package record before restarting; the untouched
  backup remains the recovery source.

## Vault guidance

Existing guidance was sufficient and no convention conflict was found. Three
implementation-established rules were missing as standalone durable captures
and were added inbox-first, then validated and auto-committed:

- `inbox/botster-package-surface-semantics-live-in-ui-contract-while-hub-owns-admission.md`
- `inbox/plugin-surface-requests-require-a-declared-id-and-operation.md`
- `inbox/cold-extraction-allows-only-dependency-ordered-dead-source-overlap.md`

These await the vault document/connect/update/verify promotion pipeline; no
direct `notes/` writes were made.

## Assumptions

- Changing the Rust/TypeScript type authority without changing existing JSON
  field names is not a daemon protocol-version change; the conformance fixture
  and npm package versions carry the shipped artifact change.
- Packages may legitimately declare no plugin surfaces, but no render/action
  request is admitted without a matching declaration.
- The time-bounded Core source overlap is allowed only under the answered
  dependency sequence and is not a compatibility waiver.
