# Extract UiNode into the Hub-owned UI contract

## Target and context

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785192683_691772`
- Assigned worktree: the Project Pipelines ticket worktree, clean at
  `333f042454b8b4b4ae85877370b40ae509913261` when planned.
- Repository charter: [[botster-hub-playbook]]
- Role and surface guidance: [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-hub-client-playbook]], and
  [[project-pipelines-playbook]].
- Architecture maps and required context: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]], [[identity]], and [[goals]].
- Contract and ownership notes loaded:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[botster core ui and capability contracts must avoid product gravity]],
  [[botster core contract surface needs consumer proof]],
  [[plugin surface handlers must validate against hub locked uinode contract]],
  [[botster plugin modal state belongs in client-local presentation state]],
  [[presentation policy uses auto resolution not separate dialog sheet and fullscreen primitives]],
  [[uinode kind agnostic prop checks must be schema owned]],
  [[core uiaction has no label so clients must not synthesize one]],
  [[form placeholders must not seed submitted values]],
  [[generated typescript dtos must encode serde field optionality]],
  [[generated dto drift tests need symmetric field and type checks]],
  [[adapter test fixtures must validate translation not passthrough]],
  [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]],
  [[conformance fixture revisions must be unique per published content]],
  [[published fixture readmes are part of the shipped contract]],
  [[plugin conformance packages prove shared contracts while examples prove product behavior]],
  [[runtime client acceptance must render delivered snapshots through real registry]],
  [[hub test support npm releases need external consumer smoke]], and
  [[external client hub tests use subprocess spawned hub test support]].
- Workflow and verification notes loaded:
  [[cold turkey migrations eliminate dual code paths and version suffixes]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[plan steps need reviewable plan artifacts]], and
  [[project pipelines checklist worker timeouts require artifact evidence fallback]].

The binding human answer to `question_1785192989_753456` requires a standalone
`@trybotster/ui-contract` package generated from the authoritative Rust
`botster-ui-contract` crate. It owns TypeScript types/schema and shared
renderer-neutral fixtures. `@trybotster/hub-test-support` pins and consumes it,
adding only Hub transport/harness material. It must not copy, canonically own,
or compatibility-re-export the UI contract.

## Current repository facts

- The Cargo workspace currently contains the Hub, `botster-hub-client`, and
  `botster-hub-test-support`. The new crate belongs beside the two existing
  client-facing crates.
- `Cargo.lock` pins `botster-core` and `botster-core-daemon` at
  `49159e7373ffc2cdbb26c856bb3c738841a42742`.
- The pinned Core revision owns the complete contract in
  `crates/botster-core/src/contract/ui.rs` and its primary tests in
  `crates/botster-core/tests/ui_contract_test.rs`. Core test support owns the
  current renderer fixture builder in
  `crates/botster-core-test-support/src/ui_conformance.rs`.
- Hub production code imports `UiNode` and `UiActionResult` directly from Core
  in `src/runtime.rs`, `src/client_api.rs`, and `src/daemon_transport.rs`.
  `HubRuntime::render_plugin_surface` already provides the production
  deserialization and validation entry point.
- The daemon action request is currently a compatibility-shaped tuple of
  `package_name`, `surface_id`, `action_id`, and arbitrary `payload`, while the
  canonical Core `UiActionRequest` already distinguishes `values` from
  `payload`. Generated TypeScript exposes that same legacy tuple and treats
  action results as untyped JSON.
- `DaemonPluginSurface` and `DaemonUiTreeSnapshot` currently carry untyped JSON
  bodies because `botster-hub-client` cannot depend on the Core UI module.
- `@trybotster/hub-test-support@0.1.11` is both the checked-in and current
  registry release. Its generated metadata records protocol version 3 and
  conformance fixture revision 18. The
  `@trybotster/ui-contract` registry coordinate does not yet exist.

## Scope

### 1. Establish one self-contained Rust authority

Add `crates/botster-ui-contract` as a workspace member and normal Cargo
dependency of the Hub, `botster-hub-client`, and `botster-hub-test-support`.
Move the complete pinned Core UI vocabulary and validation implementation into
it in one pass:

- all current node kinds, ids, children, slots, bindings, responsive values,
  capabilities, fallbacks, application vocabulary, host placeholders, custom
  fallback rules, field schemas, Dialog/Form/Button contracts, action request
  and result types, and validation errors;
- the complete Core validation and serialization test matrix, adjusted only
  for the new crate boundary and ticket-required semantics;
- the renderer-neutral conformance fixture source currently in Core test
  support.

The new crate must have no `botster-core`, Hub runtime, renderer, Lua, browser,
TUI, or marketplace dependency. Replace Core's `RequestId` alias with a
contract-owned transparent string request id so the serialized shape remains a
string without retaining Core ownership.

Preserve existing serde names and validation behavior for moved vocabulary
unless this ticket explicitly replaces that behavior. This is extraction plus
the cohesive dialog/action/presentation contract below, not an opportunity to
rename primitives or redesign unrelated props.

The replacement model is an explicit cold switch: remove `UiTreeUpdateRef` and
`UiActionResult.tree_update` rather than carrying the opaque patch/replacement
reference beside the new direct replacement tree. The new accepted-result
field is the sole tree replacement path.

### 2. Define the cross-client interaction contract coherently

Implement these semantics as typed Rust values with matching wire
discriminants, TypeScript declarations, schema, and fixtures:

- Presentation state is client-local and scoped by the active Hub/package
  surface. Authored payloads carry a local key and value, not a global store
  name or renderer policy.
- Presentation actions are typed `set`, `clear`, and `toggle` operations.
  They are not magic plugin action ids and do not travel to a plugin worker.
- Conditional bindings support both presence/truth evaluation and typed JSON
  equality. The equality fixture must select one workspace id without adding
  workspace-specific vocabulary to the contract.
- Dialog visibility derives from a scoped presentation binding and node
  presence. `Dialog` must reject `props.open`; renderers do not get a
  compatibility exception.
- Keep one `Dialog` primitive and its existing
  `auto`/`inline`/`overlay`/`sheet`/`fullscreen` presentation intent. Clients
  choose renderer policy.
- `Form` carries an explicit submit label and canonical owner action. Form
  drafts serialize only in `UiActionRequest.values`; the action descriptor's
  optional non-form metadata stays in `UiActionRequest.payload`.
- An accepted `UiActionResult` may carry typed presentation operations,
  including closing the dialog key, and an inline owner-authored replacement
  `UiNode` tree. The replacement validates before it can cross the Hub
  boundary.
- A rejected result retains the existing tree and presentation state and may
  carry field/form errors and normalized values. Validation rejects
  accepted-only replacement or close effects on rejected/error results.
  Deferred/error behavior remains represented but cannot silently apply an
  accepted replacement.

Use one canonical action request/result envelope everywhere. Do not add legacy
field aliases, a second action enum, `action_id + payload` fallback decoding,
the removed `UiTreeUpdateRef`/`tree_update` fields, or Hub-local mirror structs.

### 3. Cold-switch the Hub and daemon protocol

- Change `src/runtime.rs` to deserialize and validate plugin trees and action
  results through `botster-ui-contract`, then pass the canonical
  `UiActionRequest` JSON to the owning plugin worker. This is an intentional
  plugin-worker ABI change: handlers receive the full request envelope and
  must read form drafts from `arguments.values` and non-form metadata from
  `arguments.payload`, not flat legacy arguments.
- Change `src/client_api.rs` so plugin action routing accepts the canonical
  request rather than separate action id and arbitrary payload fields.
- Change `crates/botster-hub-client` and `src/daemon_transport.rs` so
  `PluginSurfaceAction` contains `package_name` plus the typed canonical
  request; surface and action identity come from that request. Make plugin
  surface/snapshot trees and plugin action results typed with
  `botster-ui-contract` public types instead of `serde_json::Value`.
- Preserve Hub admission, package/surface ownership checks, diagnostics, and
  the existing worker execution path. Hub owns validation and routing policy,
  not presentation-state storage, dialog layout, focus, or replacement
  rendering.
- Remove every direct Hub import of Core UI types. There is no compatibility
  action envelope or duplicate Hub runtime definition.

Because request semantics change, implementation must intentionally advance
the daemon protocol version. If shared fixture bytes change, allocate a
conformance revision above every already published meaning; do not assume 19
until registry/release history is rechecked. Derive compatibility and support
matrix values from the source constants and regenerate
`first-party-client-support-matrix.json`.

This is a deliberate pre-production flag day. Clients pinned to protocol 3 or
`@trybotster/hub-test-support@0.1.11` will correctly report a compatibility
mismatch after the new Hub lands until their routed adoption tickets consume
the new artifacts. Required landing order is: Hub producer; TUI kit; TUI and
Web plus plugin adopters; Core removal; final integration.

### 4. Generate the standalone TypeScript contract and shared fixtures

Add `packages/ui-contract` with the registry coordinate
`@trybotster/ui-contract`, initially `0.1.0` if the coordinate remains unused
at implementation time. Generate, from the Rust crate:

- serde-accurate TypeScript declarations for every public node, action,
  binding, presentation, request, and result type;
- a machine-readable schema for plugin/client validation and editor tooling;
- shared JSON conformance fixtures covering the moved vocabulary plus dialog
  presence, presentation set/clear/toggle, equality binding, explicit form
  submit labeling, canonical values/payload, accepted close/replacement, and
  rejected retention/errors.

The checked-in generated files must have a deterministic generate/check path.
Parity tests must compare field sets, discriminants, mapped value types, and
serde optionality in both directions rather than checking for token presence.

Update `botster-hub-client`'s generated daemon protocol to import the UI types
from `@trybotster/ui-contract`. Update
`@trybotster/hub-test-support` to declare and pin that normal npm dependency
and consume its shipped fixtures for Hub-specific transport/runtime proof. Do
not copy the TypeScript contract or fixtures into hub-test-support. Because its
published contents, dependency graph, protocol version, and conformance
metadata change, bump hub-test-support from the currently published `0.1.11`
to a new unused package version and update every current README/metadata/docs
claim that names the version or revision.

If registry authentication or 2FA blocks publication, stop after the merged,
packed, externally installable artifacts are ready. The operator handoff must
publish `@trybotster/ui-contract` first, then publish the bumped
`@trybotster/hub-test-support` that declares it, and report the exact
`npm publish --access public` command for each package directory. Do not create
a publication-only ticket.

### 5. Update contract fixtures and documentation

- Extend the canonical plugin contract matrix source with dialog/form/button
  examples and accepted/rejected action results using the new envelope.
  Regenerate its crate/npm mirrors; do not edit mirrors independently.
- Cold-switch `examples/project-pipelines/plugin.lua` to the worker-visible
  canonical request, required Form submit label, and direct accepted/rejected
  result semantics. Update `examples/project-pipelines/README.md`, which is
  part of the shipped first-party plugin contract. Inspect
  `examples/synthetic-plugin/**` and record that it authors no UI, or update it
  if that inspection proves otherwise.
- Update `README.md` crate/package ownership, `docs/client-protocol.md`, the new
  package README, and the plugin contract matrix README to name
  `botster-ui-contract` and `@trybotster/ui-contract` as the authority.
- Describe `ui_tree_snapshot.body` as a typed validated tree and document the
  canonical daemon action request/result JSON, presentation scoping, direct
  replacement behavior, protocol/conformance changes, and manual publication
  boundary.
- Scrub old claims that Core is the authoritative UiNode source from touched
  current documentation. Historical plan/report artifacts remain historical
  and are not bulk rewritten.

## Non-scope

- No edits to `botster-core`, `botster-web`, `botster-tui`,
  `botster-tui-kit`, `botster-workspaces`, or the external
  `botster-project-pipelines` repository in this run.
- No renderer implementation, focus manager, React/Ionic component, Ratatui
  widget, workspace product policy, entity store, Git/worktree behavior, or
  session spawning behavior.
- No marketplace package manifest or transitive Botster package installation.
  Cargo and npm dependencies are build/protocol dependencies.
- No compatibility re-export from hub-test-support, legacy action envelope,
  `props.open` exception, duplicate Hub struct, Core fallback import, or
  feature/version suffix.
- No speculative primitive additions, unrelated daemon DTO cleanup, or broad
  rewrite of historical plans/reports.
- No npm publication attempt that needs credentials or 2FA.

## Ownership boundaries and dependencies

- `botster-ui-contract` owns renderer-neutral UI vocabulary, wire
  serialization, semantic validation, presentation operations/predicates,
  generated TypeScript/schema, and shared fixtures.
- Hub runtime owns trusted plugin output validation, package/surface/action
  routing, diagnostics, and daemon projection. It does not own renderer policy
  or client-local presentation state.
- `botster-hub-client` owns daemon framing, compatibility descriptors, and
  typed references to the UI contract; it does not redefine or re-export the
  contract for compatibility.
- `botster-hub-test-support` owns real-Hub transport/runtime harnesses and pins
  the UI package for those proofs. It does not own copied contract fixtures.
- Web and TUI clients own local scoped presentation stores and rendering. Their
  adoption belongs to the existing Web, TUI kit, and TUI project tickets.
- The live external Project Pipelines plugin has a different current action
  ABI and is an explicit downstream adopter, not hidden non-scope.
  `ticket_1785194090_628084` targets
  `tgt_a72ca1a83d504385b8648f71409119ab` and is durably blocked on this producer
  by `dependency_1785194093_410838`.
- Core still contains the old source until the separate Core removal ticket
  runs after consumers switch. This run makes the Hub-owned package
  authoritative by removing all Hub/Core UI imports; it must not silently
  broaden into the Core repository. Project completion requires
  `ticket_1785192713_586798` to delete the obsolete Core surface.
- There is no prerequisite ticket blocking this producer change. This ticket
  is instead a prerequisite artifact for the existing Web
  (`ticket_1785192696_321546`), TUI kit
  (`ticket_1785192700_939910`), TUI (`ticket_1785192707_900922`), the registered
  Project Pipelines plugin adoption (`ticket_1785194090_628084`), Core removal,
  and final integration tickets. Those runs must consume merged/published
  artifacts, not sibling-worktree overrides.

## Assumptions and unknowns

- Binding answer: the TypeScript package is the standalone
  `@trybotster/ui-contract`; hub-test-support only consumes it.
- Assumption: Rust crate and npm package start at `0.1.0`, subject to a final
  registry/package-name check immediately before packing or publishing.
- Assumption: package/surface scope is supplied by the host client context and
  is not author-controlled data inside each presentation action. This prevents
  one plugin surface from mutating another surface's local state.
- Decision: remove the current opaque `UiTreeUpdateRef` and `tree_update`
  field. An inline validated accepted-result replacement is the only tree
  update path; no mutually exclusive compatibility form remains.
- Assumption: the existing duplicate `plugin_surface.body` and
  `ui_tree_snapshot.body` response fields remain only where current protocol
  documentation still requires them; both must be serialized from the same
  typed validated node. Removing that response compatibility is not requested.
- Unknown to resolve during implementation: the highest conformance revision
  already assigned by any merged or published branch. Recheck before choosing
  the new constant.
- Unknown to resolve during implementation: whether npm publication is
  available without interactive credentials. This does not block building,
  packing, and external tarball consumption proof.
- Expected flag-day consequence: old first-party client pins will report
  compatibility mismatch until their routed adoption tickets land. That is
  intentional evidence of the cold switch, not a reason to retain protocol-3
  decoding.

## Affected surfaces and likely files

- Workspace/dependency graph: `Cargo.toml`, `Cargo.lock`.
- New Rust authority:
  `crates/botster-ui-contract/Cargo.toml`,
  `crates/botster-ui-contract/src/**`,
  `crates/botster-ui-contract/tests/**`,
  `crates/botster-ui-contract/fixtures/**`, and generated artifacts.
- Hub production path: `src/runtime.rs`, `src/client_api.rs`,
  `src/daemon_transport.rs`.
- Hub/client protocol: `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Hub conformance support:
  `crates/botster-hub-test-support/Cargo.toml`,
  `crates/botster-hub-test-support/src/lib.rs`,
  `crates/botster-hub-test-support/examples/node_package_assets.rs`, and
  relevant tests.
- Standalone npm package: `packages/ui-contract/**`.
- Hub npm harness package: `packages/hub-test-support/package.json`,
  generation/check scripts, metadata/API/tests, and the generated daemon
  protocol artifact, without copied UI fixtures/types.
- Canonical plugin fixture and generated mirrors:
  `fixtures/plugins/plugin-contract-matrix/**`,
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`, and
  `packages/hub-test-support/fixtures/plugin-contract-matrix/**`.
- First-party in-repo plugin surfaces:
  `examples/project-pipelines/plugin.lua`,
  `examples/project-pipelines/README.md`, and an explicit audit of
  `examples/synthetic-plugin/**`.
- Runtime/protocol tests: `tests/hub_lua_runtime_test.rs`,
  `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, and
  `tests/hub_test_support_conformance_test.rs`.
- Current docs: `README.md`, `docs/client-protocol.md`, and fixture/package
  READMEs.

## Risks and mitigations

- **Incomplete extraction:** copying only types while leaving validation or
  fixtures in Core would preserve the wrong authority. Move the full module,
  tests, and conformance source, then use absence searches for direct Core UI
  imports in Hub code.
- **False cold switch:** accepting both daemon action shapes would make Web/TUI
  behavior ambiguous. Deserialize only the canonical envelope and include a
  negative old-shape test.
- **External plugin ABI break:** the live `botster-project-pipelines` handlers
  read flat arguments and its Form lacks the required submit label. Keep that
  repository out of this run and enforce its registered dependent adoption
  ticket after merged artifacts exist.
- **Renderer policy leakage:** a Hub-side presentation store or dialog layout
  choice would violate the charter. Hub validates/forwards typed effects;
  client harnesses model their application.
- **Cross-surface state mutation:** author-controlled scope could reach another
  surface. Derive scope from the admitted active Hub/package/surface context
  and fixture this isolation.
- **Rejected-action data loss:** a generic result applier could close or replace
  on rejection. Contract validation plus accepted/rejected fixtures must make
  retention explicit.
- **Schema drift:** separately handwritten Rust, TypeScript, JSON schema, and
  fixtures can disagree. Generate TS/schema/fixture bytes from the Rust-owned
  source and enforce symmetric parity.
- **Fixture provenance false proof:** target-shaped fixtures alone can bypass
  Hub serialization. Include source-shaped plugin Lua payloads through the
  real daemon path and assert the typed downstream result.
- **Protocol compatibility bookkeeping:** request semantics require a protocol
  bump, while fixture content has its own revision. Advance each for its own
  reason and derive published metadata from constants.
- **Flag-day compatibility failures:** protocol-3 clients will reject the new
  Hub until adoption. Land producer first, then TUI kit, TUI/Web/plugin
  adopters, Core removal, and integration; do not weaken compatibility checks.
- **Published package mismatch:** workspace-local imports can hide missing
  package files or dependencies. Pack exact tarballs, install in clean
  consumers, and verify metadata, schema, types, and fixtures.
- **Stale build artifacts:** same-version path dependencies can hide old DTO
  shapes. Use fresh target directories for the final live-Hub/downstream smoke.
- **Scope explosion into consumers/Core:** downstream renderer and Core removal
  work already has repository-routed tickets. Record dependency seams and stop
  at merged artifacts.

## Acceptance checks and downstream proof

### Contract unit and generation checks

- `./test.sh -p botster-ui-contract`
  - all moved node kinds and validators round-trip with unchanged serde shapes;
  - Dialog validates only through presence/presentation binding and rejects
    `props.open`;
  - presentation set/clear/toggle and cross-surface isolation validate;
  - presence and equality predicates resolve deterministically;
  - Form requires an explicit submit label;
  - form request values and non-form payload remain distinct;
  - accepted close/replacement validates and round-trips;
  - rejected results retain presentation/tree and reject accepted-only effects;
  - `UiTreeUpdateRef` and `tree_update` no longer deserialize or appear in
    generated Rust/TypeScript/schema output.
- Run the crate's deterministic TypeScript/schema/fixture generation check and
  assert symmetric Rust/TypeScript field, discriminant, type, and optionality
  parity.
- Demonstrate a narrow negative control: the new focused checks fail when
  `props.open` is admitted, old action envelope decoding is restored, or
  rejected effects are applied.

### Hub and protocol checks

- `./test.sh -p botster-hub-client`
- `./test.sh -p botster-hub-test-support`
- `./test.sh --test hub_lua_runtime_test`
- `./test.sh --test hub_client_api_test`
- `./test.sh --test hub_daemon_lifecycle_test`
- `./test.sh --test hub_test_support_conformance_test`
- A real isolated Hub/plugin-worker/daemon flow must:
  - render both the canonical plugin-contract-matrix fixture and the in-repo
    `examples/project-pipelines` first-party surface;
  - reject malformed/`props.open` trees at `HubRuntime::render_plugin_surface`;
  - serialize the validated typed snapshot;
  - accept only canonical `UiActionRequest` with exact
    package/surface/action/node/kind/values/payload identity;
  - return a typed rejected result without close/replacement;
  - return a typed accepted result with scoped close and validated replacement.
- The in-repo Project Pipelines example must prove both accepted and rejected
  actions through the real worker using `arguments.values`/`payload`, and its
  rendered Form must carry the explicit submit label. This proof complements,
  rather than substitutes for, the generic contract matrix.
- Confirm the production call chain is
  daemon request → `HubClientApi` → `HubRuntime` → plugin worker → new contract
  validation → typed daemon response. Source existence alone is insufficient.
- Run the full wrapper: `./test.sh`.

### Package and external-consumer checks

- Run `packages/ui-contract` generation/check and package tests.
- Run `node packages/hub-test-support/scripts/sync-assets.mjs --check`.
- Run `npm test --prefix packages/hub-test-support`.
- Run `npm pack --dry-run --json` and `npm pack --json` from both npm package
  directories with isolated temporary caches.
- Install the exact `@trybotster/ui-contract` tarball into a clean temporary
  Node consumer and prove imports of TypeScript declarations/schema/fixtures,
  package version, exported paths, and fixture checksums.
- Install the exact hub-test-support tarball into another clean consumer with
  the packed UI contract available and prove it resolves the pinned normal
  dependency and consumes the UI package fixtures while adding Hub-specific
  protocol/harness evidence.
- Verify the bumped hub-test-support version is unused before packing, and that
  its generated metadata, support matrix, README, dependency pin, protocol
  version, and conformance revision agree.
- Inspect packed contents for missing files and local paths/secrets. If
  publication is blocked by credentials/2FA, attach the verified tarball
  evidence and exact operator publish command instead of opening another
  ticket.

### Repository gates

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `git diff --check`
- Search current production/test-support sources for direct
  `botster_core::Ui*`, copied UI TypeScript declarations/fixtures under
  hub-test-support, the old `PluginSurfaceAction` tuple, `props.open`
  exceptions, and compatibility aliases. Expected matches must be only
  intentional negative tests or historical documentation.

Downstream Web/TUI renderer click/keyboard proof belongs to their target
repository tickets. This producer ticket supplies the normal merged Cargo/npm
artifacts and shared fixtures they must consume; it must not use local
sibling-worktree overrides to simulate those later adoptions.

## Vault gaps worth capturing

- Capture the settled typed wire shape for surface-scoped presentation
  actions/predicates and accepted replacement semantics after implementation;
  the existing modal-state note names the ownership decision but not this
  standalone cross-client schema.
- Capture the package boundary that `@trybotster/ui-contract` owns canonical
  fixtures while hub-test-support pins and consumes them only if implementation
  confirms this is a durable pattern for other shared contracts.
- Capture any generator rule needed to keep Rust serde, TypeScript, and JSON
  schema parity if the implementation discovers a repeatable failure mode not
  already covered by the loaded DTO drift notes.
- Capture the charter amendment: this ticket/project intentionally supersedes
  the Hub charter's general shared-contract exclusion by following
  [[botster core ui and capability contracts must avoid product gravity]] and
  placing product-speed UI vocabulary in a sibling package.
- No additional vault capture is warranted at Plan time; these are candidates
  contingent on implemented evidence.

## Convention fit

There is one explicit charter exception, not a silent no-conflict claim:
[[botster-hub-playbook]] normally excludes reusable shared contracts and
[[botster-hub-client-playbook]] excludes Core UI implementation types. The
ticket and project north star deliberately supersede that older ownership rule
for this UI surface, supported by
[[botster core ui and capability contracts must avoid product gravity]]'s
sibling-contract-package guidance. The new crate remains separate from Hub
runtime policy and from the narrow daemon-client crate, so the exception does
not turn Hub runtime into a design-system monolith.

Otherwise the plan follows the cold-switch instruction, keeps the Hub as
trusted validator/router rather than renderer, keeps hub-test-support
downstream-shaped, uses the repository wrapper for Rust tests, and makes
generated/published artifacts prove the actual consumer path.
